"""记录的二进制编解码。

一条物理记录由三部分组成：

1. **MVCC 头**：谁插入的、谁删的、版本链前驱、逻辑根是谁、最新版本在哪；
2. **NULL 位图**：每个列一位，1 表示该列为 NULL；
3. **列数据**：非 NULL 的列按 schema 顺序紧凑排列。

把 MVCC 头放在行内（而不是外部映射表）的好处是：扫描一页就能完成可见性
判断，不需要额外的随机 I/O；代价是每行多 36 字节。
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence, Tuple

from ..datatypes import TypeSpec, decode_value, encode_value
from ..errors import CorruptedPageError, RowTooLargeError
from ..storage.page import INVALID_PAGE_ID, PAGE_SIZE

#: 版本链与根指针里的"无"值
INVALID_SLOT = 0xFFFF

FLAG_DELETED = 0x01  # 保留：逻辑删除标记（当前用 xmax 表达删除）
FLAG_ROOT = 0x02  # 本记录是某个逻辑行的根（索引指向它）
FLAG_ABORTED = 0x04  # 该版本所属事务已回滚

# u64 xmin | u64 xmax | u32 prev_page | u16 prev_slot | u8 flags | u8 reserved
# u32 root_page | u16 root_slot | u32 newest_page | u16 newest_slot | u16 ncols
_HDR = struct.Struct("<QQIHBBIHIHH")
HEADER_SIZE = _HDR.size  # 38

assert HEADER_SIZE == 38, HEADER_SIZE

RID = Tuple[int, int]  # (page_id, slot_id)


def rid_tuple(page_id: int, slot_id: int) -> RID:
    return (page_id, slot_id)


def is_valid_rid(page_id: int, slot: int) -> bool:
    return page_id != INVALID_PAGE_ID and slot != INVALID_SLOT


@dataclass
class RecordHeader:
    xmin: int = 0
    xmax: int = 0
    prev_page: int = INVALID_PAGE_ID
    prev_slot: int = INVALID_SLOT
    flags: int = 0
    reserved: int = 0
    root_page: int = INVALID_PAGE_ID
    root_slot: int = INVALID_SLOT
    newest_page: int = INVALID_PAGE_ID
    newest_slot: int = INVALID_SLOT
    ncols: int = 0

    @property
    def is_root(self) -> bool:
        return bool(self.flags & FLAG_ROOT)

    @property
    def is_aborted(self) -> bool:
        return bool(self.flags & FLAG_ABORTED)

    @property
    def prev_rid(self) -> Optional[RID]:
        if is_valid_rid(self.prev_page, self.prev_slot):
            return (self.prev_page, self.prev_slot)
        return None

    @property
    def root_rid(self) -> Optional[RID]:
        if is_valid_rid(self.root_page, self.root_slot):
            return (self.root_page, self.root_slot)
        return None

    @property
    def newest_rid(self) -> Optional[RID]:
        if is_valid_rid(self.newest_page, self.newest_slot):
            return (self.newest_page, self.newest_slot)
        return None


@dataclass
class DecodedRecord:
    header: RecordHeader
    values: list[Any]
    size: int

    @property
    def is_tombstone(self) -> bool:
        return self.header.flags & FLAG_DELETED and False


def null_bitmap_size(ncols: int) -> int:
    return (ncols + 7) // 8


def estimate_max_row_size(columns: Sequence) -> int:
    """最坏情况下的行大小，用于提前拒绝而不是写到一半失败。"""
    ncols = len(columns)
    return (
        HEADER_SIZE
        + null_bitmap_size(ncols)
        + sum(col.type.storage_size_upper_bound() for col in columns)
    )


def encode_record(
    columns: Sequence,
    values: Iterable[Any],
    *,
    xmin: int,
    xmax: int = 0,
    prev: Optional[RID] = None,
    flags: int = 0,
    root: Optional[RID] = None,
    newest: Optional[RID] = None,
) -> bytes:
    """把一行 Python 值编码成字节。

    每个值都会被列的 ``TypeSpec.coerce`` 校验一遍——越界、类型不符、字符串
    超长都会在这里被拦下，而不是变成磁盘上的垃圾。
    """
    vals = list(values)
    ncols = len(columns)
    if len(vals) != ncols:
        raise ValueError(f"列数为 {ncols}，收到 {len(vals)} 个值")

    coerced: list[Any] = []
    for col, v in zip(columns, vals):
        cv = col.type.coerce(v)
        if cv is None and not col.nullable:
            from ..errors import NotNullViolationError

            raise NotNullViolationError(f"列 {col.name} 不允许 NULL")
        coerced.append(cv)

    bm_bytes = null_bitmap_size(ncols)
    bitmap = bytearray(bm_bytes)
    for i, v in enumerate(coerced):
        if v is None:
            bitmap[i // 8] |= 1 << (i % 8)

    data_parts: list[bytes] = []
    for col, v in zip(columns, coerced):
        if v is None:
            continue
        data_parts.append(encode_value(col.type, v))
    body = b"".join(data_parts)

    prev_page, prev_slot = prev if prev else (INVALID_PAGE_ID, INVALID_SLOT)
    root_page, root_slot = root if root else (INVALID_PAGE_ID, INVALID_SLOT)
    newest_page, newest_slot = newest if newest else (INVALID_PAGE_ID, INVALID_SLOT)

    hdr = _HDR.pack(
        xmin,
        xmax,
        prev_page,
        prev_slot,
        flags,
        0,
        root_page,
        root_slot,
        newest_page,
        newest_slot,
        ncols,
    )
    record = hdr + bytes(bitmap) + body
    if len(record) > PAGE_SIZE:
        raise RowTooLargeError(
            f"单行大小 {len(record)} 字节超过页容量 {PAGE_SIZE}",
            hint="拆分表结构，或缩短 TEXT/VARCHAR 内容；本引擎不支持溢出页",
        )
    return record


def decode_header(data: bytes) -> RecordHeader:
    if len(data) < HEADER_SIZE:
        raise CorruptedPageError("记录头被截断", detail=f"len={len(data)}")
    fields = _HDR.unpack_from(data, 0)
    return RecordHeader(*fields)


def decode_record(
    columns: Sequence,
    data: bytes,
    *,
    allow_missing: bool = False,
) -> DecodedRecord:
    header = decode_header(data)
    ncols = len(columns)
    if header.ncols > ncols:
        raise CorruptedPageError(
            "记录的列数比表定义多",
            detail=f"record ncols={header.ncols} schema ncols={ncols}",
        )
    if header.ncols < ncols and not allow_missing:
        raise CorruptedPageError(
            "记录的列数与表定义不一致",
            detail=f"record ncols={header.ncols} schema ncols={ncols}",
            hint="表结构在不支持 ALTER TABLE 的情况下被改动过",
        )

    stored = header.ncols
    bm_bytes = null_bitmap_size(stored)
    pos = HEADER_SIZE
    bitmap = data[pos : pos + bm_bytes]
    pos += bm_bytes

    values: list[Any] = []
    for i in range(stored):
        col = columns[i]
        if bitmap[i // 8] & (1 << (i % 8)):
            values.append(None)
            continue
        v, pos = decode_value(col.type, data, pos)
        values.append(v)
    # ALTER TABLE ADD COLUMN 之后的旧记录没有新列：按默认值补齐
    for i in range(stored, ncols):
        values.append(columns[i].default)

    return DecodedRecord(header=header, values=values, size=len(data))


__all__ = [
    "RID",
    "INVALID_SLOT",
    "FLAG_DELETED",
    "FLAG_ROOT",
    "FLAG_ABORTED",
    "HEADER_SIZE",
    "RecordHeader",
    "DecodedRecord",
    "encode_record",
    "decode_record",
    "decode_header",
    "null_bitmap_size",
    "estimate_max_row_size",
    "rid_tuple",
]
