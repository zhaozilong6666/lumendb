"""预写日志（Write-Ahead Log）。

WAL 是 LumenDB 能够"崩溃后仍然正确"的唯一理由。核心规则只有一条：

    任何脏页落盘之前，覆盖该页修改的日志必须已经落盘。

日志记录是**物理页日志**：记录某个页某个偏移的旧值和新值。选择物理日志
而不是逻辑日志，是为了让 redo 天然幂等——同一批日志重放两次的结果与一次
完全相同，恢复逻辑因此可以被直接断言，而不需要"相信"它。
"""

from __future__ import annotations

import os
import struct
import threading
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Iterator

from ..errors import StorageError
from .page import PAGE_SIZE

# u64 lsn + u32 xid + u8 type + u32 payload_len = 17 字节
_HEADER = struct.Struct("<QIBI")
HEADER_SIZE = _HEADER.size


class LogType(IntEnum):
    BEGIN = 1
    COMMIT = 2
    ABORT = 3
    UPDATE = 4  # 页内物理更新（带 before/after image）
    CLR = 5  # 补偿记录，undo 时用来阻止二次回滚
    CHECKPOINT = 6
    PAGE_IMAGE = 7  # 新页的完整镜像，用于让物理副本重建页分配后的初始状态


@dataclass(frozen=True)
class LogRecord:
    lsn: int
    xid: int
    rtype: int
    payload: bytes

    @property
    def is_update_like(self) -> bool:
        return self.rtype in (LogType.UPDATE, LogType.CLR, LogType.PAGE_IMAGE)

    def encode(self) -> bytes:
        return _HEADER.pack(self.lsn, self.xid, self.rtype, len(self.payload)) + self.payload

    @classmethod
    def decode(cls, buf: bytes, pos: int) -> tuple["LogRecord", int]:
        if pos + HEADER_SIZE > len(buf):
            raise StorageError("WAL 记录头被截断")
        lsn, xid, rtype, plen = _HEADER.unpack_from(buf, pos)
        pos += HEADER_SIZE
        if pos + plen > len(buf):
            raise StorageError(
                "WAL 记录体被截断",
                detail=f"lsn={lsn} payload_len={plen}",
            )
        payload = bytes(buf[pos : pos + plen])
        return cls(lsn, xid, rtype, payload), pos + plen


# ---------------------------------------------------------------------------
# 页更新的 payload 编解码
# ---------------------------------------------------------------------------

_PAGE_UPDATE_HEAD = struct.Struct("<IHH")  # page_id, offset, old_len


def encode_page_update(page_id: int, offset: int, old: bytes, new: bytes) -> bytes:
    """``u32 page_id | u16 offset | u16 old_len | old | u16 new_len | new``"""
    return (
        _PAGE_UPDATE_HEAD.pack(page_id, offset, len(old))
        + old
        + struct.pack("<H", len(new))
        + new
    )


def decode_page_update(payload: bytes) -> tuple[int, int, bytes, bytes]:
    if len(payload) < _PAGE_UPDATE_HEAD.size:
        raise StorageError("页更新日志头被截断")
    page_id, offset, old_len = _PAGE_UPDATE_HEAD.unpack_from(payload, 0)
    pos = _PAGE_UPDATE_HEAD.size
    if pos + old_len + 2 > len(payload):
        raise StorageError("页更新日志的 before image 被截断")
    old = payload[pos : pos + old_len]
    pos += old_len
    new_len = struct.unpack_from("<H", payload, pos)[0]
    pos += 2
    if pos + new_len > len(payload):
        raise StorageError("页更新日志的 after image 被截断")
    new = payload[pos : pos + new_len]
    return page_id, offset, bytes(old), bytes(new)


_PAGE_IMAGE_HEAD = struct.Struct("<I")  # page_id


def encode_page_image(page_id: int, image: bytes) -> bytes:
    """``u32 page_id | 完整页字节``"""
    if len(image) != PAGE_SIZE:
        raise StorageError("页镜像长度不是 PAGE_SIZE")
    return _PAGE_IMAGE_HEAD.pack(page_id) + bytes(image)


def decode_page_image(payload: bytes) -> tuple[int, bytes]:
    if len(payload) < _PAGE_IMAGE_HEAD.size:
        raise StorageError("页镜像日志头被截断")
    page_id = _PAGE_IMAGE_HEAD.unpack_from(payload, 0)[0]
    image = payload[_PAGE_IMAGE_HEAD.size :]
    if len(image) != PAGE_SIZE:
        raise StorageError(
            "页镜像长度不匹配",
            detail=f"page_id={page_id} len={len(image)}",
        )
    return page_id, bytes(image)


def encode_checkpoint(active_xids: list[int]) -> bytes:
    return struct.pack("<I", len(active_xids)) + b"".join(
        struct.pack("<I", x) for x in active_xids
    )


def decode_checkpoint(payload: bytes) -> list[int]:
    if len(payload) < 4:
        raise StorageError("checkpoint 日志被截断")
    n = struct.unpack_from("<I", payload, 0)[0]
    return [struct.unpack_from("<I", payload, 4 + 4 * i)[0] for i in range(n)]


# ---------------------------------------------------------------------------
# WAL 写入器 / 读取器
# ---------------------------------------------------------------------------


class WAL:
    """追加写日志。

    LSN 定义为"记录在日志文件中的字节偏移 + 1"。用偏移而不是抽象序号，
    好处是 ``flush(lsn)`` 可以直接退化成"文件长度是否已经覆盖 lsn"，
    不需要维护第二套映射表。0 保留表示"无日志"。
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        self._fh = open(self.path, "ab")
        self._size = os.path.getsize(self.path) if existed else 0
        self._pending = bytearray()
        self._next_lsn = self._size + 1
        self._last_lsn = 0
        # 多个提交线程可能同时追加；文件写入与 pending 必须成组保护。
        self._lock = threading.RLock()
        if self._size:
            for record in self.iter_records(1):
                self._last_lsn = record.lsn

    # -- 属性 ---------------------------------------------------------------

    @property
    def last_lsn(self) -> int:
        """最后一条记录自身的 LSN（不是下一条的字节偏移）。"""
        with self._lock:
            return self._last_lsn

    @property
    def next_lsn(self) -> int:
        with self._lock:
            return self._next_lsn

    @property
    def durable_lsn(self) -> int:
        """已经落盘的最后一条记录的 lsn（0 表示日志为空）。"""
        with self._lock:
            return self._size if self._size else 0

    # -- 追加 ---------------------------------------------------------------

    def append(self, xid: int, rtype: int, payload: bytes = b"") -> int:
        with self._lock:
            lsn = self._next_lsn
            rec = LogRecord(lsn, xid, int(rtype), payload)
            data = rec.encode()
            self._pending += data
            self._next_lsn += len(data)
            self._last_lsn = lsn
            return lsn

    def begin(self, xid: int) -> int:
        return self.append(xid, LogType.BEGIN)

    def commit(self, xid: int) -> int:
        return self.append(xid, LogType.COMMIT)

    def abort(self, xid: int) -> int:
        return self.append(xid, LogType.ABORT)

    def log_page_update(
        self,
        xid: int,
        page_id: int,
        offset: int,
        old: bytes,
        new: bytes,
        *,
        clr: bool = False,
    ) -> int:
        payload = encode_page_update(page_id, offset, old, new)
        return self.append(xid, LogType.CLR if clr else LogType.UPDATE, payload)

    def log_page_image(self, xid: int, page_id: int, image: bytes) -> int:
        return self.append(xid, LogType.PAGE_IMAGE, encode_page_image(page_id, image))

    def checkpoint(self, active_xids: list[int]) -> int:
        return self.append(0, LogType.CHECKPOINT, encode_checkpoint(active_xids))

    # -- 持久化 -------------------------------------------------------------

    def flush(self) -> int:
        """把内存中的日志写入文件并 fsync，返回已持久化的最后 lsn。

        这是"WAL 规则"的执行点：Buffer Pool 在写任何脏页之前必须调用它。
        多个调用者可以安全并发；只有真正有 pending 的调用会做一次 ``fsync``，
        这使后到的提交可以搭上前一次刷盘的顺风车。
        """
        with self._lock:
            if self._pending:
                self._fh.write(self._pending)
                self._pending.clear()
                self._fh.flush()
                os.fsync(self._fh.fileno())
                self._size = self._next_lsn - 1
            else:
                # 即使没有新日志，也可能需要把之前的写刷下去
                self._fh.flush()
            return self._size

    def flush_upto(self, lsn: int) -> None:
        """确保所有 lsn <= 目标值的记录都已落盘。"""
        if lsn <= 0:
            return
        with self._lock:
            if lsn <= self._size:
                return
            self.flush()

    # -- 读取 ---------------------------------------------------------------

    def iter_records(self, start_lsn: int = 1) -> Iterator[LogRecord]:
        """从 start_lsn 开始顺序读取日志。会先把 pending 刷盘，保证视图一致。"""
        with self._lock:
            self.flush()
            with open(self.path, "rb") as fh:
                buf = fh.read()
        pos = 0
        while pos + HEADER_SIZE <= len(buf):
            try:
                rec, pos = LogRecord.decode(buf, pos)
            except StorageError:
                # 日志尾巴被截断（崩溃时正在写），丢弃残尾
                break
            if rec.lsn < start_lsn:
                continue
            yield rec

    def read_all(self) -> list[LogRecord]:
        return list(self.iter_records(1))

    def close(self) -> None:
        with self._lock:
            self.flush()
            if not self._fh.closed:
                self._fh.close()

    def __enter__(self) -> "WAL":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover
        return f"<WAL {self.path} next_lsn={self._next_lsn}>"


__all__ = [
    "WAL",
    "LogRecord",
    "LogType",
    "HEADER_SIZE",
    "encode_page_update",
    "decode_page_update",
    "encode_checkpoint",
    "decode_checkpoint",
]
