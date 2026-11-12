"""页格式。

所有持久化数据都被切成固定 4096 字节的页。页是 I/O 的最小单位，
也是 Buffer Pool 的管理单位，还是 WAL 物理日志的作用对象。

字节级布局见 docs/storage-format.md。这里只强调两个不变量：

1. **RID 稳定**：堆页删除记录时槽位保留为 tombstone（``length = 0``），
   槽位编号永不复用。MVCC 版本链依赖这一点——旧版本即使"已删除"，
   也必须还能通过原 RID 找到，否则回滚和快照读都会拿到错误数据。
2. **页自描述**：页头里存了自己的 ``page_id``。写入错误的页会被
   Buffer Pool 的断言抓住，而不是悄悄污染另一个表。
"""

from __future__ import annotations

import struct
from enum import IntEnum
from typing import Iterator, Optional, Tuple

PAGE_SIZE = 4096
INVALID_PAGE_ID = 0xFFFFFFFF
META_PAGE_ID = 0

#: 元数据页魔数 "LMND"
MAGIC = 0x4C4D4E44

# 通用页头偏移（heap 与 btree 共用前 32 字节的骨架）
OFF_PAGE_TYPE = 0
OFF_SLOT_OR_KEYS = 2
OFF_FREE_START = 4
OFF_FREE_END = 6
OFF_NEXT_PAGE = 8
OFF_PREV_PAGE = 12
OFF_PAGE_ID = 16
OFF_PAGE_LSN = 24  # u64：日志偏移可能超过 32 位，因此不能压成 u32
HEADER_SIZE = 32

_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")


class PageType(IntEnum):
    FREE = 0
    HEAP = 1
    BTREE_INTERNAL = 2
    BTREE_LEAF = 3
    META = 4
    CATALOG = 5


class Page:
    """一页原始字节 + 通用页头访问。

    子类（``HeapPage`` / B+ 树的叶页与内部页）在这层之上解释自己的布局。
    """

    __slots__ = ("page_id", "buf")

    def __init__(
        self,
        page_id: int = INVALID_PAGE_ID,
        data: Optional[bytes | bytearray] = None,
    ) -> None:
        if data is None:
            self.buf = bytearray(PAGE_SIZE)
            self.page_id = page_id
            self.set_u16(OFF_PAGE_TYPE, PageType.FREE)
        else:
            if len(data) != PAGE_SIZE:
                raise ValueError(f"页大小必须是 {PAGE_SIZE}，收到 {len(data)}")
            self.buf = bytearray(data)
            self.page_id = page_id

    # -- 通用整数读写 -------------------------------------------------------

    def get_u8(self, off: int) -> int:
        return self.buf[off]

    def set_u8(self, off: int, v: int) -> None:
        self.buf[off] = v & 0xFF

    def get_u16(self, off: int) -> int:
        return _U16.unpack_from(self.buf, off)[0]

    def set_u16(self, off: int, v: int) -> None:
        _U16.pack_into(self.buf, off, v & 0xFFFF)

    def get_u32(self, off: int) -> int:
        return _U32.unpack_from(self.buf, off)[0]

    def set_u32(self, off: int, v: int) -> None:
        _U32.pack_into(self.buf, off, v & 0xFFFFFFFF)

    def get_u64(self, off: int) -> int:
        return _U64.unpack_from(self.buf, off)[0]

    def set_u64(self, off: int, v: int) -> None:
        _U64.pack_into(self.buf, off, v & 0xFFFFFFFFFFFFFFFF)

    # -- 页头属性 -----------------------------------------------------------

    @property
    def page_type(self) -> int:
        return self.get_u16(OFF_PAGE_TYPE)

    @page_type.setter
    def page_type(self, t: int) -> None:
        self.set_u16(OFF_PAGE_TYPE, t)

    @property
    def next_page(self) -> int:
        return self.get_u32(OFF_NEXT_PAGE)

    @next_page.setter
    def next_page(self, pid: int) -> None:
        self.set_u32(OFF_NEXT_PAGE, pid)

    @property
    def prev_page(self) -> int:
        return self.get_u32(OFF_PREV_PAGE)

    @prev_page.setter
    def prev_page(self, pid: int) -> None:
        self.set_u32(OFF_PREV_PAGE, pid)

    @property
    def page_lsn(self) -> int:
        return self.get_u64(OFF_PAGE_LSN)

    @page_lsn.setter
    def page_lsn(self, lsn: int) -> None:
        self.set_u64(OFF_PAGE_LSN, lsn)

    def view(self, cls: type["Page"]) -> "Page":
        """返回同一块字节上的另一种页视图。

        Buffer Pool 只缓存字节（``Page``），具体布局由上层按 ``page_type``
        解释。视图与原始页共享 ``buf``，因此修改立刻对缓存生效；这避免了
        "同一页先被通用方式加载、后被特定方式访问"时的类型冲突。
        """
        obj = object.__new__(cls)
        obj.page_id = self.page_id
        obj.buf = self.buf
        return obj

    def __repr__(self) -> str:  # pragma: no cover
        return f"<{type(self).__name__} id={self.page_id} type={self.page_type}>"


class HeapPage(Page):
    """slotted page：页头 + 槽目录（向下）+ 记录区（自页尾向上）。"""

    __slots__ = ()

    # -- 布局属性 -----------------------------------------------------------

    @property
    def nslots(self) -> int:
        return self.get_u16(OFF_SLOT_OR_KEYS)

    @nslots.setter
    def nslots(self, n: int) -> None:
        self.set_u16(OFF_SLOT_OR_KEYS, n)

    @property
    def free_start(self) -> int:
        return self.get_u16(OFF_FREE_START)

    @free_start.setter
    def free_start(self, v: int) -> None:
        self.set_u16(OFF_FREE_START, v)

    @property
    def free_end(self) -> int:
        return self.get_u16(OFF_FREE_END)

    @free_end.setter
    def free_end(self, v: int) -> None:
        self.set_u16(OFF_FREE_END, v)

    # -- 生命周期 -----------------------------------------------------------

    def init_heap(self, page_id: int) -> None:
        """清空并初始化为一个空的堆页。"""
        self.buf[:] = bytes(PAGE_SIZE)
        self.page_id = page_id
        self.set_u16(OFF_PAGE_TYPE, PageType.HEAP)
        self.set_u16(OFF_SLOT_OR_KEYS, 0)
        self.set_u16(OFF_FREE_START, HEADER_SIZE)
        self.set_u16(OFF_FREE_END, PAGE_SIZE)
        self.set_u32(OFF_NEXT_PAGE, INVALID_PAGE_ID)
        self.set_u32(OFF_PREV_PAGE, INVALID_PAGE_ID)
        self.set_u32(OFF_PAGE_ID, page_id)
        self.page_lsn = 0

    # -- 空间核算 -----------------------------------------------------------

    @property
    def free_space(self) -> int:
        return self.free_end - self.free_start

    @property
    def slot_dir_end(self) -> int:
        """最后一个槽目录项的结束偏移。"""
        return HEADER_SIZE + 4 * self.nslots

    def can_fit(self, size: int) -> bool:
        """能否再插入一条 ``size`` 字节的记录（含 4 字节槽位开销）。"""
        return self.free_space >= size + 4

    @property
    def live_count(self) -> int:
        return sum(1 for _ in self.iter_live_slots())

    # -- 记录操作 -----------------------------------------------------------

    def _slot_entry_off(self, slot: int) -> int:
        return HEADER_SIZE + 4 * slot

    def insert_record(self, payload: bytes) -> Optional[int]:
        """插入记录，返回新槽位编号；空间不足返回 ``None``。"""
        size = len(payload)
        if size == 0:
            raise ValueError("记录体不能为空（空长度被保留给 tombstone）")
        if not self.can_fit(size):
            return None

        new_end = self.free_end - size
        self.buf[new_end : new_end + size] = payload

        slot = self.nslots
        entry = self._slot_entry_off(slot)
        self.set_u16(entry, new_end)
        self.set_u16(entry + 2, size)

        self.nslots = slot + 1
        self.free_start = entry + 4
        self.free_end = new_end
        return slot

    def get_record(self, slot: int) -> Optional[bytes]:
        """读取记录；槽位不存在或已删除返回 ``None``。"""
        if slot < 0 or slot >= self.nslots:
            return None
        entry = self._slot_entry_off(slot)
        off = self.get_u16(entry)
        length = self.get_u16(entry + 2)
        if length == 0:
            return None
        return bytes(self.buf[off : off + length])

    def record_length(self, slot: int) -> int:
        if slot < 0 or slot >= self.nslots:
            return 0
        return self.get_u16(self._slot_entry_off(slot) + 2)

    def delete_record(self, slot: int) -> bool:
        """标记删除：槽位变 tombstone，记录体留待 compact 回收。"""
        if slot < 0 or slot >= self.nslots:
            return False
        if self.record_length(slot) == 0:
            return False
        self.set_u16(self._slot_entry_off(slot) + 2, 0)
        return True

    def set_record(self, slot: int, payload: bytes) -> bool:
        """原地覆盖记录，仅当新内容不超过原长度时成功。

        MVCC 的 UPDATE 不使用这个接口——它总是"标记旧版本 + 插入新版本"，
        以保证快照读能看到旧值。
        """
        if slot < 0 or slot >= self.nslots:
            return False
        entry = self._slot_entry_off(slot)
        old_len = self.get_u16(entry + 2)
        if old_len == 0 or len(payload) > old_len:
            return False
        off = self.get_u16(entry)
        self.buf[off : off + len(payload)] = payload
        self.set_u16(entry + 2, len(payload))
        return True

    def compact(self) -> int:
        """把存活记录重新紧凑排列，返回回收的字节数。

        tombstone 槽位**不回收**（RID 稳定性要求），只回收记录体占用的空间。
        """
        live: list[Tuple[int, bytes]] = []
        for slot in range(self.nslots):
            rec = self.get_record(slot)
            if rec is not None:
                live.append((slot, rec))

        before = PAGE_SIZE - self.free_end
        end = PAGE_SIZE
        for slot, rec in live:
            end -= len(rec)
            self.buf[end : end + len(rec)] = rec
            entry = self._slot_entry_off(slot)
            self.set_u16(entry, end)
            self.set_u16(entry + 2, len(rec))
        self.free_end = end
        after = PAGE_SIZE - self.free_end
        # 记录区占用从 before 降到 after，回收量是差值
        return before - after

    def iter_live_slots(self) -> Iterator[Tuple[int, bytes]]:
        for slot in range(self.nslots):
            rec = self.get_record(slot)
            if rec is not None:
                yield slot, rec

    def check_integrity(self) -> None:
        """结构性自检。任何一条不满足都说明页已损坏。"""
        from ..errors import CorruptedPageError

        pid = self.get_u32(OFF_PAGE_ID)
        if pid != self.page_id:
            raise CorruptedPageError(
                "页头 page_id 与所在位置不符",
                detail=f"header={pid} actual={self.page_id}",
            )
        if self.free_start < HEADER_SIZE or self.free_end > PAGE_SIZE:
            raise CorruptedPageError(
                "空闲指针越界",
                detail=f"free_start={self.free_start} free_end={self.free_end}",
            )
        if self.free_start > self.free_end:
            raise CorruptedPageError(
                "槽目录与记录区重叠",
                detail=f"free_start={self.free_start} free_end={self.free_end}",
            )
        if self.free_start != HEADER_SIZE + 4 * self.nslots:
            raise CorruptedPageError(
                "free_start 与槽数量不一致",
                detail=f"free_start={self.free_start} nslots={self.nslots}",
            )
        for slot in range(self.nslots):
            entry = self._slot_entry_off(slot)
            off = self.get_u16(entry)
            length = self.get_u16(entry + 2)
            if length == 0:
                continue
            if off < self.free_end or off + length > PAGE_SIZE:
                raise CorruptedPageError(
                    f"槽 {slot} 指向记录区之外",
                    detail=f"off={off} len={length} free_end={self.free_end}",
                )


__all__ = [
    "PAGE_SIZE",
    "INVALID_PAGE_ID",
    "META_PAGE_ID",
    "MAGIC",
    "HEADER_SIZE",
    "PageType",
    "Page",
    "HeapPage",
]
