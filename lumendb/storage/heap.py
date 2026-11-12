"""堆文件：一个表的所有行版本。

设计要点
========

**版本链方向**：每次 UPDATE 写一个新版本，新版本通过 ``prev`` 指回旧版本，
旧版本被置上 ``xmax``。于是"最新的在链头，回溯能拿到更旧的值"。

**索引如何找到它**：索引里存的是**根 RID**（第一次插入那条记录的 RID），
根记录始终不变；根记录头部有一个 ``newest`` 指针，指向当前最新版本。
这样 UPDATE 不需要改索引，回滚也不需要恢复索引——这是选择"根记录 + 最新
指针"而不是"索引直接存最新 RID"的原因。

**扫描如何去重**：扫描时遍历所有物理版本，用 MVCC 可见性过滤。快照隔离
保证了同一个逻辑行在同一时刻最多只有一个版本可见，所以不需要额外的去重。

**日志粒度**：插入/删除记录时按"页头 + 槽位 + 记录体"三段写物理日志，
而不是整页镜像。整页镜像会让每行产生 8KB 日志，一万行就是 80MB。
"""

from __future__ import annotations

import struct
from typing import Any, Iterator, Optional, Sequence, Tuple

from ..catalog.schema import TableDef
from ..errors import CorruptedPageError, RowTooLargeError, StorageError
from .bufferpool import BufferPoolManager
from .page import INVALID_PAGE_ID, PAGE_SIZE, HeapPage
from .record import (
    FLAG_ROOT,
    DecodedRecord,
    RecordHeader,
    RID,
    decode_record,
    encode_record,
    rid_tuple,
)
from .txn import Transaction, TransactionManager

#: 一页最多能放多少槽位（受页头之后的目录空间限制）
_MAX_SLOTS = (PAGE_SIZE - 32) // 4

#: 找空闲页时最多向前回退多少页，超过就新开一页，避免退化成 O(n²)
_MAX_BACKWARD_SCAN = 8


class HeapFile:
    """一个表的堆文件。所有方法都要求传入活跃事务。"""

    def __init__(
        self,
        buffer_pool: BufferPoolManager,
        table: TableDef,
        txn_mgr: TransactionManager,
    ) -> None:
        self._bp = buffer_pool
        self.table = table
        self._txns = txn_mgr

    # ------------------------------------------------------------------
    # 页管理
    # ------------------------------------------------------------------

    def _fetch(self, page_id: int) -> HeapPage:
        return self._bp.fetch_page(page_id, HeapPage)  # type: ignore[return-value]

    def _release(self, page: HeapPage, *, dirty: bool = False) -> None:
        self._bp.unpin(page.page_id, dirty=dirty)

    def _append_page(self, txn: Transaction) -> HeapPage:
        """在链尾追加一个新页，返回处于 pin 状态的页。"""
        page = self._bp.new_page(HeapPage)
        # 新页的"初始化"本身必须是一条能重放的日志：否则崩溃恢复后这一页
        # 仍是磁盘上的空页（page_type=FREE），而后续的行插入日志只覆盖了
        # 页头里的几个计数器，页类型永远补不回来。
        old_init = bytes(page.buf)
        page.init_heap(page.page_id)
        self._bp.log_page_update(txn.xid, page, 0, old_init, bytes(page.buf))
        self._bp.mark_dirty(page)

        last_id = self.table.last_page
        if last_id == INVALID_PAGE_ID:
            self.table.root_page = page.page_id
        else:
            link = self._fetch(last_id)
            try:
                old_next = bytes(link.buf[8:12])
                new_next = struct.pack("<I", page.page_id)
                self._apply_and_log(txn, link, 8, new_next, old_next)
                # 新页的 prev 指针也要进日志，否则恢复后反向链会断
                old_prev = bytes(page.buf[12:16])
                self._apply_and_log(
                    txn, page, 12, struct.pack("<I", link.page_id), old_prev
                )
            finally:
                self._release(link, dirty=True)
        self.table.last_page = page.page_id
        return page

    def _find_page_with_space(self, txn: Transaction, size: int) -> HeapPage:
        """从链尾向前找一段有空间的页；都满了就新开一页。"""
        page_id = self.table.last_page
        steps = 0
        while page_id != INVALID_PAGE_ID and steps <= _MAX_BACKWARD_SCAN:
            page = self._fetch(page_id)
            if page.can_fit(size):
                return page
            prev_id = page.prev_page
            self._release(page)
            page_id = prev_id
            steps += 1
        return self._append_page(txn)

    # ------------------------------------------------------------------
    # 物理写入原语（都带 WAL）
    # ------------------------------------------------------------------

    def _apply_and_log(
        self,
        txn: Transaction,
        page: HeapPage,
        offset: int,
        new: bytes,
        old: Optional[bytes] = None,
    ) -> None:
        if old is None:
            old = bytes(page.buf[offset : offset + len(new)])
        self._bp.log_page_update(txn.xid, page, offset, old, new)
        page.buf[offset : offset + len(new)] = new
        self._bp.mark_dirty(page)

    def _insert_raw(self, txn: Transaction, page: HeapPage, payload: bytes) -> Optional[int]:
        """把一条已编码的记录放进页里，返回槽位号。"""
        size = len(payload)
        if not page.can_fit(size):
            return None

        slot = page.nslots
        entry_off = 32 + 4 * slot
        new_end = page.free_end - size

        # ① 页头：nslots / free_start / free_end
        old_hdr = bytes(page.buf[2:8])
        new_hdr = struct.pack("<HHH", slot + 1, entry_off + 4, new_end)
        self._apply_and_log(txn, page, 2, new_hdr, old_hdr)

        # ② 槽目录项
        old_slot = bytes(page.buf[entry_off : entry_off + 4])
        new_slot = struct.pack("<HH", new_end, size)
        self._apply_and_log(txn, page, entry_off, new_slot, old_slot)

        # ③ 记录体
        old_body = bytes(page.buf[new_end : new_end + size])
        self._apply_and_log(txn, page, new_end, payload, old_body)

        return slot

    def _set_xmax(self, txn: Transaction, page: HeapPage, slot: int, xmax: int) -> None:
        """给某个版本打上"被谁删除/更新"的标记（行内 +8 处的 8 字节）。"""
        off = 8
        old = struct.pack("<Q", _record_u64(page, slot, off))
        self._apply_and_log(txn, page, self._abs_offset(page, slot, off), struct.pack("<Q", xmax), old)

    def _abs_offset(self, page: HeapPage, slot: int, rel: int) -> int:
        entry_off = 32 + 4 * slot
        rec_off = page.get_u16(entry_off)
        return rec_off + rel

    def _set_newest(
        self, txn: Transaction, page: HeapPage, slot: int, newest: RID
    ) -> None:
        """更新根记录的 newest 指针（+30 处 u32 page_id + u16 slot）。"""
        rel_newest = 30
        base = self._abs_offset(page, slot, rel_newest)
        old = bytes(page.buf[base : base + 6])
        new = struct.pack("<IH", newest[0], newest[1])
        self._apply_and_log(txn, page, base, new, old)

    # ------------------------------------------------------------------
    # 读
    # ------------------------------------------------------------------

    def _decode(self, page: HeapPage, slot: int) -> Optional[DecodedRecord]:
        raw = page.get_record(slot)
        if raw is None:
            return None
        return decode_record(self.table.columns, raw)

    def _read_rid(self, rid: RID) -> Optional[Tuple[HeapPage, DecodedRecord]]:
        """读取物理版本，返回仍处于 pin 状态的页。调用方负责 unpin。

        RID 指向不存在的页（例如索引里存着一个已被回收的页号）时返回 None，
        而不是让存储层的异常冒到查询层——对上层来说"这一行不存在"是正常结果。
        """
        page_id, slot = rid
        try:
            page = self._fetch(page_id)
        except StorageError:
            return None
        rec = self._decode(page, slot)
        if rec is None:
            self._release(page)
            return None
        return page, rec

    def _visible(self, txn: Transaction, rec: DecodedRecord) -> bool:
        h = rec.header
        return self._txns.is_visible(txn, h.xmin, h.xmax, h.flags)

    def resolve_root(self, rid: RID, header: RecordHeader) -> RID:
        """给定任意版本，返回该逻辑行的根 RID。"""
        if header.is_root and header.root_rid is None:
            return rid
        root = header.root_rid
        return root if root is not None else rid

    def newest_rid(self, root_rid: RID) -> RID:
        """根记录里的 newest 指针；没有则说明根自己就是最新的。"""
        got = self._read_rid(root_rid)
        if got is None:
            raise StorageError(f"根记录 {root_rid} 不存在")
        page, rec = got
        try:
            newest = rec.header.newest_rid
            return newest if newest is not None else root_rid
        finally:
            self._release(page)

    def find_version(self, txn: Transaction, rid: RID) -> Optional[Tuple[RID, DecodedRecord]]:
        """从一个 RID 出发，沿版本链回溯，找到对 txn 可见的那个版本。"""
        cur: Optional[RID] = rid
        visited = 0
        while cur is not None and visited < 10_000:
            got = self._read_rid(cur)
            if got is None:
                return None
            page, rec = got
            try:
                if self._visible(txn, rec):
                    return cur, rec
                cur = rec.header.prev_rid
            finally:
                self._release(page)
            visited += 1
        return None

    def get(self, txn: Transaction, root_rid: RID) -> Optional[list[Any]]:
        """按根 RID 取当前快照下可见的那一版。索引查找最终都会走到这里。"""
        newest = self.newest_rid(root_rid)
        found = self.find_version(txn, newest)
        return found[1].values if found else None

    def scan(self, txn: Transaction) -> Iterator[Tuple[RID, list[Any]]]:
        """顺序扫描所有可见行，产出 ``(根 RID, 值列表)``。

        产出根 RID 而不是物理 RID，是因为索引与后续 UPDATE/DELETE 都按根 RID
        定位逻辑行——把物理 RID 交给上层会让"更新后再删"指向错误的版本。
        """
        page_id = self.table.root_page
        while page_id != INVALID_PAGE_ID:
            page = self._fetch(page_id)
            next_id = page.next_page
            try:
                for slot in range(page.nslots):
                    rec = self._decode(page, slot)
                    if rec is None or not self._visible(txn, rec):
                        continue
                    root = self.resolve_root((page_id, slot), rec.header)
                    yield root, rec.values
            finally:
                self._release(page)
            page_id = next_id

    def count(self, txn: Transaction) -> int:
        return sum(1 for _ in self.scan(txn))

    # ------------------------------------------------------------------
    # 写
    # ------------------------------------------------------------------

    def insert(self, txn: Transaction, values: Sequence[Any]) -> RID:
        txn.require_active()
        payload = encode_record(
            self.table.columns,
            values,
            xmin=txn.xid,
            flags=FLAG_ROOT,
        )
        if len(payload) > PAGE_SIZE - 32 - 4:
            raise RowTooLargeError(
                f"行大小 {len(payload)} 字节，超出单页可容纳的上限",
                detail=f"列：{', '.join(self.table.column_names)}",
            )

        page = self._find_page_with_space(txn, len(payload))
        try:
            if page.nslots >= _MAX_SLOTS:
                raise StorageError("页的槽位目录已满")
            slot = self._insert_raw(txn, page, payload)
            if slot is None:
                raise StorageError(
                    "页空间核算与实际写入不一致",
                    detail=f"payload={len(payload)} free_space={page.free_space}",
                )
            self.table.row_count_hint += 1
            return (page.page_id, slot)
        finally:
            self._release(page, dirty=True)

    def _latest_version_rid(self, root_rid: RID) -> RID:
        """不判可见性，直接拿链上最新的物理版本。"""
        newest = self.newest_rid(root_rid)
        # newest 本身可能就是最新；沿 prev 正向没有指针，所以 newest 就是最新
        return newest

    def update(self, txn: Transaction, root_rid: RID, values: Sequence[Any]) -> RID:
        """写一条新版本，返回新版本的 RID。"""
        txn.require_active()
        newest = self._latest_version_rid(root_rid)
        got = self._read_rid(newest)
        if got is None:
            raise StorageError(f"要更新的行 {root_rid} 不存在")
        page, rec = got
        try:
            old_rid = newest
            old_header = rec.header
        finally:
            self._release(page)

        payload = encode_record(
            self.table.columns,
            values,
            xmin=txn.xid,
            prev=old_rid,
            root=root_rid,
            flags=0,
        )

        page = self._find_page_with_space(txn, len(payload))
        try:
            slot = self._insert_raw(txn, page, payload)
            if slot is None:
                raise StorageError("页空间核算与实际写入不一致")
            new_rid = (page.page_id, slot)
        finally:
            self._release(page, dirty=True)

        # 旧版本打上 xmax，并把根的 newest 指向新版本
        old_page = self._fetch(old_rid[0])
        try:
            self._set_xmax(txn, old_page, old_rid[1], txn.xid)
        finally:
            self._release(old_page, dirty=True)

        root_page = self._fetch(root_rid[0])
        try:
            self._set_newest(txn, root_page, root_rid[1], new_rid)
        finally:
            self._release(root_page, dirty=True)

        return new_rid

    def delete(self, txn: Transaction, root_rid: RID) -> int:
        """把当前可见版本标记为已删除，返回受影响行数（0 或 1）。"""
        txn.require_active()
        try:
            newest = self._latest_version_rid(root_rid)
        except StorageError:
            # RID 指向不存在的页/槽位：按"没有这一行"处理，而不是抛存储错误
            return 0
        found = self.find_version(txn, newest)
        if found is None:
            return 0
        rid, rec = found
        page = self._fetch(rid[0])
        try:
            self._set_xmax(txn, page, rid[1], txn.xid)
        finally:
            self._release(page, dirty=True)
        self.table.row_count_hint = max(0, self.table.row_count_hint - 1)
        return 1


def _record_u64(page: HeapPage, slot: int, rel: int) -> int:
    entry_off = 32 + 4 * slot
    rec_off = page.get_u16(entry_off)
    return struct.unpack_from("<Q", page.buf, rec_off + rel)[0]


__all__ = ["HeapFile", "_MAX_SLOTS"]
