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
from ..errors import CorruptedPageError, NotSupportedError, RowTooLargeError, StorageError
from .btree import BPlusTree, KeyCodec
from .bufferpool import BufferPoolManager
from .page import INVALID_PAGE_ID, PAGE_SIZE, HeapPage
from .record import (
    FLAG_ABORTED,
    FLAG_ROOT,
    DecodedRecord,
    RecordHeader,
    RID,
    decode_header,
    decode_record,
    encode_record,
    rid_tuple,
)
from .txn import Transaction, TransactionManager, TxnState

#: 一页最多能放多少槽位（受页头之后的目录空间限制）
_MAX_SLOTS = (PAGE_SIZE - 32) // 4

#: 在线 VACUUM 一次性建立版本链索引时允许的物理版本上限。
#: 每个版本只保留 RecordHeader（几十字节），但 100 万版本仍会到百 MB 级；
#: 超过上限就失败关闭，提示改用离线 VACUUM，避免"在线"把内存打满。
VACUUM_ONLINE_MAX_VERSIONS = 250_000

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
        # 页链形状（链尾指针、last_page、新页分配）的保护：页 latch 只护住
        # 单页内容，链的推进要靠这把跨 HeapFile 实例共享的表级锁。
        self._shape_lock = buffer_pool.shape_lock(table.name)

    # ------------------------------------------------------------------
    # 页管理
    # ------------------------------------------------------------------

    def _fetch(self, page_id: int) -> HeapPage:
        return self._bp.fetch_page(page_id, HeapPage)  # type: ignore[return-value]

    def _release(self, page: HeapPage, *, dirty: bool = False) -> None:
        self._bp.unpin(page.page_id, dirty=dirty)

    def _append_page(self, txn: Transaction) -> HeapPage:
        """在链尾追加一个新页，返回处于 pin 状态的页。"""
        page = self._bp.new_page(HeapPage, owner=txn)
        # 新页的"初始化"本身必须是一条能重放的日志：否则崩溃恢复后这一页
        # 仍是磁盘上的空页（page_type=FREE），而后续的行插入日志只覆盖了
        # 页头里的几个计数器，页类型永远补不回来。初始化与整页镜像日志
        # 在页 latch 内完成，避免刷盘线程看到只改了一半的页头。
        with page.latch:
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
        # "读 last_page → 判空间 → 必要时追加新页"必须原子，否则两个写者
        # 会各追加一页，链尾指针互相覆盖（探针里表现为页链断开 + 丢行）。
        with self._shape_lock:
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
        # 读旧值 → 写日志 → 写页三步必须原子：否则两个写者交错时，
        # 后者的 before image 已经含了前者的改动，WAL 重放会算错。
        with page.latch:
            if old is None:
                old = bytes(page.buf[offset : offset + len(new)])
            self._bp.log_page_update(txn.xid, page, offset, old, new)
            page.buf[offset : offset + len(new)] = new
            self._bp.mark_dirty(page)

    def _insert_raw(self, txn: Transaction, page: HeapPage, payload: bytes) -> Optional[int]:
        """把一条已编码的记录放进页里，返回槽位号。"""
        size = len(payload)
        # 选页时看到的空间在并发写者插进来之后可能已经不够了，所以这里
        # 在页 latch 里**重新核算**一次；核算失败返回 None，由调用方重选页。
        with page.latch:
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

    def _record_row_undo(self, txn: Transaction, undo) -> None:
        """保存点逻辑回滚：只在 ``capture_row_undo`` 打开时记录堆写逆操作。"""
        if txn.capture_row_undo:
            txn.row_undo_log.append(undo)

    def _set_flags(
        self, txn: Transaction, page: HeapPage, slot: int, flags: int
    ) -> None:
        """改记录头 flags（+22 处 1 字节），用于把版本标成 ABORTED。"""
        rel = 22
        with page.latch:
            base = self._abs_offset(page, slot, rel)
            old = bytes(page.buf[base : base + 1])
            self._apply_and_log(txn, page, base, bytes([flags & 0xFF]), old)

    def _set_xmax(self, txn: Transaction, page: HeapPage, slot: int, xmax: int) -> None:
        """给某个版本打上"被谁删除/更新"的标记（行内 +8 处的 8 字节）。"""
        off = 8
        with page.latch:
            old = struct.pack("<Q", _record_u64(page, slot, off))
            self._apply_and_log(
                txn,
                page,
                self._abs_offset(page, slot, off),
                struct.pack("<Q", xmax),
                old,
            )

    def _abs_offset(self, page: HeapPage, slot: int, rel: int) -> int:
        entry_off = 32 + 4 * slot
        rec_off = page.get_u16(entry_off)
        return rec_off + rel

    def _set_newest(
        self, txn: Transaction, page: HeapPage, slot: int, newest: RID
    ) -> None:
        """更新根记录的 newest 指针（+30 处 u32 page_id + u16 slot）。"""
        rel_newest = 30
        with page.latch:
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
        return self._decode_raw(raw)

    def _decode_raw(self, raw: bytes) -> DecodedRecord:
        return decode_record(self.table.columns, raw, allow_missing=True)

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
        try:
            with page.latch:
                raw = page.get_record(slot)
        except BaseException:
            self._release(page)
            raise
        if raw is None:
            self._release(page)
            return None
        return page, self._decode_raw(raw)

    def _visible(self, txn: Transaction, rec: DecodedRecord, *, ri: bool = False) -> bool:
        h = rec.header
        return self._txns.is_visible(txn, h.xmin, h.xmax, h.flags, ri=ri)

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

    def find_version(
        self, txn: Transaction, rid: RID, *, ri: bool = False
    ) -> Optional[Tuple[RID, DecodedRecord]]:
        """从一个 RID 出发，沿版本链回溯，找到对 txn 可见的那个版本。

        ``ri=True`` 时改用引用完整性可见性（"已提交即可见"），外键校验
        与级联收集必须走这条路径，否则并发提交的父/子行会被漏掉。
        """
        cur: Optional[RID] = rid
        visited = 0
        while cur is not None and visited < 10_000:
            got = self._read_rid(cur)
            if got is None:
                return None
            page, rec = got
            try:
                if self._visible(txn, rec, ri=ri):
                    return cur, rec
                cur = rec.header.prev_rid
            finally:
                self._release(page)
            visited += 1
        return None

    def _newest_for_root(self, root_rid: RID) -> Optional[RID]:
        got = self._read_rid(root_rid)
        if got is None:
            return None
        page, rec = got
        try:
            return rec.header.newest_rid or root_rid
        finally:
            self._release(page)

    def get(
        self, txn: Transaction, root_rid: RID, *, ri: bool = False
    ) -> Optional[list[Any]]:
        """按根 RID 取当前快照下可见的那一版。索引查找最终都会走到这里。

        根记录可能已经被在线 VACUUM 回收（整条版本链对所有快照都不可见），
        而索引项仍是陈旧的 RID。这里把"根不存在"当成行不存在返回 ``None``，
        与 ``find_version`` 的半途缺失语义一致；真正的页损坏仍由
        ``_read_rid`` 抛 ``CorruptedPageError``，不会被吞掉。

        并发热点更新还有一个瞬时窗口：读完根记录的 ``newest`` 指针后、真正
        沿链回溯前，另一个事务可能已经提交新版本，把旧 ``newest`` 的 xmax
        打成已提交。此时从旧指针出发会一路看到"全部已被更新"，返回 ``None``，
        但新版本其实已经在根上。``ri=True`` 的自动提交 UPDATE 会因此把一次
        自增误判成"行不存在"，热点基准最终少 1。这里在找不到版本时重读
        ``newest``；指针前进了就重试，稳定不变才认定行确实不可见。
        """
        newest = self._newest_for_root(root_rid)
        if newest is None:
            return None
        for _ in range(6):
            found = self.find_version(txn, newest, ri=ri)
            if found is not None:
                return found[1].values
            latest = self._newest_for_root(root_rid)
            if latest is None or latest == newest:
                return None
            newest = latest
        return None

    def get_at(
        self,
        root_rid: RID,
        commit_lsns: dict[int, int],
        target_lsn: int,
        *,
        assume_committed_before: int = 0,
        strict_unknown_xmax: bool = False,
    ) -> Optional[list[Any]]:
        """按历史目标点从根 RID 取可见版本；供 AS OF 索引回堆使用。"""
        newest = self._newest_for_root(root_rid)
        if newest is None:
            return None
        cur: Optional[RID] = newest
        visited = 0
        while cur is not None and visited < 10_000:
            got = self._read_rid(cur)
            if got is None:
                return None
            page, rec = got
            try:
                if self._visible_header_at(
                    rec.header,
                    commit_lsns,
                    target_lsn,
                    assume_committed_before=assume_committed_before,
                    strict_unknown_xmax=strict_unknown_xmax,
                ):
                    return rec.values
                cur = rec.header.prev_rid
            finally:
                self._release(page)
            visited += 1
        return None

    def collect_pages(self) -> list[int]:
        """收集从根可达的整条堆页链（只读遍历，不改页内容）。

        ``ALTER TABLE`` 的 shadow heap 切换根页后，旧链不再被当前目录引用；
        调用方可在提交后把这份列表交给空闲页表复用。历史下界在 DDL 开始时
        已经前移，因此旧链不再供 ``AS OF`` 读取。
        """
        page_ids: list[int] = []
        visited: set[int] = set()
        page_id = self.table.root_page
        while page_id != INVALID_PAGE_ID:
            if page_id in visited:
                raise CorruptedPageError(
                    f"堆页链出现环：{page_id}",
                    detail=f"表 {self.table.name} 的 next_page 链重复访问",
                )
            visited.add(page_id)
            page = self._fetch(page_id)
            try:
                next_id = page.next_page
            finally:
                self._release(page)
            page_ids.append(page_id)
            page_id = next_id
        return page_ids

    def _scan_with(self, visible) -> Iterator[Tuple[RID, list[Any]]]:
        """按调用方给的可见性谓词顺序扫描，产出 ``(根 RID, 值列表)``。

        产出根 RID 而不是物理 RID，是因为索引与后续 UPDATE/DELETE 都按根 RID
        定位逻辑行——把物理 RID 交给上层会让"更新后再删"指向错误的版本。
        """
        page_id = self.table.root_page
        while page_id != INVALID_PAGE_ID:
            page = self._fetch(page_id)
            try:
                # 先把这一页的 (slot, 记录) 在 latch 内整页解码成快照，再在
                # latch 外做可见性判断与 yield：写者不会把 slot 目录改到一半，
                # 调用方也不会因为拿着页 latch 做业务逻辑而堵住写者。
                with page.latch:
                    next_id = page.next_page
                    # 只在 latch 内拷贝每条记录的原始字节；解码、可见性判断、
                    # yield 都放到 latch 外，避免长扫描把写者饿死。
                    raws = [
                        page.get_record(slot) for slot in range(page.nslots)
                    ]
                for slot, raw in enumerate(raws):
                    if raw is None:
                        continue
                    rec = self._decode_raw(raw)
                    if not visible(rec):
                        continue
                    root = self.resolve_root((page_id, slot), rec.header)
                    yield root, rec.values
            finally:
                self._release(page)
            page_id = next_id

    def scan(
        self, txn: Transaction, *, ri: bool = False
    ) -> Iterator[Tuple[RID, list[Any]]]:
        """按当前事务快照扫描所有可见行。"""
        yield from self._scan_with(lambda rec: self._visible(txn, rec, ri=ri))

    def scan_at(
        self,
        commit_lsns: dict[int, int],
        target_lsn: int,
        *,
        assume_committed_before: int = 0,
        strict_unknown_xmax: bool = False,
    ) -> Iterator[Tuple[RID, list[Any]]]:
        """按提交 LSN 截止点扫描；不伪造 Transaction 快照。

        ``commit_lsns`` 是 ``xid -> COMMIT LSN`` 的账本。目标时刻之前提交的
        xmin 才可见；目标时刻之前提交的 xmax 才表示删除。

        默认对账本里没有的 xid 一律按不可见处理，宁缺毋滥。SQL 的 AS OF
        路径会多给一个 ``assume_committed_before`` 水位：xmin 早于该水位
        且仍留在堆里的版本，视为"水位前已提交"；只要它的 xmax 没有在
        水位后提交，目标点（目标点必须不早于水位）就仍应看到它。这样
        checkpoint 之前的存量行不会因为账本裁剪而全部消失。

        ``strict_unknown_xmax=True`` 时，非零 xmax 如果既不在账本里、也
        没有已知结局，就报错而不是猜"未提交"。历史读宁可明确拒绝，也
        不能把已被删除的旧版本复活。
        """
        yield from self._scan_with(
            lambda rec: self._visible_at(
                rec,
                commit_lsns,
                target_lsn,
                assume_committed_before=assume_committed_before,
                strict_unknown_xmax=strict_unknown_xmax,
            )
        )

    def _visible_at(
        self,
        rec: DecodedRecord,
        commit_lsns: dict[int, int],
        target_lsn: int,
        *,
        assume_committed_before: int = 0,
        strict_unknown_xmax: bool = False,
    ) -> bool:
        return self._visible_header_at(
            rec.header,
            commit_lsns,
            target_lsn,
            assume_committed_before=assume_committed_before,
            strict_unknown_xmax=strict_unknown_xmax,
        )

    def _visible_header_at(
        self,
        h: RecordHeader,
        commit_lsns: dict[int, int],
        target_lsn: int,
        *,
        assume_committed_before: int = 0,
        strict_unknown_xmax: bool = False,
    ) -> bool:
        """按提交 LSN 判断一条版本头是否可见；在线 VACUUM 只解头部也能用。"""
        if h.flags & FLAG_ABORTED:
            return False

        def state(xid: int) -> Optional[TxnState]:
            return self._txns.state_of(xid)

        def committed_before_floor(xid: int) -> bool:
            if target_lsn < assume_committed_before:
                return False
            known = state(xid)
            if known in (TxnState.ABORTED, TxnState.ACTIVE):
                return False
            # 已提交但账本被 checkpoint 裁剪；或结局表已清理。目标点不早
            # 于水位时，这种"水位前提交"的假设成立。
            return True

        def committed(xid: int) -> bool:
            if xid == 0:
                return True  # 未写入
            lsn = commit_lsns.get(xid)
            if lsn is not None:
                return lsn <= target_lsn
            if assume_committed_before > 0:
                return committed_before_floor(xid)
            return False

        if not committed(h.xmin):
            return False
        if h.xmax == 0:
            return True
        lsn = commit_lsns.get(h.xmax)
        if lsn is not None:
            return lsn > target_lsn
        known = state(h.xmax)
        if known in (TxnState.ABORTED, TxnState.ACTIVE):
            return True  # 删除未生效
        if strict_unknown_xmax and assume_committed_before > 0:
            raise NotSupportedError(
                "历史读遇到无法归类的删除版本",
                detail=(
                    f"xmax={h.xmax} 既不在提交账本里，也没有已回滚/活跃"
                    f"结局；目标 LSN={target_lsn}，水位 LSN={assume_committed_before}"
                ),
                hint="换一个不早于当前历史下界的目标点，或先 VACUUM 掉死版本",
            )
        return True

    def count(self, txn: Transaction) -> int:
        return sum(1 for _ in self.scan(txn))

    # ------------------------------------------------------------------
    # 写
    # ------------------------------------------------------------------

    def insert(self, txn: Transaction, values: Sequence[Any]) -> RID:
        txn.require_active()
        txn.wrote = True
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

        # 页 latch 只覆盖"页内结构 + 日志"；选页（页链/目录）仍是短锁覆盖
        # 不到的窗口，所以并发写者把页写满后这里要**重选页**，不能直接报错。
        for _attempt in range(4):
            page = self._find_page_with_space(txn, len(payload))
            try:
                if page.nslots >= _MAX_SLOTS:
                    raise StorageError("页的槽位目录已满")
                slot = self._insert_raw(txn, page, payload)
                if slot is None:
                    continue
                self.table.row_count_hint += 1
                rid = (page.page_id, slot)
                self._record_row_undo(
                    txn, lambda rid=rid: self._undo_insert(txn, rid)
                )
                return rid
            finally:
                self._release(page, dirty=True)
        raise StorageError(
            "连续多页都装不下这条记录，放弃插入",
            detail=f"payload={len(payload)} attempts=4",
        )

    def _undo_insert(self, txn: Transaction, rid: RID) -> None:
        """把保存点之后插入的版本标成 ABORTED；槽位不回收，留给 VACUUM。"""
        got = self._read_rid(rid)
        if got is None:
            return
        page, rec = got
        try:
            self._set_flags(txn, page, rid[1], rec.header.flags | FLAG_ABORTED)
        finally:
            self._release(page, dirty=True)
        self.table.row_count_hint = max(0, self.table.row_count_hint - 1)

    def _latest_version_rid(self, root_rid: RID) -> RID:
        """不判可见性，直接拿链上最新的物理版本。

        版本链是单向的：新版本用 ``prev`` 指回旧版本，根记录用 ``newest``
        指向当前链头。所以"最新"就是根记录的 ``newest``（根自己没被更新过
        时 ``newest`` 为空，此时根即最新）。
        """
        return self.newest_rid(root_rid)

    def update(
        self,
        txn: Transaction,
        root_rid: RID,
        values: Sequence[Any],
        *,
        ri: bool = False,
    ) -> RID:
        """写一条新版本，返回新版本的 RID。

        ``ri=True`` 只在并发 DML 的自动提交 UPDATE 上使用：该语句已经在
        行锁上等到前一个写者提交，必须从"最新已提交版本"继续，否则快照里
        的旧版本会让 first-updater-wins 把刚等到的写机会又判成 40001。
        """
        txn.require_active()
        txn.wrote = True
        newest = self._latest_version_rid(root_rid)
        # 必须从"当前快照可见"的版本出发，而不是从 newest 出发：
        # abort 只写 ABORT 记录、不做进程内 undo，newest 可能仍指向一个
        # 已中止的版本。若直接从那版继续，根记录的 xmax 永远不会被设置，
        # 根和最新版本会同时可见，扫描出两行。
        found = self.find_version(txn, newest, ri=ri)
        if found is None:
            raise StorageError(f"要更新的行 {root_rid} 不存在")
        old_rid, old_rec = found
        # [Review-Fix] First-updater-wins：如果链头的最新版本不是本事务应该
        # 改写的那一版，说明另一个事务在快照之后写过这一行。继续写会覆盖根
        # 记录的 xmax/newest，把对方的版本变成不可达垃圾，而两个版本都没有
        # xmax，于是同一个逻辑行会同时可见（实测 SELECT 出两行）。
        self._raise_if_concurrent_write(txn, newest, old_rid, root_rid)
        old_header = old_rec.header

        payload = encode_record(
            self.table.columns,
            values,
            xmin=txn.xid,
            prev=old_rid,
            root=root_rid,
            flags=0,
        )

        slot = None
        for _ in range(4):
            page = self._find_page_with_space(txn, len(payload))
            try:
                slot = self._insert_raw(txn, page, payload)
            finally:
                self._release(page, dirty=True)
            if slot is not None:
                break
        if slot is None:
            # 选页到真正插入之间可能被别的写者占满；和 _insert_row 一样重试，
            # 不能把"竞争导致的空间变化"当成页损坏。
            raise StorageError("连续多页都装不下这条更新版本，放弃更新")
        new_rid = (page.page_id, slot)

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

        self._record_row_undo(
            txn,
            lambda: self._undo_update(
                txn,
                new_rid,
                old_rid,
                root_rid,
                old_header.xmax,
                newest,
            ),
        )
        return new_rid

    def _undo_update(
        self,
        txn: Transaction,
        new_rid: RID,
        old_rid: RID,
        root_rid: RID,
        old_xmax: int,
        old_newest: RID,
    ) -> None:
        """撤销一次保存点后的 UPDATE：废弃新版本、清回旧版本 xmax、复位 newest。"""
        got = self._read_rid(new_rid)
        if got is not None:
            page, rec = got
            try:
                self._set_flags(
                    txn, page, new_rid[1], rec.header.flags | FLAG_ABORTED
                )
            finally:
                self._release(page, dirty=True)
        got = self._read_rid(old_rid)
        if got is not None:
            page, _rec = got
            try:
                self._set_xmax(txn, page, old_rid[1], old_xmax)
            finally:
                self._release(page, dirty=True)
        got = self._read_rid(root_rid)
        if got is not None:
            page, _rec = got
            try:
                self._set_newest(txn, page, root_rid[1], old_newest)
            finally:
                self._release(page, dirty=True)

    def _raise_if_concurrent_write(
        self, txn: Transaction, newest: RID, target: RID, root_rid: RID
    ) -> None:
        """[Review-Fix] 检测"快照之后有别人写过这一行"。

        ``newest`` 是链头，``target`` 是本事务按快照决定要写的版本。两者不同
        说明链上存在一个不属于本事务的更新版本；只有它是已回滚版本时才可以
        安全忽略（进程内 abort 会撤销页写，崩溃恢复会把它的输出 undo 掉）。
        """
        if newest == target:
            return
        got = self._read_rid(newest)
        if got is None:
            return
        page, rec = got
        try:
            other = rec.header.xmin
            state = self._txns.state_of(other)
        finally:
            self._release(page)
        if other == txn.xid or state is TxnState.ABORTED:
            return
        from ..errors import SerializationError

        raise SerializationError(
            f"行 {root_rid} 已被另一事务更新",
            detail=f"并发事务 xid={other}，当前事务快照 {txn.snapshot_xid}",
            hint="重试整个事务；快照隔离下写写冲突必须由应用层重试",
        )

    def delete(self, txn: Transaction, root_rid: RID) -> int:
        """把当前可见版本标记为已删除，返回受影响行数（0 或 1）。"""
        txn.require_active()
        txn.wrote = True
        try:
            newest = self._latest_version_rid(root_rid)
        except StorageError:
            # RID 指向不存在的页/槽位：按"没有这一行"处理，而不是抛存储错误
            return 0
        found = self.find_version(txn, newest)
        if found is None:
            return 0
        rid, rec = found
        self._raise_if_concurrent_write(txn, newest, rid, root_rid)
        page = self._fetch(rid[0])
        try:
            self._set_xmax(txn, page, rid[1], txn.xid)
        finally:
            self._release(page, dirty=True)
        self.table.row_count_hint = max(0, self.table.row_count_hint - 1)
        self._record_row_undo(
            txn, lambda: self._undo_delete(txn, rid, rec.header.xmax)
        )
        return 1

    def _undo_delete(self, txn: Transaction, rid: RID, old_xmax: int) -> None:
        """撤销一次保存点后的 DELETE：把旧版本的 xmax 清回原值。"""
        got = self._read_rid(rid)
        if got is None:
            return
        page, _rec = got
        try:
            self._set_xmax(txn, page, rid[1], old_xmax)
        finally:
            self._release(page, dirty=True)
        self.table.row_count_hint += 1

    def vacuum_online(  # noqa: C901 - 版本链遍历与逐页回收集中在一个流程里
        self,
        txn: Transaction,
        active_txns: Sequence[Transaction] = (),
        *,
        max_versions: int = VACUUM_ONLINE_MAX_VERSIONS,
        retain_lsn: int = 0,
        commit_lsns: Optional[dict[int, int]] = None,
    ) -> tuple[int, int, int]:
        """保 RID 的在线回收：不重排槽位、不重建索引，保留活跃快照要的版本。

        ``retain_lsn`` > 0 时额外保留“在该历史目标点仍可见”的版本，用于
        ``VACUUM ... RETAIN LSN``。这样 VACUUM 不会把用户显式要求保留的
        历史窗口物理删掉。未知 ``xmax`` 按“可能未提交”保守保留，不因为
        账本边界拒绝维护操作。

        与 ``vacuum`` 的区别：
        - 不把存活行重新打包，槽位号不变，所有 RID 与索引项继续有效；
        - 只删除“对 VACUUM 事务和所有传入的活跃事务都不可见”的旧版本；
        - 每个逻辑行保留根记录，并保留从 ``newest`` 到最老可见版本的整条
          前缀链；更老的版本才回收。这样活跃快照沿 ``prev`` 仍能走得到；
        - 页内只做 tombstone + ``compact``，回收记录体空间，但页链不缩短。

        前提：调用方已经让所有在途语句退出（``exclusive_guard``）；活跃事务
        只能处于“空闲”状态，不能在扫描/回收期间写页。
        """
        txn.require_active()
        txn.wrote = True
        cap = int(max_versions)
        if cap <= 0:
            raise ValueError("max_versions 必须是正整数")

        # 只保留每个版本的 RecordHeader，不保留 DecodedRecord 里的值。
        # 在线 VACUUM 的决策只需要 xmin/xmax/flags 与版本链指针；把整行
        # 反序列化进 dict 会让 10 万行表在"在线"路径上吃掉几百 MB 到 GB。
        headers: dict[RID, RecordHeader] = {}
        page_ids: list[int] = []
        page_id = self.table.root_page
        while page_id != INVALID_PAGE_ID:
            page = self._fetch(page_id)
            next_id = page.next_page
            page_ids.append(page_id)
            try:
                for slot in range(page.nslots):
                    raw = page.get_record(slot)
                    if raw is None:
                        continue
                    if len(headers) >= cap:
                        raise StorageError(
                            "在线 VACUUM 的版本数超过上限",
                            detail=(
                                f"表 {self.table.name} 超过 "
                                f"{cap} 个物理版本；"
                                "在线模式只保留 header 元数据，但仍需一次性"
                                "建立版本链索引"
                            ),
                            hint="对大表改用离线 VACUUM，或先清理历史版本",
                        )
                    # 只解 38 字节头，不碰列值。旧实现 decode_record 会把
                    # 每一行的值都物化，10 万行表直接吃掉几百 MB。
                    headers[(page_id, slot)] = decode_header(raw)
            finally:
                self._release(page)
            page_id = next_id

        viewers: list[Transaction] = [txn]
        viewers.extend(t for t in active_txns if t.xid != txn.xid)

        def visible(header: RecordHeader) -> bool:
            if any(
                self._txns.is_visible(
                    viewer, header.xmin, header.xmax, header.flags
                )
                for viewer in viewers
            ):
                return True
            if retain_lsn > 0:
                if commit_lsns is None:
                    raise ValueError("retain_lsn 需要提交账本")
                return self._visible_header_at(
                    header,
                    commit_lsns,
                    retain_lsn,
                    assume_committed_before=retain_lsn,
                )
            return False

        keep: set[RID] = set()
        for rid, header in headers.items():
            if not header.is_root:
                continue
            cur = header.newest_rid or rid
            chain: list[RID] = []
            seen: set[RID] = set()
            while cur in headers and cur not in seen:
                chain.append(cur)
                seen.add(cur)
                cur = headers[cur].prev_rid
            if not chain:
                # 版本链断在根上：保守保留，避免把可能仍被索引引用的槽位删掉。
                keep.add(rid)
                continue
            deepest = -1
            for i, version_rid in enumerate(chain):
                if visible(headers[version_rid]):
                    deepest = i
            if deepest < 0:
                # 整条链对所有快照都不可见：行已经彻底死亡，根记录也不再
                # 是任何活版本的入口。索引里的陈旧 RID 由 heap.get() 当成
                # 缺失行返回 None，扫描/唯一性检查都会跳过，可以连根回收。
                continue
            keep.add(rid)
            keep.update(chain[: deepest + 1])

        # 可见但不在任何根链上的孤儿版本（历史遗留）也保留，避免在线
        # 回收把某个活跃快照还能扫到的版本删掉。
        for rid, header in headers.items():
            if rid not in keep and visible(header):
                keep.add(rid)

        # 先驱逐指向“不再保留的物理版本”的索引项。必须在重置/复用页之前
        # 完成；否则同一页槽位被新行复用后，旧键的索引项会命中新行。
        stale_index_entries = self._purge_stale_index_entries(txn, keep)

        dead = 0
        rewritten: set[int] = set()
        for pid in page_ids:
            page = self._fetch(pid)
            dirty = False
            try:
                with page.latch:
                    old = bytes(page.buf)
                    removed = 0
                    for slot in range(page.nslots):
                        rid = (pid, slot)
                        if (
                            rid in headers
                            and rid not in keep
                            and page.record_length(slot) > 0
                        ):
                            page.delete_record(slot)
                            removed += 1
                    if removed:
                        page.compact()
                        self._log_page_image(txn, page, old)
                        dead += removed
                        rewritten.add(pid)
                        dirty = True
            finally:
                self._release(page, dirty=dirty)

        # 空页的旧槽位目录/版本链已经被整页重置，计进“重写页数”。
        rewritten.update(self._reclaim_empty_pages(txn, page_ids, keep))

        live_roots: set[RID] = set()
        for rid, header in headers.items():
            if self._txns.is_visible(
                txn, header.xmin, header.xmax, header.flags
            ):
                live_roots.add(self.resolve_root(rid, header))
        self.table.row_count_hint = len(live_roots)
        # ``stale_index_entries`` 目前只用于保证页复用安全；调用方不需要
        # 这个计数，保留局部变量让日志/调试能看到实际清理量。
        del stale_index_entries
        return len(live_roots), dead, len(rewritten)

    def vacuum(self, txn: Transaction) -> tuple[int, int, int]:
        """回收死版本，并把存活行重排成"每行只剩一条根元组"。

        返回 ``(保留行数, 回收版本数, 重写页数)``。

        前提：**没有其它活跃事务**。本引擎的 ``TransactionManager`` 同一时刻
        只允许一个活跃事务，所以这条前提在进程内天然成立；如果以后放开并发，
        就必须引入 oldest-active-xmin 水位，否则会删掉旧快照仍然需要的版本。

        注意：重排会改变所有行的 RID，调用方必须随后重建该表的所有索引
        （执行器里的 ``VACUUM`` 就是这么做的）。
        """
        txn.require_active()
        txn.wrote = True
        active = self._txns.active_xids
        if len(active) > 1:
            raise StorageError(
                "VACUUM 需要表上没有并发事务",
                detail=f"当前活跃事务：{active}",
                hint="等其它事务结束后再重试",
            )

        keep: list[tuple[list[Any], int]] = []
        dead = 0
        page_id = self.table.root_page
        while page_id != INVALID_PAGE_ID:
            page = self._fetch(page_id)
            next_id = page.next_page
            try:
                for slot in range(page.nslots):
                    rec = self._decode(page, slot)
                    if rec is None:
                        continue
                    if self._visible(txn, rec):
                        keep.append((rec.values, rec.header.xmin))
                    else:
                        dead += 1
            finally:
                self._release(page)
            page_id = next_id

        index = 0
        rewritten = 0
        page_id = self.table.root_page
        while page_id != INVALID_PAGE_ID:
            page = self._fetch(page_id)
            next_id, prev_id = page.next_page, page.prev_page
            try:
                old = bytes(page.buf)
                page.init_heap(page_id)
                page.next_page = next_id
                page.prev_page = prev_id
                index = self._pack_rows(page, keep, index, txn)
                self._log_page_image(txn, page, old)
                rewritten += 1
            finally:
                self._release(page, dirty=True)
            page_id = next_id

        # 存活行比原来多（理论上不该发生，但页头可以有碎片）：补新页
        while index < len(keep):
            page = self._append_page(txn)
            try:
                old = bytes(page.buf)
                index = self._pack_rows(page, keep, index, txn)
                if page.nslots == 0:
                    raise RowTooLargeError(
                        "有一行放不进空页，VACUUM 中止",
                        hint="检查行大小是否接近页容量上限",
                    )
                self._log_page_image(txn, page, old)
                rewritten += 1
            finally:
                self._release(page, dirty=True)

        self.table.row_count_hint = len(keep)
        self._release_empty_tail(txn)
        return len(keep), dead, rewritten

    def _release_empty_tail(self, txn: Transaction) -> int:
        """摘掉离线 VACUUM 后落在链尾的空页，并在提交后交给空闲页表。

        离线 VACUUM 按链顺序从根页开始重排，因此空页只可能出现在尾部。
        这里只改前一页的 ``next`` 和目录里的 ``last_page``；被摘页的槽位
        目录不再有人读取，提交后由 ``DiskManager`` 复用。根页即使为空也
        必须保留，保证表始终有一条可写页链。
        """
        page_id = self.table.root_page
        last_nonempty = INVALID_PAGE_ID
        empty_ids: list[int] = []
        while page_id != INVALID_PAGE_ID:
            page = self._fetch(page_id)
            try:
                if page.nslots:
                    if empty_ids:
                        raise StorageError(
                            "离线 VACUUM 的空页不在链尾，拒绝摘链",
                            detail=f"page={page_id} 在空页 {empty_ids[0]} 之后仍有记录",
                        )
                    last_nonempty = page_id
                else:
                    empty_ids.append(page_id)
                page_id = page.next_page
            finally:
                self._release(page)
        if not empty_ids:
            return 0
        if last_nonempty == INVALID_PAGE_ID:
            # 全表为空：根页保留（保证始终有一条可写页链），它后面的空页
            # 全部可以摘掉。根页本身即使为空也不释放。
            root = self.table.root_page
            tail = self._fetch(root)
            try:
                old = bytes(tail.buf)
                self._apply_and_log(
                    txn, tail, 8, struct.pack("<I", INVALID_PAGE_ID), old[8:12]
                )
            finally:
                self._release(tail, dirty=True)
            self.table.last_page = root
            txn.freed_pages.update(pid for pid in empty_ids if pid != root)
            return sum(1 for pid in empty_ids if pid != root)
        tail = self._fetch(last_nonempty)
        try:
            old = bytes(tail.buf)
            self._apply_and_log(
                txn, tail, 8, struct.pack("<I", INVALID_PAGE_ID), old[8:12]
            )
        finally:
            self._release(tail, dirty=True)
        self.table.last_page = last_nonempty
        txn.freed_pages.update(empty_ids)
        return len(empty_ids)

    def _pack_rows(
        self, page: HeapPage, rows: list[tuple[list[Any], int]], index: int, txn: Transaction
    ) -> int:
        """把 ``rows[index:]`` 尽量塞进已清空的页，返回下一个待放行的下标。"""
        while index < len(rows):
            values, xmin = rows[index]
            slot = page.nslots
            payload = encode_record(
                self.table.columns,
                values,
                xmin=xmin,
                flags=FLAG_ROOT,
                root=(page.page_id, slot),
                newest=(page.page_id, slot),
            )
            if page.insert_record(payload) is None:
                return index
            index += 1
        return index

    def _reclaim_empty_pages(
        self, txn: Transaction, page_ids: Sequence[int], keep: set[RID]
    ) -> set[int]:
        """把没有任何保留版本的页重置后移到链尾，供后续插入复用；返回页号集合。

        只处理 ``keep`` 里没有任何 RID 的页：这些页上的根记录也已经被
        回收，索引项由 :meth:`_purge_stale_index_entries` 先清空。页号保持
        不变（索引项已清空，不会再有旧 RID 命中新行），但槽位目录和版本链
        一起归零，等于把整页空间交还给 ``_find_page_with_space``。

        必须在 ``exclusive_guard`` 下执行；链指针的改写在 ``_shape_lock``
        内完成，页内容仍按页 latch + 整页 WAL 落盘。
        """
        empty = [
            pid
            for pid in page_ids
            if not any(rid[0] == pid for rid in keep)
        ]
        if not empty:
            return set()
        reclaimed: set[int] = set()
        with self._shape_lock:
            for pid in empty:
                page = self._fetch(pid)
                try:
                    with page.latch:
                        next_id = page.next_page
                        prev_id = page.prev_page
                    if prev_id == INVALID_PAGE_ID and next_id == INVALID_PAGE_ID:
                        # 唯一的一页：保留为根页，只清空槽位。
                        old = bytes(page.buf)
                        page.init_heap(pid)
                        self._log_page_image(txn, page, old)
                        reclaimed.add(pid)
                        continue

                    # 先摘链：更新前驱/后继与目录里的根/尾指针。
                    if prev_id == INVALID_PAGE_ID:
                        self.table.root_page = next_id
                        nxt = self._fetch(next_id)
                        try:
                            self._apply_and_log(
                                txn,
                                nxt,
                                12,
                                struct.pack("<I", INVALID_PAGE_ID),
                                bytes(nxt.buf[12:16]),
                            )
                        finally:
                            self._release(nxt, dirty=True)
                    else:
                        prv = self._fetch(prev_id)
                        try:
                            self._apply_and_log(
                                txn,
                                prv,
                                8,
                                struct.pack("<I", next_id),
                                bytes(prv.buf[8:12]),
                            )
                        finally:
                            self._release(prv, dirty=True)
                    if next_id != INVALID_PAGE_ID:
                        nxt = self._fetch(next_id)
                        try:
                            self._apply_and_log(
                                txn,
                                nxt,
                                12,
                                struct.pack("<I", prev_id),
                                bytes(nxt.buf[12:16]),
                            )
                        finally:
                            self._release(nxt, dirty=True)
                    elif prev_id != INVALID_PAGE_ID:
                        self.table.last_page = prev_id

                    # 重置该页，再挂到当前链尾。
                    old = bytes(page.buf)
                    page.init_heap(pid)
                    tail_id = self.table.last_page
                    if tail_id != INVALID_PAGE_ID and tail_id != pid:
                        tail = self._fetch(tail_id)
                        try:
                            self._apply_and_log(
                                txn,
                                tail,
                                8,
                                struct.pack("<I", pid),
                                bytes(tail.buf[8:12]),
                            )
                        finally:
                            self._release(tail, dirty=True)
                        page.prev_page = tail_id
                    self.table.last_page = pid
                    self._log_page_image(txn, page, old)
                    reclaimed.add(pid)
                finally:
                    self._release(page, dirty=True)
        return reclaimed

    def _purge_stale_index_entries(
        self, txn: Transaction, keep: set[RID]
    ) -> int:
        """清掉指向已回收 RID 的索引项，返回实际删除的条目数。

        在线 VACUUM 此前只回收堆页里的死版本，索引项故意保留给活跃快照和
        尚未重排的 RID 使用；但这些 RID 一旦对所有快照都不可见、记录体也被
        回收，索引项就只剩两类作用：让查询多回表一次，以及让唯一索引的
        陈旧命中越积越多。这里在 ``exclusive_guard`` 之后把 ``keep`` 之外的
        条目按 ``(key, rid)`` 精确删除；删除按完整键 + RID 定位，不会误删
        同键的其它活版本。

        只在页级回收之前调用：页面稍后会被重置复用，不能再留着任何指向
        旧槽位的索引项，否则新行可能“继承”旧键的索引命中。
        """
        removed = 0
        for idx in self.table.indexes.values():
            specs = [self.table.column(c).type for c in idx.columns]
            tree = BPlusTree(
                self._bp,
                KeyCodec(specs),
                idx.root_page,
                name=idx.name,
                root_cell=idx,
            )
            stale: list[tuple[tuple, RID]] = []
            for full_key, rid in tree.scan_all(txn):
                if rid in keep:
                    continue
                # scan_all 的键末尾带 RID 两列；B+ 树 delete 只接收索引列值。
                stale.append((tuple(full_key[:-2]), rid))
            for values, rid in stale:
                if tree.delete(txn, list(values), rid):
                    removed += 1
        return removed

    def _log_page_image(self, txn: Transaction, page: HeapPage, old: bytes) -> None:
        """整页重写也要进 WAL：否则恢复后看到的是旧内容，行就凭空复活了。"""
        new = bytes(page.buf)
        if new == old:
            return
        self._bp.log_page_update(txn.xid, page, 0, old, new)
        self._bp.mark_dirty(page)

    def truncate(self, txn: Transaction) -> int:
        """清空整表：页链保留，每一页重置为空堆页。

        比"逐行 DELETE + VACUUM"便宜得多：只需要按页写整页日志，代价与行数无关。
        索引由调用方重建（执行器的 ``TRUNCATE`` 会做），返回清空前的行数估计。
        """
        txn.require_active()
        txn.wrote = True
        removed = self.table.row_count_hint
        page_id = self.table.root_page
        while page_id != INVALID_PAGE_ID:
            page = self._fetch(page_id)
            next_id, prev_id = page.next_page, page.prev_page
            try:
                old = bytes(page.buf)
                page.init_heap(page_id)
                page.next_page = next_id
                page.prev_page = prev_id
                self._log_page_image(txn, page, old)
            finally:
                self._release(page, dirty=True)
            page_id = next_id
        self.table.row_count_hint = 0
        return removed


def _record_u64(page: HeapPage, slot: int, rel: int) -> int:
    entry_off = 32 + 4 * slot
    rec_off = page.get_u16(entry_off)
    return struct.unpack_from("<Q", page.buf, rec_off + rel)[0]


__all__ = ["HeapFile", "_MAX_SLOTS"]
