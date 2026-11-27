"""崩溃恢复。

恢复遵循 ARIES 的三段式，但在日志语义上做了简化：

1. **analysis**：扫一遍日志，重建"谁提交了、谁还活着"。
2. **redo**：从 checkpoint 之后重放所有页修改。用 ``pageLSN >= rec.lsn`` 跳过
   已经落盘的页，所以重放是幂等的——同一批日志重放一次和三次结果相同。
3. **undo**：对没有 COMMIT 的事务，按 LSN 逆序把 before image 写回去。

为什么 undo 之后也要推进 ``pageLSN``
====================================

这一步容易被漏掉。如果不推进，下次启动的 redo 会把 after image 重新盖上来，
把刚撤销掉的数据又"复活"，于是回滚后的行在下一次崩溃恢复后重新出现。
把被 undo 的页的 pageLSN 提到该日志的 LSN，redo 就会正确地跳过它。

补偿记录（CLR）在这个实现里省略了：undo 应用的是 before image，重复应用结果
不变（幂等），所以即使崩溃发生在 undo 中途，下次启动再撤销一遍仍然收敛到
同一个状态。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Optional

from ..errors import StorageError
from .bufferpool import BufferPoolManager
from .commit_ledger import CommitLedger
from .history import HistoryWindow
from .pager import DiskManager
from .txn import TransactionManager
from .wal import (
    LogType,
    WAL,
    decode_page_image,
    decode_page_update,
)


@dataclass
class RecoveryReport:
    scanned: int = 0
    redo_applied: int = 0
    undo_applied: int = 0
    losers: list[int] = field(default_factory=list)
    committed: list[int] = field(default_factory=list)
    #: 当前 WAL 窗口内每个已提交事务的 COMMIT 记录 LSN。
    commit_lsns: dict[int, int] = field(default_factory=dict)
    checkpoint_lsn: int = 0

    def summary(self) -> str:
        return (
            f"扫描 {self.scanned} 条日志，redo {self.redo_applied} 段，"
            f"undo {self.undo_applied} 段；"
            f"未提交事务 {len(self.losers)} 个"
        )


class RecoveryManager:
    def __init__(
        self,
        disk: DiskManager,
        wal: WAL,
        buffer_pool: BufferPoolManager,
        txn_mgr: TransactionManager,
        history: Optional[HistoryWindow] = None,
        commit_ledger: Optional[CommitLedger] = None,
    ) -> None:
        self._disk = disk
        self._wal = wal
        self._bp = buffer_pool
        self._txns = txn_mgr
        self._history = history
        self._commit_ledger = commit_ledger
        #: 并发 DML 下多个线程可能同时触发 `_maybe_checkpoint`。账本重写、
        #: checkpoint_lsn 推进、WAL 裁剪必须是一个串行序列；否则后一个
        #: checkpoint 可能拿旧快照覆盖前一个账本，再把 WAL 裁掉。
        self._checkpoint_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 恢复
    # ------------------------------------------------------------------

    def recover(self) -> RecoveryReport:
        records = self._wal.read_all()
        report = RecoveryReport(scanned=len(records))
        if not records:
            return report

        # ---- analysis -------------------------------------------------
        start_lsn = self._disk.meta.checkpoint_lsn
        report.checkpoint_lsn = start_lsn
        states: dict[int, str] = {}
        max_xid = 0
        for rec in records:
            max_xid = max(max_xid, rec.xid)
            if rec.rtype == LogType.BEGIN:
                states[rec.xid] = "active"
            elif rec.rtype == LogType.COMMIT:
                states[rec.xid] = "committed"
                # 不能只收 ``lsn > checkpoint_lsn`` 的提交：崩溃可能发生在
                # “元数据已推进 checkpoint、WAL 还没来得及裁剪”之间。那些
                # 早于 checkpoint 的 COMMIT 记录如果被跳过，而持久账本又
                # 恰好还没写入，提交历史就永久丢失。这里全收，checkpoint
                # 收尾时再按账本保留策略裁剪。
                report.commit_lsns[rec.xid] = rec.lsn
            elif rec.rtype == LogType.ABORT:
                states[rec.xid] = "aborted"

        losers = {xid for xid, st in states.items() if st == "active"}
        committed = {xid for xid, st in states.items() if st == "committed"}
        aborted = {xid for xid, st in states.items() if st == "aborted"}
        report.losers = sorted(losers)
        report.committed = sorted(committed)

        # ---- redo -----------------------------------------------------
        for rec in records:
            if rec.rtype not in (LogType.UPDATE, LogType.PAGE_IMAGE):
                continue
            if rec.lsn <= start_lsn:
                continue
            if rec.rtype == LogType.PAGE_IMAGE:
                page_id, image = decode_page_image(rec.payload)
                self._disk.ensure_page(page_id)
                page = self._bp.fetch_page(page_id)
                try:
                    if page.page_lsn < rec.lsn:
                        page.buf[:] = image
                        page.page_lsn = rec.lsn
                        self._bp.mark_dirty(page)
                        report.redo_applied += 1
                finally:
                    self._bp.unpin(page_id)
                continue
            page_id, offset, _old, new = decode_page_update(rec.payload)
            self._disk.ensure_page(page_id)
            page = self._bp.fetch_page(page_id)
            try:
                if page.page_lsn < rec.lsn:
                    page.buf[offset : offset + len(new)] = new
                    page.page_lsn = rec.lsn
                    self._bp.mark_dirty(page)
                    report.redo_applied += 1
            finally:
                self._bp.unpin(page_id)
        if report.redo_applied:
            self._bp.flush_all()

        # ---- undo -----------------------------------------------------
        if losers:
            for rec in reversed(records):
                if rec.rtype != LogType.UPDATE or rec.xid not in losers:
                    continue
                page_id, offset, old, new = decode_page_update(rec.payload)
                if not old:
                    # 逻辑回滚记录（old_len 哨兵）：DML 的堆页/B+ 树页不参与
                    # 物理 undo，死行靠 xmin/xmax 可见性与 VACUUM 回收。
                    continue
                page = self._bp.fetch_page(page_id)
                try:
                    # [Review-Fix] 条件 undo：只有页面上仍是本记录的 after
                    # image 时才回滚。若同一字节区间已被更高 LSN 的已提交
                    # 事务改写，说明 loser 的修改早就被别人覆盖了；这时写回
                    # before image 会把已提交的数据抹掉（实测：两个事务先后
                    # 更新同一行，loser 的 undo 把赢家的 newest 指针改回去，
                    # 已提交的更新直接消失）。
                    if bytes(page.buf[offset : offset + len(new)]) != new:
                        continue
                    page.buf[offset : offset + len(old)] = old
                    # 关键：推进 pageLSN，防止下次 redo 把撤销结果覆盖回去
                    page.page_lsn = rec.lsn
                    self._bp.mark_dirty(page)
                    report.undo_applied += 1
                finally:
                    self._bp.unpin(page_id)
            self._bp.flush_all()

        # ---- 收尾 -----------------------------------------------------
        self._txns.register_recovered(
            committed=committed, aborted=aborted | losers, max_xid=max_xid
        )
        if losers:
            for xid in losers:
                self._wal.abort(xid)
            self._wal.flush()

        self.checkpoint()
        return report

    # ------------------------------------------------------------------
    # checkpoint
    # ------------------------------------------------------------------

    def checkpoint(self) -> int:
        """串行执行一次 checkpoint；并发触发时排队。"""
        with self._checkpoint_lock:
            return self._checkpoint_locked()

    def _checkpoint_locked(self) -> int:
        """把所有脏页落盘，然后写一条 checkpoint 日志并记录到元数据页。

        并发 DML 不拿 ``Database._write_lock``，所以 flush 与 checkpoint 之间
        仍可能有写者标脏。这里用 ``clean_barrier`` 把 CHECKPOINT 记录追加放在
        “脏页集合为空且 ``_lock`` 在手”的临界区内，消除“记录早于 checkpoint、
        页却没落盘”的漏页窗口。
        """
        with self._bp.clean_barrier():
            # 这里已经持有 BufferPool._lock，不能再拿 TransactionManager 锁：
            # 提交路径是 TM 锁 → BP 锁（_mark_committed 的 clear_undo），
            # 反过来取就是 ABBA 死锁。active_xids_for_checkpoint 只读集合，
            # 且此时新事务即使 BEGIN 也写不了页，不会在 checkpoint 前留脏页。
            lsn = self._wal.checkpoint(self._txns.active_xids_for_checkpoint())
        self._wal.flush()

        # 账本必须先于元数据页里的 checkpoint_lsn 落盘。若账本写入失败，
        # checkpoint_lsn 保持旧值，WAL 不裁剪，恢复仍能从 WAL 重建全部提交；
        # 反过来先推进 checkpoint_lsn 再写账本，崩溃就可能同时失去两条路径。
        retained: Optional[list[tuple[int, int]]] = None
        if self._commit_ledger is not None:
            entries = self._durable_commit_entries(lsn)
            retained = self._commit_ledger.write(entries)

        self._disk.meta.checkpoint_lsn = lsn
        self._disk._persist_meta()
        self._disk.sync()
        if self._history is not None:
            # checkpoint 只登记固化点，不限制历史读；结构下界由 VACUUM/DDL
            # 自己持久化，账本下界随后跟着最老保留提交走。
            self._history.observe_checkpoint(lsn)
        # 分段 WAL 可以在 checkpoint 后回收旧段。安全下界是活跃事务的
        # BEGIN LSN：这些事务崩溃后仍需要 before image 做 undo。没有活跃
        # 事务时，checkpoint 之前的所有记录都可以回收。
        active_start = self._txns.active_first_lsn
        safe_lsn = lsn if active_start is None else min(active_start, lsn)
        truncate = getattr(self._wal, "truncate_before", None)
        if truncate is not None:
            truncate(safe_lsn)
        if retained is not None:
            # 持久账本已经覆盖 checkpoint 之前的提交；内存账本对齐磁盘账本，
            # 不再按 checkpoint LSN 一刀切。历史读的账本下界也跟着最老
            # 保留提交走：保留窗口裁短时抬高，首次写入旧提交时降低。
            self._txns.replace_commit_log(retained)
            if self._history is not None and retained:
                self._history.observe_ledger(retained[0][1])
        else:
            prune = getattr(self._txns, "prune_commit_log_before", None)
            if prune is not None:
                # 没有持久账本时保持旧语义：以 checkpoint LSN 为界。
                prune(lsn)
                if self._history is not None:
                    self._history.observe_ledger(lsn)
        return lsn

    def _durable_commit_entries(self, checkpoint_lsn: int) -> dict[int, int]:
        """收集 checkpoint 记录之前已经落盘的 COMMIT 条目。

        不能只读 ``TransactionManager.commit_log()``：一个提交可能已经把
        COMMIT 记录追加到 WAL、但还卡在 ``_mark_committed`` 的 BP 锁上，
        此时内存账本还没有它。checkpoint 已经把 WAL flush 到 ``lsn``，所以
        直接扫当前 WAL 里的 COMMIT 记录是唯一没有竞态的取证方式。WAL 窗口
        通常只有几 MB，且 checkpoint 本身就是维护动作，这个扫描可以接受。
        """
        entries = dict(self._txns.commit_log())
        for rec in self._wal.read_all():
            if rec.rtype != LogType.COMMIT or rec.lsn > checkpoint_lsn:
                continue
            previous = entries.get(rec.xid)
            if previous is None or rec.lsn > previous:
                entries[rec.xid] = rec.lsn
        return entries


__all__ = ["RecoveryManager", "RecoveryReport"]
