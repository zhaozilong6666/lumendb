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

from dataclasses import dataclass, field
from typing import Optional

from ..errors import StorageError
from .bufferpool import BufferPoolManager
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
    ) -> None:
        self._disk = disk
        self._wal = wal
        self._bp = buffer_pool
        self._txns = txn_mgr

    # ------------------------------------------------------------------
    # 恢复
    # ------------------------------------------------------------------

    def recover(self) -> RecoveryReport:
        records = self._wal.read_all()
        report = RecoveryReport(scanned=len(records))
        if not records:
            return report

        # ---- analysis -------------------------------------------------
        states: dict[int, str] = {}
        max_xid = 0
        for rec in records:
            max_xid = max(max_xid, rec.xid)
            if rec.rtype == LogType.BEGIN:
                states[rec.xid] = "active"
            elif rec.rtype == LogType.COMMIT:
                states[rec.xid] = "committed"
            elif rec.rtype == LogType.ABORT:
                states[rec.xid] = "aborted"

        losers = {xid for xid, st in states.items() if st == "active"}
        committed = {xid for xid, st in states.items() if st == "committed"}
        aborted = {xid for xid, st in states.items() if st == "aborted"}
        report.losers = sorted(losers)
        report.committed = sorted(committed)

        start_lsn = self._disk.meta.checkpoint_lsn
        report.checkpoint_lsn = start_lsn

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
                page_id, offset, old, _new = decode_page_update(rec.payload)
                page = self._bp.fetch_page(page_id)
                try:
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
        """把所有脏页落盘，然后写一条 checkpoint 日志并记录到元数据页。"""
        self._bp.flush_all()
        lsn = self._wal.checkpoint(self._txns.active_xids)
        self._wal.flush()
        self._disk.meta.checkpoint_lsn = lsn
        self._disk._persist_meta()
        self._disk.sync()
        # 分段 WAL 可以在 checkpoint 后回收旧段。安全下界是活跃事务的
        # BEGIN LSN：这些事务崩溃后仍需要 before image 做 undo。没有活跃
        # 事务时，checkpoint 之前的所有记录都可以回收。
        active_start = self._txns.active_first_lsn
        safe_lsn = lsn if active_start is None else min(active_start, lsn)
        truncate = getattr(self._wal, "truncate_before", None)
        if truncate is not None:
            truncate(safe_lsn)
        return lsn


__all__ = ["RecoveryManager", "RecoveryReport"]
