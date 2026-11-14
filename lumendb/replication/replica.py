"""只读副本的物理 WAL 应用。

主库 checkpoint 后复制数据文件作为基准备份，副本从 ``checkpoint_lsn + 1``
开始读取主库 WAL，把页更新重放到自己的 Buffer Pool，并用主库事务状态控制
MVCC 可见性。记录可以直接来自同进程的 ``primary.wal``，也可以通过
``WALServer`` / ``WALClient`` 的 TCP 流读取；两种路径使用同一套物理重放。

当前约束：
- 基准备份要求主库没有活跃事务；否则复制到的是包含未提交版本的数据文件。
- 基准备份没有冻结并发写；调用方应在主库静默时备份，或接受“流会补偿”
  的窗口。
- 副本假设单线程应用 WAL；查询与 apply 不应并发。
- 主库 WAL 的保留依赖 ``SegmentedWAL.retain_lsn``；旧单文件 WAL 没有该
  属性，只能用于测试或手动控制 checkpoint。
"""

from __future__ import annotations

import os
import threading
from pathlib import Path

from ..engine.database import Database
from ..errors import StorageError, TransactionError
from ..storage.txn import TxnState
from ..storage.page import PageType
from ..storage.wal import LogType, decode_page_image, decode_page_update
from .fence import PromotionFence


class Replica:
    def __init__(
        self,
        path: str | Path,
        *,
        applied_lsn: int = 0,
        wal_segment_bytes: int = 1024 * 1024,
    ) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        marker = self.path / "replica.lsn"
        if marker.exists():
            applied_lsn = int(marker.read_text().strip() or "0")
        self.db = Database(
            self.path,
            read_only=True,
            checkpoint_wal_bytes=0,
            wal_segment_bytes=wal_segment_bytes,
        )
        self._applied_lsn = applied_lsn
        # apply 与只读查询不能同时碰到 Buffer Pool / catalog。
        self._lock = threading.RLock()
        self._write_marker()

    # ------------------------------------------------------------------

    @classmethod
    def bootstrap(cls, primary: Database, path: str | Path) -> "Replica":
        """从主库的 checkpoint 基准备份创建副本。

        复制前先让主库 checkpoint，保证数据页与 pageLSN 一致；基准备份不
        拷贝 WAL，副本只应用 checkpoint 之后的记录。
        """
        if primary.txns.active_xids:
            raise TransactionError(
                "基准备份要求主库没有活跃事务",
                hint="先让所有事务 COMMIT / ROLLBACK，再 checkpoint 和复制",
            )
        data, base_lsn = primary.snapshot()
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        data_path = target / "lumendb.data"
        if data_path.exists():
            raise TransactionError(
                "副本目录已经有数据文件",
                hint="换一个空目录，或先手动确认旧副本可以删除",
            )
        data_path.write_bytes(data)
        retain = getattr(primary.wal, "retain_lsn", None)
        if retain is not None:
            primary.wal.retain_lsn = base_lsn
        return cls(target, applied_lsn=base_lsn)

    @classmethod
    def bootstrap_from_server(
        cls,
        host: str,
        port: int,
        path: str | Path,
        *,
        timeout: float = 10.0,
        token: str | None = None,
    ) -> "Replica":
        """通过 TCP 拉取主库基准备份并打开副本。"""
        from .wal_stream import WALClient

        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        data_path = target / "lumendb.data"
        if data_path.exists():
            raise TransactionError(
                "副本目录已经有数据文件",
                hint="换一个空目录，或先手动确认旧副本可以删除",
            )
        base_lsn = WALClient(
            host, port, timeout=timeout, token=token
        ).backup_to(data_path)
        return cls(target, applied_lsn=base_lsn)

    # ------------------------------------------------------------------

    @property
    def applied_lsn(self) -> int:
        with self._lock:
            return self._applied_lsn

    def lag_lsn(self, primary: Database) -> int:
        """尚未应用的 WAL 字节数；没有待应用记录时返回 0。"""
        with self._lock:
            if next(primary.wal.iter_records(self._applied_lsn + 1), None) is None:
                return 0
            return max(1, primary.wal.next_lsn - self._applied_lsn)

    def lag_from_server(
        self,
        host: str,
        port: int,
        *,
        timeout: float = 10.0,
        token: str | None = None,
    ) -> int:
        """通过 TCP 读取主库当前 LSN，计算尚未应用的字节数。"""
        from .wal_stream import WALClient

        with self._lock:
            status = WALClient(host, port, timeout=timeout, token=token).status()
            lag = status["current_lsn"] - self._applied_lsn
            return max(0, lag)

    def apply_from_primary(self, primary: Database) -> int:
        """读取主库 WAL 中尚未应用的部分，返回本次应用的记录数。"""
        with self._lock:
            base_lsn = getattr(primary.wal, "base_lsn", 0)
            if base_lsn and self._applied_lsn + 1 < base_lsn:
                raise StorageError(
                    "副本落后于主库已回收的 WAL，需要重新基准备份",
                    detail=f"applied_lsn={self._applied_lsn} base_lsn={base_lsn}",
                )
            records = primary.wal.iter_records(self._applied_lsn + 1)
            return self.apply(records, primary=primary)

    def replicate_from_server(
        self,
        host: str,
        port: int,
        *,
        max_records: int = 1000,
        timeout: float = 10.0,
        token: str | None = None,
    ) -> int:
        """通过 TCP 拉取一批 WAL 记录并应用，返回本次应用条数。"""
        with self._lock:
            from .wal_stream import WALClient

            client = WALClient(host, port, timeout=timeout, token=token)
            records = client.fetch(self._applied_lsn, max_records=max_records)
            applied = self.apply(records, primary=None)
            if applied:
                client.ack(self._applied_lsn)
            return applied

    def apply(self, records, *, primary: Database | None = None) -> int:
        with self._lock:
            count = 0
            for record in records:
                if record.lsn <= self._applied_lsn:
                    continue
                if record.rtype == LogType.PAGE_IMAGE:
                    self._apply_page_image(record)
                elif record.rtype in (LogType.UPDATE, LogType.CLR):
                    self._apply_page_update(record, primary)
                elif record.rtype == LogType.BEGIN:
                    self.db.txns.note_replica_state(record.xid, TxnState.ACTIVE)
                elif record.rtype == LogType.COMMIT:
                    self.db.txns.note_replica_state(record.xid, TxnState.COMMITTED)
                elif record.rtype == LogType.ABORT:
                    self.db.txns.note_replica_state(record.xid, TxnState.ABORTED)
                # CHECKPOINT 只对主库自己的恢复有意义；副本按 LSN 继续追。
                self._applied_lsn = record.lsn
                count += 1

            if count:
                # 先把页写下去，再推进副本 LSN；崩溃后最多重放一段记录，
                # pageLSN 会让重复应用变成幂等操作。
                self.db.bp.flush_all()
                self.db.catalog.load()
                self._write_marker()
                if primary is not None:
                    retain = getattr(primary.wal, "retain_lsn", None)
                    if retain is not None:
                        primary.wal.retain_lsn = self._applied_lsn
            return count

    def _apply_page_image(self, record) -> None:
        page_id, image = decode_page_image(record.payload)
        self.db.disk.ensure_page(page_id)
        page = self.db.bp.fetch_page(page_id)
        try:
            if page.page_lsn < record.lsn:
                page.buf[:] = image
                page.page_lsn = record.lsn
                self.db.bp.mark_dirty(page)
        finally:
            self.db.bp.unpin(page_id)

    def _apply_page_update(self, record, primary: Database | None) -> None:
        page_id, offset, _old, new = decode_page_update(record.payload)
        self.db.disk.ensure_page(page_id)
        page = self.db.bp.fetch_page(page_id)
        try:
            # 新页的“分配 + 初始化”不是一条独立 WAL 记录：主库只把后续
            # 页内 diff 写进 WAL。副本看到 FREE 页时，先向主库取一次完整
            # 页镜像（在真实网络实现里就是一个 PAGE_IMAGE 请求），再应用
            # 当前 diff。已有页则只按 pageLSN 应用 diff。
            if page.page_type == PageType.FREE and primary is not None:
                primary_page = primary.bp.fetch_page(page_id)
                try:
                    page.buf[:] = primary_page.buf
                    page.page_lsn = primary_page.page_lsn
                finally:
                    primary.bp.unpin(page_id)
                self.db.bp.mark_dirty(page)
            if page.page_lsn < record.lsn:
                page.buf[offset : offset + len(new)] = new
                page.page_lsn = record.lsn
                self.db.bp.mark_dirty(page)
        finally:
            self.db.bp.unpin(page_id)

    def _write_marker(self) -> None:
        marker = self.path / "replica.lsn"
        tmp = self.path / "replica.lsn.tmp"
        tmp.write_text(str(self._applied_lsn), encoding="utf-8")
        os.replace(tmp, marker)

    # ------------------------------------------------------------------

    def execute_one(self, sql: str, params=None):
        with self._lock:
            return self.db.execute_one(sql, params)

    def promote(
        self,
        *,
        fence_path: str | Path | None = None,
        node_id: str = "replica",
    ) -> Database:
        """关闭只读句柄并以读写模式重新打开，完成手动提升。

        若给出 ``fence_path``，先原子写入提升 marker；旧主库如果配置了同一
        路径的 ``PromotionFence.assert_writable`` 写回调，就会在下一笔写语句
        上拒绝，形成最小的防双写约束。没有自动选举和网络租约。
        """
        with self._lock:
            if fence_path is not None:
                PromotionFence(fence_path, node_id=node_id).promote(
                    self._applied_lsn
                )
            self.db.close()
            self.db = Database(
                self.path,
                read_only=False,
                checkpoint_wal_bytes=0,
            )
            return self.db


__all__ = ["Replica"]
