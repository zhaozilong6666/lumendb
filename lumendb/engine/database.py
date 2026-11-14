"""数据库门面：把存储、目录、执行器组装成一个可用的对象。

CLI 与 PostgreSQL 协议服务端都只依赖这一层，它们不需要知道页、WAL、B+ 树。
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Optional

from ..catalog.catalog import Catalog
from ..catalog.store import CatalogStore
from ..errors import NotSupportedError, StorageError, TransactionError
from ..sql.ast import (
    Begin,
    Commit,
    ReleaseSavepoint,
    Rollback,
    RollbackToSavepoint,
    Savepoint,
    SetConstraints,
)
from ..sql.parser import parse_sql
from ..storage.bufferpool import BufferPoolManager
from ..storage.pager import DiskManager
from ..storage.recovery import RecoveryManager
from ..storage.txn import Transaction, TransactionManager
from ..storage.wal import WAL
from ..storage.wal_segments import SegmentedWAL
from .executor import Executor, Result

_READ_ONLY_STATEMENTS = {
    "Query",
    "Select",
    "Explain",
    "ShowTables",
    "ShowViews",
    "Checkpoint",
    "Begin",
    "Commit",
    "Rollback",
    "Savepoint",
    "ReleaseSavepoint",
    "RollbackToSavepoint",
    "SetConstraints",
}


def _is_read_only_statement(statement: object) -> bool:
    return type(statement).__name__ in _READ_ONLY_STATEMENTS


_PURE_READ_STATEMENTS = {"Query", "Select", "Explain", "ShowTables", "ShowViews"}


class Database:
    def __init__(
        self,
        path: str | Path,
        *,
        pool_size: int = 64,
        checkpoint_wal_bytes: int = 8 * 1024 * 1024,
        wal_segment_bytes: int = 1024 * 1024,
        read_only: bool = False,
    ) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self._read_only = read_only
        # 写路径串行化：既保护单文件 DiskManager，也作为基准备份的写冻结门。
        self._write_lock = threading.RLock()
        self.disk = DiskManager(self.path / "lumendb.data")
        # 兼容旧库：如果已有单文件 WAL，继续用它；新库默认使用分段 WAL，
        # 这样 checkpoint 后可以回收旧段。
        legacy_wal = self.path / "lumendb.wal"
        if legacy_wal.exists():
            self.wal = WAL(legacy_wal)
        else:
            self.wal = SegmentedWAL(
                self.path / "wal", segment_bytes=wal_segment_bytes
            )
        self.bp = BufferPoolManager(self.disk, self.wal, pool_size=pool_size)
        self.txns = TransactionManager(self.disk, self.wal, self.bp)
        self._checkpoint_wal_bytes = checkpoint_wal_bytes

        self.recovery = RecoveryManager(
            self.disk, self.wal, self.bp, self.txns
        )
        self.recovery_report = self.recovery.recover()

        self.catalog = Catalog(CatalogStore(self.bp))
        self.catalog.load()
        self.executor = Executor(self.bp, self.catalog, self.txns, self.wal, self.disk)

        self._txn: Optional[Transaction] = None

    # ------------------------------------------------------------------

    @property
    def in_transaction(self) -> bool:
        return self._txn is not None

    def execute(self, sql: str, params: Optional[list[Any]] = None) -> list[Result]:
        """执行一段 SQL，返回每条语句的结果。

        没有显式 ``BEGIN`` 时，每条语句各自成为一个事务（自动提交）。
        ``params`` 依次绑定 SQL 里的 ``$1``、``$2``……（扩展协议用）。
        """
        statements = parse_sql(sql)
        return [self._run(statement, params) for statement in statements]

    def execute_one(
        self, sql: str, params: Optional[list[Any]] = None
    ) -> Result:
        results = self.execute(sql, params)
        if not results:
            raise TransactionError("SQL 里没有可执行的语句")
        return results[-1]

    # ------------------------------------------------------------------

    def _run(self, statement, params: Optional[list[Any]] = None) -> Result:
        if self._read_only and not _is_read_only_statement(statement):
            raise NotSupportedError(
                "只读副本拒绝写入",
                hint="SELECT / EXPLAIN / SHOW 可以执行，写操作需要在主库完成",
            )
        if type(statement).__name__ not in _PURE_READ_STATEMENTS:
            with self._write_lock:
                return self._run_unlocked(statement, params)
        return self._run_unlocked(statement, params)

    def _run_unlocked(
        self, statement, params: Optional[list[Any]] = None
    ) -> Result:
        if isinstance(statement, Begin):
            if self._txn is not None:
                raise TransactionError(
                    "已经在事务中",
                    hint="先 COMMIT 或 ROLLBACK",
                )
            self._txn = self.txns.begin()
            return Result(tag="BEGIN")

        if isinstance(statement, Commit):
            if self._txn is None:
                raise TransactionError("没有活动事务")
            txn = self._txn
            self._txn = None
            try:
                self.executor.validate_deferred_foreign_keys(txn)
                self.executor.validate_deferred_unique_constraints(txn)
            except BaseException:
                self.txns.abort(txn)
                raise
            self.txns.commit(txn)
            self._maybe_checkpoint()
            return Result(tag="COMMIT")

        if isinstance(statement, Rollback):
            if self._txn is None:
                raise TransactionError("没有活动事务")
            txn = self._txn
            self._txn = None
            self.txns.abort(txn)
            return Result(tag="ROLLBACK")

        if isinstance(statement, Savepoint):
            if self._txn is None:
                raise TransactionError("SAVEPOINT 必须在事务中执行")
            self.txns.savepoint(self._txn, statement.name)
            return Result(tag="SAVEPOINT")

        if isinstance(statement, ReleaseSavepoint):
            if self._txn is None:
                raise TransactionError("RELEASE 必须在事务中执行")
            self.txns.release(self._txn, statement.name)
            return Result(tag="RELEASE")

        if isinstance(statement, RollbackToSavepoint):
            if self._txn is None:
                raise TransactionError("ROLLBACK TO 必须在事务中执行")
            self.txns.rollback_to(self._txn, statement.name)
            return Result(tag="ROLLBACK")

        if isinstance(statement, SetConstraints) and self._txn is None:
            raise TransactionError(
                "SET CONSTRAINTS 只能在显式事务中执行",
                hint="先执行 BEGIN，再切换约束检查时机",
            )

        if self._txn is not None:
            return self.executor.run(statement, self._txn, params)

        txn = self.txns.begin()
        try:
            result = self.executor.run(statement, txn, params)
            self.executor.validate_deferred_foreign_keys(txn)
            self.executor.validate_deferred_unique_constraints(txn)
        except BaseException:
            self.txns.abort(txn)
            raise
        self.txns.commit(txn)
        self._maybe_checkpoint()
        return result

    # ------------------------------------------------------------------

    def checkpoint(self) -> None:
        with self._write_lock:
            self.recovery.checkpoint()

    def snapshot(self) -> tuple[bytes, int]:
        """冻结写、做一次 checkpoint，然后返回数据文件快照和基础 LSN。

        写锁会阻止新的 ``Database._run`` 写语句进入；已经开启的活跃事务会被
        拒绝（基准备份不能包含未提交版本）。读查询仍可继续，因为快照只读
        数据文件，不改变 Buffer Pool。
        """
        with self._write_lock:
            if self.txns.active_xids:
                raise TransactionError(
                    "基准备份要求主库没有活跃事务",
                    hint="先让所有事务 COMMIT / ROLLBACK，再 checkpoint 和复制",
                )
            self.recovery.checkpoint()
            return self.disk.snapshot_bytes(), self.disk.meta.checkpoint_lsn

    def _maybe_checkpoint(self) -> None:
        """WAL 自上次 checkpoint 增长超过阈值时自动落一次 checkpoint。

        checkpoint 是提交后的维护动作，失败不能把已经持久化的提交改判为
        失败；这里只吞掉存储层错误，编程错误仍然照常暴露。
        """
        if self._checkpoint_wal_bytes <= 0:
            return
        if (
            self.wal.next_lsn - self.disk.meta.checkpoint_lsn
            >= self._checkpoint_wal_bytes
        ):
            try:
                self.recovery.checkpoint()
            except (StorageError, OSError):
                # WAL 中的提交记录已经是持久的；下一次启动会重新恢复。
                return

    def maybe_checkpoint(self) -> None:
        """给服务端等非 ``Database._run`` 提交路径使用的公开钩子。"""
        self._maybe_checkpoint()

    def close(self) -> None:
        if self._txn is not None:
            self.txns.abort(self._txn)
            self._txn = None
        try:
            self.bp.flush_all()
        finally:
            self.bp.close()
            self.wal.close()
            self.disk.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()


__all__ = ["Database"]
