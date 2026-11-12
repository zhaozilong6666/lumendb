"""数据库门面：把存储、目录、执行器组装成一个可用的对象。

CLI 与 PostgreSQL 协议服务端都只依赖这一层，它们不需要知道页、WAL、B+ 树。
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..catalog.catalog import Catalog
from ..catalog.store import CatalogStore
from ..errors import TransactionError
from ..sql.ast import Begin, Commit, Rollback
from ..sql.parser import parse_sql
from ..storage.bufferpool import BufferPoolManager
from ..storage.pager import DiskManager
from ..storage.recovery import RecoveryManager
from ..storage.txn import Transaction, TransactionManager
from ..storage.wal import WAL
from .executor import Executor, Result


class Database:
    def __init__(self, path: str | Path, *, pool_size: int = 64) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.disk = DiskManager(self.path / "lumendb.data")
        self.wal = WAL(self.path / "lumendb.wal")
        self.bp = BufferPoolManager(self.disk, self.wal, pool_size=pool_size)
        self.txns = TransactionManager(self.disk, self.wal)

        self.recovery_report = RecoveryManager(
            self.disk, self.wal, self.bp, self.txns
        ).recover()

        self.catalog = Catalog(CatalogStore(self.bp))
        self.catalog.load()
        self.executor = Executor(self.bp, self.catalog, self.txns, self.wal, self.disk)

        self._txn: Optional[Transaction] = None

    # ------------------------------------------------------------------

    @property
    def in_transaction(self) -> bool:
        return self._txn is not None

    def execute(self, sql: str) -> list[Result]:
        """执行一段 SQL，返回每条语句的结果。

        没有显式 ``BEGIN`` 时，每条语句各自成为一个事务（自动提交）。
        """
        statements = parse_sql(sql)
        return [self._run(statement) for statement in statements]

    def execute_one(self, sql: str) -> Result:
        results = self.execute(sql)
        if not results:
            raise TransactionError("SQL 里没有可执行的语句")
        return results[-1]

    # ------------------------------------------------------------------

    def _run(self, statement) -> Result:
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
            self.txns.commit(txn)
            return Result(tag="COMMIT")

        if isinstance(statement, Rollback):
            if self._txn is None:
                raise TransactionError("没有活动事务")
            txn = self._txn
            self._txn = None
            self.txns.abort(txn)
            return Result(tag="ROLLBACK")

        if self._txn is not None:
            return self.executor.run(statement, self._txn)

        txn = self.txns.begin()
        try:
            result = self.executor.run(statement, txn)
        except BaseException:
            self.txns.abort(txn)
            raise
        self.txns.commit(txn)
        return result

    # ------------------------------------------------------------------

    def checkpoint(self) -> None:
        RecoveryManager(self.disk, self.wal, self.bp, self.txns).checkpoint()

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
