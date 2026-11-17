"""数据库门面：把存储、目录、执行器组装成一个可用的对象。

CLI 与 PostgreSQL 协议服务端都只依赖这一层，它们不需要知道页、WAL、B+ 树。
"""

from __future__ import annotations

import threading
import logging
import contextlib
from pathlib import Path
from typing import Any, Callable, Optional

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
from ..storage.lock import LockManager
from ..storage.pager import DiskManager
from ..storage.recovery import RecoveryManager
from ..storage.txn import Transaction, TransactionManager
from ..storage.wal import WAL
from ..storage.wal_segments import SegmentedWAL
from .executor import Executor, Result

_log = logging.getLogger(__name__)

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

#: 会重排/重写堆页的语句：VACUUM 重排 RID 并重建索引，TRUNCATE 清空页链，
#: ALTER TYPE / DROP COLUMN 走 shadow heap（写新页再切根）。这些操作期间，
#: 一个正在扫页的 SELECT 会读到"槽位表已改、记录已搬走"的中间态——所以它们
#: 必须与所有在途只读语句互斥，而不只是与写语句互斥。
_STRUCTURAL_STATEMENTS = {
    "Vacuum",
    "Truncate",
    "AlterTableAlterType",
    "AlterTableDropColumn",
}


class Database:
    def __init__(
        self,
        path: str | Path,
        *,
        pool_size: int = 64,
        checkpoint_wal_bytes: int = 8 * 1024 * 1024,
        checkpoint_every_commits: int = 0,
        wal_segment_bytes: int = 1024 * 1024,
        read_only: bool = False,
        write_fence: Optional[Callable[[], None]] = None,
        commit_barrier: Optional[Callable[[int], None]] = None,
        wal_retain_lsn: int = 0,
        fence_path: str | Path | None = None,
        lease_path: str | Path | None = None,
        node_id: str = "primary",
    ) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self._read_only = read_only
        if fence_path is not None:
            # 共享 fencing marker：旧主库崩溃重启后重新打开数据库时，必须
            # 立即知道"别的节点已经被提升过"，否则裸 Database 打开不检查
            # fence，重启后的旧主库会双写。非只读打开时立即校验；若调用方
            # 没给更强的写围栏（租约），就把 fence 的 assert_writable 持续
            # 接进写路径（每笔写重读 marker，别的节点后来提升也能立即停写）。
            from ..replication.fence import PromotionFence

            fence = PromotionFence(fence_path, node_id=node_id)
            if not read_only:
                fence.assert_writable()
                if write_fence is None:
                    write_fence = fence.assert_writable
        if lease_path is not None:
            # TTL 写租约：与 fence 同源的重启防护。租约文件持有者不是自己
            # 且未过期 → 旧主库重启后不能写；过期则放行（等待重新 acquire
            # 的语义由调用方处理）。持续检查同样接进写路径。
            from ..replication.lease import Lease, LeaseFence

            lease = Lease(lease_path, node_id)
            if not read_only:
                lease.assert_writable()
                if write_fence is None:
                    write_fence = LeaseFence(lease).assert_writable
        self._write_fence = write_fence
        #: 同步复制的提交屏障（参数是 COMMIT 记录的 LSN）。
        self._commit_barrier = commit_barrier
        # 写路径串行化：既保护单文件 DiskManager，也作为基准备份的写冻结门。
        self._write_lock = threading.RLock()
        #: **引擎级语句锁**：Executor 把每条语句的状态（binder / 参数 / CTE
        #: 作用域 / 递归工作表）存在实例字段上，所以同一个 Database 上不能
        #: 有两条语句同时在跑。读语句不走 ``_write_lock``（备份期间要能继续
        #: 读），于是"两个线程同时 SELECT"会互相覆盖 binder，拿到对方的参数
        #: 或丢失 CTE 作用域。
        #:
        #: 这把锁只包住"引擎执行"这一段：它是 RLock、不参与 quiesce，
        #: close()/snapshot() 仍然只拿 ``_write_lock``，所以"备份时读可以继续"
        #: 的语义不变。要真正支持并发语句，必须把 Executor 的语句状态改成
        #: 按调用传递的上下文（见 docs/concurrency.md 的路线）。
        self._statement_lock = threading.RLock()
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
        # 复制槽的启动顺序：在恢复和任何自动 checkpoint 之前就恢复保留点，
        # 否则主库重启后的第一次 checkpoint 可能把槽位之前的段回收掉。
        if wal_retain_lsn > 0 and isinstance(self.wal, SegmentedWAL):
            self.wal.retain_lsn = wal_retain_lsn
        self.bp = BufferPoolManager(self.disk, self.wal, pool_size=pool_size)
        #: 关闭标志：放在 TransactionManager 之前，提交点围栏回调在初始化
        #: 阶段就能安全读取它。
        self._closed = False
        #: 正在执行的只读语句数；close() 要等它们退出再关磁盘句柄。
        self._readers = 0
        #: 结构性写（VACUUM / TRUNCATE / shadow heap 重写）正在独占期间为 True：
        #: 新的只读语句必须等它结束，否则会读到重排中的页。
        self._no_readers = False
        #: 注意这个 Condition 绑在 ``_write_lock`` 上：``with self._readers_cv``
        #: 等于拿写者锁。读语句只在此处进出各一次（计数 +1/-1），粒度极短，
        #: 所以并发读仍然成立。
        self._readers_cv = threading.Condition(self._write_lock)
        #: 锁管理器：写语句在 ``Executor.run`` 里申请事务级写者锁（单写者模型）。
        self.locks = LockManager()
        self.txns = TransactionManager(
            self.disk,
            self.wal,
            self.bp,
            self.locks,
            # 提交点也用统一入口：它同时检查 fence 与 _closed。只传裸
            # _write_fence 会让"关闭后仍提交"漏过关闭检查。
            write_fence=self.assert_writable,
            commit_barrier=self._commit_barrier,
        )
        self._checkpoint_wal_bytes = checkpoint_wal_bytes
        self._checkpoint_every_commits = max(0, checkpoint_every_commits)
        self._commits_since_checkpoint = 0

        self.recovery = RecoveryManager(
            self.disk, self.wal, self.bp, self.txns
        )
        self.recovery_report = self.recovery.recover()

        self.catalog = Catalog(CatalogStore(self.bp))
        self.catalog.load()
        self.executor = Executor(
            self.bp,
            self.catalog,
            self.txns,
            self.wal,
            self.disk,
            self.locks,
            read_only=self._read_only,
        )

        self._txn: Optional[Transaction] = None

    # ------------------------------------------------------------------

    def set_commit_barrier(
        self, barrier: Optional[Callable[[int], None]]
    ) -> None:
        """安装/替换同步复制的提交屏障（提升之后由 ``Replica`` 安装）。"""
        self._commit_barrier = barrier
        self.txns.set_commit_barrier(barrier)

    def make_executor(self) -> Executor:
        """为**一个会话**创建独立的执行器（共享缓冲池/目录/事务/锁）。

        `Executor` 把每条语句的状态（binder、`$n` 参数、CTE 作用域、递归工作表）
        存在实例字段上，所以"同一个执行器"天生只能串行使用。要并发跑语句，
        最小的一步不是重写执行器，而是让每个会话拿一个自己的执行器：
        它们共享 `bp/catalog/txns/wal/locks`，只有语句状态是私有的。

        真正的写并发还需要行/键锁（见 docs/concurrency.md 第 2 步）；
        在那之前，独立执行器只保证"互不踩状态"，不保证同时写同一份数据安全。
        """
        return Executor(
            self.bp,
            self.catalog,
            self.txns,
            self.wal,
            self.disk,
            self.locks,
            read_only=self._read_only,
        )

    def assert_writable(self) -> None:
        """写围栏的**唯一入口**。

        任何新增写路径都必须调用它：``Database._run``、``PGWireServer``
        的 DML/COMMIT 分支，以及 ``TransactionManager.commit``（提交点
        复检，见那里的注释）。只在一个地方检查的后果已经出现过两次：
        线协议会话直接调 ``executor.run``，绕过了 ``Database._run``。
        """
        if self._closed:
            raise StorageError(
                "数据库已关闭",
                hint="这是并发降级（step_down/close）后的预期错误，请重试或换节点",
            )
        fence = self._write_fence
        if fence is not None:
            fence()

    @contextlib.contextmanager
    def read_guard(self):
        """只读语句的关闭互斥门。

        close() 只拿 ``_write_lock`` 时，一个正在扫描页的 SELECT 仍可能在
        flush/close 之后碰到已关闭的 DiskManager/buffer pool。这里用同一个
        锁做 reader 计数：close 置位 ``_closed`` 后等所有在途读者退出。
        """
        with self._readers_cv:
            while self._no_readers:
                self._readers_cv.wait()
            if self._closed:
                raise StorageError(
                    "数据库已关闭",
                    hint="这是并发降级（step_down/close）后的预期错误，请重试或换节点",
                )
            self._readers += 1
        try:
            yield
        finally:
            with self._readers_cv:
                self._readers -= 1
                if self._readers == 0:
                    self._readers_cv.notify_all()

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

    @contextlib.contextmanager
    def exclusive_guard(self):
        """结构性写独占门：等所有在途只读语句退出，并在期间阻止新读者进入。

        为什么必须有：``HeapFile.vacuum`` 虽然会检查 ``active_xids``，但那是
        "检查 → 重写"之间的 TOCTOU——检查通过后新开的读事务照样能扫到正在重排
        的页（slot 目录与记录位置在变）。只读语句不拿 ``_write_lock``，所以
        写者锁挡不住它们，必须靠这个门。

        语义上也更好：此前 VACUUM 遇到并发读会直接报
        "VACUUM 需要表上没有并发事务"，读密集型系统里永远跑不起来；现在它
        先等读者退出（毫秒级）再独占执行，然后放行。
        """
        with self._readers_cv:  # 等价于拿 _write_lock（本方法本来就是写路径）
            self._no_readers = True
            while self._readers:
                self._readers_cv.wait()
            try:
                yield
            finally:
                self._no_readers = False
                self._readers_cv.notify_all()

    def _run(self, statement, params: Optional[list[Any]] = None) -> Result:
        if self._closed:
            raise StorageError(
                "数据库已关闭",
                hint="这是并发降级（step_down/close）后的预期错误，请重试或换节点",
            )
        if self._read_only and not _is_read_only_statement(statement):
            raise NotSupportedError(
                "只读副本拒绝写入",
                hint="SELECT / EXPLAIN / SHOW 可以执行，写操作需要在主库完成",
            )
        with self._statement_lock:
            return self._run_locked(statement, params)

    def _run_locked(self, statement, params: Optional[list[Any]]) -> Result:
        if type(statement).__name__ not in _PURE_READ_STATEMENTS:
            guard = (
                self.exclusive_guard()
                if type(statement).__name__ in _STRUCTURAL_STATEMENTS
                else self._write_lock
            )
            with guard:
                # [Review-Fix] 关闭/围栏检查必须在拿到 _write_lock **之后**：
                # 入口检查通过后可能在等锁期间被 close 关掉，此时再写就会
                # 落到已经关闭的 WAL/磁盘句柄上。两个检查都放进锁内，
                # 与 close() 的持锁顺序一致。
                if self._closed:
                    raise StorageError(
                        "数据库已关闭",
                        hint="这是并发降级（step_down/close）后的预期错误，请重试或换节点",
                    )
                if not self._read_only:
                    self.assert_writable()
                return self._run_unlocked(statement, params)
        with self.read_guard():
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
            try:
                self.txns.commit(txn)
            except BaseException:
                # WAL 写 COMMIT 记录失败（磁盘满 / IO 错误）：事务既没持久化，
                # 就不该留下改过的页和写者锁——单写者模型下那把锁一泄漏，
                # 之后每个写事务都会拿到 40001，数据库等于永久只读。
                # 反过来，COMMIT 记录已经落盘的情况（例如同步复制屏障超时）
                # 不能 undo：事务事实上已提交，调用方拿到的是"结果未知"。
                if not getattr(txn, "commit_record_written", False):
                    self.txns.abort(txn)
                raise
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

        txn = self.txns.begin(
            read_only=type(statement).__name__ in _PURE_READ_STATEMENTS
        )
        try:
            result = self.executor.run(statement, txn, params)
            # [Review-Fix] 延迟约束只可能被写语句弄坏；对 SELECT / EXPLAIN /
            # SHOW 也跑一遍补检，会让一次纯读变成 O(全库行数) 的扫描，而且在
            # 库中已存在重复键时把读请求也判成 23505（实测）。
            if type(statement).__name__ not in _PURE_READ_STATEMENTS:
                self.executor.validate_deferred_foreign_keys(txn)
                self.executor.validate_deferred_unique_constraints(txn)
        except BaseException:
            self.txns.abort(txn)
            raise
        try:
            self.txns.commit(txn)
        except BaseException:
            # 同上：自动提交路径的 COMMIT 记录写失败时，也要把事务回滚掉，
            # 否则页上已改的数据和写者锁都会留在原地。
            if not getattr(txn, "commit_record_written", False):
                self.txns.abort(txn)
            raise
        self._maybe_checkpoint()
        return result

    # ------------------------------------------------------------------

    def checkpoint(self) -> None:
        with self._write_lock:
            self.recovery.checkpoint()
            self._commits_since_checkpoint = 0

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
            self._commits_since_checkpoint = 0
            return self.disk.snapshot_bytes(), self.disk.meta.checkpoint_lsn

    def snapshot_file(self, target: str | Path) -> int:
        """与 :meth:`snapshot` 相同的写冻结语义，但把数据文件复制到 target。

        这样大库不需要把整个数据文件读成 ``bytes``；服务端可以复制到临时
        文件后再分块发送。返回基础 checkpoint LSN。
        """
        with self._write_lock:
            if self.txns.active_xids:
                raise TransactionError(
                    "基准备份要求主库没有活跃事务",
                    hint="先让所有事务 COMMIT / ROLLBACK，再 checkpoint 和复制",
                )
            self.recovery.checkpoint()
            self._commits_since_checkpoint = 0
            self.disk.copy_to(target)
            return self.disk.meta.checkpoint_lsn

    def snapshot_pages(
        self, base_lsn: int, target_dir: str | Path
    ) -> tuple[int, int]:
        """导出页级增量基准备份：只写 pageLSN > base_lsn 的页。

        返回 ``(checkpoint_lsn, page_count)``。页 0 总是包含，因为元数据页
        保存文件头、checkpoint LSN 和目录指针。调用方在拿到页文件后自行
        传输；写锁只覆盖 checkpoint 和页扫描，不覆盖网络发送。
        """
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        base_lsn = max(0, int(base_lsn))
        with self._write_lock:
            if self.txns.active_xids:
                raise TransactionError(
                    "增量基准备份要求主库没有活跃事务",
                    hint="先让所有事务 COMMIT / ROLLBACK，再 checkpoint 和扫描页",
                )
            self.recovery.checkpoint()
            self._commits_since_checkpoint = 0
            checkpoint_lsn = self.disk.meta.checkpoint_lsn
            count = 0
            for page_id in range(self.disk.num_pages):
                data = self.disk.read_page(page_id)
                page_lsn = int.from_bytes(data[24:32], "little")
                if page_id == 0 or page_lsn > base_lsn:
                    (target / f"{page_id:08d}.page").write_bytes(data)
                    count += 1
        return checkpoint_lsn, count

    def _maybe_checkpoint(self) -> None:
        """按 WAL 字节或累计提交数自动落一次 checkpoint。

        checkpoint 是提交后的维护动作，失败不能把已经持久化的提交改判为
        失败；这里只吞掉存储层错误，编程错误仍然照常暴露。两个阈值都可为 0
        表示关闭对应策略；同时配置时任意一个命中都会触发。
        """
        self._commits_since_checkpoint += 1
        by_bytes = (
            self._checkpoint_wal_bytes > 0
            and self.wal.next_lsn - self.disk.meta.checkpoint_lsn
            >= self._checkpoint_wal_bytes
        )
        by_commits = (
            self._checkpoint_every_commits > 0
            and self._commits_since_checkpoint >= self._checkpoint_every_commits
        )
        if not (by_bytes or by_commits):
            return
        try:
            self.recovery.checkpoint()
            self._commits_since_checkpoint = 0
        except (StorageError, OSError):
            # WAL 中的提交记录已经是持久的；下一次启动会重新恢复。
            return

    def maybe_checkpoint(self) -> None:
        """给服务端等非 ``Database._run`` 提交路径使用的公开钩子。"""
        self._maybe_checkpoint()

    def close(self) -> None:
        """关闭数据库句柄。

        [Review-Fix] 关闭必须与写语句互斥：并发降级（``Replica.step_down``）
        会在另一个线程里调到这里，若直接 ``flush_all`` + 关 fd，正在执行的
        语句会把页写到已经关闭的句柄上。
        """
        with self._write_lock:
            if self._closed:
                return  # 幂等：step_down() 之后 Replica.close() 还会再调一次
            self._closed = True
            # 先等只读语句退出，再 abort 事务/关闭句柄；反过来的话，一个
            # 正在扫描页的 SELECT 可能在 undo 之后继续用已失效的页。
            self._readers_cv.notify_all()
            while self._readers:
                self._readers_cv.wait()
            if self._txn is not None:
                self.txns.abort(self._txn)
                self._txn = None
            # 线协议事务不经过 self._txn；不 abort 它们会在 close 之后继续
            # 持有写者锁/未提交页，甚至把 COMMIT 写进已关闭的 WAL。
            try:
                aborted = self.txns.abort_all()
                if aborted:
                    _log.info("关闭数据库时回滚了 %s 个活动事务", aborted)
            except Exception:
                _log.exception("关闭数据库时回滚活动事务失败")
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
