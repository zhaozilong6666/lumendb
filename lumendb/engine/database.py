"""数据库门面：把存储、目录、执行器组装成一个可用的对象。

CLI 与 PostgreSQL 协议服务端都只依赖这一层，它们不需要知道页、WAL、B+ 树。
"""

from __future__ import annotations

import threading
import logging
import contextlib
import os
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable, Optional

from ..catalog.catalog import Catalog
from ..catalog.store import CatalogStore
from ..errors import NotSupportedError, SerializationError, StorageError, TransactionError
from ..sql.ast import (
    Begin,
    Commit,
    ReleaseSavepoint,
    Rollback,
    RollbackToSavepoint,
    Savepoint,
    SetConstraints,
    Vacuum,
)
from ..sql.parser import parse_sql
from ..storage.bufferpool import BufferPoolManager
from ..storage.lock import LockManager
from ..storage.pager import DiskManager
from ..storage.recovery import RecoveryManager
from ..storage.txn import Transaction, TransactionManager, TxnState
from ..storage.wal import WAL
from ..storage.wal_segments import SegmentedWAL
from .executor import Executor, Result, _SCHEMA_CHANGING_STATEMENTS

_log = logging.getLogger(__name__)


class _FairRLock:
    """FIFO 重入锁，避免读语句风暴把写语句饿死。

    ``threading.RLock`` 不保证公平：多个只读线程释放后立刻重新竞争，等待中
    的写线程可能长期拿不到锁（实测 400 行 INSERT + 3 个 COUNT 读者要 477s）。
    这里给每个非重入等待者排一个 ticket，释放时唤醒所有等待者、只让队首进入；
    新来的读者排到写者后面，写者不会被无限插队。跨线程释放语义与
    ``_CrossThreadRLock`` 一致（保存点事务需要）。
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._owner: Optional[int] = None
        self._depth = 0
        self._waiters: deque[object] = deque()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        me = threading.get_ident()
        with self._condition:
            if self._owner == me:
                self._depth += 1
                return True
            if not blocking:
                return False
            ticket = object()
            self._waiters.append(ticket)
            try:
                deadline = None if timeout is None or timeout < 0 else time.monotonic() + timeout
                while self._owner is not None or self._waiters[0] is not ticket:
                    if deadline is None:
                        self._condition.wait()
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            self._waiters.remove(ticket)
                            self._condition.notify_all()
                            return False
                        self._condition.wait(remaining)
            except BaseException:
                if ticket in self._waiters:
                    self._waiters.remove(ticket)
                self._condition.notify_all()
                raise
            self._waiters.popleft()
            self._owner = me
            self._depth = 1
            return True

    def release(self) -> None:
        with self._condition:
            if self._depth <= 0:
                raise RuntimeError("release unlocked lock")
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
                self._condition.notify_all()

    def __enter__(self) -> "_FairRLock":
        self.acquire()
        return self

    def __exit__(self, *exc_info) -> None:
        self.release()

    @property
    def waiters(self) -> int:
        with self._condition:
            return len(self._waiters)


class _CrossThreadRLock:
    """可跨线程释放的重入锁。

    线协议把每条语句丢进线程池执行，事务级保存点却要把写者锁一直握到
    COMMIT/ROLLBACK。``threading.RLock`` 的释放必须由加锁线程完成，保存点
    事务跨 worker 时会在 COMMIT 路径抛 "cannot release a lock owned by
    another thread"。这里保留同线程重入语义，但允许持有者之外的线程释放：
    锁的互斥性由 ``_owner`` 与条件变量维护，谁释放只影响深度计数。
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._owner: Optional[int] = None
        self._depth = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        me = threading.get_ident()
        with self._condition:
            if not blocking and self._owner not in (None, me):
                return False
            if timeout is not None and timeout >= 0:
                # 目前没有调用方用超时；保留参数兼容 threading.Lock 签名。
                deadline = time.monotonic() + timeout
                while self._owner not in (None, me):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._condition.wait(remaining)
            else:
                while self._owner not in (None, me):
                    self._condition.wait()
            self._owner = me
            self._depth += 1
            return True

    def release(self) -> None:
        with self._condition:
            if self._depth <= 0:
                raise RuntimeError("release unlocked lock")
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
                self._condition.notify()

    def __enter__(self) -> "_CrossThreadRLock":
        self.acquire()
        return self

    def __exit__(self, *exc_info) -> None:
        self.release()

    def _is_owned(self) -> bool:
        """兼容内部诊断：当前线程是否持有这把锁。"""
        with self._condition:
            return self._owner == threading.get_ident()


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


_PURE_READ_STATEMENTS = {
    "Query",
    "Select",
    "Explain",
    "ShowTables",
    "ShowViews",
    "ShowCatalogVersions",
    "ShowColumns",
    "ShowStatistics",
    "ShowCommitLog",
}

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


def _blocks_active_readers(statement) -> bool:
    """DDL 是否会让旧快照读到切换后的新结构。

    ``ALTER TABLE`` 的任何子命令都会改目录里的表定义；旧快照事务在 DDL
    之后继续查询时，可能按新列布局解码旧计划，或看到新列默认值，破坏
    快照隔离。``TRUNCATE`` 更是直接换掉页链；``DROP TABLE/VIEW/INDEX`` 与
    ``REINDEX`` 会让旧快照手里的对象/索引根失效。``CREATE TABLE`` / 新建
    索引不影响已有表的快照，不必为此挡住只读事务。
    """
    if statement is None:
        return False
    name = type(statement).__name__
    return (
        name == "Truncate"
        or name.startswith("AlterTable")
        or name in {"DropTable", "DropView", "DropIndex", "Reindex"}
    )


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
        concurrent_dml: bool = False,
        logical_savepoints: bool = True,
        vacuum_online_max_versions: int = 250_000,
    ) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self._read_only = read_only
        version_cap = int(vacuum_online_max_versions)
        if version_cap <= 0:
            raise ValueError("vacuum_online_max_versions 必须是正整数")
        #: 在线 VACUUM 一次性建立版本链索引的物理版本上限。默认 250,000；
        #: 超限失败关闭，提示改用离线 VACUUM。产品可按内存预算调低/调高。
        self._vacuum_online_max_versions = version_cap
        #: 写并发模式：DML 不拿 ``_write_lock``，改由行/键锁 + 页 latch +
        #: ``read_guard`` 与 DDL/close 互斥。默认关闭；环境变量可让全套测试
        #: 用同一条路径跑一遍（CI 门禁用）。
        if not concurrent_dml:
            concurrent_dml = os.environ.get("LUMEN_CONCURRENT_DML") == "1"
        self._concurrent_dml = concurrent_dml
        #: 保存点走行级逻辑 undo（实验）：不拿全局写者锁，靠逐条堆写逆操作
        #: 撤回。只覆盖 DML；事务内 DDL/TRUNCATE/VACUUM 会被拒绝。
        self._logical_savepoints = bool(logical_savepoints and concurrent_dml)
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
        self._write_lock = _CrossThreadRLock()
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
        self._statement_lock = _FairRLock()
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
        self.bp.logical_abort = concurrent_dml
        #: 关闭标志：放在 TransactionManager 之前，提交点围栏回调在初始化
        #: 阶段就能安全读取它。
        self._closed = False
        #: 正在执行的只读语句数；close() 要等它们退出再关磁盘句柄。
        self._readers = 0
        #: 结构性写（VACUUM / TRUNCATE / shadow heap 重写）正在独占期间为 True：
        #: 新的只读语句必须等它结束，否则会读到重排中的页。
        self._no_readers = False
        #: 读者计数用独立的小锁，不再借 ``_write_lock``。并发 DML 的每条语句
        #: 都要进出 ``read_guard``；如果这里拿写者锁，等于把并发语句重新串行
        #: 到同一把全局锁上（实测 4 线程插入吞吐反而低于默认模式）。
        #: ``exclusive_guard`` / ``close`` 仍持有 ``_write_lock`` 挡住默认写者，
        #: 再通过本 Condition 等读者退出。
        self._readers_lock = threading.Lock()
        self._readers_cv = threading.Condition(self._readers_lock)
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
        self.txns.install_commit_lsns(self.recovery_report.commit_lsns)

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
            concurrent_dml=self._concurrent_dml,
            vacuum_online_max_versions=self._vacuum_online_max_versions,
        )

        self._txn: Optional[Transaction] = None
        #: 当前持有保存点独占门的事务。线协议事务不挂在 ``_txn`` 上，close
        #: 时也要靠它找到并释放保存点模式。
        self._savepoint_txn: Optional[Transaction] = None

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
            concurrent_dml=self._concurrent_dml,
            vacuum_online_max_versions=self._vacuum_online_max_versions,
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
            while self._no_readers and not self._closed:
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
        with self._write_lock:
            with self._readers_cv:
                self._no_readers = True
                while self._readers:
                    self._readers_cv.wait()
            try:
                yield
            finally:
                with self._readers_cv:
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

    def _enter_savepoint_mode(self, txn: Transaction) -> None:
        """并发 DML 下第一次 SAVEPOINT：把事务切回"物理可回滚"并独占写者锁。

        逻辑回滚没法实现 ``ROLLBACK TO SAVEPOINT``——保存点之后的 INSERT 已经
        写进堆页且没有原像，回滚只能靠"整事务可见性"，而事务还要继续提交。
        正确性优先：保存点事务拿住 ``_write_lock`` 到事务结束，期间没有别的
        写者，页原像就不会被并发写撕裂；代价是该事务存续期间写操作串行。
        默认（非并发）模式不需要这一层。

        [修正] 旧实现要求"全库没有其它活跃事务"，否则直接 0A000。这个门禁
        过严：只要把在途语句排空、再让后续语句在 ``_no_readers`` 门口排队，
        保存点事务就不会和别的写者交错，物理原像也不会被覆盖。已经开始的
        未提交事务可以留着，它们在保存点事务结束前不能进入下一条写语句。
        """
        if not self._concurrent_dml or txn.physical_savepoint:
            return
        if self._logical_savepoints:
            # 行级逻辑 undo：保存点事务继续和别的写者并行，只记录自己的
            # INSERT/UPDATE/DELETE 逆操作；ROLLBACK TO 逐条撤回。
            txn.capture_row_undo = True
            return
        self._acquire_physical_savepoint(txn)

    def _acquire_physical_savepoint(
        self,
        txn: Transaction,
        *,
        guard_row_undo: bool = False,
        block_active_readers: bool = False,
    ) -> None:
        """把事务切到物理页原像回滚，并独占写者锁到事务结束。

        ``guard_row_undo=True`` 用于逻辑保存点的 late upgrade：拿到
        ``_write_lock`` 并把在途读者排空后，再检查别的活跃事务是否仍持有
        行 undo / 未提交写入 / 旧快照。此时 ``_no_readers`` 已经挡住新的
        DML，检查通过后不会再产生新的行 undo；若检查失败，必须把刚拿的
        门完整复位再抛错，不能让事务停在半升级状态。
        """
        self._write_lock.acquire()
        try:
            with self._readers_cv:
                # 先置位再等：新语句不得进入，已有语句执行完退出。
                self._no_readers = True
                self._readers_cv.notify_all()
                while self._readers:
                    self._readers_cv.wait()
            # 在途 DML 已排空，新的 DML 又被 _no_readers 挡住；这份检查
            # （行 undo + 其它未提交写入/旧快照）才是稳定的。放在设置
            # physical_savepoint 之前，失败时事务仍保持逻辑保存点状态。
            if guard_row_undo:
                self._assert_no_ddl_blockers(
                    txn, block_active_readers=block_active_readers
                )
        except BaseException:
            with self._readers_cv:
                self._no_readers = False
                self._readers_cv.notify_all()
            self._write_lock.release()
            raise
        txn.capture_row_undo = False
        txn.row_undo_log.clear()
        txn.physical_savepoint = True
        self._savepoint_txn = txn
        self.bp.logical_abort = False

    def _run_in_physical_savepoint(self, statement, params):
        """保存点事务已独占写者锁：直接执行，跳过 read_guard 免得等自己。"""
        if self._closed:
            raise StorageError(
                "数据库已关闭",
                hint="这是并发降级（step_down/close）后的预期错误，请重试或换节点",
            )
        if not self._read_only:
            self.assert_writable()
        return self._run_unlocked(statement, params)

    def _maybe_upgrade_logical_savepoint(self, statement, txn) -> bool:
        """逻辑保存点遇到 DDL/TRUNCATE 时，按需升级成物理保存点。

        只有保存点之后还没有堆 DML（``row_undo_log`` 为空）才能升级：此时
        页原像从保存点开始，DDL 的页回滚不会和行 undo 的 RID/槽位假设冲突。
        一旦已有行 undo，必须拒绝；升级拿门后还要检查别的活跃事务的行 undo、
        未提交写入或旧快照，否则这条晚升级路径会绕过普通 ``ddl_guard``。
        """
        if not (
            self._concurrent_dml
            and txn is not None
            and getattr(txn, "capture_row_undo", False)
            and type(statement) in _SCHEMA_CHANGING_STATEMENTS
        ):
            return False
        if txn.row_undo_log:
            raise NotSupportedError(
                "逻辑保存点内有 DML 后不能再执行 DDL/TRUNCATE",
                detail="页原像回滚会和行级 undo 的 RID/槽位假设冲突",
                hint="先 ROLLBACK TO SAVEPOINT 或 COMMIT 整个事务，再执行结构变更",
            )
        self._acquire_physical_savepoint(
            txn,
            guard_row_undo=True,
            block_active_readers=_blocks_active_readers(statement),
        )
        return True

    def _row_undo_holder(
        self, exclude: Transaction | None = None
    ) -> Transaction | None:
        """找一个仍持有行 undo 的活跃事务；没有则返回 None。"""
        for txn in self.txns.active_transactions():
            if txn is exclude:
                continue
            if getattr(txn, "capture_row_undo", False) and txn.row_undo_log:
                return txn
        return None

    def _assert_no_row_undo(self, txn: Transaction | None) -> None:
        """DDL 前拒绝仍有行 undo 的活跃事务。

        ALTER DROP COLUMN / ALTER TYPE 会改行布局，逻辑保存点的行 undo 之后
        再按新 schema 解码旧版本会直接报列数不符。这里宁可让 DDL 返回可重试
        的 40001，也不允许它在别的保存点事务未撤回时改结构。
        """
        blocker = self._row_undo_holder(txn)
        if blocker is not None:
            raise SerializationError(
                "逻辑保存点事务持有行 undo，DDL/TRUNCATE 暂不可执行",
                detail=f"事务 {getattr(blocker, 'xid', '?')} 的保存点行 undo 尚未撤回",
                hint="等该事务 COMMIT/ROLLBACK 后重试",
            )

    def _other_writer_holder(
        self, exclude: Transaction | None = None
    ) -> Transaction | None:
        """找另一个仍有未提交写入的活跃事务；没有则返回 None。

        DDL 的 shadow heap 重写只复制对 DDL 事务可见的行。若别的会话已经
        INSERT/UPDATE 但尚未 COMMIT，这些行不会被复制进新 heap；该会话
        之后 COMMIT，数据就落在已经被目录替换掉的旧 heap 里，查询静默
        少行。逻辑保存点门只覆盖 row undo，普通未提交写必须单独挡住。
        """
        for txn in self.txns.active_transactions():
            if txn is exclude:
                continue
            if getattr(txn, "wrote", False):
                return txn
        return None

    def _active_txn_holder(
        self, exclude: Transaction | None = None
    ) -> Transaction | None:
        """找另一个仍活跃的事务；没有则返回 None。"""
        for txn in self.txns.active_transactions():
            if txn is not exclude:
                return txn
        return None

    def _assert_no_ddl_blockers(
        self,
        txn: Transaction | None,
        *,
        block_active_readers: bool = False,
    ) -> None:
        """DDL/TRUNCATE 前拒绝行 undo 或其它未提交写入。

        ``block_active_readers`` 用于 shadow heap 重写类结构变更：旧事务的
        快照仍引用旧 heap，结构切换后它再查询会看到空结果或错误布局。
        这类语句不仅要挡住写入者，也要挡住仍在活跃的只读事务。
        """
        self._assert_no_row_undo(txn)
        blocker = self._other_writer_holder(txn)
        if blocker is not None:
            raise SerializationError(
                "其它事务有未提交写入，DDL/TRUNCATE 暂不可执行",
                detail=f"事务 {getattr(blocker, 'xid', '?')} 尚未 COMMIT/ROLLBACK",
                hint="等该事务结束后重试",
            )
        if block_active_readers:
            blocker = self._active_txn_holder(txn)
            if blocker is not None:
                raise SerializationError(
                    "其它事务仍在活跃，DDL/TRUNCATE 暂不可执行",
                    detail=f"事务 {getattr(blocker, 'xid', '?')} 的快照仍可能引用旧结构",
                    hint="等该事务 COMMIT/ROLLBACK 后重试",
                )

    @contextlib.contextmanager
    def ddl_guard(self, txn: Transaction | None = None, statement=None):
        """DDL/TRUNCATE 的独占门：先拒绝行 undo/未提交写，再与所有读者互斥。

        检查分两次：拿门前的检查挡住已经存在的保存点行 undo；拿到
        ``exclusive_guard`` 后再查一次，挡住"检查通过、拿门之前"新写入的
        行 undo。拿门后 ``_no_readers`` 会挡住新的 DML，因此第二次检查通过
        后结构变更期间不会再有行 undo 产生。shadow heap 重写类语句还会
        挡住仍在活跃的旧快照事务。
        """
        block_active_readers = _blocks_active_readers(statement)
        self._assert_no_ddl_blockers(
            txn, block_active_readers=block_active_readers
        )
        with self.exclusive_guard():
            self._assert_no_ddl_blockers(
                txn, block_active_readers=block_active_readers
            )
            if self._closed:
                raise StorageError(
                    "数据库已关闭",
                    hint="这是并发降级（step_down/close）后的预期错误，请重试或换节点",
                )
            if not self._read_only:
                self.assert_writable()
            yield

    def _exit_savepoint_mode(self, txn: Transaction) -> None:
        """事务结束：恢复逻辑回滚并释放保存点模式占用的写者锁。"""
        if getattr(txn, "capture_row_undo", False):
            txn.capture_row_undo = False
            txn.row_undo_log.clear()
        if not getattr(txn, "physical_savepoint", False):
            return
        txn.physical_savepoint = False
        if self._savepoint_txn is txn:
            self._savepoint_txn = None
        self.bp.logical_abort = self._concurrent_dml
        with self._readers_cv:
            self._no_readers = False
            self._readers_cv.notify_all()
        self._write_lock.release()

    def wait_for_vacuum_window(self, xid: Optional[int] = None) -> bool:
        """并发模式下先等一个没有其它活跃事务的 VACUUM 窗口。

        重排 RID / 重建索引不能和别的活跃事务共存；旧行为是一进独占门就撞
        ``active_xids`` 直接报错。现在先在门外等，最多 5s；超时不报错，
        执行器会转“保 RID 的在线回收”，保留活跃快照仍需要的版本。
        返回是否真的等到了空窗口。
        """
        if self._concurrent_dml:
            return self.txns.wait_until_no_others(xid, timeout=5.0)
        return True

    def _run_locked(self, statement, params: Optional[list[Any]]) -> Result:
        # 保存点事务已经持有写者锁并置了 ``_no_readers``：它自己的后续语句
        # 不能再进 ``read_guard``，否则会等自己放行。这里直接执行，关闭/围栏
        # 检查仍然保留。
        if (
            self._concurrent_dml
            and self._txn is not None
            and getattr(self._txn, "physical_savepoint", False)
        ):
            return self._run_in_physical_savepoint(statement, params)

        if self._maybe_upgrade_logical_savepoint(statement, self._txn):
            return self._run_in_physical_savepoint(statement, params)

        # 并发模式下第一次 SAVEPOINT：先拿写者锁、排空在途语句，再把事务
        # 切到物理回滚。必须在进 ``read_guard`` 之前做，否则当前语句自己
        # 也在读者计数里，会永远等不到 0。
        if (
            isinstance(statement, Savepoint)
            and self._concurrent_dml
            and self._txn is not None
            and not getattr(self._txn, "physical_savepoint", False)
        ):
            self._enter_savepoint_mode(self._txn)
            try:
                if self._closed:
                    raise StorageError(
                        "数据库已关闭",
                        hint="这是并发降级（step_down/close）后的预期错误，请重试或换节点",
                    )
                if not self._read_only:
                    self.assert_writable()
                return self._run_unlocked(statement, params)
            except BaseException:
                # 保存点还没登记成功就失败了：不能把库一直锁在独占模式。
                self._exit_savepoint_mode(self._txn)
                raise

        # 并发写模式下，DDL/TRUNCATE 必须先确认没有别的逻辑保存点事务仍
        # 持有行 undo；否则结构变更会改行布局，回滚时按新 schema 解码旧版本
        # 会直接报列数不符。这里用 ddl_guard 同时承担独占读者门。
        if self._concurrent_dml and type(statement) in _SCHEMA_CHANGING_STATEMENTS:
            with self.ddl_guard(self._txn, statement):
                return self._run_unlocked(statement, params)

        # VACUUM 要重排 RID/重建索引：先在独占门外等其它活跃事务退出，
        # 再走结构性写的 exclusive_guard。默认模式没有并发活跃事务，不额外等。
        if isinstance(statement, Vacuum):
            self.wait_for_vacuum_window(
                self._txn.xid if self._txn is not None else None
            )

        if type(statement).__name__ not in _PURE_READ_STATEMENTS:
            if (
                self._concurrent_dml
                and type(statement).__name__ not in _STRUCTURAL_STATEMENTS
            ):
                # 并发写模式：DML 只登记为"读者"，与 DDL/close 的独占门互斥；
                # 真正的写冲突交给行/键锁与页 latch。
                guard = self.read_guard()
            else:
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
                self._exit_savepoint_mode(txn)
                raise
            try:
                self.txns.commit(txn)
            except BaseException:
                # 只有 COMMITTED 才说明 WAL 已 fsync 成功（或至少提交点已跨过）。
                # `commit_record_written` 只表示 COMMIT 记录进了内存 buffer：
                # fsync 失败时它既没有持久化、事务也仍是 ACTIVE；此时必须走
                # abort 撤销页写并追加 ABORT 记录，否则它会一直持有 undo，
                # 后续事务改同一页后，再回滚就会把后来的提交一起覆盖。
                # 同步复制屏障失败时 txn.state 已是 COMMITTED，不能 undo。
                if txn.state is not TxnState.COMMITTED:
                    self.txns.abort(txn)
                self._exit_savepoint_mode(txn)
                raise
            self._maybe_checkpoint()
            self._exit_savepoint_mode(txn)
            return Result(tag="COMMIT")

        if isinstance(statement, Rollback):
            if self._txn is None:
                raise TransactionError("没有活动事务")
            txn = self._txn
            self._txn = None
            self.txns.abort(txn)
            self._exit_savepoint_mode(txn)
            return Result(tag="ROLLBACK")

        if isinstance(statement, Savepoint):
            if self._txn is None:
                raise TransactionError("SAVEPOINT 必须在事务中执行")
            self._enter_savepoint_mode(self._txn)
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
        txn.statement_scoped = True
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
            # 同上：用事务状态而不是“COMMIT 记录已写”来判断。记录进 buffer
            # 不等于 fsync 成功；未 COMMITTED 就必须撤销，避免半提交事务
            # 的 undo 在后续提交之后才执行、把别人的页改动覆盖掉。
            if txn.state is not TxnState.COMMITTED:
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
            # 关闭期间置 no_readers：新读者不再进门，而是直接收到
            # "数据库已关闭"；否则持续涌入的读者会让这里等不到 0。
            with self._readers_cv:
                self._no_readers = True
                self._readers_cv.notify_all()
                while self._readers:
                    self._readers_cv.wait()
            if self._txn is not None:
                txn = self._txn
                self._txn = None
                try:
                    self.txns.abort(txn)
                except Exception:
                    # abort 的 undo 失败现在会抛错（锁已释放、状态已落
                    # ABORTED），但 close 必须继续关句柄；异常只留痕。
                    _log.exception("关闭数据库时回滚当前事务失败，继续关闭")
                # 保存点事务还握着写者锁并置了 _no_readers；同线程 close 是
                # 可重入进来的，不在这里放门会留下永久 no_readers。
                self._exit_savepoint_mode(txn)
            # 线协议事务不经过 self._txn；不 abort 它们会在 close 之后继续
            # 持有写者锁/未提交页，甚至把 COMMIT 写进已关闭的 WAL。
            try:
                aborted = self.txns.abort_all()
                if aborted:
                    _log.info("关闭数据库时回滚了 %s 个活动事务", aborted)
            except Exception:
                _log.exception("关闭数据库时回滚活动事务失败")
            # abort_all 不带数据库门面的保存点状态；线协议事务也可能握着
            # 保存点写者锁，这里统一释放，避免 close 之后 no_readers 永久为真。
            if self._savepoint_txn is not None:
                self._exit_savepoint_mode(self._savepoint_txn)
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
