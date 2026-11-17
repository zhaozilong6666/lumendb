"""事务管理与 MVCC 可见性。

LumenDB 的并发控制只有一套机制：**快照隔离（snapshot isolation）**。

* 每个事务开始时拿一个 ``snapshot_xid`` 和当时仍活跃的事务集合；
* 一个版本对事务 T 可见，当且仅当插入者已提交且早于 T 的快照，并且删除者
  未提交或晚于 T 的快照；
* 写冲突由行级锁在更高层处理（``storage/lock.py``）。

这里刻意不实现"读已提交"与"可重复读"两套代码路径：多一套路径就多一倍
组合测试，而面试里真正被问的是"你的可见性规则是什么、为什么正确"。
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Callable, Optional

from ..errors import TransactionAbortedError, TransactionError
from .pager import DiskManager
from .wal import WAL

if TYPE_CHECKING:  # pragma: no cover
    from .bufferpool import BufferPoolManager
    from .lock import LockManager
    from .record import RID

_log = logging.getLogger(__name__)

#: 事务号预留块大小。崩溃后重启时，下一个 xid 从这个块的边界继续，
#: 保证不会复用已经写进记录头的事务号。
XID_RESERVE_BLOCK = 1024

#: 事务结局表保留的最近条目数。见 ``TransactionManager._prune_states``。
STATE_RETAIN = 1024


class TxnState(IntEnum):
    ACTIVE = 1
    COMMITTED = 2
    ABORTED = 3


@dataclass
class TxnSavepoint:
    """事务内保存点：记录页 undo、DDL 内存 undo 与延迟约束状态。"""

    name: str
    page_depth: int
    undo_depth: int
    #: 保存点建立时的延迟外键队列；ROLLBACK TO 必须一并恢复，否则保存点
    #: 之后的 INSERT/DELETE 会把已撤销的检查残留到提交阶段。
    deferred_fk_rows: frozenset[tuple[str, "RID"]] = field(default_factory=frozenset)
    deferred_ref_checks: frozenset[tuple[str, str, tuple[Any, ...]]] = field(
        default_factory=frozenset
    )
    constraint_modes: dict[str, str] = field(default_factory=dict)


@dataclass
class Transaction:
    xid: int
    snapshot_xid: int
    active_at_start: frozenset[int]
    state: TxnState = TxnState.ACTIVE
    first_lsn: int = 0
    last_lsn: int = 0
    #: COMMIT 记录是否已经写进 WAL。**不能用 last_lsn 判断**：事务开始时
    #: last_lsn 就被置成 first_lsn（非 0），WAL 写失败时它不会变回 0。
    #: 提交失败后要靠这个标记区分"没持久化（必须回滚）"与"记录已落盘、
    #: 只是后续步骤失败（事实上已提交，不能 undo）"。
    commit_record_written: bool = False
    #: DDL 的内存逆操作；abort 时逆序执行，页级 undo 由 WAL 负责。
    undo_log: list = field(default_factory=list)
    #: SAVEPOINT 栈；ROLLBACK TO 只撤销保存点之后的页/对象变更。
    savepoints: list[TxnSavepoint] = field(default_factory=list)
    #: 延迟外键：提交时需要重新检查的行，以及被删除/改写的被引用键。
    deferred_fk_rows: set[tuple[str, RID]] = field(default_factory=set)
    deferred_ref_checks: set[tuple[str, str, tuple[Any, ...]]] = field(
        default_factory=set
    )
    #: ``SET CONSTRAINTS`` 对当前事务生效的约束时机覆盖。
    constraint_modes: dict[str, str] = field(default_factory=dict)
    #: 本事务写过（或 DDL 改过）的表名。
    #:
    #: 提交前补检延迟 UNIQUE 时要按表扫描，没有这个集合就只能"每张表都扫一遍"
    #: ——一个只写 1 行的语句也会变成 O(全库行数)。保守取并集即可：多扫一张表
    #: 只是浪费，不会漏判。
    touched_tables: set[str] = field(default_factory=set)
    #: 本事务是否真的写过页。提交点复检写围栏时用它区分"只读事务"：
    #: 自动提交的 SELECT 也会 begin/commit 一对，不能因为旧主被 fence 掉
    #: 就连读都不让做。
    wrote: bool = False
    #: 入口就确定"这条语句只会读"的事务（自动提交的 SELECT/EXPLAIN/SHOW）。
    #: 它们不写 BEGIN/COMMIT/ABORT 记录，所以既不争 WAL 锁，也不在提交时
    #: 触发 fsync；MVCC 只需要内存里的结局表。
    read_only: bool = False

    def require_active(self) -> None:
        if self.state is not TxnState.ACTIVE:
            raise TransactionAbortedError(
                f"事务 {self.xid} 已 {'提交' if self.state is TxnState.COMMITTED else '回滚'}，不能继续使用",
                hint="BEGIN 一个新事务",
            )

    @property
    def is_active(self) -> bool:
        return self.state is TxnState.ACTIVE

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Txn {self.xid} {self.state.name}>"


class TransactionManager:
    """事务号分配、状态维护与可见性判定。"""

    def __init__(
        self,
        disk: DiskManager,
        wal: WAL,
        buffer_pool: Optional["BufferPoolManager"] = None,
        locks: Optional["LockManager"] = None,
        write_fence: Optional[Callable[[], None]] = None,
        commit_barrier: Optional[Callable[[int], None]] = None,
    ) -> None:
        self._disk = disk
        self._wal = wal
        self._bp = buffer_pool
        self._locks = locks
        #: 写围栏回调（租约/任期/提升 marker）。它在**提交点**再检查一次：
        #: 语句入口的检查不足以保证安全，一条长语句可以跨越租约到期。
        self._write_fence = write_fence
        #: 提交屏障：本地 COMMIT 记录落盘后、返回成功前调用，参数是本事务
        #: COMMIT 记录的 LSN。同步复制用它等"多数派已持有该 LSN"。
        self._commit_barrier = commit_barrier
        self._lock = threading.RLock()
        self._states: dict[int, TxnState] = {}
        self._active: set[int] = set()
        #: 活动事务对象表。线协议事务不一定挂在 Database._txn 上；关闭数据库
        #: 时需要能逐个 abort，否则它们会继续持有写者锁/未提交页。
        self._txns: dict[int, Transaction] = {}
        #: 活跃事务的 BEGIN LSN；checkpoint 回收 WAL 时必须保留到这里。
        self._active_starts: dict[int, int] = {}
        self._current: Optional[Transaction] = None
        # 组提交：提交记录先入队，由一个 leader 统一刷盘；其余提交等待
        # durable_lsn 覆盖自己的目标 LSN 后再标记为 COMMITTED。
        self._commit_condition = threading.Condition(self._lock)
        self._commit_queue: list[tuple[Transaction, int]] = []
        self._commit_leader = False
        self._commit_error: Optional[BaseException] = None
        self._xid_ceiling = max(disk.meta.next_xid, 1)
        self._next_xid = self._xid_ceiling

    # -- xid 分配 -----------------------------------------------------------

    def _allocate_xid(self) -> int:
        with self._lock:
            if self._next_xid >= self._xid_ceiling:
                # 预留新块并把上界持久化。崩溃后从这里继续，绝不会回溯。
                self._xid_ceiling += XID_RESERVE_BLOCK
                self._disk.meta.next_xid = self._xid_ceiling
                self._disk._persist_meta()
                self._disk.sync()
            xid = self._next_xid
            self._next_xid += 1
            return xid

    # -- 生命周期 -----------------------------------------------------------

    def begin(self, read_only: bool = False) -> Transaction:
        """开启事务。

        ``read_only=True`` 给"入口就确定只读"的语句用（自动提交的
        SELECT / EXPLAIN / SHOW）：不写 BEGIN 记录，因此不抢 WAL 写锁、
        不在提交时触发 fsync。只读事务不产生任何页写，崩溃恢复不需要
        认识它；但它的快照起点仍登记进 ``_active_starts``，否则长读事务
        要读的旧版本页会被 checkpoint 提前回收。
        """
        with self._lock:
            xid = self._allocate_xid()
            active_at_start = frozenset(self._active)
            txn = Transaction(
                xid=xid,
                snapshot_xid=xid,
                active_at_start=active_at_start,
                read_only=read_only,
            )
            self._states[xid] = TxnState.ACTIVE
            self._active.add(xid)
            self._txns[xid] = txn
            if read_only:
                txn.first_lsn = self._wal.next_lsn
            else:
                txn.first_lsn = self._wal.begin(xid)
            txn.last_lsn = txn.first_lsn
            self._active_starts[xid] = txn.first_lsn
            self._current = txn
            return txn

    def commit(self, txn: Transaction) -> None:
        follower = False
        with self._lock:
            txn.require_active()
            if txn.read_only and not txn.wrote:
                # 只读事务没有要持久化的东西：不写 COMMIT 记录、不进组提交
                # 队列、不触发 fsync。这是"每次 SELECT 都 fsync 一次"的
                # 根因——组提交的 leader 必然调 wal.flush()。
                self._mark_committed(txn)
                return
            # [Review-Fix] 提交点复检写围栏：租约/任期可能在语句执行期间
            # 已经过期或被更高的任期取代。此处抛错 → COMMIT 记录不会被
            # 追加，调用方 abort，没有“已持久化的已提交事务”产生。
            if self._write_fence is not None and txn.wrote:
                self._write_fence()
            txn.last_lsn = self._wal.commit(txn.xid)
            txn.commit_record_written = True
            self._commit_queue.append((txn, txn.last_lsn))
            follower = self._commit_leader
            if not follower:
                self._commit_leader = True

        if follower:
            # 已有 leader 在刷盘；等它覆盖到自己的 LSN。
            with self._lock:
                while txn.state is TxnState.ACTIVE:
                    self._commit_condition.wait()
                    if self._commit_error is not None:
                        raise TransactionError(
                            f"事务 {txn.xid} 提交失败：WAL 刷盘错误"
                        ) from self._commit_error
        else:
            try:
                self._run_commit_group()
            except BaseException as exc:
                with self._lock:
                    self._commit_error = exc
                    self._commit_leader = False
                    self._commit_condition.notify_all()
                raise

        # 同步复制的屏障：本地已 fsync，但要等多数派追上这个 LSN 才对调用方
        # 报告成功。注意这里**不再持锁**——等水位可能要几十毫秒，持 TM 锁
        # 会把同批其它提交一起卡住。
        self._await_majority(txn)

    def set_commit_barrier(
        self, barrier: Optional[Callable[[int], None]]
    ) -> None:
        """替换提交屏障（None 表示关闭同步复制）。"""
        self._commit_barrier = barrier

    def _await_majority(self, txn: Transaction) -> None:
        barrier = self._commit_barrier
        if barrier is None or not txn.wrote:
            # 只读事务（自动提交的 SELECT 也算）没有需要复制的写入，
            # 不能让它们去等水位——否则每次查询都要卡到超时。
            return
        barrier(int(txn.last_lsn))

    def _run_commit_group(self) -> None:
        """leader 循环：把当前队列里的提交合并成一次 WAL 刷盘。

        新提交可能在前一次 ``flush`` 之后才追加，所以队列非空时继续循环；
        每个被覆盖的提交立即标记为 COMMITTED 并唤醒等待者。
        """
        while True:
            with self._lock:
                batch = self._commit_queue
                self._commit_queue = []
            durable = self._wal.flush()
            with self._lock:
                committed_any = False
                remaining: list[tuple[Transaction, int]] = []
                for pending, target in batch:
                    if pending.state is not TxnState.ACTIVE:
                        continue
                    if target <= durable:
                        self._mark_committed(pending)
                        committed_any = True
                    else:
                        remaining.append((pending, target))
                if remaining:
                    self._commit_queue.extend(remaining)
                if self._commit_queue:
                    if committed_any:
                        self._commit_condition.notify_all()
                    continue
                self._commit_leader = False
                self._commit_condition.notify_all()
                return

    def _mark_committed(self, txn: Transaction) -> None:
        txn.state = TxnState.COMMITTED
        self._states[txn.xid] = TxnState.COMMITTED
        self._active.discard(txn.xid)
        self._active_starts.pop(txn.xid, None)
        self._txns.pop(txn.xid, None)
        if self._bp is not None:
            self._bp.clear_undo(txn.xid)
        txn.undo_log.clear()
        txn.savepoints.clear()
        txn.touched_tables.clear()
        if self._current is txn:
            self._current = None
        self._release_locks(txn.xid)
        self._prune_states()

    def _release_locks(self, xid: int) -> None:
        """事务结束时释放它持有的全部锁（写者锁在这里被交出）。"""
        if self._locks is not None:
            self._locks.release_all(xid)

    def _prune_states(self) -> None:
        """回收事务结局表里的陈旧条目，避免长跑服务端无界增长。

        不变量：只有 ``xid < 最老活跃事务`` 的结局才可能被丢掉。原因有两条：
        1. 更新的事务的快照号更大，``_committed_before`` 对未知 xid 的结论
           是"已经结束且已提交"——对已提交事务正好正确；
        2. 已回滚事务的页写在 abort / 崩溃恢复时已经被物理撤销，不会留下
           带该 xmin 的可见记录，所以"当成已提交"也不会让回滚的数据复活。

        额外保留 ``STATE_RETAIN`` 条最近条目，为 ``state_of`` 的调用方
        （并发写检测、副本重放）留出回溯窗口。
        """
        if len(self._states) <= STATE_RETAIN * 2:
            return
        floor = self._next_xid
        if self._active:
            floor = min(self._active)
        floor = max(0, floor - STATE_RETAIN)
        for xid in [x for x in self._states if x < floor]:
            del self._states[xid]

    def abort(self, txn: Transaction) -> None:
        with self._lock:
            if txn.state is not TxnState.ACTIVE:
                return
            if txn.read_only and not txn.wrote:
                # 同理：没有页写要撤销，也不该往 WAL 追加 ABORT 再 flush。
                txn.state = TxnState.ABORTED
                self._states[txn.xid] = TxnState.ABORTED
                self._active.discard(txn.xid)
                self._active_starts.pop(txn.xid, None)
                self._txns.pop(txn.xid, None)
                if self._current is txn:
                    self._current = None
                self._release_locks(txn.xid)
                self._prune_states()
                return
            try:
                # 先撤销页级写入：它必须发生在 DDL 的 Catalog 对象 undo 之前，
                # 这样 CatalogStore 的 root 回滚看到的是旧根页号，不会误判。
                if self._bp is not None:
                    self._bp.undo_xid(txn.xid)
                # DDL 修改的是内存里的 Catalog 对象；WAL 只能恢复页，不能替
                # 调用方恢复已变异的 Python 对象，所以这里再逆序执行内存 undo。
                while txn.undo_log:
                    undo = txn.undo_log.pop()
                    try:
                        undo()
                    except Exception:  # pragma: no cover
                        # 吞掉但要留痕：静默失败会让目录停在半回滚状态，
                        # 事后只能靠恢复日志猜当时发生了什么。
                        _log.exception(
                            "事务 %s 的目录 undo 回调抛错，目录可能已偏离回滚前的状态",
                            txn.xid,
                        )
                txn.savepoints.clear()
                txn.touched_tables.clear()
                txn.last_lsn = self._wal.abort(txn.xid)
                self._wal.flush()
            except BaseException:
                # [Review-Fix] WAL 写失败（磁盘满 / IO 错误）时 abort 自己也会
                # 抛错。若就此退出，事务仍登记为 ACTIVE、写者锁仍被持有——
                # 单写者模型下这台库从此永久只能读（后续写事务一律 40001），
                # 比"页没撤销干净"的后果严重得多。所以无论如何都要收尾：
                # 状态落 ABORTED、簿记清干净、锁一定交回去。
                _log.exception(
                    "事务 %s 的 ABORT 记录写入失败，仍按已回滚收尾（页可能未完全撤销）",
                    txn.xid,
                )
                raise
            finally:
                txn.state = TxnState.ABORTED
                self._states[txn.xid] = TxnState.ABORTED
                self._active.discard(txn.xid)
                self._active_starts.pop(txn.xid, None)
                self._txns.pop(txn.xid, None)
                if self._current is txn:
                    self._current = None
                self._release_locks(txn.xid)
                self._prune_states()

    def abort_all(self) -> int:
        """回滚所有仍处于 ACTIVE 的事务；返回回滚数量。

        数据库关闭/降级时必须先把它们 abort：否则 pgwire 事务可以继续持有
        写者锁、未提交页和延迟约束状态，在 close 之后才 COMMIT。
        """
        with self._lock:
            pending = [
                txn
                for txn in self._txns.values()
                if txn.state is TxnState.ACTIVE
            ]
        for txn in pending:
            self.abort(txn)
        return len(pending)

    # -- SAVEPOINT ---------------------------------------------------------

    def savepoint(self, txn: Transaction, name: str) -> TxnSavepoint:
        """记录当前页 undo / 对象 undo 的深度。"""
        with self._lock:
            txn.require_active()
            sp = TxnSavepoint(
                name=name,
                page_depth=(
                    self._bp.undo_depth(txn.xid) if self._bp is not None else 0
                ),
                undo_depth=len(txn.undo_log),
                deferred_fk_rows=frozenset(txn.deferred_fk_rows),
                deferred_ref_checks=frozenset(txn.deferred_ref_checks),
                constraint_modes=dict(txn.constraint_modes),
            )
            txn.savepoints.append(sp)
            return sp

    def _find_savepoint(self, txn: Transaction, name: str) -> int:
        # 同名保存点可以重复建立；最近建立的那个先被找到。
        for i in range(len(txn.savepoints) - 1, -1, -1):
            if txn.savepoints[i].name == name:
                return i
        raise TransactionError(
            f"保存点 {name} 不存在",
            hint="先执行 SAVEPOINT",
        )

    def rollback_to(self, txn: Transaction, name: str) -> int:
        """撤销到保存点，但保留该保存点和更早的保存点。"""
        with self._lock:
            txn.require_active()
            idx = self._find_savepoint(txn, name)
            target = txn.savepoints[idx]
            pages = 0
            if self._bp is not None:
                pages = self._bp.undo_to(txn.xid, target.page_depth)
            while len(txn.undo_log) > target.undo_depth:
                undo = txn.undo_log.pop()
                try:
                    undo()
                except Exception:  # pragma: no cover
                    pass
            # 延迟约束队列和 SET CONSTRAINTS 覆盖也属于事务状态，必须回到
            # 保存点建立时的样子；仅回滚页/DDL 会让已撤销的检查或模式泄漏。
            txn.deferred_fk_rows = set(target.deferred_fk_rows)
            txn.deferred_ref_checks = set(target.deferred_ref_checks)
            txn.constraint_modes = dict(target.constraint_modes)
            # 保存点之后的保存点已经被这次回滚销毁。
            del txn.savepoints[idx + 1 :]
            return pages

    def release(self, txn: Transaction, name: str) -> None:
        """释放保存点；它和它之后建立的保存点一起失效。"""
        with self._lock:
            txn.require_active()
            idx = self._find_savepoint(txn, name)
            del txn.savepoints[idx:]

    # -- 查询状态 -----------------------------------------------------------

    @property
    def current(self) -> Optional[Transaction]:
        return self._current

    @property
    def active_xids(self) -> list[int]:
        with self._lock:
            return sorted(self._active)

    @property
    def active_first_lsn(self) -> int | None:
        """最老的活跃事务 BEGIN LSN；没有活跃事务时返回 None。"""
        with self._lock:
            if not self._active_starts:
                return None
            return min(self._active_starts.values())

    def state_of(self, xid: int) -> Optional[TxnState]:
        return self._states.get(xid)

    def note_replica_state(self, xid: int, state: TxnState) -> None:
        """副本重放 WAL 时更新事务可见性表，不参与本机提交。"""
        with self._lock:
            self._states[xid] = state
            if state is TxnState.ACTIVE:
                self._active.add(xid)
            else:
                self._active.discard(xid)
                self._active_starts.pop(xid, None)
            if xid + 1 > self._next_xid:
                self._next_xid = xid + 1

    def _committed_before(self, xid: int, txn: Transaction) -> bool:
        """该 xid 的修改在 txn 的快照里是否已经提交。"""
        if xid == 0:
            return True  # 未写入
        if xid == txn.xid:
            return True  # 自己插入的版本对自己可见
        if xid >= txn.snapshot_xid:
            return False  # 快照之后才开始的事务，一定不可见
        if xid in txn.active_at_start:
            return False  # 快照时还活着，当时未提交
        state = self._states.get(xid)
        if state is None:
            # 状态表里没有，说明它早于本次进程的所有事务，必然已结束。
            # 已回滚的事务的修改在恢复阶段被 undo 掉了，不会留在页里。
            return True
        return state is TxnState.COMMITTED

    def _committed_now(self, xid: int, txn: Transaction) -> bool:
        """[Review-Fix] 引用完整性用的可见性：只看"现在是否已提交"。

        普通快照读必须坚持 ``snapshot_xid`` 的可见性；但外键校验（PostgreSQL
        的 RI 触发器用的是 ``SnapshotDirty``）不能被快照冻住，否则：

        * 父行由并发事务提交后，本事务的快照看不见它 → 合法的子行插入被
          误判成 23503（实测复现）；
        * 删除父行时看不见并发事务刚提交的子行 → 提交后留下悬空引用。

        因此 RI 路径把"已提交"当作可见，未提交/已回滚仍不可见。
        """
        if xid == 0:
            return True
        if xid == txn.xid:
            return True
        state = self._states.get(xid)
        if state is None:
            # 早于本进程的事务：恢复阶段已把 loser 的修改 undo 掉，
            # 剩下的必然已提交。
            return xid < txn.snapshot_xid
        return state is TxnState.COMMITTED

    def is_visible(
        self,
        txn: Transaction,
        xmin: int,
        xmax: int,
        flags: int = 0,
        *,
        ri: bool = False,
    ) -> bool:
        from .record import FLAG_ABORTED

        if flags & FLAG_ABORTED:
            return False
        check = self._committed_now if ri else self._committed_before
        if not check(xmin, txn):
            return False
        if xmax == 0:
            return True
        if xmax == txn.xid:
            return False  # 自己删除的，自己看不见
        return not check(xmax, txn)

    # -- 恢复支持 -----------------------------------------------------------

    def register_recovered(
        self, committed: set[int], aborted: set[int], max_xid: int
    ) -> None:
        """恢复阶段结束后，把日志里的事务结局登记回内存状态表。"""
        with self._lock:
            for xid in committed:
                self._states[xid] = TxnState.COMMITTED
            for xid in aborted:
                self._states[xid] = TxnState.ABORTED
            self._active_starts.clear()
            if max_xid + 1 > self._next_xid:
                self._next_xid = max_xid + 1
            if self._next_xid >= self._xid_ceiling:
                self._xid_ceiling = self._next_xid + XID_RESERVE_BLOCK
                self._disk.meta.next_xid = self._xid_ceiling
                self._disk._persist_meta()


__all__ = ["Transaction", "TransactionManager", "TxnState", "XID_RESERVE_BLOCK"]
