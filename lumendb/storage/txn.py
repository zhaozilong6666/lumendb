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

import threading
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Optional

from ..errors import TransactionAbortedError, TransactionError
from .pager import DiskManager
from .wal import WAL

if TYPE_CHECKING:  # pragma: no cover
    from .bufferpool import BufferPoolManager
    from .record import RID

#: 事务号预留块大小。崩溃后重启时，下一个 xid 从这个块的边界继续，
#: 保证不会复用已经写进记录头的事务号。
XID_RESERVE_BLOCK = 1024


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
    ) -> None:
        self._disk = disk
        self._wal = wal
        self._bp = buffer_pool
        self._lock = threading.RLock()
        self._states: dict[int, TxnState] = {}
        self._active: set[int] = set()
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

    def begin(self) -> Transaction:
        with self._lock:
            xid = self._allocate_xid()
            active_at_start = frozenset(self._active)
            txn = Transaction(xid=xid, snapshot_xid=xid, active_at_start=active_at_start)
            self._states[xid] = TxnState.ACTIVE
            self._active.add(xid)
            txn.first_lsn = self._wal.begin(xid)
            txn.last_lsn = txn.first_lsn
            self._active_starts[xid] = txn.first_lsn
            self._current = txn
            return txn

    def commit(self, txn: Transaction) -> None:
        with self._lock:
            txn.require_active()
            txn.last_lsn = self._wal.commit(txn.xid)
            self._commit_queue.append((txn, txn.last_lsn))
            if self._commit_leader:
                # 已有 leader 在刷盘；等它覆盖到自己的 LSN。
                while txn.state is TxnState.ACTIVE:
                    self._commit_condition.wait()
                    if self._commit_error is not None:
                        raise TransactionError(
                            f"事务 {txn.xid} 提交失败：WAL 刷盘错误"
                        ) from self._commit_error
                return
            self._commit_leader = True

        try:
            self._run_commit_group()
        except BaseException as exc:
            with self._lock:
                self._commit_error = exc
                self._commit_leader = False
                self._commit_condition.notify_all()
            raise

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
        if self._bp is not None:
            self._bp.clear_undo(txn.xid)
        txn.undo_log.clear()
        txn.savepoints.clear()
        if self._current is txn:
            self._current = None

    def abort(self, txn: Transaction) -> None:
        with self._lock:
            if txn.state is not TxnState.ACTIVE:
                return
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
                except Exception:  # pragma: no cover - 撤销本身不应改变事务结局
                    pass
            txn.savepoints.clear()
            txn.last_lsn = self._wal.abort(txn.xid)
            self._wal.flush()
            txn.state = TxnState.ABORTED
            self._states[txn.xid] = TxnState.ABORTED
            self._active.discard(txn.xid)
            self._active_starts.pop(txn.xid, None)
            if self._current is txn:
                self._current = None

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

    def is_visible(
        self,
        txn: Transaction,
        xmin: int,
        xmax: int,
        flags: int = 0,
    ) -> bool:
        from .record import FLAG_ABORTED

        if flags & FLAG_ABORTED:
            return False
        if not self._committed_before(xmin, txn):
            return False
        if xmax == 0:
            return True
        if xmax == txn.xid:
            return False  # 自己删除的，自己看不见
        return not self._committed_before(xmax, txn)

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
