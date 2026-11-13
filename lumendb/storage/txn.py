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
from typing import TYPE_CHECKING, Optional

from ..errors import TransactionAbortedError, TransactionError
from .pager import DiskManager
from .wal import WAL

if TYPE_CHECKING:  # pragma: no cover
    from .bufferpool import BufferPoolManager

#: 事务号预留块大小。崩溃后重启时，下一个 xid 从这个块的边界继续，
#: 保证不会复用已经写进记录头的事务号。
XID_RESERVE_BLOCK = 1024


class TxnState(IntEnum):
    ACTIVE = 1
    COMMITTED = 2
    ABORTED = 3


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
        self._current: Optional[Transaction] = None
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
            self._current = txn
            return txn

    def commit(self, txn: Transaction) -> None:
        with self._lock:
            txn.require_active()
            txn.last_lsn = self._wal.commit(txn.xid)
            # 提交必须落盘：否则"已提交"的承诺会在崩溃时失效
            self._wal.flush()
            txn.state = TxnState.COMMITTED
            self._states[txn.xid] = TxnState.COMMITTED
            self._active.discard(txn.xid)
            if self._bp is not None:
                self._bp.clear_undo(txn.xid)
            # DDL 的内存撤销信息在提交后不再需要。
            txn.undo_log.clear()
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
            txn.last_lsn = self._wal.abort(txn.xid)
            self._wal.flush()
            txn.state = TxnState.ABORTED
            self._states[txn.xid] = TxnState.ABORTED
            self._active.discard(txn.xid)
            if self._current is txn:
                self._current = None

    # -- 查询状态 -----------------------------------------------------------

    @property
    def current(self) -> Optional[Transaction]:
        return self._current

    @property
    def active_xids(self) -> list[int]:
        with self._lock:
            return sorted(self._active)

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
            if max_xid + 1 > self._next_xid:
                self._next_xid = max_xid + 1
            if self._next_xid >= self._xid_ceiling:
                self._xid_ceiling = self._next_xid + XID_RESERVE_BLOCK
                self._disk.meta.next_xid = self._xid_ceiling
                self._disk._persist_meta()


__all__ = ["Transaction", "TransactionManager", "TxnState", "XID_RESERVE_BLOCK"]
