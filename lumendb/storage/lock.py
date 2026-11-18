"""行级锁与死锁检测。

MVCC 解决了"读不阻塞写"，但解决不了"两个事务同时改同一行"。那类冲突必须
有人退让，否则后写者会覆盖先写者的修改（丢失更新）。

LumenDB 用**行级二模式锁**：

* ``S``（共享）：可以多个事务同时持有；
* ``X``（排他）：只能一个事务持有，且与任何其他锁互斥；
* 允许 S→X 升级，但只有当自己是没有其他持有者时才成功。

死锁用**等待图**检测：事务等待时把"我在等谁"记进图里，插入一条边后立即
做一次 DFS。发现环就中止发起等待的那个事务——不是因为"它错了"，而是因为
它是一个确定的、可复现的选择点。生产系统通常挑"代价最小"的牺牲者，
这里选择最简单的规则并把原因写进错误信息。
"""

from __future__ import annotations

import threading
import time
from enum import IntEnum
from typing import Hashable, Optional

from ..errors import DeadlockError, TransactionError


class LockMode(IntEnum):
    SHARED = 1
    EXCLUSIVE = 2


class LockTimeoutError(TransactionError):
    sqlstate = "55P03"


class LockManager:
    def __init__(self, default_timeout: float = 10.0) -> None:
        self._cond = threading.Condition()
        self._granted: dict[Hashable, dict[int, LockMode]] = {}
        #: 每个事务持有的资源集合。`release_all` 必须只看自己持有的，
        #: 否则每次提交都要扫全系统的锁表——批量插入会持有几千个键锁，
        #: 多会话下这一步会变成 O(总锁数)。
        self._held: dict[int, set[Hashable]] = {}
        self._waits_for: dict[int, set[int]] = {}
        self._timeout = default_timeout

    # ------------------------------------------------------------------
    # 兼容性
    # ------------------------------------------------------------------

    def _conflicting_holders(
        self, resource: Hashable, xid: int, mode: LockMode
    ) -> set[int]:
        holders = self._granted.get(resource, {})
        out: set[int] = set()
        for hx, hmode in holders.items():
            if hx == xid:
                continue
            if mode is LockMode.EXCLUSIVE or hmode is LockMode.EXCLUSIVE:
                out.add(hx)
        return out

    def _can_grant(self, resource: Hashable, xid: int, mode: LockMode) -> bool:
        holders = self._granted.setdefault(resource, {})
        if xid in holders:
            current = holders[xid]
            if current is LockMode.EXCLUSIVE:
                return True
            if mode is LockMode.SHARED:
                return True
            # 升级 S → X：不能有其他持有者
            return not any(h != xid for h in holders)
        return not self._conflicting_holders(resource, xid, mode)

    # ------------------------------------------------------------------
    # 等待图
    # ------------------------------------------------------------------

    def _find_cycle(self, start: int) -> Optional[list[int]]:
        def dfs(node: int, path: list[int]) -> Optional[list[int]]:
            for nxt in self._waits_for.get(node, ()):
                if nxt in path:
                    return path[path.index(nxt) :] + [nxt]
                found = dfs(nxt, path + [nxt])
                if found:
                    return found
            return None

        return dfs(start, [start])

    def _clear_waits(self, xid: int) -> None:
        self._waits_for.pop(xid, None)
        for waiters in self._waits_for.values():
            waiters.discard(xid)

    # ------------------------------------------------------------------
    # 获取 / 释放
    # ------------------------------------------------------------------

    def acquire(
        self,
        xid: int,
        resource: Hashable,
        mode: LockMode,
        timeout: Optional[float] = None,
    ) -> None:
        timeout = self._timeout if timeout is None else timeout
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._can_grant(resource, xid, mode):
                    self._granted.setdefault(resource, {})[xid] = max(
                        self._granted[resource].get(xid, 0), mode
                    )
                    self._held.setdefault(xid, set()).add(resource)
                    self._clear_waits(xid)
                    return

                blockers = self._conflicting_holders(resource, xid, mode)
                self._waits_for[xid] = blockers
                cycle = self._find_cycle(xid)
                if cycle is not None:
                    self._clear_waits(xid)
                    raise DeadlockError(
                        "检测到死锁，当前事务被中止",
                        detail="等待环：" + " -> ".join(str(x) for x in cycle),
                        hint="重试事务，或调整加锁顺序",
                    )

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._clear_waits(xid)
                    raise LockTimeoutError(
                        f"等待资源 {resource} 超时（{timeout}s）",
                        hint="检查是否有长事务一直持有该锁",
                    )
                self._cond.wait(min(remaining, 0.5))

    def try_acquire(self, xid: int, resource: Hashable, mode: LockMode) -> bool:
        with self._cond:
            if self._can_grant(resource, xid, mode):
                self._granted.setdefault(resource, {})[xid] = mode
                self._held.setdefault(xid, set()).add(resource)
                return True
            return False

    def release(self, xid: int, resource: Hashable) -> None:
        with self._cond:
            holders = self._granted.get(resource)
            if holders and xid in holders:
                del holders[xid]
                if not holders:
                    self._granted.pop(resource, None)
            owned = self._held.get(xid)
            if owned is not None:
                owned.discard(resource)
                if not owned:
                    self._held.pop(xid, None)
            self._clear_waits(xid)
            self._cond.notify_all()

    def release_all(self, xid: int) -> None:
        """释放该事务持有的全部锁。只看自己持有的集合，不扫全表。"""
        with self._cond:
            for resource in self._held.pop(xid, set()):
                holders = self._granted.get(resource)
                if not holders:
                    continue
                holders.pop(xid, None)
                if not holders:
                    self._granted.pop(resource, None)
            self._clear_waits(xid)
            self._cond.notify_all()

    def held_count(self, xid: Optional[int] = None) -> int:
        """当前持有的锁数量（观测用：批量写的锁表规模）。"""
        with self._cond:
            if xid is None:
                return sum(len(v) for v in self._granted.values())
            return len(self._held.get(xid, ()))

    def holders(self, resource: Hashable) -> dict[int, LockMode]:
        with self._cond:
            return dict(self._granted.get(resource, {}))

    def reset(self) -> None:
        with self._cond:
            self._granted.clear()
            self._waits_for.clear()
            self._cond.notify_all()


def row_resource(table: str, root_rid) -> tuple:
    """行锁的资源标识。用根 RID 而不是物理 RID：同一逻辑行的多个版本共用一把锁。"""
    return ("row", table, root_rid[0], root_rid[1])


#: 事务级写者锁的资源标识。
#:
#: 为什么需要它（而不是只用行锁）：
#: 1. 目录（`catalog/*`）是"单根指针 + 页链"，任何写语句提交前都会 persist
#:    一次，两条并发写事务必然命中同一批目录字节；行锁挡不住这种冲突，
#:    而物理 undo 又无法按事务区分字节来源。
#: 2. 唯一索引的项会在 DELETE 时被**物理删除**，并发的 INSERT 因此看不到
#:    未提交的键；如果删除方回滚，索引项会被恢复，主键就重复了。
#: 3. 索引还缺 xmin/xmax，多个写者之间的可见性无法按版本区分。
#:
#: 结论：这一版的并发模型是"读并发、写单写者"——写事务之间用 40001
#: 快速失败，而不是排队等待。失败而不是等待的另一个原因是线协议在单条
#: 语句外还持有一把 asyncio 锁：在这里阻塞会把别的连接的提交也一起卡死。
WRITER_RESOURCE = ("writer", "*")


def writer_resource() -> tuple:
    return WRITER_RESOURCE


def key_resource(table: str, index: str, key: tuple) -> tuple:
    """唯一索引的**键级**锁。

    为什么需要它：唯一性判断读的是"物理索引项"，而 `DELETE` 会立刻把索引项
    摘掉。并发 INSERT 同一键时看不到未提交的删除，等删除方回滚、索引项被恢复，
    主键就重复了。键锁让"改这个键"的事务互斥，回滚/提交之后再判断唯一性。
    """
    return ("ukey", table, index, tuple(key))


def catalog_resource() -> tuple:
    """目录保存的互斥锁：目录是"单根指针 + 页链"，整份保存必须独占。"""
    return ("catalog", "*")


def ddl_resource() -> tuple:
    """DDL 的事务级互斥：schema 形状改变不能与另一个 DDL 并发。"""
    return ("ddl", "*")


__all__ = [
    "LockManager",
    "LockMode",
    "LockTimeoutError",
    "row_resource",
    "WRITER_RESOURCE",
    "catalog_resource",
    "ddl_resource",
    "key_resource",
    "writer_resource",
]
