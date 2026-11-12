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
                return True
            return False

    def release(self, xid: int, resource: Hashable) -> None:
        with self._cond:
            holders = self._granted.get(resource)
            if holders and xid in holders:
                del holders[xid]
                if not holders:
                    self._granted.pop(resource, None)
            self._clear_waits(xid)
            self._cond.notify_all()

    def release_all(self, xid: int) -> None:
        with self._cond:
            for resource in list(self._granted.keys()):
                holders = self._granted[resource]
                if xid in holders:
                    del holders[xid]
                    if not holders:
                        del self._granted[resource]
            self._clear_waits(xid)
            self._cond.notify_all()

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


__all__ = ["LockManager", "LockMode", "LockTimeoutError", "row_resource"]
