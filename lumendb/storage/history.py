"""历史读窗口的水位。

``AS OF SYSTEM TIME LSN`` 只能读到“仍然保证完整”的区间。这个对象维护两条
独立的下界：

* **结构下界**：VACUUM 会物理回收旧版本，DDL 会改变列布局。目标点落在这些
  操作开始之前时，旧页已经不能安全读回。结构下界会持久化到元数据页，
  崩溃重启后不会因为内存丢失而错误放宽。
* **账本下界**：持久提交账本只保留最近若干条提交。目标点早于账本最老条目
  时，无法判断非零 ``xmax`` 是已提交删除还是已回滚删除，必须拒绝。

checkpoint 本身不再抬高历史下界：它只把脏页落盘、裁剪 WAL；页上的旧版本
没有被删除，提交账本也已经跨 checkpoint 持久化。因此可读窗口是
``[max(结构下界, 账本最老 LSN), latest_commit_lsn]``。

`assume_committed_before` 仍然保留：账本最老 LSN 之前的 ``xmin`` 可以按“水位
前已提交”处理；但未知 ``xmax`` 绝不猜，宁可报错也不复活已删除版本。
"""

from __future__ import annotations

import threading
from typing import Callable, Optional


class HistoryWindow:
    """进程内历史读下界；VACUUM / DDL 推进结构下界并持久化。"""

    def __init__(
        self,
        checkpoint_lsn: int = 0,
        *,
        structural_floor_lsn: int = 0,
        ledger_floor_lsn: int = 0,
        persist_structural: Optional[Callable[[int], None]] = None,
    ) -> None:
        self._lock = threading.Lock()
        self._checkpoint_lsn = max(0, int(checkpoint_lsn))
        self._structural_floor_lsn = max(0, int(structural_floor_lsn))
        #: 账本为空时退回 checkpoint：没有提交账本就只能从最近一次固化点
        #: 开始承诺历史读。
        ledger = int(ledger_floor_lsn)
        self._ledger_floor_lsn = ledger if ledger > 0 else self._checkpoint_lsn
        self._persist_structural = persist_structural

    @property
    def checkpoint_lsn(self) -> int:
        with self._lock:
            return self._checkpoint_lsn

    @property
    def structural_floor_lsn(self) -> int:
        with self._lock:
            return self._structural_floor_lsn

    @property
    def ledger_floor_lsn(self) -> int:
        with self._lock:
            return self._ledger_floor_lsn

    @property
    def floor_lsn(self) -> int:
        with self._lock:
            return max(self._structural_floor_lsn, self._ledger_floor_lsn)

    def advance(self, lsn: int) -> int:
        """把结构下界抬到至少 ``lsn``；返回推进后的可读下界。

        VACUUM / DDL 的调用方必须保证：推进下界发生在任何页回收或 schema
        变更之前。这样即使进程在操作中途崩溃，重启后的下界也已经覆盖到
        操作开始点，不会把半回收的旧版本当成可读历史。
        """
        value = max(0, int(lsn))
        with self._lock:
            advanced = value > self._structural_floor_lsn
            if advanced:
                self._structural_floor_lsn = value
            floor = max(self._structural_floor_lsn, self._ledger_floor_lsn)
            callback = self._persist_structural if advanced else None
        if callback is not None:
            callback(value)
        return floor

    def observe_checkpoint(self, lsn: int) -> int:
        """登记新的 checkpoint LSN；checkpoint 本身不限制历史读。"""
        value = max(0, int(lsn))
        with self._lock:
            self._checkpoint_lsn = value
            return max(self._structural_floor_lsn, self._ledger_floor_lsn)

    def observe_ledger(self, oldest_lsn: int) -> int:
        """把账本下界更新为当前保留的最老提交 LSN。

        账本保留窗口可能因裁剪而**抬高**（更老的提交被丢掉），也可能在
        第一次 checkpoint 后**降低**（WAL 里的旧提交被写进持久账本）。这里
        直接替换账本下界，最终可读下界仍由 ``max(结构, 账本)`` 决定。
        """
        value = max(0, int(oldest_lsn))
        with self._lock:
            self._ledger_floor_lsn = value
            return max(self._structural_floor_lsn, self._ledger_floor_lsn)
