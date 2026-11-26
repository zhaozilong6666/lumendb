"""历史读窗口的水位。

`AS OF SYSTEM TIME LSN` 只能读到"仍然保证完整"的区间。这个对象把三条
会破坏历史的操作抬成同一个下界：

* checkpoint 会裁剪更早的 COMMIT 账本；目标点落在 checkpoint 之前时，
  已存在行的 xmin 可能不在账本里，不能再精确判断提交顺序。
* VACUUM 会物理回收旧版本；目标点落在 VACUUM 开始之前时，被回收的
  版本已经不可能再读回来。
* DDL 会改变列布局；目标点落在 DDL 之前时，当前 schema 不能安全解码
  旧页。

因此窗口是 ``[floor_lsn, latest_commit_lsn]``，而不是整个 WAL 历史。
`assume_committed_before` 允许把 xmin 早于水位、但仍在堆里的版本当成
"水位前已提交"：这些行既然在水位时刻仍然存在，只要 xmax 尚未在水位后
提交，它们在目标点就是可见的。这个假设只在目标点不低于水位时成立。
"""

from __future__ import annotations

import threading


class HistoryWindow:
    """进程内历史读下界；checkpoint / VACUUM / DDL 都会推进它。"""

    def __init__(self, checkpoint_lsn: int = 0) -> None:
        self._lock = threading.Lock()
        self._checkpoint_lsn = max(0, int(checkpoint_lsn))
        self._floor_lsn = self._checkpoint_lsn

    @property
    def checkpoint_lsn(self) -> int:
        with self._lock:
            return self._checkpoint_lsn

    @property
    def floor_lsn(self) -> int:
        with self._lock:
            return self._floor_lsn

    def advance(self, lsn: int) -> int:
        """把历史下界抬到至少 ``lsn``；返回推进后的下界。"""
        value = max(0, int(lsn))
        with self._lock:
            if value > self._floor_lsn:
                self._floor_lsn = value
            return self._floor_lsn

    def observe_checkpoint(self, lsn: int) -> int:
        """checkpoint 完成后登记新的 checkpoint LSN。

        checkpoint 的截断发生在 `lsn` 之前，因此 `lsn` 之前的历史不再可
        精确判断；下界至少抬到 `lsn`。
        """
        value = max(0, int(lsn))
        with self._lock:
            self._checkpoint_lsn = value
            if value > self._floor_lsn:
                self._floor_lsn = value
            return self._floor_lsn
