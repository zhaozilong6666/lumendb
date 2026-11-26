"""故障注入点（fault points）。

## 为什么需要它

现有崩溃测试只覆盖"**拔电**"这一种坏法：绕过 buffer pool 直接写页、丢掉未落盘的
dirty page。但下面这些一条都没测过——而它们在生产里比拔电更常见：

* `fsync` 返回 `ENOSPC`/`EIO`（磁盘满、坏扇区）；
* 页写到一半（torn write）；
* WAL 追加失败（日志盘满）；
* 锁等待超时。

这些路径一旦"静默成功"（把没落盘的写当成已提交），就是数据丢失。
这套注入点是给它们补测试用的**结构**，不是给生产用的开关。

## 用法

默认全关，零开销（`_armed()` 只是查一个空集合）。

```python
from lumendb import faults

faults.arm("wal.fsync@2")          # 第 2 次 fsync 抛一次 OSError
faults.arm("disk.write_page@0.5")  # 每次写页有 50% 概率抛错
faults.arm("wal.append@1:io")      # 第 1 次追加抛 OSError（默认也是 io）

with faults.disabled():            # 测试收尾时确保干净
    ...
```

规格字符串：``<点>[@<第 n 次> | @<概率>][:<error>]``

* ``wal.fsync@3``：该点第 3 次被触发时抛错（只抛一次，之后恢复正常）；
* ``wal.fsync@0.3``：每次触发有 30% 概率抛错；
* ``:`` 后可写 ``io``（`OSError`，默认）、``diskfull``（`OSError` 带 ENOSPC）、
  ``torn``（只写一半，用于页写）。
"""

from __future__ import annotations

import os
import random
import threading
from dataclasses import dataclass, field
from typing import Optional

#: 已知的注入点（也是文档）：新增调用点时请同时加到这里和 tests/unit/test_faults.py
POINTS = frozenset(
    {
        "wal.append",
        "wal.fsync",
        "wal_segment.append",
        "wal_segment.fsync",
        "disk.write_page",
        "page.fetch",
        "disk.sync",
        "disk.allocate_page",
    }
)

_lock = threading.Lock()
_hits: dict[str, int] = {}
_specs: dict[str, "_Spec"] = {}
#: 嵌套 ``disabled()`` 的深度；只有最外层退出才重新放行。
_disable_depth = 0


@dataclass
class _Spec:
    every: Optional[int] = None  # 第 n 次触发
    probability: Optional[float] = None  # 概率触发
    kind: str = "io"  # io / diskfull / torn
    fired: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


def arm(spec: str) -> None:
    """挂上一个注入点。``arm("wal.fsync@2")``。"""
    point, sep, rest = spec.partition("@")
    point = point.strip()
    if point not in POINTS:
        raise ValueError(f"未知故障点 {point!r}，可用：{sorted(POINTS)}")
    if not sep:
        raise ValueError(f"规格缺少 @：{spec!r}（例如 wal.fsync@2）")
    target, _, kind = rest.partition(":")
    target = target.strip()
    kind = (kind or "io").strip()
    if kind not in ("io", "diskfull", "torn"):
        raise ValueError(f"未知错误类型 {kind!r}")
    spec_obj = _Spec(kind=kind)
    if "." in target:
        probability = float(target)
        if not 0.0 <= probability <= 1.0:
            raise ValueError("概率必须在 [0, 1]")
        spec_obj.probability = probability
    else:
        spec_obj.every = max(1, int(target))
    with _lock:
        _specs[point] = spec_obj
        _hits[point] = 0


def disarm(point: Optional[str] = None) -> None:
    with _lock:
        if point is None:
            _specs.clear()
            _hits.clear()
        else:
            _specs.pop(point, None)
            _hits.pop(point, None)


def _armed() -> bool:
    return _disable_depth == 0 and bool(_specs)


class disabled:
    """上下文管理器：临时关掉所有注入点（测试收尾用）。

    支持嵌套：内层退出不能把外层的关闭状态提前恢复。
    """

    def __enter__(self) -> None:
        global _disable_depth
        _disable_depth += 1

    def __exit__(self, *exc) -> None:
        global _disable_depth
        _disable_depth = max(0, _disable_depth - 1)


def _error(kind: str) -> BaseException:
    if kind == "diskfull":
        return OSError(getattr(os, "ENOSPC", 28), "No space left on device")
    return OSError(5, "injected I/O error")


def _fire(point: str, kinds: frozenset[str] | None = None) -> Optional[str]:
    """命中时返回错误种类；未命中/点未挂/类型不匹配返回 None。

    ``kinds`` 用于区分“会完整写失败”的 io/diskfull 与“只写半页”的 torn：
    同一注入点被不同语义的调用方轮询时，不能重复计数，也不能让 torn
    在普通 ``check()`` 里提前抛成完整失败。
    """
    if not _armed():
        return None
    with _lock:
        spec = _specs.get(point)
        if spec is None:
            return None
        if kinds is not None and spec.kind not in kinds:
            return None
        _hits[point] = _hits.get(point, 0) + 1
        count = _hits[point]
        with spec.lock:
            if spec.every is not None:
                if count != spec.every:
                    return None
            elif spec.probability is not None:
                if random.random() >= spec.probability:
                    return None
            spec.fired += 1
            return spec.kind


def triggered(point: str) -> bool:
    """该点本次是否应该报完整 I/O 失败（不抛异常，供调用方决定怎么报错）。"""
    return _fire(point, frozenset({"io", "diskfull"})) is not None


def check(point: str) -> None:
    """在注入点调用：命中就抛 ``OSError``。

    调用点必须把这里的异常当成**普通 I/O 失败**处理——也就是说，任何"已提交"
    的判定必须发生在 fsync 成功之后。注入测试就是在验证这一点。
    """
    kind = _fire(point, frozenset({"io", "diskfull"}))
    if kind is not None:
        raise _error(kind)


def torn_after(point: str, size: int) -> Optional[int]:
    """页写注入用：返回"只写前多少个字节"，None 表示完整写。

    只有 ``kind == torn`` 时生效；普通 io/diskfull 不会在这里计数，交给
    ``check()``。调用方据此截断写入长度并报错。
    """
    if _fire(point, frozenset({"torn"})) is None:
        return None
    return max(0, size // 2)


def hits(point: Optional[str] = None) -> int:
    with _lock:
        if point is None:
            return sum(_hits.values())
        return _hits.get(point, 0)


def armed_points() -> dict[str, str]:
    """当前挂着的注入点（观测/断言用）。"""
    with _lock:
        return {
            point: (
                f"n={spec.every}" if spec.every is not None
                else f"p={spec.probability}"
            )
            + f":{spec.kind}"
            for point, spec in _specs.items()
        }


__all__ = [
    "POINTS",
    "arm",
    "armed_points",
    "check",
    "disabled",
    "disarm",
    "hits",
    "torn_after",
    "triggered",
]
