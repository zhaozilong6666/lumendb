"""Windows 下的原子替换重试。

`os.replace(tmp, target)` 在 Windows 上可能被杀毒软件、搜索索引或备份
进程短暂锁住目标文件，抛出瞬时性 `PermissionError [WinError 5]`（源与
目标都未改变，重试是安全的）。这里用有限次、线性退避的重试把这种
瞬时锁吸收掉；其他异常原样抛出，不吞错。
"""

from __future__ import annotations

import os
import time
from pathlib import Path

__all__ = ["atomic_replace"]


def atomic_replace(
    src: Path | str,
    dst: Path | str,
    *,
    tries: int = 3,
    delay: float = 0.01,
) -> None:
    """``os.replace`` 的瞬时锁重试版本；失败语义与原调用完全一致。"""
    last: PermissionError | None = None
    for attempt in range(max(1, int(tries))):
        try:
            os.replace(src, dst)
            return
        except PermissionError as exc:
            last = exc
            if attempt < max(1, int(tries)) - 1:
                time.sleep(max(0.0, float(delay)) * (attempt + 1))
    assert last is not None  # tries >= 1 保证至少进入一次循环
    raise last
