"""手动提升的 fencing 文件。

不做自动故障切换，只提供一个可被主库和副本共同看到的原子 marker：

- 副本提升时写入 ``{"promoted_node": ..., "lsn": ...}``；
- 主库每次写语句前检查 marker，发现提升节点不是自己就拒绝写入。

这是“防双写”的最小协议。它依赖两个节点能访问同一个文件（共享盘、挂载
目录或测试里的临时目录）；没有网络租约、没有超时仲裁，因此不是完整的
自动 failover。
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..errors import NotSupportedError


class PromotionFence:
    def __init__(self, path: str | Path, node_id: str = "primary") -> None:
        self.path = Path(path)
        self.node_id = node_id
        self._cache_key: tuple[int, int] | None = None
        self._cache_data: dict | None = None

    def read(self) -> dict | None:
        try:
            stat = self.path.stat()
        except OSError:
            self._cache_key = None
            self._cache_data = None
            return None
        key = (stat.st_mtime_ns, stat.st_size)
        if key == self._cache_key:
            return self._cache_data
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self._cache_key = None
            self._cache_data = None
            return None
        self._cache_key = key
        self._cache_data = data if isinstance(data, dict) else None
        return self._cache_data

    def assert_writable(self) -> None:
        marker = self.read()
        if marker is None:
            return
        promoted = marker.get("promoted_node")
        if promoted and promoted != self.node_id:
            raise NotSupportedError(
                f"节点已被提升副本 fence（promoted_node={promoted}）",
                hint="旧主库不能继续写入；需要人工确认拓扑后再恢复",
            )

    def promote(self, lsn: int = 0) -> None:
        existing = self.read()
        if existing is not None and int(existing.get("lsn", 0)) > int(lsn):
            raise NotSupportedError(
                "fence 中已有更高的提升 LSN，拒绝用旧副本覆盖",
                detail=f"fence_lsn={existing.get('lsn')} requested={lsn}",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "promoted_node": self.node_id,
            "lsn": int(lsn),
        }
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            encoding="utf-8",
        )
        os.replace(tmp, self.path)

    def clear(self) -> None:
        self._cache_key = None
        self._cache_data = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


__all__ = ["PromotionFence"]
