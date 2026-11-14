"""复制跟随与自动提升的最小控制器。

它只做三件事：

1. 周期性从 ``WALServer`` 拉一批 WAL 并 apply；
2. 连续网络失败达到阈值后，调用 ``Replica.promote``；
3. 如果给了 ``fence_path``，提升前先写共享 marker，旧主库通过
   ``PromotionFence.assert_writable`` 拒绝后续写入。

没有心跳租约、没有多数派仲裁、没有客户端重定向。它的定位是“单副本、
共享 fence 文件”的自动提升 MVP；网络分区时如果两个节点看不到同一个
fence 文件，仍可能双写，部署时必须明确这一点。
"""

from __future__ import annotations

from pathlib import Path

from ..errors import StorageError
from .replica import Replica
from .wal_stream import WALClient


class ReplicationMonitor:
    def __init__(
        self,
        replica: Replica,
        host: str,
        port: int,
        *,
        fence_path: str | Path | None = None,
        node_id: str = "replica",
        max_failures: int = 3,
        timeout: float = 5.0,
        max_records: int = 1000,
    ) -> None:
        self.replica = replica
        self.host = host
        self.port = port
        self.fence_path = Path(fence_path) if fence_path is not None else None
        self.node_id = node_id
        self.max_failures = max(1, int(max_failures))
        self.timeout = timeout
        self.max_records = max_records
        self.failures = 0
        self.promoted = False
        self.last_error: str | None = None

    def poll(self) -> str:
        """执行一轮跟随；返回 ``healthy`` / ``degraded`` / ``promoted`` / ``needs_attention``。"""
        if self.promoted:
            return "promoted"
        try:
            WALClient(self.host, self.port, timeout=self.timeout).status()
            self.replica.replicate_from_server(
                self.host,
                self.port,
                max_records=self.max_records,
                timeout=self.timeout,
            )
            self.failures = 0
            self.last_error = None
            return "healthy"
        except StorageError as exc:
            # 槽位已被回收等逻辑错误：继续重试没有意义，也不应该自动提升。
            self.last_error = str(exc)
            return "needs_attention"
        except OSError as exc:
            self.failures += 1
            self.last_error = str(exc)
            if self.failures >= self.max_failures:
                self._promote()
                return "promoted"
            return "degraded"

    def _promote(self) -> None:
        self.replica.promote(
            fence_path=self.fence_path,
            node_id=self.node_id,
        )
        self.promoted = True


__all__ = ["ReplicationMonitor"]
