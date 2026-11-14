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

import ssl
import threading
import time
from pathlib import Path

from ..errors import RedirectError, StorageError
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
        token: str | None = None,
        compress: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
        auto_rebase: bool = True,
        poll_interval: float = 1.0,
    ) -> None:
        self.replica = replica
        self.host = host
        self.port = port
        self.token = token
        self.compress = compress
        self.ssl_context = ssl_context
        self.server_hostname = server_hostname
        self.replica_id = replica_id
        self.auto_rebase = auto_rebase
        self.poll_interval = max(0.05, float(poll_interval))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.fence_path = Path(fence_path) if fence_path is not None else None
        self.node_id = node_id
        self.max_failures = max(1, int(max_failures))
        self.timeout = timeout
        self.max_records = max_records
        self.failures = 0
        self.promoted = False
        self.last_error: str | None = None
        self.redirects = 0

    def poll(self) -> str:
        """执行一轮跟随；返回 ``healthy`` / ``degraded`` / ``promoted`` / ``needs_attention``。"""
        if self.promoted:
            return "promoted"
        try:
            WALClient(
                self.host,
                self.port,
                timeout=self.timeout,
                token=self.token,
                compress=self.compress,
                ssl_context=self.ssl_context,
                server_hostname=self.server_hostname,
                replica_id=self.replica_id,
            ).status()
            self.replica.replicate_from_server(
                self.host,
                self.port,
                max_records=self.max_records,
                timeout=self.timeout,
                token=self.token,
                compress=self.compress,
                ssl_context=self.ssl_context,
                server_hostname=self.server_hostname,
                replica_id=self.replica_id,
            )
            self.failures = 0
            self.last_error = None
            return "healthy"
        except RedirectError as exc:
            self.redirects += 1
            if self.redirects <= 3:
                self.host = exc.host
                self.port = exc.port
                return self.poll()
            self.last_error = str(exc)
            return "needs_attention"
        except StorageError as exc:
            # 槽位被回收时 WAL 已经断开，但旧基准备份仍可能有效；先尝试
            # 页级增量重建，成功就继续追，失败才交给人工判断。
            self.last_error = str(exc)
            if self.auto_rebase:
                try:
                    self.replica.rebase_from_server(
                        self.host,
                        self.port,
                        timeout=self.timeout,
                        token=self.token,
                        compress=self.compress,
                        ssl_context=self.ssl_context,
                        server_hostname=self.server_hostname,
                        replica_id=self.replica_id,
                    )
                    self.failures = 0
                    self.last_error = None
                    return "rebuilt"
                except (OSError, StorageError):
                    pass
            return "needs_attention"
        except OSError as exc:
            self.failures += 1
            self.last_error = str(exc)
            if self.failures >= self.max_failures:
                self._promote()
                return "promoted"
            return "degraded"

    def start(self, interval: float | None = None) -> "ReplicationMonitor":
        """在后台线程里周期执行 :meth:`poll`，直到 :meth:`stop`。"""
        if self._thread is not None and self._thread.is_alive():
            return self
        if interval is not None:
            self.poll_interval = max(0.05, float(interval))
        self._stop.clear()

        def loop() -> None:
            while not self._stop.wait(self.poll_interval):
                status = self.poll()
                if status in ("promoted", "needs_attention"):
                    break

        self._thread = threading.Thread(
            target=loop, name="lumendb-replication-monitor", daemon=True
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _promote(self) -> None:
        self.replica.promote(
            fence_path=self.fence_path,
            node_id=self.node_id,
        )
        self.promoted = True


__all__ = ["ReplicationMonitor"]
