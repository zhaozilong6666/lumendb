"""复制跟随与自动提升的最小控制器。

它只做三件事：

1. 周期性从 ``WALServer`` 拉一批 WAL 并 apply；
2. 连续网络失败达到阈值后，调用 ``Replica.promote``；
3. 如果给了 ``fence_path``，提升前先写共享 marker，旧主库通过
   ``PromotionFence.assert_writable`` 拒绝后续写入。

没有心跳租约；多数派投票和客户端重定向已做 MVP。它的定位是“单副本、
共享 fence 文件”的自动提升 MVP；网络分区时如果两个节点看不到同一个
fence 文件，仍可能双写，部署时必须明确这一点。
"""

from __future__ import annotations

import logging
import ssl
import threading
import time
from enum import Enum
from pathlib import Path

from ..errors import NotSupportedError, RedirectError, StorageError
from .replica import Replica
from .wal_stream import WALClient, WALServer

_log = logging.getLogger(__name__)


class MonitorState(str, Enum):
    """跟随器的显式状态。

    ``poll()`` 的返回值保持向后兼容（``healthy`` / ``degraded`` / ``promoted``
    / ``rebuilt`` / ``needs_attention``），但内部状态更细：调用方需要区分
    "已经提升但后来被降级"和"从未提升"时看 ``monitor.state``。
    """

    FOLLOWING = "following"      # 正在追主库
    PROMOTING = "promoting"      # 正在投票/提升
    PROMOTED = "promoted"        # 已提升且可写
    READ_ONLY = "read_only"      # 曾被提升，现在被降级为只读
    NEEDS_ATTENTION = "needs_attention"  # 需要人工介入


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
        quorum=None,
        lease_path: str | Path | None = None,
        lease_ttl: float = 10.0,
        lease_keeper_interval: float | None = None,
        lease_renew_with_quorum: bool = False,
        lease_loss_stepdown: bool = False,
        lease_keeper_auto: bool = True,
        heartbeat_interval: float | None = None,
        sync_replication: bool = False,
        sync_timeout: float = 2.0,
        serve_on: tuple[str, int] | None = None,
        election_timeout: float | None = None,
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
        self.quorum = quorum
        self.poll_interval = max(0.05, float(poll_interval))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.fence_path = Path(fence_path) if fence_path is not None else None
        self.node_id = node_id
        self.lease_path = Path(lease_path) if lease_path is not None else None
        self.lease_ttl = lease_ttl
        self.lease_keeper_interval = lease_keeper_interval
        self.lease_renew_with_quorum = lease_renew_with_quorum
        self.lease_loss_stepdown = lease_loss_stepdown
        self.lease_keeper_auto = lease_keeper_auto
        self.heartbeat_interval = heartbeat_interval
        self.sync_replication = sync_replication
        self.sync_timeout = sync_timeout
        #: 提升成功后自动起 WALServer 的地址（None = 不起）；其他 follower
        #: 可以从这里追新主库，分叉 follower 也用它自动重建。
        self.serve_on = serve_on
        #: 心跳超时驱动选举：follower 超过这么久没收到 leader 心跳就自动
        #: campaign（走 quorum 看门狗）。None = 关闭，只靠 status 失败计数。
        self.election_timeout = election_timeout
        #: 提升成功后由 monitor 启动的复制服务（未提升/未配置 serve_on 时为 None）。
        self.server: WALServer | None = None
        self.max_failures = max(1, int(max_failures))
        self.timeout = timeout
        self.max_records = max_records
        self.failures = 0
        self.promoted = False
        self.state = MonitorState.FOLLOWING
        self.last_error: str | None = None
        self.redirects = 0
        self.demotions = 0
        # 降级由 Replica 的 keeper/心跳线程触发，monitor 必须能被"反向通知"，
        # 否则 lease 丢失后它会一直认为自己是 promoted 的主库。
        self.replica.add_step_down_listener(self._handle_step_down)

    @property
    def last_push_rejections(self) -> dict[str, str]:
        """提升后 push_entries_to_peers 最近被 peers 拒绝的原因（快照）。

        follower 在同一 LSN 上看到不同任期（历史分叉）时会拒绝并要求
        重建（``rebuild_required``）；无人值守运行时可以轮询这个字典
        把卡住的副本暴露给告警/人工介入，原因变化时 replica 侧还会打
        warning 日志。
        """
        return dict(getattr(self.replica, "last_push_rejections", {}))

    def poll(self) -> str:
        """执行一轮跟随；返回 ``healthy`` / ``degraded`` / ``promoted`` / ``needs_attention``。"""
        if self.state is MonitorState.PROMOTED:
            return "promoted"
        if self.state in (MonitorState.READ_ONLY, MonitorState.NEEDS_ATTENTION):
            return "needs_attention"
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
            if self.state is MonitorState.FOLLOWING:
                pass
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
                if self._promote():
                    return "promoted"
                return "needs_attention"
            return "degraded"

    def start(self, interval: float | None = None) -> "ReplicationMonitor":
        """在后台线程里周期执行 :meth:`poll`，直到 :meth:`stop`。"""
        if self._thread is not None and self._thread.is_alive():
            return self
        if interval is not None:
            self.poll_interval = max(0.05, float(interval))
        self._stop.clear()
        self._start_election_watchdog()

        def loop() -> None:
            while not self._stop.wait(self.poll_interval):
                try:
                    status = self.poll()
                except Exception as exc:  # noqa: BLE001 - 监控线程不能因单次异常退出
                    # poll 已经处理了 OSError/StorageError/RedirectError；
                    # 其余异常（协议解析、内存错误等）记录下来继续轮询，
                    # 否则一次坏响应就会让自动切换永久失效。
                    self.failures += 1
                    self.last_error = f"poll crashed: {exc}"
                    _log.exception("复制监控 poll 抛出未预期异常")
                    continue
                if status in ("promoted", "needs_attention"):
                    break

        self._thread = threading.Thread(
            target=loop, name="lumendb-replication-monitor", daemon=True
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._stop_election_watchdog()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
        server = self.server
        self.server = None
        if server is not None:
            try:
                server.stop()
            except OSError:
                pass

    def _promote(self) -> bool:
        self.state = MonitorState.PROMOTING
        try:
            if self.quorum is not None:
                self.replica.promote_with_quorum(
                    self.quorum,
                    fence_path=self.fence_path,
                    lease_path=self.lease_path,
                    lease_ttl=self.lease_ttl,
                    lease_keeper_interval=self.lease_keeper_interval,
                    lease_keeper_auto=self.lease_keeper_auto,
                    lease_renew_with_quorum=self.lease_renew_with_quorum,
                    lease_loss_stepdown=self.lease_loss_stepdown,
                    # 双候选同窗竞选时，对方可能以更高任期当选：心跳轮
                    # 观察到更高任期就降级回只读，不留双主窗口。
                    on_higher_term=self._on_higher_term,
                    # 无人值守自动提升后必须有心跳维持 leader：没显式配置
                    # 时用选举超时的一半，否则其他 follower 会再次超时，
                    # 整个集群陷入反复选举。
                    heartbeat_interval=self.heartbeat_interval
                    or (
                        self.election_timeout / 2.0
                        if self.election_timeout
                        else None
                    ),
                    sync_replication=self.sync_replication,
                    sync_timeout=self.sync_timeout,
                    node_id=self.quorum.node_id,
                )
            else:
                self.replica.promote(
                    fence_path=self.fence_path,
                    node_id=self.node_id,
                )
        except (NotSupportedError, StorageError) as exc:
            self.last_error = str(exc)
            # 竞选类失败与"根本不能当主库"（持久化/闸门损坏）要分开：
            # 前者在下一个选举超时重试——票被瓜分时输家回 FOLLOWING，
            # 之后被赢家心跳重新喂住自然退出；fence 里已有更高提升 LSN
            # 说明另一个主库更超前，正确动作同样是回去跟随它。
            if self.quorum is not None and any(
                marker in str(exc)
                for marker in (
                    "预投票",
                    "投票",
                    "多数派",
                    "更高的提升 LSN",
                )
            ):
                self.state = MonitorState.FOLLOWING
            else:
                self.state = MonitorState.NEEDS_ATTENTION
            return False
        self.promoted = True
        self.state = MonitorState.PROMOTED
        # 自己已经是 leader：心跳由自己发出，看门狗再等别人的心跳会
        # 在超时后把新主库逼停，必须立刻停掉。
        self._stop_election_watchdog()
        self._start_serving()
        return True

    def _start_election_watchdog(self) -> None:
        """接线 quorum 选举看门狗：心跳超时 → follower 自动竞选。

        只有配置了 ``election_timeout`` 且副本挂在 quorum 上才生效。
        看门狗回调直通 :meth:`_promote`，绕过 status 失败计数——主库
        崩溃时 follower 的复制流连接可能还挂着（半开连接），只有心跳
        超时才是可靠的"leader 失联"信号。
        """
        if self.election_timeout is None or self.quorum is None:
            return
        self.quorum.election_timeout = float(self.election_timeout)
        self.quorum.on_leader_lost = self._on_leader_lost
        self.quorum.start_election_watchdog()

    def _stop_election_watchdog(self) -> None:
        if self.quorum is not None and hasattr(
            self.quorum, "stop_election_watchdog"
        ):
            self.quorum.stop_election_watchdog()
            self.quorum.on_leader_lost = None

    def _on_leader_lost(self) -> None:
        """leader 心跳超时：只有仍在跟随状态才发起竞选。

        PROMOTING（竞选进行中）不重复触发；_promote 失败会落到
        NEEDS_ATTENTION，也不再自动重试，留给人工介入。
        """
        if self.state is MonitorState.FOLLOWING:
            self._promote()

    def _start_serving(self) -> None:
        """提升成功后对外服务复制流，并把自己登记为分叉重建端点。

        其他 follower 的 monitor 会被旧主库的 redirect 导到这里；分叉的
        follower 则通过 append_entries 里的 rebuild_host 直接重建。起服务
        失败只记日志——提升本身已经成功，不能因此回退。
        """
        if self.serve_on is None:
            return
        try:
            self.server = WALServer(
                self.replica.db,
                host=self.serve_on[0],
                port=int(self.serve_on[1]),
                token=self.token,
            ).start()
            self.replica.rebuild_from = (self.server.host, self.server.port)
            self.replica.rebuild_token = self.token
            _log.info(
                "提升完成，复制服务监听 %s:%s", self.server.host, self.server.port
            )
        except OSError as exc:
            _log.warning("提升后起复制服务失败（follower 无法追流/重建）: %s", exc)
            self.last_error = f"serve failed: {exc}"

    def _on_higher_term(self, term: int) -> None:
        """心跳轮观察到更高任期：本节点已不再是 leader，自动降级。

        双候选同时竞选时，一方会以更高任期当选；本主库的心跳从 peer
        收到更高任期后必须停写，否则留下双主窗口。
        """
        if not self.promoted:
            return
        self.replica.step_down(f"观察到更高任期 {term}")

    def _handle_step_down(self, reason: str) -> None:
        """Replica 被降级时把状态扣回 needs_attention，而不是继续报 promoted。"""
        self.demotions += 1
        self.promoted = False
        self.state = MonitorState.NEEDS_ATTENTION
        self.last_error = f"stepped down: {reason}"


__all__ = ["ReplicationMonitor"]
