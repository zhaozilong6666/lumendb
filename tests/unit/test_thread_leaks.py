"""后台线程不能泄漏：创建 → 关闭之后必须回到基线。

这一组不关心"谁先停"，只关心**有没有人忘了停**。历史上出现过：
`WALServer` 每个连接一个 handler 线程不回收、副本重建线程留下孤儿、
keeper/心跳在线程里自 join。线程泄漏不会立刻报错，只会让进程越跑越慢，
所以用"名字前缀计数 + deadline 轮询"把它变成可断言的。

豁免：`wal_stream` 的 per-connection handler 线程不归 ServiceRegistry 管
（它们的生命周期是连接），这里用 `WALServer.stop()` 作为兜底断言。
"""

from __future__ import annotations

import threading
import time

import pytest

from lumendb.engine.database import Database
from lumendb.replication import (
    Lease,
    QuorumNode,
    Replica,
    ReplicationMonitor,
    WALServer,
)

PREFIX = "lumendb-"


def _lumen_threads() -> set[str]:
    return {
        t.name
        for t in threading.enumerate()
        if t is not threading.current_thread() and t.name.startswith(PREFIX)
    }


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _count(*prefixes: str) -> int:
    """按名字前缀数线程**条数**。

    只断言"自己起的那些没残留"，而且用计数而不是集合：
    整库跑测试时别的用例可能同时有同名线程（keeper/heartbeat 的名字是固定的），
    用集合会把"我们漏了一个"和"别人还活着一个"混成同一个元素。
    """
    return sum(
        1
        for t in threading.enumerate()
        if t is not threading.current_thread()
        and any(t.name.startswith(p) for p in prefixes)
    )


def _dropped_to(baseline: int, *prefixes: str, timeout: float = 5.0) -> bool:
    return _wait_until(lambda: _count(*prefixes) <= baseline, timeout)


def test_replica_with_keeper_and_heartbeat_stops_everything(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    replica = Replica.bootstrap(primary, tmp_path / "replica")
    node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json").start()
    baseline = _count("lumendb-heartbeat", "lumendb-lease")
    try:
        replica.promote_with_quorum(
            node,
            lease_path=tmp_path / "lease.json",
            lease_ttl=30.0,
            heartbeat_interval=0.05,
        )
        assert _wait_until(
            lambda: _count("lumendb-heartbeat", "lumendb-lease") > baseline, 2.0
        ), "提升后应该有后台任务在跑"
        status = replica.services_status()
        assert status["running"], "提升后应该有后台任务在跑"
    finally:
        replica.close()
        node.stop()
        primary.close()
    assert _dropped_to(baseline, "lumendb-heartbeat", "lumendb-lease")


def test_quorum_node_and_wal_server_stop_their_threads(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    node = QuorumNode("n1", state_path=tmp_path / "n1.vote.json").start()
    baseline = _count("lumendb-wal", "lumendb-quorum")
    try:
        assert _wait_until(
            lambda: _count("lumendb-wal", "lumendb-quorum") > 0, 2.0
        ), "WALServer/QuorumNode 应该各起了线程"
    finally:
        server.stop()
        node.stop()
        primary.close()
    assert _dropped_to(max(0, baseline - 2), "lumendb-wal", "lumendb-quorum")


def test_replication_monitor_stop_leaves_no_threads(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(server.host, server.port, tmp_path / "r")
    node = QuorumNode("n2", state_path=tmp_path / "n2.vote.json").start()
    monitor = ReplicationMonitor(
        replica,
        server.host,
        server.port,
        quorum=node,
        poll_interval=0.05,
        lease_path=tmp_path / "lease.json",
        heartbeat_interval=0.05,
    )
    try:
        monitor.start()
        assert _wait_until(
            lambda: _count("lumendb-replication-monitor") > 0, 2.0
        )
    finally:
        monitor.stop()
        replica.close()
        node.stop()
        server.stop()
        primary.close()
    assert _wait_until(lambda: _count("lumendb-replication-monitor") == 0, 5.0)


def test_repeated_promote_and_step_down_does_not_accumulate_threads(tmp_path):
    """反复提升/降级：后台线程必须"停一个换一个"，不能累加。"""
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    replica = Replica.bootstrap(primary, tmp_path / "replica")
    node = QuorumNode("n3", state_path=tmp_path / "n3.vote.json").start()
    lease = tmp_path / "lease.json"
    baseline = _count("lumendb-heartbeat", "lumendb-lease")
    try:
        for _ in range(3):
            replica.promote_with_quorum(
                node, lease_path=lease, lease_ttl=30.0, heartbeat_interval=0.05
            )
            replica.step_down("cycle")
        status = replica.services_status()
        assert status["running"] == [], status
    finally:
        replica.close()
        node.stop()
        primary.close()
    assert _dropped_to(baseline, "lumendb-heartbeat", "lumendb-lease")
