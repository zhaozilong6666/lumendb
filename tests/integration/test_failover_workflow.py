"""故障切换端到端演练：多数派提升 → 新主库服务 → 副本接入 → append 追平。

这是第 4 层"主库崩溃后副本可提升"的完整验收路径：不是单测某一个
组件，而是把 campaign / promote / serve_on / bootstrap / append_entries
串成一条真实调用链，任何一环接线断了都会在这里暴露。
"""

from __future__ import annotations

import time

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError
from lumendb.replication import Replica, ReplicationMonitor, WALServer
from lumendb.replication.quorum import QuorumNode


def _wait_for(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_full_failover_workflow(tmp_path):
    # 1. 主库起服务，写入基线数据。
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for i in range(20):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")
    server = WALServer(primary).start()

    nodes: list[QuorumNode] = []
    for i in range(3):
        nodes.append(
            QuorumNode(
                f"n{i}",
                state_path=tmp_path / f"n{i}.vote.json",
                timeout=1.0,
            ).start()
        )
    peers = {n.node_id: (n.host, n.port) for n in nodes}
    for node in nodes:
        node.peers = {p: a for p, a in peers.items() if p != node.node_id}

    candidate = None
    follower = None
    monitor = None
    try:
        # 2. 候选副本从旧主库追平。
        candidate = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "candidate"
        )
        assert candidate.execute_one("SELECT COUNT(*) FROM t").rows == [(20,)]

        # 3. 旧主库宕机；monitor 连续失败后自动多数派提升并起服务。
        server.stop()
        primary.close()
        monitor = ReplicationMonitor(
            candidate,
            server.host,
            server.port,
            fence_path=tmp_path / "fence.json",
            max_failures=1,
            timeout=0.2,
            quorum=nodes[0],
            heartbeat_interval=0.05,
            lease_keeper_auto=False,
            serve_on=("127.0.0.1", 0),
        )
        assert monitor.poll() == "promoted"
        assert nodes[0].state.term >= 1
        assert monitor.server is not None

        # 4. 新主库继续写，旧数据保留。
        candidate.execute_one("INSERT INTO t VALUES (20)")

        # 5. 新副本直接从新主库接入（bootstrap 走新主库的 serve 端点）。
        follower = Replica.bootstrap_from_server(
            monitor.server.host,
            monitor.server.port,
            tmp_path / "follower",
        )
        assert follower.execute_one("SELECT COUNT(*) FROM t").rows == [(21,)]

        # 6. 新副本挂进仲裁：心跳轮填 match，append_entries 追后续写入。
        follower.attach_quorum(nodes[1])
        candidate.execute_one("INSERT INTO t VALUES (21)")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(22,)],
            timeout=10.0,
        ), "提升后 append_entries 没有把新写入推到 follower"

        # 7. 拒绝面干净：follower 追平后不应残留分叉/前缀拒绝。
        assert follower.execute_one("SELECT MAX(id) FROM t").rows == [(21,)]
    finally:
        if monitor is not None:
            monitor.stop()
        if follower is not None:
            follower.close()
        if candidate is not None:
            candidate.close()
        for node in nodes:
            node.stop()
        server.stop()
        if not primary._closed:
            primary.close()


def test_heartbeat_timeout_drives_follower_promotion(tmp_path):
    """心跳超时驱动选举：leader 心跳停止（半开连接）→ follower 自动竞选。

    这是 status 失败计数路径覆盖不到的场景：leader 崩溃或分区后，
    follower 的复制流连接可能还挂着，只有心跳停摆才是可靠的失联信号。
    """
    from lumendb.replication.failover import ReplicationMonitor

    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for i in range(10):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")
    server = WALServer(primary).start()

    nodes = [
        QuorumNode(
            f"n{i}",
            state_path=tmp_path / f"n{i}.vote.json",
            timeout=1.0,
        ).start()
        for i in range(3)
    ]
    peers = {n.node_id: (n.host, n.port) for n in nodes}
    for node in nodes:
        node.peers = {p: a for p, a in peers.items() if p != node.node_id}

    fence_path = tmp_path / "fence.json"
    leader = None
    follower = None
    monitor = None
    try:
        # leader 手动提升并持续发心跳；follower 从旧主库追平后挂仲裁。
        leader = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "leader"
        )
        leader.promote_with_quorum(
            nodes[0],
            fence_path=fence_path,
            lease_keeper_auto=False,
            heartbeat_interval=0.05,
        )
        follower = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "follower"
        )
        follower.attach_quorum(nodes[1])
        monitor = ReplicationMonitor(
            follower,
            server.host,
            server.port,
            fence_path=fence_path,
            max_failures=100,  # status 路径不让它先触发，只测心跳超时
            timeout=0.2,
            quorum=nodes[1],
            lease_keeper_auto=False,
            election_timeout=0.5,
            serve_on=("127.0.0.1", 0),
        )
        monitor.start(interval=0.1)

        # 先确认 follower 收到过 leader 心跳（基线建立）。
        assert _wait_for(
            lambda: nodes[1]._last_heartbeat_at is not None, timeout=5.0
        ), "leader 心跳没有到达 follower 的仲裁节点"

        # leader 心跳停摆（进程还活着）：follower 应在选举超时后自动提升。
        heartbeat = leader._heartbeat
        assert heartbeat is not None
        heartbeat.stop()
        assert _wait_for(lambda: monitor.promoted, timeout=10.0), (
            "心跳超时后 follower 没有自动竞选提升"
        )

        # 新主库可写、旧数据完好，且起了复制服务。
        follower.execute_one("INSERT INTO t VALUES (10)")
        assert follower.execute_one("SELECT COUNT(*) FROM t").rows == [(11,)]
        assert monitor.server is not None

        # 旧 leader 仍存活时：共享 fence 已指向新主库，它的写必须被拒绝。
        with pytest.raises(NotSupportedError, match="围栏|fence"):
            leader.db.execute_one("INSERT INTO t VALUES (11)")
    finally:
        if monitor is not None:
            monitor.stop()
        if follower is not None:
            follower.close()
        if leader is not None:
            leader.close()
        for node in nodes:
            node.stop()
        server.stop()
        primary.close()
