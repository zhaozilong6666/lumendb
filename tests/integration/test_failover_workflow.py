"""故障切换端到端演练：多数派提升 → 新主库服务 → 副本接入 → append 追平。

这是第 4 层"主库崩溃后副本可提升"的完整验收路径：不是单测某一个
组件，而是把 campaign / promote / serve_on / bootstrap / append_entries
串成一条真实调用链，任何一环接线断了都会在这里暴露。
"""

from __future__ import annotations

import time

from lumendb.engine.database import Database
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
