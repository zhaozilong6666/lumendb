"""多数派提交水位（P2 轻量版）的回归测试。

要解决的具体故障：主库已提交 WAL 到 LSN 150，只发给了一个副本，然后宕机；
监控器把另一个只到 100 的副本提升为主库——已提交的 [101..150] 静默丢失。

轻量版协议：

* 每个副本把自己的**实际复制进度** `match_lsn` 报给 leader（心跳响应里带）；
* leader 取"自己 + 各 peer 的 match_lsn"的第多数派大的值作为 `quorum_commit_lsn`；
* 候选提升时必须 `last_lsn >= quorum_commit_lsn`，否则拒绝。
"""

from __future__ import annotations

import socket
import time

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError
from lumendb.replication import HeartbeatLoop, Replica, WALServer
from lumendb.replication.quorum import QuorumNode, VoteState


def _cluster(tmp_path, count: int = 3):
    nodes = []
    for i in range(count):
        node = QuorumNode(
            f"n{i}", state_path=tmp_path / f"n{i}.vote.json", timeout=0.5
        ).start()
        nodes.append(node)
    peers = {node.node_id: (node.host, node.port) for node in nodes}
    for node in nodes:
        node.peers = {
            pid: address for pid, address in peers.items() if pid != node.node_id
        }
    return nodes


def _stop(nodes):
    for node in nodes:
        node.stop()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class TestQuorumCommitWatermark:
    def test_heartbeat_round_advances_to_the_majority_position(self, tmp_path):
        nodes = _cluster(tmp_path)
        try:
            nodes[1].observe_applied(100)
            nodes[2].observe_applied(120)
            nodes[0].heartbeat_round(150)
            # 三个节点的 match_lsn = [150, 100, 120]，多数派 = 2 → 第 2 大 = 120
            assert nodes[0].quorum_commit_lsn == 120
            assert nodes[0].state.commit_lsn == 120
        finally:
            _stop(nodes)

    def test_watermark_survives_reload(self, tmp_path):
        nodes = _cluster(tmp_path)
        try:
            nodes[1].observe_applied(100)
            nodes[2].observe_applied(120)
            nodes[0].heartbeat_round(150)
            reloaded = VoteState(tmp_path / "n0.vote.json")
            assert reloaded.commit_lsn == 120
            assert reloaded.match_lsn == 150
        finally:
            _stop(nodes)

    def test_no_majority_means_no_advance(self, tmp_path):
        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json", timeout=0.2)
        node.peers = {"dead": ("127.0.0.1", _free_port())}
        try:
            node.heartbeat_round(500)
            assert node.quorum_commit_lsn == 0, "拿不到多数派响应时不能推进水位"
        finally:
            node.stop()

    def test_witness_does_not_inflate_the_watermark(self, tmp_path):
        """只听到 leader 进度的 witness 不能伪造 match_lsn。

        旧实现里 `heartbeat()` 会把 leader 的 last_lsn 记成自己的 last_lsn；
        如果水位直接用它，两个没有数据的 witness 就能把水位抬到 leader 的位置，
        于是"多数派已持有"变成一句空话，提升落后副本照样丢数据。
        """
        nodes = _cluster(tmp_path)
        try:
            # n1 / n2 只收到心跳（听到 150），但没有任何数据
            nodes[1].heartbeat(1, "n0", 150)
            nodes[2].heartbeat(1, "n0", 150)
            assert nodes[1].state.last_lsn == 150
            assert nodes[1].state.match_lsn == 0
            nodes[0].heartbeat_round(150)
            assert nodes[0].quorum_commit_lsn == 0, "witness 不能抬高提交水位"
        finally:
            _stop(nodes)

    def test_match_lsn_is_monotonic(self, tmp_path):
        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json")
        try:
            node.observe_applied(50)
            node.observe_applied(20)  # 落后不会把进度改小
            assert node.state.match_lsn == 50
        finally:
            node.stop()


class TestPromotionGate:
    def test_campaign_refuses_a_lagging_candidate(self, tmp_path):
        nodes = _cluster(tmp_path)
        try:
            nodes[0].observe_applied(150)
            nodes[1].observe_applied(150)
            nodes[2].observe_applied(150)
            nodes[0].heartbeat_round(150)
            assert nodes[0].quorum_commit_lsn == 150

            with pytest.raises(NotSupportedError, match="落后于多数派已提交水位") as exc:
                nodes[0].campaign(120)
            assert "candidate_lsn=120" in (exc.value.detail or "")
        finally:
            _stop(nodes)

    def test_campaign_allows_a_caught_up_candidate(self, tmp_path):
        nodes = _cluster(tmp_path)
        try:
            for node in nodes:
                node.observe_applied(150)
            nodes[0].heartbeat_round(150)
            term = nodes[0].campaign(150)
            assert term >= 1
        finally:
            _stop(nodes)

    def test_campaign_is_not_blocked_without_a_watermark(self, tmp_path):
        """全新集群（水位还是 0）不能被闸门挡住。"""
        nodes = _cluster(tmp_path)
        try:
            assert nodes[0].quorum_commit_lsn == 0
            assert nodes[0].campaign(0) >= 1
        finally:
            _stop(nodes)

    def test_promote_with_quorum_refuses_a_lagging_replica(self, tmp_path):
        nodes = _cluster(tmp_path)
        # 完全没有数据的副本：applied_lsn = 0，远落后于集群水位 100
        replica = Replica(tmp_path / "lagging", applied_lsn=0)
        try:
            nodes[0].observe_commit(100)
            with pytest.raises(NotSupportedError, match="落后"):
                replica.promote_with_quorum(nodes[0], lease_keeper_auto=False)
            assert replica.db._read_only, "被拒绝提升的副本必须仍然是只读"
        finally:
            _stop(nodes)
            replica.close()


class TestReplicaReportsAppliedLsn:
    def test_attach_quorum_reports_progress(self, tmp_path):
        primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        server = WALServer(primary).start()
        replica = Replica.bootstrap_from_server(server.host, server.port, tmp_path / "r")
        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json")
        try:
            replica.attach_quorum(node)
            primary.execute_one("INSERT INTO t VALUES (1)")
            replica.replicate_from_server(server.host, server.port)
            assert replica._applied_lsn > 0
            assert node.state.match_lsn == replica._applied_lsn
        finally:
            node.stop()
            replica.close()
            server.stop()
            primary.close()

    def test_heartbeat_loop_exposes_the_watermark(self, tmp_path):
        nodes = _cluster(tmp_path)
        try:
            nodes[1].observe_applied(80)
            nodes[2].observe_applied(90)
            loop = HeartbeatLoop(nodes[0], lambda: 120, interval=0.05).start()
            try:
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline and loop.last_quorum_commit_lsn == 0:
                    time.sleep(0.02)
                assert loop.last_quorum_commit_lsn == 90
            finally:
                loop.stop()
        finally:
            _stop(nodes)
