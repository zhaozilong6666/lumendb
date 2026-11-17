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
            f"n{i}", state_path=tmp_path / f"n{i}.vote.json", timeout=1.0
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


def _advance_watermark(node, last_lsn: int, *, expect: int, timeout: float = 8.0) -> None:
    """在 deadline 内反复跑心跳轮次，直到水位到达 expect。

    单次 ``heartbeat_round`` 需要多数派 peer 在 ``node.timeout`` 内应答；整库
    跑测试时机器可能很忙，命中一次超时就拿不到多数派（水位不推进），于是
    断言假失败。测试不该假定"一次就成功"，但**被测语义**仍然是"多数派够了
    才推进"，所以这里只重试轮次，不放宽断言。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        node.heartbeat_round(last_lsn)
        if node.quorum_commit_lsn == expect:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"水位没有推进到 {expect}：当前 {node.quorum_commit_lsn}"
        f"（last_lsn={last_lsn}, acks 可能一直不足）"
    )


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
            _advance_watermark(nodes[0], 150, expect=120)
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
            _advance_watermark(nodes[0], 150, expect=120)
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

    def test_observe_applied_also_advances_last_lsn(self, tmp_path):
        """真正 apply 过 100 的副本，不能对外声称自己只“听到”到 0。

        否则预投票会用一个陈旧的 last_lsn 放行落后候选，绕过
        ``candidate_lsn >= last_lsn`` 的投票安全性检查。
        """
        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json")
        try:
            node.observe_applied(100)
            assert node.state.match_lsn == 100
            assert node.state.last_lsn == 100
        finally:
            node.stop()

    def test_heartbeat_propagates_the_commit_watermark(self, tmp_path):
        """水位必须随心跳传播，否则新候选本地看到的 commit_lsn 还是 0。"""
        nodes = _cluster(tmp_path)
        try:
            nodes[1].observe_applied(100)
            nodes[2].observe_applied(120)
            nodes[0].heartbeat_round(150)
            assert nodes[0].quorum_commit_lsn == 120
            # 第一轮把水位算出来；下一轮心跳才可能把它带给 follower。
            nodes[0].heartbeat_round(150)
            assert nodes[1].state.commit_lsn == 120
            assert nodes[2].state.commit_lsn == 120
        finally:
            _stop(nodes)


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

    def test_pre_vote_rejects_a_candidate_behind_peers_that_have_applied(self, tmp_path):
        """两个 peer 已 apply 到 100、候选只有 80 时，预投票必须拒绝。

        没有 ``observe_applied -> last_lsn`` 的联动时，这两个 peer 的
        last_lsn 还是 0，会误把 80 的候选放进多数派；候选一旦当选，
        重新广播日志前就可能覆盖/丢弃已应用到 100 的日志。
        """
        nodes = _cluster(tmp_path)
        try:
            nodes[0].observe_applied(100)
            nodes[2].observe_applied(100)
            nodes[1].observe_applied(80)
            term_before = nodes[1].state.term
            with pytest.raises(NotSupportedError, match="预投票未通过多数派"):
                nodes[1].campaign(80)
            assert nodes[1].state.term == term_before, "预投票失败不能推进任期"
        finally:
            _stop(nodes)

    def test_campaign_reads_a_peer_watermark_when_local_is_zero(self, tmp_path):
        """候选本地水位为 0，但 peer 已经知道 100：闸门不能被本地值绕过。"""
        nodes = _cluster(tmp_path)
        try:
            nodes[0].observe_commit(100)
            with pytest.raises(NotSupportedError, match="落后于多数派已提交水位"):
                nodes[1].campaign(80)
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

    def test_promoted_replica_heartbeat_tracks_the_writable_wal(self, tmp_path):
        """提升后的正常 ``db.execute`` 也要推进心跳水位。

        复用旧 ``_applied_lsn`` 会让 leader 在心跳里一直报 0；三节点里只有
        一个 follower 可达时，多数派水位就永远算不上去，同步复制屏障会把
        本来可以提交的事务卡成 40003。
        """
        nodes = _cluster(tmp_path)
        replica = Replica(tmp_path / "promoted", applied_lsn=0)
        try:
            db = replica.promote_with_quorum(
                nodes[0],
                heartbeat_interval=0.05,
                sync_replication=True,
                sync_timeout=0.5,
            )
            # leader + n1 是多数派；n2 没有收到数据。
            nodes[1].observe_applied(10**9)
            db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
            assert db.wal.last_lsn > 0
            deadline = time.monotonic() + 2
            while (
                time.monotonic() < deadline
                and nodes[0].quorum_commit_lsn < db.wal.last_lsn
            ):
                time.sleep(0.02)
            assert nodes[0].quorum_commit_lsn >= db.wal.last_lsn
        finally:
            replica.close()
            _stop(nodes)


class TestStatusRpc:
    def test_status_exposes_the_watermark_over_the_wire(self, tmp_path):
        """运维排查"水位为什么不前进"必须能从网络读到这些字段。"""
        from lumendb.replication.quorum import QuorumClient

        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json").start()
        try:
            node.observe_applied(42)
            node.observe_commit(30)
            node.observe_lsn(99)
            status = QuorumClient.status(node.host, node.port)
            assert status["node_id"] == "n0"
            assert status["match_lsn"] == 42
            assert status["commit_lsn"] == 30
            assert status["last_lsn"] == 99
            assert status["type"] == "status_response"
        finally:
            node.stop()

    def test_status_rejects_a_bad_token(self, tmp_path):
        from lumendb.errors import StorageError
        from lumendb.replication.quorum import QuorumClient

        node = QuorumNode(
            "n0", state_path=tmp_path / "n0.vote.json", token="s3cret"
        ).start()
        try:
            with pytest.raises(StorageError, match="认证"):
                QuorumClient.status(node.host, node.port, token="wrong")
            assert QuorumClient.status(node.host, node.port, token="s3cret")["node_id"] == "n0"
        finally:
            node.stop()


class TestHeartbeatSurvivesRestart:
    def test_status_reports_the_last_leader_after_restart(self, tmp_path):
        """节点重启后仍要能报出"最近一次心跳来自谁"。

        只放内存的版本重启后就什么都不知道了，排障时分不清"主库没发心跳"
        和"我重启过"——这是上一轮 review 明确点出的缺口。
        """
        from lumendb.replication.quorum import QuorumClient

        state_path = tmp_path / "n0.vote.json"
        node = QuorumNode(
            "n0",
            state_path=state_path,
            heartbeat_persist_interval=0.0,  # 每次心跳都落盘，便于断言
        ).start()
        try:
            node.heartbeat(3, "primary", 120, commit_lsn=100)
            status = QuorumClient.status(node.host, node.port)
            assert status["last_heartbeat_from"] == "primary"
            assert status["last_heartbeat_at"]
        finally:
            node.stop()

        restarted = QuorumNode(
            "n0", state_path=state_path, heartbeat_persist_interval=0.0
        ).start()
        try:
            status = QuorumClient.status(restarted.host, restarted.port)
            assert status["last_heartbeat_from"] == "primary"
            assert status["commit_lsn"] == 100
            assert status["last_heartbeat_at"] == pytest.approx(
                node.state.last_heartbeat_at
            )
        finally:
            restarted.stop()

    def test_stale_heartbeat_is_not_overwritten_by_an_older_term(self, tmp_path):
        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json")
        try:
            node.heartbeat(5, "new-leader", 200, commit_lsn=180)
            accepted = node.heartbeat(4, "old-leader", 100)
            assert accepted["ok"] is False
            assert node.state.last_heartbeat_from == "new-leader"
            assert node.state.commit_lsn == 180
        finally:
            node.stop()
