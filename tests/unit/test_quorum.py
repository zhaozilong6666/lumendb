"""多数派投票与提升 fence 的集成测试。"""

from __future__ import annotations

import shutil
import time

import pytest

from lumendb.errors import NotSupportedError
from lumendb.engine.database import Database
from lumendb.replication import HeartbeatLoop, Lease, PromotionFence, Replica, ReplicationMonitor, WALServer
from lumendb.replication.quorum import QuorumClient, QuorumNode, VoteState
from lumendb.replication.wal_stream import _record_to_json
from lumendb.storage.wal import LogRecord, LogType


def _make_primary(tmp_path):
    return Database(tmp_path / "primary", checkpoint_wal_bytes=0)


def _cluster(tmp_path, count: int = 3):
    nodes = []
    for i in range(count):
        node = QuorumNode(
            f"n{i}",
            state_path=tmp_path / f"n{i}.vote.json",
            timeout=1.0,
        ).start()
        nodes.append(node)
    peers = {
        node.node_id: (node.host, node.port)
        for node in nodes
    }
    for node in nodes:
        node.peers = {
            peer_id: address
            for peer_id, address in peers.items()
            if peer_id != node.node_id
        }
    return nodes


def _stop(nodes):
    for node in nodes:
        node.stop()


def _wait_for(predicate, timeout: float = 3.0) -> bool:
    """轮询等待条件成立。

    心跳循环跑在后台线程里，固定 ``sleep(0.2)`` 在负载高的机器上会假失败
    （CI 上偶发过一次）。这里改成"最多等 timeout，条件成立就返回"。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_campaign_wins_majority_and_writes_fence(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        fence_path = tmp_path / "fence.json"
        term = nodes[0].campaign(last_lsn=120, fence_path=fence_path)
        assert term == 1
        from lumendb.replication import PromotionFence

        marker = PromotionFence(fence_path, node_id="n0").read()
        assert marker == {"promoted_node": "n0", "lsn": 120}
    finally:
        _stop(nodes)


def test_campaign_refuses_without_majority(tmp_path):
    """拿不到多数派时必须拒绝提升，而且**不能推进任期**。

    预投票（pre-vote）先探询意向：失败就保持原任期返回，避免分区里的节点
    反复重试把自己的任期越推越高，恢复后把健康的主库逼停。
    """
    nodes = _cluster(tmp_path)
    try:
        nodes[1].stop()
        nodes[2].stop()
        term_before = nodes[0].state.term
        with pytest.raises(NotSupportedError, match="预投票未通过多数派"):
            nodes[0].campaign(last_lsn=10)
        assert nodes[0].state.term == term_before, "预投票失败不能推进任期"
        # 显式关掉预投票时仍然走旧路径：先推任期再投票，失败报"多数派投票失败"
        with pytest.raises(NotSupportedError, match="多数派投票失败"):
            nodes[0].campaign(last_lsn=10, pre_vote=False)
        assert nodes[0].state.term == term_before + 1
    finally:
        _stop(nodes)


def test_higher_term_rejects_stale_candidate(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[0].campaign(last_lsn=5)
        granted_new = nodes[1].request_vote(3, "n2", 5)
        assert granted_new["granted"] is True
        stale = nodes[1].request_vote(2, "n0", 6)
        assert stale["term"] == 3
        assert stale["granted"] is False
    finally:
        _stop(nodes)


def test_vote_compares_last_term_before_lsn(tmp_path):
    """同 LSN 但任期更旧的候选不能拿到票（Raft 的 (last_term,last_lsn) 规则）。"""
    nodes = _cluster(tmp_path)
    try:
        nodes[1].observe_applied(100, term=5)
        assert nodes[1].state.last_term == 5
        # 心跳只带 leader 当前任期，不能把它污染成日志条目的任期
        nodes[1].heartbeat(5, "n0", 100)
        assert nodes[1].state.last_term == 5
        # 120 的 LSN 更大，但最后日志任期 4 比本地 5 旧 → 拒绝
        stale = nodes[1].request_vote(6, "n2", 120, last_term=4)
        assert stale["granted"] is False
        # 同任期下 LSN 更大 → 可以授予
        newer = nodes[1].request_vote(6, "n2", 120, last_term=5)
        assert newer["granted"] is True
        # 预投票同样执行 prefix/term 检查，且不改任期
        term_before = nodes[1].state.term
        pre = nodes[1].request_vote(7, "n3", 120, last_term=4, pre_vote=True)
        assert pre["granted"] is False
        assert nodes[1].state.term == term_before
    finally:
        _stop(nodes)


def test_campaign_pre_vote_rejects_older_last_term(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[0].observe_applied(100, term=6)
        nodes[2].observe_applied(100, term=6)
        nodes[1].observe_applied(100, term=5)
        with pytest.raises(NotSupportedError, match="预投票未通过多数派"):
            nodes[1].campaign(100)
    finally:
        _stop(nodes)


def test_pre_vote_stops_on_a_peer_with_higher_term(tmp_path):
    """一个 peer 的任期更高，就说明本轮意向过时；不能靠剩余票凑多数派。"""
    nodes = _cluster(tmp_path)
    try:
        nodes[2].state.term = 9
        nodes[2].state.save()
        with pytest.raises(NotSupportedError, match="观察到更高任期"):
            nodes[0].campaign(0, pre_vote=True)
        # 预投票不改任期，候选仍停留在原值
        assert nodes[0].state.term == 0
    finally:
        _stop(nodes)


def test_heartbeat_rpc_updates_peer_and_rejects_stale_term(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[0].campaign(last_lsn=10)
        response = QuorumClient.heartbeat(
            nodes[1].host, nodes[1].port, 1, "n0", 20
        )
        assert response["ok"] is True
        assert nodes[1].state.last_lsn == 20
        stale = QuorumClient.heartbeat(
            nodes[1].host, nodes[1].port, 0, "n0", 30
        )
        assert stale["ok"] is False
        assert stale["term"] == 1
    finally:
        _stop(nodes)


def test_quorum_lease_renewal_requires_majority(tmp_path):
    nodes = _cluster(tmp_path)
    lease_path = tmp_path / "lease.json"
    try:
        nodes[0].campaign(
            last_lsn=10,
            lease_path=lease_path,
            lease_ttl=30,
        )
        initial = Lease(lease_path, "n0", ttl=30).read()["expires_at"]
        renewed = nodes[0].renew_lease(
            10,
            lease_path=lease_path,
            lease_ttl=30,
            now=initial - 1,
        )
        assert renewed > initial
        after_renew = Lease(lease_path, "n0", ttl=30).read()["expires_at"]
        nodes[1].stop()
        nodes[2].stop()
        with pytest.raises(NotSupportedError, match="多数派心跳不足"):
            nodes[0].renew_lease(
                10,
                lease_path=lease_path,
                lease_ttl=30,
                now=initial + 1,
            )
        assert Lease(lease_path, "n0", ttl=30).read()["expires_at"] == after_renew
    finally:
        _stop(nodes)


def test_voter_rejects_candidate_behind_its_lsn(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[2].observe_lsn(100)
        behind = nodes[2].request_vote(4, "n1", 99)
        assert behind["granted"] is False
        caught_up = nodes[2].request_vote(4, "n1", 100)
        assert caught_up["granted"] is True
    finally:
        _stop(nodes)


def test_vote_state_survives_restart(tmp_path):
    state_path = tmp_path / "state.json"
    node = QuorumNode("n0", state_path=state_path)
    assert node.state.term == 0
    assert node.request_vote(7, "n0", 0)["granted"] is True
    restarted = QuorumNode("n0", state_path=state_path)
    assert restarted.state.term == 7
    assert restarted.state.voted_for == "n0"
    assert VoteState(state_path).term == 7


def test_replica_promotes_only_after_quorum(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    replica = Replica.bootstrap(primary, tmp_path / "replica")
    nodes = _cluster(tmp_path)
    try:
        fence_path = tmp_path / "fence.json"
        replica.promote_with_quorum(nodes[0], fence_path=fence_path)
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        replica.execute_one("INSERT INTO t VALUES (2)")
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
        assert PromotionFence(fence_path, node_id="n0").read()["promoted_node"] == "n0"
    finally:
        _stop(nodes)
        replica.db.close()
        primary.close()



def test_monitor_uses_quorum_for_auto_promotion(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    server = WALServer(primary).start()
    replica = None
    nodes = _cluster(tmp_path)
    try:
        replica = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "replica"
        )
        server.stop()
        monitor = ReplicationMonitor(
            replica,
            server.host,
            server.port,
            fence_path=tmp_path / "fence.json",
            max_failures=1,
            timeout=0.2,
            quorum=nodes[0],
        )
        assert monitor.poll() == "promoted"
        replica.execute_one("INSERT INTO t VALUES (2)")
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
    finally:
        _stop(nodes)
        if replica is not None:
            replica.db.close()
        server.stop()
        primary.close()


def test_monitor_promotion_starts_the_heartbeat_push(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    replica = None
    follower = None
    nodes = _cluster(tmp_path)
    try:
        replica = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "replica"
        )
        follower = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "follower"
        )
        server.stop()
        follower.attach_quorum(nodes[1])
        monitor = ReplicationMonitor(
            replica,
            server.host,
            server.port,
            fence_path=tmp_path / "fence.json",
            max_failures=1,
            timeout=0.2,
            quorum=nodes[0],
            heartbeat_interval=0.05,
            lease_keeper_auto=False,
        )
        assert monitor.poll() == "promoted"
        # monitor 提升走的 promote_with_quorum 应该已经把心跳循环挂上：
        # 新写入会被 append_entries 推到另一个真实副本。
        replica.execute_one("INSERT INTO t VALUES (1)")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)],
            timeout=5.0,
        ), "monitor 提升后的心跳循环没有推送新写入"
    finally:
        _stop(nodes)
        if replica is not None:
            replica.db.close()
        if follower is not None:
            follower.db.close()
        server.stop()
        primary.close()


def test_monitor_loop_survives_unexpected_poll_errors(tmp_path, monkeypatch):
    """poll() 抛未预期异常时后台轮询线程必须继续跑，而不是静默退出。"""
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    replica = None
    monitor = None
    try:
        replica = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "replica"
        )
        server.stop()
        import lumendb.replication.wal_stream as ws

        def boom(self, *args, **kwargs):
            raise ValueError("boom")

        monkeypatch.setattr(ws.WALClient, "status", boom)
        monitor = ReplicationMonitor(
            replica,
            server.host,
            server.port,
            max_failures=1,
            timeout=0.2,
            poll_interval=0.05,
        )
        monitor.start()
        assert _wait_for(
            lambda: monitor.last_error == "poll crashed: boom",
            timeout=3.0,
        ), f"监控线程没有记录未预期异常：last_error={monitor.last_error!r}"
    finally:
        if monitor is not None:
            monitor.stop()
        server.stop()
        if replica is not None:
            replica.db.close()
        primary.close()


def test_monitor_auto_promotion_uses_lease(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    server = WALServer(primary).start()
    replica = None
    nodes = _cluster(tmp_path)
    lease_path = tmp_path / "lease.json"
    try:
        replica = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "replica"
        )
        server.stop()
        monitor = ReplicationMonitor(
            replica,
            server.host,
            server.port,
            fence_path=tmp_path / "fence.json",
            lease_path=lease_path,
            lease_ttl=30,
            max_failures=1,
            timeout=0.2,
            quorum=nodes[0],
        )
        assert monitor.poll() == "promoted"
        assert Lease(lease_path, "n0").read()["holder"] == "n0"
        replica.execute_one("INSERT INTO t VALUES (2)")
        Lease(lease_path, "n0").release()
        with pytest.raises(NotSupportedError, match="没有有效写租约"):
            replica.execute_one("INSERT INTO t VALUES (3)")
    finally:
        _stop(nodes)
        if replica is not None:
            replica.db.close()
        server.stop()
        primary.close()


def test_monitor_refuses_promotion_without_quorum(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    replica = None
    nodes = _cluster(tmp_path)
    try:
        replica = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "replica"
        )
        nodes[1].stop()
        nodes[2].stop()
        server.stop()
        monitor = ReplicationMonitor(
            replica,
            server.host,
            server.port,
            max_failures=1,
            timeout=0.2,
            quorum=nodes[0],
        )
        assert monitor.poll() == "needs_attention"
        assert replica.db._read_only is True
    finally:
        _stop(nodes)
        if replica is not None:
            replica.db.close()
        server.stop()
        primary.close()


def test_heartbeat_round_steps_down_on_higher_term(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[1].state.term = 7
        nodes[1].state.save()
        acks, errors = nodes[0].heartbeat_round(0)
        assert nodes[0].state.term == 7
        assert nodes[0].state.voted_for is None
        assert any("higher term" in item for item in errors)
        assert acks >= 1
    finally:
        _stop(nodes)


def test_renew_lease_rejects_stale_term(tmp_path):
    node = QuorumNode("n0", state_path=tmp_path / "vote.json")
    lease_path = tmp_path / "lease.json"
    node.campaign(0, lease_path=lease_path, lease_ttl=30)
    node.state.term += 1
    node.state.save()
    with pytest.raises(NotSupportedError, match="任期已过期"):
        node.renew_lease(0, lease_path=lease_path, lease_ttl=30)


def test_heartbeat_loop_reports_higher_term(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[1].state.term = 7
        nodes[1].state.save()
        seen = []
        loop = HeartbeatLoop(
            nodes[0],
            lambda: 0,
            interval=0.05,
            on_higher_term=seen.append,
        ).start()
        try:
            assert _wait_for(lambda: seen and nodes[0].state.term == 7), (
                f"心跳循环没有观察到更高任期：term={nodes[0].state.term} seen={seen}"
            )
            assert seen[-1] >= 7
            assert loop.last_acks >= 1
        finally:
            loop.stop()
    finally:
        _stop(nodes)


def test_heartbeat_loop_survives_callback_error(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[1].state.term = 7
        nodes[1].state.save()

        def bad_callback(term):
            raise RuntimeError("boom")

        loop = HeartbeatLoop(
            nodes[0],
            lambda: 0,
            interval=0.05,
            on_higher_term=bad_callback,
        ).start()
        try:
            assert _wait_for(
                lambda: nodes[0].state.term == 7
                and loop.last_callback_error == "boom"
            ), (
                "回调异常没有被记录："
                f"term={nodes[0].state.term} err={loop.last_callback_error!r}"
            )
            assert loop.last_acks >= 1
        finally:
            loop.stop()
    finally:
        _stop(nodes)


def test_pre_vote_does_not_touch_vote_state(tmp_path):
    """预投票只回答意向：不改任期、不记 voted_for、不落盘。"""
    nodes = _cluster(tmp_path)
    try:
        before = (nodes[1].state.term, nodes[1].state.voted_for)
        response = nodes[1].request_vote(5, "n0", 100, pre_vote=True)
        assert response["pre_vote"] is True
        assert response["granted"] is True  # 会投给它
        assert (nodes[1].state.term, nodes[1].state.voted_for) == before
    finally:
        _stop(nodes)


def test_pre_vote_rejects_a_lagging_candidate(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[1].observe_lsn(500)
        response = nodes[1].request_vote(1, "n0", 100, pre_vote=True)
        assert response["granted"] is False
        assert nodes[1].state.term == 0
    finally:
        _stop(nodes)


def test_append_entries_applies_a_contiguous_batch(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        applied = []
        nodes[0].set_log_applier(lambda records: applied.extend(records) or len(records))
        first = LogRecord(1, 7, LogType.BEGIN, b"")
        second = LogRecord(
            first.lsn + len(first.encode()), 7, LogType.COMMIT, b""
        )
        response = nodes[0].append_entries(
            1,
            "leader",
            0,
            0,
            [_record_to_json(first), _record_to_json(second)],
            commit_lsn=second.lsn,
        )
        assert response["ok"] is True
        assert applied == [first, second]
        assert nodes[0].state.match_lsn == second.lsn
        assert nodes[0].state.match_term == 1
        assert nodes[0].quorum_commit_lsn == second.lsn

        # 同一个前缀不能重复追加；leader 应回退到更早的位置重发。
        stale = nodes[0].append_entries(
            1,
            "leader",
            0,
            0,
            [_record_to_json(first)],
        )
        assert stale["ok"] is False
        assert stale["reason"] == "prefix_mismatch"
    finally:
        _stop(nodes)


def test_append_entries_rejects_a_stale_leader_term(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[0].state.term = 5
        nodes[0].state.save()
        response = nodes[0].append_entries(4, "leader", 0, 0, [])
        assert response["ok"] is False
        assert response["reason"] == "stale_term"
        assert response["term"] == 5
    finally:
        _stop(nodes)


def test_append_entries_rejects_an_oversized_batch(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        bogus = [
            _record_to_json(LogRecord(1, 7, LogType.BEGIN, b""))
        ] * 1001
        response = nodes[0].append_entries(1, "leader", 0, 0, bogus)
        assert response["ok"] is False
        assert "条目过多" in response["reason"]
    finally:
        _stop(nodes)


def test_append_entries_survives_a_raising_applier(tmp_path):
    """applier 抛任意异常都回退成拒绝，且不打死节点的服务循环。"""
    nodes = _cluster(tmp_path)
    try:
        def boom(records):
            raise OSError("boom")

        nodes[1].set_log_applier(boom)
        first = LogRecord(1, 7, LogType.BEGIN, b"")
        response = QuorumClient.append_entries(
            "127.0.0.1",
            nodes[1].port,
            1,
            "leader",
            0,
            0,
            [_record_to_json(first)],
            timeout=1.0,
        )
        assert response.get("ok") is False
        assert "日志重放失败" in response.get("reason", "")
        # 同一个节点还活着：心跳仍然应答。
        ack = QuorumClient.heartbeat(
            "127.0.0.1", nodes[1].port, 1, "leader", 1, timeout=1.0
        )
        assert ack.get("ok") is True
    finally:
        _stop(nodes)


def test_append_entries_rejects_an_internal_gap(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        applied = []
        nodes[0].set_log_applier(lambda records: applied.extend(records) or len(records))
        first = LogRecord(1, 7, LogType.BEGIN, b"")
        gap = LogRecord(first.lsn + len(first.encode()) + 100, 7, LogType.COMMIT, b"")
        response = nodes[0].append_entries(
            1,
            "leader",
            0,
            0,
            [_record_to_json(first), _record_to_json(gap)],
        )
        assert response["ok"] is False
        assert response["reason"] == "日志条目内部不连续"
        assert applied == []
    finally:
        _stop(nodes)


def test_append_entries_rpc_roundtrip(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        applied = []
        nodes[1].set_log_applier(
            lambda records: applied.extend(records) or len(records)
        )
        first = LogRecord(1, 7, LogType.BEGIN, b"")
        response = QuorumClient.append_entries(
            nodes[1].host,
            nodes[1].port,
            1,
            "n0",
            0,
            0,
            [_record_to_json(first)],
            commit_lsn=first.lsn,
        )
        assert response["ok"] is True
        assert applied == [first]
        assert response["match_lsn"] == first.lsn
        assert nodes[1].state.commit_lsn == first.lsn
    finally:
        _stop(nodes)


def test_append_entries_moves_a_real_replica_forward(tmp_path):
    """append_entries 不是只改元数据：entries 必须真正落到副本页上。"""
    primary = _make_primary(tmp_path)
    replica = Replica.bootstrap(primary, tmp_path / "replica")
    node = QuorumNode("r", state_path=tmp_path / "r.vote.json").start()
    try:
        replica.attach_quorum(node)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        primary.execute_one("INSERT INTO t VALUES (1, 'one')")
        records = list(primary.wal.iter_records(replica.applied_lsn + 1))
        response = QuorumClient.append_entries(
            node.host,
            node.port,
            1,
            "primary",
            replica.applied_lsn,
            0,
            [_record_to_json(record) for record in records],
            commit_lsn=records[-1].lsn,
        )
        assert response["ok"] is True
        assert response["match_lsn"] == records[-1].lsn
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
    finally:
        node.stop()
        replica.db.close()
        primary.close()


def test_push_entries_to_peers_advances_a_real_replica(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    leader = Replica.bootstrap(primary, tmp_path / "leader")
    follower = Replica.bootstrap(primary, tmp_path / "follower")
    nodes = _cluster(tmp_path, count=2)
    try:
        follower.attach_quorum(nodes[1])
        leader.promote()
        leader.attach_quorum(nodes[0])
        leader.execute_one("INSERT INTO t VALUES (1, 'one')")
        assert leader.db.wal.last_lsn > follower.applied_lsn

        nodes[0].heartbeat_round(leader.db.wal.last_lsn)
        batch = list(leader.db.wal.iter_records(follower.applied_lsn + 1))
        assert batch
        pushed = leader.push_entries_to_peers(nodes[0])
        assert pushed.get("n1", 0) > 0
        assert follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        # 推送成功后多数派水位要立刻计入 peer 的 match_lsn，不等下一轮心跳。
        assert nodes[0].quorum_commit_lsn == batch[-1].lsn
        # status 快照里能看到每个 peer 的复制位置，便于外部诊断滞后。
        assert nodes[0].status()["peer_matches"]["n1"] == batch[-1].lsn
    finally:
        _stop(nodes)
        leader.db.close()
        follower.db.close()
        primary.close()


def test_push_stops_when_a_peer_reports_a_higher_term(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    leader = Replica.bootstrap(primary, tmp_path / "leader")
    follower = Replica.bootstrap(primary, tmp_path / "follower")
    nodes = _cluster(tmp_path, count=2)
    try:
        follower.attach_quorum(nodes[1])
        leader.promote()
        leader.attach_quorum(nodes[0])
        leader.execute_one("INSERT INTO t VALUES (1, 'one')")
        nodes[0].heartbeat_round(leader.db.wal.last_lsn)
        match_before = dict(nodes[0].last_matches)
        nodes[1].state.term = nodes[0].state.term + 1
        pushed = leader.push_entries_to_peers(nodes[0])
        assert pushed == {}, "peer 已进入更高任期，本轮不该再推送任何批次"
        assert nodes[0].last_matches == match_before
        assert follower.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
    finally:
        _stop(nodes)
        leader.db.close()
        follower.db.close()
        primary.close()


def test_heartbeat_loop_pushes_entries_to_a_real_replica(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    leader = Replica.bootstrap(primary, tmp_path / "leader")
    # 两个副本必须从同一个基线开始，否则第二次 bootstrap 的 checkpoint
    # 会让 follower 的 applied_lsn 比 leader 还新，预投票会拒绝提升。
    source = tmp_path / "leader"
    leader.db.close()
    shutil.copytree(source, tmp_path / "follower")
    leader.db = Database(source, read_only=True, checkpoint_wal_bytes=0)
    follower = Replica(tmp_path / "follower")
    nodes = _cluster(tmp_path, count=2)
    try:
        follower.attach_quorum(nodes[1])
        leader.promote_with_quorum(
            nodes[0],
            heartbeat_interval=0.1,
            lease_keeper_auto=False,
        )
        leader.execute_one("INSERT INTO t VALUES (1, 'one')")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)],
            timeout=3.0,
        ), "心跳循环没有把 append_entries 推到副本"
    finally:
        _stop(nodes)
        leader.db.close()
        follower.db.close()
        primary.close()


def test_heartbeat_push_catches_up_during_concurrent_writes(tmp_path):
    """自动推送与主库写并发：心跳线程在迭代 WAL 的同时，写线程在追加。"""
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    leader = Replica.bootstrap(primary, tmp_path / "leader")
    source = tmp_path / "leader"
    leader.db.close()
    shutil.copytree(source, tmp_path / "follower")
    leader.db = Database(source, read_only=True, checkpoint_wal_bytes=0)
    follower = Replica(tmp_path / "follower")
    nodes = _cluster(tmp_path, count=2)
    try:
        follower.attach_quorum(nodes[1])
        leader.promote_with_quorum(
            nodes[0],
            heartbeat_interval=0.05,
            lease_keeper_auto=False,
        )
        for i in range(200):
            leader.execute_one(f"INSERT INTO t VALUES ({i}, 'v{i}')")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(200,)],
            timeout=10.0,
        ), "心跳推送没有在并发写入下追平副本"
        # 追平后再写几笔，确认批次边界没有被并发追加破坏。
        for i in range(200, 205):
            leader.execute_one(f"INSERT INTO t VALUES ({i}, 'v{i}')")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(205,)],
            timeout=10.0,
        ), "追平后的增量批次没有送达"
    finally:
        _stop(nodes)
        leader.db.close()
        follower.db.close()
        primary.close()


def test_heartbeat_push_resumes_after_leader_restart(tmp_path):
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    leader = Replica.bootstrap(primary, tmp_path / "leader")
    source = tmp_path / "leader"
    leader.db.close()
    shutil.copytree(source, tmp_path / "follower")
    leader.db = Database(source, read_only=True, checkpoint_wal_bytes=0)
    follower = Replica(tmp_path / "follower")
    nodes = _cluster(tmp_path, count=2)
    try:
        follower.attach_quorum(nodes[1])
        leader.promote_with_quorum(
            nodes[0],
            heartbeat_interval=0.05,
            lease_keeper_auto=False,
        )
        leader.execute_one("INSERT INTO t VALUES (1, 'one')")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)],
            timeout=5.0,
        ), "重启前没有追平"
        # 模拟 leader 重启：Replica.close() 会把本地 WAL 进度写回 replica.lsn，
        # 再以只读句柄重新打开、重新竞选提升（直接 leader.db.close() 会绕过
        # _adopt_local_lsn，重启后会把落后进度拿去竞选）。
        leader.close()
        leader.db = Database(source, read_only=True, checkpoint_wal_bytes=0)
        leader.promote_with_quorum(
            nodes[0],
            heartbeat_interval=0.05,
            lease_keeper_auto=False,
        )
        for i in range(2, 12):
            leader.execute_one(f"INSERT INTO t VALUES ({i}, 'v{i}')")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(11,)],
            timeout=10.0,
        ), "leader 重启后心跳推送没有恢复"
    finally:
        _stop(nodes)
        leader.db.close()
        follower.db.close()
        primary.close()
