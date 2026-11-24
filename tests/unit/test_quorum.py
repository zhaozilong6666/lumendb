"""多数派投票与提升 fence 的集成测试。"""

from __future__ import annotations

import shutil
import socket
import time

import pytest

from lumendb.errors import NotSupportedError, StorageError
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


def test_monitor_exposes_push_rejections(tmp_path):
    """提升后 push 拒绝原因必须能从 monitor 读到（无人值守可观测性）。"""
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
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
            heartbeat_interval=0.05,
            lease_keeper_auto=False,
        )
        assert monitor.poll() == "promoted"
        assert monitor.last_push_rejections == {}
        reason = "日志分歧: lsn=1 本地任期 2，leader 任期 3"
        replica.last_push_rejections["n9"] = reason
        snapshot = monitor.last_push_rejections
        assert snapshot == {"n9": reason}
        # 返回的是快照，不是内部 dict 引用。
        replica.last_push_rejections["n9"] = "别的"
        assert snapshot["n9"] == reason
        assert monitor.last_push_rejections == {"n9": "别的"}
    finally:
        _stop(nodes)
        if replica is not None:
            replica.close()
        server.stop()
        primary.close()


def test_diverged_follower_rebuilds_from_leader_hint(tmp_path):
    """历史分叉的 follower 收到 leader 的重建端点后自动重新基准备份并追平。"""
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    replica = None
    follower = None
    rebuild_server = None
    monitor = None
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
        # 提升后的 leader 对外服务基准备份，供分叉 follower 自愈。
        rebuild_server = WALServer(replica.db).start()
        replica.rebuild_from = (rebuild_server.host, rebuild_server.port)
        # 给 follower 编一段假任期：同一 LSN 上它以为任期是 999，leader 报
        # 的是自己的任期 → 拼接点分叉 → 触发自动重建。
        nodes[1].state.record_term(follower.applied_lsn, 999)
        replica.execute_one("INSERT INTO t VALUES (1)")
        assert _wait_for(
            lambda: follower.last_rebuild is not None
            or follower.last_rebuild_error is not None,
            timeout=10.0,
        ), f"分叉 follower 没有完成自动重建 (error={follower.last_rebuild_error!r})"
        assert follower.last_rebuild is not None, follower.last_rebuild_error
        assert follower.last_rebuild_error is None
        assert follower.last_rebuild["prev_lsn"] >= 1
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)],
            timeout=5.0,
        ), "重建后 follower 没有看到 leader 的数据"
        # 重建后 append 链路应恢复：再写一条，follower 通过 append_entries 追上。
        replica.execute_one("INSERT INTO t VALUES (2)")
        assert _wait_for(
            lambda: follower.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)],
            timeout=5.0,
        ), "重建后 append_entries 链路没有恢复"
    finally:
        if monitor is not None:
            monitor.stop()
        if rebuild_server is not None:
            rebuild_server.stop()
        _stop(nodes)
        if replica is not None:
            replica.close()
        if follower is not None:
            follower.close()
        server.stop()
        primary.close()


def test_monitor_serves_replication_stream_after_promotion(tmp_path):
    """提升后的 monitor 自动起复制服务，新 follower 能直接从这里 bootstrap。"""
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    replica = None
    late_follower = None
    monitor = None
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
            heartbeat_interval=0.05,
            lease_keeper_auto=False,
            serve_on=("127.0.0.1", 0),
            token="secret",
        )
        assert monitor.poll() == "promoted"
        assert monitor.server is not None
        assert replica.rebuild_from == (monitor.server.host, monitor.server.port)
        assert replica.rebuild_token == "secret"
        replica.execute_one("INSERT INTO t VALUES (1)")
        late_follower = Replica.bootstrap_from_server(
            monitor.server.host,
            monitor.server.port,
            tmp_path / "late",
            token="secret",
        )
        assert late_follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        # token 不匹配必须被拒绝，重建自愈不会在错误凭据上反复重试成功。
        with pytest.raises(StorageError):
            Replica.bootstrap_from_server(
                monitor.server.host, monitor.server.port, tmp_path / "bad"
            )
    finally:
        if monitor is not None:
            monitor.stop()
        if late_follower is not None:
            late_follower.close()
        _stop(nodes)
        if replica is not None:
            replica.close()
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


def test_vote_state_term_segments_persist_and_query(tmp_path):
    path = tmp_path / "state.json"
    state = VoteState(path)
    assert state.term_at(50) is None
    state.record_term(100, 3)
    state.record_term(200, 3)  # 同任期延续：不新增分段
    state.record_term(300, 5)
    state.record_term(250, 4)  # LSN 不前进：忽略
    assert state.log_terms == [[100, 3], [300, 5]]
    assert state.term_at(99) is None
    assert state.term_at(100) == 3
    assert state.term_at(299) == 3
    assert state.term_at(300) == 5
    assert state.term_at(9999) == 5
    reloaded = VoteState(path)
    assert reloaded.log_terms == [[100, 3], [300, 5]]
    assert reloaded.term_at(250) == 3


def test_append_entries_records_exact_terms_from_entry_terms(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        nodes[0].set_log_applier(lambda records: len(records))
        first = LogRecord(1, 7, LogType.BEGIN, b"")
        second = LogRecord(first.lsn + len(first.encode()), 7, LogType.COMMIT, b"")
        response = nodes[0].append_entries(
            5,
            "leader",
            0,
            0,
            [_record_to_json(first), _record_to_json(second)],
            commit_lsn=second.lsn,
            entry_terms=[5, 5],
        )
        assert response["ok"] is True
        assert nodes[0].state.term_at(first.lsn) == 5
        assert nodes[0].state.term_at(second.lsn) == 5

        # entry_terms 长度不一致必须被拒绝。
        bad = nodes[0].append_entries(
            5,
            "leader",
            second.lsn,
            5,
            [_record_to_json(LogRecord(101, 7, LogType.BEGIN, b""))],
            entry_terms=[5, 5],
        )
        assert bad["ok"] is False
        assert "entry_terms" in bad["reason"]
    finally:
        _stop(nodes)


def test_append_entries_detects_divergent_history_at_the_join_point(tmp_path):
    """同一 LSN、不同任期 = 两条历史分叉：拒绝重放并要求重建。"""
    nodes = _cluster(tmp_path)
    try:
        applied = []
        nodes[0].set_log_applier(lambda records: applied.extend(records) or len(records))
        nodes[0].state.observe_applied(100, term=3)
        first = LogRecord(101, 7, LogType.BEGIN, b"")
        second = LogRecord(first.lsn + len(first.encode()), 7, LogType.COMMIT, b"")
        ok = nodes[0].append_entries(
            3,
            "leader",
            100,
            3,
            [_record_to_json(first), _record_to_json(second)],
            commit_lsn=second.lsn,
            entry_terms=[3, 3],
        )
        assert ok["ok"] is True
        assert nodes[0].state.term_at(second.lsn) == 3

        # leader 声称同一 LSN 处任期是 9：拼接点分歧，绝不能重放。
        rejected = nodes[0].append_entries(
            4,
            "leader",
            second.lsn,
            9,
            [],
        )
        assert rejected["ok"] is False
        assert rejected["reason"].startswith("日志分歧")
        assert rejected.get("rebuild_required") is True
        assert len(applied) == 2, "分歧批次不得重放"
    finally:
        _stop(nodes)


def test_append_entries_gap_demands_rebuild(tmp_path):
    """重放时发现 WAL 缺口（记录不连续）= 追不平，同样要求重建而非死循环。"""
    node = QuorumNode(
        "n0",
        state_path=tmp_path / "n0.vote.json",
        timeout=1.0,
    ).start()
    try:
        node.state.observe_applied(100, term=3)

        def gap_applier(records):
            raise StorageError(
                "复制记录不连续，拒绝跳过缺失的 WAL",
                detail="expected_lsn=104 got_lsn=200",
            )

        node.set_log_applier(gap_applier)
        first = LogRecord(101, 7, LogType.BEGIN, b"")
        rejected = node.append_entries(
            3,
            "leader",
            100,
            3,
            [_record_to_json(first)],
        )
        assert rejected["ok"] is False
        assert rejected["rebuild_required"] is True
        assert rejected["reason"].startswith("日志缺口")

        # 其他异常（坏批次）仍然只是普通拒绝，不触发重建。
        def bad_applier(records):
            raise ValueError("坏批次")

        node.set_log_applier(bad_applier)
        rejected = node.append_entries(
            3,
            "leader",
            100,
            3,
            [_record_to_json(first)],
        )
        assert rejected["ok"] is False
        assert not rejected.get("rebuild_required")
        assert rejected["reason"].startswith("日志重放失败")
    finally:
        node.stop()


def test_election_watchdog_fires_when_leader_heartbeats_stop(tmp_path):
    """follower 心跳超时 → on_leader_lost 回调；从未收到心跳同样触发。"""
    node = QuorumNode(
        "n0",
        state_path=tmp_path / "n0.vote.json",
        timeout=1.0,
    ).start()
    try:
        fired = []

        node.election_timeout = 0.2
        node.on_leader_lost = lambda: fired.append(time.monotonic())
        node.start_election_watchdog()

        # 从未收到过心跳：集群可能还没有 leader，同样应超时触发。
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not fired:
            time.sleep(0.02)
        assert fired, "从未收到心跳时看门狗也必须触发"

        # 收到心跳会重置计时：停止回调后先喂一次心跳，短暂窗口内不触发。
        node.stop_election_watchdog()
        node.on_leader_lost = lambda: fired.append("second")
        node.election_timeout = 0.15
        node.start_election_watchdog()
        node.heartbeat(0, "leader", 0, commit_lsn=0)
        node.stop_election_watchdog()
        assert all(v != "second" for v in fired), "收到心跳后不应立即触发"
    finally:
        node.stop()


def test_append_entries_divergence_is_visible_over_the_wire(tmp_path):
    """RPC 路径同样携带 entry_terms，并能把 rebuild_required 送回 leader。"""
    nodes = _cluster(tmp_path, count=2)
    try:
        nodes[1].set_log_applier(lambda records: len(records))
        nodes[1].state.observe_applied(100, term=3)
        first = LogRecord(101, 7, LogType.BEGIN, b"")
        second = LogRecord(first.lsn + len(first.encode()), 7, LogType.COMMIT, b"")
        response = QuorumClient.append_entries(
            "127.0.0.1",
            nodes[1].port,
            3,
            "n0",
            100,
            3,
            [_record_to_json(first), _record_to_json(second)],
            commit_lsn=second.lsn,
            entry_terms=[3, 3],
            timeout=1.0,
        )
        assert response["ok"] is True
        assert nodes[1].state.term_at(second.lsn) == 3

        divergent = QuorumClient.append_entries(
            "127.0.0.1",
            nodes[1].port,
            4,
            "n0",
            second.lsn,
            9,
            [],
            timeout=1.0,
        )
        assert divergent["ok"] is False
        assert divergent.get("rebuild_required") is True
        assert "日志分歧" in divergent["reason"]
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


def test_heartbeat_loop_pushes_multiple_batches_until_caught_up():
    """落后超过一批的副本在同一轮心跳里连续推多批，而不是等下一轮。"""
    calls = []

    class FakeState:
        term = 1

    class FakeNode:
        timeout = 1.0

        def __init__(self):
            self.state = FakeState()
            self.peers = {}

        @property
        def quorum_commit_lsn(self):
            return 0

        def heartbeat_round(self, last_lsn, abort_on_higher_term=False):
            return 1, []

    node = FakeNode()
    remaining = [{"n1": 1000}, {"n1": 1000}, {"n1": 300}, {}]

    def push(_node):
        calls.append(1)
        result = remaining.pop(0)
        if not result:
            # 批次推完就停循环，避免下一轮把 last_pushed 重置成 {}。
            loop._stop.set()
        return result

    loop = HeartbeatLoop(
        node,
        lambda: 5000,
        push_entries=push,
        interval=0.05,
        max_batches_per_round=10,
    )
    loop.start()
    try:
        assert _wait_for(lambda: len(calls) >= 4, timeout=2.0), (
            f"多批推送没有跑完：calls={len(calls)}"
        )
        assert loop.last_pushed == {"n1": 2300}
    finally:
        loop.stop()


def test_heartbeat_loop_caps_batches_per_round():
    calls = []

    class FakeState:
        term = 1

    class FakeNode:
        timeout = 1.0

        def __init__(self):
            self.state = FakeState()
            self.peers = {}

        @property
        def quorum_commit_lsn(self):
            return 0

        def heartbeat_round(self, last_lsn, abort_on_higher_term=False):
            return 1, []

    def push(_node):
        calls.append(1)
        if len(calls) >= 2:
            # 达到封顶后停循环，观察最后一轮累计值。
            loop._stop.set()
        return {"n1": 1000}

    loop = HeartbeatLoop(
        FakeNode(),
        lambda: 5000,
        push_entries=push,
        interval=0.05,
        max_batches_per_round=2,
    )
    loop.start()
    try:
        assert _wait_for(lambda: len(calls) >= 2, timeout=2.0)
        assert len(calls) == 2, "每轮推送批次必须受 max_batches_per_round 封顶"
        assert loop.last_pushed == {"n1": 2000}
    finally:
        loop.stop()


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


def test_quorum_server_not_blocked_by_a_stalled_connection():
    """一个只连不发的停滞连接不得挡住其他节点的投票/心跳。"""
    node = QuorumNode("a", timeout=2.0).start()
    stall = None
    try:
        stall = socket.create_connection((node.host, node.port), timeout=2.0)
        started = time.monotonic()
        ack = QuorumClient.heartbeat(
            "127.0.0.1", node.port, 1, "a", 0, timeout=1.0
        )
        elapsed = time.monotonic() - started
        assert ack.get("ok") is True, ack
        assert elapsed < 0.8, f"心跳被停滞连接拖了 {elapsed:.2f}s"
    finally:
        if stall is not None:
            stall.close()
        node.stop()


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
            # Windows CI 上重放这批记录偶发超过默认 2s（run 278 的 TimeoutError）；
            # 这里只是测试的传输等待，放大到 5s，避免把慢盘抖动记成代码失败。
            timeout=5.0,
        )
        assert response["ok"] is True
        assert response["match_lsn"] == records[-1].lsn
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
    finally:
        node.stop()
        replica.db.close()
        primary.close()


def test_campaign_absorbs_voter_progress_into_last_matches(tmp_path):
    """投票响应携带投票者进度：新 leader 首轮推批不必再等一轮心跳。"""
    nodes = _cluster(tmp_path)
    try:
        # n1 有真实进度；n2 是空节点（last_lsn=0，不影响吸收）。
        nodes[1].observe_applied(50, term=1)
        nodes[0].observe_applied(100, term=1)
        term = nodes[0].campaign(100)
        assert term >= 1
        # 三节点集群、候选自己 100 >= 所有投票者进度，全票通过。
        assert nodes[0].last_matches.get("n1") == 50
        assert nodes[0].last_match_terms.get("n1") == 1
        # 空节点报 0：不写进 last_matches，避免把候选的拼接点拉回 0。
        assert "n2" not in nodes[0].last_matches or nodes[0].last_matches["n2"] == 0
    finally:
        _stop(nodes)


def test_campaign_immediately_verifies_majority_commit_index(tmp_path):
    """当选即算多数派水位：新 leader 的 verified_commit_lsn 立刻可用。

    修复前 verified_commit_lsn 只由 heartbeat_round 推进，新 leader 的第一
    笔同步复制写要空等一个心跳周期；当选时用本轮赞成票进度算一次，屏障就能
    立刻有"多数派自证"的水位。
    """
    nodes = _cluster(tmp_path)
    try:
        for node in nodes:
            node.observe_applied(100, term=1)
        term = nodes[0].campaign(100)
        assert term >= 1
        # 三节点：自己 100 + 两票 100 → 立即自证到 100。
        assert nodes[0].verified_commit_lsn == 100
        assert nodes[0].quorum_commit_lsn == 100
    finally:
        _stop(nodes)


def test_campaign_commit_index_takes_the_majority_median_not_the_max(tmp_path):
    """投票者落后时，当选水位取第 majority 大值，不能把只有自己持有的当多数派。"""
    nodes = _cluster(tmp_path)
    try:
        nodes[1].observe_applied(40, term=1)
        nodes[2].observe_applied(40, term=1)
        nodes[0].observe_applied(100, term=1)
        term = nodes[0].campaign(100)
        assert term >= 1
        # lsns=[100,40,40] → 第 2 大 = 40。
        assert nodes[0].verified_commit_lsn == 40
    finally:
        _stop(nodes)


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


def test_push_backoff_skips_peer_without_applier_and_recovers(tmp_path):
    """持续拒绝的 peer 退避；修复（装上 applier）后成功路径立即恢复。

    无 applier 的 peer 是配置错误——每轮心跳空推 1000 条纯属浪费，
    ``_push_backoff`` 在退避期内跳过它；拒绝原因也进入
    ``last_push_rejections`` 供监控读取。
    """
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    leader = Replica.bootstrap(primary, tmp_path / "leader")
    follower = Replica.bootstrap(primary, tmp_path / "follower")
    nodes = _cluster(tmp_path, count=2)
    try:
        leader.promote()
        leader.attach_quorum(nodes[0])
        leader.execute_one("INSERT INTO t VALUES (1, 'one')")
        # follower 不 attach：peer n1 没有日志重放函数，推批会被拒绝。
        nodes[0].heartbeat_round(leader.db.wal.last_lsn)

        pushed = leader.push_entries_to_peers(nodes[0])
        assert pushed == {}
        assert "本节点没有绑定日志重放函数" in leader.last_push_rejections.get(
            "n1", ""
        )
        # 退避期内再推：直接跳过，不重复空推。
        assert "n1" in leader._push_backoff
        pushed = leader.push_entries_to_peers(nodes[0])
        assert pushed == {}
        # 修复：给 follower 装上 applier，并把退避时刻拨回过去。
        # attach_quorum 会把 follower 本地 applied 立即报到自己的
        # match_lsn，leader 需要再收一轮心跳刷新 last_matches。
        follower.attach_quorum(nodes[1])
        leader._push_backoff["n1"] = 0
        # Windows CI 慢盘上 follower apply 一批可能超过默认 1s；这里验的是
        # “修复后退避能恢复”，不是传输延迟，给客户端留足超时。
        nodes[0].timeout = 5.0
        nodes[0].heartbeat_round(leader.db.wal.last_lsn)
        pushed = leader.push_entries_to_peers(nodes[0])
        assert pushed.get("n1", 0) > 0
        assert follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        assert "n1" not in leader._push_backoff
        assert "n1" not in leader.last_push_rejections
    finally:
        _stop(nodes)
        leader.db.close()
        follower.db.close()
        primary.close()


def test_push_entries_transport_timeout_backs_off_instead_of_raising(tmp_path, monkeypatch):
    """单个 peer 超时只记拒绝并退避，不能把整轮推送和调用方一起拖垮。"""
    primary = _make_primary(tmp_path)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    leader = Replica.bootstrap(primary, tmp_path / "leader")
    nodes = _cluster(tmp_path, count=2)
    try:
        leader.promote()
        leader.attach_quorum(nodes[0])
        leader.execute_one("INSERT INTO t VALUES (1, 'one')")
        nodes[0].heartbeat_round(leader.db.wal.last_lsn)

        def timeout(*_args, **_kwargs):
            raise TimeoutError("timed out")

        monkeypatch.setattr(QuorumClient, "append_entries", timeout)
        pushed = leader.push_entries_to_peers(nodes[0])
        assert pushed == {}
        assert "传输失败" in leader.last_push_rejections.get("n1", "")
        assert "n1" in leader._push_backoff
    finally:
        _stop(nodes)
        leader.db.close()
        primary.close()
