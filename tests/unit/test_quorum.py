"""多数派投票与提升 fence 的集成测试。"""

from __future__ import annotations

import pytest

from lumendb.errors import NotSupportedError
from lumendb.engine.database import Database
from lumendb.replication import PromotionFence, Replica
from lumendb.replication.quorum import QuorumNode, VoteState


def _make_primary(tmp_path):
    return Database(tmp_path / "primary", checkpoint_wal_bytes=0)


def _cluster(tmp_path, count: int = 3):
    nodes = []
    for i in range(count):
        node = QuorumNode(
            f"n{i}",
            state_path=tmp_path / f"n{i}.vote.json",
            timeout=0.5,
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
    nodes = _cluster(tmp_path)
    try:
        nodes[1].stop()
        nodes[2].stop()
        with pytest.raises(NotSupportedError, match="多数派投票失败"):
            nodes[0].campaign(last_lsn=10)
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
