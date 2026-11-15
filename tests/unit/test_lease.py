"""写租约的单元测试与 quorum 集成。"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError, StorageError
from lumendb.replication import Lease, LeaseKeeper, Replica, WALServer
from lumendb.replication.quorum import QuorumNode


def _cluster(tmp_path: Path) -> list[QuorumNode]:
    nodes = [
        QuorumNode(f"n{i}", state_path=tmp_path / f"n{i}.vote.json", timeout=0.5).start()
        for i in range(3)
    ]
    peers = {node.node_id: (node.host, node.port) for node in nodes}
    for node in nodes:
        node.peers = {
            peer_id: address
            for peer_id, address in peers.items()
            if peer_id != node.node_id
        }
    return nodes


def _stop(nodes: list[QuorumNode]) -> None:
    for node in nodes:
        node.stop()


def test_lease_acquire_renew_and_expire(tmp_path):
    lease = Lease(tmp_path / "lease.json", "primary", ttl=10)
    assert lease.read() is None
    expires = lease.acquire(term=1, now=100)
    assert expires == 110
    lease.assert_writable(now=101)
    assert lease.renew(now=105) == 115
    lease.assert_writable(now=114)
    with pytest.raises(NotSupportedError, match="已经过期"):
        lease.assert_writable(now=115)


def test_lease_rejects_other_holder_and_term_regression(tmp_path):
    first = Lease(tmp_path / "lease.json", "n0", ttl=10)
    second = Lease(tmp_path / "lease.json", "n1", ttl=10)
    first.acquire(term=2, now=100)
    with pytest.raises(NotSupportedError, match="仍被其他节点持有"):
        second.acquire(term=3, now=105)
    second.acquire(term=3, now=110)
    assert second.read()["holder"] == "n1"
    (tmp_path / "lease.json").write_text(
        json.dumps({"holder": "n1", "term": 3, "expires_at": 100}),
        encoding="utf-8",
    )
    with pytest.raises(NotSupportedError, match="任期"):
        first.acquire(term=1, now=111)


def test_lease_renew_requires_current_holder_and_unexpired(tmp_path):
    first = Lease(tmp_path / "lease.json", "n0", ttl=10)
    second = Lease(tmp_path / "lease.json", "n1", ttl=10)
    with pytest.raises(NotSupportedError, match="不存在"):
        first.renew(now=0)
    first.acquire(term=1, now=0)
    with pytest.raises(NotSupportedError, match="不属于当前节点"):
        second.renew(now=1)
    with pytest.raises(NotSupportedError, match="已经过期"):
        first.renew(now=10)
    first.acquire(term=2, now=10)
    assert first.renew(now=11) == 21


def test_lease_corrupt_state_is_not_silently_reset(tmp_path):
    path = tmp_path / "lease.json"
    path.write_text("{not-json", encoding="utf-8")
    with pytest.raises(StorageError, match="损坏"):
        Lease(path, "n0").read()


def test_quorum_campaign_writes_lease_and_fence(tmp_path):
    nodes = _cluster(tmp_path)
    try:
        lease_path = tmp_path / "lease.json"
        fence_path = tmp_path / "fence.json"
        term = nodes[0].campaign(
            last_lsn=42,
            fence_path=fence_path,
            lease_path=lease_path,
            lease_ttl=5,
        )
        assert term == 1
        marker = json.loads(lease_path.read_text(encoding="utf-8"))
        assert marker["holder"] == "n0"
        assert marker["term"] == 1
        assert json.loads(fence_path.read_text(encoding="utf-8"))["promoted_node"] == "n0"
    finally:
        _stop(nodes)


def test_replica_promote_with_quorum_holds_lease(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    replica = None
    nodes = _cluster(tmp_path)
    try:
        replica = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "replica"
        )
        lease_path = tmp_path / "lease.json"
        replica.promote_with_quorum(
            nodes[0],
            lease_path=lease_path,
            lease_ttl=30,
        )
        marker = json.loads(lease_path.read_text(encoding="utf-8"))
        assert marker["holder"] == "n0"
        assert marker["term"] == 1
    finally:
        _stop(nodes)
        if replica is not None:
            replica.db.close()
        server.stop()
        primary.close()

def test_lease_lock_times_out_when_lock_file_exists(tmp_path):
    lease = Lease(tmp_path / "lease.json", "n0", ttl=10, lock_timeout=0.05)
    lease.lock_path.write_text("stale", encoding="utf-8")
    with pytest.raises(StorageError, match="锁"):
        lease.acquire(term=1, now=0)


def test_lease_keeper_renews_until_stopped(tmp_path):
    lease = Lease(tmp_path / "lease.json", "n0", ttl=0.4)
    lease.acquire(term=1)
    first = lease.read()["expires_at"]
    keeper = LeaseKeeper(lease, interval=0.05).start()
    try:
        time.sleep(0.15)
        renewed = lease.read()["expires_at"]
        assert renewed > first
        assert keeper.last_error is None
    finally:
        keeper.stop()


def test_lease_keeper_uses_renew_callback(tmp_path):
    lease = Lease(tmp_path / "lease.json", "n0", ttl=0.4)
    lease.acquire(term=1)
    calls = []

    def renew():
        calls.append(1)
        return lease.renew()

    keeper = LeaseKeeper(lease, interval=0.05, renew=renew).start()
    try:
        time.sleep(0.12)
        assert calls
        assert keeper.last_error is None
    finally:
        keeper.stop()


def test_lease_keeper_calls_on_failure(tmp_path):
    lease = Lease(tmp_path / "lease.json", "n0", ttl=0.4)
    lease.acquire(term=1)
    failures = []

    def renew():
        raise NotSupportedError("quorum lost")

    keeper = LeaseKeeper(
        lease,
        interval=0.05,
        renew=renew,
        on_failure=lambda item: failures.append(item.last_error),
    ).start()
    try:
        time.sleep(0.12)
        assert failures
        assert keeper.lost is True
    finally:
        keeper.stop()

class _NoopQuorum:
    node_id = "n0"

    def campaign(self, last_lsn, *, fence_path=None, lease_path=None, lease_ttl=10.0):
        if lease_path is not None:
            Lease(lease_path, self.node_id, ttl=lease_ttl).acquire(1)
        return 1

    def renew_lease(self, last_lsn, *, lease_path, lease_ttl=10.0, now=None):
        return Lease(lease_path, self.node_id, ttl=lease_ttl).renew(now=now)


def test_promote_with_quorum_starts_lease_keeper(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    replica = Replica.bootstrap(primary, tmp_path / "replica")
    lease_path = tmp_path / "lease.json"
    try:
        replica.promote_with_quorum(
            _NoopQuorum(),
            lease_path=lease_path,
            lease_ttl=0.4,
            lease_keeper_interval=0.05,
            lease_renew_with_quorum=True,
        )
        first = Lease(lease_path, "n0", ttl=0.4).read()["expires_at"]
        time.sleep(0.15)
        renewed = Lease(lease_path, "n0", ttl=0.4).read()["expires_at"]
        assert renewed > first
    finally:
        replica.close()
        primary.close()


def test_promoted_primary_enforces_lease_write_fence(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    replica = Replica.bootstrap(primary, tmp_path / "replica")
    lease_path = tmp_path / "lease.json"
    try:
        replica.promote_with_quorum(
            _NoopQuorum(),
            lease_path=lease_path,
            lease_ttl=30,
        )
        replica.execute_one("INSERT INTO t VALUES (1)")
        Lease(lease_path, "n0", ttl=30).release()
        with pytest.raises(NotSupportedError, match="没有有效写租约"):
            replica.execute_one("INSERT INTO t VALUES (2)")
    finally:
        replica.close()
        primary.close()
