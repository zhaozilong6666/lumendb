"""同步复制（quorum-ack commit）的回归测试。

语义：本地 COMMIT 记录 fsync 之后，还要等"多数派已持有该 LSN"的水位追平，
才把成功报给客户端。超时抛 `CommitOutcomeUnknown`（SQLSTATE `40003`），
**不回滚**——数据已经本地持久化，把它当成"没写进去"才是真正的错误。

覆盖点：

1. 屏障本身：水位已达标直接返回；没达标超时抛 40003；
2. 与 `TransactionManager.commit` 的接线：写语句超时后**数据仍在**（outcome
   unknown 的语义），读语句不会被屏障拖住；
3. `promote_with_quorum(sync_replication=True)` 必须配心跳循环，否则直接拒绝；
4. 端到端：三节点 + 心跳循环，多数派追上后写成功；多数派掉队时写超时。
"""

from __future__ import annotations

import threading
import time

import pytest

from lumendb.engine.database import Database
from lumendb.errors import CommitOutcomeUnknown, NotSupportedError
from lumendb.replication import QuorumCommitBarrier, Replica, WALServer
from lumendb.replication.quorum import QuorumNode


class _FakeNode:
    """只有水位的最小节点桩，用来单独测屏障。"""

    def __init__(self, commit_lsn: int = 0) -> None:
        self.quorum_commit_lsn = commit_lsn


class TestBarrier:
    def test_returns_immediately_when_already_committed(self):
        barrier = QuorumCommitBarrier(_FakeNode(500), timeout=0.5)
        barrier(400)  # 水位已经超过目标，不等待
        barrier(500)
        assert barrier.timeouts == 0

    def test_raises_when_watermark_never_catches_up(self):
        barrier = QuorumCommitBarrier(_FakeNode(10), timeout=0.05, poll_interval=0.005)
        with pytest.raises(CommitOutcomeUnknown) as exc:
            barrier(100)
        assert exc.value.sqlstate == "40003"
        assert "commit_lsn=100" in (exc.value.detail or "")
        assert barrier.timeouts == 1

    def test_waits_until_the_watermark_advances(self):
        node = _FakeNode(0)
        barrier = QuorumCommitBarrier(node, timeout=2.0, poll_interval=0.005)

        def advance():
            time.sleep(0.05)
            node.quorum_commit_lsn = 200

        thread = threading.Thread(target=advance)
        thread.start()
        try:
            started = time.monotonic()
            barrier(150)
            assert time.monotonic() - started >= 0.04
            assert barrier.last_observed >= 150
        finally:
            thread.join()


class TestCommitIntegration:
    def _db(self, tmp_path, node):
        """建库 -> 建表 -> 再装屏障。

        建表本身也要提交，如果先装屏障，setup 就会因为水位为 0 而超时。
        """
        db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.set_commit_barrier(
            QuorumCommitBarrier(node, timeout=0.05, poll_interval=0.005)
        )
        return db

    def test_write_times_out_but_stays_committed(self, tmp_path):
        """outcome unknown：客户端拿到 40003，但数据确实写进去了。"""
        node = _FakeNode(0)
        db = self._db(tmp_path, node)
        try:
            with pytest.raises(CommitOutcomeUnknown):
                db.execute_one("INSERT INTO t VALUES (1)")
            assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            db.close()

    def test_write_succeeds_once_the_majority_catches_up(self, tmp_path):
        node = _FakeNode(0)
        db = self._db(tmp_path, node)
        try:
            with pytest.raises(CommitOutcomeUnknown):
                db.execute_one("INSERT INTO t VALUES (1)")
            node.quorum_commit_lsn = 10**9
            db.execute_one("INSERT INTO t VALUES (2)")
            assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
        finally:
            db.close()

    def test_read_only_statements_are_not_blocked(self, tmp_path):
        node = _FakeNode(0)  # 水位永远是 0
        db = self._db(tmp_path, node)
        try:
            started = time.monotonic()
            for _ in range(5):
                db.execute_one("SELECT COUNT(*) FROM t")
            assert time.monotonic() - started < 0.5, "只读事务不该等多数派水位"
        finally:
            db.close()


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


class TestReplicaSyncReplication:
    def test_sync_replication_requires_a_heartbeat_loop(self, tmp_path):
        nodes = _cluster(tmp_path)
        replica = Replica(tmp_path / "r", applied_lsn=0)
        try:
            with pytest.raises(NotSupportedError, match="心跳循环"):
                replica.promote_with_quorum(nodes[0], sync_replication=True)
        finally:
            replica.close()
            for node in nodes:
                node.stop()

    def test_end_to_end_commit_waits_for_the_majority(self, tmp_path):
        nodes = _cluster(tmp_path)
        replica = Replica(tmp_path / "r", applied_lsn=0)
        try:
            db = replica.promote_with_quorum(
                nodes[0],
                heartbeat_interval=0.05,
                sync_replication=True,
                sync_timeout=0.15,
            )
            # 多数派还没追上：DDL 也会超时，但**结果未知**——表其实已经建好。
            with pytest.raises(CommitOutcomeUnknown):
                db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
            assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]

            # 多数派追上之后（副本 attach 后会上报 match_lsn），写就能成功
            nodes[1].observe_applied(10**9)
            nodes[2].observe_applied(10**9)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline and nodes[0].quorum_commit_lsn == 0:
                time.sleep(0.02)
            assert nodes[0].quorum_commit_lsn > 0
            db.execute_one("INSERT INTO t VALUES (1)")
            assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
            assert replica.commit_barrier() is not None
        finally:
            replica.close()
            for node in nodes:
                node.stop()
