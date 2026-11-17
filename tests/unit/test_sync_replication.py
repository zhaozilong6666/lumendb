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

import shutil
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

    def test_sync_commit_waits_for_a_pushed_follower(self, tmp_path):
        """2 节点：COMMIT 只有等心跳循环把 append_entries 推到真实副本并计入
        多数派水位后才返回——这是同步复制走推送路径的端到端用例。"""
        primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        leader = Replica.bootstrap(primary, tmp_path / "leader")
        source = tmp_path / "leader"
        leader.db.close()
        shutil.copytree(source, tmp_path / "follower")
        leader.db = Database(source, read_only=True, checkpoint_wal_bytes=0)
        follower = Replica(tmp_path / "follower")
        nodes = _cluster(tmp_path, count=2)
        try:
            follower.attach_quorum(nodes[1])
            db = leader.promote_with_quorum(
                nodes[0],
                heartbeat_interval=0.05,
                lease_keeper_auto=False,
                sync_replication=True,
                sync_timeout=5.0,
            )
            db.execute_one("INSERT INTO t VALUES (1)")
            # 返回时多数派水位已覆盖 COMMIT，副本必然已经重放这批记录。
            assert follower.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            leader.close()
            follower.close()
            primary.close()
            for node in nodes:
                node.stop()


class TestWatermarkProvenance:
    """提交水位有两个来源，屏障只能信"多数派自证"的那个。

    follower 会把 leader 心跳/append_entries 里抄来的水位记进 `commit_lsn`，
    这对竞选闸门是对的（"我还落后多少"）。但一个抄来高水位的节点如果被手动
    提升成主库，`commit_lsn` 仍然很高；屏障若直接信它，就会把"根本没复制
    出去"的写入当成已提交——同步复制静默退化成异步。
    """

    def test_hint_does_not_satisfy_the_barrier(self):
        node = _FakeNode(0)
        node.verified_commit_lsn = 0
        node.quorum_commit_lsn = 10_000  # 从 leader 抄来的高水位
        barrier = QuorumCommitBarrier(node, timeout=0.05, poll_interval=0.005)
        with pytest.raises(CommitOutcomeUnknown):
            barrier(600)

    def test_verified_watermark_satisfies_the_barrier(self):
        node = _FakeNode(0)
        node.quorum_commit_lsn = 10_000
        node.verified_commit_lsn = 800
        barrier = QuorumCommitBarrier(node, timeout=0.05, poll_interval=0.005)
        barrier(600)  # 多数派自证到 800，600 可以放行
        assert barrier.timeouts == 0

    def test_node_separates_hint_from_verified(self, tmp_path):
        from lumendb.replication.quorum import QuorumNode

        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json")
        try:
            node.observe_commit(1000)  # leader 口头告知
            assert node.quorum_commit_lsn == 1000
            assert node.verified_commit_lsn == 0
            node.observe_commit(700, verified=True)  # 心跳按多数派算出
            assert node.quorum_commit_lsn == 1000  # 不后退
            assert node.verified_commit_lsn == 700
        finally:
            node.stop()

    def test_append_entries_hint_is_not_verified(self, tmp_path):
        from lumendb.replication.quorum import QuorumNode

        node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json")
        try:
            # 空批次 + 声称提交到 500：只能算"听说"，不能自证
            node.append_entries(1, "leader", 0, 0, [], commit_lsn=500)
            assert node.quorum_commit_lsn == 500
            assert node.verified_commit_lsn == 0
        finally:
            node.stop()
