"""提升生命周期、状态机与心跳扇出的回归测试。

这一组补的是"功能已实现但没接线"的缺口：

* `promote_with_quorum` 只起 keeper 不起心跳 → 新主库的任期视图永远不动；
* 租约丢了以后 `ReplicationMonitor` 仍然报 `promoted`；
* `lease_path` 给了但没给 keeper 周期 → 主库在 TTL 后静默停写；
* `step_down()` 关库时不等在途写；
* `heartbeat_round` 串行扇出 → 一个慢 peer 拖慢整轮。
"""

from __future__ import annotations

import threading
import time

import pytest

from lumendb.engine.database import Database
from lumendb.services import ServiceRegistry
from lumendb.replication import (
    Lease,
    MonitorState,
    Replica,
    ReplicationMonitor,
    WALServer,
)
from lumendb.replication import quorum as quorum_mod
from lumendb.replication.quorum import QuorumNode


def _primary(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    return primary


def _single_node(tmp_path):
    node = QuorumNode("n0", state_path=tmp_path / "n0.vote.json", timeout=0.5).start()
    return node


class _NoopQuorum:
    """只要 campaign/renew 不抛错就行的替代品（单节点，无需网络）。"""

    node_id = "n0"

    def __init__(self) -> None:
        self.state = type("S", (), {"term": 1})()

    def campaign(self, last_lsn, **kwargs):
        return self.state.term

    def renew_lease(self, last_lsn, **kwargs):
        return time.time() + 30


def _replica_ready(tmp_path):
    primary = _primary(tmp_path)
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(server.host, server.port, tmp_path / "r")
    return primary, server, replica


class TestPromotionLifecycle:
    def test_lease_path_auto_starts_a_keeper(self, tmp_path):
        """给了 lease_path 却没给周期时必须自动起 keeper，否则 TTL 后静默停写。"""
        primary, server, replica = _replica_ready(tmp_path)
        try:
            replica.promote_with_quorum(
                _NoopQuorum(), lease_path=tmp_path / "lease.json", lease_ttl=30
            )
            assert replica._lease_keeper is not None, "keeper 没有自动启动"
        finally:
            replica.close()
            server.stop()
            primary.close()

    def test_auto_keeper_can_be_disabled_with_a_warning(self, tmp_path):
        primary, server, replica = _replica_ready(tmp_path)
        try:
            with pytest.warns(RuntimeWarning, match="没有启动续租线程"):
                replica.promote_with_quorum(
                    _NoopQuorum(),
                    lease_path=tmp_path / "lease.json",
                    lease_ttl=30,
                    lease_keeper_auto=False,
                )
            assert replica._lease_keeper is None
        finally:
            replica.close()
            server.stop()
            primary.close()

    def test_heartbeat_interval_starts_the_loop_and_close_stops_it(self, tmp_path):
        primary, server, replica = _replica_ready(tmp_path)
        node = _single_node(tmp_path)
        try:
            replica.promote_with_quorum(
                node,
                lease_path=tmp_path / "lease.json",
                heartbeat_interval=0.05,
            )
            assert replica._heartbeat is not None
            time.sleep(0.15)
            assert replica._heartbeat.last_observed_term >= 1
        finally:
            replica.close()
            assert replica._heartbeat is None
            node.stop()
            server.stop()
            primary.close()

    def test_step_down_stops_background_threads_before_closing(self, tmp_path):
        primary, server, replica = _replica_ready(tmp_path)
        node = _single_node(tmp_path)
        try:
            replica.promote_with_quorum(
                node,
                lease_path=tmp_path / "lease.json",
                heartbeat_interval=0.05,
            )
            old_db = replica.db
            replica.step_down("test")
            assert replica._heartbeat is None
            assert replica._lease_keeper is None
            assert old_db._closed, "降级后旧句柄必须已关闭"
            with pytest.raises(Exception):
                old_db.execute_one("INSERT INTO t VALUES (1)")
            # 新句柄是只读的
            assert replica.db.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
        finally:
            replica.close()
            node.stop()
            server.stop()
            primary.close()


class TestMonitorStateMachine:
    def test_poll_reports_promoted_then_needs_attention_after_step_down(self, tmp_path):
        primary, server, replica = _replica_ready(tmp_path)
        node = _single_node(tmp_path)
        monitor = ReplicationMonitor(
            replica,
            server.host,
            server.port,
            quorum=node,
            lease_path=tmp_path / "lease.json",
            lease_keeper_interval=0.05,
            heartbeat_interval=0.05,
            poll_interval=0.05,
        )
        try:
            assert monitor.poll() in ("healthy", "rebuilt")
            assert monitor.state is MonitorState.FOLLOWING
            assert monitor._promote() is True
            assert monitor.state is MonitorState.PROMOTED
            assert monitor.poll() == "promoted"

            replica.step_down("lease lost in test")
            assert monitor.state is MonitorState.NEEDS_ATTENTION
            assert monitor.poll() == "needs_attention"
            assert monitor.demotions == 1
            assert "stepped down" in (monitor.last_error or "")
        finally:
            monitor.stop()
            replica.close()
            node.stop()
            server.stop()
            primary.close()

    def test_promote_failure_marks_needs_attention(self, tmp_path):
        primary, server, replica = _replica_ready(tmp_path)

        class Failing:
            node_id = "n0"
            state = type("S", (), {"term": 1})()

            def campaign(self, last_lsn, **kwargs):
                from lumendb.errors import NotSupportedError

                raise NotSupportedError("没有拿到多数票")

        monitor = ReplicationMonitor(replica, server.host, server.port, quorum=Failing())
        try:
            assert monitor._promote() is False
            assert monitor.state is MonitorState.NEEDS_ATTENTION
            assert monitor.poll() == "needs_attention"
        finally:
            monitor.stop()
            replica.close()
            server.stop()
            primary.close()


class TestHeartbeatFanOut:
    def test_slow_peer_does_not_serialize_the_round(self, tmp_path, monkeypatch):
        """一个慢 peer 不能让整轮耗时变成 N × timeout。"""
        node = _single_node(tmp_path)
        node.peers = {"n0": (node.host, node.port), "slow": ("127.0.0.1", 1), "slow2": ("127.0.0.1", 2)}
        calls: list[str] = []

        def fake_send(host, port, term, leader_id, last_lsn, commit_lsn, last_term, timeout, token):
            calls.append(f"{host}:{port}")
            time.sleep(0.3)
            raise OSError("peer down")

        monkeypatch.setattr(quorum_mod, "_send_heartbeat", fake_send)
        try:
            started = time.monotonic()
            acks, errors = node.heartbeat_round(0)
            elapsed = time.monotonic() - started
            assert acks == 1  # 只有自己
            assert len(errors) == 2
            assert elapsed < 0.55, f"扇出仍然是串行的：{elapsed:.2f}s"
        finally:
            node.stop()

    def test_error_order_follows_peer_declaration(self, tmp_path, monkeypatch):
        node = _single_node(tmp_path)
        node.peers = {"b": ("127.0.0.1", 2), "a": ("127.0.0.1", 1)}

        def fake_send(host, port, *args, **kwargs):
            time.sleep(0.05 if port == 2 else 0.0)
            raise OSError(f"boom-{port}")

        monkeypatch.setattr(quorum_mod, "_send_heartbeat", fake_send)
        try:
            _, errors = node.heartbeat_round(0)
            assert errors[0].startswith("b:"), errors
            assert errors[1].startswith("a:"), errors
        finally:
            node.stop()


class TestStepDownWaitsForInflightWrites:
    def test_step_down_blocks_until_write_lock_is_free(self, tmp_path):
        """降级关库必须在 _write_lock 内，否则会和在途写抢文件句柄。"""
        path = tmp_path / "node"
        db = Database(path, checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")

        replica = Replica.__new__(Replica)  # 只测关闭路径，不需要完整副本
        replica.path = path
        replica._lock = threading.RLock()
        replica._lease_keeper = None
        replica._heartbeat = None
        replica._services = ServiceRegistry("test")
        replica._step_down_listeners = []
        replica.db = db

        holding = threading.Event()
        release = threading.Event()

        def writer():
            with db._write_lock:
                holding.set()
                release.wait(3)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            assert holding.wait(2)
            done = threading.Event()

            def do_step_down():
                replica.step_down("test")
                done.set()

            closer = threading.Thread(target=do_step_down)
            closer.start()
            assert not done.wait(0.2), "step_down 没有等在途写结束"
            release.set()
            assert done.wait(3), "释放写锁后 step_down 应该完成"
            closer.join(timeout=3)
            # 旧句柄已关闭，新句柄只读
            assert db._closed
            assert replica.db.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
            replica.db.close()
        finally:
            release.set()
            thread.join(timeout=3)
            db.close()

    def test_close_is_idempotent(self, tmp_path):
        db = Database(tmp_path / "db2", checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.close()
        db.close()  # step_down()/Replica.close() 会重复关闭，不能抛异常


def test_monitor_background_services_are_registry_managed(tmp_path):
    primary, server, replica = _replica_ready(tmp_path)
    node = _single_node(tmp_path)
    monitor = ReplicationMonitor(
        replica,
        server.host,
        server.port,
        quorum=node,
        election_timeout=0.2,
        poll_interval=0.05,
    )
    try:
        assert monitor.services_status()["running"] == []
        monitor.start()
        status = monitor.services_status()
        assert set(status["running"]) == {"poll", "election-watchdog"}
        assert status["healthy"] is True
    finally:
        monitor.stop()
        assert monitor.services_status()["running"] == []
        monitor.start()
        assert set(monitor.services_status()["running"]) == {
            "poll",
            "election-watchdog",
        }
        monitor.stop()
        assert monitor.services_status()["running"] == []
        assert all(
            not t.name.startswith("lumendb-replication-monitor")
            for t in threading.enumerate()
        )
        replica.close()
        node.stop()
        server.stop()
        primary.close()
