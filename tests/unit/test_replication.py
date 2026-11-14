"""只读副本的物理 WAL 应用测试。"""

from __future__ import annotations

import threading

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError, RedirectError, StorageError, TransactionError
from lumendb.replication import (
    PromotionFence,
    Replica,
    ReplicationMonitor,
    WALClient,
    WALServer,
)
from lumendb.storage.pager import DiskManager
from lumendb.storage.page import PageType


def make_primary(tmp_path):
    return Database(tmp_path / "primary", checkpoint_wal_bytes=0)


class TestReplica:
    def test_disk_manager_ensure_page_extends_with_free_pages(self, tmp_path):
        dm = DiskManager(tmp_path / "lumendb.data")
        try:
            dm.ensure_page(4)
            assert dm.num_pages == 5
            assert dm.meta.next_page_id >= 5
            page = dm.read_page(4)
            assert page[0] == 0  # PageType.FREE
        finally:
            dm.close()

    def test_bootstrap_apply_and_read_only(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        primary.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

        replica = Replica.bootstrap(primary, tmp_path / "replica")
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
        with pytest.raises(NotSupportedError, match="只读副本拒绝写入"):
            replica.execute_one("INSERT INTO t VALUES (3, 'c')")

        primary.execute_one("INSERT INTO t VALUES (3, 'c')")
        assert replica.lag_lsn(primary) > 0
        applied = replica.apply_from_primary(primary)
        assert applied > 0
        assert replica.lag_lsn(primary) == 0
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(3,)]
        primary.close()
        replica.db.close()

    def test_apply_new_pages_after_checkpoint(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        for i in range(100):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'before-{i}')")
        replica = Replica.bootstrap(primary, tmp_path / "replica")

        for i in range(100, 220):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'after-{i}')")
        replica.apply_from_primary(primary)
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(220,)]
        primary.close()
        replica.db.close()

    def test_apply_new_pages_without_primary_page_fallback(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        replica = Replica.bootstrap(primary, tmp_path / "replica")
        for i in range(120):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'v{i}')")
        records = list(primary.wal.iter_records(replica.applied_lsn + 1))
        assert replica.apply(records, primary=None) > 0
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(120,)]
        primary.close()
        replica.db.close()

    def test_replication_stream_requires_token(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        server = WALServer(primary, token="s3cret").start()
        replica = None
        try:
            with pytest.raises(StorageError, match="认证"):
                WALClient(server.host, server.port).status()
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica", token="s3cret"
            )
            primary.execute_one("INSERT INTO t VALUES (1)")
            assert (
                replica.replicate_from_server(
                    server.host,
                    server.port,
                    token="s3cret",
                )
                > 0
            )
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            if replica is not None:
                replica.db.close()
            server.stop()
            primary.close()

    def test_redirect_points_client_to_new_primary(self, tmp_path):
        primary_one = make_primary(tmp_path)
        primary_two = Database(tmp_path / "primary2", checkpoint_wal_bytes=0)
        primary_two.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        primary_two.execute_one("INSERT INTO t VALUES (1)")
        target = WALServer(primary_two).start()
        redirect = WALServer(
            primary_one, redirect_to=(target.host, target.port)
        ).start()
        try:
            with pytest.raises(RedirectError) as caught:
                WALClient(redirect.host, redirect.port).status()
            assert (caught.value.host, caught.value.port) == (
                target.host,
                target.port,
            )
            status = WALClient(target.host, target.port).status()
            assert status["current_lsn"] > 0
        finally:
            redirect.stop()
            target.stop()
            primary_one.close()
            primary_two.close()

    def test_backup_stream_auto_acks_new_slot(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        primary.execute_one("INSERT INTO t VALUES (1)")
        slots = tmp_path / "slots"
        server = WALServer(primary, slot_dir=slots).start()
        try:
            from lumendb.replication.wal_stream import WALClient

            base = WALClient(
                server.host, server.port, replica_id="new-replica"
            ).backup_to(tmp_path / "base.data")
            assert (slots / "new-replica.confirmed").exists()
            assert primary.wal.retain_lsn == base
        finally:
            server.stop()
            primary.close()

    def test_incremental_rebase_after_wal_gap(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        for i in range(100):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        server = WALServer(primary, slot_dir=tmp_path / "slots").start()
        replica = None
        try:
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica", replica_id="r1"
            )
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(100,)]
            base = replica.applied_lsn
            for i in range(100, 120):
                primary.execute_one(f"INSERT INTO t VALUES ({i})")
            replica.replicate_from_server(server.host, server.port, replica_id="r1")
            for i in range(120, 140):
                primary.execute_one(f"INSERT INTO t VALUES ({i})")
            checkpoint_lsn = replica.rebase_from_server(
                server.host, server.port, replica_id="r1"
            )
            assert checkpoint_lsn > base
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(140,)]
            for i in range(140, 150):
                primary.execute_one(f"INSERT INTO t VALUES ({i})")
            replica.replicate_from_server(server.host, server.port, replica_id="r1")
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(150,)]
        finally:
            if replica is not None:
                replica.db.close()
            server.stop()
            primary.close()

    def test_drop_replica_slot_recomputes_retain(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        primary.execute_one("INSERT INTO t VALUES (1)")
        slots = tmp_path / "slots"
        server = WALServer(primary, slot_dir=slots).start()
        try:
            from lumendb.replication.wal_stream import WALClient

            first = WALClient(
                server.host, server.port, replica_id="first"
            ).backup_to(tmp_path / "first.data")
            second = WALClient(
                server.host, server.port, replica_id="second"
            ).backup_to(tmp_path / "second.data")
            assert server.list_slots() == {"first": first, "second": second}
            assert primary.wal.retain_lsn == min(first, second)
            assert server.drop_slot("first") is True
            assert primary.wal.retain_lsn == second
            assert not (slots / "first.confirmed").exists()
            assert server.drop_slot("second") is True
            assert primary.wal.retain_lsn == 0
        finally:
            server.stop()
            primary.close()

    def test_multiple_replica_slots_retain_minimum(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        slots = tmp_path / "slots"
        server = WALServer(primary, slot_dir=slots).start()
        r1 = r2 = None
        try:
            r1 = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "r1", replica_id="r1"
            )
            r2 = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "r2", replica_id="r2"
            )
            for i in range(20):
                primary.execute_one(f"INSERT INTO t VALUES ({i})")
            r1.replicate_from_server(server.host, server.port, replica_id="r1")
            r2.replicate_from_server(
                server.host, server.port, replica_id="r2", max_records=1000
            )
            assert r1.execute_one("SELECT COUNT(*) FROM t").rows == [(20,)]
            assert r2.execute_one("SELECT COUNT(*) FROM t").rows == [(20,)]
            assert primary.wal.retain_lsn == min(
                r1.applied_lsn, r2.applied_lsn
            )
            assert (slots / "r1.confirmed").exists()
            assert (slots / "r2.confirmed").exists()
            remote_slots = WALClient(server.host, server.port).slots()
            assert remote_slots == {"r1": r1.applied_lsn, "r2": r2.applied_lsn}
        finally:
            for replica in (r1, r2):
                if replica is not None:
                    replica.db.close()
            server.stop()
            primary.close()

        reopened = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
        try:
            server = WALServer(reopened, slot_dir=slots)
            try:
                assert set(server._replica_slots) == {"r1", "r2"}
                assert reopened.wal.retain_lsn == min(server._replica_slots.values())
            finally:
                server.stop()
        finally:
            reopened.close()

    def test_compressed_stream_roundtrip(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        for i in range(50):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'row-{i}')")
        server = WALServer(primary).start()
        replica = None
        try:
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica", compress=True
            )
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(50,)]
            primary.execute_one("INSERT INTO t VALUES (50, 'net')")
            assert (
                replica.replicate_from_server(
                    server.host, server.port, compress=True
                )
                > 0
            )
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(51,)]
        finally:
            if replica is not None:
                replica.db.close()
            server.stop()
            primary.close()

    def test_tcp_backup_and_wal_stream_apply(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        server = WALServer(primary).start()
        replica = None
        try:
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica"
            )
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
            primary.execute_one("INSERT INTO t VALUES (1, 'net')")
            status = WALClient(server.host, server.port).status()
            assert status["current_lsn"] > 0
            assert status["confirmed_lsn"] == 0
            assert replica.lag_from_server(server.host, server.port) > 0
            assert replica.replicate_from_server(server.host, server.port) > 0
            assert replica.lag_from_server(server.host, server.port) == 0
            assert WALClient(server.host, server.port).status()["confirmed_lsn"] == replica.applied_lsn
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            if replica is not None:
                replica.db.close()
            server.stop()
            primary.close()

    def test_ddl_after_base_is_replayed_through_catalog_page(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE base (id INT PRIMARY KEY)")
        replica = Replica.bootstrap(primary, tmp_path / "replica")
        primary.execute_one("CREATE TABLE fresh (id INT PRIMARY KEY, name TEXT)")
        primary.execute_one("INSERT INTO fresh VALUES (1, 'x')")
        replica.apply_from_primary(primary)
        assert replica.execute_one("SELECT COUNT(*) FROM fresh").rows == [(1,)]
        primary.close()
        replica.db.close()

    def test_aborted_transaction_is_not_visible_on_replica(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        replica = Replica.bootstrap(primary, tmp_path / "replica")
        primary.execute_one("BEGIN")
        primary.execute_one("INSERT INTO t VALUES (1)")
        primary.execute_one("ROLLBACK")
        replica.apply_from_primary(primary)
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
        primary.close()
        replica.db.close()

    def test_monitor_follows_redirect(self, tmp_path):
        primary_one = make_primary(tmp_path)
        primary_two = Database(tmp_path / "primary2", checkpoint_wal_bytes=0)
        primary_two.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        primary_two.execute_one("INSERT INTO t VALUES (1)")
        target = WALServer(primary_two).start()
        redirect = WALServer(
            primary_one, redirect_to=(target.host, target.port)
        ).start()
        replica = None
        try:
            replica = Replica.bootstrap_from_server(
                target.host, target.port, tmp_path / "replica"
            )
            monitor = ReplicationMonitor(replica, redirect.host, redirect.port)
            assert monitor.poll() == "healthy"
            assert monitor.host == target.host
            assert monitor.port == target.port
        finally:
            if replica is not None:
                replica.db.close()
            redirect.stop()
            target.stop()
            primary_one.close()
            primary_two.close()

    def test_monitor_background_thread_catches_up(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        primary.execute_one("INSERT INTO t VALUES (1)")
        server = WALServer(primary).start()
        replica = None
        monitor = None
        try:
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica"
            )
            monitor = ReplicationMonitor(
                replica, server.host, server.port, poll_interval=0.1
            ).start(interval=0.1)
            primary.execute_one("INSERT INTO t VALUES (2)")
            deadline = 3.0
            while deadline > 0:
                if replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]:
                    break
                threading.Event().wait(0.05)
                deadline -= 0.05
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
        finally:
            if monitor is not None:
                monitor.stop()
            if replica is not None:
                replica.db.close()
            server.stop()
            primary.close()

    def test_monitor_rebuilds_after_wal_gap(self, tmp_path, monkeypatch):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        server = WALServer(primary).start()
        replica = None
        try:
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica"
            )
            calls = []

            def fail_replicate(*args, **kwargs):
                raise StorageError("需要重新基准备份")

            def rebuild(*args, **kwargs):
                calls.append(1)
                return 1

            monkeypatch.setattr(replica, "replicate_from_server", fail_replicate)
            monkeypatch.setattr(replica, "rebase_from_server", rebuild)
            monitor = ReplicationMonitor(replica, server.host, server.port)
            assert monitor.poll() == "rebuilt"
            assert monitor.poll() == "rebuilt"
            assert len(calls) == 2
        finally:
            if replica is not None:
                replica.db.close()
            server.stop()
            primary.close()

    def test_monitor_promotes_after_network_failures(self, tmp_path):
        fence_path = tmp_path / "promotion.fence"
        primary_fence = PromotionFence(fence_path, node_id="primary")
        primary = Database(
            tmp_path / "primary",
            checkpoint_wal_bytes=0,
            write_fence=primary_fence.assert_writable,
        )
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        primary.execute_one("INSERT INTO t VALUES (1)")
        server = WALServer(primary).start()
        replica = None
        try:
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica"
            )
            primary.execute_one("INSERT INTO t VALUES (2)")
            replica.replicate_from_server(server.host, server.port)
            server.stop()
            monitor = ReplicationMonitor(
                replica,
                server.host,
                server.port,
                fence_path=fence_path,
                max_failures=2,
                timeout=0.2,
            )
            assert monitor.poll() == "degraded"
            assert monitor.poll() == "promoted"
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
            with pytest.raises(NotSupportedError):
                primary.execute_one("INSERT INTO t VALUES (3)")
        finally:
            if replica is not None and not replica.db._read_only:
                replica.db.close()
            server.stop()
            primary.close()

    def test_promotion_fence_blocks_old_primary_writes(self, tmp_path):
        fence_path = tmp_path / "promotion.fence"
        primary_fence = PromotionFence(fence_path, node_id="primary")
        primary = Database(
            tmp_path / "primary",
            checkpoint_wal_bytes=0,
            write_fence=primary_fence.assert_writable,
        )
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        primary.execute_one("INSERT INTO t VALUES (1, 'before')")
        replica = Replica.bootstrap(primary, tmp_path / "replica")
        primary.execute_one("INSERT INTO t VALUES (2, 'after')")
        replica.apply_from_primary(primary)

        replica.promote(fence_path=fence_path, node_id="replica")
        with pytest.raises(NotSupportedError):
            primary.execute_one("INSERT INTO t VALUES (3, 'stale')")
        # 读仍然允许，旧主可以降级为只读观察者。
        assert primary.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
        primary.close()

    def test_snapshot_freezes_concurrent_writes(self, tmp_path, monkeypatch):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        entered = threading.Event()
        release = threading.Event()
        done = threading.Event()

        original = primary.disk.snapshot_bytes

        def slow_snapshot():
            entered.set()
            assert release.wait(2)
            return original()

        monkeypatch.setattr(primary.disk, "snapshot_bytes", slow_snapshot)

        def writer():
            primary.execute_one("INSERT INTO t VALUES (1)")
            done.set()

        snapshot_thread = threading.Thread(target=primary.snapshot)
        snapshot_thread.start()
        assert entered.wait(2)
        thread = threading.Thread(target=writer)
        thread.start()
        assert not done.is_set()
        release.set()
        snapshot_thread.join(2)
        thread.join(2)
        assert done.is_set()
        primary.close()

    def test_replica_restart_resumes_from_marker(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        replica = Replica.bootstrap(primary, tmp_path / "replica")
        for i in range(5):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        replica.apply_from_primary(primary)
        replica.db.close()

        for i in range(5, 10):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        resumed = Replica(tmp_path / "replica")
        resumed.apply_from_primary(primary)
        assert resumed.execute_one("SELECT COUNT(*) FROM t").rows == [(10,)]
        primary.close()
        resumed.db.close()

    def test_backup_rejects_active_transaction(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT)")
        primary.execute_one("BEGIN")
        try:
            with pytest.raises(TransactionError, match="没有活跃事务"):
                Replica.bootstrap(primary, tmp_path / "replica")
        finally:
            primary.execute_one("ROLLBACK")
            primary.close()

    def test_promote_keeps_only_replicated_prefix(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        replica = Replica.bootstrap(primary, tmp_path / "replica")
        primary.execute_one("INSERT INTO t VALUES (1)")
        replica.apply_from_primary(primary)
        primary.execute_one("INSERT INTO t VALUES (2)")
        primary.close()

        promoted = replica.promote()
        assert promoted.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        promoted.close()

    def test_replication_slot_survives_server_restart(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        slot = tmp_path / "replication.slot"
        server = WALServer(primary, slot_path=slot).start()
        replica = None
        try:
            replica = Replica.bootstrap_from_server(
                server.host, server.port, tmp_path / "replica"
            )
            primary.execute_one("INSERT INTO t VALUES (1)")
            replica.replicate_from_server(server.host, server.port)
            assert int(slot.read_text()) == replica.applied_lsn
        finally:
            server.stop()

        primary.checkpoint()
        restarted = WALServer(primary, slot_path=slot).start()
        try:
            primary.execute_one("INSERT INTO t VALUES (2)")
            replica.replicate_from_server(restarted.host, restarted.port)
            assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
        finally:
            replica.db.close()
            restarted.stop()
            primary.close()

    def test_promote_after_primary_close(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        replica = Replica.bootstrap(primary, tmp_path / "replica")
        primary.execute_one("INSERT INTO t VALUES (1, 'a')")
        replica.apply_from_primary(primary)
        primary.close()

        promoted = replica.promote()
        promoted.execute_one("INSERT INTO t VALUES (2, 'b')")
        assert promoted.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
        promoted.close()

    def test_client_follows_server_redirect(self, tmp_path):
        primary = make_primary(tmp_path)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        primary.execute_one("INSERT INTO t VALUES (1)")
        target = WALServer(primary).start()
        redirect = WALServer(
            primary, redirect_to=(target.host, target.port)
        ).start()
        try:
            client = WALClient(
                redirect.host, redirect.port, follow_redirects=True
            )
            status = client.status()
            assert status["current_lsn"] > 0
            assert client.host == target.host
        finally:
            redirect.stop()
            target.stop()
            primary.close()

    def test_fence_rejects_stale_promotion_lsn(self, tmp_path):
        fence_path = tmp_path / "fence.json"
        PromotionFence(fence_path, node_id="a").promote(lsn=10)
        with pytest.raises(NotSupportedError):
            PromotionFence(fence_path, node_id="b").promote(lsn=5)
        PromotionFence(fence_path, node_id="b").promote(lsn=11)
        assert PromotionFence(fence_path, node_id="b").read()["lsn"] == 11
