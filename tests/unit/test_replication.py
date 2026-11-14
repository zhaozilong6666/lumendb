"""只读副本的物理 WAL 应用测试。"""

from __future__ import annotations

import threading

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError, TransactionError
from lumendb.replication import (
    PromotionFence,
    Replica,
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
