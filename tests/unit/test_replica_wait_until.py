"""`Replica.wait_until`：读己之写（重定向到副本后先等目标 LSN）。"""

from __future__ import annotations

from lumendb.engine.database import Database
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALServer


def _primary_with_rows(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
    for i in range(20):
        primary.execute_one(f"INSERT INTO t VALUES ({i}, 'user{i}')")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    return primary, server, replica


def test_wait_until_returns_after_new_writes_are_applied(tmp_path):
    primary, server, replica = _primary_with_rows(tmp_path)
    try:
        for i in range(20, 40):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'user{i}')")
        target = primary.wal.last_lsn
        assert replica.applied_lsn < target

        assert replica.wait_until(target, server.host, server.port, timeout=5.0)
        assert replica.applied_lsn >= target
        # 追平后的查询能看到新行。
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(40,)]
        assert replica.execute_one("SELECT name FROM t WHERE id = 39").rows == [
            ("user39",)
        ]
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_wait_until_returns_true_immediately_when_already_caught_up(tmp_path):
    primary, server, replica = _primary_with_rows(tmp_path)
    try:
        assert replica.applied_lsn >= primary.wal.last_lsn
        assert replica.wait_until(
            primary.wal.last_lsn, server.host, server.port, timeout=5.0
        )
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_wait_until_times_out_for_an_unreachable_target(tmp_path):
    """目标 LSN 超过主库当前进度时，不能假装成功；必须在超时后返回 False。"""
    primary, server, replica = _primary_with_rows(tmp_path)
    try:
        beyond = primary.wal.last_lsn + 1_000_000
        started = __import__("time").perf_counter()
        ok = replica.wait_until(beyond, server.host, server.port, timeout=0.3)
        elapsed = __import__("time").perf_counter() - started
        assert ok is False
        assert elapsed >= 0.29, f"应在超时后才返回，实际 {elapsed:.3f}s"
        assert replica.applied_lsn <= primary.wal.last_lsn
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_wait_until_with_binary_stream(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for i in range(10):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica", binary_records=True
    )
    try:
        for i in range(10, 30):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        target = primary.wal.last_lsn
        assert replica.wait_until(
            target,
            server.host,
            server.port,
            timeout=5.0,
            binary_records=True,
        )
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(30,)]
    finally:
        replica.close()
        server.stop()
        primary.close()
