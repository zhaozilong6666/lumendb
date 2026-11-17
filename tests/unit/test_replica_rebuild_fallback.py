"""全量重建回退：增量重建失败时保留旧文件并重拉完整基准备份。"""

from __future__ import annotations

from pathlib import Path

from lumendb.engine.database import Database
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALServer


def test_rebuild_from_scratch_resyncs_and_preserves_old_file(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
    for i in range(50):
        primary.execute_one(f"INSERT INTO t VALUES ({i}, 'u{i}')")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    try:
        # 主库继续写，但副本不追：模拟"落后 + 增量重建失败"。
        for i in range(50, 80):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'u{i}')")

        old_size = (tmp_path / "replica" / "lumendb.data").stat().st_size
        base_lsn = replica.rebuild_from_scratch(server.host, server.port)

        assert base_lsn == primary.wal.last_lsn
        assert replica.applied_lsn == base_lsn
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(80,)]
        assert replica.execute_one("SELECT name FROM t WHERE id = 79").rows == [
            ("u79",)
        ]

        # 旧文件保留在 broken-<时间戳>/ 下，供事后取证，没有被删除。
        preserved_dirs = sorted(
            p for p in (tmp_path / "replica").glob("broken-*") if p.is_dir()
        )
        assert preserved_dirs, "应该有 broken-* 目录"
        kept = preserved_dirs[-1] / "lumendb.data"
        assert kept.exists() and kept.stat().st_size == old_size
        assert replica.last_rebuild_preserved == str(preserved_dirs[-1])

        # 重建后仍是只读副本。
        assert replica.db._read_only
        try:
            replica.execute_one("INSERT INTO t VALUES (999, 'no')")
        except Exception as exc:  # noqa: PERF203 - 只读拒绝
            assert "只读" in str(exc) or "read-only" in str(exc).lower()
        else:  # pragma: no cover
            raise AssertionError("只读副本接受了写入")
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_rebuild_from_scratch_is_idempotent_on_a_clean_directory(tmp_path):
    """目录里没有数据文件时也能重建（不产生 broken 目录）。"""
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    try:
        replica.db.close()
        # 模拟数据文件丢失后重新拉取。
        (tmp_path / "replica" / "lumendb.data").unlink()
        base_lsn = replica.rebuild_from_scratch(server.host, server.port)
        assert base_lsn == primary.wal.last_lsn
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        assert replica.last_rebuild_preserved == ""
        assert not list((tmp_path / "replica").glob("broken-*"))
    finally:
        replica.close()
        server.stop()
        primary.close()
