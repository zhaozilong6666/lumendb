"""旧主库重新加入集群：必须全量重建，不能走页级增量。

旧主库崩溃前可能已经提交了一批"从没复制出去"的事务（分叉尾巴）。页级增量
重建只覆盖新主库 pageLSN 更新过的页，这些分叉页不会被覆盖 —— 副本会带着
新主库不认的行继续服务。所以 `Replica.rejoin_from_server` 走全量基准备份，
并把旧目录整体归档（不删除）。
"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALServer


def test_stale_primary_rejoins_as_read_only_replica(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for i in range(10):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(10,)]

    # 旧主库在"复制之前"又提交了 5 行：新主库永远看不到它们（分叉尾巴）。
    for i in range(10, 15):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")
    assert primary.execute_one("SELECT COUNT(*) FROM t").rows == [(15,)]

    # 副本提升为新主库（只带它已复制的 10 行）。
    newdb = replica.promote()
    server.stop()
    primary.close()

    new_server = WALServer(newdb).start()
    old = None
    try:
        old, archived = Replica.rejoin_from_server(
            new_server.host, new_server.port, tmp_path / "primary"
        )
        # 现场归档而不是删除：旧目录改名 <name>.stale-<时间戳>。
        assert archived is not None and archived.exists()
        assert (archived / "lumendb.data").exists()
        # 分叉的 5 行不在集群里：重建后只能看到 10 行。
        assert old.execute_one("SELECT COUNT(*) FROM t").rows == [(10,)]
        # 重新加入的节点只能是只读副本。
        with pytest.raises(NotSupportedError):
            old.execute_one("INSERT INTO t VALUES (99)")
        # 新主库继续写，旧主库作为副本追平。
        newdb.execute_one("INSERT INTO t VALUES (100)")
        for _ in range(50):
            if old.replicate_from_server(new_server.host, new_server.port) == 0:
                break
        assert old.execute_one("SELECT COUNT(*) FROM t").rows == [(11,)]
    finally:
        if old is not None:
            old.close()
        new_server.stop()
        newdb.close()


def test_rejoin_with_empty_target_does_not_archive(tmp_path):
    """目标目录还是空的（或不存在）时没有现场可留，不产生归档目录。"""
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    server = WALServer(primary).start()
    fresh = None
    try:
        fresh, archived = Replica.rejoin_from_server(
            server.host, server.port, tmp_path / "fresh"
        )
        assert archived is None
        assert fresh.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
    finally:
        if fresh is not None:
            fresh.close()
        server.stop()
        primary.close()
