"""超长事务的跨批原子应用：没有 COMMIT 就一条都不应用。

背景：``max_records`` 是有界的，但一个事务可以更大（一次 300 行插入带二级
索引就会产生几百条 WAL 记录）。早期实现遇到"整批没有事务边界"时退回批尾，
副本会先应用表页、后应用索引页；读者在中间看到 COUNT 从 1 慢慢涨到 299，
索引扫描与顺序扫描对不上。现在的语义只有两个可见状态：**事务前**或**事务后**。
"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.errors import StorageError
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALServer


def test_oversized_transaction_never_visible_half_applied(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT, pad VARCHAR(32))")
    primary.execute_one("CREATE INDEX idx_t_id ON t (id)")

    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    try:
        # 一个事务写 300 行；每行有堆页记录 + 索引页记录，总数远大于 max_records。
        primary.execute_one("BEGIN")
        for i in range(300):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'pad-{i}')")
        primary.execute_one("COMMIT")

        counts: list[int] = []
        applied_sizes: list[int] = []
        for _ in range(400):
            applied = replica.replicate_from_server(
                server.host, server.port, max_records=40
            )
            applied_sizes.append(applied)

            count = replica.execute_one("SELECT COUNT(*) FROM t").rows[0][0]
            counts.append(count)
            # 核心断言：读者只允许看到 0（事务前）或 300（事务后）。
            assert count in (0, 300), f"读者看到了半个事务：COUNT={count}"
            if count == 300:
                table = replica.db.catalog.get_table("t")
                tree = replica.db.executor._tree_for(table, table.indexes["idx_t_id"])
                stats = tree.check_invariants()  # 破坏时不变量会抛 BTreeInvariantError
                assert stats["entries"] == 300, f"索引项数不对：{stats}"
            if applied == 0 and not replica._pending_records and count == 300:
                break
        else:
            raise AssertionError("副本在 400 轮内没有追平")

        assert applied_sizes.count(0) >= 1, "应该出现过「整批都在事务中间」的批次"
        assert replica._pending_records == []
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(300,)]
        assert replica.execute_one("SELECT COUNT(*) FROM t WHERE id = 299").rows == [(1,)]
        assert primary.execute_one("SELECT COUNT(*) FROM t").rows == [(300,)]
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_max_pending_records_is_constructor_tunable(tmp_path):
    """暂存上限要能在构造时调，而不是只能事后改私有字段。"""
    replica = Replica(tmp_path / "r", max_pending_records=25)
    try:
        assert replica._max_pending_records == 25
    finally:
        replica.close()


def test_pending_cap_fails_loudly_instead_of_tearing(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT)")
    primary.execute_one("CREATE INDEX idx_t_id ON t (id)")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(server.host, server.port, tmp_path / "r")
    try:
        primary.execute_one("BEGIN")
        for i in range(200):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        primary.execute_one("COMMIT")

        replica._max_pending_records = 30
        with pytest.raises(StorageError, match="暂存上限"):
            replica.replicate_from_server(server.host, server.port, max_records=40)
        # 报错前不能已经应用了半批。
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
    finally:
        replica.close()
        server.stop()
        primary.close()
