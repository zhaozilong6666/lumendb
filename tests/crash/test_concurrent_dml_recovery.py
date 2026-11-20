"""并发 DML + 逻辑回滚的崩溃恢复：loser 的页写入没有物理 undo。"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql


def _run(server, session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


def _count(server, db, sql: str) -> list:
    return _run(server, Session(database=db), sql)[0].rows


def _crash(db: Database) -> None:
    """崩溃：WAL 落盘、数据页留在缓冲池里不写，直接关磁盘句柄。"""
    db.wal.flush()
    db.wal.close()
    db.disk.close()


def test_loser_updates_have_no_physical_undo(tmp_path):
    path = tmp_path / "db"
    db = Database(path, concurrent_dml=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    db.execute_one("CREATE INDEX idx_v ON t (v)")

    committed = Session(database=db)
    _run(server, committed, "BEGIN")
    for i in range(60):
        _run(server, committed, f"INSERT INTO t VALUES ({i}, {i % 3}, 'c{i}')")
    _run(server, committed, "COMMIT")

    loser = Session(database=db)
    _run(server, loser, "BEGIN")
    for i in range(60, 120):
        _run(server, loser, f"INSERT INTO t VALUES ({i}, {i % 3}, 'l{i}')")
    # 不 COMMIT：模拟进程崩溃时它仍是 ACTIVE

    _crash(db)

    db2 = Database(path, concurrent_dml=True)
    server2 = PGWireServer(db2)
    try:
        assert _count(server2, db2, "SELECT COUNT(*) FROM t") == [(60,)]
        assert _count(server2, db2, "SELECT COUNT(*) FROM t WHERE id >= 60") == [(0,)]
        table = db2.catalog.get_table("t")
        tree = db2.executor._tree_for(table, table.indexes["idx_v"])
        inv = tree.check_invariants()
        assert inv["entries"] >= 60

        # 逻辑垃圾由 VACUUM 回收：可见行仍是 60，索引条目回落到 60
        db2.execute_one("VACUUM")
        tree = db2.executor._tree_for(table, table.indexes["idx_v"])
        assert tree.check_invariants()["entries"] == 60
        assert _count(server2, db2, "SELECT COUNT(*) FROM t") == [(60,)]
    finally:
        db2.close()
