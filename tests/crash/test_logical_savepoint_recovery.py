"""行级逻辑保存点的崩溃恢复：undo 只改页与版本链，必须能重放成一致状态。"""

from __future__ import annotations

from lumendb.engine.database import Database
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql


def _run(server, session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


def _rows(server, db, sql: str) -> list:
    return _run(server, Session(database=db), sql)[0].rows


def _crash(db: Database) -> None:
    db.wal.flush()
    db.wal.close()
    db.disk.close()


def _open(path) -> tuple[Database, PGWireServer]:
    db = Database(path, concurrent_dml=True, logical_savepoints=True)
    return db, PGWireServer(db)


def test_committed_logical_rollback_survives_crash(tmp_path):
    path = tmp_path / "logical_sp_crash"
    db, server = _open(path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10), (3, 30)")

    session = Session(database=db)
    _run(server, session, "BEGIN")
    _run(server, session, "SAVEPOINT s")
    _run(server, session, "UPDATE t SET v = 11 WHERE id = 1")
    _run(server, session, "DELETE FROM t WHERE id = 3")
    _run(server, session, "INSERT INTO t VALUES (2, 20)")
    _run(server, session, "ROLLBACK TO SAVEPOINT s")
    _run(server, session, "COMMIT")
    _crash(db)

    db2, server2 = _open(path)
    try:
        assert _rows(server2, db2, "SELECT id, v FROM t ORDER BY id") == [
            (1, 10),
            (3, 30),
        ]
        assert _rows(server2, db2, "SELECT COUNT(*) FROM t") == [(2,)]
    finally:
        db2.close()


def test_logical_rollback_then_new_dml_survives_crash(tmp_path):
    path = tmp_path / "logical_sp_crash_again"
    db, server = _open(path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10)")

    session = Session(database=db)
    _run(server, session, "BEGIN")
    _run(server, session, "SAVEPOINT s")
    _run(server, session, "UPDATE t SET v = 11 WHERE id = 1")
    _run(server, session, "ROLLBACK TO SAVEPOINT s")
    _run(server, session, "UPDATE t SET v = 12 WHERE id = 1")
    _run(server, session, "INSERT INTO t VALUES (4, 40)")
    _run(server, session, "COMMIT")
    _crash(db)

    db2, server2 = _open(path)
    try:
        assert _rows(server2, db2, "SELECT id, v FROM t ORDER BY id") == [
            (1, 12),
            (4, 40),
        ]
    finally:
        db2.close()


def test_uncommitted_logical_rollback_crash_keeps_prior_data(tmp_path):
    path = tmp_path / "logical_sp_crash_loser"
    db, server = _open(path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10)")

    session = Session(database=db)
    _run(server, session, "BEGIN")
    _run(server, session, "SAVEPOINT s")
    _run(server, session, "INSERT INTO t VALUES (99, 90)")
    _run(server, session, "ROLLBACK TO SAVEPOINT s")
    _run(server, session, "UPDATE t SET v = 77 WHERE id = 1")
    # 不 COMMIT：整个事务在恢复时会被中止。
    _crash(db)

    db2, server2 = _open(path)
    try:
        assert _rows(server2, db2, "SELECT id, v FROM t ORDER BY id") == [(1, 10)]
        assert _rows(server2, db2, "SELECT COUNT(*) FROM t") == [(1,)]
    finally:
        db2.close()
