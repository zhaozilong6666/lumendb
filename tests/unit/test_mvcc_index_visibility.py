"""唯一索引延迟回收的可见性回归。

背景（`docs/concurrency.md`）：DELETE/UPDATE 原先当场物理摘掉索引项。MVCC 下
被删/被改的旧版本在提交前对别的快照仍然可见，索引项一摘，并发读者走索引扫描
就看不到这一行——键锁只防重复键，不解决读可见性。

修法：索引项与堆里的死版本一起等 VACUUM 重建索引时回收；唯一性改按 heap
可见性判（命中索引项后回堆里看当前版本，行已删或键已改走都不算占用），
索引扫描也要用堆里的当前版本复核键（防陈旧项返回错误行）。
"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.errors import DuplicateKeyError, SerializationError
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql


def _run(server: PGWireServer, session: Session, sql: str):
    return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]


def _rows(server: PGWireServer, session: Session, sql: str) -> list:
    return _run(server, session, sql)[0].rows


@pytest.fixture
def env(tmp_path):
    db = Database(tmp_path / "db")
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, code INT, pad VARCHAR(16))")
    for i in range(1, 21):
        db.execute_one(
            "INSERT INTO t VALUES ($1, $2, $3)", [i, i * 10, f"p{i}"]
        )
    db.execute_one("CREATE UNIQUE INDEX uq_code ON t (code)")
    db.execute_one("CREATE INDEX idx_code ON t (code)")
    yield db, server
    db.close()


def _uses_index(server: PGWireServer, session: Session, sql: str) -> bool:
    plan = "\n".join(r[0] for r in _rows(server, session, f"EXPLAIN {sql}"))
    return "IndexScan" in plan


def test_reader_sees_row_deleted_by_uncommitted_txn(env):
    """别人的 DELETE 还没提交：本快照走索引扫描必须仍能看到这一行。"""
    db, server = env
    reader, deleter = Session(database=db), Session(database=db)
    query = "SELECT id FROM t WHERE code = 30"
    assert _uses_index(server, reader, query)

    _run(server, deleter, "BEGIN")
    _run(server, deleter, "DELETE FROM t WHERE id = 3")
    _run(server, reader, "BEGIN")
    try:
        assert _rows(server, reader, query) == [(3,)]
    finally:
        _run(server, reader, "ROLLBACK")
    _run(server, deleter, "COMMIT")

    # 提交之后，任何新语句都看不到这一行（本引擎是读已提交语义）。
    after = Session(database=db)
    _run(server, after, "BEGIN")
    try:
        assert _rows(server, after, query) == []
    finally:
        _run(server, after, "ROLLBACK")


def test_reinsert_same_key_after_delete_in_same_txn(env):
    """自己删掉的行，同事务内重新插同一唯一键必须允许（陈旧索引项不算占用）。"""
    db, server = env
    s = Session(database=db)
    _run(server, s, "BEGIN")
    _run(server, s, "DELETE FROM t WHERE id = 5")
    _run(server, s, "INSERT INTO t VALUES (105, 50, 'new')")
    _run(server, s, "COMMIT")
    assert _rows(server, s, "SELECT COUNT(*) FROM t WHERE code = 50") == [(1,)]
    assert _rows(server, s, "SELECT id FROM t WHERE code = 50") == [(105,)]


def test_duplicate_of_live_row_still_rejected(env):
    db, server = env
    with pytest.raises(DuplicateKeyError) as exc:
        _run(server, Session(database=db), "INSERT INTO t VALUES (99, 30, 'dup')")
    assert exc.value.sqlstate == "23505"


def test_insert_same_key_waits_for_uncommitted_delete(env):
    """删除方未提交时插同键：键锁让后来者 40001，不能靠"索引项还在"报 23505。"""
    db, server = env
    deleter, inserter = Session(database=db), Session(database=db)
    _run(server, deleter, "BEGIN")
    _run(server, inserter, "BEGIN")
    _run(server, deleter, "DELETE FROM t WHERE id = 3")
    with pytest.raises(SerializationError):
        _run(server, inserter, "INSERT INTO t VALUES (103, 30, 'late')")
    _run(server, deleter, "COMMIT")
    _run(server, inserter, "ROLLBACK")
    _run(server, inserter, "INSERT INTO t VALUES (103, 30, 'late')")
    assert _rows(server, inserter, "SELECT id FROM t WHERE code = 30") == [(103,)]


def test_reader_on_old_index_key_during_uncommitted_update(env):
    """别人的 UPDATE 改键未提交：本快照按旧键查索引仍要拿到旧的可见版本。"""
    db, server = env
    writer, reader = Session(database=db), Session(database=db)
    _run(server, writer, "BEGIN")
    _run(server, writer, "UPDATE t SET code = 31 WHERE id = 3")
    _run(server, reader, "BEGIN")
    try:
        assert _rows(server, reader, "SELECT id, code FROM t WHERE code = 30") == [(3, 30)]
    finally:
        _run(server, reader, "ROLLBACK")
    _run(server, writer, "COMMIT")
    fresh = Session(database=db)
    _run(server, fresh, "BEGIN")
    try:
        assert _rows(server, fresh, "SELECT id, code FROM t WHERE code = 30") == []
        assert _rows(server, fresh, "SELECT id FROM t WHERE code = 31") == [(3,)]
    finally:
        _run(server, fresh, "ROLLBACK")


def test_index_lookup_ignores_stale_key_after_update(env):
    """改键后旧索引项还在：按旧键做索引扫描不能把行当成命中返回。"""
    db, server = env
    s = Session(database=db)
    _run(server, s, "BEGIN")
    _run(server, s, "UPDATE t SET code = 77 WHERE id = 7")
    _run(server, s, "COMMIT")
    assert _rows(server, s, "SELECT id FROM t WHERE code = 70") == []
    assert _rows(server, s, "SELECT id FROM t WHERE code = 77") == [(7,)]


def test_index_range_ignores_stale_entries(env):
    db, server = env
    s = Session(database=db)
    _run(server, s, "BEGIN")
    _run(server, s, "UPDATE t SET code = 95 WHERE id = 9")  # 90 被搬出 [80, 90]
    _run(server, s, "COMMIT")
    assert _rows(server, s, "SELECT id FROM t WHERE code BETWEEN 80 AND 90") == [
        (8,),
    ]
    assert _rows(server, s, "SELECT id FROM t WHERE code = 95") == [(9,)]


def test_update_key_back_and_forth_does_not_duplicate_rows(env):
    """键改回原值时陈旧索引项已存在，不能再插一条 (key, rid) 让同行走两次。"""
    db, server = env
    s = Session(database=db)
    _run(server, s, "BEGIN")
    _run(server, s, "UPDATE t SET code = 44 WHERE id = 4")
    _run(server, s, "UPDATE t SET code = 40 WHERE id = 4")
    _run(server, s, "COMMIT")
    assert _rows(server, s, "SELECT id FROM t WHERE code = 40") == [(4,)]


def test_vacuum_reclaims_stale_index_entries(env):
    """VACUUM 重建索引后，陈旧项被回收：树条目数回到活行数。"""
    db, server = env
    s = Session(database=db)
    _run(server, s, "BEGIN")
    _run(server, s, "DELETE FROM t WHERE id = 2")
    _run(server, s, "COMMIT")
    table = db.catalog.get_table("t")
    tree = db.executor._tree_for(table, table.indexes["idx_code"])
    # 延迟回收：删除已提交，但索引项还在（这是可见性修复的代价）。
    before = tree.check_invariants()["entries"]
    assert before >= 20
    _run(server, s, "VACUUM t")
    tree = db.executor._tree_for(table, table.indexes["idx_code"])
    assert tree.check_invariants()["entries"] == 19
