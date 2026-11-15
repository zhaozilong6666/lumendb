"""跨层不变量：把"两个 agent 各自修过的坑"钉死成回归。

这些不变量分别在 `test_concurrency.py`、`test_write_fence.py`、
`test_quorum_commit.py`、`test_sync_replication.py` 里有更细的用例；这里
只做一件事：**用最少的代码把"整条链路"再走一遍**，因为并发/复制这几层
是被两个人先后改过的，单元测试都绿不代表组合起来还对。

历史背景（都真实发生过）：

* 延迟外键在线协议提交点漏检 → 单条 INSERT 就能写入悬空引用；
* 两个写事务同时改同一行 → 一个逻辑行 `SELECT` 出两行；
* 外键校验被快照冻住 → 并发提交的父行"不存在"，合法插入报 23503；
* 父行删除看不见并发提交的子行 → 提交后悬空引用；
* 崩溃恢复的 undo 覆盖已提交写入 → 已提交的 UPDATE / CREATE TABLE 消失。
"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.errors import (
    DuplicateKeyError,
    ForeignKeyViolationError,
    SerializationError,
)
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql


def _run(server: PGWireServer, session: Session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


@pytest.fixture
def env(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    server = PGWireServer(db)
    yield db, server
    db.close()


def test_deferred_fk_cannot_be_smuggled_in_through_the_wire(env):
    """线协议提交点必须补检延迟外键（否则单条 INSERT 就能写坏数据）。"""
    db, server = env
    db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
    db.execute_one(
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) "
        "DEFERRABLE INITIALLY DEFERRED)"
    )
    session = Session(database=db)
    _run(server, session, "BEGIN")
    _run(server, session, "INSERT INTO c VALUES (1, 999)")  # 延迟，语句内放行
    with pytest.raises(ForeignKeyViolationError) as exc:
        _run(server, session, "COMMIT")
    assert exc.value.sqlstate == "23503"
    assert _run(server, session, "SELECT COUNT(*) FROM c")[0].rows == [(0,)]


def test_two_writers_cannot_share_the_database(env):
    """写者锁：第二个写事务 40001，失败后可以重试；行数永远对得上。"""
    db, server = env
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 0)")
    first, second = Session(database=db), Session(database=db)

    _run(server, first, "BEGIN")
    _run(server, second, "BEGIN")
    _run(server, first, "UPDATE t SET v = 111 WHERE id = 1")
    with pytest.raises(SerializationError) as exc:
        _run(server, second, "UPDATE t SET v = 222 WHERE id = 1")
    assert exc.value.sqlstate == "40001"

    _run(server, first, "COMMIT")
    _run(server, second, "ROLLBACK")
    # 一个逻辑行只能有一个可见版本，且是赢家的值
    assert _run(server, first, "SELECT id, v FROM t")[0].rows == [(1, 111)]
    assert _run(server, first, "SELECT COUNT(*) FROM t")[0].rows == [(1,)]


def test_deleting_a_parent_sees_a_concurrently_committed_child(env):
    """RI 可见性：父行删除必须看到并发事务刚提交的子行，否则留悬空引用。"""
    db, server = env
    db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
    db.execute_one("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
    db.execute_one("INSERT INTO p VALUES (1)")
    deleter, child_writer = Session(database=db), Session(database=db)

    _run(server, deleter, "BEGIN")  # 快照早于子行提交
    _run(server, child_writer, "INSERT INTO c VALUES (10, 1)")
    with pytest.raises(ForeignKeyViolationError) as exc:
        _run(server, deleter, "DELETE FROM p WHERE id = 1")
    assert exc.value.sqlstate == "23503"
    _run(server, deleter, "ROLLBACK")
    assert _run(server, child_writer, "SELECT COUNT(*) FROM p")[0].rows == [(1,)]


def test_child_insert_sees_a_concurrently_committed_parent(env):
    """反方向：父行在快照之后提交，子行插入不能误报 23503。"""
    db, server = env
    db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
    db.execute_one("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
    reader, parent_writer = Session(database=db), Session(database=db)

    _run(server, reader, "BEGIN")
    _run(server, parent_writer, "INSERT INTO p VALUES (1)")
    _run(server, reader, "INSERT INTO c VALUES (1, 1)")  # 不能报错
    _run(server, reader, "COMMIT")
    assert _run(server, reader, "SELECT COUNT(*) FROM c")[0].rows == [(1,)]


def test_rollback_does_not_resurrect_a_deleted_unique_key(env):
    """回滚不能把并发插入的唯一键"复活"成重复行。"""
    db, server = env
    db.execute_one("CREATE TABLE u (id INT PRIMARY KEY)")
    db.execute_one("INSERT INTO u VALUES (1)")
    deleter, inserter = Session(database=db), Session(database=db)

    _run(server, deleter, "BEGIN")
    _run(server, deleter, "DELETE FROM u WHERE id = 1")
    with pytest.raises(SerializationError):
        _run(server, inserter, "INSERT INTO u VALUES (1)")
    _run(server, deleter, "ROLLBACK")
    with pytest.raises(DuplicateKeyError) as exc:
        _run(server, inserter, "INSERT INTO u VALUES (1)")
    assert exc.value.sqlstate == "23505"
    assert _run(server, inserter, "SELECT COUNT(*) FROM u")[0].rows == [(1,)]


def test_concurrent_ddl_is_serialized(env):
    """目录是单根指针 + 页链：两个 DDL 事务不能同时写。"""
    db, server = env
    first, second = Session(database=db), Session(database=db)
    _run(server, first, "BEGIN")
    _run(server, first, "CREATE TABLE a (id INT)")
    _run(server, second, "BEGIN")
    with pytest.raises(SerializationError):
        _run(server, second, "CREATE TABLE b (id INT)")
    _run(server, first, "COMMIT")
    _run(server, second, "ROLLBACK")
    _run(server, second, "CREATE TABLE b (id INT)")
    names = sorted(row[0] for row in _run(server, second, "SHOW TABLES")[0].rows)
    assert names == ["a", "b"]
