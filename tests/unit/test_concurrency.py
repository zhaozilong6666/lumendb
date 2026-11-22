"""并发与引用完整性的回归测试。

这一组用例补的是"两条连接同时写"这条空白：在此之前的 1000+ 个测试里，
没有任何一个用例让两个事务真正并发地写同一行、同一外键或同一个唯一键，
所以下面这些失效模式在实现里活了很久：

* 并发更新同一行 → 版本链被覆盖，一个逻辑行 `SELECT` 出两行；
* 并发删除父行 / 插入子行 → 提交后留下悬空外键；
* 回滚把并发插入的唯一键"复活" → 主键重复；
* 线协议提交不校验延迟约束 → 单条 INSERT 就能写坏数据；
* 快照冻结导致外键误报 23503。

本文件用 ``PGWireServer._run_statement`` + ``Session`` 复现线协议路径
（它和真实 socket 走的是同一个函数），避免为了并发语义再开一层协议栈。
"""

from __future__ import annotations

import threading
import time

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
    return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]


def _rows(server: PGWireServer, session: Session, sql: str) -> list:
    return _run(server, session, sql)[0].rows


@pytest.fixture
def env(tmp_path):
    db = Database(tmp_path / "db")
    server = PGWireServer(db)
    yield db, server
    db.close()


# ---------------------------------------------------------------------------
# 写写冲突
# ---------------------------------------------------------------------------


class TestWriteWriteConflict:
    """写并发：不同行/不同键可以同时写，只有真正冲突才 40001。

    这一组用例在"单写者"时期断言的是"第二个写事务一律 40001"。行/键锁落地后，
    冲突的粒度从"整个数据库"缩到"这一行 / 这一个唯一键"——所以这里既要有
    "不同行可以并行"的正向用例，也要保留"同一行必须失败"的防线。
    """

    def test_two_writers_on_different_rows_both_commit(self, env):
        db, server = env
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 0)")
        db.execute_one("INSERT INTO t VALUES (2, 0)")
        first, second = Session(database=db), Session(database=db)

        _run(server, first, "BEGIN")
        _run(server, second, "BEGIN")
        _run(server, first, "UPDATE t SET v = 111 WHERE id = 1")
        _run(server, second, "UPDATE t SET v = 222 WHERE id = 2")  # 不冲突
        _run(server, first, "COMMIT")
        _run(server, second, "COMMIT")
        assert _run(server, first, "SELECT id, v FROM t ORDER BY id")[0].rows == [
            (1, 111),
            (2, 222),
        ]

    def test_two_writers_on_the_same_row_conflict(self, env):
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
        assert _run(server, first, "SELECT COUNT(*) FROM t")[0].rows == [(1,)]

    def test_two_writers_on_the_same_unique_key_conflict(self, env):
        db, server = env
        db.execute_one("CREATE TABLE u (id INT PRIMARY KEY, code INT)")
        db.execute_one("CREATE UNIQUE INDEX uq_code ON u (code)")
        first, second = Session(database=db), Session(database=db)

        _run(server, first, "BEGIN")
        _run(server, second, "BEGIN")
        _run(server, first, "INSERT INTO u VALUES (1, 7)")
        with pytest.raises(SerializationError):
            _run(server, second, "INSERT INTO u VALUES (2, 7)")
        _run(server, first, "COMMIT")
        _run(server, second, "ROLLBACK")
        with pytest.raises(DuplicateKeyError) as exc:
            _run(server, second, "INSERT INTO u VALUES (2, 7)")  # 提交后是真冲突
        assert exc.value.sqlstate == "23505"

    def test_stale_snapshot_update_is_rejected(self, env):
        """对方的快照早于我的提交时，我的 UPDATE 必须 40001，不能覆盖版本链。

        这是 first-updater-wins：写者锁只能挡住"同时"写，挡不住"我先提交、
        你带着旧快照再写"。旧实现会把根记录的 newest 指针改回去，让两个版本
        同时可见（一个逻辑行 SELECT 出两行）。
        """
        db, server = env
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 0)")
        stale, writer = Session(database=db), Session(database=db)

        _run(server, stale, "BEGIN")  # 快照建立在这里
        _run(server, writer, "BEGIN")
        _run(server, writer, "UPDATE t SET v = 111 WHERE id = 1")
        _run(server, writer, "COMMIT")

        with pytest.raises(SerializationError) as exc:
            _run(server, stale, "UPDATE t SET v = 222 WHERE id = 1")
        assert exc.value.sqlstate == "40001"

        _run(server, stale, "ROLLBACK")
        assert _rows(server, stale, "SELECT id, v FROM t") == [(1, 111)]
        assert _rows(server, stale, "SELECT COUNT(*) FROM t") == [(1,)]

    def test_stale_snapshot_delete_is_rejected(self, env):
        db, server = env
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 0)")
        stale, writer = Session(database=db), Session(database=db)

        _run(server, stale, "BEGIN")
        _run(server, writer, "BEGIN")
        _run(server, writer, "UPDATE t SET v = 1 WHERE id = 1")
        _run(server, writer, "COMMIT")

        with pytest.raises(SerializationError):
            _run(server, stale, "DELETE FROM t WHERE id = 1")
        _run(server, stale, "ROLLBACK")
        assert _rows(server, stale, "SELECT COUNT(*) FROM t") == [(1,)]

    def test_reader_is_not_blocked_by_writer(self, env):
        """写者锁只挡写：另一个连接照常按快照读，且读不到未提交数据。"""
        db, server = env
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        s1, s2 = Session(database=db), Session(database=db)

        _run(server, s1, "BEGIN")
        _run(server, s1, "INSERT INTO t VALUES (1)")
        assert _rows(server, s2, "SELECT COUNT(*) FROM t") == [(0,)]
        _run(server, s1, "COMMIT")
        assert _rows(server, s2, "SELECT COUNT(*) FROM t") == [(1,)]

    def test_concurrent_ddl_is_serialized(self, env):
        """并发 DDL 必须串行化：目录是"单根指针 + 页链"，两个事务同时写会互相踩。"""
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

        names = sorted(r[0] for r in _rows(server, second, "SHOW TABLES"))
        assert names == ["a", "b"]

    def test_autocommit_statement_waits_behind_a_row_lock(self, env):
        """自动提交语句受行锁约束：它撞的是"那一行"，不是整个库。

        默认模式仍然快速失败；并发 DML 模式改成排队等待，所以这个用例在
        两种模式下断言不同：并发模式下必须等到 holder 回滚后再成功。
        """
        db, server = env
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 0)")
        holder = Session(database=db)
        _run(server, holder, "BEGIN")
        _run(server, holder, "UPDATE t SET v = 1 WHERE id = 1")

        if not db._concurrent_dml:
            with pytest.raises(SerializationError):
                db.execute_one("UPDATE t SET v = 2 WHERE id = 1")
        else:
            errors: list[BaseException] = []
            done = threading.Event()

            def waiter() -> None:
                try:
                    db.execute_one("UPDATE t SET v = 2 WHERE id = 1")
                except BaseException as exc:  # pragma: no cover - 失败时打印
                    errors.append(exc)
                finally:
                    done.set()

            thread = threading.Thread(target=waiter)
            thread.start()
            # 让它先撞上行锁；即便这里提前回滚，下面的断言仍然成立。
            time.sleep(0.2)
            assert not done.is_set(), "并发模式下的自动提交 UPDATE 应当等待行锁"
            _run(server, holder, "ROLLBACK")
            assert done.wait(10.0), "回滚后等待方应当被唤醒"
            thread.join(1.0)
            assert errors == [], errors[0] if errors else None
            assert db.execute_one("SELECT v FROM t WHERE id = 1").rows == [(2,)]

        # 不同行不受影响
        db.execute_one("INSERT INTO t VALUES (2, 2)")
        if not db._concurrent_dml:
            _run(server, holder, "ROLLBACK")


# ---------------------------------------------------------------------------
# 唯一键：回滚不能把并发插入的键"复活"
# ---------------------------------------------------------------------------


class TestUniqueKeyUnderRollback:
    def test_concurrent_insert_of_deleted_key_is_rejected(self, env):
        db, server = env
        db.execute_one("CREATE TABLE u (id INT PRIMARY KEY)")
        db.execute_one("INSERT INTO u VALUES (1)")
        deleter, inserter = Session(database=db), Session(database=db)

        _run(server, deleter, "BEGIN")
        _run(server, deleter, "DELETE FROM u WHERE id = 1")
        # 未提交的删除已经物理摘掉了索引项；没有写者锁时这里会插入成功，
        # 删除方回滚后索引项被恢复，主键就重复了。
        with pytest.raises(SerializationError):
            _run(server, inserter, "INSERT INTO u VALUES (1)")

        _run(server, deleter, "ROLLBACK")
        # 回滚恢复了索引项，键仍被占用：插入必须 23505，而不是造出第二行。
        with pytest.raises(DuplicateKeyError) as exc:
            _run(server, inserter, "INSERT INTO u VALUES (1)")
        assert exc.value.sqlstate == "23505"
        assert _rows(server, inserter, "SELECT COUNT(*) FROM u") == [(1,)]


# ---------------------------------------------------------------------------
# 外键：并发提交不能留下悬空引用，也不能误报
# ---------------------------------------------------------------------------


class TestForeignKeyConcurrency:
    def test_delete_sees_child_committed_after_snapshot(self, env):
        """父行删除必须看到并发事务刚提交的子行（RI 可见性），否则留悬空引用。"""
        db, server = env
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        deleter, child_writer = Session(database=db), Session(database=db)

        _run(server, deleter, "BEGIN")  # 快照早于子行提交
        _run(server, child_writer, "INSERT INTO c VALUES (10, 1)")  # 自动提交

        with pytest.raises(ForeignKeyViolationError) as exc:
            _run(server, deleter, "DELETE FROM p WHERE id = 1")
        assert exc.value.sqlstate == "23503"
        _run(server, deleter, "ROLLBACK")
        assert _rows(server, child_writer, "SELECT COUNT(*) FROM p") == [(1,)]

    def test_child_insert_sees_parent_committed_after_snapshot(self, env):
        """父行在快照之后提交时，子行插入不能误报 23503（PostgreSQL 同语义）。"""
        db, server = env
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
        reader, parent_writer = Session(database=db), Session(database=db)

        _run(server, reader, "BEGIN")
        _run(server, parent_writer, "INSERT INTO p VALUES (1)")
        _run(server, reader, "INSERT INTO c VALUES (1, 1)")  # 不能报错
        _run(server, reader, "COMMIT")
        assert _rows(server, reader, "SELECT COUNT(*) FROM c") == [(1,)]

    def test_uncommitted_child_is_still_invisible(self, env):
        """未提交的子行不可见：外键校验不能因此读到脏数据。"""
        db, server = env
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        other = Session(database=db)
        _run(server, other, "BEGIN")
        _run(server, other, "INSERT INTO c VALUES (5, 1)")
        # 子行尚未提交，父行删除仍应被阻止（RI 可见性把"已提交"当可见，
        # 而"未提交"依然不可见——这一条由写者锁兜底，见下一个断言）。
        with pytest.raises(SerializationError):
            db.execute_one("DELETE FROM p WHERE id = 1")
        _run(server, other, "ROLLBACK")


# ---------------------------------------------------------------------------
# 延迟约束：线协议提交点必须补检
# ---------------------------------------------------------------------------


class TestDeferredConstraintsOnTheWire:
    def test_single_autocommit_insert_cannot_write_dangling_reference(self, env):
        """一条语句就能写坏数据：延迟外键被跳过而没人补检（已修）。"""
        db, server = env
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        with pytest.raises(ForeignKeyViolationError) as exc:
            db.execute_one("INSERT INTO c VALUES (1, 999)")
        assert exc.value.sqlstate == "23503"
        assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM c") == [(0,)]

    def test_explicit_commit_validates_deferred_foreign_key(self, env):
        db, server = env
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        s = Session(database=db)
        _run(server, s, "BEGIN")
        _run(server, s, "INSERT INTO c VALUES (1, 999)")  # 延迟：语句内放行
        with pytest.raises(ForeignKeyViolationError) as exc:
            _run(server, s, "COMMIT")
        assert exc.value.sqlstate == "23503"
        s2 = Session(database=db)
        assert _rows(server, s2, "SELECT COUNT(*) FROM c") == [(0,)]

    def test_explicit_commit_validates_deferred_unique(self, env):
        db, server = env
        db.execute_one(
            "CREATE TABLE u (id INT, code INT, "
            "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY DEFERRED)"
        )
        s = Session(database=db)
        _run(server, s, "BEGIN")
        _run(server, s, "INSERT INTO u VALUES (1, 7)")
        _run(server, s, "INSERT INTO u VALUES (2, 7)")
        with pytest.raises(DuplicateKeyError) as exc:
            _run(server, s, "COMMIT")
        assert exc.value.sqlstate == "23505"

    def test_read_statement_is_not_judged_by_deferred_constraints(self, env, monkeypatch):
        """纯读语句不能进延迟约束补检（旧实现会让 SELECT 变全表扫描并可返回 23505）。"""
        db, server = env
        db.execute_one(
            "CREATE TABLE u (id INT, code INT, "
            "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY DEFERRED)"
        )
        calls: list[int] = []
        original = type(db.executor).validate_deferred_unique_constraints

        def spy(self, txn, **kwargs):
            calls.append(txn.xid)
            return original(self, txn, **kwargs)

        monkeypatch.setattr(
            type(db.executor), "validate_deferred_unique_constraints", spy
        )

        db.execute_one("SELECT COUNT(*) FROM u")
        db.execute_one("SHOW TABLES")
        assert calls == [], "读语句不该跑延迟约束补检"

        db.execute_one("INSERT INTO u VALUES (1, 7)")
        assert len(calls) == 1, "写语句仍然必须在提交点补检"
