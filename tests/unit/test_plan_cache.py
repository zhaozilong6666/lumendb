"""绑定计划缓存：命中、DDL 失效、参数语句不缓存。"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.plan.binder import Binder


@pytest.fixture()
def counts(monkeypatch):
    calls = {"bind": 0}
    original = Binder.bind_query

    def spy(self, query):
        calls["bind"] += 1
        return original(self, query)

    monkeypatch.setattr(Binder, "bind_query", spy)
    return calls


@pytest.fixture()
def db(tmp_path):
    database = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    database.execute_one("CREATE TABLE t (id INT, name VARCHAR(16))")
    for i in range(200):
        database.execute_one(f"INSERT INTO t VALUES ({i}, 'u{i}')")
    yield database
    database.close()


def test_same_statement_binds_once(db, counts):
    sql = "SELECT name FROM t WHERE id = 7"
    assert db.execute_one(sql).rows == [("u7",)]
    assert db.execute_one(sql).rows == [("u7",)]
    assert db.execute_one(sql).rows == [("u7",)]
    assert counts["bind"] == 1, "同一 AST 的重复查询不应重新绑定"


def test_alter_table_invalidates_plan(db, counts):
    assert [c.name for c in db.execute_one("SELECT * FROM t WHERE id = 1").columns] == [
        "id", "name",
    ]
    db.execute_one("ALTER TABLE t ADD COLUMN extra INT DEFAULT 9")
    result = db.execute_one("SELECT * FROM t WHERE id = 1")
    assert [c.name for c in result.columns] == ["id", "name", "extra"]
    assert result.rows == [(1, "u1", 9)]
    assert counts["bind"] == 2, "DDL 后必须重新绑定"


def test_create_index_invalidates_plan(db, counts):
    sql = "SELECT name FROM t WHERE id = 150"
    db.execute_one(sql)
    db.execute_one("CREATE INDEX idx_id ON t (id)")
    db.execute_one(sql)
    assert counts["bind"] == 2, "CREATE INDEX 后计划应失效（执行路径可能变化）"


def test_parameterized_statements_are_never_cached(db, counts):
    sql = "SELECT name FROM t WHERE id = $1"
    assert db.execute_one(sql, [3]).rows == [("u3",)]
    assert db.execute_one(sql, [4]).rows == [("u4",)]
    # 参数在绑定阶段折成字面量，缓存会把 $1 冻结成 3，必须逐次重绑
    assert counts["bind"] == 2


def test_cached_index_plan_sees_entries_after_root_split(db, counts):
    """计划里只存索引名，不存 root_page；插入把索引根页撑分裂后仍要查到新行。"""
    db.execute_one("CREATE INDEX idx_id ON t (id)")
    sql = "SELECT name FROM t WHERE id = 500"
    assert db.execute_one(sql).rows == []

    for i in range(2000):
        # 用参数化 INSERT：SQL 文本只有一条，SELECT 的 AST 不会被解析缓存挤掉。
        # （2000 条各不相同的 SQL 会把 SELECT 的 AST 淘汰，弱引用随之失效，
        #   此时重新绑定是正确行为，不是缓存缺陷。）
        db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, f"v{i}"])

    assert db.execute_one(sql).rows == [("v500",)]
    # CREATE INDEX 抬高版本 → 第一次 SELECT 重绑（1 次）；之后 INSERT 不改
    # schema，索引分裂也不改，第二次 SELECT 必须继续命中缓存。
    assert counts["bind"] == 1


def test_truncate_invalidates_plan(db, counts):
    db.execute_one("SELECT name FROM t WHERE id = 5")
    db.execute_one("TRUNCATE t")
    assert db.execute_one("SELECT name FROM t WHERE id = 5").rows == []
    assert counts["bind"] == 2
