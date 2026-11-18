"""绑定计划缓存与内部页缓存：性能优化的正确性护栏。

这两块缓存是后加的（`8046502` 绑定计划缓存、`1bd8ca9` 内部页解码缓存），
收益很大但都属于"一旦失效判断写错就会返回错结果"的那类优化。这里不测性能，
只测**失效条件**：

* 计划缓存按 `(id(AST), catalog.schema_version)`：DDL 必须让旧计划失效，
  失败的 DDL 也不能让半旧计划活下来；
* 内部页缓存按 `(页号, pageLSN)`：页被写过就不能再命中旧条目，
  索引重建（页号复用）后也不能。
"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.errors import BinderError, ColumnNotFoundError
from lumendb.sql.parser import parse_sql


@pytest.fixture
def db(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (a INT PRIMARY KEY, b TEXT)")
    for i in range(1, 6):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'v{i}')")
    yield db
    db.close()


def _run_ast(db, sql: str):
    """用同一个 AST 对象跑两次，走计划缓存的命中路径。"""
    ast = parse_sql(sql)[0]
    txn = db.txns.begin()
    try:
        result = db.executor.run(ast, txn)
        rows = list(result.rows)
    finally:
        db.txns.commit(txn)
    return ast, rows


class TestPlanCacheInvalidation:
    def test_same_ast_reuses_the_plan_and_stays_correct(self, db):
        ast, first = _run_ast(db, "SELECT a, b FROM t ORDER BY a")
        assert first == [(1, "v1"), (2, "v2"), (3, "v3"), (4, "v4"), (5, "v5")]
        # 数据变了，但计划可以复用：结果必须跟着数据走
        db.execute_one("INSERT INTO t VALUES (6, 'v6')")
        txn = db.txns.begin()
        try:
            rows = list(db.executor.run(ast, txn).rows)
        finally:
            db.txns.commit(txn)
        assert rows[-1] == (6, "v6")
        assert len(rows) == 6

    def test_add_column_invalidates_the_cached_plan(self, db):
        sql = "SELECT a, b FROM t ORDER BY a"
        ast, before = _run_ast(db, sql)
        db.execute_one("ALTER TABLE t ADD COLUMN c INT DEFAULT 7")
        # 同一个 AST 再跑：schema_version 变了，必须重新绑定
        txn = db.txns.begin()
        try:
            after = list(db.executor.run(ast, txn).rows)
        finally:
            db.txns.commit(txn)
        assert after == before
        assert db.execute_one("SELECT c FROM t WHERE a = 1").rows == [(7,)]

    def test_drop_column_does_not_leave_a_stale_plan(self, db):
        ast, _ = _run_ast(db, "SELECT b FROM t")
        db.execute_one("ALTER TABLE t DROP COLUMN b")
        txn = db.txns.begin()
        try:
            # 旧计划若还活着，这里会返回旧列；正确行为是重新绑定后报错
            with pytest.raises((ColumnNotFoundError, BinderError)):
                db.executor.run(ast, txn)
        finally:
            db.txns.abort(txn)

    def test_failed_ddl_still_invalidates(self, db):
        """失败的 DDL 也要清缓存（commit 里写的就是这个取舍）。"""
        ast, before = _run_ast(db, "SELECT a FROM t ORDER BY a")
        with pytest.raises(Exception):
            db.execute_one("ALTER TABLE t ADD COLUMN a INT")  # 列名冲突
        txn = db.txns.begin()
        try:
            after = list(db.executor.run(ast, txn).rows)
        finally:
            db.txns.commit(txn)
        assert after == before

    def test_drop_and_recreate_table_resets_the_plan(self, db):
        # 用 b 列：重建后的表没有 b，旧计划若活着就会"悄悄成功"
        ast, _ = _run_ast(db, "SELECT b FROM t")
        db.execute_one("DROP TABLE t")
        db.execute_one("CREATE TABLE t (a INT PRIMARY KEY, other TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        txn = db.txns.begin()
        try:
            with pytest.raises((ColumnNotFoundError, BinderError)):
                db.executor.run(ast, txn)  # 列 b 已经不存在
        finally:
            db.txns.abort(txn)


class TestInternalPageCacheInvalidation:
    def test_point_lookups_survive_many_splits(self, db):
        """大量插入触发页分裂（pageLSN 推进）：叶子/内部页缓存不能给出旧路由。"""
        for i in range(100, 400):
            db.execute_one(f"INSERT INTO t VALUES ({i}, 'x')")
        # 每个点查都必须命中自己那一行
        for i in range(100, 400, 37):
            rows = db.execute_one(f"SELECT a FROM t WHERE a = {i}").rows
            assert rows == [(i,)]
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(305,)]

    def test_index_rebuild_invalidates_cached_internal_pages(self, db):
        """DROP/CREATE INDEX 会复用页号：缓存必须靠 schema_version 整体清空。"""
        db.execute_one("CREATE INDEX idx_b ON t (b)")
        assert db.execute_one("SELECT a FROM t WHERE b = 'v3'").rows == [(3,)]
        db.execute_one("DROP INDEX idx_b")
        db.execute_one("CREATE INDEX idx_b ON t (b)")
        db.execute_one("REINDEX TABLE t")
        for i in range(1, 6):
            assert db.execute_one(f"SELECT a FROM t WHERE b = 'v{i}'").rows == [(i,)]

    def test_truncate_then_reinsert_keeps_lookups_correct(self, db):
        db.execute_one("TRUNCATE TABLE t")
        for i in range(1, 4):
            db.execute_one(f"INSERT INTO t VALUES ({i}, 'z{i}')")
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(3,)]
        assert db.execute_one("SELECT a FROM t WHERE a = 2").rows == [(2,)]
