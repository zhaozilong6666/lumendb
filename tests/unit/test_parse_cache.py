"""SQL 解析缓存的三条语义：复用 AST、每次返回新 list、执行不改写 AST。

缓存的安全前提不是"所有 AST 都 frozen"（顶层 ``Query`` 的 ``ctes`` 在解析
WITH 子句时会被填一次，所以不能整体标 frozen），而是"**解析完成后没人再改
它**"。最后一条测试用真实执行来钉这个前提。
"""

from __future__ import annotations

from lumendb.sql.ast import Query
from lumendb.sql.parser import _parse_sql_cached, parse_sql


def test_same_sql_shares_ast_but_returns_fresh_list():
    first = parse_sql("SELECT 1")
    second = parse_sql("SELECT 1")
    # 每次都是新的 list：调用方 append 不会污染缓存。
    assert first is not second
    # AST 节点复用：这正是省下的词法 + 语法分析。
    assert first[0] is second[0]
    assert isinstance(first[0], Query)
    assert _parse_sql_cached.cache_info().hits >= 1


def test_different_sql_does_not_share_ast():
    a = parse_sql("SELECT 1")
    b = parse_sql("SELECT 2")
    assert a[0] is not b[0]


def test_multi_statement_sql_is_cached_as_a_whole():
    list_a = parse_sql("SELECT 1; SELECT 2")
    list_b = parse_sql("SELECT 1; SELECT 2")
    assert len(list_a) == len(list_b) == 2
    assert list_a[0] is list_b[0]
    assert list_a[1] is list_b[1]


def test_execution_does_not_mutate_cached_ast(tmp_path):
    """真实执行若干次后，缓存里的 AST 必须与解析时完全一致。

    这是缓存能成立的前提：绑定/执行阶段只读 AST，改的是 Catalog 里的表对象。
    如果哪天有人在执行路径上给 AST 赋值，这条会立刻失败。
    """
    from lumendb.engine.database import Database

    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    try:
        db.execute_one("CREATE TABLE t (id INT, name VARCHAR(8))")
        db.execute_one("INSERT INTO t VALUES (1, 'a')")
        sql = "SELECT id FROM t WHERE id = 1"
        before = repr(parse_sql(sql)[0])
        for _ in range(3):
            assert db.execute_one(sql).affected in (0, 1)
        assert repr(parse_sql(sql)[0]) == before
        # WITH 子句走的是解析期填 ctes 的那条路径，同样不能被后续执行改写。
        with_sql = "WITH c AS (SELECT 1 AS x) SELECT x FROM c"
        before_with = repr(parse_sql(with_sql)[0])
        db.execute_one(with_sql)
        assert repr(parse_sql(with_sql)[0]) == before_with
    finally:
        db.close()
