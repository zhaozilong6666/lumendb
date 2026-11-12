"""执行引擎的端到端测试：从 SQL 文本一路走到结果行。"""

import pytest

from lumendb.engine.database import Database
from lumendb.errors import (
    ColumnNotFoundError,
    DuplicateKeyError,
    NotNullViolationError,
    NotSupportedError,
    SQLSyntaxError,
    TableNotFoundError,
    TypeMismatchError,
    UndefinedParameterError,
)


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "db")
    yield database
    database.close()


def rows(db, sql, params=None):
    return db.execute_one(sql, params).rows


def setup_users(db):
    db.execute_one(
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, age INT)"
    )
    db.execute_one(
        "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)"
    )


class TestParameters:
    """``$n`` 参数：绑定阶段折成字面量，类型检查照常生效。"""

    def test_select_with_param(self, db):
        setup_users(db)
        result = db.execute_one(
            "SELECT name FROM users WHERE id = $1", params=[2]
        )
        assert result.rows == [("bob",)]

    def test_insert_with_params(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        result = db.execute_one(
            "INSERT INTO t VALUES ($1, $2), ($1, $3)",
            params=[7, "x", "y"],
        )
        assert result.affected == 2
        assert rows(db, "SELECT a, b FROM t ORDER BY b") == [(7, "x"), (7, "y")]

    def test_update_and_delete_with_params(self, db):
        setup_users(db)
        result = db.execute_one(
            "UPDATE users SET age = $1 WHERE id = $2", params=[40, 1]
        )
        assert result.affected == 1
        assert rows(db, "SELECT age FROM users WHERE id = 1") == [(40,)]
        result = db.execute_one("DELETE FROM users WHERE id = $1", params=[2])
        assert result.affected == 1
        assert rows(db, "SELECT id FROM users ORDER BY id") == [(1,), (3,)]

    def test_param_type_mismatch_is_reported(self, db):
        setup_users(db)
        with pytest.raises(TypeMismatchError):
            db.execute_one("SELECT name FROM users WHERE id = $1", params=["oops"])

    def test_missing_param(self, db):
        setup_users(db)
        with pytest.raises(UndefinedParameterError):
            db.execute_one("SELECT name FROM users WHERE id = $1")
        with pytest.raises(UndefinedParameterError):
            db.execute_one("SELECT name FROM users WHERE id = $2", params=[1])

    def test_param_in_expression_and_case(self, db):
        setup_users(db)
        assert rows(
            db,
            "SELECT CASE WHEN id = $1 THEN 'me' ELSE 'other' END FROM users "
            "ORDER BY id",
            params=[2],
        ) == [("other",), ("me",), ("other",)]

    def test_param_in_order_by_and_limit(self, db):
        setup_users(db)
        assert rows(
            db, "SELECT id FROM users ORDER BY id DESC LIMIT $1", params=[2]
        ) == [(3,), (2,)]


class TestVacuum:
    def test_vacuum_rebuilds_index_and_keeps_data(self, db):
        setup_users(db)
        db.execute_one("UPDATE users SET age = age + 1")
        db.execute_one("DELETE FROM users WHERE id = 2")
        result = db.execute_one("VACUUM users")
        assert "2 rows kept" in result.tag
        assert "4 versions reclaimed" in result.tag
        assert rows(db, "SELECT id, age FROM users ORDER BY id") == [(1, 31), (3, 36)]

        plan = "\n".join(
            r[0] for r in rows(db, "EXPLAIN SELECT name FROM users WHERE id = 3")
        )
        assert "IndexScan" in plan
        assert rows(db, "SELECT name FROM users WHERE id = 3") == [("carol",)]

    def test_vacuum_all_tables(self, db):
        db.execute_one("CREATE TABLE a (x INT)")
        db.execute_one("CREATE TABLE b (y INT)")
        db.execute_one("INSERT INTO a VALUES (1)")
        db.execute_one("INSERT INTO b VALUES (2)")
        result = db.execute_one("VACUUM")
        assert result.tag.startswith("VACUUM 2 tables")
        assert rows(db, "SELECT x FROM a") == [(1,)]
        assert rows(db, "SELECT y FROM b") == [(2,)]

    def test_unique_constraint_survives_vacuum(self, db):
        setup_users(db)
        db.execute_one("VACUUM users")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO users VALUES (1, 'dup', 1)")

    def test_vacuum_unknown_table(self, db):
        with pytest.raises(TableNotFoundError):
            db.execute_one("VACUUM nope")


class TestOnConflict:
    def test_do_nothing_skips_duplicate(self, db):
        setup_users(db)
        result = db.execute_one(
            "INSERT INTO users VALUES (1, 'dup', 9) ON CONFLICT DO NOTHING"
        )
        assert result.affected == 0
        assert rows(db, "SELECT name FROM users WHERE id = 1") == [("alice",)]

    def test_do_nothing_mixed_rows(self, db):
        setup_users(db)
        result = db.execute_one(
            "INSERT INTO users VALUES (4, 'dave', 20), (1, 'dup', 1) "
            "ON CONFLICT DO NOTHING"
        )
        assert result.affected == 1
        assert rows(db, "SELECT id FROM users ORDER BY id") == [(1,), (2,), (3,), (4,)]

    def test_do_nothing_leaves_no_dead_version(self, db):
        setup_users(db)
        db.execute_one("VACUUM users")
        db.execute_one("INSERT INTO users VALUES (1, 'dup', 1) ON CONFLICT DO NOTHING")
        result = db.execute_one("VACUUM users")
        assert "0 versions reclaimed" in result.tag

    def test_without_clause_still_errors(self, db):
        setup_users(db)
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO users VALUES (1, 'dup', 1)")


class TestTruncate:
    def test_truncate_clears_rows_and_indexes(self, db):
        setup_users(db)
        result = db.execute_one("TRUNCATE TABLE users")
        assert result.affected == 3
        assert rows(db, "SELECT id FROM users") == []
        plan = "\n".join(
            r[0] for r in rows(db, "EXPLAIN SELECT name FROM users WHERE id = 1")
        )
        assert "IndexScan" in plan
        assert rows(db, "SELECT name FROM users WHERE id = 1") == []
        # 唯一索引已清空，同一个主键可以重新插入
        db.execute_one("INSERT INTO users VALUES (1, 'new', 1)")
        assert rows(db, "SELECT name FROM users WHERE id = 1") == [("new",)]

    def test_truncate_without_table_keyword(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1), (2)")
        assert db.execute_one("TRUNCATE t").affected == 2
        assert rows(db, "SELECT a FROM t") == []

    def test_truncate_unknown_table(self, db):
        with pytest.raises(TableNotFoundError):
            db.execute_one("TRUNCATE nope")


class TestDdl:
    def test_create_and_show(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        result = db.execute_one("SHOW TABLES")
        assert result.rows[0][0] == "t"

    def test_if_not_exists(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE TABLE IF NOT EXISTS t (a INT)")
        assert db.execute_one("SHOW TABLES").rows[0][0] == "t"

    def test_duplicate_table(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        with pytest.raises(Exception):
            db.execute_one("CREATE TABLE t (a INT)")

    def test_drop_table(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("DROP TABLE t")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM t")

    def test_drop_table_if_exists(self, db):
        db.execute_one("DROP TABLE IF EXISTS nope")

    def test_create_index(self, db):
        setup_users(db)
        assert db.execute_one("CREATE INDEX idx_age ON users (age)").tag == "CREATE INDEX"

    def test_create_index_backfills(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_name ON users (name)")
        # 索引回填后按索引查询必须能查到
        assert rows(db, "SELECT id FROM users WHERE name = 'bob'") == [(2,)]

    def test_unique_index_creation_fails_on_duplicates(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1), (1)")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("CREATE UNIQUE INDEX uq ON t (a)")

    def test_drop_index(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_age ON users (age)")
        db.execute_one("DROP INDEX idx_age")
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]


class TestInsert:
    def test_basic(self, db):
        setup_users(db)
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]

    def test_with_column_list(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t (a) VALUES (1)")
        assert rows(db, "SELECT a, b FROM t") == [(1, None)]

    def test_not_null_violation(self, db):
        setup_users(db)
        with pytest.raises(NotNullViolationError):
            db.execute_one("INSERT INTO users VALUES (4, NULL, 20)")

    def test_primary_key_duplicate(self, db):
        setup_users(db)
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO users VALUES (1, 'dup', 1)")

    def test_wrong_column_count(self, db):
        setup_users(db)
        with pytest.raises(Exception):
            db.execute_one("INSERT INTO users VALUES (1, 'x')")

    def test_type_coercion(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES ('42')")
        assert rows(db, "SELECT a FROM t") == [(42,)]

    def test_int_range_checked(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        with pytest.raises(Exception):
            db.execute_one("INSERT INTO t VALUES (99999999999)")


class TestSelectBasics:
    def test_star(self, db):
        setup_users(db)
        assert len(rows(db, "SELECT * FROM users")) == 3

    def test_projection(self, db):
        setup_users(db)
        assert rows(db, "SELECT name FROM users ORDER BY id") == [
            ("alice",),
            ("bob",),
            ("carol",),
        ]

    def test_where(self, db):
        setup_users(db)
        assert rows(db, "SELECT name FROM users WHERE age > 26 ORDER BY id") == [
            ("alice",),
            ("carol",),
        ]

    def test_alias(self, db):
        setup_users(db)
        result = db.execute_one("SELECT name AS n FROM users ORDER BY id LIMIT 1")
        assert result.columns[0].name == "n"
        assert result.rows == [("alice",)]

    def test_order_desc(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users ORDER BY id DESC") == [(3,), (2,), (1,)]

    def test_order_by_two_keys(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 2), (1, 1), (2, 5)")
        assert rows(db, "SELECT a, b FROM t ORDER BY a, b") == [(1, 1), (1, 2), (2, 5)]

    def test_limit_offset(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1") == [(2,)]

    def test_distinct(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1), (1), (2)")
        assert rows(db, "SELECT DISTINCT a FROM t ORDER BY a") == [(1,), (2,)]

    def test_no_from(self, db):
        assert rows(db, "SELECT 1") == [(1,)]

    def test_expression_in_projection(self, db):
        setup_users(db)
        assert rows(db, "SELECT age * 2 FROM users WHERE id = 1") == [(60,)]


class TestExplainAnalyze:
    def test_reports_actual_time_and_rows(self, db):
        setup_users(db)
        plan = "\n".join(
            r[0] for r in rows(db, "EXPLAIN ANALYZE SELECT name FROM users WHERE age > 30")
        )
        assert "actual time=" in plan
        assert "rows=1" in plan
        assert "Execution:" in plan
        assert "SeqScan" in plan

    def test_uses_index_and_shows_it(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_age ON users (age)")
        plan = "\n".join(
            r[0] for r in rows(db, "EXPLAIN ANALYZE SELECT id FROM users WHERE age > 30")
        )
        assert "IndexRangeScan" in plan
        assert "rows=1" in plan

    def test_query_still_works_after_analyze(self, db):
        setup_users(db)
        rows(db, "EXPLAIN ANALYZE SELECT id FROM users")
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]


class TestIndexLookup:
    def test_uses_index_in_explain(self, db):
        setup_users(db)
        plan = "\n".join(r[0] for r in rows(db, "EXPLAIN SELECT name FROM users WHERE id = 2"))
        assert "IndexScan" in plan
        assert "users_pkey" in plan

    def test_secondary_index_in_explain(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_name ON users (name)")
        plan = "\n".join(r[0] for r in rows(db, "EXPLAIN SELECT id FROM users WHERE name = 'bob'"))
        assert "idx_name" in plan

    def test_index_lookup_returns_row(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_name ON users (name)")
        assert rows(db, "SELECT id FROM users WHERE name = 'carol'") == [(3,)]

    def test_index_lookup_missing_value(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_name ON users (name)")
        assert rows(db, "SELECT id FROM users WHERE name = 'nobody'") == []

    def test_no_index_falls_back_to_scan(self, db):
        setup_users(db)
        plan = "\n".join(r[0] for r in rows(db, "EXPLAIN SELECT * FROM users WHERE age = 30"))
        assert "SeqScan" in plan

    def test_index_stays_consistent_after_update(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_age ON users (age)")
        db.execute_one("UPDATE users SET age = 40 WHERE id = 2")
        assert rows(db, "SELECT id FROM users WHERE age = 40") == [(2,)]
        assert rows(db, "SELECT id FROM users WHERE age = 25") == []

    def test_index_stays_consistent_after_delete(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_age ON users (age)")
        db.execute_one("DELETE FROM users WHERE id = 1")
        assert rows(db, "SELECT id FROM users WHERE age = 30") == []

    def test_null_not_indexed(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, NULL)")
        db.execute_one("CREATE INDEX idx_b ON t (b)")
        assert rows(db, "SELECT a FROM t WHERE b = 1") == []
        assert rows(db, "SELECT COUNT(*) FROM t") == [(1,)]


class TestAggregate:
    def test_count_star(self, db):
        setup_users(db)
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]

    def test_count_column_skips_nulls(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1), (NULL), (3)")
        assert rows(db, "SELECT COUNT(a) FROM t") == [(2,)]

    def test_sum_avg_min_max(self, db):
        setup_users(db)
        assert rows(db, "SELECT SUM(age), AVG(age), MIN(age), MAX(age) FROM users") == [
            (90, 30.0, 25, 35)
        ]

    def test_group_by(self, db):
        db.execute_one("CREATE TABLE t (k INT, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)")
        assert rows(db, "SELECT k, SUM(v) FROM t GROUP BY k ORDER BY k") == [(1, 30), (2, 30)]

    def test_having_filters_groups(self, db):
        db.execute_one("CREATE TABLE t (k INT, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30), (2, 1)")
        assert rows(db, "SELECT k FROM t GROUP BY k HAVING SUM(v) > 30 ORDER BY k") == [(2,)]
        assert rows(db, "SELECT k FROM t GROUP BY k HAVING COUNT(*) > 5") == []

    def test_group_by_expression(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1), (2), (3), (4), (5), (6)")
        assert rows(
            db, "SELECT id % 2, COUNT(*) FROM t GROUP BY id % 2 ORDER BY id % 2"
        ) == [(0, 3), (1, 3)]
        # 别名可以用于 ORDER BY，分组表达式按同形匹配
        assert rows(
            db,
            "SELECT id % 2 AS parity, COUNT(*) AS n FROM t "
            "GROUP BY id % 2 ORDER BY parity DESC",
        ) == [(1, 3), (0, 3)]

    def test_group_by_expression_in_having(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        assert rows(
            db,
            "SELECT id % 2 FROM t GROUP BY id % 2 HAVING COUNT(*) > 2",
        ) == [(1,)]

    def test_distinct_aggregates(self, db):
        db.execute_one("CREATE TABLE t (v INT)")
        db.execute_one("INSERT INTO t VALUES (10), (10), (20), (NULL)")
        assert rows(
            db, "SELECT COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) FROM t"
        ) == [(2, 30, 15.0)]

    def test_string_concat(self, db):
        setup_users(db)
        db.execute_one("UPDATE users SET name = name || '!' WHERE id = 1")
        assert rows(db, "SELECT name FROM users WHERE id = 1") == [("alice!",)]
        assert rows(db, "SELECT 'a' || 'b'") == [("ab",)]
        assert rows(db, "SELECT 'a' || NULL") == [(None,)]

    def test_aggregate_on_empty_table(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        assert rows(db, "SELECT COUNT(*), SUM(a) FROM t") == [(0, None)]

    def test_group_by_without_agg(self, db):
        db.execute_one("CREATE TABLE t (k INT)")
        db.execute_one("INSERT INTO t VALUES (1), (1), (2)")
        assert rows(db, "SELECT k FROM t GROUP BY k ORDER BY k") == [(1,), (2,)]

    def test_aggregate_rejects_bad_type(self, db):
        db.execute_one("CREATE TABLE t (s TEXT)")
        with pytest.raises(Exception):
            db.execute_one("SELECT SUM(s) FROM t")


class TestIndexRange:
    def _ages(self, db):
        db.execute_one("CREATE TABLE t (id INT, age INT)")
        db.execute_one("CREATE INDEX idx_age ON t (age)")
        db.execute_one(
            "INSERT INTO t VALUES "
            + ", ".join(f"({i}, {10 + i})" for i in range(10))
        )

    def test_explain_shows_range_scan(self, db):
        self._ages(db)
        plan = "\n".join(r[0] for r in rows(db, "EXPLAIN SELECT id FROM t WHERE age > 12"))
        assert "IndexRangeScan" in plan
        assert "idx_age" in plan

    def test_range_operators_and_boundaries(self, db):
        self._ages(db)
        assert rows(db, "SELECT id FROM t WHERE age > 12 ORDER BY id") == [
            (3,), (4,), (5,), (6,), (7,), (8,), (9,)
        ]
        assert rows(db, "SELECT id FROM t WHERE age >= 12 ORDER BY id") == [
            (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)
        ]
        assert rows(db, "SELECT id FROM t WHERE age < 12 ORDER BY id") == [(0,), (1,)]
        assert rows(db, "SELECT id FROM t WHERE age <= 12 ORDER BY id") == [
            (0,), (1,), (2,)
        ]
        assert rows(db, "SELECT id FROM t WHERE 12 < age ORDER BY id") == [
            (3,), (4,), (5,), (6,), (7,), (8,), (9,)
        ]
        assert rows(db, "SELECT id FROM t WHERE age BETWEEN 12 AND 14 ORDER BY id") == [
            (2,), (3,), (4,)
        ]

    def test_range_excludes_all_duplicate_boundary_values(self, db):
        """边界值的 RID 不能让它"漏"进结果。"""
        db.execute_one("CREATE TABLE t (age INT)")
        db.execute_one("CREATE INDEX idx_age ON t (age)")
        db.execute_one("INSERT INTO t VALUES (30), (30), (30), (31)")
        assert rows(db, "SELECT COUNT(*) FROM t WHERE age > 30") == [(1,)]
        assert rows(db, "SELECT COUNT(*) FROM t WHERE age >= 30") == [(4,)]
        assert rows(db, "SELECT COUNT(*) FROM t WHERE age < 31") == [(3,)]

    def test_range_on_text_index(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_name ON users (name)")
        assert rows(db, "SELECT id FROM users WHERE name > 'b' ORDER BY id") == [
            (2,), (3,)
        ]
        assert rows(db, "SELECT id FROM users WHERE name <= 'b' ORDER BY id") == [(1,)]

    def test_range_without_index_falls_back(self, db):
        setup_users(db)
        plan = "\n".join(r[0] for r in rows(db, "EXPLAIN SELECT id FROM users WHERE age > 30"))
        assert "SeqScan" in plan
        assert rows(db, "SELECT id FROM users WHERE age > 30") == [(3,)]


class TestUpdateDelete:
    def test_update_all(self, db):
        setup_users(db)
        assert db.execute_one("UPDATE users SET age = 99").affected == 3

    def test_update_where(self, db):
        setup_users(db)
        assert db.execute_one("UPDATE users SET age = 1 WHERE id = 1").affected == 1
        assert rows(db, "SELECT age FROM users WHERE id = 1") == [(1,)]

    def test_update_uses_old_value(self, db):
        setup_users(db)
        db.execute_one("UPDATE users SET age = age + 1 WHERE id = 1")
        assert rows(db, "SELECT age FROM users WHERE id = 1") == [(31,)]

    def test_delete_where(self, db):
        setup_users(db)
        assert db.execute_one("DELETE FROM users WHERE age < 30").affected == 1
        assert rows(db, "SELECT id FROM users ORDER BY id") == [(1,), (3,)]

    def test_delete_all(self, db):
        setup_users(db)
        assert db.execute_one("DELETE FROM users").affected == 3
        assert rows(db, "SELECT COUNT(*) FROM users") == [(0,)]

    def test_delete_updates_unique_index(self, db):
        setup_users(db)
        db.execute_one("DELETE FROM users WHERE id = 1")
        db.execute_one("INSERT INTO users VALUES (1, 'new', 1)")

    def test_update_preserves_row_count(self, db):
        setup_users(db)
        db.execute_one("UPDATE users SET age = 50")
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]

    def test_update_multi_page_does_not_self_feed(self, db):
        """回归：跨页 UPDATE 曾经边扫边写，把新版本又当成待更新行，无限增长。"""
        db.execute_one("CREATE TABLE big (id INT PRIMARY KEY, v INT)")
        for start in range(1, 501, 100):
            values = ", ".join(f"({i}, {i})" for i in range(start, start + 100))
            db.execute_one(f"INSERT INTO big VALUES {values}")
        db.execute_one("UPDATE big SET v = v + 1")
        assert rows(db, "SELECT COUNT(*) FROM big") == [(500,)]
        assert rows(db, "SELECT v FROM big WHERE id = 500") == [(501,)]


class TestCaseWhen:
    def setup(self, db):
        db.execute_one("CREATE TABLE people (id INT PRIMARY KEY, age INT, name TEXT)")
        db.execute_one("INSERT INTO people VALUES (1, 10, 'a'), (2, 20, 'b'), (3, NULL, 'c')")

    def test_searched_case(self, db):
        self.setup(db)
        assert rows(db, "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM people ORDER BY id") == [
            ("minor",),
            ("adult",),
            ("adult",),
        ]

    def test_simple_case_compares_operand(self, db):
        self.setup(db)
        assert rows(db, "SELECT CASE id WHEN 2 THEN 'two' WHEN 3 THEN 'three' ELSE 'other' END FROM people ORDER BY id") == [
            ("other",),
            ("two",),
            ("three",),
        ]

    def test_else_is_optional(self, db):
        self.setup(db)
        assert rows(db, "SELECT CASE WHEN age IS NULL THEN 0 END FROM people ORDER BY id") == [
            (None,),
            (None,),
            (0,),
        ]

    def test_null_operand_matches_nothing(self, db):
        self.setup(db)
        assert rows(db, "SELECT CASE age WHEN 10 THEN 'ten' ELSE 'none' END FROM people ORDER BY id") == [
            ("ten",),
            ("none",),
            ("none",),
        ]

    def test_case_inside_aggregate(self, db):
        self.setup(db)
        assert rows(db, "SELECT SUM(CASE WHEN age >= 18 THEN 1 ELSE 0 END) FROM people") == [(1,)]

    def test_branch_type_mismatch_is_rejected(self, db):
        self.setup(db)
        with pytest.raises(TypeMismatchError):
            db.execute_one("SELECT CASE WHEN id = 1 THEN 1 ELSE 'x' END FROM people")

    def test_non_boolean_when_condition_is_rejected(self, db):
        self.setup(db)
        with pytest.raises(TypeMismatchError):
            db.execute_one("SELECT CASE WHEN age THEN 1 END FROM people")

    def test_parser_requires_at_least_one_branch(self):
        from lumendb.sql.parser import parse_statement

        with pytest.raises(SQLSyntaxError):
            parse_statement("SELECT CASE END")


class TestJoin:
    def setup(self, db):
        db.execute_one("CREATE TABLE a (id INT, x TEXT)")
        db.execute_one("CREATE TABLE b (id INT, y TEXT)")
        db.execute_one("INSERT INTO a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3')")
        db.execute_one("INSERT INTO b VALUES (1, 'b1'), (3, 'b3')")

    def test_inner_join(self, db):
        self.setup(db)
        assert rows(
            db, "SELECT a.id, b.y FROM a JOIN b ON a.id = b.id ORDER BY a.id"
        ) == [(1, "b1"), (3, "b3")]

    def test_left_join(self, db):
        self.setup(db)
        assert rows(
            db, "SELECT a.id, b.y FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id"
        ) == [(1, "b1"), (2, None), (3, "b3")]

    def test_comma_join(self, db):
        self.setup(db)
        assert rows(db, "SELECT COUNT(*) FROM a, b") == [(6,)]

    def test_ambiguous_column(self, db):
        self.setup(db)
        with pytest.raises(Exception):
            db.execute_one("SELECT id FROM a JOIN b ON a.id = b.id")

    def test_explain_uses_hash_join(self, db):
        self.setup(db)
        plan = "\n".join(
            r[0] for r in rows(db, "EXPLAIN SELECT a.id FROM a JOIN b ON a.id = b.id")
        )
        assert "HashJoin (INNER)" in plan
        assert "a.id = b.id" in plan

    def test_comma_join_stays_nested_loop(self, db):
        self.setup(db)
        plan = "\n".join(
            r[0] for r in rows(db, "EXPLAIN SELECT a.id FROM a, b")
        )
        assert "HashJoin" not in plan

    def test_hash_join_composite_key(self, db):
        db.execute_one("CREATE TABLE a (id INT, x INT)")
        db.execute_one("CREATE TABLE b (id INT, y INT)")
        db.execute_one("INSERT INTO a VALUES (1, 10), (2, 20), (2, 21)")
        db.execute_one("INSERT INTO b VALUES (1, 10), (2, 20), (2, 99), (2, 21)")
        assert rows(
            db,
            "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id AND a.x = b.y",
        ) == [(3,)]

    def test_hash_join_null_keys_never_match(self, db):
        db.execute_one("CREATE TABLE a (id INT, x TEXT)")
        db.execute_one("CREATE TABLE b (id INT, y TEXT)")
        db.execute_one("INSERT INTO a VALUES (1, 'a'), (NULL, 'n'), (2, 'b')")
        db.execute_one("INSERT INTO b VALUES (1, 'x'), (NULL, 'y'), (2, 'z')")
        assert rows(
            db,
            "SELECT a.x, b.y FROM a LEFT JOIN b ON a.id = b.id WHERE a.id IS NULL",
        ) == [("n", None)]
        assert rows(
            db,
            "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id",
        ) == [(2,)]

    def test_hash_join_duplicate_keys(self, db):
        self.setup(db)
        db.execute_one("INSERT INTO b VALUES (1, 'b1-again'), (1, 'b1-third')")
        assert rows(
            db,
            "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id",
        ) == [(4,)]

    def test_hash_join_residual_predicate(self, db):
        db.execute_one("CREATE TABLE a (id INT, x INT)")
        db.execute_one("CREATE TABLE b (id INT, y INT)")
        db.execute_one("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)")
        db.execute_one("INSERT INTO b VALUES (1, 15), (2, 10), (3, 30)")
        assert rows(
            db,
            "SELECT a.id FROM a JOIN b ON a.id = b.id AND a.x < b.y ORDER BY a.id",
        ) == [(1,)]

    def test_three_way_hash_join(self, db):
        self.setup(db)
        db.execute_one("CREATE TABLE c (id INT, z TEXT)")
        db.execute_one("INSERT INTO c VALUES (1, 'c1'), (3, 'c3')")
        assert rows(
            db,
            "SELECT a.id, b.y, c.z FROM a JOIN b ON a.id = b.id "
            "JOIN c ON b.id = c.id ORDER BY a.id",
        ) == [(1, "b1", "c1"), (3, "b3", "c3")]


class TestAlterTable:
    def test_add_column_with_default_pads_old_rows(self, db):
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("ALTER TABLE t ADD COLUMN age INT DEFAULT 18")
        assert rows(db, "SELECT id, name, age FROM t ORDER BY id") == [
            (1, "a", 18), (2, "b", 18)
        ]
        db.execute_one("INSERT INTO t (id, name) VALUES (3, 'c')")
        assert rows(db, "SELECT age FROM t WHERE id = 3") == [(18,)]

    def test_add_nullable_column_pads_null(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("ALTER TABLE t ADD COLUMN note TEXT")
        assert rows(db, "SELECT id, note FROM t") == [(1, None)]

    def test_old_rows_can_be_updated_and_indexed(self, db):
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("ALTER TABLE t ADD COLUMN age INT DEFAULT 10")
        db.execute_one("UPDATE t SET age = 20 WHERE id = 2")
        db.execute_one("CREATE INDEX idx_age ON t (age)")
        assert rows(db, "SELECT id FROM t WHERE age = 10 ORDER BY id") == [(1,)]
        assert rows(db, "SELECT id FROM t WHERE age = 20 ORDER BY id") == [(2,)]

    def test_duplicate_column_rejected(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        with pytest.raises(Exception):
            db.execute_one("ALTER TABLE t ADD COLUMN id INT")

    def test_not_null_without_default_rejected(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE t ADD COLUMN age INT NOT NULL")

    def test_survives_reopen(self, tmp_path):
        path = tmp_path / "alter_db"
        db = Database(path)
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a')")
        db.execute_one("ALTER TABLE t ADD COLUMN age INT DEFAULT 7")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT id, name, age FROM t") == [(1, "a", 7)]
        db2.execute_one("VACUUM")
        assert rows(db2, "SELECT age FROM t") == [(7,)]
        db2.close()


class TestUnion:
    def test_union_all_keeps_duplicates(self, db):
        assert rows(db, "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 2") == [
            (1,), (2,), (2,)
        ]

    def test_union_dedupes(self, db):
        assert rows(db, "SELECT 1 AS x UNION SELECT 1 UNION SELECT 2") == [(1,), (2,)]

    def test_union_null_is_one_row(self, db):
        assert rows(db, "SELECT NULL AS x UNION SELECT NULL") == [(None,)]

    def test_union_order_by_output_alias_and_limit(self, db):
        assert rows(
            db,
            "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 "
            "ORDER BY x DESC LIMIT 2",
        ) == [(3,), (2,)]

    def test_union_order_by_ordinal(self, db):
        assert rows(
            db,
            "SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY 1",
        ) == [(1,), (2,), (3,)]

    def test_union_different_sources(self, db):
        setup_users(db)
        assert rows(
            db,
            "SELECT id FROM users WHERE id < 3 "
            "UNION ALL SELECT id FROM users WHERE id = 2",
        ) == [(1,), (2,), (2,)]

    def test_union_column_count_mismatch(self, db):
        with pytest.raises(TypeMismatchError):
            db.execute_one("SELECT 1, 2 UNION ALL SELECT 1")

    def test_union_type_mismatch(self, db):
        with pytest.raises(TypeMismatchError):
            db.execute_one("SELECT 1 AS x UNION ALL SELECT 'a'")

    def test_union_explain_plan(self, db):
        plan = "\n".join(
            r[0] for r in rows(db, "EXPLAIN SELECT 1 UNION ALL SELECT 2")
        )
        assert "UnionAll" in plan


class TestExpressions:
    def test_like(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users WHERE name LIKE 'a%'") == [(1,)]

    def test_like_underscore(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users WHERE name LIKE 'b_b'") == [(2,)]

    def test_not_like(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users WHERE name NOT LIKE 'a%' ORDER BY id") == [(2,), (3,)]

    def test_in(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users WHERE id IN (1, 3) ORDER BY id") == [(1,), (3,)]

    def test_between(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users WHERE age BETWEEN 25 AND 30 ORDER BY id") == [
            (1,),
            (2,),
        ]

    def test_is_null(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1), (NULL)")
        assert rows(db, "SELECT COUNT(*) FROM t WHERE a IS NULL") == [(1,)]

    def test_not(self, db):
        setup_users(db)
        assert rows(db, "SELECT COUNT(*) FROM users WHERE NOT age > 26") == [(1,)]

    def test_cast(self, db):
        setup_users(db)
        assert rows(db, "SELECT CAST('7' AS INT)") == [(7,)]

    def test_scalar_function_length(self, db):
        setup_users(db)
        assert rows(db, "SELECT LENGTH(name) FROM users WHERE id = 1") == [(5,)]

    def test_upper(self, db):
        setup_users(db)
        assert rows(db, "SELECT UPPER(name) FROM users WHERE id = 1") == [("ALICE",)]

    def test_coalesce(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (NULL, 5)")
        assert rows(db, "SELECT COALESCE(a, b) FROM t") == [(5,)]

    def test_division_by_zero(self, db):
        with pytest.raises(Exception):
            db.execute_one("SELECT 1 / 0")

    def test_string_comparison_error(self, db):
        setup_users(db)
        with pytest.raises(Exception):
            db.execute_one("SELECT id FROM users WHERE name > 5")


class TestSubquery:
    def test_scalar_subquery(self, db):
        setup_users(db)
        assert rows(db, "SELECT (SELECT COUNT(*) FROM users)") == [(3,)]

    def test_in_subquery(self, db):
        setup_users(db)
        db.execute_one("CREATE TABLE vip (id INT)")
        db.execute_one("INSERT INTO vip VALUES (1), (3)")
        assert rows(db, "SELECT id FROM users WHERE id IN (SELECT id FROM vip) ORDER BY id") == [
            (1,),
            (3,),
        ]

    def test_exists(self, db):
        setup_users(db)
        assert rows(db, "SELECT EXISTS (SELECT 1 FROM users)") == [(True,)]


class TestTransactions:
    def test_rollback_discards_changes(self, db):
        setup_users(db)
        db.execute("BEGIN")
        db.execute("INSERT INTO users VALUES (9, 'ghost', 1)")
        db.execute("ROLLBACK")
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]

    def test_commit_persists(self, db):
        setup_users(db)
        db.execute("BEGIN")
        db.execute("INSERT INTO users VALUES (9, 'kept', 1)")
        db.execute("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM users") == [(4,)]

    def test_double_begin_rejected(self, db):
        db.execute("BEGIN")
        with pytest.raises(Exception):
            db.execute("BEGIN")
        db.execute("ROLLBACK")

    def test_commit_without_begin(self, db):
        with pytest.raises(Exception):
            db.execute("COMMIT")


class TestPersistence:
    def test_data_survives_reopen(self, tmp_path):
        path = tmp_path / "db"
        db = Database(path)
        setup_users(db)
        db.execute_one("CREATE INDEX idx_age ON users (age)")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT COUNT(*) FROM users") == [(3,)]
        assert rows(db2, "SELECT id FROM users WHERE age = 30") == [(1,)]
        db2.close()

    def test_index_survives_reopen(self, tmp_path):
        path = tmp_path / "db"
        db = Database(path)
        setup_users(db)
        db.execute_one("CREATE INDEX idx_name ON users (name)")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT id FROM users WHERE name = 'alice'") == [(1,)]
        db2.close()

    def test_checkpoint(self, db):
        setup_users(db)
        assert db.execute_one("CHECKPOINT").tag == "CHECKPOINT"
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]


class TestErrorQuality:
    def test_unknown_column_has_hint(self, db):
        setup_users(db)
        with pytest.raises(ColumnNotFoundError) as exc:
            db.execute_one("SELECT nope FROM users")
        assert "id" in str(exc.value)

    def test_unsupported_syntax(self, db):
        with pytest.raises(NotSupportedError):
            db.execute_one("WITH x AS (SELECT 1) SELECT * FROM x")

    def test_keyword_as_name_rejected(self, db):
        with pytest.raises(Exception):
            db.execute_one("SELECT FROM")
