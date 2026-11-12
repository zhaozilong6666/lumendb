"""执行引擎的端到端测试：从 SQL 文本一路走到结果行。"""

import pytest

from lumendb.engine.database import Database
from lumendb.errors import (
    ColumnNotFoundError,
    DuplicateKeyError,
    NotNullViolationError,
    NotSupportedError,
    TableNotFoundError,
)


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "db")
    yield database
    database.close()


def rows(db, sql):
    return db.execute_one(sql).rows


def setup_users(db):
    db.execute_one(
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, age INT)"
    )
    db.execute_one(
        "INSERT INTO users VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35)"
    )


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

    def test_having(self, db):
        db.execute_one("CREATE TABLE t (k INT, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)")
        assert rows(db, "SELECT k FROM t GROUP BY k HAVING SUM(v) > 25 ORDER BY k") == [(1,), (2,)]

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
            db.execute_one("ALTER TABLE t ADD COLUMN x INT")

    def test_keyword_as_name_rejected(self, db):
        with pytest.raises(Exception):
            db.execute_one("SELECT FROM")
