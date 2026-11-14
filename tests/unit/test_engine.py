"""执行引擎的端到端测试：从 SQL 文本一路走到结果行。"""

import pytest

from lumendb.engine.database import Database
from lumendb.errors import (
    BinderError,
    CheckViolationError,
    ColumnNotFoundError,
    DuplicateKeyError,
    ForeignKeyViolationError,
    IndexAlreadyExistsError,
    NotNullViolationError,
    NotSupportedError,
    SQLSyntaxError,
    TableNotFoundError,
    TransactionError,
    TypeMismatchError,
    UndefinedParameterError,
    TableAlreadyExistsError,
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

    def test_vacuum_full_and_reindex_table_syntax(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a')")
        db.execute_one("CREATE INDEX idx_name ON t (name)")
        assert "1 rows kept" in db.execute_one("VACUUM FULL t").tag
        assert db.execute_one("REINDEX TABLE t").tag.startswith("REINDEX")
        assert rows(db, "SELECT id FROM t WHERE name = 'a'") == [(1,)]

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

    def test_conflict_update_set_default(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, n INT DEFAULT 5)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 99) "
            "ON CONFLICT DO UPDATE SET n = DEFAULT RETURNING id, n"
        )
        assert result.rows == [(1, 5)]

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

    def test_truncate_cascade(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))"
        )
        db.execute_one(
            "CREATE TABLE g (id INT PRIMARY KEY, cid INT REFERENCES c(id))"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1, 1)")
        db.execute_one("INSERT INTO g VALUES (1, 1)")
        result = db.execute_one("TRUNCATE TABLE p CASCADE")
        assert result.affected == 3
        assert rows(db, "SELECT COUNT(*) FROM p") == [(0,)]
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]
        assert rows(db, "SELECT COUNT(*) FROM g") == [(0,)]

    def test_truncate_cascade_rollback(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("TRUNCATE p CASCADE")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT COUNT(*) FROM p") == [(1,)]
        assert rows(db, "SELECT COUNT(*) FROM c") == [(1,)]

    def test_truncate_multiple_tables_with_parent_child(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1), (2)")
        db.execute_one("INSERT INTO c VALUES (1), (2)")
        result = db.execute_one("TRUNCATE TABLE p, c")
        assert result.affected == 4
        assert rows(db, "SELECT COUNT(*) FROM p") == [(0,)]
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]

    def test_truncate_multiple_rejects_missing_dependent(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
        db.execute_one("CREATE TABLE g (cid INT REFERENCES c(id))")
        with pytest.raises(BinderError, match="不能直接 TRUNCATE"):
            db.execute_one("TRUNCATE p, c")

    def test_truncate_multiple_cascade_includes_transitive_children(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))")
        db.execute_one("CREATE TABLE g (cid INT REFERENCES c(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1, 1)")
        db.execute_one("INSERT INTO g VALUES (1)")
        result = db.execute_one("TRUNCATE p, c CASCADE")
        assert result.affected == 3
        assert rows(db, "SELECT COUNT(*) FROM p") == [(0,)]
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]
        assert rows(db, "SELECT COUNT(*) FROM g") == [(0,)]

    def test_truncate_multiple_rejects_duplicate_name(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        with pytest.raises(BinderError, match="重复指定"):
            db.execute_one("TRUNCATE t, t")


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

    def test_create_index_if_not_exists(self, db):
        setup_users(db)
        db.execute_one("CREATE INDEX idx_age ON users (age)")
        result = db.execute_one("CREATE INDEX IF NOT EXISTS idx_age ON users (age)")
        assert result.tag == "CREATE INDEX"
        assert "已存在" in result.notice
        with pytest.raises(IndexAlreadyExistsError):
            db.execute_one("CREATE INDEX idx_age ON users (age)")

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

    def test_default_values(self, db):
        db.execute_one(
            "CREATE TABLE t (id INT PRIMARY KEY DEFAULT 1, "
            "name TEXT DEFAULT 'anon', age INT)"
        )
        result = db.execute_one(
            "INSERT INTO t DEFAULT VALUES RETURNING id, name, age"
        )
        assert result.affected == 1
        assert result.rows == [(1, "anon", None)]
        db.execute_one(
            "INSERT INTO t DEFAULT VALUES ON CONFLICT DO NOTHING"
        )
        assert rows(db, "SELECT COUNT(*) FROM t") == [(1,)]

    def test_default_values_missing_not_null_rejected(self, db):
        db.execute_one("CREATE TABLE t (id INT NOT NULL)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("INSERT INTO t DEFAULT VALUES")

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

    def test_insert_select(self, db):
        db.execute_one("CREATE TABLE src (id INT, name TEXT)")
        db.execute_one("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        db.execute_one("CREATE TABLE dst (id INT, name TEXT)")
        result = db.execute_one(
            "INSERT INTO dst SELECT id, name FROM src WHERE id >= 2"
        )
        assert result.affected == 2
        assert rows(db, "SELECT id, name FROM dst ORDER BY id") == [
            (2, "b"), (3, "c")
        ]

    def test_insert_select_into_column_list(self, db):
        db.execute_one("CREATE TABLE src (id INT, name TEXT)")
        db.execute_one("INSERT INTO src VALUES (1, 'a')")
        db.execute_one("CREATE TABLE dst (id INT, name TEXT, age INT)")
        db.execute_one("INSERT INTO dst (id, name) SELECT id, name FROM src")
        assert rows(db, "SELECT id, name, age FROM dst") == [(1, "a", None)]

    def test_insert_select_from_self_is_materialized(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1), (2)")
        db.execute_one("INSERT INTO t SELECT * FROM t")
        assert rows(db, "SELECT COUNT(*) FROM t") == [(4,)]

    def test_insert_select_column_count_mismatch(self, db):
        db.execute_one("CREATE TABLE src (a INT, b INT)")
        db.execute_one("CREATE TABLE dst (a INT)")
        with pytest.raises(BinderError):
            db.execute_one("INSERT INTO dst SELECT a, b FROM src")

    def test_insert_select_on_conflict(self, db):
        db.execute_one("CREATE TABLE src (id INT)")
        db.execute_one("INSERT INTO src VALUES (1), (1), (2)")
        db.execute_one("CREATE TABLE dst (id INT PRIMARY KEY)")
        result = db.execute_one(
            "INSERT INTO dst SELECT id FROM src ON CONFLICT DO NOTHING"
        )
        assert result.affected == 2
        assert rows(db, "SELECT COUNT(*) FROM dst") == [(2,)]

    def test_on_conflict_do_update_existing_row(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, n INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 20) "
            "ON CONFLICT DO UPDATE SET n = n + 5 RETURNING id, n"
        )
        assert result.affected == 1
        assert result.rows == [(1, 15)]
        assert rows(db, "SELECT id, n FROM t") == [(1, 15)]

    def test_column_unique_constraint_is_enforced(self, db):
        db.execute_one("CREATE TABLE t (id INT, name TEXT UNIQUE)")
        db.execute_one("INSERT INTO t VALUES (1, 'same')")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (2, 'same')")

    def test_table_constraints_composite_unique(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT, b INT, CONSTRAINT uq_ab UNIQUE (a, b))"
        )
        db.execute_one("INSERT INTO t VALUES (1, 2)")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1, 2)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 2) "
            "ON CONFLICT ON CONSTRAINT uq_ab DO UPDATE SET b = 3 "
            "RETURNING a, b"
        )
        assert result.affected == 1
        assert result.rows == [(1, 3)]

    def test_table_constraints_composite_primary(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT, b INT, CONSTRAINT pk_ab PRIMARY KEY (a, b))"
        )
        db.execute_one("INSERT INTO t VALUES (1, 2)")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1, 2)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("INSERT INTO t VALUES (NULL, 3)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 2) "
            "ON CONFLICT ON CONSTRAINT pk_ab DO UPDATE SET b = 3 RETURNING a, b"
        )
        assert result.affected == 1
        assert result.rows == [(1, 3)]

    def test_table_constraints_survive_reopen(self, tmp_path):
        path = tmp_path / "reopen"
        first = Database(path)
        first.execute_one(
            "CREATE TABLE t (a INT, b INT, CONSTRAINT uq_ab UNIQUE (a, b))"
        )
        first.execute_one("INSERT INTO t VALUES (1, 2)")
        first.close()
        second = Database(path)
        with pytest.raises(DuplicateKeyError):
            second.execute_one("INSERT INTO t VALUES (1, 2)")
        second.execute_one("INSERT INTO t VALUES (1, 3)")
        second.close()

    def test_table_constraints_reject_bad_ddl(self, db):
        with pytest.raises(BinderError):
            db.execute_one(
                "CREATE TABLE t1 (a INT, b INT, UNIQUE (a, missing))"
            )
        with pytest.raises(NotSupportedError):
            db.execute_one(
                "CREATE TABLE t2 (a INT PRIMARY KEY, b INT, PRIMARY KEY (a, b))"
            )
        with pytest.raises(NotSupportedError):
            db.execute_one(
                "CREATE TABLE t3 (a INT, b INT, PRIMARY KEY (a), PRIMARY KEY (b))"
            )

    def test_alter_add_unique_constraint(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 2)")
        db.execute_one(
            "ALTER TABLE t ADD CONSTRAINT uq_ab UNIQUE (a, b)"
        )
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1, 2)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 2) "
            "ON CONFLICT ON CONSTRAINT uq_ab DO NOTHING"
        )
        assert result.affected == 0

    def test_alter_add_unique_rejects_existing_duplicates(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 2), (1, 2)")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("ALTER TABLE t ADD UNIQUE (a, b)")

    def test_alter_add_primary_key(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 2), (3, 4)")
        db.execute_one(
            "ALTER TABLE t ADD CONSTRAINT pk_ab PRIMARY KEY (a, b)"
        )
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1, 2)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("INSERT INTO t VALUES (NULL, 5)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 2) "
            "ON CONFLICT ON CONSTRAINT pk_ab DO NOTHING"
        )
        assert result.affected == 0

    def test_alter_add_primary_key_rejects_nulls_and_duplicates(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, NULL)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("ALTER TABLE t ADD PRIMARY KEY (a, b)")
        db.execute_one("CREATE TABLE u (a INT, b INT)")
        db.execute_one("INSERT INTO u VALUES (1, 2), (1, 2)")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("ALTER TABLE u ADD PRIMARY KEY (a, b)")

    def test_alter_add_primary_key_rollback_restores_table(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 2)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t ADD PRIMARY KEY (a, b)")
        db.execute_one("ROLLBACK")
        # 回滚后不应残留主键索引/列标志，重复行仍可插入。
        db.execute_one("INSERT INTO t VALUES (1, 2)")

    def test_view_select_and_reflects_writes(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        db.execute_one(
            "CREATE VIEW v AS SELECT a, b FROM t WHERE a > 0"
        )
        assert rows(db, "SELECT * FROM v ORDER BY a") == [(1, "x"), (2, "y")]
        db.execute_one("INSERT INTO t VALUES (3, 'z')")
        assert rows(db, "SELECT b FROM v WHERE a = 3") == [("z",)]
        db.execute_one("DROP VIEW v")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM v")

    def test_view_can_reference_view(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        db.execute_one("CREATE VIEW v1 AS SELECT a FROM t")
        db.execute_one("CREATE VIEW v2 AS SELECT a FROM v1")
        assert rows(db, "SELECT * FROM v2") == [(1,)]

    def test_view_in_join_with_alias(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        db.execute_one("CREATE TABLE u (a INT, c TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        db.execute_one("INSERT INTO u VALUES (1, 'y')")
        db.execute_one("CREATE VIEW v AS SELECT a, b FROM t")
        assert rows(
            db,
            "SELECT x.a, x.b, u.c FROM v AS x JOIN u ON x.a = u.a",
        ) == [(1, "x", "y")]

    def test_view_survives_reopen(self, tmp_path):
        path = tmp_path / "viewdb"
        first = Database(path)
        first.execute_one("CREATE TABLE t (a INT, b TEXT)")
        first.execute_one("INSERT INTO t VALUES (1, 'x')")
        first.execute_one("CREATE VIEW v (x, y) AS SELECT a, b FROM t")
        first.close()
        second = Database(path)
        assert second.execute_one("SELECT x, y FROM v").rows == [(1, "x")]
        second.close()

    def test_view_dependencies_survive_reopen(self, tmp_path):
        path = tmp_path / "viewdeps"
        first = Database(path)
        first.execute_one("CREATE TABLE t (a INT)")
        first.execute_one("CREATE VIEW v AS SELECT a FROM t")
        first.close()
        second = Database(path)
        with pytest.raises(BinderError):
            second.execute_one("DROP TABLE t")
        second.execute_one("DROP VIEW v")
        second.execute_one("DROP TABLE t")
        second.close()

    def test_create_or_replace_view_swaps_definition(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        assert rows(db, "SELECT * FROM v") == [(1,)]
        db.execute_one("CREATE OR REPLACE VIEW v AS SELECT b FROM t")
        assert rows(db, "SELECT * FROM v") == [("x",)]
        with pytest.raises(TableAlreadyExistsError):
            db.execute_one("CREATE VIEW v AS SELECT a FROM t")

    def test_show_views(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        result = db.execute_one("SHOW VIEWS")
        assert result.rows[0][0] == "v"
        assert "SELECT" in result.rows[0][1]

    def test_view_column_list_renames_output(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        db.execute_one("CREATE VIEW v (x, y) AS SELECT a, b FROM t")
        assert rows(db, "SELECT x, y FROM v") == [(1, "x")]
        with pytest.raises(BinderError):
            db.execute_one("CREATE VIEW bad (x) AS SELECT a, b FROM t")

    def test_view_with_cte_column_names(self, db):
        db.execute_one(
            "CREATE VIEW v AS WITH c(n) AS (SELECT 1) SELECT n FROM c"
        )
        assert rows(db, "SELECT * FROM v") == [(1,)]

    def test_create_view_rollback(self, db):
        db.execute_one("BEGIN")
        db.execute_one("CREATE VIEW v AS SELECT 1")
        db.execute_one("ROLLBACK")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM v")

    def test_or_replace_view_failure_keeps_old(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        with pytest.raises(ColumnNotFoundError):
            db.execute_one("CREATE OR REPLACE VIEW v AS SELECT nope FROM t")
        assert rows(db, "SELECT * FROM v") == [(1,)]

    def test_view_is_read_only(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        with pytest.raises(TableNotFoundError):
            db.execute_one("INSERT INTO v VALUES (1)")

    def test_view_supports_aggregate_and_setop(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE TABLE u (a INT)")
        db.execute_one("INSERT INTO t VALUES (1), (1), (2)")
        db.execute_one("INSERT INTO u VALUES (2), (3)")
        db.execute_one(
            "CREATE VIEW v AS SELECT a FROM t UNION SELECT a FROM u"
        )
        assert rows(db, "SELECT * FROM v ORDER BY a") == [(1,), (2,), (3,)]
        db.execute_one("CREATE VIEW counts AS SELECT count(*) AS n FROM t")
        assert rows(db, "SELECT n FROM counts") == [(3,)]

    def test_view_definition_can_use_cte(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1), (2)")
        db.execute_one(
            "CREATE VIEW v AS WITH x AS (SELECT a FROM t WHERE a > 1) "
            "SELECT a FROM x"
        )
        assert rows(db, "SELECT * FROM v") == [(2,)]

    def test_cte_explicit_column_names(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        assert rows(
            db,
            "WITH c(x, y) AS (SELECT a, b FROM t) SELECT x, y FROM c",
        ) == [(1, "x")]
        with pytest.raises(BinderError):
            db.execute_one("WITH c(x) AS (SELECT a, b FROM t) SELECT * FROM c")
        with pytest.raises(BinderError):
            db.execute_one("WITH c(x, x) AS (SELECT a, b FROM t) SELECT * FROM c")

    def test_recursive_cte_generates_sequence(self, db):
        result = rows(
            db,
            "WITH RECURSIVE c AS ("
            "SELECT 1 AS n UNION ALL "
            "SELECT n + 1 FROM c WHERE n < 5"
            ") SELECT n FROM c",
        )
        assert result == [(1,), (2,), (3,), (4,), (5,)]

    def test_recursive_cte_column_list(self, db):
        assert rows(
            db,
            "WITH RECURSIVE c(n) AS ("
            "SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3"
            ") SELECT n FROM c",
        ) == [(1,), (2,), (3,)]

    def test_recursive_cte_union_deduplicates(self, db):
        result = rows(
            db,
            "WITH RECURSIVE c AS ("
            "SELECT 1 AS n UNION "
            "SELECT n + 1 FROM c WHERE n < 3"
            ") SELECT n FROM c",
        )
        assert result == [(1,), (2,), (3,)]

    def test_view_definition_can_use_recursive_cte(self, db):
        db.execute_one(
            "CREATE VIEW v AS WITH RECURSIVE c AS ("
            "SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 3"
            ") SELECT n FROM c"
        )
        assert rows(db, "SELECT * FROM v") == [(1,), (2,), (3,)]

    def test_recursive_cte_walks_a_table_hierarchy(self, db):
        db.execute_one("CREATE TABLE tree (id INT, parent INT)")
        db.execute_one(
            "INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2), (4, 1)"
        )
        result = rows(
            db,
            "WITH RECURSIVE descendants AS ("
            "SELECT id, parent FROM tree WHERE parent IS NULL "
            "UNION ALL "
            "SELECT t.id, t.parent FROM tree AS t "
            "JOIN descendants AS d ON t.parent = d.id"
            ") SELECT id FROM descendants ORDER BY id",
        )
        assert result == [(1,), (2,), (3,), (4,)]

    def test_order_by_nulls_first_last(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (NULL), (1), (2)")
        assert rows(db, "SELECT a FROM t ORDER BY a ASC NULLS FIRST") == [
            (None,),
            (1,),
            (2,),
        ]
        assert rows(db, "SELECT a FROM t ORDER BY a ASC NULLS LAST") == [
            (1,),
            (2,),
            (None,),
        ]
        assert rows(db, "SELECT a FROM t ORDER BY a DESC NULLS FIRST") == [
            (None,),
            (2,),
            (1,),
        ]
        assert rows(db, "SELECT a FROM t ORDER BY a DESC NULLS LAST") == [
            (2,),
            (1,),
            (None,),
        ]

    def test_drop_table_blocked_by_view_dependency(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        with pytest.raises(BinderError):
            db.execute_one("DROP TABLE t")
        db.execute_one("DROP VIEW v")
        db.execute_one("DROP TABLE t")

    def test_drop_view_blocked_by_dependent_view(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v1 AS SELECT a FROM t")
        db.execute_one("CREATE VIEW v2 AS SELECT a FROM v1")
        with pytest.raises(BinderError):
            db.execute_one("DROP VIEW v1")
        db.execute_one("DROP VIEW v2")
        db.execute_one("DROP VIEW v1")

    def test_or_replace_view_cannot_reference_itself(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        with pytest.raises(BinderError):
            db.execute_one("CREATE OR REPLACE VIEW v AS SELECT a FROM v")
        assert rows(db, "SELECT * FROM v") == []

    def test_rename_table_blocked_by_view_dependency(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t RENAME TO u")
        db.execute_one("DROP VIEW v")
        db.execute_one("ALTER TABLE t RENAME TO u")
        db.execute_one("SELECT * FROM u")

    def test_drop_table_cascade_removes_views(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        db.execute_one("DROP TABLE t CASCADE")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM v")
        assert rows(db, "SHOW VIEWS") == []

    def test_replace_view_refreshes_indirect_dependencies(self, db):
        db.execute_one("CREATE TABLE old (a INT)")
        db.execute_one("CREATE TABLE new (a INT)")
        db.execute_one("INSERT INTO old VALUES (1)")
        db.execute_one("INSERT INTO new VALUES (2)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM old")
        db.execute_one("CREATE VIEW top AS SELECT a FROM v")
        db.execute_one("CREATE OR REPLACE VIEW v AS SELECT a FROM new")
        assert rows(db, "SELECT * FROM top") == [(2,)]
        # 旧表不再被引用，可以删除；新表仍被间接依赖，必须拒绝。
        db.execute_one("DROP TABLE old")
        with pytest.raises(BinderError):
            db.execute_one("DROP TABLE new")

    def test_replace_view_rejected_when_dependent_breaks(self, db):
        db.execute_one("CREATE TABLE t (a INT, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 2)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        db.execute_one("CREATE VIEW top AS SELECT a FROM v")
        with pytest.raises(BinderError):
            db.execute_one("CREATE OR REPLACE VIEW v AS SELECT b FROM t")
        # 自动提交语句失败时整体回滚，旧视图仍可用。
        assert rows(db, "SELECT * FROM top") == [(1,)]

    def test_replace_view_dependency_rollback(self, db):
        db.execute_one("CREATE TABLE old (a INT)")
        db.execute_one("CREATE TABLE new (a INT)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM old")
        db.execute_one("CREATE VIEW top AS SELECT a FROM v")
        db.execute_one("BEGIN")
        db.execute_one("CREATE OR REPLACE VIEW v AS SELECT a FROM new")
        db.execute_one("ROLLBACK")
        with pytest.raises(BinderError):
            db.execute_one("DROP TABLE old")
        db.execute_one("DROP TABLE new")

    def test_drop_view_cascade_removes_dependents(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE VIEW v1 AS SELECT a FROM t")
        db.execute_one("CREATE VIEW v2 AS SELECT a FROM v1")
        db.execute_one("DROP VIEW v1 CASCADE")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM v2")

    def test_drop_cascade_rollback_restores_views(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("CREATE VIEW v AS SELECT a FROM t")
        db.execute_one("BEGIN")
        db.execute_one("DROP TABLE t CASCADE")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT * FROM v") == [(1,)]

    def test_on_conflict_do_update_uses_target_index(self, db):
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a')")
        db.execute_one("CREATE UNIQUE INDEX u_name ON t (name)")
        db.execute_one(
            "INSERT INTO t VALUES (2, 'a') "
            "ON CONFLICT (name) DO UPDATE SET id = 2"
        )
        assert rows(db, "SELECT id, name FROM t") == [(2, "a")]

    def test_on_conflict_do_update_inserts_when_no_conflict(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, n INT)")
        result = db.execute_one(
            "INSERT INTO t VALUES (2, 20) ON CONFLICT DO UPDATE SET n = 99"
        )
        assert result.affected == 1
        assert rows(db, "SELECT * FROM t") == [(2, 20)]

    def test_on_conflict_do_update_requires_matching_unique_index(self, db):
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        with pytest.raises(BinderError):
            db.execute_one(
                "INSERT INTO t VALUES (1, 'a') "
                "ON CONFLICT (name) DO UPDATE SET id = 2"
            )

    def test_on_conflict_do_update_aborts_on_other_unique_conflict(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("CREATE UNIQUE INDEX u_name ON t (name)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        with pytest.raises(DuplicateKeyError):
            db.execute_one(
                "INSERT INTO t VALUES (1, 'b') "
                "ON CONFLICT (id) DO UPDATE SET name = 'b'"
            )
        assert rows(db, "SELECT id, name FROM t ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]

    def test_on_conflict_do_update_where_condition(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, n INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")

        result = db.execute_one(
            "INSERT INTO t VALUES (1, 20) "
            "ON CONFLICT DO UPDATE SET n = 20 WHERE n < 10"
        )
        assert result.affected == 0
        assert rows(db, "SELECT id, n FROM t") == [(1, 10)]

        result = db.execute_one(
            "INSERT INTO t VALUES (1, 20) "
            "ON CONFLICT DO UPDATE SET n = 20 WHERE n = 10 RETURNING id, n"
        )
        assert result.affected == 1
        assert result.rows == [(1, 20)]
        assert rows(db, "SELECT id, n FROM t") == [(1, 20)]

    def test_on_conflict_do_update_where_condition_insert_select(self, db):
        db.execute_one("CREATE TABLE src (id INT)")
        db.execute_one("INSERT INTO src VALUES (1)")
        db.execute_one("CREATE TABLE dst (id INT PRIMARY KEY, n INT)")
        db.execute_one("INSERT INTO dst VALUES (1, 10)")
        result = db.execute_one(
            "INSERT INTO dst SELECT id, 20 FROM src "
            "ON CONFLICT DO UPDATE SET n = 20 WHERE n = 10"
        )
        assert result.affected == 1
        assert rows(db, "SELECT id, n FROM dst") == [(1, 20)]

    def test_on_conflict_do_update_uses_excluded_values(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, n INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 20) "
            "ON CONFLICT DO UPDATE SET n = n + excluded.n RETURNING id, n"
        )
        assert result.affected == 1
        assert result.rows == [(1, 30)]

    def test_on_conflict_do_update_where_uses_excluded(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, n INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 20) "
            "ON CONFLICT DO UPDATE SET n = excluded.n "
            "WHERE excluded.n > n RETURNING id, n"
        )
        assert result.affected == 1
        assert result.rows == [(1, 20)]

    def test_on_conflict_on_constraint_target(self, db):
        db.execute_one(
            "CREATE TABLE t (id INT PRIMARY KEY, name TEXT UNIQUE, n INT)"
        )
        db.execute_one("INSERT INTO t VALUES (1, 'a', 10)")
        result = db.execute_one(
            "INSERT INTO t VALUES (1, 'b', 20) "
            "ON CONFLICT ON CONSTRAINT t_pkey "
            "DO UPDATE SET n = excluded.n RETURNING id, name, n"
        )
        assert result.affected == 1
        assert result.rows == [(1, "a", 20)]

    def test_on_conflict_on_constraint_do_nothing(self, db):
        db.execute_one(
            "CREATE TABLE t (id INT PRIMARY KEY, name TEXT UNIQUE, n INT)"
        )
        db.execute_one("INSERT INTO t VALUES (1, 'a', 10)")
        result = db.execute_one(
            "INSERT INTO t VALUES (2, 'a', 20) "
            "ON CONFLICT ON CONSTRAINT t_name_key DO NOTHING"
        )
        assert result.affected == 0
        assert rows(db, "SELECT id, name, n FROM t") == [(1, "a", 10)]

    def test_on_conflict_on_constraint_missing(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        with pytest.raises(BinderError):
            db.execute_one(
                "INSERT INTO t VALUES (1, 'a') "
                "ON CONFLICT ON CONSTRAINT no_such_constraint DO NOTHING"
            )

    def test_on_conflict_excluded_unknown_column(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, n INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        with pytest.raises(ColumnNotFoundError):
            db.execute_one(
                "INSERT INTO t VALUES (1, 20) "
                "ON CONFLICT DO UPDATE SET n = excluded.missing"
            )

    def test_on_conflict_do_nothing_with_target_only_skips_target(self, db):
        db.execute_one(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT UNIQUE, b INT UNIQUE)"
        )
        db.execute_one("INSERT INTO t VALUES (1, 1, 1)")

        # a 冲突：按目标索引跳过。
        result = db.execute_one(
            "INSERT INTO t VALUES (2, 1, 9) ON CONFLICT (a) DO NOTHING"
        )
        assert result.affected == 0

        # a 不冲突、b 冲突：目标索引以外的唯一约束必须报错。
        with pytest.raises(DuplicateKeyError):
            db.execute_one(
                "INSERT INTO t VALUES (3, 3, 1) ON CONFLICT (a) DO NOTHING"
            )
        assert rows(db, "SELECT id, a, b FROM t ORDER BY id") == [(1, 1, 1)]

    def test_insert_select_on_conflict_do_update(self, db):
        db.execute_one("CREATE TABLE src (id INT)")
        db.execute_one("INSERT INTO src VALUES (1), (2)")
        db.execute_one("CREATE TABLE dst (id INT PRIMARY KEY, seen INT)")
        db.execute_one("INSERT INTO dst VALUES (1, 0)")
        result = db.execute_one(
            "INSERT INTO dst SELECT id, id FROM src "
            "ON CONFLICT DO UPDATE SET seen = 1"
        )
        assert result.affected == 2
        assert rows(db, "SELECT id, seen FROM dst ORDER BY id") == [
            (1, 1),
            (2, 2),
        ]

    def test_insert_select_with_param(self, db):
        db.execute_one("CREATE TABLE src (id INT)")
        db.execute_one("INSERT INTO src VALUES (1), (2)")
        db.execute_one("CREATE TABLE dst (id INT)")
        result = db.execute_one(
            "INSERT INTO dst SELECT id FROM src WHERE id = $1", [2]
        )
        assert result.affected == 1
        assert rows(db, "SELECT id FROM dst") == [(2,)]

    def test_with_insert_select(self, db):
        db.execute_one("CREATE TABLE src (id INT)")
        db.execute_one("INSERT INTO src VALUES (1), (2), (3)")
        db.execute_one("CREATE TABLE dst (id INT)")
        result = db.execute_one(
            "WITH even AS (SELECT id FROM src WHERE id % 2 = 0) "
            "INSERT INTO dst SELECT id FROM even"
        )
        assert result.affected == 1
        assert rows(db, "SELECT id FROM dst") == [(2,)]

    def test_with_insert_values_validates_cte(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        with pytest.raises(TableNotFoundError):
            db.execute_one(
                "WITH bad AS (SELECT * FROM no_such_table) "
                "INSERT INTO t VALUES (1)"
            )


class TestCreateTableAs:
    def test_basic(self, db):
        setup_users(db)
        result = db.execute_one(
            "CREATE TABLE adults AS "
            "SELECT id, name FROM users WHERE age >= 30"
        )
        assert result.affected == 2
        assert rows(db, "SELECT id, name FROM adults ORDER BY id") == [
            (1, "alice"), (3, "carol")
        ]

    def test_expression_alias(self, db):
        setup_users(db)
        db.execute_one(
            "CREATE TABLE stats AS SELECT COUNT(*) AS n FROM users"
        )
        assert rows(db, "SELECT n FROM stats") == [(3,)]

    def test_zero_rows_still_creates_table(self, db):
        setup_users(db)
        result = db.execute_one(
            "CREATE TABLE empty AS SELECT id FROM users WHERE id > 100"
        )
        assert result.affected == 0
        assert rows(db, "SELECT COUNT(*) FROM empty") == [(0,)]

    def test_union_source(self, db):
        db.execute_one("CREATE TABLE a (id INT)")
        db.execute_one("CREATE TABLE b (id INT)")
        db.execute_one("INSERT INTO a VALUES (1), (2)")
        db.execute_one("INSERT INTO b VALUES (2), (3)")
        db.execute_one(
            "CREATE TABLE merged AS SELECT id FROM a UNION SELECT id FROM b"
        )
        assert rows(db, "SELECT id FROM merged ORDER BY id") == [(1,), (2,), (3,)]

    def test_requires_alias_for_expression_name(self, db):
        with pytest.raises(BinderError):
            db.execute_one("CREATE TABLE t AS SELECT 1")

    def test_duplicate_output_names_rejected(self, db):
        with pytest.raises(BinderError):
            db.execute_one("CREATE TABLE t AS SELECT 1 AS x, 2 AS x")

    def test_if_not_exists_keeps_existing_table(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        result = db.execute_one(
            "CREATE TABLE IF NOT EXISTS t AS SELECT 2 AS id"
        )
        assert "已跳过" in (result.notice or "")
        assert rows(db, "SELECT id FROM t") == [(1,)]


class TestReturning:
    def test_insert_returning(self, db):
        setup_users(db)
        result = db.execute_one(
            "INSERT INTO users VALUES (4, 'dave', 40) RETURNING id, name AS n"
        )
        assert result.affected == 1
        assert result.rows == [(4, "dave")]
        assert [c.name for c in result.columns] == ["id", "n"]

    def test_insert_returning_star(self, db):
        setup_users(db)
        result = db.execute_one(
            "INSERT INTO users VALUES (4, 'dave', 40) RETURNING *"
        )
        assert result.rows == [(4, "dave", 40)]
        assert [c.name for c in result.columns] == ["id", "name", "age"]

    def test_insert_select_returning(self, db):
        setup_users(db)
        result = db.execute_one(
            "INSERT INTO users SELECT 4, 'dave', 40 RETURNING name"
        )
        assert result.rows == [("dave",)]

    def test_update_returning_uses_new_row(self, db):
        setup_users(db)
        result = db.execute_one(
            "UPDATE users SET age = age + 1 WHERE id = 2 RETURNING id, age"
        )
        assert result.rows == [(2, 26)]
        assert result.affected == 1

    def test_delete_returning_uses_old_row(self, db):
        setup_users(db)
        result = db.execute_one(
            "DELETE FROM users WHERE id = 2 RETURNING id, name"
        )
        assert result.rows == [(2, "bob")]
        assert result.affected == 1
        assert rows(db, "SELECT COUNT(*) FROM users") == [(2,)]

    def test_on_conflict_returns_only_inserted_rows(self, db):
        setup_users(db)
        result = db.execute_one(
            "INSERT INTO users VALUES (1, 'x', 1) "
            "ON CONFLICT DO NOTHING RETURNING id"
        )
        assert result.affected == 0
        assert result.rows == []

    def test_returning_unknown_column_does_not_write(self, db):
        setup_users(db)
        with pytest.raises(ColumnNotFoundError):
            db.execute_one(
                "INSERT INTO users VALUES (4, 'dave', 40) RETURNING nope"
            )
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]


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

    def test_limit_all_and_sql_standard_pagination(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users ORDER BY id LIMIT ALL") == [
            (1,), (2,), (3,)
        ]
        assert rows(
            db,
            "SELECT id FROM users ORDER BY id OFFSET 1 ROW "
            "FETCH NEXT 2 ROWS ONLY",
        ) == [(2,), (3,)]
        assert rows(
            db, "SELECT id FROM users ORDER BY id FETCH FIRST ROW ONLY"
        ) == [(1,)]
        assert rows(
            db,
            "SELECT id FROM users ORDER BY id FETCH NEXT $1 ROWS ONLY",
            params=[2],
        ) == [(1,), (2,)]
        assert rows(
            db, "SELECT id FROM users ORDER BY id OFFSET 2 ROWS"
        ) == [(3,)]

    def test_fetch_with_ties_keeps_peer_rows(self, db):
        db.execute_one("CREATE TABLE t (id INT, score INT)")
        db.execute_one(
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 20), (4, 30)"
        )
        assert rows(
            db,
            "SELECT id FROM t ORDER BY score FETCH FIRST 2 ROWS WITH TIES",
        ) == [(1,), (2,), (3,)]
        assert rows(
            db,
            "SELECT id FROM t ORDER BY score OFFSET 1 ROW "
            "FETCH NEXT 2 ROWS WITH TIES",
        ) == [(2,), (3,)]
        assert rows(
            db,
            "SELECT id FROM t ORDER BY score FETCH FIRST 2 ROWS ONLY",
        ) == [(1,), (2,)]
        assert rows(
            db,
            "SELECT id FROM t ORDER BY score DESC FETCH FIRST ROW WITH TIES",
        ) == [(4,)]
        assert rows(
            db, "SELECT id FROM t FETCH FIRST 2 ROWS WITH TIES"
        ) == [(1,), (2,), (3,), (4,)]
        assert rows(
            db,
            "SELECT 2 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 "
            "ORDER BY x FETCH FIRST ROW WITH TIES",
        ) == [(2,), (2,)]

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

    def test_update_set_default(self, db):
        db.execute_one(
            "CREATE TABLE t (id INT, a INT DEFAULT 5, b TEXT DEFAULT 'x', c INT)"
        )
        db.execute_one("INSERT INTO t (id) VALUES (1)")
        result = db.execute_one(
            "UPDATE t SET a = DEFAULT, b = DEFAULT, c = DEFAULT "
            "WHERE id = 1 RETURNING id, a, b, c"
        )
        assert result.rows == [(1, 5, "x", None)]
        db.execute_one("ALTER TABLE t ALTER COLUMN a SET DEFAULT 9")
        db.execute_one("UPDATE t SET a = DEFAULT WHERE id = 1")
        assert rows(db, "SELECT a FROM t WHERE id = 1") == [(9,)]

    def test_update_default_missing_not_null_rejected(self, db):
        db.execute_one("CREATE TABLE t (id INT, n INT NOT NULL)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("UPDATE t SET n = DEFAULT")

    def test_update_from(self, db):
        db.execute_one("CREATE TABLE t (id INT, v INT)")
        db.execute_one("CREATE TABLE s (id INT, delta INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        db.execute_one("INSERT INTO s VALUES (1, 5), (2, -2)")
        db.execute_one("CREATE INDEX idx_t_v ON t (v)")
        result = db.execute_one(
            "UPDATE t SET v = t.v + s.delta FROM s "
            "WHERE t.id = s.id RETURNING t.id, t.v"
        )
        assert result.affected == 2
        assert sorted(result.rows) == [(1, 15), (2, 18)]
        assert rows(db, "SELECT id, v FROM t ORDER BY id") == [
            (1, 15),
            (2, 18),
            (3, 30),
        ]
        assert rows(db, "SELECT id FROM t WHERE v = 15") == [(1,)]

    def test_update_from_empty_source(self, db):
        db.execute_one("CREATE TABLE t (id INT, v INT)")
        db.execute_one("CREATE TABLE s (id INT, delta INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        result = db.execute_one(
            "UPDATE t SET v = s.delta FROM s WHERE t.id = s.id"
        )
        assert result.affected == 0

    def test_delete_where(self, db):
        setup_users(db)
        assert db.execute_one("DELETE FROM users WHERE age < 30").affected == 1
        assert rows(db, "SELECT id FROM users ORDER BY id") == [(1,), (3,)]

    def test_delete_all(self, db):
        setup_users(db)
        assert db.execute_one("DELETE FROM users").affected == 3
        assert rows(db, "SELECT COUNT(*) FROM users") == [(0,)]

    def test_delete_using(self, db):
        db.execute_one("CREATE TABLE t (id INT, v INT)")
        db.execute_one("CREATE TABLE s (id INT, flag INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        db.execute_one("INSERT INTO s VALUES (1, 1), (2, 0)")
        db.execute_one("CREATE INDEX idx_t_id ON t (id)")
        result = db.execute_one(
            "DELETE FROM t USING s WHERE t.id = s.id AND s.flag = 1 "
            "RETURNING t.id, t.v"
        )
        assert result.affected == 1
        assert result.rows == [(1, 10)]
        assert rows(db, "SELECT id, v FROM t ORDER BY id") == [
            (2, 20),
            (3, 30),
        ]
        assert rows(db, "SELECT id FROM t WHERE id = 2") == [(2,)]

    def test_delete_using_empty_source(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("CREATE TABLE s (id INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        result = db.execute_one(
            "DELETE FROM t USING s WHERE t.id = s.id"
        )
        assert result.affected == 0
        assert rows(db, "SELECT COUNT(*) FROM t") == [(1,)]

    def test_delete_updates_unique_index(self, db):
        setup_users(db)
        db.execute_one("DELETE FROM users WHERE id = 1")
        db.execute_one("INSERT INTO users VALUES (1, 'new', 1)")

    def test_update_preserves_row_count(self, db):
        setup_users(db)
        db.execute_one("UPDATE users SET age = 50")
        assert rows(db, "SELECT COUNT(*) FROM users") == [(3,)]

    def test_update_unique_conflict_aborts(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("CREATE UNIQUE INDEX u_name ON t (name)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("UPDATE t SET name = 'b' WHERE id = 1")
        assert rows(db, "SELECT id, name FROM t ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]

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

    def test_right_join(self, db):
        self.setup(db)
        db.execute_one("INSERT INTO b VALUES (4, 'b4')")
        assert rows(
            db,
            "SELECT a.id, b.id, b.y FROM a RIGHT JOIN b ON a.id = b.id "
            "ORDER BY b.id",
        ) == [(1, 1, "b1"), (3, 3, "b3"), (None, 4, "b4")]

    def test_full_join(self, db):
        self.setup(db)
        db.execute_one("INSERT INTO b VALUES (4, 'b4')")
        result = rows(
            db,
            "SELECT a.id, b.id, b.y FROM a FULL JOIN b ON a.id = b.id",
        )
        assert set(result) == {
            (1, 1, "b1"),
            (2, None, None),
            (3, 3, "b3"),
            (None, 4, "b4"),
        }

    def test_cross_join(self, db):
        self.setup(db)
        assert rows(db, "SELECT COUNT(*) FROM a CROSS JOIN b") == [(6,)]

    def test_right_join_null_key_does_not_match(self, db):
        db.execute_one("CREATE TABLE l (id INT)")
        db.execute_one("CREATE TABLE r (id INT)")
        db.execute_one("INSERT INTO l VALUES (1), (NULL)")
        db.execute_one("INSERT INTO r VALUES (1), (NULL)")
        assert rows(
            db, "SELECT l.id, r.id FROM l RIGHT JOIN r ON l.id = r.id ORDER BY r.id"
        ) == [(1, 1), (None, None)]

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

    def test_outer_join_explain_is_nested_loop(self, db):
        self.setup(db)
        plan = "\n".join(
            r[0]
            for r in rows(
                db, "EXPLAIN SELECT a.id FROM a FULL JOIN b ON a.id = b.id"
            )
        )
        assert "HashJoin" not in plan
        assert "FULL Join" in plan

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
    def test_add_column_if_not_exists(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("ALTER TABLE t ADD COLUMN IF NOT EXISTS age INT")
        result = db.execute_one(
            "ALTER TABLE t ADD COLUMN IF NOT EXISTS age TEXT"
        )
        assert "已跳过" in result.notice
        assert rows(db, "SELECT id, age FROM t") == [(1, None)]
        db.execute_one("INSERT INTO t VALUES (2, 30)")
        assert rows(db, "SELECT id, age FROM t ORDER BY id") == [
            (1, None), (2, 30)
        ]
        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t ADD COLUMN age TEXT")

    def test_add_column_with_default_pads_old_rows(self, db):
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("ALTER TABLE t ADD COLUMN age INT DEFAULT 18")
        assert rows(db, "SELECT id, name, age FROM t ORDER BY id") == [
            (1, "a", 18), (2, "b", 18)
        ]
        db.execute_one("INSERT INTO t (id, name) VALUES (3, 'c')")
        assert rows(db, "SELECT age FROM t WHERE id = 3") == [(18,)]

    def test_alter_column_default_and_drop(self, db):
        db.execute_one("CREATE TABLE t (a INT DEFAULT 5, b INT)")
        db.execute_one("INSERT INTO t (b) VALUES (1)")
        db.execute_one("ALTER TABLE t ALTER COLUMN a SET DEFAULT 7")
        db.execute_one("INSERT INTO t (b) VALUES (2)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t ALTER COLUMN a SET DEFAULT 9")
        db.execute_one("ROLLBACK")
        db.execute_one("INSERT INTO t (b) VALUES (3)")
        db.execute_one("ALTER TABLE t ALTER COLUMN a DROP DEFAULT")
        db.execute_one("INSERT INTO t (b) VALUES (4)")
        assert rows(db, "SELECT a, b FROM t ORDER BY b") == [
            (5, 1),
            (7, 2),
            (7, 3),
            (None, 4),
        ]

    def test_alter_column_nullability(self, db):
        db.execute_one("CREATE TABLE t (id INT, age INT)")
        db.execute_one("INSERT INTO t VALUES (1, NULL)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("ALTER TABLE t ALTER COLUMN age SET NOT NULL")
        # 失败后目录保持原状，NULL 仍可写入
        db.execute_one("INSERT INTO t VALUES (2, NULL)")
        db.execute_one("UPDATE t SET age = 30 WHERE id IN (1, 2)")
        result = db.execute_one("ALTER TABLE t ALTER age SET NOT NULL")
        assert "已校验 2 行" in result.notice
        with pytest.raises(NotNullViolationError):
            db.execute_one("INSERT INTO t VALUES (3, NULL)")
        db.execute_one("ALTER TABLE t ALTER COLUMN age DROP NOT NULL")
        db.execute_one("INSERT INTO t VALUES (4, NULL)")
        assert rows(db, "SELECT id, age FROM t ORDER BY id") == [
            (1, 30),
            (2, 30),
            (4, None),
        ]

    def test_alter_column_not_null_rollback(self, db):
        db.execute_one("CREATE TABLE t (id INT, age INT)")
        db.execute_one("INSERT INTO t VALUES (1, 20)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t ALTER COLUMN age SET NOT NULL")
        db.execute_one("ROLLBACK")
        db.execute_one("INSERT INTO t VALUES (2, NULL)")
        assert rows(db, "SELECT id, age FROM t ORDER BY id") == [
            (1, 20),
            (2, None),
        ]

    def test_drop_not_null_on_primary_key_rejected(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t ALTER COLUMN id DROP NOT NULL")

    def test_alter_column_not_null_survives_reopen(self, tmp_path):
        path = tmp_path / "not_null_reopen"
        first = Database(path)
        first.execute_one("CREATE TABLE t (id INT, age INT)")
        first.execute_one("INSERT INTO t VALUES (1, 20)")
        first.execute_one("ALTER TABLE t ALTER COLUMN age SET NOT NULL")
        first.close()

        second = Database(path)
        with pytest.raises(NotNullViolationError):
            second.execute_one("INSERT INTO t VALUES (2, NULL)")
        second.close()

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

    def test_rename_constraint(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT, b INT, CONSTRAINT uq UNIQUE (a), "
            "CONSTRAINT ck CHECK (b > 0), CONSTRAINT ck_other CHECK (b < 100))"
        )
        db.execute_one("CREATE INDEX idx_b ON t (b)")
        db.execute_one("INSERT INTO t VALUES (1, 1)")
        db.execute_one("ALTER TABLE t RENAME CONSTRAINT uq TO uq2")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1, 2)")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT uq2")
        db.execute_one("INSERT INTO t VALUES (1, 2)")

        db.execute_one("ALTER TABLE t RENAME CONSTRAINT ck TO ck2")
        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t RENAME CONSTRAINT ck2 TO ck_other")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT ck2")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT ck_other")
        db.execute_one("INSERT INTO t VALUES (3, 0)")

        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE t RENAME CONSTRAINT idx_b TO idx_c")
        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t RENAME CONSTRAINT missing TO x")

    def test_rename_constraint_rollback(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT, CONSTRAINT uq UNIQUE (a))"
        )
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t RENAME CONSTRAINT uq TO uq2")
        db.execute_one("ROLLBACK")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("INSERT INTO t VALUES (2)")

    def test_rename_table_and_column(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("CREATE INDEX idx_name ON t (name)")
        db.execute_one("ALTER TABLE t RENAME TO t2")
        assert rows(db, "SELECT * FROM t2 ORDER BY id") == [(1, "a"), (2, "b")]
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM t")

        db.execute_one("ALTER TABLE t2 RENAME COLUMN name TO label")
        assert rows(db, "SELECT id, label FROM t2 WHERE label = 'b'") == [(2, "b")]
        with pytest.raises(ColumnNotFoundError):
            db.execute_one("SELECT name FROM t2")

    def test_rename_table_conflict_rejected(self, db):
        db.execute_one("CREATE TABLE a (id INT)")
        db.execute_one("CREATE TABLE b (id INT)")
        with pytest.raises(TableAlreadyExistsError):
            db.execute_one("ALTER TABLE a RENAME TO b")

    def test_rename_survives_reopen(self, tmp_path):
        path = tmp_path / "rename_db"
        db = Database(path)
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("CREATE INDEX idx_name ON t (name)")
        db.execute_one("ALTER TABLE t RENAME TO t2")
        db.execute_one("ALTER TABLE t2 RENAME COLUMN name TO label")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT id, label FROM t2 ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]
        db2.close()

    def test_ddl_rollback_restores_catalog_memory(self, db):
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1)")

        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t RENAME TO t2")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT * FROM t") == [(1,)]
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM t2")

        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t ADD COLUMN note TEXT")
        db.execute_one("ROLLBACK")
        with pytest.raises(ColumnNotFoundError):
            db.execute_one("SELECT note FROM t")

        db.execute_one("BEGIN")
        db.execute_one("CREATE TABLE x (id INT)")
        db.execute_one("ROLLBACK")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM x")

        db.execute_one("BEGIN")
        db.execute_one("DROP TABLE t")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT * FROM t") == [(1,)]

    def test_ddl_rollback_survives_reopen(self, tmp_path):
        path = tmp_path / "rollback_reopen"
        db = Database(path)
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t RENAME TO t2")
        db.execute_one("ROLLBACK")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT * FROM t") == [(1,)]
        with pytest.raises(TableNotFoundError):
            db2.execute_one("SELECT * FROM t2")
        db2.close()

    def test_truncate_rollback_restores_heap_after_reopen(self, tmp_path):
        path = tmp_path / "truncate_rollback"
        db = Database(path)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("BEGIN")
        db.execute_one("TRUNCATE TABLE t")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT id, name FROM t ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT id, name FROM t ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]
        db2.execute_one("INSERT INTO t VALUES (3, 'c')")
        assert rows(db2, "SELECT id FROM t ORDER BY id") == [(1,), (2,), (3,)]
        db2.close()

    def test_vacuum_rollback_restores_rows_and_index(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("CREATE INDEX idx_name ON t (name)")
        db.execute_one("BEGIN")
        db.execute_one("VACUUM")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT id, name FROM t ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]
        assert rows(db, "SELECT id FROM t WHERE name = 'b'") == [(2,)]

    def test_reindex_rollback_restores_index(self, tmp_path):
        path = tmp_path / "reindex_rollback"
        db = Database(path)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("CREATE INDEX idx_name ON t (name)")
        db.execute_one("BEGIN")
        db.execute_one("REINDEX t")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT id FROM t WHERE name = 'b'") == [(2,)]
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT id FROM t WHERE name = 'a'") == [(1,)]
        db2.close()

    def test_create_unique_index_rollback(self, db):
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("BEGIN")
        db.execute_one("CREATE UNIQUE INDEX u_name ON t (name)")
        db.execute_one("ROLLBACK")
        db.execute_one("INSERT INTO t VALUES (1, 'same'), (2, 'same')")
        assert rows(db, "SELECT COUNT(*) FROM t") == [(2,)]

    def test_failed_dml_transaction_restores_table_roots(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO t VALUES (1, 'a')")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1, 'dup')")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT COUNT(*) FROM t") == [(0,)]
        db.execute_one("INSERT INTO t VALUES (2, 'b')")
        assert rows(db, "SELECT * FROM t") == [(2, "b")]

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


class TestAlterColumnType:
    def test_int_to_bigint_rewrites_rows_and_indexes(self, db):
        db.execute_one(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT CHECK (a > 0))"
        )
        db.execute_one("CREATE UNIQUE INDEX uq_a ON t (a)")
        db.execute_one("INSERT INTO t VALUES (1, 1), (2, 2)")
        result = db.execute_one(
            "ALTER TABLE t ALTER COLUMN a TYPE BIGINT"
        )
        assert "重写 2 行" in result.notice
        db.execute_one("INSERT INTO t VALUES (3, 3000000000)")
        assert rows(
            db, "SELECT id FROM t WHERE a = 3000000000"
        ) == [(3,)]
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (4, 1)")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (5, 0)")

    def test_text_to_int_conversion_and_failure_without_mutation(self, db):
        db.execute_one("CREATE TABLE t (a TEXT)")
        db.execute_one("INSERT INTO t VALUES ('123')")
        db.execute_one("ALTER TABLE t ALTER COLUMN a SET DATA TYPE INT")
        db.execute_one("INSERT INTO t VALUES (4)")
        assert rows(db, "SELECT a FROM t ORDER BY a") == [(4,), (123,)]

        db.execute_one("CREATE TABLE u (a TEXT)")
        db.execute_one("INSERT INTO u VALUES ('not-a-number')")
        with pytest.raises(TypeMismatchError):
            db.execute_one("ALTER TABLE u ALTER COLUMN a TYPE INT")
        assert rows(db, "SELECT a FROM u") == [("not-a-number",)]
        db.execute_one("INSERT INTO u VALUES ('still-text')")

    def test_type_change_rollback_and_reopen(self, tmp_path):
        path = tmp_path / "alter_type"
        db = Database(path)
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t ALTER COLUMN a TYPE BIGINT")
        db.execute_one("ROLLBACK")
        with pytest.raises(TypeMismatchError):
            db.execute_one("INSERT INTO t VALUES (2147483648)")
        db.execute_one("ALTER TABLE t ALTER COLUMN a TYPE BIGINT")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT a FROM t") == [(1,)]
        db2.execute_one("INSERT INTO t VALUES (2147483648)")
        assert rows(db2, "SELECT a FROM t ORDER BY a") == [
            (1,),
            (2147483648,),
        ]
        db2.close()

    def test_type_change_primary_key_is_rebuilt(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, note TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a')")
        db.execute_one("ALTER TABLE t ALTER COLUMN id TYPE BIGINT")
        db.execute_one("INSERT INTO t VALUES (3000000000, 'b')")
        assert rows(db, "SELECT note FROM t WHERE id = 3000000000") == [
            ("b",)
        ]
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1, 'dup')")
        with pytest.raises(NotNullViolationError):
            db.execute_one("INSERT INTO t VALUES (NULL, 'null')")

    def test_type_change_unique_conflict_rolls_back(self, db):
        db.execute_one("CREATE TABLE t (a TEXT)")
        db.execute_one("CREATE UNIQUE INDEX uq_a ON t (a)")
        db.execute_one("INSERT INTO t VALUES ('1'), ('01')")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("ALTER TABLE t ALTER COLUMN a TYPE INT")
        assert rows(db, "SELECT a FROM t ORDER BY a") == [("01",), ("1",)]
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES ('1')")
        db.execute_one("INSERT INTO t VALUES ('2')")

    def test_type_change_converts_default_and_preserves_check(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT DEFAULT 7 CHECK (a > 0))"
        )
        db.execute_one("INSERT INTO t DEFAULT VALUES")
        db.execute_one("ALTER TABLE t ALTER COLUMN a TYPE BIGINT")
        db.execute_one("INSERT INTO t DEFAULT VALUES")
        assert rows(db, "SELECT a FROM t ORDER BY a") == [(7,), (7,)]
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (0)")

    def test_varchar_shrink_is_validated_before_mutation(self, db):
        db.execute_one("CREATE TABLE t (a VARCHAR(5))")
        db.execute_one("INSERT INTO t VALUES ('abc')")
        with pytest.raises(TypeMismatchError):
            db.execute_one(
                "ALTER TABLE t ALTER COLUMN a TYPE VARCHAR(2)"
            )
        assert rows(db, "SELECT a FROM t") == [("abc",)]
        db.execute_one("INSERT INTO t VALUES ('abcd')")

    def test_type_change_using_expression(self, db):
        db.execute_one("CREATE TABLE t (a TEXT)")
        db.execute_one("INSERT INTO t VALUES ('123')")
        db.execute_one(
            "ALTER TABLE t ALTER COLUMN a TYPE INT USING CAST(a AS INT)"
        )
        db.execute_one("INSERT INTO t VALUES (4)")
        assert rows(db, "SELECT a FROM t ORDER BY a") == [(4,), (123,)]

        db.execute_one("CREATE TABLE u (a INT)")
        db.execute_one("INSERT INTO u VALUES (5)")
        db.execute_one(
            "ALTER TABLE u ALTER COLUMN a TYPE TEXT USING a || ''"
        )
        db.execute_one("INSERT INTO u VALUES ('x')")
        assert rows(db, "SELECT a FROM u ORDER BY a") == [("5",), ("x",)]

    def test_using_expression_failure_rolls_back(self, db):
        db.execute_one("CREATE TABLE t (a TEXT)")
        db.execute_one("INSERT INTO t VALUES ('not-a-number')")
        with pytest.raises(TypeMismatchError):
            db.execute_one(
                "ALTER TABLE t ALTER COLUMN a TYPE INT USING CAST(a AS INT)"
            )
        assert rows(db, "SELECT a FROM t") == [("not-a-number",)]
        db.execute_one("INSERT INTO t VALUES ('still-text')")


class TestDropColumn:
    def test_drop_column_rebuilds_indexes_and_reopen(self, tmp_path):
        path = tmp_path / "drop_column"
        db = Database(path)
        db.execute_one("CREATE TABLE t (a INT, b TEXT, c INT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x', 10), (2, 'y', 20)")
        db.execute_one("CREATE INDEX idx_a ON t (a)")
        db.execute_one("ALTER TABLE t DROP COLUMN b")
        assert rows(db, "SELECT a, c FROM t ORDER BY a") == [
            (1, 10),
            (2, 20),
        ]
        assert rows(db, "SELECT c FROM t WHERE a = 2") == [(20,)]
        db.execute_one("INSERT INTO t VALUES (3, 30)")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT a, c FROM t ORDER BY a") == [
            (1, 10),
            (2, 20),
            (3, 30),
        ]
        db2.execute_one("INSERT INTO t VALUES (4, 40)")
        db2.close()

    def test_drop_column_guards(self, db):
        db.execute_one(
            "CREATE TABLE t (id INT PRIMARY KEY, b INT CHECK (b > 0), c TEXT UNIQUE)"
        )
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE t DROP COLUMN id")
        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t DROP COLUMN b")
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE t DROP COLUMN c")

        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t DROP COLUMN missing")
        result = db.execute_one(
            "ALTER TABLE t DROP COLUMN IF EXISTS missing"
        )
        assert "不存在" in result.notice

    def test_drop_last_column_rejected(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE t DROP COLUMN a")

    def test_drop_column_rollback(self, db):
        db.execute_one("CREATE TABLE t (a INT, b TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t DROP COLUMN b")
        db.execute_one("INSERT INTO t VALUES (2)")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT a, b FROM t") == [(1, "x")]
        db.execute_one("INSERT INTO t VALUES (3, 'z')")

    def test_drop_column_cascade_removes_referencing_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY, note TEXT)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        with pytest.raises(NotSupportedError, match="外键"):
            db.execute_one("ALTER TABLE p DROP COLUMN id")
        result = db.execute_one("ALTER TABLE p DROP COLUMN id CASCADE")
        assert "级联删除" in result.notice
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        db.execute_one("INSERT INTO c VALUES (99)")

    def test_drop_column_cascade_removes_local_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id), note TEXT)")
        result = db.execute_one("ALTER TABLE c DROP COLUMN pid CASCADE")
        assert "级联删除" in result.notice
        assert db.catalog.get_table("c").foreign_keys == {}
        db.execute_one("INSERT INTO c VALUES ('x')")

    def test_drop_column_cascade_removes_index_and_check(self, db):
        db.execute_one("CREATE TABLE t (a INT CHECK (a > 0), b TEXT)")
        db.execute_one("CREATE UNIQUE INDEX t_a_uq ON t (a)")
        result = db.execute_one("ALTER TABLE t DROP COLUMN a CASCADE")
        assert "级联删除" in result.notice
        assert "t_a_check" not in db.catalog.get_table("t").checks
        assert "t_a_uq" not in db.catalog.get_table("t").indexes
        db.execute_one("INSERT INTO t VALUES ('x')")

    def test_drop_column_cascade_rollback_restores_dependencies(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY, note TEXT)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE p DROP COLUMN id CASCADE")
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        db.execute_one("ROLLBACK")
        assert "p_pkey" in db.catalog.get_table("p").indexes
        assert "c_pid_fkey" in db.catalog.get_table("c").foreign_keys
        db.execute_one("INSERT INTO p VALUES (1, 'x')")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (2)")

    def test_drop_column_preserves_unrelated_check(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT CHECK (a > 0), b TEXT)"
        )
        db.execute_one("INSERT INTO t VALUES (1, 'x')")
        db.execute_one("ALTER TABLE t DROP COLUMN b")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (0)")
        db.execute_one("INSERT INTO t VALUES (2)")
        assert rows(db, "SELECT a FROM t ORDER BY a") == [(1,), (2,)]


class TestDropConstraintIndexes:
    def test_drop_unique_constraint_allows_duplicates(self, db):
        db.execute_one("CREATE TABLE t (a INT UNIQUE, b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT t_a_key")
        db.execute_one("INSERT INTO t VALUES (1, 20)")
        assert rows(db, "SELECT b FROM t ORDER BY b") == [(10,), (20,)]

    def test_drop_primary_key_constraint_keeps_not_null(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT t_pkey")
        db.execute_one("INSERT INTO t VALUES (1)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("INSERT INTO t VALUES (NULL)")
        assert rows(db, "SELECT id FROM t ORDER BY id") == [(1,), (1,)]

    def test_drop_constraint_rejects_plain_index(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("CREATE INDEX idx_a ON t (a)")
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE t DROP CONSTRAINT idx_a")
        db.execute_one("DROP INDEX idx_a")

    def test_drop_unique_constraint_rejects_dependent_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT UNIQUE)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        with pytest.raises(NotSupportedError, match="仍被外键"):
            db.execute_one("ALTER TABLE p DROP CONSTRAINT p_id_key")
        db.execute_one("ALTER TABLE p DROP CONSTRAINT p_id_key CASCADE")
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        db.execute_one("INSERT INTO c VALUES (99)")

    def test_drop_constraint_cascade_rollback_restores_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE p DROP CONSTRAINT p_pkey CASCADE")
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        db.execute_one("ROLLBACK")
        assert "c_pid_fkey" in db.catalog.get_table("c").foreign_keys
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (99)")

    def test_drop_constraint_cascade_counts_multiple_fks(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("CREATE TABLE g (pid INT REFERENCES p(id))")
        result = db.execute_one(
            "ALTER TABLE p DROP CONSTRAINT p_pkey CASCADE"
        )
        assert "级联删除 2 个外键" in result.notice
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        assert "g_pid_fkey" not in db.catalog.get_table("g").foreign_keys

    def test_drop_unique_index_cascade_removes_dependent_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT)")
        db.execute_one("CREATE UNIQUE INDEX p_id_uq ON p (id)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        with pytest.raises(NotSupportedError, match="仍被外键"):
            db.execute_one("DROP INDEX p_id_uq")
        result = db.execute_one("DROP INDEX p_id_uq CASCADE")
        assert "级联删除 1 个外键" in result.notice
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        db.execute_one("INSERT INTO c VALUES (99)")

    def test_drop_index_cascade_rollback_restores_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT)")
        db.execute_one("CREATE UNIQUE INDEX p_id_uq ON p (id)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("BEGIN")
        db.execute_one("DROP INDEX p_id_uq CASCADE")
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        db.execute_one("ROLLBACK")
        assert "c_pid_fkey" in db.catalog.get_table("c").foreign_keys
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (99)")

    def test_drop_table_level_and_added_unique_constraints(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT, b INT, UNIQUE (a, b))"
        )
        db.execute_one("INSERT INTO t VALUES (1, 2)")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT t_a_b_key")
        db.execute_one("INSERT INTO t VALUES (1, 2)")

        db.execute_one("CREATE TABLE u (a INT)")
        db.execute_one("INSERT INTO u VALUES (1)")
        db.execute_one(
            "ALTER TABLE u ADD CONSTRAINT uq_u UNIQUE (a)"
        )
        db.execute_one("ALTER TABLE u DROP CONSTRAINT uq_u")
        db.execute_one("INSERT INTO u VALUES (1)")

    def test_drop_index_rejects_constraint_index_and_rollback(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute_one("INSERT INTO t VALUES (1)")
        with pytest.raises(NotSupportedError):
            db.execute_one("DROP INDEX t_pkey")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT t_pkey")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("ROLLBACK")
        db.execute_one("INSERT INTO t VALUES (2)")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1)")


class TestCheckConstraints:
    def test_column_check_enforced_on_insert_and_update(self, db):
        db.execute_one("CREATE TABLE t (a INT CHECK (a > 0), b INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (0, 10)")
        with pytest.raises(CheckViolationError):
            db.execute_one("UPDATE t SET a = -1 WHERE b = 10")
        db.execute_one("UPDATE t SET a = 2 WHERE b = 10")
        assert rows(db, "SELECT a FROM t") == [(2,)]

    def test_check_null_is_not_a_violation(self, db):
        db.execute_one("CREATE TABLE t (a INT CHECK (a > 0), b INT)")
        db.execute_one("INSERT INTO t VALUES (NULL, 1)")
        assert rows(db, "SELECT a FROM t") == [(None,)]

    def test_table_check_with_multiple_columns(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT, b INT, CONSTRAINT ck CHECK (a < b))"
        )
        db.execute_one("INSERT INTO t VALUES (1, 2)")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (2, 1)")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (2, 2)")

    def test_add_check_scans_existing_rows(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (0)")
        with pytest.raises(CheckViolationError):
            db.execute_one("ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)")
        assert rows(db, "SELECT a FROM t") == [(0,)]
        db.execute_one("UPDATE t SET a = 1")
        result = db.execute_one(
            "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)"
        )
        assert "ck" in result.notice
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (0)")

    def test_check_survives_reopen(self, tmp_path):
        path = tmp_path / "check_reopen"
        db = Database(path)
        db.execute_one(
            "CREATE TABLE t (a INT, name TEXT, "
            "CONSTRAINT ck_a CHECK (a > 0), "
            "CONSTRAINT ck_name CHECK (name <> ''))"
        )
        db.execute_one("INSERT INTO t VALUES (7, 'ok')")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT a, name FROM t") == [(7, "ok")]
        with pytest.raises(CheckViolationError):
            db2.execute_one("INSERT INTO t VALUES (0, 'ok')")
        with pytest.raises(CheckViolationError):
            db2.execute_one("INSERT INTO t VALUES (1, '')")
        db2.close()

    def test_add_check_can_rollback(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t ADD CHECK (a > 0)")
        db.execute_one("ROLLBACK")
        db.execute_one("INSERT INTO t VALUES (-1)")
        assert rows(db, "SELECT a FROM t") == [(-1,)]

    def test_add_column_with_check_validates_existing_rows(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one(
            "ALTER TABLE t ADD COLUMN b INT DEFAULT 2 CHECK (b > 0)"
        )
        assert rows(db, "SELECT a, b FROM t") == [(1, 2)]
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (2, 0)")
        db.execute_one("INSERT INTO t VALUES (3, 1)")
        assert rows(db, "SELECT a, b FROM t ORDER BY a") == [
            (1, 2),
            (3, 1),
        ]

    def test_add_column_check_rejects_bad_default_without_mutation(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        with pytest.raises(CheckViolationError):
            db.execute_one(
                "ALTER TABLE t ADD COLUMN b INT DEFAULT 0 CHECK (b > 0)"
            )
        assert rows(db, "SELECT a FROM t") == [(1,)]
        with pytest.raises(ColumnNotFoundError):
            db.execute_one("SELECT b FROM t")

    def test_add_column_check_survives_reopen(self, tmp_path):
        path = tmp_path / "add_column_check_reopen"
        db = Database(path)
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one(
            "ALTER TABLE t ADD COLUMN b INT DEFAULT 2 CHECK (b > 0)"
        )
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT a, b FROM t") == [(1, 2)]
        with pytest.raises(CheckViolationError):
            db2.execute_one("INSERT INTO t VALUES (2, 0)")
        db2.close()

    def test_check_unknown_column_rejected_at_ddl(self, db):
        with pytest.raises(ColumnNotFoundError):
            db.execute_one(
                "CREATE TABLE t (a INT, CONSTRAINT ck CHECK (missing > 0))"
            )

    def test_check_requires_boolean_expression(self, db):
        with pytest.raises(TypeMismatchError):
            db.execute_one("CREATE TABLE t (a INT CHECK (a))")

    def test_drop_check_constraint(self, db):
        db.execute_one("CREATE TABLE t (a INT, CONSTRAINT ck CHECK (a > 0))")
        db.execute_one("INSERT INTO t VALUES (1)")
        result = db.execute_one("ALTER TABLE t DROP CONSTRAINT ck")
        assert "ck" in result.notice
        db.execute_one("INSERT INTO t VALUES (0)")
        assert rows(db, "SELECT a FROM t ORDER BY a") == [(0,), (1,)]

        with pytest.raises(BinderError):
            db.execute_one("ALTER TABLE t DROP CONSTRAINT ck")
        result = db.execute_one(
            "ALTER TABLE t DROP CONSTRAINT IF EXISTS ck"
        )
        assert "不存在" in result.notice

    def test_drop_check_can_rollback(self, db):
        db.execute_one("CREATE TABLE t (a INT, CHECK (a > 0))")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT t_check_1")
        db.execute_one("ROLLBACK")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (0)")

    def test_drop_index_constraint_rejected(self, db):
        db.execute_one("CREATE TABLE t (a INT UNIQUE)")
        with pytest.raises(NotSupportedError):
            db.execute_one("DROP INDEX t_a_key")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT t_a_key")

    def test_drop_check_survives_reopen(self, tmp_path):
        path = tmp_path / "check_drop_reopen"
        db = Database(path)
        db.execute_one("CREATE TABLE t (a INT, CONSTRAINT ck CHECK (a > 0))")
        db.execute_one("ALTER TABLE t DROP CONSTRAINT ck")
        db.close()

        db2 = Database(path)
        db2.execute_one("INSERT INTO t VALUES (-1)")
        assert rows(db2, "SELECT a FROM t") == [(-1,)]
        db2.close()

    def test_check_string_and_boolean_expression(self, db):
        db.execute_one(
            "CREATE TABLE t (name TEXT CHECK (name <> ''), "
            "active BOOLEAN CHECK (active OR NOT active))"
        )
        db.execute_one("INSERT INTO t VALUES ('a', TRUE)")
        with pytest.raises(CheckViolationError) as exc:
            db.execute_one("INSERT INTO t VALUES ('', TRUE)")
        assert exc.value.sqlstate == "23514"
        db.execute_one("INSERT INTO t VALUES (NULL, FALSE)")

    def test_duplicate_check_name_rejected_at_ddl(self, db):
        with pytest.raises(BinderError):
            db.execute_one(
                "CREATE TABLE t (a INT CHECK (a > 0), "
                "CONSTRAINT t_a_check CHECK (a < 100))"
            )

    def test_check_rename_updates_persisted_tokens(self, db):
        db.execute_one(
            "CREATE TABLE t (a INT, CONSTRAINT ck CHECK (t.a > 0))"
        )
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("ALTER TABLE t RENAME TO u")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO u VALUES (0)")
        db.execute_one("INSERT INTO u VALUES (2)")

        db.execute_one(
            "CREATE TABLE v (a INT, CONSTRAINT ck_v CHECK (v.a > 0))"
        )
        db.execute_one("ALTER TABLE v RENAME COLUMN a TO b")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO v VALUES (0)")
        db.execute_one("INSERT INTO v VALUES (3)")

    def test_check_applies_to_insert_select_and_default_values(self, db):
        db.execute_one("CREATE TABLE src (a INT)")
        db.execute_one("INSERT INTO src VALUES (0), (1)")
        db.execute_one("CREATE TABLE t (a INT CHECK (a > 0))")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t SELECT a FROM src")

        db.execute_one("CREATE TABLE d (a INT DEFAULT 0 CHECK (a > 0))")
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO d DEFAULT VALUES")


class TestCte:
    def test_filtered_cte(self, db):
        setup_users(db)
        result = rows(
            db,
            "WITH adults AS (SELECT id, name FROM users WHERE age >= 30) "
            "SELECT name FROM adults ORDER BY name",
        )
        assert result == [("alice",), ("carol",)]

    def test_cte_referenced_twice(self, db):
        setup_users(db)
        result = rows(
            db,
            "WITH young AS (SELECT id FROM users WHERE age < 30) "
            "SELECT * FROM young UNION ALL SELECT * FROM young",
        )
        assert result == [(2,), (2,)]

    def test_cte_joined_with_table(self, db):
        setup_users(db)
        result = rows(
            db,
            "WITH named AS (SELECT id, name FROM users) "
            "SELECT n.name, u.age FROM named n JOIN users u ON n.id = u.id "
            "WHERE u.age > 30 ORDER BY n.name",
        )
        assert result == [("carol", 35)]

    def test_cte_can_reference_earlier_cte(self, db):
        setup_users(db)
        result = rows(
            db,
            "WITH adults AS (SELECT id FROM users WHERE age >= 30), "
            "rich AS (SELECT id FROM adults WHERE id > 1) "
            "SELECT * FROM rich",
        )
        assert result == [(3,)]

    def test_scalar_subquery_uses_output_type(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users WHERE id = (SELECT 2)") == [(2,)]

    def test_cte_aggregate(self, db):
        setup_users(db)
        result = rows(
            db,
            "WITH stats AS (SELECT COUNT(*) AS n, age FROM users GROUP BY age) "
            "SELECT n FROM stats WHERE age = 30",
        )
        assert result == [(1,)]

    def test_cte_visible_inside_subquery(self, db):
        setup_users(db)
        result = rows(
            db,
            "WITH adults AS (SELECT id FROM users WHERE age >= 30) "
            "SELECT name FROM users WHERE id IN (SELECT id FROM adults) "
            "ORDER BY name",
        )
        assert result == [("alice",), ("carol",)]

    def test_with_update(self, db):
        setup_users(db)
        result = db.execute_one(
            "WITH target AS (SELECT 2 AS id) "
            "UPDATE users SET age = age + 1 "
            "WHERE id = (SELECT id FROM target) RETURNING id, age"
        )
        assert result.rows == [(2, 26)]

    def test_with_insert_values_uses_cte(self, db):
        setup_users(db)
        result = db.execute_one(
            "WITH x AS (SELECT 5 AS id) "
            "INSERT INTO users VALUES ((SELECT id FROM x), 'eve', 20) "
            "RETURNING id, name"
        )
        assert result.rows == [(5, "eve")]

    def test_with_delete(self, db):
        setup_users(db)
        result = db.execute_one(
            "WITH target AS (SELECT id FROM users WHERE age > 30) "
            "DELETE FROM users WHERE id IN (SELECT id FROM target) "
            "RETURNING id"
        )
        assert result.rows == [(3,)]

    def test_cte_scope_does_not_leak_to_next_statement(self, db):
        setup_users(db)
        db.execute_one("WITH one AS (SELECT 1 AS id) SELECT * FROM one")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM one")

    def test_explain_analyze_cte(self, db):
        setup_users(db)
        plan = rows(
            db,
            "EXPLAIN ANALYZE WITH adults AS "
            "(SELECT id FROM users WHERE age >= 30) SELECT * FROM adults",
        )
        text = "\n".join(r[0] for r in plan)
        assert "CteScan" in text
        assert "actual time=" in text

    def test_self_reference_is_table_not_found(self, db):
        with pytest.raises(TableNotFoundError):
            db.execute_one("WITH x AS (SELECT * FROM x) SELECT * FROM x")

    def test_duplicate_cte_name(self, db):
        with pytest.raises(Exception):
            db.execute_one(
                "WITH x AS (SELECT 1), x AS (SELECT 2) SELECT * FROM x"
            )

    def test_explain_shows_cte_scan(self, db):
        setup_users(db)
        plan = rows(
            db,
            "EXPLAIN WITH adults AS (SELECT id FROM users WHERE age >= 30) "
            "SELECT * FROM adults",
        )
        text = "\n".join(r[0] for r in plan)
        assert "CteScan" in text
        assert "Index" in text or "Scan" in text


class TestUnion:
    def test_union_all_keeps_duplicates(self, db):
        assert rows(db, "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 2") == [
            (1,), (2,), (2,)
        ]

    def test_union_dedupes(self, db):
        assert rows(db, "SELECT 1 AS x UNION SELECT 1 UNION SELECT 2") == [(1,), (2,)]

    def test_union_null_is_one_row(self, db):
        assert rows(db, "SELECT NULL AS x UNION SELECT NULL") == [(None,)]

    def test_union_null_absorbs_other_type(self, db):
        result = db.execute_one(
            "SELECT NULL AS x UNION SELECT 1 ORDER BY x"
        )
        assert result.rows == [(1,), (None,)]
        assert result.columns[0].type.name == "INT"

    def test_union_null_and_text_becomes_text(self, db):
        result = db.execute_one(
            "SELECT NULL AS x UNION SELECT 'a' ORDER BY x"
        )
        assert result.rows == [("a",), (None,)]
        assert result.columns[0].type.name == "TEXT"

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

    def test_except(self, db):
        db.execute_one("CREATE TABLE a (id INT)")
        db.execute_one("CREATE TABLE b (id INT)")
        db.execute_one("INSERT INTO a VALUES (1), (2), (2), (3)")
        db.execute_one("INSERT INTO b VALUES (2), (3), (4)")
        assert rows(
            db, "SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id"
        ) == [(1,)]

    def test_intersect(self, db):
        db.execute_one("CREATE TABLE a (id INT)")
        db.execute_one("CREATE TABLE b (id INT)")
        db.execute_one("INSERT INTO a VALUES (1), (2), (2), (3)")
        db.execute_one("INSERT INTO b VALUES (2), (3), (4)")
        assert rows(
            db, "SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id"
        ) == [(2,), (3,)]

    def test_intersect_precedence_over_union(self, db):
        assert rows(
            db,
            "SELECT 1 AS x UNION SELECT 2 AS x INTERSECT SELECT 3 AS x",
        ) == [(1,)]

    def test_except_null_is_set_member(self, db):
        assert rows(
            db, "SELECT NULL AS x INTERSECT SELECT NULL"
        ) == [(None,)]

    def test_except_all_multiset(self, db):
        db.execute_one("CREATE TABLE a (id INT)")
        db.execute_one("CREATE TABLE b (id INT)")
        db.execute_one("INSERT INTO a VALUES (1), (2), (2), (3)")
        db.execute_one("INSERT INTO b VALUES (2), (3), (4)")
        assert rows(
            db, "SELECT id FROM a EXCEPT ALL SELECT id FROM b ORDER BY id"
        ) == [(1,), (2,)]

    def test_intersect_all_multiset(self, db):
        db.execute_one("CREATE TABLE c (id INT)")
        db.execute_one("CREATE TABLE d (id INT)")
        db.execute_one("INSERT INTO c VALUES (2), (2), (3)")
        db.execute_one("INSERT INTO d VALUES (2), (2), (3), (4)")
        assert rows(
            db, "SELECT id FROM c INTERSECT ALL SELECT id FROM d ORDER BY id"
        ) == [(2,), (2,), (3,)]

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

    def test_like_escape(self, db):
        db.execute_one("CREATE TABLE t (name TEXT)")
        db.execute_one(
            "INSERT INTO t VALUES ('100%'), ('100x'), ('100_'), ('a%b')"
        )
        assert rows(
            db, "SELECT name FROM t WHERE name LIKE '100!%' ESCAPE '!'"
        ) == [("100%",)]
        assert rows(
            db, "SELECT name FROM t WHERE name LIKE '100!_%' ESCAPE '!'"
        ) == [("100_",)]
        assert rows(
            db, "SELECT name FROM t WHERE name LIKE '100%' ESCAPE '!' ORDER BY name"
        ) == [("100%",), ("100_",), ("100x",)]
        with pytest.raises(TypeMismatchError):
            rows(db, "SELECT name FROM t WHERE name LIKE '100!%' ESCAPE '!!'")
        with pytest.raises(BinderError):
            rows(db, "SELECT name FROM t WHERE name LIKE '100!' ESCAPE '!'")

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

    def test_is_distinct_from(self, db):
        assert rows(
            db,
            "SELECT NULL IS DISTINCT FROM 1, "
            "NULL IS NOT DISTINCT FROM NULL, "
            "1 IS DISTINCT FROM 1, 1 IS NOT DISTINCT FROM 2",
        ) == [(True, True, False, False)]

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

    def test_in_subquery_checks_output_type(self, db):
        setup_users(db)
        with pytest.raises(TypeMismatchError):
            rows(db, "SELECT id FROM users WHERE id IN (SELECT '1')")

    def test_in_subquery_rejects_multiple_columns(self, db):
        setup_users(db)
        with pytest.raises(BinderError):
            rows(db, "SELECT id FROM users WHERE id IN (SELECT 1, 2)")

    def test_exists(self, db):
        setup_users(db)
        assert rows(db, "SELECT EXISTS (SELECT 1 FROM users)") == [(True,)]

    def test_scalar_null_is_type_compatible(self, db):
        setup_users(db)
        assert rows(db, "SELECT id FROM users WHERE id = (SELECT NULL)") == []

    def test_case_null_branch_uses_other_branch_type(self, db):
        result = db.execute_one(
            "SELECT CASE WHEN 1 = 1 THEN NULL ELSE 2 END AS v"
        )
        assert result.rows == [(None,)]
        assert result.columns[0].type.name == "INT"

    def test_ctas_null_column_becomes_text(self, db):
        db.execute_one("CREATE TABLE snap AS SELECT NULL AS v")
        result = db.execute_one("SELECT * FROM snap")
        assert result.rows == [(None,)]
        assert result.columns[0].type.name == "TEXT"

    def test_null_arithmetic_propagates_type(self, db):
        result = db.execute_one("SELECT NULL + 1 AS v")
        assert result.rows == [(None,)]
        assert result.columns[0].type.name == "INT"

    def test_coalesce_absorbs_null_type(self, db):
        result = db.execute_one("SELECT COALESCE(NULL, 1) AS v")
        assert result.rows == [(1,)]
        assert result.columns[0].type.name == "INT"

    def test_coalesce_rejects_incompatible_types(self, db):
        with pytest.raises(TypeMismatchError):
            db.execute_one("SELECT COALESCE(1, 'a')")

    def test_sum_null_is_null(self, db):
        db.execute_one("CREATE TABLE nums (v INT)")
        db.execute_one("INSERT INTO nums VALUES (1), (2)")
        result = db.execute_one("SELECT SUM(NULL) AS s FROM nums")
        assert result.rows == [(None,)]


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

    def test_savepoint_rolls_back_only_recent_dml(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("SAVEPOINT s")
        db.execute_one("INSERT INTO t VALUES (2)")
        db.execute_one("SAVEPOINT t2")
        db.execute_one("INSERT INTO t VALUES (3)")
        db.execute_one("ROLLBACK TO SAVEPOINT t2")
        assert rows(db, "SELECT id FROM t ORDER BY id") == [(1,), (2,)]
        db.execute_one("ROLLBACK TO SAVEPOINT s")
        assert rows(db, "SELECT id FROM t ORDER BY id") == [(1,)]
        db.execute_one("COMMIT")

    def test_savepoint_survives_reopen(self, tmp_path):
        path = tmp_path / "savepoint_reopen"
        db = Database(path)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("SAVEPOINT s")
        db.execute_one("INSERT INTO t VALUES (2)")
        db.execute_one("ROLLBACK TO SAVEPOINT s")
        db.execute_one("COMMIT")
        db.close()

        db2 = Database(path)
        assert rows(db2, "SELECT id FROM t") == [(1,)]
        db2.close()

    def test_savepoint_rolls_back_ddl(self, db):
        db.execute_one("BEGIN")
        db.execute_one("SAVEPOINT s")
        db.execute_one("CREATE TABLE x (id INT)")
        db.execute_one("INSERT INTO x VALUES (1)")
        db.execute_one("ROLLBACK TO SAVEPOINT s")
        with pytest.raises(TableNotFoundError):
            db.execute_one("SELECT * FROM x")
        db.execute_one("CREATE TABLE x (id INT)")
        db.execute_one("INSERT INTO x VALUES (2)")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT * FROM x") == [(2,)]

    def test_savepoint_recovers_failed_statement(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute_one("BEGIN")
        db.execute_one("SAVEPOINT s")
        db.execute_one("INSERT INTO t VALUES (1)")
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO t VALUES (1)")
        db.execute_one("ROLLBACK TO SAVEPOINT s")
        assert rows(db, "SELECT COUNT(*) FROM t") == [(0,)]
        db.execute_one("COMMIT")

    def test_released_savepoint_cannot_be_rolled_back(self, db):
        db.execute_one("BEGIN")
        db.execute_one("SAVEPOINT s")
        db.execute_one("RELEASE SAVEPOINT s")
        with pytest.raises(TransactionError):
            db.execute_one("ROLLBACK TO SAVEPOINT s")
        db.execute_one("ROLLBACK")

    def test_savepoint_rolls_back_truncate(self, db):
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        db.execute_one("BEGIN")
        db.execute_one("SAVEPOINT s")
        db.execute_one("TRUNCATE TABLE t")
        db.execute_one("ROLLBACK TO SAVEPOINT s")
        assert rows(db, "SELECT id FROM t ORDER BY id") == [(1,), (2,)]
        db.execute_one("COMMIT")

    def test_savepoint_restores_deferred_constraint_mode(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, "
            "pid INT REFERENCES p(id) DEFERRABLE INITIALLY IMMEDIATE)"
        )
        db.execute_one("BEGIN")
        db.execute_one("SET CONSTRAINTS ALL DEFERRED")
        db.execute_one("SAVEPOINT s")
        db.execute_one("SET CONSTRAINTS ALL IMMEDIATE")
        db.execute_one("ROLLBACK TO SAVEPOINT s")
        # 保存点之前是 DEFERRED；回滚后必须恢复，而不是保留 IMMEDIATE。
        db.execute_one("INSERT INTO c VALUES (1, 99)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("COMMIT")


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


class TestForeignKey:
    def test_insert_update_delete_restrict(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY, name TEXT)")
        db.execute_one("INSERT INTO p VALUES (1, 'a'), (2, 'b')")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id))"
        )
        db.execute_one("INSERT INTO c VALUES (10, 1), (20, NULL)")

        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (30, 99)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("DELETE FROM p WHERE id = 1")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("UPDATE p SET id = 3 WHERE id = 1")

        db.execute_one("DELETE FROM c WHERE id = 10")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT id FROM p ORDER BY id") == [(2,)]
        assert rows(db, "SELECT id, pid FROM c ORDER BY id") == [(20, None)]

    def test_table_constraint_uses_primary_key(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT, "
            "CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES p)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (2)")

    def test_unique_reference_and_type_check(self, db):
        db.execute_one("CREATE TABLE p (code TEXT UNIQUE)")
        db.execute_one(
            "CREATE TABLE c (code TEXT REFERENCES p(code))"
        )
        db.execute_one("INSERT INTO p VALUES ('x')")
        db.execute_one("INSERT INTO c VALUES ('x')")
        db.execute_one("INSERT INTO c VALUES (NULL)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES ('y')")

        db.execute_one("CREATE TABLE p2 (id INT)")
        with pytest.raises(BinderError):
            db.execute_one(
                "CREATE TABLE c2 (pid INT REFERENCES p2(id))"
            )
        db.execute_one("CREATE TABLE p3 (id INT PRIMARY KEY)")
        with pytest.raises(TypeMismatchError):
            db.execute_one(
                "CREATE TABLE c3 (pid TEXT REFERENCES p3(id))"
            )

    def test_self_reference(self, db):
        db.execute_one(
            "CREATE TABLE node (id INT PRIMARY KEY, parent_id INT "
            "REFERENCES node(id))"
        )
        db.execute_one("INSERT INTO node VALUES (1, NULL), (2, 1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("DELETE FROM node WHERE id = 1")
        db.execute_one("DELETE FROM node WHERE id = 2")
        db.execute_one("DELETE FROM node WHERE id = 1")
        db.execute_one("INSERT INTO node VALUES (3, 3)")
        db.execute_one("DELETE FROM node WHERE id = 3")
        assert rows(db, "SELECT COUNT(*) FROM node") == [(0,)]

    def test_alter_add_and_drop_foreign_key(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (pid) REFERENCES p(id)"
        )
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (2)")
        db.execute_one("ALTER TABLE c DROP CONSTRAINT fk_c")
        db.execute_one("INSERT INTO c VALUES (2)")
        assert rows(db, "SELECT pid FROM c ORDER BY pid") == [(1,), (2,)]

    def test_alter_add_match_full_constraint(self, db):
        db.execute_one("CREATE TABLE p (x INT, y INT, PRIMARY KEY (x, y))")
        db.execute_one("CREATE TABLE c (a INT, b INT)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (a, b) REFERENCES p(x, y) MATCH FULL"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2)")
        db.execute_one("INSERT INTO c VALUES (1, 2), (NULL, NULL)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (1, NULL)")

    def test_alter_add_foreign_key_validates_existing_rows(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one("INSERT INTO c VALUES (99)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one(
                "ALTER TABLE c ADD CONSTRAINT fk_c "
                "FOREIGN KEY (pid) REFERENCES p(id)"
            )
        db.execute_one("INSERT INTO p VALUES (99)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (pid) REFERENCES p(id)"
        )
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (100)")

    def test_alter_add_not_valid_then_validate(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one("INSERT INTO c VALUES (99)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (pid) REFERENCES p(id) NOT VALID"
        )
        fk = db.catalog.get_table("c").foreign_keys["fk_c"]
        assert fk.validated is False
        # NOT VALID 只跳过存量行；新写入仍然立即检查。
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (100)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("ALTER TABLE c VALIDATE CONSTRAINT fk_c")
        assert fk.validated is False
        db.execute_one("INSERT INTO p VALUES (99)")
        db.execute_one("ALTER TABLE c VALIDATE CONSTRAINT fk_c")
        assert db.catalog.get_table("c").foreign_keys["fk_c"].validated is True
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (101)")

    def test_alter_not_valid_survives_reopen(self, tmp_path):
        path = tmp_path / "fk_not_valid"
        db = Database(path)
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one("INSERT INTO c VALUES (99)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (pid) REFERENCES p(id) NOT VALID"
        )
        db.close()

        db2 = Database(path)
        fk = db2.catalog.get_table("c").foreign_keys["fk_c"]
        assert fk.validated is False
        db2.execute_one("INSERT INTO p VALUES (99)")
        db2.execute_one("ALTER TABLE c VALIDATE CONSTRAINT fk_c")
        assert db2.catalog.get_table("c").foreign_keys["fk_c"].validated is True
        db2.close()

    def test_validate_constraint_rollback_restores_not_valid(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one("INSERT INTO c VALUES (99)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (pid) REFERENCES p(id) NOT VALID"
        )
        db.execute_one("INSERT INTO p VALUES (99)")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE c VALIDATE CONSTRAINT fk_c")
        assert db.catalog.get_table("c").foreign_keys["fk_c"].validated is True
        db.execute_one("ROLLBACK")
        assert db.catalog.get_table("c").foreign_keys["fk_c"].validated is False

    def test_not_valid_deferrable_new_rows_still_checked(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one("INSERT INTO c VALUES (99)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c FOREIGN KEY (pid) "
            "REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED NOT VALID"
        )
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO c VALUES (100)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(1,)]

    def test_create_table_rejects_not_valid(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        with pytest.raises(NotSupportedError):
            db.execute_one(
                "CREATE TABLE c (pid INT, CONSTRAINT fk_c "
                "FOREIGN KEY (pid) REFERENCES p(id) NOT VALID)"
            )

    def test_alter_add_check_not_valid_then_validate(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (-1)")
        db.execute_one(
            "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0) NOT VALID"
        )
        check = db.catalog.get_table("t").checks["ck"]
        assert check.validated is False
        # NOT VALID 只跳过存量行；新写入仍然立即检查。
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (-2)")
        with pytest.raises(CheckViolationError):
            db.execute_one("ALTER TABLE t VALIDATE CONSTRAINT ck")
        assert db.catalog.get_table("t").checks["ck"].validated is False
        db.execute_one("UPDATE t SET a = 1 WHERE a = -1")
        db.execute_one("ALTER TABLE t VALIDATE CONSTRAINT ck")
        assert db.catalog.get_table("t").checks["ck"].validated is True
        with pytest.raises(CheckViolationError):
            db.execute_one("INSERT INTO t VALUES (-2)")

    def test_check_not_valid_survives_reopen(self, tmp_path):
        path = tmp_path / "check_not_valid"
        db = Database(path)
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (-1)")
        db.execute_one(
            "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0) NOT VALID"
        )
        db.close()

        db2 = Database(path)
        check = db2.catalog.get_table("t").checks["ck"]
        assert check.validated is False
        db2.execute_one("UPDATE t SET a = 1 WHERE a = -1")
        db2.execute_one("ALTER TABLE t VALIDATE CONSTRAINT ck")
        assert db2.catalog.get_table("t").checks["ck"].validated is True
        db2.close()

    def test_validate_check_rollback_restores_not_valid(self, db):
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (-1)")
        db.execute_one(
            "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0) NOT VALID"
        )
        db.execute_one("UPDATE t SET a = 1 WHERE a = -1")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t VALIDATE CONSTRAINT ck")
        assert db.catalog.get_table("t").checks["ck"].validated is True
        db.execute_one("ROLLBACK")
        assert db.catalog.get_table("t").checks["ck"].validated is False

    def test_create_table_rejects_not_valid_check(self, db):
        with pytest.raises(NotSupportedError):
            db.execute_one(
                "CREATE TABLE t (a INT, CONSTRAINT ck CHECK (a > 0) NOT VALID)"
            )

    def test_drop_table_requires_cascade(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(BinderError):
            db.execute_one("DROP TABLE p")
        db.execute_one("DROP TABLE p CASCADE")
        db.execute_one("INSERT INTO c VALUES (99)")
        assert rows(db, "SELECT pid FROM c ORDER BY pid") == [(1,), (99,)]

    def test_rename_table_and_column_sync_metadata(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("ALTER TABLE p RENAME TO p_new")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (2)")
        db.execute_one("ALTER TABLE p_new RENAME COLUMN id TO code")
        db.execute_one("INSERT INTO p_new VALUES (2)")
        db.execute_one("INSERT INTO c VALUES (2)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (3)")

    def test_alter_type_cascade_removes_dependent_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY, note TEXT)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        with pytest.raises(NotSupportedError, match="外键"):
            db.execute_one("ALTER TABLE p ALTER COLUMN id TYPE BIGINT")
        result = db.execute_one(
            "ALTER TABLE p ALTER COLUMN id TYPE BIGINT CASCADE"
        )
        assert "级联删除 1 个外键" in result.notice
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        assert db.catalog.get_table("p").column("id").type.name == "BIGINT"
        db.execute_one("INSERT INTO c VALUES (99)")

    def test_alter_type_cascade_removes_local_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id), note TEXT)")
        result = db.execute_one(
            "ALTER TABLE c ALTER COLUMN pid TYPE BIGINT CASCADE"
        )
        assert "级联删除 1 个外键" in result.notice
        assert db.catalog.get_table("c").foreign_keys == {}

    def test_alter_type_cascade_rollback_restores_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY, note TEXT)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE p ALTER COLUMN id TYPE BIGINT CASCADE")
        assert "c_pid_fkey" not in db.catalog.get_table("c").foreign_keys
        db.execute_one("ROLLBACK")
        assert "c_pid_fkey" in db.catalog.get_table("c").foreign_keys
        assert db.catalog.get_table("p").column("id").type.name == "INT"
        db.execute_one("INSERT INTO p VALUES (1, 'x')")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (2)")

    def test_drop_column_and_type_rejected_for_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE p DROP COLUMN id")
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE p ALTER COLUMN id TYPE BIGINT")
        result = db.execute_one("ALTER TABLE p ALTER COLUMN id TYPE INT")
        assert "已经是" in result.notice

    def test_error_sqlstate_and_truncate_guard(self, db):
        assert ForeignKeyViolationError.sqlstate == "23503"
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(BinderError):
            db.execute_one("TRUNCATE TABLE p")

    def test_drop_unique_constraint_rejected_when_referenced(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT, "
            "CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES p(id))"
        )
        with pytest.raises(NotSupportedError):
            db.execute_one("ALTER TABLE p DROP CONSTRAINT p_pkey")
        db.execute_one("ALTER TABLE c DROP CONSTRAINT fk_c")
        db.execute_one("ALTER TABLE p DROP CONSTRAINT p_pkey")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO p VALUES (1)")
        assert rows(db, "SELECT id FROM p ORDER BY id") == [(1,), (1,)]

    def test_composite_foreign_key(self, db):
        db.execute_one(
            "CREATE TABLE p (a INT, b INT, PRIMARY KEY (a, b))"
        )
        db.execute_one(
            "CREATE TABLE c (a INT, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b))"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2), (3, 4)")
        db.execute_one("INSERT INTO c VALUES (1, 2)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (1, 3)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("DELETE FROM p WHERE a = 1 AND b = 2")

    def test_match_full_allows_all_null_or_full_key(self, db):
        db.execute_one(
            "CREATE TABLE p (x INT, y INT, PRIMARY KEY (x, y))"
        )
        db.execute_one(
            "CREATE TABLE c (a INT, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(x, y) MATCH FULL)"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2)")
        db.execute_one("INSERT INTO c VALUES (1, 2), (NULL, NULL)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (1, NULL)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("UPDATE c SET b = NULL WHERE a = 1")

    def test_deferred_fk_insert_child_then_parent(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT, pid INT, "
            "FOREIGN KEY (pid) REFERENCES p(id) "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO c VALUES (1, 1)")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT id, pid FROM c") == [(1, 1)]

    def test_deferred_fk_commit_failure(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO c VALUES (99)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]

    def test_deferred_fk_update_then_parent(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("UPDATE c SET pid = 2")
        db.execute_one("INSERT INTO p VALUES (2)")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT pid FROM c") == [(2,)]

    def test_deferred_no_action_delete_then_restore(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("DELETE FROM p WHERE id = 1")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(1,)]

    def test_deferred_no_action_delete_without_restore(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("DELETE FROM p WHERE id = 1")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM p") == [(1,)]

    def test_deferred_no_action_update_then_child(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("UPDATE p SET id = 2")
        db.execute_one("UPDATE c SET pid = 2")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT pid FROM c") == [(2,)]

    def test_deferred_no_action_update_without_child(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("UPDATE p SET id = 2")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("COMMIT")
        assert rows(db, "SELECT id FROM p") == [(1,)]

    def test_set_constraints_all_deferred(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) DEFERRABLE)"
        )
        db.execute_one("BEGIN")
        db.execute_one("SET CONSTRAINTS ALL DEFERRED")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(1,)]

    def test_set_constraints_all_deferred_no_action_delete(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "ON DELETE NO ACTION DEFERRABLE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("SET CONSTRAINTS ALL DEFERRED")
        db.execute_one("DELETE FROM p WHERE id = 1")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM p") == [(1,)]

    def test_set_constraints_all_deferred_no_action_failure(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "ON DELETE NO ACTION DEFERRABLE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("SET CONSTRAINTS ALL DEFERRED")
        db.execute_one("DELETE FROM p WHERE id = 1")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM p") == [(1,)]

    def test_set_constraints_all_immediate_detects_pending(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("SET CONSTRAINTS ALL IMMEDIATE")
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]

    def test_set_constraints_named(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT, "
            "CONSTRAINT fk_child FOREIGN KEY (pid) REFERENCES p(id) "
            "DEFERRABLE INITIALLY IMMEDIATE)"
        )
        db.execute_one("BEGIN")
        db.execute_one("SET CONSTRAINTS fk_child DEFERRED")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("COMMIT")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(1,)]

    def test_set_constraints_unknown_name(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("BEGIN")
        with pytest.raises(BinderError):
            db.execute_one("SET CONSTRAINTS nope DEFERRED")
        db.execute_one("ROLLBACK")

    def test_set_constraints_non_deferrable(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id))"
        )
        db.execute_one("BEGIN")
        with pytest.raises(NotSupportedError):
            db.execute_one("SET CONSTRAINTS c_pid_fkey DEFERRED")
        db.execute_one("ROLLBACK")

    def test_set_constraints_outside_transaction(self, db):
        with pytest.raises(TransactionError):
            db.execute_one("SET CONSTRAINTS ALL DEFERRED")

    def test_on_delete_cascade(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, "
            "FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE)"
        )
        db.execute_one("CREATE INDEX idx_c_pid ON c(pid)")
        db.execute_one("INSERT INTO p VALUES (1), (2)")
        db.execute_one("INSERT INTO c VALUES (10, 1), (20, 2)")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT id, pid FROM c ORDER BY id") == [(20, 2)]
        assert rows(db, "SELECT id FROM c WHERE pid = 1") == []
        db.execute_one("DELETE FROM p WHERE id = 2")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]

    def test_on_delete_cascade_multi_level(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, "
            "FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE)"
        )
        db.execute_one(
            "CREATE TABLE g (id INT PRIMARY KEY, cid INT, "
            "FOREIGN KEY (cid) REFERENCES c(id) ON DELETE CASCADE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (10, 1)")
        db.execute_one("INSERT INTO g VALUES (100, 10)")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]
        assert rows(db, "SELECT COUNT(*) FROM g") == [(0,)]

    def test_on_delete_cascade_restrict_sibling_rolls_back(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) ON DELETE CASCADE)"
        )
        db.execute_one(
            "CREATE TABLE d (pid INT REFERENCES p(id) ON DELETE RESTRICT)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("INSERT INTO d VALUES (1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(1,)]
        assert rows(db, "SELECT COUNT(*) FROM d") == [(1,)]

    def test_on_delete_cascade_self_reference_chain(self, db):
        db.execute_one(
            "CREATE TABLE node (id INT PRIMARY KEY, parent_id INT, "
            "FOREIGN KEY (parent_id) REFERENCES node(id) ON DELETE CASCADE)"
        )
        db.execute_one("INSERT INTO node VALUES (1, NULL), (2, 1), (3, 2)")
        db.execute_one("DELETE FROM node WHERE id = 1")
        assert rows(db, "SELECT COUNT(*) FROM node") == [(0,)]

    def test_on_update_cascade(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, "
            "FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE CASCADE)"
        )
        db.execute_one("CREATE INDEX idx_c_pid ON c(pid)")
        db.execute_one("INSERT INTO p VALUES (1), (2)")
        db.execute_one("INSERT INTO c VALUES (10, 1), (20, 2)")
        db.execute_one("UPDATE p SET id = 5 WHERE id = 1")
        assert rows(db, "SELECT id, pid FROM c ORDER BY id") == [
            (10, 5),
            (20, 2),
        ]

    def test_on_update_cascade_multi_level(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT UNIQUE REFERENCES p(id) ON UPDATE CASCADE)"
        )
        db.execute_one(
            "CREATE TABLE g (cpid INT REFERENCES c(pid) ON UPDATE CASCADE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("INSERT INTO g VALUES (1)")
        db.execute_one("UPDATE p SET id = 7 WHERE id = 1")
        assert rows(db, "SELECT pid FROM c") == [(7,)]
        assert rows(db, "SELECT cpid FROM g") == [(7,)]

    def test_on_update_cascade_rollback(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) ON UPDATE CASCADE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("UPDATE p SET id = 5 WHERE id = 1")
        assert rows(db, "SELECT pid FROM c") == [(5,)]
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT pid FROM c") == [(1,)]

    def test_on_update_cascade_self_reference(self, db):
        db.execute_one(
            "CREATE TABLE node (id INT PRIMARY KEY, parent_id INT, "
            "FOREIGN KEY (parent_id) REFERENCES node(id) ON UPDATE CASCADE)"
        )
        db.execute_one("INSERT INTO node VALUES (1, NULL), (2, 1), (3, 2)")
        db.execute_one("UPDATE node SET id = 7 WHERE id = 2")
        assert rows(db, "SELECT id, parent_id FROM node ORDER BY id") == [
            (1, None),
            (3, 7),
            (7, 1),
        ]

    def test_on_delete_set_null(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, "
            "FOREIGN KEY (pid) REFERENCES p(id) ON DELETE SET NULL)"
        )
        db.execute_one("CREATE INDEX idx_c_pid ON c(pid)")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (10, 1)")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT id, pid FROM c") == [(10, None)]

    def test_on_delete_set_null_not_null_rolls_back(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT NOT NULL REFERENCES p(id) ON DELETE SET NULL)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT id FROM p") == [(1,)]
        assert rows(db, "SELECT pid FROM c") == [(1,)]

    def test_on_update_set_null(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) ON UPDATE SET NULL)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("UPDATE p SET id = 5 WHERE id = 1")
        assert rows(db, "SELECT pid FROM c") == [(None,)]

    def test_on_update_set_null_not_null_rolls_back(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT NOT NULL REFERENCES p(id) ON UPDATE SET NULL)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(NotNullViolationError):
            db.execute_one("UPDATE p SET id = 5 WHERE id = 1")
        assert rows(db, "SELECT id FROM p") == [(1,)]
        assert rows(db, "SELECT pid FROM c") == [(1,)]

    def test_on_delete_set_null_rollback(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) ON DELETE SET NULL)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT pid FROM c") == [(None,)]
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT pid FROM c") == [(1,)]
        assert rows(db, "SELECT id FROM p") == [(1,)]

    def test_alter_add_set_null(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (pid) REFERENCES p(id) ON DELETE SET NULL"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT pid FROM c") == [(None,)]

    def test_on_delete_set_default(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT DEFAULT 2 REFERENCES p(id) "
            "ON DELETE SET DEFAULT)"
        )
        db.execute_one("INSERT INTO p VALUES (1), (2)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT pid FROM c") == [(2,)]

    def test_on_delete_set_default_missing_target_rolls_back(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT DEFAULT 9 REFERENCES p(id) "
            "ON DELETE SET DEFAULT)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT id FROM p") == [(1,)]
        assert rows(db, "SELECT pid FROM c") == [(1,)]

    def test_on_update_set_default(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT DEFAULT 2 REFERENCES p(id) "
            "ON UPDATE SET DEFAULT)"
        )
        db.execute_one("INSERT INTO p VALUES (1), (2)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("UPDATE p SET id = 5 WHERE id = 1")
        assert rows(db, "SELECT pid FROM c") == [(2,)]

    def test_on_update_set_default_missing_target_rolls_back(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT DEFAULT 1 REFERENCES p(id) "
            "ON UPDATE SET DEFAULT)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("UPDATE p SET id = 5 WHERE id = 1")
        assert rows(db, "SELECT id FROM p") == [(1,)]
        assert rows(db, "SELECT pid FROM c") == [(1,)]

    def test_on_delete_set_null_column_list(self, db):
        db.execute_one(
            "CREATE TABLE p (a INT, b INT, UNIQUE (a, b))"
        )
        db.execute_one(
            "CREATE TABLE c (a INT, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) "
            "ON DELETE SET NULL (a))"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2)")
        db.execute_one("INSERT INTO c VALUES (1, 2)")
        db.execute_one("DELETE FROM p WHERE a = 1")
        assert rows(db, "SELECT a, b FROM c") == [(None, 2)]
        fk = db.catalog.get_table("c").foreign_keys["c_a_b_fkey"]
        assert fk.on_delete == "SET NULL"
        assert fk.on_delete_columns == ("a",)

    def test_on_delete_set_default_column_list(self, db):
        db.execute_one(
            "CREATE TABLE p (a INT, b INT, UNIQUE (a, b))"
        )
        db.execute_one(
            "CREATE TABLE c (a INT DEFAULT 9, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) "
            "ON DELETE SET DEFAULT (a))"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2), (9, 2)")
        db.execute_one("INSERT INTO c VALUES (1, 2)")
        db.execute_one("DELETE FROM p WHERE a = 1")
        assert rows(db, "SELECT a, b FROM c") == [(9, 2)]

    def test_on_update_set_null_column_list(self, db):
        db.execute_one(
            "CREATE TABLE p (a INT, b INT, UNIQUE (a, b))"
        )
        db.execute_one(
            "CREATE TABLE c (a INT, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) "
            "ON UPDATE SET NULL (b))"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2)")
        db.execute_one("INSERT INTO c VALUES (1, 2)")
        db.execute_one("UPDATE p SET b = 3 WHERE a = 1")
        assert rows(db, "SELECT a, b FROM c") == [(1, None)]

    def test_on_delete_set_null_column_list_match_full_fails(self, db):
        db.execute_one(
            "CREATE TABLE p (a INT, b INT, UNIQUE (a, b))"
        )
        db.execute_one(
            "CREATE TABLE c (a INT, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) MATCH FULL "
            "ON DELETE SET NULL (a))"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2)")
        db.execute_one("INSERT INTO c VALUES (1, 2)")
        with pytest.raises(ForeignKeyViolationError, match="MATCH FULL"):
            db.execute_one("DELETE FROM p WHERE a = 1")
        assert rows(db, "SELECT a, b FROM p") == [(1, 2)]
        assert rows(db, "SELECT a, b FROM c") == [(1, 2)]

    def test_alter_add_action_column_list(self, db):
        db.execute_one("CREATE TABLE p (a INT, b INT, UNIQUE (a, b))")
        db.execute_one("CREATE TABLE c (a INT, b INT)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) "
            "ON DELETE SET NULL (a) ON UPDATE SET DEFAULT (b)"
        )
        fk = db.catalog.get_table("c").foreign_keys["fk_c"]
        assert fk.on_delete_columns == ("a",)
        assert fk.on_update_columns == ("b",)

    def test_action_columns_persist_across_reopen(self, tmp_path):
        path = tmp_path / "action_cols"
        database = Database(path)
        database.execute_one(
            "CREATE TABLE p (a INT, b INT, UNIQUE (a, b))"
        )
        database.execute_one(
            "CREATE TABLE c (a INT, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) "
            "ON DELETE SET NULL (a) ON UPDATE SET DEFAULT (b))"
        )
        database.close()
        reopened = Database(path)
        try:
            fk = reopened.catalog.get_table("c").foreign_keys["c_a_b_fkey"]
            assert fk.on_delete_columns == ("a",)
            assert fk.on_update_columns == ("b",)
        finally:
            reopened.close()

    def test_rename_column_rewrites_action_column_list(self, db):
        db.execute_one("CREATE TABLE p (id INT UNIQUE)")
        db.execute_one(
            "CREATE TABLE c (a INT, note TEXT, "
            "FOREIGN KEY (a) REFERENCES p(id) "
            "ON DELETE SET NULL (a) ON UPDATE SET DEFAULT (a))"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1, 'x')")
        db.execute_one("ALTER TABLE c RENAME COLUMN a TO b")
        fk = next(iter(db.catalog.get_table("c").foreign_keys.values()))
        assert fk.columns == ("b",)
        assert fk.on_delete_columns == ("b",)
        assert fk.on_update_columns == ("b",)
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT b, note FROM c") == [(None, "x")]

    def test_on_update_set_default_column_list(self, db):
        db.execute_one("CREATE TABLE p (a INT, b INT, UNIQUE (a, b))")
        db.execute_one(
            "CREATE TABLE c (a INT DEFAULT 9, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) "
            "ON UPDATE SET DEFAULT (a))"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2), (9, 3)")
        db.execute_one("INSERT INTO c VALUES (1, 2)")
        db.execute_one("UPDATE p SET b = 3 WHERE a = 1")
        assert rows(db, "SELECT a, b FROM c") == [(9, 3)]

    def test_on_update_set_null_column_list_match_full_fails(self, db):
        db.execute_one("CREATE TABLE p (a INT, b INT, UNIQUE (a, b))")
        db.execute_one(
            "CREATE TABLE c (a INT, b INT, "
            "FOREIGN KEY (a, b) REFERENCES p(a, b) MATCH FULL "
            "ON UPDATE SET NULL (b))"
        )
        db.execute_one("INSERT INTO p VALUES (1, 2)")
        db.execute_one("INSERT INTO c VALUES (1, 2)")
        with pytest.raises(ForeignKeyViolationError, match="MATCH FULL"):
            db.execute_one("UPDATE p SET b = 3 WHERE a = 1")
        assert rows(db, "SELECT a, b FROM p") == [(1, 2)]
        assert rows(db, "SELECT a, b FROM c") == [(1, 2)]

    def test_drop_index_cascade_multiple_dependents(self, db):
        db.execute_one("CREATE TABLE p (id INT)")
        db.execute_one("CREATE UNIQUE INDEX p_id_uq ON p (id)")
        db.execute_one("CREATE TABLE c1 (pid INT REFERENCES p(id))")
        db.execute_one("CREATE TABLE c2 (pid INT REFERENCES p(id))")
        result = db.execute_one("DROP INDEX p_id_uq CASCADE")
        assert "级联删除 2 个外键" in result.notice
        assert db.catalog.get_table("c1").foreign_keys == {}
        assert db.catalog.get_table("c2").foreign_keys == {}

    def test_on_delete_cascade_self_cycle(self, db):
        db.execute_one(
            "CREATE TABLE node (id INT PRIMARY KEY, parent_id INT "
            "REFERENCES node(id) ON DELETE CASCADE)"
        )
        db.execute_one("INSERT INTO node VALUES (1, NULL), (2, 1), (3, 2)")
        db.execute_one("UPDATE node SET parent_id = 3 WHERE id = 1")
        db.execute_one("DELETE FROM node WHERE id = 1")
        assert rows(db, "SELECT COUNT(*) FROM node") == [(0,)]

    def test_on_delete_cascade_diamond(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c1 (id INT PRIMARY KEY, pid INT "
            "REFERENCES p(id) ON DELETE CASCADE)"
        )
        db.execute_one(
            "CREATE TABLE c2 (id INT PRIMARY KEY, pid INT "
            "REFERENCES p(id) ON DELETE CASCADE)"
        )
        db.execute_one(
            "CREATE TABLE g (id INT PRIMARY KEY, "
            "c1 INT REFERENCES c1(id) ON DELETE CASCADE, "
            "c2 INT REFERENCES c2(id) ON DELETE CASCADE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c1 VALUES (10, 1)")
        db.execute_one("INSERT INTO c2 VALUES (20, 1)")
        db.execute_one("INSERT INTO g VALUES (100, 10, 20)")
        db.execute_one("DELETE FROM p WHERE id = 1")
        for table in ("p", "c1", "c2", "g"):
            assert rows(db, f"SELECT COUNT(*) FROM {table}") == [(0,)]

    def test_on_delete_cascade_rollback(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) ON DELETE CASCADE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]
        db.execute_one("ROLLBACK")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(1,)]

    def test_alter_add_on_delete_cascade(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT)")
        db.execute_one(
            "ALTER TABLE c ADD CONSTRAINT fk_c "
            "FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db, "SELECT COUNT(*) FROM c") == [(0,)]

    def test_rename_foreign_key_constraint(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT, "
            "CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES p(id))"
        )
        db.execute_one("ALTER TABLE c RENAME CONSTRAINT fk_c TO fk_renamed")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (2)")
        db.execute_one("ALTER TABLE c DROP CONSTRAINT fk_renamed")
        db.execute_one("INSERT INTO c VALUES (2)")

    def test_add_column_references(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (id INT)")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one(
            "ALTER TABLE c ADD COLUMN pid INT REFERENCES p(id) DEFAULT 1"
        )
        assert rows(db, "SELECT id, pid FROM c") == [(1, 1)]
        db.execute_one("INSERT INTO c VALUES (2, 1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (3, 99)")

    def test_add_column_references_failure_rolls_back(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (id INT)")
        db.execute_one("INSERT INTO c VALUES (1)")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one(
                "ALTER TABLE c ADD COLUMN pid INT REFERENCES p(id) DEFAULT 9"
            )
        with pytest.raises(ColumnNotFoundError):
            db.execute_one("SELECT pid FROM c")
        assert rows(db, "SELECT id FROM c") == [(1,)]

    def test_drop_table_cascade_rollback_restores_fk(self, db):
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.execute_one("BEGIN")
        db.execute_one("DROP TABLE p CASCADE")
        db.execute_one("ROLLBACK")
        with pytest.raises(ForeignKeyViolationError):
            db.execute_one("INSERT INTO c VALUES (2)")
        db.execute_one("INSERT INTO p VALUES (2)")
        db.execute_one("INSERT INTO c VALUES (2)")

    def test_persisted_on_delete_cascade(self, tmp_path):
        path = tmp_path / "fk_cascade"
        db = Database(path)
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one(
            "CREATE TABLE c (pid INT REFERENCES p(id) ON DELETE CASCADE)"
        )
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.close()

        db2 = Database(path)
        db2.execute_one("DELETE FROM p WHERE id = 1")
        assert rows(db2, "SELECT COUNT(*) FROM c") == [(0,)]
        db2.close()

    def test_persisted_foreign_key(self, tmp_path):
        path = tmp_path / "fk"
        db = Database(path)
        db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
        db.execute_one("CREATE TABLE c (pid INT REFERENCES p(id))")
        db.execute_one("INSERT INTO p VALUES (1)")
        db.execute_one("INSERT INTO c VALUES (1)")
        db.close()

        db2 = Database(path)
        with pytest.raises(ForeignKeyViolationError):
            db2.execute_one("INSERT INTO c VALUES (2)")
        db2.execute_one("INSERT INTO p VALUES (2)")
        db2.execute_one("INSERT INTO c VALUES (2)")
        db2.close()


class TestErrorQuality:
    def test_unknown_column_has_hint(self, db):
        setup_users(db)
        with pytest.raises(ColumnNotFoundError) as exc:
            db.execute_one("SELECT nope FROM users")
        assert "id" in str(exc.value)

    def test_unsupported_syntax(self, db):
        with pytest.raises(NotSupportedError):
            db.execute_one("CREATE TRIGGER trg")

    def test_keyword_as_name_rejected(self, db):
        with pytest.raises(Exception):
            db.execute_one("SELECT FROM")
