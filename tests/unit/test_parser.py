"""语法分析器的测试：既要验证支持的能力，也要验证不支持的语法明确报错。"""

import pytest

from lumendb.datatypes import DataType
from lumendb.errors import NotSupportedError, SQLSyntaxError
from lumendb.sql.ast import (
    Between,
    BinaryOp,
    Cast,
    CaseWhen,
    Exists,
    InList,
    InSubquery,
    IsNull,
    Like,
    Literal,
    ParamRef,
    ScalarSubquery,
    Star,
    UnaryOp,
    Vacuum,
    Truncate,
    Insert,
    ColumnRef,
    CompoundSelect,
    CreateTableAs,
    Update,
    Delete,
    AlterTableRenameColumn,
    AlterTableRenameTable,
)
from lumendb.sql.parser import parse_sql, parse_statement


class TestCreateTable:
    def test_basic(self):
        stmt = parse_statement("CREATE TABLE t (id INT, name TEXT)")
        assert stmt.name == "t"
        assert len(stmt.columns) == 2
        assert stmt.columns[0].type.kind == DataType.INT

    def test_create_table_as_select(self):
        stmt = parse_statement("CREATE TABLE t AS SELECT id, name FROM src")
        assert isinstance(stmt, CreateTableAs)
        assert stmt.name == "t"
        assert stmt.query is not None

    def test_create_table_as_if_not_exists(self):
        stmt = parse_statement(
            "CREATE TABLE IF NOT EXISTS t AS SELECT id FROM src"
        )
        assert isinstance(stmt, CreateTableAs)
        assert stmt.if_not_exists is True

    def test_all_column_options(self):
        stmt = parse_statement(
            "CREATE TABLE t (id INT PRIMARY KEY, n VARCHAR(64) NOT NULL, "
            "a INT DEFAULT 0, b BOOLEAN NULL, c INT UNIQUE)"
        )
        id_col, n_col, a_col, b_col, c_col = stmt.columns
        assert id_col.primary_key and not id_col.nullable
        assert n_col.type.kind == DataType.VARCHAR
        assert n_col.type.length == 64
        assert a_col.default == Literal(0)
        assert b_col.nullable is True  # 显式 NULL 表示可空
        assert c_col.unique

    def test_table_constraints(self):
        stmt = parse_statement(
            "CREATE TABLE t (a INT, b INT, "
            "CONSTRAINT uq_ab UNIQUE (a, b), PRIMARY KEY (a, b))"
        )
        assert len(stmt.constraints) == 2
        assert stmt.constraints[0].kind == "UNIQUE"
        assert stmt.constraints[0].name == "uq_ab"
        assert stmt.constraints[0].columns == ["a", "b"]
        assert stmt.constraints[1].kind == "PRIMARY KEY"

    def test_if_not_exists(self):
        stmt = parse_statement("CREATE TABLE IF NOT EXISTS t (id INT)")
        assert stmt.if_not_exists

    def test_varchar_requires_length(self):
        with pytest.raises(SQLSyntaxError):
            parse_statement("CREATE TABLE t (a VARCHAR)")

    def test_unknown_type(self):
        with pytest.raises(NotSupportedError):
            parse_statement("CREATE TABLE t (a MONEY)")

    def test_foreign_key_rejected(self):
        with pytest.raises(NotSupportedError) as exc:
            parse_statement("CREATE TABLE t (a INT REFERENCES other(id))")
        assert "外键" in str(exc.value)

    def test_check_constraints(self):
        stmt = parse_statement(
            "CREATE TABLE t (a INT CHECK (a > 0) "
            "CONSTRAINT a_high CHECK (a < 100), "
            "CONSTRAINT ck_pair CHECK (a <> 50))"
        )
        col_checks = stmt.columns[0].checks
        assert len(col_checks) == 2
        assert col_checks[0].name is None
        assert col_checks[1].name == "a_high"
        assert col_checks[0].tokens
        table_check = stmt.constraints[0]
        assert table_check.kind == "CHECK"
        assert table_check.name == "ck_pair"
        assert table_check.check is not None
        assert table_check.check.tokens

    def test_add_check_constraint(self):
        from lumendb.sql.ast import AlterTableAddConstraint

        stmt = parse_statement(
            "ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)"
        )
        assert isinstance(stmt, AlterTableAddConstraint)
        assert stmt.constraint.kind == "CHECK"
        assert stmt.constraint.name == "ck"

    def test_drop_check_constraint(self):
        from lumendb.sql.ast import AlterTableDropConstraint

        stmt = parse_statement("ALTER TABLE t DROP CONSTRAINT IF EXISTS ck")
        assert isinstance(stmt, AlterTableDropConstraint)
        assert stmt.name == "ck"
        assert stmt.if_exists is True

    def test_alter_column_type(self):
        from lumendb.sql.ast import AlterTableAlterType

        stmt = parse_statement(
            "ALTER TABLE t ALTER COLUMN a TYPE BIGINT"
        )
        assert isinstance(stmt, AlterTableAlterType)
        assert stmt.type.kind == DataType.BIGINT

        stmt = parse_statement(
            "ALTER TABLE t ALTER a SET DATA TYPE VARCHAR(10)"
        )
        assert stmt.type.kind == DataType.VARCHAR
        assert stmt.type.length == 10

    def test_alter_column_type_using_rejected(self):
        with pytest.raises(NotSupportedError) as exc:
            parse_statement(
                "ALTER TABLE t ALTER COLUMN a TYPE TEXT USING CAST(a AS TEXT)"
            )
        assert "USING" in str(exc.value)

    def test_create_view(self):
        from lumendb.sql.ast import CreateView

        stmt = parse_statement("CREATE VIEW v AS SELECT a FROM t WHERE a > 0")
        assert isinstance(stmt, CreateView)
        assert stmt.name == "v"
        assert stmt.tokens[0] == ("keyword", "SELECT")
        assert stmt.tokens[-1] == ("int", "0")

    def test_create_or_replace_view(self):
        from lumendb.sql.ast import CreateView

        stmt = parse_statement("CREATE OR REPLACE VIEW v AS SELECT 1")
        assert isinstance(stmt, CreateView)
        assert stmt.or_replace
        with pytest.raises(SQLSyntaxError):
            parse_statement("CREATE OR REPLACE TABLE t (a INT)")

    def test_view_column_list(self):
        from lumendb.sql.ast import CreateView

        stmt = parse_statement("CREATE VIEW v (x, y) AS SELECT a, b FROM t")
        assert isinstance(stmt, CreateView)
        assert stmt.columns == ["x", "y"]

    def test_view_with_cte(self):
        from lumendb.sql.ast import CreateView, Query

        stmt = parse_statement(
            "CREATE VIEW v AS WITH x AS (SELECT 1) SELECT * FROM x"
        )
        assert isinstance(stmt, CreateView)
        assert isinstance(stmt.query, Query)
        assert stmt.query.ctes[0].name == "x"
        with pytest.raises(NotSupportedError):
            parse_statement(
                "CREATE VIEW v AS WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x"
            )

    def test_show_views(self):
        from lumendb.sql.ast import ShowViews

        assert isinstance(parse_statement("SHOW VIEWS"), ShowViews)
        with pytest.raises(SQLSyntaxError):
            parse_statement("SHOW SOMETHING")

    def test_drop_view(self):
        from lumendb.sql.ast import DropView
        stmt = parse_statement("DROP VIEW IF EXISTS v")
        assert isinstance(stmt, DropView)
        assert stmt.name == "v"
        assert stmt.if_exists

    def test_drop_cascade(self):
        from lumendb.sql.ast import DropTable, DropView

        table = parse_statement("DROP TABLE t CASCADE")
        assert isinstance(table, DropTable)
        assert table.cascade
        view = parse_statement("DROP VIEW v CASCADE")
        assert isinstance(view, DropView)
        assert view.cascade


class TestCreateIndex:
    def test_single_column(self):
        stmt = parse_statement("CREATE INDEX i ON t (a)")
        assert stmt.name == "i" and stmt.table == "t" and stmt.columns == ["a"]

    def test_composite(self):
        stmt = parse_statement("CREATE INDEX i ON t (a, b, c)")
        assert stmt.columns == ["a", "b", "c"]

    def test_create_index_if_not_exists(self):
        stmt = parse_statement("CREATE UNIQUE INDEX IF NOT EXISTS i ON t (a)")
        assert stmt.if_not_exists and stmt.unique
        assert stmt.name == "i" and stmt.columns == ["a"]

    def test_drop_index(self):
        assert parse_statement("DROP INDEX i").name == "i"

    def test_drop_table_if_exists(self):
        stmt = parse_statement("DROP TABLE IF EXISTS a, b")
        assert stmt.if_exists and stmt.names == ["a", "b"]


class TestDml:
    def test_insert_values(self):
        stmt = parse_statement("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        assert len(stmt.rows) == 2
        assert stmt.rows[0][0] == Literal(1)

    def test_insert_with_columns(self):
        stmt = parse_statement("INSERT INTO t (id) VALUES (1)")
        assert stmt.columns == ["id"]

    def test_insert_select(self):
        stmt = parse_statement("INSERT INTO t SELECT id, name FROM src")
        assert stmt.query is not None
        assert stmt.rows == []

    def test_insert_default_values(self):
        stmt = parse_statement("INSERT INTO t DEFAULT VALUES")
        assert stmt.default_values is True
        assert stmt.rows == []
        assert stmt.query is None
        with pytest.raises(SQLSyntaxError):
            parse_statement("INSERT INTO t (id) DEFAULT VALUES")

    def test_insert_select_with_columns(self):
        stmt = parse_statement("INSERT INTO t (a, b) SELECT x, y FROM src")
        assert stmt.columns == ["a", "b"]
        assert stmt.query is not None

    def test_with_insert_select(self):
        stmt = parse_statement(
            "WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT a FROM x"
        )
        assert isinstance(stmt, Insert)
        assert stmt.ctes[0].name == "x"
        assert stmt.query is not None

    def test_update(self):
        stmt = parse_statement("UPDATE t SET a = a + 1, b = 2 WHERE id = 1")
        assert len(stmt.assignments) == 2
        assert stmt.where is not None

    def test_update_set_default(self):
        from lumendb.sql.ast import DefaultValue

        stmt = parse_statement("UPDATE t SET a = DEFAULT, b = 2")
        assert isinstance(stmt.assignments[0][1], DefaultValue)
        conflict = parse_statement(
            "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET a = DEFAULT"
        )
        assert isinstance(conflict.on_conflict_update[0][1], DefaultValue)

    def test_delete(self):
        stmt = parse_statement("DELETE FROM t WHERE id = 1")
        assert stmt.table == "t" and stmt.where is not None

    def test_delete_all(self):
        assert parse_statement("DELETE FROM t").where is None


class TestSelect:
    def test_star(self):
        stmt = parse_statement("SELECT * FROM t")
        assert isinstance(stmt.select.items[0].expr, Star)

    def test_table_star(self):
        stmt = parse_statement("SELECT t.* FROM t")
        assert stmt.select.items[0].expr.table == "t"

    def test_alias_with_as(self):
        stmt = parse_statement("SELECT id AS x FROM t")
        assert stmt.select.items[0].alias == "x"

    def test_alias_without_as(self):
        stmt = parse_statement("SELECT id x FROM t")
        assert stmt.select.items[0].alias == "x"

    def test_no_from(self):
        stmt = parse_statement("SELECT 1")
        assert stmt.select.from_ == []

    def test_distinct(self):
        assert parse_statement("SELECT DISTINCT a FROM t").select.distinct

    def test_where_group_having_order_limit(self):
        stmt = parse_statement(
            "SELECT a, COUNT(*) AS c FROM t WHERE a > 1 GROUP BY a "
            "HAVING COUNT(*) > 2 ORDER BY a DESC LIMIT 10 OFFSET 5"
        )
        sel = stmt.select
        assert sel.where is not None
        assert len(sel.group_by) == 1
        assert sel.having is not None
        assert sel.order_by[0].descending
        assert sel.limit == 10 and sel.offset == 5

    def test_multi_table_from(self):
        stmt = parse_statement("SELECT * FROM a, b")
        assert [r.name for r in stmt.select.from_] == ["a", "b"]

    def test_join(self):
        stmt = parse_statement("SELECT * FROM a JOIN b ON a.id = b.id")
        assert len(stmt.select.from_[0].joins) == 1
        assert stmt.select.from_[0].joins[0].kind == "INNER"

    def test_left_join(self):
        stmt = parse_statement("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        assert stmt.select.from_[0].joins[0].kind == "LEFT"

    def test_right_join(self):
        stmt = parse_statement("SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        assert stmt.select.from_[0].joins[0].kind == "RIGHT"

    def test_full_join(self):
        stmt = parse_statement("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")
        assert stmt.select.from_[0].joins[0].kind == "FULL"

    def test_cross_join(self):
        stmt = parse_statement("SELECT * FROM a CROSS JOIN b")
        assert stmt.select.from_[0].joins[0].kind == "INNER"
        assert stmt.select.from_[0].joins[0].on is None

    def test_left_join_requires_on(self):
        with pytest.raises(SQLSyntaxError):
            parse_statement("SELECT * FROM a LEFT JOIN b")

    def test_cte_parses(self):
        stmt = parse_statement("WITH x AS (SELECT 1 AS a) SELECT a FROM x")
        assert stmt.ctes[0].name == "x"
        assert stmt.select.from_[0].name == "x"

    def test_cte_column_list(self):
        stmt = parse_statement(
            "WITH x(a, b) AS (SELECT 1, 2) SELECT a, b FROM x"
        )
        assert stmt.ctes[0].columns == ["a", "b"]

    def test_with_update(self):
        stmt = parse_statement(
            "WITH x AS (SELECT 2 AS id) UPDATE t SET a = 1 "
            "WHERE id IN (SELECT id FROM x) RETURNING id"
        )
        assert isinstance(stmt, Update)
        assert stmt.ctes[0].name == "x"
        assert stmt.returning is not None

    def test_with_delete(self):
        stmt = parse_statement(
            "WITH x AS (SELECT 1 AS id) DELETE FROM t "
            "WHERE id IN (SELECT id FROM x)"
        )
        assert isinstance(stmt, Delete)
        assert stmt.ctes[0].name == "x"

    def test_multiple_ctes(self):
        stmt = parse_statement(
            "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM b"
        )
        assert [c.name for c in stmt.ctes] == ["a", "b"]

    def test_recursive_cte(self):
        from lumendb.sql.ast import Query

        stmt = parse_statement(
            "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM x"
        )
        assert isinstance(stmt, Query)
        assert stmt.recursive
        assert len(stmt.ctes) == 1


    def test_order_by_nulls(self):
        stmt = parse_statement("SELECT 1 ORDER BY 1 ASC NULLS FIRST")
        assert stmt.select.order_by[0].nulls_first is True
        stmt = parse_statement("SELECT 1 ORDER BY 1 DESC NULLS LAST")
        assert stmt.select.order_by[0].nulls_first is False


class TestExpressions:
    def test_arithmetic_precedence(self):
        expr = parse_statement("SELECT 1 + 2 * 3").select.items[0].expr
        assert isinstance(expr, BinaryOp) and expr.op == "+"
        assert isinstance(expr.right, BinaryOp) and expr.right.op == "*"

    def test_parentheses_override(self):
        expr = parse_statement("SELECT (1 + 2) * 3").select.items[0].expr
        assert expr.op == "*"

    def test_unary_minus(self):
        expr = parse_statement("SELECT -5").select.items[0].expr
        assert isinstance(expr, UnaryOp) and expr.op == "-"

    def test_and_binds_tighter_than_or(self):
        expr = parse_statement("SELECT 1 WHERE a OR b AND c").select.where
        assert expr.op == "OR"
        from lumendb.sql.ast import ColumnRef

        assert isinstance(expr.right, BinaryOp) and expr.right.op == "AND"

    def test_not(self):
        expr = parse_statement("SELECT 1 WHERE NOT a").select.where
        assert isinstance(expr, UnaryOp) and expr.op == "NOT"

    def test_comparison_chain(self):
        expr = parse_statement("SELECT 1 WHERE a = 1 AND b <> 2").select.where
        assert expr.op == "AND"

    def test_is_null(self):
        expr = parse_statement("SELECT 1 WHERE a IS NULL").select.where
        assert isinstance(expr, IsNull) and not expr.negated

    def test_is_not_null(self):
        expr = parse_statement("SELECT 1 WHERE a IS NOT NULL").select.where
        assert isinstance(expr, IsNull) and expr.negated

    def test_is_distinct_from(self):
        from lumendb.sql.ast import IsDistinct

        expr = parse_statement(
            "SELECT 1 WHERE a IS DISTINCT FROM b"
        ).select.where
        assert isinstance(expr, IsDistinct) and not expr.negated
        expr = parse_statement(
            "SELECT 1 WHERE a IS NOT DISTINCT FROM b"
        ).select.where
        assert isinstance(expr, IsDistinct) and expr.negated

    def test_in_list(self):
        expr = parse_statement("SELECT 1 WHERE a IN (1, 2, 3)").select.where
        assert isinstance(expr, InList) and len(expr.items) == 3

    def test_not_in_list(self):
        expr = parse_statement("SELECT 1 WHERE a NOT IN (1)").select.where
        assert isinstance(expr, InList) and expr.negated

    def test_in_subquery(self):
        expr = parse_statement("SELECT 1 WHERE a IN (SELECT id FROM t)").select.where
        assert isinstance(expr, InSubquery)

    def test_between(self):
        expr = parse_statement("SELECT 1 WHERE a BETWEEN 1 AND 5").select.where
        assert isinstance(expr, Between)
        assert expr.low == Literal(1) and expr.high == Literal(5)

    def test_not_between(self):
        expr = parse_statement("SELECT 1 WHERE a NOT BETWEEN 1 AND 5").select.where
        assert isinstance(expr, Between) and expr.negated

    def test_like(self):
        expr = parse_statement("SELECT 1 WHERE a LIKE '%x%'").select.where
        assert isinstance(expr, Like)

    def test_not_like(self):
        expr = parse_statement("SELECT 1 WHERE a NOT LIKE 'x'").select.where
        assert isinstance(expr, Like) and expr.negated

    def test_like_escape(self):
        expr = parse_statement(
            "SELECT 1 WHERE a LIKE 'x!%' ESCAPE '!'"
        ).select.where
        assert isinstance(expr, Like)
        assert isinstance(expr.escape, Literal)
        assert expr.escape.value == "!"

    def test_scalar_subquery(self):
        expr = parse_statement("SELECT (SELECT 1)").select.items[0].expr
        assert isinstance(expr, ScalarSubquery)

    def test_exists(self):
        expr = parse_statement("SELECT 1 WHERE EXISTS (SELECT 1)").select.where
        assert isinstance(expr, Exists)

    def test_function(self):
        expr = parse_statement("SELECT COUNT(*) FROM t").select.items[0].expr
        assert expr.name == "COUNT" and isinstance(expr.args[0], Star)

    def test_count_distinct(self):
        expr = parse_statement("SELECT COUNT(DISTINCT a) FROM t").select.items[0].expr
        assert expr.distinct

    def test_two_arg_function(self):
        expr = parse_statement("SELECT COALESCE(a, 0) FROM t").select.items[0].expr
        assert len(expr.args) == 2

    def test_cast(self):
        expr = parse_statement("SELECT CAST(a AS BIGINT) FROM t").select.items[0].expr
        assert isinstance(expr, Cast) and expr.target.kind == DataType.BIGINT

    def test_qualified_column(self):
        expr = parse_statement("SELECT t.a FROM t").select.items[0].expr
        assert isinstance(expr, ColumnRef) and expr.table == "t" and expr.name == "a"

    def test_boolean_literals(self):
        assert parse_statement("SELECT TRUE").select.items[0].expr == Literal(True)
        assert parse_statement("SELECT NULL").select.items[0].expr == Literal(None)

    def test_string_literal(self):
        assert parse_statement("SELECT 'x'").select.items[0].expr == Literal("x")

    def test_searched_case(self):
        stmt = parse_statement("SELECT CASE WHEN a > 0 THEN 'p' ELSE 'n' END FROM t")
        expr = stmt.select.items[0].expr
        assert isinstance(expr, CaseWhen)
        assert expr.operand is None
        assert expr.otherwise is not None
        assert len(expr.branches) == 1

    def test_simple_case(self):
        stmt = parse_statement("SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t")
        expr = stmt.select.items[0].expr
        assert isinstance(expr, CaseWhen)
        assert expr.operand is not None
        assert expr.otherwise is None
        assert len(expr.branches) == 2

    def test_case_requires_a_branch(self):
        with pytest.raises(SQLSyntaxError):
            parse_statement("SELECT CASE END")


class TestMaintenance:
    def test_vacuum_table(self):
        stmt = parse_statement("VACUUM users")
        assert isinstance(stmt, Vacuum)
        assert stmt.table == "users"

    def test_vacuum_all_tables(self):
        assert isinstance(parse_statement("VACUUM"), Vacuum)
        assert parse_statement("VACUUM").table is None

    def test_vacuum_full(self):
        stmt = parse_statement("VACUUM FULL users")
        assert isinstance(stmt, Vacuum)
        assert stmt.table == "users"
        assert parse_statement("VACUUM FULL").table is None

    def test_reindex_table_keyword(self):
        from lumendb.sql.ast import Reindex

        stmt = parse_statement("REINDEX TABLE users")
        assert isinstance(stmt, Reindex)
        assert stmt.table == "users"

    def test_truncate_with_table_keyword(self):
        stmt = parse_statement("TRUNCATE TABLE users")
        assert isinstance(stmt, Truncate)
        assert stmt.table == "users"

    def test_truncate_without_table_keyword(self):
        assert parse_statement("TRUNCATE users").table == "users"


class TestReturning:
    def test_insert_returning(self):
        stmt = parse_statement(
            "INSERT INTO t VALUES (1, 'a') RETURNING id, name AS n"
        )
        assert len(stmt.returning) == 2
        assert isinstance(stmt.returning[0].expr, ColumnRef)
        assert stmt.returning[1].alias == "n"

    def test_update_returning_star(self):
        stmt = parse_statement("UPDATE t SET x = 1 RETURNING *")
        assert isinstance(stmt.returning[0].expr, Star)

    def test_delete_returning_expression(self):
        stmt = parse_statement("DELETE FROM t WHERE id = 1 RETURNING id + 1")
        assert stmt.returning is not None
        assert stmt.returning[0].expr is not None


class TestInsertConflict:
    def test_on_conflict_do_nothing(self):
        stmt = parse_statement(
            "INSERT INTO t VALUES (1, 'a') ON CONFLICT DO NOTHING"
        )
        assert isinstance(stmt, Insert)
        assert stmt.on_conflict_nothing is True

    def test_conflict_columns_accepted(self):
        stmt = parse_statement(
            "INSERT INTO t VALUES (1) ON CONFLICT (id) DO NOTHING"
        )
        assert stmt.on_conflict_nothing is True

    def test_do_update(self):
        stmt = parse_statement(
            "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET a = 2"
        )
        assert stmt.on_conflict_update is not None
        assert stmt.on_conflict_update[0][0] == "a"
        assert isinstance(stmt.on_conflict_update[0][1], Literal)


class TestParameters:
    def test_param_in_where(self):
        stmt = parse_statement("SELECT * FROM t WHERE id = $2")
        assert isinstance(stmt.select.where.right, ParamRef)
        assert stmt.select.where.right.index == 2

    def test_param_in_limit(self):
        stmt = parse_statement("SELECT * FROM t LIMIT $1")
        assert isinstance(stmt.select.limit, ParamRef)
        assert stmt.select.limit.index == 1

    def test_limit_all(self):
        stmt = parse_statement("SELECT * FROM t ORDER BY id LIMIT ALL")
        assert stmt.select.limit is None

    def test_offset_rows_without_limit(self):
        stmt = parse_statement("SELECT * FROM t OFFSET 2 ROWS")
        assert stmt.select.limit is None
        assert stmt.select.offset == 2

    def test_fetch_first_rows_only(self):
        stmt = parse_statement(
            "SELECT * FROM t ORDER BY id OFFSET 1 ROW FETCH NEXT 2 ROWS ONLY"
        )
        assert stmt.select.limit == 2
        assert stmt.select.offset == 1
        one = parse_statement("SELECT * FROM t FETCH FIRST ROW ONLY")
        assert one.select.limit == 1

    def test_fetch_with_ties(self):
        stmt = parse_statement(
            "SELECT * FROM t ORDER BY id FETCH FIRST 2 ROWS WITH TIES"
        )
        assert stmt.select.limit == 2
        assert stmt.select.with_ties is True
        only = parse_statement(
            "SELECT * FROM t ORDER BY id FETCH FIRST 2 ROWS ONLY"
        )
        assert only.select.with_ties is False

    def test_zero_parameter_rejected(self):
        with pytest.raises(SQLSyntaxError):
            parse_statement("SELECT * FROM t WHERE id = $0")


class TestTransactions:
    def test_begin(self):
        stmt = parse_statement("BEGIN")
        assert stmt.read_only is False

    def test_start_transaction(self):
        assert parse_statement("START TRANSACTION") is not None

    def test_start_transaction_read_only(self):
        assert parse_statement("START TRANSACTION READ ONLY").read_only is True

    def test_commit(self):
        assert parse_statement("COMMIT") is not None

    def test_rollback(self):
        assert parse_statement("ROLLBACK") is not None

    def test_savepoint(self):
        stmt = parse_statement("SAVEPOINT s")
        assert stmt.name == "s"

    def test_rollback_to_savepoint(self):
        stmt = parse_statement("ROLLBACK TO SAVEPOINT s")
        assert stmt.name == "s"

    def test_release_savepoint(self):
        stmt = parse_statement("RELEASE SAVEPOINT s")
        assert stmt.name == "s"

    def test_explain(self):
        stmt = parse_statement("EXPLAIN SELECT 1")
        assert stmt.statement.select is not None
        assert stmt.analyze is False

    def test_explain_analyze(self):
        stmt = parse_statement("EXPLAIN ANALYZE SELECT 1")
        assert stmt.analyze is True
        assert stmt.statement.select is not None

    def test_show_tables(self):
        assert parse_statement("SHOW TABLES") is not None

    def test_checkpoint(self):
        assert parse_statement("CHECKPOINT") is not None


class TestCompoundSelect:
    def test_union_defaults_to_distinct(self):
        stmt = parse_statement("SELECT 1 UNION SELECT 2")
        assert isinstance(stmt.select, CompoundSelect)
        assert stmt.select.parts and len(stmt.select.parts) == 2
        assert stmt.select.all_flags == [False]

    def test_union_all(self):
        stmt = parse_statement("SELECT 1 UNION ALL SELECT 2")
        assert stmt.select.all_flags == [True]

    def test_union_tail_applies_to_the_whole_query(self):
        stmt = parse_statement("SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT 1")
        assert isinstance(stmt.select, CompoundSelect)
        assert len(stmt.select.order_by) == 1
        assert stmt.select.limit == 1
        assert stmt.select.parts[1].order_by == []

    def test_except_and_intersect(self):
        stmt = parse_statement("SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3")
        assert isinstance(stmt.select, CompoundSelect)
        assert stmt.select.ops == ["EXCEPT"]
        assert stmt.select.parts[1].ops == ["INTERSECT"]

    def test_intersect_chain(self):
        stmt = parse_statement("SELECT 1 INTERSECT SELECT 2 INTERSECT SELECT 3")
        assert stmt.select.ops == ["INTERSECT"]
        assert stmt.select.parts[0].ops == ["INTERSECT"]

    def test_except_all_flags(self):
        stmt = parse_statement("SELECT 1 EXCEPT ALL SELECT 2 INTERSECT ALL SELECT 3")
        assert stmt.select.all_flags == [True]
        assert stmt.select.ops == ["EXCEPT"]
        assert stmt.select.parts[1].all_flags == [True]
        assert stmt.select.parts[1].ops == ["INTERSECT"]


class TestAlterTable:
    def test_add_column(self):
        stmt = parse_statement("ALTER TABLE users ADD COLUMN age INT DEFAULT 18")
        assert stmt.table == "users"
        assert stmt.column.name == "age"
        assert stmt.column.default is not None

    def test_add_column_without_column_keyword(self):
        stmt = parse_statement("ALTER TABLE users ADD age INT")
        assert stmt.column.name == "age"

    def test_add_column_if_not_exists(self):
        stmt = parse_statement(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS age INT"
        )
        assert stmt.column.name == "age"
        assert stmt.if_not_exists is True

    def test_alter_column_default(self):
        from lumendb.sql.ast import AlterTableAlterDefault

        stmt = parse_statement(
            "ALTER TABLE users ALTER COLUMN age SET DEFAULT 18"
        )
        assert isinstance(stmt, AlterTableAlterDefault)
        assert stmt.column == "age"
        assert stmt.default.value == 18
        dropped = parse_statement(
            "ALTER TABLE users ALTER COLUMN age DROP DEFAULT"
        )
        assert dropped.default is None

    def test_alter_column_not_null(self):
        from lumendb.sql.ast import AlterTableAlterNullability

        stmt = parse_statement(
            "ALTER TABLE users ALTER COLUMN age SET NOT NULL"
        )
        assert isinstance(stmt, AlterTableAlterNullability)
        assert stmt.column == "age"
        assert stmt.nullable is False
        dropped = parse_statement(
            "ALTER TABLE users ALTER age DROP NOT NULL"
        )
        assert isinstance(dropped, AlterTableAlterNullability)
        assert dropped.nullable is True

    def test_primary_key_rejected(self):
        with pytest.raises(NotSupportedError):
            parse_statement("ALTER TABLE users ADD COLUMN id INT PRIMARY KEY")

    def test_add_unique_constraint(self):
        stmt = parse_statement(
            "ALTER TABLE users ADD CONSTRAINT uq_ab UNIQUE (name, age)"
        )
        assert stmt.table == "users"
        assert stmt.constraint.kind == "UNIQUE"
        assert stmt.constraint.name == "uq_ab"
        assert stmt.constraint.columns == ["name", "age"]

    def test_add_primary_key_constraint(self):
        stmt = parse_statement(
            "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (name, age)"
        )
        assert stmt.constraint.kind == "PRIMARY KEY"
        assert stmt.constraint.name == "pk_users"

    def test_rename_table(self):
        stmt = parse_statement("ALTER TABLE users RENAME TO accounts")
        assert isinstance(stmt, AlterTableRenameTable)
        assert stmt.table == "users"
        assert stmt.new_name == "accounts"

    def test_rename_table_with_table_keyword(self):
        stmt = parse_statement("ALTER TABLE users RENAME TABLE TO accounts")
        assert isinstance(stmt, AlterTableRenameTable)

    def test_rename_column(self):
        stmt = parse_statement("ALTER TABLE users RENAME COLUMN name TO label")
        assert isinstance(stmt, AlterTableRenameColumn)
        assert stmt.column == "name"
        assert stmt.new_name == "label"

    def test_rename_column_without_column_keyword(self):
        stmt = parse_statement("ALTER TABLE users RENAME name TO label")
        assert isinstance(stmt, AlterTableRenameColumn)


class TestMultipleStatements:
    def test_two_statements(self):
        stmts = parse_sql("SELECT 1; SELECT 2;")
        assert len(stmts) == 2

    def test_trailing_semicolon_optional(self):
        assert len(parse_sql("SELECT 1")) == 1

    def test_empty_statements_skipped(self):
        assert len(parse_sql(";;SELECT 1;;")) == 1

    def test_syntax_error_in_second(self):
        with pytest.raises(SQLSyntaxError):
            parse_sql("SELECT 1; SELECT FROM")

    def test_statement_without_semicolon_then_garbage(self):
        with pytest.raises(SQLSyntaxError):
            parse_sql("SELECT 1 2")


class TestErrorQuality:
    def test_error_has_position(self):
        with pytest.raises(SQLSyntaxError) as exc:
            parse_statement("SELECT id FROM")
        assert "行" in str(exc.value) and "列" in str(exc.value)

    def test_unknown_statement(self):
        with pytest.raises(SQLSyntaxError):
            parse_statement("FROBNICATE t")

    def test_unclosed_paren(self):
        with pytest.raises(SQLSyntaxError):
            parse_statement("SELECT (1 + 2")
