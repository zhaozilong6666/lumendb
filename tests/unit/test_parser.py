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
)
from lumendb.sql.parser import parse_sql, parse_statement


class TestCreateTable:
    def test_basic(self):
        stmt = parse_statement("CREATE TABLE t (id INT, name TEXT)")
        assert stmt.name == "t"
        assert len(stmt.columns) == 2
        assert stmt.columns[0].type.kind == DataType.INT

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

    def test_create_view_rejected(self):
        with pytest.raises(NotSupportedError):
            parse_statement("CREATE VIEW v AS SELECT 1")


class TestCreateIndex:
    def test_single_column(self):
        stmt = parse_statement("CREATE INDEX i ON t (a)")
        assert stmt.name == "i" and stmt.table == "t" and stmt.columns == ["a"]

    def test_composite(self):
        stmt = parse_statement("CREATE INDEX i ON t (a, b, c)")
        assert stmt.columns == ["a", "b", "c"]

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

    def test_update(self):
        stmt = parse_statement("UPDATE t SET a = a + 1, b = 2 WHERE id = 1")
        assert len(stmt.assignments) == 2
        assert stmt.where is not None

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

    def test_right_join_rejected(self):
        with pytest.raises(NotSupportedError):
            parse_statement("SELECT * FROM a RIGHT JOIN b ON a.id = b.id")

    def test_full_join_rejected(self):
        with pytest.raises(NotSupportedError):
            parse_statement("SELECT * FROM a FULL JOIN b ON a.id = b.id")

    def test_left_join_requires_on(self):
        with pytest.raises(SQLSyntaxError):
            parse_statement("SELECT * FROM a LEFT JOIN b")

    def test_cte_parses(self):
        stmt = parse_statement("WITH x AS (SELECT 1 AS a) SELECT a FROM x")
        assert stmt.ctes[0].name == "x"
        assert stmt.select.from_[0].name == "x"

    def test_multiple_ctes(self):
        stmt = parse_statement(
            "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM b"
        )
        assert [c.name for c in stmt.ctes] == ["a", "b"]

    def test_recursive_cte_rejected(self):
        with pytest.raises(NotSupportedError):
            parse_statement("WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x")


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

    def test_truncate_with_table_keyword(self):
        stmt = parse_statement("TRUNCATE TABLE users")
        assert isinstance(stmt, Truncate)
        assert stmt.table == "users"

    def test_truncate_without_table_keyword(self):
        assert parse_statement("TRUNCATE users").table == "users"


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

    def test_do_update_not_supported(self):
        with pytest.raises(NotSupportedError):
            parse_statement(
                "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET a = 2"
            )


class TestParameters:
    def test_param_in_where(self):
        stmt = parse_statement("SELECT * FROM t WHERE id = $2")
        assert isinstance(stmt.select.where.right, ParamRef)
        assert stmt.select.where.right.index == 2

    def test_param_in_limit(self):
        stmt = parse_statement("SELECT * FROM t LIMIT $1")
        assert isinstance(stmt.select.limit, ParamRef)
        assert stmt.select.limit.index == 1

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

    def test_except_still_rejected(self):
        with pytest.raises(NotSupportedError):
            parse_statement("SELECT 1 EXCEPT SELECT 2")


class TestAlterTable:
    def test_add_column(self):
        stmt = parse_statement("ALTER TABLE users ADD COLUMN age INT DEFAULT 18")
        assert stmt.table == "users"
        assert stmt.column.name == "age"
        assert stmt.column.default is not None

    def test_add_column_without_column_keyword(self):
        stmt = parse_statement("ALTER TABLE users ADD age INT")
        assert stmt.column.name == "age"

    def test_primary_key_rejected(self):
        with pytest.raises(NotSupportedError):
            parse_statement("ALTER TABLE users ADD COLUMN id INT PRIMARY KEY")


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
