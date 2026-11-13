"""SQL 解析：递归下降 + 运算符优先级爬升。

原则是"支持的语法必须正确，不支持的语法必须明确报错"。因此这里没有
"宽容解析"分支：遇到尚未实现的子语法，直接抛 ``NotSupportedError``，
而不是悄悄当成别的东西。
"""

from __future__ import annotations

from typing import Optional

from ..datatypes import DataType, TypeSpec
from ..errors import NotSupportedError, SQLSyntaxError
from .ast import (
    Between,
    Begin,
    BinaryOp,
    Cast,
    CaseWhen,
    Checkpoint,
    ColumnDef,
    ColumnRef,
    Commit,
    CompoundSelect,
    CreateIndex,
    CreateTable,
    CreateTableAs,
    CreateView,
    Cte,
    Delete,
    DefaultValue,
    DropIndex,
    DropTable,
    DropView,
    Exists,
    Explain,
    FunctionCall,
    InList,
    InSubquery,
    Insert,
    IsDistinct,
    IsNull,
    Join,
    Like,
    Literal,
    OrderItem,
    ParamRef,
    Query,
    Reindex,
    ReleaseSavepoint,
    Rollback,
    RollbackToSavepoint,
    Savepoint,
    ScalarSubquery,
    Select,
    SelectItem,
    ShowTables,
    ShowViews,
    TableConstraint,
    Star,
    Statement,
    TableRef,
    UnaryOp,
    Update,
    Vacuum,
    Truncate,
    AlterTableAddColumn,
    AlterTableAddConstraint,
    AlterTableAlterDefault,
    AlterTableAlterNullability,
    AlterTableRenameColumn,
    AlterTableRenameTable,
)
from .lexer import RESERVED, Token, tokenize

_TYPE_NAMES = {
    "INT": DataType.INT,
    "INTEGER": DataType.INT,
    "BIGINT": DataType.BIGINT,
    "TEXT": DataType.TEXT,
    "VARCHAR": DataType.VARCHAR,
    "CHAR": DataType.VARCHAR,
    "BOOLEAN": DataType.BOOLEAN,
    "BOOL": DataType.BOOLEAN,
    "FLOAT": DataType.FLOAT,
    "DOUBLE": DataType.FLOAT,
    "REAL": DataType.FLOAT,
}

_COMPARISON_OPS = frozenset({"<>", "!=", "<=", ">=", "=", "<", ">"})


class Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._i = 0

    # ------------------------------------------------------------------
    # token 游标
    # ------------------------------------------------------------------

    @property
    def _cur(self) -> Token:
        return self._tokens[self._i]

    def _advance(self) -> Token:
        tok = self._tokens[self._i]
        if tok.kind != "eof":
            self._i += 1
        return tok

    def _error(self, message: str, hint: str = "") -> SQLSyntaxError:
        tok = self._cur
        return SQLSyntaxError(
            message,
            detail=f"位置：第 {tok.line} 行第 {tok.col} 列（附近是 {tok.value!r}）",
            hint=hint,
        )

    def _match_kw(self, *names: str) -> Optional[Token]:
        if self._cur.is_keyword(*names):
            return self._advance()
        return None

    def _expect_kw(self, *names: str) -> Token:
        tok = self._match_kw(*names)
        if tok is None:
            raise self._error(f"期望关键字 {' 或 '.join(names)}")
        return tok

    def _match_punct(self, name: str) -> Optional[Token]:
        if self._cur.is_punct(name):
            return self._advance()
        return None

    def _expect_punct(self, name: str, display: str) -> Token:
        tok = self._match_punct(name)
        if tok is None:
            raise self._error(f"期望 {display}")
        return tok

    def _match_op(self, *ops: str) -> Optional[Token]:
        if self._cur.is_op(*ops):
            return self._advance()
        return None

    def _at_end(self) -> bool:
        return self._cur.kind == "eof"

    def _end_statement(self) -> None:
        """语句结束：分号（可省略）或 EOF。"""
        self._match_punct("semicolon")

    # ------------------------------------------------------------------
    # 入口
    # ------------------------------------------------------------------

    def parse_statements(self) -> list[Statement]:
        result: list[Statement] = []
        while not self._at_end():
            if self._match_punct("semicolon"):
                continue
            result.append(self._parse_statement())
            if self._cur.is_keyword("EXCEPT", "INTERSECT"):
                raise NotSupportedError(
                    "集合运算仅支持 UNION / UNION ALL",
                    hint="EXCEPT / INTERSECT 尚未实现",
                )
            if not self._at_end() and not self._cur.is_punct("semicolon"):
                raise self._error(f"语句后出现多余的内容 {self._cur.value!r}")
            self._end_statement()
        return result

    def parse_one(self) -> Statement:
        stmts = self.parse_statements()
        if len(stmts) != 1:
            raise SQLSyntaxError(f"期望 1 条语句，实际 {len(stmts)} 条")
        return stmts[0]

    # ------------------------------------------------------------------
    # 语句
    # ------------------------------------------------------------------

    def _parse_statement(self) -> Statement:
        tok = self._cur
        if tok.kind != "keyword":
            raise self._error(f"语句必须以关键字开头，实际是 {tok.value!r}")

        kw = tok.keyword
        if kw == "SELECT":
            return Query(self._parse_query_expression())
        if kw == "CREATE":
            return self._parse_create()
        if kw == "DROP":
            return self._parse_drop()
        if kw == "ALTER":
            return self._parse_alter_table()
        if kw == "WITH":
            return self._parse_with()
        if kw == "INSERT":
            return self._parse_insert()
        if kw == "UPDATE":
            return self._parse_update()
        if kw == "DELETE":
            return self._parse_delete()
        if kw in ("BEGIN", "START"):
            self._advance()
            if kw == "START":
                self._expect_kw("TRANSACTION")
            else:
                self._match_kw("TRANSACTION")
            read_only = False
            if self._match_kw("READ"):
                self._expect_kw("ONLY")
                read_only = True
            return Begin(read_only=read_only)
        if kw == "COMMIT":
            self._advance()
            self._match_kw("TRANSACTION")
            return Commit()
        if kw == "ROLLBACK":
            self._advance()
            self._match_kw("TRANSACTION")
            if self._match_kw("TO") is not None:
                self._match_kw("SAVEPOINT")
                return RollbackToSavepoint(self._parse_ident("保存点名"))
            return Rollback()
        if kw == "SAVEPOINT":
            self._advance()
            return Savepoint(self._parse_ident("保存点名"))
        if kw == "RELEASE":
            self._advance()
            self._match_kw("SAVEPOINT")
            return ReleaseSavepoint(self._parse_ident("保存点名"))
        if kw == "EXPLAIN":
            self._advance()
            analyze = self._match_kw("ANALYZE") is not None
            inner = self._parse_statement()
            return Explain(inner, analyze=analyze)
        if kw == "SHOW":
            self._advance()
            if self._match_kw("TABLES"):
                return ShowTables()
            if self._match_kw("VIEWS"):
                return ShowViews()
            raise self._error("SHOW 之后期望 TABLES 或 VIEWS")
        if kw == "CHECKPOINT":
            self._advance()
            return Checkpoint()
        if kw == "REINDEX":
            self._advance()
            table = self._parse_ident("表名")
            return Reindex(table)
        if kw == "VACUUM":
            self._advance()
            table = None
            if self._cur.kind == "ident":
                table = self._parse_ident("表名")
            return Vacuum(table)
        if kw == "TRUNCATE":
            self._advance()
            self._match_kw("TABLE")
            return Truncate(self._parse_ident("表名"))
        raise self._error(
            f"无法识别的语句起始关键字 {tok.value.upper()}",
            hint="支持 SELECT / INSERT / UPDATE / DELETE / CREATE / DROP / BEGIN / COMMIT / "
            "ROLLBACK / EXPLAIN / SHOW TABLES / CHECKPOINT / REINDEX / VACUUM",
        )

    def _parse_create(self) -> Statement:
        self._expect_kw("CREATE")
        or_replace = False
        if self._match_kw("OR"):
            self._expect_kw("REPLACE")
            or_replace = True
        unique = self._match_kw("UNIQUE") is not None
        if self._match_kw("TABLE"):
            if or_replace:
                raise self._error("OR REPLACE 只支持视图")
            if unique:
                raise self._error("UNIQUE 只能用于 CREATE INDEX")
            return self._parse_create_table()
        if self._match_kw("INDEX"):
            if or_replace:
                raise self._error("OR REPLACE 只支持视图")
            return self._parse_create_index(unique=unique)
        if unique:
            raise self._error("UNIQUE 只能用于 CREATE INDEX")
        if self._match_kw("VIEW"):
            if unique:
                raise self._error("UNIQUE 不能用于 CREATE VIEW")
            return self._parse_create_view(or_replace=or_replace)
        if self._match_kw("TRIGGER", "PROCEDURE"):
            raise NotSupportedError("触发器 / 存储过程尚未实现")
        raise self._error("CREATE 之后期望 TABLE、INDEX 或 VIEW")

    def _parse_alter_table(self) -> Statement:
        self._expect_kw("ALTER")
        self._expect_kw("TABLE")
        table = self._parse_ident("表名")
        if self._match_kw("ADD"):
            if self._cur.is_keyword("CONSTRAINT", "PRIMARY", "UNIQUE"):
                constraint = self._parse_table_constraint()
                return AlterTableAddConstraint(
                    table=table, constraint=constraint
                )
            self._match_kw("COLUMN")
            if_not_exists = False
            if self._match_kw("IF"):
                self._expect_kw("NOT")
                self._expect_kw("EXISTS")
                if_not_exists = True
            column = self._parse_column_def()
            if column.primary_key or column.unique:
                raise NotSupportedError(
                    "ALTER TABLE ADD COLUMN 暂不支持主键或唯一约束",
                    hint="先加普通列，再 CREATE UNIQUE INDEX",
                )
            return AlterTableAddColumn(
                table=table, column=column, if_not_exists=if_not_exists
            )

        if self._match_kw("ALTER"):
            self._match_kw("COLUMN")
            column = self._parse_ident("列名")
            if self._match_kw("SET"):
                if self._match_kw("NOT"):
                    self._expect_kw("NULL")
                    return AlterTableAlterNullability(
                        table=table, column=column, nullable=False
                    )
                self._expect_kw("DEFAULT")
                default = self._parse_expr()
                return AlterTableAlterDefault(
                    table=table, column=column, default=default
                )
            if self._match_kw("DROP"):
                if self._match_kw("NOT"):
                    self._expect_kw("NULL")
                    return AlterTableAlterNullability(
                        table=table, column=column, nullable=True
                    )
                self._expect_kw("DEFAULT")
                return AlterTableAlterDefault(
                    table=table, column=column, default=None
                )
            raise self._error(
                "ALTER COLUMN 之后期望 SET/DROP DEFAULT 或 SET/DROP NOT NULL"
            )

        if self._match_kw("RENAME"):
            if self._match_kw("TABLE"):
                self._expect_kw("TO")
                new_name = self._parse_ident("新表名")
                return AlterTableRenameTable(table=table, new_name=new_name)
            if self._match_kw("COLUMN"):
                old_name = self._parse_ident("旧列名")
                self._expect_kw("TO")
                new_name = self._parse_ident("新列名")
                return AlterTableRenameColumn(
                    table=table, column=old_name, new_name=new_name
                )
            if self._match_kw("TO"):
                new_name = self._parse_ident("新表名")
                return AlterTableRenameTable(table=table, new_name=new_name)
            old_name = self._parse_ident("旧列名")
            self._expect_kw("TO")
            new_name = self._parse_ident("新列名")
            return AlterTableRenameColumn(
                table=table, column=old_name, new_name=new_name
            )

        raise NotSupportedError(
            "ALTER TABLE 只支持 ADD COLUMN、ALTER COLUMN 和 RENAME",
            hint=(
                "支持 ADD [COLUMN] / ALTER [COLUMN] SET|DROP DEFAULT|NOT NULL / "
                "RENAME TO / RENAME [COLUMN] old TO new"
            ),
        )

    def _parse_with(self) -> Statement:
        self._expect_kw("WITH")
        recursive = self._match_kw("RECURSIVE") is not None
        ctes: list[Cte] = []
        while True:
            name = self._parse_ident("CTE 名")
            columns: list[str] = []
            if self._match_punct("lparen"):
                while True:
                    columns.append(self._parse_ident("CTE 列名"))
                    if not self._match_punct("comma"):
                        break
                self._expect_punct("rparen", ")")
            self._expect_kw("AS")
            self._expect_punct("lparen", "(")
            query = self._parse_query_expression()
            self._expect_punct("rparen", ")")
            ctes.append(Cte(name=name, query=query, columns=columns))
            if not self._match_punct("comma"):
                break
        if self._cur.is_keyword("SELECT"):
            return Query(
                select=self._parse_query_expression(),
                ctes=ctes,
                recursive=recursive,
            )
        if recursive:
            raise NotSupportedError(
                "递归 WITH 后面只支持 SELECT",
                hint="当前递归 CTE 只用于查询",
            )
        if self._cur.is_keyword("INSERT"):
            stmt = self._parse_insert()
            stmt.ctes = ctes
            return stmt
        if self._cur.is_keyword("UPDATE"):
            stmt = self._parse_update()
            stmt.ctes = ctes
            return stmt
        if self._cur.is_keyword("DELETE"):
            stmt = self._parse_delete()
            stmt.ctes = ctes
            return stmt
        raise self._error("WITH 之后期望 SELECT、INSERT、UPDATE 或 DELETE")

    def _parse_create_view(self, or_replace: bool = False) -> CreateView:
        name = self._parse_ident("视图名")
        columns: list[str] = []
        if self._match_punct("lparen"):
            while True:
                columns.append(self._parse_ident("视图列名"))
                if not self._match_punct("comma"):
                    break
            self._expect_punct("rparen", ")")
        self._expect_kw("AS")
        start = self._i
        query = self.parse_view_expression()
        if not isinstance(query, (Query, Select, CompoundSelect)):
            raise NotSupportedError(
                "视图定义只能是 SELECT 查询",
                hint="WITH 之后的 INSERT/UPDATE/DELETE 不能作为视图定义",
            )
        tokens = [(tok.kind, tok.value) for tok in self._tokens[start:self._i]]
        return CreateView(
            name=name,
            query=query,
            tokens=tokens,
            columns=columns,
            or_replace=or_replace,
        )

    def _parse_create_table(self) -> Statement:
        if_not_exists = False
        if self._match_kw("IF"):
            self._expect_kw("NOT")
            self._expect_kw("EXISTS")
            if_not_exists = True
        name = self._parse_ident("表名")
        if self._match_kw("AS"):
            return CreateTableAs(
                name=name,
                query=self._parse_query_expression(),
                if_not_exists=if_not_exists,
            )
        self._expect_punct("lparen", "(")
        columns: list[ColumnDef] = []
        constraints: list[TableConstraint] = []
        while True:
            if self._cur.is_keyword("CONSTRAINT", "PRIMARY", "UNIQUE"):
                constraints.append(self._parse_table_constraint())
            else:
                columns.append(self._parse_column_def())
            if not self._match_punct("comma"):
                break
        self._expect_punct("rparen", ")")
        return CreateTable(
            name=name,
            columns=columns,
            if_not_exists=if_not_exists,
            constraints=constraints,
        )

    def _parse_table_constraint(self) -> TableConstraint:
        name = None
        if self._match_kw("CONSTRAINT"):
            name = self._parse_ident("约束名")
        if self._match_kw("PRIMARY"):
            self._expect_kw("KEY")
            kind = "PRIMARY KEY"
        elif self._match_kw("UNIQUE"):
            kind = "UNIQUE"
        else:
            raise self._error("表级约束期望 PRIMARY KEY 或 UNIQUE")
        self._expect_punct("lparen", "(")
        columns = [self._parse_ident("列名")]
        while self._match_punct("comma"):
            columns.append(self._parse_ident("列名"))
        self._expect_punct("rparen", ")")
        return TableConstraint(kind=kind, columns=columns, name=name)

    def _parse_column_def(self) -> ColumnDef:
        name = self._parse_ident("列名")
        type_spec = self._parse_type()
        nullable = True
        primary_key = False
        unique = False
        default = None

        while True:
            if self._match_kw("PRIMARY"):
                self._expect_kw("KEY")
                primary_key = True
                nullable = False
            elif self._match_kw("NOT"):
                self._expect_kw("NULL")
                nullable = False
            elif self._match_kw("NULL"):
                nullable = True
            elif self._match_kw("UNIQUE"):
                unique = True
            elif self._match_kw("DEFAULT"):
                default = self._parse_expr()
            elif self._match_kw("REFERENCES"):
                raise NotSupportedError(
                    "外键约束尚未实现",
                    detail="REFERENCES 会引入跨表级联与延迟检查",
                    hint="在应用层保证引用完整性",
                )
            elif self._match_kw("CHECK"):
                raise NotSupportedError("CHECK 约束尚未实现")
            elif self._cur.is_keyword("COLLATE", "GENERATED", "AUTOINCREMENT", "AUTO_INCREMENT"):
                raise NotSupportedError(f"{self._cur.value.upper()} 尚未实现")
            else:
                break
        return ColumnDef(
            name=name,
            type=type_spec,
            nullable=nullable,
            primary_key=primary_key,
            unique=unique,
            default=default,
        )

    def _parse_type(self) -> TypeSpec:
        tok = self._cur
        if tok.kind not in ("ident", "keyword"):
            raise self._error("期望类型名")
        name = tok.value.upper()
        data_type = _TYPE_NAMES.get(name)
        if data_type is None:
            raise NotSupportedError(
                f"未知或未支持的类型 {tok.value}",
                hint="支持 INT / BIGINT / VARCHAR(n) / TEXT / BOOLEAN / FLOAT",
            )
        self._advance()
        length = None
        if self._match_punct("lparen"):
            num = self._expect_int()
            self._expect_punct("rparen", ")")
            length = num
        if data_type == DataType.VARCHAR and length is None:
            raise self._error("VARCHAR 需要指定长度，例如 VARCHAR(64)")
        return TypeSpec(data_type, length)

    def _parse_create_index(self, unique: bool = False) -> CreateIndex:
        if_not_exists = False
        if self._match_kw("IF"):
            self._expect_kw("NOT")
            self._expect_kw("EXISTS")
            if_not_exists = True
        name = self._parse_ident("索引名")
        self._expect_kw("ON")
        table = self._parse_ident("表名")
        self._expect_punct("lparen", "(")
        columns = [self._parse_ident("列名")]
        while self._match_punct("comma"):
            columns.append(self._parse_ident("列名"))
        self._expect_punct("rparen", ")")
        return CreateIndex(
            name=name,
            table=table,
            columns=columns,
            unique=unique,
            if_not_exists=if_not_exists,
        )

    def _parse_drop(self) -> Statement:
        self._expect_kw("DROP")
        if self._match_kw("TABLE"):
            if_exists = False
            if self._match_kw("IF"):
                self._expect_kw("EXISTS")
                if_exists = True
            names = [self._parse_ident("表名")]
            while self._match_punct("comma"):
                names.append(self._parse_ident("表名"))
            cascade = self._match_kw("CASCADE") is not None
            self._match_kw("RESTRICT")
            return DropTable(
                names=names, if_exists=if_exists, cascade=cascade
            )
        if self._match_kw("INDEX"):
            if_exists = False
            if self._match_kw("IF"):
                self._expect_kw("EXISTS")
                if_exists = True
            return DropIndex(name=self._parse_ident("索引名"), if_exists=if_exists)
        if self._match_kw("VIEW"):
            if_exists = False
            if self._match_kw("IF"):
                self._expect_kw("EXISTS")
                if_exists = True
            name = self._parse_ident("视图名")
            cascade = self._match_kw("CASCADE") is not None
            self._match_kw("RESTRICT")
            return DropView(name=name, if_exists=if_exists, cascade=cascade)
        raise self._error("DROP 之后期望 TABLE、INDEX 或 VIEW")

    def _parse_insert(self) -> Insert:
        self._expect_kw("INSERT")
        self._expect_kw("INTO")
        table = self._parse_ident("表名")
        columns = None
        if self._match_punct("lparen"):
            columns = [self._parse_ident("列名")]
            while self._match_punct("comma"):
                columns.append(self._parse_ident("列名"))
            self._expect_punct("rparen", ")")
        query = None
        rows: list[list] = []
        default_values = False
        if self._match_kw("DEFAULT"):
            if columns is not None:
                raise self._error("INSERT ... DEFAULT VALUES 不能指定列名")
            self._expect_kw("VALUES")
            default_values = True
        elif self._cur.is_keyword("VALUES"):
            self._advance()
            while True:
                self._expect_punct("lparen", "(")
                row = [self._parse_expr()]
                while self._match_punct("comma"):
                    row.append(self._parse_expr())
                self._expect_punct("rparen", ")")
                rows.append(row)
                if not self._match_punct("comma"):
                    break
        elif self._cur.is_keyword("SELECT"):
            query = self._parse_query_expression()
        else:
            raise self._error("INSERT 之后期望 VALUES、DEFAULT VALUES 或 SELECT")

        on_conflict_nothing = False
        conflict_columns = None
        conflict_constraint = None
        on_conflict_update = None
        on_conflict_where = None
        if self._match_kw("ON"):
            self._expect_kw("CONFLICT")
            if self._match_kw("ON"):
                self._expect_kw("CONSTRAINT")
                conflict_constraint = self._parse_ident("约束名")
            elif self._match_punct("lparen"):
                conflict_columns = [self._parse_ident("列名")]
                while self._match_punct("comma"):
                    conflict_columns.append(self._parse_ident("列名"))
                self._expect_punct("rparen", ")")
            self._expect_kw("DO")
            if self._match_kw("NOTHING"):
                on_conflict_nothing = True
            else:
                self._expect_kw("UPDATE")
                self._expect_kw("SET")
                on_conflict_update = []
                while True:
                    col = self._parse_ident("列名")
                    if self._cur.is_op("="):
                        self._advance()
                    else:
                        raise self._error("赋值处期望 =")
                    on_conflict_update.append((col, self._parse_assignment_expr()))
                    if not self._match_punct("comma"):
                        break
                if self._match_kw("WHERE"):
                    on_conflict_where = self._parse_expr()
        returning = self._parse_returning()
        return Insert(
            table=table,
            columns=columns,
            rows=rows,
            query=query,
            default_values=default_values,
            on_conflict_nothing=on_conflict_nothing,
            conflict_columns=conflict_columns,
            conflict_constraint=conflict_constraint,
            on_conflict_update=on_conflict_update,
            on_conflict_where=on_conflict_where,
            returning=returning,
        )

    def _parse_assignment_expr(self) -> Expr:
        """UPDATE / ON CONFLICT 的赋值右侧：DEFAULT 是独立语义，不是普通表达式。"""
        if self._match_kw("DEFAULT"):
            return DefaultValue()
        return self._parse_expr()

    def _parse_update(self) -> Update:
        self._expect_kw("UPDATE")
        table = self._parse_ident("表名")
        self._expect_kw("SET")
        assignments: list[tuple[str, object]] = []
        while True:
            col = self._parse_ident("列名")
            if self._cur.is_op("="):
                self._advance()
            else:
                raise self._error("赋值处期望 =")
            assignments.append((col, self._parse_assignment_expr()))
            if not self._match_punct("comma"):
                break
        where = None
        if self._match_kw("WHERE"):
            where = self._parse_expr()
        return Update(
            table=table,
            assignments=assignments,
            where=where,
            returning=self._parse_returning(),
        )

    def _parse_delete(self) -> Delete:
        self._expect_kw("DELETE")
        self._expect_kw("FROM")
        table = self._parse_ident("表名")
        where = None
        if self._match_kw("WHERE"):
            where = self._parse_expr()
        return Delete(
            table=table,
            where=where,
            returning=self._parse_returning(),
        )

    def _parse_returning(self) -> Optional[list[SelectItem]]:
        if not self._match_kw("RETURNING"):
            return None
        items = [self._parse_select_item()]
        while self._match_punct("comma"):
            items.append(self._parse_select_item())
        return items

    # ------------------------------------------------------------------
    # SELECT
    # ------------------------------------------------------------------

    def _parse_select(self) -> Select:
        """子查询用：单个 SELECT 块，允许自己的 ORDER BY / LIMIT。"""
        select = self._parse_select_core()
        select.order_by, select.limit, select.offset = self._parse_select_tail()
        return select

    def parse_query_expression(self) -> Select | CompoundSelect:
        """公开入口：把 token 流解析为一条 SELECT / 集合查询。"""
        return self._parse_query_expression()

    def parse_view_expression(self) -> Query | Select | CompoundSelect:
        """视图定义：允许 ``WITH ... SELECT``，也允许直接 SELECT / 集合查询。"""
        if self._cur.is_keyword("WITH"):
            return self._parse_with()
        return self._parse_query_expression()

    def _parse_query_expression(self) -> Select | CompoundSelect:
        """顶层查询：``SELECT ... UNION / EXCEPT / INTERSECT ...``。

        ``INTERSECT`` 的优先级高于 ``UNION`` / ``EXCEPT``；同一优先级内
        从左到右结合。尾部 ORDER BY / LIMIT 只作用于整个表达式。
        """
        node = self._parse_intersect_chain()
        while True:
            tok = self._cur
            if not tok.is_keyword("UNION", "EXCEPT"):
                break
            op = self._advance().value.upper()
            all_rows = self._set_all_flag(op)
            right = self._parse_intersect_chain()
            node = CompoundSelect(
                parts=[node, right],
                all_flags=[all_rows],
                ops=[op],
            )

        order_by, limit, offset = self._parse_select_tail()
        if isinstance(node, CompoundSelect):
            node.order_by = order_by
            node.limit = limit
            node.offset = offset
            return node
        node.order_by = order_by
        node.limit = limit
        node.offset = offset
        return node

    def _parse_intersect_chain(self) -> Select | CompoundSelect:
        node = self._parse_select_core()
        while self._cur.is_keyword("INTERSECT"):
            self._advance()
            all_rows = self._set_all_flag("INTERSECT")
            right = self._parse_select_core()
            node = CompoundSelect(
                parts=[node, right],
                all_flags=[all_rows],
                ops=["INTERSECT"],
            )
        return node

    def _set_all_flag(self, op: str) -> bool:
        if self._match_kw("ALL") is not None:
            return True
        self._match_kw("DISTINCT")
        return False

    def _parse_select_core(self) -> Select:
        self._expect_kw("SELECT")
        distinct = self._match_kw("DISTINCT") is not None

        items = [self._parse_select_item()]
        while self._match_punct("comma"):
            items.append(self._parse_select_item())

        from_: list[TableRef] = []
        if self._match_kw("FROM"):
            from_.append(self._parse_table_ref())
            while self._match_punct("comma"):
                from_.append(self._parse_table_ref())

        where = self._parse_expr() if self._match_kw("WHERE") else None

        group_by: list = []
        if self._match_kw("GROUP"):
            self._expect_kw("BY")
            group_by.append(self._parse_expr())
            while self._match_punct("comma"):
                group_by.append(self._parse_expr())

        having = None
        if self._match_kw("HAVING"):
            having = self._parse_expr()

        return Select(
            items=items,
            from_=from_,
            where=where,
            group_by=group_by,
            having=having,
            distinct=distinct,
        )

    def _parse_select_tail(self):
        order_by: list[OrderItem] = []
        if self._match_kw("ORDER"):
            self._expect_kw("BY")
            order_by.append(self._parse_order_item())
            while self._match_punct("comma"):
                order_by.append(self._parse_order_item())

        limit = None
        offset = None
        limit_seen = False
        offset_seen = False
        if self._match_kw("LIMIT"):
            limit_seen = True
            limit = self._parse_limit_value("LIMIT", allow_all=True)
            if self._match_kw("OFFSET"):
                offset_seen = True
                offset = self._parse_limit_value("OFFSET")
                self._match_kw("ROW") or self._match_kw("ROWS")
        if self._match_kw("OFFSET"):
            if offset_seen:
                raise self._error("OFFSET 重复指定")
            offset_seen = True
            offset = self._parse_limit_value("OFFSET")
            self._match_kw("ROW") or self._match_kw("ROWS")
        if self._match_kw("FETCH"):
            if limit_seen:
                raise self._error("LIMIT 和 FETCH 不能同时使用")
            if self._match_kw("FIRST"):
                pass
            elif self._match_kw("NEXT"):
                pass
            else:
                raise self._error("FETCH 之后期望 FIRST 或 NEXT")
            if self._cur.is_keyword("ROW", "ROWS"):
                limit = 1
            else:
                limit = self._parse_limit_value("FETCH")
            if not (self._match_kw("ROW") or self._match_kw("ROWS")):
                raise self._error("FETCH 之后期望 ROW 或 ROWS")
            if self._match_kw("ONLY"):
                pass
            elif self._match_kw("WITH"):
                self._expect_kw("TIES")
                raise NotSupportedError(
                    "FETCH ... WITH TIES 尚未支持",
                    hint="改用 ONLY，或在 ORDER BY 中加入唯一键后自行处理并列行",
                )
            else:
                raise self._error("FETCH 之后期望 ONLY 或 WITH TIES")
        return order_by, limit, offset

    def _parse_select_item(self) -> SelectItem:
        expr = self._parse_expr()
        alias = None
        if self._match_kw("AS"):
            alias = self._parse_ident("别名")
        elif self._cur.kind == "ident":
            alias = self._advance().value
        return SelectItem(expr=expr, alias=alias)

    def _parse_order_item(self) -> OrderItem:
        expr = self._parse_expr()
        descending = False
        if self._match_kw("ASC"):
            descending = False
        elif self._match_kw("DESC"):
            descending = True
        nulls_first: Optional[bool] = None
        if self._match_kw("NULLS"):
            if self._match_kw("FIRST"):
                nulls_first = True
            elif self._match_kw("LAST"):
                nulls_first = False
            else:
                raise self._error("NULLS 之后期望 FIRST 或 LAST")
        return OrderItem(
            expr=expr, descending=descending, nulls_first=nulls_first
        )

    def _parse_table_ref(self) -> TableRef:
        name = self._parse_ident("表名")
        alias = None
        if self._match_kw("AS"):
            alias = self._parse_ident("别名")
        elif self._cur.kind == "ident":
            alias = self._advance().value
        ref = TableRef(name=name, alias=alias)

        while True:
            kind: Optional[str] = None
            cross = False
            if self._match_kw("JOIN"):
                kind = "INNER"
            elif self._match_kw("INNER"):
                self._expect_kw("JOIN")
                kind = "INNER"
            elif self._match_kw("LEFT"):
                self._match_kw("OUTER")
                self._expect_kw("JOIN")
                kind = "LEFT"
            elif self._match_kw("RIGHT"):
                self._match_kw("OUTER")
                self._expect_kw("JOIN")
                kind = "RIGHT"
            elif self._match_kw("FULL"):
                self._match_kw("OUTER")
                self._expect_kw("JOIN")
                kind = "FULL"
            elif self._match_kw("CROSS"):
                self._expect_kw("JOIN")
                kind = "INNER"
                cross = True
            if kind is None:
                return ref
            right = TableRef(name=self._parse_ident("表名"), alias=None)
            if self._match_kw("AS"):
                right.alias = self._parse_ident("别名")
            elif self._cur.kind == "ident":
                right.alias = self._advance().value
            on = None
            if cross:
                pass
            elif self._match_kw("ON"):
                on = self._parse_expr()
            elif kind in ("LEFT", "RIGHT", "FULL"):
                raise self._error(f"{kind} JOIN 必须带 ON 条件")
            ref.joins.append(Join(kind=kind, table=right, on=on))

    # ------------------------------------------------------------------
    # 表达式
    # ------------------------------------------------------------------

    def _parse_expr(self) -> object:
        return self._parse_or()

    def _parse_or(self):
        left = self._parse_and()
        while self._match_kw("OR"):
            left = BinaryOp("OR", left, self._parse_and())
        return left

    def _parse_and(self):
        left = self._parse_not()
        while self._match_kw("AND"):
            left = BinaryOp("AND", left, self._parse_not())
        return left

    def _parse_not(self):
        if self._match_kw("NOT"):
            return UnaryOp("NOT", self._parse_not())
        return self._parse_comparison()

    def _parse_comparison(self):
        left = self._parse_additive()
        while True:
            tok = self._cur
            if tok.is_op(*_COMPARISON_OPS):
                op = self._advance().value
                if op == "!=":
                    op = "<>"
                left = BinaryOp(op, left, self._parse_additive())
                continue

            if tok.is_keyword("IS"):
                self._advance()
                negated = self._match_kw("NOT") is not None
                if self._match_kw("DISTINCT"):
                    self._expect_kw("FROM")
                    right = self._parse_additive()
                    left = IsDistinct(left, right, negated=negated)
                    continue
                self._expect_kw("NULL")
                left = IsNull(left, negated=negated)
                continue

            negated = False
            if tok.is_keyword("NOT") and self._tokens[self._i + 1].is_keyword(
                "IN", "LIKE", "BETWEEN"
            ):
                self._advance()
                negated = True
                tok = self._cur

            if tok.is_keyword("IN"):
                self._advance()
                self._expect_punct("lparen", "(")
                if self._cur.is_keyword("SELECT"):
                    query = self._parse_select()
                    self._expect_punct("rparen", ")")
                    left = InSubquery(left, query, negated=negated)
                else:
                    items = [self._parse_expr()]
                    while self._match_punct("comma"):
                        items.append(self._parse_expr())
                    self._expect_punct("rparen", ")")
                    left = InList(left, tuple(items), negated=negated)
                continue

            if tok.is_keyword("BETWEEN"):
                self._advance()
                low = self._parse_additive()
                self._expect_kw("AND")
                high = self._parse_additive()
                left = Between(left, low, high, negated=negated)
                continue

            if tok.is_keyword("LIKE"):
                self._advance()
                pattern = self._parse_additive()
                escape = None
                if self._match_kw("ESCAPE"):
                    escape = self._parse_additive()
                left = Like(
                    left, pattern, negated=negated, escape=escape
                )
                continue

            if tok.is_keyword("NOT"):
                raise self._error("NOT 之后期望 IN / LIKE / BETWEEN")
            return left

    def _parse_additive(self):
        left = self._parse_multiplicative()
        while self._cur.is_op("+", "-", "||"):
            op = self._advance().value
            left = BinaryOp(op, left, self._parse_multiplicative())
        return left

    def _parse_multiplicative(self):
        left = self._parse_unary()
        while self._cur.is_op("*", "/", "%"):
            op = self._advance().value
            left = BinaryOp(op, left, self._parse_unary())
        return left

    def _parse_unary(self):
        if self._cur.is_op("-"):
            self._advance()
            return UnaryOp("-", self._parse_unary())
        if self._cur.is_op("+"):
            self._advance()
            return self._parse_unary()
        return self._parse_primary()

    def _parse_primary(self):
        tok = self._cur

        if tok.kind == "int":
            self._advance()
            return Literal(int(tok.value))
        if tok.kind == "param":
            self._advance()
            index = int(tok.value)
            if index < 1:
                raise self._error("参数占位符从 $1 开始，$0 不合法")
            return ParamRef(index)
        if tok.kind == "float":
            self._advance()
            return Literal(float(tok.value))
        if tok.kind == "string":
            self._advance()
            return Literal(tok.value)
        if tok.is_keyword("NULL"):
            self._advance()
            return Literal(None)
        if tok.is_keyword("TRUE"):
            self._advance()
            return Literal(True)
        if tok.is_keyword("FALSE"):
            self._advance()
            return Literal(False)

        if tok.is_keyword("CAST"):
            self._advance()
            self._expect_punct("lparen", "(")
            operand = self._parse_expr()
            self._expect_kw("AS")
            target = self._parse_type()
            self._expect_punct("rparen", ")")
            return Cast(operand, target)

        if tok.is_keyword("EXISTS"):
            self._advance()
            self._expect_punct("lparen", "(")
            query = self._parse_select()
            self._expect_punct("rparen", ")")
            return Exists(query)

        if tok.is_keyword("CASE"):
            return self._parse_case()

        if tok.is_punct("lparen"):
            self._advance()
            if self._cur.is_keyword("SELECT"):
                query = self._parse_select()
                self._expect_punct("rparen", ")")
                return ScalarSubquery(query)
            expr = self._parse_expr()
            self._expect_punct("rparen", ")")
            return expr

        if tok.is_op("*"):
            self._advance()
            return Star()

        if tok.kind in ("ident", "keyword"):
            if tok.kind == "keyword" and tok.keyword in RESERVED:
                raise self._error(f"关键字 {tok.value.upper()} 不能出现在名字位置")
            name = self._advance().value
            if self._cur.is_punct("lparen"):
                return self._parse_function_call(name)
            if self._cur.is_punct("dot"):
                self._advance()
                if self._cur.is_op("*"):
                    self._advance()
                    return Star(table=name)
                col = self._parse_ident("列名")
                return ColumnRef(table=name, name=col)
            return ColumnRef(table=None, name=name)

        raise self._error(f"表达式中出现意外的 {tok.value!r}")

    def _parse_case(self) -> CaseWhen:
        start = self._cur
        self._advance()  # CASE
        operand = None
        if not self._cur.is_keyword("WHEN"):
            operand = self._parse_expr()
        branches: list[tuple[object, object]] = []
        while self._cur.is_keyword("WHEN"):
            self._advance()
            cond = self._parse_expr()
            self._expect_kw("THEN")
            value = self._parse_expr()
            branches.append((cond, value))
        otherwise = None
        if self._match_kw("ELSE") is not None:
            otherwise = self._parse_expr()
        self._expect_kw("END")
        if not branches:
            raise SQLSyntaxError(
                "CASE 至少需要一个 WHEN 分支",
                detail=f"位置：第 {start.line} 行第 {start.col} 列",
                hint="写成 CASE WHEN cond THEN value ... END",
            )
        return CaseWhen(operand, tuple(branches), otherwise)

    def _parse_function_call(self, name: str) -> FunctionCall:
        self._expect_punct("lparen", "(")
        distinct = self._match_kw("DISTINCT") is not None
        args: list = []
        if not self._cur.is_punct("rparen"):
            if self._cur.is_op("*"):
                self._advance()
                args.append(Star())
            else:
                args.append(self._parse_expr())
                while self._match_punct("comma"):
                    args.append(self._parse_expr())
        self._expect_punct("rparen", ")")
        return FunctionCall(name=name.upper(), args=tuple(args), distinct=distinct)

    def _parse_ident(self, what: str) -> str:
        tok = self._cur
        if tok.kind == "ident":
            self._advance()
            return tok.value
        if tok.kind == "keyword" and tok.keyword not in RESERVED:
            self._advance()
            return tok.value
        raise self._error(f"期望{what}")

    def _expect_int(self) -> int:
        tok = self._cur
        if tok.kind == "int":
            self._advance()
            return int(tok.value)
        raise self._error("期望一个非负整数")

    def _parse_limit_value(self, clause: str, *, allow_all: bool = False):
        """``LIMIT n`` / ``LIMIT $1``：参数在绑定阶段解析成整数。

        ``LIMIT ALL`` 只在 allow_all=True 时接受，返回 None 表示不限制行数。
        """
        if allow_all and self._match_kw("ALL"):
            return None
        tok = self._cur
        if tok.kind == "int":
            self._advance()
            return int(tok.value)
        if tok.kind == "param":
            self._advance()
            index = int(tok.value)
            if index < 1:
                raise self._error("参数占位符从 $1 开始，$0 不合法")
            return ParamRef(index)
        raise self._error(f"{clause} 期望一个非负整数或参数占位符 $n")


def parse_sql(sql: str) -> list[Statement]:
    return Parser(tokenize(sql)).parse_statements()


def parse_statement(sql: str) -> Statement:
    return Parser(tokenize(sql)).parse_one()


__all__ = ["Parser", "parse_sql", "parse_statement"]
