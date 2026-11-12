"""SQL 解析：递归下降 + 运算符优先级爬升。

原则是"支持的语法必须正确，不支持的语法必须明确报错"。因此这里没有
"宽容解析"分支：遇到 ``ALTER TABLE``、``RIGHT JOIN``、CTE 这类已知不
支持的写法，直接抛 ``NotSupportedError``，而不是悄悄当成别的东西。
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
    Checkpoint,
    ColumnDef,
    ColumnRef,
    Commit,
    CreateIndex,
    CreateTable,
    Delete,
    DropIndex,
    DropTable,
    Exists,
    Explain,
    FunctionCall,
    InList,
    InSubquery,
    Insert,
    IsNull,
    Join,
    Like,
    Literal,
    OrderItem,
    Query,
    Rollback,
    ScalarSubquery,
    Select,
    SelectItem,
    ShowTables,
    Star,
    Statement,
    TableRef,
    UnaryOp,
    Update,
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
            if self._cur.is_keyword("UNION", "EXCEPT", "INTERSECT"):
                raise NotSupportedError(
                    "集合运算（UNION / EXCEPT / INTERSECT）尚未实现",
                    hint="可以拆成多条查询，在客户端合并结果",
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
            return Query(self._parse_select())
        if kw == "CREATE":
            return self._parse_create()
        if kw == "DROP":
            return self._parse_drop()
        if kw == "ALTER":
            raise NotSupportedError(
                "ALTER TABLE 尚未实现",
                detail="当前只支持建表时定义列",
                hint="可以 DROP TABLE 后重建，或用新表名 + INSERT ... SELECT 迁移",
            )
        if kw == "WITH":
            raise NotSupportedError(
                "公共表表达式（CTE）尚未实现",
                detail="包含递归 CTE",
                hint="可以用子查询表达等价逻辑",
            )
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
            return Rollback()
        if kw == "EXPLAIN":
            self._advance()
            inner = self._parse_statement()
            return Explain(inner)
        if kw == "SHOW":
            self._advance()
            self._expect_kw("TABLES")
            return ShowTables()
        if kw == "CHECKPOINT":
            self._advance()
            return Checkpoint()
        raise self._error(
            f"无法识别的语句起始关键字 {tok.value.upper()}",
            hint="支持 SELECT / INSERT / UPDATE / DELETE / CREATE / DROP / BEGIN / COMMIT / "
            "ROLLBACK / EXPLAIN / SHOW TABLES / CHECKPOINT",
        )

    def _parse_create(self) -> Statement:
        self._expect_kw("CREATE")
        unique = self._match_kw("UNIQUE") is not None
        if self._match_kw("TABLE"):
            if unique:
                raise self._error("UNIQUE 只能用于 CREATE INDEX")
            return self._parse_create_table()
        if self._match_kw("INDEX"):
            return self._parse_create_index(unique=unique)
        if unique:
            raise self._error("UNIQUE 只能用于 CREATE INDEX")
        if self._match_kw("VIEW", "TRIGGER", "PROCEDURE"):
            raise NotSupportedError("视图 / 触发器 / 存储过程尚未实现")
        raise self._error("CREATE 之后期望 TABLE 或 INDEX")

    def _parse_create_table(self) -> CreateTable:
        if_not_exists = False
        if self._match_kw("IF"):
            self._expect_kw("NOT")
            self._expect_kw("EXISTS")
            if_not_exists = True
        name = self._parse_ident("表名")
        self._expect_punct("lparen", "(")
        columns: list[ColumnDef] = []
        while True:
            columns.append(self._parse_column_def())
            if not self._match_punct("comma"):
                break
        self._expect_punct("rparen", ")")
        # 表级约束：只接受没有约束时的空体；遇到 PRIMARY KEY (...) 之类明确报不支持
        return CreateTable(name=name, columns=columns, if_not_exists=if_not_exists)

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
        name = self._parse_ident("索引名")
        self._expect_kw("ON")
        table = self._parse_ident("表名")
        self._expect_punct("lparen", "(")
        columns = [self._parse_ident("列名")]
        while self._match_punct("comma"):
            columns.append(self._parse_ident("列名"))
        self._expect_punct("rparen", ")")
        return CreateIndex(name=name, table=table, columns=columns, unique=unique)

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
            return DropTable(names=names, if_exists=if_exists)
        if self._match_kw("INDEX"):
            if_exists = False
            if self._match_kw("IF"):
                self._expect_kw("EXISTS")
                if_exists = True
            return DropIndex(name=self._parse_ident("索引名"), if_exists=if_exists)
        raise self._error("DROP 之后期望 TABLE 或 INDEX")

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
        self._expect_kw("VALUES")
        rows: list[list] = []
        while True:
            self._expect_punct("lparen", "(")
            row = [self._parse_expr()]
            while self._match_punct("comma"):
                row.append(self._parse_expr())
            self._expect_punct("rparen", ")")
            rows.append(row)
            if not self._match_punct("comma"):
                break
        return Insert(table=table, columns=columns, rows=rows)

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
            assignments.append((col, self._parse_expr()))
            if not self._match_punct("comma"):
                break
        where = None
        if self._match_kw("WHERE"):
            where = self._parse_expr()
        return Update(table=table, assignments=assignments, where=where)

    def _parse_delete(self) -> Delete:
        self._expect_kw("DELETE")
        self._expect_kw("FROM")
        table = self._parse_ident("表名")
        where = None
        if self._match_kw("WHERE"):
            where = self._parse_expr()
        return Delete(table=table, where=where)

    # ------------------------------------------------------------------
    # SELECT
    # ------------------------------------------------------------------

    def _parse_select(self) -> Select:
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

        order_by: list[OrderItem] = []
        if self._match_kw("ORDER"):
            self._expect_kw("BY")
            order_by.append(self._parse_order_item())
            while self._match_punct("comma"):
                order_by.append(self._parse_order_item())

        limit = None
        offset = None
        if self._match_kw("LIMIT"):
            limit = self._expect_int()
            if self._match_kw("OFFSET"):
                offset = self._expect_int()

        return Select(
            items=items,
            from_=from_,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset,
            distinct=distinct,
        )

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
        return OrderItem(expr=expr, descending=descending)

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
            if self._match_kw("JOIN"):
                kind = "INNER"
            elif self._match_kw("INNER"):
                self._expect_kw("JOIN")
                kind = "INNER"
            elif self._match_kw("LEFT"):
                self._match_kw("OUTER")
                self._expect_kw("JOIN")
                kind = "LEFT"
            elif self._cur.is_keyword("RIGHT", "FULL"):
                raise NotSupportedError(
                    f"{self._cur.value.upper()} JOIN 尚未实现",
                    hint="可以交换左右表并用 LEFT JOIN，或改写为子查询",
                )
            elif self._cur.is_keyword("CROSS"):
                raise NotSupportedError(
                    "CROSS JOIN 尚未实现",
                    hint="可以用 FROM a, b 表达笛卡尔积",
                )
            if kind is None:
                return ref
            right = TableRef(name=self._parse_ident("表名"), alias=None)
            if self._match_kw("AS"):
                right.alias = self._parse_ident("别名")
            elif self._cur.kind == "ident":
                right.alias = self._advance().value
            on = None
            if self._match_kw("ON"):
                on = self._parse_expr()
            elif kind == "LEFT":
                raise self._error("LEFT JOIN 必须带 ON 条件")
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
                left = Like(left, self._parse_additive(), negated=negated)
                continue

            if tok.is_keyword("NOT"):
                raise self._error("NOT 之后期望 IN / LIKE / BETWEEN")
            return left

    def _parse_additive(self):
        left = self._parse_multiplicative()
        while self._cur.is_op("+", "-"):
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

        if tok.is_keyword("CASE", "COALESCE_NULL"):
            raise NotSupportedError("CASE 表达式尚未实现", hint="可用多个嵌套函数或 WHERE 条件替代")

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


def parse_sql(sql: str) -> list[Statement]:
    return Parser(tokenize(sql)).parse_statements()


def parse_statement(sql: str) -> Statement:
    return Parser(tokenize(sql)).parse_one()


__all__ = ["Parser", "parse_sql", "parse_statement"]
