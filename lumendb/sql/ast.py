"""SQL 语法树。

只定义本引擎真正实现的东西：AST 里出现的每个节点都必然有对应的执行路径，
没有"语法上接受但执行不了"的节点。不支持的语法在 parser 阶段就报
``NotSupportedError``。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Union

from ..datatypes import TypeSpec


# ---------------------------------------------------------------------------
# 表达式
# ---------------------------------------------------------------------------


class Expr:
    """所有表达式节点的基类（仅用于类型标注）。"""

    __slots__ = ()


@dataclass(frozen=True)
class Literal(Expr):
    value: Any

    def render(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, str):
            return "'" + self.value.replace("'", "''") + "'"
        if isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        return repr(self.value) if isinstance(self.value, float) else str(self.value)


@dataclass(frozen=True)
class DefaultValue(Expr):
    """``SET col = DEFAULT``：执行时取目录里的列默认值，仅用于 DML 赋值。"""

    def render(self) -> str:
        return "DEFAULT"


@dataclass(frozen=True)
class ParamRef(Expr):
    """参数占位符 ``$n``（n 从 1 开始）。

    绑定阶段会被折叠成 ``Literal``：本引擎不做计划缓存，所以"参数"只存在于
    语法树里，执行计划里看到的就是一个常量，不需要参数数组的运行时查找。
    """

    index: int

    def render(self) -> str:
        return f"${self.index}"


@dataclass(frozen=True)
class ColumnRef(Expr):
    table: Optional[str]
    name: str

    def render(self) -> str:
        return f"{self.table}.{self.name}" if self.table else self.name


@dataclass(frozen=True)
class Star(Expr):
    table: Optional[str] = None

    def render(self) -> str:
        return f"{self.table}.*" if self.table else "*"


@dataclass(frozen=True)
class UnaryOp(Expr):
    op: str  # "-" 或 "NOT"
    operand: Expr

    def render(self) -> str:
        if self.op == "NOT":
            return f"NOT {self.operand.render()}"
        return f"-{self.operand.render()}"


@dataclass(frozen=True)
class BinaryOp(Expr):
    op: str
    left: Expr
    right: Expr

    def render(self) -> str:
        return f"({self.left.render()} {self.op} {self.right.render()})"


@dataclass(frozen=True)
class IsNull(Expr):
    operand: Expr
    negated: bool = False

    def render(self) -> str:
        return f"{self.operand.render()} IS {'NOT ' if self.negated else ''}NULL"


@dataclass(frozen=True)
class IsDistinct(Expr):
    left: Expr
    right: Expr
    negated: bool = False

    def render(self) -> str:
        return (
            f"({self.left.render()} IS {'NOT ' if self.negated else ''}"
            f"DISTINCT FROM {self.right.render()})"
        )


@dataclass(frozen=True)
class InList(Expr):
    operand: Expr
    items: tuple[Expr, ...]
    negated: bool = False

    def render(self) -> str:
        inner = ", ".join(i.render() for i in self.items)
        return f"{self.operand.render()} {'NOT ' if self.negated else ''}IN ({inner})"


@dataclass(frozen=True)
class InSubquery(Expr):
    operand: Expr
    query: "Select"
    negated: bool = False

    def render(self) -> str:
        return f"{self.operand.render()} {'NOT ' if self.negated else ''}IN (<子查询>)"


@dataclass(frozen=True)
class Between(Expr):
    operand: Expr
    low: Expr
    high: Expr
    negated: bool = False

    def render(self) -> str:
        neg = "NOT " if self.negated else ""
        return f"{self.operand.render()} {neg}BETWEEN {self.low.render()} AND {self.high.render()}"


@dataclass(frozen=True)
class Like(Expr):
    operand: Expr
    pattern: Expr
    negated: bool = False
    escape: Optional[Expr] = None

    def render(self) -> str:
        text = f"{self.operand.render()} {'NOT ' if self.negated else ''}LIKE {self.pattern.render()}"
        if self.escape is not None:
            text += f" ESCAPE {self.escape.render()}"
        return text


@dataclass(frozen=True)
class FunctionCall(Expr):
    name: str
    args: tuple[Expr, ...]
    distinct: bool = False

    def render(self) -> str:
        inner = ", ".join(a.render() for a in self.args)
        d = "DISTINCT " if self.distinct else ""
        return f"{self.name.upper()}({d}{inner})"


@dataclass(frozen=True)
class Cast(Expr):
    operand: Expr
    target: TypeSpec

    def render(self) -> str:
        return f"CAST({self.operand.render()} AS {self.target.sql})"


@dataclass(frozen=True)
class CaseWhen(Expr):
    """``CASE [operand] WHEN cond THEN value ... [ELSE value] END``。

    ``operand`` 为空时每个 ``WHEN`` 都是布尔条件（搜索式 CASE）；
    不为空时按 ``operand = cond`` 比较（简单式 CASE）。
    """

    operand: Optional[Expr]
    branches: tuple[tuple[Expr, Expr], ...]
    otherwise: Optional[Expr]

    def render(self) -> str:
        parts = ["CASE"]
        if self.operand is not None:
            parts.append(self.operand.render())
        for cond, value in self.branches:
            parts.append(f"WHEN {cond.render()} THEN {value.render()}")
        if self.otherwise is not None:
            parts.append(f"ELSE {self.otherwise.render()}")
        parts.append("END")
        return " ".join(parts)


@dataclass(frozen=True)
class ScalarSubquery(Expr):
    query: "Select"

    def render(self) -> str:
        return "(<标量子查询>)"


@dataclass(frozen=True)
class Exists(Expr):
    query: "Select"
    negated: bool = False

    def render(self) -> str:
        return f"{'NOT ' if self.negated else ''}EXISTS (<子查询>)"


ExprNode = Union[
    Literal,
    ColumnRef,
    Star,
    UnaryOp,
    BinaryOp,
    IsNull,
    InList,
    InSubquery,
    Between,
    Like,
    FunctionCall,
    Cast,
    ScalarSubquery,
    Exists,
]


def render_expr(expr: Expr) -> str:
    """把表达式还原成 SQL 文本（EXPLAIN 与错误信息用）。"""
    renderer = getattr(expr, "render", None)
    if renderer is None:
        return repr(expr)
    return renderer()


# ---------------------------------------------------------------------------
# 查询结构
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderItem:
    expr: Expr
    descending: bool = False
    #: None 表示按 PostgreSQL 默认：ASC 时 NULL 最后，DESC 时 NULL 最前。
    nulls_first: Optional[bool] = None


@dataclass(frozen=True)
class SelectItem:
    expr: Expr
    alias: Optional[str] = None


@dataclass
class Join:
    kind: str  # "INNER" / "LEFT"
    table: "TableRef"
    on: Optional[Expr] = None


@dataclass
class TableRef:
    name: str
    alias: Optional[str] = None
    joins: list[Join] = field(default_factory=list)

    @property
    def effective_name(self) -> str:
        return self.alias or self.name


@dataclass
class Select:
    items: list[SelectItem]
    from_: list[TableRef] = field(default_factory=list)
    where: Optional[Expr] = None
    group_by: list[Expr] = field(default_factory=list)
    having: Optional[Expr] = None
    order_by: list[OrderItem] = field(default_factory=list)
    limit: Optional[Union[int, ParamRef]] = None
    offset: Optional[Union[int, ParamRef]] = None
    distinct: bool = False
    with_ties: bool = False


@dataclass
class CompoundSelect:
    """集合运算表达式；``ops`` 与 ``parts[1:]`` 一一对应。

    ``order_by`` / ``limit`` 只出现在最外层。``parts`` 允许嵌套
    ``CompoundSelect``，因此解析器可以按 SQL 优先级把 ``INTERSECT`` 收紧。
    """

    parts: list[Union[Select, "CompoundSelect"]]
    all_flags: list[bool]
    ops: list[str] = field(default_factory=list)
    order_by: list[OrderItem] = field(default_factory=list)
    limit: Optional[Union[int, ParamRef]] = None
    offset: Optional[Union[int, ParamRef]] = None
    with_ties: bool = False


# ---------------------------------------------------------------------------
# 语句
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckSpec:
    """一条 ``CHECK`` 约束：AST 用于当前语句，token 切片用于目录持久化。"""

    expr: Expr
    tokens: tuple[tuple[str, str], ...] = ()
    name: Optional[str] = None


@dataclass(frozen=True)
class ForeignKeySpec:
    """REFERENCES / FOREIGN KEY 的解析结果；ref_columns 为空时取被引用表主键。"""

    columns: list[str]
    ref_table: str
    ref_columns: list[str] = field(default_factory=list)
    name: Optional[str] = None
    on_delete: str = "RESTRICT"
    on_update: str = "RESTRICT"
    #: SET NULL / SET DEFAULT 的列级动作；空表示作用于全部外键列。
    on_delete_columns: list[str] = field(default_factory=list)
    on_update_columns: list[str] = field(default_factory=list)
    match_type: str = "SIMPLE"
    deferrable: bool = False
    initially_deferred: bool = False
    #: ALTER TABLE ADD CONSTRAINT ... NOT VALID：只约束新行，存量行暂不校验。
    not_valid: bool = False


@dataclass(frozen=True)
class ColumnDef:
    name: str
    type: TypeSpec
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default: Optional[Expr] = None
    checks: tuple[CheckSpec, ...] = ()
    references: Optional[ForeignKeySpec] = None
    primary_deferrable: bool = False
    primary_initially_deferred: bool = False
    unique_deferrable: bool = False
    unique_initially_deferred: bool = False


@dataclass(frozen=True)
class TableConstraint:
    """表级约束：复合 PRIMARY KEY、UNIQUE、CHECK 或 FOREIGN KEY。"""

    kind: str  # "PRIMARY KEY" / "UNIQUE" / "CHECK" / "FOREIGN KEY"
    columns: list[str]
    name: Optional[str] = None
    #: PRIMARY KEY / UNIQUE 也可声明延迟检查。
    deferrable: bool = False
    initially_deferred: bool = False
    check: Optional[CheckSpec] = None
    ref_table: Optional[str] = None
    ref_columns: list[str] = field(default_factory=list)
    on_delete: str = "RESTRICT"
    on_update: str = "RESTRICT"
    #: SET NULL / SET DEFAULT 的列级动作；空表示作用于全部外键列。
    on_delete_columns: list[str] = field(default_factory=list)
    on_update_columns: list[str] = field(default_factory=list)
    match_type: str = "SIMPLE"
    deferrable: bool = False
    initially_deferred: bool = False
    #: ALTER TABLE ADD CONSTRAINT ... NOT VALID：只约束新行，存量行暂不校验。
    not_valid: bool = False


@dataclass
class Statement:
    pass


@dataclass
class CreateTable(Statement):
    name: str
    columns: list[ColumnDef]
    if_not_exists: bool = False
    constraints: list[TableConstraint] = field(default_factory=list)


@dataclass
class CreateView(Statement):
    """只读视图。``tokens`` 是 ``AS`` 之后查询的词法切片，用于持久化。"""

    name: str
    query: Any
    tokens: list[tuple[str, str]]
    columns: list[str] = field(default_factory=list)
    or_replace: bool = False


@dataclass
class DropView(Statement):
    name: str
    if_exists: bool = False
    cascade: bool = False


@dataclass
class DropTable(Statement):
    names: list[str]
    if_exists: bool = False
    cascade: bool = False


@dataclass
class CreateTableAs(Statement):
    name: str
    query: Union[Select, CompoundSelect]
    if_not_exists: bool = False


@dataclass
class CreateIndex(Statement):
    name: str
    table: str
    columns: list[str]
    unique: bool = False
    if_not_exists: bool = False


@dataclass
class AlterTableAddColumn(Statement):
    table: str
    column: ColumnDef
    if_not_exists: bool = False


@dataclass
class AlterTableAddConstraint(Statement):
    table: str
    constraint: TableConstraint


@dataclass
class AlterTableDropConstraint(Statement):
    table: str
    name: str
    if_exists: bool = False
    #: DROP CONSTRAINT ... CASCADE：同时删除依赖该唯一键的外键。
    cascade: bool = False


@dataclass
class AlterTableAlterConstraint(Statement):
    """``ALTER TABLE ... ALTER CONSTRAINT name``：修改外键的延迟属性。"""

    table: str
    name: str
    #: None 表示保持原值；False 表示 NOT DEFERRABLE。
    deferrable: Optional[bool] = None
    #: None 表示保持原值；False 表示 INITIALLY IMMEDIATE。
    initially_deferred: Optional[bool] = None


@dataclass
class AlterTableValidateConstraint(Statement):
    """``ALTER TABLE ... VALIDATE CONSTRAINT name``：校验 NOT VALID 的存量行。"""

    table: str
    name: str


@dataclass
class AlterTableRenameTable(Statement):
    table: str
    new_name: str


@dataclass
class AlterTableRenameColumn(Statement):
    table: str
    column: str
    new_name: str


@dataclass
class AlterTableAlterDefault(Statement):
    table: str
    column: str
    #: None 表示 DROP DEFAULT；否则为 SET DEFAULT 的字面表达式
    default: Optional[Expr] = None


@dataclass
class AlterTableAlterNullability(Statement):
    table: str
    column: str
    #: False 表示 SET NOT NULL，True 表示 DROP NOT NULL
    nullable: bool


@dataclass
class AlterTableAlterType(Statement):
    table: str
    column: str
    type: TypeSpec
    #: 可选的 USING 表达式；为 None 时按目标类型做隐式 coerce。
    using: Optional[Expr] = None
    #: ALTER COLUMN ... TYPE ... CASCADE：同时删除依赖该列的外键。
    cascade: bool = False


@dataclass
class AlterTableDropColumn(Statement):
    table: str
    column: str
    if_exists: bool = False
    #: DROP COLUMN ... CASCADE：同时删除依赖该列的外键、索引与 CHECK。
    cascade: bool = False


@dataclass
class AlterTableRenameConstraint(Statement):
    table: str
    old_name: str
    new_name: str


@dataclass
class DropIndex(Statement):
    name: str
    if_exists: bool = False
    #: DROP INDEX ... CASCADE：同时删除依赖该唯一索引的外键。
    cascade: bool = False


@dataclass
class Insert(Statement):
    table: str
    columns: Optional[list[str]]
    rows: list[list[Expr]]
    #: ``INSERT ... SELECT`` 的来源；为 None 时使用 ``rows``
    query: Optional[Union[Select, CompoundSelect]] = None
    #: ``INSERT ... DEFAULT VALUES``：按每列默认值写入一行
    default_values: bool = False
    #: ``WITH ... INSERT`` 的作用域
    ctes: list["Cte"] = field(default_factory=list)
    #: ``ON CONFLICT DO NOTHING``：唯一键冲突时跳过该行而不是报错
    on_conflict_nothing: bool = False
    #: ``ON CONFLICT (col, ...)`` 的冲突目标；为 None 时匹配任意唯一索引
    conflict_columns: Optional[list[str]] = None
    #: ``ON CONFLICT ON CONSTRAINT name`` 的约束名；与 conflict_columns 二选一
    conflict_constraint: Optional[str] = None
    #: ``ON CONFLICT DO UPDATE SET ...`` 的赋值；为 None 时不是 DO UPDATE
    on_conflict_update: Optional[list[tuple[str, Expr]]] = None
    #: ``ON CONFLICT DO UPDATE ... WHERE condition``；在旧行上求值
    on_conflict_where: Optional[Expr] = None
    #: ``RETURNING`` 表达式及其可选别名；为 None 时不返回行
    returning: Optional[list["SelectItem"]] = None


@dataclass
class Update(Statement):
    table: str
    assignments: list[tuple[str, Expr]]
    where: Optional[Expr] = None
    returning: Optional[list["SelectItem"]] = None
    ctes: list["Cte"] = field(default_factory=list)
    #: PostgreSQL 的 UPDATE ... FROM；目前只支持单个源表，不支持 JOIN/别名。
    from_table: Optional[str] = None


@dataclass
class Delete(Statement):
    table: str
    where: Optional[Expr] = None
    returning: Optional[list["SelectItem"]] = None
    ctes: list["Cte"] = field(default_factory=list)
    #: PostgreSQL 的 DELETE ... USING；目前只支持单个源表，不支持 JOIN/别名。
    using_table: Optional[str] = None


@dataclass
class Query(Statement):
    select: Select
    ctes: list["Cte"] = field(default_factory=list)
    recursive: bool = False


@dataclass
class Cte:
    name: str
    query: Union[Select, CompoundSelect]
    #: 可选显式输出列名；空列表表示不覆盖查询自身的列名。
    columns: list[str] = field(default_factory=list)


@dataclass
class Begin(Statement):
    read_only: bool = False


@dataclass
class Commit(Statement):
    pass


@dataclass
class Rollback(Statement):
    pass


@dataclass
class Savepoint(Statement):
    name: str


@dataclass
class ReleaseSavepoint(Statement):
    name: str


@dataclass
class RollbackToSavepoint(Statement):
    name: str


@dataclass
class SetConstraints(Statement):
    """``SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }``。"""

    names: tuple[str, ...] = ()
    all_constraints: bool = False
    deferred: bool = True


@dataclass
class Explain(Statement):
    statement: Statement
    analyze: bool = False


@dataclass
class ShowTables(Statement):
    pass


@dataclass
class ShowViews(Statement):
    pass


@dataclass
class Checkpoint(Statement):
    pass


@dataclass
class Reindex(Statement):
    """``REINDEX table``：按堆表当前内容重建该表所有索引。"""

    table: str


@dataclass
class Vacuum(Statement):
    """``VACUUM [table]``：回收死版本并重排堆页；不带表名则处理所有表。"""

    table: Optional[str] = None


@dataclass
class Truncate(Statement):
    """``TRUNCATE [TABLE] name [, ...] [CASCADE | RESTRICT]``：清空整表并重建索引。"""

    table: str
    cascade: bool = False
    tables: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.tables:
            self.tables = [self.table]


__all__ = [
    "Expr",
    "Literal",
    "ColumnRef",
    "Star",
    "UnaryOp",
    "BinaryOp",
    "IsNull",
    "InList",
    "InSubquery",
    "Between",
    "Like",
    "FunctionCall",
    "Cast",
    "ScalarSubquery",
    "Exists",
    "OrderItem",
    "SelectItem",
    "Join",
    "TableRef",
    "Select",
    "CompoundSelect",
    "CheckSpec",
    "ForeignKeySpec",
    "ColumnDef",
    "Statement",
    "CreateTable",
    "CreateTableAs",
    "DropTable",
    "CreateIndex",
    "AlterTableAddColumn",
    "AlterTableAddConstraint",
    "AlterTableValidateConstraint",
    "AlterTableDropConstraint",
    "AlterTableAlterType",
    "AlterTableDropColumn",
    "AlterTableRenameConstraint",
    "DropIndex",
    "Insert",
    "Update",
    "Delete",
    "Query",
    "Cte",
    "Begin",
    "Commit",
    "Rollback",
    "Savepoint",
    "ReleaseSavepoint",
    "RollbackToSavepoint",
    "Explain",
    "ShowTables",
    "Checkpoint",
    "Reindex",
    "Vacuum",
    "Truncate",
    "render_expr",
]
