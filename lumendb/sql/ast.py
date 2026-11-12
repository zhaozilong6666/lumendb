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

    def render(self) -> str:
        return f"{self.operand.render()} {'NOT ' if self.negated else ''}LIKE {self.pattern.render()}"


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


# ---------------------------------------------------------------------------
# 语句
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnDef:
    name: str
    type: TypeSpec
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default: Optional[Expr] = None


@dataclass
class Statement:
    pass


@dataclass
class CreateTable(Statement):
    name: str
    columns: list[ColumnDef]
    if_not_exists: bool = False


@dataclass
class DropTable(Statement):
    names: list[str]
    if_exists: bool = False


@dataclass
class CreateIndex(Statement):
    name: str
    table: str
    columns: list[str]
    unique: bool = False


@dataclass
class DropIndex(Statement):
    name: str
    if_exists: bool = False


@dataclass
class Insert(Statement):
    table: str
    columns: Optional[list[str]]
    rows: list[list[Expr]]
    #: ``ON CONFLICT DO NOTHING``：唯一键冲突时跳过该行而不是报错
    on_conflict_nothing: bool = False


@dataclass
class Update(Statement):
    table: str
    assignments: list[tuple[str, Expr]]
    where: Optional[Expr] = None


@dataclass
class Delete(Statement):
    table: str
    where: Optional[Expr] = None


@dataclass
class Query(Statement):
    select: Select


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
class Explain(Statement):
    statement: Statement


@dataclass
class ShowTables(Statement):
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
    """``TRUNCATE [TABLE] name``：清空整表并重建索引。"""

    table: str


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
    "ColumnDef",
    "Statement",
    "CreateTable",
    "DropTable",
    "CreateIndex",
    "DropIndex",
    "Insert",
    "Update",
    "Delete",
    "Query",
    "Begin",
    "Commit",
    "Rollback",
    "Explain",
    "ShowTables",
    "Checkpoint",
    "Reindex",
    "Vacuum",
    "Truncate",
    "render_expr",
]
