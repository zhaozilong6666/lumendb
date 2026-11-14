"""执行计划节点。

采用最简单的火山模型节点定义：每个节点声明自己的"输出 schema"（列名 +
类型）+ 子节点，执行器按行迭代。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from contextvars import ContextVar
from typing import Any, Callable, Optional

from ..datatypes import TypeSpec
from .cost import CostEstimate


#: 当前正在执行的相关子查询的外层行。``_run_subquery`` 在求值一个子查询
#: 计划时设置它，计划里所有 ``EvalContext(row)`` 会通过 ``__post_init__``
#: 自动挂到同一个外层行上，从而让子查询里的外层列引用可以沿 parent 链取值。
_CURRENT_OUTER: ContextVar[Optional["EvalContext"]] = ContextVar(
    "lumendb_current_outer", default=None
)


@dataclass
class EvalContext:
    """表达式求值上下文：一行数据 + 该行所属分组的聚合结果 + 外层行。"""

    row: tuple
    aggs: tuple = ()
    parent: Optional["EvalContext"] = None

    def __post_init__(self) -> None:
        if self.parent is None:
            self.parent = _CURRENT_OUTER.get()

    def with_row(self, row: tuple) -> "EvalContext":
        return EvalContext(row, self.aggs, self.parent)


EvalFn = Callable[[EvalContext], Any]


@dataclass(frozen=True)
class OutputColumn:
    name: str
    type: TypeSpec


@dataclass
class PlanNode:
    columns: list[OutputColumn] = field(default_factory=list)
    #: 代价模型给出的估算；EXPLAIN 用它标注 rows≈/cpu≈。
    estimated: Optional[CostEstimate] = None

    def children(self) -> list["PlanNode"]:
        return []

    def describe(self) -> str:  # 每个节点覆写
        return type(self).__name__


@dataclass
class Scan(PlanNode):
    table: str = ""
    alias: str = ""
    #: 堆表上的过滤谓词（谓词下推后可能为空）
    filter: Optional[EvalFn] = None
    #: 是否走索引（EXPLAIN 用）
    index_name: Optional[str] = None
    index_columns: tuple = ()

    def describe(self) -> str:
        text = f"SeqScan {self.table}"
        if self.alias and self.alias != self.table:
            text += f" AS {self.alias}"
        if self.index_name:
            text += f" [index {self.index_name}({', '.join(self.index_columns)})]"
        return text


@dataclass
class IndexLookup(PlanNode):
    """等值条件命中索引时走这条路径，而不是全表扫描。"""

    table: str = ""
    alias: str = ""
    index_name: str = ""
    index_columns: tuple = ()
    key: tuple = ()

    def describe(self) -> str:
        values = ", ".join(repr(v) for v in self.key)
        return f"IndexScan {self.table} [{self.index_name}({', '.join(self.index_columns)}) = ({values})]"


@dataclass
class IndexRange(PlanNode):
    """范围条件（``>`` / ``>=`` / ``<`` / ``<=`` / ``BETWEEN``）命中索引。"""

    table: str = ""
    alias: str = ""
    index_name: str = ""
    index_columns: tuple = ()
    low: Optional[tuple] = None
    high: Optional[tuple] = None
    low_inclusive: bool = True
    high_inclusive: bool = True

    def describe(self) -> str:
        lower = ", ".join(repr(v) for v in self.low) if self.low else "-inf"
        upper = ", ".join(repr(v) for v in self.high) if self.high else "+inf"
        left = "[" if self.low_inclusive else "("
        right = "]" if self.high_inclusive else ")"
        return (
            f"IndexRangeScan {self.table} "
            f"[{self.index_name}({', '.join(self.index_columns)})] "
            f"{left}{lower}, {upper}{right}"
        )


@dataclass
class CteScan(PlanNode):
    """``WITH ...`` 定义的命名子查询；child 是已经绑定好的计划。"""

    child: Optional[PlanNode] = None
    #: 递归 CTE 的工作表引用；执行时从执行器的递归上下文取当前轮的行。
    recursive: bool = False
    name: str = ""

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        return "CteScan"


@dataclass
class RecursiveCte(PlanNode):
    """递归 CTE：先产出 seed，再反复执行 recursive 直到没有新行。"""

    name: str = ""
    seed: Optional[PlanNode] = None
    recursive: Optional[PlanNode] = None
    union_all: bool = False

    def children(self):
        return [c for c in (self.seed, self.recursive) if c is not None]

    def describe(self) -> str:
        return f"RecursiveCte {self.name}"


@dataclass
class SetOp(PlanNode):
    left: Optional[PlanNode] = None
    right: Optional[PlanNode] = None
    op: str = "UNION"
    all: bool = False

    def children(self):
        return [c for c in (self.left, self.right) if c is not None]

    def describe(self) -> str:
        if self.op == "UNION":
            return "UnionAll" if self.all else "Union"
        return self.op.title()


@dataclass
class Values(PlanNode):
    rows: list[tuple] = field(default_factory=list)

    def describe(self) -> str:
        return f"Values ({len(self.rows)} rows)"


@dataclass
class Filter(PlanNode):
    child: Optional[PlanNode] = None
    predicate: Optional[EvalFn] = None

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        return "Filter"


@dataclass
class Join(PlanNode):
    left: Optional[PlanNode] = None
    right: Optional[PlanNode] = None
    kind: str = "INNER"
    on: Optional[EvalFn] = None
    #: (left 列下标, right 列下标) 的等值键；非空时执行器走哈希连接
    hash_keys: tuple[tuple[int, int], ...] = ()
    #: EXPLAIN 用的可读键名，如 ``a.id = b.id``
    hash_labels: tuple[str, ...] = ()
    #: 哈希连接是否构建左表；仅对 INNER 生效，默认构建右表
    build_left: bool = False

    def children(self):
        return [c for c in (self.left, self.right) if c is not None]

    def describe(self) -> str:
        if self.hash_keys and self.kind in ("INNER", "LEFT"):
            keys = ", ".join(self.hash_labels) or "?"
            build = "left" if self.build_left else "right"
            return f"HashJoin ({self.kind}) on {keys} [build={build}]"
        return f"{self.kind} Join"


@dataclass
class Aggregate(PlanNode):
    child: Optional[PlanNode] = None
    group_exprs: list[EvalFn] = field(default_factory=list)
    group_labels: list[str] = field(default_factory=list)
    aggs: list[Any] = field(default_factory=list)  # list[AggSpec]

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        kinds = ", ".join(f"{a.kind}" for a in self.aggs)
        return f"Aggregate (group {len(self.group_exprs)}, {kinds or 'no agg'})"


@dataclass
class Project(PlanNode):
    child: Optional[PlanNode] = None
    exprs: list[EvalFn] = field(default_factory=list)

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        return f"Project ({', '.join(c.name for c in self.columns)})"


@dataclass
class Sort(PlanNode):
    child: Optional[PlanNode] = None
    keys: list[tuple[EvalFn, bool]] = field(default_factory=list)  # (fn, descending)

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        return f"Sort ({len(self.keys)} keys)"


@dataclass
class Limit(PlanNode):
    child: Optional[PlanNode] = None
    limit: Optional[int] = None
    offset: int = 0
    with_ties: bool = False
    #: ORDER BY 键求值函数，只用于 WITH TIES 判断并列；空表示所有行互为并列
    tie_keys: list[EvalFn] = field(default_factory=list)

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        suffix = " with ties" if self.with_ties else ""
        return f"Limit {self.limit} offset {self.offset}{suffix}"


@dataclass
class Distinct(PlanNode):
    child: Optional[PlanNode] = None
    #: 参与去重的列数（Project 可能附加了 ORDER BY 用的隐藏列）
    limit_columns: int = 0

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        return "Distinct"


__all__ = [
    "EvalContext",
    "EvalFn",
    "OutputColumn",
    "PlanNode",
    "Scan",
    "Values",
    "Filter",
    "Join",
    "Aggregate",
    "Project",
    "Sort",
    "Limit",
    "Distinct",
]
