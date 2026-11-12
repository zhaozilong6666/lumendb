"""执行计划节点。

采用最简单的火山模型节点定义：每个节点声明自己的"输出 schema"（列名 +
类型）+ 子节点，执行器按行迭代。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from ..datatypes import TypeSpec


@dataclass
class EvalContext:
    """表达式求值上下文：一行数据 + 该行所属分组的聚合结果。"""

    row: tuple
    aggs: tuple = ()

    def with_row(self, row: tuple) -> "EvalContext":
        return EvalContext(row, self.aggs)


EvalFn = Callable[[EvalContext], Any]


@dataclass(frozen=True)
class OutputColumn:
    name: str
    type: TypeSpec


@dataclass
class PlanNode:
    columns: list[OutputColumn] = field(default_factory=list)

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

    def children(self):
        return [c for c in (self.left, self.right) if c is not None]

    def describe(self) -> str:
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

    def children(self):
        return [self.child] if self.child else []

    def describe(self) -> str:
        return f"Limit {self.limit} offset {self.offset}"


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
