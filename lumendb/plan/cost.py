"""基数估算与代价模型。

这一层刻意不碰执行器，只回答三个问题：

* 一个节点大概输出多少行；
* 一次连接大概花多少 CPU；
* 哈希连接应该把哪一侧作为 build side。

估算值用于后续的连接顺序枚举和 EXPLAIN。当前执行器仍按 SQL 书写顺序构造
连接，因此这里的结果不会改变查询语义。所有函数都对输入做下限保护，避免
出现 0 行估算导致代价比较失真。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

#: 没有任何统计时的默认基数。派生表、未分析的 CTE 先用这个值。
DEFAULT_ROWS = 100.0
MIN_ROWS = 1.0

#: 单列等值谓词的选择率。多个等值键会相乘，并限制在 [MIN_SELECTIVITY, 1]。
EQ_SELECTIVITY = 0.1
MIN_SELECTIVITY = 0.01
RANGE_SELECTIVITY = 0.33
OTHER_SELECTIVITY = 0.5


@dataclass(frozen=True)
class CostEstimate:
    """一个计划节点的估算结果。"""

    rows: float
    cpu: float
    #: ``stats`` / ``default`` / ``derived``，用于 EXPLAIN 标注置信度。
    confidence: str = "default"
    #: ``nested_loop`` / ``hash`` / ``scan`` / ``filter``，用于可读输出。
    algorithm: str = "scan"

    @property
    def total(self) -> float:
        """行数 + CPU 的简单加权，供连接顺序枚举比较。"""
        return self.rows + self.cpu


def _positive(value: float) -> float:
    return max(float(value), MIN_ROWS)


def scan_estimate(row_count_hint: int, *, indexed: bool = False) -> CostEstimate:
    """基表扫描估算。

    ``row_count_hint`` 由堆表在插入/删除时维护，0 是合法的空表，因此统一
    下限为 1 行；索引扫描的 CPU 系数更低。
    """
    rows = _positive(row_count_hint)
    cpu = rows * (0.2 if indexed else 1.0)
    return CostEstimate(rows=rows, cpu=cpu, confidence="stats", algorithm="scan")


def unknown_estimate(*, confidence: str = "default") -> CostEstimate:
    """派生表/未统计数据源的保守估算。"""
    return CostEstimate(rows=DEFAULT_ROWS, cpu=DEFAULT_ROWS, confidence=confidence)


def filter_estimate(
    child: CostEstimate, selectivity: float = OTHER_SELECTIVITY
) -> CostEstimate:
    """过滤估算；选择率限制在 ``[MIN_SELECTIVITY, 1]``。"""
    selectivity = min(max(float(selectivity), MIN_SELECTIVITY), 1.0)
    rows = _positive(child.rows * selectivity)
    return CostEstimate(
        rows=rows,
        cpu=child.cpu + rows,
        confidence=child.confidence,
        algorithm="filter",
    )


def equality_selectivity(keys: int) -> float:
    """多个等值键的联合选择率，下限 ``MIN_SELECTIVITY``。"""
    if keys <= 0:
        return 1.0
    return max(MIN_SELECTIVITY, EQ_SELECTIVITY**keys)


def project_estimate(child: CostEstimate, *, width: int = 1) -> CostEstimate:
    """投影/表达式求值估算；行数不变，CPU 按输出宽度增加。"""
    return CostEstimate(
        rows=child.rows,
        cpu=child.cpu + child.rows * max(1, width) * 0.1,
        confidence=child.confidence,
        algorithm="project",
    )


def sort_estimate(child: CostEstimate, *, keys: int = 1) -> CostEstimate:
    """排序估算；CPU 按 ``n log n`` 增长，行数不变。"""
    rows = child.rows
    # rows 为 0 时 log 无意义；用 2 作为下限只影响空输入的 CPU 估算。
    comparisons = rows * math.log2(max(rows, 2.0)) * max(1, keys)
    return CostEstimate(
        rows=rows,
        cpu=child.cpu + comparisons,
        confidence=child.confidence,
        algorithm="sort",
    )


def limit_estimate(
    child: CostEstimate, *, limit: Optional[int], offset: int = 0
) -> CostEstimate:
    """LIMIT/OFFSET 估算；输出行数允许为 0，但不会为负。"""
    available = max(0.0, child.rows - max(0, offset))
    rows = available if limit is None else min(available, max(0, limit))
    return CostEstimate(
        rows=max(0.0, rows),
        cpu=child.cpu + rows,
        confidence=child.confidence,
        algorithm="limit",
    )


def aggregate_estimate(child: CostEstimate, *, groups: int = 1) -> CostEstimate:
    """聚合估算。

    无分组时固定输出 1 行；有分组时没有列统计，使用 ``sqrt(输入行数)``
    作为分组数的保守估计。CPU 至少覆盖扫描输入的代价。
    """
    rows = 1.0 if groups <= 1 else max(1.0, math.sqrt(child.rows))
    return CostEstimate(
        rows=rows,
        cpu=child.cpu + child.rows,
        confidence=child.confidence if groups <= 1 else "derived",
        algorithm="aggregate",
    )


def distinct_estimate(child: CostEstimate) -> CostEstimate:
    """去重估算；没有列统计时按 90% 输入行数保守估计。"""
    return CostEstimate(
        rows=max(1.0, child.rows * 0.9),
        cpu=child.cpu + child.rows,
        confidence=child.confidence,
        algorithm="distinct",
    )


def setop_estimate(
    left: CostEstimate, right: CostEstimate, *, distinct: bool = False
) -> CostEstimate:
    """集合运算估算；``UNION ALL`` 直接相加，去重形态再乘 0.9。"""
    rows = left.rows + right.rows
    if distinct:
        rows *= 0.9
    return CostEstimate(
        rows=max(1.0, rows),
        cpu=left.cpu + right.cpu + rows,
        confidence=(
            left.confidence if left.confidence == right.confidence else "derived"
        ),
        algorithm="setop",
    )


def join_estimate(
    left: CostEstimate,
    right: CostEstimate,
    *,
    equi_keys: int = 0,
) -> CostEstimate:
    """连接估算。

    有等值键时优先哈希连接：代价是两侧行数之和；没有等值键时只能嵌套循环，
    代价是两侧行数之积。输出行数按等值键选择率计算，并下限为 1。
    """
    if equi_keys > 0:
        rows = _positive(left.rows * right.rows * equality_selectivity(equi_keys))
        cpu = left.cpu + right.cpu + left.rows + right.rows
        algorithm = "hash"
    else:
        rows = _positive(left.rows * right.rows)
        cpu = left.cpu + right.cpu + left.rows * right.rows
        algorithm = "nested_loop"
    confidence = (
        left.confidence
        if left.confidence == right.confidence
        else "derived"
    )
    return CostEstimate(
        rows=rows, cpu=cpu, confidence=confidence, algorithm=algorithm
    )


def choose_build_side(left: CostEstimate, right: CostEstimate) -> str:
    """哈希连接选择较小的 build side；相等时保持右侧，保持现有习惯。"""
    return "left" if left.rows < right.rows else "right"
