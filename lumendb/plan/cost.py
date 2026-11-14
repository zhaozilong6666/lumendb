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

from dataclasses import dataclass

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
