"""代价模型单元测试。"""

from __future__ import annotations

import pytest

from lumendb.plan.cost import (
    DEFAULT_ROWS,
    MIN_SELECTIVITY,
    aggregate_estimate,
    choose_build_side,
    distinct_estimate,
    equality_selectivity,
    filter_estimate,
    join_estimate,
    limit_estimate,
    project_estimate,
    scan_estimate,
    setop_estimate,
    sort_estimate,
    unknown_estimate,
)


def test_scan_estimate_uses_row_hint_and_clamps_empty_table():
    assert scan_estimate(0).rows == 1
    assert scan_estimate(120).rows == 120
    assert scan_estimate(120, indexed=True).cpu < scan_estimate(120).cpu
    assert scan_estimate(120).confidence == "stats"


def test_unknown_estimate_is_conservative():
    est = unknown_estimate()
    assert est.rows == DEFAULT_ROWS
    assert est.confidence == "default"


def test_filter_estimate_clamps_selectivity():
    child = scan_estimate(100)
    assert filter_estimate(child, 0).rows == 1
    assert filter_estimate(child, 1).rows == 100
    assert filter_estimate(child, 0.5).rows == 50


def test_equality_selectivity_has_floor():
    assert equality_selectivity(0) == 1.0
    assert equality_selectivity(1) == 0.1
    assert equality_selectivity(2) == pytest.approx(0.01)
    assert equality_selectivity(5) == MIN_SELECTIVITY


def test_join_estimate_prefers_hash_when_equi_keys_exist():
    left = scan_estimate(10)
    right = scan_estimate(20)
    est = join_estimate(left, right, equi_keys=1)
    assert est.rows == pytest.approx(20)
    assert est.algorithm == "hash"
    assert est.cpu == pytest.approx(10 + 20 + 10 + 20)


def test_join_estimate_uses_nested_loop_without_keys():
    est = join_estimate(scan_estimate(10), scan_estimate(20))
    assert est.rows == 200
    assert est.algorithm == "nested_loop"
    assert est.cpu >= 200


def test_choose_build_side_prefers_smaller_left():
    assert choose_build_side(scan_estimate(5), scan_estimate(50)) == "left"
    assert choose_build_side(scan_estimate(50), scan_estimate(5)) == "right"
    assert choose_build_side(scan_estimate(5), scan_estimate(5)) == "right"


def test_project_and_sort_preserve_rows():
    child = scan_estimate(100)
    projected = project_estimate(child, width=3)
    sorted_est = sort_estimate(projected, keys=2)
    assert projected.rows == 100
    assert sorted_est.rows == 100
    assert projected.cpu > child.cpu
    assert sorted_est.cpu > projected.cpu


def test_limit_estimate_handles_offset_and_zero():
    child = scan_estimate(100)
    assert limit_estimate(child, limit=10).rows == 10
    assert limit_estimate(child, limit=10, offset=2).rows == 10
    assert limit_estimate(child, limit=100, offset=2).rows == 98
    assert limit_estimate(child, limit=None, offset=98).rows == 2
    assert limit_estimate(child, limit=0).rows == 0
    assert limit_estimate(child, limit=None, offset=150).rows == 0


def test_aggregate_estimate_uses_group_count():
    child = scan_estimate(100)
    assert aggregate_estimate(child).rows == 1
    assert aggregate_estimate(child).confidence == "stats"
    assert aggregate_estimate(child, groups=4).rows == pytest.approx(10)
    assert aggregate_estimate(child, groups=4).confidence == "derived"


def test_distinct_and_setop_estimates():
    child = scan_estimate(100)
    assert distinct_estimate(child).rows == 90
    left = scan_estimate(10)
    right = scan_estimate(20)
    assert setop_estimate(left, right).rows == 30
    assert setop_estimate(left, right, distinct=True).rows == pytest.approx(27)
