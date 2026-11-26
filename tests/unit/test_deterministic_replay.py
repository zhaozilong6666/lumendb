"""确定性重放骨架的真实场景。

不是概率性压测：每个种子固定键位/初始值、固定闸门、固定断言顺序。失败时
轨迹里能直接看到 ``split.start`` / ``install.end`` / ``lock.acquire.*`` 的因果序。
"""

from __future__ import annotations

import pathlib

import pytest

from tests.deterministic.scenario_row_lock import run_row_case
from tests.deterministic.scenario_split import run_case


@pytest.mark.parametrize("seed", range(10))
def test_split_parent_install_window_is_deterministic(tmp_path: pathlib.Path, seed: int) -> None:
    trace, detail = run_case(seed, tmp_path / f"split-seed-{seed}")
    assert detail["stats"]["entries"] == 9 + 1 + detail["case"].follower_count
    # 轨迹里必须留下完整的因果链，不能只有最终状态。
    assert trace.count("split.start", thread="splitter") >= 1
    assert trace.count("split.start", thread="follower") >= 1
    assert trace.count("install.end", thread="splitter") >= 1


@pytest.mark.parametrize("seed", range(3))
def test_row_lock_wait_is_deterministic(tmp_path: pathlib.Path, seed: int) -> None:
    trace, detail = run_row_case(seed, tmp_path / f"row-seed-{seed}")
    assert detail["final"] == detail["initial"] + 2
    assert trace.count("lock.acquire.start", thread="writer-b") >= 1
    assert trace.count("lock.acquire.end", thread="writer-b") >= 1
