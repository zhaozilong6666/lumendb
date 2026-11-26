"""确定性重放命令行：``python -m tests.deterministic.replay --seeds 10``。

每个 seed 使用独立的临时目录，固定键位/初始值、固定闸门、固定因果断言。
失败会打印带序号的事件轨迹；同一 seed 再跑一次会走到同一个交错点。
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import tempfile

from .scenario_ddl import run_ddl_case
from .scenario_row_lock import run_row_case
from .scenario_split import run_case


def _run_split(seed: int, root: pathlib.Path) -> str:
    trace, detail = run_case(seed, root / f"split-{seed}")
    return (
        f"split entries={detail['stats']['entries']} depth={detail['stats']['depth']} "
        f"split_key={detail['case'].split_key}"
    )


def _run_row(seed: int, root: pathlib.Path) -> str:
    trace, detail = run_row_case(seed, root / f"row-{seed}")
    return f"row initial={detail['initial']} final={detail['final']}"


def _run_ddl(seed: int, root: pathlib.Path) -> str:
    trace, detail = run_ddl_case(seed, root / f"ddl-{seed}")
    return f"ddl rows={detail['rows']}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LumenDB 确定性重放")
    parser.add_argument("--seeds", type=int, default=10, help="跑 0..N-1 个种子")
    parser.add_argument("--seed", type=int, default=None, help="只跑一个种子")
    parser.add_argument(
        "--scenario",
        choices=["split", "row", "ddl", "all"],
        default="all",
        help="split=叶分裂结构窗口；row=两个事务写同一行；ddl=未提交写挡住并发 DDL",
    )
    args = parser.parse_args(argv)

    seeds = [args.seed] if args.seed is not None else list(range(args.seeds))
    scenarios = ["split", "row", "ddl"] if args.scenario == "all" else [args.scenario]
    failures = 0
    for seed in seeds:
        with tempfile.TemporaryDirectory(prefix=f"lumendb_replay_{seed}_") as td:
            root = pathlib.Path(td)
            for scenario in scenarios:
                try:
                    if scenario == "split":
                        detail = _run_split(seed, root)
                    elif scenario == "row":
                        detail = _run_row(seed, root)
                    else:
                        detail = _run_ddl(seed, root)
                except BaseException as exc:  # noqa: BLE001
                    failures += 1
                    print(f"[seed {seed}][{scenario}] FAIL: {type(exc).__name__}: {exc}")
                    if "seq | thread" in str(exc):
                        print(str(exc))
                    continue
                print(f"[seed {seed}][{scenario}] PASS {detail}")
    if failures:
        print(f"{failures}/{len(seeds) * len(scenarios)} 个场景失败")
        return 1
    print(f"{len(seeds) * len(scenarios)}/{len(seeds) * len(scenarios)} 个场景通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
