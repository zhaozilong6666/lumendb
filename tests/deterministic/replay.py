"""确定性重放命令行：``python -m tests.deterministic.replay --seeds 10``。

每个 seed 使用独立的临时目录，固定键位、固定闸门、固定因果断言。失败会
打印带序号的事件轨迹；同一 seed 再跑一次会走到同一个交错点。
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import tempfile

from .scenario_split import run_case


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LumenDB 确定性重放")
    parser.add_argument("--seeds", type=int, default=10, help="跑 0..N-1 个种子")
    parser.add_argument("--seed", type=int, default=None, help="只跑一个种子")
    args = parser.parse_args(argv)

    seeds = [args.seed] if args.seed is not None else list(range(args.seeds))
    failures = 0
    for seed in seeds:
        with tempfile.TemporaryDirectory(prefix=f"lumendb_replay_{seed}_") as td:
            try:
                trace, detail = run_case(seed, pathlib.Path(td))
            except BaseException as exc:  # noqa: BLE001
                failures += 1
                print(f"[seed {seed}] FAIL: {type(exc).__name__}: {exc}")
                if "seq | thread" in str(exc):
                    print(str(exc))
                continue
            print(
                f"[seed {seed}] PASS entries={detail['stats']['entries']} "
                f"depth={detail['stats']['depth']} "
                f"split_key={detail['case'].split_key} "
                f"follower={detail['case'].follower_start}"
                f"+{detail['case'].follower_count}"
            )
    if failures:
        print(f"{failures}/{len(seeds)} seeds 失败")
        return 1
    print(f"{len(seeds)}/{len(seeds)} seeds 通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
