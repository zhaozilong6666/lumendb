#!/usr/bin/env python3
"""变异检查：把护栏故意改坏，看对应用例会不会红。

原理来自 `shotplan` 项目的 `evals/mutation`：**只统计测试数量说明不了测试有没有用**。
真正的判据是"把被测逻辑改坏之后，那条用例会不会失败"。这里把它做成本仓库可跑的
最小版本：

```
python scripts/mutation_check.py --check   # 只校验清单没腐烂（秒级，可进 CI）
python scripts/mutation_check.py           # 逐条变异 + 跑用例（分钟级）
```

加固新护栏时，往 `MUTATIONS` 里加一条：`文件 / 原片段 / 改坏后的片段 / 应该变红的用例`。
如果变异之后用例仍然是绿的，说明那条用例没有真正守住这个行为——这正是要发现的东西。
"""

from __future__ import annotations

import argparse
import dataclasses
import pathlib
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent


@dataclasses.dataclass(frozen=True)
class Mutation:
    label: str
    file: str
    original: str
    mutated: str
    target: str
    why: str = ""
    #: "fail" = 变异后目标用例必须变红（默认）；
    #: "pass" = 变异后仍然绿，且这是**已知**的（冗余保护，写清为什么）。
    expected: str = "fail"


MUTATIONS: list[Mutation] = [
    Mutation(
        label="键锁失效",
        file="lumendb/engine/executor.py",
        original="            self._lock_key(table, idx, key, txn)",
        mutated="            pass  # mutation: 插入索引项前不拿键锁",
        target=(
            "tests/unit/test_concurrent_dml.py"
            "::test_on_conflict_do_nothing_still_locks_the_unique_key"
        ),
        why=(
            "唯一性检查前的那把键锁只是第一道；真正把两个事务隔开的是插入索引项"
            "前的这把。去掉它，两个未提交事务会同时把同一个键写进物理索引，"
            "测试应看到 23505 或双写，而不是等锁得到的 40001。"
        ),
    ),
    Mutation(
        label="first-updater-wins 失效",
        file="lumendb/storage/heap.py",
        original="        self._raise_if_concurrent_write(txn, newest, old_rid, root_rid)",
        mutated="        pass  # mutation: 不检查链头版本",
        target=(
            "tests/unit/test_concurrency.py::TestWriteWriteConflict"
            "::test_stale_snapshot_update_is_rejected"
        ),
        why="去掉版本重查后，旧快照写入会覆盖 newest，一个逻辑行出现两个可见版本",
    ),
    Mutation(
        label="行锁失效",
        file="lumendb/engine/executor.py",
        original="        self._lock_row(table, rid, txn)",
        mutated="        pass  # mutation: 改行前不拿行锁",
        target=(
            "tests/unit/test_concurrency.py::TestWriteWriteConflict"
            "::test_two_writers_on_the_same_row_conflict"
        ),
        why=(
            "去掉行锁后，同一行并发仍然是 40001——因为 first-updater-wins "
            "（_raise_if_concurrent_write）在堆层又挡了一次。也就是说 X 行锁目前"
            "是**冗余保护**：它真正不可替代的地方是等锁式并发（还没做）与父行 S 锁"
            "的配合。这条变异保持 expected=pass，作为「已知冗余」的记录。"
        ),
        expected="pass",
    ),
    Mutation(
        label="外键取消失效",
        file="lumendb/engine/executor.py",
        original="                        self._lock_parent_row(ref_table, rid, txn)",
        mutated="                        pass  # mutation: 校验父行时不锁",
        target=(
            "tests/unit/test_concurrency.py::TestForeignKeyConcurrency"
            "::test_uncommitted_child_is_still_invisible"
        ),
        why="去掉父行 S 锁后，父行删除与子行插入可以同时成功，提交后悬空引用",
    ),
]

_IGNORE = shutil.ignore_patterns(
    ".git", "__pycache__", ".pytest_cache", "*.pyc", "lumendb_data", "data",
    ".mypy_cache", ".ruff_cache", "*.data", "*.wal",
)


def _check_manifest() -> int:
    """只做静态校验：清单里的片段必须还在源码里（否则变异脚本会静默失效）。"""
    problems = 0
    for mutation in MUTATIONS:
        path = ROOT / mutation.file
        if not path.exists():
            print(f"[清单] 文件不存在：{mutation.file}", file=sys.stderr)
            problems += 1
            continue
        if mutation.original not in path.read_text(encoding="utf-8"):
            print(
                f"[清单] {mutation.label}：片段已不在 {mutation.file} 里，需要更新清单",
                file=sys.stderr,
            )
            problems += 1
        if not (ROOT / mutation.target.split("::")[0]).exists():
            print(f"[清单] {mutation.label}：目标用例文件不存在 {mutation.target}", file=sys.stderr)
            problems += 1
    if problems:
        return 1
    print(f"[清单] {len(MUTATIONS)} 条变异全部有效（片段与目标用例都存在）")
    return 0


def _copy_tree(target: pathlib.Path) -> None:
    shutil.copytree(ROOT, target, ignore=_IGNORE, dirs_exist_ok=True)


def _run_mutation(mutation: Mutation, workdir: pathlib.Path) -> bool:
    """返回 True 表示"变异后用例变红"（符合预期）。"""
    path = workdir / mutation.file
    original_text = path.read_text(encoding="utf-8")
    if mutation.original not in original_text:
        print(f"[{mutation.label}] 片段缺失，跳过（先跑 --check）", file=sys.stderr)
        return False
    path.write_text(
        original_text.replace(mutation.original, mutation.mutated, 1), encoding="utf-8"
    )
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", mutation.target, "-q", "-x", "--no-header"],
            cwd=workdir,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
        )
    finally:
        path.write_text(original_text, encoding="utf-8")
    survived = result.returncode == 0
    if mutation.expected == "pass":
        if survived:
            print(f"[{mutation.label}] ✓ 变异后仍绿（已知冗余，符合预期）")
            return True
        print(f"[{mutation.label}] ✗ 预期仍然绿，但用例变红了：保护关系变了，请更新清单")
        return False
    if survived:
        print(f"[{mutation.label}] ✗ 变异后用例仍然通过：护栏没守住。{mutation.why}")
        tail = (result.stdout or "").strip().splitlines()[-3:]
        for line in tail:
            print(f"    {line}")
        return False
    print(f"[{mutation.label}] ✓ 变异后用例变红（符合预期）")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="变异检查：护栏改坏后必须有用例变红")
    parser.add_argument("--check", action="store_true", help="只校验清单有效性，不跑变异")
    parser.add_argument(
        "--only", default=None, help="只跑标签包含该子串的变异（调试用）"
    )
    args = parser.parse_args(argv)

    if args.check:
        return _check_manifest()

    targets = [
        m for m in MUTATIONS if args.only is None or args.only in m.label
    ]
    if not targets:
        print("没有匹配的变异", file=sys.stderr)
        return 2

    survived = 0
    for mutation in targets:
        with tempfile.TemporaryDirectory(prefix="lumendb-mutation-") as tmp:
            workdir = pathlib.Path(tmp) / "repo"
            _copy_tree(workdir)
            if not _run_mutation(mutation, workdir):
                survived += 1

    total = len(targets)
    red = sum(1 for m in targets if m.expected == "fail")
    known_green = total - red
    print(
        f"变异检查：{total - survived}/{total} 条符合预期"
        f"（{red} 条变异后应变红，{known_green} 条已知冗余应变绿）"
    )
    return 0 if survived == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
