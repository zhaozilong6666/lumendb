#!/usr/bin/env python3
"""质量门禁：一条命令跑完"该跑的检查"，退出码即结论。

设计原则（借 shotplan 的 `make gates`）：**判定不经过模型**。门禁只认命令的退出码，
不认"我觉得没问题"。默认档位是分钟级、可进 CI 的，重活放到 `--full`：

```
python scripts/gates.py            # 清单有效性 + 文档/代码漂移 + 全量测试
python scripts/gates.py --full     # 追加变异检查（每条护栏改坏后必须有用例变红）
python scripts/gates.py --skip-tests   # 只跑静态检查（提交前 10 秒用）
```

每一步都是一个子进程，失败立刻中止并打印命令，便于复现。
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def _run(label: str, command: list[str], cwd: pathlib.Path = ROOT) -> bool:
    print(f"\n=== {label}\n$ {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        print(f"✗ {label} 失败（exit={result.returncode}）：{' '.join(command)}")
        return False
    print(f"✓ {label}")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LumenDB 质量门禁")
    parser.add_argument("--skip-tests", action="store_true", help="跳过全量测试")
    parser.add_argument("--full", action="store_true", help="追加变异检查（分钟级）")
    args = parser.parse_args(argv)

    python = sys.executable
    steps: list[tuple[str, list[str]]] = []

    if not args.skip_tests:
        steps.append(("全量测试", [python, "-m", "pytest", "tests", "-q"]))

    steps.append(
        ("变异清单有效性", [python, "scripts/mutation_check.py", "--check"])
    )

    memory = ROOT / "tools" / "project_memory.py"
    if memory.exists():
        steps.append(("账本漂移检查", [python, str(memory), "drift"]))

    if args.full:
        steps.append(("变异检查", [python, "scripts/mutation_check.py"]))

    for label, command in steps:
        if not _run(label, command):
            return 1

    print(f"\n门禁通过（{len(steps)} 项）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
