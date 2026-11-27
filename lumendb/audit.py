"""自审计：一条命令跑完"这个仓库到底被验证了什么"。

多数项目能给出的是"测试数量"。审计要回答的是另外三个问题：

1. **测试本身有效吗**——变异检查：把护栏改坏，对应用例会不会红；
2. **文档和代码还对得上吗**——账本漂移（`project_memory drift`）；
3. **有没有人忘了停后台线程**——线程泄漏不变量。

这不是新算法，而是把已有的四件检查串成一份**可复现的报告**。判定全部发生在子进程里，
只认退出码；不认"我觉得没问题"。

```bash
python -m lumendb audit              # 全量：pytest + 变异清单 + 漂移 + 泄漏
python -m lumendb audit --fast       # 跳过全量测试（提交前 10 秒用）
python -m lumendb audit --full       # 追加真跑变异（分钟级）
python -m lumendb audit --out report.md
```
"""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional


@dataclass
class Step:
    name: str
    command: list[str]
    note: str = ""
    ok: bool = False
    seconds: float = 0.0
    output_tail: str = ""
    skipped: bool = False


@dataclass
class AuditReport:
    root: Path
    steps: list[Step] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(step.ok for step in self.steps if not step.skipped)

    @property
    def failed(self) -> list[Step]:
        return [s for s in self.steps if not s.ok and not s.skipped]

    def as_dict(self) -> dict:
        return {
            "root": str(self.root),
            "ok": self.ok,
            "steps": [
                {
                    "name": s.name,
                    "command": s.command,
                    "ok": s.ok,
                    "skipped": s.skipped,
                    "seconds": round(s.seconds, 2),
                    "note": s.note,
                    "output_tail": s.output_tail,
                }
                for s in self.steps
            ],
        }


def _tail(text: str, lines: int = 6) -> str:
    stripped = [line for line in (text or "").strip().splitlines() if line.strip()]
    return "\n".join(stripped[-lines:])


def plan_steps(
    root: Path, *, fast: bool = False, full: bool = False
) -> list[tuple[str, list[str], str]]:
    """审计要跑哪些命令（顺序固定，便于对比历史报告）。"""
    python = sys.executable
    steps: list[tuple[str, list[str], str]] = []

    if not fast:
        steps.append(
            ("全量测试", [python, "-m", "pytest", "tests", "-q"], "退出码即结论")
        )
    steps.append(
        (
            "线程泄漏不变量",
            [python, "-m", "pytest", "tests/unit/test_thread_leaks.py", "-q"],
            "创建→关闭后不得残留 lumendb-* 线程",
        )
    )
    mutation = root / "scripts" / "mutation_check.py"
    if mutation.exists():
        steps.append(
            (
                "变异清单有效性",
                [python, str(mutation), "--check"],
                "清单片段还在源码里（否则变异会静默跳过）",
            )
        )
        if full:
            steps.append(
                (
                    "变异检查（真跑）",
                    [python, str(mutation)],
                    "把护栏改坏，对应用例必须变红",
                )
            )
    memory = root / "tools" / "project_memory.py"
    if memory.exists():
        steps.append(
            (
                "账本漂移",
                [python, str(memory), "drift"],
                "文档里的数字/锚点与代码一致",
            )
        )
        steps.append(
            (
                "账本证据",
                [python, str(memory), "verify", "--check"],
                "每条事实的锚点可复核（只读，不改写账本；含不可复核的 CI 类条目）",
            )
        )
    return steps


def run_audit(
    root: Optional[Path] = None,
    *,
    fast: bool = False,
    full: bool = False,
    timeout: float = 1800.0,
) -> AuditReport:
    root = Path(root or Path.cwd())
    report = AuditReport(root=root)
    for name, command, note in plan_steps(root, fast=fast, full=full):
        step = Step(name=name, command=command, note=note)
        started = time.perf_counter()
        try:
            result = subprocess.run(
                command,
                cwd=root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
            )
            step.ok = result.returncode == 0
            step.output_tail = _tail(result.stdout + "\n" + result.stderr)
        except FileNotFoundError:
            step.ok = False
            step.output_tail = "命令不存在"
        except subprocess.TimeoutExpired:
            step.ok = False
            step.output_tail = f"超时（>{timeout:.0f}s）"
        step.seconds = time.perf_counter() - started
        report.steps.append(step)
    return report


def render(report: AuditReport) -> str:
    lines = ["# LumenDB 自审计报告", ""]
    lines.append(f"- 仓库：`{report.root}`")
    lines.append(f"- 结论：**{'通过' if report.ok else '未通过'}**")
    lines.append("")
    lines.append("| 检查 | 结果 | 耗时 | 说明 |")
    lines.append("|---|---|---:|---|")
    for step in report.steps:
        mark = "跳过" if step.skipped else ("✓" if step.ok else "✗")
        lines.append(f"| {step.name} | {mark} | {step.seconds:.1f}s | {step.note} |")
    failures = report.failed
    if failures:
        lines.append("")
        lines.append("## 失败明细")
        for step in failures:
            lines.append("")
            lines.append(f"### {step.name}")
            lines.append("```")
            lines.append("$ " + " ".join(step.command))
            lines.append(step.output_tail or "(无输出)")
            lines.append("```")
    lines.append("")
    lines.append(
        "> 审计只回答「验证了什么」，不回答「功能全不全」。"
        "AS OF 已接 SQL，可读窗口为 [max(VACUUM/DDL 结构下界, 持久提交账本最老 "
        "LSN), latest_commit_lsn]；账本只保留最近若干条提交，并随基准备份/"
        "增量备份进入副本，副本应用 COMMIT 时续写账本；"
        "`VACUUM ... RETAIN LSN` 可保留一个显式目标点；索引历史读只做"
        "候选键回堆解析，跨 checkpoint 历史 schema 解码仍未覆盖。"
        "文件级回收由元数据页空闲表（最多 1008 项）承接离线 VACUUM 空链尾、"
        "旧索引树和普通 DML 旧目录中间根，关闭前先 checkpoint 再截文件尾；"
        "page 0 的整页刷盘/淘汰会用内存 meta 字段叠加快照，避免缓存旧副本"
        "覆盖空闲表与高水位；溢出或崩溃丢表只泄漏空间，不会复用仍被引用的页。"
        "并发写模式下显式事务内 VACUUM 返回 0A000（逻辑回滚无法还原物理回收），"
        "自动提交 VACUUM 仍走在线/离线路径；ALTER TABLE 的 shadow heap 切换根页"
        "后旧堆页登记进空闲表，回滚则丢弃登记、保留旧根；DROP TABLE / "
        "DROP INDEX 删除目录项前收集堆页与索引树页，提交后同样回收；"
        "DROP CONSTRAINT 删除唯一/主键索引同样先收集旧树页；"
        "串行物理回滚在本事务开始后没有别的提交、回滚时也没有别的活跃事务时，"
        "会把本次新分配且已无人引用的页还给空闲表；并发逻辑回滚和重叠场景"
        "保守跳过，宁可泄漏也不复用仍被页链引用的页。"
        "其它边界见 docs/as-of-system-time.md、docs/concurrency.md 与 roadmap。"
    )
    return "\n".join(lines)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    fast = "--fast" in args
    full = "--full" in args
    out: Optional[Path] = None
    if "--out" in args:
        index = args.index("--out")
        if index + 1 < len(args):
            out = Path(args[index + 1])
    report = run_audit(fast=fast, full=full)
    text = render(report)
    print(text)
    if out is not None:
        out.write_text(text, encoding="utf-8")
        print(f"\n报告已写入 {out}")
    return 0 if report.ok else 1


__all__ = ["AuditReport", "Step", "main", "plan_steps", "render", "run_audit"]
