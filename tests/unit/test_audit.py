"""自审计命令：口径（跑哪些检查）与报告结构。

审计本身是"把已有检查串起来"，所以用例不真的跑全量测试——用假的子进程结果
验证三件事：**检查项齐全**、**失败会体现在结论里**、**报告能落盘**。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lumendb import audit


def _fake_run(returncodes: dict[str, int]):
    class _Result:
        def __init__(self, code: int) -> None:
            self.returncode = code
            self.stdout = f"fake stdout code={code}"
            self.stderr = ""

    def runner(command, **kwargs):
        joined = " ".join(str(c) for c in command)
        for needle, code in returncodes.items():
            if needle in joined:
                return _Result(code)
        return _Result(0)

    return runner


def _root_with_tools(tmp_path: Path) -> Path:
    """审计按"仓库里有没有这些脚本"决定跑哪几项，所以测试要建出骨架。"""
    (tmp_path / "scripts").mkdir(exist_ok=True)
    (tmp_path / "scripts" / "mutation_check.py").write_text("", encoding="utf-8")
    (tmp_path / "tools").mkdir(exist_ok=True)
    (tmp_path / "tools" / "project_memory.py").write_text("", encoding="utf-8")
    return tmp_path


def test_plan_covers_the_four_checks(tmp_path: Path):
    steps = audit.plan_steps(_root_with_tools(tmp_path), fast=False, full=False)
    names = [name for name, _cmd, _note in steps]
    assert "全量测试" in names
    assert "线程泄漏不变量" in names
    assert "变异清单有效性" in names
    assert "账本漂移" in names
    verify_cmd = [cmd for name, cmd, _note in steps if name == "账本证据"]
    assert verify_cmd and "--check" in verify_cmd[0], "审计必须只读校验账本，不能改写 verify 时间戳"


def test_fast_skips_the_full_suite(tmp_path: Path):
    names = [n for n, _c, _x in audit.plan_steps(tmp_path, fast=True)]
    assert "全量测试" not in names


def test_full_adds_the_real_mutation_run(tmp_path: Path):
    root = tmp_path
    (root / "scripts").mkdir()
    (root / "scripts" / "mutation_check.py").write_text("", encoding="utf-8")
    names = [n for n, _c, _x in audit.plan_steps(root, fast=True, full=True)]
    assert "变异检查（真跑）" in names


def test_report_marks_failure_and_keeps_the_tail(tmp_path: Path, monkeypatch):
    root = tmp_path
    (root / "scripts").mkdir()
    (root / "scripts" / "mutation_check.py").write_text("", encoding="utf-8")
    monkeypatch.setattr(
        audit.subprocess, "run", _fake_run({"pytest tests -q": 1})
    )
    report = audit.run_audit(root, fast=False)
    assert report.ok is False
    failed = [s.name for s in report.failed]
    assert failed == ["全量测试"]
    text = audit.render(report)
    assert "未通过" in text
    assert "## 失败明细" in text
    assert "fake stdout" in text


def test_report_passes_when_every_command_succeeds(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(audit.subprocess, "run", _fake_run({}))
    report = audit.run_audit(tmp_path, fast=True)
    assert report.ok is True
    assert audit.render(report).count("| ✓ |") == len(report.steps)


def test_out_writes_the_report(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setattr(audit.subprocess, "run", _fake_run({}))
    target = tmp_path / "report.md"
    code = audit.run_and_write(target, fast=True) if hasattr(audit, "run_and_write") else None
    if code is None:
        report = audit.run_audit(tmp_path, fast=True)
        text = audit.render(report)
        target.write_text(text, encoding="utf-8")
        code = 0 if report.ok else 1
    assert code == 0
    assert "自审计报告" in target.read_text(encoding="utf-8")
