"""tools.context_pack 的语义测试：预算、单任务、索引漂移、封存与交接包。"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tools import context_pack as cp
from tools.project_memory import find_git

BASE_TEXT = """# 常驻

规则：改实现必须带测试。
"""

TASK_TEMPLATE = """---
id: T-20261119-1
status: {status}
opened: 2026-11-19
base: {base}
---
# 当前任务

## 目标
做一件具体的事。

## 验收
{accept}

## 不做
- 不改实现。

## 前置快照
{dep}
"""

LEDGER_LINE = json.dumps(
    {
        "id": "f-1",
        "kind": "decision",
        "claim": "测试用记录",
        "created": "2026-11-19T00:00:00+08:00",
    },
    ensure_ascii=False,
) + "\n"


def _git(root: Path, *args: str) -> subprocess.CompletedProcess:
    exe = find_git()
    assert exe, "测试需要 git"
    proc = subprocess.run(
        [exe, "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.returncode == 0, proc.stderr
    return proc


def _commit(root: Path, message: str = "init") -> str:
    _git(root, "add", "-A")
    _git(root, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", message)
    return _git(root, "rev-parse", "--short", "HEAD").stdout.strip()


def _snap_text(
    commit: str,
    *,
    task: str = "T-20261119-1",
    title: str = "做一件具体的事",
    status: str = "sealed",
    facts: str = "",
    modules: str = "tests/unit",
    keywords: str = "context",
    supersedes: str = "",
    body: str | None = None,
) -> str:
    body = body or (
        "## 结论\n做完了。\n\n## 改动\n只改测试。\n\n## 证据\npytest 全绿。\n\n## 遗留\n无。\n"
    )
    return f"""---
task: {task}
title: {title}
date: 2026-11-19
commit: {commit}
modules: [{modules}]
keywords: [{keywords}]
facts: [{facts}]
supersedes: [{supersedes}]
status: {status}
---
# {title}

{body}
"""


def _make_repo(
    tmp_path: Path,
    *,
    base: str = BASE_TEXT,
    status: str = "active",
    accept: str = "- check 通过。",
    dep: str = "无",
    snapshots: dict[str, str] | None = None,
    ledger: str = "",
) -> tuple[Path, str]:
    root = tmp_path
    (root / "docs" / "context" / "snapshots").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "memory").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "context" / "base.md").write_text(base, encoding="utf-8")
    (root / "docs" / "memory" / "ledger.jsonl").write_text(ledger, encoding="utf-8")
    _git(root, "init", "-q")
    sha = _commit(root, "base")
    (root / "docs" / "context" / "task.md").write_text(
        TASK_TEMPLATE.format(status=status, base=sha, accept=accept, dep=dep),
        encoding="utf-8",
    )
    for name, text in (snapshots or {}).items():
        (root / "docs" / "context" / "snapshots" / name).write_text(text, encoding="utf-8")
    repo = cp.ContextRepo(root)
    repo.write_index()
    return root, sha


def _codes(findings: list[cp.Finding]) -> set[str]:
    return {f.code for f in findings}


def _run(root: Path, *args: str) -> int:
    return cp.main(["--root", str(root), *args])


def test_estimate_tokens_counts_cjk_and_ascii_conservatively():
    assert cp.estimate_tokens("") == 0
    assert cp.estimate_tokens("中文") == 2
    # ASCII 按 3 字符/token：12 个字符算 4 个，真实分词器约 3 个
    assert cp.estimate_tokens("abcdefghijkl") == 4
    long_text = "x" * 300
    assert cp.estimate_tokens(long_text) > cp.estimate_tokens("x" * 30)


def test_minimal_repo_passes(tmp_path):
    root, _ = _make_repo(tmp_path)
    assert cp.check(cp.ContextRepo(root)) == []


def test_duplicate_objective_section_fails(tmp_path):
    task = TASK_TEMPLATE.format(status="active", base="__BASE__", accept="- ok", dep="无")
    task = task.replace("## 目标\n做一件具体的事。", "## 目标\n做一件具体的事。\n\n## 目标\n再做一件。")
    root, _ = _make_repo(tmp_path)
    path = root / "docs" / "context" / "task.md"
    path.write_text(task.replace("__BASE__", _git(root, "rev-parse", "--short", "HEAD").stdout.strip()), encoding="utf-8")
    assert "CTX004" in _codes(cp.check(cp.ContextRepo(root)))


def test_empty_acceptance_fails(tmp_path):
    root, _ = _make_repo(tmp_path, accept="- 无")
    assert "CTX004" in _codes(cp.check(cp.ContextRepo(root)))


def test_resident_budget_is_enforced(tmp_path):
    root, _ = _make_repo(tmp_path)
    findings = cp.check(cp.ContextRepo(root), budget=10)
    assert "CTX006" in _codes(findings)


def test_drifting_numbers_in_base_fail(tmp_path):
    root, _ = _make_repo(tmp_path, base=BASE_TEXT + "\n全量 1344 passed，见 09376cb。\n")
    codes = _codes(cp.check(cp.ContextRepo(root)))
    assert "CTX007" in codes


def test_missing_dependency_snapshot_fails(tmp_path):
    root, _ = _make_repo(tmp_path, dep="20260101-nope.md")
    assert "CTX005" in _codes(cp.check(cp.ContextRepo(root)))


def test_done_without_sealed_snapshot_fails(tmp_path):
    root, sha = _make_repo(tmp_path, status="done")
    assert "CTX012" in _codes(cp.check(cp.ContextRepo(root)))

    (root / "docs" / "context" / "snapshots" / "20261119-done.md").write_text(
        _snap_text(sha), encoding="utf-8"
    )
    cp.ContextRepo(root).write_index()
    assert "CTX012" not in _codes(cp.check(cp.ContextRepo(root)))


def test_index_drift_is_detected_and_fixed(tmp_path):
    root, _ = _make_repo(tmp_path)
    index = root / "docs" / "context" / "snapshots" / "index.md"
    index.write_text(index.read_text(encoding="utf-8") + "| 手写 | - | - | - | - | - | - |\n", encoding="utf-8")
    assert "CTX008" in _codes(cp.check(cp.ContextRepo(root)))

    _run(root, "index", "--write")
    assert cp.check(cp.ContextRepo(root)) == []


def test_snapshot_commit_must_exist(tmp_path):
    root, _ = _make_repo(
        tmp_path,
        snapshots={"20261119-bad.md": _snap_text("deadbee")},
    )
    assert "CTX010" in _codes(cp.check(cp.ContextRepo(root)))


def test_snapshot_fact_must_exist_in_ledger(tmp_path):
    root, sha = _make_repo(tmp_path)
    snap = root / "docs" / "context" / "snapshots" / "20261119-fact.md"
    snap.write_text(_snap_text(sha, facts="f-nope"), encoding="utf-8")
    cp.ContextRepo(root).write_index()
    assert "CTX011" in _codes(cp.check(cp.ContextRepo(root)))

    (root / "docs" / "memory" / "ledger.jsonl").write_text(LEDGER_LINE, encoding="utf-8")
    snap.write_text(_snap_text(sha, facts="f-1"), encoding="utf-8")
    cp.ContextRepo(root).write_index()
    assert "CTX011" not in _codes(cp.check(cp.ContextRepo(root)))


def test_seal_rejects_placeholders_and_accepts_filled_body(tmp_path):
    root, sha = _make_repo(tmp_path)
    draft = root / "docs" / "context" / "snapshots" / "20261119-draft.md"
    draft.write_text(
        _snap_text(sha, status="draft", body="## 结论\n（待填）\n\n## 改动\n（待填）\n\n## 证据\n（待填）\n\n## 遗留\n（待填）\n"),
        encoding="utf-8",
    )
    cp.ContextRepo(root).write_index()
    assert _run(root, "seal", "--file", draft.name) == 1

    draft.write_text(_snap_text(sha, status="draft"), encoding="utf-8")
    assert _run(root, "seal", "--file", draft.name) == 0
    assert "status: sealed" in draft.read_text(encoding="utf-8")


def test_next_refuses_until_snapshot_sealed(tmp_path):
    root, sha = _make_repo(tmp_path)
    assert _run(root, "next", "--to", "下一件事", "--accept", "做完") == 1

    (root / "docs" / "context" / "snapshots" / "20261119-done.md").write_text(
        _snap_text(sha), encoding="utf-8"
    )
    cp.ContextRepo(root).write_index()
    assert _run(root, "next", "--to", "下一件事", "--accept", "做完") == 0
    text = (root / "docs" / "context" / "task.md").read_text(encoding="utf-8")
    assert "status: active" in text
    assert "## 目标\n下一件事" in text


def test_pack_includes_only_declared_dependencies(tmp_path, capsys):
    root, sha = _make_repo(tmp_path)
    (root / "docs" / "context" / "snapshots" / "20261118-dep.md").write_text(
        _snap_text(sha, title="依赖的那一份"), encoding="utf-8"
    )
    (root / "docs" / "context" / "snapshots" / "20261117-other.md").write_text(
        _snap_text(sha, title="不该被带上"), encoding="utf-8"
    )
    task = root / "docs" / "context" / "task.md"
    text = task.read_text(encoding="utf-8").replace("## 前置快照\n无", "## 前置快照\n20261118-dep.md")
    task.write_text(text, encoding="utf-8")
    cp.ContextRepo(root).write_index()

    assert _run(root, "pack") == 0
    out = capsys.readouterr().out
    assert "依赖的那一份" in out
    assert "不该被带上" not in out
    assert "常驻" in out


def test_budget_threshold_returns_2_at_70_percent(capsys):
    assert cp.main(["budget", "--used", "140000", "--window", "200000"]) == 2
    assert "封存" in capsys.readouterr().out
    assert cp.main(["budget", "--used", "100000", "--window", "200000"]) == 0
    assert cp.main(["budget", "--used", "110000", "--window", "200000"]) == 0
    assert "注意收敛" in capsys.readouterr().out


def test_new_fills_commit_and_modules_from_git(tmp_path):
    root = tmp_path
    (root / "docs" / "context" / "snapshots").mkdir(parents=True)
    (root / "docs" / "memory").mkdir(parents=True)
    (root / "docs" / "context" / "base.md").write_text(BASE_TEXT, encoding="utf-8")
    (root / "docs" / "memory" / "ledger.jsonl").write_text("", encoding="utf-8")
    _git(root, "init", "-q")
    first = _commit(root, "first")
    (root / "lumendb").mkdir()
    (root / "lumendb" / "x.py").write_text("x = 1\n", encoding="utf-8")
    (root / "tests").mkdir()
    (root / "tests" / "test_x.py").write_text("def test_x(): pass\n", encoding="utf-8")
    second = _commit(root, "second")
    (root / "docs" / "context" / "task.md").write_text(
        TASK_TEMPLATE.format(status="active", base=first, accept="- ok", dep="无"),
        encoding="utf-8",
    )
    cp.ContextRepo(root).write_index()

    assert _run(root, "new", "--slug", "row-lock", "--title", "行锁", "--date", "20261119") == 0
    text = (root / "docs" / "context" / "snapshots" / "20261119-row-lock.md").read_text(encoding="utf-8")
    assert f"commit: {second}" in text
    assert "lumendb" in text
    assert "status: draft" in text


def test_sealed_snapshot_over_budget_fails(tmp_path):
    root, sha = _make_repo(tmp_path)
    big = (
        "## 结论\n做完了。\n\n## 改动\n"
        + ("x" * 9000)  # ASCII 按 3 字符/token => 约 3000 token，超过 2000
        + "\n\n## 证据\npytest 全绿。\n\n## 遗留\n无。\n"
    )
    snap = root / "docs" / "context" / "snapshots" / "20261119-big.md"
    snap.write_text(_snap_text(sha, body=big), encoding="utf-8")
    cp.ContextRepo(root).write_index()
    assert "CTX016" in _codes(cp.check(cp.ContextRepo(root)))


def test_query_recalls_snapshot_by_keyword(tmp_path, capsys):
    root, sha = _make_repo(tmp_path)
    (root / "docs" / "context" / "snapshots" / "20261119-row-lock.md").write_text(
        _snap_text(
            sha,
            title="行锁粒度",
            keywords="lock, row",
            body="## 结论\n用行/键锁替换单写者锁。\n\n## 改动\n改执行器。\n\n## 证据\n全绿。\n\n## 遗留\n无。\n",
        ),
        encoding="utf-8",
    )
    (root / "docs" / "context" / "snapshots" / "20261118-other.md").write_text(
        _snap_text(
            sha,
            title="别的事",
            keywords="other",
            body="## 结论\n无关。\n\n## 改动\nx。\n\n## 证据\ny。\n\n## 遗留\nz。\n",
        ),
        encoding="utf-8",
    )
    cp.ContextRepo(root).write_index()

    assert _run(root, "query", "行锁") == 0
    out = capsys.readouterr().out
    assert "row-lock" in out
    assert "other" not in out
    assert "行锁粒度" in out


def test_query_scores_multi_term_matches(tmp_path, capsys):
    root, sha = _make_repo(tmp_path)
    (root / "docs" / "context" / "snapshots" / "20261119-both.md").write_text(
        _snap_text(
            sha,
            title="崩溃恢复与 WAL",
            keywords="wal, crash",
            body="## 结论\nWAL 支撑崩溃恢复。\n\n## 改动\nx。\n\n## 证据\ny。\n\n## 遗留\nz。\n",
        ),
        encoding="utf-8",
    )
    (root / "docs" / "context" / "snapshots" / "20261118-one.md").write_text(
        _snap_text(
            sha,
            title="只有 WAL",
            keywords="wal",
            body="## 结论\n只有 WAL。\n\n## 改动\nx。\n\n## 证据\ny。\n\n## 遗留\nz。\n",
        ),
        encoding="utf-8",
    )
    cp.ContextRepo(root).write_index()

    assert _run(root, "query", "wal", "crash") == 0
    out = capsys.readouterr().out
    # 命中两个词的排在命中一个词的前面
    assert out.index("both") < out.index("one")
    assert "[2/2]" in out
    assert "[1/2]" in out


def test_supersedes_reference_must_exist(tmp_path):
    root, sha = _make_repo(tmp_path)
    snap = root / "docs" / "context" / "snapshots" / "20261119-new.md"
    snap.write_text(_snap_text(sha, supersedes="20261115-nope.md"), encoding="utf-8")
    cp.ContextRepo(root).write_index()
    assert "CTX017" in _codes(cp.check(cp.ContextRepo(root)))


def test_supersedes_self_fails(tmp_path):
    root, sha = _make_repo(tmp_path)
    snap = root / "docs" / "context" / "snapshots" / "20261119-self.md"
    snap.write_text(_snap_text(sha, supersedes="20261119-self.md"), encoding="utf-8")
    cp.ContextRepo(root).write_index()
    assert "CTX017" in _codes(cp.check(cp.ContextRepo(root)))


def test_query_marks_superseded_snapshot(tmp_path, capsys):
    root, sha = _make_repo(tmp_path)
    (root / "docs" / "context" / "snapshots" / "20261118-old.md").write_text(
        _snap_text(sha, title="旧结论：单写者锁", keywords="lock"),
        encoding="utf-8",
    )
    (root / "docs" / "context" / "snapshots" / "20261119-new.md").write_text(
        _snap_text(sha, title="新结论：行锁", keywords="lock", supersedes="20261118-old.md"),
        encoding="utf-8",
    )
    cp.ContextRepo(root).write_index()

    assert _run(root, "query", "lock") == 0
    out = capsys.readouterr().out
    assert "已过时" in out
    assert "20261119-new.md" in out
    assert "被 20261119-new.md 替代" in out
