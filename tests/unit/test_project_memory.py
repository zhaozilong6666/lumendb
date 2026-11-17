"""tools.project_memory 的语义测试：分词、BM25、衰减、去重、数字冲突、证据校验。"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from tools.project_memory import (
    BM25,
    Fact,
    Ledger,
    _parse_when,
    extract_metrics,
    facts_from_git,
    find_git,
    jaccard,
    search,
    simhash,
    tokenize,
    verify_fact,
    _tokens,
)


def _fact(fact_id: str, claim: str, *, created: str | None = None, tags=(), evidence=(), kind="measurement"):
    return Fact(
        id=fact_id,
        kind=kind,
        claim=claim,
        created=created or datetime.now().astimezone().replace(microsecond=0).isoformat(),
        tags=list(tags),
        evidence=list(evidence),
    )


def test_tokenize_keeps_identifiers_and_splits_cjk():
    tokens = tokenize("btree._scan_range 把等值当范围")
    assert "btree._scan_range" in tokens
    assert "scan" in tokens and "range" in tokens
    assert "等值" in tokens
    # 中文既有单字也有二元组
    assert "等" in tokens
    assert "把等" in tokens or "等值" in tokens


def test_jaccard_and_simhash_flag_near_duplicates(tmp_path):
    ledger = Ledger(tmp_path / "ledger.jsonl", root=tmp_path)
    first = _fact("a", "解析缓存把参数化点查从 0.149ms 降到 0.096ms", tags=["perf"])
    second = _fact("b", "解析缓存把参数化点查从 0.149ms 降到 0.096ms（同一条 SQL）", tags=["perf"])
    ledger.append(first)
    assert jaccard(_tokens(first.search_text()), _tokens(second.search_text())) > 0.8
    assert [f.id for f in ledger.near_duplicates(second)] == ["a"]
    unrelated = _fact("c", "副本切换事务边界，避免索引页错位", tags=["replication"])
    assert ledger.near_duplicates(unrelated) == []


def test_bm25_prefers_exact_identifier():
    facts = [
        _fact("a", "解析缓存提交 dcab1b6 把点查降到 0.096ms", tags=["perf"]),
        _fact("b", "外键子表插入是最慢的基准项", tags=["bench"]),
    ]
    hits = search(facts, "dcab1b6", top_k=2, half_life_days=10**6)
    assert hits and hits[0]["fact"].id == "a"
    assert hits[0]["bm25"] > 0


def test_time_decay_prefers_newer_fact():
    now = datetime.now().astimezone()
    old = _fact("old", "点查 0.47ms", created=(now - timedelta(days=400)).isoformat())
    new = _fact("new", "点查 0.47ms", created=(now - timedelta(days=1)).isoformat())
    hits = search([old, new], "点查 0.47ms", top_k=2, half_life_days=120, now=now)
    assert [h["fact"].id for h in hits] == ["new", "old"]
    assert hits[0]["decay"] > hits[1]["decay"]
    # 原始 BM25 分相同，差异只来自衰减
    assert abs(hits[0]["bm25"] - hits[1]["bm25"]) < 1e-9


def test_number_conflict_detected_between_stale_measurements(tmp_path):
    ledger = Ledger(tmp_path / "ledger.jsonl", root=tmp_path)
    ledger.append(_fact("a", "索引点查 0.13 ms/次，全表 10.2ms/次", tags=["bench", "point-lookup"]))
    fresh = _fact("b", "索引点查 0.47 ms/次，全表 48ms/次", tags=["bench", "point-lookup"])
    conflicts = ledger.number_conflicts(fresh)
    assert conflicts, "同一主题下 0.13 vs 0.47 应该报警"
    other = conflicts[0][0]
    assert other.id == "a"
    # 完全不同的主题不该互相报警
    unrelated = _fact("c", "VACUUM 32ms 回收 500 个死版本", tags=["vacuum"])
    assert ledger.number_conflicts(unrelated) == []


def test_extract_metrics_only_counts_units():
    assert extract_metrics("0.149ms → 0.096ms（−35.6%）") == [
        (0.149, "ms"),
        (0.096, "ms"),
        (35.6, "%"),
    ]
    # 行号、提交哈希、纯数字都不算指标
    assert extract_metrics("database.py:364 与 d105d1b，1289 passed") == []


def test_ledger_is_append_only_and_reloadable(tmp_path):
    ledger = Ledger(tmp_path / "ledger.jsonl", root=tmp_path)
    first = _fact("a", "第一条")
    ledger.append(first)
    ledger.append(_fact("b", "第二条"))
    reloaded = Ledger(tmp_path / "ledger.jsonl", root=tmp_path)
    assert [f.id for f in reloaded.facts] == ["a", "b"]
    # 文件是逐行 JSON，便于 diff
    lines = (tmp_path / "ledger.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["claim"] == "第一条"


def test_verify_file_anchor(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "mod.py").write_text("def _parse_sql_cached(sql):\n    pass\n", encoding="utf-8")
    ok_fact = _fact(
        "a",
        "解析缓存存在",
        evidence=[{"type": "file", "value": "pkg/mod.py", "anchor": "_parse_sql_cached"}],
    )
    status, details = verify_fact(ok_fact, root=tmp_path)
    assert status == "ok" and details[0]["status"] == "ok"

    stale = _fact(
        "b",
        "锚点已消失",
        evidence=[{"type": "file", "value": "pkg/mod.py", "anchor": "lru_cache"}],
    )
    status, details = verify_fact(stale, root=tmp_path)
    assert status == "failed" and "找不到锚点" in details[0]["detail"]

    missing = _fact(
        "c",
        "文件被删",
        evidence=[{"type": "file", "value": "pkg/gone.py", "anchor": ""}],
    )
    assert verify_fact(missing, root=tmp_path)[0] == "failed"


def test_verify_commit_against_temp_repo(tmp_path):
    git = find_git()
    if not git:
        pytest.skip("本机没有 git")
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run([git, "-C", str(repo), "init", "-q"], check=True)
    (repo / "a.txt").write_text("hello\n", encoding="utf-8")
    subprocess.run([git, "-C", str(repo), "add", "a.txt"], check=True)
    subprocess.run(
        [git, "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "first"],
        check=True,
    )
    head = subprocess.run(
        [git, "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()

    good = _fact("a", "提交存在", evidence=[{"type": "commit", "value": head}])
    assert verify_fact(good, root=repo)[0] == "ok"

    bad = _fact("b", "提交不存在", evidence=[{"type": "commit", "value": "0" * 40}])
    assert verify_fact(bad, root=repo)[0] == "failed"


def test_ingest_git_marks_commits_verified_and_dedupes(tmp_path):
    git = find_git()
    if not git:
        pytest.skip("本机没有 git")
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run([git, "-C", str(repo), "init", "-q"], check=True)
    for name, message in (("a.txt", "perf(parser): 解析缓存"), ("b.txt", "fix(replica): 事务边界")):
        (repo / name).write_text(name, encoding="utf-8")
        subprocess.run([git, "-C", str(repo), "add", name], check=True)
        subprocess.run(
            [git, "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", message],
            check=True,
        )
    ledger = Ledger(tmp_path / "ledger.jsonl", root=repo)
    fresh = facts_from_git(repo, "-5", ledger)
    assert len(fresh) == 2
    assert all(f.kind == "commit" and f.confidence == "verified" for f in fresh)
    assert {f.claim for f in fresh} == {"perf(parser): 解析缓存", "fix(replica): 事务边界"}
    assert any("perf" in f.tags for f in fresh)
    for fact in fresh:
        ledger.append(fact)
    assert facts_from_git(repo, "-5", ledger) == [], "同一提交不该灌第二遍"


def test_search_respects_kind_and_tag_filters():
    facts = [
        _fact("a", "索引点查 0.47ms", tags=["bench"], kind="measurement"),
        _fact("b", "索引 latch 暂不做", tags=["concurrency"], kind="decision"),
    ]
    hits = search(facts, "索引", top_k=5, kind="decision", half_life_days=10**6)
    assert [h["fact"].id for h in hits] == ["b"]
    hits = search(facts, "索引", top_k=5, tag="bench", half_life_days=10**6)
    assert [h["fact"].id for h in hits] == ["a"]


def test_status_command_summarizes_project(tmp_path, capsys):
    git = find_git()
    if not git:
        pytest.skip("本机没有 git")
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run([git, "-C", str(repo), "init", "-q"], check=True)
    (repo / "a.txt").write_text("x", encoding="utf-8")
    subprocess.run([git, "-C", str(repo), "add", "a.txt"], check=True)
    subprocess.run(
        [git, "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t",
         "commit", "-qm", "feat(replica): 事务边界"],
        check=True,
    )
    head = subprocess.run(
        [git, "-C", str(repo), "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    ledger = Ledger(tmp_path / "ledger.jsonl", root=repo)
    ledger.append(_fact("q1", "本地提交是否 push 到远端，等待确认", kind="open_question", tags=["git"]))
    ledger.append(
        _fact(
            "t1", "全量测试 1302 passed（182.9s）",
            kind="measurement", tags=["tests"],
            evidence=[{"type": "commit", "value": head}],
        )
    )

    from tools.project_memory import main

    assert main(["--ledger", str(ledger.path), "--root", str(repo), "status"]) == 0
    out = capsys.readouterr().out
    assert "项目状态" in out
    assert head in out
    assert "未决问题（1）" in out
    assert "1302" in out
    assert "校验" in out

