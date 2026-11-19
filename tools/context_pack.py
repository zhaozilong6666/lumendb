"""三层上下文包：常驻层、当前任务层、按需快照层。

为什么不是"再写一份笔记"
------------------------
长周期项目里上下文会膨胀，不是因为信息多，而是因为**信息没有落脚点**：
每轮都要把"项目是什么、上次做到哪、为什么这么选"重新塞进对话。反过来，
把历史都写进一个 NOTES.md 也不行——它只会变成第二份需要维护、且没人核对
的文档，最后和代码一起过期。

所以这里分三层，各自只回答一个问题：

====================  ==========================  ==============================
层                    位置                        回答什么
====================  ==========================  ==============================
常驻 base             ``docs/context/base.md``    这个项目是什么、哪些规则不能破
当前任务 task         ``docs/context/task.md``    这一轮只做哪一件、怎么算做完
按需快照 snapshots/   ``docs/context/snapshots/`` 某件事当时为什么那么做、证据在哪
====================  ==========================  ==============================

常驻层默认进上下文，必须短；快照默认不进，靠索引单份召回，用完即弃。
账本（``docs/memory/ledger.jsonl``）仍然是**唯一的事实来源**：快照只写
叙述和决策，数字与结论一律引用账本 id，``check`` 会核对 id 是否真的存在。

哪些规则是机器检查的（不是靠自觉）
----------------------------------
* **常驻预算**：base + task 的估算 token 超过 ``RESIDENT_BUDGET`` 就失败。
  估算偏保守（ASCII 按 3 字符/token，实际约 4），中英混排时通常高于真实占用。
* **常驻层不许写会漂移的数字**：``1344 passed``、7 位以上 commit 哈希一律
  拒绝。要写现状就写"用哪条命令查"，而不是抄一个当天数字。
* **单任务**：task.md 只能有一个 ``# 当前任务``，必需章节（目标/验收/不做）
  缺一个或为空就失败；重复章节也算失败。
* **索引是生成物**：``index.md`` 由本脚本重写，手改后 ``check`` 会发现漂移。
  手写索引一定会落后于文件列表，这在这个仓库里已经被验证过很多次。
* **任务完成必封存**：``status: done`` 却没有 sealed 快照 -> 失败；
  ``next`` 也拒绝在未封存时开下一个任务。
* **封存不许留占位**：sealed 快照里出现 ``（待填）``/``TODO``/``TBD`` -> 失败。
* **证据可解析**：快照引用的 commit 必须存在，引用的账本 id 必须存在。

70% 阈值为什么是显式命令而不是自动触发
--------------------------------------
脚本读不到宿主对话的 token 计数，假装能读只会得到一个恒为 0 的指标
（这类"看起来在工作"的检查比没有更糟）。所以强制封存做成
``budget --used N --window M``：数字由人从界面读一次传进来，超过 70%
返回码 2。真正自动的是另外两条：常驻超预算、done 无快照——它们只依赖仓库
里的文件，CI 每次都能跑。

用法见 ``docs/context/README.md``。
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

from tools.project_memory import Ledger, find_git

REPO_ROOT = Path(__file__).resolve().parents[1]

RESIDENT_BUDGET = 500
HANDOFF_BUDGET = 4000
SNAPSHOT_BUDGET = 2000
WARN_RATIO = 0.50
FORCE_RATIO = 0.70

REQUIRED_SECTIONS = ("目标", "验收", "不做")
SNAPSHOT_SECTIONS = ("结论", "改动", "证据", "遗留")
PLACEHOLDERS = ("（待填）", "TODO", "TBD", "待补充")

_NAME_RE = re.compile(r"^(\d{8})-([a-z0-9][a-z0-9-]*)\.md$")
_TASK_ID_RE = re.compile(r"^T-\d{8}-\d+$")
_DRIFT_COUNT_RE = re.compile(r"\b\d[\d,]*\s*(?:passed|tests?|个测试|条测试)\b", re.I)
_COMMIT_RE = re.compile(r"\b[0-9a-f]{7,40}\b")
_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_ASCII_RUN_RE = re.compile(r"[A-Za-z0-9_./:\\@+-]+")
_OTHER_RE = re.compile(r"[^\sA-Za-z0-9_./:\\@+-]")


# ---------------------------------------------------------------------------
# token 估算：确定性、偏保守、不依赖任何分词器
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    """粗略的 token 数，用来做预算上限。

    中文按 1 字 1 token（常见分词器大致如此）；ASCII 按 3 字符 1 token
    （真实约 4 字符，留出余量）；换行 1 token；其余符号两个算一个。
    这不是严格上界，但在这个仓库的中英混排文本上通常偏高，足以当预算闸。
    """
    if not text:
        return 0
    cjk = len(_CJK_RE.findall(text))
    rest = _CJK_RE.sub(" ", text)
    ascii_tokens = sum(max(1, math.ceil(len(run) / 3)) for run in _ASCII_RUN_RE.findall(rest))
    other = len(_OTHER_RE.findall(rest))
    return cjk + ascii_tokens + other // 2 + text.count("\n")


def drift_hits(text: str) -> list[str]:
    """常驻层里不该出现的"会过期的数字"。"""
    hits = [m.group(0) for m in _DRIFT_COUNT_RE.finditer(text)]
    hits += [m.group(0) for m in _COMMIT_RE.finditer(text)]
    return hits


# ---------------------------------------------------------------------------
# 解析：front matter 只支持 key: value / key: [a, b]，够用就不引 yaml
# ---------------------------------------------------------------------------


class ContextError(ValueError):
    pass


_FM_RE = re.compile(r"\A---\r?\n(.*?)\r?\n---[ \t]*\r?\n?", re.S)
_H2_RE = re.compile(r"^##\s+(.+?)\s*$", re.M)


def _split_list(raw: str) -> list[str]:
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    if not raw.strip():
        return []
    return [part.strip().strip("'\"") for part in raw.split(",") if part.strip()]


def parse_front_matter(text: str) -> tuple[dict[str, object], str]:
    match = _FM_RE.match(text)
    if not match:
        return {}, text
    data: dict[str, object] = {}
    for line_no, line in enumerate(match.group(1).splitlines(), 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, value = stripped.partition(":")
        if not sep or not key.strip():
            raise ContextError(f"front matter 第 {line_no} 行不是 key: value：{line!r}")
        value = value.strip()
        data[key.strip()] = _split_list(value) if value.startswith("[") else value
    return data, text[match.end() :]


def split_sections(body: str) -> tuple[list[str], dict[str, str]]:
    """返回 (标题顺序, 标题->正文)。标题允许重复，重复由调用方判断。"""
    names: list[str] = []
    sections: dict[str, str] = {}
    matches = list(_H2_RE.finditer(body))
    for index, match in enumerate(matches):
        name = match.group(1)
        end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        names.append(name)
        sections[name] = body[match.end() : end].strip()
    return names, sections


def _meaningful(text: str) -> bool:
    lines = []
    for line in text.splitlines():
        cleaned = line.strip().lstrip("-*").strip()
        if cleaned and cleaned not in ("无", "（无）", "(无)"):
            lines.append(cleaned)
    return bool(lines)


def _parse_depends(body: str) -> list[str]:
    """读 ``## 前置快照`` 一节；也兼容写成一行 ``前置依赖快照：x.md``。"""
    _, sections = split_sections(body)
    raw = sections.get("前置快照", "").strip()
    if not raw:
        return []
    raw = re.sub(r"^前置依赖快照[：:]\s*", "", raw)
    if raw in ("无", "（无）", "(无)"):
        return []
    return [part.strip().strip("`") for part in re.split(r"[,，\s]+", raw) if part.strip()]


# ---------------------------------------------------------------------------
# git
# ---------------------------------------------------------------------------


def _git(root: Path, *args: str) -> subprocess.CompletedProcess:
    exe = find_git()
    if not exe:
        raise ContextError("找不到 git 可执行文件")
    return subprocess.run(
        [exe, "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def head_sha(root: Path) -> Optional[str]:
    try:
        proc = _git(root, "rev-parse", "--short", "HEAD")
    except ContextError:
        return None
    return proc.stdout.strip() if proc.returncode == 0 else None


def _is_shallow(root: Path) -> bool:
    try:
        proc = _git(root, "rev-parse", "--is-shallow-repository")
    except ContextError:
        return False
    return proc.returncode == 0 and proc.stdout.strip() == "true"


def commit_exists(root: Path, sha: str) -> bool:
    try:
        proc = _git(root, "rev-parse", "--verify", "--quiet", f"{sha}^{{commit}}")
    except ContextError:
        return False
    return proc.returncode == 0


def commits_since(root: Path, sha: str) -> Optional[int]:
    try:
        proc = _git(root, "rev-list", "--count", f"{sha}..HEAD")
    except ContextError:
        return None
    if proc.returncode != 0:
        return None
    return int(proc.stdout.strip() or 0)


def changed_modules(root: Path, base: str, limit: int = 8) -> list[str]:
    """从 base 到 HEAD 改动的模块目录，用来给快照打关键词之外的结构标签。

    按改动文件数降序，改动最多的模块排在前面——截断时不会把主要工作
    所在的目录丢掉。
    """
    try:
        proc = _git(root, "diff", "--name-only", f"{base}..HEAD")
    except ContextError:
        return []
    if proc.returncode != 0:
        return []
    counts: dict[str, int] = {}
    for line in proc.stdout.splitlines():
        parts = line.strip().split("/")
        if len(parts) >= 3 and parts[0] in ("lumendb", "tests", "docs"):
            module = "/".join(parts[:2])
        elif len(parts) >= 2:
            module = parts[0]
        elif parts and parts[0]:
            module = "root"
        else:
            continue
        counts[module] = counts.get(module, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [module for module, _ in ranked[:limit]]


# ---------------------------------------------------------------------------
# 模型
# ---------------------------------------------------------------------------


@dataclass
class Finding:
    level: str  # fail / warn
    code: str
    message: str
    path: str = ""

    def render(self) -> str:
        where = f" {self.path}" if self.path else ""
        return f"[{self.level.upper()}] {self.code}{where}  {self.message}"


@dataclass
class Task:
    id: str
    status: str
    opened: str
    base: str
    body: str
    path: Path
    depends: list[str] = field(default_factory=list)
    sections: dict[str, str] = field(default_factory=dict)
    section_names: list[str] = field(default_factory=list)


@dataclass
class Snapshot:
    path: Path
    name: str
    task: str
    title: str
    day: str
    commit: str
    modules: list[str]
    keywords: list[str]
    facts: list[str]
    status: str
    body: str
    sections: dict[str, str]
    section_names: list[str]


# ---------------------------------------------------------------------------
# 仓库
# ---------------------------------------------------------------------------


class ContextRepo:
    def __init__(self, root: Path = REPO_ROOT) -> None:
        self.root = Path(root)
        self.dir = self.root / "docs" / "context"
        self.base_path = self.dir / "base.md"
        self.task_path = self.dir / "task.md"
        self.snap_dir = self.dir / "snapshots"
        self.index_path = self.snap_dir / "index.md"
        self.ledger_path = self.root / "docs" / "memory" / "ledger.jsonl"

    # -- 读 ---------------------------------------------------------------
    def read(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def load_task(self) -> Task:
        if not self.task_path.exists():
            raise ContextError(f"缺少 {self.task_path.relative_to(self.root)}")
        meta, body = parse_front_matter(self.read(self.task_path))
        for key in ("id", "status", "opened", "base"):
            if not meta.get(key):
                raise ContextError(f"task.md 缺少 front matter 字段：{key}")
        task_id = str(meta["id"])
        if not _TASK_ID_RE.match(task_id):
            raise ContextError(f"task id 格式应是 T-YYYYMMDD-N，实际 {task_id!r}")
        status = str(meta["status"])
        if status not in ("active", "done"):
            raise ContextError(f"task status 只能是 active/done，实际 {status!r}")
        names, sections = split_sections(body)
        return Task(
            id=task_id,
            status=status,
            opened=str(meta["opened"]),
            base=str(meta["base"]),
            body=body,
            path=self.task_path,
            depends=_parse_depends(body),
            sections=sections,
            section_names=names,
        )

    def snapshots(self) -> list[Snapshot]:
        if not self.snap_dir.exists():
            return []
        out: list[Snapshot] = []
        for path in sorted(self.snap_dir.glob("*.md")):
            if path.name == "index.md":
                continue
            meta, body = parse_front_matter(self.read(path))
            names, sections = split_sections(body)
            out.append(
                Snapshot(
                    path=path,
                    name=path.name,
                    task=str(meta.get("task", "")),
                    title=str(meta.get("title", "")),
                    day=str(meta.get("date", "")),
                    commit=str(meta.get("commit", "")),
                    modules=[str(x) for x in _as_list(meta.get("modules"))],
                    keywords=[str(x) for x in _as_list(meta.get("keywords"))],
                    facts=[str(x) for x in _as_list(meta.get("facts"))],
                    status=str(meta.get("status", "")),
                    body=body,
                    sections=sections,
                    section_names=names,
                )
            )
        return out

    # -- 写 ---------------------------------------------------------------
    def render_index(self) -> str:
        rows = [
            "<!-- 由 tools/context_pack.py 生成；手改会被 check 判为漂移。 -->",
            "# 快照索引",
            "",
            "先看这里定位，只读命中那一份；不要按目录遍历。",
            "",
            "| 快照 | 日期 | 任务 | 提交 | 模块 | 关键词 | 账本 | 状态 |",
            "|---|---|---|---|---|---|---|---|",
        ]
        items = sorted(self.snapshots(), key=lambda s: (s.day, s.name), reverse=True)
        for snap in items:
            modules = "、".join(snap.modules) if snap.modules else "-"
            keywords = "、".join(snap.keywords) if snap.keywords else "-"
            facts = "、".join(snap.facts) if snap.facts else "-"
            rows.append(
                f"| [{snap.name}]({snap.name}) | {snap.day or '-'} | {snap.task or '-'} "
                f"| `{snap.commit or '-'}` | {modules} | {keywords} | {facts} | {snap.status or '-'} |"
            )
        if not items:
            rows.append("| （暂无） | - | - | - | - | - | - | - |")
        return "\n".join(rows) + "\n"

    def write_index(self) -> None:
        self.snap_dir.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(self.render_index(), encoding="utf-8", newline="\n")


def _as_list(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    text = str(value).strip()
    return [text] if text else []


# ---------------------------------------------------------------------------
# 检查
# ---------------------------------------------------------------------------


def check(repo: ContextRepo, budget: int = RESIDENT_BUDGET) -> list[Finding]:
    findings: list[Finding] = []
    rel = lambda p: str(p.relative_to(repo.root))  # noqa: E731
    shallow = _is_shallow(repo.root)

    for path in (repo.base_path, repo.task_path, repo.index_path):
        if not path.exists():
            findings.append(Finding("fail", "CTX001", "文件不存在", rel(path)))
    if not repo.base_path.exists() or not repo.task_path.exists():
        return findings

    base_text = repo.read(repo.base_path)
    task_text = repo.read(repo.task_path)

    # 常驻预算
    resident = estimate_tokens(base_text) + estimate_tokens(task_text)
    if resident > budget:
        findings.append(
            Finding(
                "fail",
                "CTX006",
                f"常驻 {resident} token 超过预算 {budget}：常驻层只放规则，细节进快照",
                "docs/context",
            )
        )

    # 会漂移的数字
    for hit in drift_hits(base_text):
        findings.append(Finding("fail", "CTX007", f"常驻层出现会过期的数字：{hit}", rel(repo.base_path)))
    meta, task_body = parse_front_matter(task_text)
    for hit in drift_hits(task_body):
        findings.append(Finding("fail", "CTX007", f"任务正文出现会过期的数字：{hit}", rel(repo.task_path)))

    # 任务结构
    try:
        task = repo.load_task()
    except ContextError as exc:
        findings.append(Finding("fail", "CTX002", str(exc), rel(repo.task_path)))
        return findings

    if task_body.count("# 当前任务") > 1:
        findings.append(Finding("fail", "CTX004", "出现多个 # 当前任务：一份 task.md 只能有一个任务", rel(repo.task_path)))
    dupes = [name for name in REQUIRED_SECTIONS if task.section_names.count(name) > 1]
    if dupes:
        findings.append(
            Finding("fail", "CTX004", f"任务章节重复：{'、'.join(dupes)}——像把两个任务塞进了一份文件", rel(repo.task_path))
        )
    for name in REQUIRED_SECTIONS:
        if name not in task.sections:
            findings.append(Finding("fail", "CTX004", f"缺少章节：## {name}", rel(repo.task_path)))
        elif not _meaningful(task.sections[name]):
            findings.append(Finding("fail", "CTX004", f"章节为空：## {name}", rel(repo.task_path)))

    # 任务起点必须是真实提交：base 写错会让 new 的 modules 静默变空
    if task.base and task.base != "unknown" and not shallow and not commit_exists(repo.root, task.base):
        findings.append(Finding("fail", "CTX010", f"任务的 base commit 不存在：{task.base}", rel(repo.task_path)))

    # 依赖快照
    try:
        snaps = repo.snapshots()
    except ContextError as exc:
        findings.append(Finding("fail", "CTX009", f"快照无法解析：{exc}", "docs/context/snapshots"))
        return findings
    by_name = {s.name: s for s in snaps}
    for dep in task.depends:
        if dep not in by_name:
            findings.append(Finding("fail", "CTX005", f"前置快照不存在：{dep}", rel(repo.task_path)))
        elif by_name[dep].status != "sealed":
            findings.append(Finding("fail", "CTX005", f"前置快照还没封存：{dep}", rel(repo.task_path)))
        if repo.index_path.exists() and dep not in repo.read(repo.index_path):
            findings.append(Finding("fail", "CTX005", f"前置快照未进索引：{dep}", rel(repo.task_path)))

    # 索引漂移
    if repo.index_path.exists():
        current = repo.read(repo.index_path)
        if current.rstrip("\n") != repo.render_index().rstrip("\n"):
            findings.append(
                Finding("fail", "CTX008", "索引与快照实际内容不一致（跑 index --write）", rel(repo.index_path))
            )

    # 快照本身
    sealed_by_task: dict[str, list[Snapshot]] = {}
    for snap in snaps:
        if not _NAME_RE.match(snap.name):
            findings.append(Finding("fail", "CTX009", "文件名应是 YYYYMMDD-slug.md", rel(snap.path)))
        if snap.status not in ("draft", "sealed"):
            findings.append(Finding("fail", "CTX009", f"status 只能是 draft/sealed，实际 {snap.status!r}", rel(snap.path)))
        for key, value in (("task", snap.task), ("title", snap.title), ("date", snap.day), ("commit", snap.commit)):
            if not value:
                findings.append(Finding("fail", "CTX009", f"缺 front matter 字段：{key}", rel(snap.path)))
        if snap.status != "sealed":
            continue
        sealed_by_task.setdefault(snap.task, []).append(snap)

        for name in SNAPSHOT_SECTIONS:
            if name not in snap.sections or not _meaningful(snap.sections[name]):
                findings.append(Finding("fail", "CTX009", f"封存快照缺少有效章节：## {name}", rel(snap.path)))
        for placeholder in PLACEHOLDERS:
            if placeholder in snap.body:
                findings.append(Finding("fail", "CTX014", f"封存快照仍含占位符：{placeholder}", rel(snap.path)))
        if not snap.modules:
            findings.append(
                Finding("fail", "CTX015", "封存快照没有 modules（base..HEAD 没有改动？先提交再封存）", rel(snap.path))
            )
        if not snap.keywords:
            findings.append(Finding("fail", "CTX015", "封存快照没有 keywords，索引里会定位不到", rel(snap.path)))
        body_tokens = estimate_tokens(snap.body)
        if body_tokens > SNAPSHOT_BUDGET:
            findings.append(
                Finding(
                    "fail",
                    "CTX016",
                    f"封存快照正文 {body_tokens} token 超过 {SNAPSHOT_BUDGET}：快照只写结论和证据，细节拆到下一份",
                    rel(snap.path),
                )
            )
        if snap.commit and not shallow and not commit_exists(repo.root, snap.commit):
            findings.append(Finding("fail", "CTX010", f"commit 不存在：{snap.commit}", rel(snap.path)))

    # 账本引用
    if repo.ledger_path.exists():
        known = {fact.id for fact in Ledger(repo.ledger_path, repo.root).facts}
        for snap in snaps:
            if snap.status != "sealed":
                continue
            for fact_id in snap.facts:
                if fact_id not in known:
                    findings.append(Finding("fail", "CTX011", f"引用的账本记录不存在：{fact_id}", rel(snap.path)))

    # 任务与快照的对应
    if task.status == "done" and not sealed_by_task.get(task.id):
        findings.append(
            Finding("fail", "CTX012", "任务标了 done，但没有对应的 sealed 快照（先 new + seal）", rel(repo.task_path))
        )
    for task_id, items in sealed_by_task.items():
        if len(items) > 1:
            names = "、".join(s.name for s in items)
            findings.append(Finding("fail", "CTX013", f"同一任务有多份封存快照：{task_id} -> {names}", "docs/context/snapshots"))

    return findings


# ---------------------------------------------------------------------------
# 命令
# ---------------------------------------------------------------------------


def cmd_check(args: argparse.Namespace) -> int:
    repo = ContextRepo(Path(args.root))
    findings = check(repo, budget=args.budget)
    failures = [f for f in findings if f.level == "fail"]
    for finding in findings:
        print(finding.render())
    base = repo.base_path if repo.base_path.exists() else None
    task = repo.task_path if repo.task_path.exists() else None
    resident = 0
    if base and task:
        resident = estimate_tokens(repo.read(base)) + estimate_tokens(repo.read(task))
    print(f"\n常驻估算 {resident}/{args.budget} token；失败 {len(failures)} 项")
    return 1 if failures else 0


def cmd_index(args: argparse.Namespace) -> int:
    repo = ContextRepo(Path(args.root))
    if args.write:
        repo.write_index()
        print(f"已写入 {repo.index_path}")
        return 0
    sys.stdout.write(repo.render_index())
    return 0


def cmd_new(args: argparse.Namespace) -> int:
    repo = ContextRepo(Path(args.root))
    task = repo.load_task()
    if task.status != "active":
        print(f"当前任务 {task.id} 状态是 {task.status}，先 next 开新任务", file=sys.stderr)
        return 1
    if not re.match(r"^[a-z0-9][a-z0-9-]*$", args.slug):
        print("slug 只能是小写字母、数字、连字符", file=sys.stderr)
        return 1
    day = args.date or date.today().strftime("%Y%m%d")
    if not re.match(r"^\d{8}$", day):
        print("--date 格式是 YYYYMMDD", file=sys.stderr)
        return 1
    path = repo.snap_dir / f"{day}-{args.slug}.md"
    if path.exists():
        print(f"已存在：{path}", file=sys.stderr)
        return 1
    commit = head_sha(repo.root) or "unknown"
    modules = changed_modules(repo.root, task.base) if task.base else []
    facts = args.fact or []
    keywords = args.keyword or [part for part in args.slug.split("-") if part]
    meta = [
        "---",
        f"task: {task.id}",
        f"title: {args.title}",
        f"date: {day[:4]}-{day[4:6]}-{day[6:]}",
        f"commit: {commit}",
        f"modules: [{', '.join(modules)}]",
        f"keywords: [{', '.join(keywords)}]",
        f"facts: [{', '.join(facts)}]",
        "status: draft",
        "---",
    ]
    body = [
        "",
        f"# {args.title}",
        "",
        "## 结论",
        "（待填）这一轮改变了什么判断？一句话。",
        "",
        "## 改动",
        "（待填）改了哪些文件，为什么是这些文件。",
        "",
        "## 证据",
        "（待填）测试/基准/账本 id；没有测量的数字不要写。",
        "",
        "## 遗留",
        "（待填）没做完的、以及下一步该谁接手。",
        "",
    ]
    repo.snap_dir.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(meta + body), encoding="utf-8", newline="\n")
    repo.write_index()
    print(f"已创建草稿 {path.relative_to(repo.root)}（commit {commit}）")
    print("填完后：python -m tools.context_pack seal")
    return 0


def _select_snapshot(repo: ContextRepo, args: argparse.Namespace) -> Optional[Snapshot]:
    snaps = repo.snapshots()
    if args.file:
        target = Path(args.file).name
        return next((s for s in snaps if s.name == target), None)
    task = repo.load_task()
    drafts = [s for s in snaps if s.task == task.id and s.status == "draft"]
    if len(drafts) == 1:
        return drafts[0]
    if len(drafts) > 1:
        raise ContextError(f"任务 {task.id} 有多份草稿，用 --file 指定")
    return None


def cmd_seal(args: argparse.Namespace) -> int:
    repo = ContextRepo(Path(args.root))
    try:
        snap = _select_snapshot(repo, args)
    except ContextError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if snap is None:
        print("没有可封存的草稿（先 new）", file=sys.stderr)
        return 1
    problems = []
    for name in SNAPSHOT_SECTIONS:
        if name not in snap.sections or not _meaningful(snap.sections[name]):
            problems.append(f"## {name} 为空")
    for placeholder in PLACEHOLDERS:
        if placeholder in snap.body:
            problems.append(f"仍有占位符 {placeholder}")
    if problems:
        for problem in problems:
            print(f"无法封存：{problem}", file=sys.stderr)
        return 1
    text = repo.read(snap.path)
    text = re.sub(r"(?m)^status:\s*draft\s*$", "status: sealed", text, count=1)
    snap.path.write_text(text, encoding="utf-8", newline="\n")
    repo.write_index()
    print(f"已封存 {snap.name}")
    return 0


def cmd_next(args: argparse.Namespace) -> int:
    repo = ContextRepo(Path(args.root))
    task = repo.load_task()
    to = str(args.to or "").strip() or "未命名任务"
    sealed = [s for s in repo.snapshots() if s.task == task.id and s.status == "sealed"]
    if not sealed:
        print(
            f"当前任务 {task.id} 还没有封存快照。先 new -> 填写 -> seal，再开下一个。\n"
            "（这条规则就是'任务完成必触发快照'：没有例外。）",
            file=sys.stderr,
        )
        return 1
    today = date.today().strftime("%Y%m%d")
    used = {s.task for s in repo.snapshots() if s.task.startswith(f"T-{today}-")}
    seq = len(used) + 1
    new_id = f"T-{today}-{seq}"
    base = head_sha(repo.root) or "unknown"
    accepts = args.accept or []
    if not accepts:
        print("至少给一条 --accept：没有验收标准的任务不算任务", file=sys.stderr)
        return 1
    nots = args.not_do or ["无"]
    body = [
        "---",
        f"id: {new_id}",
        "status: active",
        f"opened: {date.today().isoformat()}",
        f"base: {base}",
        "---",
        "# 当前任务",
        "",
        f"## 目标\n{to}",
        "",
        "## 验收\n" + "\n".join(f"- {item}" for item in accepts),
        "",
        "## 不做\n" + "\n".join(f"- {item}" for item in nots),
        "",
        "## 前置快照\n" + (", ".join(args.dep) if args.dep else "无"),
        "",
    ]
    repo.task_path.write_text("\n".join(body), encoding="utf-8", newline="\n")
    print(f"已开新任务 {new_id}（base {base}）")
    print("注意：验收项现在就要写清楚，check 不接受空验收。")
    return 0


def cmd_pack(args: argparse.Namespace) -> int:
    repo = ContextRepo(Path(args.root))
    if not repo.base_path.exists() or not repo.task_path.exists():
        print("缺少 base.md 或 task.md，先跑 check", file=sys.stderr)
        return 1
    task = repo.load_task()
    snaps = {s.name: s for s in repo.snapshots()}
    missing = [dep for dep in task.depends if dep not in snaps]
    if missing:
        print(f"前置快照不存在：{'、'.join(missing)}", file=sys.stderr)
        return 1
    parts = [
        "<!-- 交接包：base + task + 前置快照。按顺序读，读完不要去找别的历史。 -->",
        f"<!-- task={task.id} deps={len(task.depends)} -->",
        "",
        "<!-- ===== docs/context/base.md ===== -->",
        repo.read(repo.base_path).strip(),
        "",
        "<!-- ===== docs/context/task.md ===== -->",
        repo.read(repo.task_path).strip(),
    ]
    for dep in task.depends:
        parts += ["", f"<!-- ===== snapshots/{dep} ===== -->", snaps[dep].body.strip()]
    text = "\n".join(parts) + "\n"
    tokens = estimate_tokens(text)
    print(text)
    print(f"<!-- 估算 {tokens} token（上限 {HANDOFF_BUDGET}） -->", file=sys.stderr)
    if tokens > HANDOFF_BUDGET:
        print("交接包过大：只带 task 声明的依赖快照，不要整目录打包", file=sys.stderr)
        return 1
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """按关键词检索快照：标题/关键词/模块/正文，任一命中即可，按命中数排序。

    快照的整点就是"按需单份召回"，但没有检索入口就退化成"手翻目录"——
    这条命令把召回补上：记不清是哪份时，用词搜出候选，再只读命中的一份。
    """
    repo = ContextRepo(Path(args.root))
    snaps = repo.snapshots()
    terms = [t.strip().lower() for t in args.terms if t.strip()]
    if not terms:
        print("至少给一个检索词", file=sys.stderr)
        return 1
    if args.status:
        snaps = [s for s in snaps if s.status == args.status]

    scored: list[tuple[int, Snapshot]] = []
    for snap in snaps:
        hay = " ".join(
            [snap.name, snap.title, snap.task, snap.day, snap.commit]
            + snap.modules
            + snap.keywords
            + snap.facts
            + [snap.body]
        ).lower()
        score = sum(1 for term in terms if term in hay)
        if score:
            scored.append((score, snap))
    scored.sort(key=lambda item: (item[0], item[1].day, item[1].name), reverse=True)

    if not scored:
        print(f"没有快照命中：{'、'.join(args.terms)}")
        return 1
    for score, snap in scored:
        concl = snap.sections.get("结论", "").strip().splitlines()
        concl = concl[0] if concl else ""
        print(f"[{score}/{len(terms)}] {snap.name}  [{snap.status}]")
        print(f"  {snap.title}  ·  {snap.day}  ·  {snap.commit}")
        print(f"  模块 {', '.join(snap.modules) or '-'}  关键词 {', '.join(snap.keywords) or '-'}")
        if concl:
            print(f"  结论 {concl[:160]}")
        print()
    return 0


def cmd_budget(args: argparse.Namespace) -> int:
    if args.window <= 0:
        print("--window 必须是正数", file=sys.stderr)
        return 1
    ratio = args.used / args.window
    verdict = "ok"
    if ratio >= FORCE_RATIO:
        verdict = "seal"
    elif ratio >= WARN_RATIO:
        verdict = "warn"
    if args.json:
        print(json.dumps({"used": args.used, "window": args.window, "ratio": round(ratio, 4), "verdict": verdict}, ensure_ascii=False))
    else:
        print(f"上下文 {args.used}/{args.window} = {ratio:.0%}")
        if verdict == "seal":
            print("≥70%：先封存快照，再继续下一轮。")
        elif verdict == "warn":
            print("≥50%：注意收敛，别再把整段历史读进来。")
        else:
            print("正常。")
    return 2 if verdict == "seal" else 0


def cmd_status(args: argparse.Namespace) -> int:
    repo = ContextRepo(Path(args.root))
    base_text = repo.read(repo.base_path) if repo.base_path.exists() else ""
    task_text = repo.read(repo.task_path) if repo.task_path.exists() else ""
    resident = estimate_tokens(base_text) + estimate_tokens(task_text)
    print("LumenDB 上下文状态")
    print(f"  常驻    base+task ≈ {resident}/{RESIDENT_BUDGET} token")
    try:
        task = repo.load_task()
    except ContextError as exc:
        print(f"  任务    无法解析：{exc}")
        return 1
    print(f"  任务    {task.id} [{task.status}] base {task.base}")
    print(f"  依赖    {', '.join(task.depends) or '无'}")

    try:
        snaps = repo.snapshots()
    except ContextError as exc:
        print(f"  快照    无法解析：{exc}")
        return 1
    sealed = [s for s in snaps if s.status == "sealed"]
    drafts = [s for s in snaps if s.status == "draft"]
    print(f"  快照    {len(snaps)} 份（sealed {len(sealed)}，draft {len(drafts)}）")
    if sealed:
        latest = max(sealed, key=lambda s: (s.day, s.name))
        drift = commits_since(repo.root, latest.commit)
        tail = "无法计算" if drift is None else f"{drift} 个提交"
        print(f"  最近封存 {latest.name} @ {latest.commit}，之后 {tail}")
    if repo.index_path.exists():
        stale = repo.read(repo.index_path).rstrip("\n") != repo.render_index().rstrip("\n")
        print(f"  索引    {'过期（跑 index --write）' if stale else '与文件一致'}")
    else:
        print("  索引    缺失")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.context_pack",
        description="三层上下文：常驻 base、单任务 task、按需快照（索引与预算可校验）",
    )
    parser.add_argument("--root", default=str(REPO_ROOT), help="仓库根目录")
    sub = parser.add_subparsers(dest="command", required=True)

    check_cmd = sub.add_parser("check", help="校验常驻预算、任务结构、索引、快照证据")
    check_cmd.add_argument("--budget", type=int, default=RESIDENT_BUDGET)
    check_cmd.set_defaults(func=cmd_check)

    index_cmd = sub.add_parser("index", help="打印或重写快照索引")
    index_cmd.add_argument("--write", action="store_true")
    index_cmd.set_defaults(func=cmd_index)

    new_cmd = sub.add_parser("new", help="为当前任务创建快照草稿（自动填 commit/modules）")
    new_cmd.add_argument("--slug", required=True)
    new_cmd.add_argument("--title", required=True)
    new_cmd.add_argument("--date", default=None, help="YYYYMMDD，默认今天")
    new_cmd.add_argument("--fact", action="append", default=[], help="关联的账本 id，可重复")
    new_cmd.add_argument("--keyword", action="append", default=[], help="检索关键词，默认取 slug 分段")
    new_cmd.set_defaults(func=cmd_new)

    seal_cmd = sub.add_parser("seal", help="把草稿封存（拒绝占位符和空章节）")
    seal_cmd.add_argument("--file", default=None)
    seal_cmd.set_defaults(func=cmd_seal)

    next_cmd = sub.add_parser("next", help="封存后开下一个任务（未封存则拒绝）")
    next_cmd.add_argument("--to", required=True, help="下一个任务的目标")
    next_cmd.add_argument("--accept", action="append", default=[], help="验收项，可重复")
    next_cmd.add_argument("--not-do", action="append", default=[], help="明确不做的事，可重复")
    next_cmd.add_argument("--dep", action="append", default=[], help="前置快照文件名，可重复")
    next_cmd.set_defaults(func=cmd_next)

    pack_cmd = sub.add_parser("pack", help="输出交接包：base + task + 前置快照")
    pack_cmd.set_defaults(func=cmd_pack)

    query_cmd = sub.add_parser("query", help="按关键词检索快照（标题/关键词/模块/正文）")
    query_cmd.add_argument("terms", nargs="+", help="检索词，可多个；任一命中即入选，按命中数排序")
    query_cmd.add_argument("--status", default=None, choices=("draft", "sealed"), help="只查某种状态")
    query_cmd.set_defaults(func=cmd_query)

    budget_cmd = sub.add_parser("budget", help="按当前用量判断是否必须封存（≥70% 退出码 2）")
    budget_cmd.add_argument("--used", type=int, required=True)
    budget_cmd.add_argument("--window", type=int, default=200000)
    budget_cmd.add_argument("--json", action="store_true")
    budget_cmd.set_defaults(func=cmd_budget)

    status_cmd = sub.add_parser("status", help="常驻占用、任务、快照、索引")
    status_cmd.set_defaults(func=cmd_status)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
