"""LumenDB 项目记忆：带证据、可校验、可检索的长期事实账本。

为什么不是又一个 TODO markdown
------------------------------
长周期项目里最危险的不是"忘了做什么"，而是把"我记得的数字"当成事实。
所以这里的每条记录都必须挂**机器可核对的证据**：

* ``commit``  —— 这个提交还在不在（git rev-parse）
* ``file``    —— 文件还在不在，关键锚点（函数名/常量的字面串）还在不在
* ``test``    —— 覆盖它的测试文件还在不在；加 ``--run-tests`` 才真的跑
* ``command`` —— 复现命令，默认只记录、标记为 manual（不擅自执行外部命令）

``verify`` 会把每条证据重新核对一遍。这样"我记错了"和"记录本身过期了"
被同一个机制抓住，而不是靠人再读一遍。

用到的算法（都是标准库实现，确定性、可解释、离线可跑）
------------------------------------------------------
====================  ==========================================  ==========================
问题                  算法                                        为什么是它
====================  ==========================================  ==========================
按问题找相关记录      Okapi BM25 (k1=1.2, b=0.75)                 语料里全是标识符（commit
                                                                 哈希、文件路径、错误码），
                                                                 精确 token 匹配正是 BM25
                                                                 的强项；嵌入模型反而容易
                                                                 把 ``dcab1b6`` 稀释掉
旧事实不该压过新事实  指数半衰期（默认 120 天，0.5^(age/hl)）      "上个月测的"该排在"三月前
                                                                 测的"前面，但不该被彻底
                                                                 丢弃——原始 BM25 分仍然
                                                                 一起输出
写入时发现重复        SimHash(64bit) + Jaccard                    同一件事换个说法记两遍，
                                                                 以后一定会被当成两条证据
数字互相矛盾          单位归一 + 同主题数值比对（>25% 报警）        "0.13ms" 和 "0.47ms" 同时
                                                                 躺在账本里，早晚会被误引；
                                                                 宁可在写入时就吵起来
中文分词              字符 + 二元组（bigram），ASCII 按标识符切     无词典依赖，能匹配
                                                                 "解析缓存" / "缓存解析"
====================  ==========================================  ==========================

账本是 append-only 的：修正一条旧结论的正确做法是**追加一条新事实并写
``supersedes``**，而不是改旧行——历史可以被推翻，但不该被抹掉。
（``verify`` 只回写 ``verify`` 字段，不改 claim。）

用法见 ``docs/memory/README.md``；命令行入口是 ``python -m tools.project_memory``。
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = REPO_ROOT / "docs" / "memory" / "ledger.jsonl"

FACT_KINDS = (
    "decision",      # 做了什么取舍，为什么
    "measurement",   # 测出来的数字，怎么测的
    "bug",           # 修掉的真 bug，根因是什么
    "pitfall",       # 走过的死路：别再试
    "open_question", # 还没定的事，等谁拍板
    "commit",        # 由 git 自动灌入的提交记录
    "contract",      # 对使用者的承诺（语义/不变量）
    "external",      # 引用仓库外的知识（PG 语义、论文、规范）：必须带 source 出处
)

CONFIDENCE = ("verified", "measured", "inferred", "unverified")

# 外部事实的"新鲜度"门控：verified_at 超过这个天数没核对，视为需重新核对出处。
FRESHNESS_DAYS = 180

# 单位归一表：先匹配长的，避免 "ms" 被 "s" 抢走。
_UNIT_PATTERNS = (
    ("行/秒", "rows/s"),
    ("次/秒", "ops/s"),
    ("毫秒", "ms"),
    ("ms", "ms"),
    ("µs", "us"),
    ("us", "us"),
    ("GB", "GB"),
    ("MB", "MB"),
    ("KB", "KB"),
    ("倍", "x"),
    ("%", "%"),
    ("秒", "s"),
)
_NUM_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(" + "|".join(re.escape(p) for p, _ in _UNIT_PATTERNS) + r")"
)
_UNIT_ALIAS = dict(_UNIT_PATTERNS)


# ---------------------------------------------------------------------------
# 分词：ASCII 标识符整体 + 按分隔符拆开，中文按 字 + 二元组
# ---------------------------------------------------------------------------

_ASCII_RE = re.compile(r"[A-Za-z0-9_./\\:@#$%^&*+=~-]+")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]+")
_SPLIT_RE = re.compile(r"[^A-Za-z0-9]+")


def tokenize(text: str) -> list[str]:
    """把一段文本切成检索 token。

    中文用「单字 + 二元组」是为了不依赖词典也能匹配语序变化（"解析缓存"
    和"缓存解析"共享 ``解析``/``缓存`` 之外的 ``析缓`` 之类噪声，但二元组
    的召回收益远大于噪声）。ASCII 标识符既保留整体（``btree._scan_range``）
    也保留拆开的片段（``btree``/``scan``/``range``），因为人既会整串引用，
    也会只写后半截。
    """
    tokens: list[str] = []
    for match in _ASCII_RE.finditer(text):
        raw = match.group(0)
        low = raw.lower().strip("._-/:")
        if not low:
            continue
        tokens.append(low)
        if len(low) > 1:
            for part in _SPLIT_RE.split(low):
                if len(part) > 1:
                    tokens.append(part)
    for match in _CJK_RE.finditer(text):
        run = match.group(0)
        tokens.extend(run)
        tokens.extend(run[i : i + 2] for i in range(len(run) - 1))
    return tokens


def _tokens(text: str) -> set[str]:
    return set(tokenize(text))


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ---------------------------------------------------------------------------
# SimHash：用于"同一件事记了两遍"的近似重复检测
# ---------------------------------------------------------------------------

_HASH_BITS = 64


def simhash(text: str) -> int:
    """64 位 SimHash。按 token 频次加权投票每一位的符号。"""
    vector = [0] * _HASH_BITS
    for token in tokenize(text):
        h = int(hashlib.blake2b(token.encode("utf-8"), digest_size=8).hexdigest(), 16)
        for bit in range(_HASH_BITS):
            vector[bit] += 1 if (h >> bit) & 1 else -1
    out = 0
    for bit in range(_HASH_BITS):
        if vector[bit] > 0:
            out |= 1 << bit
    return out


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


# ---------------------------------------------------------------------------
# 数字抽取与单位归一（数字矛盾检测的基础）
# ---------------------------------------------------------------------------


def extract_metrics(text: str) -> list[tuple[float, str]]:
    """抽出 ``(数值, 归一单位)``。只认带单位的数字，避免把行号/提交哈希当指标。"""
    out: list[tuple[float, str]] = []
    for raw_value, raw_unit in _NUM_RE.findall(text):
        unit = _UNIT_ALIAS.get(raw_unit, raw_unit)
        try:
            value = float(raw_value)
        except ValueError:
            continue
        out.append((value, unit))
    return out


_UNIT_WORDS = {
    "ms",
    "us",
    "s",
    "x",
    "%",
    "gb",
    "mb",
    "kb",
    "rows",
    "ops",
    "倍",
    "秒",
    "毫秒",
    "行",
    "次",
}


def _topic_tokens(text: str) -> set[str]:
    """去掉数字与单位后的主题 token——判"是不是在说同一件事"。

    关键在于**把数值剥离出去**：``0.13ms`` 和 ``0.47ms`` 的主题是"索引点查
    延迟"，不是"0.13 与 0.47"。否则两个数字自己会把 Jaccard 拉低，冲突检测
    永远不触发。
    """
    keep: set[str] = set()
    for token in tokenize(text):
        if token and token[0].isdigit():
            continue
        if token.lower() in _UNIT_WORDS:
            continue
        if len(token) == 1 and "\u4e00" <= token <= "\u9fff":
            # 单字太泛（"的""是"全在里面），二元组已经携带了信息
            continue
        keep.add(token)
    return keep


# ---------------------------------------------------------------------------
# Fact / Ledger
# ---------------------------------------------------------------------------


@dataclass
class Fact:
    id: str
    kind: str
    claim: str
    created: str
    confidence: str = "measured"
    tags: list[str] = field(default_factory=list)
    evidence: list[dict] = field(default_factory=list)
    supersedes: Optional[str] = None
    verify: dict = field(default_factory=dict)
    source: str = ""
    verified_at: str = ""

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Fact":
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    def search_text(self) -> str:
        parts = [self.claim, self.kind, " ".join(self.tags), self.source]
        parts.extend(str(e.get("value", "")) for e in self.evidence)
        return " ".join(parts)


def _now_iso() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def _parse_when(value: str) -> datetime:
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return datetime.now().astimezone()
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return stamp


class Ledger:
    def __init__(self, path: Path = DEFAULT_LEDGER, root: Path = REPO_ROOT) -> None:
        self.path = Path(path)
        self.root = Path(root)
        self.facts: list[Fact] = []
        self.load()

    # -- IO ---------------------------------------------------------------
    def load(self) -> None:
        self.facts = []
        if not self.path.exists():
            return
        for line_no, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(), 1
        ):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                self.facts.append(Fact.from_dict(json.loads(line)))
            except (json.JSONDecodeError, TypeError) as exc:
                raise ValueError(f"{self.path}:{line_no} 不是合法的账本行：{exc}") from exc

    def append(self, fact: Fact) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(fact.to_dict(), ensure_ascii=False) + "\n")
        self.facts.append(fact)

    def rewrite(self) -> None:
        """回写整个账本（只用于更新 verify 字段；claim 本身不该改）。"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8", newline="\n") as handle:
            for fact in self.facts:
                handle.write(json.dumps(fact.to_dict(), ensure_ascii=False) + "\n")
        os.replace(tmp, self.path)

    def get(self, fact_id: str) -> Optional[Fact]:
        for fact in self.facts:
            if fact.id == fact_id:
                return fact
        return None

    def new_id(self, claim: str) -> str:
        digest = hashlib.sha1(f"{claim}|{_now_iso()}".encode("utf-8")).hexdigest()[:6]
        candidate = f"f-{datetime.now().strftime('%Y%m%d')}-{digest}"
        existing = {f.id for f in self.facts}
        suffix = 1
        while candidate in existing:
            candidate = f"f-{datetime.now().strftime('%Y%m%d')}-{digest}{suffix}"
            suffix += 1
        return candidate

    # -- 写入期检查 -------------------------------------------------------
    def near_duplicates(self, fact: Fact, threshold: float = 0.8) -> list[Fact]:
        mine = _tokens(fact.search_text())
        mine_hash = simhash(fact.search_text())
        hits = []
        for other in self.facts:
            if other.id == fact.id:
                continue
            theirs = _tokens(other.search_text())
            if jaccard(mine, theirs) >= threshold:
                hits.append(other)
            elif hamming(mine_hash, simhash(other.search_text())) <= 3:
                hits.append(other)
        return hits

    def number_conflicts(self, fact: Fact, rel_tolerance: float = 0.25) -> list[tuple]:
        """同一主题、同一单位、数值差 > 25% —— 很可能是旧测量没被取代。"""
        mine = extract_metrics(fact.claim)
        if not mine:
            return []
        my_topic = _topic_tokens(fact.claim)
        findings = []
        for other in self.facts:
            if other.id == fact.id:
                # 一条 claim 里常有"从 A 降到 B"的前后两个数字，它们不是
                # 自相矛盾，只是同一测量 before/after；跳过与自身的比较。
                continue
            if fact.supersedes and other.id == fact.supersedes:
                # 本条就是来取代它的，旧值不该再报冲突。
                continue
            if other.supersedes and other.supersedes == fact.id:
                # 反方向：本条已被对方取代，也不该拿旧值去吵新值。
                continue
            theirs = extract_metrics(other.claim)
            if not theirs:
                continue
            topic = _topic_tokens(other.claim)
            if jaccard(my_topic, topic) < 0.5:
                continue
            for my_value, my_unit in mine:
                for their_value, their_unit in theirs:
                    if my_unit != their_unit:
                        continue
                    base = max(abs(my_value), abs(their_value), 1e-9)
                    if abs(my_value - their_value) / base > rel_tolerance:
                        findings.append((other, my_value, my_unit, their_value))
        return findings


# ---------------------------------------------------------------------------
# BM25 检索 + 时间衰减
# ---------------------------------------------------------------------------


class BM25:
    def __init__(
        self, docs: Sequence[Sequence[str]], k1: float = 1.2, b: float = 0.75
    ) -> None:
        self.k1 = k1
        self.b = b
        self.docs = [list(d) for d in docs]
        self.n = len(self.docs)
        self.avgdl = (sum(len(d) for d in self.docs) / self.n) if self.n else 0.0
        self.df: dict[str, int] = {}
        for doc in self.docs:
            for token in set(doc):
                self.df[token] = self.df.get(token, 0) + 1

    def idf(self, token: str) -> float:
        freq = self.df.get(token, 0)
        if self.n == 0:
            return 0.0
        return math.log(1 + (self.n - freq + 0.5) / (freq + 0.5))

    def score(self, query: Sequence[str]) -> list[float]:
        scores = [0.0] * self.n
        if not self.n or self.avgdl <= 0:
            return scores
        for token in query:
            idf = self.idf(token)
            if idf <= 0:
                continue
            for index, doc in enumerate(self.docs):
                tf = doc.count(token)
                if not tf:
                    continue
                denom = tf + self.k1 * (1 - self.b + self.b * len(doc) / self.avgdl)
                scores[index] += idf * tf * (self.k1 + 1) / denom
        return scores


def search(
    facts: Sequence[Fact],
    query: str,
    *,
    top_k: int = 5,
    half_life_days: float = 120.0,
    now: Optional[datetime] = None,
    kind: Optional[str] = None,
    tag: Optional[str] = None,
) -> list[dict]:
    """返回排好序的命中：BM25 × 时间衰减，但两个分数都原样给出。"""
    now = now or datetime.now().astimezone()
    selected = [
        f
        for f in facts
        if (kind is None or f.kind == kind) and (tag is None or tag in f.tags)
    ]
    if not selected:
        return []
    engine = BM25([tokenize(f.search_text()) for f in selected])
    raw = engine.score(tokenize(query))
    results = []
    for fact, base in zip(selected, raw):
        age_days = max((now - _parse_when(fact.created)).total_seconds() / 86400.0, 0.0)
        decay = 0.5 ** (age_days / half_life_days) if half_life_days > 0 else 1.0
        results.append(
            {
                "fact": fact,
                "bm25": base,
                "decay": decay,
                "score": base * decay,
                "age_days": age_days,
            }
        )
    results.sort(key=lambda item: (item["score"], item["fact"].created), reverse=True)
    return [r for r in results if r["bm25"] > 0][:top_k]


# ---------------------------------------------------------------------------
# 证据校验
# ---------------------------------------------------------------------------

GIT_CANDIDATES = (
    os.environ.get("LUMENDB_GIT_EXE", ""),
    r"D:\git\Git\cmd\git.exe",
    "/usr/bin/git",
)


def find_git() -> Optional[str]:
    found = shutil.which("git")
    if found:
        return found
    for candidate in GIT_CANDIDATES:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def _git(args: Sequence[str], root: Path) -> subprocess.CompletedProcess:
    exe = find_git()
    if not exe:
        raise RuntimeError("找不到 git 可执行文件")
    return subprocess.run(
        [exe, "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def verify_fact(
    fact: Fact,
    *,
    root: Path = REPO_ROOT,
    run_tests: bool = False,
    run_commands: bool = False,
    timeout: float = 300.0,
) -> tuple[str, list[dict]]:
    """逐条核对证据，返回 (聚合状态, 每条证据的核对结果)。

    聚合规则：任何一条 ``failed`` → ``failed``；否则有 ``unchecked``/``manual``
    → ``partial``；全部 ``ok`` → ``ok``。unknown 证据类型一律 ``failed``，
    因为无法核对的证据不该被当成证据。
    """
    details: list[dict] = []
    for item in fact.evidence:
        kind = str(item.get("type", ""))
        value = str(item.get("value", ""))
        anchor = str(item.get("anchor", "") or "")
        status, detail = "ok", ""
        try:
            if kind == "commit":
                proc = _git(["rev-parse", "--verify", "--quiet", f"{value}^{{commit}}"], root)
                if proc.returncode == 0:
                    detail = f"commit {proc.stdout.strip()[:12]} 存在"
                else:
                    status, detail = "failed", "commit 不存在（可能被 rebase/删分支）"
            elif kind == "file":
                path = root / value
                if not path.exists():
                    status, detail = "failed", f"文件不存在：{value}"
                elif anchor:
                    text = path.read_text(encoding="utf-8", errors="replace")
                    if anchor in text:
                        detail = f"锚点命中：{anchor}"
                    else:
                        status, detail = "failed", f"文件在，但找不到锚点：{anchor}"
                else:
                    detail = "文件存在"
            elif kind == "test":
                path = root / value
                if not path.exists():
                    status, detail = "failed", f"测试文件不存在：{value}"
                elif run_tests:
                    proc = subprocess.run(
                        [sys.executable, "-m", "pytest", value, "-q"],
                        cwd=str(root),
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=timeout,
                    )
                    if proc.returncode == 0:
                        tail = (proc.stdout or "").strip().splitlines()[-1:]
                        detail = f"测试通过：{tail[0] if tail else 'ok'}"
                    else:
                        status, detail = "failed", "测试未通过"
                else:
                    status, detail = "unchecked", "未跑（加 --run-tests 才执行）"
            elif kind == "command":
                if run_commands:
                    status, detail = "manual", "已记录；自动执行复现命令不默认开启"
                else:
                    status, detail = "manual", "复现命令，需人工执行"
            else:
                status, detail = "failed", f"未知证据类型：{kind}"
        except subprocess.TimeoutExpired:
            status, detail = "failed", "校验超时"
        except (OSError, RuntimeError) as exc:
            status, detail = "failed", f"校验失败：{exc}"
        details.append({"type": kind, "value": value, "anchor": anchor, "status": status, "detail": detail})

    statuses = {d["status"] for d in details}
    if "failed" in statuses:
        aggregate = "failed"
    elif not details:
        aggregate = "unverified"
    elif statuses <= {"ok"}:
        aggregate = "ok"
    else:
        aggregate = "partial"
    return aggregate, details


def contract_findings(fact: Fact, now: Optional[datetime] = None) -> list[dict]:
    """外部事实的知识契约检查：source 非空、verified_at 不在未来、180 天新鲜度。

    内部事实（decision/measurement/...）的证据在仓库里，靠锚点核对；外部事实
    （"PG 的 X 是这样"）的证据在仓库外，没法跑锚点，只能靠"带出处 + 记录核对
    时间 + 过期重查"。这条契约就是让"编造的外部知识"在落账时被机器拦住。
    """
    if fact.kind != "external":
        return []
    now = now or datetime.now().astimezone()
    out: list[dict] = []
    if not fact.source.strip():
        out.append(
            {
                "type": "contract",
                "value": "source",
                "anchor": "",
                "status": "failed",
                "detail": "外部事实没有 source 出处（编造的外部知识最危险）",
            }
        )
    verified = fact.verified_at.strip()
    if not verified:
        out.append(
            {
                "type": "contract",
                "value": "verified_at",
                "anchor": "",
                "status": "failed",
                "detail": "外部事实没有 verified_at 核对时间",
            }
        )
    else:
        try:
            stamp = datetime.fromisoformat(verified)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=now.tzinfo)
            if stamp > now:
                out.append(
                    {
                        "type": "contract",
                        "value": "verified_at",
                        "anchor": "",
                        "status": "failed",
                        "detail": "verified_at 在未来（核对时间不能晚于现在）",
                    }
                )
            elif (now - stamp).days > FRESHNESS_DAYS:
                out.append(
                    {
                        "type": "contract",
                        "value": "verified_at",
                        "anchor": "",
                        "status": "partial",
                        "detail": f"超过 {FRESHNESS_DAYS} 天未核对出处，引用前需重查",
                    }
                )
        except ValueError:
            out.append(
                {
                    "type": "contract",
                    "value": "verified_at",
                    "anchor": "",
                    "status": "failed",
                    "detail": "verified_at 不是合法 ISO 时间",
                }
            )
    return out


# ---------------------------------------------------------------------------
# 从 git 灌入（机器生成的锚点，最不容易出错的那类记录）
# ---------------------------------------------------------------------------

_PREFIX_TAGS = {
    "perf": ["perf"],
    "fix": ["fix"],
    "feat": ["feat"],
    "docs": ["docs"],
    "test": ["test"],
    "refactor": ["refactor"],
    "bench": ["bench"],
}


def facts_from_git(
    root: Path = REPO_ROOT, rev_range: str = "-20", ledger: Optional[Ledger] = None
) -> list[Fact]:
    proc = _git(["log", rev_range, "--format=%H%x1f%ad%x1f%s", "--date=short"], root)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git log 失败")
    known = {e.get("value") for f in (ledger.facts if ledger else []) for e in f.evidence}
    out: list[Fact] = []
    for line in proc.stdout.splitlines():
        parts = line.split("\x1f")
        if len(parts) != 3:
            continue
        commit_hash, day, subject = parts
        if commit_hash in known:
            continue
        tags: list[str] = []
        match = re.match(r"([a-z]+)(?:\((.*?)\))?", subject)
        if match:
            tags.extend(_PREFIX_TAGS.get(match.group(1), [match.group(1)]))
            if match.group(2):
                tags.extend(re.split(r"[,/ ]+", match.group(2)))
        out.append(
            Fact(
                id=f"g-{commit_hash[:7]}",
                kind="commit",
                claim=subject,
                created=f"{day}T00:00:00",
                confidence="verified",
                tags=[t for t in tags if t][:6],
                evidence=[{"type": "commit", "value": commit_hash, "anchor": ""}],
            )
        )
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_evidence(raw: str) -> dict:
    parts = raw.split(":", 2)
    if len(parts) < 2:
        raise argparse.ArgumentTypeError("证据格式是 TYPE:VALUE[:ANCHOR]")
    return {"type": parts[0], "value": parts[1], "anchor": parts[2] if len(parts) > 2 else ""}


def _print_fact(fact: Fact, prefix: str = "") -> None:
    status = fact.verify.get("status", "-") if fact.verify else "-"
    print(f"{prefix}{fact.id}  [{fact.kind}] {fact.claim}")
    print(f"    conf={fact.confidence} verify={status} tags={','.join(fact.tags) or '-'}")
    if fact.source or fact.verified_at:
        print(f"    source={fact.source or '-'}  verified_at={fact.verified_at or '-'}")
    for item in fact.evidence:
        anchor = f" :: {item['anchor']}" if item.get("anchor") else ""
        print(f"    · {item['type']}:{item['value']}{anchor}")


def cmd_add(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    fact = Fact(
        id=ledger.new_id(args.claim),
        kind=args.kind,
        claim=args.claim,
        created=_now_iso(),
        confidence=args.confidence,
        tags=[t.strip() for t in (args.tags or "").split(",") if t.strip()],
        evidence=[
            _parse_evidence(e) if isinstance(e, str) else e
            for e in (args.evidence or [])
        ],
        supersedes=args.supersedes,
        source=(args.source or "").strip(),
        verified_at=args.verified_at or _now_iso(),
    )
    if fact.kind not in FACT_KINDS:
        print(f"kind 必须是 {FACT_KINDS} 之一", file=sys.stderr)
        return 2
    if fact.kind == "external" and not fact.source:
        print("外部事实必须带 --source 出处（URL/文档/规范），否则无法核对真伪", file=sys.stderr)
        return 2
    if not fact.evidence:
        print("警告：没有证据的记录以后无法校验（建议至少给一条 file/commit/test）", file=sys.stderr)

    for other in ledger.number_conflicts(fact):
        print(f"⚠ 数字冲突：{other[0].id} 说 {other[3]}{other[2]}，本条说 {other[1]}{other[2]}")

    ledger.append(fact)
    print(f"已记录 {fact.id}")
    _print_fact(fact)
    dups = ledger.near_duplicates(fact)
    if dups:
        print("⚠ 疑似重复（换个说法记了同一件事？）：")
        for other in dups:
            print(f"    {other.id}  {other.claim}")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    hits = search(
        ledger.facts,
        args.text,
        top_k=args.top_k,
        half_life_days=args.half_life,
        kind=args.kind,
        tag=args.tag,
    )
    if not hits:
        print("（没有命中）")
        return 1
    for item in hits:
        fact = item["fact"]
        age = item["age_days"]
        print(
            f"{item['score']:7.3f}  bm25={item['bm25']:6.3f} decay={item['decay']:.2f} "
            f"age={age:5.1f}d  {fact.id} [{fact.kind}] {fact.claim}"
        )
        for evidence in fact.evidence:
            print(f"            ↳ {evidence['type']}:{evidence['value']}")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    targets = ledger.facts if not args.ids else [ledger.get(i) for i in args.ids]
    targets = [t for t in targets if t is not None]
    if args.only_stale:
        targets = [t for t in targets if t.verify.get("status") not in ("ok",)]
    if not targets:
        print("没有需要校验的记录")
        return 0
    failures = 0
    for fact in targets:
        status, details = verify_fact(
            fact, root=Path(args.root), run_tests=args.run_tests, run_commands=args.run_commands
        )
        # 外部事实额外套一层"知识契约"：出处 + 核对时间 + 新鲜度
        details = details + contract_findings(fact)
        statuses = {d["status"] for d in details}
        if "failed" in statuses:
            status = "failed"
        elif not details:
            status = "unverified"
        elif statuses <= {"ok"}:
            status = "ok"
        else:
            status = "partial"
        fact.verify = {"status": status, "checked_at": _now_iso(), "detail": details}
        print(f"{status:8s} {fact.id}  {fact.claim[:60]}")
        for detail in details:
            if detail["status"] != "ok":
                print(f"         - {detail['type']}:{detail['value']} → {detail['status']}（{detail['detail']}）")
        if status == "failed":
            failures += 1
    ledger.rewrite()
    print(f"\n校验完成：{len(targets)} 条，失败 {failures} 条")
    return 1 if failures else 0


def cmd_ingest_git(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    fresh = facts_from_git(Path(args.root), args.rev_range, ledger)
    for fact in fresh:
        ledger.append(fact)
    print(f"从 git 灌入 {len(fresh)} 条提交记录（范围 {args.rev_range}）")
    return 0


def cmd_dups(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    found = 0
    for i, fact in enumerate(ledger.facts):
        mine = _tokens(fact.search_text())
        for other in ledger.facts[i + 1 :]:
            theirs = _tokens(other.search_text())
            if jaccard(mine, theirs) >= 0.75:
                print(f"{fact.id} ≈ {other.id}  jaccard={jaccard(mine, theirs):.2f}")
                print(f"    A: {fact.claim}")
                print(f"    B: {other.claim}")
                found += 1
    print(f"共 {found} 对疑似重复")
    return 0 if found == 0 else 1


def cmd_conflicts(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    superseded = _superseded_ids(ledger.facts)
    active = [f for f in ledger.facts if f.id not in superseded]
    found = 0
    for fact in active:
        for other, mine, unit, theirs in ledger.number_conflicts(fact):
            if other.id in superseded:
                continue
            if other.id >= fact.id:
                continue
            print(f"{fact.id} ({mine}{unit}) vs {other.id} ({theirs}{unit})")
            print(f"    A: {fact.claim}")
            print(f"    B: {other.claim}")
            found += 1
    print(f"共 {found} 处数字冲突")
    return 0 if found == 0 else 1


_TEST_RUN_RE = re.compile(r"(\d+)\s*passed")


def _git_brief(root: Path) -> dict:
    """尽量取出 git 状态；没有 git 就返回占位符，不影响 status 其余部分。"""
    info = {"head": "-", "subject": "-", "branch": "-", "ahead": "未知"}
    if find_git() is None:
        return info
    try:
        head = _git(["rev-parse", "--short", "HEAD"], root)
        if head.returncode != 0:
            return info
        info["head"] = head.stdout.strip()
        info["subject"] = _git(["log", "-1", "--format=%s"], root).stdout.strip()
        info["branch"] = _git(["rev-parse", "--abbrev-ref", "HEAD"], root).stdout.strip()
        upstream = _git(["rev-list", "--count", "@{upstream}..HEAD"], root)
        if upstream.returncode == 0:
            info["ahead"] = f"领先 upstream {upstream.stdout.strip()} 个提交"
        else:
            info["ahead"] = "未配置 upstream（提交只在本地）"
    except RuntimeError:
        pass
    return info


def _latest_test_run(facts: Sequence[Fact]) -> Optional[Fact]:
    """找最近一条带 ``N passed`` 的测试记录（tags 含 tests/ci）。"""
    best: Optional[Fact] = None
    for fact in facts:
        if "tests" not in fact.tags and "ci" not in fact.tags:
            continue
        if not _TEST_RUN_RE.search(fact.claim):
            continue
        if best is None or fact.created > best.created:
            best = fact
    return best


def cmd_status(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    info = _git_brief(Path(args.root))
    print("LumenDB 项目状态")
    print(f"  HEAD    {info['head']}  {info['subject']}")
    print(f"  分支    {info['branch']}  {info['ahead']}")

    total = len(ledger.facts)
    by_kind: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for fact in ledger.facts:
        by_kind[fact.kind] = by_kind.get(fact.kind, 0) + 1
        status = fact.verify.get("status", "unverified")
        by_status[status] = by_status.get(status, 0) + 1
    kinds = "，".join(f"{k} {by_kind[k]}" for k in FACT_KINDS if by_kind.get(k))
    print(f"  账本    {total} 条" + (f"：{kinds}" if kinds else ""))
    checks = "，".join(f"{k}={v}" for k, v in sorted(by_status.items()))
    print(f"  校验    {checks or '未校验'}")

    test_fact = _latest_test_run(ledger.facts)
    if test_fact:
        commits = [
            str(e.get("value", ""))[:7]
            for e in test_fact.evidence
            if e.get("type") == "commit"
        ]
        at = f"  @ {commits[0]}" if commits else ""
        print(f"  最近测试 {test_fact.claim}{at}  （{test_fact.created[:10]}）")

    questions = [
        f
        for f in ledger.facts
        if f.kind == "open_question"
        and f.id not in {x.supersedes for x in ledger.facts if x.supersedes}
    ]
    if questions:
        print(f"  未决问题（{len(questions)}）")
        for fact in questions[:6]:
            print(f"    - {fact.claim}")

    failed = [f for f in ledger.facts if f.verify.get("status") == "failed"]
    if failed:
        print(f"  待处理：{len(failed)} 条校验失败")
        for fact in failed[:5]:
            print(f"    ! {fact.id}  {fact.claim[:56]}")
    no_evidence = [f for f in ledger.facts if not f.evidence]
    if no_evidence:
        print(f"  待处理：{len(no_evidence)} 条没有证据")
    return 0


def _superseded_ids(facts: Sequence["Fact"]) -> set[str]:
    """所有已被其它记录取代的 id（出现在任一条 ``supersedes`` 字段里的 id）。"""
    return {f.supersedes for f in facts if f.supersedes}


def _iter_number_conflicts(ledger: "Ledger") -> list[tuple]:
    """跨全账本找出"同主题、同单位、数值差 >25%"的冲突对（去重、去被替代）。

    只拿仍有效的记录两两比较：已经被第三条记录取代的旧值，不该再和谁吵。
    """
    superseded = _superseded_ids(ledger.facts)
    active = [f for f in ledger.facts if f.id not in superseded]
    found: list[tuple] = []
    seen: set[frozenset] = set()
    for fact in active:
        for other, mine, unit, theirs in ledger.number_conflicts(fact):
            if other.id in superseded:
                continue
            pair = frozenset((fact.id, other.id))
            if pair in seen:
                continue
            seen.add(pair)
            found.append((fact.id, other.id, mine, theirs, unit))
    return found


def cmd_digest(args: argparse.Namespace) -> int:
    """续作交接视图：未决问题 + 需要处理 + 数字冲突 + 各主题最新结论。

    和 ``status`` 的区别：status 回答"现在在哪、还剩什么"；digest 回答
    "接手的人该先信哪几条、先处理哪几条"。它把四个最容易被忽视的信号
    （未决问题、校验失败/无证据、数字互相矛盾、每个 kind 最新的几条）
    压在一页里，是 agent 之间或隔段时间回来时的快速对齐入口。
    """
    ledger = Ledger(Path(args.ledger), Path(args.root))
    info = _git_brief(Path(args.root))
    facts = ledger.facts
    superseded = {f.supersedes for f in facts if f.supersedes}
    active = [f for f in facts if f.id not in superseded]

    print("LumenDB 续作交接")
    print(f"  HEAD    {info['head']}  {info['subject']}")
    print(f"  账本    {len(facts)} 条（{len(active)} 条仍有效）")

    questions = [f for f in active if f.kind == "open_question"]
    if questions:
        print("\n【未决问题】")
        for f in questions:
            print(f"  ? {f.claim}  ({f.id})")

    failed = [f for f in active if f.verify.get("status") == "failed"]
    no_evidence = [f for f in active if not f.evidence]
    if failed or no_evidence:
        print("\n【需要处理】")
        for f in failed:
            print(f"  ! 校验失败  {f.id}  {f.claim[:64]}")
        for f in no_evidence:
            print(f"  ! 无证据    {f.id}  {f.claim[:64]}")

    conflicts = _iter_number_conflicts(ledger)
    if conflicts:
        print(f"\n【数字冲突（同主题同单位差>25%）】共 {len(conflicts)} 处")
        for fact_id, other_id, mine, theirs, unit in conflicts[:8]:
            print(f"  ~ {fact_id} ({mine}{unit})  vs  {other_id} ({theirs}{unit})")

    print("\n【最新结论（每类取最近 3 条，被替代的已隐藏）】")
    for kind in ("measurement", "decision", "bug", "contract"):
        items = sorted(
            (f for f in active if f.kind == kind),
            key=lambda f: f.created,
            reverse=True,
        )[:3]
        if not items:
            continue
        print(f"  [{kind}]")
        for f in items:
            print(f"    {f.created[:10]}  {f.claim[:80]}  ({f.id})")
    return 0


def _readme_checklist(root: Path) -> tuple[list[str], list[str]]:
    """解析 README「## 路线图」节的 ``- [x]`` / ``- [ ]`` 清单。

    返回 ``(done, open)``。只认那个小节里的勾选行，避免把正文里的其他
    ``- [ ]`` 语法（如果将来出现）也算进去。
    """
    readme = root / "README.md"
    if not readme.exists():
        return [], []
    done: list[str] = []
    open_: list[str] = []
    in_section = False
    for line in readme.read_text(encoding="utf-8").splitlines():
        if line.strip() == "## 路线图":
            in_section = True
            continue
        if in_section and line.startswith("## ") and line.strip() != "## 路线图":
            break
        if not in_section:
            continue
        stripped = line.strip()
        if stripped.startswith("- [x]") or stripped.startswith("- [X]"):
            done.append(stripped[5:].strip())
        elif stripped.startswith("- [ ]"):
            open_.append(stripped[5:].strip())
    return done, open_


def cmd_roadmap(args: argparse.Namespace) -> int:
    """把 README「路线图」清单和账本证据拼成一张"还剩什么"的表。

    路线图本身写在 markdown 里，是唯一的完成状态来源；这个命令把它变成
    程序可查询、可对账的东西：每一条未完成项都试着在账本里找最近的
    相关记录，标出"上次讨论是什么时候、有没有证据"，避免接手的人
    只看到一句待办、不知道它已经走到哪一步。
    """
    ledger = Ledger(Path(args.ledger), Path(args.root))
    root = Path(args.root)
    done, open_ = _readme_checklist(root)

    print("LumenDB 路线图（README「路线图」× 账本）")
    print(f"  完成 {len(done)}，未完成 {len(open_)}")

    if not open_:
        print("  （没有未完成项）")
    else:
        print("\n【未完成项】")
        discussable = [f for f in ledger.facts if f.kind != "commit"]
        superseded = {f.supersedes for f in ledger.facts if f.supersedes}
        for item in open_:
            hits = search(
                [f for f in discussable if f.id not in superseded],
                item,
                top_k=1,
                half_life_days=10**6,
            )
            print(f"  - {item}")
            if hits:
                f = hits[0]["fact"]
                print(f"      最近相关 {f.id}  {f.created[:10]}  {f.claim[:64]}")
            else:
                print("      账本里没有相关记录")

    questions = [
        f
        for f in ledger.facts
        if f.kind == "open_question"
        and f.id not in {x.supersedes for x in ledger.facts if x.supersedes}
    ]
    if questions:
        print("\n【阻塞项（open_question）】")
        for f in questions:
            print(f"  ? {f.claim}")
    return 0


def cmd_drift(args: argparse.Namespace) -> int:
    """文档漂移对账：把活文档里写的 ``N passed`` 测试数拿去和账本最新一次
    测试结果对比，揪出"更新了账本却忘了改文档"的旧数字。

    只扫 README 和 ``docs/`` 下的活文档；文件名含 ``milestone`` 的是冻结的
    历史快照（如外键里程碑里写的 959 passed），本来就不该跟着最新数字走，
    跳过。改完文档后重跑本命令应归零——这是防幻觉最实用的一道程序闸。
    """
    ledger = Ledger(Path(args.ledger), Path(args.root))
    root = Path(args.root)
    latest = _latest_test_run(ledger.facts)
    if latest is None:
        print("账本里没有带 N passed 的测试记录，无法对账")
        return 0
    want = int(_TEST_RUN_RE.search(latest.claim).group(1))

    docs = [root / "README.md"] + sorted((root / "docs").glob("*.md"))
    drift: list[tuple[str, int, int]] = []
    for p in docs:
        if not p.exists() or "milestone" in p.name:
            continue
        for lineno, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            for mm in _TEST_RUN_RE.finditer(line):
                got = int(mm.group(1))
                if got != want:
                    drift.append((str(p.relative_to(root)), lineno, got))

    if not drift:
        print(f"文档测试数一致：全部 {want} passed")
        return 0
    print(f"文档测试数漂移（账本最新 {want} passed，改完重跑本命令应归零）：")
    for doc, lineno, got in drift:
        print(f"  {doc}:{lineno}  写 {got} passed")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    """开场先看这个：按优先级排出"现在该先做哪件"。

    四个优先级：
      P0 需要你拍板（open_question，没解决就推进不下去）；
      P1 需要修（账本自身出错：校验失败 / 没有证据）；
      P2 需要决策（同主题同单位的数字互相矛盾，先定哪个是当前值）；
      P3 可推进的实现工作（README 未完成项，按"失修天数"降序——最久
         没人碰的排最前，避免被新热闹淹没）。

    它和 digest/roadmap 的分工：digest 给"最近发生了什么"，roadmap 给
    "还剩什么"，plan 给"按优先级先做什么"。三个合起来就是每轮开场不看
    笔记、不凭印象的完整对齐。
    """
    ledger = Ledger(Path(args.ledger), Path(args.root))
    root = Path(args.root)
    now = datetime.now().astimezone()

    facts = ledger.facts
    superseded = {f.supersedes for f in facts if f.supersedes}
    active = [f for f in facts if f.id not in superseded]
    discussable = [f for f in active if f.kind != "commit"]

    print("LumenDB 行动清单（按优先级）")

    # P0：需要用户拍板
    questions = [f for f in active if f.kind == "open_question"]
    if questions:
        print("\n[P0] 需要你拍板")
        for f in sorted(questions, key=lambda f: f.created):
            days = (now - _parse_when(f.created)).days
            print(f"  ? {f.claim}  （{days} 天前）")
    else:
        print("\n[P0] 需要你拍板：无")

    # P1：账本自身出错
    failed = [f for f in active if f.verify.get("status") == "failed"]
    no_evidence = [f for f in active if not f.evidence]
    problems = failed + no_evidence
    if problems:
        print("\n[P1] 需要修（账本记录不再可信）")
        for f in problems:
            why = "校验失败" if f.verify.get("status") == "failed" else "无证据"
            print(f"  ! {why}  {f.id}  {f.claim[:64]}")
    else:
        print("\n[P1] 需要修：无")

    # P2：数字冲突
    conflicts = _iter_number_conflicts(ledger)
    if conflicts:
        print(f"\n[P2] 需要决策（数字矛盾，共 {len(conflicts)} 处）")
        for fact_id, other_id, mine, theirs, unit in conflicts[:6]:
            print(f"  ~ {fact_id} ({mine}{unit})  vs  {other_id} ({theirs}{unit})")
    else:
        print("\n[P2] 需要决策：无")

    # P3：可推进的实现工作，按失修天数降序
    done, open_ = _readme_checklist(root)
    if open_:
        print(f"\n[P3] 可推进的实现工作（{len(open_)} 项，按失修程度排序）")
        scored = []
        for item in open_:
            hits = search(discussable, item, top_k=1, half_life_days=10**6)
            if hits:
                latest = hits[0]["fact"]
                days = max(0, (now - _parse_when(latest.created)).days)
                scored.append((days, item, latest))
            else:
                scored.append((999999, item, None))
        for days, item, latest in sorted(scored, key=lambda t: t[0], reverse=True):
            tail = f"  失修 {days} 天" if days < 999999 else "  从未有过记录"
            if latest is not None:
                tail += f"（最近 {latest.id} {latest.claim[:48]}）"
            print(f"  - {item}{tail}")
    else:
        print("\n[P3] 可推进的实现工作：无")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    total = len(ledger.facts)
    print(f"记录总数：{total}")
    by_kind: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for fact in ledger.facts:
        by_kind[fact.kind] = by_kind.get(fact.kind, 0) + 1
        status = fact.verify.get("status", "unverified")
        by_status[status] = by_status.get(status, 0) + 1
    for kind in FACT_KINDS:
        if by_kind.get(kind):
            print(f"  {kind:13s} {by_kind[kind]}")
    print("校验状态：" + "，".join(f"{k}={v}" for k, v in sorted(by_status.items())))
    if total:
        coverage = (by_status.get("ok", 0) + by_status.get("partial", 0)) / total
        print(f"有校验记录的比例：{coverage:.0%}")
    stale = [
        f
        for f in ledger.facts
        if f.verify.get("status") in ("failed",)
    ]
    if stale:
        print(f"⚠ 校验失败的记录 {len(stale)} 条（下次写文档前先处理）：")
        for fact in stale[:5]:
            print(f"    {fact.id} {fact.claim[:60]}")
    no_evidence = [f for f in ledger.facts if not f.evidence]
    if no_evidence:
        print(f"⚠ 无证据的记录 {len(no_evidence)} 条")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    ledger = Ledger(Path(args.ledger), Path(args.root))
    fact = ledger.get(args.id)
    if fact is None:
        print(f"没有 {args.id}", file=sys.stderr)
        return 1
    _print_fact(fact)
    if fact.verify:
        print("    校验：", fact.verify.get("status"), fact.verify.get("checked_at"))
        for detail in fact.verify.get("detail", []):
            print(f"      {detail['status']:9s} {detail['type']}:{detail['value']} {detail['detail']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.project_memory",
        description="LumenDB 项目记忆：带证据、可校验、可检索的长期事实账本",
    )
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER), help="账本路径")
    parser.add_argument("--root", default=str(REPO_ROOT), help="证据的相对根目录")
    sub = parser.add_subparsers(dest="command", required=True)

    add = sub.add_parser("add", help="追加一条事实")
    add.add_argument("--kind", required=True, choices=FACT_KINDS)
    add.add_argument("--claim", required=True)
    add.add_argument("--tags", default="")
    add.add_argument("--confidence", default="measured", choices=CONFIDENCE)
    add.add_argument("--evidence", action="append", type=_parse_evidence, default=[])
    add.add_argument("--supersedes", default=None)
    add.add_argument("--source", default="", help="外部事实的出处（URL/文档/规范）；kind=external 时必填")
    add.add_argument("--verified-at", default="", help="核对出处的时间（ISO），默认=现在")
    add.set_defaults(func=cmd_add)

    query = sub.add_parser("query", help="按问题检索（BM25 + 时间衰减）")
    query.add_argument("text")
    query.add_argument("--top-k", type=int, default=5)
    query.add_argument("--half-life", type=float, default=120.0)
    query.add_argument("--kind", default=None, choices=FACT_KINDS)
    query.add_argument("--tag", default=None)
    query.set_defaults(func=cmd_query)

    verify = sub.add_parser("verify", help="核对证据是否还成立")
    verify.add_argument("--ids", nargs="*", default=[])
    verify.add_argument("--only-stale", action="store_true")
    verify.add_argument("--run-tests", action="store_true")
    verify.add_argument("--run-commands", action="store_true")
    verify.set_defaults(func=cmd_verify)

    ingest = sub.add_parser("ingest-git", help="把 git 提交灌成 anchor 记录")
    ingest.add_argument("--rev-range", default="-20")
    ingest.set_defaults(func=cmd_ingest_git)

    dups = sub.add_parser("dups", help="找出几乎重复的记录")
    dups.set_defaults(func=cmd_dups)

    conflicts = sub.add_parser("conflicts", help="找出互相矛盾的数字")
    conflicts.set_defaults(func=cmd_conflicts)

    stats = sub.add_parser("stats", help="总览")
    stats.set_defaults(func=cmd_stats)

    status = sub.add_parser("status", help="一页看清：git / 账本 / 最近测试 / 未决问题")
    status.set_defaults(func=cmd_status)

    digest = sub.add_parser(
        "digest", help="续作交接：未决问题 + 需处理 + 数字冲突 + 最新结论"
    )
    digest.set_defaults(func=cmd_digest)

    roadmap = sub.add_parser(
        "roadmap", help="README「路线图」清单 × 账本证据：还剩什么、每条上次讨论"
    )
    roadmap.set_defaults(func=cmd_roadmap)

    drift = sub.add_parser(
        "drift", help="文档漂移对账：文档里的测试数 vs 账本最新测试数（跳过 milestone）"
    )
    drift.set_defaults(func=cmd_drift)

    plan = sub.add_parser(
        "plan", help="按优先级排序的行动清单：先拍板→先修→先决策→先推进"
    )
    plan.set_defaults(func=cmd_plan)

    show = sub.add_parser("show", help="看一条记录")
    show.add_argument("id")
    show.set_defaults(func=cmd_show)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
