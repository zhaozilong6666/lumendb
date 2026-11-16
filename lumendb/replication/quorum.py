"""跨节点多数派投票，给手动/自动提升加一道仲裁。

这不是完整的 Raft：已有 heartbeat RPC、多数派续租和独立 ``HeartbeatLoop``，
但仍没有日志复制协商，也没有基于随机超时的完整 leader 选举。它解决的是
**两个副本同时提升、各自写自己的 fence** 的问题；网络分区期间如果多数派
不可达，候选节点会明确失败，而不是在本地单方面提升。

投票状态持久化到每个节点自己的 JSON 文件：``term`` / ``voted_for`` /
``last_lsn``。持久化只做原子替换 + fsync；没有 WAL，因为投票状态是少量
元数据，损坏时宁可报错也不静默重置。
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import threading
from concurrent.futures import CancelledError, ThreadPoolExecutor
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..errors import NotSupportedError, StorageError
from .fence import PromotionFence
from .lease import Lease

_log = logging.getLogger(__name__)

_NODE_ID_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")


def _validate_node_id(node_id: str) -> str:
    value = str(node_id)
    if not _NODE_ID_RE.fullmatch(value):
        raise ValueError("node_id 只能包含字母、数字、下划线、点和短横线")
    return value


def _encode_line(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _decode_line(line: bytes) -> dict[str, Any]:
    try:
        data = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StorageError("投票请求不是合法 JSON") from exc
    if not isinstance(data, dict):
        raise StorageError("投票请求必须是 JSON 对象")
    return data


class VoteState:
    """单节点的持久化投票状态。"""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else None
        self.term = 0
        self.voted_for: str | None = None
        #: 本节点**听到过**的最高 LSN（心跳/投票里学到的集群进度）。
        #: 用它拒绝落后的候选，不代表本节点真的持有这些数据。
        self.last_lsn = 0
        #: ``last_lsn`` 处日志条目的任期（Raft 投票比较的第二维）。
        #: 只有 LSN 不够：同 LSN 但任期更旧的候选不能拿到票，否则旧 leader
        #: 的尾段可以覆盖新任期已经协商过的日志。
        self.last_term = 0
        #: 本节点**真正复制到**的 LSN。心跳把它报给 leader，用来算多数派水位。
        #: 与 ``last_lsn`` 分开是必须的：一个只投票不带数据的 witness 节点
        #: 也能听到集群进度，如果拿它当 match_lsn，leader 会误以为多数派
        #: 已经持有该 LSN，从而允许提升一个落后的副本。
        self.match_lsn = 0
        #: ``match_lsn`` 处日志条目的任期。
        self.match_term = 0
        #: 多数派确认过的提交水位（本节点目前知道的上界）。
        self.commit_lsn = 0
        #: 本节点**已应用**日志的任期分段：``[[start_lsn, term], ...]``，
        #: ``start_lsn`` 单调递增。append_entries 逐条带任期时精确记录，
        #: 用于拼接点校验：同一 LSN 但任期不同说明两条历史分叉了。
        #: 只增不删——任期分段不会因滞后心跳而回退。
        self.log_terms: list[list[int]] = []
        #: 最近一次 leader 心跳（落盘）。只放内存的话，节点重启后
        #: `status` 会报"从没收到过心跳"，排障时分不清"主库没发"和
        #: "我重启过"——这正是上一轮 review 指出的缺口。
        self.last_heartbeat_at: float = 0.0
        self.last_heartbeat_from: str | None = None
        #: 上次把心跳写盘的时间，用于把 fsync 频率限制在 persist_interval 内。
        self._persisted_heartbeat_at: float = 0.0
        self._lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise StorageError(
                "投票状态损坏，拒绝用默认任期继续",
                detail=str(self.path),
            ) from exc
        if not isinstance(raw, dict):
            raise StorageError("投票状态必须是 JSON 对象", detail=str(self.path))
        self.term = max(0, int(raw.get("term", 0)))
        voted_for = raw.get("voted_for")
        self.voted_for = str(voted_for) if voted_for is not None else None
        self.last_lsn = max(0, int(raw.get("last_lsn", 0)))
        self.last_term = max(0, int(raw.get("last_term", 0)))
        self.match_lsn = max(0, int(raw.get("match_lsn", 0)))
        self.match_term = max(0, int(raw.get("match_term", 0)))
        self.commit_lsn = max(0, int(raw.get("commit_lsn", 0)))
        self.log_terms = []
        for raw_pair in raw.get("log_terms", []) or []:
            try:
                start_lsn, term = raw_pair
                self.log_terms.append([max(0, int(start_lsn)), max(0, int(term))])
            except (TypeError, ValueError):
                continue
        self.last_heartbeat_at = max(0.0, float(raw.get("last_heartbeat_at", 0.0)))
        from_leader = raw.get("last_heartbeat_from")
        self.last_heartbeat_from = (
            str(from_leader) if from_leader is not None else None
        )
        self._persisted_heartbeat_at = self.last_heartbeat_at

    def save(self) -> None:
        with self._lock:
            if self.path is None:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "term": int(self.term),
                "voted_for": self.voted_for,
                "last_lsn": int(self.last_lsn),
                "last_term": int(self.last_term),
                "match_lsn": int(self.match_lsn),
                "match_term": int(self.match_term),
                "commit_lsn": int(self.commit_lsn),
                "log_terms": [list(pair) for pair in self.log_terms],
                "last_heartbeat_at": round(float(self.last_heartbeat_at), 6),
                "last_heartbeat_from": self.last_heartbeat_from,
            }
            tmp = self.path.with_name(self.path.name + ".tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                )
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)

    def observe_lsn(self, lsn: int) -> None:
        """记录"听到过"的最高 LSN（投票安全用，不代表持有数据）。

        注意这里**不能**顺手把 leader 的任期写成 ``last_term``：心跳只说明
        leader 当前任期，不说明该 LSN 处日志条目的任期。把 leader term 当成
        last_term 会让一个日志相同的节点拒绝合法候选。
        """
        with self._lock:
            value = max(0, int(lsn))
            if value > self.last_lsn:
                self.last_lsn = value
                self.save()

    def term_at(self, lsn: int) -> int | None:
        """返回 ``lsn`` 处本节点**已应用**日志的任期；未知（没记录过）返回 ``None``。

        这是拼接点校验的依据：leader 用它发送 ``prev_lsn`` 处的真实任期，
        follower 用它核对"同一 LSN 是否来自同一历史"。**不要**用当前
        ``term`` 代替：当前任期可能比该 LSN 处日志的任期新。
        """
        with self._lock:
            value = max(0, int(lsn))
            found: int | None = None
            for start_lsn, term in self.log_terms:
                if start_lsn <= value:
                    found = int(term)
                else:
                    break
            return found

    def record_term(self, lsn: int, term: int) -> None:
        """记录 ``lsn`` 处日志的任期（append_entries 逐条携带时精确记录）。

        只追加不覆盖：任期分段是"这段历史长什么样"的事实，不能因为后来
        听到滞后心跳就往回退。同任期自然合并，``lsn`` 不前进时忽略。
        """
        with self._lock:
            value = max(0, int(lsn))
            t = max(0, int(term))
            if t == 0:
                return
            if self.log_terms:
                last_start, last_term = self.log_terms[-1]
                if last_term == t:
                    return  # 同任期延续，不需要新分段
                if value <= last_start:
                    return  # 不能写入比已知分段更早的任期
            self.log_terms.append([value, t])
            self.save()

    def observe_applied(self, lsn: int, term: int | None = None) -> None:
        """记录本节点***真正复制到***的 LSN；心跳会把它报给 leader。

        真正 apply 到某个 LSN，意味着本节点既持有它、也"听到过"它；因此
        ``last_lsn`` 必须一起前进。否则一个已经持有 100 的副本会因为没收到
        心跳而把 ``last_lsn`` 留在 0，投票时放行一个只有 80 的候选，直接
        破坏"候选至少持有全部已提交日志"的前提。
        """
        with self._lock:
            value = max(0, int(lsn))
            changed = False
            if value > self.match_lsn:
                self.match_lsn = value
                if term is not None:
                    self.match_term = max(0, int(term))
                changed = True
            if value > self.last_lsn:
                self.last_lsn = value
                if term is not None:
                    self.last_term = max(0, int(term))
                changed = True
            if changed:
                self.save()

    def observe_heartbeat(
        self, leader: str, at: float, *, persist_interval: float = 5.0
    ) -> None:
        """记录最近一次 leader 心跳（落盘，但限制 fsync 频率）。

        心跳每 `interval` 秒就来一次，每条都 fsync 会把仲裁节点变成写热点；
        所以只在"来源变了"或"距上次落盘超过 ``persist_interval``"时写盘。
        内存里的值始终是最新的，`status` 读到的是最新值。
        """
        with self._lock:
            self.last_heartbeat_at = float(at)
            self.last_heartbeat_from = str(leader)
            if (
                self.last_heartbeat_from is not None
                and self.last_heartbeat_at - self._persisted_heartbeat_at
                >= max(0.0, float(persist_interval))
            ) or self._persisted_heartbeat_at == 0.0:
                self._persisted_heartbeat_at = self.last_heartbeat_at
                self.save()

    def observe_commit(self, lsn: int) -> None:
        """推进多数派提交水位（只前进，不后退）。"""
        with self._lock:
            value = max(0, int(lsn))
            if value > self.commit_lsn:
                self.commit_lsn = value
                self.save()


class QuorumClient:
    """一个极简的投票 RPC 客户端。"""

    @staticmethod
    def request_vote(
        host: str,
        port: int,
        term: int,
        candidate_id: str,
        last_lsn: int,
        *,
        last_term: int = 0,
        timeout: float = 2.0,
        token: str | None = None,
        pre_vote: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "vote_request",
            "term": int(term),
            "candidate_id": candidate_id,
            "last_lsn": int(last_lsn),
            "last_term": int(last_term),
        }
        if pre_vote:
            payload["pre_vote"] = True
        if token is not None:
            payload["token"] = token
        sock = socket.create_connection((host, port), timeout=timeout)
        try:
            sock.settimeout(timeout)
            sock.sendall(_encode_line(payload))
            line = sock.makefile("rb").readline()
            if not line:
                raise StorageError("投票节点没有返回响应")
            data = _decode_line(line)
            if data.get("type") == "error":
                raise StorageError(str(data.get("message", "投票失败")))
            if data.get("type") != "vote_response":
                raise StorageError("投票节点返回了未知响应")
            return data
        finally:
            try:
                sock.close()
            except OSError:
                pass


    @staticmethod
    def status(
        host: str,
        port: int,
        *,
        timeout: float = 2.0,
        token: str | None = None,
    ) -> dict[str, Any]:
        """读取仲裁节点状态：任期、投票、`match_lsn` / `commit_lsn`、最近心跳。

        只读操作，不会推进任期或投票。运维排查"为什么提升被拒""水位为什么不
        前进"时，先看这个接口，再看主库日志。
        """
        payload: dict[str, Any] = {"type": "status"}
        if token is not None:
            payload["token"] = token
        sock = socket.create_connection((host, port), timeout=timeout)
        try:
            sock.settimeout(timeout)
            sock.sendall(_encode_line(payload))
            line = sock.makefile("rb").readline()
            if not line:
                raise StorageError("仲裁节点没有返回状态响应")
            data = _decode_line(line)
            if data.get("type") == "error":
                raise StorageError(str(data.get("message", "状态查询失败")))
            if data.get("type") != "status_response":
                raise StorageError("仲裁节点返回了未知状态响应")
            return data
        finally:
            try:
                sock.close()
            except OSError:
                pass

    @staticmethod
    def append_entries(
        host: str,
        port: int,
        term: int,
        leader_id: str,
        prev_lsn: int,
        prev_term: int,
        entries: list[dict[str, Any]],
        *,
        commit_lsn: int = 0,
        entry_terms: list[Any] | None = None,
        timeout: float = 2.0,
        token: str | None = None,
    ) -> dict[str, Any]:
        """发送一批日志条目（每项是 ``wal_stream._record_to_json`` 的结果）。

        这是 append_entries 的最小传输层：服务端先检查 ``prev_lsn/prev_term``
        与本地已应用日志尾部是否一致，再通过绑定的 applier 重放 entries。
        它不替代 WAL 流，也不做 Raft 的日志截断/回填；leader 侧仍可用
        WAL 流把失联副本追上。

        ``entry_terms`` 与 entries 等长（``None`` 表示 leader 不确定该条的
        任期）；follower 用它逐条记录精确任期，支撑拼接点分叉检测。
        """
        payload: dict[str, Any] = {
            "type": "append_entries",
            "term": int(term),
            "leader_id": str(leader_id),
            "prev_lsn": int(prev_lsn),
            "prev_term": int(prev_term),
            "entries": list(entries),
            "commit_lsn": int(commit_lsn),
        }
        if entry_terms is not None:
            payload["entry_terms"] = list(entry_terms)
        if token is not None:
            payload["token"] = token
        sock = socket.create_connection((host, port), timeout=timeout)
        try:
            sock.settimeout(timeout)
            sock.sendall(_encode_line(payload))
            line = sock.makefile("rb").readline()
            if not line:
                raise StorageError("仲裁节点没有返回日志追加响应")
            data = _decode_line(line)
            if data.get("type") == "error":
                raise StorageError(str(data.get("message", "日志追加失败")))
            if data.get("type") != "append_entries_response":
                raise StorageError("仲裁节点返回了未知日志追加响应")
            return data
        finally:
            try:
                sock.close()
            except OSError:
                pass

    @staticmethod
    def heartbeat(
        host: str,
        port: int,
        term: int,
        leader_id: str,
        last_lsn: int,
        *,
        commit_lsn: int = 0,
        timeout: float = 2.0,
        token: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "heartbeat",
            "term": int(term),
            "leader_id": str(leader_id),
            "last_lsn": int(last_lsn),
            "commit_lsn": int(commit_lsn),
        }
        if token is not None:
            payload["token"] = token
        sock = socket.create_connection((host, port), timeout=timeout)
        try:
            sock.settimeout(timeout)
            sock.sendall(_encode_line(payload))
            line = sock.makefile("rb").readline()
            if not line:
                raise StorageError("仲裁节点没有返回心跳响应")
            data = _decode_line(line)
            if data.get("type") == "error":
                raise StorageError(str(data.get("message", "心跳失败")))
            if data.get("type") != "heartbeat_response":
                raise StorageError("仲裁节点返回了未知心跳响应")
            return data
        finally:
            try:
                sock.close()
            except OSError:
                pass


def _request_status(
    host: str,
    port: int,
    timeout: float,
    token: str | None,
) -> dict[str, Any]:
    """读取一个仲裁节点的状态快照（只读，不改任期）。"""
    return QuorumClient.status(
        host, port, timeout=timeout, token=token
    )


def _send_heartbeat(
    host: str,
    port: int,
    term: int,
    leader_id: str,
    last_lsn: int,
    commit_lsn: int,
    timeout: float,
    token: str | None,
) -> dict[str, Any]:
    """线程池里发一次心跳；把 ``QuorumClient.heartbeat`` 的关键字参数摊平。"""
    return QuorumClient.heartbeat(
        host,
        port,
        term,
        leader_id,
        last_lsn,
        commit_lsn=commit_lsn,
        timeout=timeout,
        token=token,
    )


class QuorumNode:
    """一个参与多数派投票的节点。

    ``peers`` 是**其他节点**的映射：``{node_id: (host, port)}``。集群总节点数为
    ``len(peers) + 1``（如果 peers 误包含自己，则按 peers 数量计算并跳过自己）。
    每个节点只监听投票 RPC；WAL 流仍由 :class:`WALServer` 提供。
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        peers: dict[str, tuple[str, int]] | None = None,
        state_path: str | Path | None = None,
        token: str | None = None,
        timeout: float = 2.0,
        heartbeat_persist_interval: float = 5.0,
    ) -> None:
        self.node_id = _validate_node_id(node_id)
        self.host = host
        self.port = int(port)
        self.peers = dict(peers or {})
        self.token = token
        self.timeout = timeout
        self.state = VoteState(state_path)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._handlers: list[threading.Thread] = []
        # append_entries 的重放是串行的；不能和 apply 并发。
        self._append_lock = threading.Lock()
        self._log_applier: Callable[[list[Any]], int] | None = None
        #: 最近一轮心跳里各 peer 真正 apply 到的 LSN / 任期。leader 侧的
        #: ``push_entries_to_peers`` 从这里取得 append_entries 的 prev 边界。
        self.last_matches: dict[str, int] = {}
        self.last_match_terms: dict[str, int] = {}
        self.heartbeat_persist_interval = max(0.0, float(heartbeat_persist_interval))
        # 从持久化状态恢复：重启后的 status 不应该谎称"从没收到过心跳"。
        self._last_heartbeat_at = self.state.last_heartbeat_at or None
        self._last_heartbeat_from = self.state.last_heartbeat_from

    def observe_lsn(self, lsn: int) -> None:
        """记录本节点已经追平到的 LSN；投票时用它拒绝落后的候选。"""
        with self._lock:
            self.state.observe_lsn(lsn)

    def observe_applied(self, lsn: int, term: int | None = None) -> None:
        """记录本节点**真正复制到**的 LSN；心跳响应会带上它，用于算水位。"""
        with self._lock:
            self.state.observe_applied(lsn, term=term)

    def observe_commit(self, lsn: int) -> None:
        """记录已知的多数派提交水位（由心跳轮次或上层运维工具推进）。"""
        with self._lock:
            self.state.observe_commit(lsn)

    def set_log_applier(self, applier: Callable[[list[Any]], int] | None) -> None:
        """绑定 append_entries 的本地重放函数（通常是 ``Replica.apply``）。"""
        self._log_applier = applier

    @property
    def quorum_commit_lsn(self) -> int:
        """本节点目前知道的"多数派已持有"的 LSN 上界。"""
        return self.state.commit_lsn

    def _majority(self) -> int:
        total = len(self.peers) + (0 if self.node_id in self.peers else 1)
        return total // 2 + 1

    def _advance_commit_lsn(self, self_lsn: int, matches: dict[str, int]) -> int:
        """用"自己 + 本轮报上来的 match_lsn"算多数派水位，并推进持久化。"""
        lsns = [max(0, int(self_lsn))] + [max(0, int(v)) for v in matches.values()]
        majority = self._majority()
        if len(lsns) < majority:
            return self.quorum_commit_lsn
        lsns.sort(reverse=True)
        watermark = lsns[majority - 1]
        with self._lock:
            self.state.observe_commit(watermark)
            return self.state.commit_lsn

    def refresh_commit_lsn(self, self_lsn: int) -> int:
        """立即用最近一轮心跳/推送记录到的 peer ``match_lsn`` 重算水位。

        ``heartbeat_round`` 每轮都会算一次；append_entries 成功之后等下一轮
        心跳才重算会让同步复制的屏障多等一个心跳周期。推送方在成功后调用
        本方法，把已经真实 apply 的进度立刻计入多数派水位。
        """
        return self._advance_commit_lsn(
            self_lsn, dict(getattr(self, "last_matches", {}))
        )

    def request_vote(
        self,
        term: int,
        candidate_id: str,
        last_lsn: int,
        last_term: int = 0,
        pre_vote: bool = False,
    ) -> dict[str, Any]:
        # 与 append_entries 的 apply 串行：日志重放期间不能让一次投票/心跳
        # 把任期抬高，否则一个已经过期的 leader 仍会把条目写进副本。
        with self._append_lock:
            return self._request_vote_impl(
                term, candidate_id, last_lsn, last_term, pre_vote
            )

    def _request_vote_impl(
        self,
        term: int,
        candidate_id: str,
        last_lsn: int,
        last_term: int = 0,
        pre_vote: bool = False,
    ) -> dict[str, Any]:
        """处理投票/预投票。

        预投票（``pre_vote=True``）只回答"如果现在投票，会不会投给你"，
        **不写任何状态**：不改任期、不记 voted_for。这样少数派分区里的节点
        反复尝试提升时，不会把自己的任期越推越高，也就不会在分区恢复后
        用更高的任期把健康的主库逼停。
        """
        candidate = _validate_node_id(candidate_id)
        requested = max(0, int(term))
        candidate_lsn = max(0, int(last_lsn))
        candidate_term = max(0, int(last_term))

        def log_is_newer_or_equal() -> bool:
            # Raft 的投票安全性比较的是 (last_term, last_lsn)，不是单独的 LSN。
            # 同 LSN 下任期更旧的候选必须被拒绝；否则旧 leader 已经写过、
            # 新任期尚未协商的尾段可能被它重新当选后覆盖。
            if candidate_term != self.state.last_term:
                return candidate_term > self.state.last_term
            return candidate_lsn >= self.state.last_lsn

        if pre_vote:
            with self._lock:
                would_grant = (
                    requested >= self.state.term
                    and self.state.voted_for in (None, candidate)
                    and log_is_newer_or_equal()
                )
                return {
                    "type": "vote_response",
                    "term": self.state.term,
                    "granted": bool(would_grant),
                    "pre_vote": True,
                }
        with self._lock:
            if requested < self.state.term:
                return {
                    "type": "vote_response",
                    "term": self.state.term,
                    "granted": False,
                }
            if requested > self.state.term:
                self.state.term = requested
                self.state.voted_for = None
                self.state.save()
            if self.state.voted_for not in (None, candidate):
                return {
                    "type": "vote_response",
                    "term": self.state.term,
                    "granted": False,
                }
            if not log_is_newer_or_equal():
                return {
                    "type": "vote_response",
                    "term": self.state.term,
                    "granted": False,
                }
            self.state.voted_for = candidate
            self.state.save()
            return {
                "type": "vote_response",
                "term": self.state.term,
                "granted": True,
            }

    def status(self) -> dict[str, Any]:
        """返回本节点状态快照（供 `QuorumClient.status` 使用）。"""
        with self._lock:
            return {
                "type": "status_response",
                "node_id": self.node_id,
                "term": self.state.term,
                "voted_for": self.state.voted_for,
                "last_lsn": self.state.last_lsn,
                "last_term": self.state.last_term,
                "match_lsn": self.state.match_lsn,
                "match_term": self.state.match_term,
                "commit_lsn": self.state.commit_lsn,
                # 从 state 读：节点重启后这里仍然能报出最近一次心跳来源
                # （内存字段重启即丢，那正是排障最需要的信息）。
                "last_heartbeat_at": self.state.last_heartbeat_at or None,
                "last_heartbeat_from": self.state.last_heartbeat_from,
                "peers": sorted(
                    pid for pid in self.peers if pid != self.node_id
                ),
                # leader 视角最近一次心跳/推送后各 peer 已确认的复制位置，
                # 诊断复制滞后用；心跳线程写入这些字段时不上锁，快照尽力而为。
                "peer_matches": {
                    pid: int(lsn) for pid, lsn in self.last_matches.items()
                },
                "peer_match_terms": {
                    pid: int(term) for pid, term in self.last_match_terms.items()
                },
            }

    def heartbeat(
        self,
        term: int,
        leader_id: str,
        last_lsn: int,
        commit_lsn: int = 0,
    ) -> dict[str, Any]:
        with self._append_lock:
            return self._heartbeat_impl(term, leader_id, last_lsn, commit_lsn)

    def _heartbeat_impl(
        self,
        term: int,
        leader_id: str,
        last_lsn: int,
        commit_lsn: int = 0,
    ) -> dict[str, Any]:
        """更新 leader 心跳；旧任期不更新活跃时间，新任期推进节点任期。"""
        leader = _validate_node_id(leader_id)
        requested = max(0, int(term))
        with self._lock:
            if requested < self.state.term:
                return {
                    "type": "heartbeat_response",
                    "term": self.state.term,
                    "ok": False,
                }
            if requested > self.state.term:
                self.state.term = requested
                self.state.voted_for = None
                self.state.save()
            # 注意：``last_lsn`` 只进 "听到过" 水位（用于拒绝落后候选），
            # 不进 ``match_lsn``——leader 的心跳不代表本节点持有这些数据。
            self.state.observe_lsn(int(last_lsn))
            # 多数派水位随心跳传播，候选提升前才能读到集群已知的 commit_lsn；
            # 只存在 leader 本地的话，换主时闸门会退化成 0。
            self.state.observe_commit(int(commit_lsn))
            now = time.time()
            self._last_heartbeat_at = now
            self._last_heartbeat_from = leader
            self.state.observe_heartbeat(
                leader, now, persist_interval=self.heartbeat_persist_interval
            )
            return {
                "type": "heartbeat_response",
                "term": self.state.term,
                "ok": True,
                "match_lsn": self.state.match_lsn,
                "match_term": self.state.match_term,
                "commit_lsn": self.state.commit_lsn,
            }

    def append_entries(
        self,
        term: int,
        leader_id: str,
        prev_lsn: int,
        prev_term: int,
        entries: list[Any],
        *,
        commit_lsn: int = 0,
        entry_terms: list[Any] | None = None,
    ) -> dict[str, Any]:
        """处理一批 append_entries。

        ``prev_lsn/prev_term`` 必须与本地已应用日志尾部完全一致；不匹配时
        返回 ``ok=False``，让 leader 从更早的位置重发。entries 是
        ``wal_stream._record_to_json`` 的字典，按顺序交给绑定的 applier
        （通常是 ``Replica.apply``）。

        ``entry_terms`` 是与 entries 等长的任期数组（可为 ``None`` 表示
        leader 也不确定），follower 逐条记进 ``VoteState.log_terms``。它
        支撑**拼接点任期校验**：同一 LSN、不同任期 = 两条历史分叉了，
        直接重放会把正确的已提交日志静默覆盖，所以拒绝并标记
        ``rebuild_required``。这里仍然没有日志截断/回填——分叉后的修复
        手段是重新基准备份，而不是截断。
        """
        leader = _validate_node_id(leader_id)
        requested = max(0, int(term))
        previous = max(0, int(prev_lsn))
        previous_term = max(0, int(prev_term))
        if not isinstance(entries, list):
            return self._append_rejection("entries 必须是数组")
        if len(entries) > 1000:
            return self._append_rejection("单次 append_entries 条目过多")
        terms: list[int | None] = []
        if entry_terms is not None:
            if not isinstance(entry_terms, list) or len(entry_terms) != len(entries):
                return self._append_rejection("entry_terms 必须与 entries 等长")
            for raw_term in entry_terms:
                if raw_term is None:
                    terms.append(None)
                    continue
                try:
                    terms.append(max(0, int(raw_term)))
                except (TypeError, ValueError):
                    return self._append_rejection("entry_terms 必须是整数数组")

        with self._append_lock:
            with self._lock:
                if requested < self.state.term:
                    return {
                        "type": "append_entries_response",
                        "term": self.state.term,
                        "ok": False,
                        "reason": "stale_term",
                        "match_lsn": self.state.match_lsn,
                        "match_term": self.state.match_term,
                    }
                if requested > self.state.term:
                    self.state.term = requested
                    self.state.voted_for = None
                    self.state.save()
                if previous != self.state.match_lsn:
                    return {
                        "type": "append_entries_response",
                        "term": self.state.term,
                        "ok": False,
                        "reason": "prefix_mismatch",
                        "match_lsn": self.state.match_lsn,
                        "match_term": self.state.match_term,
                    }
                local_tail_term = self.state.term_at(previous)
                if local_tail_term is None:
                    # follower 对这段历史没有任期知识（典型：刚通过基准备份
                    # 同步，备份流不携带任期）。首次接触选择信任 leader 的
                    # 拼接点任期；本批 entry_terms 落盘后，之后每一次推进
                    # 都有精确任期可校验。
                    local_tail_term = previous_term
                if previous_term != local_tail_term:
                    # 前缀 LSN 一致但任期不一致：这不是"落后了重发就行"，
                    # 而是同一 LSN 上存在两条不同的历史。重放会把本节点
                    # 已提交的日志静默覆盖成分叉版本，唯一安全的修复是
                    # 重新基准备份。leader 端据此跳过该 peer 的推进。
                    return self._append_rejection(
                        (
                            f"日志分歧: lsn={previous} 本地任期 "
                            f"{local_tail_term}，leader 任期 {previous_term}"
                        ),
                        term=self.state.term,
                        rebuild_required=True,
                    )
                applier = self._log_applier
                if entries and applier is None:
                    return self._append_rejection(
                        "本节点没有绑定日志重放函数", term=self.state.term
                    )

            records: list[Any] = []
            if entries:
                from .wal_stream import _record_from_json

                try:
                    records = [_record_from_json(item) for item in entries]
                except (KeyError, TypeError, ValueError, StorageError) as exc:
                    return self._append_rejection(
                        f"日志条目无法解码: {exc}", term=self.state.term
                    )
                if records and records[0].lsn <= previous:
                    return self._append_rejection(
                        "日志条目没有越过 prev_lsn", term=self.state.term
                    )
                for previous_record, current_record in zip(records, records[1:]):
                    if current_record.lsn != previous_record.lsn + len(
                        previous_record.encode()
                    ):
                        return self._append_rejection(
                            "日志条目内部不连续", term=self.state.term
                        )
                try:
                    applier(records)  # type: ignore[misc]
                except Exception as exc:  # noqa: BLE001 - 重放异常一律回退为拒绝
                    # 覆盖 StorageError 之外的意外异常（OSError/ValueError 等）：
                    # 不能让单个畸形批次把请求线程或整个服务循环打死。
                    return self._append_rejection(
                        f"日志重放失败: {exc}", term=self.state.term
                    )
                last_lsn = int(records[-1].lsn)
                with self._lock:
                    self.state.observe_applied(last_lsn, term=requested)
                    if terms:
                        # 逐条精确记录任期分段；重放成功后才有意义，
                        # 失败批次绝不能把任期记进去。
                        for record, entry_term in zip(records, terms):
                            if entry_term is not None:
                                self.state.record_term(
                                    int(record.lsn), int(entry_term)
                                )
            with self._lock:
                self.state.observe_commit(int(commit_lsn))
                return {
                    "type": "append_entries_response",
                    "term": self.state.term,
                    "ok": True,
                    "match_lsn": self.state.match_lsn,
                    "match_term": self.state.match_term,
                    "commit_lsn": self.state.commit_lsn,
                }

    def _append_rejection(
        self,
        reason: str,
        *,
        term: int | None = None,
        rebuild_required: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            response: dict[str, Any] = {
                "type": "append_entries_response",
                "term": self.state.term if term is None else int(term),
                "ok": False,
                "reason": str(reason),
                "match_lsn": self.state.match_lsn,
                "match_term": self.state.match_term,
            }
            if rebuild_required:
                response["rebuild_required"] = True
            return response

    def heartbeat_round(
        self,
        last_lsn: int,
        *,
        abort_on_higher_term: bool = False,
    ) -> tuple[int, list[str]]:
        """向 peers 并发发送当前任期心跳，返回 (ack 数, 错误列表)。

        如果 peer 返回更高任期，本节点立即推进任期并清空投票；这使旧主库
        在租约到期前就能从下一次写围栏检查中发现自己已经过期。

        并发扇出：以前是逐节点同步发，N 个 peer 里只要有一个慢节点，整轮
        就要等 N × timeout；而这一轮的结果决定租约能不能续，慢租约直接
        把主库推到"自己停写"。现在总耗时约等于单个 timeout。结果按 peer
        声明顺序回收，`last_acks` / `last_errors` 的语义不变（测试仍然按
        串行语义断言）。

        ``abort_on_higher_term=True`` 时，一旦某个 peer 回了更高任期，就
        取消还没开始的请求（已经发出的只能等它超时），用于尽快降级。
        """
        with self._lock:
            term = self.state.term
            commit_lsn = self.state.commit_lsn
            peers = [
                (peer_id, address)
                for peer_id, address in self.peers.items()
                if peer_id != self.node_id
            ]
        # 我自己确实持有到 last_lsn（这是本地 WAL 的位置），登记成 match_lsn，
        # 这样即使之后角色变化，本节点报出的进度也是真实的。
        self.observe_applied(last_lsn)
        acks = {self.node_id}
        outcomes: dict[str, tuple[dict | None, BaseException | None]] = {}
        matches: dict[str, int] = {}
        match_terms: dict[str, int] = {}
        highest_term = term
        if peers:
            with ThreadPoolExecutor(
                max_workers=len(peers), thread_name_prefix="lumendb-heartbeat"
            ) as pool:
                futures = {
                    pool.submit(
                        _send_heartbeat,
                        address[0],
                        int(address[1]),
                        term,
                        self.node_id,
                        int(last_lsn),
                        int(commit_lsn),
                        self.timeout,
                        self.token,
                    ): peer_id
                    for peer_id, address in peers
                }
                for future, peer_id in list(futures.items()):
                    try:
                        outcomes[peer_id] = (future.result(), None)
                    except (OSError, ValueError, StorageError, CancelledError) as exc:
                        outcomes[peer_id] = (None, exc)
                    response = outcomes[peer_id][0]
                    if (
                        abort_on_higher_term
                        and response is not None
                        and max(0, int(response.get("term", 0))) > term
                    ):
                        for pending in futures:
                            pending.cancel()

        errors: list[str] = []
        for peer_id, _address in peers:
            response, exc = outcomes.get(peer_id, (None, None))
            if exc is not None:
                errors.append(f"{peer_id}: {exc}")
                continue
            if response is None:
                errors.append(f"{peer_id}: 请求被取消")
                continue
            response_term = max(0, int(response.get("term", 0)))
            if response_term > highest_term:
                highest_term = response_term
            if response.get("ok") and response_term <= term:
                acks.add(peer_id)
                matches[peer_id] = int(response.get("match_lsn", 0))
                match_terms[peer_id] = int(response.get("match_term", 0))
        self.last_matches = dict(matches)
        self.last_match_terms = dict(match_terms)
        if highest_term > term:
            with self._lock:
                self.state.term = highest_term
                self.state.voted_for = None
                self.state.save()
            errors.append(f"observed higher term {highest_term}")
        # 多数派水位：自己和 peers 的 match_lsn 取第 majority 大的值。
        # 只有拿到多数派响应时才推进；不足多数派时保持原水位不动。
        # 观察到更高任期说明本节点已经不是 leader，不能再替集群推进水位。
        if highest_term <= term:
            self._advance_commit_lsn(last_lsn, matches)
        return len(acks), errors

    def renew_lease(
        self,
        last_lsn: int,
        *,
        lease_path: str | Path,
        lease_ttl: float = 10.0,
        now: float | None = None,
    ) -> float:
        """多数派心跳确认后续租；拿不到多数派就拒绝续租。"""
        total_nodes = len(self.peers) + (0 if self.node_id in self.peers else 1)
        majority = total_nodes // 2 + 1
        acks, errors = self.heartbeat_round(last_lsn)
        if acks < majority:
            detail = (
                f"heartbeats={acks}/{total_nodes} majority={majority}; "
                + ("; ".join(errors) if errors else "no errors")
            )
            raise NotSupportedError(
                "多数派心跳不足，拒绝续租",
                detail=detail,
            )
        lease = Lease(lease_path, self.node_id, ttl=lease_ttl)
        return lease.renew(now=now, term=self.state.term)

    def _pre_vote(self, candidate_lsn: int) -> None:
        """预投票：只探询意向，不改任期/投票。多数派不通过就抛错。"""
        with self._lock:
            term = self.state.term + 1
            candidate_term = self.state.last_term
            votes = {self.node_id}
            total_nodes = len(self.peers) + (
                0 if self.node_id in self.peers else 1
            )
            majority = total_nodes // 2 + 1
            peers = [
                (peer_id, address)
                for peer_id, address in self.peers.items()
                if peer_id != self.node_id
            ]
        errors: list[str] = []
        highest_term = term
        for peer_id, address in peers:
            try:
                response = QuorumClient.request_vote(
                    address[0],
                    int(address[1]),
                    term,
                    self.node_id,
                    candidate_lsn,
                    last_term=candidate_term,
                    timeout=self.timeout,
                    token=self.token,
                    pre_vote=True,
                )
            except (OSError, ValueError, StorageError) as exc:
                errors.append(f"{peer_id}: {exc}")
                continue
            peer_term = int(response.get("term", 0))
            if peer_term > highest_term:
                highest_term = peer_term
            if response.get("granted"):
                votes.add(peer_id)
        if highest_term > term:
            # 预投票本身不推进任期；但看到更高任期就说明本轮意向已经过时，
            # 不能拿其余节点的票硬凑多数派。
            raise NotSupportedError(
                "预投票观察到更高任期，放弃本次提升",
                detail=f"observed_term={highest_term} proposed_term={term}",
                hint="等更高任期的主库完成选举，或重新读取水位后再发起",
            )
        if len(votes) < majority:
            detail = (
                f"pre_votes={len(votes)}/{total_nodes} majority={majority}; "
                + ("; ".join(errors) if errors else "no errors")
            )
            raise NotSupportedError(
                "预投票未通过多数派，未推进任期",
                detail=detail,
                hint="确认 peers 可达后再发起提升；任期保持原值，不会干扰现有主库",
            )

    def _known_commit_lsn(self) -> int:
        """水位不能只信本地：候选可能错过 leader 的心跳。

        多数派水位已经随心跳传播，但一个刚恢复/分区的候选本地仍可能是 0；
        提升闸门如果只读本地 `commit_lsn`，就会把落后候选放进来。这里向
        可达 peer 读一次 status，取本地与 peer 的最大值——与"听到过"的
        `last_lsn` 不同，`commit_lsn` 是集群级水位，取最大值是保守方向。
        """
        known = self.quorum_commit_lsn
        for peer_id, address in self.peers.items():
            if peer_id == self.node_id:
                continue
            try:
                status = QuorumClient.status(
                    address[0],
                    int(address[1]),
                    timeout=self.timeout,
                    token=self.token,
                )
            except (OSError, ValueError, StorageError):
                continue
            known = max(known, int(status.get("commit_lsn", 0) or 0))
        return known

    def campaign(
        self,
        last_lsn: int,
        *,
        fence_path: str | Path | None = None,
        lease_path: str | Path | None = None,
        lease_ttl: float = 10.0,
        pre_vote: bool = True,
    ) -> int:
        """争取多数派投票；成功返回任期，失败抛 ``NotSupportedError``。

        若给了 ``lease_path``，拿到多数票后还必须获取写租约；租约仍被旧主库
        持有时不会抢占，候选节点需要等 TTL 过期后再重试。
        """
        candidate_lsn = max(0, int(last_lsn))
        # [P2] 提升闸门：候选必须至少追平"多数派已提交水位"。没有这道闸门时，
        # 提升一个落后副本会静默丢掉主库已经提交、但还没发给它的尾段。
        known_commit = self._known_commit_lsn()
        if candidate_lsn < known_commit:
            raise NotSupportedError(
                "候选落后于多数派已提交水位，拒绝提升",
                detail=(
                    f"candidate_lsn={candidate_lsn} "
                    f"quorum_commit_lsn={known_commit}"
                ),
                hint="先让副本追平 WAL（或做一次增量基准备份）再发起提升",
            )
        if pre_vote:
            # 先问一轮"会不会投我"，拿不到多数就**不推进任期**直接返回。
            self._pre_vote(candidate_lsn)
        with self._lock:
            candidate_term = self.state.last_term
            term = self.state.term + 1
            self.state.term = term
            self.state.voted_for = self.node_id
            self.state.observe_lsn(candidate_lsn)
            self.state.save()

        votes = {self.node_id}
        total_nodes = len(self.peers) + (0 if self.node_id in self.peers else 1)
        majority = total_nodes // 2 + 1
        highest_term = term
        errors: list[str] = []
        for peer_id, address in self.peers.items():
            if peer_id == self.node_id:
                continue
            try:
                response = QuorumClient.request_vote(
                    address[0],
                    int(address[1]),
                    term,
                    self.node_id,
                    candidate_lsn,
                    last_term=candidate_term,
                    timeout=self.timeout,
                    token=self.token,
                )
            except (OSError, ValueError, StorageError) as exc:
                errors.append(f"{peer_id}: {exc}")
                continue
            peer_term = int(response.get("term", 0))
            if peer_term > highest_term:
                highest_term = peer_term
            if response.get("granted"):
                votes.add(peer_id)

        if highest_term > term:
            with self._lock:
                self.state.term = highest_term
                self.state.voted_for = None
                self.state.save()
            # 这些"授予票数"是上一任期的票，不能拿它们宣布本轮当选。
            raise NotSupportedError(
                "投票观察到更高任期，放弃本次提升",
                detail=f"observed_term={highest_term} proposed_term={term}",
                hint="让本节点跟随更高任期，再由下一轮选举决定主库",
            )

        if len(votes) < majority:
            detail = (
                f"votes={len(votes)}/{total_nodes} majority={majority}; "
                + ("; ".join(errors) if errors else "no errors")
            )
            raise NotSupportedError(
                "多数派投票失败，拒绝在本节点单方面提升",
                detail=detail,
            )

        # 候选现在确定持有到 candidate_lsn 的数据，记录下来供后续水位计算。
        self.observe_applied(candidate_lsn, term=candidate_term)
        if lease_path is not None:
            Lease(lease_path, self.node_id, ttl=lease_ttl).acquire(term)
        if fence_path is not None:
            PromotionFence(fence_path, node_id=self.node_id).promote(candidate_lsn)
        return term

    def start(self) -> "QuorumNode":
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self.host, self.port = self._sock.getsockname()
        self._stop.clear()
        self._handlers: list[threading.Thread] = []
        self._thread = threading.Thread(
            target=self._serve, name=f"lumendb-quorum-{self.node_id}", daemon=True
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
        for handler in list(self._handlers):
            handler.join(timeout=1)

    def _serve(self) -> None:
        sock = self._sock
        if sock is None:
            return
        while not self._stop.is_set():
            try:
                conn, _addr = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            # 每连接一个线程：串行处理会让一个死 peer 的停滞连接占满
            # conn 超时窗口，把其他节点的投票/心跳全部拖到超时。
            # _handle 内部路径（投票/心跳/append）都串行在 _append_lock
            # 上，所以并发处理是安全的。
            self._handlers[:] = [h for h in self._handlers if h.is_alive()]
            handler = threading.Thread(
                target=self._handle_conn, args=(conn,), daemon=True
            )
            self._handlers.append(handler)
            handler.start()

    def _handle_conn(self, conn: socket.socket) -> None:
        try:
            self._handle(conn)
        except Exception:  # noqa: BLE001 - 单个坏请求不能打死服务线程
            _log.exception("仲裁请求处理失败，连接已关闭")
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(self.timeout)
            line = conn.makefile("rb").readline()
            if not line:
                return
            request = _decode_line(line)
            if self.token is not None and request.get("token") != self.token:
                conn.sendall(
                    _encode_line({"type": "error", "message": "仲裁认证失败"})
                )
                return
            request_type = request.get("type")
            if request_type == "vote_request":
                response = self.request_vote(
                    int(request.get("term", 0)),
                    str(request.get("candidate_id", "")),
                    int(request.get("last_lsn", 0)),
                    int(request.get("last_term", 0)),
                    bool(request.get("pre_vote", False)),
                )
            elif request_type == "status":
                response = self.status()
            elif request_type == "heartbeat":
                response = self.heartbeat(
                    int(request.get("term", 0)),
                    str(request.get("leader_id", "")),
                    int(request.get("last_lsn", 0)),
                    int(request.get("commit_lsn", 0)),
                )
            elif request_type == "append_entries":
                response = self.append_entries(
                    int(request.get("term", 0)),
                    str(request.get("leader_id", "")),
                    int(request.get("prev_lsn", 0)),
                    int(request.get("prev_term", 0)),
                    request.get("entries", []),
                    commit_lsn=int(request.get("commit_lsn", 0)),
                    entry_terms=request.get("entry_terms"),
                )
            else:
                conn.sendall(
                    _encode_line({"type": "error", "message": "未知仲裁请求"})
                )
                return
            conn.sendall(_encode_line(response))
        except (OSError, ValueError, StorageError) as exc:
            try:
                conn.sendall(_encode_line({"type": "error", "message": str(exc)}))
            except OSError:
                pass


class QuorumCommitBarrier:
    """同步复制的提交屏障：等"多数派已持有"的水位追到目标 LSN。

    它被装在 `TransactionManager` 上，在本地 COMMIT 记录 fsync 之后、返回
    成功之前调用。语义是：

    * 水位追上了 → 正常返回，客户端看到的 COMMIT 才代表"多数派都有"；
    * 超时没追上 → 抛 :class:`CommitOutcomeUnknown`（SQLSTATE `40003`）。
      注意**不能**把它当成"没写进去"：数据已经本地落盘，后续可能被复制出去。
      正确处理是重新读一次确认，或交给幂等重试。

    水位由 `heartbeat_round` 推进，所以同步复制必须配心跳循环；没有心跳时
    水位永远不动，每笔写都会等到超时。
    """

    def __init__(
        self,
        node,
        *,
        timeout: float = 2.0,
        poll_interval: float = 0.01,
    ) -> None:
        self.node = node
        self.timeout = max(0.0, float(timeout))
        self.poll_interval = max(0.001, float(poll_interval))
        self.last_observed = 0
        self.timeouts = 0

    @property
    def quorum_commit_lsn(self) -> int:
        return int(getattr(self.node, "quorum_commit_lsn", 0))

    def __call__(self, lsn: int) -> None:
        target = max(0, int(lsn))
        if self.quorum_commit_lsn >= target:
            return
        deadline = time.monotonic() + self.timeout
        while True:
            observed = self.quorum_commit_lsn
            self.last_observed = observed
            if observed >= target:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(self.poll_interval, remaining))
        self.timeouts += 1
        from ..errors import CommitOutcomeUnknown

        raise CommitOutcomeUnknown(
            "本地已提交，但多数派水位没能在超时内追平",
            detail=(
                f"commit_lsn={target} quorum_commit_lsn={self.quorum_commit_lsn} "
                f"timeout={self.timeout}s"
            ),
            hint=(
                "结果未知：先读一次确认这一笔是否生效，再决定重试；"
                "如果持续发生，检查副本是否掉线或心跳循环是否在运行"
            ),
        )


class HeartbeatLoop:
    """后台 leader 心跳循环：定期向 peers 发 ``heartbeat``。

    与 :class:`lumendb.replication.lease.LeaseKeeper` 分开：lease keeper 负责续租，
    本类只维护 leader 活跃度和任期视图。观察到更高任期时调用
    ``on_higher_term(term)``，上层可据此停写或降级。
    """

    def __init__(
        self,
        node: QuorumNode,
        last_lsn: Callable[[], int],
        *,
        interval: float = 0.5,
        on_higher_term: Callable[[int], None] | None = None,
        push_entries: Callable[[QuorumNode], dict[str, int]] | None = None,
        max_batches_per_round: int = 10,
    ) -> None:
        self.node = node
        self.last_lsn = last_lsn
        self.interval = max(0.05, float(interval))
        self.on_higher_term = on_higher_term
        self.push_entries = push_entries
        # 每轮心跳之后最多连续推多少批：落后副本不用等十几个心跳周期才能
        # 追平，同时封顶防止单轮无限拉长（长期落后仍走 WAL 流/rebuild）。
        self.max_batches_per_round = max(1, int(max_batches_per_round))
        self.last_acks = 0
        self.last_errors: list[str] = []
        self.last_pushed: dict[str, int] = {}
        self.last_callback_error: str | None = None
        self.last_observed_term = node.state.term
        #: 最近一轮心跳之后本节点知道的多数派提交水位。
        self.last_quorum_commit_lsn = node.quorum_commit_lsn
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> "HeartbeatLoop":
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="lumendb-heartbeat-loop",
                daemon=True,
            )
            self._thread.start()
            return self

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            thread = self._thread
            self._thread = None
        if thread is not None and thread is not threading.current_thread():
            # 线程可能正卡在一轮心跳的 socket 等待里；join 超时必须覆盖
            # 剩余等待，否则解释器关闭时线程还在提交新请求。
            thread.join(
                timeout=max(
                    1.0,
                    self.interval + getattr(self.node, "timeout", 2.0) + 0.5,
                )
            )

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            term_before = self.node.state.term
            try:
                acks, errors = self.node.heartbeat_round(self.last_lsn())
            except Exception as exc:  # noqa: BLE001 - 解释器关闭时 ThreadPool 也会抛
                acks, errors = 0, [str(exc)]
                if self._stop.is_set():
                    break
            self.last_acks = acks
            self.last_errors = errors[-32:]
            self.last_quorum_commit_lsn = self.node.quorum_commit_lsn
            term_after = self.node.state.term
            self.last_observed_term = term_after
            if (
                self.push_entries is not None
                and term_after == term_before
            ):
                try:
                    accumulated: dict[str, int] = {}
                    for _ in range(self.max_batches_per_round):
                        pushed = self.push_entries(self.node) or {}
                        if not pushed:
                            break
                        for peer_id, count in pushed.items():
                            accumulated[peer_id] = (
                                accumulated.get(peer_id, 0) + count
                            )
                    self.last_pushed = accumulated
                except Exception as exc:  # noqa: BLE001 - 后台线程不能因推送失败退出
                    # 推送失败（包括写围栏拒绝/租约丢失）不能拖垮心跳线程；
                    # 下一次心跳仍会继续尝试，上层可看 last_errors。
                    # 有界保留最近 32 条，长期失败的 peer 不会让列表无限增长。
                    self.last_errors = (
                        self.last_errors + [f"append_entries: {exc}"]
                    )[-32:]
            if term_after > term_before and self.on_higher_term is not None:
                try:
                    self.on_higher_term(term_after)
                    self.last_callback_error = None
                except Exception as exc:  # 后台线程不能因上层回调失败而静默退出
                    self.last_callback_error = str(exc)

    def __enter__(self) -> "HeartbeatLoop":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


__all__ = [
    "HeartbeatLoop",
    "QuorumClient",
    "QuorumCommitBarrier",
    "QuorumNode",
    "VoteState",
]
