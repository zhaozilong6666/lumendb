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
import os
import re
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..errors import NotSupportedError, StorageError
from .fence import PromotionFence
from .lease import Lease

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
        #: 本节点**真正复制到**的 LSN。心跳把它报给 leader，用来算多数派水位。
        #: 与 ``last_lsn`` 分开是必须的：一个只投票不带数据的 witness 节点
        #: 也能听到集群进度，如果拿它当 match_lsn，leader 会误以为多数派
        #: 已经持有该 LSN，从而允许提升一个落后的副本。
        self.match_lsn = 0
        #: 多数派确认过的提交水位（本节点目前知道的上界）。
        self.commit_lsn = 0
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
        self.match_lsn = max(0, int(raw.get("match_lsn", 0)))
        self.commit_lsn = max(0, int(raw.get("commit_lsn", 0)))

    def save(self) -> None:
        with self._lock:
            if self.path is None:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "term": int(self.term),
                "voted_for": self.voted_for,
                "last_lsn": int(self.last_lsn),
                "match_lsn": int(self.match_lsn),
                "commit_lsn": int(self.commit_lsn),
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
        """记录"听到过"的最高 LSN（投票安全用，不代表持有数据）。"""
        with self._lock:
            value = max(0, int(lsn))
            if value > self.last_lsn:
                self.last_lsn = value
                self.save()

    def observe_applied(self, lsn: int) -> None:
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
                changed = True
            if value > self.last_lsn:
                self.last_lsn = value
                changed = True
            if changed:
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
        timeout: float = 2.0,
        token: str | None = None,
        pre_vote: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "vote_request",
            "term": int(term),
            "candidate_id": candidate_id,
            "last_lsn": int(last_lsn),
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
        self._last_heartbeat_at: float | None = None
        self._last_heartbeat_from: str | None = None

    def observe_lsn(self, lsn: int) -> None:
        """记录本节点已经追平到的 LSN；投票时用它拒绝落后的候选。"""
        with self._lock:
            self.state.observe_lsn(lsn)

    def observe_applied(self, lsn: int) -> None:
        """记录本节点**真正复制到**的 LSN；心跳响应会带上它，用于算水位。"""
        with self._lock:
            self.state.observe_applied(lsn)

    def observe_commit(self, lsn: int) -> None:
        """记录已知的多数派提交水位（由心跳轮次或上层运维工具推进）。"""
        with self._lock:
            self.state.observe_commit(lsn)

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

    def request_vote(
        self,
        term: int,
        candidate_id: str,
        last_lsn: int,
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
        if pre_vote:
            with self._lock:
                would_grant = (
                    requested >= self.state.term
                    and self.state.voted_for in (None, candidate)
                    and candidate_lsn >= self.state.last_lsn
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
            if candidate_lsn < self.state.last_lsn:
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
                "match_lsn": self.state.match_lsn,
                "commit_lsn": self.state.commit_lsn,
                "last_heartbeat_at": self._last_heartbeat_at,
                "last_heartbeat_from": self._last_heartbeat_from,
                "peers": sorted(
                    pid for pid in self.peers if pid != self.node_id
                ),
            }

    def heartbeat(
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
            self._last_heartbeat_at = time.time()
            self._last_heartbeat_from = leader
            return {
                "type": "heartbeat_response",
                "term": self.state.term,
                "ok": True,
                "match_lsn": self.state.match_lsn,
                "commit_lsn": self.state.commit_lsn,
            }

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
                    except (OSError, ValueError, StorageError) as exc:
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
        for peer_id, address in peers:
            try:
                response = QuorumClient.request_vote(
                    address[0],
                    int(address[1]),
                    term,
                    self.node_id,
                    candidate_lsn,
                    timeout=self.timeout,
                    token=self.token,
                    pre_vote=True,
                )
            except (OSError, ValueError, StorageError) as exc:
                errors.append(f"{peer_id}: {exc}")
                continue
            if response.get("granted"):
                votes.add(peer_id)
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
        self.observe_applied(candidate_lsn)
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
            try:
                self._handle(conn)
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
    ) -> None:
        self.node = node
        self.last_lsn = last_lsn
        self.interval = max(0.05, float(interval))
        self.on_higher_term = on_higher_term
        self.last_acks = 0
        self.last_errors: list[str] = []
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
            thread.join(timeout=max(1.0, self.interval + 0.5))

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            term_before = self.node.state.term
            try:
                acks, errors = self.node.heartbeat_round(self.last_lsn())
            except (OSError, ValueError, StorageError) as exc:
                acks, errors = 0, [str(exc)]
            self.last_acks = acks
            self.last_errors = errors
            self.last_quorum_commit_lsn = self.node.quorum_commit_lsn
            term_after = self.node.state.term
            self.last_observed_term = term_after
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
