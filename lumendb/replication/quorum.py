"""跨节点多数派投票，给手动/自动提升加一道仲裁。

这不是完整的 Raft：没有 leader 心跳、没有任期租约、没有日志复制协商，
只实现“一个副本想提升，必须先拿到集群多数派的投票”。它解决的是
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
import time
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
        self.last_lsn = 0
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

    def save(self) -> None:
        with self._lock:
            if self.path is None:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "term": int(self.term),
                "voted_for": self.voted_for,
                "last_lsn": int(self.last_lsn),
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
        with self._lock:
            value = max(0, int(lsn))
            if value > self.last_lsn:
                self.last_lsn = value
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
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "vote_request",
            "term": int(term),
            "candidate_id": candidate_id,
            "last_lsn": int(last_lsn),
        }
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
    def heartbeat(
        host: str,
        port: int,
        term: int,
        leader_id: str,
        last_lsn: int,
        *,
        timeout: float = 2.0,
        token: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "heartbeat",
            "term": int(term),
            "leader_id": str(leader_id),
            "last_lsn": int(last_lsn),
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

    def request_vote(
        self, term: int, candidate_id: str, last_lsn: int
    ) -> dict[str, Any]:
        candidate = _validate_node_id(candidate_id)
        requested = max(0, int(term))
        candidate_lsn = max(0, int(last_lsn))
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

    def heartbeat(
        self, term: int, leader_id: str, last_lsn: int
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
            self.state.observe_lsn(int(last_lsn))
            self._last_heartbeat_at = time.time()
            self._last_heartbeat_from = leader
            return {
                "type": "heartbeat_response",
                "term": self.state.term,
                "ok": True,
            }

    def heartbeat_round(self, last_lsn: int) -> tuple[int, list[str]]:
        """向 peers 发送当前任期心跳，返回 (ack 数, 错误列表)。

        如果 peer 返回更高任期，本节点立即推进任期并清空投票；这使旧主库
        在租约到期前就能从下一次写围栏检查中发现自己已经过期。
        """
        with self._lock:
            term = self.state.term
            acks = {self.node_id}
            errors: list[str] = []
        highest_term = term
        for peer_id, address in self.peers.items():
            if peer_id == self.node_id:
                continue
            try:
                response = QuorumClient.heartbeat(
                    address[0],
                    int(address[1]),
                    term,
                    self.node_id,
                    int(last_lsn),
                    timeout=self.timeout,
                    token=self.token,
                )
            except (OSError, ValueError, StorageError) as exc:
                errors.append(f"{peer_id}: {exc}")
                continue
            response_term = max(0, int(response.get("term", 0)))
            if response_term > highest_term:
                highest_term = response_term
            if response.get("ok") and response_term <= term:
                acks.add(peer_id)
        if highest_term > term:
            with self._lock:
                self.state.term = highest_term
                self.state.voted_for = None
                self.state.save()
            errors.append(f"observed higher term {highest_term}")
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
        return lease.renew(now=now)

    def campaign(
        self,
        last_lsn: int,
        *,
        fence_path: str | Path | None = None,
        lease_path: str | Path | None = None,
        lease_ttl: float = 10.0,
    ) -> int:
        """争取多数派投票；成功返回任期，失败抛 ``NotSupportedError``。

        若给了 ``lease_path``，拿到多数票后还必须获取写租约；租约仍被旧主库
        持有时不会抢占，候选节点需要等 TTL 过期后再重试。
        """
        candidate_lsn = max(0, int(last_lsn))
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
                )
            elif request_type == "heartbeat":
                response = self.heartbeat(
                    int(request.get("term", 0)),
                    str(request.get("leader_id", "")),
                    int(request.get("last_lsn", 0)),
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


__all__ = ["QuorumClient", "QuorumNode", "VoteState"]
