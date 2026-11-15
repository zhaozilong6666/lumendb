"""基于共享文件和 TTL 的写租约 MVP。

``PromotionFence`` 解决的是“副本已经提升、旧主库看到 marker 后停写”。如果
旧主库与 fence 文件失联，它仍然可能继续写。租约再加一道自限：主库必须
周期性 ``renew``，超过 TTL 没有续租就自行拒绝写入；新节点只有在旧租约
过期后才能 ``acquire``。

边界：

- 依赖共享文件系统和大致同步的墙钟；没有 NTP 保证时会受时钟漂移影响。
- 租约不是 Raft leader lease：没有心跳 RPC、没有 quorum 续租、没有日志
  协商。它只把“旧主库永久写”缩小成“最多写一个 TTL 窗口”。
- ``acquire`` 不会抢占仍然有效的租约，即使候选任期更高；调用方需要等旧
  租约过期，或先确认旧主库已经停止。
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from ..errors import NotSupportedError, StorageError


class Lease:
    """一个持有者、一个任期、一个到期时间。"""

    def __init__(
        self,
        path: str | Path,
        node_id: str,
        *,
        ttl: float = 10.0,
    ) -> None:
        self.path = Path(path)
        self.node_id = str(node_id)
        self.ttl = max(0.1, float(ttl))

    def read(self) -> dict | None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise StorageError(
                "租约状态损坏，拒绝用默认租约继续",
                detail=str(self.path),
            ) from exc
        if not isinstance(raw, dict):
            raise StorageError("租约状态必须是 JSON 对象", detail=str(self.path))
        return {
            "holder": raw.get("holder"),
            "term": max(0, int(raw.get("term", 0))),
            "expires_at": float(raw.get("expires_at", 0.0)),
        }

    def _write(self, holder: str, term: int, expires_at: float) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "holder": holder,
            "term": int(term),
            "expires_at": float(expires_at),
        }
        tmp = self.path.with_name(self.path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"), ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def acquire(
        self,
        term: int = 0,
        *,
        now: float | None = None,
    ) -> float:
        """获取租约；仍有效的其他持有者会让这次获取失败。"""
        current_time = time.time() if now is None else float(now)
        current = self.read()
        if current is not None:
            holder = current.get("holder")
            if (
                holder
                and holder != self.node_id
                and float(current["expires_at"]) > current_time
            ):
                raise NotSupportedError(
                    "租约仍被其他节点持有，拒绝抢占",
                    detail=(
                        f"holder={holder} expires_at={current['expires_at']} "
                        f"now={current_time}"
                    ),
                )
            if int(current["term"]) > int(term):
                raise NotSupportedError(
                    "租约任期比候选任期更高，拒绝回退",
                    detail=f"lease_term={current['term']} requested={term}",
                )
        expires_at = current_time + self.ttl
        self._write(self.node_id, term, expires_at)
        return expires_at

    def renew(self, *, now: float | None = None) -> float:
        """续租；只有当前持有者且租约未过期时才能续。"""
        current_time = time.time() if now is None else float(now)
        current = self.read()
        if current is None:
            raise NotSupportedError("租约不存在，不能续租")
        if current.get("holder") != self.node_id:
            raise NotSupportedError(
                "租约不属于当前节点，不能续租",
                detail=f"holder={current.get('holder')} node={self.node_id}",
            )
        if float(current["expires_at"]) <= current_time:
            raise NotSupportedError(
                "租约已经过期，不能续租；必须重新 acquire",
                detail=f"expires_at={current['expires_at']} now={current_time}",
            )
        expires_at = current_time + self.ttl
        self._write(self.node_id, int(current["term"]), expires_at)
        return expires_at

    def assert_writable(self, *, now: float | None = None) -> None:
        """Database 的写回调：不是当前持有者或已过期就拒绝写入。"""
        current_time = time.time() if now is None else float(now)
        current = self.read()
        if current is None:
            raise NotSupportedError(
                "没有有效写租约，拒绝写入",
                hint="主库必须先 acquire 或 renew 租约",
            )
        if current.get("holder") != self.node_id:
            raise NotSupportedError(
                "写租约属于其他节点，拒绝写入",
                detail=f"holder={current.get('holder')} node={self.node_id}",
            )
        if float(current["expires_at"]) <= current_time:
            raise NotSupportedError(
                "写租约已经过期，旧主库必须停止写入",
                detail=f"expires_at={current['expires_at']} now={current_time}",
            )

    def release(self) -> None:
        """当前持有者主动释放租约；删除失败时保留文件，让 TTL 兜底。"""
        current = self.read()
        if current is None:
            return
        if current.get("holder") != self.node_id:
            raise NotSupportedError(
                "租约不属于当前节点，不能释放",
                detail=f"holder={current.get('holder')} node={self.node_id}",
            )
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


__all__ = ["Lease"]
