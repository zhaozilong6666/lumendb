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
- ``acquire`` / ``renew`` / ``release`` 用 ``<lease>.lock`` 做跨进程串行化；
  进程崩溃留下的锁文件不会被自动删除，必须人工确认后清理，避免误抢。
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
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
        lock_timeout: float = 5.0,
    ) -> None:
        self.path = Path(path)
        self.node_id = str(node_id)
        self.ttl = max(0.1, float(ttl))
        self.lock_timeout = max(0.05, float(lock_timeout))
        self.lock_path = self.path.with_name(self.path.name + ".lock")

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

    @contextmanager
    def _exclusive(self):
        """用 O_EXCL 锁文件把 acquire/renew/release 串行化。

        锁文件残留时按失败关闭处理：不会自动删除未知进程留下的锁。
        """
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.lock_timeout
        fd = None
        while True:
            try:
                fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
                break
            except FileExistsError:
                if time.monotonic() >= deadline:
                    raise StorageError(
                        "租约锁等待超时，可能存在残留锁文件",
                        detail=str(self.lock_path),
                    )
                time.sleep(0.01)
        try:
            os.write(fd, str(os.getpid()).encode("ascii"))
            yield
        finally:
            try:
                os.close(fd)
            finally:
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

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
        with self._exclusive():
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
        with self._exclusive():
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

    def assert_writable(
        self,
        *,
        now: float | None = None,
        term: int | None = None,
    ) -> None:
        """Database 的写回调：不是当前持有者、已过期或任期落后就拒绝写入。"""
        current_time = time.time() if now is None else float(now)
        current = self.read()
        if current is None:
            raise NotSupportedError(
                "没有有效写租约，拒绝写入",
                hint="主库必须先 acquire 或 renew 租约",
            )
        if term is not None and int(term) > int(current["term"]):
            raise NotSupportedError(
                "本节点任期已过期，旧主库必须停止写入",
                detail=f"observed_term={term} lease_term={current['term']}",
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
        with self._exclusive():
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


class LeaseFence:
    """把租约和本节点当前任期组合成 ``Database.write_fence``。

    ``term_provider`` 通常返回 ``QuorumNode.state.term``。旧主库收到更高任期
    心跳后，即使租约还没过期，下一笔写也会立即被拒绝，不必等到 TTL。
    """

    def __init__(
        self,
        lease: Lease,
        term_provider: Callable[[], int] | None = None,
    ) -> None:
        self.lease = lease
        self._term_provider = term_provider

    def assert_writable(self) -> None:
        term = None
        if self._term_provider is not None:
            raw = self._term_provider()
            term = int(raw) if raw is not None else None
        self.lease.assert_writable(term=term)

    def __call__(self) -> None:
        self.assert_writable()


class LeaseKeeper:
    """后台续租线程；只负责续租，不负责重新 acquire。"""

    def __init__(
        self,
        lease: Lease,
        interval: float | None = None,
        *,
        renew=None,
        on_failure=None,
    ) -> None:
        self.lease = lease
        self.interval = max(
            0.05,
            float(interval) if interval is not None else lease.ttl / 3,
        )
        self._renew = renew if renew is not None else lease.renew
        self._on_failure = on_failure
        self.last_error: str | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def lost(self) -> bool:
        """续租失败后为 True；表示当前节点不应再作为可写主库。"""
        return self.last_error is not None

    def start(self) -> "LeaseKeeper":
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="lumendb-lease-keeper",
                daemon=True,
            )
            self._thread.start()
            return self

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            thread = self._thread
            self._thread = None
        if thread is not None:
            thread.join(timeout=max(1.0, self.interval + 0.5))

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self._renew()
                self.last_error = None
            except (NotSupportedError, StorageError) as exc:
                self._notify_failure(str(exc))
                return
            except BaseException as exc:  # noqa: BLE001 - keep keeper alive-ish
                self._notify_failure(str(exc))
                return

    def _notify_failure(self, message: str) -> None:
        self.last_error = message
        if self._on_failure is None:
            return
        try:
            self._on_failure(self)
        except BaseException as exc:  # noqa: BLE001 - callback must not mask loss
            self.last_error = f"{message}; on_failure: {exc}"

    def __enter__(self) -> "LeaseKeeper":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


__all__ = ["Lease", "LeaseFence", "LeaseKeeper"]
