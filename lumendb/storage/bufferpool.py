"""缓冲池管理器。

Buffer Pool 是"磁盘页"与"内存页"之间的唯一桥梁。它做三件事：

1. **缓存**：把常用页留在内存里，减少系统调用；
2. **pin**：正在被算子使用的页不允许被淘汰——否则算子手里的引用会指向
   一块已经被别人复用的内存，这类 bug 极难复现；
3. **执行 WAL 规则**：脏页淘汰或刷盘之前，先把覆盖它的日志落盘。

第 3 点看起来只是加一行 ``wal.flush_upto(page.page_lsn)``，但它是整个崩溃
恢复正确性的支点。少了它，恢复时重放日志会得到"页里已经是新值、日志却说
应该改"的不可判定状态。
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Optional

from ..errors import StorageError
from .page import INVALID_PAGE_ID, PAGE_SIZE, Page
from .pager import DiskManager
from .wal import WAL


class BufferPoolStats:
    __slots__ = ("hits", "misses", "evictions", "page_writes", "wal_flushes")

    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.page_writes = 0
        self.wal_flushes = 0

    @property
    def accesses(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        return self.hits / self.accesses if self.accesses else 0.0

    def as_dict(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "page_writes": self.page_writes,
            "wal_flushes": self.wal_flushes,
            "hit_rate": round(self.hit_rate, 4),
        }


class BufferPoolManager:
    """固定容量的页缓存。

    所有公开方法都可重入加锁；页对象本身不是线程安全的，调用方在持有页
    期间应避免其他线程同时写同一页（这由更高层的锁管理器负责）。
    """

    def __init__(self, disk: DiskManager, wal: WAL, pool_size: int = 256) -> None:
        if pool_size <= 2:
            raise ValueError("pool_size 至少为 3（需要留出淘汰余量）")
        self._disk = disk
        self._wal = wal
        self.pool_size = pool_size
        self._lock = threading.RLock()
        # page_id -> Page，按 LRU 顺序：最左是最近最少使用
        self._frames: "OrderedDict[int, Page]" = OrderedDict()
        self._pins: dict[int, int] = {}
        self._dirty: set[int] = set()
        # xid -> [(page_id, offset, before_image)]，用于显式 ROLLBACK 的页级撤销
        self._undo: dict[int, list[tuple[int, int, bytes]]] = {}
        # 新分配、尚未写入 WAL 的页；第一次更新时先记一条完整页镜像。
        self._fresh_pages: set[int] = set()
        self.stats = BufferPoolStats()

    # -- 查询状态 -----------------------------------------------------------

    @property
    def size(self) -> int:
        return len(self._frames)

    @property
    def pinned(self) -> int:
        return sum(1 for n in self._pins.values() if n > 0)

    def is_dirty(self, page_id: int) -> bool:
        return page_id in self._dirty

    # -- 获取 / 释放 --------------------------------------------------------

    def fetch_page(self, page_id: int, page_cls: type[Page] = Page) -> Page:
        """取页并 pin，返回的页必须由调用方 ``unpin``。

        帧里永远缓存 ``Page``（纯字节 + 通用页头）；``page_cls`` 只决定返回
        哪种"视图"。视图与缓存共享同一块 ``buf``，所以写入不会丢。
        """
        with self._lock:
            if page_id == INVALID_PAGE_ID:
                raise StorageError("不能取无效页")

            page = self._frames.get(page_id)
            if page is not None:
                self._pins[page_id] = self._pins.get(page_id, 0) + 1
                self._frames.move_to_end(page_id)
                self.stats.hits += 1
                return page if page_cls is Page else page.view(page_cls)

            self.stats.misses += 1
            self._ensure_room()
            raw = self._disk.read_page(page_id)
            page = Page(page_id, raw)
            self._frames[page_id] = page
            self._pins[page_id] = 1
            return page if page_cls is Page else page.view(page_cls)

    def new_page(self, page_cls: type[Page] = Page) -> Page:
        """分配新页并 pin。返回的页处于未初始化状态，由调用方填入内容。"""
        with self._lock:
            self._ensure_room()
            page_id = self._disk.allocate_page()
            raw = self._disk.read_page(page_id)
            page = Page(page_id, raw)
            self._frames[page_id] = page
            self._pins[page_id] = 1
            self._dirty.add(page_id)
            self._fresh_pages.add(page_id)
            return page if page_cls is Page else page.view(page_cls)

    def unpin(self, page_id: int, *, dirty: bool = False) -> None:
        with self._lock:
            if page_id not in self._pins:
                raise StorageError(f"页 {page_id} 不在缓冲池中，无法 unpin")
            if self._pins[page_id] <= 0:
                raise StorageError(f"页 {page_id} 的 pin 计数已为 0，出现重复 unpin")
            self._pins[page_id] -= 1
            if dirty:
                self._dirty.add(page_id)

    def mark_dirty(self, page: Page) -> None:
        with self._lock:
            self._dirty.add(page.page_id)

    # -- 日志与脏页 ---------------------------------------------------------

    def log_page_update(
        self,
        xid: int,
        page: Page,
        offset: int,
        old: bytes,
        new: bytes,
        *,
        clr: bool = False,
    ) -> int:
        """写一条物理页日志，并把 pageLSN 推进到该日志。

        把"写日志 + 设 pageLSN + 标脏"绑成一个操作，是为了让上层没有机会
        只做其中一两个——那是 WAL 规则最常见的破坏方式。
        """
        with self._lock:
            if page.page_id in self._fresh_pages:
                # 页分配与初始化不单独记日志；第一次修改前补一条完整镜像，
                # 让副本/恢复不依赖主库磁盘上的页分配状态。
                self._wal.log_page_image(xid, page.page_id, bytes(page.buf))
                self._fresh_pages.discard(page.page_id)
            lsn = self._wal.log_page_update(
                xid, page.page_id, offset, old, new, clr=clr
            )
            page.page_lsn = lsn
            self._dirty.add(page.page_id)
            if not clr:
                self._undo.setdefault(xid, []).append(
                    (page.page_id, offset, bytes(old))
                )
            return lsn

    def clear_undo(self, xid: int) -> None:
        """事务提交后丢弃页级撤销信息，避免长事务让缓冲池无限增长。"""
        with self._lock:
            self._undo.pop(xid, None)

    def undo_depth(self, xid: int) -> int:
        """返回该事务当前累计的页级撤销记录数，供 SAVEPOINT 打点。"""
        with self._lock:
            return len(self._undo.get(xid, []))

    def undo_xid(self, xid: int) -> int:
        """逆序恢复该事务写过的全部页，并写 CLR 推进 pageLSN。"""
        with self._lock:
            entries = self._undo.pop(xid, [])
            return self._apply_undo_entries(xid, entries)

    def undo_to(self, xid: int, mark: int) -> int:
        """只撤销 ``mark`` 之后的页写，保留更早的撤销记录。

        ``ROLLBACK TO SAVEPOINT`` 使用这个入口：保存点之前的页原像仍然
        需要留给后续事务级 abort；保存点之后的原像在应用后丢弃。
        """
        with self._lock:
            entries = self._undo.get(xid, [])
            if mark < 0:
                mark = 0
            if mark >= len(entries):
                return 0
            tail = entries[mark:]
            del entries[mark:]
            return self._apply_undo_entries(xid, tail)

    def _apply_undo_entries(
        self, xid: int, entries: list[tuple[int, int, bytes]]
    ) -> int:
        """在已持有 ``_lock`` 的前提下逆序应用页原像。"""
        if not entries:
            return 0
        touched: set[int] = set()
        for page_id, offset, old in reversed(entries):
            page = self.fetch_page(page_id)
            try:
                current = bytes(page.buf[offset : offset + len(old)])
                if current == old:
                    continue
                lsn = self.log_page_update(
                    xid=xid, page=page, offset=offset, old=current, new=old, clr=True
                )
                page.buf[offset : offset + len(old)] = old
                # old image 里包含旧的 pageLSN；写回后必须重新盖上 CLR
                # 的 LSN，否则重启 redo 会按旧 LSN 重放原始 UPDATE。
                page.page_lsn = lsn
                touched.add(page_id)
            finally:
                self.unpin(page_id, dirty=True)
        for page_id in touched:
            self.flush_page(page_id)
        return len(touched)

    def flush_page(self, page_id: int) -> None:
        """把单个脏页写回磁盘（先刷 WAL）。"""
        with self._lock:
            page = self._frames.get(page_id)
            if page is None:
                return
            if page_id not in self._dirty:
                return
            self._apply_wal_rule(page)
            self._disk.write_page(page_id, page.buf)
            self._dirty.discard(page_id)
            self.stats.page_writes += 1

    def flush_all(self) -> int:
        """刷掉所有脏页并 fsync 数据文件，返回刷掉的页数。"""
        with self._lock:
            dirty = list(self._dirty)
            for page_id in dirty:
                page = self._frames.get(page_id)
                if page is None:
                    # 页已不在池里（被淘汰时写过），只是脏标记残留
                    self._dirty.discard(page_id)
                    continue
                self._apply_wal_rule(page)
                self._disk.write_page(page_id, page.buf)
                self._dirty.discard(page_id)
                self.stats.page_writes += 1
            self._disk.sync()
            return len(dirty)

    def _apply_wal_rule(self, page: Page) -> None:
        """WAL 规则的唯一执行点。"""
        if page.page_lsn > 0:
            before = self._wal.durable_lsn
            self._wal.flush_upto(page.page_lsn)
            if self._wal.durable_lsn != before:
                self.stats.wal_flushes += 1

    # -- 淘汰 ---------------------------------------------------------------

    def _ensure_room(self) -> None:
        """腾出一个空位；全部页被 pin 时明确报错而不是死等。"""
        while len(self._frames) >= self.pool_size:
            victim = self._find_victim()
            if victim is None:
                raise StorageError(
                    "缓冲池已满且所有页都被 pin 住",
                    detail=f"pool_size={self.pool_size} pinned={self.pinned}",
                    hint="检查是否有算子忘记 unpin，或调大 pool_size",
                )
            self._evict(victim)

    def _find_victim(self) -> Optional[int]:
        for page_id in self._frames.keys():
            if self._pins.get(page_id, 0) == 0:
                return page_id
        return None

    def _evict(self, page_id: int) -> None:
        page = self._frames.pop(page_id)
        self._pins.pop(page_id, None)
        if page_id in self._dirty:
            self._apply_wal_rule(page)
            self._disk.write_page(page_id, page.buf)
            self._dirty.discard(page_id)
            self.stats.page_writes += 1
        self.stats.evictions += 1

    # -- 生命周期 -----------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self.flush_all()
            self._frames.clear()
            self._pins.clear()
            self._dirty.clear()

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<BufferPool size={self.size}/{self.pool_size} "
            f"pinned={self.pinned} dirty={len(self._dirty)} "
            f"hit_rate={self.stats.hit_rate:.2%}>"
        )


__all__ = ["BufferPoolManager", "BufferPoolStats"]
