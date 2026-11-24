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
import contextlib
from collections import OrderedDict
from typing import Optional

from ..errors import StorageError
from .page import INVALID_PAGE_ID, PAGE_SIZE, Page, PageType
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
        #: 每张表一把"形状锁"（页链分配新页 / 目录字节），按表名共享。
        #: ``HeapFile`` 是每次执行新建的短命对象，锁不能挂在它身上，否则两个
        #: 写者各拿一把锁等于没锁（见 docs/concurrency.md 第 2 步的前置）。
        self._shape_locks: dict[str, threading.RLock] = {}
        # page_id -> Page，按 LRU 顺序：最左是最近最少使用
        self._frames: "OrderedDict[int, Page]" = OrderedDict()
        self._pins: dict[int, int] = {}
        self._dirty: set[int] = set()
        # xid -> [(page_id, offset, before_image)]，用于显式 ROLLBACK 的页级撤销
        self._undo: dict[int, list[tuple[int, int, bytes]]] = {}
        # 新分配、尚未写入 WAL 的页；第一次更新时先记一条完整页镜像。
        self._fresh_pages: set[int] = set()
        #: 逻辑回滚：并发 DML 下堆页/B+ 树页不记物理原像；死行靠 xmin/xmax
        #: 可见性与 VACUUM 回收。物理 undo 在并发写者交错时必然撕裂。
        self.logical_abort = False
        #: 线程局部覆盖（None=跟随 logical_abort，True=强制逻辑，False=强制物理）：
        #: DDL 需要物理原像来撤回 TRUNCATE/DROP，DML 的目录保存则不能物理回滚。
        self._undo_override = threading.local()
        # page_id -> 单调版本号：任何一次内容变化（写日志、新分配、从磁盘换入）
        # 都 +1。B+ 树的内部页解码缓存用它当键的一部分——不能拿 pageLSN 当键：
        # 页被换出再换入后 pageLSN 归零，缓存里 (页号, 0) 的旧条目会一直命中，
        # 按旧分隔键路由（实测：父页已收下新孩子，缓存仍回旧 children）。
        self._page_versions: dict[int, int] = {}
        self._version_seq = 0
        self.stats = BufferPoolStats()

    # -- 查询状态 -----------------------------------------------------------

    def shape_lock(self, table_name: str) -> threading.RLock:
        """返回某张表的形状锁（页链追加 / 新页分配用），同库同名共享一把。"""
        with self._lock:
            lock = self._shape_locks.get(table_name)
            if lock is None:
                lock = threading.RLock()
                self._shape_locks[table_name] = lock
            return lock

    @property
    def size(self) -> int:
        return len(self._frames)

    @property
    def pinned(self) -> int:
        return sum(1 for n in self._pins.values() if n > 0)

    def is_dirty(self, page_id: int) -> bool:
        return page_id in self._dirty

    def page_version(self, page_id: int) -> int:
        """页内容的单调版本号（0 = 从未被读过/写过）。供解码缓存做失效键。"""
        with self._lock:
            return self._page_versions.get(page_id, 0)

    def _bump_version(self, page_id: int) -> None:
        self._version_seq += 1
        self._page_versions[page_id] = self._version_seq

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
            self._bump_version(page_id)
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
            self._bump_version(page_id)
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
        """标脏并推进页版本。

        ``log_page_update`` 已经为日志路径推进过版本；但复制应用、目录页
        直接改字节这类路径会绕过它直接 ``mark_dirty``。版本不推进的话，
        B+ 树内部页解码缓存会继续命中旧条目，刷盘线程也可能把“快照后又被
        改过”的页误判成未变。
        """
        with self._lock:
            self._dirty.add(page.page_id)
            self._bump_version(page.page_id)

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
        # 页 latch 先串行同一页的"记日志 + 盖 pageLSN"；全局 _lock 只保护
        # fresh/dirty/undo/version 这些簿记，不再把 WAL append 的磁盘等待
        # 包进缓冲池全局锁里。否则多线程写不同页也会在 WAL 上排成一队。
        with page.latch:
            with self._lock:
                fresh = page.page_id in self._fresh_pages
                logical = self._undo_is_logical(page)
                if not clr and not logical:
                    self._undo.setdefault(xid, []).append(
                        (page.page_id, offset, bytes(old))
                    )
                self._dirty.add(page.page_id)
                self._bump_version(page.page_id)
            if fresh:
                # 页分配与初始化不单独记日志；第一次修改前补一条完整镜像，
                # 让副本/恢复不依赖主库磁盘上的页分配状态。
                self._wal.log_page_image(xid, page.page_id, bytes(page.buf))
                with self._lock:
                    self._fresh_pages.discard(page.page_id)
            lsn = self._wal.log_page_update(
                xid, page.page_id, offset, old, new, clr=clr, no_undo=logical
            )
            page.page_lsn = lsn
            return lsn

    def _undo_is_logical(self, page: Page) -> bool:
        """这一页的回滚是否走逻辑路径（不记物理原像）。"""
        override = getattr(self._undo_override, "value", None)
        if override is not None:
            return override
        if not self.logical_abort:
            return False
        return page.page_type in (
            PageType.HEAP,
            PageType.BTREE_INTERNAL,
            PageType.BTREE_LEAF,
        )

    @contextlib.contextmanager
    def logical_undo_scope(self):
        """本线程内强制把页写入标成"无物理原像"（并发 DML 的目录保存）。"""
        previous = getattr(self._undo_override, "value", None)
        self._undo_override.value = True
        try:
            yield
        finally:
            self._undo_override.value = previous

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
        # 不在 _lock 里应用页原像：应用过程要拿页 latch，锁序固定
        # 「页 latch → 缓冲池锁」，与 log_page_update 一致；否则会和并发
        # 写者形成 ABBA。
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
        """逆序应用页原像；本方法不要求调用方持有 ``_lock``。"""
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
        """把单个脏页写回磁盘（先刷 WAL）。

        不能拿着 ``_lock`` 直接写 ``page.buf``：写者持有页 latch 时可能刚把
        pageLSN 推进到新 LSN、还没改字节，这时磁盘若写入旧内容，崩溃恢复会
        因为 pageLSN 已变而跳过 redo，静默丢掉这次修改。这里先短暂拿
        ``_lock`` pin 页并记下版本，随后按「页 latch → 拷贝」拿一致快照，
        释放 latch 后再写盘；写完只在版本未变时清脏标记，写者若在写盘期间
        改过页，脏标记会保留到下一次 flush。
        """
        with self._lock:
            page = self._frames.get(page_id)
            if page is None:
                # 页已被淘汰（淘汰时写过），只是脏标记残留。
                self._dirty.discard(page_id)
                return
            if page_id not in self._dirty:
                return
            # 临时 pin，防止拿到 page 引用后立刻被淘汰/复用。
            self._pins[page_id] = self._pins.get(page_id, 0) + 1
            version = self._page_versions.get(page_id, 0)
        try:
            with page.latch:
                data = bytes(page.buf)
                page_lsn = page.page_lsn
            if page_lsn > 0:
                self._wal.flush_upto(page_lsn)
            self._disk.write_page(page_id, data)
            with self._lock:
                if self._page_versions.get(page_id, 0) == version:
                    self._dirty.discard(page_id)
                self.stats.page_writes += 1
        finally:
            self.unpin(page_id)

    def flush_all(self) -> int:
        """刷掉所有脏页并 fsync 数据文件，返回实际刷过的页次数。

        并发写者可能在两轮之间把页重新标脏；checkpoint 必须等脏页集合真正
        空掉再推进 checkpoint_lsn，否则“WAL 记录早于 checkpoint、页却还没
        落盘”的页会在恢复时被跳过。这里循环到 ``_dirty`` 为空：每轮开始
        时短暂持 ``_lock`` 取快照，flush 本身不持全局锁。

        ``flush_page`` 在写盘期间发现版本变了会把脏标记留下，所以本轮写完
        仍可能有余项，下一轮自然收敛；返回的是所有轮次实际 flush 的次数。
        """
        flushed = 0
        while True:
            with self._lock:
                dirty = list(self._dirty)
            if not dirty:
                break
            for page_id in dirty:
                self.flush_page(page_id)
                flushed += 1
        self._disk.sync()
        return flushed

    @contextlib.contextmanager
    def clean_barrier(self):
        """把脏页刷空，并在 ``_lock`` 内保持"没有脏页"直到退出。

        checkpoint 需要在"所有页都干净"的点上写 CHECKPOINT 记录：否则并发
        写者可能在 flush 与 checkpoint 之间标脏一页，而它的 WAL 记录 LSN 又
        早于 checkpoint_lsn，恢复时会被跳过。这里循环 flush 直到 ``_dirty``
        为空，然后拿着 ``_lock`` 让调用方追加 CHECKPOINT 记录——写者要标脏
        必须先拿 ``_lock``，所以屏障内不会出现新的 dirty 页。

        调用方只应在屏障内做"追加 CHECKPOINT 记录"这种短操作，不要做 fsync
        或页写；``flush_all`` 的 fsync 在进入屏障前已经完成。
        """
        while True:
            self.flush_all()
            self._lock.acquire()
            if not self._dirty:
                break
            self._lock.release()
        try:
            yield
        finally:
            self._lock.release()

    def _apply_wal_rule(self, page: Page) -> None:
        """淘汰路径的 WAL 规则执行点（flush_page 用自己抓到的快照 LSN）。"""
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
        # flush_all 会短暂 pin 页并在页 latch 下取快照；不要在持有全局
        # _lock 时调用，否则与「页 latch → _lock」的写者形成 ABBA。
        self.flush_all()
        with self._lock:
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
