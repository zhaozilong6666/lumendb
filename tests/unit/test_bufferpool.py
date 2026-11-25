"""缓冲池的单元测试。

这里要证明三件事：
1. LRU 淘汰顺序正确，且被 pin 的页绝不会被淘汰；
2. 脏页淘汰/刷盘时，WAL 一定先落盘（WAL 规则）；
3. 缓冲池不会把"同一个页"缓存成两份。
"""

import threading

import pytest

from lumendb.errors import StorageError
from lumendb.storage.bufferpool import BufferPoolManager
from lumendb.storage.page import INVALID_PAGE_ID, PAGE_SIZE, HeapPage, Page
from lumendb.storage.pager import DiskManager
from lumendb.storage.wal import WAL


@pytest.fixture
def env(tmp_path):
    dm = DiskManager(tmp_path / "data.db")
    wal = WAL(tmp_path / "wal.log")
    bp = BufferPoolManager(dm, wal, pool_size=3)
    yield bp, dm, wal
    bp.close()
    wal.close()
    dm.close()


def init_heap_page(bp, page_id=None):
    page = bp.new_page(HeapPage)
    page.init_heap(page.page_id)
    return page


class TestFetchAndPin:
    def test_new_page_is_pinned(self, env):
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        assert bp.size == 1
        assert bp._pins[page.page_id] == 1
        assert bp.is_dirty(page.page_id)
        bp.unpin(page.page_id)

    def test_hit_and_miss_counters(self, env):
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        bp.unpin(page.page_id)
        page2 = bp.fetch_page(page.page_id, HeapPage)
        assert bp.stats.misses == 0
        assert bp.stats.hits == 1
        bp.fetch_page(page.page_id, HeapPage)
        assert bp.stats.hits == 2
        bp.unpin(page.page_id, dirty=False)
        bp.unpin(page.page_id, dirty=False)

    def test_same_page_never_cached_twice(self, env):
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        bp.unpin(page.page_id)
        a = bp.fetch_page(page.page_id, HeapPage)
        b = bp.fetch_page(page.page_id, HeapPage)
        assert a.buf is b.buf  # 同一块字节
        assert bp.size == 1
        bp.unpin(page.page_id)
        bp.unpin(page.page_id)

    def test_fetch_invalid_page(self, env):
        bp, dm, wal = env
        with pytest.raises(StorageError):
            bp.fetch_page(INVALID_PAGE_ID)

    def test_generic_page_request_accepts_any_subclass(self, env):
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        bp.unpin(page.page_id)
        got = bp.fetch_page(page.page_id)  # 默认 Page：不关心具体类型
        assert isinstance(got, Page)
        assert got.buf is page.buf
        bp.unpin(page.page_id)

    def test_views_share_the_same_buffer(self, env):
        """同一页的两种视图必须共享字节：否则改动会丢。"""
        bp, dm, wal = env
        raw_page = bp.new_page()  # 以通用 Page 加载
        heap_view = raw_page.view(HeapPage)
        heap_view.init_heap(raw_page.page_id)
        bp.unpin(raw_page.page_id)

        fetched = bp.fetch_page(raw_page.page_id, HeapPage)
        assert isinstance(fetched, HeapPage)
        fetched.insert_record(b"shared")
        bp.unpin(raw_page.page_id)

        assert raw_page.get_u16(2) > 0  # 通用视图也看到了槽位数变化
        assert HeapPage(raw_page.page_id, bytes(raw_page.buf)).get_record(0) == b"shared"


class TestUnpin:
    def test_double_unpin_is_an_error(self, env):
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        bp.unpin(page.page_id)
        with pytest.raises(StorageError):
            bp.unpin(page.page_id)

    def test_unpin_unknown_page(self, env):
        bp, dm, wal = env
        with pytest.raises(StorageError):
            bp.unpin(999)

    def test_dirty_flag_on_unpin(self, env):
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        bp.unpin(page.page_id, dirty=True)
        assert bp.is_dirty(page.page_id)


class TestEviction:
    def test_lru_order(self, env):
        bp, dm, wal = env
        pages = []
        for _ in range(3):
            p = bp.new_page(HeapPage)
            bp.unpin(p.page_id)
            pages.append(p.page_id)

        # 访问第 0 页，让它变成"最近使用"
        bp.fetch_page(pages[0], HeapPage)
        bp.unpin(pages[0])

        # 再取新页：应该淘汰 pages[1]（最久未使用）
        bp.new_page(HeapPage)
        assert pages[1] not in bp._frames
        assert pages[0] in bp._frames
        assert pages[2] in bp._frames

    def test_pinned_pages_are_never_evicted(self, env):
        bp, dm, wal = env
        held = []
        for _ in range(3):
            held.append(bp.new_page(HeapPage))  # 全部保持 pin

        with pytest.raises(StorageError) as exc:
            bp.new_page(HeapPage)
        assert "pin" in str(exc.value)
        for p in held:
            assert p.page_id in bp._frames

    def test_eviction_writes_dirty_page(self, env):
        bp, dm, wal = env
        ids = []
        for _ in range(3):
            p = bp.new_page(HeapPage)
            p.init_heap(p.page_id)
            p.insert_record(b"payload")
            bp.unpin(p.page_id, dirty=True)
            ids.append(p.page_id)

        bp.new_page(HeapPage)  # 触发淘汰 ids[0]
        raw = dm.read_page(ids[0])
        hs = HeapPage(ids[0], raw)
        assert hs.get_record(0) == b"payload"
        assert ids[0] not in bp._dirty


class TestWalRule:
    def test_dirty_page_is_not_written_before_wal_is_durable(self, env):
        """核心断言：写页之前，覆盖该页的日志必须已经 fsync。"""
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        page.init_heap(page.page_id)

        lsn = bp.log_page_update(1, page, 40, b"", b"abc")
        assert page.page_lsn == lsn
        assert wal.durable_lsn < lsn  # 日志还没落盘

        bp.flush_page(page.page_id)
        assert wal.durable_lsn >= lsn  # 先刷了日志
        assert dm.read_page(page.page_id) == bytes(page.buf)
        bp.unpin(page.page_id)

    def test_flush_does_not_clear_dirty_flag_if_page_changes_after_snapshot(
        self, env, monkeypatch
    ):
        """刷盘快照取完后写者又改页：脏标记必须保留，下次 flush 写新内容。"""
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        page.init_heap(page.page_id)
        bp.log_page_update(1, page, 40, b"", b"old")
        pid = page.page_id

        original = dm.write_page
        entered = threading.Event()
        release = threading.Event()

        def slow_write(page_id, data):
            if page_id == pid:
                entered.set()
                assert release.wait(5)
            original(page_id, data)

        monkeypatch.setattr(dm, "write_page", slow_write)
        flusher = threading.Thread(target=lambda: bp.flush_page(pid))
        flusher.start()
        assert entered.wait(5)

        # flush 已取完快照、还没清脏标记；此时写者推进 pageLSN 并改字节。
        with page.latch:
            old = bytes(page.buf[40:43])
            bp.log_page_update(1, page, 40, old, b"new")
            page.buf[40:43] = b"new"
            bp.mark_dirty(page)
        release.set()
        flusher.join(5)
        assert not flusher.is_alive()
        assert bp.is_dirty(pid), "快照后的写者修改不能被 flush 清掉"

        monkeypatch.setattr(dm, "write_page", original)
        bp.flush_page(pid)
        assert dm.read_page(pid) == bytes(page.buf)
        assert not bp.is_dirty(pid)
        bp.unpin(pid)

    def test_eviction_also_respects_wal_rule(self, env):
        bp, dm, wal = env
        victim = bp.new_page(HeapPage)
        victim.init_heap(victim.page_id)
        lsn = bp.log_page_update(1, victim, 40, b"", b"xyz")
        bp.unpin(victim.page_id, dirty=True)

        # 把池填满，让 victim 成为 LRU 受害者
        for _ in range(2):
            p = bp.new_page(HeapPage)
            p.init_heap(p.page_id)
            bp.unpin(p.page_id)

        bp.new_page(HeapPage)  # 触发淘汰 victim，必须先刷 WAL
        assert victim.page_id not in bp._frames
        assert wal.durable_lsn >= lsn
        assert dm.read_page(victim.page_id) == bytes(victim.buf)

    def test_clean_barrier_flushes_pages_dirtied_during_flush(self, env, monkeypatch):
        """屏障内必须看到空脏页集合；flush 期间新标脏的页由下一轮补上。"""
        bp, dm, wal = env
        first = bp.new_page(HeapPage)
        first.init_heap(first.page_id)
        bp.unpin(first.page_id, dirty=True)
        original = bp.flush_page
        added = {"done": False}

        def flush_then_dirty(page_id):
            original(page_id)
            if not added["done"]:
                added["done"] = True
                late = bp.new_page(HeapPage)
                late.init_heap(late.page_id)
                bp.unpin(late.page_id, dirty=True)

        monkeypatch.setattr(bp, "flush_page", flush_then_dirty)
        with bp.clean_barrier():
            assert not bp._dirty
            assert added["done"]

    def test_flush_all_repeats_until_dirty_set_is_empty(self, env, monkeypatch):
        """flush 期间新标脏的页也必须落盘，否则 checkpoint 会漏页。"""
        bp, dm, wal = env
        first = bp.new_page(HeapPage)
        first.init_heap(first.page_id)
        bp.unpin(first.page_id, dirty=True)
        original = bp.flush_page
        added = {"done": False}

        def flush_then_dirty(page_id):
            original(page_id)
            if not added["done"]:
                added["done"] = True
                late = bp.new_page(HeapPage)
                late.init_heap(late.page_id)
                bp.unpin(late.page_id, dirty=True)

        monkeypatch.setattr(bp, "flush_page", flush_then_dirty)
        flushed = bp.flush_all()
        assert flushed >= 2
        assert not bp._dirty

    def test_clean_barrier_waits_for_inflight_page_writes(self, env):
        """checkpoint 的最终 fsync 不能漏掉一个已经在飞的 flush_page。

        旧实现里 flush_all 在锁外 fsync；如果另一个线程随后才把脏页写盘，
        checkpoint 可能在这个写盘前推进 checkpoint_lsn，崩溃恢复就会跳过
        该页的 redo（历史 COUNT=7486 / 页链尾部丢失的窗口之一）。
        """
        bp, dm, wal = env
        entered = threading.Event()

        def run_barrier():
            with bp.clean_barrier():
                entered.set()

        # 模拟一个已经开始、尚未完成的页写。
        with bp._lock:
            bp._inflight_writes += 1
        thread = threading.Thread(target=run_barrier)
        thread.start()
        try:
            assert not entered.wait(0.2), "屏障没有等待在飞的页写"
            with bp._lock:
                bp._inflight_writes -= 1
                bp._write_cv.notify_all()
            assert entered.wait(2)
        finally:
            with bp._lock:
                if bp._inflight_writes:
                    bp._inflight_writes -= 1
                    bp._write_cv.notify_all()
            thread.join(5)

    def test_mark_dirty_bumps_page_version(self, env):
        """绕过 log_page_update 的直接写也要让解码缓存/刷盘版本失效。"""
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        page.init_heap(page.page_id)
        before = bp.page_version(page.page_id)
        bp.mark_dirty(page)
        assert bp.page_version(page.page_id) > before

    def test_log_page_update_marks_dirty_and_sets_lsn(self, env):
        bp, dm, wal = env
        page = bp.new_page(HeapPage)
        page.init_heap(page.page_id)
        bp.unpin(page.page_id)
        bp.fetch_page(page.page_id, HeapPage)
        bp.log_page_update(7, page, 40, b"old", b"newer")
        assert page.page_lsn > 0
        assert bp.is_dirty(page.page_id)
        bp.unpin(page.page_id)


class TestFlush:
    def test_flush_all_writes_everything(self, env):
        bp, dm, wal = env
        ids = []
        for _ in range(3):
            p = bp.new_page(HeapPage)
            p.init_heap(p.page_id)
            p.insert_record(b"x" * 10)
            bp.unpin(p.page_id, dirty=True)
            ids.append(p.page_id)

        assert bp.flush_all() == 3
        for pid in ids:
            raw = dm.read_page(pid)
            hs = HeapPage(pid, raw)
            assert hs.get_record(0) == b"x" * 10
        assert not bp._dirty

    def test_flush_clean_page_is_noop(self, env):
        bp, dm, wal = env
        bp.new_page(HeapPage)
        page_id = list(bp._frames)[0]
        bp.unpin(page_id)
        bp.flush_page(page_id)  # 未标脏，第一次因 new_page 是脏的会写
        assert bp.stats.page_writes == 1
        bp.flush_page(page_id)
        assert bp.stats.page_writes == 1


class TestStats:
    def test_hit_rate(self, env):
        bp, dm, wal = env
        p = bp.new_page(HeapPage)
        bp.unpin(p.page_id)
        for _ in range(3):
            bp.fetch_page(p.page_id, HeapPage)
            bp.unpin(p.page_id)
        assert bp.stats.accesses == 3
        assert bp.stats.hit_rate == 1.0

    def test_stats_snapshot_is_serializable(self, env):
        bp, dm, wal = env
        d = bp.stats.as_dict()
        assert set(d) == {
            "hits",
            "misses",
            "evictions",
            "page_writes",
            "wal_flushes",
            "hit_rate",
        }
