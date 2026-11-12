"""缓冲池的单元测试。

这里要证明三件事：
1. LRU 淘汰顺序正确，且被 pin 的页绝不会被淘汰；
2. 脏页淘汰/刷盘时，WAL 一定先落盘（WAL 规则）；
3. 缓冲池不会把"同一个页"缓存成两份。
"""

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
