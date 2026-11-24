"""磁盘管理器的单元测试。"""

import os
import threading
import time
from pathlib import Path

import pytest

from lumendb.errors import CorruptedPageError, PageNotFoundError
from lumendb.storage.page import INVALID_PAGE_ID, MAGIC, PAGE_SIZE, PageType
from lumendb.storage.pager import DiskManager


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"


class TestDiskManagerInit:
    def test_creates_file_with_meta_page(self, db_path):
        dm = DiskManager(db_path)
        assert db_path.exists()
        assert dm.num_pages == 1
        assert dm.meta.next_page_id == 1
        assert dm.meta.next_xid == 1
        meta_raw = dm.read_page(0)
        assert len(meta_raw) == PAGE_SIZE
        dm.close()

    def test_reopen_preserves_meta(self, db_path):
        dm = DiskManager(db_path)
        dm.meta.next_xid = 42
        dm.meta.next_page_id = 3
        dm.close()

        dm2 = DiskManager(db_path)
        assert dm2.meta.next_xid == 42
        assert dm2.meta.next_page_id == 3
        dm2.close()

    def test_rejects_foreign_file(self, db_path):
        # 一整页的垃圾数据：大小合法，但魔数不对
        db_path.write_bytes((b"not a database!!" * 300)[:PAGE_SIZE])
        with pytest.raises(CorruptedPageError) as exc:
            DiskManager(db_path)
        assert "魔数" in str(exc.value)

    def test_rejects_unaligned_file(self, db_path):
        db_path.write_bytes(bytes(PAGE_SIZE) * 2 + b"x")
        with pytest.raises(CorruptedPageError) as exc:
            DiskManager(db_path)
        assert "整数倍" in str(exc.value)


class TestPageAllocation:
    def test_allocate_is_monotonic(self, db_path):
        dm = DiskManager(db_path)
        assert dm.allocate_page() == 1
        assert dm.allocate_page() == 2
        assert dm.allocate_page() == 3
        assert dm.num_pages == 4
        assert dm.meta.next_page_id == 4
        dm.close()

    def test_new_page_is_free_and_self_describing(self, db_path):
        import struct

        dm = DiskManager(db_path)
        pid = dm.allocate_page()
        raw = dm.read_page(pid)
        assert struct.unpack_from("<H", raw, 0)[0] == PageType.FREE
        assert struct.unpack_from("<I", raw, 16)[0] == pid
        dm.close()

    def test_read_out_of_range(self, db_path):
        dm = DiskManager(db_path)
        with pytest.raises(PageNotFoundError):
            dm.read_page(1)
        with pytest.raises(PageNotFoundError):
            dm.read_page(-1)
        dm.close()

    def test_write_unallocated_page_rejected(self, db_path):
        dm = DiskManager(db_path)
        with pytest.raises(PageNotFoundError):
            dm.write_page(5, bytes(PAGE_SIZE))
        dm.close()

    def test_write_wrong_size_rejected(self, db_path):
        dm = DiskManager(db_path)
        pid = dm.allocate_page()
        with pytest.raises(Exception):
            dm.write_page(pid, b"short")
        dm.close()


class TestPersistence:
    def test_written_page_survives_reopen(self, db_path):
        dm = DiskManager(db_path)
        pid = dm.allocate_page()
        payload = bytes(range(256)) * 16  # 正好 4096
        dm.write_page(pid, payload)
        dm.sync()
        dm.close()

        dm2 = DiskManager(db_path)
        assert dm2.read_page(pid) == payload
        dm2.close()

    def test_sync_actually_writes_to_disk(self, db_path):
        dm = DiskManager(db_path)
        pid = dm.allocate_page()
        dm.write_page(pid, b"\xAA" * PAGE_SIZE)
        dm.sync()
        # 绕过文件句柄直接读文件，确认内容真的在磁盘上
        with open(db_path, "rb") as fh:
            fh.seek(pid * PAGE_SIZE)
            assert fh.read(PAGE_SIZE) == b"\xAA" * PAGE_SIZE
        dm.close()


class TestContextManager:
    def test_with_statement_closes(self, db_path):
        with DiskManager(db_path) as dm:
            dm.allocate_page()
        assert dm._fh.closed


class TestConcurrentIo:
    def test_write_page_serializes_file_position(self, db_path):
        """刷盘线程并发写不同页时，seek/write 必须原子。

        回归：DiskManager 只有一个共享文件句柄；若 write_page 不持进程内
        IO 锁，线程 A seek 到页 A 后、write 前，线程 B 可能 seek 到页 B，
        A 随后把页 A 写到页 B 的偏移。现场表现为堆页链里出现页号字段
        与目标页号不符的 B+ 树页，读取时抛 ``记录头被截断``。
        """
        dm = DiskManager(db_path)
        page_a = dm.allocate_page()
        page_b = dm.allocate_page()
        real = dm._fh

        class BlockingHandle:
            def __init__(self, inner, blocked_page):
                self.inner = inner
                self.blocked_offset = blocked_page * PAGE_SIZE
                self.entered = threading.Event()
                self.release = threading.Event()
                self.other_entered = threading.Event()
                self.pos = 0

            def seek(self, pos):
                if pos == self.blocked_offset:
                    self.entered.set()
                    self.release.wait(timeout=2)
                elif not self.release.is_set():
                    # 页 A 还卡在 seek/write 之间，另一个线程已经进来了。
                    self.other_entered.set()
                self.pos = pos

            def write(self, data):
                self.inner.seek(self.pos)
                self.inner.write(data)

            def flush(self):
                self.inner.flush()

            @property
            def closed(self):
                return self.inner.closed

        proxy = BlockingHandle(real, page_a)
        dm._fh = proxy

        def payload(pid: int) -> bytes:
            buf = bytearray(PAGE_SIZE)
            import struct

            struct.pack_into("<H", buf, 0, PageType.HEAP)
            struct.pack_into("<I", buf, 16, pid)
            struct.pack_into("<I", buf, 32, pid)
            return bytes(buf)

        def write_page(pid: int) -> None:
            dm.write_page(pid, payload(pid))

        first = threading.Thread(target=write_page, args=(page_a,))
        first.start()
        assert proxy.entered.wait(timeout=2)
        second = threading.Thread(target=write_page, args=(page_b,))
        second.start()
        time.sleep(0.05)
        proxy.release.set()
        first.join(timeout=3)
        second.join(timeout=3)
        assert not proxy.other_entered.is_set()

        dm._fh = real
        for pid in (page_a, page_b):
            import struct

            raw = dm.read_page(pid)
            assert struct.unpack_from("<H", raw, 0)[0] == PageType.HEAP
            assert struct.unpack_from("<I", raw, 16)[0] == pid
            assert struct.unpack_from("<I", raw, 32)[0] == pid
        dm.close()
