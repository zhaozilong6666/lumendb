"""数据目录独占锁的测试：同一目录不能被两个"进程/句柄"同时打开。"""

import pytest

from lumendb.storage.filelock import DataDirLockedError, FileLock
from lumendb.storage.pager import DiskManager


class TestFileLock:
    def test_second_acquirer_is_rejected(self, tmp_path):
        first = FileLock(tmp_path / "x.lock")
        first.acquire()
        second = FileLock(tmp_path / "x.lock")
        with pytest.raises(DataDirLockedError):
            second.acquire()
        first.release()

    def test_lock_is_reusable_after_release(self, tmp_path):
        first = FileLock(tmp_path / "x.lock")
        first.acquire()
        first.release()
        second = FileLock(tmp_path / "x.lock")
        second.acquire()
        assert second.held
        second.release()

    def test_reacquire_is_idempotent(self, tmp_path):
        lock = FileLock(tmp_path / "x.lock")
        lock.acquire()
        lock.acquire()
        lock.release()
        assert not lock.held

    def test_release_without_acquire_is_noop(self, tmp_path):
        FileLock(tmp_path / "x.lock").release()

    def test_context_manager(self, tmp_path):
        with FileLock(tmp_path / "x.lock") as lock:
            assert lock.held
        assert not lock.held

    def test_wait_timeout_eventually_fails(self, tmp_path):
        holder = FileLock(tmp_path / "x.lock")
        holder.acquire()
        waiter = FileLock(tmp_path / "x.lock")
        with pytest.raises(DataDirLockedError):
            waiter.acquire(timeout=0.3)
        holder.release()


class TestDiskManagerLocking:
    def test_same_file_cannot_be_opened_twice(self, tmp_path):
        first = DiskManager(tmp_path / "db.data")
        with pytest.raises(DataDirLockedError):
            DiskManager(tmp_path / "db.data")
        first.close()


class TestDatabaseLifecycle:
    def test_reopen_after_close(self, tmp_path):
        first = DiskManager(tmp_path / "db.data")
        first.close()
        second = DiskManager(tmp_path / "db.data")
        assert second.num_pages == 1
        second.close()
