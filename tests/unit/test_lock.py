"""行锁管理器的测试：兼容矩阵、重入、超时与死锁检测。"""

import threading
import time

import pytest

from lumendb.errors import DeadlockError
from lumendb.storage.lock import LockManager, LockMode, LockTimeoutError, row_resource


@pytest.fixture
def locks():
    manager = LockManager(default_timeout=1.0)
    yield manager
    manager.reset()


class TestCompatibility:
    def test_shared_locks_coexist(self, locks):
        locks.acquire(1, "r", LockMode.SHARED)
        locks.acquire(2, "r", LockMode.SHARED)
        assert locks.holders("r") == {1: LockMode.SHARED, 2: LockMode.SHARED}

    def test_exclusive_blocks_shared(self, locks):
        locks.acquire(1, "r", LockMode.EXCLUSIVE)
        assert locks.try_acquire(2, "r", LockMode.SHARED) is False

    def test_shared_blocks_exclusive(self, locks):
        locks.acquire(1, "r", LockMode.SHARED)
        assert locks.try_acquire(2, "r", LockMode.EXCLUSIVE) is False

    def test_exclusive_blocks_exclusive(self, locks):
        locks.acquire(1, "r", LockMode.EXCLUSIVE)
        assert locks.try_acquire(2, "r", LockMode.EXCLUSIVE) is False

    def test_different_resources_independent(self, locks):
        locks.acquire(1, "a", LockMode.EXCLUSIVE)
        assert locks.try_acquire(2, "b", LockMode.EXCLUSIVE) is True

    def test_same_xid_reentrant(self, locks):
        locks.acquire(1, "r", LockMode.SHARED)
        # 同一事务再次申请共享锁不应阻塞
        locks.acquire(1, "r", LockMode.SHARED, timeout=0.5)
        assert locks.holders("r") == {1: LockMode.SHARED}

    def test_lock_upgrade_same_xid(self, locks):
        locks.acquire(1, "r", LockMode.SHARED)
        locks.acquire(1, "r", LockMode.EXCLUSIVE, timeout=0.5)
        assert locks.holders("r")[1] == LockMode.EXCLUSIVE

    def test_upgrade_blocked_by_other_shared(self, locks):
        locks.acquire(1, "r", LockMode.SHARED)
        locks.acquire(2, "r", LockMode.SHARED)
        assert locks.try_acquire(1, "r", LockMode.EXCLUSIVE) is False


class TestRelease:
    def test_release_frees_resource(self, locks):
        locks.acquire(1, "r", LockMode.EXCLUSIVE)
        locks.release(1, "r")
        assert locks.holders("r") == {}
        assert locks.try_acquire(2, "r", LockMode.EXCLUSIVE) is True

    def test_release_unknown_resource_is_noop(self, locks):
        locks.release(99, "never-held")

    def test_release_all(self, locks):
        locks.acquire(1, "a", LockMode.EXCLUSIVE)
        locks.acquire(1, "b", LockMode.EXCLUSIVE)
        locks.release_all(1)
        assert locks.holders("a") == {}
        assert locks.holders("b") == {}

    def test_release_does_not_touch_other_xid(self, locks):
        locks.acquire(1, "r", LockMode.SHARED)
        locks.acquire(2, "r", LockMode.SHARED)
        locks.release(1, "r")
        assert locks.holders("r") == {2: LockMode.SHARED}


class TestTimeout:
    def test_waits_then_times_out(self, locks):
        locks.acquire(1, "r", LockMode.EXCLUSIVE)
        started = time.monotonic()
        with pytest.raises(LockTimeoutError):
            locks.acquire(2, "r", LockMode.SHARED, timeout=0.3)
        assert time.monotonic() - started < 2.0

    def test_blocked_waiter_acquires_after_release(self, locks):
        locks.acquire(1, "r", LockMode.EXCLUSIVE)
        acquired = threading.Event()
        error: list[BaseException] = []

        def waiter():
            try:
                locks.acquire(2, "r", LockMode.SHARED, timeout=2.0)
                acquired.set()
            except BaseException as exc:  # pragma: no cover
                error.append(exc)

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.15)
        assert not acquired.is_set()
        locks.release(1, "r")
        thread.join(timeout=3.0)
        assert acquired.is_set()
        assert not error


class TestDeadlock:
    def test_two_transactions_deadlock(self, locks):
        a_ready = threading.Event()
        b_ready = threading.Event()
        results: dict[int, BaseException] = {}

        def worker(xid, first, second, own_event, other_event):
            locks.acquire(xid, first, LockMode.EXCLUSIVE, timeout=1.5)
            own_event.set()
            other_event.wait(timeout=1.5)
            try:
                locks.acquire(xid, second, LockMode.EXCLUSIVE, timeout=1.5)
            except BaseException as exc:
                results[xid] = exc

        t1 = threading.Thread(target=worker, args=(1, "a", "b", a_ready, b_ready))
        t2 = threading.Thread(target=worker, args=(2, "b", "a", b_ready, a_ready))
        t1.start()
        t2.start()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)

        assert results, "应当至少有一个事务被检测出死锁"
        assert any(isinstance(exc, DeadlockError) for exc in results.values())


class TestRowResource:
    def test_row_resource_shape(self):
        resource = row_resource("users", (3, 7))
        assert resource == ("row", "users", 3, 7)

    def test_row_resource_distinguishes_slots(self):
        assert row_resource("t", (1, 1)) != row_resource("t", (1, 2))
