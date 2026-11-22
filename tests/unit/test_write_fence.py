"""写围栏（fencing）的回归测试。

围栏的意义是"被废黜的主库不再写"。它只有在**每一个**写入口都检查时才有意义，
所以这里覆盖三件事：

1. 线协议会话（不经过 `Database._run`）也必须被拦住；
2. 检查点不止语句入口：长语句可以跨过租约到期，提交点必须再查一次；
3. 只读事务不受影响——被 fence 的旧主库仍然要能当只读观察者。
"""

from __future__ import annotations

import time

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError, StorageError
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql


def _run(server: PGWireServer, session: Session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


class _Fence:
    """可手动失效的围栏，模拟租约到期 / 观察到更高任期。"""

    def __init__(self) -> None:
        self.ok = True
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1
        if not self.ok:
            raise NotSupportedError(
                "写租约已失效", hint="旧主库必须停写，等待人工确认拓扑"
            )


@pytest.fixture
def env(tmp_path):
    fence = _Fence()
    db = Database(tmp_path / "db", write_fence=fence)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    yield db, fence
    db.close()


class TestWirePathHonoursTheFence:
    def test_wire_insert_is_rejected(self, env):
        db, fence = env
        server = PGWireServer(db)
        session = Session(database=db)

        _run(server, session, "BEGIN")
        fence.ok = False
        with pytest.raises(NotSupportedError, match="写租约已失效"):
            _run(server, session, "INSERT INTO t VALUES (1)")
        _run(server, session, "ROLLBACK")

    def test_wire_autocommit_insert_is_rejected(self, env):
        db, fence = env
        server = PGWireServer(db)
        session = Session(database=db)
        fence.ok = False
        with pytest.raises(NotSupportedError, match="写租约已失效"):
            _run(server, session, "INSERT INTO t VALUES (1)")
        assert _run(server, session, "SELECT COUNT(*) FROM t")[0].rows == [(0,)]

    def test_fenced_primary_can_still_read(self, env):
        """只读事务不能因为围栏而被拒——旧主库降级后还要能当观察者。"""
        db, fence = env
        db.execute_one("INSERT INTO t VALUES (1)")
        server = PGWireServer(db)
        session = Session(database=db)
        fence.ok = False
        assert _run(server, session, "SELECT COUNT(*) FROM t")[0].rows == [(1,)]
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]


class TestCommitPointRecheck:
    def test_commit_fails_when_fence_expired_during_statement(self, env):
        """语句入口通过了检查，提交前租约失效 → 不能落盘。"""
        db, fence = env
        server = PGWireServer(db)
        session = Session(database=db)

        _run(server, session, "BEGIN")
        _run(server, session, "INSERT INTO t VALUES (1)")  # 此刻围栏有效
        fence.ok = False
        with pytest.raises(NotSupportedError, match="写租约已失效"):
            _run(server, session, "COMMIT")

        # 事务被 abort，行不落盘；重新打开库也看不到它。
        fence.ok = True
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]

    def test_autocommit_commit_is_checked(self, env):
        db, fence = env
        calls_before = fence.calls
        db.execute_one("INSERT INTO t VALUES (7)")
        # 语句入口 + 提交点各一次
        assert fence.calls - calls_before >= 2

    def test_fence_is_checked_while_holding_the_write_lock(self, env):
        db, fence = env
        original = db.assert_writable
        observed: list[bool] = []

        def spy() -> None:
            lock = db._write_lock
            observed.append(
                bool(getattr(lock, "_is_owned", lambda: False)())
            )
            original()

        db.assert_writable = spy  # type: ignore[method-assign]
        # 用结构性写：它在两种并发模式下都走 ``exclusive_guard``（持有
        # ``_write_lock``）；并发 DML 模式的普通 INSERT 只登记 read_guard，
        # 本来就刻意不拿写者锁。
        db.execute_one("TRUNCATE t")
        assert observed and all(observed), "围栏检查必须在 _write_lock 之内执行"


class TestFencePathOnOpen:
    """崩溃重启防护：旧主库重开数据库时必须感知共享 fence marker。

    提升副本写 fence 后，旧主库进程重启、用同一 fence_path 重新打开
    Database 时，打开即被拒绝（非只读）；只读打开放行（当观察者）。
    打开时 fence 还没写、之后才被提升，则持续检查在下一笔写上拦住。
    """

    def test_open_rejects_when_fence_points_elsewhere(self, tmp_path):
        from lumendb.replication.fence import PromotionFence

        data = tmp_path / "old-primary"
        fence_path = tmp_path / "fence.json"
        db = Database(data)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute_one("INSERT INTO t VALUES (1)")
        db.close()

        PromotionFence(fence_path, node_id="new-leader").promote(lsn=100)
        with pytest.raises(NotSupportedError, match="fence"):
            Database(data, fence_path=fence_path, node_id="old-primary")

    def test_open_read_only_is_allowed_as_observer(self, tmp_path):
        from lumendb.replication.fence import PromotionFence

        data = tmp_path / "old-primary"
        fence_path = tmp_path / "fence.json"
        db = Database(data)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.close()

        PromotionFence(fence_path, node_id="new-leader").promote(lsn=100)
        observer = Database(
            data, read_only=True, fence_path=fence_path, node_id="old-primary"
        )
        assert observer.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
        observer.close()

    def test_fence_written_after_open_blocks_the_next_write(self, tmp_path):
        from lumendb.replication.fence import PromotionFence

        data = tmp_path / "old-primary"
        fence_path = tmp_path / "fence.json"
        db = Database(data, fence_path=fence_path, node_id="old-primary")
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        try:
            db.execute_one("INSERT INTO t VALUES (1)")
            PromotionFence(fence_path, node_id="new-leader").promote(lsn=100)
            with pytest.raises(NotSupportedError, match="fence"):
                db.execute_one("INSERT INTO t VALUES (2)")
        finally:
            db.close()

    def test_open_when_fence_points_to_self_is_writable(self, tmp_path):
        from lumendb.replication.fence import PromotionFence

        data = tmp_path / "primary"
        fence_path = tmp_path / "fence.json"
        PromotionFence(fence_path, node_id="primary").promote(lsn=10)
        db = Database(data, fence_path=fence_path, node_id="primary")
        try:
            db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
            db.execute_one("INSERT INTO t VALUES (1)")
            assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            db.close()


class TestLeasePathOnOpen:
    """租约版的重启防护：与 fence 同源，语义更严——无租约/他人持有/
    已过期一律拒绝写模式，旧主库必须重新 acquire 才能恢复写。

    旧主库重启后，租约持有者不是自己 → 拒绝写模式打开；自己仍是
    持有者且未过期 → 可写（提升后的新主库重启场景）。
    """

    def test_open_rejects_when_lease_held_by_other(self, tmp_path):
        from lumendb.replication.lease import Lease

        data = tmp_path / "old-primary"
        lease_path = tmp_path / "lease.json"
        db = Database(data)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.close()

        Lease(lease_path, "new-leader", ttl=10.0).acquire(term=2)
        with pytest.raises(NotSupportedError, match="租约"):
            Database(data, lease_path=lease_path, node_id="old-primary")

    def test_open_after_expiry_still_rejected_until_reacquire(self, tmp_path):
        from lumendb.replication.lease import Lease

        data = tmp_path / "old-primary"
        lease_path = tmp_path / "lease.json"
        db = Database(data)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.close()

        lease = Lease(lease_path, "new-leader", ttl=0.1)
        lease.acquire(term=2)
        deadline = time.monotonic() + 2.0
        while (
            time.monotonic() < deadline
            and lease.read()["expires_at"] > time.time()
        ):
            time.sleep(0.01)
        assert lease.read()["expires_at"] <= time.time()
        # 过期不等于放行：旧主库必须显式重新 acquire。
        with pytest.raises(NotSupportedError, match="租约"):
            Database(data, lease_path=lease_path, node_id="old-primary")
        # 重新 acquire（自己成为持有者）后可以写。
        Lease(lease_path, "old-primary", ttl=10.0).acquire(term=3)
        reopened = Database(data, lease_path=lease_path, node_id="old-primary")
        try:
            reopened.execute_one("INSERT INTO t VALUES (1)")
            assert reopened.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            reopened.close()

    def test_open_when_lease_is_mine(self, tmp_path):
        from lumendb.replication.lease import Lease

        data = tmp_path / "new-leader"
        lease_path = tmp_path / "lease.json"
        Lease(lease_path, "new-leader", ttl=10.0).acquire(term=2)
        db = Database(data, lease_path=lease_path, node_id="new-leader")
        try:
            db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
            db.execute_one("INSERT INTO t VALUES (1)")
            assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            db.close()

    def test_lease_expiry_after_open_blocks_the_next_write(self, tmp_path):
        from lumendb.replication.lease import Lease

        data = tmp_path / "old-primary"
        lease_path = tmp_path / "lease.json"
        # 先用长 TTL 打开并建表，避免 Windows 上“打开数据库”本身超过
        # 0.15s 就把租约耗光（见 test 的历史 flake）；随后再缩短租约续期。
        lease = Lease(lease_path, "old-primary", ttl=10.0)
        lease.acquire(term=2)
        db = Database(data, lease_path=lease_path, node_id="old-primary")
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        try:
            db.execute_one("INSERT INTO t VALUES (1)")
            lease.ttl = 0.15
            lease.renew(term=2)
            time.sleep(0.25)
            with pytest.raises(NotSupportedError, match="租约"):
                db.execute_one("INSERT INTO t VALUES (2)")
        finally:
            db.close()


class TestClosedDatabase:
    def test_internal_write_after_close_is_rejected(self, env):
        db, _ = env
        db.close()
        with pytest.raises(StorageError, match="已关闭"):
            db.execute_one("INSERT INTO t VALUES (1)")

    def test_wire_write_after_close_is_rejected(self, env):
        """pgwire 直接调 executor；assert_writable 也必须知道库已关闭。"""
        db, _ = env
        server = PGWireServer(db)
        session = Session(database=db)
        db.close()
        with pytest.raises(StorageError, match="已关闭"):
            _run(server, session, "INSERT INTO t VALUES (1)")

    def test_close_aborts_an_idle_wire_transaction(self, env):
        """关闭时线上事务不挂在 Database._txn 上，也必须被回滚。"""
        from lumendb.storage.txn import TxnState

        db, _ = env
        server = PGWireServer(db)
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "INSERT INTO t VALUES (1)")
        txn = session.txn
        assert txn is not None and txn.state is TxnState.ACTIVE
        db.close()
        assert txn.state is TxnState.ABORTED
        assert txn.first_lsn is not None

    def test_close_waits_for_an_active_reader(self, env):
        """close 不能把仍在扫页的 SELECT 关在门外。"""
        import threading
        import time

        db, _ = env
        entered = threading.Event()
        release = threading.Event()

        def reader() -> None:
            with db.read_guard():
                entered.set()
                release.wait(3)

        reader_thread = threading.Thread(target=reader)
        reader_thread.start()
        assert entered.wait(2)
        close_thread = threading.Thread(target=db.close)
        close_thread.start()
        time.sleep(0.05)
        assert close_thread.is_alive(), "close 必须等 reader 退出"
        release.set()
        reader_thread.join(2)
        close_thread.join(3)
        assert not close_thread.is_alive()
