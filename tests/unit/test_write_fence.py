"""写围栏（fencing）的回归测试。

围栏的意义是"被废黜的主库不再写"。它只有在**每一个**写入口都检查时才有意义，
所以这里覆盖三件事：

1. 线协议会话（不经过 `Database._run`）也必须被拦住；
2. 检查点不止语句入口：长语句可以跨过租约到期，提交点必须再查一次；
3. 只读事务不受影响——被 fence 的旧主库仍然要能当只读观察者。
"""

from __future__ import annotations

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
        db.execute_one("INSERT INTO t VALUES (1)")
        assert observed and all(observed), "围栏检查必须在 _write_lock 之内执行"


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
