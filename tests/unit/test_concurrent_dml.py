"""并发 DML 模式（``Database(concurrent_dml=True)``）的回归。

默认模式仍由 ``_write_lock`` 串行，这里只覆盖"真正并行"的那条路径：
- DML 语句不再拿写者锁，由行/键锁 + 页 latch + ``read_guard`` 与 DDL/close 互斥；
- 目录保存改为短阻塞、用后立即释放；
- 索引根页号是共享单元（多个 BPlusTree 实例看到同一个当前根）；
- 回滚走逻辑路径：堆页/B+ 树页不写物理 undo，死行靠可见性与 VACUUM 回收。
"""

from __future__ import annotations

import threading
import time
import traceback

import pytest

from lumendb.engine.database import Database
from lumendb.errors import (
    DuplicateKeyError,
    NotSupportedError,
    SerializationError,
    TransactionError,
)
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.storage.heap import HeapFile
from lumendb.sql.parser import parse_sql


def _run(server: PGWireServer, session: Session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


def _rows(server: PGWireServer, session: Session, sql: str) -> list:
    return _run(server, session, sql)[0].rows


@pytest.fixture
def env(tmp_path):
    db = Database(tmp_path / "db", concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    db.execute_one("CREATE INDEX idx_v ON t (v)")
    server = PGWireServer(db)
    yield db, server
    db.close()


@pytest.fixture
def physical_env(tmp_path):
    """显式关闭逻辑保存点，专门守住独占写者锁的旧路径。"""
    db = Database(
        tmp_path / "db-physical", concurrent_dml=True, logical_savepoints=False
    )
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    db.execute_one("CREATE INDEX idx_v ON t (v)")
    server = PGWireServer(db)
    yield db, server
    db.close()


def _run_threads(workers) -> list[str]:
    errors: list[str] = []

    def wrap(fn):
        def inner():
            try:
                fn()
            except Exception:
                errors.append(traceback.format_exc())

        return inner

    threads = [threading.Thread(target=wrap(fn)) for fn in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def test_concurrent_inserts_all_commit(env):
    db, server = env
    per_writer = 60

    def worker(w: int):
        s = Session(database=db)
        _run(server, s, "BEGIN")
        for i in range(w * per_writer, (w + 1) * per_writer):
            _run(server, s, f"INSERT INTO t VALUES ({i}, {i % 7}, 'x{i}')")
        _run(server, s, "COMMIT")

    errors = _run_threads([lambda w=w: worker(w) for w in range(4)])
    assert errors == [], errors[0]
    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [
        (4 * per_writer,)
    ]
    assert _rows(server, Session(database=db), "SELECT COUNT(DISTINCT id) FROM t") == [
        (4 * per_writer,)
    ]
    table = db.catalog.get_table("t")
    for idx in table.indexes.values():
        tree = db.executor._tree_for(table, idx)
        inv = tree.check_invariants()
        assert inv["entries"] == 4 * per_writer


def test_concurrent_mixed_dml_with_rollbacks(env):
    db, server = env
    per_writer = 40

    def worker(w: int):
        s = Session(database=db)
        _run(server, s, "BEGIN")
        base = w * per_writer
        for i in range(base, base + per_writer):
            _run(server, s, f"INSERT INTO t VALUES ({i}, {i % 5}, 'x{i}')")
        _run(server, s, f"UPDATE t SET v = 99 WHERE id = {base}")
        _run(server, s, f"DELETE FROM t WHERE id = {base + 1}")
        if w % 2 == 0:
            _run(server, s, "ROLLBACK")
        else:
            _run(server, s, "COMMIT")

    errors = _run_threads([lambda w=w: worker(w) for w in range(4)])
    assert errors == [], errors[0]
    # 两个提交线程各留 (per_writer - 1) 行；两个回滚线程不留行
    expected = 2 * (per_writer - 1)
    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(expected,)]
    assert _rows(server, Session(database=db), "SELECT id FROM t WHERE id = 1") == []
    table = db.catalog.get_table("t")
    tree = db.executor._tree_for(table, table.indexes["idx_v"])
    # 逻辑回滚会留下不可见的索引项/死行，不变量仍必须成立
    inv = tree.check_invariants()
    assert inv["entries"] >= expected


def test_logical_rollback_is_invisible_and_vacuum_reclaims(env):
    db, server = env
    s = Session(database=db)
    _run(server, s, "BEGIN")
    for i in range(100):
        _run(server, s, f"INSERT INTO t VALUES ({i}, {i % 3}, 'r{i}')")
    _run(server, s, "ROLLBACK")

    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(0,)]
    table = db.catalog.get_table("t")
    tree = db.executor._tree_for(table, table.indexes["idx_v"])
    assert tree.check_invariants()["entries"] == 100  # 逻辑垃圾，等 VACUUM 回收

    db.execute_one("VACUUM")
    tree = db.executor._tree_for(table, table.indexes["idx_v"])
    assert tree.check_invariants()["entries"] == 0
    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(0,)]

    # VACUUM 之后新事务照常可写
    s2 = Session(database=db)
    _run(server, s2, "BEGIN")
    _run(server, s2, "INSERT INTO t VALUES (1000, 1, 'after')")
    _run(server, s2, "COMMIT")
    assert _rows(server, s2, "SELECT COUNT(*) FROM t") == [(1,)]


def test_concurrent_delete_and_readers(env):
    """写入 + 删除与并发读者交错：读者不能读到已删行以外的东西（不抛异常）。"""
    db, server = env
    s = Session(database=db)
    _run(server, s, "BEGIN")
    for i in range(300):
        _run(server, s, f"INSERT INTO t VALUES ({i}, {i % 4}, 'p{i}')")
    _run(server, s, "COMMIT")

    errors: list[str] = []
    seen_counts: list[int] = []

    def writer():
        w = Session(database=db)
        _run(server, w, "BEGIN")
        for i in range(0, 300, 2):
            _run(server, w, f"DELETE FROM t WHERE id = {i}")
        _run(server, w, "COMMIT")

    def reader():
        r = Session(database=db)
        for _ in range(20):
            counts = _rows(server, r, "SELECT COUNT(*) FROM t")
            seen_counts.append(counts[0][0])

    threads = [
        threading.Thread(target=lambda: _wrap(errors, writer)),
        threading.Thread(target=lambda: _wrap(errors, reader)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [], errors[0]
    # 删除已提交后，计数只能是 150（读已提交语义下中途可能读到 151..300）
    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(150,)]
    assert all(150 <= c <= 300 for c in seen_counts), seen_counts


def test_savepoint_lock_can_be_released_by_another_thread(physical_env):
    """保存点可能在别的 worker 建；COMMIT 跨线程释放不能自锁/报 owner 错。"""
    db, server = physical_env
    s = Session(database=db)
    ready = threading.Event()
    errors: list[str] = []

    def begin_with_savepoint():
        try:
            _run(server, s, "BEGIN")
            _run(server, s, "SAVEPOINT cross_thread")
            ready.set()
        except Exception:
            errors.append(traceback.format_exc())
            ready.set()

    thread = threading.Thread(target=begin_with_savepoint)
    thread.start()
    assert ready.wait(2.0), "建保存点超时"
    _run(server, s, "INSERT INTO t VALUES (7, 7, 'x')")
    _run(server, s, "COMMIT")
    thread.join()
    assert errors == [], errors[0]
    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t WHERE id = 7") == [
        (1,)
    ]


def test_vacuum_falls_back_to_online_with_idle_snapshot(env, monkeypatch):
    """5s 等不到空窗口时转保 RID 在线回收，活跃快照仍看到旧版本。"""
    db, server = env
    seed = Session(database=db)
    _run(server, seed, "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')")

    snap = Session(database=db)
    _run(server, snap, "BEGIN")
    assert _rows(server, snap, "SELECT id, v FROM t ORDER BY id") == [
        (1, 1),
        (2, 2),
        (3, 3),
    ]

    writer = Session(database=db)
    _run(server, writer, "BEGIN")
    _run(server, writer, "UPDATE t SET v = 11 WHERE id = 1")
    _run(server, writer, "DELETE FROM t WHERE id = 2")
    _run(server, writer, "COMMIT")

    # 直接模拟 5s 超时：不让测试真的等 5 秒，但保留“活跃事务还在”的状态。
    monkeypatch.setattr(db.txns, "wait_until_no_others", lambda *a, **k: False)
    result = server._run_statement(
        Session(database=db), parse_sql("VACUUM")[0]
    )
    assert "online" in result.tag
    assert _rows(server, snap, "SELECT id, v FROM t ORDER BY id") == [
        (1, 1),
        (2, 2),
        (3, 3),
    ]
    assert _rows(server, Session(database=db), "SELECT id, v FROM t ORDER BY id") == [
        (1, 11),
        (3, 3),
    ]
    _run(server, snap, "COMMIT")


def test_vacuum_with_active_ddl_refuses_online(env, monkeypatch):
    """活跃 DDL 事务持有目录锁/物理 undo，VACUUM 不能和它交错。"""
    db, server = env
    ddl = Session(database=db)
    _run(server, ddl, "BEGIN")
    _run(server, ddl, "CREATE TABLE u (id INT PRIMARY KEY)")
    monkeypatch.setattr(db.txns, "wait_until_no_others", lambda *a, **k: False)
    try:
        with pytest.raises(TransactionError):
            server._run_statement(Session(database=db), parse_sql("VACUUM t")[0])
    finally:
        _run(server, ddl, "ROLLBACK")


def test_vacuum_waits_for_active_transaction(env):
    """线协议 VACUUM 先等在途事务退出，而不是看到 active_xids 就报错。"""
    db, server = env
    holder = Session(database=db)
    ready = threading.Event()
    release = threading.Event()
    errors: list[str] = []
    result: list = []

    def hold_transaction():
        try:
            _run(server, holder, "BEGIN")
            _run(server, holder, "INSERT INTO t VALUES (999, 9, 'hold')")
            ready.set()
            release.wait(2.0)
            _run(server, holder, "COMMIT")
        except Exception:
            errors.append(traceback.format_exc())

    def vacuum():
        try:
            result.append(
                server._run_statement(
                    Session(database=db), parse_sql("VACUUM")[0]
                )
            )
        except Exception:
            errors.append(traceback.format_exc())

    ht = threading.Thread(target=hold_transaction)
    vt = threading.Thread(target=vacuum)
    ht.start()
    assert ready.wait(2.0), "持有事务未启动"
    vt.start()
    # VACUUM 此刻应真的停在等待窗口里；如果它直接返回，说明门禁没生效。
    vt.join(timeout=0.2)
    assert vt.is_alive(), "VACUUM 未等待活跃事务就返回了"
    release.set()
    vt.join(timeout=10)
    ht.join(timeout=10)
    assert not vt.is_alive() and not ht.is_alive()
    assert errors == [], errors[0]
    assert result and result[0].tag.startswith("VACUUM")
    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(1,)]


def test_close_with_savepoint_releases_the_gate(physical_env):
    """同线程 close 进到保存点事务：abort 后必须放门，不能留下 no_readers。"""
    db, server = physical_env
    session = Session(database=db)
    _run(server, session, "BEGIN")
    _run(server, session, "SAVEPOINT s")
    _run(server, session, "INSERT INTO t VALUES (9, 9, 'z')")
    db.close()
    assert db._no_readers is False


def test_savepoint_waits_for_exclusive_and_rolls_back(physical_env):
    """并发模式下保存点不再 0A000：排空在途语句后独占，回滚只撤保存点后写入。"""
    db, server = physical_env
    a = Session(database=db)
    b = Session(database=db)
    _run(server, a, "BEGIN")
    _run(server, b, "BEGIN")
    _run(server, a, "INSERT INTO t VALUES (1, 1, 'a')")
    _run(server, b, "SAVEPOINT s")
    _run(server, b, "INSERT INTO t VALUES (2, 2, 'b')")
    _run(server, b, "ROLLBACK TO SAVEPOINT s")
    assert _rows(server, b, "SELECT COUNT(*) FROM t WHERE id = 2") == [(0,)]
    _run(server, b, "COMMIT")
    _run(server, a, "COMMIT")
    assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(1,)]


def _wrap(errors: list[str], fn):
    try:
        fn()
    except Exception:
        errors.append(traceback.format_exc())


def test_hot_row_increments_serialize_without_lost_updates(env):
    """自动提交的热点自增：锁后重读必须保住每一次增量，且不再是 40001 风暴。"""
    db, server = env
    _run(server, Session(database=db), "INSERT INTO t VALUES (999999, 0, 'hot')")
    threads = 4
    per_thread = 25
    barrier = threading.Barrier(threads)
    errors = _run_threads(
        [
            lambda: (
                barrier.wait(),
                [
                    _run(
                        server,
                        Session(database=db),
                        "UPDATE t SET v = v + 1 WHERE id = 999999",
                    )
                    for _ in range(per_thread)
                ],
            )
            for _ in range(threads)
        ]
    )
    assert errors == [], errors[0]
    assert _rows(
        server, Session(database=db), "SELECT v FROM t WHERE id = 999999"
    ) == [(threads * per_thread,)]


def test_autocommit_update_waits_for_holder_then_rereads_latest(env):
    """自动提交 UPDATE 在行锁上等待后重读最新已提交版本，不能拿旧快照覆盖。"""
    db, server = env
    _run(server, Session(database=db), "INSERT INTO t VALUES (1, 0, 'rc')")
    holder = Session(database=db)
    _run(server, holder, "BEGIN")
    _run(server, holder, "UPDATE t SET v = v + 1 WHERE id = 1")

    started = threading.Event()
    errors: list[str] = []

    def waiter():
        started.set()
        try:
            _run(server, Session(database=db), "UPDATE t SET v = v + 1 WHERE id = 1")
        except Exception:
            errors.append(traceback.format_exc())

    thread = threading.Thread(target=waiter)
    thread.start()
    assert started.wait(5)
    # 让 waiter 先到行锁上排队，再提交 holder；若 waiter 没等待就会提前失败。
    time.sleep(0.3)
    _run(server, holder, "COMMIT")
    thread.join(5)
    assert not thread.is_alive()
    assert errors == [], errors[0]
    assert _rows(server, Session(database=db), "SELECT v FROM t WHERE id = 1") == [
        (2,)
    ]


def test_explicit_transaction_update_stays_snapshot_isolation(env):
    """显式事务不享受自动提交 UPDATE 的"锁后重读最新"：旧快照写同一行应 40001。

    自动提交 UPDATE 是 READ COMMITTED 色彩（锁后重读最新已提交版本）；
    显式事务仍是快照隔离 + first-updater-wins。这条差异必须由测试钉住，
    否则以后有人把 ``latest`` 无脑设成 ``self._concurrent_dml`` 会静默改变
    显式事务的隔离级别。
    """
    db, server = env
    _run(server, Session(database=db), "INSERT INTO t VALUES (1, 0, 'iso')")
    a = Session(database=db)
    b = Session(database=db)
    _run(server, a, "BEGIN")
    _run(server, a, "UPDATE t SET v = v + 1 WHERE id = 1")
    _run(server, b, "BEGIN")
    # b 的快照仍停在 a 修改前：SELECT 看到 0，UPDATE 必须拒绝而不是覆盖 a。
    assert _rows(server, b, "SELECT v FROM t WHERE id = 1") == [(0,)]
    with pytest.raises(SerializationError):
        _run(server, b, "UPDATE t SET v = v + 1 WHERE id = 1")
    _run(server, b, "ROLLBACK")
    _run(server, a, "COMMIT")
    assert _rows(server, Session(database=db), "SELECT v FROM t WHERE id = 1") == [
        (1,)
    ]


def test_concurrent_dml_defaults_to_logical_savepoints(tmp_path):
    """并发模式不显式传参时，保存点默认走行级逻辑 undo。"""
    db = Database(tmp_path / "logical_sp_default", concurrent_dml=True)
    try:
        assert db._logical_savepoints is True
    finally:
        db.close()


def test_logical_savepoint_does_not_block_other_writer(tmp_path):
    """行级逻辑 undo：保存点事务存续期间，另一个写者仍能提交。"""
    db = Database(tmp_path / "logical_sp", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    try:
        first = Session(database=db)
        _run(server, first, "BEGIN")
        _run(server, first, "SAVEPOINT s")
        _run(server, first, "INSERT INTO t VALUES (1, 10)")

        second = Session(database=db)
        _run(server, second, "INSERT INTO t VALUES (2, 20)")

        _run(server, first, "ROLLBACK TO SAVEPOINT s")
        assert _rows(server, Session(database=db), "SELECT id FROM t ORDER BY id") == [
            (2,)
        ]
        _run(server, first, "INSERT INTO t VALUES (3, 30)")
        _run(server, first, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT id FROM t ORDER BY id") == [
            (2,),
            (3,),
        ]
    finally:
        db.close()


def test_logical_savepoint_undoes_update_and_delete(tmp_path):
    db = Database(tmp_path / "logical_sp_dml", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10), (2, 20)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "UPDATE t SET v = 11 WHERE id = 1")
        _run(server, session, "DELETE FROM t WHERE id = 2")
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM t ORDER BY id"
        ) == [(1, 10), (2, 20)]
        _run(server, session, "COMMIT")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM t ORDER BY id"
        ) == [(1, 10), (2, 20)]
    finally:
        db.close()


def test_logical_savepoint_can_write_again_after_rollback(tmp_path):
    db = Database(tmp_path / "logical_sp_again", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "UPDATE t SET v = 11 WHERE id = 1")
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        _run(server, session, "UPDATE t SET v = 12 WHERE id = 1")
        _run(server, session, "COMMIT")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM t"
        ) == [(1, 12)]
    finally:
        db.close()


def test_logical_savepoint_release_restores_ddl_path(tmp_path):
    db = Database(tmp_path / "logical_sp_release", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "INSERT INTO t VALUES (1, 10)")
        _run(server, session, "RELEASE SAVEPOINT s")
        # 所有保存点释放后回到普通事务：DDL 可以用物理页原像。
        _run(server, session, "CREATE TABLE x (id INT PRIMARY KEY)")
        _run(server, session, "INSERT INTO x VALUES (2)")
        _run(server, session, "COMMIT")
        assert "x" in [table.name for table in db.catalog.list_tables()]
        assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM x") == [(1,)]
    finally:
        db.close()


def test_logical_savepoint_release_inner_keeps_outer_undo(tmp_path):
    """释放内层保存点不能丢掉外层保存点还需要回滚的行 undo。"""
    db = Database(tmp_path / "logical_sp_release_inner", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT a")
        _run(server, session, "INSERT INTO t VALUES (2, 20)")
        _run(server, session, "SAVEPOINT b")
        _run(server, session, "INSERT INTO t VALUES (3, 30)")
        _run(server, session, "RELEASE SAVEPOINT b")
        _run(server, session, "ROLLBACK TO SAVEPOINT a")
        _run(server, session, "COMMIT")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM t ORDER BY id"
        ) == [(1, 10)]
    finally:
        db.close()


def test_logical_savepoint_upgrades_after_rollback_to_savepoint(tmp_path):
    """行 undo 被 ROLLBACK TO 撤空后，同一事务仍可升级物理保存点做 DDL。"""
    db = Database(tmp_path / "logical_sp_upgrade_after_rollback", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    db.execute_one("INSERT INTO t VALUES (1, 1, 'a')")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "INSERT INTO t VALUES (2, 2, 'b')")
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        _run(server, session, "ALTER TABLE t DROP COLUMN pad")
        _run(server, session, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1)]
    finally:
        db.close()


def test_logical_savepoint_without_dml_upgrades_for_ddl(tmp_path):
    db = Database(tmp_path / "logical_sp_ddl", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        # 保存点后还没有行 undo：DDL 可以把事务升级成物理保存点。
        _run(server, session, "CREATE TABLE x (id INT PRIMARY KEY)")
        _run(server, session, "INSERT INTO x VALUES (1)")
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        assert "x" not in [table.name for table in db.catalog.list_tables()]
        _run(server, session, "COMMIT")
        assert "x" not in [table.name for table in db.catalog.list_tables()]
        assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(0,)]
    finally:
        db.close()


def test_logical_savepoint_rejects_ddl_after_dml(tmp_path):
    db = Database(tmp_path / "logical_sp_ddl_after_dml", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "INSERT INTO t VALUES (1, 10)")
        with pytest.raises(NotSupportedError, match="逻辑保存点内有 DML 后不能再执行 DDL/TRUNCATE"):
            _run(server, session, "CREATE TABLE x (id INT PRIMARY KEY)")
        # 被拒绝的结构变更不能污染事务；行 undo 仍可正常回滚。
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        _run(server, session, "COMMIT")
        assert "x" not in [table.name for table in db.catalog.list_tables()]
        assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(0,)]
    finally:
        db.close()


def test_logical_savepoint_upgrades_then_truncate_rolls_back(tmp_path):
    db = Database(tmp_path / "logical_sp_truncate", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10), (2, 20)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        # 保存点后先 TRUNCATE 再 INSERT：升级成物理页原像后两者都能撤回。
        _run(server, session, "TRUNCATE TABLE t")
        _run(server, session, "INSERT INTO t VALUES (9, 90)")
        # 已升级成物理保存点：其它会话被 _no_readers 挡在门外，本会话直读。
        assert _run(server, session, "SELECT id FROM t")[0].rows == [(9,)]
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        assert _run(server, session, "SELECT id, v FROM t ORDER BY id")[0].rows == [
            (1, 10),
            (2, 20),
        ]
        _run(server, session, "COMMIT")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM t ORDER BY id"
        ) == [(1, 10), (2, 20)]
    finally:
        db.close()


def test_logical_savepoint_upgrade_via_database_exec(tmp_path):
    """进程内 Database.execute 路径也要走同一套 DDL 升级逻辑。"""
    db = Database(tmp_path / "logical_sp_exec", concurrent_dml=True, logical_savepoints=True)
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("BEGIN")
        db.execute_one("SAVEPOINT s")
        db.execute_one("CREATE TABLE x (id INT PRIMARY KEY)")
        db.execute_one("INSERT INTO x VALUES (1)")
        db.execute_one("ROLLBACK TO SAVEPOINT s")
        db.execute_one("COMMIT")
        assert "x" not in [table.name for table in db.catalog.list_tables()]
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
    finally:
        db.close()


def test_ddl_guard_blocks_other_uncommitted_writer(tmp_path):
    """其它会话有未提交写时，DDL 不能改写 shadow heap。

    shadow heap 只复制 DDL 事务可见的行；未提交行不会被复制。若此时允许
    ALTER 替换目录，等那个事务 COMMIT，行就落在已被替换掉的旧 heap 里，
    查询会静默少行。
    """
    db = Database(
        tmp_path / "ddl_guard_uncommitted_writer",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
        db.execute_one("INSERT INTO t VALUES (1, 1, 'a')")
        server = PGWireServer(db)
        a = Session(database=db)
        b = Session(database=db)
        _run(server, a, "BEGIN")
        _run(server, a, "INSERT INTO t VALUES (2, 2, 'b')")
        with pytest.raises(TransactionError, match="未提交写入"):
            _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        _run(server, a, "COMMIT")
        _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1), (2, 2)]
    finally:
        db.close()


def test_ddl_guard_blocks_other_active_reader_for_shadow_rewrite(tmp_path):
    """shadow heap 结构变更必须挡住活跃旧快照，不能让它之后读到空表。"""
    db = Database(
        tmp_path / "ddl_guard_active_reader",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
        db.execute_one("INSERT INTO t VALUES (1, 1, 'a')")
        server = PGWireServer(db)
        a = Session(database=db)
        b = Session(database=db)
        _run(server, a, "BEGIN")
        assert _rows(server, a, "SELECT * FROM t") == [(1, 1, "a")]
        with pytest.raises(TransactionError, match="仍在活跃"):
            _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        # DDL 被挡住后，旧快照仍然完整，而不是被切到新 heap 上读到空表。
        assert _rows(server, a, "SELECT * FROM t") == [(1, 1, "a")]
        _run(server, a, "COMMIT")
        _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1)]
    finally:
        db.close()


def test_alter_add_column_blocks_active_reader(tmp_path):
    """ALTER TABLE ADD/RENAME 也会改旧快照看到的表定义，必须挡住活跃读者。"""
    db = Database(
        tmp_path / "ddl_guard_active_reader_add",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
        db.execute_one("INSERT INTO t VALUES (1, 1, 'a')")
        server = PGWireServer(db)
        a = Session(database=db)
        b = Session(database=db)
        _run(server, a, "BEGIN")
        assert _rows(server, a, "SELECT * FROM t") == [(1, 1, "a")]
        with pytest.raises(TransactionError, match="仍在活跃"):
            _run(server, b, "ALTER TABLE t ADD COLUMN extra INT")
        assert _rows(server, a, "SELECT * FROM t") == [(1, 1, "a")]
        _run(server, a, "COMMIT")
        _run(server, b, "ALTER TABLE t ADD COLUMN extra INT")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [
            (1, 1, "a", None)
        ]
    finally:
        db.close()


def test_drop_table_blocks_active_reader(tmp_path):
    """DROP TABLE 不能把活跃旧快照手里的表悄悄摘掉。"""
    db = Database(
        tmp_path / "ddl_guard_drop_table_reader",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 1)")
        server = PGWireServer(db)
        a = Session(database=db)
        b = Session(database=db)
        _run(server, a, "BEGIN")
        assert _rows(server, a, "SELECT * FROM t") == [(1, 1)]
        with pytest.raises(TransactionError, match="仍在活跃"):
            _run(server, b, "DROP TABLE t")
        assert _rows(server, a, "SELECT * FROM t") == [(1, 1)]
        _run(server, a, "COMMIT")
        _run(server, b, "DROP TABLE t")
        assert _rows(server, Session(database=db), "SHOW TABLES") == []
    finally:
        db.close()


def test_logical_savepoint_upgrade_ddl_blocks_other_row_undo(tmp_path):
    """保存点后无 DML 的 DDL 升级路径也必须检查别的会话行 undo。

    S2 自己还没有行 DML，ALTER 会走 ``_maybe_upgrade_logical_savepoint``
    拿物理保存点。如果只在普通 DDL 门前检查，S2 的升级路径会绕过门，
    在 S1 还持有行 undo 时改掉行布局，S1 回滚时按新 schema 解码旧版本
    就会报列数不符。
    """
    db = Database(
        tmp_path / "logical_sp_upgrade_guard",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
        db.execute_one("INSERT INTO t VALUES (1, 1, 'a')")
        server = PGWireServer(db)
        a = Session(database=db)
        b = Session(database=db)
        _run(server, a, "BEGIN")
        _run(server, a, "SAVEPOINT s")
        _run(server, a, "UPDATE t SET v = 2 WHERE id = 1")
        _run(server, b, "BEGIN")
        _run(server, b, "SAVEPOINT s2")
        with pytest.raises(TransactionError, match="行 undo"):
            _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        # 升级失败不能泄漏写者门：S1 还能正常回滚提交，B 也能重试。
        assert _rows(server, a, "SELECT * FROM t") == [(1, 2, "a")]
        _run(server, a, "ROLLBACK TO SAVEPOINT s")
        _run(server, a, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1, "a")]
        _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        _run(server, b, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1)]
    finally:
        db.close()


def test_logical_savepoint_blocks_other_session_ddl(tmp_path):
    """别的会话不能在逻辑保存点持有行 undo 时改行布局。

    ALTER DROP COLUMN 走 shadow heap 改 schema；若此时允许执行，保存点
    ROLLBACK 会拿旧记录按新列数解码，直接报 CorruptedPageError。DDL 必须
    返回可重试的 40001，等保存点事务结束再改。
    """
    db = Database(
        tmp_path / "logical_sp_ddl_guard", concurrent_dml=True, logical_savepoints=True
    )
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
        db.execute_one("INSERT INTO t VALUES (1, 1, 'a')")
        server = PGWireServer(db)
        a = Session(database=db)
        b = Session(database=db)
        _run(server, a, "BEGIN")
        _run(server, a, "SAVEPOINT s")
        _run(server, a, "UPDATE t SET v = 2 WHERE id = 1")
        with pytest.raises(TransactionError, match="行 undo"):
            _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        _run(server, a, "ROLLBACK TO SAVEPOINT s")
        _run(server, a, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1, "a")]
        # 保存点事务结束后，同一个结构变更可以正常执行。
        _run(server, b, "ALTER TABLE t DROP COLUMN pad")
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1)]
    finally:
        db.close()


def test_logical_savepoint_rejects_truncate_after_dml(tmp_path):
    db = Database(tmp_path / "logical_sp_truncate", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10), (2, 20)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "INSERT INTO t VALUES (3, 30)")
        with pytest.raises(NotSupportedError, match="逻辑保存点内有 DML 后不能再执行 DDL/TRUNCATE"):
            _run(server, session, "TRUNCATE TABLE t")
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        _run(server, session, "COMMIT")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM t ORDER BY id"
        ) == [(1, 10), (2, 20)]
    finally:
        db.close()


def test_logical_savepoint_rejects_vacuum_with_row_undo(tmp_path):
    db = Database(tmp_path / "logical_sp_vacuum", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "INSERT INTO t VALUES (1, 10)")
        with pytest.raises(NotSupportedError, match="逻辑保存点事务内不能执行 VACUUM"):
            _run(server, session, "VACUUM")
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        _run(server, session, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(0,)]
    finally:
        db.close()


def test_logical_savepoint_reuses_key_and_row_after_rollback(tmp_path):
    db = Database(tmp_path / "logical_sp_reuse", concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (2, 20)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT s")
        _run(server, session, "INSERT INTO t VALUES (1, 10)")
        _run(server, session, "DELETE FROM t WHERE id = 2")
        _run(server, session, "ROLLBACK TO SAVEPOINT s")
        # 被撤销的插入会留下 ABORTED 根记录；唯一键检查必须能跳过它。
        _run(server, session, "INSERT INTO t VALUES (1, 11)")
        # 被撤销的删除也必须能重新更新/再次删除。
        _run(server, session, "UPDATE t SET v = 21 WHERE id = 2")
        _run(server, session, "DELETE FROM t WHERE id = 2")
        _run(server, session, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT id, v FROM t") == [(1, 11)]
    finally:
        db.close()


def test_logical_savepoint_restores_unique_key_on_update_rollback(tmp_path):
    """保存点内 UPDATE 唯一键后回滚：旧键仍被占用，新键可以重新插入。"""
    db = Database(
        tmp_path / "logical_sp_unique_update",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE u (id INT PRIMARY KEY, v INT UNIQUE)")
    db.execute_one("INSERT INTO u VALUES (1, 10), (2, 20)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT sp")
        _run(server, session, "UPDATE u SET v = 30 WHERE id = 1")
        _run(server, session, "ROLLBACK TO SAVEPOINT sp")
        _run(server, session, "COMMIT")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM u ORDER BY id"
        ) == [(1, 10), (2, 20)]
        # 旧键恢复占用，新键释放
        with pytest.raises(DuplicateKeyError):
            db.execute_one("INSERT INTO u VALUES (3, 10)")
        db.execute_one("INSERT INTO u VALUES (3, 30)")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM u ORDER BY id"
        ) == [(1, 10), (2, 20), (3, 30)]
    finally:
        db.close()


def test_logical_savepoint_restores_cascade_deleted_children(tmp_path):
    """保存点内 DELETE 父行触发 ON DELETE CASCADE，回滚后子行必须全部回来。"""
    db = Database(
        tmp_path / "logical_sp_cascade",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE p (id INT PRIMARY KEY)")
    db.execute_one(
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) "
        "ON DELETE CASCADE)"
    )
    db.execute_one("INSERT INTO p VALUES (1)")
    db.execute_one("INSERT INTO c VALUES (10, 1), (11, 1)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT sp")
        _run(server, session, "DELETE FROM p WHERE id = 1")
        assert _rows(server, session, "SELECT COUNT(*) FROM c") == [(0,)]
        _run(server, session, "ROLLBACK TO SAVEPOINT sp")
        _run(server, session, "COMMIT")
        fresh = Session(database=db)
        assert _rows(server, fresh, "SELECT COUNT(*) FROM p") == [(1,)]
        assert _rows(
            server, fresh, "SELECT id, pid FROM c ORDER BY id"
        ) == [(10, 1), (11, 1)]
    finally:
        db.close()


def test_logical_savepoint_undoes_upsert_conflict_update(tmp_path):
    """ON CONFLICT DO UPDATE 走的是 UPDATE 路径，保存点回滚要撤掉新值。"""
    db = Database(
        tmp_path / "logical_sp_upsert",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT sp")
        _run(
            server,
            session,
            "INSERT INTO t VALUES (1, 99) ON CONFLICT (id) DO UPDATE "
            "SET v = excluded.v",
        )
        assert _rows(server, session, "SELECT id, v FROM t") == [(1, 99)]
        _run(server, session, "ROLLBACK TO SAVEPOINT sp")
        _run(server, session, "COMMIT")
        assert _rows(
            server, Session(database=db), "SELECT id, v FROM t"
        ) == [(1, 10)]
    finally:
        db.close()


def test_rollback_to_reports_undo_failure_instead_of_swallowing(tmp_path):
    """行 undo 抛错时必须把事务标成失败，不能静默继续让别人 COMMIT。"""
    db = Database(
        tmp_path / "logical_sp_undo_failure",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    try:
        session = Session(database=db)
        _run(server, session, "BEGIN")
        _run(server, session, "SAVEPOINT sp")

        def boom() -> None:
            raise RuntimeError("模拟行 undo 失败")

        session.txn.row_undo_log.append(boom)
        with pytest.raises(TransactionError) as exc:
            _run(server, session, "ROLLBACK TO SAVEPOINT sp")
        assert "撤销回调失败" in str(exc.value)
        # 事务仍可显式回滚结束，不会把半回滚状态留给下一次 COMMIT
        _run(server, session, "ROLLBACK")
    finally:
        db.close()


def test_abort_reports_undo_failure_but_still_releases_the_transaction(tmp_path):
    """ABORT 的目录 undo 失败时必须报错，但仍要落 ABORTED、释放锁。"""
    db = Database(tmp_path / "abort_undo_failure", concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    session = Session(database=db)
    server = PGWireServer(db)
    try:
        _run(server, session, "BEGIN")
        _run(server, session, "INSERT INTO t VALUES (1)")

        def boom() -> None:
            raise RuntimeError("模拟目录 undo 失败")

        session.txn.undo_log.append(boom)
        with pytest.raises(TransactionError) as exc:
            db.txns.abort(session.txn)
        assert "ABORT 的撤销回调失败" in str(exc.value)
        assert db.txns.active_xids == []
        # 锁已释放，新事务可以继续写
        fresh = Session(database=db)
        _run(server, fresh, "BEGIN")
        _run(server, fresh, "INSERT INTO t VALUES (2)")
        _run(server, fresh, "COMMIT")
        assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(1,)]
    finally:
        db.close()


def test_abort_all_continues_after_one_transaction_fails(tmp_path):
    """批量 ABORT 中一个 undo 抛错，不能把后面的 ACTIVE 事务留在锁上。"""
    db = Database(tmp_path / "abort_all_failure", concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = PGWireServer(db)
    first = Session(database=db)
    second = Session(database=db)
    try:
        _run(server, first, "BEGIN")
        _run(server, first, "INSERT INTO t VALUES (1)")
        _run(server, second, "BEGIN")
        _run(server, second, "INSERT INTO t VALUES (2)")

        def boom() -> None:
            raise RuntimeError("模拟目录 undo 失败")

        first.txn.undo_log.append(boom)
        with pytest.raises(TransactionError) as exc:
            db.txns.abort_all()
        assert "批量 ABORT" in str(exc.value)
        assert db.txns.active_xids == []
        assert _rows(server, Session(database=db), "SELECT COUNT(*) FROM t") == [(0,)]
    finally:
        db.close()


def test_drop_view_index_and_reindex_block_active_reader(tmp_path):
    """DROP VIEW/INDEX、REINDEX 也会让旧快照手里的对象或索引根失效。"""
    db = Database(
        tmp_path / "ddl_guard_drop_view_index_reindex",
        concurrent_dml=True,
        logical_savepoints=True,
    )
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("CREATE INDEX idx_v ON t (v)")
        db.execute_one("CREATE VIEW vw AS SELECT * FROM t")
        db.execute_one("INSERT INTO t VALUES (1, 1)")
        server = PGWireServer(db)
        reader = Session(database=db)
        writer = Session(database=db)
        _run(server, reader, "BEGIN")
        assert _rows(server, reader, "SELECT * FROM vw") == [(1, 1)]
        for sql in ("DROP VIEW vw", "DROP INDEX idx_v", "REINDEX TABLE t"):
            with pytest.raises(TransactionError, match="仍在活跃"):
                _run(server, writer, sql)
            # 被挡住后旧快照仍然可读，索引和视图都没有被提前摘掉。
            assert _rows(server, reader, "SELECT * FROM vw") == [(1, 1)]
        _run(server, reader, "COMMIT")
        for sql in ("DROP VIEW vw", "DROP INDEX idx_v", "REINDEX TABLE t"):
            _run(server, writer, sql)
        assert _rows(server, Session(database=db), "SELECT * FROM t") == [(1, 1)]
    finally:
        db.close()


def test_close_still_closes_handles_when_current_txn_undo_fails(tmp_path):
    """close 遇到 ABORT undo 失败也要把 WAL/磁盘句柄关掉，不能半开。"""
    db = Database(tmp_path / "close_undo_failure", concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    db.execute("BEGIN")
    db.execute("INSERT INTO t VALUES (1)")

    def boom() -> None:
        raise RuntimeError("模拟目录 undo 失败")

    db._txn.undo_log.append(boom)
    try:
        db.close()
    finally:
        assert db._closed is True
    # 再关一次必须幂等，不重复释放
    db.close()


def test_online_vacuum_reclaims_indexed_dead_root_without_blocking_reinsert(tmp_path):
    """整条死链连根回收后，陈旧索引项回堆为空，同主键重插不能误报重复。"""
    db = Database(tmp_path / "vacuum_dead_root_index", concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (1, 10), (2, 20)")
    db.execute_one("DELETE FROM t WHERE id = 2")
    reader = Session(database=db)
    server = PGWireServer(db)
    try:
        _run(server, reader, "BEGIN")
        vac = db.txns.begin()
        table = db.catalog.get_table("t")
        kept, dead, _pages = HeapFile(db.bp, table, db.txns).vacuum_online(
            vac, db.txns.active_transactions()
        )
        db.txns.commit(vac)
        assert (kept, dead) == (1, 1)
        _run(server, reader, "COMMIT")

        writer = Session(database=db)
        _run(server, writer, "INSERT INTO t VALUES (2, 20)")
        assert _rows(server, writer, "SELECT id, v FROM t ORDER BY id") == [
            (1, 10),
            (2, 20),
        ]
    finally:
        db.close()
