"""并发 DML 模式（``Database(concurrent_dml=True)``）的回归。

默认模式仍由 ``_write_lock`` 串行，这里只覆盖"真正并行"的那条路径：
- DML 语句不再拿写者锁，由行/键锁 + 页 latch + ``read_guard`` 与 DDL/close 互斥；
- 目录保存改为短阻塞、用后立即释放；
- 索引根页号是共享单元（多个 BPlusTree 实例看到同一个当前根）；
- 回滚走逻辑路径：堆页/B+ 树页不写物理 undo，死行靠可见性与 VACUUM 回收。
"""

from __future__ import annotations

import threading
import traceback

import pytest

from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError, TransactionError
from lumendb.server.pgwire import PGWireServer, Session
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
