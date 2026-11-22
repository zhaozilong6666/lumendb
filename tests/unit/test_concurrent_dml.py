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


def test_savepoint_lock_can_be_released_by_another_thread(env):
    """保存点可能在别的 worker 建；COMMIT 跨线程释放不能自锁/报 owner 错。"""
    db, server = env
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


def test_close_with_savepoint_releases_the_gate(env):
    """同线程 close 进到保存点事务：abort 后必须放门，不能留下 no_readers。"""
    db, server = env
    session = Session(database=db)
    _run(server, session, "BEGIN")
    _run(server, session, "SAVEPOINT s")
    _run(server, session, "INSERT INTO t VALUES (9, 9, 'z')")
    db.close()
    assert db._no_readers is False


def test_savepoint_waits_for_exclusive_and_rolls_back(env):
    """并发模式下保存点不再 0A000：排空在途语句后独占，回滚只撤保存点后写入。"""
    db, server = env
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
