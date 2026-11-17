"""结构性写（VACUUM / TRUNCATE / shadow heap）与并发读的互斥门。"""

from __future__ import annotations

import threading
import time

from lumendb.engine.database import Database
from lumendb.sql.parser import parse_sql


def _open(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
    for i in range(600):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'pad{i}')")
    return db


def test_vacuum_waits_for_readers_instead_of_failing(tmp_path):
    """VACUUM 不再因为"有并发读"报错：它等读者退出后执行。

    此前 ``HeapFile.vacuum`` 会在 ``active_xids`` 非空时直接抛
    StorageError——读密集型系统里 VACUUM 永远跑不起来。
    """
    db = _open(tmp_path)
    read_exec = db.make_executor()
    count_stmt = parse_sql("SELECT COUNT(*) FROM t")[0]
    stop = threading.Event()
    errors: list[str] = []
    vac_ok = [0]

    def reader() -> None:
        try:
            while not stop.is_set():
                with db.read_guard():
                    txn = db.txns.begin(read_only=True)
                    try:
                        assert read_exec.run(count_stmt, txn).rows == [(600,)]
                    finally:
                        db.txns.commit(txn)
        except BaseException as exc:
            errors.append(f"reader: {exc!r}")

    def vacummer() -> None:
        try:
            for _ in range(3):
                db.execute_one("VACUUM t")
                vac_ok[0] += 1
        except BaseException as exc:
            errors.append(f"vacuum: {exc!r}")

    readers = [threading.Thread(target=reader) for _ in range(3)]
    vac = threading.Thread(target=vacummer)
    for r in readers:
        r.start()
    vac.start()
    vac.join()
    stop.set()
    for r in readers:
        r.join()

    assert errors == [], f"出现异常: {errors[:3]}"
    assert vac_ok[0] == 3, "VACUUM 在并发读下应当全部成功"
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(600,)]
    db.close()


def test_reader_blocks_while_exclusive_guard_is_held(tmp_path):
    """结构性写独占期间，新的只读语句必须等它结束。"""
    db = _open(tmp_path)
    holding = threading.Event()
    hold_until = 0.10
    reader_entered_at: list[float] = []

    def holder() -> None:
        with db.exclusive_guard():
            holding.set()
            time.sleep(hold_until)

    def reader() -> None:
        holding.wait()
        with db.read_guard():
            reader_entered_at.append(time.perf_counter())

    h = threading.Thread(target=holder)
    r = threading.Thread(target=reader)
    h.start()
    r.start()
    h.join()
    r.join()

    assert reader_entered_at, "读者应当最终进入"
    assert reader_entered_at[0] > time.perf_counter() - 0.02, "读者应在独占结束后进入"
    db.close()


def test_exclusive_guard_waits_for_inflight_reader(tmp_path):
    """反向：有在途读者时，结构性写必须等它退出才能进入。"""
    db = _open(tmp_path)
    reader_inside = threading.Event()
    reader_hold = 0.08
    held_for: list[float] = []

    def reader() -> None:
        with db.read_guard():
            reader_inside.set()
            time.sleep(reader_hold)

    def writer() -> None:
        reader_inside.wait()
        started = time.perf_counter()
        with db.exclusive_guard():
            held_for.append(time.perf_counter() - started)

    r = threading.Thread(target=reader)
    w = threading.Thread(target=writer)
    r.start()
    w.start()
    r.join()
    w.join()

    assert held_for and held_for[0] >= reader_hold * 0.6, (
        f"结构性写应等读者退出（实测等待 {held_for[0]:.3f}s）"
    )
    db.close()


def test_structural_statements_are_marked(tmp_path):
    """从 SQL 进到 AST 的 VACUUM / TRUNCATE / ALTER TYPE 都要被识别为结构性。"""
    from lumendb.engine.database import _STRUCTURAL_STATEMENTS

    for sql in (
        "VACUUM t",
        "TRUNCATE t",
        "ALTER TABLE t ALTER COLUMN pad TYPE VARCHAR(8)",
    ):
        name = type(parse_sql(sql)[0]).__name__
        assert name in _STRUCTURAL_STATEMENTS, f"{sql} → {name} 未被标记"
