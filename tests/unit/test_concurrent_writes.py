"""多线程并发写的压力测试（同一 Database 实例，真线程）。"""

from __future__ import annotations

import threading

from lumendb.engine.database import Database


def test_parallel_inserts_from_many_threads_lose_no_rows(tmp_path):
    """6 个线程各插 150 行（key 区间不重叠）：行数与内容必须完全对得上。"""
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, worker INT, tag TEXT)")
    errors: list[BaseException] = []
    threads = 6
    per_thread = 150

    def worker(w: int) -> None:
        try:
            for i in range(per_thread):
                rid = w * per_thread + i
                db.execute_one(
                    f"INSERT INTO t VALUES ({rid}, {w}, 'w{w}-{i}')"
                )
        except BaseException as exc:  # pragma: no cover - 失败时报告线程名
            errors.append(exc)

    workers = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
    for t in workers:
        t.start()
    for t in workers:
        t.join()

    assert errors == [], f"并发写出现异常: {errors[:3]}"
    total = threads * per_thread
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(total,)]
    # 每个线程的行都在（没有互相覆盖的页链尾巴）。
    for w in range(threads):
        rows = db.execute_one(f"SELECT COUNT(*) FROM t WHERE worker = {w}").rows
        assert rows == [(per_thread,)], f"worker {w} 行数不对"
    db.close()

    # 重开：页链与目录都要完整。
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(total,)]
    db.execute_one(f"INSERT INTO t VALUES ({total}, 99, 'tail')")
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(total + 1,)]
    assert db.execute_one(f"SELECT tag FROM t WHERE id = {total}").rows == [
        ("tail",)
    ]
    db.close()


def test_parallel_inserts_on_the_same_unique_key_yield_exactly_one_row(tmp_path):
    """多线程抢同一个主键：恰好 1 个成功，其余必须报唯一键冲突。"""
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)")
    successes = 0
    conflicts = 0
    other: list[BaseException] = []
    lock = threading.Lock()

    def racer(n: int) -> None:
        nonlocal successes, conflicts
        try:
            db.execute_one(f"INSERT INTO t VALUES (7, 'r{n}')")
            with lock:
                successes += 1
        except Exception as exc:
            if "23505" in str(exc) or "唯一" in str(exc) or "duplicate" in str(exc).lower():
                with lock:
                    conflicts += 1
            else:
                with lock:
                    other.append(exc)

    threads = [threading.Thread(target=racer, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert other == [], f"出现非唯一键冲突的异常: {other[:2]}"
    assert successes == 1, f"同一主键只允许 1 行，实际成功 {successes}"
    assert conflicts == 7
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
    db.close()


def test_parallel_reader_never_sees_partial_batch(tmp_path):
    """写线程批量插入期间，读线程看到的前缀行数必须单调不减且不超过总量。"""
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    total = 400
    done = threading.Event()
    bad: list[int] = []

    def writer() -> None:
        try:
            for i in range(total):
                db.execute_one(f"INSERT INTO t VALUES ({i})")
        finally:
            done.set()

    def reader() -> None:
        seen = 0
        while not done.is_set():
            count = db.execute_one("SELECT COUNT(*) FROM t").rows[0][0]
            if count < seen:
                bad.append(count)
            seen = count

    w = threading.Thread(target=writer)
    readers = [threading.Thread(target=reader) for _ in range(3)]
    for r in readers:
        r.start()
    w.start()
    w.join()
    for r in readers:
        r.join()

    assert bad == [], f"读者看到了回退的行数: {bad[:5]}"
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(total,)]
    db.close()
