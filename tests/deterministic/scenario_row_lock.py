"""第二个确定性重放场景：两个事务写同一行，后写者必须等前写者提交。

不是“跑很多线程碰运气”，而是用闸门固定：
1. A 显式事务已拿到行锁、改完值，停在 COMMIT 前；
2. B 的自动提交 UPDATE 进入 ``LockManager.acquire`` 并阻塞；
3. 断言 B 的 acquire 在 A 提交前没有返回；
4. 放行 A 提交，B 唤醒后锁后重读，最终值必须是两次增量，不是 40001。
"""

from __future__ import annotations

import pathlib
import threading

from lumendb.engine.database import Database
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql

from .trace import HoldPoint, Trace


def _run(server: PGWireServer, session: Session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


def run_row_case(seed: int, tmp_path: pathlib.Path) -> tuple[Trace, dict]:
    initial = seed + 1
    db = Database(tmp_path, concurrent_dml=True)
    server = PGWireServer(db)
    trace = Trace()
    hold = HoldPoint(f"row{seed}.before_commit", trace)
    errors: list[str] = []
    original_acquire = db.locks.acquire
    original_try = db.locks.try_acquire

    def traced_acquire(xid, resource, mode, timeout=None):
        trace.mark("lock.acquire.start", xid=xid, resource=repr(resource))
        try:
            return original_acquire(xid, resource, mode, timeout)
        finally:
            trace.mark("lock.acquire.end", xid=xid, resource=repr(resource))

    def traced_try(xid, resource, mode):
        ok = original_try(xid, resource, mode)
        trace.mark("lock.try", xid=xid, ok=ok, resource=repr(resource))
        return ok

    db.locks.acquire = traced_acquire  # type: ignore[method-assign]
    db.locks.try_acquire = traced_try  # type: ignore[method-assign]

    def setup() -> None:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
        db.execute_one(f"INSERT INTO t VALUES (1, {initial}, 'row')")

    def writer_a() -> None:
        try:
            session = Session(database=db)
            _run(server, session, "BEGIN")
            _run(server, session, "UPDATE t SET v = v + 1 WHERE id = 1")
            hold.wait()
            trace.mark("A.commit.start")
            _run(server, session, "COMMIT")
            trace.mark("A.commit.end")
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"A: {type(exc).__name__}: {exc}")

    def writer_b() -> None:
        try:
            session = Session(database=db)
            trace.mark("B.update.start")
            _run(server, session, "UPDATE t SET v = v + 1 WHERE id = 1")
            trace.mark("B.update.end")
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"B: {type(exc).__name__}: {exc}")

    a = threading.Thread(target=writer_a, name="writer-a")
    b = threading.Thread(target=writer_b, name="writer-b")
    try:
        setup()
        a.start()
        assert hold.entered.wait(timeout=5), f"A 没有停在提交前；\n{trace.dump()}"
        b.start()
        trace.wait_for("lock.acquire.start", thread="writer-b", timeout=5)
        assert trace.count("lock.acquire.end", thread="writer-b") == 0, (
            "B 在 A 提交前就拿到了行锁；\n" + trace.dump()
        )
        hold.release()
        a.join(timeout=30)
        b.join(timeout=30)
        assert not a.is_alive() and not b.is_alive(), "线程未退出"
        assert errors == [], f"行锁场景出错: {errors[:2]}；\n{trace.dump()}"
        assert trace.count("lock.acquire.end", thread="writer-b") >= 1
        assert trace.first_seq("A.commit.start", thread="writer-a") < trace.first_seq(
            "lock.acquire.end", thread="writer-b"
        ), "B 的锁必须在 A 提交后才释放；\n" + trace.dump()
        rows = _run(server, Session(database=db), "SELECT v FROM t WHERE id = 1")[0].rows
        assert rows == [(initial + 2,)], f"两次增量应都保留，实际 {rows}"
        return trace, {"initial": initial, "final": rows[0][0]}
    finally:
        hold.release()
        db.close()
