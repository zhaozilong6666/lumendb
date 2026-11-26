"""第三个确定性重放场景：有未提交写入时，并发 DDL 必须被拒绝。

关键不是“DDL 偶尔失败”，而是固定住这个窗口：

1. A 显式事务已插入一行，但停在 COMMIT 前；
2. B 的 ``ALTER TABLE ... ADD COLUMN`` 在 A 提交前尝试；
3. 断言 DDL 在 A 提交前被 40001 拒绝，而不是悄悄重写 shadow heap；
4. 放行 A 提交，B 重试 DDL 成功；
5. 最终两行都在，且都能按新列形状读出——未提交行没有落在旧 heap 里丢失。
"""

from __future__ import annotations

import pathlib
import threading

from lumendb.engine.database import Database
from lumendb.errors import TransactionError
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql

from .trace import HoldPoint, Trace


def _run(server: PGWireServer, session: Session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


def run_ddl_case(seed: int, tmp_path: pathlib.Path) -> tuple[Trace, dict]:
    db = Database(tmp_path, concurrent_dml=True, logical_savepoints=True)
    server = PGWireServer(db)
    trace = Trace()
    hold = HoldPoint(f"ddl{seed}.before_commit", trace)
    errors: list[str] = []

    def setup() -> None:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
        db.execute_one("INSERT INTO t VALUES (1, 1, 'a')")

    def writer_a() -> None:
        try:
            session = Session(database=db)
            _run(server, session, "BEGIN")
            _run(server, session, f"INSERT INTO t VALUES ({seed + 2}, {seed + 2}, 'b')")
            trace.mark("A.insert.done")
            hold.wait()
            trace.mark("A.commit.start")
            _run(server, session, "COMMIT")
            trace.mark("A.commit.end")
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"A: {type(exc).__name__}: {exc}")

    a = threading.Thread(target=writer_a, name="writer-a")
    try:
        setup()
        a.start()
        assert hold.entered.wait(timeout=5), f"A 没有停在提交前；\n{trace.dump()}"
        session_b = Session(database=db)
        try:
            trace.mark("B.ddl.first")
            _run(server, session_b, "ALTER TABLE t ADD COLUMN extra INT")
        except TransactionError as exc:
            trace.mark("B.ddl.rejected", sqlstate=getattr(exc, "sqlstate", "?"))
        else:
            raise AssertionError("A 未提交时 DDL 竟然成功；\n" + trace.dump())
        assert trace.count("B.ddl.rejected") == 1
        hold.release()
        a.join(timeout=30)
        assert not a.is_alive(), "A 未退出"
        assert errors == [], f"DDL 场景出错: {errors[:2]}；\n{trace.dump()}"
        trace.mark("B.ddl.retry")
        _run(server, session_b, "ALTER TABLE t ADD COLUMN extra INT")
        rows = _run(
            server, Session(database=db), "SELECT id, v, extra FROM t ORDER BY id"
        )[0].rows
        assert rows == [(1, 1, None), (seed + 2, seed + 2, None)], (
            f"DDL 后行丢失或形状不符: {rows}；\n{trace.dump()}"
        )
        assert trace.first_seq("B.ddl.rejected") < trace.first_seq("A.commit.end")
        return trace, {"rows": rows, "seed": seed}
    finally:
        hold.release()
        db.close()
