"""第四个 Cherry 故障窗口：COMMIT 记录进 WAL 但 fsync 未返回。

固定交错：

1. 写线程把 INSERT 的 COMMIT 记录交给 WAL，停在 ``fsync`` 内部；
2. 主线程在 fsync 未完成时查询：这一行必须**不可见**；
3. 放行 fsync 后写线程提交成功，行才出现；
4. 关闭重开，行仍在。

如果提交判定发生在 fsync 之前，第 2 步会看到行；如果 fsync 失败被静默吞掉，
第 4 步会丢行。这个闸门钉住的是“可见性必须晚于 durable”。
"""

from __future__ import annotations

import pathlib
import threading

from lumendb.engine.database import Database
from lumendb.storage import wal_segments

from .trace import HoldPoint, Trace


def run_fsync_case(seed: int, tmp_path: pathlib.Path) -> tuple[Trace, dict]:
    db = Database(tmp_path / "db", concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    # 这个只读快照只用来做 MVCC 可见性判断，不执行 SQL：Database 的
    # ``_statement_lock`` 会把同一句柄上的并发 SELECT 串行化，若在这里查
    # 表会等写线程提交，测不到 fsync 窗口。
    reader = db.txns.begin(read_only=True)
    real_commit = db.txns.commit
    writer_holder: dict[str, object] = {}

    def recording_commit(txn):
        writer_holder["txn"] = txn
        return real_commit(txn)

    db.txns.commit = recording_commit
    trace = Trace()
    hold = HoldPoint(f"fsync{seed}.durable", trace)
    real_fsync = wal_segments.os.fsync
    calls = {"n": 0}
    errors: list[str] = []

    def blocking_fsync(fd: int) -> None:
        if threading.current_thread().name == "fsync-writer" and calls["n"] == 0:
            calls["n"] += 1
            trace.mark("fsync.start")
            hold.wait()
            trace.mark("fsync.end")
        real_fsync(fd)

    def writer() -> None:
        try:
            db.execute_one(f"INSERT INTO t VALUES ({seed}, {seed})")
            trace.mark("writer.done")
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"writer: {type(exc).__name__}: {exc}")

    wal_segments.os.fsync = blocking_fsync
    thread = threading.Thread(target=writer, name="fsync-writer")
    try:
        thread.start()
        assert hold.entered.wait(timeout=10), f"写线程没有停在 fsync；\n{trace.dump()}"
        writer_txn = writer_holder.get("txn")
        assert writer_txn is not None, "拿不到写事务"
        visible_before = db.txns.is_visible(
            reader, writer_txn.xid, 0, ri=True
        )
        trace.mark("reader.check", visible=visible_before)
        assert not visible_before, f"fsync 未完成时行已可见；\n{trace.dump()}"
        assert writer_txn.state.name == "ACTIVE", "fsync 返回前事务不能标记 COMMITTED"
        hold.release()
        thread.join(timeout=30)
        assert not thread.is_alive(), "写线程未退出"
        assert errors == [], f"fsync 场景出错: {errors[:2]}；\n{trace.dump()}"
        visible_after = db.txns.is_visible(
            reader, writer_txn.xid, 0, ri=True
        )
        trace.mark("reader.after", visible=visible_after)
        assert visible_after, "fsync 完成后事务仍不可见"
        rows_after = db.execute_one("SELECT id, v FROM t").rows
        assert rows_after == [(seed, seed)], f"fsync 完成后行仍不可见: {rows_after}"
    finally:
        wal_segments.os.fsync = real_fsync
        hold.release()
        thread.join(timeout=5)
        db.txns.commit(reader)
        db.close()

    reopened = Database(tmp_path / "db")
    try:
        rows = reopened.execute_one("SELECT id, v FROM t").rows
        assert rows == [(seed, seed)], f"重开后 fsync 过的行丢了: {rows}"
    finally:
        reopened.close()
    return trace, {"visible_before": visible_before, "rows_after": rows_after}


if __name__ == "__main__":  # pragma: no cover
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        trace, detail = run_fsync_case(0, pathlib.Path(td))
        print(trace.dump())
        print(detail)
