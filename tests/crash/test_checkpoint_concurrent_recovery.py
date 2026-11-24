"""并发 DML 与 checkpoint 交错的崩溃恢复：checkpoint 不能跳过在途脏页。

修复前 ``Recovery.checkpoint`` 先 ``flush_all`` 再追加 CHECKPOINT 记录。
并发 DML 不拿 ``Database._write_lock``，写者可以在“页已刷完、记录还没写”
的窗口里提交：它的 WAL 记录 LSN 会落在新 checkpoint_lsn 之前，但页还在
缓冲池里。崩溃恢复从 checkpoint 之后 redo，这条更新就被跳过，页回退到旧值。
"""

from __future__ import annotations

import threading

from lumendb.engine.database import Database


def _crash(db: Database) -> None:
    db.wal.flush()
    db.wal.close()
    db.disk.close()


def test_checkpoint_does_not_skip_page_dirtied_during_flush(tmp_path, monkeypatch):
    path = tmp_path / "db"
    db = Database(path, concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (0, 0)")
    # 先清一次，让基线 checkpoint 之后只剩本用例要制造的脏页。
    db.checkpoint()
    # 写者提交后不再触发自动 checkpoint，避免它去抢 _write_lock。
    monkeypatch.setattr(db, "_maybe_checkpoint", lambda: None)
    # 制造一个待刷脏页：checkpoint 会先把它写盘，再被下面的写者改成 v=1。
    db.execute_one("UPDATE t SET v = 2 WHERE id = 0")

    original_flush_all = db.bp.flush_all
    armed = {"value": True}
    writer_go = threading.Event()
    writer_done = threading.Event()

    def writer() -> None:
        assert writer_go.wait(10)
        db.execute_one("UPDATE t SET v = 1 WHERE id = 0")
        writer_done.set()

    def flush_all_then_wait() -> int:
        count = original_flush_all()
        if armed["value"]:
            armed["value"] = False
            # flush 已返回、CHECKPOINT 记录还没写：让写者在这个缝里提交。
            writer_go.set()
            assert writer_done.wait(10)
        return count

    monkeypatch.setattr(db.bp, "flush_all", flush_all_then_wait)
    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        db.checkpoint()
    finally:
        thread.join(10)

    _crash(db)

    db2 = Database(path, concurrent_dml=True)
    try:
        assert db2.execute_one("SELECT v FROM t WHERE id = 0").rows == [(1,)]
    finally:
        db2.close()
