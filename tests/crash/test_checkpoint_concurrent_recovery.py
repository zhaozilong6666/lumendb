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


def test_checkpoint_does_not_deadlock_with_commit_holding_txn_lock(tmp_path, monkeypatch):
    """checkpoint 在 BufferPool 锁内不能再取 TransactionManager 锁。

    提交路径是 TM 锁 → BP 锁（``_mark_committed`` → ``clear_undo``），
    checkpoint 的 ``clean_barrier`` 却先拿 BP 锁。如果它在屏障里再调用
    会拿 TM 锁的 ``active_xids``，两边就形成 ABBA 死锁：8 线程基准实测
    卡在 ``flush_page`` 的 checkpoint 与 ``active_xids`` 两处。
    """
    db = Database(tmp_path / "db", concurrent_dml=True)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    db.execute_one("INSERT INTO t VALUES (0, 0)")

    tm_locked = threading.Event()
    release_tm = threading.Event()
    reached_wal_checkpoint = threading.Event()
    checkpoint_done = threading.Event()

    original_wal_checkpoint = db.wal.checkpoint

    def observed_wal_checkpoint(active_xids):
        # 能走到这里，说明屏障里没有因为 active_xids 去抢 TM 锁。
        reached_wal_checkpoint.set()
        return original_wal_checkpoint(active_xids)

    monkeypatch.setattr(db.wal, "checkpoint", observed_wal_checkpoint)

    def hold_tm_lock() -> None:
        with db.txns._lock:
            tm_locked.set()
            assert release_tm.wait(10)

    def run_checkpoint() -> None:
        db.checkpoint()
        checkpoint_done.set()

    holder = threading.Thread(target=hold_tm_lock, daemon=True)
    checker = threading.Thread(target=run_checkpoint, daemon=True)
    holder.start()
    assert tm_locked.wait(10)
    checker.start()
    try:
        # 修复前 checkpoint 会停在屏障里的 active_xids，到不了 WAL 追加点。
        assert reached_wal_checkpoint.wait(2), "checkpoint 在 BP 锁内抢 TM 锁：ABBA 死锁"
    finally:
        release_tm.set()
        holder.join(10)
        checker.join(10)
        assert checkpoint_done.is_set()
        db.close()


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
