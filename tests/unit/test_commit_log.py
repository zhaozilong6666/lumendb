"""SHOW COMMIT LOG：跨 checkpoint 的持久提交 LSN 账本。"""

import pytest

from lumendb.engine.database import Database
from lumendb.errors import SQLSyntaxError


def test_commit_log_tracks_writes_and_rebuilds_after_reopen(tmp_path):
    db = Database(tmp_path)
    assert db.execute_one("SHOW COMMIT LOG").rows == []
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    db.execute_one("INSERT INTO t VALUES (1)")
    db.execute_one("SELECT * FROM t")  # 只读自动提交不应进账本
    rows = db.execute_one("SHOW COMMIT LOG").rows
    assert len(rows) == 2
    assert all(row[1] > 0 for row in rows)
    assert rows[0][1] < rows[1][1]
    db.close()

    db = Database(tmp_path)
    assert db.execute_one("SHOW COMMIT LOG").rows == rows
    db.close()


def test_commit_log_limit_and_validation(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT)")
    for value in range(3):
        db.execute_one(f"INSERT INTO t VALUES ({value})")
    all_rows = db.execute_one("SHOW COMMIT LOG").rows
    assert len(all_rows) == 4
    assert db.execute_one("SHOW COMMIT LOG LIMIT 2").rows == all_rows[-2:]
    assert db.execute_one("SHOW COMMIT LOG LIMIT 0").rows == []
    with pytest.raises(SQLSyntaxError):
        db.execute_one("SHOW COMMIT LOG LIMIT -1")
    db.close()


def test_checkpoint_keeps_wal_recoverable_when_ledger_write_fails(
    tmp_path, monkeypatch
):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT)")
    db.execute_one("INSERT INTO t VALUES (1)")
    before = db.disk.meta.checkpoint_lsn

    with monkeypatch.context() as patch:
        def fail_write(entries):
            raise OSError("模拟账本落盘失败")

        patch.setattr(db.commit_ledger, "write", fail_write)
        with pytest.raises(OSError, match="账本落盘失败"):
            db.checkpoint()

    # 账本先写、checkpoint_lsn 后推进：写入失败时元数据必须停在旧值，
    # WAL 也没有被裁，重启仍能重建提交账本。
    assert db.disk.meta.checkpoint_lsn == before
    db.close()

    db = Database(tmp_path)
    rows = db.execute_one("SHOW COMMIT LOG").rows
    assert len(rows) == 2
    db.close()


def test_replace_commit_log_keeps_commits_after_checkpoint_snapshot(tmp_path):
    db = Database(tmp_path)
    try:
        # checkpoint 生成账本快照后、替换内存账本前，可能有新提交；对齐
        # 磁盘账本时不能把它们误删。
        db.txns.install_commit_lsns({1: 10, 2: 20})
        db.txns.replace_commit_log([(1, 10)])
        assert db.txns.commit_log() == [(1, 10), (2, 20)]

        db.txns.replace_commit_log([(2, 20)])
        assert db.txns.commit_log() == [(2, 20)]
    finally:
        db.close()


def test_checkpoint_preserves_commit_log_in_persistent_ledger(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT)")
    db.execute_one("INSERT INTO t VALUES (1)")
    assert len(db.execute_one("SHOW COMMIT LOG").rows) == 2
    db.checkpoint()
    # checkpoint 会裁 WAL 旧段，但持久账本保留最近提交；SHOW COMMIT LOG
    # 仍应看到 checkpoint 之前的两个提交点，而不是变成空表。
    assert len(db.execute_one("SHOW COMMIT LOG").rows) == 2
    db.execute_one("INSERT INTO t VALUES (2)")
    assert len(db.execute_one("SHOW COMMIT LOG").rows) == 3
    db.close()

    db = Database(tmp_path)
    assert len(db.execute_one("SHOW COMMIT LOG").rows) == 3
    db.close()
