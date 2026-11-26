"""SHOW COMMIT LOG：当前 WAL 窗口内的提交 LSN 账本。"""

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


def test_checkpoint_trims_commit_log_to_current_wal_window(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT)")
    db.execute_one("INSERT INTO t VALUES (1)")
    assert len(db.execute_one("SHOW COMMIT LOG").rows) == 2
    db.checkpoint()
    assert db.execute_one("SHOW COMMIT LOG").rows == []
    db.execute_one("INSERT INTO t VALUES (2)")
    assert len(db.execute_one("SHOW COMMIT LOG").rows) == 1
    db.close()

    db = Database(tmp_path)
    assert len(db.execute_one("SHOW COMMIT LOG").rows) == 1
    db.close()
