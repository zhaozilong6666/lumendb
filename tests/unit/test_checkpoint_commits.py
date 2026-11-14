"""按提交数触发的自动 checkpoint。"""

from lumendb.engine.database import Database


def _count(database: Database, table: str) -> int:
    return database.execute_one(f"SELECT COUNT(*) FROM {table}").rows[0][0]


def test_auto_checkpoint_every_n_commits(tmp_path):
    path = tmp_path / "commit-checkpoint"
    db = Database(path, checkpoint_wal_bytes=0, checkpoint_every_commits=3)
    db.execute_one("CREATE TABLE t (id INT)")
    db.checkpoint()
    before = db.disk.meta.checkpoint_lsn

    db.execute_one("INSERT INTO t VALUES (1)")
    assert db._commits_since_checkpoint == 1
    assert db.disk.meta.checkpoint_lsn == before
    db.execute_one("INSERT INTO t VALUES (2)")
    assert db._commits_since_checkpoint == 2
    assert db.disk.meta.checkpoint_lsn == before
    db.execute_one("INSERT INTO t VALUES (3)")
    assert db._commits_since_checkpoint == 0
    assert db.disk.meta.checkpoint_lsn > before
    db.close()

    reopened = Database(path)
    assert _count(reopened, "t") == 3
    reopened.close()


def test_commit_checkpoint_counts_one_commit_per_transaction(tmp_path):
    path = tmp_path / "txn-commit-checkpoint"
    db = Database(path, checkpoint_wal_bytes=0, checkpoint_every_commits=2)
    db.execute_one("CREATE TABLE t (id INT)")
    db.checkpoint()
    before = db.disk.meta.checkpoint_lsn

    db.execute_one("BEGIN")
    db.execute_one("INSERT INTO t VALUES (1)")
    db.execute_one("INSERT INTO t VALUES (2)")
    assert db._commits_since_checkpoint == 0
    db.execute_one("COMMIT")
    assert db._commits_since_checkpoint == 1
    assert db.disk.meta.checkpoint_lsn == before
    db.close()
