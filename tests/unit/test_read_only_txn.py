"""只读事务不写 WAL（自动提交的 SELECT 不再 fsync）。"""

from __future__ import annotations

from lumendb.engine.database import Database
from lumendb.storage.txn import TxnState


def _open(tmp_path):
    return Database(tmp_path / "db", checkpoint_wal_bytes=0)


def _seed(db):
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    for i in range(10):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'n{i}')")


def test_auto_commit_select_does_not_touch_wal(tmp_path):
    """自动提交的 SELECT 不写 BEGIN/COMMIT 记录，也不触发 fsync。"""
    db = _open(tmp_path)
    try:
        _seed(db)
        base = db.wal.next_lsn
        for _ in range(50):
            assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(10,)]
        assert db.wal.next_lsn == base, "只读语句不应产生任何 WAL 记录"
        # 反复读之后活跃集合为空（没有泄漏 ACTIVE 事务）。
        assert db.txns.active_xids == []
    finally:
        db.close()


def test_show_and_explain_are_read_only_too(tmp_path):
    db = _open(tmp_path)
    try:
        _seed(db)
        base = db.wal.next_lsn
        db.execute_one("SHOW TABLES")
        db.execute_one("EXPLAIN SELECT * FROM t")
        assert db.wal.next_lsn == base
    finally:
        db.close()


def test_write_transaction_still_logs_begin_and_commit(tmp_path):
    """写事务必须继续写 BEGIN/COMMIT：只读优化不能顺手把写路径也关掉。"""
    db = _open(tmp_path)
    try:
        db.execute_one("CREATE TABLE t (id INT)")
        base = db.wal.next_lsn
        db.execute_one("INSERT INTO t VALUES (1)")
        assert db.wal.next_lsn > base
    finally:
        db.close()


def test_explicit_read_only_transaction_lifecycle(tmp_path):
    db = _open(tmp_path)
    try:
        _seed(db)
        base = db.wal.next_lsn
        txn = db.txns.begin(read_only=True)
        assert txn.read_only is True
        assert db.txns.state_of(txn.xid) is TxnState.ACTIVE
        assert txn.xid in db.txns.active_xids
        # 快照起点必须登记：长读事务要阻止 checkpoint 回收它还要读的旧版本。
        assert txn.first_lsn == base

        db.txns.commit(txn)
        assert db.txns.state_of(txn.xid) is TxnState.COMMITTED
        assert txn.xid not in db.txns.active_xids
        assert db.wal.next_lsn == base, "只读事务提交不应写 WAL"
    finally:
        db.close()


def test_read_only_abort_does_not_write_wal(tmp_path):
    db = _open(tmp_path)
    try:
        _seed(db)
        base = db.wal.next_lsn
        txn = db.txns.begin(read_only=True)
        db.txns.abort(txn)
        assert db.txns.state_of(txn.xid) is TxnState.ABORTED
        assert db.txns.active_xids == []
        assert db.wal.next_lsn == base, "只读事务回滚不应写 WAL"
    finally:
        db.close()


def test_reopen_without_read_records_keeps_data(tmp_path):
    """只读事务不写记录：崩溃/重启后数据与行数不受影响。"""
    db = _open(tmp_path)
    try:
        _seed(db)
        for _ in range(10):
            db.execute_one("SELECT COUNT(*) FROM t")
    finally:
        db.close()
    db = _open(tmp_path)
    try:
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(10,)]
        db.execute_one("INSERT INTO t VALUES (10, 'n10')")
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(11,)]
    finally:
        db.close()
