"""WAL 写失败（磁盘满）时的事务状态机。

磁盘满不是"理论上的故障"：WAL 追加 COMMIT 记录时 ``write`` 可能返回 ENOSPC，
此时事务既没有持久化、页面上却可能已经改了数据。正确的行为是三条：

1. 调用方拿到明确的错误（不能假装提交成功）；
2. 失败事务的改动对后续查询不可见（页级 undo 或者干脆不让它污染页面）；
3. 写者状态机不被卡死（后续事务还能继续写）。
"""

from __future__ import annotations

import errno

import pytest

from lumendb.engine.database import Database


def test_commit_record_failure_leaves_no_dirty_state(tmp_path, monkeypatch):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT)")

    real_commit = db.wal.commit

    def boom(xid: int) -> int:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(db.wal, "commit", boom)
    with pytest.raises(OSError):
        db.execute_one("INSERT INTO t VALUES (1)")
    monkeypatch.setattr(db.wal, "commit", real_commit)

    # 1) 失败的那条不能出现——页面上即使被改过，也不能当已提交读出来。
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
    # 2) 写者状态机没有被卡死：后续写还能成功。
    db.execute_one("INSERT INTO t VALUES (2)")
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
    db.close()


def test_auto_commit_statement_failure_is_reported_and_recoverable(
    tmp_path, monkeypatch
):
    """多语句事务里 COMMIT 失败：整笔都不算数，且库仍可用。"""
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    real_commit = db.wal.commit

    def boom(xid: int) -> int:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(db.wal, "commit", boom)
    with pytest.raises(OSError):
        db.execute_one("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")
    monkeypatch.setattr(db.wal, "commit", real_commit)

    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
    db.execute_one("INSERT INTO t VALUES (3)")
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
    db.close()


def test_disk_full_does_not_wrap_the_writer_lock(tmp_path, monkeypatch):
    """最坏情况：COMMIT 与 ABORT 记录都写不进 WAL（磁盘彻底满）。

    此时页可能已经不干净（undo 记录也没法持久化），但**写者锁必须交出来**，
    否则单写者模型下这台库永久只能读。这条测试直接看锁管理器里还有没有
    持有者，比"再写一条试试"更接近不变量本身。
    """
    from lumendb.storage.lock import writer_resource

    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT)")

    def boom(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    for name in ("commit", "abort", "flush"):
        monkeypatch.setattr(db.wal, name, boom)

    with pytest.raises(OSError):
        db.execute_one("INSERT INTO t VALUES (1)")

    monkeypatch.undo()

    # 不变量 1：写者锁不能留在任何事务名下。磁盘满是可以恢复的外部故障，
    # 不能因为一次 ABORT 记录写不进去就把整台库变成永久只读。
    assert not db.executor._locks.holders(writer_resource()), (
        "磁盘满之后写者锁泄漏了：这台库将永久无法写入"
    )

    # 不变量 2：内存里的"半撤销页"绝不能落盘。flush 失败 → 该页仍是脏页，
    # 重启后从磁盘读到的是事务开始前的版本，所以失败的那一行必须不存在。
    # （已知边界：内存页可能处于半撤销状态，此进程内继续读可能抛
    #  struct.error —— 那是"进程内页已坏"，不是持久化数据坏了。正确处置
    #  是重启 / 从副本重建，见 docs/concurrency.md。）
    try:
        db.close()
    except OSError:
        pass  # 关闭时再刷一次脏页，磁盘仍满就会继续失败——预期之内

    reopened = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    try:
        assert reopened.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
    finally:
        reopened.close()