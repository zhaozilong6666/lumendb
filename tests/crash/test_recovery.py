"""崩溃恢复的集成测试。

这些用例模拟真实的崩溃：数据页只在 Buffer Pool 内存里改过，从未落盘；
WAL 已经 fsync。然后丢掉整个进程状态，重新打开数据库，看恢复逻辑能不能
把"已提交的事务还在、未提交的事务消失"这件事做对。
"""

from dataclasses import dataclass

import pytest

from lumendb.catalog.catalog import Catalog
from lumendb.catalog.schema import Column, TableDef
from lumendb.catalog.store import CatalogStore
from lumendb.datatypes import DataType, TypeSpec
from lumendb.engine.database import Database
from lumendb.storage.bufferpool import BufferPoolManager
from lumendb.storage.heap import HeapFile
from lumendb.storage.pager import DiskManager
from lumendb.storage.recovery import RecoveryManager
from lumendb.storage.txn import TransactionManager
from lumendb.storage.wal import WAL
from lumendb.storage.wal_segments import SegmentedWAL


@dataclass
class Handle:
    dm: DiskManager
    wal: WAL
    bp: BufferPoolManager
    txns: TransactionManager
    cat: Catalog

    def heap(self, table: str) -> HeapFile:
        return HeapFile(self.bp, self.cat.get_table(table), self.txns)


def open_db(
    path,
    pool_size=8,
    recover=True,
    segmented=False,
    segment_bytes=64,
) -> Handle:
    dm = DiskManager(path / "data.db")
    if segmented:
        wal = SegmentedWAL(path / "wal", segment_bytes=segment_bytes)
    else:
        wal = WAL(path / "wal.log")
    bp = BufferPoolManager(dm, wal, pool_size=pool_size)
    txns = TransactionManager(dm, wal)
    if recover:
        RecoveryManager(dm, wal, bp, txns).recover()
    cat = Catalog(CatalogStore(bp))
    cat.load()
    return Handle(dm=dm, wal=wal, bp=bp, txns=txns, cat=cat)


def crash(handle: Handle) -> None:
    """模拟崩溃：WAL 落盘，数据页不落盘。"""
    handle.wal.flush()
    handle.wal.close()
    handle.dm.close()  # 只写元数据页，不 flush 数据页


@pytest.fixture
def db_path(tmp_path):
    return tmp_path


def columns():
    return [
        Column("id", TypeSpec(DataType.INT), nullable=False, primary_key=True),
        Column("name", TypeSpec(DataType.TEXT)),
    ]


class TestVacuumRecovery:
    def test_vacuum_page_images_replay(self, db_path):
        h = open_db(db_path)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        h.txns.commit(txn)

        txn = h.txns.begin()
        heap = h.heap("t")
        rids = [heap.insert(txn, [i, f"v{i}"]) for i in range(30)]
        h.cat.mark_dirty()
        h.cat.persist(txn)  # 让 root_page 也进目录
        h.txns.commit(txn)

        txn = h.txns.begin()
        for rid in rids[:8]:
            heap.update(txn, rid, [rid[1], "updated"])
        h.txns.commit(txn)

        txn = h.txns.begin()
        kept, dead, _ = heap.vacuum(txn)
        h.txns.commit(txn)
        assert (kept, dead) == (30, 8)

        # 数据页全部留在内存里没落盘，只保留 WAL —— 然后崩溃
        crash(h)

        h2 = open_db(db_path)
        txn = h2.txns.begin()
        seen = sorted(values[0] for _, values in h2.heap("t").scan(txn))
        assert seen == list(range(30))
        updated = [
            values[1] for _, values in h2.heap("t").scan(txn) if values[0] < 8
        ]
        assert updated == ["updated"] * 8
        h2.txns.commit(txn)
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()


class TestTruncateRecovery:
    def test_truncate_page_images_replay(self, db_path):
        h = open_db(db_path)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        heap = h.heap("t")
        for i in range(20):
            heap.insert(txn, [i, f"v{i}"])
        h.cat.persist(txn)
        h.txns.commit(txn)

        txn = h.txns.begin()
        removed = heap.truncate(txn)
        h.cat.persist(txn)
        h.txns.commit(txn)
        assert removed == 20

        crash(h)

        h2 = open_db(db_path)
        txn = h2.txns.begin()
        assert list(h2.heap("t").scan(txn)) == []
        h2.txns.commit(txn)
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()


class TestCommittedSurvives:
    def test_committed_rows_survive_crash(self, db_path):
        h = open_db(db_path)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        table = h.cat.get_table("t")
        heap = HeapFile(h.bp, table, h.txns)
        for i in range(20):
            heap.insert(txn, [i, f"row{i}"])
        h.cat.persist(txn)  # root_page 等元数据要一起落盘
        h.txns.commit(txn)

        crash(h)

        h2 = open_db(db_path)
        reader = h2.txns.begin()
        rows = list(HeapFile(h2.bp, h2.cat.get_table("t"), h2.txns).scan(reader))
        h2.txns.commit(reader)
        assert len(rows) == 20
        assert sorted(r[1][0] for r in rows) == list(range(20))
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()

    def test_uncommitted_rows_are_rolled_back(self, db_path):
        h = open_db(db_path)
        setup = h.txns.begin()
        h.cat.create_table(setup, "t", columns())
        h.cat.persist(setup)
        h.txns.commit(setup)

        txn = h.txns.begin()
        heap = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        heap.insert(txn, [1, "ghost"])
        heap.insert(txn, [2, "ghost2"])
        # 故意不提交

        crash(h)

        h2 = open_db(db_path)
        reader = h2.txns.begin()
        rows = list(HeapFile(h2.bp, h2.cat.get_table("t"), h2.txns).scan(reader))
        h2.txns.commit(reader)
        assert rows == []
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()

    def test_mixed_committed_and_lost(self, db_path):
        h = open_db(db_path)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        heap = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        heap.insert(txn, [1, "keep"])
        h.cat.persist(txn)
        h.txns.commit(txn)

        txn2 = h.txns.begin()
        heap2 = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        heap2.insert(txn2, [2, "lose"])

        crash(h)

        h2 = open_db(db_path)
        reader = h2.txns.begin()
        rows = list(HeapFile(h2.bp, h2.cat.get_table("t"), h2.txns).scan(reader))
        h2.txns.commit(reader)
        assert [r[1][0] for r in rows] == [1]
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()


class TestSavepointCrashRecovery:
    def test_savepoint_rollback_then_crash(self, db_path):
        path = db_path / "savepoint_crash"
        db = Database(path)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO t VALUES (2, 20)")
        db.execute_one("SAVEPOINT s")
        db.execute_one("INSERT INTO t VALUES (3, 30)")
        db.execute_one("ROLLBACK TO SAVEPOINT s")

        # 崩溃：只保证 WAL 落盘，数据页留在 Buffer Pool，不执行 commit。
        db.wal.flush()
        db.wal.close()
        db.disk.close()

        db2 = Database(path)
        result = db2.execute_one("SELECT id, v FROM t ORDER BY id")
        assert result.rows == [(1, 10)]
        db2.close()


class TestRecoveryReport:
    def test_report_counts_losers(self, db_path):
        h = open_db(db_path)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        h.cat.persist(txn)
        h.txns.commit(txn)

        loser = h.txns.begin()
        heap = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        heap.insert(loser, [9, "x"])
        crash(h)

        # 重新打开时手动跑恢复，检查报告
        dm = DiskManager(db_path / "data.db")
        wal = WAL(db_path / "wal.log")
        bp = BufferPoolManager(dm, wal, pool_size=8)
        txns = TransactionManager(dm, wal)
        report = RecoveryManager(dm, wal, bp, txns).recover()
        assert report.scanned > 0
        assert report.undo_applied > 0
        assert loser.xid in report.losers
        bp.close()
        wal.close()
        dm.close()

    def test_repeated_recovery_is_idempotent(self, db_path):
        h = open_db(db_path)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        heap = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        heap.insert(txn, [1, "a"])
        h.cat.persist(txn)
        h.txns.commit(txn)
        crash(h)

        h2 = open_db(db_path)
        rows_first = None
        reader = h2.txns.begin()
        rows_first = sorted(v[0] for _, v in HeapFile(h2.bp, h2.cat.get_table("t"), h2.txns).scan(reader))
        h2.txns.commit(reader)
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()

        # 再恢复一次，结果必须一致
        h3 = open_db(db_path)
        reader2 = h3.txns.begin()
        rows_second = sorted(v[0] for _, v in HeapFile(h3.bp, h3.cat.get_table("t"), h3.txns).scan(reader2))
        h3.txns.commit(reader2)
        assert rows_first == rows_second == [1]
        h3.bp.close()
        h3.wal.close()
        h3.dm.close()


class TestCheckpoint:
    def test_checkpoint_persists_pages(self, db_path):
        h = open_db(db_path)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        heap = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        heap.insert(txn, [42, "answer"])
        h.cat.persist(txn)
        h.txns.commit(txn)

        RecoveryManager(h.dm, h.wal, h.bp, h.txns).checkpoint()
        crash(h)

        h2 = open_db(db_path)
        report_reader = h2.txns.begin()
        rows = list(HeapFile(h2.bp, h2.cat.get_table("t"), h2.txns).scan(report_reader))
        h2.txns.commit(report_reader)
        assert [r[1][0] for r in rows] == [42]
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()

    def test_segmented_checkpoint_with_active_txn_then_crash(self, db_path):
        path = db_path / "active-checkpoint-crash"
        db = Database(path, wal_segment_bytes=64, checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT)")
        db.execute_one("BEGIN")
        for i in range(20):
            db.execute_one(f"INSERT INTO t VALUES ({i})")
        # checkpoint 会把未提交的脏页也刷下去；WAL 必须保留到活跃事务的
        # BEGIN，否则重启后无法 undo 这些行。
        db.checkpoint()
        db.wal.flush()
        db.wal.close()
        db.disk.close()

        db2 = Database(path)
        assert db2.execute_one("SELECT COUNT(*) FROM t").rows == [(0,)]
        db2.close()

    def test_checkpoint_then_crash_uses_post_checkpoint_wal(self, db_path):
        path = db_path / "checkpoint-then-crash"
        db = Database(path, wal_segment_bytes=64, checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT)")
        for i in range(20):
            db.execute_one(f"INSERT INTO t VALUES ({i})")
        db.checkpoint()
        for i in range(20, 25):
            db.execute_one(f"INSERT INTO t VALUES ({i})")
        db.wal.flush()
        db.wal.close()
        db.disk.close()

        db2 = Database(path)
        assert db2.execute_one("SELECT COUNT(*) FROM t").rows == [(25,)]
        db2.close()

    def test_crash_during_checkpoint_before_meta_persist(self, db_path, monkeypatch):
        h = open_db(db_path, segmented=True, segment_bytes=64)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        h.cat.persist(txn)
        heap = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        heap.insert(txn, [7, "seven"])
        h.cat.persist(txn)
        h.txns.commit(txn)

        def fail_meta():
            raise RuntimeError("crash during checkpoint")

        with pytest.raises(RuntimeError, match="crash during checkpoint"):
            monkeypatch.setattr(h.dm, "_persist_meta", fail_meta)
            RecoveryManager(h.dm, h.wal, h.bp, h.txns).checkpoint()
        monkeypatch.undo()

        # checkpoint 已经把脏页刷盘、也写了 checkpoint 日志，但元数据页
        # 还没更新；这正是“checkpoint 中崩溃”的窗口。
        crash(h)
        h2 = open_db(db_path, segmented=True, segment_bytes=64)
        reader = h2.txns.begin()
        rows = list(HeapFile(h2.bp, h2.cat.get_table("t"), h2.txns).scan(reader))
        h2.txns.commit(reader)
        assert [r[1][0] for r in rows] == [7]
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()

    def test_crash_after_segment_rotation(self, db_path):
        h = open_db(db_path, segmented=True, segment_bytes=64)
        txn = h.txns.begin()
        h.cat.create_table(txn, "t", columns())
        h.cat.persist(txn)
        heap = HeapFile(h.bp, h.cat.get_table("t"), h.txns)
        for i in range(20):
            heap.insert(txn, [i, f"v{i}"])
        h.cat.persist(txn)
        h.txns.commit(txn)
        assert len(h.wal.segments) > 1
        crash(h)

        h2 = open_db(db_path, segmented=True, segment_bytes=64)
        reader = h2.txns.begin()
        rows = list(HeapFile(h2.bp, h2.cat.get_table("t"), h2.txns).scan(reader))
        h2.txns.commit(reader)
        assert sorted(r[1][0] for r in rows) == list(range(20))
        h2.bp.close()
        h2.wal.close()
        h2.dm.close()
