"""堆表与 MVCC 的集成测试。

这些用例断言的不是"能插能查"，而是几条对用户可见的语义承诺：

* 快照隔离：事务开始时已存在的行，在整个事务期间看到的都是同一个版本；
* 更新不等于原地改：旧版本必须还能被更早的快照读到；
* 回滚之后，别人看不到你的修改，你自己也看不到；
* 跨页之后，扫描仍然连续且不重复。
"""

import pytest

from lumendb.catalog.schema import Column, TableDef
from lumendb.datatypes import DataType, TypeSpec
from lumendb.errors import RowTooLargeError, TransactionAbortedError
from lumendb.storage.bufferpool import BufferPoolManager
from lumendb.storage.heap import HeapFile
from lumendb.storage.pager import DiskManager
from lumendb.storage.txn import TransactionManager
from lumendb.storage.wal import WAL


def make_table(name="users"):
    return TableDef(
        name=name,
        columns=[
            Column("id", TypeSpec(DataType.INT), nullable=False, primary_key=True),
            Column("name", TypeSpec(DataType.TEXT)),
            Column("age", TypeSpec(DataType.INT)),
        ],
    )


@pytest.fixture
def env(tmp_path):
    dm = DiskManager(tmp_path / "data.db")
    wal = WAL(tmp_path / "wal.log")
    bp = BufferPoolManager(dm, wal, pool_size=16)
    txns = TransactionManager(dm, wal)
    table = make_table()
    heap = HeapFile(bp, table, txns)
    yield heap, txns, bp, table
    bp.close()
    wal.close()
    dm.close()


class TestInsertAndScan:
    def test_single_insert(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        rid = heap.insert(txn, [1, "Ada", 36])
        txns.commit(txn)

        reader = txns.begin()
        rows = list(heap.scan(reader))
        txns.commit(reader)
        assert len(rows) == 1
        assert rows[0][0] == rid
        assert rows[0][1] == [1, "Ada", 36]

    def test_multiple_rows_preserve_order(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        for i in range(50):
            heap.insert(txn, [i, f"user{i}", 20 + i % 30])
        txns.commit(txn)

        reader = txns.begin()
        rows = list(heap.scan(reader))
        txns.commit(reader)
        assert len(rows) == 50
        assert [r[1][0] for r in rows] == list(range(50))

    def test_null_values_roundtrip(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        heap.insert(txn, [1, None, None])
        txns.commit(txn)

        reader = txns.begin()
        rows = list(heap.scan(reader))
        txns.commit(reader)
        assert rows[0][1] == [1, None, None]

    def test_oversized_row_rejected(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        with pytest.raises(RowTooLargeError):
            heap.insert(txn, [1, "x" * 5000, 30])
        txns.abort(txn)

    def test_insert_after_abort_is_invisible(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        heap.insert(txn, [1, "ghost", 1])
        txns.abort(txn)

        reader = txns.begin()
        assert list(heap.scan(reader)) == []
        txns.commit(reader)

    def test_get_by_root_rid(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        rid = heap.insert(txn, [7, "Grace", 45])
        txns.commit(txn)

        reader = txns.begin()
        assert heap.get(reader, rid) == [7, "Grace", 45]
        txns.commit(reader)


class TestCrossPage:
    def test_rows_span_multiple_pages(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        # 每行约 100 字节，页容量约 4000，因此必然跨页
        for i in range(300):
            heap.insert(txn, [i, "n" * 60, i])
        txns.commit(txn)

        # 断言真的跨了页：链尾不同于链头，且缓冲池里有多页
        assert table.last_page != table.root_page
        assert bp.size >= 3
        reader = txns.begin()
        rows = list(heap.scan(reader))
        txns.commit(reader)
        assert len(rows) == 300
        ids = [r[1][0] for r in rows]
        assert ids == list(range(300))
        assert len(set(ids)) == 300

    def test_page_chain_is_consistent(self, env):
        heap, txns, bp, table = env
        txn = txns.begin()
        for i in range(200):
            heap.insert(txn, [i, "x" * 80, i])
        txns.commit(txn)

        # 正向走一遍链，再反向走一遍，页数应一致
        page_id = table.root_page
        forward = []
        while page_id != 0xFFFFFFFF:
            page = bp.fetch_page(page_id)
            forward.append(page_id)
            nxt = page.next_page
            bp.unpin(page_id)
            page_id = nxt

        page_id = table.last_page
        backward = []
        while page_id != 0xFFFFFFFF:
            page = bp.fetch_page(page_id)
            backward.append(page_id)
            prv = page.prev_page
            bp.unpin(page_id)
            page_id = prv

        assert forward == list(reversed(backward))


class TestUpdateAndSnapshot:
    def test_update_is_visible_to_new_snapshot(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "Ada", 36])
        txns.commit(t)

        t2 = txns.begin()
        heap.update(t2, rid, [1, "Ada", 37])
        txns.commit(t2)

        reader = txns.begin()
        assert heap.get(reader, rid) == [1, "Ada", 37]
        txns.commit(reader)

    def test_old_snapshot_still_sees_old_version(self, env):
        """快照隔离的核心承诺：别人改了，我的事务里还是旧值。"""
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "Ada", 36])
        txns.commit(t)

        reader = txns.begin()  # 先开快照
        writer = txns.begin()
        heap.update(writer, rid, [1, "Ada", 37])
        txns.commit(writer)

        assert heap.get(reader, rid) == [1, "Ada", 36]
        # 同一快照下多次读，结果必须稳定
        assert heap.get(reader, rid) == [1, "Ada", 36]
        txns.commit(reader)

    def test_uncommitted_update_invisible_to_others(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "Ada", 36])
        txns.commit(t)

        writer = txns.begin()
        heap.update(writer, rid, [1, "Ada", 99])

        reader = txns.begin()
        assert heap.get(reader, rid) == [1, "Ada", 36]
        rows = list(heap.scan(reader))
        assert len(rows) == 1  # 不能因为两个版本就数出两行
        txns.commit(reader)

        txns.abort(writer)
        reader2 = txns.begin()
        assert heap.get(reader2, rid) == [1, "Ada", 36]
        txns.commit(reader2)

    def test_multiple_updates_chain(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "v0", 0])
        txns.commit(t)

        for i in range(1, 6):
            w = txns.begin()
            heap.update(w, rid, [1, f"v{i}", i])
            txns.commit(w)

        reader = txns.begin()
        assert heap.get(reader, rid) == [1, "v5", 5]
        rows = list(heap.scan(reader))
        assert len(rows) == 1
        txns.commit(reader)

    def test_update_then_delete(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "Ada", 36])
        txns.commit(t)

        w = txns.begin()
        heap.update(w, rid, [1, "Ada", 37])
        txns.commit(w)

        d = txns.begin()
        assert heap.delete(d, rid) == 1
        txns.commit(d)

        reader = txns.begin()
        assert heap.get(reader, rid) is None
        assert list(heap.scan(reader)) == []
        txns.commit(reader)


class TestDelete:
    def test_delete_hides_row(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "Ada", 36])
        txns.commit(t)

        d = txns.begin()
        heap.delete(d, rid)
        txns.commit(d)

        reader = txns.begin()
        assert list(heap.scan(reader)) == []
        txns.commit(reader)

    def test_delete_invisible_to_earlier_snapshot(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "Ada", 36])
        txns.commit(t)

        reader = txns.begin()  # 快照早于删除
        d = txns.begin()
        heap.delete(d, rid)
        txns.commit(d)

        assert heap.get(reader, rid) == [1, "Ada", 36]
        rows = list(heap.scan(reader))
        assert len(rows) == 1
        txns.commit(reader)

    def test_double_delete_returns_zero(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        rid = heap.insert(t, [1, "Ada", 36])
        txns.commit(t)

        d1 = txns.begin()
        assert heap.delete(d1, rid) == 1
        txns.commit(d1)

        d2 = txns.begin()
        assert heap.delete(d2, rid) == 0
        txns.commit(d2)

    def test_delete_unknown_rid(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        assert heap.delete(t, (999, 999)) == 0
        txns.abort(t)


class TestTxnGuard:
    def test_operations_require_active_txn(self, env):
        heap, txns, bp, table = env
        t = txns.begin()
        txns.abort(t)
        with pytest.raises(TransactionAbortedError):
            heap.insert(t, [1, "x", 1])
