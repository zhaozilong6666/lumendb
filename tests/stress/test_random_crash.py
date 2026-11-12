"""随机崩溃一致性测试。

固定用例只能覆盖"想得到"的崩溃点。这里用固定种子的随机操作序列，反复
执行"插入 / 更新 / 删除 → 提交或回滚 → 直接崩溃"，每次重开后把整张表
与测试侧维护的"已提交模型"逐行比对：

- 已提交事务所做的修改必须全部还在（持久性）；
- 未提交事务的修改必须全部消失（原子性）；
- 不能出现重复行或半行。

用固定种子是为了失败可复现：失败信息里会带上 seed，重跑同一个 seed 就能
得到同一条操作序列。
"""

import random

import pytest

from lumendb.catalog.catalog import Catalog
from lumendb.catalog.schema import Column
from lumendb.catalog.store import CatalogStore
from lumendb.datatypes import DataType, TypeSpec
from lumendb.storage.bufferpool import BufferPoolManager
from lumendb.storage.heap import HeapFile
from lumendb.storage.pager import DiskManager
from lumendb.storage.recovery import RecoveryManager
from lumendb.storage.txn import TransactionManager
from lumendb.storage.wal import WAL


class Handle:
    def __init__(self, path):
        self.dm = DiskManager(path / "data.db")
        self.wal = WAL(path / "wal.log")
        self.bp = BufferPoolManager(self.dm, self.wal, pool_size=8)
        self.txns = TransactionManager(self.dm, self.wal)
        RecoveryManager(self.dm, self.wal, self.bp, self.txns).recover()
        self.cat = Catalog(CatalogStore(self.bp))
        self.cat.load()

    def heap(self) -> HeapFile:
        return HeapFile(self.bp, self.cat.get_table("t"), self.txns)

    def crash(self):
        self.wal.flush()
        self.dm.close()


def columns():
    return [
        Column("id", TypeSpec(DataType.INT), nullable=False, primary_key=True),
        Column("name", TypeSpec(DataType.TEXT)),
    ]


def scan_rows(handle):
    txn = handle.txns.begin()
    rows = [(values[0], values[1], rid) for rid, values in handle.heap().scan(txn)]
    handle.txns.commit(txn)
    return rows


def assert_matches(handle, expected, seed, step):
    rows = scan_rows(handle)
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids)), f"seed={seed} step={step}: 出现重复行 {ids}"
    actual = {r[0]: r[1] for r in rows}
    assert actual == expected, f"seed={seed} step={step}: 实际={actual} 期望={expected}"


def run_seed(path, seed):
    path.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    handle = Handle(path)

    setup = handle.txns.begin()
    handle.cat.create_table(setup, "t", columns())
    heap = handle.heap()
    heap.insert(setup, [1, "v1"])
    handle.cat.persist(setup)
    handle.txns.commit(setup)

    expected = {1: "v1"}
    next_id = 2
    step = 0

    def rebuild_rids():
        return {rid_values[0]: rid_values[2] for rid_values in scan_rows(handle)}

    rids = rebuild_rids()

    for _ in range(14):
        step += 1
        txn = handle.txns.begin()
        heap = handle.heap()
        pending = []  # (action, id, value|None)
        pending_deleted: set[int] = set()
        alive = [row_id for row_id in expected if row_id not in pending_deleted]

        for _ in range(rng.randint(0, 3)):
            choices = ["insert", "insert", "update", "delete"]
            action = rng.choice(choices)
            if action == "insert":
                rid = heap.insert(txn, [next_id, f"v{next_id}"])
                pending.append(("insert", next_id, f"v{next_id}", rid))
                next_id += 1
            elif action == "update" and alive:
                row_id = rng.choice(alive)
                rid = rids[row_id]
                value = f"u{step}_{row_id}"
                heap.update(txn, rid, [row_id, value])
                pending.append(("update", row_id, value, rid))
            elif action == "delete" and len(alive) > 1:
                row_id = rng.choice(alive)
                rid = rids[row_id]
                heap.delete(txn, rid)
                pending.append(("delete", row_id, None, rid))
                pending_deleted.add(row_id)
                alive.remove(row_id)

        outcome = rng.choices(
            ["commit", "abort", "crash"], weights=[6, 2, 2], k=1
        )[0]
        if outcome == "commit":
            handle.txns.commit(txn)
            for action, row_id, value, rid in pending:
                if action == "insert":
                    expected[row_id] = value
                    rids[row_id] = rid
                elif action == "update":
                    expected[row_id] = value
                elif action == "delete":
                    expected.pop(row_id, None)
                    rids.pop(row_id, None)
        elif outcome == "abort":
            handle.txns.abort(txn)
        else:
            # 事务做到一半直接崩溃：恢复必须把它 undo 掉
            handle.crash()
            handle = Handle(path)
            assert_matches(handle, expected, seed, step)
            rids = rebuild_rids()

        if step % 5 == 0:
            handle.crash()
            handle = Handle(path)
            assert_matches(handle, expected, seed, step)
            rids = rebuild_rids()

    handle.crash()
    handle = Handle(path)
    assert_matches(handle, expected, seed, step)


@pytest.mark.parametrize("seed", range(20))
def test_random_crash_consistency(tmp_path, seed):
    run_seed(tmp_path / f"seed{seed}", seed)
