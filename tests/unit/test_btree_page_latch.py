"""B+ 树叶页级 latch：并发插入必须互斥，否则丢行、槽位乱序。

背景：B+ 树叶子插入是"读整页 → 改副本 → 原子换回"，单看一次换回是原子的
（GIL + 一次 memcpy），但"二分定位 → 读页 → 插入"整段不是原子。两个线程
都基于同一份旧页算出下标，后写者会把先写者的槽位顶乱（槽位目录不再保持键序）
或覆盖（丢行）。修复前用探针可稳定复现：``(1050) 之后出现 (1044)``。

本用例绕过 `Database._write_lock` 直接调 `BPlusTree.insert`，把并发叶插入的
不变量（键序、条目数）钉住。分裂/合并的多页原子性仍需 latch crabbing，见
docs/concurrency.md §第 4 步。
"""

from __future__ import annotations

import threading

from lumendb.datatypes import DataType, TypeSpec
from lumendb.engine.database import Database
from lumendb.storage.btree import BPlusTree, KeyCodec


def test_concurrent_leaf_inserts_keep_order_and_count(tmp_path) -> None:
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.INT)]), unique=False, name="idx")

    # 先单线程插一条建立根叶，避开"两个线程同时初始化根页"的竞争。
    t0 = db.txns.begin()
    tree.insert(t0, (-1,), (0, 0))
    db.txns.commit(t0)

    threads = 2
    per_thread = 100  # 共 201 条，全部落在同一片叶（INT 叶约容 330 条），不分裂
    errors: list[str] = []
    barrier = threading.Barrier(threads)

    def worker(wid: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for i in range(per_thread):
                key = wid * 1000 + i  # 两线程键不相交，但落在同一片叶
                tree.insert(txn, (key,), (1, key))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001 - 失败要能看见
            errors.append(f"worker{wid}: {type(exc).__name__}: {exc}")

    try:
        workers = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
        for th in workers:
            th.start()
        for th in workers:
            th.join(timeout=60)

        assert errors == [], f"并发插入出错: {errors[:2]}"
        stats = tree.check_invariants()  # 键序 / 叶子链 / 深度不一致会在这里抛错
        assert stats["entries"] == 1 + threads * per_thread
        assert stats["leaf_pages"] == 1, "201 条应落在同一片叶，未触发分裂"
    finally:
        db.close()
