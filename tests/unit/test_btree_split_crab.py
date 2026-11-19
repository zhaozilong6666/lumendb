"""B+ 树叶分裂的 crabbing：并发插入触发同一片叶分裂时不许丢行、断链。

叶插入的单页 latch（test_btree_page_latch）只保证"同一片叶的槽位目录不被打乱"，
但分裂是**多页**操作：本叶 + 新右叶 + 原后继叶（改 prev 回指）+ 父页（插分隔键）。
修复前，`_leaf_insert_at` 在返回"需要分裂"时就释放了页 latch，两个线程会各自基于
同一份旧页执行分裂，各建一个右页并重写本叶 → 丢行 / 叶子链断裂。

修复后，"发现满 → 读页 → 分裂"整段在同一叶 latch 内完成；第二个线程进入时叶已
分裂成前半（不再满），会走普通插入而不是重复分裂。

本用例先单线程预置一棵多层叶树，再让多个线程并发向**同一片叶的键范围**插入，
触发该叶分裂（父页有空间、不触发内部页/根分裂——那是 B-link 的下一个课题），
最后用 `check_invariants` 钉住键序、叶子链、条目数。
"""

from __future__ import annotations

import threading

from lumendb.datatypes import DataType, TypeSpec
from lumendb.engine.database import Database
from lumendb.storage.btree import BPlusTree, KeyCodec


def test_concurrent_leaf_splits_keep_invariants(tmp_path) -> None:
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.INT)]), unique=False, name="idx")

    # 单线程预置 600 条，建立多层叶结构（INT 叶约容 330 条，600 条会分裂出多片叶）
    t0 = db.txns.begin()
    for i in range(600):
        tree.insert(t0, (i,), (0, i))
    db.txns.commit(t0)
    base = tree.check_invariants()

    threads = 2
    per_thread = 150  # 共 300 条，全部落在 values [0,150) 的最左叶范围，必触发分裂
    errors: list[str] = []
    barrier = threading.Barrier(threads)

    def worker(wid: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for i in range(per_thread):
                # 同一 values 范围、不同 rid → 键相交、落同一片叶，反复触发叶分裂
                tree.insert(txn, (i,), (wid + 1, i))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001 - 失败要能看见
            errors.append(f"worker{wid}: {type(exc).__name__}: {exc}")

    try:
        workers = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
        for th in workers:
            th.start()
        for th in workers:
            th.join(timeout=60)

        assert errors == [], f"并发叶分裂出错: {errors[:3]}"
        stats = tree.check_invariants()  # 键序 / 叶子链 / 深度不一致会在这里抛错
        assert stats["entries"] == base["entries"] + threads * per_thread
        assert stats["leaf_pages"] >= base["leaf_pages"]
    finally:
        db.close()
