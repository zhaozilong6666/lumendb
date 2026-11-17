"""页级 latch：同一页的结构修改必须互斥。

背景：`BufferPoolManager` 只有一把全局锁，页内容的读-改-日志-写原本靠
`Database._write_lock` 把所有写语句串行兜着。放开写并发之前，页内必须先
自己上锁——否则两个写者会同时拿到同一个 slot（实测 2×300 行出现 87 个重复
RID、丢掉 87 行、页链断开，见 docs/concurrency.md 的"第 2 步的前置"）。
"""

from __future__ import annotations

import threading

from lumendb.engine.database import Database
from lumendb.storage.heap import HeapFile
from lumendb.storage.page import HeapPage, Page


def test_page_latch_is_shared_by_views() -> None:
    """`view()` 共享同一块字节，也必须共享同一把 latch。"""
    page = Page(7)
    view = page.view(HeapPage)
    assert view.latch is page.latch
    assert view.buf is page.buf


def test_concurrent_inserts_never_reuse_a_slot(tmp_path) -> None:
    """绕过 `Database._write_lock` 并发插同一张表：RID 唯一、行数不丢。

    这里刻意调 `HeapFile.insert`（而不是 `Database.execute`），因为后者会拿
    写锁把两个线程串起来。修复前同一场景 2×300 行会拿到 87 个重复 RID、
    表里只扫到 513 行。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    table = db.catalog.get_table("t")
    heap = HeapFile(db.bp, table, db.txns)

    threads = 2
    per_thread = 150
    rids: list[tuple[int, int]] = []
    errors: list[str] = []
    barrier = threading.Barrier(threads)

    def worker(worker_id: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            got = [
                heap.insert(txn, (worker_id * 1000 + i, i))
                for i in range(per_thread)
            ]
            db.txns.commit(txn)
            rids.extend(got)
        except BaseException as exc:  # noqa: BLE001 - 失败要能看见
            errors.append(f"worker{worker_id}: {type(exc).__name__}: {exc}")

    try:
        workers = [
            threading.Thread(target=worker, args=(w,)) for w in range(threads)
        ]
        for th in workers:
            th.start()
        for th in workers:
            th.join(timeout=60)

        assert errors == [], f"并发插入出错: {errors[:2]}"
        expected = threads * per_thread
        assert len(rids) == expected
        assert len(set(rids)) == expected, "同一个 slot 被分配给了两条记录"
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(expected,)]
    finally:
        db.close()
