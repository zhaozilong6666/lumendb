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

import random
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


def test_concurrent_inserts_from_empty_trigger_root_split(tmp_path) -> None:
    """从空树并发插相交键：必触发建根竞态 + 根分裂 + B-link 沿 next 路由。

    修复前有两个真实 bug：(1) 建根的双重检查里，后到的线程发现根已被并发建好
    时仍 `return`，静默丢弃自己的 key（最左叶最小键丢 3 条）；(2) 内部页/叶子
    没有 high key，分裂后查找/插入者按旧 keys 二分会把 key 路由到左半最右孩子，
    ``check_invariants`` 报「键不小于上界」。本用例钉住这两条回归。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.INT)]), unique=False, name="idx")

    threads = 4
    per_thread = 120  # 480 条，从空树出发必触发一次根分裂
    errors: list[str] = []
    barrier = threading.Barrier(threads)

    def worker(wid: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for i in range(per_thread):
                tree.insert(txn, (i,), (wid, i))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"worker{wid}: {type(exc).__name__}: {exc}")

    try:
        workers = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
        for th in workers:
            th.start()
        for th in workers:
            th.join(timeout=120)

        assert errors == [], f"并发建根/根分裂出错: {errors[:3]}"
        stats = tree.check_invariants()
        assert stats["entries"] == threads * per_thread, "并发插入不得丢行"
        # 显式钉住"最小键"：建根竞态曾静默丢 key=0（后到线程被 return 吞掉）
        txn = db.txns.begin()
        try:
            assert len(tree.lookup(txn, (0,))) == threads, "最小键 0 必须保留全部副本"
        finally:
            db.txns.abort(txn)
    finally:
        db.close()


def test_concurrent_inserts_trigger_internal_splits(tmp_path) -> None:
    """预置多层树后并发插相交键，触发内部页分裂（B-link 沿 next 路由）。"""
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    # 用长键值压小叶容量（一叶约 5 条），少量数据即可建立多层内部页
    tree = BPlusTree(
        db.bp, KeyCodec([TypeSpec(DataType.VARCHAR, 512)]), unique=False, name="idx"
    )

    def long_key(i: int) -> str:
        return "k" + "x" * 400 + f"{i:05d}"

    t0 = db.txns.begin()
    for i in range(2000):
        tree.insert(t0, (long_key(i),), (0, i))
    db.txns.commit(t0)
    base = tree.check_invariants()
    assert base["depth"] >= 2, f"预置应建立多层内部页，实际 depth={base['depth']}"

    threads = 4
    per_thread = 200  # 落在 values 最左范围（与预置相交、rid 不同），必触发内部页/根分裂
    errors: list[str] = []
    barrier = threading.Barrier(threads)

    def worker(wid: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for i in range(per_thread):
                tree.insert(txn, (long_key(i),), (wid + 1, i))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"worker{wid}: {type(exc).__name__}: {exc}")

    try:
        workers = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
        for th in workers:
            th.start()
        for th in workers:
            th.join(timeout=120)

        assert errors == [], f"并发内部页分裂出错: {errors[:3]}"
        stats = tree.check_invariants()  # 内部页 high key 路由错会在这里抛「键不小于上界」
        assert stats["entries"] == base["entries"] + threads * per_thread
    finally:
        db.close()


def test_sequential_delete_large_tree_no_orphan(tmp_path) -> None:
    """顺序删除整棵多层长键值树：每次 delete 都要返回 True，最终树空。

    回归：B-link 落地后 ``_detach_if_empty`` 会把「分隔键已删光、只剩 1 个孩子
    的内部页」当成空页直接摘除，它的唯一孩子随之成为孤儿。实测 2000 条长键值
    顺序删除时 ``delete`` 失败 700 次（失败键呈每 30 个里 5 个的规律），但
    ``check_invariants`` 的 entries 却是 0——因为丢掉的整片叶已经不在树里。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(
        db.bp, KeyCodec([TypeSpec(DataType.VARCHAR, 512)]), unique=False, name="idx"
    )

    def long_key(i: int) -> str:
        return "k" + "x" * 400 + f"{i:05d}"

    n = 2000
    t0 = db.txns.begin()
    for i in range(n):
        tree.insert(t0, (long_key(i),), (0, i))
    db.txns.commit(t0)
    base = tree.check_invariants()
    assert base["depth"] >= 2, f"长键值 2000 条应建立多层内部页，实际 depth={base['depth']}"

    t1 = db.txns.begin()
    failed: list[int] = []
    for i in range(n):
        if not tree.delete(t1, (long_key(i),), (0, i)):
            failed.append(i)
    db.txns.commit(t1)

    try:
        assert failed == [], f"顺序删除失败 {len(failed)} 次，前几个 {failed[:5]}"
        assert tree.check_invariants()["entries"] == 0, "删除后树应为空"
    finally:
        db.close()


def test_delete_half_keeps_leaf_depth_uniform(tmp_path) -> None:
    """删掉一半再插回一半：数据完整、叶子深度必须唯一。

    两个连环坑都在这里钉住：
    - 只摘内部页却不清理它的空子树：留下孤儿叶，最左叶 prev 指向已删页；
    - 把退化内部页的唯一孩子（叶子）提到内部页层：这条路径深度少一层，
      叶子深度变成 {3,4}。

    正解是自底向上递归塌陷：空叶子先摘并修链，只有「0 个孩子」的内部页
    才跟着塌，而「只剩 1 个孩子」的退化页保留，深度对所有路径一致。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(
        db.bp, KeyCodec([TypeSpec(DataType.VARCHAR, 512)]), unique=False, name="idx"
    )

    def long_key(i: int) -> str:
        return "k" + "x" * 400 + f"{i:05d}"

    t0 = db.txns.begin()
    for i in range(2000):
        tree.insert(t0, (long_key(i),), (0, i))
    db.txns.commit(t0)
    base = tree.check_invariants()
    assert base["depth"] >= 2

    t1 = db.txns.begin()
    for i in range(1000):
        assert tree.delete(t1, (long_key(i),), (0, i))
    db.txns.commit(t1)
    after_del = tree.check_invariants()  # 深度不一致会直接抛错
    assert after_del["entries"] == 1000

    t2 = db.txns.begin()
    for i in range(2000, 3000):
        tree.insert(t2, (long_key(i),), (0, i))
    db.txns.commit(t2)

    try:
        final = tree.check_invariants()
        assert final["entries"] == 2000
    finally:
        db.close()


def test_concurrent_reader_scans_during_insert_splits(tmp_path) -> None:
    """写线程插入触发叶/内部页分裂时，读线程的 scan 必须看到有序、完整的键。

    读者路径（`_descend` / `scan_all`）不拿页 latch，靠两点保证一致：
    - 分裂的"左页改半 + high key + next 指向右页"是**一次整页提交**，读者
      要么看到分裂前、要么看到分裂后，不会看到"左页还含右半但 next 已指向右页"；
    - 读者沿叶子链 next 走，天然跳过正在分裂的中间态（B-link 的语义）。
    这个用例把"写者分裂 + 两个读者并发全表扫"的组合钉住：结果无异常、键序单调。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.INT)]), unique=False, name="idx")

    t0 = db.txns.begin()
    for i in range(600):
        tree.insert(t0, (i,), (0, i))
    db.txns.commit(t0)

    stop = threading.Event()
    errors: list[str] = []
    bad_order = [0]

    def reader() -> None:
        try:
            while not stop.is_set():
                keys = [k for k, _ in tree.scan_all(None)]
                if any(keys[j] > keys[j + 1] for j in range(len(keys) - 1)):
                    bad_order[0] += 1
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"reader: {type(exc).__name__}: {exc}")

    def writer() -> None:
        try:
            txn = db.txns.begin()
            for i in range(600, 1400):
                tree.insert(txn, (i,), (1, i))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"writer: {type(exc).__name__}: {exc}")

    readers = [threading.Thread(target=reader) for _ in range(2)]
    for r in readers:
        r.start()
    w = threading.Thread(target=writer)
    w.start()
    w.join(timeout=120)
    stop.set()
    for r in readers:
        r.join(timeout=10)

    try:
        assert errors == [], f"读写并发出错: {errors[:3]}"
        assert bad_order[0] == 0, f"scan 出现 {bad_order[0]} 次键序错乱"
        assert tree.check_invariants()["entries"] == 1400
    finally:
        db.close()


def test_random_order_delete_large_tree(tmp_path) -> None:
    """随机顺序删完整棵多层树：每次 delete 都 True、最终树空。

    顺序删除只踩到"最左叶一片片摘掉"这一条路径；随机删除会同时踩中间叶摘除、
    右边界叶摘除、以及"先摘中间再摘两端"的组合，覆盖更碎的塌陷与叶子链修复。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(
        db.bp, KeyCodec([TypeSpec(DataType.VARCHAR, 512)]), unique=False, name="idx"
    )

    def long_key(i: int) -> str:
        return "k" + "x" * 400 + f"{i:05d}"

    n = 2000
    t0 = db.txns.begin()
    for i in range(n):
        tree.insert(t0, (long_key(i),), (0, i))
    db.txns.commit(t0)
    assert tree.check_invariants()["depth"] >= 2

    order = list(range(n))
    random.Random(42).shuffle(order)
    t1 = db.txns.begin()
    failed: list[int] = []
    for i in order:
        if not tree.delete(t1, (long_key(i),), (0, i)):
            failed.append(i)
    db.txns.commit(t1)

    try:
        assert failed == [], f"随机删除失败 {len(failed)} 次，前几个 {failed[:5]}"
        assert tree.check_invariants()["entries"] == 0
    finally:
        db.close()


def test_concurrent_deletes_same_leaf_no_lost_delete(tmp_path) -> None:
    """两个线程并发删同一片叶的不同键：一条删除都不许丢。

    回归：`_delete_recursive` 叶子分支的"bisect 定位 → 读键"若在 latch 外，
    两个线程会基于同一份旧页各算 idx，后写者把先写者的删除结果覆盖回去。
    实测 300 条单叶里双线程删 200 条，丢 27 条（delete 返回 False），
    叶里反而剩下 127 条。修法是把定位/读/删整段放进同一个页 latch。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.INT)]), unique=False, name="idx")

    t0 = db.txns.begin()
    for i in range(300):
        tree.insert(t0, (i,), (0, i))
    db.txns.commit(t0)
    assert tree.check_invariants()["leaf_pages"] == 1, "300 条 INT 应落在同一片叶"

    errors: list[str] = []
    fails = [0]
    fails_lock = threading.Lock()
    barrier = threading.Barrier(2)

    def worker(lo: int, hi: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            local = 0
            for i in range(lo, hi):
                if not tree.delete(txn, (i,), (0, i)):
                    local += 1
            with fails_lock:
                fails[0] += local
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"{lo}-{hi}: {type(exc).__name__}: {exc}")

    workers = [
        threading.Thread(target=worker, args=(0, 100)),
        threading.Thread(target=worker, args=(100, 200)),
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=60)

    try:
        assert errors == [], f"并发删除出错: {errors[:2]}"
        assert fails[0] == 0, f"并发删除丢了 {fails[0]} 条（delete 返回 False）"
        assert tree.check_invariants()["entries"] == 100
    finally:
        db.close()


def test_concurrent_delete_across_leaves_no_lost_delete(tmp_path) -> None:
    """两个线程并发跨多片叶删完整棵树（走内部页摘孩子 + 叶子链修复路径）。

    与"同叶"用例互补：这里两个线程各自把一半键删空，会多次触发内部页摘孩子、
    叶子链前后邻居互接、以及根塌陷。断言一条删除都不丢、树最终为空、不变量成立。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.INT)]), unique=False, name="idx")

    t0 = db.txns.begin()
    for i in range(3000):
        tree.insert(t0, (i,), (0, i))
    db.txns.commit(t0)
    assert tree.check_invariants()["leaf_pages"] > 5, "3000 条 INT 应有多片叶"

    errors: list[str] = []
    fails = [0]
    fails_lock = threading.Lock()
    barrier = threading.Barrier(2)

    def worker(lo: int, hi: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            local = 0
            for i in range(lo, hi):
                if not tree.delete(txn, (i,), (0, i)):
                    local += 1
            with fails_lock:
                fails[0] += local
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"{lo}-{hi}: {type(exc).__name__}: {exc}")

    workers = [
        threading.Thread(target=worker, args=(0, 1500)),
        threading.Thread(target=worker, args=(1500, 3000)),
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=180)

    try:
        assert errors == [], f"跨叶并发删除出错: {errors[:2]}"
        assert fails[0] == 0, f"跨叶并发删除丢了 {fails[0]} 条"
        assert tree.check_invariants()["entries"] == 0
    finally:
        db.close()


def test_concurrent_reader_scan_during_delete_never_truncates(tmp_path) -> None:
    """删除并发进行时，读者全表扫不许看到"比最终还少"的行数。

    回归（2026-11-19 实测）：读者在空叶被摘 / 根塌陷窗口会撞到两种坏结果——
    ①读到旧 nkeys + 新槽位表，抛 `槽位偏移越界：0`；
    ②下探落进正在被 purge 的子树，拿到一片已摘除的空叶（prev 也是 INVALID，
    看起来像链首），沿链 next 已清零 → 扫到 0 条。

    修法三件：整页快照在页 latch 内一次读完（`_leaf_page_view`）；链首必须
    「prev==INVALID 且有键」（`_scan_head`，空树除外）；跳页用期望前驱校验，
    陈旧 next 就回前驱找回真正后继（`_advance_leaf`）。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.INT)]), unique=False, name="idx")

    total, keep = 3000, 1500
    t0 = db.txns.begin()
    for i in range(total):
        tree.insert(t0, (i,), (0, i))
    db.txns.commit(t0)

    counts: list[int] = []
    errors: list[str] = []
    stop = threading.Event()

    def reader() -> None:
        try:
            while not stop.is_set():
                ks = [k for k, _ in tree.scan_all(None)]
                if any(ks[j] > ks[j + 1] for j in range(len(ks) - 1)):
                    errors.append("键序错乱")
                    return
                counts.append(len(ks))
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}")

    def writer() -> None:
        try:
            txn = db.txns.begin()
            for i in range(total - keep):
                tree.delete(txn, (i,), (0, i))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"writer: {type(exc).__name__}: {exc}")

    readers = [threading.Thread(target=reader) for _ in range(2)]
    for r in readers:
        r.start()
    w = threading.Thread(target=writer)
    w.start()
    w.join(timeout=180)
    stop.set()
    for r in readers:
        r.join(timeout=20)

    try:
        assert errors == [], f"读写并发出错: {errors[:3]}"
        assert counts, "读者一次都没扫完"
        # 物理删除只会让可见行数单调下降，任何一次扫描都不该少于最终行数。
        assert min(counts) >= keep, f"扫到 {min(counts)} 条，少于最终 {keep} 条"
        assert tree.check_invariants()["entries"] == keep
    finally:
        db.close()


def test_concurrent_adjacent_deletes_keep_leaf_chain(tmp_path) -> None:
    """四线程乱序删**相邻叶**时，叶子链必须始终首尾相接。

    回归（2026-11-20 实测，crabbing 删除落地后暴露）：4 线程各删四分之一、
    顺序随机，删完 12000 条后 ``check_invariants`` 报
    「叶子链断裂：页 197 的 next 是 224，但中序下一个叶是 203」。根因是
    「摘 A」与「摘 A 的相邻叶 B」交错：两边各自基于旧的 prev/next 做指针
    手术，A 把 B 重新写回链上、或让仍在树上的叶被跳过。修法是链锁
    ``_link_lock`` 把摘链的三次指针写做成原子（分裂改写邻居 prev 同样持锁）。

    用 120 字节 TEXT 键压低扇出，4000 条即可造出 depth>=2 的多层树，
    让"祖父-父-子"的摘除与塌陷都参与并发。
    """
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.TEXT)]), unique=False, name="idx")
    total, pad = 4000, 120

    def key_of(i: int) -> tuple:
        return (f"{i:08d}".ljust(pad, "x"),)

    t0 = db.txns.begin()
    for i in range(total):
        tree.insert(t0, key_of(i), (0, i))
    db.txns.commit(t0)
    assert tree.check_invariants()["depth"] >= 2, "键长不够，没造出多层树"

    errors: list[str] = []
    barrier = threading.Barrier(4)

    def worker(wid: int) -> None:
        try:
            rnd = random.Random(wid + 7)
            keys = list(range(wid, total, 4))
            rnd.shuffle(keys)
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for k in keys:
                tree.delete(txn, key_of(k), (0, k))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"worker{wid}: {type(exc).__name__}: {exc}")

    try:
        workers = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
        for th in workers:
            th.start()
        for th in workers:
            th.join(timeout=180)

        assert errors == [], f"并发删除出错: {errors[:3]}"
        stats = tree.check_invariants()  # 叶子链 / 键序 / 深度不一致会在这里抛错
        assert stats["entries"] == 0, f"还有 {stats['entries']} 条没删掉"
        assert list(tree.scan_all(None)) == []
    finally:
        db.close()



