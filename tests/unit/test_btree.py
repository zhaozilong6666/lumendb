"""B+ 树的测试。

最有价值的一个用例是 ``TestRandomizedAgainstModel``：随机插入/删除 800 次，
与 Python ``dict`` 逐键比对。只要分裂、路由或叶子链有一处算错，这个用例
就会在某个随机种子下暴露出来。
"""

import random

import pytest

from lumendb.datatypes import DataType, TypeSpec
from lumendb.errors import BTreeInvariantError, DuplicateKeyError
from lumendb.storage.bufferpool import BufferPoolManager
from lumendb.storage.btree import BPlusTree, KeyCodec
from lumendb.storage.pager import DiskManager
from lumendb.storage.txn import TransactionManager
from lumendb.storage.wal import WAL


@pytest.fixture
def env(tmp_path):
    dm = DiskManager(tmp_path / "data.db")
    wal = WAL(tmp_path / "wal.log")
    bp = BufferPoolManager(dm, wal, pool_size=32)
    txns = TransactionManager(dm, wal)
    yield bp, txns
    bp.close()
    wal.close()
    dm.close()


def make_tree(bp, spec=None, unique=False):
    codec = KeyCodec([spec or TypeSpec(DataType.VARCHAR, 24)])
    return BPlusTree(bp, codec, unique=unique, name="test_idx")


class TestEmptyTree:
    def test_lookup_on_empty(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        assert tree.lookup(txn, ("a",)) == []
        assert list(tree.scan_all(txn)) == []
        assert tree.check_invariants()["entries"] == 0
        txns.commit(txn)

    def test_delete_on_empty_returns_false(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        assert tree.delete(txn, ("a",), (1, 0)) is False
        txns.commit(txn)


class TestSingleLevel:
    def test_insert_and_lookup(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        tree.insert(txn, ("alpha",), (1, 3))
        assert tree.lookup(txn, ("alpha",)) == [(1, 3)]
        assert tree.lookup(txn, ("beta",)) == []
        txns.commit(txn)

    def test_multiple_entries_sorted_scan(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        keys = ["m", "a", "z", "c", "b"]
        for i, k in enumerate(keys):
            tree.insert(txn, (k,), (i, 0))
        got = [k[0] for k, _ in tree.scan_all(txn)]
        assert got == sorted(keys)
        txns.commit(txn)

    def test_non_unique_index_keeps_all_rids(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        rid_a, rid_b = (1, 0), (2, 5)
        tree.insert(txn, ("same",), rid_a)
        tree.insert(txn, ("same",), rid_b)
        hits = tree.lookup(txn, ("same",))
        assert sorted(hits) == sorted([rid_a, rid_b])
        txns.commit(txn)

    def test_unique_index_rejects_duplicate(self, env):
        bp, txns = env
        tree = make_tree(bp, unique=True)
        txn = txns.begin()
        tree.insert(txn, ("k1",), (1, 0))
        with pytest.raises(DuplicateKeyError):
            tree.insert(txn, ("k1",), (2, 0))
        txns.commit(txn)

    def test_same_key_same_rid_is_idempotent(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        tree.insert(txn, ("dup",), (1, 0))
        tree.insert(txn, ("dup",), (1, 0))
        assert tree.lookup(txn, ("dup",)) == [(1, 0)]
        txns.commit(txn)


class TestMultiLevel:
    def test_many_inserts_build_deeper_tree(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        n = 3000
        for i in range(n):
            tree.insert(txn, (f"key-{i:06d}",), (i // 100 + 1, i % 100))
        stats = tree.check_invariants()
        assert stats["entries"] == n
        assert stats["depth"] >= 1
        assert stats["leaf_pages"] >= 2
        # 全部键都找得到
        for i in range(0, n, 137):
            assert len(tree.lookup(txn, (f"key-{i:06d}",))) == 1
        txns.commit(txn)

    def test_all_entries_via_scan(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        n = 1200
        for i in range(n):
            tree.insert(txn, (f"v{i:04d}",), (1, i))
        got = [k[0] for k, _ in tree.scan_all(txn)]
        assert got == [f"v{i:04d}" for i in range(n)]
        txns.commit(txn)


class TestRangeScan:
    def test_inclusive_bounds(self, env):
        bp, txns = env
        codec = KeyCodec([TypeSpec(DataType.INT)])
        tree = BPlusTree(bp, codec, name="int_idx")
        txn = txns.begin()
        for i in range(100):
            tree.insert(txn, (i,), (1, i))

        lo = (10, 0, 0)
        hi = (20, 2**31 - 1, 2**31 - 1)
        got = [k[0] for k, _ in tree._scan_range(lo, hi, True, True)]
        assert got == list(range(10, 21))
        txns.commit(txn)

    def test_half_open_bounds(self, env):
        bp, txns = env
        codec = KeyCodec([TypeSpec(DataType.INT)])
        tree = BPlusTree(bp, codec, name="int_idx")
        txn = txns.begin()
        for i in range(50):
            tree.insert(txn, (i,), (1, i))
        lo = (5, 0, 0)
        hi = (15, 0, 0)
        got = [k[0] for k, _ in tree._scan_range(lo, hi, True, False)]
        assert got == list(range(5, 15))
        txns.commit(txn)


class TestDelete:
    def test_delete_existing_and_missing(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        tree.insert(txn, ("x",), (1, 0))
        assert tree.delete(txn, ("x",), (1, 0)) is True
        assert tree.delete(txn, ("x",), (1, 0)) is False
        assert tree.lookup(txn, ("x",)) == []
        txns.commit(txn)

    def test_delete_keeps_others(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        for i in range(20):
            tree.insert(txn, (f"k{i:02d}",), (1, i))
        for i in range(0, 20, 2):
            tree.delete(txn, (f"k{i:02d}",), (1, i))
        got = [k[0] for k, _ in tree.scan_all(txn)]
        assert got == [f"k{i:02d}" for i in range(1, 20, 2)]
        txns.commit(txn)

    def test_delete_all_collapses_tree(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        for i in range(500):
            tree.insert(txn, (f"k{i:04d}",), (1, i))
        for i in range(500):
            tree.delete(txn, (f"k{i:04d}",), (1, i))
        assert list(tree.scan_all(txn)) == []
        assert tree.check_invariants()["entries"] == 0
        txns.commit(txn)

    def test_delete_key_removes_all(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        for rid in [(1, 0), (2, 0), (3, 0)]:
            tree.insert(txn, ("multi",), rid)
        assert tree.delete_key(txn, ("multi",)) == 3
        assert tree.lookup(txn, ("multi",)) == []
        txns.commit(txn)


class TestRandomizedAgainstModel:
    def test_random_insert_delete_matches_dict(self, env):
        bp, txns = env
        tree = make_tree(bp, TypeSpec(DataType.VARCHAR, 16))
        rng = random.Random(20261112)
        model: dict[str, set] = {}
        txn = txns.begin()

        for step in range(800):
            key = f"k{rng.randint(0, 120):03d}"
            rid = (rng.randint(1, 30), rng.randint(0, 40))
            if rng.random() < 0.72:
                tree.insert(txn, (key,), rid)
                model.setdefault(key, set()).add(rid)
            else:
                existed = tree.delete(txn, (key,), rid)
                if existed:
                    if key in model:
                        model[key].discard(rid)
                        if not model[key]:
                            del model[key]
                    else:  # pragma: no cover - 说明实现与模型不一致
                        raise AssertionError(f"删除成功但模型里没有 {(key, rid)}")

            if step % 40 == 0:
                tree.check_invariants()
                for k in list(model)[:6]:
                    assert set(tree.lookup(txn, (k,))) == model[k], (k, step)

        # 全量比对
        seen = {(k[0], (k[1], k[2])) for k, _ in tree.scan_all(txn)}
        expected = {(k, rid) for k, rids in model.items() for rid in rids}
        assert seen == expected

        stats = tree.check_invariants()
        assert stats["entries"] == len(expected)
        txns.commit(txn)


class TestInvariants:
    def test_invariant_check_detects_broken_page(self, env):
        bp, txns = env
        tree = make_tree(bp)
        txn = txns.begin()
        for i in range(10):
            tree.insert(txn, (f"k{i}",), (1, i))

        # 手动篡改：把根页的 nkeys 改大，校验应该抓出来
        page = bp.fetch_page(tree.root_page)
        try:
            page.set_u16(2, 999)
            bp.mark_dirty(page)
        finally:
            bp.unpin(tree.root_page, dirty=True)
        with pytest.raises(BTreeInvariantError):
            tree.check_invariants()
        txns.abort(txn)
