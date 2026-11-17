"""``BPlusTree.lookup`` 的前缀二分快路径：与通用范围扫描逐键对照。

快路径最容易错的两处：
* 跨叶重复键（同一前缀的键分散在多个叶子，靠 next_page 串起来）；
* 多列索引只比较索引列前缀、忽略自动追加的 RID。

所以这里不只测"能查到"，而是把 ``lookup`` 的结果与旧的参考实现
``_scan_range(values+MIN_RID, values+MAX_RID)`` 逐键比对。
"""

from __future__ import annotations

import random

import pytest

from lumendb.engine.database import Database
from lumendb.errors import BTreeInvariantError
from lumendb.storage.btree import BPlusTree, MAX_RID, MIN_RID


def _reference_lookup(tree: BPlusTree, values) -> list:
    lo = tuple(values) + MIN_RID
    hi = tuple(values) + MAX_RID
    return sorted(rid for _, rid in tree._scan_range(lo, hi, True, True))


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("lookup")
    db = Database(tmp / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT, bucket INT, code INT, pad VARCHAR(16))")
    for i in range(2000):
        db.execute_one(f"INSERT INTO t VALUES ({i}, {i % 10}, {i % 7}, 'u{i}')")
    db.execute_one("CREATE INDEX idx_id ON t (id)")
    db.execute_one("CREATE INDEX idx_bucket ON t (bucket)")
    db.execute_one("CREATE UNIQUE INDEX idx_code_id ON t (code, id)")
    txn = db.txns.begin(read_only=True)
    table = db.catalog.get_table("t")
    trees = {
        "id": db.executor._tree_for(table, table.indexes["idx_id"]),
        "bucket": db.executor._tree_for(table, table.indexes["idx_bucket"]),
        "code_id": db.executor._tree_for(table, table.indexes["idx_code_id"]),
    }
    yield txn, trees
    db.close()


def test_lookup_matches_reference_for_unique_and_duplicate_keys(env):
    txn, trees = env
    # 唯一键：首尾与随机抽样
    for value in (0, 1, 1999, 777):
        got = trees["id"].lookup(txn, [value])
        assert got == _reference_lookup(trees["id"], [value])
        assert len(got) == 1

    # 重复键：每个 bucket 200 条，跨多个叶子
    for bucket in range(10):
        got = sorted(trees["bucket"].lookup(txn, [bucket]))
        assert got == _reference_lookup(trees["bucket"], [bucket]), (
            f"bucket={bucket} 与参考实现不一致"
        )
        assert len(got) == 200

    # 不存在的键
    for missing in (-1, 2000, 5000):
        assert trees["id"].lookup(txn, [missing]) == []
        assert trees["bucket"].lookup(txn, [missing]) == []


def test_lookup_handles_multi_column_prefixes(env):
    txn, trees = env
    # (code, id) 唯一索引：完整键
    for code in range(7):
        for row in (0, 1999):
            values = [code, row]
            got = trees["code_id"].lookup(txn, values)
            assert got == _reference_lookup(trees["code_id"], values)

    # 只给第一列时，旧实现与新实现都按"前缀"匹配（tuple 比较的自然语义），
    # 这里确认两者一致，而不是断言它为空。
    assert trees["code_id"].lookup(txn, [3]) == _reference_lookup(trees["code_id"], [3])
    assert len(trees["code_id"].lookup(txn, [3])) > 1

    # 随机 120 组完整/不完整键做对照
    rng = random.Random(20261117)
    for _ in range(120):
        code = rng.randrange(0, 9)
        row = rng.randrange(0, 2100)
        values = [code, row]
        assert trees["code_id"].lookup(txn, values) == _reference_lookup(
            trees["code_id"], values
        )


def test_lookup_on_empty_index_returns_empty(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE e (id INT)")
    db.execute_one("CREATE INDEX idx_e ON e (id)")
    txn = db.txns.begin(read_only=True)
    table = db.catalog.get_table("e")
    tree = db.executor._tree_for(table, table.indexes["idx_e"])
    try:
        assert tree.lookup(txn, [1]) == []
        assert tree.lookup_one(txn, [1]) is None
    finally:
        db.close()


def test_lookup_one_rejects_duplicates_on_declared_unique_index(env):
    txn, trees = env
    # idx_code_id 声明为 UNIQUE，正常数据下 lookup_one 正常返回
    rid = trees["code_id"].lookup_one(txn, [3, 3])
    assert rid is not None
    # 非唯一索引上 lookup_one 遇到多条必须报错（它只用于声明的唯一索引）
    with pytest.raises(BTreeInvariantError):
        trees["bucket"].lookup_one(txn, [3])
