"""B+ 树内部页解码缓存：页 LSN 失效、DDL 清空、结果始终正确。"""

from __future__ import annotations

from lumendb.engine.database import Database


def _build(tmp_path, rows: int):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT, bucket INT, pad VARCHAR(16))")
    for i in range(rows):
        db.execute_one("INSERT INTO t VALUES ($1, $2, $3)", [i, i % 10, f"u{i}"])
    db.execute_one("CREATE INDEX idx_id ON t (id)")
    db.execute_one("CREATE INDEX idx_bucket ON t (bucket)")
    return db


def test_internal_cache_hits_and_stays_correct_after_splits(tmp_path):
    db = _build(tmp_path, 1000)
    table = db.catalog.get_table("t")
    tree = db.executor._tree_for(table, table.indexes["idx_id"])
    txn = db.txns.begin(read_only=True)
    try:
        first = tree.lookup(txn, [500])
        assert len(first) == 1
        cache = db.executor._internal_page_cache
        size = len(cache)
        assert size > 0, "点查应至少缓存一层内部页"
        second = tree.lookup(txn, [500])
        assert second == first
        assert len(cache) == size, "同一页同一 LSN 应命中缓存，不新增条目"
    finally:
        db.txns.abort(txn)

    # 再插入 1000 行，索引根页会分裂；新树的查找必须看到新行
    for i in range(1000, 2000):
        db.execute_one("INSERT INTO t VALUES ($1, $2, $3)", [i, i % 10, f"u{i}"])
    table = db.catalog.get_table("t")
    tree2 = db.executor._tree_for(table, table.indexes["idx_id"])
    txn2 = db.txns.begin(read_only=True)
    try:
        assert len(tree2.lookup(txn2, [1500])) == 1
        bucket_tree = db.executor._tree_for(table, table.indexes["idx_bucket"])
        for bucket in range(10):
            assert len(bucket_tree.lookup(txn2, [bucket])) == 200
    finally:
        db.txns.abort(txn2)
    db.close()


def test_internal_cache_cleared_on_ddl(tmp_path):
    db = _build(tmp_path, 500)
    table = db.catalog.get_table("t")
    tree = db.executor._tree_for(table, table.indexes["idx_id"])
    txn = db.txns.begin(read_only=True)
    try:
        tree.lookup(txn, [42])
        assert db.executor._internal_page_cache
    finally:
        db.txns.abort(txn)

    db.execute_one("CREATE INDEX idx_id_pad ON t (id, pad)")
    table = db.catalog.get_table("t")
    db.executor._tree_for(table, table.indexes["idx_id_pad"])
    assert db.executor._internal_page_cache == {}, "DDL 后页号可能复用，缓存必须整体清空"
    db.close()
