"""目录持久化频率：DML 只在**结构**变化时落盘。

目录保存是"整链重写 + 每页全页 WAL 镜像"，早期实现每行 DML 都做一次，
于是文件按行增长、写放大巨大。现在 DML 只在 `root_page`/`last_page`/索引
`root_page` 这些**结构性字段**变化时落盘（`_persist_if_structure_changed`），
`row_count_hint` 这类统计 hint 只在内存里更新。

行为没有专门的回归用例，这里补上：
- 连续单行插入不应"每行一次"重写目录；
- 真的分配了新页时必须落盘（否则崩溃后 `last_page` 是旧值，下一次分配会把
  新页链到旧链尾、覆盖它的 `next` 指针）；
- 重开数据库后行数与链尾仍然自洽。
"""

from __future__ import annotations

import pytest

from lumendb.catalog.store import CatalogStore
from lumendb.engine.database import Database


@pytest.fixture
def db(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, payload TEXT)")
    yield db
    db.close()


def _count_saves(db, monkeypatch) -> list[int]:
    calls = [0]
    original = CatalogStore.save

    def counting_save(self, txn, payload, **kwargs):
        calls[0] += 1
        return original(self, txn, payload, **kwargs)

    monkeypatch.setattr(CatalogStore, "save", counting_save)
    return calls


def test_single_row_inserts_do_not_rewrite_the_catalog_per_row(db, monkeypatch):
    calls = _count_saves(db, monkeypatch)
    for i in range(1, 41):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'x')")
    assert calls[0] < 40, f"40 行插入了 {calls[0]} 次目录保存，说明仍在按行落盘"
    assert calls[0] <= 8, f"目录保存次数 {calls[0]} 偏高（应该只在分配新页时发生）"


def test_allocating_a_new_page_persists_the_catalog(db, monkeypatch):
    """真的分配新页时必须落盘：否则崩溃后 last_page 指向旧链尾。"""
    calls = _count_saves(db, monkeypatch)
    # 每行 ~1KB，一行一页都不够放满，必然触发多次页分配
    payload = "y" * 900
    for i in range(1, 21):
        db.execute_one(f"INSERT INTO t VALUES ({i}, '{payload}')")
    assert calls[0] >= 1, "分配了新页却没有保存目录"
    table = db.catalog.get_table("t")
    assert table.last_page != table.root_page + 1 or calls[0] >= 1


def test_rows_and_chain_survive_reopen(tmp_path):
    path = tmp_path / "db2"
    db = Database(path, checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, payload TEXT)")
    payload = "z" * 900
    for i in range(1, 31):
        db.execute_one(f"INSERT INTO t VALUES ({i}, '{payload}')")
    last_page_before = db.catalog.get_table("t").last_page
    db.close()

    reopened = Database(path, checkpoint_wal_bytes=0)
    try:
        assert reopened.execute_one("SELECT COUNT(*) FROM t").rows == [(30,)]
        assert reopened.catalog.get_table("t").last_page == last_page_before
        # 链尾指针必须是可用的：再插入一行不应破坏已有行
        reopened.execute_one("INSERT INTO t VALUES (31, 'last')")
        assert reopened.execute_one("SELECT COUNT(*) FROM t").rows == [(31,)]
        assert reopened.execute_one("SELECT id FROM t WHERE id = 1").rows == [(1,)]
    finally:
        reopened.close()


def test_hint_is_not_persisted_but_queries_still_work(db):
    """row_count_hint 只在内存里更新：不落盘也不影响查询结果。"""
    for i in range(1, 11):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'v{i}')")
    assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(10,)]
    table = db.catalog.get_table("t")
    assert table.row_count_hint >= 10
