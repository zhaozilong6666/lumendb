"""DML 的目录持久化频率与页链完整性（结构变化才 persist）。"""

from __future__ import annotations

from lumendb.catalog import catalog as catalog_module
from lumendb.engine.database import Database


def _open(tmp_path):
    return Database(tmp_path / "db", checkpoint_wal_bytes=0)


def test_dml_persists_catalog_only_when_structure_changes(tmp_path, monkeypatch):
    """单页内的插入不写目录；分配新页时才写（每页一次，不是每行一次）。"""
    db = _open(tmp_path)
    calls = {"n": 0}
    real = catalog_module.Catalog.persist

    def counting(cat_self, txn):
        calls["n"] += 1
        return real(cat_self, txn)

    monkeypatch.setattr(catalog_module.Catalog, "persist", counting)
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        base = calls["n"]
        assert base >= 1  # 建表本身必须落盘

        # 第一行会分配首页（root_page 从 INVALID 变成真实页）→ 结构变化，
        # 必须 persist：否则崩溃恢复后表找不到第一页。
        db.execute_one("INSERT INTO t VALUES (0, 'x')")
        assert calls["n"] == base + 1, "分配首页必须落盘"
        base = calls["n"]

        # 小行、少量：都在首页内 → 不动结构 → 不 persist。
        for i in range(1, 5):
            db.execute_one(f"INSERT INTO t VALUES ({i}, 'x')")
        assert calls["n"] == base, "单页内插入不应重写整份目录"

        # 大行、多行：必然跨页 → 每分配一页 persist 一次。
        payload = "y" * 400
        for i in range(5, 45):
            db.execute_one(f"INSERT INTO t VALUES ({i}, '{payload}')")
        assert calls["n"] > base
        # 每页至少能装一行，40 行不可能只写一次目录——但也不该到 40 次。
        assert calls["n"] < base + 40
    finally:
        db.close()


def test_reopen_after_multi_page_inserts_keeps_page_chain(tmp_path):
    """跨页插入后关闭并重开：行数一致，且能继续插入（页链没被覆盖）。"""
    db = _open(tmp_path)
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        payload = "z" * 400
        for i in range(50):
            db.execute_one(f"INSERT INTO t VALUES ({i}, '{payload}')")
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(50,)]
    finally:
        db.close()

    db = _open(tmp_path)
    try:
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(50,)]
        for i in range(50, 80):
            db.execute_one(f"INSERT INTO t VALUES ({i}, '{payload}')")
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(80,)]
        # 链尾之后还能找到新行（不是覆盖旧链）。
        assert db.execute_one("SELECT id FROM t WHERE id = 79").rows == [(79,)]
    finally:
        db.close()


def test_delete_does_not_rewrite_catalog(tmp_path, monkeypatch):
    """删除只改 row_count_hint（统计 hint）→ 不重写目录。"""
    db = _open(tmp_path)
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        for i in range(5):
            db.execute_one(f"INSERT INTO t VALUES ({i})")
        calls = {"n": 0}
        real = catalog_module.Catalog.persist

        def counting(cat_self, txn):
            calls["n"] += 1
            return real(cat_self, txn)

        monkeypatch.setattr(catalog_module.Catalog, "persist", counting)
        db.execute_one("DELETE FROM t WHERE id = 3")
        assert calls["n"] == 0
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(4,)]
    finally:
        db.close()
