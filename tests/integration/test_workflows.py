"""跨组件的真实工作流测试：事务、索引一致性、组合查询、重开校验。

单元测试验证的是"每个零件对"，这里验证的是"拼起来对"。
"""

import pytest

from lumendb.engine.database import Database
from lumendb.errors import DuplicateKeyError


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "db")
    yield database
    database.close()


@pytest.fixture
def shop(db):
    db.execute_one("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(40) NOT NULL, price INT, cat TEXT)")
    db.execute_one("CREATE TABLE sales (id INT PRIMARY KEY, pid INT, qty INT)")
    db.execute_one(
        "INSERT INTO products VALUES (1, 'pen', 3, 'office'), (2, 'desk', 300, 'furniture'), "
        "(3, 'lamp', 60, 'office'), (4, 'chair', 200, 'furniture')"
    )
    db.execute_one(
        "INSERT INTO sales VALUES (10, 1, 100), (11, 3, 20), (12, 2, 1), (13, 1, 50)"
    )
    return db


def rows(db, sql):
    return db.execute_one(sql).rows


class TestTransactions:
    def test_rollback_discards_update(self, shop):
        shop.execute_one("BEGIN")
        shop.execute_one("UPDATE products SET price = 999 WHERE id = 1")
        shop.execute_one("ROLLBACK")
        assert rows(shop, "SELECT price FROM products WHERE id = 1") == [(3,)]

    def test_commit_makes_rows_visible_after_reopen(self, tmp_path):
        db = Database(tmp_path / "tx")
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        db.execute_one("BEGIN")
        db.execute_one("INSERT INTO t VALUES (1, 10)")
        db.execute_one("COMMIT")
        db.close()

        reopened = Database(tmp_path / "tx")
        try:
            assert reopened.execute_one("SELECT v FROM t WHERE id = 1").rows == [(10,)]
        finally:
            reopened.close()

    def test_rollback_of_insert_is_invisible_to_other_txn(self, shop):
        shop.execute_one("BEGIN")
        shop.execute_one("INSERT INTO products VALUES (99, 'ghost', 1, 'x')")
        shop.execute_one("ROLLBACK")
        assert rows(shop, "SELECT COUNT(*) FROM products") == [(4,)]


class TestIndexConsistency:
    def test_index_scan_matches_full_scan(self, shop):
        for name, idx in [("no_index", None), ("with_index", "idx_price")]:
            if idx:
                shop.execute_one(f"CREATE INDEX {idx} ON products (price)")
            scanned = rows(shop, "SELECT id FROM products WHERE price = 200 ORDER BY id")
            explained = shop.execute_one("EXPLAIN SELECT id FROM products WHERE price = 200")
            assert scanned == [(4,)]
            plan = "\n".join(str(r[0]) for r in explained.rows)
            if idx:
                assert "IndexScan products [idx_price(price) = (200)]" in plan, plan
            else:
                assert "SeqScan" in plan, plan

    def test_unique_index_blocks_duplicate_and_keeps_rows_unchanged(self, shop):
        shop.execute_one("CREATE UNIQUE INDEX uniq_name ON products (name)")
        with pytest.raises(DuplicateKeyError):
            shop.execute_one("INSERT INTO products VALUES (7, 'pen', 1, 'office')")
        assert rows(shop, "SELECT COUNT(*) FROM products") == [(4,)]

    def test_update_that_moves_an_indexed_key_keeps_lookups_correct(self, shop):
        shop.execute_one("CREATE INDEX idx_cat ON products (cat)")
        shop.execute_one("UPDATE products SET cat = 'mixed' WHERE id IN (1, 2)")
        assert rows(shop, "SELECT id FROM products WHERE cat = 'office' ORDER BY id") == [(3,)]
        assert rows(shop, "SELECT id FROM products WHERE cat = 'mixed' ORDER BY id") == [(1,), (2,)]


class TestCombinedQueries:
    def test_join_group_having_order_limit(self, shop):
        result = rows(
            shop,
            "SELECT p.cat, SUM(s.qty) AS total FROM products p JOIN sales s ON p.id = s.pid "
            "GROUP BY p.cat HAVING SUM(s.qty) > 100 ORDER BY total DESC LIMIT 1",
        )
        assert result == [("office", 170)]

    def test_case_when_inside_aggregate_over_a_table(self, shop):
        result = rows(
            shop,
            "SELECT SUM(CASE WHEN price >= 200 THEN 1 ELSE 0 END) FROM products",
        )
        assert result == [(2,)]

    def test_order_by_null_goes_last(self, shop):
        shop.execute_one("INSERT INTO products VALUES (5, 'mat', NULL, 'x')")
        assert rows(shop, "SELECT price FROM products ORDER BY price")[-1] == (None,)

    def test_delete_then_unique_insert_reuses_key(self, shop):
        shop.execute_one("DELETE FROM products WHERE id = 1")
        shop.execute_one("INSERT INTO products VALUES (1, 'pen2', 4, 'office')")
        assert rows(shop, "SELECT name FROM products WHERE id = 1") == [("pen2",)]
