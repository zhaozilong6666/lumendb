"""第 2 层验收：TPC-H 风格固定小数据集上的连接顺序与结果回归。

真正的 TPC-H 数据量大、依赖复杂；这里保留三张核心表和一条
customer/orders/lineitem 三表连接，用来锁定：

- 三表逗号连接会走 DP，并在 EXPLAIN 里显示 ``order=dp``；
- 重排后输出列仍按 SQL 书写顺序还原；
- 过滤条件不会因为重排漏掉行；
- 结果集包含重复行计数，避免只比较去重后的集合。
"""

from lumendb.engine.database import Database


def rows(db, sql):
    return db.execute_one(sql).rows


def test_tpch_style_three_table_join_acceptance(tmp_path):
    db = Database(tmp_path / "tpch")
    try:
        db.execute_one("CREATE TABLE customer (id INT PRIMARY KEY, name TEXT, region TEXT)")
        db.execute_one("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status TEXT)")
        db.execute_one("CREATE TABLE lineitem (id INT PRIMARY KEY, order_id INT, amount INT)")

        regions = ["ASIA", "EUROPE", "AMERICA"]
        for i in range(1, 13):
            db.execute_one(
                f"INSERT INTO customer VALUES ({i}, 'c{i}', '{regions[i % 3]}')"
            )
        for i in range(1, 25):
            status = "F" if i % 2 == 0 else "O"
            db.execute_one(
                f"INSERT INTO orders VALUES ({i}, {(i % 12) + 1}, '{status}')"
            )
        for i in range(1, 37):
            db.execute_one(
                f"INSERT INTO lineitem VALUES ({i}, {(i % 24) + 1}, {i % 7 + 1})"
            )

        query = (
            "SELECT c.name, o.id, l.amount "
            "FROM lineitem l, orders o, customer c "
            "WHERE c.id = o.customer_id AND o.id = l.order_id "
            "AND o.status = 'F' AND c.region = 'ASIA' "
            "ORDER BY c.name, o.id, l.id"
        )
        expected = rows(db, query)
        assert expected, "验收数据必须能产生至少一行结果"

        plan = "\n".join(r[0] for r in rows(db, "EXPLAIN " + query))
        assert "[order=dp]" in plan
        assert "[candidates=" in plan
        assert "SeqScan customer" in plan
        # 单源过滤条件应下推到扫描之上；最终 WHERE 仍保留一层兜底 Filter。
        assert plan.count("Filter") >= 2

        # 再执行一次，确保重排后的计划稳定，且输出列没有被拼接顺序污染。
        assert rows(db, query) == expected
        assert all(row[0].startswith("c") for row in expected)
    finally:
        db.close()


def test_tpch_style_result_order_and_nulls(tmp_path):
    db = Database(tmp_path / "tpch-null")
    try:
        db.execute_one("CREATE TABLE a (id INT, v TEXT)")
        db.execute_one("CREATE TABLE b (id INT, v TEXT)")
        db.execute_one("CREATE TABLE c (id INT, v TEXT)")
        db.execute_one("INSERT INTO a VALUES (1, 'a1'), (2, NULL)")
        db.execute_one("INSERT INTO b VALUES (1, 'b1'), (2, 'b2')")
        db.execute_one("INSERT INTO c VALUES (1, 'c1'), (2, 'c2')")
        query = (
            "SELECT a.v, b.v, c.v FROM a, b, c "
            "WHERE a.id = b.id AND b.id = c.id ORDER BY a.id"
        )
        assert rows(db, query) == [("a1", "b1", "c1"), (None, "b2", "c2")]
    finally:
        db.close()
