"""一键演示 LumenDB 的完整链路：DDL → DML → 索引 → 查询 → 聚合 → 更新。

不需要启动服务，也不依赖第三方库：

    python examples/demo.py
"""

import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lumendb.engine.database import Database


def render(result) -> str:
    if not result.columns:
        return result.tag or f"affected={result.affected}"
    names = [c.name for c in result.columns]
    lines = [" | ".join(names), "-+-".join("-" * len(n) for n in names)]
    for row in result.rows:
        lines.append(" | ".join("NULL" if v is None else str(v) for v in row))
    return "\n".join(lines)


def run(db: Database, sql: str) -> None:
    print(f"\n>>> {sql}")
    print(render(db.execute_one(sql)))


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="lumendb-demo-"))
    try:
        db = Database(root)
        run(db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, age INT)")
        run(db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
        run(
            db,
            "INSERT INTO users VALUES (1, 'Ada', 36), (2, 'Linus', 55), (3, 'Grace', 45)",
        )
        run(db, "INSERT INTO orders VALUES (10, 1, 120), (11, 1, 300), (12, 3, 80)")
        run(db, "CREATE INDEX idx_users_age ON users (age)")

        print("\n--- EXPLAIN：等值条件命中主键索引 ---")
        for row in db.execute_one("EXPLAIN SELECT name FROM users WHERE id = 2").rows:
            print(row[0])

        print("\n--- EXPLAIN：范围条件命中 age 索引 ---")
        for row in db.execute_one("EXPLAIN SELECT name FROM users WHERE age > 40").rows:
            print(row[0])

        run(db, "SELECT name, age FROM users WHERE age > 40 ORDER BY age DESC")
        run(db, "SELECT COUNT(*) FROM users")
        run(
            db,
            "SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id "
            "GROUP BY u.name ORDER BY u.name",
        )
        run(db, "UPDATE users SET age = age + 1 WHERE id = 1")
        run(db, "SELECT id, name, age FROM users ORDER BY id")
        run(db, "SELECT name FROM users WHERE id = 99")
        run(db, "DROP TABLE orders")

        print("\n--- 关闭并重新打开：数据来自磁盘，不是内存 ---")
        db.close()
        db2 = Database(root)
        run(db2, "SELECT COUNT(*) FROM users")
        db2.close()
        print(f"\n数据目录: {root}")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
