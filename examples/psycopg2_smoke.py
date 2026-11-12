"""用真正的 libpq 客户端（psycopg2）连 LumenDB，验证线协议不是"自测自过"。

先安装驱动（不进项目依赖）：
    python -m pip install psycopg2-binary

再运行：
    python examples/psycopg2_smoke.py
"""

from __future__ import annotations

import asyncio
import socket
import tempfile
import threading
import time
from pathlib import Path

import psycopg2

from lumendb.engine.database import Database
from lumendb.server.pgwire import PGWireServer

PORT = 55432


def wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"服务端没有在 {port} 端口起来")


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="lumendb-psycopg2-"))
    db = Database(root / "db")
    server = PGWireServer(db, port=PORT)
    thread = threading.Thread(
        target=lambda: asyncio.run(server.serve_forever()), daemon=True
    )
    thread.start()
    wait_for_port(PORT)

    def connect():
        return psycopg2.connect(
            host="127.0.0.1", port=PORT, user="lumendb", dbname="lumendb"
        )

    conn = connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    conn.commit()
    cur.execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Linus')")
    conn.commit()

    # libpq 的扩展查询：参数由客户端编码，服务端解码后再绑定
    cur.execute("SELECT name FROM users WHERE id = %s", (2,))
    assert cur.fetchall() == [("Linus",)]

    # 光标描述来自 RowDescription
    assert [d.name for d in cur.description] == ["name"]

    # 两个连接：未提交的数据对另一个连接不可见
    other = connect()
    other_cur = other.cursor()
    cur.execute("INSERT INTO users VALUES (3, 'Grace')")
    other_cur.execute("SELECT COUNT(*) FROM users")
    assert other_cur.fetchall() == [(2,)]
    other.commit()
    conn.commit()
    other_cur.execute("SELECT COUNT(*) FROM users")
    assert other_cur.fetchall() == [(3,)]
    other.commit()

    # 唯一键冲突必须按 SQLSTATE 23505 抛成 UniqueViolation
    try:
        cur.execute("INSERT INTO users VALUES (1, 'dup')")
        conn.commit()
        raise AssertionError("重复主键没有报错")
    except psycopg2.errors.UniqueViolation as exc:
        assert exc.pgcode == "23505"
        conn.rollback()

    cur.execute("SELECT id, name FROM users ORDER BY id")
    assert cur.fetchall() == [(1, "Ada"), (2, "Linus"), (3, "Grace")]
    conn.commit()

    cur.close()
    conn.close()
    other_cur.close()
    other.close()
    print("psycopg2 连接验证通过：DDL / 参数化查询 / 事务隔离 / SQLSTATE 23505")


if __name__ == "__main__":
    main()
