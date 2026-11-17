"""复制流长连接：一条 TCP 连接上复用多个请求。"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALClient, WALServer


def _server(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
    for i in range(30):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'user{i}')")
    return db, WALServer(db, idle_timeout=5.0).start()


def test_keepalive_reuses_one_connection_for_many_requests(tmp_path):
    db, server = _server(tmp_path)
    try:
        client = WALClient(server.host, server.port, keepalive=True)
        try:
            for _ in range(5):
                status = client.status()
                assert status["current_lsn"] > 0
            # 5 个请求只应该建 1 条连接。
            assert server.connection_count == 1
            # 同一连接上换一种请求类型也没问题（默认 → 二进制帧）。
            client.binary_records = True
            records_bin = client.fetch(0, max_records=100)
            assert records_bin
        finally:
            client.close()
    finally:
        server.stop()
        db.close()


def test_without_keepalive_each_request_is_a_new_connection(tmp_path):
    db, server = _server(tmp_path)
    try:
        for _ in range(3):
            WALClient(server.host, server.port).status()
        assert server.connection_count == 3
    finally:
        server.stop()
        db.close()


def test_keepalive_survives_a_compressed_and_a_binary_request_on_one_connection(tmp_path):
    """压缩与二进制帧都是逐请求的：同一条连接上混用要都能正确解析。"""
    db, server = _server(tmp_path)
    try:
        client = WALClient(server.host, server.port, keepalive=True)
        try:
            json_records = client.fetch(0, max_records=200)
            client.compress = True
            compressed_records = client.fetch(0, max_records=200)
            client.compress = False
            client.binary_records = True
            binary_records = client.fetch(0, max_records=200)
        finally:
            client.close()
        assert len(json_records) == len(compressed_records) == len(binary_records) > 0
        for a, b, c in zip(json_records, compressed_records, binary_records):
            assert (a.lsn, a.payload) == (b.lsn, b.payload) == (c.lsn, c.payload)
        assert server.connection_count == 1
    finally:
        server.stop()
        db.close()


def test_keepalive_client_reconnects_after_the_server_closed_the_connection(tmp_path):
    """服务端关掉连接后，下一次请求要能自己重连，而不是永久失能。"""
    db, server = _server(tmp_path)
    try:
        client = WALClient(server.host, server.port, keepalive=True)
        try:
            assert client.status()["current_lsn"] > 0
            client.close()  # 模拟连接断开
            assert client.status()["current_lsn"] > 0
        finally:
            client.close()
        assert server.connection_count == 2
    finally:
        server.stop()
        db.close()


def test_keepalive_is_off_by_default(tmp_path):
    db, server = _server(tmp_path)
    try:
        client = WALClient(server.host, server.port)
        assert client.keepalive is False
        client.status()
        client.close()
    finally:
        server.stop()
        db.close()


def test_replica_can_catch_up_over_a_keepalive_connection(tmp_path):
    """Replica 侧也接起来了：一批批 fetch 复用一条连接，内容仍然正确。"""
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for i in range(50):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")
    server = WALServer(primary, idle_timeout=5.0).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    try:
        # 基准备份带走了前 50 行；再写 30 行，才有可增量应用的部分。
        for i in range(50, 80):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        client = WALClient(server.host, server.port, keepalive=True)
        try:
            start = replica.applied_lsn
            applied = 0
            while True:
                batch = client.fetch(start, max_records=10)
                if not batch:
                    break
                applied += replica.apply(batch)
                start = batch[-1].lsn
        finally:
            client.close()
        assert applied > 0
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(80,)]
        # 6 批记录挤在一条连接上（重连余量留 1 次）。
        assert server.connection_count <= 2
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_replicate_from_server_reuses_one_connection_when_asked(tmp_path):
    """Replica 层也支持复用：一批批追平时不再每批握手一次。"""
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for i in range(20):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")
    server = WALServer(primary, idle_timeout=5.0).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    try:
        for i in range(20, 60):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        applied = 0
        while True:
            n = replica.replicate_from_server(
                server.host,
                server.port,
                max_records=10,
                keepalive=True,
                timeout=5.0,
            )
            if n == 0:
                break
            applied += n
        assert applied > 0
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(60,)]
        # 1 条基准备份连接 + 1 条增量连接（重连余量留 1 次）。
        assert server.connection_count <= 3
    finally:
        replica.close()
        server.stop()
        primary.close()
