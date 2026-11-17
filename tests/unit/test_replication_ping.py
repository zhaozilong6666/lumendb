"""应用层心跳：`ping` / `pong` 把 TCP 半开变成一次可捕获的失败。

长连接复用之后，如果对端进程被 kill（没有 FIN/RST 发出），客户端那条
ESTABLISHED 连接在下一次 `fetch` 之前都不会报错；探活必须靠一次真实往返。
"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.errors import StorageError
from lumendb.replication.wal_stream import WALClient, WALServer


def test_ping_returns_current_lsn(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    server = WALServer(primary).start()
    try:
        client = WALClient(server.host, server.port)
        lsn = client.ping()
        assert lsn >= 0
        # 写一笔之后 LSN 前进，ping 立刻反映出来。
        primary.execute_one("INSERT INTO t VALUES (1)")
        assert client.ping() > lsn
        client.close()
    finally:
        server.stop()
        primary.close()


def test_ping_reuses_keepalive_connection(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    server = WALServer(primary).start()
    client = WALClient(server.host, server.port, keepalive=True)
    try:
        baseline = server.connection_count
        for _ in range(20):
            assert client.ping() >= 0
        # keepalive：20 次探活只应开一条连接。
        assert server.connection_count - baseline == 1
    finally:
        client.close()
        server.stop()
        primary.close()


def test_ping_rejects_wrong_token(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    server = WALServer(primary, token="s3cret").start()
    try:
        client = WALClient(server.host, server.port)
        with pytest.raises(StorageError):
            client.ping()
        client.close()
        ok = WALClient(server.host, server.port, token="s3cret")
        assert ok.ping() >= 0
        ok.close()
    finally:
        server.stop()
        primary.close()


def test_ping_fails_when_server_stopped(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    server = WALServer(primary).start()
    client = WALClient(server.host, server.port, keepalive=True)
    try:
        assert client.ping() >= 0
    finally:
        server.stop()
        primary.close()
    # 服务端已停：探活必须报错，而不是悄悄返回 0。
    with pytest.raises((StorageError, OSError, EOFError)):
        client.ping()
    client.close()
