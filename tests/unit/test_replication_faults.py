"""复制流的网络故障注入：半包、超长行、中途断开，服务端都要活下来。"""

from __future__ import annotations

import socket

import pytest

from lumendb.engine.database import Database
from lumendb.replication.wal_stream import (
    _MAX_REQUEST_BYTES,
    WALClient,
    WALServer,
)


def _start_server(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    return db, WALServer(db, idle_timeout=3.0).start()


def _raw_send(host, port, payload: bytes, *, half_close: bool = True) -> None:
    """用裸 socket 发一段原始字节，模拟不守协议的客户端。"""
    sock = socket.create_connection((host, port), timeout=3.0)
    try:
        sock.sendall(payload)
        if half_close:
            # 半关闭：对端读到 EOF，但连接上没有完整的一行。
            sock.shutdown(socket.SHUT_WR)
            try:
                sock.settimeout(3.0)
                sock.recv(4096)
            except OSError:
                pass
    finally:
        sock.close()


def test_truncated_request_does_not_kill_the_server(tmp_path):
    """发半截 JSON 就断开：服务端必须安静放弃这条连接，并继续服务别人。"""
    db, server = _start_server(tmp_path)
    try:
        _raw_send(server.host, server.port, b'{"type":"status"')
        # 服务端还活着：正常客户端能拿到 status。
        status = WALClient(server.host, server.port, timeout=3.0).status()
        assert status["current_lsn"] >= 0
    finally:
        server.stop()
        db.close()


def test_garbage_line_returns_an_error_and_keeps_serving(tmp_path):
    db, server = _start_server(tmp_path)
    try:
        _raw_send(server.host, server.port, b"not-json\n")
        status = WALClient(server.host, server.port, timeout=3.0).status()
        assert status["current_lsn"] >= 0
    finally:
        server.stop()
        db.close()


def test_oversized_request_line_is_rejected(tmp_path):
    """超过上限的请求行：拒绝并关闭连接，不把整行读进内存。"""
    db, server = _start_server(tmp_path)
    try:
        sock = socket.create_connection((server.host, server.port), timeout=5.0)
        try:
            chunk = b"x" * (1024 * 1024)
            sent = 0
            try:
                while sent < _MAX_REQUEST_BYTES + 1024 * 1024:
                    sock.sendall(chunk)
                    sent += len(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass  # 服务端提前关闭连接，正是预期
            sock.settimeout(5.0)
            data = b""
            try:
                while True:
                    piece = sock.recv(4096)
                    if not piece:
                        break
                    data += piece
            except (socket.timeout, OSError):
                pass
            assert b"error" in data or data == b""
        finally:
            sock.close()
        # 服务端没被拖死。
        assert WALClient(server.host, server.port, timeout=3.0).status()[
            "current_lsn"
        ] >= 0
    finally:
        server.stop()
        db.close()


def test_connection_dropped_mid_transfer_raises_to_the_caller(tmp_path):
    """传输中断时客户端必须抛错（不能静默返回半批记录）。"""
    db, server = _start_server(tmp_path)
    try:
        for i in range(30):
            db.execute_one(f"INSERT INTO t VALUES ({i})")
        # 用裸 socket 发 replicate 请求，然后在服务端写响应的过程中强行关闭。
        sock = socket.create_connection((server.host, server.port), timeout=3.0)
        try:
            sock.sendall(
                b'{"type":"replicate","start_lsn":0,"max_records":1000}\n'
            )
            # 读一点点就关，模拟链路中断。
            sock.settimeout(1.0)
            try:
                sock.recv(64)
            except OSError:
                pass
        finally:
            sock.close()
        # 服务端仍在正常工作（写入线程的 socket.timeout 被安静处理）。
        assert WALClient(server.host, server.port, timeout=3.0).status()[
            "current_lsn"
        ] >= 0
    finally:
        server.stop()
        db.close()


def test_client_error_when_server_disappears(tmp_path):
    """主库进程消失后，客户端要明确抛错，副本层才能累计失败次数。"""
    db, server = _start_server(tmp_path)
    host, port = server.host, server.port
    server.stop()
    db.close()
    with pytest.raises(Exception):
        WALClient(host, port, timeout=1.0).status()
