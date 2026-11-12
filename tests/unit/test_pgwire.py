"""PostgreSQL 线协议的测试：用真实 socket 走一遍启动 + 简单查询。"""

import asyncio
import struct

import pytest

from lumendb.engine.database import Database
from lumendb.server.pgwire import PGWireServer


async def _read_message(reader):
    kind = await reader.readexactly(1)
    length = struct.unpack("!i", await reader.readexactly(4))[0]
    return kind, await reader.readexactly(length - 4)


async def _read_until_ready(reader):
    messages = []
    while True:
        kind, payload = await _read_message(reader)
        messages.append((kind, payload))
        if kind == b"Z":
            return messages


def _parse_row_description(payload):
    count = struct.unpack("!h", payload[:2])[0]
    pos = 2
    names = []
    for _ in range(count):
        end = payload.find(b"\x00", pos)
        names.append(payload[pos:end].decode())
        pos = end + 1 + 18  # 跳过表 OID / 列号 / 类型 OID / 长度 / 修饰 / 格式
    return names


def _parse_data_row(payload):
    count = struct.unpack("!h", payload[:2])[0]
    pos = 2
    values = []
    for _ in range(count):
        size = struct.unpack("!i", payload[pos : pos + 4])[0]
        pos += 4
        if size < 0:
            values.append(None)
        else:
            values.append(payload[pos : pos + size].decode())
            pos += size
    return values


async def _run_session(db, port_holder, script):
    server = await asyncio.start_server(script["handler"], "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    port_holder.append(port)
    await script["body"](port)
    server.close()
    await server.wait_closed()


class TestStartup:
    def test_startup_handshake(self, tmp_path):
        db = Database(tmp_path / "db")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = (
                struct.pack("!i", 196608)
                + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
            )
            writer.write(struct.pack("!i", len(payload) + 4) + payload)
            await writer.drain()
            messages = await _read_until_ready(reader)
            kinds = [k for k, _ in messages]
            assert b"R" in kinds
            assert b"Z" in kinds
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_ssl_request_answered_with_n(self, tmp_path):
        db = Database(tmp_path / "db")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(struct.pack("!i", 8) + struct.pack("!i", 80877103))
            await writer.drain()
            assert await reader.readexactly(1) == b"N"
            payload = (
                struct.pack("!i", 196608)
                + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
            )
            writer.write(struct.pack("!i", len(payload) + 4) + payload)
            await writer.drain()
            await _read_until_ready(reader)
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()


class TestSimpleQuery:
    def test_select_returns_rows(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (a INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = (
                struct.pack("!i", 196608)
                + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
            )
            writer.write(struct.pack("!i", len(payload) + 4) + payload)
            await writer.drain()
            await _read_until_ready(reader)

            writer.write(b"Q" + struct.pack("!i", 12) + b"SELECT 1;\x00")
            await writer.drain()
            messages = await _read_until_ready(reader)
            kinds = [k for k, _ in messages]
            assert b"T" in kinds and b"D" in kinds and b"C" in kinds
            rows = [_parse_data_row(p) for k, p in messages if k == b"D"]
            assert rows == [["1"]]
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_row_description_names(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (a INT, name TEXT)")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = (
                struct.pack("!i", 196608)
                + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
            )
            writer.write(struct.pack("!i", len(payload) + 4) + payload)
            await writer.drain()
            await _read_until_ready(reader)

            sql = "SELECT a, name FROM t"
            body_bytes = sql.encode() + b"\x00"
            writer.write(b"Q" + struct.pack("!i", len(body_bytes) + 4) + body_bytes)
            await writer.drain()
            messages = await _read_until_ready(reader)
            desc = next(p for k, p in messages if k == b"T")
            assert _parse_row_description(desc) == ["a", "name"]
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_error_response(self, tmp_path):
        db = Database(tmp_path / "db")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = (
                struct.pack("!i", 196608)
                + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
            )
            writer.write(struct.pack("!i", len(payload) + 4) + payload)
            await writer.drain()
            await _read_until_ready(reader)

            sql = "SELECT * FROM nope"
            body_bytes = sql.encode() + b"\x00"
            writer.write(b"Q" + struct.pack("!i", len(body_bytes) + 4) + body_bytes)
            await writer.drain()
            messages = await _read_until_ready(reader)
            assert b"E" in [k for k, _ in messages]
            err = next(p for k, p in messages if k == b"E")
            assert b"42P01" in err  # undefined_table
            assert b"nope" in err
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_update_command_tag(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (a INT)")
        db.execute_one("INSERT INTO t VALUES (1)")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = (
                struct.pack("!i", 196608)
                + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
            )
            writer.write(struct.pack("!i", len(payload) + 4) + payload)
            await writer.drain()
            await _read_until_ready(reader)

            sql = "UPDATE t SET a = 2"
            body_bytes = sql.encode() + b"\x00"
            writer.write(b"Q" + struct.pack("!i", len(body_bytes) + 4) + body_bytes)
            await writer.drain()
            messages = await _read_until_ready(reader)
            tag = next(p for k, p in messages if k == b"C")
            assert tag.rstrip(b"\x00") == b"UPDATE 1"
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_percent_style_semicolon_in_string(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (s TEXT)")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = (
                struct.pack("!i", 196608)
                + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
            )
            writer.write(struct.pack("!i", len(payload) + 4) + payload)
            await writer.drain()
            await _read_until_ready(reader)

            sql = "INSERT INTO t VALUES ('a;b')"
            body_bytes = sql.encode() + b"\x00"
            writer.write(b"Q" + struct.pack("!i", len(body_bytes) + 4) + body_bytes)
            await writer.drain()
            messages = await _read_until_ready(reader)
            assert b"E" not in [k for k, _ in messages]
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()
