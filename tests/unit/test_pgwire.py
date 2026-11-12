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


class TestExtendedQuery:
    """扩展协议：Parse/Bind/Describe/Execute/Sync + ``$n`` 参数。"""

    @staticmethod
    def _msg(kind: bytes, body: bytes) -> bytes:
        return kind + struct.pack("!i", len(body) + 4) + body

    def _parse(self, sql: str, oids: list[int]) -> bytes:
        body = b"\x00" + sql.encode() + b"\x00"
        body += struct.pack("!h", len(oids))
        body += b"".join(struct.pack("!I", oid) for oid in oids)
        return self._msg(b"P", body)

    def _bind(self, values, *, formats=None, portal="") -> bytes:
        body = portal.encode() + b"\x00" + b"\x00"
        if formats is None:
            body += struct.pack("!h", 0)
        else:
            body += struct.pack("!h", len(formats))
            body += b"".join(struct.pack("!h", f) for f in formats)
        body += struct.pack("!h", len(values))
        for value in values:
            if value is None:
                body += struct.pack("!i", -1)
            else:
                body += struct.pack("!i", len(value)) + value
        body += struct.pack("!h", 0)  # 结果格式：全部文本
        return self._msg(b"B", body)

    def _execute(self, portal="") -> bytes:
        return self._msg(b"E", portal.encode() + b"\x00" + struct.pack("!i", 0))

    async def _ready(self, reader, writer):
        payload = (
            struct.pack("!i", 196608)
            + b"user\x00lumendb\x00database\x00lumendb\x00\x00"
        )
        writer.write(struct.pack("!i", len(payload) + 4) + payload)
        await writer.drain()
        await _read_until_ready(reader)

    def test_select_with_param(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await self._ready(reader, writer)
            writer.write(self._parse("SELECT name FROM t WHERE id = $1", [23]))
            writer.write(self._bind([b"2"]))
            writer.write(self._execute())
            writer.write(self._msg(b"S", b""))
            await writer.drain()
            messages = await _read_until_ready(reader)
            kinds = [k for k, _ in messages]
            assert kinds[:2] == [b"1", b"2"]  # ParseComplete + BindComplete
            rows = [_parse_data_row(p) for k, p in messages if k == b"D"]
            assert rows == [["y"]]
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_binary_int8_param(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await self._ready(reader, writer)
            writer.write(self._parse("SELECT name FROM t WHERE id = $1", [20]))
            writer.write(self._bind([struct.pack("!q", 1)], formats=[1]))
            writer.write(self._execute())
            writer.write(self._msg(b"S", b""))
            await writer.drain()
            messages = await _read_until_ready(reader)
            rows = [_parse_data_row(p) for k, p in messages if k == b"D"]
            assert rows == [["x"]]
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_unknown_oid_infers_int(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (id INT, name TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await self._ready(reader, writer)
            writer.write(self._parse("SELECT name FROM t WHERE id = $1", [0]))
            writer.write(self._bind([b"1"]))
            writer.write(self._execute())
            writer.write(self._msg(b"S", b""))
            await writer.drain()
            messages = await _read_until_ready(reader)
            rows = [_parse_data_row(p) for k, p in messages if k == b"D"]
            assert rows == [["x"]]
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_parameter_description_oids(self, tmp_path):
        db = Database(tmp_path / "db")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await self._ready(reader, writer)
            writer.write(self._parse("SELECT * FROM t WHERE a = $1", [23]))
            writer.write(self._msg(b"D", b"S\x00"))
            writer.write(self._msg(b"S", b""))
            await writer.drain()
            messages = await _read_until_ready(reader)
            param_desc = next(p for k, p in messages if k == b"t")
            count = struct.unpack("!h", param_desc[:2])[0]
            oid = struct.unpack("!I", param_desc[2:6])[0]
            assert (count, oid) == (1, 23)
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_missing_param_reports_42p02(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (a INT)")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await self._ready(reader, writer)
            sql = "SELECT * FROM t WHERE a = $1"
            body_bytes = sql.encode() + b"\x00"
            writer.write(
                b"Q" + struct.pack("!i", len(body_bytes) + 4) + body_bytes
            )
            await writer.drain()
            messages = await _read_until_ready(reader)
            err = next(p for k, p in messages if k == b"E")
            assert b"42P02" in err
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()

    def test_bind_count_mismatch(self, tmp_path):
        db = Database(tmp_path / "db")
        db.execute_one("CREATE TABLE t (a INT)")
        server = PGWireServer(db)

        async def body(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            await self._ready(reader, writer)
            writer.write(self._parse("SELECT * FROM t WHERE a = $1", [23]))
            writer.write(self._bind([]))
            writer.write(self._msg(b"S", b""))
            await writer.drain()
            messages = await _read_until_ready(reader)
            err = next(p for k, p in messages if k == b"E")
            assert b"08P01" in err
            writer.close()
            await writer.wait_closed()

        asyncio.run(_run_session(db, [], {"handler": server._handle, "body": body}))
        db.close()
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
