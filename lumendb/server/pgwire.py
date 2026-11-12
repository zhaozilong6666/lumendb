"""PostgreSQL v3 线协议服务端。

为什么值得做：数据库自研项目最容易停留在"跑测试的库"，接不上任何真实工具。
实现了 wire protocol 之后，``psql`` / DBeaver / Navicat / 任何 JDBC 驱动都能
直接连上来——演示效果和"这是一个真数据库"的说服力完全不同。

协议要点（v3，消息都以一个字节的类型码开头，后跟 int32 长度）：
- 客户端 StartupMessage 没有类型码，直接 长度 + 协议号 + 键值对；
- 认证走 ``AuthenticationOk``（trust 模式，不校验密码）；
- 简单查询 ``Q`` 的响应是 RowDescription + 若干 DataRow + CommandComplete + ReadyForQuery；
- 扩展查询 ``Parse/Bind/Describe/Execute/Sync`` 做一个最小实现：只接受没有
  参数占位符的语句，有 ``$1`` 就明确报不支持，而不是猜。
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass, field
from typing import Any, Optional

from ..datatypes import DataType
from ..engine.database import Database
from ..engine.executor import ColumnMeta, Result
from ..errors import ClientDisconnected, LumenError, NotSupportedError, ProtocolError

PROTOCOL_VERSION = 196608  # 3.0
SSL_REQUEST_CODE = 80877103
CANCEL_REQUEST_CODE = 80877102

# PostgreSQL 的类型 OID
_OID_BY_TYPE = {
    DataType.INT: 23,  # int4
    DataType.BIGINT: 20,  # int8
    DataType.FLOAT: 701,  # float8
    DataType.BOOLEAN: 16,  # bool
    DataType.TEXT: 25,  # text
    DataType.VARCHAR: 1043,  # varchar
}

_TYPLEN_BY_TYPE = {
    DataType.INT: 4,
    DataType.BIGINT: 8,
    DataType.FLOAT: 8,
    DataType.BOOLEAN: 1,
    DataType.TEXT: -1,
    DataType.VARCHAR: -1,
}


@dataclass
class PreparedStatement:
    name: str
    sql: str


@dataclass
class Portal:
    name: str
    statement: PreparedStatement


@dataclass
class Session:
    database: Database
    prepared: dict[str, PreparedStatement] = field(default_factory=dict)
    portals: dict[str, Portal] = field(default_factory=dict)
    #: 扩展协议里"已经执行过、等 Sync 后一起回 ReadyForQuery"的状态
    in_extended: bool = False


class PGWireServer:
    def __init__(self, database: Database, host: str = "127.0.0.1", port: int = 5432) -> None:
        self.database = database
        self.host = host
        self.port = port
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------

    async def serve_forever(self) -> None:
        server = await asyncio.start_server(self._handle, self.host, self.port)
        addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
        print(f"LumenDB 正在监听 {addrs}")
        print(f"用 psql 连接：psql -h {self.host} -p {self.port} -U lumendb -d lumendb")
        async with server:
            await server.serve_forever()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = Session(database=self.database)
        try:
            await self._startup(reader, writer)
            while True:
                try:
                    kind, payload = await self._read_message(reader)
                except ClientDisconnected:
                    break
                keep_going = await self._dispatch(session, writer, kind, payload)
                if not keep_going:
                    break
        except ClientDisconnected:
            pass
        except Exception:  # pragma: no cover - 兜底，避免连接协程把服务器带崩
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 读消息
    # ------------------------------------------------------------------

    async def _startup(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        length = struct.unpack("!i", await reader.readexactly(4))[0]
        if length < 8 or length > 65536:
            raise ProtocolError(f"StartupMessage 长度异常：{length}")
        body = await reader.readexactly(length - 4)
        code = struct.unpack("!i", body[:4])[0]

        if code == SSL_REQUEST_CODE:
            # 我们不做 TLS，回 'N' 让客户端退回明文
            writer.write(b"N")
            await writer.drain()
            return await self._startup(reader, writer)

        if code == CANCEL_REQUEST_CODE:
            raise ClientDisconnected()

        if code != PROTOCOL_VERSION:
            raise ProtocolError(
                f"不支持的协议版本 {code}",
                detail="本服务端只实现 PostgreSQL v3 协议",
            )

        params = _parse_startup_params(body[4:])
        self._client_encoding = params.get("client_encoding", "UTF8")

        self._send(writer, b"R", struct.pack("!i", 0))  # AuthenticationOk
        for key, value in (
            ("server_version", "14.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
        ):
            self._send(writer, b"S", _cstr(key) + _cstr(value))
        self._send(writer, b"K", struct.pack("!ii", 1234, 5678))
        self._send(writer, b"Z", b"I")
        await writer.drain()

    async def _read_message(self, reader: asyncio.StreamReader):
        try:
            kind = await reader.readexactly(1)
            length = struct.unpack("!i", await reader.readexactly(4))[0]
            payload = await reader.readexactly(length - 4)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            raise ClientDisconnected()
        return kind, payload

    def _send(self, writer: asyncio.StreamWriter, kind: bytes, payload: bytes) -> None:
        writer.write(kind + struct.pack("!i", len(payload) + 4) + payload)

    # ------------------------------------------------------------------
    # 分派
    # ------------------------------------------------------------------

    async def _dispatch(self, session: Session, writer, kind: bytes, payload: bytes) -> bool:
        if kind == b"X":  # Terminate
            return False
        if kind == b"Q":
            sql = _decode_cstr(payload)
            await self._simple_query(session, writer, sql)
            return True
        if kind == b"P":
            await self._parse(session, writer, payload)
            return True
        if kind == b"B":
            await self._bind(session, writer, payload)
            return True
        if kind == b"D":
            await self._describe(session, writer, payload)
            return True
        if kind == b"E":
            await self._execute(session, writer, payload)
            return True
        if kind == b"S":  # Sync
            self._send(writer, b"Z", b"I")
            await writer.drain()
            return True
        if kind == b"H":  # Flush
            await writer.drain()
            return True
        if kind == b"C":  # Close
            self._send(writer, b"3", b"")
            return True
        # 其它消息（如 Describe 变体）暂不实现，回 ReadyForQuery 保持连接
        self._send(writer, b"Z", b"I")
        await writer.drain()
        return True

    # -- 简单查询 -------------------------------------------------------

    async def _simple_query(self, session: Session, writer, sql: str) -> None:
        if not sql.strip():
            self._send(writer, b"I", b"")
            self._send(writer, b"Z", b"I")
            await writer.drain()
            return
        async with self._lock:
            try:
                results = session.database.execute(sql)
            except LumenError as exc:
                self._send_error(writer, exc)
                self._send(writer, b"Z", b"I")
                await writer.drain()
                return
            except Exception as exc:  # pragma: no cover
                self._send_error(writer, LumenError(str(exc)))
                self._send(writer, b"Z", b"I")
                await writer.drain()
                return

        for result in results:
            if result.columns:
                self._send_row_description(writer, result.columns)
                for row in result.rows:
                    self._send_data_row(writer, row)
            self._send(writer, b"C", _cstr(result.tag or "OK"))
        self._send(writer, b"Z", b"I")
        await writer.drain()

    # -- 扩展查询 -------------------------------------------------------

    async def _parse(self, session: Session, writer, payload: bytes) -> None:
        name, rest = _split_cstr(payload)
        sql, rest = _split_cstr(rest)
        nparams = struct.unpack("!h", rest[:2])[0]
        if nparams > 0:
            self._send_error(
                writer,
                NotSupportedError(
                    "扩展协议里的参数占位符尚未实现",
                    hint="用简单查询（psql / DBeaver 默认）执行带参数之外的语句",
                ),
            )
            return
        session.prepared[name] = PreparedStatement(name=name, sql=sql)
        self._send(writer, b"1", b"")  # ParseComplete

    async def _bind(self, session: Session, writer, payload: bytes) -> None:
        portal_name, rest = _split_cstr(payload)
        stmt_name, rest = _split_cstr(rest)
        stmt = session.prepared.get(stmt_name)
        if stmt is None:
            self._send_error(writer, ProtocolError(f"未找到预处理语句 {stmt_name!r}"))
            return
        session.portals[portal_name] = Portal(name=portal_name, statement=stmt)
        self._send(writer, b"2", b"")  # BindComplete

    async def _describe(self, session: Session, writer, payload: bytes) -> None:
        kind = payload[:1]
        name, _ = _split_cstr(payload[1:])
        if kind == b"S":
            stmt = session.prepared.get(name)
            if stmt is None:
                self._send_error(writer, ProtocolError(f"未找到预处理语句 {name!r}"))
                return
            # 参数描述：0 个参数
            self._send(writer, b"t", struct.pack("!h", 0))
            return
        portal = session.portals.get(name)
        if portal is None:
            self._send_error(writer, ProtocolError(f"未找到门户 {name!r}"))
            return
        # 执行一次拿到列信息（结果集很小，代价可接受）
        async with self._lock:
            try:
                result = session.database.execute_one(portal.statement.sql)
            except LumenError as exc:
                self._send_error(writer, exc)
                return
        if result.columns:
            self._send_row_description(writer, result.columns, include_header=True)
        else:
            self._send(writer, b"n", b"")  # NoData

    async def _execute(self, session: Session, writer, payload: bytes) -> None:
        portal_name, rest = _split_cstr(payload)
        portal = session.portals.get(portal_name)
        if portal is None:
            self._send_error(writer, ProtocolError(f"未找到门户 {portal_name!r}"))
            return
        async with self._lock:
            try:
                result = session.database.execute_one(portal.statement.sql)
            except LumenError as exc:
                self._send_error(writer, exc)
                return
        if result.columns:
            for row in result.rows:
                self._send_data_row(writer, row)
        self._send(writer, b"C", _cstr(result.tag or "OK"))

    # -- 结果与错误 -----------------------------------------------------

    def _send_row_description(
        self, writer, columns: list[ColumnMeta], include_header: bool = False
    ) -> None:
        parts = [struct.pack("!h", len(columns))]
        for col in columns:
            oid = _OID_BY_TYPE.get(col.type.kind, 25)
            typlen = _TYPLEN_BY_TYPE.get(col.type.kind, -1)
            parts.append(
                _cstr(col.name)
                + struct.pack("!IhIhih", 0, 0, oid, typlen, -1, 0)
            )
        self._send(writer, b"T", b"".join(parts))

    def _send_data_row(self, writer, row: tuple) -> None:
        parts = [struct.pack("!h", len(row))]
        for value in row:
            if value is None:
                parts.append(struct.pack("!i", -1))
            else:
                text = _format_value(value).encode("utf-8")
                parts.append(struct.pack("!i", len(text)) + text)
        self._send(writer, b"D", b"".join(parts))

    def _send_error(self, writer, exc: LumenError) -> None:
        fields = [b"S" + _cstr("ERROR"), b"V" + _cstr("ERROR")]
        fields.append(b"C" + _cstr(exc.sqlstate))
        fields.append(b"M" + _cstr(exc.message))
        if exc.detail:
            fields.append(b"D" + _cstr(exc.detail))
        if exc.hint:
            fields.append(b"H" + _cstr(exc.hint))
        self._send(writer, b"E", b"".join(fields) + b"\x00")


# ---------------------------------------------------------------------------
# 编解码辅助
# ---------------------------------------------------------------------------


def _cstr(text: str) -> bytes:
    return text.encode("utf-8") + b"\x00"


def _decode_cstr(payload: bytes) -> str:
    return payload.rstrip(b"\x00").decode("utf-8", errors="replace")


def _split_cstr(payload: bytes) -> tuple[str, bytes]:
    index = payload.find(b"\x00")
    if index < 0:
        return _decode_cstr(payload), b""
    return payload[:index].decode("utf-8", errors="replace"), payload[index + 1 :]


def _parse_startup_params(body: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    for chunk in body.split(b"\x00"):
        if not chunk:
            continue
        key, _, value = chunk.partition(b"\x00") if b"\x00" in chunk else (chunk, b"", b"")
        result[key.decode("utf-8", "replace")] = value.decode("utf-8", "replace")
    return result


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float) and value == int(value):
        return str(int(value)) if abs(value) < 1e16 else repr(value)
    return str(value)


__all__ = ["PGWireServer", "Session"]
