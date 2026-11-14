"""极简 TCP 物理 WAL 流。

协议是换行分隔的 JSON，所有二进制内容用 base64：

客户端 -> 服务端
    {"type": "backup"}
    {"type": "replicate", "start_lsn": N, "max_records": M}

服务端 -> 客户端
    {"type": "backup", "checkpoint_lsn": N, "data": "<base64>"}
    {"type": "record", "lsn": N, "xid": N, "rtype": N, "payload": "<base64>"}
    {"type": "end"}
    {"type": "error", "message": "..."}

这是可替换的传输层：``Replica`` 只依赖“按 LSN 递增拿到 LogRecord”的语义，
以后换成二进制帧、压缩或 TLS 都不需要改物理重放逻辑。
"""

from __future__ import annotations

import base64
import json
import os
import socket
import tempfile
import threading
from pathlib import Path
from typing import Iterable

from ..errors import StorageError, TransactionError
from ..storage.wal import LogRecord


def _send_line(sock: socket.socket, payload: dict) -> None:
    sock.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))


def _record_to_json(record: LogRecord) -> dict:
    return {
        "type": "record",
        "lsn": record.lsn,
        "xid": record.xid,
        "rtype": int(record.rtype),
        "payload": base64.b64encode(record.payload).decode("ascii"),
    }


def read_slot(path: str | Path) -> int:
    """读取复制槽的 ``confirmed_lsn``；不存在或损坏时返回 0。"""
    slot = Path(path)
    if not slot.exists():
        return 0
    try:
        return int(slot.read_text().strip() or "0")
    except (OSError, ValueError):
        return 0


def _record_from_json(data: dict) -> LogRecord:
    return LogRecord(
        lsn=int(data["lsn"]),
        xid=int(data["xid"]),
        rtype=int(data["rtype"]),
        payload=base64.b64decode(data["payload"]),
    )


class WALServer:
    """把主库 WAL 暴露成只读 TCP 流。"""

    def __init__(
        self,
        primary,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        slot_path: str | Path | None = None,
        token: str | None = None,
    ) -> None:
        self.primary = primary
        self.token = token
        self.slot_path = Path(slot_path) if slot_path is not None else None
        self._confirmed_lsn = 0
        if self.slot_path is not None:
            self._confirmed_lsn = read_slot(self.slot_path)
            retain = getattr(primary.wal, "retain_lsn", None)
            if retain is not None and self._confirmed_lsn:
                primary.wal.retain_lsn = self._confirmed_lsn
        self._stop = threading.Event()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self.host, self.port = self._sock.getsockname()
        self._thread: threading.Thread | None = None
        self._handlers: list[threading.Thread] = []

    def start(self) -> "WALServer":
        self._thread = threading.Thread(
            target=self._accept_loop, name="lumendb-wal-server", daemon=True
        )
        self._thread.start()
        return self

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(10)
            handler = threading.Thread(target=self._handle, args=(conn,), daemon=True)
            self._handlers.append(handler)
            handler.start()

    def _handle(self, conn: socket.socket) -> None:
        try:
            line = conn.makefile("rb").readline()
            if not line:
                return
            request = json.loads(line.decode("utf-8"))
            if self.token is not None and request.get("token") != self.token:
                _send_line(conn, {"type": "error", "message": "复制流认证失败"})
                return
            kind = request.get("type")
            if kind == "backup":
                self._handle_backup(conn)
            elif kind == "backup_stream":
                self._handle_backup_stream(conn, request)
            elif kind == "replicate":
                self._handle_replicate(conn, request)
            elif kind == "ack":
                self._handle_ack(conn, request)
            elif kind == "status":
                self._handle_status(conn)
            else:
                _send_line(conn, {"type": "error", "message": "未知复制请求"})
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            try:
                _send_line(conn, {"type": "error", "message": str(exc)})
            except OSError:
                pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle_backup(self, conn: socket.socket) -> None:
        try:
            data, checkpoint_lsn = self.primary.snapshot()
        except TransactionError as exc:
            _send_line(conn, {"type": "error", "message": str(exc)})
            return
        retain = getattr(self.primary.wal, "retain_lsn", None)
        if retain is not None:
            self.primary.wal.retain_lsn = checkpoint_lsn
        _send_line(
            conn,
            {
                "type": "backup",
                "checkpoint_lsn": checkpoint_lsn,
                "data": base64.b64encode(data).decode("ascii"),
            },
        )

    def _handle_backup_stream(self, conn: socket.socket, request: dict) -> None:
        chunk_size = max(4096, min(int(request.get("chunk_size", 256 * 1024)), 8 * 1024 * 1024))
        fd, temp_name = tempfile.mkstemp(prefix="lumendb-backup-", suffix=".data")
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            try:
                checkpoint_lsn = self.primary.snapshot_file(temp_path)
            except TransactionError as exc:
                _send_line(conn, {"type": "error", "message": str(exc)})
                return
            retain = getattr(self.primary.wal, "retain_lsn", None)
            if retain is not None:
                self.primary.wal.retain_lsn = checkpoint_lsn
            with open(temp_path, "rb") as fh:
                while True:
                    chunk = fh.read(chunk_size)
                    if not chunk:
                        break
                    _send_line(
                        conn,
                        {
                            "type": "backup_chunk",
                            "data": base64.b64encode(chunk).decode("ascii"),
                        },
                    )
            _send_line(
                conn, {"type": "backup_end", "checkpoint_lsn": checkpoint_lsn}
            )
        finally:
            try:
                temp_path.unlink()
            except OSError:
                pass

    def _handle_replicate(self, conn: socket.socket, request: dict) -> None:
        start_lsn = int(request.get("start_lsn", 0))
        max_records = max(0, int(request.get("max_records", 1000)))
        base_lsn = getattr(self.primary.wal, "base_lsn", 0)
        if base_lsn and start_lsn + 1 < base_lsn:
            _send_line(
                conn,
                {
                    "type": "error",
                    "message": "副本落后于主库已回收的 WAL，需要重新基准备份",
                },
            )
            return
        retain = getattr(self.primary.wal, "retain_lsn", None)
        if retain is not None and start_lsn > 0:
            self.primary.wal.retain_lsn = start_lsn

        count = 0
        for record in self.primary.wal.iter_records(start_lsn + 1):
            _send_line(conn, _record_to_json(record))
            count += 1
            if max_records and count >= max_records:
                break
        _send_line(conn, {"type": "end", "next_lsn": self.primary.wal.next_lsn})

    def _handle_status(self, conn: socket.socket) -> None:
        wal = self.primary.wal
        _send_line(
            conn,
            {
                "type": "status",
                "current_lsn": int(getattr(wal, "last_lsn", 0)),
                "base_lsn": getattr(wal, "base_lsn", 0),
                "confirmed_lsn": self._confirmed_lsn,
            },
        )

    def _handle_ack(self, conn: socket.socket, request: dict) -> None:
        lsn = max(0, int(request.get("lsn", 0)))
        if lsn > self._confirmed_lsn:
            self._confirmed_lsn = lsn
            retain = getattr(self.primary.wal, "retain_lsn", None)
            if retain is not None:
                self.primary.wal.retain_lsn = lsn
            if self.slot_path is not None:
                tmp = self.slot_path.with_name(self.slot_path.name + ".tmp")
                tmp.write_text(str(lsn), encoding="utf-8")
                os.replace(tmp, self.slot_path)
        _send_line(conn, {"type": "ack", "confirmed_lsn": self._confirmed_lsn})

    def stop(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        if self._thread is not None:
            self._thread.join(timeout=2)
        for handler in list(self._handlers):
            handler.join(timeout=1)


class WALClient:
    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 10.0,
        *,
        token: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.token = token

    def _request(self, payload: dict) -> Iterable[dict]:
        if self.token is not None:
            payload = dict(payload)
            payload["token"] = self.token
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        try:
            sock.settimeout(self.timeout)
            _send_line(sock, payload)
            file = sock.makefile("rb")
            for line in file:
                data = json.loads(line.decode("utf-8"))
                if data.get("type") == "error":
                    raise StorageError(data.get("message", "复制请求失败"))
                yield data
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def backup_to(self, target: str | Path, *, chunk_size: int = 256 * 1024) -> int:
        """分块接收基准备份并落盘，返回基础 checkpoint LSN。"""
        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".part")
        try:
            checkpoint_lsn = None
            with open(tmp, "wb") as fh:
                for data in self._request(
                    {"type": "backup_stream", "chunk_size": chunk_size}
                ):
                    if data.get("type") == "backup_chunk":
                        fh.write(base64.b64decode(data["data"]))
                    elif data.get("type") == "backup_end":
                        checkpoint_lsn = int(data["checkpoint_lsn"])
                        break
            if checkpoint_lsn is None:
                raise StorageError("基准备份流没有结束标记")
            os.replace(tmp, target)
            return checkpoint_lsn
        except Exception:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            raise

    def backup(self) -> tuple[bytes, int]:
        for data in self._request({"type": "backup"}):
            if data.get("type") == "backup":
                return (
                    base64.b64decode(data["data"]),
                    int(data["checkpoint_lsn"]),
                )
        raise StorageError("主库没有返回基准备份")

    def ack(self, lsn: int) -> int:
        for data in self._request({"type": "ack", "lsn": lsn}):
            if data.get("type") == "ack":
                return int(data["confirmed_lsn"])
        return 0

    def status(self) -> dict:
        """读取主库当前 LSN、WAL 基础 LSN 和槽位确认 LSN。"""
        for data in self._request({"type": "status"}):
            if data.get("type") == "status":
                return {
                    "current_lsn": int(data.get("current_lsn", 0)),
                    "base_lsn": int(data.get("base_lsn", 0)),
                    "confirmed_lsn": int(data.get("confirmed_lsn", 0)),
                }
        return {"current_lsn": 0, "base_lsn": 0, "confirmed_lsn": 0}

    def fetch(self, start_lsn: int, max_records: int = 1000) -> list[LogRecord]:
        records: list[LogRecord] = []
        for data in self._request(
            {"type": "replicate", "start_lsn": start_lsn, "max_records": max_records}
        ):
            if data.get("type") == "record":
                records.append(_record_from_json(data))
            elif data.get("type") == "end":
                break
        return records
