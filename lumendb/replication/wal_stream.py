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
以后换成二进制帧或 TLS 都不需要改物理重放逻辑。

``WALClient(compress=True)`` 会让请求带 ``compress`` 标志；服务端把每一行
响应变成 ``z<base64(zlib(json))>``，客户端按首字节（``{`` 或 ``z``）解码。
压缩只作用于响应，请求仍然很小；它减少基准备份和 WAL 页数据的网络字节，
不改变 LSN/记录语义。
"""

from __future__ import annotations

import base64
import json
import os
import re
import socket
import ssl
import tempfile
import threading
import zlib
from pathlib import Path
from typing import Iterable

from ..errors import RedirectError, StorageError, TransactionError
from ..storage.wal import LogRecord


def _encode_line(payload: dict, compress: bool = False) -> bytes:
    raw = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
    if not compress:
        return raw
    # 压缩行以 ``z`` 开头；未压缩行以 ``{`` 开头，客户端据首字节区分。
    body = base64.b64encode(zlib.compress(raw[:-1])).decode("ascii")
    return ("z" + body + "\n").encode("ascii")


def _decode_line(line: bytes) -> dict:
    line = line.strip()
    if line.startswith(b"{"):
        return json.loads(line.decode("utf-8"))
    if line.startswith(b"z"):
        raw = zlib.decompress(base64.b64decode(line[1:]))
        return json.loads(raw.decode("utf-8"))
    raise ValueError("无法识别的复制流行格式")


def _send_line(
    sock: socket.socket, payload: dict, *, compress: bool = False
) -> None:
    sock.sendall(_encode_line(payload, compress=compress))


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
        slot_dir: str | Path | None = None,
        token: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        redirect_to: tuple[str, int] | None = None,
    ) -> None:
        if slot_path is not None and slot_dir is not None:
            raise ValueError("slot_path 与 slot_dir 只能配置一个")
        self.primary = primary
        self.token = token
        self.ssl_context = ssl_context
        self.redirect_to = redirect_to
        self.slot_path = Path(slot_path) if slot_path is not None else None
        self.slot_dir = Path(slot_dir) if slot_dir is not None else None
        self._confirmed_lsn = 0
        self._replica_slots: dict[str, int] = {}
        self._slot_lock = threading.Lock()
        if self.slot_dir is not None:
            self.slot_dir.mkdir(parents=True, exist_ok=True)
            for path in self.slot_dir.glob("*.confirmed"):
                self._replica_slots[path.name[: -len(".confirmed")]] = read_slot(path)
            self._refresh_retain()
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

    def _replica_id(self, request: dict) -> str | None:
        raw = request.get("replica_id")
        if raw is None:
            return None
        replica_id = str(raw)
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", replica_id):
            raise StorageError("replica_id 只能包含字母、数字、下划线、点和短横线")
        return replica_id

    def _slot_confirm(self, replica_id: str | None) -> int:
        if replica_id is None:
            return self._confirmed_lsn
        with self._slot_lock:
            return self._replica_slots.get(replica_id, 0)

    def _refresh_retain(self) -> None:
        """多副本时保留点取所有已知槽位确认 LSN 的最小值。"""
        retain = getattr(self.primary.wal, "retain_lsn", None)
        if retain is None:
            return
        with self._slot_lock:
            if self._replica_slots:
                self.primary.wal.retain_lsn = min(self._replica_slots.values())
            else:
                self.primary.wal.retain_lsn = 0

    def list_slots(self) -> dict[str, int]:
        with self._slot_lock:
            return dict(self._replica_slots)

    def drop_slot(self, replica_id: str) -> bool:
        """删除一个确认槽位；副本退役时使用，调用方自行确认它不再需要 WAL。"""
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", replica_id):
            raise StorageError("replica_id 只能包含字母、数字、下划线、点和短横线")
        with self._slot_lock:
            existed = replica_id in self._replica_slots
            self._replica_slots.pop(replica_id, None)
            path = self.slot_dir / f"{replica_id}.confirmed" if self.slot_dir else None
        if path is not None:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        self._refresh_retain()
        return existed

    def _persist_replica_slot(self, replica_id: str, lsn: int) -> None:
        if self.slot_dir is None:
            return
        target = self.slot_dir / f"{replica_id}.confirmed"
        tmp = target.with_name(target.name + ".tmp")
        tmp.write_text(str(lsn), encoding="utf-8")
        os.replace(tmp, target)

    def _record_replica_ack(self, replica_id: str, lsn: int) -> int:
        lsn = max(0, int(lsn))
        with self._slot_lock:
            if lsn > self._replica_slots.get(replica_id, 0):
                self._replica_slots[replica_id] = lsn
                self._persist_replica_slot(replica_id, lsn)
        self._refresh_retain()
        return self._slot_confirm(replica_id)

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
            if self.ssl_context is not None:
                try:
                    conn = self.ssl_context.wrap_socket(conn, server_side=True)
                except (ssl.SSLError, OSError):
                    try:
                        conn.close()
                    except OSError:
                        pass
                    continue
            conn.settimeout(10)
            handler = threading.Thread(target=self._handle, args=(conn,), daemon=True)
            self._handlers.append(handler)
            handler.start()

    def _handle(self, conn: socket.socket) -> None:
        compress = False
        try:
            line = conn.makefile("rb").readline()
            if not line:
                return
            request = _decode_line(line)
            compress = bool(request.get("compress"))
            if self.token is not None and request.get("token") != self.token:
                _send_line(
                    conn,
                    {"type": "error", "message": "复制流认证失败"},
                    compress=compress,
                )
                return
            if self.redirect_to is not None:
                _send_line(
                    conn,
                    {
                        "type": "redirect",
                        "host": self.redirect_to[0],
                        "port": int(self.redirect_to[1]),
                    },
                    compress=compress,
                )
                return
            kind = request.get("type")
            if kind == "backup":
                self._handle_backup(conn, request)
            elif kind == "backup_stream":
                self._handle_backup_stream(conn, request)
            elif kind == "incremental":
                self._handle_incremental(conn, request)
            elif kind == "replicate":
                self._handle_replicate(conn, request)
            elif kind == "ack":
                self._handle_ack(conn, request)
            elif kind == "status":
                self._handle_status(conn, request)
            else:
                _send_line(
                    conn,
                    {"type": "error", "message": "未知复制请求"},
                    compress=compress,
                )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            try:
                _send_line(
                    conn, {"type": "error", "message": str(exc)}, compress=compress
                )
            except OSError:
                pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle_backup(self, conn: socket.socket, request: dict) -> None:
        compress = bool(request.get("compress"))
        try:
            data, checkpoint_lsn = self.primary.snapshot()
        except TransactionError as exc:
            _send_line(conn, {"type": "error", "message": str(exc)}, compress=compress)
            return
        replica_id = self._replica_id(request)
        if replica_id is not None:
            self._record_replica_ack(replica_id, checkpoint_lsn)
        retain = getattr(self.primary.wal, "retain_lsn", None)
        if retain is not None and self.slot_dir is None:
            self.primary.wal.retain_lsn = checkpoint_lsn
        _send_line(
            conn,
            {
                "type": "backup",
                "checkpoint_lsn": checkpoint_lsn,
                "data": base64.b64encode(data).decode("ascii"),
            },
            compress=compress,
        )

    def _handle_backup_stream(self, conn: socket.socket, request: dict) -> None:
        compress = bool(request.get("compress"))
        chunk_size = max(4096, min(int(request.get("chunk_size", 256 * 1024)), 8 * 1024 * 1024))
        fd, temp_name = tempfile.mkstemp(prefix="lumendb-backup-", suffix=".data")
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            try:
                checkpoint_lsn = self.primary.snapshot_file(temp_path)
            except TransactionError as exc:
                _send_line(conn, {"type": "error", "message": str(exc)}, compress=compress)
                return
            replica_id = self._replica_id(request)
            if replica_id is not None:
                self._record_replica_ack(replica_id, checkpoint_lsn)
            retain = getattr(self.primary.wal, "retain_lsn", None)
            if retain is not None and self.slot_dir is None:
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
                        compress=compress,
                    )
            _send_line(
                conn,
                {"type": "backup_end", "checkpoint_lsn": checkpoint_lsn},
                compress=compress,
            )
        finally:
            try:
                temp_path.unlink()
            except OSError:
                pass

    def _handle_incremental(self, conn: socket.socket, request: dict) -> None:
        """页级增量基准备份：只传 pageLSN > base_lsn 的页。"""
        compress = bool(request.get("compress"))
        base_lsn = max(0, int(request.get("base_lsn", 0)))
        fd, temp_name = tempfile.mkstemp(prefix="lumendb-pages-", suffix="")
        os.close(fd)
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        temp_dir = Path(temp_name + ".dir")
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            try:
                checkpoint_lsn, count = self.primary.snapshot_pages(
                    base_lsn, temp_dir
                )
            except TransactionError as exc:
                _send_line(conn, {"type": "error", "message": str(exc)}, compress=compress)
                return
            _send_line(
                conn,
                {
                    "type": "incremental_begin",
                    "checkpoint_lsn": checkpoint_lsn,
                    "page_count": count,
                },
                compress=compress,
            )
            for path in sorted(temp_dir.glob("*.page")):
                page_id = int(path.stem)
                _send_line(
                    conn,
                    {
                        "type": "page",
                        "page_id": page_id,
                        "data": base64.b64encode(path.read_bytes()).decode("ascii"),
                    },
                    compress=compress,
                )
            _send_line(
                conn,
                {"type": "incremental_end", "checkpoint_lsn": checkpoint_lsn},
                compress=compress,
            )
        finally:
            for path in list(temp_dir.glob("*")):
                try:
                    path.unlink()
                except OSError:
                    pass
            try:
                temp_dir.rmdir()
            except OSError:
                pass

    def _handle_replicate(self, conn: socket.socket, request: dict) -> None:
        compress = bool(request.get("compress"))
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
                compress=compress,
            )
            return
        retain = getattr(self.primary.wal, "retain_lsn", None)
        if retain is not None and self.slot_dir is None and start_lsn > 0:
            self.primary.wal.retain_lsn = start_lsn
        self._refresh_retain()

        count = 0
        for record in self.primary.wal.iter_records(start_lsn + 1):
            _send_line(conn, _record_to_json(record), compress=compress)
            count += 1
            if max_records and count >= max_records:
                break
        _send_line(
            conn,
            {"type": "end", "next_lsn": self.primary.wal.next_lsn},
            compress=compress,
        )

    def _handle_status(self, conn: socket.socket, request: dict) -> None:
        wal = self.primary.wal
        replica_id = self._replica_id(request)
        _send_line(
            conn,
            {
                "type": "status",
                "current_lsn": int(getattr(wal, "last_lsn", 0)),
                "base_lsn": getattr(wal, "base_lsn", 0),
                "confirmed_lsn": self._slot_confirm(replica_id),
                "replica_id": replica_id,
            },
            compress=bool(request.get("compress")),
        )

    def _handle_ack(self, conn: socket.socket, request: dict) -> None:
        compress = bool(request.get("compress"))
        replica_id = self._replica_id(request)
        lsn = max(0, int(request.get("lsn", 0)))
        if replica_id is None:
            if lsn > self._confirmed_lsn:
                self._confirmed_lsn = lsn
                retain = getattr(self.primary.wal, "retain_lsn", None)
                if retain is not None:
                    self.primary.wal.retain_lsn = lsn
                if self.slot_path is not None:
                    tmp = self.slot_path.with_name(self.slot_path.name + ".tmp")
                    tmp.write_text(str(lsn), encoding="utf-8")
                    os.replace(tmp, self.slot_path)
            confirmed = self._confirmed_lsn
        else:
            confirmed = self._record_replica_ack(replica_id, lsn)
        _send_line(
            conn,
            {"type": "ack", "confirmed_lsn": confirmed, "replica_id": replica_id},
            compress=compress,
        )

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
        compress: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.token = token
        self.compress = compress
        self.ssl_context = ssl_context
        self.server_hostname = server_hostname or host
        self.replica_id = replica_id

    def _request(self, payload: dict) -> Iterable[dict]:
        payload = dict(payload)
        if self.token is not None:
            payload["token"] = self.token
        if self.compress:
            payload["compress"] = True
        if self.replica_id is not None:
            payload["replica_id"] = self.replica_id
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        try:
            if self.ssl_context is not None:
                sock = self.ssl_context.wrap_socket(
                    sock, server_hostname=self.server_hostname
                )
            sock.settimeout(self.timeout)
            _send_line(sock, payload)
            file = sock.makefile("rb")
            for line in file:
                data = _decode_line(line)
                if data.get("type") == "error":
                    raise StorageError(data.get("message", "复制请求失败"))
                if data.get("type") == "redirect":
                    raise RedirectError(
                        str(data.get("host", "")), int(data.get("port", 0))
                    )
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

    def incremental_to(
        self, target_dir: str | Path, base_lsn: int
    ) -> tuple[int, int]:
        """接收页级增量备份，返回 ``(checkpoint_lsn, page_count)``。

        只写回传输来的页文件；调用方负责在副本数据库关闭时把它们落回
        数据文件，然后重新打开数据库。
        """
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        checkpoint_lsn = None
        expected_count = None
        page_count = 0
        for data in self._request(
            {"type": "incremental", "base_lsn": int(base_lsn)}
        ):
            if data.get("type") == "incremental_begin":
                checkpoint_lsn = int(data["checkpoint_lsn"])
                expected_count = int(data.get("page_count", 0))
            elif data.get("type") == "page":
                page_id = int(data["page_id"])
                (target / f"{page_id:08d}.page").write_bytes(
                    base64.b64decode(data["data"])
                )
                page_count += 1
            elif data.get("type") == "incremental_end":
                checkpoint_lsn = int(data["checkpoint_lsn"])
                break
        if checkpoint_lsn is None:
            raise StorageError("增量基准备份缺少结束标记")
        if expected_count is not None and page_count != expected_count:
            raise StorageError(
                "增量基准备份页数不完整",
                detail=f"收到 {page_count} 页，期望 {expected_count} 页",
            )
        return checkpoint_lsn, page_count

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
