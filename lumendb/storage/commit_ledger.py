"""跨 checkpoint 的持久提交账本。

``SHOW COMMIT LOG`` 与 ``AS OF SYSTEM TIME`` 都需要 ``xid -> COMMIT LSN``。
WAL 在 checkpoint 后会裁掉旧段，只靠恢复扫描会丢掉 checkpoint 之前的提交；
本对象把账本单独落成一个文件，让这两条读路径不依赖 WAL 的保留窗口。

文件是定长二进制格式（页大小无关）：头部记录魔数、版本与条目数，正文按
LSN 升序排列 ``xid, commit_lsn``。每次 checkpoint 在推进元数据页里的
``checkpoint_lsn`` **之前**原子重写该文件；写入失败就不推进 checkpoint，
WAL 仍保留可恢复的 COMMIT 记录。这样不会出现“元数据说已 checkpoint，但
账本和 WAL 都没有旧提交”的窗口。

账本只保留最近 ``max_entries`` 条提交，避免长跑库无限增长。更早的提交在
目标点不早于历史下界时可以由“水位前已提交”假设覆盖；超出保留窗口的
未知 ``xmax`` 仍走严格报错，不猜已删除版本的状态。
"""

from __future__ import annotations

import os
import struct
import threading
from pathlib import Path
from typing import Iterable

from ..errors import StorageError

_MAGIC = b"LUMENCGL"
_VERSION = 1
#: magic(8) + version(2) + count(8)
_HEADER = struct.Struct("<8sHQ")
_ENTRY = struct.Struct("<QQ")


class CommitLedger:
    """磁盘上的提交 LSN 账本；读写都按 LSN 升序。"""

    def __init__(
        self,
        path: str | Path,
        *,
        max_entries: int = 100_000,
        read_only: bool = False,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("commit ledger 的 max_entries 必须是正整数")
        self.path = Path(path)
        self.max_entries = int(max_entries)
        self.read_only = bool(read_only)
        self._lock = threading.Lock()
        self._entries = self._load()

    # -- 读 -----------------------------------------------------------------

    def _load(self) -> list[tuple[int, int]]:
        if not self.path.exists():
            return []
        data = self.path.read_bytes()
        if len(data) < _HEADER.size:
            raise StorageError(
                "提交账本文件被截断",
                detail=f"{self.path} 只有 {len(data)} 字节",
                hint="从备份恢复该文件，或删除后让 WAL 重新构建账本",
            )
        magic, version, count = _HEADER.unpack_from(data, 0)
        if magic != _MAGIC:
            raise StorageError(
                "提交账本文件魔数不匹配",
                detail=f"{self.path} magic={magic!r}",
                hint="确认数据目录没有被别的数据库文件覆盖",
            )
        if version != _VERSION:
            raise StorageError(
                f"不支持的提交账本版本 {version}",
                detail=f"当前支持版本 {_VERSION}",
            )
        expected = _HEADER.size + count * _ENTRY.size
        if len(data) != expected:
            raise StorageError(
                "提交账本条目数与文件长度不符",
                detail=f"count={count} expected={expected} actual={len(data)}",
                hint="检查磁盘是否损坏，或从备份恢复",
            )
        entries: list[tuple[int, int]] = []
        pos = _HEADER.size
        for _ in range(count):
            xid, lsn = _ENTRY.unpack_from(data, pos)
            pos += _ENTRY.size
            if xid <= 0 or lsn <= 0:
                raise StorageError(
                    "提交账本包含非法条目",
                    detail=f"xid={xid} lsn={lsn}",
                )
            entries.append((int(xid), int(lsn)))
        return entries

    def entries(self) -> list[tuple[int, int]]:
        """返回当前账本中的 ``(xid, commit_lsn)``，按 LSN 升序。"""
        with self._lock:
            return list(self._entries)

    # -- 写 -----------------------------------------------------------------

    def _compact(
        self, entries: Iterable[tuple[int, int]] | dict[int, int]
    ) -> list[tuple[int, int]]:
        by_xid: dict[int, int] = {}
        items = entries.items() if isinstance(entries, dict) else entries
        for xid, lsn in items:
            xid = int(xid)
            lsn = int(lsn)
            if xid <= 0 or lsn <= 0:
                continue
            previous = by_xid.get(xid)
            if previous is None or lsn > previous:
                by_xid[xid] = lsn
        ordered = sorted(by_xid.items(), key=lambda item: (item[1], item[0]))
        return ordered[-self.max_entries :]

    def _encode(self, entries: Iterable[tuple[int, int]]) -> bytes:
        body = bytearray(_HEADER.pack(_MAGIC, _VERSION, len(entries)))
        for xid, lsn in entries:
            body.extend(_ENTRY.pack(xid, lsn))
        return bytes(body)

    def to_bytes(self, max_lsn: int | None = None) -> bytes:
        """导出当前账本的完整二进制，供基准备份/增量备份携带。

        ``max_lsn`` 用于把账本裁剪到数据文件对应的 checkpoint：并发
        checkpoint 可能在数据文件快照之后又写入更新的提交点，不能把
        这些“账本领先数据”的条目发给副本。
        """
        with self._lock:
            entries = self._entries
            if max_lsn is not None:
                limit = int(max_lsn)
                entries = [item for item in entries if item[1] <= limit]
            return self._encode(entries)

    def write(
        self, entries: Iterable[tuple[int, int]] | dict[int, int]
    ) -> list[tuple[int, int]]:
        """原子重写账本并 fsync；返回实际保留的条目。

        先写同目录临时文件，再 ``os.replace``，最后 fsync 目录。这样崩溃
        不会留下半截账本；旧文件在新文件就位前一直可读。
        """
        if self.read_only:
            return self.entries()
        retained = self._compact(entries)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            body = self._encode(retained)
            temp = self.path.with_name(self.path.name + ".tmp")
            with open(temp, "w+b") as fh:
                fh.write(body)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temp, self.path)
            self._fsync_dir()
            self._entries = retained
            return list(retained)

    def replace(self, entries: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
        """同 :meth:`write`；命名给“整表替换”语义的调用方。"""
        return self.write(entries)

    def _fsync_dir(self) -> None:
        """POSIX 上把目录项也落盘；Windows 不支持打开目录，忽略即可。"""
        try:
            fd = os.open(str(self.path.parent), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)


__all__ = ["CommitLedger"]
