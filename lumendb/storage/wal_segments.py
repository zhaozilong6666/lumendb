"""分段 WAL。

单文件 WAL 的 LSN 直接等于文件偏移 + 1，简单但无法回收：文件只会变长。
分段 WAL 保持同样的“LSN 是逻辑字节偏移 + 1”语义，把日志拆成多个段文件：

    wal-00000000000000000001.log
    wal-00000000000000001234.log

文件名里的数字是该段第一条记录的 LSN。记录本身仍然连续追加，所以重启后
从最后一条记录就能恢复 ``next_lsn``，不需要额外的映射表。

回收发生在 checkpoint 之后：所有脏页已经落盘，已提交事务的 redo 信息不再
需要；只有活跃事务的 before image 还必须保留，用于崩溃后的 undo。因此
``truncate_before`` 只删除“最后一条记录 LSN 小于安全 LSN”的整段，绝不拆段、
不改写已有记录的 LSN。这样 pageLSN 比较仍然成立，恢复也可以继续从
``checkpoint_lsn`` 之后开始。
"""

from __future__ import annotations

import os
import re
import threading
from pathlib import Path
from typing import Iterator

from ..errors import StorageError
from .wal import HEADER_SIZE, LogRecord, WAL

_SEGMENT_RE = re.compile(r"^wal-(\d{20})\.log$")


def _segment_path(directory: Path, first_lsn: int) -> Path:
    return directory / f"wal-{first_lsn:020d}.log"


def _read_segment(path: Path) -> tuple[list[LogRecord], int]:
    """读取一个段，返回 (完整记录, 有效字节数)。

    尾部不完整的记录来自崩溃时写到一半的 append。调用方可以据此截掉残尾，
    但不能把它当作一条合法日志。
    """
    data = path.read_bytes()
    records: list[LogRecord] = []
    pos = 0
    while pos + HEADER_SIZE <= len(data):
        try:
            record, next_pos = LogRecord.decode(data, pos)
        except StorageError:
            break
        records.append(record)
        pos = next_pos
    return records, pos


class SegmentedWAL(WAL):
    """WAL 的分段实现，接口与 :class:`WAL` 保持一致。"""

    def __init__(self, path: str | Path, *, segment_bytes: int = 1024 * 1024) -> None:
        if segment_bytes <= 0:
            raise ValueError("segment_bytes 必须大于 0")
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.segment_bytes = segment_bytes
        self._lock = threading.RLock()
        self._pending: list[tuple[int, bytes]] = []
        self._fh = None
        self._current_path: Path | None = None
        self._current_size = 0
        self._size = 0
        self._next_lsn = 1

        files = self._discover()
        if files:
            last = files[-1]
            records, valid_end = _read_segment(last)
            file_size = last.stat().st_size
            if records:
                if valid_end < file_size:
                    with open(last, "r+b") as fh:
                        fh.truncate(valid_end)
                self._size = records[-1].lsn
                self._next_lsn = records[-1].lsn + len(records[-1].encode())
                self._fh = open(last, "ab")
                self._current_path = last
                self._current_size = valid_end
            else:
                # 崩溃发生在段的第一条记录上，整个段没有可用日志。
                last.unlink()
                files = files[:-1]
                if files:
                    last = files[-1]
                    records, valid_end = _read_segment(last)
                    if records:
                        if valid_end < last.stat().st_size:
                            with open(last, "r+b") as fh:
                                fh.truncate(valid_end)
                        self._size = records[-1].lsn
                        self._next_lsn = records[-1].lsn + len(records[-1].encode())
                        self._fh = open(last, "ab")
                        self._current_path = last
                        self._current_size = valid_end

    # -- 段文件 -------------------------------------------------------------

    def _discover(self) -> list[Path]:
        found: list[tuple[int, Path]] = []
        for entry in self.path.iterdir():
            if not entry.is_file():
                continue
            match = _SEGMENT_RE.match(entry.name)
            if match is not None:
                found.append((int(match.group(1)), entry))
        found.sort(key=lambda item: item[0])
        return [entry for _, entry in found]

    @property
    def segments(self) -> list[Path]:
        with self._lock:
            return self._discover()

    @property
    def base_lsn(self) -> int:
        with self._lock:
            files = self._discover()
            if not files:
                return 0
            records, _ = _read_segment(files[0])
            return records[0].lsn if records else 0

    # -- 追加与刷盘 ---------------------------------------------------------

    def append(self, xid: int, rtype: int, payload: bytes = b"") -> int:
        with self._lock:
            lsn = self._next_lsn
            record = LogRecord(lsn, xid, int(rtype), payload)
            data = record.encode()
            self._pending.append((lsn, data))
            self._next_lsn += len(data)
            return lsn

    def _ensure_current(self, first_lsn: int) -> None:
        if self._fh is not None:
            return
        path = _segment_path(self.path, first_lsn)
        self._fh = open(path, "ab")
        self._current_path = path
        self._current_size = 0

    def _finish_current(self) -> None:
        if self._fh is None:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._fh = None
        self._current_path = None
        self._current_size = 0

    def flush(self) -> int:
        with self._lock:
            if not self._pending:
                if self._fh is not None:
                    self._fh.flush()
                return self._size
            last_written = self._size
            for lsn, data in self._pending:
                if (
                    self._fh is not None
                    and self._current_size > 0
                    and self._current_size + len(data) > self.segment_bytes
                ):
                    self._finish_current()
                self._ensure_current(lsn)
                assert self._fh is not None
                self._fh.write(data)
                self._current_size += len(data)
                last_written = lsn
            assert self._fh is not None
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._pending.clear()
            self._size = last_written
            return self._size

    # -- 读取与回收 ---------------------------------------------------------

    def iter_records(self, start_lsn: int = 1) -> Iterator[LogRecord]:
        with self._lock:
            self.flush()
            chunks = [(path, path.read_bytes()) for path in self._discover()]
        for _path, data in chunks:
            pos = 0
            while pos + HEADER_SIZE <= len(data):
                try:
                    record, next_pos = LogRecord.decode(data, pos)
                except StorageError:
                    break
                if record.lsn >= start_lsn:
                    yield record
                pos = next_pos

    def truncate_before(self, safe_lsn: int) -> int:
        """删除最后一条记录 LSN 小于 ``safe_lsn`` 的整段，返回删除段数。

        当前正在写的段永远保留；即使它里面全是旧记录，下一次追加也会继续
        使用它，避免出现“文件句柄指向已删除文件”的状态。
        """
        with self._lock:
            self.flush()
            removed = 0
            files = self._discover()
            for path in files[:-1]:
                records, _ = _read_segment(path)
                if not records:
                    path.unlink()
                    removed += 1
                    continue
                if records[-1].lsn < safe_lsn:
                    path.unlink()
                    removed += 1
            return removed

    def close(self) -> None:
        with self._lock:
            self.flush()
            if self._fh is not None:
                self._fh.close()
                self._fh = None


__all__ = ["SegmentedWAL"]
