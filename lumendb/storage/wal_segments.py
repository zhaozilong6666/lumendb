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
from ..faults import check as _fault_check
from .wal import HEADER_SIZE, LogRecord, WAL, WALCorruptionError

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
        except WALCorruptionError:
            raise
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
        # 文件写入/轮转/回收单独一把锁：flush 在 fsync 期间释放 _lock，
        # 让其它线程继续 append 到 _pending，下一轮 flush 就能搭上更大的一批。
        # 锁序固定为 _flush_lock → _lock。
        self._flush_lock = threading.RLock()
        self._pending: list[tuple[int, bytes]] = []
        self._fh = None
        self._current_path: Path | None = None
        self._current_size = 0
        self._size = 0
        self._next_lsn = 1
        self._last_lsn = 0
        #: 下游副本仍需要的 LSN；为 0 表示没有保留约束。
        self._retain_lsn = 0

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
                self._last_lsn = records[-1].lsn
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
                        self._last_lsn = records[-1].lsn
                        self._fh = open(last, "ab")
                        self._current_path = last
                        self._current_size = valid_end

    # -- 段文件 -------------------------------------------------------------

    def seed_next_lsn(self, next_lsn: int, *, last_lsn: int = 0) -> None:
        """把空的分段 WAL 接到一个更高的逻辑 LSN。

        只允许在还没有任何段文件、也没有待写记录时调用。用于只读副本提升
        成主库：副本的数据页来自基准备份，但本地没有 WAL；如果新主库从
        LSN 1 重新开始，已经复制到高位的副本会拒绝后续记录。
        """
        with self._lock:
            if (
                self._fh is not None
                or self._pending
                or self._last_lsn
                or self._size
                or self._discover()
            ):
                raise StorageError("WAL 非空，不能重设起始 LSN")
            next_lsn = int(next_lsn)
            last_lsn = int(last_lsn)
            if next_lsn < 1 or last_lsn < 0 or last_lsn >= next_lsn:
                raise ValueError("WAL 的 LSN 边界非法")
            self._next_lsn = next_lsn
            self._last_lsn = last_lsn

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

    @property
    def retain_lsn(self) -> int:
        with self._lock:
            return self._retain_lsn

    @retain_lsn.setter
    def retain_lsn(self, lsn: int) -> None:
        with self._lock:
            self._retain_lsn = max(0, int(lsn))

    # -- 追加与刷盘 ---------------------------------------------------------

    def append(self, xid: int, rtype: int, payload: bytes = b"") -> int:
        _fault_check("wal_segment.append")
        with self._lock:
            lsn = self._next_lsn
            record = LogRecord(lsn, xid, int(rtype), payload)
            data = record.encode()
            self._pending.append((lsn, data))
            self._next_lsn += len(data)
            self._last_lsn = lsn
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
        """把 pending 记录写盘并 fsync，返回已持久化的最后 LSN。

        只把 ``_pending`` 的交换放在 ``_lock`` 里；文件写入/轮转/fsync 在
        ``_flush_lock`` 下进行，不再长期占着追加锁。这样提交线程在 leader
        刷盘期间仍能追加 COMMIT 记录，落到下一轮同一批 flush 里。
        """
        _fault_check("wal_segment.fsync")
        with self._flush_lock:
            with self._lock:
                if not self._pending:
                    if self._fh is not None:
                        self._fh.flush()
                    return self._size
                pending = self._pending
                self._pending = []
                last_written = self._size
            try:
                for lsn, data in pending:
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
            except BaseException:
                # 写失败时把这一批放回队首，避免 pending 凭空消失；文件里
                # 可能已有半批记录，重试的重复记录由恢复端按 LSN 去重语义
                # 处理（与旧实现 flush 失败后 pending 未清空的行为一致）。
                with self._lock:
                    self._pending[0:0] = pending
                raise
            with self._lock:
                self._size = last_written
            return last_written

    def flush_upto(self, lsn: int) -> None:
        """确保所有 lsn <= 目标值的记录都已落盘。

        不能沿用基类的“持 _lock 调 flush”：本类的 flush 要先拿 _flush_lock，
        锁序是 _flush_lock → _lock，持锁调用会 ABBA。
        """
        if lsn <= 0:
            return
        with self._lock:
            if lsn <= self._size:
                return
        self.flush()

    # -- 读取与回收 ---------------------------------------------------------

    def iter_records(self, start_lsn: int = 1) -> Iterator[LogRecord]:
        with self._flush_lock:
            with self._lock:
                self.flush()
                chunks = [(path, path.read_bytes()) for path in self._discover()]
        for _path, data in chunks:
            pos = 0
            while pos + HEADER_SIZE <= len(data):
                try:
                    record, next_pos = LogRecord.decode(data, pos)
                except WALCorruptionError:
                    raise
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
        with self._flush_lock:
            with self._lock:
                self.flush()
                removed = 0
                if self._retain_lsn > 0:
                    # retain_lsn 是下游还需要的最早 LSN；不能按 checkpoint
                    # 的安全 LSN 把 retain 之后、checkpoint 之前的记录删掉。
                    safe_lsn = min(safe_lsn, self._retain_lsn)
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
        with self._flush_lock:
            with self._lock:
                self.flush()
                if self._fh is not None:
                    self._fh.close()
                    self._fh = None


__all__ = ["SegmentedWAL"]
