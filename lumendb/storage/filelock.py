"""数据目录独占锁：同一时刻只允许一个进程打开一个数据库文件。

为什么需要：页缓存、事务表、WAL 全部按"单进程独占整个数据目录"设计。
如果两个进程同时打开同一份数据文件，各自的缓冲池会互相覆盖对方的脏页，
WAL 也会交错写入——数据会坏，而且坏得很难查。所以宁可在打开时就拒绝，
也不要在运行时给出错误结果。

实现方式：在数据文件旁边放一个 ``.lock`` 文件，用操作系统级的字节范围锁
（Windows 用 ``msvcrt.locking``，POSIX 用 ``fcntl.flock``）。锁随进程退出
自动释放，即使进程被强杀也不会留下"僵尸锁"。
"""

from __future__ import annotations

import errno
import os
import sys
import time
from pathlib import Path
from typing import Optional, TextIO

from ..errors import StorageError

if sys.platform == "win32":  # pragma: no cover - 平台分支
    import msvcrt
else:  # pragma: no cover - 平台分支
    import fcntl


class DataDirLockedError(StorageError):
    """数据目录已被另一个进程占用。"""

    sqlstate = "55006"


class FileLock:
    """基于一个锁文件的独占锁，可重入释放、支持等待超时。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._fh: Optional[TextIO] = None

    @property
    def held(self) -> bool:
        return self._fh is not None

    def acquire(self, timeout: float = 0.0) -> None:
        if self._fh is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+b")
        deadline = time.monotonic() + timeout
        while True:
            try:
                self._try_lock(fh)
                self._fh = fh
                return
            except OSError as exc:
                if not self._is_contention(exc):
                    fh.close()
                    raise
                if time.monotonic() >= deadline:
                    fh.close()
                    raise DataDirLockedError(
                        "数据库目录已被另一个进程占用",
                        detail=f"锁文件：{self.path}",
                        hint="关闭占用该目录的进程（例如另一个 shell 或 serve），或换一个 --path",
                    ) from exc
                time.sleep(0.05)

    def release(self) -> None:
        fh, self._fh = self._fh, None
        if fh is None:
            return
        try:
            self._unlock(fh)
        finally:
            fh.close()

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()

    # -- 平台差异 -----------------------------------------------------------

    def _try_lock(self, fh) -> None:
        fh.seek(0)
        if fh.read(1) == b"":
            fh.write(b"\0")
            fh.flush()
            fh.seek(0)
        if sys.platform == "win32":  # pragma: no cover
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:  # pragma: no cover
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(self, fh) -> None:
        try:
            if sys.platform == "win32":  # pragma: no cover
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:  # pragma: no cover
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            # 关闭文件句柄本身就会释放锁，这里失败不影响正确性
            pass

    @staticmethod
    def _is_contention(exc: OSError) -> bool:
        return exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK, errno.EPERM, 13, 35, 36)
