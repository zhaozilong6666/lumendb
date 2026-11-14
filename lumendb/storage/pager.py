"""磁盘管理器：数据库文件的页分配、读写与元数据页。

这一层只认识"页"这一个概念，不知道页里装的是什么。所有页号都是文件内的
线性偏移（``page_id * PAGE_SIZE``），因此页号本身就是地址——没有额外的
间接层，也没有需要维护的空闲页链表（删除的页由上层决定是否回收）。
"""

from __future__ import annotations

import os
import shutil
import struct
from pathlib import Path
from typing import Optional

from ..errors import CorruptedPageError, PageNotFoundError, StorageError
from .filelock import DataDirLockedError, FileLock
from .page import INVALID_PAGE_ID, MAGIC, PAGE_SIZE, Page, PageType

VERSION = 1

# 元数据页（page 0）的专有布局
META_OFF_MAGIC = 0
META_OFF_VERSION = 4
META_OFF_PAGE_SIZE = 6
META_OFF_NEXT_PAGE = 8
META_OFF_CATALOG_ROOT = 12
META_OFF_NEXT_XID = 16
# 注意：24..32 是通用页头的 pageLSN，元数据页不能复用它存 checkpoint。
# 把 checkpoint_lsn 放到页头之后的 40 字节处，避免每次刷新元数据时
# 把 pageLSN 覆盖成 0，导致恢复阶段重放已经回滚的 catalog_root 更新。
META_OFF_CHECKPOINT_LSN = 40


class MetaPage(Page):
    """page 0：魔数、版本、下一个页号、目录根、下一个事务号、checkpoint LSN。"""

    __slots__ = ()

    def init_meta(self) -> None:
        self.buf[:] = bytes(PAGE_SIZE)
        self.page_id = 0
        self.set_u16(META_OFF_MAGIC, 0)  # 占位
        self.set_u32(META_OFF_MAGIC, MAGIC)
        self.set_u16(META_OFF_VERSION, VERSION)
        self.set_u16(META_OFF_PAGE_SIZE, PAGE_SIZE)
        self.set_u32(META_OFF_NEXT_PAGE, 1)
        self.set_u32(META_OFF_CATALOG_ROOT, 0)
        self.set_u64(META_OFF_NEXT_XID, 1)
        self.set_u64(META_OFF_CHECKPOINT_LSN, 0)

    def validate(self) -> None:
        magic = self.get_u32(META_OFF_MAGIC)
        if magic != MAGIC:
            raise CorruptedPageError(
                "不是 LumenDB 数据文件（元数据页魔数不匹配）",
                detail=f"expected 0x{MAGIC:08X}, got 0x{magic:08X}",
                hint="确认 --data 指向的是 LumenDB 数据库目录，或重新 init",
            )
        version = self.get_u16(META_OFF_VERSION)
        if version != VERSION:
            raise CorruptedPageError(
                f"不支持的数据库版本 {version}",
                detail=f"本引擎支持的版本为 {VERSION}",
            )
        page_size = self.get_u16(META_OFF_PAGE_SIZE)
        if page_size != PAGE_SIZE:
            raise CorruptedPageError(
                f"页大小不匹配：文件为 {page_size}，引擎为 {PAGE_SIZE}",
            )

    @property
    def next_page_id(self) -> int:
        return self.get_u32(META_OFF_NEXT_PAGE)

    @next_page_id.setter
    def next_page_id(self, v: int) -> None:
        self.set_u32(META_OFF_NEXT_PAGE, v)

    @property
    def catalog_root(self) -> int:
        return self.get_u32(META_OFF_CATALOG_ROOT)

    @catalog_root.setter
    def catalog_root(self, v: int) -> None:
        self.set_u32(META_OFF_CATALOG_ROOT, v)

    @property
    def next_xid(self) -> int:
        return self.get_u64(META_OFF_NEXT_XID)

    @next_xid.setter
    def next_xid(self, v: int) -> None:
        self.set_u64(META_OFF_NEXT_XID, v)

    @property
    def checkpoint_lsn(self) -> int:
        return self.get_u64(META_OFF_CHECKPOINT_LSN)

    @checkpoint_lsn.setter
    def checkpoint_lsn(self, v: int) -> None:
        self.set_u64(META_OFF_CHECKPOINT_LSN, v)


class DiskManager:
    """数据库文件的门面。

    线程安全：文件指针的 seek/write 组合用一把锁保护。真正的并发控制在
    Buffer Pool 与事务管理器里，这一层只保证单次读写不相互穿插。
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # 先拿独占锁再碰数据文件：两个进程同时打开同一目录会互相覆盖脏页。
        self._lock = FileLock(self.path.with_name(self.path.name + ".lock"))
        self._lock.acquire()

        try:
            existed = self.path.exists() and self.path.stat().st_size > 0
            self._fh = open(self.path, "r+b" if self.path.exists() else "w+b")

            if not existed:
                self._initialize()
            else:
                self._size = os.path.getsize(self.path)
                if self._size % PAGE_SIZE != 0:
                    raise CorruptedPageError(
                        "数据文件大小不是页大小的整数倍",
                        detail=f"size={self._size} page_size={PAGE_SIZE}",
                    )
                if self._size == 0:  # pragma: no cover - 防御
                    self._initialize()

            self._meta = MetaPage(0, self.read_page(0))
            self._meta.validate()
        except BaseException:
            self._lock.release()
            raise

    # -- 初始化 -------------------------------------------------------------

    def _initialize(self) -> None:
        raw = bytearray(PAGE_SIZE)
        meta = MetaPage(0)
        meta.init_meta()
        self._fh.seek(0)
        self._fh.write(meta.buf)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._size = PAGE_SIZE

    # -- 属性 ---------------------------------------------------------------

    @property
    def num_pages(self) -> int:
        return os.path.getsize(self.path) // PAGE_SIZE

    @property
    def meta(self) -> MetaPage:
        return self._meta

    def _persist_meta(self) -> None:
        """把 next_page_id / next_xid / checkpoint_lsn 写回 page 0。

        故意用"读-改-写"而不是直接覆盖整页：``catalog_root`` 属于目录层，
        由 CatalogStore 通过 Buffer Pool 自己维护，这里不能把它盖回旧值。
        """
        raw = self.read_page(0)
        buf = bytearray(raw)
        struct.pack_into("<I", buf, META_OFF_NEXT_PAGE, self._meta.next_page_id)
        struct.pack_into("<Q", buf, META_OFF_NEXT_XID, self._meta.next_xid)
        struct.pack_into("<Q", buf, META_OFF_CHECKPOINT_LSN, self._meta.checkpoint_lsn)
        self._fh.seek(0)
        self._fh.write(buf)
        self._fh.flush()

    # -- 页 I/O -------------------------------------------------------------

    def allocate_page(self) -> int:
        """分配一个新页号。新页被初始化为 FREE 并写好自描述的 page_id。

        立即落盘而不是延后，是为了让崩溃后的页号分配保持单调——
        否则恢复时可能把两批数据分配到同一个页号。
        """
        pid = self.num_pages
        buf = bytearray(PAGE_SIZE)
        struct.pack_into("<H", buf, 0, PageType.FREE)
        struct.pack_into("<I", buf, 16, pid)

        self._fh.seek(pid * PAGE_SIZE)
        self._fh.write(buf)
        self._fh.flush()

        self._meta.next_page_id = pid + 1
        self._persist_meta()
        return pid

    def ensure_page(self, page_id: int) -> None:
        """把文件扩展到至少包含 ``page_id``；用于副本应用主库 WAL。

        主库分配新页时并不写一条“分配日志”，物理 WAL 只记录页内容。
        副本从较旧的基准备份开始时，可能先看到某个新页的 UPDATE；这里按
        页号补齐中间的 FREE 页，使 redo 能落在正确的文件偏移上。
        """
        if page_id < 0:
            raise StorageError("页号不能为负数")
        current = self.num_pages
        if page_id < current:
            return
        for pid in range(current, page_id + 1):
            buf = bytearray(PAGE_SIZE)
            struct.pack_into("<H", buf, 0, PageType.FREE)
            struct.pack_into("<I", buf, 16, pid)
            self._fh.seek(pid * PAGE_SIZE)
            self._fh.write(buf)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        if self._meta.next_page_id <= page_id:
            self._meta.next_page_id = page_id + 1
            self._persist_meta()

    def read_page(self, page_id: int) -> bytes:
        if page_id < 0 or page_id >= self.num_pages:
            raise PageNotFoundError(
                f"页 {page_id} 不存在",
                detail=f"文件共 {self.num_pages} 页",
            )
        self._fh.seek(page_id * PAGE_SIZE)
        data = self._fh.read(PAGE_SIZE)
        if len(data) != PAGE_SIZE:
            raise PageNotFoundError(
                f"页 {page_id} 读取不完整",
                detail=f"读到 {len(data)} 字节，期望 {PAGE_SIZE}",
            )
        return data

    def write_page(self, page_id: int, data: bytes | bytearray) -> None:
        if len(data) != PAGE_SIZE:
            raise StorageError(f"写入长度必须是 {PAGE_SIZE}，收到 {len(data)}")
        if page_id < 0 or page_id >= self.num_pages:
            raise PageNotFoundError(f"页 {page_id} 未分配，不能写入")
        self._fh.seek(page_id * PAGE_SIZE)
        self._fh.write(data)

    def sync(self) -> None:
        """把文件缓冲与元数据刷到磁盘（fsync，不是 fflush）。"""
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def snapshot_bytes(self) -> bytes:
        """读取当前数据文件的完整快照。

        调用方负责保证没有并发写；``Database.snapshot`` 会用写锁做这件事。
        """
        self._fh.flush()
        return self.path.read_bytes()

    def copy_to(self, target: str | Path) -> None:
        """在调用方持锁时把数据文件复制到 target，不把整库读进内存。"""
        self._fh.flush()
        shutil.copyfile(self.path, Path(target))

    def write_page_bytes(self, page_id: int, data: bytes) -> None:
        """把一页原始字节写回数据文件；用于增量基准备份的页级恢复。

        调用方负责保证没有并发读写；副本在重放线程之外执行这个操作。
        """
        if len(data) != PAGE_SIZE:
            raise ValueError(f"页大小必须是 {PAGE_SIZE}，收到 {len(data)}")
        with self._lock:
            self._fh.seek(page_id * PAGE_SIZE)
            self._fh.write(data)
            self._fh.flush()

    def close(self) -> None:
        if not self._fh.closed:
            self._persist_meta()
            self._fh.flush()
            self._fh.close()
        self._lock.release()

    def __enter__(self) -> "DiskManager":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover
        return f"<DiskManager {self.path} pages={self.num_pages}>"


__all__ = ["DiskManager", "MetaPage", "VERSION"]
