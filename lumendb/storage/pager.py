"""磁盘管理器：数据库文件的页分配、读写与元数据页。

这一层只认识"页"这一个概念，不知道页里装的是什么。所有页号都是文件内的
线性偏移（``page_id * PAGE_SIZE``），因此页号本身就是地址。元数据页维护
一个容量有限的空闲页号数组：上层在提交后把不再可达的页交进来，分配时优先
复用；数组之外没有额外的空闲页链表文件。
"""

from __future__ import annotations

import os
import shutil
import struct
import threading
from pathlib import Path
from typing import Iterable, Optional

from ..errors import CorruptedPageError, PageNotFoundError, StorageError
from ..faults import check as _fault_check
from ..faults import torn_after as _fault_torn
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
#: 历史读的结构下界（VACUUM / DDL 开始 LSN）。与 checkpoint 分开保存：
#: checkpoint 不再限制历史读，只有真正回收旧版本或改 schema 的操作才限制。
META_OFF_HISTORY_FLOOR = 48
#: 空闲页号数组：u32 count + 最多 ``META_FREE_CAPACITY`` 个 u32 page_id。
#: 放在元数据页尾部，避免再引入一个需要单独 fsync 的 sidecar；配额满时
#: 新释放的页不再入表（只泄漏空间，不会误用仍被引用的页）。
META_OFF_FREE_COUNT = 56
META_OFF_FREE_LIST = 64
META_FREE_CAPACITY = (PAGE_SIZE - META_OFF_FREE_LIST) // 4


class MetaPage(Page):
    """page 0：魔数、版本、下一个页号、目录根、下一个事务号、checkpoint LSN、空闲页表。"""

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
        self.set_u64(META_OFF_HISTORY_FLOOR, 0)
        self.set_u32(META_OFF_FREE_COUNT, 0)

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
        if self.free_count > META_FREE_CAPACITY:
            raise CorruptedPageError(
                "元数据页的空闲页数量越界",
                detail=f"count={self.free_count} capacity={META_FREE_CAPACITY}",
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

    @property
    def history_floor_lsn(self) -> int:
        return self.get_u64(META_OFF_HISTORY_FLOOR)

    @history_floor_lsn.setter
    def history_floor_lsn(self, v: int) -> None:
        self.set_u64(META_OFF_HISTORY_FLOOR, v)

    @property
    def free_count(self) -> int:
        return self.get_u32(META_OFF_FREE_COUNT)

    def free_page_ids(self) -> list[int]:
        count = min(self.free_count, META_FREE_CAPACITY)
        return [
            self.get_u32(META_OFF_FREE_LIST + i * 4) for i in range(count)
        ]

    def set_free_page_ids(self, page_ids: list[int]) -> None:
        if len(page_ids) > META_FREE_CAPACITY:
            raise ValueError(
                f"空闲页数 {len(page_ids)} 超过元数据页容量 {META_FREE_CAPACITY}"
            )
        self.set_u32(META_OFF_FREE_COUNT, len(page_ids))
        for i, pid in enumerate(page_ids):
            self.set_u32(META_OFF_FREE_LIST + i * 4, pid)


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
        #: 同一个文件句柄的 seek/write 必须原子。BufferPool 的刷盘线程会在
        #: 不持缓冲池全局锁的情况下并发写不同页；两个线程各自 seek 后交错
        #: write，会把一页写到另一页的偏移（页内 page_id 与目标页号不符）。
        #: 这是进程内 IO 串行锁，与跨进程的 FileLock 是两件事。
        self._io_lock = threading.RLock()

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
            self._sanitize_free_list()
        except BaseException:
            self._lock.release()
            raise

    # -- 初始化 -------------------------------------------------------------

    def _initialize(self) -> None:
        raw = bytearray(PAGE_SIZE)
        meta = MetaPage(0)
        meta.init_meta()
        with self._io_lock:
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

    def _sanitize_free_list(self) -> None:
        """丢弃越界/重复的空闲页号，避免启动后把仍不存在的页当可用页。"""
        raw = self._meta.free_page_ids()
        seen: set[int] = set()
        valid: list[int] = []
        # 以文件实际页数为界，而不是 next_page_id：后者在崩溃后可能落后
        # 于已扩展的文件，拿它当界会把文件里真实存在的空闲页误删。
        limit = self.num_pages
        for pid in raw:
            if pid <= 0 or pid >= limit or pid in seen:
                continue
            seen.add(pid)
            valid.append(pid)
        if valid != raw:
            self._meta.set_free_page_ids(valid)
            self._persist_meta()

    def _persist_meta(self) -> None:
        """把 next_page_id / next_xid / checkpoint_lsn 写回 page 0。

        故意用"读-改-写"而不是直接覆盖整页：``catalog_root`` 属于目录层，
        由 CatalogStore 通过 Buffer Pool 自己维护，这里不能把它盖回旧值。
        空闲页数组也在同一页里，和 next_page_id 一起原子写回；它是空间
        回收用的提示，不是数据库正确性的唯一来源。
        """
        with self._io_lock:
            raw = self.read_page(0)
            buf = bytearray(raw)
            struct.pack_into("<I", buf, META_OFF_NEXT_PAGE, self._meta.next_page_id)
            struct.pack_into("<Q", buf, META_OFF_NEXT_XID, self._meta.next_xid)
            struct.pack_into("<Q", buf, META_OFF_CHECKPOINT_LSN, self._meta.checkpoint_lsn)
            struct.pack_into("<Q", buf, META_OFF_HISTORY_FLOOR, self._meta.history_floor_lsn)
            free_ids = self._meta.free_page_ids()
            struct.pack_into("<I", buf, META_OFF_FREE_COUNT, len(free_ids))
            for i, pid in enumerate(free_ids):
                struct.pack_into("<I", buf, META_OFF_FREE_LIST + i * 4, pid)
            self._fh.seek(0)
            self._fh.write(buf)
            self._fh.flush()

    # -- 页 I/O -------------------------------------------------------------

    @property
    def free_page_count(self) -> int:
        return len(self._meta.free_page_ids())

    def free_page_ids(self) -> list[int]:
        return self._meta.free_page_ids()

    def allocate_page(self) -> int:
        """分配一个页号：优先复用空闲页，否则扩展文件。

        复用页会先整体重写成 FREE 页，因此旧页内容不会泄漏给新对象；
        立即落盘并更新元数据页，保持崩溃后的页号分配单调。
        """
        _fault_check("disk.allocate_page")
        with self._io_lock:
            free_ids = self._meta.free_page_ids()
            if free_ids:
                pid = free_ids.pop()
                self._meta.set_free_page_ids(free_ids)
            else:
                pid = self.num_pages
            buf = bytearray(PAGE_SIZE)
            struct.pack_into("<H", buf, 0, PageType.FREE)
            struct.pack_into("<I", buf, 16, pid)

            self._fh.seek(pid * PAGE_SIZE)
            self._fh.write(buf)
            self._fh.flush()

            if self._meta.next_page_id <= pid:
                self._meta.next_page_id = pid + 1
            self._persist_meta()
            return pid

    def validate_free_list(self) -> int:
        """恢复结束后按页头复核空闲表，丢掉已被重新分配/复用的页。

        持久化的空闲表可能落后于实际页分配：崩溃前最后一次 ``_persist_meta``
        之后，某个空闲页可能已被分配并写入一个已提交对象。恢复 redo 完成后
        这些页的页头不再是 ``FREE``；这里逐项读页头复核，非 FREE 或页号
        不匹配的条目直接丢弃（只泄漏，不复用）。最多 1008 项，开销可控。
        """
        valid: list[int] = []
        dropped = 0
        with self._io_lock:
            for pid in self._meta.free_page_ids():
                if pid <= 0 or pid >= self.num_pages:
                    dropped += 1
                    continue
                self._fh.seek(pid * PAGE_SIZE)
                raw = self._fh.read(PAGE_SIZE)
                if len(raw) != PAGE_SIZE:
                    dropped += 1
                    continue
                page = Page(pid, raw)
                if (
                    page.page_type != PageType.FREE
                    or page.get_u32(16) != pid
                ):
                    dropped += 1
                    continue
                valid.append(pid)
            if valid != self._meta.free_page_ids():
                self._meta.set_free_page_ids(valid)
        return dropped

    def release_pages(self, page_ids: Iterable[int]) -> int:
        """把一批已不再被引用的页号加入空闲表，返回实际接受的页数。

        这是**提示性回收**：空闲表满了或页号非法就跳过，最多是空间泄漏，
        不会把仍被引用的页交给分配器。调用方必须在提交点之后调用，并且
        确认这些页已经不在任何页链/索引/目录版本里。

        接受前先把页头重写成 ``FREE``（只改页头/整页清零，不走 WAL——页已
        不可达）。这样崩溃后若空闲表落后于实际分配，``validate_free_list``
        能凭页头识别"已被重新分配的旧空闲项"并丢弃，不会复用已提交页。
        这里只更新内存里的 meta，不立刻落盘：空闲表是空间复用提示，不是
        正确性依赖；崩溃丢掉它只会让那批页在下一次 checkpoint 前不可复用。
        ``close`` / ``checkpoint`` 会随 ``next_page_id`` 一起持久化。
        """
        accepted = 0
        with self._io_lock:
            free_ids = self._meta.free_page_ids()
            existing = set(free_ids)
            limit = self.num_pages
            for raw in page_ids:
                pid = int(raw)
                if pid <= 0 or pid >= limit or pid in existing:
                    continue
                if len(free_ids) >= META_FREE_CAPACITY:
                    break
                buf = bytearray(PAGE_SIZE)
                struct.pack_into("<H", buf, 0, PageType.FREE)
                struct.pack_into("<I", buf, 16, pid)
                try:
                    self._fh.seek(pid * PAGE_SIZE)
                    self._fh.write(buf)
                except OSError:
                    # 释放是提示性回收，不能把一次写失败升级成提交失败。
                    continue
                free_ids.append(pid)
                existing.add(pid)
                accepted += 1
            if accepted:
                try:
                    self._fh.flush()
                except OSError:
                    # 刷不动也只影响本进程内复用；页头已经在写成功时落盘，
                    # 崩溃后 validate_free_list 会按页头复核。
                    pass
                self._meta.set_free_page_ids(free_ids)
        return accepted

    def truncate_free_tail(self) -> int:
        """从文件尾移除连续的空闲页，返回物理截掉的页数。

        只会处理文件末尾的空闲页；中间的空页仍留在空闲表里等复用。
        必须在 Buffer Pool 已关闭/没有 pin 的页时调用（``Database.close``）。
        """
        removed = 0
        with self._io_lock:
            free_ids = self._meta.free_page_ids()
            free_set = set(free_ids)
            end = self.num_pages
            while end > 1 and (end - 1) in free_set:
                end -= 1
                removed += 1
            if removed == 0:
                return 0
            kept = [pid for pid in free_ids if pid < end]
            self._fh.truncate(end * PAGE_SIZE)
            self._fh.flush()
            os.fsync(self._fh.fileno())
            # 截尾后高水位必须跟文件实际页数对齐；只在 next > end 时改会
            # 让一个陈旧的低水位继续留着，下一次启动的 _sanitize 会误删
            # 文件里仍然存在的空闲项。
            self._meta.next_page_id = end
            self._meta.set_free_page_ids(kept)
            self._persist_meta()
        return removed

    def ensure_page(self, page_id: int) -> None:
        """把文件扩展到至少包含 ``page_id``；用于副本应用主库 WAL。

        主库分配新页时并不写一条“分配日志”，物理 WAL 只记录页内容。
        副本从较旧的基准备份开始时，可能先看到某个新页的 UPDATE；这里按
        页号补齐中间的 FREE 页，使 redo 能落在正确的文件偏移上。
        """
        if page_id < 0:
            raise StorageError("页号不能为负数")
        with self._io_lock:
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
        with self._io_lock:
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

    def _overlay_meta_fields(self, data: bytes | bytearray) -> bytes:
        """把 DiskManager 内存里的权威 meta 字段叠到 page 0 快照上。

        ``BufferPool`` 可能缓存 page 0 的旧副本；若整页 flush 直接落盘，
        会把 ``_persist_meta`` 刚写下的 next_page_id / checkpoint / 历史下界
        / 空闲表覆盖回旧值。目录根 ``catalog_root`` 不叠：它由 CatalogStore
        通过 BufferPool 维护，反而要以页快照为准。
        """
        buf = bytearray(data)
        struct.pack_into("<I", buf, META_OFF_NEXT_PAGE, self._meta.next_page_id)
        struct.pack_into("<Q", buf, META_OFF_NEXT_XID, self._meta.next_xid)
        struct.pack_into("<Q", buf, META_OFF_CHECKPOINT_LSN, self._meta.checkpoint_lsn)
        struct.pack_into("<Q", buf, META_OFF_HISTORY_FLOOR, self._meta.history_floor_lsn)
        free_ids = self._meta.free_page_ids()
        struct.pack_into("<I", buf, META_OFF_FREE_COUNT, len(free_ids))
        for i, pid in enumerate(free_ids):
            struct.pack_into("<I", buf, META_OFF_FREE_LIST + i * 4, pid)
        return bytes(buf)

    def _write_page_locked(
        self, page_id: int, data: bytes | bytearray
    ) -> None:
        """已持 ``_io_lock`` 的页写底层；故障注入与 torn 语义集中在这里。"""
        cut = _fault_torn("disk.write_page", len(data))
        if cut is not None:
            # torn write：前半页新内容、后半页旧内容，然后明确报错。
            # 不能在这里写完整页再抛——那样测试不到恢复/校验路径。
            self._fh.seek(page_id * PAGE_SIZE)
            self._fh.write(data[:cut])
            self._fh.flush()
            raise OSError(5, "injected torn page write")
        _fault_check("disk.write_page")
        self._fh.seek(page_id * PAGE_SIZE)
        self._fh.write(data)

    def write_page(self, page_id: int, data: bytes | bytearray) -> None:
        if len(data) != PAGE_SIZE:
            raise StorageError(f"写入长度必须是 {PAGE_SIZE}，收到 {len(data)}")
        with self._io_lock:
            if page_id < 0 or page_id >= self.num_pages:
                raise PageNotFoundError(f"页 {page_id} 未分配，不能写入")
            self._write_page_locked(page_id, data)

    def write_meta_page(self, data: bytes | bytearray) -> None:
        """写 page 0：先用内存 meta 字段覆盖旧页快照，再按普通页写下。

        专门给 BufferPool 的整页刷盘路径用；恢复/备份的 ``write_page``
        仍按原始页镜像写，避免把 WAL redo 里的 page 0 字段改掉。
        """
        if len(data) != PAGE_SIZE:
            raise StorageError(f"写入长度必须是 {PAGE_SIZE}，收到 {len(data)}")
        with self._io_lock:
            if self.num_pages < 1:
                raise PageNotFoundError("页 0 不存在")
            self._write_page_locked(0, self._overlay_meta_fields(data))

    def sync(self) -> None:
        """把文件缓冲与元数据刷到磁盘（fsync，不是 fflush）。"""
        with self._io_lock:
            self._fh.flush()
            _fault_check("disk.sync")
            os.fsync(self._fh.fileno())

    def snapshot_bytes(self) -> bytes:
        """读取当前数据文件的完整快照。

        调用方负责保证没有并发写；``Database.snapshot`` 会用写锁做这件事。
        """
        with self._io_lock:
            self._fh.flush()
            return self.path.read_bytes()

    def copy_to(self, target: str | Path) -> None:
        """在调用方持锁时把数据文件复制到 target，不把整库读进内存。"""
        with self._io_lock:
            self._fh.flush()
            shutil.copyfile(self.path, Path(target))

    def write_page_bytes(self, page_id: int, data: bytes) -> None:
        """把一页原始字节写回数据文件；用于增量基准备份的页级恢复。

        调用方负责保证没有并发读写；副本在重放线程之外执行这个操作。
        """
        if len(data) != PAGE_SIZE:
            raise ValueError(f"页大小必须是 {PAGE_SIZE}，收到 {len(data)}")
        with self._io_lock:
            self._fh.seek(page_id * PAGE_SIZE)
            self._fh.write(data)
            self._fh.flush()

    def close(self) -> None:
        if not self._fh.closed:
            with self._io_lock:
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
