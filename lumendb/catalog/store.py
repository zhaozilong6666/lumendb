"""目录（catalog）的持久化。

为什么不用单独文件
==================

把目录存成 ``catalog.json`` 看起来省事，但会立刻引入一个一致性难题：目录
与数据分属两个文件，无法用同一份 WAL 保证"表定义写进去了但数据页没写"的
中间状态。把目录放进数据文件的页链里，它就成了普通页，跟着 WAL 与恢复
流程走，不需要第二套机制。

代价是目录更新必须走事务与日志，读目录也要经过 Buffer Pool。对一个目录
规模只有几十张表的系统，这完全可以接受。
"""

from __future__ import annotations

import struct
from typing import Optional

from ..errors import CatalogError
from ..storage.bufferpool import BufferPoolManager
from ..storage.page import INVALID_PAGE_ID, PAGE_SIZE, Page, PageType
from ..storage.pager import META_OFF_CATALOG_ROOT
from ..storage.txn import Transaction

CHUNK_SIZE = PAGE_SIZE - 32


class CatalogStore:
    """目录的字节容器：一条页链，每页装 4064 字节。"""

    def __init__(self, buffer_pool: BufferPoolManager) -> None:
        self._bp = buffer_pool

    # ------------------------------------------------------------------

    def load(self) -> Optional[bytes]:
        # page 0 是元数据页，catalog_root 存在 +12 处。
        # 走 Buffer Pool 而不是直接读磁盘，是为了让未落盘的目录更新也能被看到。
        meta = self._bp.fetch_page(0)
        try:
            root = meta.get_u32(META_OFF_CATALOG_ROOT)
            next_page_id = meta.get_u32(8)
        finally:
            self._bp.unpin(0)

        if root == INVALID_PAGE_ID or root == 0:
            return None

        parts: list[bytes] = []
        page_id = root
        guard = 0
        while page_id != INVALID_PAGE_ID:
            page = self._bp.fetch_page(page_id)
            try:
                if page.page_type != PageType.CATALOG:
                    raise CatalogError(
                        f"页 {page_id} 不是目录页（类型 {page.page_type}）",
                        hint="数据文件可能已损坏",
                    )
                parts.append(bytes(page.buf[32:PAGE_SIZE]))
                nxt = page.next_page
            finally:
                self._bp.unpin(page_id)
            page_id = nxt
            guard += 1
            if guard > 100_000:
                raise CatalogError("目录页链出现环")
        # 最后一页的数据区只用了一部分，其余是 0 填充；JSON 内容不含 \x00，
        # 因此按尾部零字节裁剪即可，不需要额外的长度前缀。
        return b"".join(parts).rstrip(b"\x00")

    # ------------------------------------------------------------------

    def save(self, txn: Transaction, payload: bytes) -> None:
        """把整份目录写成一条新页链，并把根页号写回元数据页。

        每次保存都新建链（旧链成为垃圾页）。目录只在 DDL 时变化，这个取舍
        换来的是"目录更新始终是一个整体"，不会出现半新半旧的两条链。
        """
        if not payload:
            return

        chunks = [
            payload[i : i + CHUNK_SIZE] for i in range(0, len(payload), CHUNK_SIZE)
        ]
        pages: list[Page] = []
        for _ in chunks:
            page = self._bp.new_page(Page)
            page.buf[:] = bytes(PAGE_SIZE)
            page.page_type = PageType.CATALOG
            page.set_u32(16, page.page_id)
            page.next_page = INVALID_PAGE_ID
            page.prev_page = pages[-1].page_id if pages else INVALID_PAGE_ID
            pages.append(page)
        try:
            for i, (page, chunk) in enumerate(zip(pages, chunks)):
                page.buf[32 : 32 + len(chunk)] = chunk
                if i + 1 < len(pages):
                    page.next_page = pages[i + 1].page_id
                self._bp.log_page_update(
                    txn.xid, page, 0, bytes(PAGE_SIZE), bytes(page.buf)
                )
                self._bp.mark_dirty(page)

            # 先把根页号写进元数据页（同一个事务、同一条 WAL）
            meta = self._bp.fetch_page(0)
            try:
                old = bytes(meta.buf[META_OFF_CATALOG_ROOT : META_OFF_CATALOG_ROOT + 4])
                new = struct.pack("<I", pages[0].page_id)
                self._bp.log_page_update(
                    txn.xid, meta, META_OFF_CATALOG_ROOT, old, new
                )
                meta.buf[META_OFF_CATALOG_ROOT : META_OFF_CATALOG_ROOT + 4] = new
                self._bp.mark_dirty(meta)
            finally:
                self._bp.unpin(0, dirty=True)
        finally:
            for page in pages:
                self._bp.unpin(page.page_id, dirty=True)


__all__ = ["CatalogStore", "CHUNK_SIZE"]
