"""B+ 树索引。

为什么用 B+ 树而不是哈希索引
============================

哈希索引只能做等值查询。而 SQL 里最常见的谓词恰恰是范围（``age > 30``、
``BETWEEN``、``ORDER BY``），B+ 树的叶子链让范围扫描只需顺序读叶页，
不需要回表到每个子树。代价是写放大，但读多写少的典型业务里这是正确的取舍。

实现要点
========

1. **叶子是 slotted page**：记录字节从页头向上增长，槽位目录（每项 2 字节，
   存记录偏移）从页尾向下增长。插入只在记录区末尾追加字节并移动目录，
   查找走页内二分——不再每插一行就把整页解码再重编码。删除只摘槽位，
   留下的碎片在页内空间不足时通过一次 compact 回收，仍不够才分裂。
   内部节点仍是紧凑变长布局：它们远小于叶子数量，重写成本可以忽略。
2. **分隔键不要求精确**。内部节点的 ``key[i]`` 只保证 ``<= 右子树的所有键``。
   删除最小值之后不需要回写父节点：旧分隔键仍然是一个合法下界，路由不会错。
3. **删除是惰性的**：只从叶子摘除条目，不触发借位/合并。节点变空时从父节点
   摘除，父节点只剩一个孩子时把孩子的页内容复制上来并降低树高（"塌陷"）。
   代价是节点可能偏稀疏（页利用率下降），空间回收交给 ``REINDEX``；
   收益是删除路径只有"摘除"和"塌陷"两种动作，正确性可以用属性测试穷举。
4. **日志粒度是"变化区间"**：每次只记录 ``[首个变化字节, 最后一个变化字节]``，
   区间定位按 128 字节块跳过相同前缀/后缀，避免逐字节比较。
"""

from __future__ import annotations

import bisect
import struct
from typing import Any, Iterator, Optional, Sequence, Tuple

from ..datatypes import DataType, TypeSpec, decode_value, encode_value
from ..errors import (
    BTreeInvariantError,
    DuplicateKeyError,
    StorageError,
)
from .bufferpool import BufferPoolManager
from .page import INVALID_PAGE_ID, PAGE_SIZE, Page, PageType
from .record import RID
from .txn import Transaction

HEADER_SIZE = 32
INTERNAL_CHILD_SIZE = 4
LEAF_RID_SIZE = 6
#: 叶子页头 offset 4：记录区末尾偏移（记录区向上增长，槽位目录向下增长）
LEAF_DATA_END_OFF = 4
LEAF_SLOT_SIZE = 2
#: RID 在键里的哨兵值（页号 0 是元数据页，永远不会是堆页）
MIN_RID = (0, 0)
MAX_RID = (2**31 - 1, 2**31 - 1)


class _NodeOverflow(Exception):
    """内部信号：序列化后超出页容量，需要分裂。"""


class KeyCodec:
    """索引键 = 列值 + RID，编码后保持字典序。"""

    def __init__(self, column_specs: Sequence[TypeSpec]) -> None:
        self.column_specs = list(column_specs)
        # 末两位是 RID，让非唯一索引的键也保持唯一
        self.specs = self.column_specs + [TypeSpec(DataType.INT), TypeSpec(DataType.INT)]

    def encode(self, key: Sequence[Any]) -> bytes:
        if len(key) != len(self.specs):
            raise StorageError(
                f"索引键长度不符：期望 {len(self.specs)}，收到 {len(key)}"
            )
        return b"".join(encode_value(spec, v) for spec, v in zip(self.specs, key))

    def decode(self, buf: bytes, pos: int) -> Tuple[Tuple[Any, ...], int]:
        values = []
        for spec in self.specs:
            v, pos = decode_value(spec, buf, pos)
            values.append(v)
        return tuple(values), pos

    @property
    def n_columns(self) -> int:
        return len(self.column_specs)


class BPlusTree:
    """一棵持久化的 B+ 树。索引项永远是 ``(列值..., page_id, slot)``。"""

    def __init__(
        self,
        buffer_pool: BufferPoolManager,
        codec: KeyCodec,
        root_page: int = INVALID_PAGE_ID,
        *,
        unique: bool = False,
        name: str = "index",
    ) -> None:
        self._bp = buffer_pool
        self.codec = codec
        self.root_page = root_page
        self.unique = unique
        self.name = name

    # ------------------------------------------------------------------
    # 页结构
    # ------------------------------------------------------------------

    def _init_page(self, page: Page, page_type: PageType) -> None:
        page.buf[:] = bytes(PAGE_SIZE)
        page.page_type = page_type
        page.set_u16(2, 0)
        page.next_page = INVALID_PAGE_ID
        page.prev_page = INVALID_PAGE_ID
        page.set_u32(16, page.page_id)
        page.page_lsn = 0
        self._bp.mark_dirty(page)

    def _init_leaf(self, page: Page) -> None:
        self._init_page(page, PageType.BTREE_LEAF)
        page.set_u16(LEAF_DATA_END_OFF, HEADER_SIZE)

    def _init_internal(self, page: Page) -> None:
        self._init_page(page, PageType.BTREE_INTERNAL)

    def _is_leaf(self, page: Page) -> bool:
        return page.page_type == PageType.BTREE_LEAF

    def _nkeys(self, page: Page) -> int:
        return page.get_u16(2)

    # -- 叶子的槽位目录 -----------------------------------------------------
    #
    # 叶子布局（PAGE_SIZE = 4096）：
    #
    #   [页头 32B][记录字节 → 向上增长 ........ 空闲 ........ ← 槽位目录向下增长]
    #
    # 槽位 i 是一个 u16，指向第 i 条记录的起始偏移；记录 = 编码后的键 + 6B RID。
    # 目录按逻辑顺序排列，因此二分查找只需要解码 O(log n) 个键，插入也只需要
    # 在记录区末尾追加字节并 memmove 目录，而不是重写整页。

    def _leaf_data_end(self, page: Page) -> int:
        end = page.get_u16(LEAF_DATA_END_OFF)
        return end if end >= HEADER_SIZE else HEADER_SIZE

    @staticmethod
    def _leaf_slot_pos(index: int) -> int:
        return PAGE_SIZE - LEAF_SLOT_SIZE * (index + 1)

    def _leaf_free(self, page: Page) -> int:
        """记录区与槽位目录之间的空闲字节数。"""
        return self._leaf_slot_pos(self._nkeys(page)) - self._leaf_data_end(page)

    def _leaf_key_at(self, page: Page, index: int) -> Tuple[tuple, RID, int]:
        """读第 index 个槽位，返回 ``(键, RID, 记录末尾偏移)``。"""
        pos = page.get_u16(self._leaf_slot_pos(index))
        if not HEADER_SIZE <= pos < PAGE_SIZE - LEAF_RID_SIZE:
            raise BTreeInvariantError(
                f"叶子页 {page.page_id} 的槽位 {index} 偏移越界：{pos}"
            )
        try:
            key, pos = self.codec.decode(page.buf, pos)
            if pos + LEAF_RID_SIZE > PAGE_SIZE:
                raise ValueError("RID 越界")
            rid = (page.get_u32(pos), page.get_u16(pos + 4))
        except (ValueError, struct.error) as exc:
            raise BTreeInvariantError(
                f"叶子页 {page.page_id} 的槽位 {index} 解码失败",
                detail=str(exc),
            ) from exc
        return key, rid, pos + LEAF_RID_SIZE

    def _leaf_bisect(self, page: Page, key: tuple) -> int:
        """页内二分：返回第一个 ``>= key`` 的槽位下标。"""
        lo, hi = 0, self._nkeys(page)
        while lo < hi:
            mid = (lo + hi) // 2
            k, _, _ = self._leaf_key_at(page, mid)
            if k < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _leaf_compact(self, txn: Transaction, page: Page) -> None:
        """把记录区重新紧凑排列，回收删除留下的碎片。"""
        keys, rids = self._read_leaf(page)
        self._commit_page(
            txn,
            page,
            self._encode_leaf(page.page_id, keys, rids, page.next_page, page.prev_page),
        )

    def _leaf_insert_at(
        self, txn: Transaction, page: Page, index: int, key: tuple, rid: RID
    ) -> bool:
        """在指定槽位插入一条记录；空间不够时先 compact，仍不够返回 False。"""
        kb = self.codec.encode(key)
        rec_len = len(kb) + LEAF_RID_SIZE
        needed = rec_len + LEAF_SLOT_SIZE
        if self._leaf_free(page) < needed:
            self._leaf_compact(txn, page)
            if self._leaf_free(page) < needed:
                return False

        n = self._nkeys(page)
        data_end = self._leaf_data_end(page)
        buf = bytearray(page.buf)
        buf[data_end : data_end + len(kb)] = kb
        struct.pack_into("<IH", buf, data_end + len(kb), rid[0], rid[1])

        # 把 index..n-1 的槽位整体向页头方向挪 2 字节，给新槽位腾位置
        src_start = PAGE_SIZE - LEAF_SLOT_SIZE * n
        src_end = self._leaf_slot_pos(index) + LEAF_SLOT_SIZE
        chunk = bytes(buf[src_start:src_end])
        buf[src_start - LEAF_SLOT_SIZE : src_end - LEAF_SLOT_SIZE] = chunk

        struct.pack_into("<H", buf, self._leaf_slot_pos(index), data_end)
        struct.pack_into("<H", buf, 2, n + 1)
        struct.pack_into("<H", buf, LEAF_DATA_END_OFF, data_end + rec_len)
        self._commit_page(txn, page, buf)
        return True

    def _leaf_delete_at(self, txn: Transaction, page: Page, index: int) -> None:
        """摘掉一个槽位。记录字节留作碎片，由后续 compact 回收。"""
        n = self._nkeys(page)
        buf = bytearray(page.buf)
        src_start = PAGE_SIZE - LEAF_SLOT_SIZE * n
        src_end = self._leaf_slot_pos(index)
        chunk = bytes(buf[src_start:src_end])
        buf[src_start + LEAF_SLOT_SIZE : src_end + LEAF_SLOT_SIZE] = chunk
        buf[src_start : src_start + LEAF_SLOT_SIZE] = bytes(LEAF_SLOT_SIZE)
        struct.pack_into("<H", buf, 2, n - 1)
        self._commit_page(txn, page, buf)

    # -- 解析 ---------------------------------------------------------------

    def _read_leaf(self, page: Page) -> Tuple[list, list]:
        """按槽位顺序读出整页（分裂、compact、不变量检查用）。"""
        n = self._nkeys(page)
        keys: list = []
        rids: list = []
        for i in range(n):
            key, rid, _ = self._leaf_key_at(page, i)
            keys.append(key)
            rids.append(rid)
        return keys, rids

    def _read_internal(self, page: Page) -> Tuple[list, list]:
        n = self._nkeys(page)
        keys: list = []
        children: list = []
        pos = HEADER_SIZE
        if pos + INTERNAL_CHILD_SIZE > PAGE_SIZE:
            raise BTreeInvariantError(f"内部页 {page.page_id} 页头越界")
        children.append(page.get_u32(pos))
        pos += INTERNAL_CHILD_SIZE
        buf = page.buf
        for _ in range(n):
            try:
                k, pos = self.codec.decode(buf, pos)
                if pos + INTERNAL_CHILD_SIZE > PAGE_SIZE:
                    raise ValueError("孩子指针越界")
                child = page.get_u32(pos)
            except (ValueError, struct.error) as exc:
                raise BTreeInvariantError(
                    f"内部页 {page.page_id} 的槽位越界，nkeys={n}",
                    detail=str(exc),
                ) from exc
            keys.append(k)
            children.append(child)
            pos += INTERNAL_CHILD_SIZE
        return keys, children

    # -- 序列化 -------------------------------------------------------------

    def _encode_leaf(
        self, page_id: int, keys: list, rids: list, next_page: int, prev_page: int
    ) -> bytearray:
        buf = bytearray(PAGE_SIZE)
        struct.pack_into("<H", buf, 0, PageType.BTREE_LEAF)
        struct.pack_into("<H", buf, 2, len(keys))
        struct.pack_into("<I", buf, 8, next_page)
        struct.pack_into("<I", buf, 12, prev_page)
        struct.pack_into("<I", buf, 16, page_id)
        pos = HEADER_SIZE
        for i, (k, rid) in enumerate(zip(keys, rids)):
            kb = self.codec.encode(k)
            rec_len = len(kb) + LEAF_RID_SIZE
            if pos + rec_len > self._leaf_slot_pos(i + 1):
                raise _NodeOverflow()
            buf[pos : pos + len(kb)] = kb
            struct.pack_into("<IH", buf, pos + len(kb), rid[0], rid[1])
            struct.pack_into("<H", buf, self._leaf_slot_pos(i), pos)
            pos += rec_len
        struct.pack_into("<H", buf, LEAF_DATA_END_OFF, pos)
        return buf

    def _encode_internal(
        self, page_id: int, keys: list, children: list, next_page: int, prev_page: int
    ) -> bytearray:
        buf = bytearray(PAGE_SIZE)
        struct.pack_into("<H", buf, 0, PageType.BTREE_INTERNAL)
        struct.pack_into("<H", buf, 2, len(keys))
        struct.pack_into("<I", buf, 8, next_page)
        struct.pack_into("<I", buf, 12, prev_page)
        struct.pack_into("<I", buf, 16, page_id)
        if len(children) != len(keys) + 1:
            raise BTreeInvariantError(
                f"内部页孩子数 {len(children)} 与键数 {len(keys)} 不匹配"
            )
        pos = HEADER_SIZE
        struct.pack_into("<I", buf, pos, children[0])
        pos += INTERNAL_CHILD_SIZE
        for k, child in zip(keys, children[1:]):
            kb = self.codec.encode(k)
            if pos + len(kb) + 4 > PAGE_SIZE:
                raise _NodeOverflow()
            buf[pos : pos + len(kb)] = kb
            pos += len(kb)
            struct.pack_into("<I", buf, pos, child)
            pos += 4
        return buf

    def _commit_page(self, txn: Transaction, page: Page, new_buf: bytearray) -> None:
        """把新页内容写成"变化区间"日志，再更新页。

        区间定位按 128 字节块跳过相同的前后缀：叶子插入通常只改记录区末尾
        和页尾的槽位目录，逐字节比较会成为插入路径上最大的开销之一。
        """
        old = bytes(page.buf)
        newb = bytes(new_buf)
        if old == newb:
            return
        block = 128
        first = 0
        while first < PAGE_SIZE and old[first : first + block] == newb[first : first + block]:
            first += block
        while first < PAGE_SIZE and old[first] == newb[first]:
            first += 1

        last = PAGE_SIZE
        while last - block >= first and old[last - block : last] == newb[last - block : last]:
            last -= block
        while last > first and old[last - 1] == newb[last - 1]:
            last -= 1

        self._bp.log_page_update(txn.xid, page, first, old[first:last], newb[first:last])
        page.buf[:] = new_buf
        self._bp.mark_dirty(page)

    # ------------------------------------------------------------------
    # 查找
    # ------------------------------------------------------------------

    def _child_index(self, keys: list, key: tuple) -> int:
        return bisect.bisect_right(keys, key)

    def _descend(self, key: tuple) -> Page:
        if self.root_page == INVALID_PAGE_ID:
            raise BTreeInvariantError(f"索引 {self.name} 的根页不存在")
        page = self._bp.fetch_page(self.root_page)
        while not self._is_leaf(page):
            keys, children = self._read_internal(page)
            idx = self._child_index(keys, key)
            child_id = children[idx]
            self._bp.unpin(page.page_id)
            page = self._bp.fetch_page(child_id)
        return page

    def _leftmost_leaf(self) -> Page:
        if self.root_page == INVALID_PAGE_ID:
            raise BTreeInvariantError(f"索引 {self.name} 的根页不存在")
        page = self._bp.fetch_page(self.root_page)
        while not self._is_leaf(page):
            _, children = self._read_internal(page)
            child_id = children[0]
            self._bp.unpin(page.page_id)
            page = self._bp.fetch_page(child_id)
        return page

    def lookup(self, txn: Transaction, values: Sequence[Any]) -> list[RID]:
        lo = tuple(values) + MIN_RID
        hi = tuple(values) + MAX_RID
        return [rid for _, rid in self._scan_range(lo, hi, True, True)]

    def lookup_one(self, txn: Transaction, values: Sequence[Any]) -> Optional[RID]:
        hits = self.lookup(txn, values)
        if len(hits) > 1:
            raise BTreeInvariantError(
                f"唯一索引 {self.name} 出现重复键",
                detail=f"key={tuple(values)} 命中 {len(hits)} 条",
            )
        return hits[0] if hits else None

    # ------------------------------------------------------------------
    # 插入
    # ------------------------------------------------------------------

    def insert(self, txn: Transaction, values: Sequence[Any], rid: RID) -> None:
        txn.require_active()
        key = tuple(values) + (rid[0], rid[1])

        if self.unique:
            if self.lookup(txn, values):
                raise DuplicateKeyError(
                    f"唯一索引 {self.name} 冲突",
                    detail=f"键 {tuple(values)} 已存在",
                    hint="换一个键值，或去掉 UNIQUE 约束",
                )

        if self.root_page == INVALID_PAGE_ID:
            page = self._bp.new_page(Page)
            try:
                self._init_leaf(page)
                self._commit_page(
                    txn,
                    page,
                    self._encode_leaf(
                        page.page_id, [key], [rid], INVALID_PAGE_ID, INVALID_PAGE_ID
                    ),
                )
                self.root_page = page.page_id
            finally:
                self._bp.unpin(page.page_id, dirty=True)
            return

        split = self._insert_recursive(txn, self.root_page, key, rid)
        if split is not None:
            sep_key, right_id = split
            new_root = self._bp.new_page(Page)
            try:
                self._init_internal(new_root)
                self._commit_page(
                    txn,
                    new_root,
                    self._encode_internal(
                        new_root.page_id,
                        [sep_key],
                        [self.root_page, right_id],
                        INVALID_PAGE_ID,
                        INVALID_PAGE_ID,
                    ),
                )
                self.root_page = new_root.page_id
            finally:
                self._bp.unpin(new_root.page_id, dirty=True)

    def _insert_recursive(
        self, txn: Transaction, page_id: int, key: tuple, rid: RID
    ) -> Optional[Tuple[tuple, int]]:
        page = self._bp.fetch_page(page_id)
        try:
            if self._is_leaf(page):
                idx = self._leaf_bisect(page, key)
                if idx < self._nkeys(page):
                    existing, _, _ = self._leaf_key_at(page, idx)
                    if existing == key:
                        return None  # 同一 (key, rid) 幂等
                if self._leaf_insert_at(txn, page, idx, key, rid):
                    return None
                # compact 之后仍放不下才分裂
                keys, rids = self._read_leaf(page)
                keys.insert(idx, key)
                rids.insert(idx, rid)
                return self._split_leaf(txn, page, keys, rids)

            keys, children = self._read_internal(page)
            idx = self._child_index(keys, key)
            split = self._insert_recursive(txn, children[idx], key, rid)
            if split is None:
                return None

            sep, right_id = split
            keys.insert(idx, sep)
            children.insert(idx + 1, right_id)
            try:
                self._commit_page(
                    txn,
                    page,
                    self._encode_internal(
                        page.page_id, keys, children, page.next_page, page.prev_page
                    ),
                )
                return None
            except _NodeOverflow:
                return self._split_internal(txn, page, keys, children)
        finally:
            self._bp.unpin(page_id, dirty=True)

    def _split_leaf(
        self, txn: Transaction, page: Page, keys: list, rids: list
    ) -> Tuple[tuple, int]:
        mid = (len(keys) + 1) // 2
        right_id = self._bp.new_page(Page).page_id
        right = self._bp.fetch_page(right_id)
        try:
            self._init_leaf(right)
            old_next = page.next_page

            self._commit_page(
                txn,
                right,
                self._encode_leaf(right_id, keys[mid:], rids[mid:], old_next, page.page_id),
            )
            # 原来的后继页要指回新的右页
            if old_next != INVALID_PAGE_ID:
                nxt = self._bp.fetch_page(old_next)
                try:
                    old_prev = bytes(nxt.buf[12:16])
                    new_prev = struct.pack("<I", right_id)
                    self._bp.log_page_update(txn.xid, nxt, 12, old_prev, new_prev)
                    nxt.buf[12:16] = new_prev
                    self._bp.mark_dirty(nxt)
                finally:
                    self._bp.unpin(old_next, dirty=True)

            self._commit_page(
                txn,
                page,
                self._encode_leaf(
                    page.page_id, keys[:mid], rids[:mid], right_id, page.prev_page
                ),
            )
            return keys[mid], right_id
        finally:
            self._bp.unpin(right_id, dirty=True)

    def _split_internal(
        self, txn: Transaction, page: Page, keys: list, children: list
    ) -> Tuple[tuple, int]:
        mid = len(keys) // 2
        sep = keys[mid]
        right_id = self._bp.new_page(Page).page_id
        right = self._bp.fetch_page(right_id)
        try:
            self._init_internal(right)
            self._commit_page(
                txn,
                right,
                self._encode_internal(
                    right_id,
                    keys[mid + 1 :],
                    children[mid + 1 :],
                    INVALID_PAGE_ID,
                    INVALID_PAGE_ID,
                ),
            )
            self._commit_page(
                txn,
                page,
                self._encode_internal(
                    page.page_id,
                    keys[:mid],
                    children[: mid + 1],
                    INVALID_PAGE_ID,
                    INVALID_PAGE_ID,
                ),
            )
            return sep, right_id
        finally:
            self._bp.unpin(right_id, dirty=True)

    # ------------------------------------------------------------------
    # 删除（惰性：摘除空节点 + 塌陷）
    # ------------------------------------------------------------------

    def delete(self, txn: Transaction, values: Sequence[Any], rid: RID) -> bool:
        txn.require_active()
        if self.root_page == INVALID_PAGE_ID:
            return False
        key = tuple(values) + (rid[0], rid[1])
        deleted = self._delete_recursive(txn, self.root_page, key)
        if deleted:
            self._collapse_root(txn)
        return deleted

    def delete_key(self, txn: Transaction, values: Sequence[Any]) -> int:
        rids = self.lookup(txn, values)
        for rid in rids:
            self.delete(txn, values, rid)
        return len(rids)

    def _delete_recursive(self, txn: Transaction, page_id: int, key: tuple) -> bool:
        page = self._bp.fetch_page(page_id)
        try:
            if self._is_leaf(page):
                idx = self._leaf_bisect(page, key)
                if idx >= self._nkeys(page):
                    return False
                existing, _, _ = self._leaf_key_at(page, idx)
                if existing != key:
                    return False
                self._leaf_delete_at(txn, page, idx)
                return True

            keys, children = self._read_internal(page)
            idx = self._child_index(keys, key)
            deleted = self._delete_recursive(txn, children[idx], key)
            if not deleted:
                return False

            self._detach_if_empty(txn, page, idx)
            return True
        finally:
            self._bp.unpin(page_id, dirty=True)

    def _detach_if_empty(self, txn: Transaction, parent: Page, idx: int) -> None:
        """孩子页变空时把它从父节点摘掉，并修复叶子链。"""
        keys, children = self._read_internal(parent)
        if idx >= len(children):
            return
        child_id = children[idx]
        child = self._bp.fetch_page(child_id)
        try:
            if self._nkeys(child) != 0:
                return
            # 空叶子摘除前修复兄弟指针，避免范围扫描走进死链。
            # 边界情形也要处理：最左叶的前驱是 INVALID 时，右邻居的 prev
            # 必须被置成 INVALID，不能因为"没有左邻居"就跳过。
            if self._is_leaf(child):
                prev_id, next_id = child.prev_page, child.next_page
                if prev_id != INVALID_PAGE_ID:
                    self._set_left_next(txn, prev_id, next_id, child_id)
                if next_id != INVALID_PAGE_ID:
                    self._set_right_prev(txn, next_id, prev_id, child_id)
                self._clear_links(txn, child)
        finally:
            self._bp.unpin(child_id)

        if len(children) <= 1:
            return  # 只有一个孩子，交给 _collapse_root / 上层塌陷处理

        del children[idx]
        if idx > 0:
            del keys[idx - 1]
        elif keys:
            del keys[0]
        self._commit_page(
            txn,
            parent,
            self._encode_internal(
                parent.page_id, keys, children, parent.next_page, parent.prev_page
            ),
        )

    def _set_left_next(
        self, txn: Transaction, page_id: int, value: int, expected: int
    ) -> None:
        """把左邻居的 next 指针改成 ``value``；它当前必须指向被摘除页。"""
        page = self._bp.fetch_page(page_id)
        try:
            if page.next_page != expected:
                raise BTreeInvariantError(
                    f"叶子链不一致：页 {page_id} 的 next 是 {page.next_page}，期望 {expected}"
                )
            old = bytes(page.buf[8:12])
            new = struct.pack("<I", value)
            self._bp.log_page_update(txn.xid, page, 8, old, new)
            page.buf[8:12] = new
            self._bp.mark_dirty(page)
        finally:
            self._bp.unpin(page_id, dirty=True)

    def _set_right_prev(
        self, txn: Transaction, page_id: int, value: int, expected: int
    ) -> None:
        """把右邻居的 prev 指针改成 ``value``；它当前必须指向被摘除页。"""
        page = self._bp.fetch_page(page_id)
        try:
            if page.prev_page != expected:
                raise BTreeInvariantError(
                    f"叶子链不一致：页 {page_id} 的 prev 是 {page.prev_page}，期望 {expected}"
                )
            old = bytes(page.buf[12:16])
            new = struct.pack("<I", value)
            self._bp.log_page_update(txn.xid, page, 12, old, new)
            page.buf[12:16] = new
            self._bp.mark_dirty(page)
        finally:
            self._bp.unpin(page_id, dirty=True)

    def _clear_links(self, txn: Transaction, page: Page) -> None:
        """被摘除的页把 next/prev 清零，避免它以后被误当成链上节点。"""
        for off in (8, 12):
            old = bytes(page.buf[off : off + 4])
            new = struct.pack("<I", INVALID_PAGE_ID)
            if old == new:
                continue
            self._bp.log_page_update(txn.xid, page, off, old, new)
            page.buf[off : off + 4] = new
        self._bp.mark_dirty(page)

    def _collapse_root(self, txn: Transaction) -> None:
        """根是内部节点且只剩一个孩子时，把孩子的页内容搬到根上，降低树高。"""
        while self.root_page != INVALID_PAGE_ID:
            root = self._bp.fetch_page(self.root_page)
            try:
                if self._is_leaf(root):
                    return
                keys, children = self._read_internal(root)
                if len(children) > 1:
                    return
                # 把唯一孩子的整页内容复制到根页：父指针不用改，树高减一
                child_id = children[0]
                child = self._bp.fetch_page(child_id)
                try:
                    new_buf = bytearray(child.buf)
                    struct.pack_into("<I", new_buf, 16, self.root_page)
                    if self._is_leaf(child):
                        # 整棵树只剩这一个叶子：它必须自成一条空链
                        struct.pack_into("<I", new_buf, 8, INVALID_PAGE_ID)
                        struct.pack_into("<I", new_buf, 12, INVALID_PAGE_ID)
                    self._commit_page(txn, root, new_buf)
                finally:
                    self._bp.unpin(child_id)
            finally:
                self._bp.unpin(self.root_page, dirty=True)

    # ------------------------------------------------------------------
    # 扫描
    # ------------------------------------------------------------------

    def _scan_range(
        self,
        low: tuple,
        high: tuple,
        low_inclusive: bool,
        high_inclusive: bool,
    ) -> Iterator[Tuple[tuple, RID]]:
        if self.root_page == INVALID_PAGE_ID:
            return
        page = self._descend(low)
        try:
            start = self._leaf_bisect(page, low)
            while True:
                n = self._nkeys(page)
                for i in range(start, n):
                    k, rid, _ = self._leaf_key_at(page, i)
                    if k < low or (not low_inclusive and k == low):
                        continue
                    if k > high or (not high_inclusive and k == high):
                        return
                    yield k, rid
                next_id = page.next_page
                if next_id == INVALID_PAGE_ID:
                    return
                current = page
                page = None  # 先置空，避免 finally 重复 unpin
                self._bp.unpin(current.page_id)
                page = self._bp.fetch_page(next_id)
                start = 0
        finally:
            if page is not None:
                self._bp.unpin(page.page_id)

    def scan_range(
        self,
        width: int,
        low: Optional[tuple] = None,
        high: Optional[tuple] = None,
        low_inclusive: bool = True,
        high_inclusive: bool = True,
    ) -> Iterator[Tuple[tuple, RID]]:
        """按"索引列前缀"做范围扫描。

        边界只比较前 ``width`` 个索引列，忽略 KeyCodec 自动追加的 RID。
        否则 ``col > 10`` 会把 RID 更大的 ``(10, ...)`` 也当成大于边界，
        结果里混进边界值本身。
        """
        if self.root_page == INVALID_PAGE_ID:
            return
        page = self._descend(low) if low is not None else self._leftmost_leaf()
        try:
            start = self._leaf_bisect(page, low) if low is not None else 0
            while True:
                n = self._nkeys(page)
                for i in range(start, n):
                    k, rid, _ = self._leaf_key_at(page, i)
                    prefix = k[:width]
                    if low is not None:
                        if prefix < low:
                            continue
                        if not low_inclusive and prefix == low:
                            continue
                    if high is not None:
                        if prefix > high:
                            return
                        if not high_inclusive and prefix == high:
                            return
                    yield k, rid
                next_id = page.next_page
                if next_id == INVALID_PAGE_ID:
                    return
                current = page
                page = None
                self._bp.unpin(current.page_id)
                page = self._bp.fetch_page(next_id)
                start = 0
        finally:
            if page is not None:
                self._bp.unpin(page.page_id)

    def scan_all(self, txn: Transaction) -> Iterator[Tuple[tuple, RID]]:
        if self.root_page == INVALID_PAGE_ID:
            return
        page = self._leftmost_leaf()
        try:
            while True:
                keys, rids = self._read_leaf(page)
                for k, rid in zip(keys, rids):
                    yield k, rid
                next_id = page.next_page
                if next_id == INVALID_PAGE_ID:
                    return
                current = page
                page = None
                self._bp.unpin(current.page_id)
                page = self._bp.fetch_page(next_id)
        finally:
            if page is not None:
                self._bp.unpin(page.page_id)

    def rebuild(self, txn: Transaction, entries: Sequence[Tuple[Sequence[Any], RID]]) -> None:
        """丢弃旧树并从给定条目重建。用于 REINDEX 回收稀疏页。"""
        self.root_page = INVALID_PAGE_ID
        for values, rid in entries:
            self.insert(txn, values, rid)

    # ------------------------------------------------------------------
    # 调试
    # ------------------------------------------------------------------

    def check_invariants(self) -> dict:
        if self.root_page == INVALID_PAGE_ID:
            return {"depth": 0, "entries": 0, "pages": 0, "leaf_pages": 0}
        stats = {"depth": 0, "entries": 0, "pages": 0, "leaf_pages": 0}
        leaf_depths: set[int] = set()
        leaf_chain: list[tuple[int, int, int, int]] = []

        def walk(page_id: int, depth: int, low, high) -> None:
            page = self._bp.fetch_page(page_id)
            try:
                stats["pages"] += 1
                if self._is_leaf(page):
                    stats["leaf_pages"] += 1
                    leaf_chain.append(
                        (page_id, page.next_page, page.prev_page, self._nkeys(page))
                    )
                    leaf_depths.add(depth)
                    keys, rids = self._read_leaf(page)
                    stats["entries"] += len(keys)
                    if len(keys) != len(rids):
                        raise BTreeInvariantError(f"页 {page_id} 键与 RID 数量不一致")
                    for prev, cur in zip(keys, keys[1:]):
                        if prev > cur:
                            raise BTreeInvariantError(
                                f"页 {page_id} 的槽位目录未保持键序",
                                detail=f"{prev} 之后出现 {cur}",
                            )
                    for k in keys:
                        if k[0] is None:  # pragma: no cover
                            raise BTreeInvariantError("叶子页出现 NULL 键")
                        if low is not None and k < low:
                            raise BTreeInvariantError(f"页 {page_id} 的键小于下界")
                        if high is not None and k >= high:
                            raise BTreeInvariantError(f"页 {page_id} 的键不小于上界")
                    return

                keys, children = self._read_internal(page)
                if len(children) != len(keys) + 1:
                    raise BTreeInvariantError(
                        f"内部页 {page_id} 孩子数与键数不匹配"
                    )
                for i, child in enumerate(children):
                    lo = keys[i - 1] if i > 0 else low
                    hi = keys[i] if i < len(keys) else high
                    walk(child, depth + 1, lo, hi)
            finally:
                self._bp.unpin(page_id)

        walk(self.root_page, 0, None, None)
        if len(leaf_depths) > 1:
            raise BTreeInvariantError(f"叶子深度不一致：{leaf_depths}")
        # 叶子链必须与树的中序顺序完全一致：任何"摘除空叶但没有修好邻居
        # 指针"的错误都会在这里暴露，而不是等到某个偶发的删除路径上才断言失败。
        self._verify_leaf_chain(leaf_chain)
        stats["depth"] = next(iter(leaf_depths)) if leaf_depths else 0
        return stats

    def _verify_leaf_chain(self, chain: list[tuple[int, int, int, int]]) -> None:
        if not chain:
            return
        if chain[0][2] != INVALID_PAGE_ID:
            raise BTreeInvariantError(
                f"最左叶 {chain[0][0]} 的 prev 应为 INVALID，实际是 {chain[0][2]}"
            )
        for i, (page_id, next_page, prev_page, _) in enumerate(chain):
            if i + 1 < len(chain):
                expected_next = chain[i + 1][0]
                if next_page != expected_next:
                    raise BTreeInvariantError(
                        f"叶子链断裂：页 {page_id} 的 next 是 {next_page}，"
                        f"但中序下一个叶是 {expected_next}"
                    )
                if chain[i + 1][2] != page_id:
                    raise BTreeInvariantError(
                        f"叶子链断裂：页 {chain[i + 1][0]} 的 prev 是 "
                        f"{chain[i + 1][2]}，期望 {page_id}"
                    )
            elif next_page != INVALID_PAGE_ID:
                raise BTreeInvariantError(
                    f"最右叶 {page_id} 的 next 应为 INVALID，实际是 {next_page}"
                )
        for page_id, _, _, nkeys in chain:
            if nkeys == 0 and not (len(chain) == 1 and page_id == self.root_page):
                raise BTreeInvariantError("叶子链上出现了空叶（应在摘除空叶时移除）")


def key_from_row(row: Sequence[Any], column_indexes: Sequence[int]) -> tuple:
    return tuple(row[i] for i in column_indexes)


def index_specs_for_table(table, index_def) -> list[TypeSpec]:
    return [table.column(c).type for c in index_def.columns]


__all__ = ["BPlusTree", "KeyCodec", "key_from_row", "index_specs_for_table"]
