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
4. **日志粒度是"变化区间"**：把页内互不相连的变化拆成若干
   ``[首个变化字节, 最后一个变化字节]`` 的小区间，区间之间的大片零区不写
   进 WAL；区间定位与拆分见 ``_diff_ranges``。
"""

from __future__ import annotations

import bisect
import struct
import threading
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
#: 内部页页头偏移 4 的 u8 标志：1 表示记录区开头有 high key（右兄弟最小键）。
HIGH_KEY_FLAG_OFF = 4
#: 叶子页页头偏移 6 的 u8 标志：1 表示记录区开头有 high key（右兄弟最小键）。
#: 叶子页偏移 4 是 LEAF_DATA_END_OFF（数据区末尾），6-7 空闲，故叶子用 6。
LEAF_HIGH_KEY_FLAG_OFF = 6
LEAF_RID_SIZE = 6
#: 叶子页头 offset 4：记录区末尾偏移（记录区向上增长，槽位目录向下增长）
LEAF_DATA_END_OFF = 4
LEAF_SLOT_SIZE = 2
#: RID 在键里的哨兵值（页号 0 是元数据页，永远不会是堆页）
MIN_RID = (0, 0)
MAX_RID = (2**31 - 1, 2**31 - 1)


def _diff_ranges(old: bytes, new: bytes) -> list[tuple[int, int]]:
    """返回若干互不相连的 ``[first, last)`` 变化区间。

    B+ 树叶子是 slotted page：头部的键数、页首的记录区、页尾的槽位目录
    可能同时变化，中间夹着成片未变化的空闲区。按“变化游程 + 间隔”拆成
    多条，比把中间零区一起写进 WAL 更省空间；间隔小于 8 字节时合并，
    避免一条插入产生过多小记录。
    """
    size = len(old)
    ranges: list[tuple[int, int]] = []
    i = 0
    while i < size:
        if old[i] == new[i]:
            i += 1
            continue
        start = i
        end = i + 1
        i += 1
        while i < size:
            if old[i] != new[i]:
                end = i + 1
                i += 1
                continue
            # 向后看一段相同区；短间隔合并，长间隔断开成两条区间。
            j = i
            while j < size and old[j] == new[j]:
                j += 1
            if j - i >= 8 or j >= size:
                break
            i = j
        ranges.append((start, end))
    return ranges


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
        internal_cache: Optional[dict] = None,
    ) -> None:
        self._bp = buffer_pool
        self.codec = codec
        self.root_page = root_page
        self.unique = unique
        self.name = name
        #: 根页号是整棵树的全局状态；根分裂（建新根）与根塌陷（降树高）都改它，
        #: 必须串行，否则两个线程同时建新根会把 `root_page` 后写覆盖、丢一棵子树。
        self._root_lock = threading.RLock()
        #: 只给 ``_descend`` 用的内部页解码缓存，键是 (页号, pageLSN)。
        #: 由执行器跨语句持有（每次查询都新建 BPlusTree，缓存挂在树上没意义）。
        #: 页被写过后 pageLSN 会推进，旧条目自然再也不会命中。
        self._internal_cache = internal_cache

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
        page.set_u8(LEAF_HIGH_KEY_FLAG_OFF, 0)

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

    def _leaf_high_key(self, page: Page) -> Optional[tuple]:
        """读叶子 high key（右兄弟最小键），可能为 None。

        叶子也参与 B-link：并发分裂后，左叶必须知道自己从哪个键开始"不属于
        本页"，插入/查找者读到 key >= high key 时沿 next 去右兄弟。
        """
        if not page.get_u8(LEAF_HIGH_KEY_FLAG_OFF):
            return None
        try:
            high_key, _ = self.codec.decode(page.buf, HEADER_SIZE)
            return high_key
        except (ValueError, struct.error) as exc:
            raise BTreeInvariantError(
                f"叶子页 {page.page_id} 的 high key 解码失败",
                detail=str(exc),
            ) from exc

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

    def _leaf_prefix_at(self, page: Page, index: int, width: int) -> tuple:
        """只解码索引列（不含自动追加的 RID 两列）。

        等值查找的二分只需要比较索引列前缀：键 = 索引列 + RID，
        ``lo = values + MIN_RID``；前缀相等时整键一定 ``>= lo``（RID 有下界），
        前缀更小时整键一定 ``< lo``。所以前缀比较与全键比较给出同一个下界，
        每轮探测可以少解两个 INT（实测单列索引 ``lookup`` 35.2 → 28.0 us）。
        """
        pos = page.get_u16(self._leaf_slot_pos(index))
        if not HEADER_SIZE <= pos < PAGE_SIZE - LEAF_RID_SIZE:
            raise BTreeInvariantError(
                f"叶子页 {page.page_id} 的槽位 {index} 偏移越界：{pos}"
            )
        values: list = []
        try:
            for spec in self.codec.column_specs[:width]:
                value, pos = decode_value(spec, page.buf, pos)
                values.append(value)
        except (ValueError, struct.error) as exc:
            raise BTreeInvariantError(
                f"叶子页 {page.page_id} 的槽位 {index} 前缀解码失败",
                detail=str(exc),
            ) from exc
        return tuple(values)

    def _leaf_bisect_prefix(self, page: Page, prefix: tuple) -> int:
        """按索引列前缀二分：返回第一个 ``前缀 >= prefix`` 的槽位下标。"""
        lo, hi = 0, self._nkeys(page)
        width = len(prefix)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._leaf_prefix_at(page, mid, width) < prefix:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _leaf_compact(self, txn: Transaction, page: Page) -> None:
        """把记录区重新紧凑排列，回收删除留下的碎片。"""
        with page.latch:
            keys, rids, high_key = self._read_leaf(page)
            self._commit_page(
                txn,
                page,
                self._encode_leaf(
                    page.page_id, keys, rids, page.next_page, page.prev_page, high_key
                ),
            )

    def _leaf_insert_at(
        self, txn: Transaction, page: Page, key: tuple, rid: RID
    ) -> Optional[int]:
        """在叶内插入一条记录；空间不够时先 compact，仍不够返回插入位置（需分裂）。

        整个"二分定位 → 幂等判断 → 紧凑 → 插入"都在页 latch 内：下标必须在
        latch 内算，否则两个线程会各自基于同一份旧页算出下标，后写者把先写者
        的槽位顶乱（探针里表现为"槽位目录未保持键序"）。返回 ``None`` 表示已
        插入或已存在（幂等）；返回 int 表示该页已满、需要分裂，值是 key 在
        完整键序里的插入位置。

        注意：本方法**只负责单页插入，不负责分裂**。分裂必须与"发现满"发生在
        同一个页 latch 内，否则两个线程会各自基于同一份旧页分裂；需要分裂语义
        时请用 ``_leaf_insert_or_split``。
        """
        with page.latch:
            idx = self._leaf_bisect(page, key)
            nkeys = self._nkeys(page)
            if idx < nkeys:
                existing, _, _ = self._leaf_key_at(page, idx)
                if existing == key:
                    return None  # 同一 (key, rid) 幂等
            kb = self.codec.encode(key)
            rec_len = len(kb) + LEAF_RID_SIZE
            needed = rec_len + LEAF_SLOT_SIZE
            if self._leaf_free(page) < needed:
                self._leaf_compact(txn, page)
                if self._leaf_free(page) < needed:
                    return idx

            n = self._nkeys(page)
            data_end = self._leaf_data_end(page)
            buf = bytearray(page.buf)
            buf[data_end : data_end + len(kb)] = kb
            struct.pack_into("<IH", buf, data_end + len(kb), rid[0], rid[1])

            # 把 index..n-1 的槽位整体向页头方向挪 2 字节，给新槽位腾位置
            src_start = PAGE_SIZE - LEAF_SLOT_SIZE * n
            src_end = self._leaf_slot_pos(idx) + LEAF_SLOT_SIZE
            chunk = bytes(buf[src_start:src_end])
            buf[src_start - LEAF_SLOT_SIZE : src_end - LEAF_SLOT_SIZE] = chunk

            struct.pack_into("<H", buf, self._leaf_slot_pos(idx), data_end)
            struct.pack_into("<H", buf, 2, n + 1)
            struct.pack_into("<H", buf, LEAF_DATA_END_OFF, data_end + rec_len)
            self._commit_page(txn, page, buf)
            return None

    def _leaf_insert_or_split(
        self, txn: Transaction, page: Page, key: tuple, rid: RID
    ) -> Optional[Tuple[tuple, int]]:
        """在叶内插入；空间不够时在同一页 latch 内分裂。

        "发现满 → 读页 → 分裂"必须发生在同一个 latch 内：若 `_leaf_insert_at`
        释放 latch 后再分裂，两个线程会各自基于同一份旧页分裂，各建一个右页
        并重写本叶，导致丢行 / 叶子链断裂。返回 ``None`` 表示已插入/已存在；
        返回 ``(sep_key, right_id)`` 表示本叶已分裂，调用方需把分隔键插入父页。
        """
        with page.latch:
            idx = self._leaf_insert_at(txn, page, key, rid)  # RLock 重入本页 latch
            if idx is None:
                return None
            keys, rids, _ = self._read_leaf(page)
            keys.insert(idx, key)
            rids.insert(idx, rid)
            # `_split_leaf` 会重入 page.latch（RLock），锁序 page → right → next
            return self._split_leaf(txn, page, keys, rids)

    def _leaf_delete_at(self, txn: Transaction, page: Page, index: int) -> None:
        """摘掉一个槽位。记录字节留作碎片，由后续 compact 回收。"""
        with page.latch:
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

    def _read_leaf(self, page: Page) -> Tuple[list, list, Optional[tuple]]:
        """按槽位顺序读出整页（分裂、compact、不变量检查用），外加 high key。"""
        n = self._nkeys(page)
        keys: list = []
        rids: list = []
        for i in range(n):
            key, rid, _ = self._leaf_key_at(page, i)
            keys.append(key)
            rids.append(rid)
        return keys, rids, self._leaf_high_key(page)

    def _read_internal(self, page: Page) -> Tuple[list, list, Optional[tuple]]:
        """读内部页的键/孩子，外加 high key（右兄弟的最小键，可能为 None）。

        high key 是 B-link 路由的关键：内部页分裂后，左页所有键都小于右页，
        但左页自己不知道右页从哪个键开始。high key 把"右兄弟的最小键"记在左页，
        查找者读到 key >= high key 时就知道要沿 next 去右兄弟。
        """
        n = self._nkeys(page)
        keys: list = []
        children: list = []
        pos = HEADER_SIZE
        high_key: Optional[tuple] = None
        if page.get_u8(HIGH_KEY_FLAG_OFF):
            try:
                high_key, pos = self.codec.decode(page.buf, pos)
            except (ValueError, struct.error) as exc:
                raise BTreeInvariantError(
                    f"内部页 {page.page_id} 的 high key 解码失败",
                    detail=str(exc),
                ) from exc
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
        return keys, children, high_key

    def _read_internal_cached(self, page: Page) -> Tuple[tuple, tuple, Optional[tuple]]:
        """``_descend`` 专用的只读版本：按 (页号, pageLSN) 缓存解码结果。

        内部页是"键 + 孩子指针"数组，没有叶子那种槽位目录，定位一个孩子必须
        把整页键都解码出来。点查每下降一层就要付一次这个成本，而树的顶部几层
        几乎不变——缓存它们是纯收益。缓存条目按页 LSN 失效：页被写过后 LSN
        推进，旧键再也不会命中；返回元组，避免调用方原地修改缓存内容。
        """
        cache = self._internal_cache
        if cache is None:
            return self._read_internal(page)
        key = (page.page_id, page.page_lsn)
        hit = cache.get(key)
        if hit is not None:
            return hit
        keys, children, high_key = self._read_internal(page)
        entry = (tuple(keys), tuple(children), high_key)
        if len(cache) >= 128:
            # 逐条淘汰；树顶就那么几页，128 条足够覆盖热点。
            cache.pop(next(iter(cache)))
        cache[key] = entry
        return entry

    # -- 序列化 -------------------------------------------------------------

    def _encode_leaf(
        self,
        page_id: int,
        keys: list,
        rids: list,
        next_page: int,
        prev_page: int,
        high_key: Optional[tuple] = None,
    ) -> bytearray:
        buf = bytearray(PAGE_SIZE)
        struct.pack_into("<H", buf, 0, PageType.BTREE_LEAF)
        struct.pack_into("<H", buf, 2, len(keys))
        struct.pack_into("<I", buf, 8, next_page)
        struct.pack_into("<I", buf, 12, prev_page)
        struct.pack_into("<I", buf, 16, page_id)
        pos = HEADER_SIZE
        if high_key is not None:
            struct.pack_into("<B", buf, LEAF_HIGH_KEY_FLAG_OFF, 1)
            hkb = self.codec.encode(high_key)
            buf[pos : pos + len(hkb)] = hkb
            pos += len(hkb)
        else:
            struct.pack_into("<B", buf, LEAF_HIGH_KEY_FLAG_OFF, 0)
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
        self,
        page_id: int,
        keys: list,
        children: list,
        next_page: int,
        prev_page: int,
        high_key: Optional[tuple] = None,
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
        if high_key is not None:
            struct.pack_into("<B", buf, HIGH_KEY_FLAG_OFF, 1)
            hkb = self.codec.encode(high_key)
            buf[pos : pos + len(hkb)] = hkb
            pos += len(hkb)
        else:
            struct.pack_into("<B", buf, HIGH_KEY_FLAG_OFF, 0)
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

        如果页中间有成片的未变化零区（叶子插入常见：槽位目录在页尾、
        记录区在页首），拆成多条互不相连的 UPDATE 记录，避免把中间
        零区也写进 WAL。多条记录仍按 pageLSN 顺序重放，恢复/复制语义
        不变。
        """
        old = bytes(page.buf)
        newb = bytes(new_buf)
        if old == newb:
            return
        ranges = _diff_ranges(old, newb)
        if len(ranges) > 32:
            # 变化过于零碎时退回单区间，避免一条插入产生几十条日志。
            first = next(
                i for i, (a, b) in enumerate(zip(old, newb)) if a != b
            )
            last = PAGE_SIZE
            while last > first and old[last - 1] == newb[last - 1]:
                last -= 1
            ranges = [(first, last)]
        # 读旧值 → 写日志 → 写页三步在同一页 latch 内完成（与 HeapFile 的
        # `_apply_and_log` 同款语义）：页内容的读-改-写必须原子，否则两个写者
        # 交错时后者的 before image 已含前者的改动，WAL 重放会算错。这是
        # Step 4 的前半段；分裂/合并的多页原子性仍需 crabbing（见 `_split_leaf`）。
        with page.latch:
            for first, last in ranges:
                self._bp.log_page_update(
                    txn.xid, page, first, old[first:last], newb[first:last]
                )
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
            keys, children, high_key, next_id = self._snapshot_internal(page)
            if (
                high_key is not None
                and key >= high_key
                and next_id != INVALID_PAGE_ID
            ):
                # key 越过本页上界 → 沿 next 右移（B-link 路由）。这是内部页
                # 并发分裂正确性的关键：分裂后左页不再包含右半的键，查找者若
                # 仍按左页 keys 二分会把 key 路由到左半最右孩子（错），必须先
                # 用 high key 判明"该去右兄弟"。
                nxt = next_id
                self._bp.unpin(page.page_id)
                page = self._bp.fetch_page(nxt)
                continue
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
            _, children, _ = self._read_internal(page)
            child_id = children[0]
            self._bp.unpin(page.page_id)
            page = self._bp.fetch_page(child_id)
        return page

    def _snapshot_leaf(self, page: Page) -> Tuple[list, list, int, int]:
        """在页 latch 内整页读出一份一致的叶子快照：(keys, rids, next, prev)。

        必须一次读完：``_read_leaf`` 是先读 ``nkeys`` 再按槽位逐个解键，如果
        中途撞上写者换页，就会拿"旧 nkeys + 新槽位表"组合去取偏移——实测会
        throw ``叶子页 2 的槽位 150 偏移越界：0``（那条槽位在新布局里是空位）。
        latch 内读让 nkeys 与槽位表来自同一份页内容。
        """
        with page.latch:
            keys, rids, _ = self._read_leaf(page)
            return keys, rids, page.next_page, page.prev_page

    def _snapshot_internal(self, page: Page) -> Tuple[list, list, Optional[tuple], int]:
        """内部页的整页一致快照（键/孩子/high key/next），锁语义同 ``_snapshot_leaf``。"""
        with page.latch:
            keys, children, high_key = self._read_internal_cached(page)
            return list(keys), list(children), high_key, page.next_page

    def _leftmost_leaf_verified(self, attempts: int = 32) -> Optional[Page]:
        """拿到**真正链首**的叶子页（已 pin），供全表扫描起步。

        并发删除时下探可能落进正在被 purge 的子树，落点有三种状态：

        * 普通链中段：``prev != INVALID`` → 沿 ``prev`` 回溯到链首；
        * 真链首（还有数据）：``prev == INVALID`` 且 ``nkeys > 0``；
        * 已摘除的空页：``prev == INVALID`` 且 ``next == INVALID`` 且它**不是**
          根（``_clear_links`` 把被摘页的前后指针都清零，看起来像孤立的链首，
          沿它扫就会"扫到 0 条"）。

        第三种状态无法原地修复，只能重新下探；整棵树真的为空时，根就是一片
        空叶，那是合法结果，直接返回。
        """
        for _ in range(attempts):
            page = self._leftmost_leaf()
            # 回溯到链首；上限 64 跳，防叶子链异常时打转。
            for _ in range(64):
                with page.latch:
                    prev = page.prev_page
                    nkeys = self._nkeys(page)
                    next_id = page.next_page
                if prev == INVALID_PAGE_ID:
                    break
                pid = prev
                self._bp.unpin(page.page_id)
                page = self._bp.fetch_page(pid)
            else:
                self._bp.unpin(page.page_id)
                continue
            if nkeys > 0 or next_id != INVALID_PAGE_ID:
                return page
            if self.root_page == page.page_id and self._is_leaf(page):
                return page  # 整棵树为空：根就是这片空叶
            self._bp.unpin(page.page_id)
        return None

    def lookup(self, txn: Transaction, values: Sequence[Any]) -> list[RID]:
        """等值查找：二分只解索引列，命中项才解整键。

        等价于 ``[rid for _, rid in _scan_range(values+MIN_RID, values+MAX_RID)]``，
        但省掉了通用扫描器的生成器 / 区间比较开销，并且二分每轮少解两个 RID
        整数。同一进程交错 A/B（2000 键、单列索引）：轻载 35.2 → 28.0 us
        （−20.5%），重载 150 → 128 us（−14.6%）；重复键（每桶 200 条）约 −4%。

        跨叶重复键：本叶扫完后若最后一个前缀仍等于目标，沿 ``next_page`` 继续，
        直到遇到更大的前缀；不会漏掉分布在多个叶子上的重复键。
        """
        if self.root_page == INVALID_PAGE_ID:
            return []
        target = tuple(values)
        width = len(target)
        if width > len(self.codec.column_specs):
            # 键比索引列还长：这样的键不可能存在（旧实现同样返回空），
            # 提前退出，免得把整棵树扫一遍。
            return []
        lo = target + MIN_RID
        page = self._descend(lo)
        hits: list[RID] = []
        try:
            while True:
                # 定位与逐键解码必须在同一把 latch 内：否则 bisect 用的槽位表
                # 与随后读到的键可能来自不同版本（扫描器实测撞出过
                # 「槽位偏移越界：0」）。
                with page.latch:
                    start = self._leaf_bisect_prefix(page, target)
                    keys, rids, _ = self._read_leaf(page)
                    next_id = page.next_page
                for key, rid in zip(keys[start:], rids[start:]):
                    prefix = key[:width]
                    if prefix == target:
                        hits.append(rid)
                    elif prefix > target:
                        # 叶子按完整键有序，前缀一旦越过目标就不会再回来。
                        return hits
                if next_id == INVALID_PAGE_ID:
                    return hits
                current = page
                page = None  # 先置空，避免 finally 重复 unpin
                self._bp.unpin(current.page_id)
                page = self._bp.fetch_page(next_id)
        finally:
            if page is not None:
                self._bp.unpin(page.page_id)

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
            # 两个线程同时插第一条时都要建根，双重检查 + 树级锁串行
            with self._root_lock:
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
                    return  # 真的建根，本次插入完成
            # root 已被并发线程建好：不要 return，fall through 到下面的普通
            # 插入，否则本次 key 会被静默丢弃（丢最左叶最小键）。

        # 普通插入不碰根锁；只有触发根分裂时才拿锁，锁内重读 root_page，
        # 发现别的线程已经分裂过根就重试（乐观 + 重试，避免串行化所有插入）。
        while True:
            root_before = self.root_page
            split = self._insert_recursive(txn, root_before, key, rid)
            if split is None:
                return
            with self._root_lock:
                if self.root_page != root_before:
                    continue  # 根已被别的线程分裂，本次 split 基于过期根，重试
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
                            [root_before, right_id],
                            INVALID_PAGE_ID,
                            INVALID_PAGE_ID,
                        ),
                    )
                    self.root_page = new_root.page_id
                finally:
                    self._bp.unpin(new_root.page_id, dirty=True)
                return

    def _insert_recursive(
        self, txn: Transaction, page_id: int, key: tuple, rid: RID
    ) -> Optional[Tuple[tuple, int]]:
        while True:
            page = self._bp.fetch_page(page_id)
            if self._is_leaf(page):
                with page.latch:
                    high_key = self._leaf_high_key(page)
                    if (
                        high_key is not None
                        and key >= high_key
                        and page.next_page != INVALID_PAGE_ID
                    ):
                        # B-link：叶子分裂后本页已是左半，key 归右兄弟，沿 next
                        # 右移后重试。若不判，key 会被错插进左叶（漏分右半键）。
                        page_id = page.next_page
                        self._bp.unpin(page.page_id, dirty=True)
                        continue
                    # 插入/分裂都在 `_leaf_insert_or_split` 的同一页 latch 内完成
                    result = self._leaf_insert_or_split(txn, page, key, rid)
                self._bp.unpin(page_id, dirty=True)
                return result

            # 内部页：持 latch 完成"读键 → 递归插孩子 → 插分隔键 / 分裂"。
            # 两个线程同时往同一内部页插分隔键时会各自基于同一份旧 keys 覆盖，
            # 丢分隔键 = 丢子树 = 丢行；锁序保持 父页 → 子页 → 叶 → 后继 单向，
            # 且 `_commit_page`/`_split_internal` 会重入本页 latch（RLock）。
            with page.latch:
                keys, children, high_key = self._read_internal(page)
                if (
                    high_key is not None
                    and key >= high_key
                    and page.next_page != INVALID_PAGE_ID
                ):
                    # B-link：key 越过本页上界，沿 next 右移后重试。并发下本页
                    # 可能已被别的线程分裂成左半，若仍按 keys 二分会把 key 路由
                    # 到左半最右孩子（错）。
                    page_id = page.next_page
                    self._bp.unpin(page.page_id, dirty=True)
                    continue

                idx = self._child_index(keys, key)
                split = self._insert_recursive(txn, children[idx], key, rid)
                if split is None:
                    self._bp.unpin(page_id, dirty=True)
                    return None

                sep, right_id = split
                keys.insert(idx, sep)
                children.insert(idx + 1, right_id)
                try:
                    self._commit_page(
                        txn,
                        page,
                        self._encode_internal(
                            page.page_id, keys, children, page.next_page,
                            page.prev_page, high_key,
                        ),
                    )
                    result = None
                except _NodeOverflow:
                    result = self._split_internal(txn, page, keys, children)
                self._bp.unpin(page_id, dirty=True)
                return result

    def _split_leaf(
        self, txn: Transaction, page: Page, keys: list, rids: list
    ) -> Tuple[tuple, int]:
        mid = (len(keys) + 1) // 2
        sep = keys[mid]
        right = self._bp.new_page(Page)  # new_page 已 pin，勿再 fetch（否则双 pin 泄漏）
        right_id = right.page_id
        try:
            self._init_leaf(right)
            old_next = page.next_page
            old_high = self._leaf_high_key(page)

            # 右页继承旧 high key，next 指向旧后继，prev 指向左页
            self._commit_page(
                txn,
                right,
                self._encode_leaf(
                    right_id, keys[mid:], rids[mid:], old_next, page.page_id, old_high
                ),
            )
            # 原来的后继页要指回新的右页
            if old_next != INVALID_PAGE_ID:
                nxt = self._bp.fetch_page(old_next)
                try:
                    with nxt.latch:
                        if nxt.prev_page != page.page_id:
                            raise BTreeInvariantError(
                                f"叶子链不一致：页 {old_next} 的 prev 是 {nxt.prev_page}，"
                                f"期望 {page.page_id}"
                            )
                        old_prev = bytes(nxt.buf[12:16])
                        new_prev = struct.pack("<I", right_id)
                        self._bp.log_page_update(txn.xid, nxt, 12, old_prev, new_prev)
                        nxt.buf[12:16] = new_prev
                        self._bp.mark_dirty(nxt)
                finally:
                    self._bp.unpin(old_next, dirty=True)

            # 左页 high key = sep（右兄弟最小键），next 指向右页
            self._commit_page(
                txn,
                page,
                self._encode_leaf(
                    page.page_id, keys[:mid], rids[:mid], right_id, page.prev_page, sep
                ),
            )
            return sep, right_id
        finally:
            self._bp.unpin(right_id, dirty=True)

    def _split_internal(
        self, txn: Transaction, page: Page, keys: list, children: list
    ) -> Tuple[tuple, int]:
        mid = len(keys) // 2
        sep = keys[mid]
        old_high = self._read_internal(page)[2]
        old_next = page.next_page
        right = self._bp.new_page(Page)  # new_page 已 pin，勿再 fetch（否则双 pin 泄漏）
        right_id = right.page_id
        try:
            self._init_internal(right)
            # 右页继承旧 high key，next 指向旧后继，prev 指向左页
            self._commit_page(
                txn,
                right,
                self._encode_internal(
                    right_id,
                    keys[mid + 1 :],
                    children[mid + 1 :],
                    old_next,
                    page.page_id,
                    old_high,
                ),
            )
            # 旧后继页的 prev 要指回新的右页（与叶分裂同款）
            if old_next != INVALID_PAGE_ID:
                nxt = self._bp.fetch_page(old_next)
                try:
                    with nxt.latch:
                        if nxt.prev_page != page.page_id:
                            raise BTreeInvariantError(
                                f"内部页链不一致：页 {old_next} 的 prev 是 {nxt.prev_page}，"
                                f"期望 {page.page_id}"
                            )
                        old_prev = bytes(nxt.buf[12:16])
                        new_prev = struct.pack("<I", right_id)
                        self._bp.log_page_update(txn.xid, nxt, 12, old_prev, new_prev)
                        nxt.buf[12:16] = new_prev
                        self._bp.mark_dirty(nxt)
                finally:
                    self._bp.unpin(old_next, dirty=True)

            # 左页 high key = sep（右兄弟最小键），next 指向右页
            self._commit_page(
                txn,
                page,
                self._encode_internal(
                    page.page_id,
                    keys[:mid],
                    children[: mid + 1],
                    right_id,
                    page.prev_page,
                    sep,
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
        deleted, root_empty = self._delete_recursive(txn, self.root_page, key)
        if deleted and root_empty:
            self._collapse_root(txn)
        return deleted

    def delete_key(self, txn: Transaction, values: Sequence[Any]) -> int:
        rids = self.lookup(txn, values)
        for rid in rids:
            self.delete(txn, values, rid)
        return len(rids)

    def _delete_recursive(
        self, txn: Transaction, page_id: int, key: tuple
    ) -> tuple[bool, bool]:
        """删除 key，返回 ``(deleted, page_now_empty)``。

        ``page_now_empty`` 表示本页已经彻底空掉（叶子 0 键 / 内部页 0 个孩子），
        需要上层把它从 ``children`` 里摘掉。这里必须区分两种「空」：

        * 叶子 0 键 → 真空，摘掉并修叶子链；
        * 内部页「0 个孩子」→ 真空，向上塌陷；
        * 内部页「只剩 1 个孩子」→ **不算空**，保留为退化单孩子页。

        保留退化页是关键：若把它的唯一孩子（一片叶子）提升到内部页层，
        这条路径的叶子深度会少一层，其余路径不变，``check_invariants`` 的
        「叶子深度一致」当场失败（实测删掉前 1000 条后 depth 变成 {3,4}）。
        只有等到这片叶子也空掉，退化页才会跟着变空并整体塌陷。
        """
        page = self._bp.fetch_page(page_id)
        try:
            if self._is_leaf(page):
                # 定位 → 读键 → 删必须在同一个页 latch 内：两个线程并发删
                # 同一片叶的不同键时，各自基于旧页算 idx，后写者会把先写者
                # 的删除结果覆盖回去（实测 300 条叶双线程删 200 条，丢 27 条）。
                with page.latch:
                    idx = self._leaf_bisect(page, key)
                    if idx >= self._nkeys(page):
                        return False, False
                    existing, _, _ = self._leaf_key_at(page, idx)
                    if existing != key:
                        return False, False
                    self._leaf_delete_at(txn, page, idx)
                    return True, self._nkeys(page) == 0

            # 内部页：读孩子下标 → 递归下探 → 摘除空孩子必须在同一个页 latch 内。
            # 否则两个线程在同一父页下各摘一个孩子，各自基于旧 children 列表算
            # idx，后写者会把先写者摘掉的那个槽位又摘一次（删错孩子/叶子链断）。
            # 锁序是严格的「父 → 子」下行（孩子自身在递归里再拿自己的 latch），
            # 返回时先释放孩子再释放父；没有任何路径是「子 → 父」，故不成环。
            with page.latch:
                keys, children, _ = self._read_internal(page)
                idx = self._child_index(keys, key)
                deleted, child_empty = self._delete_recursive(txn, children[idx], key)
                if not deleted:
                    return False, False
                if not child_empty:
                    return True, False
                if len(children) <= 1:
                    # 本页只有这一个孩子，摘掉就成 0 个孩子：交给上层继续塌陷。
                    return True, True
                self._remove_child(txn, page, idx)
                return True, False
        finally:
            self._bp.unpin(page_id, dirty=True)

    def _remove_child(self, txn: Transaction, parent: Page, idx: int) -> None:
        """摘除已空的 ``parent.children[idx]``，并修复叶子链。

        调用方保证 ``len(children) > 1``：只剩一个孩子的退化情形由
        ``_delete_recursive`` 自己向上返回「本页变空」，不在这里摘成 0 个孩子。
        """
        keys, children, high_key = self._read_internal(parent)
        if idx >= len(children):
            return
        child_id = children[idx]
        child = self._bp.fetch_page(child_id)
        try:
            if self._nkeys(child) != 0:
                return
            if self._is_leaf(child):
                self._unlink_leaf(txn, child)
            else:
                # 内部页 child：它整棵子树都已经空了，必须把里面每一片空叶
                # 也从叶子链上摘掉。只摘内部页会留下"孤儿叶"——真实 bug：
                # 删掉前 1000 条后，剩下的最左叶 prev 还指向已删除的页 237。
                self._purge_empty_tree(txn, child)
        finally:
            self._bp.unpin(child_id)

        del children[idx]
        if idx > 0:
            del keys[idx - 1]
        elif keys:
            del keys[0]
        self._commit_page(
            txn,
            parent,
            self._encode_internal(
                parent.page_id, keys, children, parent.next_page, parent.prev_page,
                high_key,
            ),
        )

    def _unlink_leaf(self, txn: Transaction, leaf: Page) -> None:
        """把一片空叶从叶子链上摘掉：前后邻居互接，叶子自身指针清零。

        边界情形也要处理：最左叶的前驱是 INVALID 时，右邻居的 prev
        必须被置成 INVALID，不能因为"没有左邻居"就跳过。
        """
        prev_id, next_id = leaf.prev_page, leaf.next_page
        if prev_id != INVALID_PAGE_ID:
            self._set_left_next(txn, prev_id, next_id, leaf.page_id)
        if next_id != INVALID_PAGE_ID:
            self._set_right_prev(txn, next_id, prev_id, leaf.page_id)
        self._clear_links(txn, leaf)

    def _purge_empty_tree(self, txn: Transaction, page: Page) -> None:
        """把一棵整体已空的子树的叶子全部从叶子链上摘干净（内部页递归下探）。

        逐叶修链对这种连续子树也成立：被摘的叶在链上首尾相接，每摘一片，
        它的前驱 next 就自动指向下个仍待摘的叶，最后落在外部的后继上。
        """
        if self._is_leaf(page):
            self._unlink_leaf(txn, page)
            return
        ckeys, cchildren, _ = self._read_internal(page)
        for cid in cchildren:
            c = self._bp.fetch_page(cid)
            try:
                self._purge_empty_tree(txn, c)
            finally:
                self._bp.unpin(cid)

    def _set_left_next(
        self, txn: Transaction, page_id: int, value: int, expected: int
    ) -> None:
        """把左邻居的 next 指针改成 ``value``；它当前必须指向被摘除页。"""
        page = self._bp.fetch_page(page_id)
        try:
            with page.latch:
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
            with page.latch:
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
        with page.latch:
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
        # 根塌陷改的是根页内容，与根分裂（建新根）并发会互相覆盖；树级锁串行，
        # 锁序保持 _root_lock → 根页 latch，与 insert 建新根路径一致，无 ABBA。
        with self._root_lock:
            self._collapse_root_locked(txn)

    def _collapse_root_locked(self, txn: Transaction) -> None:
        while self.root_page != INVALID_PAGE_ID:
            root = self._bp.fetch_page(self.root_page)
            try:
                if self._is_leaf(root):
                    return
                keys, children, _ = self._read_internal(root)
                if len(children) == 0:
                    # 根被删成了 0 个孩子的空内部页：整棵树已经空。
                    # 正常删除路径不会走到这里（_delete_recursive 只对
                    # 「1 个孩子」返回变空、不真摘），纯属防御。
                    self.root_page = INVALID_PAGE_ID
                    return
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

    def _leaf_page_view(
        self, page: Page, low: Optional[tuple]
    ) -> Tuple[list, list, int]:
        """latch 内取「从 low 起」的键/rid 列表与 next；``low=None`` 表示整页。

        扫描器不能"先 bisect 再逐键读"：那两步之间写者换页会让下标与槽位表
        来自不同版本。整段放进同一把 latch，顺带把 next 一起取出来，保证
        "本页读到的内容"与"接下来去哪一页"是同一个时刻的视图。
        """
        with page.latch:
            if low is None:
                keys, rids, _ = self._read_leaf(page)
            else:
                start = self._leaf_bisect(page, low)
                keys, rids, _ = self._read_leaf(page)
                keys, rids = keys[start:], rids[start:]
            return keys, rids, page.next_page

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
            keys, rids, next_id = self._leaf_page_view(page, low)
            while True:
                for k, rid in zip(keys, rids):
                    if k < low or (not low_inclusive and k == low):
                        continue
                    if k > high or (not high_inclusive and k == high):
                        return
                    yield k, rid
                if next_id == INVALID_PAGE_ID:
                    return
                current = page
                page = None  # 先置空，避免 finally 重复 unpin
                self._bp.unpin(current.page_id)
                page = self._bp.fetch_page(next_id)
                keys, rids, next_id = self._leaf_page_view(page, None)
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
        page = self._descend(low) if low is not None else self._leftmost_leaf_verified()
        if page is None:
            raise BTreeInvariantError(
                f"索引 {self.name} 的叶子链在并发改写中无法稳定读到链首"
            )
        try:
            keys, rids, next_id = self._leaf_page_view(page, low)
            while True:
                for k, rid in zip(keys, rids):
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
                if next_id == INVALID_PAGE_ID:
                    return
                current = page
                page = None
                self._bp.unpin(current.page_id)
                page = self._bp.fetch_page(next_id)
                keys, rids, next_id = self._leaf_page_view(page, None)
        finally:
            if page is not None:
                self._bp.unpin(page.page_id)

    def scan_all(self, txn: Transaction) -> Iterator[Tuple[tuple, RID]]:
        if self.root_page == INVALID_PAGE_ID:
            return
        page = self._leftmost_leaf_verified()
        if page is None:
            raise BTreeInvariantError(
                f"索引 {self.name} 的叶子链在并发改写中无法稳定读到链首"
            )
        try:
            while True:
                keys, rids, next_id = self._leaf_page_view(page, None)
                for k, rid in zip(keys, rids):
                    yield k, rid
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
                    keys, rids, leaf_high = self._read_leaf(page)
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
                        if leaf_high is not None and k >= leaf_high:
                            raise BTreeInvariantError(
                                f"页 {page_id} 的键不小于叶子 high key"
                            )
                    return

                keys, children, high_key = self._read_internal(page)
                if len(children) != len(keys) + 1:
                    raise BTreeInvariantError(
                        f"内部页 {page_id} 孩子数与键数不匹配"
                    )
                for i, child in enumerate(children):
                    lo = keys[i - 1] if i > 0 else low
                    # 最右孩子的上界是 high key（右兄弟最小键），没有 high key
                    # 才退回父传上界
                    hi = keys[i] if i < len(keys) else (
                        high_key if high_key is not None else high
                    )
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
