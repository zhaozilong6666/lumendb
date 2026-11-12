"""页格式的单元测试。

重点不是"能读能写"，而是几条结构性不变量：
* RID 稳定（删除不复用槽位）
* 空闲空间核算准确
* compact 之后存活记录内容不变
* 损坏页能被自检抓住
"""

import random

import pytest

from lumendb.errors import CorruptedPageError
from lumendb.storage.page import (
    HEADER_SIZE,
    INVALID_PAGE_ID,
    PAGE_SIZE,
    HeapPage,
    Page,
    PageType,
)


class TestRawPage:
    def test_default_is_empty(self):
        p = Page(3)
        assert p.page_type == PageType.FREE
        assert len(p.buf) == PAGE_SIZE

    def test_wrong_size_rejected(self):
        with pytest.raises(ValueError):
            Page(0, bytes(100))

    def test_int_roundtrip(self):
        p = Page(0)
        p.set_u16(10, 0xBEEF)
        assert p.get_u16(10) == 0xBEEF
        p.set_u32(20, 0xDEADBEEF)
        assert p.get_u32(20) == 0xDEADBEEF
        p.set_u64(28, 2**63 + 7)
        assert p.get_u64(28) == 2**63 + 7


class TestHeapPageBasics:
    def make(self, page_id=1):
        p = HeapPage(page_id)
        p.init_heap(page_id)
        return p

    def test_init_state(self):
        p = self.make()
        assert p.page_type == PageType.HEAP
        assert p.nslots == 0
        assert p.free_start == HEADER_SIZE
        assert p.free_end == PAGE_SIZE
        assert p.free_space == PAGE_SIZE - HEADER_SIZE
        assert p.next_page == INVALID_PAGE_ID
        p.check_integrity()

    def test_insert_and_read(self):
        p = self.make()
        slot = p.insert_record(b"hello")
        assert slot == 0
        assert p.get_record(0) == b"hello"
        assert p.free_space == PAGE_SIZE - HEADER_SIZE - 4 - 5
        p.check_integrity()

    def test_multiple_records_are_independent(self):
        p = self.make()
        payloads = [bytes([i]) * (i + 1) for i in range(5)]
        for pl in payloads:
            p.insert_record(pl)
        for i, pl in enumerate(payloads):
            assert p.get_record(i) == pl
        p.check_integrity()

    def test_empty_record_rejected(self):
        p = self.make()
        with pytest.raises(ValueError):
            p.insert_record(b"")

    def test_fill_until_full(self):
        p = self.make()
        inserted = 0
        while p.can_fit(100):
            assert p.insert_record(b"x" * 100) == inserted
            inserted += 1
        assert p.insert_record(b"x" * 100) is None
        assert p.free_space < 100 + 4
        p.check_integrity()

    def test_record_too_large_never_silently_truncated(self):
        p = self.make()
        huge = b"y" * (PAGE_SIZE + 10)
        assert p.insert_record(huge) is None
        assert p.nslots == 0


class TestDeletionSemantics:
    def test_tombstone_is_not_reused(self):
        """RID 稳定性：删掉的槽位不能被后来的插入复用。"""
        p = HeapPage(1)
        p.init_heap(1)
        p.insert_record(b"a")
        p.insert_record(b"b")
        p.delete_record(0)
        assert p.get_record(0) is None
        new_slot = p.insert_record(b"c")
        assert new_slot == 2  # 而不是 0
        assert p.get_record(1) == b"b"
        assert p.get_record(2) == b"c"
        p.check_integrity()

    def test_delete_twice_returns_false(self):
        p = HeapPage(1)
        p.init_heap(1)
        p.insert_record(b"a")
        assert p.delete_record(0) is True
        assert p.delete_record(0) is False
        assert p.delete_record(99) is False

    def test_out_of_range_reads(self):
        p = HeapPage(1)
        p.init_heap(1)
        assert p.get_record(-1) is None
        assert p.get_record(0) is None


class TestCompact:
    def test_compact_preserves_live_records(self):
        p = HeapPage(1)
        p.init_heap(1)
        payloads = [f"row-{i}".encode() for i in range(10)]
        for pl in payloads:
            p.insert_record(pl)
        for slot in range(0, 10, 2):
            p.delete_record(slot)

        before_free = p.free_space
        reclaimed = p.compact()
        assert reclaimed > 0
        assert p.free_space > before_free

        for slot in range(1, 10, 2):
            assert p.get_record(slot) == payloads[slot]
        for slot in range(0, 10, 2):
            assert p.get_record(slot) is None
        p.check_integrity()

    def test_compact_is_idempotent(self):
        p = HeapPage(1)
        p.init_heap(1)
        for i in range(5):
            p.insert_record(b"z" * 50)
        p.delete_record(0)
        p.compact()
        first_free = p.free_space
        assert p.compact() == 0
        assert p.free_space == first_free
        p.check_integrity()

    def test_compact_on_empty_page(self):
        p = HeapPage(1)
        p.init_heap(1)
        assert p.compact() == 0
        assert p.free_space == PAGE_SIZE - HEADER_SIZE


class TestIntegrityCheck:
    def test_detects_wrong_page_id(self):
        p = HeapPage(1)
        p.init_heap(1)
        p.set_u32(16, 2)  # 手动篡改页头
        with pytest.raises(CorruptedPageError):
            p.check_integrity()

    def test_detects_overlapping_free_pointers(self):
        p = HeapPage(1)
        p.init_heap(1)
        p.insert_record(b"abc")
        p.set_u16(4, PAGE_SIZE - 1)  # free_start 被人为推到页尾
        with pytest.raises(CorruptedPageError):
            p.check_integrity()

    def test_detects_slot_pointing_outside(self):
        p = HeapPage(1)
        p.init_heap(1)
        p.insert_record(b"abc")
        p.set_u16(HEADER_SIZE, 0)  # 槽位偏移指向页头
        with pytest.raises(CorruptedPageError):
            p.check_integrity()


class TestRandomizedAgainstModel:
    def test_random_operations_match_dict_model(self):
        """随机插入/删除/压缩，与 dict 模型逐条对照。"""
        rng = random.Random(20261112)
        p = HeapPage(1)
        p.init_heap(1)
        model = {}

        def try_insert(payload):
            slot = p.insert_record(payload)
            if slot is not None:
                model[slot] = payload
            return slot

        for step in range(400):
            op = rng.random()
            if op < 0.7:
                payload = bytes(rng.randrange(256) for _ in range(rng.randint(1, 40)))
                try_insert(payload)
            elif op < 0.9 and model:
                slot = rng.choice(list(model))
                assert p.delete_record(slot) is True
                del model[slot]
            else:
                p.compact()

            if step % 25 == 0:
                p.check_integrity()
                for slot, payload in model.items():
                    assert p.get_record(slot) == payload
                # 已删除的槽位必须保持 None
                for slot in range(p.nslots):
                    if slot not in model:
                        assert p.get_record(slot) is None
