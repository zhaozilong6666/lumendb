from lumendb.storage.btree import _diff_ranges
from lumendb.storage.page import PAGE_SIZE


def test_diff_ranges_splits_disjoint_changes():
    old = bytes(PAGE_SIZE)
    new = bytearray(old)
    new[2:6] = b"abcd"
    new[300:304] = b"wxyz"
    ranges = _diff_ranges(old, bytes(new))
    assert ranges == [(2, 6), (300, 304)]


def test_diff_ranges_merges_short_gaps():
    old = bytes(PAGE_SIZE)
    new = bytearray(old)
    new[10:12] = b"ab"
    new[14:16] = b"cd"
    assert _diff_ranges(old, bytes(new)) == [(10, 16)]


def test_diff_ranges_covers_full_page_change():
    old = bytes(PAGE_SIZE)
    new = bytes([1]) * PAGE_SIZE
    assert _diff_ranges(old, new) == [(0, PAGE_SIZE)]
