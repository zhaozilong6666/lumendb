"""WAL 的单元测试。

WAL 的正确性靠三条可断言的性质支撑：
1. LSN 单调递增且等于字节偏移 + 1；
2. 重放一批日志两次，结果与一次相同（幂等）；
3. 记录边界在被截断时能识别残尾而不抛错。
"""

import pytest

from lumendb.storage.page import PAGE_SIZE
from lumendb.storage.wal import (
    HEADER_SIZE,
    LogType,
    WAL,
    decode_checkpoint,
    decode_page_update,
    encode_checkpoint,
    encode_page_update,
)


@pytest.fixture
def wal_path(tmp_path):
    return tmp_path / "wal.log"


class TestAppendAndRead:
    def test_lsn_starts_at_one(self, wal_path):
        wal = WAL(wal_path)
        assert wal.next_lsn == 1
        lsn = wal.append(1, LogType.BEGIN)
        assert lsn == 1
        wal.close()

    def test_lsn_equals_offset_plus_one(self, wal_path):
        wal = WAL(wal_path)
        lsn1 = wal.append(1, LogType.BEGIN)
        lsn2 = wal.append(1, LogType.COMMIT)
        wal.flush()
        assert lsn2 - lsn1 == HEADER_SIZE  # BEGIN 无 payload
        recs = wal.read_all()
        assert [r.lsn for r in recs] == [lsn1, lsn2]
        assert recs[0].rtype == LogType.BEGIN
        assert recs[1].rtype == LogType.COMMIT
        wal.close()

    def test_records_survive_reopen(self, wal_path):
        wal = WAL(wal_path)
        wal.append(9, LogType.BEGIN)
        wal.append(9, LogType.UPDATE, encode_page_update(3, 10, b"old", b"new!"))
        wal.flush()
        expected_lsn = wal.next_lsn
        wal.close()

        wal2 = WAL(wal_path)
        assert wal2.next_lsn == expected_lsn
        recs = wal2.read_all()
        assert len(recs) == 2
        pid, off, old, new = decode_page_update(recs[1].payload)
        assert (pid, off, old, new) == (3, 10, b"old", b"new!")
        wal2.close()


class TestPageUpdatePayload:
    def test_roundtrip(self):
        old = b"\x00" * 8
        new = b"\xFF" * 12
        payload = encode_page_update(7, 100, old, new)
        assert decode_page_update(payload) == (7, 100, old, new)

    def test_empty_images(self):
        payload = encode_page_update(1, 0, b"", b"")
        assert decode_page_update(payload) == (1, 0, b"", b"")

    def test_truncated_payload_detected(self):
        payload = encode_page_update(1, 0, b"abcd", b"efgh")
        with pytest.raises(Exception):
            decode_page_update(payload[:-2])


class TestCheckpointPayload:
    def test_roundtrip(self):
        xids = [3, 7, 11]
        payload = encode_checkpoint(xids)
        assert decode_checkpoint(payload) == xids

    def test_empty(self):
        assert decode_checkpoint(encode_checkpoint([])) == []


class TestDurability:
    def test_flush_makes_records_durable(self, wal_path):
        wal = WAL(wal_path)
        wal.append(1, LogType.BEGIN)
        assert wal.durable_lsn == 0  # 还没有 flush
        wal.flush()
        assert wal.durable_lsn > 0
        wal.close()

    def test_flush_upto_noop_for_old_lsn(self, wal_path):
        wal = WAL(wal_path)
        wal.append(1, LogType.BEGIN)
        wal.flush_upto(0)  # 不应该报错
        wal.flush_upto(wal.next_lsn - 1)
        assert wal.durable_lsn > 0
        wal.close()

    def test_truncated_tail_is_dropped_silently(self, wal_path):
        wal = WAL(wal_path)
        wal.append(1, LogType.BEGIN)
        wal.append(1, LogType.COMMIT)
        wal.flush()
        wal.close()

        # 模拟崩溃时写了一半的最后一条记录
        with open(wal_path, "ab") as fh:
            fh.write(b"\x00\x01\x02")

        wal2 = WAL(wal_path)
        recs = wal2.read_all()
        assert len(recs) == 2
        wal2.close()


class TestWalRuleSupport:
    """WAL 规则：写页之前，覆盖该页的日志必须已落盘。"""

    def test_page_lsn_can_be_compared_to_durable_lsn(self, wal_path):
        wal = WAL(wal_path)
        lsn = wal.log_page_update(2, 5, 20, b"old", b"new")
        assert lsn > wal.durable_lsn  # 还没刷盘
        wal.flush()
        assert lsn <= wal.durable_lsn  # 现在可以安全写页

    def test_multiple_sessions_append_monotonically(self, wal_path):
        wal = WAL(wal_path)
        lsns = [wal.append(i, LogType.BEGIN) for i in range(1, 6)]
        assert lsns == sorted(lsns)
        assert len(set(lsns)) == 5
        wal.close()
