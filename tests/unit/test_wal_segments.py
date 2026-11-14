"""分段 WAL 的轮转、重启与安全截断。"""

from __future__ import annotations

import threading

from lumendb.storage.wal import LogType, encode_page_update
from lumendb.storage.wal_segments import SegmentedWAL


def make_wal(tmp_path, segment_bytes=64):
    return SegmentedWAL(tmp_path / "wal", segment_bytes=segment_bytes)


class TestSegmentedWal:
    def test_segments_rotate_and_reopen(self, tmp_path):
        wal = make_wal(tmp_path)
        lsns = [
            wal.append(1, LogType.UPDATE, encode_page_update(1, i, b"o", b"n" * 20))
            for i in range(12)
        ]
        wal.flush()
        expected_next = wal.next_lsn
        assert len(wal.segments) > 1
        records = wal.read_all()
        assert [record.lsn for record in records] == lsns
        wal.close()

        reopened = make_wal(tmp_path)
        assert reopened.next_lsn == expected_next
        assert len(reopened.read_all()) == 12
        reopened.close()

    def test_truncate_before_keeps_segment_containing_safe_lsn(self, tmp_path):
        wal = make_wal(tmp_path, segment_bytes=17)
        lsns = [wal.append(1, LogType.BEGIN) for _ in range(10)]
        wal.flush()
        before = len(wal.segments)
        assert before > 1

        removed = wal.truncate_before(lsns[4])
        assert removed > 0
        assert len(wal.segments) < before
        assert wal.base_lsn == lsns[4]
        assert [record.lsn for record in wal.read_all()] == lsns[4:]
        wal.close()

    def test_retain_lsn_prevents_checkpoint_from_dropping_replica_window(self, tmp_path):
        wal = make_wal(tmp_path, segment_bytes=17)
        lsns = [wal.append(1, LogType.BEGIN) for _ in range(10)]
        wal.flush()
        wal.retain_lsn = lsns[4]

        wal.truncate_before(lsns[8])
        retained = [record.lsn for record in wal.read_all()]
        assert lsns[4] in retained or wal.base_lsn <= lsns[4]
        assert all(lsn >= wal.base_lsn for lsn in retained)

        wal.retain_lsn = 0
        wal.truncate_before(lsns[8])
        assert wal.base_lsn >= lsns[4]
        assert all(lsn >= lsns[4] for lsn in [record.lsn for record in wal.read_all()])
        wal.close()

    def test_partial_tail_is_ignored_and_append_continues(self, tmp_path):
        wal = make_wal(tmp_path)
        wal.append(1, LogType.BEGIN)
        wal.append(1, LogType.UPDATE, encode_page_update(1, 0, b"a", b"b"))
        wal.flush()
        expected_next = wal.next_lsn
        last = wal.segments[-1]
        wal.close()

        with open(last, "ab") as fh:
            fh.write(b"\x00" * 7)  # 崩溃时写到一半的记录头

        reopened = make_wal(tmp_path)
        assert reopened.next_lsn == expected_next
        assert len(reopened.read_all()) == 2
        reopened.append(2, LogType.COMMIT)
        reopened.flush()
        assert [record.xid for record in reopened.read_all()] == [1, 1, 2]
        reopened.close()

    def test_concurrent_appends_keep_every_record(self, tmp_path):
        wal = make_wal(tmp_path, segment_bytes=128)
        barrier = threading.Barrier(6)
        threads = []

        def worker(worker_id: int) -> None:
            barrier.wait()
            for i in range(20):
                wal.append(worker_id * 100 + i, LogType.BEGIN)

        for worker_id in range(6):
            thread = threading.Thread(target=worker, args=(worker_id,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        wal.flush()
        records = wal.read_all()
        assert len(records) == 120
        assert len({record.lsn for record in records}) == 120
        wal.close()
