"""分段 WAL 的轮转、重启与安全截断。"""

from __future__ import annotations

import threading

import lumendb.storage.wal_segments as wal_segments
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

    def test_append_during_flush_is_not_blocked_or_lost(self, tmp_path, monkeypatch):
        """leader 在 fsync 期间必须让其它线程继续 append。

        旧实现把整个 flush（含 fsync）包在 _lock 里，提交线程会被磁盘等待
        卡住，组提交批次被压到 1 个事务；这里用一次可控的慢 fsync 验证
        append 不再阻塞，并且下一轮 flush 能把它落盘。
        """
        wal = make_wal(tmp_path, segment_bytes=1024)
        first = wal.append(1, LogType.BEGIN)
        entered = threading.Event()
        release = threading.Event()
        real_fsync = wal_segments.os.fsync
        calls = {"n": 0}

        def blocking_fsync(fd):
            calls["n"] += 1
            if calls["n"] == 1:
                entered.set()
                assert release.wait(timeout=5)
            return real_fsync(fd)

        monkeypatch.setattr(wal_segments.os, "fsync", blocking_fsync)

        errors: list[BaseException] = []

        def leader() -> None:
            try:
                wal.flush()
            except BaseException as exc:  # noqa: BLE001 - 带回主线程断言
                errors.append(exc)

        flush_thread = threading.Thread(target=leader)
        flush_thread.start()
        assert entered.wait(timeout=5), "flush 没有进入 fsync"

        appended: list[int] = []

        def writer() -> None:
            appended.append(wal.append(2, LogType.COMMIT))

        append_thread = threading.Thread(target=writer)
        append_thread.start()
        append_thread.join(timeout=1.0)
        assert not append_thread.is_alive(), "append 被正在 fsync 的 flush 阻塞了"
        release.set()
        flush_thread.join(timeout=5)
        assert not flush_thread.is_alive()
        assert errors == []

        wal.flush()
        records = wal.read_all()
        assert [record.lsn for record in records] == [first, appended[0]]
        assert [record.xid for record in records] == [1, 2]
        wal.close()
