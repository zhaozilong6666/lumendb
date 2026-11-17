"""副本 apply 的原子单元是事务：组边界不得切在事务中间。

背景：一条 DML 会拆成多条 WAL 记录（表页一条、每个索引页各一条）。
按"记录数"切组会让副本读者在组边界看到"表页已应用、索引页还没应用"，
顺序扫描与索引扫描对不上（严重时 `BTreeInvariantError`）。见
`docs/replication-correctness.md`。
"""

from __future__ import annotations

import threading
import time

from lumendb.engine.database import Database
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALServer
from lumendb.storage.wal import LogRecord, LogType


def _rec(lsn: int, rtype: int) -> LogRecord:
    return LogRecord(lsn=lsn, xid=7, rtype=rtype, payload=b"")


def _cutter() -> Replica:
    """只测纯函数 ``_transaction_cut``：不需要真的打开数据库。"""
    return Replica.__new__(Replica)


U, C, A = LogType.UPDATE, LogType.COMMIT, LogType.ABORT


def test_cut_lands_after_commit() -> None:
    todo = [_rec(1, U), _rec(2, U), _rec(3, C), _rec(4, U)]
    assert _cutter()._transaction_cut(todo, 0, 4, 4) == 3


def test_cut_falls_back_to_batch_tail_without_boundary() -> None:
    todo = [_rec(1, U), _rec(2, U)]
    assert _cutter()._transaction_cut(todo, 0, 2, 2) == 2


def test_cut_prefers_last_boundary_inside_window() -> None:
    todo = [
        _rec(1, U),
        _rec(2, C),
        _rec(3, U),
        _rec(4, U),
        _rec(5, C),
        _rec(6, U),
    ]
    assert _cutter()._transaction_cut(todo, 0, 6, 3) == 2


def test_cut_extends_forward_to_keep_transaction_whole() -> None:
    """窗口内没有边界时向后延伸，绝不把一个事务切成两半。"""
    todo = [_rec(i, U) for i in range(1, 9)] + [_rec(9, C)]
    assert _cutter()._transaction_cut(todo, 0, 9, 2) == 9


def test_cut_counts_abort_as_boundary() -> None:
    todo = [_rec(1, U), _rec(2, A), _rec(3, U)]
    assert _cutter()._transaction_cut(todo, 0, 3, 3) == 2


def test_cut_respects_start_offset() -> None:
    todo = [_rec(1, C), _rec(2, U), _rec(3, U), _rec(4, C)]
    assert _cutter()._transaction_cut(todo, 1, 4, 1) == 3


def test_replica_never_sees_half_a_transaction(tmp_path) -> None:
    """端到端：一条 DML 拆成表页 + 索引页多条记录，chunk=1 也不能错位。

    判据是"同一个已应用前缀内，顺序扫描与索引扫描行数相同"，且过程中
    不出现 `BTreeInvariantError`。修复前同一场景能复现 7 次错位。
    """
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    primary.execute_one("CREATE INDEX idx_v ON t (v)")
    for i in range(40):
        primary.execute_one(f"INSERT INTO t VALUES ({i}, {i})")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(server.host, server.port, tmp_path / "replica")
    replica._apply_chunk_size = 1
    try:
        stop = threading.Event()
        errors: list[str] = []
        mismatches: list[tuple[int, int]] = []

        def writer() -> None:
            try:
                for i in range(40, 240):
                    primary.execute_one(f"INSERT INTO t VALUES ({i}, {i})")
            finally:
                stop.set()

        def applier() -> None:
            try:
                while not stop.is_set():
                    if not replica.replicate_from_server(
                        server.host, server.port, max_records=500
                    ):
                        time.sleep(0.001)
            except BaseException as exc:
                errors.append(f"applier: {exc!r}")

        def reader() -> None:
            try:
                while not stop.is_set():
                    with replica.snapshot_read():
                        a = replica.execute_one("SELECT COUNT(*) FROM t").rows[0][0]
                        b = replica.execute_one(
                            "SELECT COUNT(*) FROM t WHERE v >= 0"
                        ).rows[0][0]
                    if a != b:
                        mismatches.append((a, b))
            except BaseException as exc:
                errors.append(f"reader: {exc!r}")

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=applier),
            threading.Thread(target=reader),
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=60)

        assert errors == [], f"出现异常: {errors[:2]}"
        assert mismatches == [], f"同一前缀内两种扫描不一致: {mismatches[:5]}"
        # 追平到 240 行（批被事务边界截短，需要多轮）
        for _ in range(200):
            if replica.replicate_from_server(server.host, server.port, max_records=500) == 0:
                break
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(240,)]
    finally:
        replica.close()
        server.stop()
        primary.close()
