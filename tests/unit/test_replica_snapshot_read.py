"""副本一致快照读：`snapshot_read` 把多条语句固定在同一已应用前缀。

注意：本文件测的是 **API 语义**（上下文期间 apply 不交出锁、两次查询看到
同一个前缀）。"读者在组边界看到半个事务"那个缺口已在事务级应用单元里修掉
（分组只切在 COMMIT/ABORT 之后），见 `docs/replication-correctness.md`。
"""

from __future__ import annotations

import threading

from lumendb.engine.database import Database
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALServer


def _setup(tmp_path, rows: int = 40):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE a (id INT PRIMARY KEY)")
    primary.execute_one("CREATE TABLE b (id INT PRIMARY KEY)")
    for i in range(rows):
        primary.execute_one(f"INSERT INTO a VALUES ({i})")
        primary.execute_one(f"INSERT INTO b VALUES ({i})")
    server = WALServer(primary).start()
    replica = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "replica"
    )
    return primary, server, replica


def test_snapshot_read_holds_the_lock_against_apply(tmp_path):
    """上下文期间 apply 必须拿不到锁——否则它会在组边界插进来。"""
    primary, server, replica = _setup(tmp_path)
    try:
        with replica.snapshot_read():
            assert replica._snapshot_depth == 1
            blocked = threading.Event()

            def probe() -> None:
                # apply 的锁被快照读者持有，这里应当阻塞。
                if replica._lock.acquire(timeout=0.2):
                    replica._lock.release()
                    blocked.set()

            thread = threading.Thread(target=probe)
            thread.start()
            thread.join(timeout=1.0)
            assert not blocked.is_set(), "快照读期间不该让 apply 拿到锁"
        # 退出后锁立刻可用。
        assert replica._snapshot_depth == 0
        assert replica._lock.acquire(timeout=0.2)
        replica._lock.release()
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_snapshot_read_sees_a_consistent_prefix_across_two_tables(tmp_path):
    """同一事务里成对写入的两张表：快照读里两边的行数必须一致。"""
    primary, server, replica = _setup(tmp_path)
    replica._apply_chunk_size = 1
    observed = []
    stop = threading.Event()
    try:
        errors: list[str] = []

        def writer() -> None:
            # 一次 execute 里的 BEGIN…COMMIT 就是一个事务：a 与 b 必须
            # 一起可见，副本上不存在"只看到 a 的新行"的前缀。
            try:
                for i in range(40, 60):
                    primary.execute(
                        "BEGIN;"
                        f"INSERT INTO a VALUES ({i});"
                        f"INSERT INTO b VALUES ({i});"
                        "COMMIT"
                    )
            except BaseException as exc:  # noqa: BLE001 - 失败要能看见
                errors.append(f"writer: {exc!r}")
            finally:
                stop.set()

        def applier() -> None:
            while not stop.is_set():
                try:
                    replica.replicate_from_server(
                        server.host, server.port, max_records=200
                    )
                except Exception:  # 收工时连接已断
                    return

        def reader() -> None:
            rounds = 0
            while not stop.is_set() and rounds < 15:
                rounds += 1
                try:
                    with replica.snapshot_read():
                        ca = replica.execute_one("SELECT COUNT(*) FROM a").rows[0][0]
                        cb = replica.execute_one("SELECT COUNT(*) FROM b").rows[0][0]
                except Exception:  # 收工时副本句柄已关闭，正常退出
                    return
                observed.append((ca, cb))

        threads = [
            threading.Thread(target=writer, daemon=True),
            threading.Thread(target=applier, daemon=True),
            threading.Thread(target=reader, daemon=True),
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=15)

        assert errors == [], f"写线程/apply 出错（测试会假相通过）: {errors[:2]}"
        assert observed, "读者至少应该成功读到一次"
        bad = [pair for pair in observed if pair[0] != pair[1]]
        assert not bad, f"快照读看到了不一致的跨表前缀: {bad[:6]}"
    finally:
        replica.close()
        server.stop()
        primary.close()


def test_snapshot_read_is_reentrant_and_exception_safe(tmp_path):
    primary, server, replica = _setup(tmp_path, rows=10)
    try:
        with replica.snapshot_read():
            with replica.snapshot_read():
                assert replica.execute_one("SELECT COUNT(*) FROM a").rows == [(10,)]
            assert replica._snapshot_depth == 1
        assert replica._snapshot_depth == 0
        try:
            with replica.snapshot_read():
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert replica._snapshot_depth == 0, "异常退出必须把计数还回去"
    finally:
        replica.close()
        server.stop()
        primary.close()
