"""第三个 Cherry 场景：副本 apply 中途换主。

固定交错：

1. 副本先应用完第一个事务，在事务组边界把数据锁交给读者（``apply`` 的
   ``_pending_reads`` 让出窗口）；
2. 闸门停在这里，主线程执行 ``promote``，把只读句柄换成读写句柄；
3. 放行后旧 apply 循环发现 ``self.db is not database``，把剩余记录放回暂存区，
   不写进已经关闭的旧句柄；
4. 新主库再从暂存区幂等重放，最终所有行都在。

如果换主时旧 apply 继续写旧句柄，最后查询会少行——不是崩溃，而是静默丢数据。
"""

from __future__ import annotations

import pathlib
import threading
import time

import lumendb.replication.replica as replica_module
from lumendb.engine.database import Database
from lumendb.replication.replica import Replica

from .trace import HoldPoint, Trace


def run_replica_case(seed: int, tmp_path: pathlib.Path) -> tuple[Trace, dict]:
    primary = Database(tmp_path / "primary", concurrent_dml=True)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    primary.execute_one("INSERT INTO t VALUES (1, 1)")
    replica = Replica.bootstrap(primary, tmp_path / "replica")
    # 每条事务单独成组，才能停在“第一组已应用、后续组未应用”的窗口。
    replica._apply_chunk_size = 1
    replica._pending_reads = 1  # 让 apply 在组边界走真正的 sleep 让出窗口
    trace = Trace()
    hold = HoldPoint(f"replica{seed}.after_first_chunk", trace)
    errors: list[str] = []
    original_sleep = replica_module.time.sleep

    def fake_sleep(seconds: float) -> None:
        if threading.current_thread().name == "replica-applier":
            trace.mark("apply.yield", applied=replica._applied_lsn)
            hold.wait()
            trace.mark("apply.resume")
            return
        original_sleep(seconds)

    for offset in range(3):
        row_id = seed + 2 + offset
        primary.execute_one(f"INSERT INTO t VALUES ({row_id}, {row_id})")

    def applier() -> None:
        try:
            replica.apply_from_primary(primary)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"apply: {type(exc).__name__}: {exc}")

    replica_module.time.sleep = fake_sleep
    thread = threading.Thread(target=applier, name="replica-applier")
    try:
        thread.start()
        assert hold.entered.wait(timeout=10), f"apply 没停在组边界；\n{trace.dump()}"
        trace.mark("promote.start")
        replica.promote(node_id="replica")
        trace.mark("promote.end")
        assert trace.count("apply.yield") == 1
        assert trace.first_seq("apply.yield") < trace.first_seq("promote.start")
        hold.release()
        thread.join(timeout=30)
        assert not thread.is_alive(), "apply 线程未退出"
        assert errors == [], f"换主场景出错: {errors[:2]}；\n{trace.dump()}"
        # 换主时剩余记录必须回到暂存区，而不是写进旧句柄。
        assert replica._pending_records, "换主后剩余记录丢了"
        applied = replica.apply([])
        assert applied > 0, "暂存区没有被新主库重放"
        rows = replica.db.execute_one("SELECT id, v FROM t ORDER BY id").rows
        expected = [(1, 1)] + [(seed + 2 + i, seed + 2 + i) for i in range(3)]
        assert rows == expected, f"换主后丢行: {rows}，期望 {expected}；\n{trace.dump()}"
        return trace, {"rows": rows, "applied_after_promote": applied}
    finally:
        replica_module.time.sleep = original_sleep
        hold.release()
        replica.db.close()
        primary.close()
