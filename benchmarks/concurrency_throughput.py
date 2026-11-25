"""并发 DML 吞吐基准：默认写锁串行 vs ``concurrent_dml``。

只为量化，不参与 CI。两个模式各自新建数据库，跑同一组负载：
- insert：每个线程独立事务插入固定行数，主键与二级索引都写；
- hot_update：所有线程更新同一行，观察行锁争用下的退化。

用法：
    python benchmarks/concurrency_throughput.py
    python benchmarks/concurrency_throughput.py --threads 8 --rows 2000
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lumendb.engine.database import Database
from lumendb.errors import SerializationError
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql
from lumendb.storage.heap import HeapFile
from lumendb.storage.page import INVALID_PAGE_ID, HeapPage


def _run(server: PGWireServer, session: Session, sql: str):
    return [server._run_statement(session, statement) for statement in parse_sql(sql)]


def _diagnose_heap(database: Database) -> dict:
    """COUNT 对不上时保存页链现场，避免只看到“少了 1300 行”却不知道哪页断。"""
    table = database.catalog.get_table("bench")
    diag: dict = {
        "root_page": table.root_page,
        "last_page": table.last_page,
        "row_count_hint": table.row_count_hint,
        "disk_pages": database.disk.num_pages,
    }
    pages: list[dict] = []
    seen: set[int] = set()
    page_id = table.root_page
    try:
        while page_id != INVALID_PAGE_ID:
            if page_id in seen:
                diag["cycle_at"] = page_id
                break
            seen.add(page_id)
            current = page_id
            page = database.bp.fetch_page(current, HeapPage)
            try:
                pages.append(
                    {
                        "page": current,
                        "type": page.page_type,
                        "page_id": page.page_id,
                        "nslots": page.nslots,
                        "next": page.next_page,
                    }
                )
                page_id = page.next_page
            finally:
                database.bp.unpin(current)
    except Exception as exc:  # noqa: BLE001 - 诊断路径要把异常本身带回去
        diag["chain_error"] = repr(exc)
    diag["chain"] = pages

    txn = database.txns.begin(read_only=True)
    try:
        diag["direct_scan_count"] = sum(
            1 for _ in HeapFile(database.bp, table, database.txns).scan(txn)
        )
    except Exception as exc:  # noqa: BLE001
        diag["scan_error"] = repr(exc)
    finally:
        database.txns.commit(txn)
    return diag


def _check_result(
    database: Database,
    count: int,
    hot_value: int,
    threads: int,
    rows: int,
    updates: int,
) -> None:
    """基准不只打印数字：计数或热点值不符就失败，并带上页链现场。"""
    expected_rows = threads * rows + 1
    if count != expected_rows:
        diag = _diagnose_heap(database)
        raise RuntimeError(
            f"COUNT 不一致：期望 {expected_rows}，实际 {count}；"
            + json.dumps(diag, ensure_ascii=False)
        )
    expected_hot = threads * updates
    if hot_value != expected_hot:
        raise RuntimeError(
            f"热点值不一致：期望 {expected_hot}，实际 {hot_value}"
        )


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * p))))
    return ordered[index]


def _summary(latencies: list[float], elapsed: float, ops: int) -> dict:
    return {
        "ops": ops,
        "elapsed_s": round(elapsed, 3),
        "ops_per_s": round(ops / elapsed, 1) if elapsed else 0.0,
        "p50_ms": round(_percentile(latencies, 0.50) * 1000, 2),
        "p95_ms": round(_percentile(latencies, 0.95) * 1000, 2),
        "p99_ms": round(_percentile(latencies, 0.99) * 1000, 2),
        "max_ms": round(max(latencies, default=0.0) * 1000, 2),
        "mean_ms": round(statistics.mean(latencies) * 1000, 2) if latencies else 0.0,
    }


def _run_threads(threads: int, fn) -> list[str]:
    errors: list[str] = []
    start = threading.Barrier(threads + 1)

    def wrap(worker: int) -> None:
        try:
            start.wait(timeout=30)
            fn(worker)
        except Exception as exc:  # noqa: BLE001 - 基准要把异常带回主线程
            errors.append(f"worker {worker}: {exc!r}")

    workers = [threading.Thread(target=wrap, args=(i,)) for i in range(threads)]
    for worker in workers:
        worker.start()
    started = time.perf_counter()
    start.wait(timeout=30)
    for index, worker in enumerate(workers):
        worker.join(timeout=300)
        if worker.is_alive():
            # 之前这里直接返回：慢机器上还有 worker 在写，主线程就开始
            # COUNT/热点读，基准会把“尚未写完”误报成丢行。超时必须显式失败。
            errors.append(f"worker {index}: 未在 300s 内结束")
    return errors, time.perf_counter() - started


def _bench_insert(server: PGWireServer, threads: int, rows_per_thread: int) -> dict:
    lock = threading.Lock()
    latencies: list[float] = []

    def worker(worker_id: int) -> None:
        session = Session(database=server.database)
        base = worker_id * rows_per_thread
        for offset in range(rows_per_thread):
            row_id = base + offset
            started = time.perf_counter()
            _run(
                server,
                session,
                f"INSERT INTO bench (id, v, pad) VALUES ({row_id}, {row_id % 17}, 'p{row_id}')",
            )
            elapsed = time.perf_counter() - started
            with lock:
                latencies.append(elapsed)

    errors, elapsed = _run_threads(threads, worker)
    if errors:
        raise RuntimeError("; ".join(errors))
    return _summary(latencies, elapsed, threads * rows_per_thread)


def _bench_hot_update(server: PGWireServer, threads: int, updates_per_thread: int) -> dict:
    setup = Session(database=server.database)
    _run(server, setup, "INSERT INTO bench (id, v, pad) VALUES (999999, 0, 'hot')")
    lock = threading.Lock()
    latencies: list[float] = []
    retries = 0

    def worker(worker_id: int) -> None:
        nonlocal retries
        session = Session(database=server.database)
        for _ in range(updates_per_thread):
            started = time.perf_counter()
            attempts = 0
            while True:
                try:
                    _run(server, session, "UPDATE bench SET v = v + 1 WHERE id = 999999")
                    break
                except SerializationError:
                    attempts += 1
                    if attempts > 200:
                        raise
                    with lock:
                        retries += 1
                    time.sleep(0.001)
            elapsed = time.perf_counter() - started
            with lock:
                latencies.append(elapsed)

    errors, elapsed = _run_threads(threads, worker)
    if errors:
        raise RuntimeError("; ".join(errors))
    summary = _summary(latencies, elapsed, threads * updates_per_thread)
    summary["retries"] = retries
    return summary


def bench_mode(concurrent: bool, threads: int, rows: int, updates: int) -> dict:
    temp_dir = tempfile.TemporaryDirectory(prefix="lumendb-bench-")
    database = Database(Path(temp_dir.name) / "db", concurrent_dml=concurrent)
    server = PGWireServer(database)
    try:
        database.execute_one("CREATE TABLE bench (id INT PRIMARY KEY, v INT, pad VARCHAR(32))")
        database.execute_one("CREATE INDEX idx_bench_v ON bench (v)")
        insert = _bench_insert(server, threads, rows)
        hot = _bench_hot_update(server, threads, updates)
        count = _run(server, Session(database=database), "SELECT COUNT(*) FROM bench")[0].rows
        hot_value = _run(
            server, Session(database=database), "SELECT v FROM bench WHERE id = 999999"
        )[0].rows[0][0]
        final_rows = int(count[0][0])
        final_hot = int(hot_value)
        _check_result(
            database, final_rows, final_hot, threads, rows, updates
        )
        return {
            "mode": "concurrent_dml" if concurrent else "default_write_lock",
            "threads": threads,
            "insert": insert,
            "hot_update": hot,
            "hot_value": final_hot,
            "final_rows": final_rows,
        }
    finally:
        server.shutdown()
        database.close()
        temp_dir.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--rows", type=int, default=500, help="每个线程插入行数")
    parser.add_argument("--updates", type=int, default=100, help="每个线程热点更新次数")
    parser.add_argument("--json", type=Path, default=None)
    args = parser.parse_args()

    results = [
        bench_mode(False, args.threads, args.rows, args.updates),
        bench_mode(True, args.threads, args.rows, args.updates),
    ]
    text = json.dumps(results, ensure_ascii=False, indent=2)
    print(text)
    if args.json:
        args.json.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
