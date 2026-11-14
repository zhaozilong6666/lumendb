"""复制路径的可复现基准：基准备份、WAL 追平、副本查询。

跑法：
    python -m bench.replication --rows 2000

输出 Markdown 表格，数字来自本机实测；不同机器会变。这个脚本存在的目的
是给第 4 层一个“延迟/吞吐/丢失窗口”的量化基线，而不是只凭单元测试说
复制能跑。
"""

from __future__ import annotations

import argparse
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lumendb.engine.database import Database
from lumendb.replication import Replica, WALClient, WALServer


def _insert_batch(db: Database, start: int, count: int, batch: int = 200) -> None:
    rows = []
    for i in range(start, start + count):
        rows.append(f"({i}, 'user{i}', {i % 100})")
        if len(rows) == batch:
            db.execute_one("INSERT INTO t VALUES " + ", ".join(rows))
            rows = []
    if rows:
        db.execute_one("INSERT INTO t VALUES " + ", ".join(rows))


def run(root: Path, rows: int) -> dict:
    primary = Database(root / "primary", checkpoint_wal_bytes=0, pool_size=512)
    primary.execute_one(
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32), score INT)"
    )
    _insert_batch(primary, 0, min(rows, 200))

    server = WALServer(primary).start()
    replica = None
    try:
        start = time.perf_counter()
        replica = Replica.bootstrap_from_server(
            server.host, server.port, root / "replica"
        )
        bootstrap_s = time.perf_counter() - start

        # 让副本先追平基准备份产生的 CHECKPOINT 之后的所有记录。
        while replica.replicate_from_server(server.host, server.port) > 0:
            pass

        before_lag = replica.lag_from_server(server.host, server.port)
        start = time.perf_counter()
        _insert_batch(primary, min(rows, 200), max(0, rows - min(rows, 200)))
        write_s = time.perf_counter() - start
        after_lag = replica.lag_from_server(server.host, server.port)

        start = time.perf_counter()
        applied = 0
        while True:
            n = replica.replicate_from_server(
                server.host, server.port, max_records=1000
            )
            if n == 0:
                break
            applied += n
        apply_s = time.perf_counter() - start
        final_lag = replica.lag_from_server(server.host, server.port)

        start = time.perf_counter()
        count = replica.execute_one("SELECT COUNT(*) FROM t").rows[0][0]
        query_s = time.perf_counter() - start

        status = WALClient(server.host, server.port).status()
        last_lsn = status["current_lsn"]
        base_lsn = status["base_lsn"]
        return {
            "rows": rows,
            "bootstrap_s": bootstrap_s,
            "write_s": write_s,
            "apply_s": apply_s,
            "applied_records": applied,
            "records_per_s": applied / apply_s if apply_s else 0,
            "before_lag": before_lag,
            "after_lag": after_lag,
            "final_lag": final_lag,
            "query_s": query_s,
            "count": count,
            "last_lsn": last_lsn,
            "base_lsn": base_lsn,
            "bytes": primary.disk.path.stat().st_size,
        }
    finally:
        if replica is not None:
            replica.db.close()
        server.stop()
        primary.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="LumenDB 复制基准")
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()
    if args.rows < 1 or args.repeat < 1:
        parser.error("--rows/--repeat 必须 >= 1")

    samples = []
    with tempfile.TemporaryDirectory(prefix="lumendb-repl-bench-") as tmp:
        for i in range(args.repeat):
            root = Path(tmp) / f"run{i}"
            if root.exists():
                shutil.rmtree(root)
            samples.append(run(root, args.rows))

    best = min(samples, key=lambda s: s["apply_s"])
    med_apply = statistics.median(s["apply_s"] for s in samples)
    med_write = statistics.median(s["write_s"] for s in samples)
    med_boot = statistics.median(s["bootstrap_s"] for s in samples)
    lines = [
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| 数据行数 | {best['rows']} |",
        f"| 基准备份耗时（最好） | {best['bootstrap_s']:.3f} s |",
        f"| 基准备份耗时（中位） | {med_boot:.3f} s |",
        f"| 主库写入耗时（中位） | {med_write:.3f} s |",
        f"| 副本追平耗时（最好） | {best['apply_s']:.3f} s |",
        f"| 副本追平耗时（中位） | {med_apply:.3f} s |",
        f"| 应用记录数 | {best['applied_records']} |",
        f"| 应用吞吐（记录/秒） | {best['records_per_s']:.0f} |",
        f"| 写入前 LSN 延迟（字节） | {best['before_lag']} |",
        f"| 写入后 LSN 延迟（字节） | {best['after_lag']} |",
        f"| 追平后 LSN 延迟（字节） | {best['final_lag']} |",
        f"| 副本 COUNT(*) 查询 | {best['query_s'] * 1000:.2f} ms |",
        f"| 副本行数 | {best['count']} |",
        f"| 主库数据文件 | {best['bytes']} B |",
        f"| 最后记录 LSN / base LSN | {best['last_lsn']} / {best['base_lsn']} |",
    ]
    table = "\n".join(lines)
    print(table)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(table + "\n", encoding="utf-8")
        print(f"\n已写入 {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
