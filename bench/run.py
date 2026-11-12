"""基准测试：给出可复现的数字，而不是"感觉很快"。

跑法：
    python -m bench.run

输出一张 Markdown 表格，直接贴进 README。所有数字都来自本机实测，
换机器会变；这也是为什么脚本本身要进仓库——别人能自己复现。
"""

from __future__ import annotations

import argparse
import random
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lumendb.engine.database import Database


def timed(fn: Callable[[], None], *, repeat: int = 3) -> tuple[float, float]:
    """跑 repeat 次，返回 (最好成绩秒数, 中位数秒数)。"""
    samples = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return min(samples), statistics.median(samples)


def fresh_db(root: Path, name: str) -> Database:
    path = root / name
    if path.exists():
        shutil.rmtree(path)
    return Database(path, pool_size=1024)


def bench_insert(root: Path, rows: int) -> dict:
    def run() -> None:
        db = fresh_db(root, "insert")
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32), score INT)")
        batch = []
        for i in range(rows):
            batch.append(f"({i}, 'user{i}', {i % 100})")
            if len(batch) == 200:
                db.execute_one("INSERT INTO t VALUES " + ", ".join(batch))
                batch = []
        if batch:
            db.execute_one("INSERT INTO t VALUES " + ", ".join(batch))
        db.close()

    best, median = timed(run, repeat=1)
    return {
        "name": "批量插入（含主键索引维护）",
        "rows": rows,
        "seconds": best,
        "per_sec": rows / best,
    }


def bench_insert_no_index(root: Path, rows: int) -> dict:
    def run() -> None:
        db = fresh_db(root, "insert_noidx")
        db.execute_one("CREATE TABLE t (id INT, name VARCHAR(32), score INT)")
        batch = []
        for i in range(rows):
            batch.append(f"({i}, 'user{i}', {i % 100})")
            if len(batch) == 200:
                db.execute_one("INSERT INTO t VALUES " + ", ".join(batch))
                batch = []
        if batch:
            db.execute_one("INSERT INTO t VALUES " + ", ".join(batch))
        db.close()

    best, median = timed(run, repeat=1)
    return {
        "name": "批量插入（无索引）",
        "rows": rows,
        "seconds": best,
        "per_sec": rows / best,
    }


def _prepare_scan_db(root: Path, rows: int, name: str = "scan") -> Path:
    path = root / name
    if path.exists():
        shutil.rmtree(path)
    db = Database(path, pool_size=1024)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32), score INT)")
    batch = []
    for i in range(rows):
        batch.append(f"({i}, 'user{i}', {i % 100})")
        if len(batch) == 200:
            db.execute_one("INSERT INTO t VALUES " + ", ".join(batch))
            batch = []
    if batch:
        db.execute_one("INSERT INTO t VALUES " + ", ".join(batch))
    db.close()
    return path


def bench_scan(root: Path, rows: int) -> dict:
    path = _prepare_scan_db(root, rows)

    def run() -> None:
        db = Database(path, pool_size=1024)
        db.execute_one("SELECT COUNT(*) FROM t")
        db.close()

    best, median = timed(run)
    return {
        "name": "全表扫描 COUNT(*)",
        "rows": rows,
        "seconds": best,
        "per_sec": rows / best,
    }


def bench_point_lookup_index(root: Path, rows: int) -> dict:
    path = _prepare_scan_db(root, rows)
    # 索引回填单独做，不混进点查的耗时里
    builder = Database(path, pool_size=1024)
    builder.execute_one("CREATE INDEX idx_id ON t (id)")
    builder.close()
    probes = min(500, max(50, rows // 10))

    def run() -> None:
        db = Database(path, pool_size=1024)
        db.execute_one("BEGIN")
        random.seed(7)
        keys = [random.randrange(rows) for _ in range(probes)]
        for key in keys:
            db.execute_one(f"SELECT name FROM t WHERE id = {key}")
        db.execute_one("COMMIT")
        db.close()

    best, median = timed(run, repeat=1)
    return {
        "name": f"索引点查（{probes} 次）",
        "rows": probes,
        "seconds": best,
        "per_sec": probes / best,
    }


def bench_point_lookup_scan(root: Path, rows: int) -> dict:
    path = _prepare_scan_db(root, rows)
    probes = min(50, max(10, rows // 100))

    def run() -> None:
        db = Database(path, pool_size=1024)
        db.execute_one("BEGIN")
        random.seed(7)
        keys = [random.randrange(rows) for _ in range(probes)]
        for key in keys:
            db.execute_one(f"SELECT name FROM t WHERE id = {key}")
        db.execute_one("COMMIT")
        db.close()

    best, median = timed(run, repeat=1)
    return {
        "name": f"无索引点查（{probes} 次，全表扫描）",
        "rows": probes,
        "seconds": best,
        "per_sec": probes / best,
    }


def bench_update(root: Path, rows: int) -> dict:
    path = _prepare_scan_db(root, rows)
    updated = min(500, rows)

    def run() -> None:
        db = Database(path, pool_size=1024)
        db.execute_one(f"UPDATE t SET score = score + 1 WHERE id < {updated}")
        db.close()

    best, median = timed(run, repeat=1)
    return {
        "name": f"更新 {updated} 行",
        "rows": updated,
        "seconds": best,
        "per_sec": updated / best,
    }


def bench_group_by(root: Path, rows: int) -> dict:
    path = _prepare_scan_db(root, rows)

    def run() -> None:
        db = Database(path, pool_size=1024)
        db.execute_one("SELECT score, COUNT(*), AVG(score) FROM t GROUP BY score")
        db.close()

    best, median = timed(run)
    return {
        "name": "GROUP BY（100 个分组）",
        "rows": rows,
        "seconds": best,
        "per_sec": rows / best,
    }


def bench_vacuum(root: Path, rows: int) -> dict:
    """先制造 500 个死版本，再单独计时 VACUUM 的压实成本。"""
    updated = min(500, rows)
    samples = []
    for _ in range(3):
        path = _prepare_scan_db(root, rows, name="vacuum")
        db = Database(path, pool_size=1024)
        db.execute_one(f"UPDATE t SET score = score + 1 WHERE id < {updated}")
        start = time.perf_counter()
        result = db.execute_one("VACUUM t")
        samples.append(time.perf_counter() - start)
        assert "rows kept" in result.tag
        db.close()
    best = min(samples)
    return {
        "name": f"VACUUM 压实 {rows} 行（回收 {updated} 个死版本）",
        "rows": rows,
        "seconds": best,
        "per_sec": rows / best,
    }


def bench_truncate(root: Path, rows: int) -> dict:
    samples = []
    for _ in range(3):
        path = _prepare_scan_db(root, rows, name="truncate")
        db = Database(path, pool_size=1024)
        start = time.perf_counter()
        result = db.execute_one("TRUNCATE TABLE t")
        samples.append(time.perf_counter() - start)
        assert result.affected == rows
        db.close()
    best = min(samples)
    return {
        "name": f"TRUNCATE 清空 {rows} 行",
        "rows": rows,
        "seconds": best,
        "per_sec": rows / best,
    }


def bench_range_index(root: Path, rows: int) -> dict:
    path = _prepare_scan_db(root, rows, name="range_idx")
    builder = Database(path, pool_size=1024)
    builder.execute_one("CREATE INDEX idx_score ON t (score)")
    builder.close()
    probes = min(100, max(20, rows // 40))

    def run() -> None:
        db = Database(path, pool_size=1024)
        db.execute_one("BEGIN")
        for _ in range(probes):
            db.execute_one("SELECT COUNT(*) FROM t WHERE score >= 99")
        db.execute_one("COMMIT")
        db.close()

    best, median = timed(run, repeat=1)
    return {
        "name": f"索引范围查询（score >= 99，{probes} 次）",
        "rows": probes,
        "seconds": best,
        "per_sec": probes / best,
    }


def bench_range_scan(root: Path, rows: int) -> dict:
    path = _prepare_scan_db(root, rows, name="range_scan")
    probes = min(30, max(10, rows // 100))

    def run() -> None:
        db = Database(path, pool_size=1024)
        db.execute_one("BEGIN")
        for _ in range(probes):
            db.execute_one("SELECT COUNT(*) FROM t WHERE score >= 99")
        db.execute_one("COMMIT")
        db.close()

    best, median = timed(run, repeat=1)
    return {
        "name": f"无索引范围查询（score >= 99，{probes} 次，全表扫描）",
        "rows": probes,
        "seconds": best,
        "per_sec": probes / best,
    }


BENCHMARKS = [
    bench_insert,
    bench_insert_no_index,
    bench_scan,
    bench_group_by,
    bench_update,
    bench_vacuum,
    bench_truncate,
    bench_point_lookup_index,
    bench_point_lookup_scan,
    bench_range_index,
    bench_range_scan,
]


def main() -> int:
    parser = argparse.ArgumentParser(description="LumenDB 基准测试")
    parser.add_argument("--rows", type=int, default=1000, help="基础数据量")
    parser.add_argument("--out", type=Path, default=None, help="输出 Markdown 文件")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="lumendb-bench-") as tmp:
        root = Path(tmp)
        results = []
        for bench in BENCHMARKS:
            print(f"running {bench.__name__} ...", flush=True)
            results.append(bench(root, args.rows))

    lines = [
        "| 场景 | 行数 | 用时 (s) | 吞吐 (行/秒) |",
        "|---|---:|---:|---:|",
    ]
    for r in results:
        lines.append(
            f"| {r['name']} | {r['rows']} | {r['seconds']:.3f} | {r['per_sec']:.0f} |"
        )
    table = "\n".join(lines)
    print()
    print(table)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(table + "\n", encoding="utf-8")
        print(f"\n已写入 {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
