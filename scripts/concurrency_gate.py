"""Step 4 并发门禁：把"绕过 _write_lock 的并发探针"固化成可重复执行的判据。

用法：
    python scripts/concurrency_gate.py                # 默认 3 轮进程内探针
    python scripts/concurrency_gate.py --rounds 5
    python scripts/concurrency_gate.py --quick        # 只跑 1 轮 + 单测

它检查四件事（每一项都对应一个实测过的真实故障）：

1. ``btree_page_latch``    同一片叶双线程并发插入：不丢行、不乱序（页 latch）。
2. ``btree_split_chain``   叶/内部页分裂 + 删除 + 并发读者：键序、叶子链、条目数。
3. ``heap_insert_race``    2 线程 × 300 行绕过写锁直接 ``HeapFile.insert``：
   RID 不重复分配、行数不丢。
4. ``btree_delete_race``   4 线程乱序删除 + 1 并发读者（120 字节 TEXT 键造
   depth>=2 的树）：读者不截断、不重复；删完叶子链不断、树能完全塌陷。

输出是机器可读的 ``GATE <name> rounds=.. ok=.. status=PASS|FAIL``，退出码非 0
表示有门禁未通过。所有临时库都建在系统临时目录，跑完即删，不碰工作树。
"""

from __future__ import annotations

import argparse
import pathlib
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import traceback

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from lumendb.datatypes import DataType, TypeSpec          # noqa: E402
from lumendb.engine.database import Database              # noqa: E402
from lumendb.storage.btree import BPlusTree, KeyCodec     # noqa: E402
from lumendb.storage.heap import HeapFile                 # noqa: E402


# ----------------------------------------------------------------------
# 进程内探针
# ----------------------------------------------------------------------
def _probe_heap_insert_race(db: Database) -> dict:
    """2 线程各插 300 行（绕过 _write_lock）：RID 唯一、scan 行数完整。"""
    table = db.catalog.get_table("t")
    hf = HeapFile(db.bp, table, db.txns)
    threads = 2
    per_thread = 300
    barrier = threading.Barrier(threads)
    errors: list[str] = []
    rids: list[tuple] = []
    lock = threading.Lock()

    def worker(wid: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for i in range(per_thread):
                rid = hf.insert(txn, (wid * per_thread + i,))
                with lock:
                    rids.append(rid)
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"w{wid}: {type(exc).__name__}: {exc}")

    workers = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
    for th in workers:
        th.start()
    for th in workers:
        th.join(timeout=60)

    txn = db.txns.begin()
    rows = sum(1 for _ in hf.scan(txn))
    db.txns.commit(txn)
    return {
        "errors": errors[:3],
        "uniq_rids": len(set(rids)),
        "inserted": threads * per_thread,
        "scan_rows": rows,
    }


def _probe_btree_delete_race(db: Database) -> dict:
    """4 线程乱序删完 12000 条长键树 + 1 并发读者，校验叶子链与终态。"""
    tree = BPlusTree(db.bp, KeyCodec([TypeSpec(DataType.TEXT)]), unique=False, name="idx")
    total, pad, threads = 12000, 120, 4

    def key_of(i: int) -> tuple:
        return (f"{i:08d}".ljust(pad, "x"),)

    t0 = db.txns.begin()
    for i in range(total):
        tree.insert(t0, key_of(i), (0, i))
    db.txns.commit(t0)
    pre = tree.check_invariants()

    errors: list[str] = []
    counts: list[int] = []
    stop = threading.Event()
    barrier = threading.Barrier(threads)

    def deleter(wid: int) -> None:
        try:
            rnd = random.Random(wid + 7)
            keys = list(range(wid, total, threads))
            rnd.shuffle(keys)
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for k in keys:
                tree.delete(txn, key_of(k), (0, k))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"del{wid}: {type(exc).__name__}: {exc}")

    def reader() -> None:
        try:
            while not stop.is_set():
                ks = [k for k, _ in tree.scan_all(None)]
                if ks != sorted(ks):
                    errors.append("reader: 键序错乱")
                    return
                if len(ks) != len(set(ks)):
                    errors.append("reader: 重复键")
                    return
                counts.append(len(ks))
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"reader: {type(exc).__name__}: {exc}")

    rth = threading.Thread(target=reader)
    dels = [threading.Thread(target=deleter, args=(w,)) for w in range(threads)]
    rth.start()
    for th in dels:
        th.start()
    for th in dels:
        th.join(timeout=180)
    stop.set()
    rth.join(timeout=60)

    post = tree.check_invariants()
    return {
        "errors": errors[:3],
        "pre": pre,
        "post": post,
        "scans": len(counts),
        "min_rows": min(counts) if counts else None,
        "final_rows": len(list(tree.scan_all(None))),
    }


def _probe_btree_insert_crab(db: Database) -> dict:
    """4 线程并发插相交长键，触发叶/内部页/根分裂（插入 crabbing 的核心场景）。"""
    tree = BPlusTree(
        db.bp, KeyCodec([TypeSpec(DataType.VARCHAR, 512)]), unique=False, name="idx"
    )

    def long_key(i: int) -> str:
        return "k" + "x" * 400 + f"{i:05d}"

    t0 = db.txns.begin()
    for i in range(2000):
        tree.insert(t0, (long_key(i),), (0, i))
    db.txns.commit(t0)
    base = tree.check_invariants()

    threads, per = 4, 200
    errors: list[str] = []
    barrier = threading.Barrier(threads)

    def worker(wid: int) -> None:
        try:
            txn = db.txns.begin()
            barrier.wait(timeout=10)
            for i in range(per):
                tree.insert(txn, (long_key(i),), (wid + 1, i))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"w{wid}: {type(exc).__name__}: {exc}")

    workers = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
    for th in workers:
        th.start()
    for th in workers:
        th.join(timeout=180)

    post = tree.check_invariants()
    return {
        "errors": errors[:3],
        "pre": base,
        "post": post,
        "expect_entries": base["entries"] + threads * per,
    }


# ----------------------------------------------------------------------
# pytest 组
# ----------------------------------------------------------------------
PYTEST_GROUPS = {
    "btree_page_latch": ["tests/unit/test_btree_page_latch.py"],
    "btree_split_chain": [
        "tests/unit/test_btree_split_crab.py",
        "tests/unit/test_btree.py",
    ],
}


def _run_pytest(paths: list[str]) -> tuple[bool, str]:
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", *paths, "-q"],
        cwd=str(REPO), capture_output=True, text=True, timeout=900,
    )
    tail = "\n".join((proc.stdout or "").strip().splitlines()[-3:])
    return proc.returncode == 0, tail


# ----------------------------------------------------------------------
# 编排
# ----------------------------------------------------------------------
def _check_probe(name: str, build, rounds: int, env: str) -> dict:
    """跑 ``rounds`` 轮进程内探针；``build`` 负责按需准备库。"""
    results = []
    failures = 0
    for r in range(rounds):
        tmpdir = tempfile.mkdtemp(prefix=f"lumendb_gate_{name}_")
        db = None
        try:
            db, detail = build(pathlib.Path(tmpdir))
            results.append(detail)
            if _probe_failed(name, detail):
                failures += 1
        except BaseException as exc:  # noqa: BLE001
            failures += 1
            results.append({"exception": f"{type(exc).__name__}: {exc}",
                            "tb": traceback.format_exc(limit=3)})
        finally:
            if db is not None:
                try:
                    db.close()
                except BaseException:  # noqa: BLE001
                    pass
            shutil.rmtree(tmpdir, ignore_errors=True)
    return {"name": name, "rounds": rounds, "ok": rounds - failures,
            "status": "PASS" if failures == 0 else "FAIL",
            "results": results}


def _probe_failed(name: str, detail: dict) -> bool:
    if detail.get("exception"):
        return True
    if detail.get("errors"):
        return True
    if name == "heap_insert_race":
        return not (detail["uniq_rids"] == detail["inserted"] == detail["scan_rows"])
    if name == "btree_delete_race":
        post = detail["post"]
        if post["entries"] != 0 or post["depth"] != 0:
            return True
        return detail["final_rows"] != 0 or detail["scans"] == 0
    if name == "btree_insert_crab":
        post = detail["post"]
        if post["entries"] != detail["expect_entries"]:
            return True
        return post["depth"] < 2  # 本场景必须造出多层树，否则没测到内部页/根分裂
    return False


def _build_heap(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0)
    db.execute("CREATE TABLE t (id INT)")
    return db, _probe_heap_insert_race(db)


def _build_btree(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0)
    return db, _probe_btree_delete_race(db)


def _build_insert_crab(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0)
    return db, _probe_btree_insert_crab(db)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="LumenDB Step 4 并发门禁")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--quick", action="store_true", help="只跑 1 轮")
    args = ap.parse_args(argv)
    rounds = 1 if args.quick else max(1, args.rounds)

    checks: list[dict] = []
    for name, paths in PYTEST_GROUPS.items():
        ok, tail = _run_pytest(paths)
        checks.append({"name": name, "rounds": 1, "ok": 1 if ok else 0,
                       "status": "PASS" if ok else "FAIL", "detail": tail})
        print(f"GATE {name} rounds=1 ok={1 if ok else 0} status="
              f"{'PASS' if ok else 'FAIL'}", flush=True)

    for name, build in (("heap_insert_race", _build_heap),
                        ("btree_insert_crab", _build_insert_crab),
                        ("btree_delete_race", _build_btree)):
        res = _check_probe(name, build, rounds, name)
        checks.append(res)
        print(f"GATE {name} rounds={res['rounds']} ok={res['ok']} "
              f"status={res['status']}", flush=True)

    bad = [c for c in checks if c["status"] != "PASS"]
    if bad:
        print("\n未通过的门禁：")
        for c in bad:
            print(f"- {c['name']}: {str(c.get('results') or c.get('detail'))[:800]}")
        return 1
    print(f"\n全部 {len(checks)} 项门禁通过（进程内探针 {rounds} 轮）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
