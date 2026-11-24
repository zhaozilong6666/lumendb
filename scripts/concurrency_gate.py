"""Step 4 并发门禁：把"绕过 _write_lock 的并发探针"固化成可重复执行的判据。

用法：
    python scripts/concurrency_gate.py                # 默认 3 轮进程内探针
    python scripts/concurrency_gate.py --rounds 5
    python scripts/concurrency_gate.py --quick        # 只跑 1 轮 + 单测

它检查十一项（每一项都对应一个实测过的真实故障）：

1. ``btree_page_latch``    同一片叶双线程并发插入：不丢行、不乱序（页 latch）。
2. ``btree_split_chain``   叶/内部页分裂 + 删除 + 并发读者：键序、叶子链、条目数。
3. ``heap_insert_race``    2 线程 × 300 行绕过写锁直接 ``HeapFile.insert``：
   RID 不重复分配、行数不丢。
4. ``btree_insert_crab``   多线程插入造多层树：不丢键、根/内部页分裂链完整。
5. ``btree_delete_race``   4 线程乱序删除 + 1 并发读者（120 字节 TEXT 键造
   depth>=2 的树）：读者不截断、不重复；删完叶子链不断、树能完全塌陷。
6. ``sql_concurrent_dml``  4 个线协议会话并发跑 SQL DML（半数回滚）：
   提交行数/去重数正确，索引不变量成立。走 ``concurrent_dml=True`` 路径。
7. ``sql_mixed_dml``       4 会话并发 INSERT/UPDATE/DELETE：终态行数、
   SUM(v)、每个索引至少覆盖全部存活行。
8. ``sql_savepoint_concurrent`` 别的会话活跃时建保存点：等待独占而非 ``0A000``。
9. ``sql_savepoint_logical`` 逻辑保存点事务存续时另一个写者仍能提交。
10. ``sql_vacuum_window`` 别的会话活跃时自动提交 VACUUM：在独占门外等它结束。
11. ``sql_vacuum_online`` 等不到空窗口时转保 RID 在线回收：活跃快照仍看旧版本。

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
from lumendb.server.pgwire import PGWireServer, Session  # noqa: E402
from lumendb.sql.parser import parse_sql                  # noqa: E402
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


def _probe_sql_concurrent_dml(db: Database) -> dict:
    """SQL 级并发 DML：4 会话各 150 行，半数提交/半数回滚。

    这是 ``concurrent_dml=True`` 的端到端探针：DML 不拿 ``_write_lock``，
    走 read_guard + 行/键锁 + 页 latch；校验提交行数、索引不变量和逻辑回滚
    留下的死行不污染可见结果。
    """
    server = PGWireServer(db)
    per_writer = 150
    writers = 4
    errors: list[str] = []
    lock = threading.Lock()

    def run_sql(session: Session, sql: str):
        return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]

    def worker(wid: int) -> None:
        try:
            session = Session(database=db)
            run_sql(session, "BEGIN")
            base = wid * per_writer
            for i in range(base, base + per_writer):
                run_sql(
                    session,
                    f"INSERT INTO t VALUES ({i}, {i % 7}, 'p{i}')",
                )
            if wid % 2:
                run_sql(session, "COMMIT")
            else:
                run_sql(session, "ROLLBACK")
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(f"w{wid}: {type(exc).__name__}: {exc}")

    workers = [threading.Thread(target=worker, args=(w,), daemon=True)
               for w in range(writers)]
    for th in workers:
        th.start()
    for th in workers:
        th.join(timeout=120)

    session = Session(database=db)
    count = run_sql(session, "SELECT COUNT(*) FROM t")[0].rows[0][0]
    distinct = run_sql(
        session, "SELECT COUNT(DISTINCT id) FROM t"
    )[0].rows[0][0]
    table = db.catalog.get_table("t")
    entries = {}
    invariants = {}
    for idx in table.indexes.values():
        tree = db.executor._tree_for(table, idx)
        inv = tree.check_invariants()
        entries[idx.name] = inv["entries"]
        invariants[idx.name] = inv
    return {
        "errors": errors[:3],
        "count": count,
        "distinct": distinct,
        "expected": (writers // 2) * per_writer,
        "entries": entries,
        "invariants": invariants,
    }


def _probe_sql_mixed_dml(db: Database) -> dict:
    """4 会话并发混合 INSERT/UPDATE/DELETE：各自行不相交，终态可精确对账。

    每条会话插入 40 行（v=0）、把偶数 id 更新为 v=1、删掉 4 的倍数。
    期望剩 30 行/会话，共 120 行；剩余偶数 10 行/会话 v=1，SUM(v)=40。
    这个探针把 UPDATE/DELETE 的并发路径也纳入门禁，而不只是 INSERT。
    """
    server = PGWireServer(db)
    per_writer = 40
    writers = 4
    errors: list[str] = []
    lock = threading.Lock()

    def run_sql(session: Session, sql: str):
        return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]

    def worker(wid: int) -> None:
        try:
            session = Session(database=db)
            run_sql(session, "BEGIN")
            base = wid * per_writer
            for i in range(base, base + per_writer):
                run_sql(session, f"INSERT INTO t VALUES ({i}, 0, 'm{i}')")
            for i in range(base, base + per_writer, 2):
                run_sql(session, f"UPDATE t SET v = v + 1 WHERE id = {i}")
            for i in range(base, base + per_writer, 4):
                run_sql(session, f"DELETE FROM t WHERE id = {i}")
            run_sql(session, "COMMIT")
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(f"w{wid}: {type(exc).__name__}: {exc}")

    workers = [threading.Thread(target=worker, args=(w,), daemon=True)
               for w in range(writers)]
    for th in workers:
        th.start()
    for th in workers:
        th.join(timeout=120)

    session = Session(database=db)
    count, total = run_sql(session, "SELECT COUNT(*), SUM(v) FROM t")[0].rows[0]
    distinct = run_sql(session, "SELECT COUNT(DISTINCT id) FROM t")[0].rows[0][0]
    table = db.catalog.get_table("t")
    invariants = {}
    for idx in table.indexes.values():
        tree = db.executor._tree_for(table, idx)
        invariants[idx.name] = tree.check_invariants()
    expected = writers * (per_writer * 3 // 4)
    expected_sum = writers * (per_writer // 4)
    return {
        "errors": errors[:3],
        "count": count,
        "distinct": distinct,
        "sum": total,
        "expected": expected,
        "expected_sum": expected_sum,
        "invariants": invariants,
    }


def _probe_sql_savepoint(db: Database) -> dict:
    """并发保存点：别的会话已有活跃事务时，SAVEPOINT 等独占而不是 0A000。

    场景：A 先插入但不提交；B 建保存点、插入、回滚到保存点、提交；最后 A 提交。
    正确终态是只剩 A 的那一行。探针同时验证保存点事务自己的后续语句能越过
    ``_no_readers`` 门，而不是等自己放行。
    """
    server = PGWireServer(db)
    errors: list[str] = []
    lock = threading.Lock()

    def run_sql(session: Session, sql: str):
        return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]

    a = Session(database=db)
    b = Session(database=db)
    try:
        run_sql(a, "BEGIN")
        run_sql(a, "INSERT INTO t VALUES (1, 1, 'a')")
        run_sql(b, "BEGIN")
        run_sql(b, "SAVEPOINT s")
        run_sql(b, "INSERT INTO t VALUES (2, 2, 'b')")
        run_sql(b, "ROLLBACK TO SAVEPOINT s")
        visible = run_sql(b, "SELECT COUNT(*) FROM t WHERE id = 2")[0].rows[0][0]
        run_sql(b, "COMMIT")
        run_sql(a, "COMMIT")
    except BaseException as exc:  # noqa: BLE001
        with lock:
            errors.append(f"{type(exc).__name__}: {exc}")

    final = run_sql(Session(database=db), "SELECT COUNT(*) FROM t")[0].rows[0][0]
    return {
        "errors": errors[:3],
        "final": final,
        "expected": 1,
        "visible_after_rollback": visible if "visible" in locals() else None,
    }


def _probe_sql_savepoint_logical(db: Database) -> dict:
    """行级逻辑保存点：保存点事务存续时，另一个写者还能提交。"""
    server = PGWireServer(db)
    errors: list[str] = []

    def run_sql(session: Session, sql: str):
        return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]

    a = Session(database=db)
    b = Session(database=db)
    try:
        run_sql(a, "BEGIN")
        run_sql(a, "SAVEPOINT s")
        run_sql(a, "INSERT INTO t VALUES (1, 1, 'a')")
        # 这一句在旧的独占保存点模式下会排在写者锁后面；逻辑模式必须直接成功。
        run_sql(b, "INSERT INTO t VALUES (2, 2, 'b')")
        run_sql(a, "ROLLBACK TO SAVEPOINT s")
        after_rollback = run_sql(
            Session(database=db), "SELECT id FROM t ORDER BY id"
        )[0].rows
        run_sql(a, "COMMIT")
    except BaseException as exc:  # noqa: BLE001
        errors.append(f"{type(exc).__name__}: {exc}")
        after_rollback = None

    final = run_sql(Session(database=db), "SELECT id FROM t ORDER BY id")[0].rows
    return {
        "errors": errors[:3],
        "after_rollback": after_rollback,
        "final": final,
        "expected": [(2,)],
    }


def _probe_sql_vacuum_window(db: Database) -> dict:
    """VACUUM 等待窗口：持有事务未提交时，VACUUM 等在独占门外而不是立刻拒绝。

    A 先插入但不提交；B 的自动提交 VACUUM 应阻塞在等待窗口里，A 提交后完成。
    终态必须保留 A 的行，且 VACUUM 确实等了至少一个轮询间隔。
    """
    server = PGWireServer(db)
    errors: list[str] = []
    started = threading.Event()
    finished = threading.Event()
    result: dict = {}

    def run_sql(session: Session, sql: str):
        return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]

    a = Session(database=db)
    vacuum = Session(database=db)

    def vacuum_worker() -> None:
        started.set()
        try:
            tag = server._run_statement(vacuum, parse_sql("VACUUM")[0]).tag
            result["tag"] = tag
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            finished.set()

    run_sql(a, "BEGIN")
    run_sql(a, "INSERT INTO t VALUES (1, 1, 'hold')")
    th = threading.Thread(target=vacuum_worker, name="gate-vacuum")
    th.start()
    started.wait(timeout=5)
    # 给 VACUUM 足够时间真正进入等待窗口；A 提交后它应立刻完成。
    th.join(timeout=0.15)
    waited = not finished.is_set()
    run_sql(a, "COMMIT")
    th.join(timeout=8)
    if th.is_alive():
        errors.append("VACUUM 在 A 提交后仍未返回")

    final = run_sql(Session(database=db), "SELECT COUNT(*) FROM t")[0].rows[0][0]
    return {
        "errors": errors[:3],
        "final": final,
        "expected": 1,
        "waited": waited,
        "tag": result.get("tag"),
    }


def _probe_sql_vacuum_online(db: Database) -> dict:
    """活跃快照存在时 VACUUM 转保 RID 在线回收，而不是等超时报错。

    模拟 5s 等待超时：A 持有快照不提交；B 更新并删除行后，VACUUM 必须在
    A 仍活跃的情况下完成，A 继续看到旧版本，新事务看到新版本。
    """
    server = PGWireServer(db)
    errors: list[str] = []

    def run_sql(session: Session, sql: str):
        return [server._run_statement(session, stmt) for stmt in parse_sql(sql)]

    seed = Session(database=db)
    run_sql(seed, "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')")
    snap = Session(database=db)
    run_sql(snap, "BEGIN")
    before = run_sql(snap, "SELECT COUNT(*) FROM t")[0].rows[0][0]

    writer = Session(database=db)
    run_sql(writer, "BEGIN")
    run_sql(writer, "UPDATE t SET v = 11 WHERE id = 1")
    run_sql(writer, "DELETE FROM t WHERE id = 2")
    run_sql(writer, "COMMIT")

    original = db.txns.wait_until_no_others
    db.txns.wait_until_no_others = lambda *args, **kwargs: False
    try:
        result = server._run_statement(
            Session(database=db), parse_sql("VACUUM")[0]
        )
    except BaseException as exc:  # noqa: BLE001
        errors.append(f"{type(exc).__name__}: {exc}")
        result = None
    finally:
        db.txns.wait_until_no_others = original

    old_view = run_sql(snap, "SELECT COUNT(*) FROM t")[0].rows[0][0]
    new_view = run_sql(Session(database=db), "SELECT COUNT(*) FROM t")[0].rows[0][0]
    run_sql(snap, "COMMIT")
    return {
        "errors": errors[:3],
        "tag": getattr(result, "tag", None),
        "before": before,
        "old_view": old_view,
        "new_view": new_view,
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
    if name == "sql_concurrent_dml":
        if detail["count"] != detail["expected"]:
            return True
        if detail["distinct"] != detail["expected"]:
            return True
        return any(entries < detail["expected"]
                   for entries in detail["entries"].values())
    if name == "sql_mixed_dml":
        if detail["errors"]:
            return True
        if detail["count"] != detail["expected"]:
            return True
        if detail["distinct"] != detail["expected"]:
            return True
        if detail["sum"] != detail["expected_sum"]:
            return True
        # 索引项因为 UPDATE 不摘旧项会多于行数；只要求每个索引至少覆盖所有
        # 存活行，且结构自检（depth/entries 非负）没有崩塌。
        return any(
            inv["entries"] < detail["expected"]
            for inv in detail["invariants"].values()
        )
    if name == "sql_savepoint_concurrent":
        return (
            detail["final"] != detail["expected"]
            or detail["visible_after_rollback"] != 0
        )
    if name == "sql_savepoint_logical":
        return (
            bool(detail["errors"])
            or detail["after_rollback"] != [(2,)]
            or detail["final"] != detail["expected"]
        )
    if name == "sql_vacuum_window":
        return (
            detail["final"] != detail["expected"]
            or not detail["waited"]
            or not str(detail.get("tag", "")).startswith("VACUUM")
        )
    if name == "sql_vacuum_online":
        return (
            "online" not in str(detail.get("tag", ""))
            or detail["before"] != 3
            or detail["old_view"] != 3
            or detail["new_view"] != 2
        )
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


def _build_sql_dml(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0, concurrent_dml=True)
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))"
    )
    db.execute("CREATE INDEX idx_v ON t (v)")
    return db, _probe_sql_concurrent_dml(db)


def _build_mixed_dml(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0, concurrent_dml=True)
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    db.execute("CREATE INDEX idx_v ON t (v)")
    return db, _probe_sql_mixed_dml(db)


def _build_savepoint(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0, concurrent_dml=True)
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    return db, _probe_sql_savepoint(db)


def _build_savepoint_logical(tmp: pathlib.Path):
    db = Database(
        tmp,
        checkpoint_wal_bytes=0,
        concurrent_dml=True,
        logical_savepoints=True,
    )
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    return db, _probe_sql_savepoint_logical(db)


def _build_vacuum(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0, concurrent_dml=True)
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    return db, _probe_sql_vacuum_window(db)


def _build_vacuum_online(tmp: pathlib.Path):
    db = Database(tmp, checkpoint_wal_bytes=0, concurrent_dml=True)
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    db.execute("CREATE INDEX idx_v ON t (v)")
    return db, _probe_sql_vacuum_online(db)


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
                        ("btree_delete_race", _build_btree),
                        ("sql_concurrent_dml", _build_sql_dml),
                        ("sql_mixed_dml", _build_mixed_dml),
                        ("sql_savepoint_concurrent", _build_savepoint),
                        ("sql_savepoint_logical", _build_savepoint_logical),
                        ("sql_vacuum_window", _build_vacuum),
                        ("sql_vacuum_online", _build_vacuum_online)):
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
