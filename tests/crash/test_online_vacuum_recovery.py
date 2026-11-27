"""在线 VACUUM 的崩溃恢复：只回收不可见版本，崩溃后表仍必须一致。

在线模式靠整页 before/after image 写 WAL。VACUUM 是维护操作，崩溃恢复可以
按 WAL 重放到已落盘的回收结果，也可以撤销；两种结果都不能丢掉已提交的行，
索引也不能指到断掉的版本链。
"""

from __future__ import annotations

from lumendb.engine.database import Database
from lumendb.server.pgwire import PGWireServer, Session
from lumendb.sql.parser import parse_sql
from lumendb.storage.heap import HeapFile
from lumendb.storage.page import INVALID_PAGE_ID


def _run(server, session, sql: str):
    return [server._run_statement(session, st) for st in parse_sql(sql)]


def _count(db: Database, sql: str) -> list:
    return _run(PGWireServer(db), Session(database=db), sql)[0].rows


def _physical_versions(db: Database, name: str = "t") -> int:
    """数堆里实际存在的记录体，不按可见性过滤。"""
    table = db.catalog.get_table(name)
    heap = HeapFile(db.bp, table, db.txns)
    total = 0
    page_id = table.root_page
    while page_id != INVALID_PAGE_ID:
        page = heap._fetch(page_id)
        try:
            total += sum(
                1
                for slot in range(page.nslots)
                if heap._decode(page, slot) is not None
            )
            next_id = page.next_page
        finally:
            heap._release(page)
        page_id = next_id
    return total


def _crash(db: Database) -> None:
    """把已写 WAL 的脏页也刷盘，模拟“页落盘后进程死掉”。"""
    db.bp.flush_all()
    db.wal.flush()
    db.wal.close()
    db.disk.close()


def test_crashed_online_vacuum_keeps_visible_rows(tmp_path, monkeypatch):
    path = tmp_path / "db"
    db = Database(path, concurrent_dml=True)
    server = PGWireServer(db)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(16))")
    for i in range(20):
        db.execute_one(f"INSERT INTO t VALUES ({i}, {i}, 'p{i}')")
    for _ in range(3):
        for i in range(20):
            db.execute_one(f"UPDATE t SET v = v + 1 WHERE id = {i}")
    db.execute_one("CREATE INDEX idx_v ON t (v)")

    # 空闲快照事务：让 VACUUM 的 5s 等待超时，转在线保 RID。
    idle = Session(database=db)
    _run(server, idle, "BEGIN")
    _run(server, idle, "SELECT COUNT(*) FROM t")
    monkeypatch.setattr(
        db.txns, "wait_until_no_others", lambda *args, **kwargs: False
    )

    before = _physical_versions(db)
    vacuum = Session(database=db)
    # 自动提交路径：并发写模式下显式事务内 VACUUM 会被拒绝（逻辑回滚不能
    # 还原物理回收的旧版本）；崩溃恢复不依赖外层事务。
    result = _run(server, vacuum, "VACUUM")[0]
    assert result.affected > 0
    after_vacuum = _physical_versions(db)
    assert after_vacuum < before

    # 不 COMMIT，直接把页刷盘后崩溃；恢复可以重放回收，但不能丢可见行。
    _crash(db)

    db2 = Database(path, concurrent_dml=True)
    server2 = PGWireServer(db2)
    try:
        recovered = _physical_versions(db2)
        assert after_vacuum <= recovered <= before
        assert _count(db2, "SELECT COUNT(*) FROM t") == [(20,)]
        assert _count(db2, "SELECT SUM(v) FROM t") == [(250,)]
        # 走索引扫描复核版本链：被回收的旧版本不能把当前行带断。
        assert _count(db2, "SELECT COUNT(*) FROM t WHERE v >= 3") == [(20,)]
        table = db2.catalog.get_table("t")
        db2.execute_one("VACUUM")
        tree = db2.executor._tree_for(table, table.indexes["idx_v"])
        assert tree.check_invariants()["entries"] == 20
        assert _count(db2, "SELECT COUNT(*) FROM t") == [(20,)]
    finally:
        db2.close()


def test_crashed_online_vacuum_page_reclaim_keeps_chain_and_rows(tmp_path):
    """整页回收把空页移到链尾后崩溃：恢复不能丢可见行或把索引指到旧槽位。"""
    path = tmp_path / "db-reclaim"
    db = Database(path, concurrent_dml=True)
    db.execute_one(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT, pad VARCHAR(1000))"
    )
    pad = "x" * 900
    for i in range(18):
        db.execute_one(f"INSERT INTO t VALUES ({i}, {i}, '{pad}')")
    db.execute_one("CREATE INDEX idx_v ON t (v)")
    table = db.catalog.get_table("t")
    heap = HeapFile(db.bp, table, db.txns)

    reader = db.txns.begin()
    page_of: dict[int, list[int]] = {}
    for rid, values in heap.scan(reader):
        page, _rec = heap._read_rid(rid)
        try:
            page_of.setdefault(page.page_id, []).append(values[0])
        finally:
            db.bp.unpin(page.page_id)
    db.txns.commit(reader)
    assert len(page_of) >= 3
    target_page = next(pid for pid, ids in page_of.items() if len(ids) >= 2)
    target_ids = page_of[target_page]
    for row_id in target_ids:
        db.execute_one(f"DELETE FROM t WHERE id = {row_id}")

    # 删除提交后再开空闲快照，它不需要被删行，在线 VACUUM 可以整页回收。
    idle = db.txns.begin()
    vac = db.txns.begin()
    heap.vacuum_online(vac, db.txns.active_transactions())
    db.catalog.mark_dirty()
    db.catalog.persist(vac)
    db.txns.commit(vac)
    assert table.last_page == target_page

    # 重置后的页应立即能复用；旧键的索引项不得把新行当成命中。
    new_row = db.execute_one(
        f"INSERT INTO t VALUES (99, 99, '{'y' * 100}')"
    )
    assert new_row.affected == 1
    assert db.execute_one("SELECT COUNT(*) FROM t").rows[0][0] == (
        18 - len(target_ids) + 1
    )
    db.txns.abort(idle)

    db.bp.flush_all()
    db.wal.flush()
    db.wal.close()
    db.disk.close()

    db2 = Database(path, concurrent_dml=True)
    try:
        table2 = db2.catalog.get_table("t")
        heap2 = HeapFile(db2.bp, table2, db2.txns)
        count = db2.execute_one("SELECT COUNT(*) FROM t").rows[0][0]
        assert count == 18 - len(target_ids) + 1
        scan_count = db2.execute_one("SELECT COUNT(*) FROM t WHERE v >= 0").rows[0][0]
        assert scan_count == count
        tree = db2.executor._tree_for(table2, table2.indexes["idx_v"])
        assert tree.check_invariants()["entries"] == count
    finally:
        db2.close()
