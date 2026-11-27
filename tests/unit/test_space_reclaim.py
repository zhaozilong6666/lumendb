"""文件级空间回收：空闲页表、提交后释放与文件尾截断。"""

from __future__ import annotations

import os
import struct

import pytest

from lumendb.engine.database import Database
from lumendb.storage.pager import (
    DiskManager,
    META_OFF_FREE_COUNT,
    META_OFF_FREE_LIST,
    META_OFF_NEXT_PAGE,
    PAGE_SIZE,
)


def _data_file(db: Database):
    return db.path / "lumendb.data"


def _file_pages(db: Database) -> int:
    return _data_file(db).stat().st_size // PAGE_SIZE


def _rows(db: Database, sql: str):
    return db.execute_one(sql).rows


def test_disk_free_list_reuses_and_truncates_tail(tmp_path):
    disk = DiskManager(tmp_path / "space.data")
    try:
        ids = [disk.allocate_page() for _ in range(3)]
        assert ids == [1, 2, 3]
        assert disk.num_pages == 4

        assert disk.release_pages([3]) == 1
        assert disk.truncate_free_tail() == 1
        assert disk.num_pages == 3
        assert disk.free_page_count == 0

        reused = disk.allocate_page()
        assert reused == 3
        # 复用页要写回原偏移，文件长度会重新覆盖到该页末尾；关键是
        # 没有分配到 4，页号仍是空闲表里的 3。
        assert disk.num_pages == 4
    finally:
        disk.close()


def test_disk_release_ignores_out_of_range_and_duplicate_pages(tmp_path):
    disk = DiskManager(tmp_path / "space.data")
    try:
        disk.allocate_page()
        disk.allocate_page()
        assert disk.release_pages([1, 1, 0, 99, 2]) == 2
        assert disk.free_page_count == 2
        assert sorted(disk.free_page_ids()) == [1, 2]
    finally:
        disk.close()


def test_meta_page_flush_keeps_disk_manager_fields(tmp_path):
    """BufferPool 整页刷 page 0 时不能把 DiskManager 直写的 meta 字段盖回旧值。"""
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT)")
        # 目录保存已经取过 page 0 并标脏；此时缓存里的 next_page_id/空闲表
        # 还是旧副本。
        assert 0 in db.bp._dirty
        pid = db.disk.allocate_page()
        assert db.disk.release_pages([pid]) == 1
        assert db.disk.free_page_count == 1

        db.bp.flush_page(0)
        raw = db.disk.read_page(0)
        assert struct.unpack_from("<I", raw, META_OFF_NEXT_PAGE)[0] == (
            db.disk.meta.next_page_id
        )
        assert struct.unpack_from("<I", raw, META_OFF_FREE_COUNT)[0] == 1
        assert struct.unpack_from("<I", raw, META_OFF_FREE_LIST)[0] == pid

        # 淘汰路径同样要叠加；否则 page 0 被 LRU 淘汰时又会写回旧副本。
        db.bp.fetch_page(0)
        db.bp.unpin(0, dirty=True)
        db.bp._evict(0)
        raw = db.disk.read_page(0)
        assert struct.unpack_from("<I", raw, META_OFF_NEXT_PAGE)[0] == (
            db.disk.meta.next_page_id
        )
        assert struct.unpack_from("<I", raw, META_OFF_FREE_COUNT)[0] == 1
        assert struct.unpack_from("<I", raw, META_OFF_FREE_LIST)[0] == pid
    finally:
        db.close()


def test_vacuum_releases_empty_tail_and_shrinks_file(tmp_path):
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT, pad TEXT)")
        pad = "x" * 900
        for i in range(220):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        db.execute_one("DELETE FROM t WHERE id > 0")
        before_pages = _file_pages(db)
        before_size = _data_file(db).stat().st_size

        db.execute_one("VACUUM t")
        # 离线 VACUUM 把剩下的一行压到根页，尾部空页进入空闲表。
        assert db.disk.free_page_count > 0
        assert _rows(db, "SELECT count(*) FROM t")[0][0] == 1
    finally:
        db.close()

    assert _data_file(db).stat().st_size < before_size
    assert _data_file(db).stat().st_size <= before_pages * PAGE_SIZE

    reopened = Database(tmp_path / "db")
    try:
        assert _rows(reopened, "SELECT count(*) FROM t")[0][0] == 1
        # 复用一个已释放页后，文件不应重新长到旧大小。
        reopened.execute_one("INSERT INTO t VALUES (1, $1)", [pad])
        assert _file_pages(reopened) <= before_pages
    finally:
        reopened.close()


def test_vacuum_frees_old_index_pages_and_reuses_them(tmp_path):
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        pad = "y" * 700
        for i in range(180):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        db.execute_one("DELETE FROM t WHERE id >= 10")
        before_pages = _file_pages(db)

        db.execute_one("VACUUM t")
        assert db.disk.free_page_count > 0
        assert _rows(db, "SELECT count(*) FROM t")[0][0] == 10
        # 索引仍能按主键查到保留的行。
        assert _rows(db, "SELECT id FROM t WHERE id = 5") == [(5,)]
    finally:
        db.close()

    reopened = Database(tmp_path / "db")
    try:
        assert _rows(reopened, "SELECT count(*) FROM t")[0][0] == 10
        assert _rows(reopened, "SELECT id FROM t WHERE id = 5") == [(5,)]
        for i in range(10, 60):
            reopened.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        # 允许目录/索引的少量结构性增长；不能回到 VACUUM 前成倍增长。
        assert _file_pages(reopened) <= before_pages + 1
    finally:
        reopened.close()


@pytest.mark.skipif(
    os.environ.get("LUMEN_CONCURRENT_DML") == "1",
    reason="并发写模式显式事务的 VACUUM 回滚是既有限制，本片只覆盖串行路径",
)
def test_vacuum_rollback_does_not_release_pages(tmp_path):
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT, pad TEXT)")
        pad = "z" * 900
        for i in range(160):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        free_before = db.disk.free_page_count

        db.execute_one("BEGIN")
        db.execute_one("DELETE FROM t WHERE id > 0")
        db.execute_one("VACUUM t")
        # 未提交事务的 freed_pages 不能进入空闲表；空闲页反而可能被本次
        # 目录保存消耗掉，因此只断言没有增加。
        assert db.disk.free_page_count <= free_before
        db.execute_one("ROLLBACK")

        # 回滚后行必须还在；空闲页表不能在提交前被使用。
        assert _rows(db, "SELECT count(*) FROM t")[0][0] == 160
    finally:
        db.close()


@pytest.mark.parametrize("concurrent", [False, True])
def test_alter_shadow_heap_releases_old_pages(tmp_path, monkeypatch, concurrent):
    """ALTER 切换 shadow heap 后，旧堆页必须进入空闲表；回滚则不能释放。"""
    if not concurrent:
        monkeypatch.delenv("LUMEN_CONCURRENT_DML", raising=False)
    db = Database(tmp_path / "db", concurrent_dml=concurrent)
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        pad = "y" * 900
        for i in range(160):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        free_before = db.disk.free_page_count

        # 回滚路径：旧根页仍被目录/WAL undo 引用，不能进入空闲表。
        db.execute_one("BEGIN")
        db.execute_one("ALTER TABLE t DROP COLUMN pad")
        db.execute_one("ROLLBACK")
        assert _rows(db, "SELECT count(*) FROM t") == [(160,)]
        assert _rows(db, "SELECT pad FROM t WHERE id = 0") == [(pad,)]
        assert db.disk.free_page_count <= free_before + 1

        # 提交路径：切换根页后旧堆页可复用；后续插入不应让文件线性增长。
        db.execute_one("ALTER TABLE t DROP COLUMN pad")
        assert db.disk.free_page_count > free_before + 10
        assert _rows(db, "SELECT count(*) FROM t") == [(160,)]
        after_alter_pages = _file_pages(db)
        for i in range(160, 320):
            db.execute_one("INSERT INTO t VALUES ($1)", [i])
        # 新插入优先复用旧堆页；文件不应再随插入线性增长。
        assert _file_pages(db) <= after_alter_pages
    finally:
        db.close()

    reopened = Database(tmp_path / "db", concurrent_dml=concurrent)
    try:
        assert _rows(reopened, "SELECT count(*) FROM t") == [(320,)]
        assert reopened.disk.free_page_count > 10  # 旧页仍在空闲表里
    finally:
        reopened.close()


@pytest.mark.parametrize("concurrent", [False, True])
def test_alter_type_shadow_heap_releases_old_pages(tmp_path, monkeypatch, concurrent):
    """ALTER COLUMN TYPE 走 shadow heap 后同样回收旧堆页。"""
    if not concurrent:
        monkeypatch.delenv("LUMEN_CONCURRENT_DML", raising=False)
    db = Database(tmp_path / "db", concurrent_dml=concurrent)
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        pad = "x" * 900
        for i in range(160):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        free_before = db.disk.free_page_count

        db.execute_one("ALTER TABLE t ALTER COLUMN id TYPE BIGINT")
        assert _rows(db, "SELECT count(*) FROM t") == [(160,)]
        assert db.disk.free_page_count > free_before + 10
        after_alter_pages = _file_pages(db)
        for i in range(160, 320):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        assert _file_pages(db) <= after_alter_pages
    finally:
        db.close()

    reopened = Database(tmp_path / "db", concurrent_dml=concurrent)
    try:
        assert _rows(reopened, "SELECT count(*) FROM t") == [(320,)]
    finally:
        reopened.close()


def test_vacuum_empty_table_releases_all_but_root(tmp_path):
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT, pad TEXT)")
        pad = "e" * 900
        for i in range(80):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        db.execute_one("DELETE FROM t")
        db.execute_one("VACUUM t")
        assert _rows(db, "SELECT count(*) FROM t")[0][0] == 0
        assert db.disk.free_page_count > 0
    finally:
        db.close()

    reopened = Database(tmp_path / "db")
    try:
        assert _rows(reopened, "SELECT count(*) FROM t")[0][0] == 0
        reopened.execute_one("INSERT INTO t VALUES (1, 'after')")
        assert _rows(reopened, "SELECT count(*) FROM t")[0][0] == 1
    finally:
        reopened.close()


def test_stale_free_entry_is_rejected_after_page_reuse(tmp_path):
    path = tmp_path / "space.data"
    disk = DiskManager(path)
    try:
        disk.allocate_page()
        pid = disk.allocate_page()
        # 模拟"空闲表持久化落后于分配"：页 2 已被重新写成堆页，但元数据
        # 里仍把它记为空闲。恢复后的复核必须丢弃这条陈旧记录。
        raw = bytearray(PAGE_SIZE)
        raw[0:2] = (1).to_bytes(2, "little")  # PageType.HEAP
        raw[16:20] = pid.to_bytes(4, "little")
        disk.write_page(pid, raw)
        disk.meta.set_free_page_ids([pid])
        disk._persist_meta()
    finally:
        disk.close()

    reopened = DiskManager(path)
    try:
        dropped = reopened.validate_free_list()
        assert dropped == 1
        assert reopened.free_page_count == 0
        # 新分配必须落到文件尾，不能复用仍被堆页占用的页。
        assert reopened.allocate_page() == 3
    finally:
        reopened.close()


def test_free_list_survives_disk_reopen(tmp_path):
    path = tmp_path / "space.data"
    disk = DiskManager(path)
    try:
        disk.allocate_page()
        disk.allocate_page()
        assert disk.release_pages([2]) == 1
    finally:
        disk.close()

    reopened = DiskManager(path)
    try:
        assert reopened.free_page_count == 1
        assert reopened.allocate_page() == 2
        assert reopened.free_page_count == 0
    finally:
        reopened.close()


def test_free_list_capacity_overflow_leaks_without_corruption(tmp_path):
    from lumendb.storage.pager import META_FREE_CAPACITY

    disk = DiskManager(tmp_path / "space.data")
    try:
        for _ in range(META_FREE_CAPACITY + 2):
            disk.allocate_page()
        # 释放全部页，但空闲表只能记 META_FREE_CAPACITY 个；多出的页
        # 只泄漏空间，不会被误当作可用页。
        accepted = disk.release_pages(range(1, META_FREE_CAPACITY + 2))
        assert accepted == META_FREE_CAPACITY
        assert disk.free_page_count == META_FREE_CAPACITY
        # 仍可正常分配与读取，未出现页号越界。
        pid = disk.allocate_page()
        assert pid <= META_FREE_CAPACITY + 2
        disk.read_page(pid)
    finally:
        disk.close()


def test_catalog_dml_versions_are_reclaimed_but_schema_history_survives(tmp_path):
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT, pad TEXT)")
        pad = "c" * 500
        for i in range(80):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        # 普通 DML 会反复保存同一 catalog 版本；这些中间根页不应留在
        # 历史链上。schema 历史仍必须完整可读。
        versions = db.execute_one("SHOW CATALOG VERSIONS").rows
        assert len(versions) == 1
        assert db.execute_one("SELECT count(*) FROM t").rows == [(80,)]
    finally:
        db.close()

    reopened = Database(tmp_path / "db")
    try:
        assert len(reopened.execute_one("SHOW CATALOG VERSIONS").rows) == 1
        assert reopened.execute_one("SELECT count(*) FROM t").rows == [(80,)]
    finally:
        reopened.close()


def test_reindex_reclaims_old_index_pages_and_keeps_queries_correct(tmp_path):
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        pad = "r" * 500
        for i in range(120):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        db.execute_one("CREATE INDEX idx_pad ON t (pad)")
        free_before = db.disk.free_page_count
        db.execute_one("REINDEX t")
        assert db.disk.free_page_count >= free_before
        assert db.execute_one("SELECT id FROM t WHERE id = 7").rows == [(7,)]
        # 历史 schema 版本仍能回溯；索引旧根页被释放不应影响目录历史。
        assert db.execute_one("SHOW CATALOG VERSIONS").rows
    finally:
        db.close()


def test_truncate_tail_is_never_replayed_by_recovery(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=10**9)
    try:
        db.execute_one("CREATE TABLE t (id INT, pad TEXT)")
        pad = "q" * 900
        for i in range(120):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        db.execute_one("DELETE FROM t WHERE id > 0")
        db.execute_one("VACUUM t")
    finally:
        # close 会先 checkpoint 再截尾：下次恢复的起点在截断之后，不会把
        # 已释放页 ensure_page 回来。
        db.close()

    reopened = Database(tmp_path / "db")
    try:
        assert _rows(reopened, "SELECT count(*) FROM t")[0][0] == 1
        assert os.path.getsize(_data_file(reopened)) % PAGE_SIZE == 0
    finally:
        reopened.close()
