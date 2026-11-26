"""故障注入点自身的用例，以及"注入之后引擎仍然诚实"的用例。

现有崩溃测试只覆盖"拔电"。这里补的是生产里更常见的坏法：fsync 失败、页写失败、
WAL 追加失败。判据只有一条：**要么成功，要么明确报错；绝不允许把没落盘的写当成已提交**。
"""

from __future__ import annotations

import os

import pytest

from lumendb import faults
from lumendb.engine.database import Database
from lumendb.errors import LumenError


@pytest.fixture(autouse=True)
def _clean_faults():
    faults.disarm()
    yield
    faults.disarm()


class TestFaultSpec:
    def test_arm_and_disarm(self):
        faults.arm("wal.fsync@2")
        assert faults.armed_points() == {"wal.fsync": "n=2:io"}
        faults.disarm("wal.fsync")
        assert faults.armed_points() == {}

    def test_probability_spec(self):
        faults.arm("disk.write_page@0.25:diskfull")
        assert faults.armed_points() == {"disk.write_page": "p=0.25:diskfull"}

    def test_unknown_point_is_rejected(self):
        with pytest.raises(ValueError):
            faults.arm("wal.nope@1")

    def test_disabled_context_suppresses_everything(self):
        faults.arm("wal.fsync@1")
        with faults.disabled():
            faults.check("wal.fsync")  # 不抛
        with pytest.raises(OSError):
            faults.check("wal.fsync")

    def test_diskfull_carries_enospc(self):
        faults.arm("wal.fsync@1:diskfull")
        with pytest.raises(OSError) as exc:
            faults.check("wal.fsync")
        assert exc.value.errno == getattr(os, "ENOSPC", 28)

    def test_every_n_fires_once(self):
        faults.arm("wal.fsync@2")
        faults.check("wal.fsync")  # 第 1 次不抛
        with pytest.raises(OSError):
            faults.check("wal.fsync")  # 第 2 次抛
        faults.check("wal.fsync")  # 第 3 次不抛

    def test_nested_disabled_context_restores_only_at_outermost_exit(self):
        faults.arm("wal.fsync@1")
        with faults.disabled():
            with faults.disabled():
                faults.check("wal.fsync")
            # 内层退出后仍应处于 disabled，否则嵌套清理会提前重新放行。
            faults.check("wal.fsync")
        with pytest.raises(OSError):
            faults.check("wal.fsync")


class TestInjectedIoFailures:
    """注入之后：已提交的必须还在，没提交的不能"看起来提交了"。"""

    def _seed(self, tmp_path) -> Database:
        db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute_one("INSERT INTO t VALUES (1, 'before')")
        return db

    def test_fsync_failure_does_not_mark_the_write_committed(self, tmp_path):
        """新库默认走分段 WAL，所以注入点是 ``wal_segment.fsync``。"""
        db = self._seed(tmp_path)
        try:
            faults.arm("wal_segment.fsync@1")
            with pytest.raises((OSError, LumenError)):
                db.execute_one("INSERT INTO t VALUES (2, 'during')")
        finally:
            faults.disarm()
            db.close()

        reopened = Database(tmp_path / "db", checkpoint_wal_bytes=0)
        try:
            rows = reopened.execute_one("SELECT id FROM t ORDER BY id").rows
            assert rows == [(1,)], "fsync 失败的那条写绝不能变成已提交"
        finally:
            reopened.close()

    def test_fsync_failure_does_not_poison_the_next_transaction(self, tmp_path):
        """一次 fsync 报错后，后续提交必须还能成功，且失败那条仍不可见。"""
        db = self._seed(tmp_path)
        try:
            faults.arm("wal_segment.fsync@1")
            with pytest.raises((OSError, LumenError)):
                db.execute_one("INSERT INTO t VALUES (2, 'during')")
            faults.disarm()
            db.execute_one("INSERT INTO t VALUES (3, 'after')")
            assert db.execute_one("SELECT id FROM t ORDER BY id").rows == [(1,), (3,)]
        finally:
            faults.disarm()
            db.close()

        reopened = Database(tmp_path / "db", checkpoint_wal_bytes=0)
        try:
            assert reopened.execute_one("SELECT id FROM t ORDER BY id").rows == [(1,), (3,)]
        finally:
            reopened.close()

    def test_page_fetch_failure_does_not_install_a_blank_page(self, tmp_path):
        """读盘 EIO 必须报错，不能把空页塞进缓存后假装没有记录。"""
        db = self._seed(tmp_path)
        try:
            # 新分配的页不在缓冲池里，下一次 fetch 必然走磁盘读。
            page_id = db.disk.allocate_page()
            faults.arm("page.fetch@1")
            with pytest.raises((OSError, LumenError)):
                db.bp.fetch_page(page_id)
            faults.disarm()
            page = db.bp.fetch_page(page_id)
            db.bp.unpin(page_id)
            assert page.page_id == page_id
            assert db.execute_one("SELECT id FROM t ORDER BY id").rows == [(1,)]
        finally:
            faults.disarm()
            db.close()

    def test_wal_append_failure_aborts_the_statement(self, tmp_path):
        db = self._seed(tmp_path)
        try:
            faults.arm("wal_segment.append@2")
            with pytest.raises((OSError, LumenError)):
                db.execute_one("INSERT INTO t VALUES (3, 'boom')")
        finally:
            faults.disarm()
        # 出错之后库仍然可用，且之前的数据完好
        faults.disarm()
        assert db.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        db.execute_one("INSERT INTO t VALUES (4, 'after')")
        db.close()

        reopened = Database(tmp_path / "db", checkpoint_wal_bytes=0)
        try:
            ids = [r[0] for r in reopened.execute_one("SELECT id FROM t ORDER BY id").rows]
            assert 1 in ids
        finally:
            reopened.close()

    def test_page_write_failure_during_checkpoint_is_reported(self, tmp_path):
        db = self._seed(tmp_path)
        try:
            faults.arm("disk.write_page@1")
            with pytest.raises((OSError, LumenError)):
                db.checkpoint()
        finally:
            faults.disarm()
            db.close()
        reopened = Database(tmp_path / "db", checkpoint_wal_bytes=0)
        try:
            assert reopened.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        finally:
            reopened.close()

    def test_disk_full_during_allocate_keeps_the_db_openable(self, tmp_path):
        db = self._seed(tmp_path)
        try:
            faults.arm("disk.allocate_page@1:diskfull")
            with pytest.raises((OSError, LumenError)):
                # 触发新页分配：足够多的行
                for i in range(2, 120):
                    db.execute_one(f"INSERT INTO t VALUES ({i}, 'row{i}')")
        finally:
            faults.disarm()
            db.close()
        reopened = Database(tmp_path / "db", checkpoint_wal_bytes=0)
        try:
            assert reopened.execute_one("SELECT COUNT(*) FROM t").rows[0][0] >= 1
        finally:
            reopened.close()


class TestFaultFrameworkEdges:
    """骨架自身的边界：单文件 WAL 重试、torn 页写、嵌套 disabled。"""

    def test_single_file_wal_fsync_failure_keeps_pending_for_retry(self, tmp_path):
        """旧单文件 WAL 的 ``flush`` 也要么落盘、要么把 pending 留着重试。

        新库默认分段 WAL，但兼容旧库的 ``WAL`` 仍走同一个 WAL 规则。旧实现
        在 ``fsync`` 抛错前就清空 pending，失败后这条 COMMIT 会凭空消失，
        重试也永远补不回来。
        """
        from lumendb.storage.wal import LogType, WAL

        wal = WAL(tmp_path / "legacy.wal")
        try:
            lsn = wal.append(1, LogType.BEGIN)
            faults.arm("wal.fsync@1")
            with pytest.raises(OSError):
                wal.flush()
            faults.disarm()
            assert wal.flush() >= lsn
            wal.close()

            reopened = WAL(tmp_path / "legacy.wal")
            try:
                assert [record.lsn for record in reopened.iter_records()] == [lsn]
            finally:
                reopened.close()
        finally:
            faults.disarm()
            if not wal._fh.closed:  # pragma: no cover - 失败路径兜底
                wal.close()

    def test_torn_page_write_writes_prefix_then_reports_error(self, tmp_path):
        """``torn`` 不能只解析不接线：页写必须真的只写前半页并报错。

        生产里的 torn write 不是“整页成功/整页失败”，而是前半页新内容、
        后半页旧内容。这个用例直接看数据文件字节，确保注入点没有退化成
        普通 I/O 错误。
        """
        from lumendb.storage.page import PAGE_SIZE, Page, PageType
        from lumendb.storage.pager import DiskManager

        disk = DiskManager(tmp_path / "data")
        try:
            page_id = disk.allocate_page()
            page = Page(page_id)
            page.page_type = PageType.HEAP
            page.buf[PAGE_SIZE // 2] = 0x7F  # 后半页必须有非零内容才能验证只写了前缀
            payload = bytes(page.buf)
            faults.arm("disk.write_page@1:torn")
            with pytest.raises(OSError):
                disk.write_page(page_id, payload)
            faults.disarm()

            raw = disk.snapshot_bytes()
            start = page_id * PAGE_SIZE
            written = raw[start : start + PAGE_SIZE]
            half = PAGE_SIZE // 2
            assert written[:half] == payload[:half]
            assert written[half:] != payload[half:]
        finally:
            faults.disarm()
            disk.close()
