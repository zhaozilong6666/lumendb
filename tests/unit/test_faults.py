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
