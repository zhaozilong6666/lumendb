from __future__ import annotations

import pytest

from lumendb.engine.database import Database
from lumendb.errors import BinderError, NotSupportedError, SQLSyntaxError
from lumendb.sql.parser import parse_sql


def _last_commit(db: Database) -> int:
    return db.txns.commit_log()[-1][1]


def test_as_of_select_reads_commit_lsn_boundaries(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        insert_lsn = _last_commit(db)
        db.execute("UPDATE t SET v = 'new' WHERE id = 1")
        update_lsn = _last_commit(db)
        db.execute("DELETE FROM t WHERE id = 1")
        delete_lsn = _last_commit(db)

        rows = db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {insert_lsn}"
        )[0].rows
        assert rows == [(1, "old")]
        rows = db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {update_lsn}"
        )[0].rows
        assert rows == [(1, "new")]
        rows = db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {delete_lsn}"
        )[0].rows
        assert rows == []
    finally:
        db.close()


def test_as_of_after_checkpoint_keeps_pre_window_rows(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        db.checkpoint()
        # checkpoint 之前插入的 xmin 已经从账本裁剪；AS OF 目标点不早于
        # checkpoint 时必须按"水位前已提交"恢复这行，而不是整表读空。
        db.execute("INSERT INTO t VALUES (2, 'two')")
        insert_two = _last_commit(db)
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {insert_two}"
        )[0].rows == [(1, "old"), (2, "two")]

        db.execute("UPDATE t SET v = 'new' WHERE id = 1")
        update_one = _last_commit(db)
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {update_one}"
        )[0].rows == [(2, "two"), (1, "new")]
    finally:
        db.close()


def test_as_of_index_lookup_reads_target_version(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("CREATE UNIQUE INDEX t_v ON t(v)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        old_lsn = _last_commit(db)
        db.execute("UPDATE t SET v = 'new' WHERE id = 1")

        rows = db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {old_lsn} WHERE v = 'old'"
        )[0].rows
        assert rows == [(1, "old")]
        # 目标点之后才出现的键不能从索引的当前项里混进来。
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {old_lsn} WHERE v = 'new'"
        )[0].rows == []

        plan = "\n".join(
            row[0]
            for row in db.execute(
                f"EXPLAIN SELECT * FROM t AS OF SYSTEM TIME LSN {old_lsn} "
                "WHERE v = 'old'"
            )[0].rows
        )
        assert "IndexScan t [t_v(v) = ('old')] [AS OF LSN" in plan
        assert "SeqScan t [" not in plan
    finally:
        db.close()


def test_as_of_index_range_reads_target_versions(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("CREATE INDEX t_v ON t(v)")
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        old_lsn = _last_commit(db)
        db.execute("UPDATE t SET v = 'z'")

        rows = db.execute(
            f"SELECT id, v FROM t AS OF SYSTEM TIME LSN {old_lsn} "
            "WHERE v BETWEEN 'a' AND 'c' ORDER BY id"
        )[0].rows
        assert rows == [(1, "a"), (2, "b"), (3, "c")]
        assert db.execute(
            f"SELECT id FROM t AS OF SYSTEM TIME LSN {old_lsn} "
            "WHERE v = 'z'"
        )[0].rows == []
    finally:
        db.close()


def test_as_of_index_after_delete_and_vacuum_retain(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("CREATE UNIQUE INDEX t_v ON t(v)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        db.execute("UPDATE t SET v = 'new' WHERE id = 1")
        update_lsn = _last_commit(db)
        db.execute("DELETE FROM t WHERE id = 1")
        db.execute(f"VACUUM t RETAIN LSN {update_lsn}")

        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {update_lsn} WHERE v = 'new'"
        )[0].rows == [(1, "new")]
        assert db.execute("SELECT * FROM t WHERE v = 'new'")[0].rows == []
    finally:
        db.close()


def test_reindex_closes_index_history_window(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        old_lsn = _last_commit(db)
        # 建索引会重建整份索引，之前的键历史不再保证在树里。
        db.execute("CREATE INDEX t_v ON t(v)")
        with pytest.raises(BinderError, match="历史窗口"):
            db.execute(
                f"SELECT * FROM t AS OF SYSTEM TIME LSN {old_lsn} WHERE v = 'old'"
            )

        db.execute("UPDATE t SET v = 'new' WHERE id = 1")
        mid_lsn = _last_commit(db)
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {mid_lsn} WHERE v = 'new'"
        )[0].rows == [(1, "new")]
        # REINDEX 同样会丢掉旧键项，所以它也必须关掉更早的索引历史窗口。
        db.execute("REINDEX t")
        with pytest.raises(BinderError, match="历史窗口"):
            db.execute(
                f"SELECT * FROM t AS OF SYSTEM TIME LSN {mid_lsn} WHERE v = 'new'"
            )
    finally:
        db.close()


def test_as_of_rejects_before_floor_and_future(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        window = db.execute("SHOW HISTORY WINDOW")[0].rows[0]
        floor = window[1]
        latest = window[2]
        with pytest.raises(BinderError, match="历史窗口"):
            db.execute(f"SELECT * FROM t AS OF SYSTEM TIME LSN {floor - 1}")
        with pytest.raises(BinderError, match="晚于最近一次提交"):
            db.execute(f"SELECT * FROM t AS OF SYSTEM TIME LSN {latest + 100}")
    finally:
        db.close()


def test_as_of_rejects_explicit_transaction(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        lsn = _last_commit(db)
        db.execute("BEGIN")
        try:
            with pytest.raises(NotSupportedError, match="显式事务"):
                db.execute(
                    f"SELECT * FROM t AS OF SYSTEM TIME LSN {lsn}"
                )
        finally:
            db.execute("ROLLBACK")
    finally:
        db.close()


def test_as_of_vacuum_advances_floor(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        insert_lsn = _last_commit(db)
        db.execute("DELETE FROM t WHERE id = 1")
        db.execute("VACUUM t")
        window = db.execute("SHOW HISTORY WINDOW")[0].rows[0]
        assert window[1] > insert_lsn
        with pytest.raises(BinderError, match="历史窗口"):
            db.execute(
                f"SELECT * FROM t AS OF SYSTEM TIME LSN {insert_lsn}"
            )

        db.execute("INSERT INTO t VALUES (2)")
        after_vacuum = _last_commit(db)
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {after_vacuum}"
        )[0].rows == [(2,)]
    finally:
        db.close()


def test_as_of_join_requires_same_target(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("CREATE TABLE u (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        first = _last_commit(db)
        db.execute("INSERT INTO u VALUES (2)")
        second = _last_commit(db)

        rows = db.execute(
            f"SELECT t.id, u.id FROM t AS OF SYSTEM TIME LSN {second} "
            f"JOIN u AS OF SYSTEM TIME LSN {second} ON t.id = u.id"
        )[0].rows
        assert rows == []
        rows = db.execute(
            f"SELECT t.id, u.id FROM t AS OF SYSTEM TIME LSN {second} "
            f"CROSS JOIN u AS OF SYSTEM TIME LSN {second}"
        )[0].rows
        assert rows == [(1, 2)]

        with pytest.raises(BinderError, match="目标点不一致"):
            db.execute(
                f"SELECT * FROM t AS OF SYSTEM TIME LSN {first} "
                f"JOIN u AS OF SYSTEM TIME LSN {second} ON t.id = u.id"
            )
        with pytest.raises(BinderError, match="目标点不一致"):
            db.execute(
                f"SELECT (SELECT COUNT(*) FROM u AS OF SYSTEM TIME LSN {second}) "
                f"FROM t AS OF SYSTEM TIME LSN {first}"
            )
    finally:
        db.close()


def test_as_of_parser_only_accepts_lsn():
    with pytest.raises(NotSupportedError, match="只支持 LSN"):
        parse_sql("SELECT * FROM t AS OF SYSTEM TIME '2026-01-01'")
    with pytest.raises(SQLSyntaxError, match="非负整数"):
        parse_sql("SELECT * FROM t AS OF SYSTEM TIME LSN")


def test_as_of_survives_reopen_with_pre_checkpoint_rows(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
    finally:
        db.close()

    db = Database(tmp_path)
    try:
        # 重开时 recover() 会做一次 checkpoint；旧行的 xmin 已被裁剪，
        # 但物理版本还在，目标点不早于新下界时应按水位前已提交恢复。
        db.execute("INSERT INTO t VALUES (2, 'two')")
        lsn = _last_commit(db)
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {lsn}"
        )[0].rows == [(1, "old"), (2, "two")]
    finally:
        db.close()


def test_as_of_uses_persistent_ledger_for_pre_checkpoint_delete(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        db.execute("DELETE FROM t WHERE id = 1")
        delete_lsn = _last_commit(db)
        db.checkpoint()
    finally:
        db.close()

    db = Database(tmp_path)
    try:
        # 旧版本仍留在堆里，xmax 指向 checkpoint 之前提交的删除事务。
        # 如果只靠当前 WAL 窗口，这个 xmax 无法归类，严格历史读会报错；
        # 持久账本必须能证明它在目标点之前已经提交。
        db.execute("INSERT INTO t VALUES (2, 'two')")
        latest = _last_commit(db)
        log_lsns = [row[1] for row in db.execute("SHOW COMMIT LOG")[0].rows]
        assert delete_lsn in log_lsns
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {latest}"
        )[0].rows == [(2, "two")]
    finally:
        db.close()


def test_as_of_reads_before_checkpoint_with_persistent_ledger(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        insert_lsn = _last_commit(db)
        db.checkpoint()
        db.execute("INSERT INTO t VALUES (2, 'two')")

        # checkpoint 只落盘和裁 WAL，不回收旧版本；持久账本仍保留两个提交，
        # 所以目标点可以落在 checkpoint 之前。
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {insert_lsn}"
        )[0].rows == [(1, "old")]
    finally:
        db.close()


def test_as_of_vacuum_floor_survives_reopen(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        insert_lsn = _last_commit(db)
        db.execute("DELETE FROM t WHERE id = 1")
        db.execute("VACUUM t")
    finally:
        db.close()

    db = Database(tmp_path)
    try:
        window = db.execute("SHOW HISTORY WINDOW")[0].rows[0]
        assert window[1] > insert_lsn
        with pytest.raises(BinderError, match="历史窗口"):
            db.execute(
                f"SELECT * FROM t AS OF SYSTEM TIME LSN {insert_lsn}"
            )
    finally:
        db.close()


def test_as_of_ddl_floor_survives_reopen(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        insert_lsn = _last_commit(db)
        db.execute("ALTER TABLE t ADD COLUMN v TEXT")
    finally:
        db.close()

    db = Database(tmp_path)
    try:
        window = db.execute("SHOW HISTORY WINDOW")[0].rows[0]
        assert window[1] > insert_lsn
        with pytest.raises(BinderError, match="历史窗口"):
            db.execute(
                f"SELECT * FROM t AS OF SYSTEM TIME LSN {insert_lsn}"
            )
    finally:
        db.close()


def test_vacuum_retain_keeps_target_versions(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        insert_lsn = _last_commit(db)
        db.execute("UPDATE t SET v = 'new' WHERE id = 1")
        update_lsn = _last_commit(db)
        db.execute("DELETE FROM t WHERE id = 1")

        result = db.execute(f"VACUUM t RETAIN LSN {update_lsn}")[0]
        assert "retain LSN" in result.tag
        assert db.execute("SELECT * FROM t")[0].rows == []
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {update_lsn}"
        )[0].rows == [(1, "new")]
        window = db.execute("SHOW HISTORY WINDOW")[0].rows[0]
        assert window[1] >= update_lsn
        with pytest.raises(BinderError, match="历史窗口"):
            db.execute(
                f"SELECT * FROM t AS OF SYSTEM TIME LSN {insert_lsn}"
            )
    finally:
        db.close()


def test_vacuum_retain_rejects_out_of_window(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        latest = _last_commit(db)
        floor = db.execute("SHOW HISTORY WINDOW")[0].rows[0][1]
        with pytest.raises(BinderError, match="早于"):
            db.execute(f"VACUUM t RETAIN LSN {floor - 1}")
        with pytest.raises(BinderError, match="晚于"):
            db.execute(f"VACUUM t RETAIN LSN {latest + 100}")
    finally:
        db.close()


def test_vacuum_retain_history_survives_reopen(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'old')")
        db.execute("UPDATE t SET v = 'new' WHERE id = 1")
        update_lsn = _last_commit(db)
        db.execute("DELETE FROM t WHERE id = 1")
        db.execute(f"VACUUM t RETAIN LSN {update_lsn}")
    finally:
        db.close()

    db = Database(tmp_path)
    try:
        assert db.execute(
            f"SELECT * FROM t AS OF SYSTEM TIME LSN {update_lsn}"
        )[0].rows == [(1, "new")]
        assert db.execute("SELECT * FROM t")[0].rows == []
    finally:
        db.close()


def test_as_of_rejects_write_statements(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("CREATE TABLE u (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        lsn = _last_commit(db)
        with pytest.raises(NotSupportedError, match="只支持只读查询"):
            db.execute(
                f"INSERT INTO u SELECT id FROM t AS OF SYSTEM TIME LSN {lsn}"
            )
        with pytest.raises(NotSupportedError, match="只支持只读查询"):
            db.execute(
                f"CREATE TABLE v AS SELECT id FROM t AS OF SYSTEM TIME LSN {lsn}"
            )
    finally:
        db.close()


def test_as_of_rejects_derived_table_source(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("INSERT INTO t VALUES (1)")
        lsn = _last_commit(db)
        with pytest.raises(NotSupportedError, match="只能直接作用于基表"):
            db.execute(
                f"SELECT * FROM (SELECT * FROM t) x AS OF SYSTEM TIME LSN {lsn}"
            )
        assert db.execute(
            f"SELECT * FROM (SELECT * FROM t AS OF SYSTEM TIME LSN {lsn}) x"
        )[0].rows == [(1,)]
    finally:
        db.close()


def test_as_of_rejects_view_source(tmp_path):
    db = Database(tmp_path)
    try:
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        db.execute("CREATE VIEW v AS SELECT * FROM t")
        db.execute("INSERT INTO t VALUES (1)")
        lsn = _last_commit(db)
        with pytest.raises(NotSupportedError, match="只能直接作用于基表"):
            db.execute(f"SELECT * FROM v AS OF SYSTEM TIME LSN {lsn}")
    finally:
        db.close()
