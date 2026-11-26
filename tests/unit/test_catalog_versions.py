"""版本化目录：只有 schema 形状变化才产生新版本，旧版本可回溯。"""

import pytest

from lumendb.engine.database import Database
from lumendb.errors import TableNotFoundError


def test_catalog_version_advances_only_on_schema_shape(tmp_path):
    db = Database(tmp_path)
    assert db.catalog.catalog_version == 0
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    assert db.catalog.catalog_version == 1
    db.execute_one("INSERT INTO t VALUES (1)")
    db.execute_one("ANALYZE t")
    assert db.catalog.catalog_version == 1, "纯页根/统计更新不能伪造 schema 版本"
    db.execute_one("ALTER TABLE t ADD COLUMN v INT")
    assert db.catalog.catalog_version == 2
    db.close()


def test_history_keeps_old_columns_and_survives_reopen(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    db.execute_one("INSERT INTO t VALUES (1)")
    db.execute_one("ALTER TABLE t ADD COLUMN v INT")
    db.execute_one("CREATE INDEX ix_v ON t (v)")
    history = db.catalog.history()
    assert [item["catalog_version"] for item in history] == [3, 2, 1]
    assert db.catalog.table_at(1, "t").column_names == ["id"]
    assert db.catalog.table_at(2, "t").column_names == ["id", "v"]
    db.close()

    db = Database(tmp_path)
    assert db.catalog.catalog_version == 3
    assert [item["catalog_version"] for item in db.catalog.history()] == [3, 2, 1]
    assert db.catalog.table_at(1, "t").column_names == ["id"]
    db.close()


def test_show_catalog_versions(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    db.execute_one("ALTER TABLE t ADD COLUMN v INT")
    result = db.execute_one("SHOW CATALOG VERSIONS")
    assert result.columns[0].name == "catalog_version"
    assert [(row[0], row[3], row[1], row[2]) for row in result.rows] == [
        (2, 1, "t", ""),
        (1, 1, "t", ""),
    ]
    db.close()


def test_rolled_back_ddl_does_not_leak_catalog_version(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    db.execute_one("BEGIN")
    db.execute_one("ALTER TABLE t ADD COLUMN ghost INT")
    db.execute_one("ROLLBACK")
    assert db.catalog.catalog_version == 1
    assert [item["catalog_version"] for item in db.catalog.history()] == [1]

    db.execute_one("ALTER TABLE t ADD COLUMN real INT")
    assert db.catalog.catalog_version == 2
    assert db.catalog.table_at(2, "t").column_names == ["id", "real"]
    db.close()


def test_reopen_does_not_mistake_dml_for_schema_change(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    db.execute_one("ALTER TABLE t ADD COLUMN v INT")
    db.close()

    db = Database(tmp_path)
    assert db.catalog.catalog_version == 2
    db.execute_one("INSERT INTO t VALUES (1, 2)")
    db.execute_one("ANALYZE t")
    assert db.catalog.catalog_version == 2
    db.execute_one("ALTER TABLE t ADD COLUMN w INT")
    assert db.catalog.catalog_version == 3
    assert [item["catalog_version"] for item in db.catalog.history()] == [3, 2, 1]
    db.close()


def test_history_keeps_view_definitions(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT)")
    db.execute_one("CREATE VIEW v1 AS SELECT id FROM t")
    db.execute_one("ALTER TABLE t ADD COLUMN z INT")
    db.execute_one("DROP VIEW v1")
    view = db.catalog.view_at(2, "v1")
    assert view is not None
    assert view["dependencies"] == ["t"]
    assert [token[1] for token in view["tokens"]] == ["SELECT", "id", "FROM", "t"]
    assert db.catalog.view_at(3, "v1") is not None
    assert db.catalog.view_at(4, "v1") is None
    versions = db.execute_one("SHOW CATALOG VERSIONS").rows
    assert versions[1][2] == "v1", "删除前的历史版本仍应能看到视图名"
    db.close()


def test_show_columns_can_target_historical_catalog_version(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    db.execute_one("ALTER TABLE t ADD COLUMN z INT DEFAULT 7")
    assert db.execute_one("SHOW COLUMNS FROM t").rows == [
        ("id", "INT", "NO", "YES", ""),
        ("v", "TEXT", "YES", "NO", ""),
        ("z", "INT", "YES", "NO", "7"),
    ]
    assert db.execute_one("SHOW COLUMNS FROM t AT CATALOG VERSION 1").rows == [
        ("id", "INT", "NO", "YES", ""),
        ("v", "TEXT", "YES", "NO", ""),
    ]
    with pytest.raises(TableNotFoundError):
        db.execute_one("SHOW COLUMNS FROM t AT CATALOG VERSION 99")
    db.close()


def test_catalog_versions_carry_created_at_and_time_lookup(tmp_path):
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    first = db.catalog.catalog_created_at
    assert first
    db.execute_one("ALTER TABLE t ADD COLUMN v INT")
    second = db.catalog.catalog_created_at
    assert second
    history = db.catalog.history()
    assert [item["created_at"] for item in history][:2] == [second, first]
    result = db.execute_one("SHOW CATALOG VERSIONS")
    assert result.columns[4].name == "created_at"
    assert result.rows[0][4] == second
    assert db.catalog.version_at_or_before("2099-01-01T00:00:00+00:00") == 2
    assert db.catalog.version_at_or_before("1970-01-01T00:00:00+00:00") is None
    db.close()


def test_show_columns_at_time_resolves_catalog_version(tmp_path):
    import time

    from lumendb.errors import BinderError

    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    first = db.catalog.catalog_created_at
    time.sleep(1.05)
    db.execute_one("ALTER TABLE t ADD COLUMN v INT")
    second = db.catalog.catalog_created_at
    assert first != second
    assert db.execute_one(
        f"SHOW COLUMNS FROM t AT TIME '{first}'"
    ).rows == [("id", "INT", "NO", "YES", "")]
    assert db.execute_one(
        f"SHOW COLUMNS FROM t AT TIME '{second}'"
    ).rows == [
        ("id", "INT", "NO", "YES", ""),
        ("v", "INT", "YES", "NO", ""),
    ]
    with pytest.raises(TableNotFoundError):
        db.execute_one("SHOW COLUMNS FROM t AT TIME '1970-01-01T00:00:00+00:00'")
    with pytest.raises(BinderError):
        db.execute_one("SHOW COLUMNS FROM t AT TIME 'not-a-time'")
    db.close()
