"""版本化目录：只有 schema 形状变化才产生新版本，旧版本可回溯。"""

import pytest

from lumendb.engine.database import Database
from lumendb.errors import TableNotFoundError
from lumendb.storage.pager import PAGE_SIZE


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
    db.execute_one("TRUNCATE TABLE t")
    assert db.catalog.catalog_version == 2, "TRUNCATE 只换根页，不能新增 schema 版本"
    db.execute_one("REINDEX TABLE t")
    assert db.catalog.catalog_version == 2, "REINDEX 只重建索引页，不能新增 schema 版本"
    db.close()


def test_repeated_truncate_does_not_accumulate_catalog_versions(tmp_path):
    db = Database(tmp_path / "db")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, pad TEXT)")
        pad = "v" * 900
        for i in range(80):
            db.execute_one("INSERT INTO t VALUES ($1, $2)", [i, pad])
        for _ in range(5):
            db.execute_one("TRUNCATE TABLE t")
            db.execute_one("INSERT INTO t VALUES (1, 'ok')")
        assert db.catalog.catalog_version == 1
        assert [h["catalog_version"] for h in db.catalog.history()] == [1]
    finally:
        db.close()

    data_file = tmp_path / "db" / "lumendb.data"
    assert data_file.stat().st_size // PAGE_SIZE <= 5
    reopened = Database(tmp_path / "db")
    try:
        assert reopened.catalog.catalog_version == 1
        assert [h["catalog_version"] for h in reopened.catalog.history()] == [1]
        assert reopened.execute_one("SELECT id FROM t").rows == [(1,)]
    finally:
        reopened.close()


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


def _fixed_catalog_clock(monkeypatch):
    from lumendb.catalog import catalog as catalog_module

    stamps = iter(
        f"2026-11-26T00:00:0{index}.000001+00:00" for index in range(10)
    )
    monkeypatch.setattr(catalog_module, "_utc_now_iso", lambda: next(stamps))


def test_show_columns_at_time_resolves_catalog_version(tmp_path, monkeypatch):
    from lumendb.errors import BinderError

    _fixed_catalog_clock(monkeypatch)
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    first = db.catalog.catalog_created_at
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


def test_show_tables_and_views_can_target_history(tmp_path, monkeypatch):
    from lumendb.errors import BinderError

    _fixed_catalog_clock(monkeypatch)
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    db.execute_one("CREATE VIEW v AS SELECT id FROM t")
    view_version = db.catalog.catalog_version
    view_time = db.catalog.catalog_created_at
    db.execute_one("ALTER TABLE t ADD COLUMN z INT")
    db.execute_one("DROP VIEW v")
    second = db.catalog.catalog_created_at
    assert view_time != second
    assert db.execute_one("SHOW TABLES").rows == [("t", 2, 0)]
    assert db.execute_one("SHOW VIEWS").rows == []
    assert db.execute_one(
        f"SHOW TABLES AT TIME '{view_time}'"
    ).rows == [("t", 1, 0)]
    assert db.execute_one(
        f"SHOW VIEWS AT TIME '{view_time}'"
    ).rows == [("v", "SELECT id FROM t")]
    assert db.execute_one("SHOW TABLES AT CATALOG VERSION 1").rows == [("t", 1, 0)]
    assert db.execute_one(
        f"SHOW VIEWS AT CATALOG VERSION {view_version}"
    ).rows == [("v", "SELECT id FROM t")]
    assert db.execute_one(
        "SHOW TABLES AT TIME '1970-01-01T00:00:00+00:00'"
    ).rows == []
    assert db.execute_one(
        "SHOW VIEWS AT TIME '1970-01-01T00:00:00+00:00'"
    ).rows == []
    with pytest.raises(BinderError):
        db.execute_one("SHOW TABLES AT CATALOG VERSION 99")
    db.close()


def test_show_statistics_can_target_history(tmp_path, monkeypatch):
    from lumendb.errors import BinderError

    _fixed_catalog_clock(monkeypatch)
    db = Database(tmp_path)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for value in range(10):
        db.execute_one(f"INSERT INTO t VALUES ({value})")
    db.execute_one("ANALYZE t")
    first = db.catalog.catalog_version
    db.execute_one("ALTER TABLE t ADD COLUMN z TEXT")
    second = db.catalog.catalog_version
    current = db.execute_one("SHOW STATISTICS FROM t")
    assert [row[1] for row in current.rows] == ["id", "z"]
    old = db.execute_one(f"SHOW STATISTICS FROM t AT CATALOG VERSION {first}")
    assert [row[1] for row in old.rows] == ["id"]
    assert old.rows[0][2:] == (10, 10, 0.0, "0.0", "9.0", 16, 0)
    assert "目录版本 1" in old.notice
    assert db.execute_one(
        "SHOW STATISTICS AT TIME '1970-01-01T00:00:00+00:00'"
    ).rows == []
    with pytest.raises(BinderError):
        db.execute_one("SHOW STATISTICS FROM t AT CATALOG VERSION 99")
    db.execute_one("CREATE TABLE u (id INT)")
    with pytest.raises(TableNotFoundError):
        db.execute_one(f"SHOW STATISTICS FROM u AT CATALOG VERSION {first}")
    db.close()
