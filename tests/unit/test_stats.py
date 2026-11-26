"""ANALYZE 与统计驱动估算：D 子系统的第一块。"""

from __future__ import annotations

import pytest

from lumendb.engine.database import Database


def _plan(db: Database, sql: str) -> str:
    return "\n".join(row[0] for row in db.execute_one("EXPLAIN " + sql).rows)


def test_analyze_computes_column_stats_and_persists(tmp_path):
    db = Database(tmp_path / "stats")
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, code INT, pad VARCHAR(16))")
    for i in range(12):
        db.execute_one(f"INSERT INTO t VALUES ({i}, {i % 4}, 'p{i}')")
    result = db.execute_one("ANALYZE t")
    assert "3 column(s)" in result.tag
    table = db.catalog.get_table("t")
    assert table.stats_rows == 12
    assert table.column_stats["code"].n_distinct == 4
    assert table.column_stats["code"].null_frac == 0.0
    db.close()

    reopened = Database(tmp_path / "stats")
    try:
        table = reopened.catalog.get_table("t")
        assert table.stats_rows == 12
        assert table.column_stats["code"].n_distinct == 4
        assert table.column_stats["id"].n_distinct == 12
    finally:
        reopened.close()


def test_analyze_changes_equality_estimate(tmp_path):
    db = Database(tmp_path / "estimate")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, code INT)")
        for i in range(100):
            db.execute_one(f"INSERT INTO t VALUES ({i}, {i % 4})")
        before = _plan(db, "SELECT id FROM t WHERE code = 1")
        assert "rows≈10" in before, before
        db.execute_one("ANALYZE t")
        after = _plan(db, "SELECT id FROM t WHERE code = 1")
        assert "rows≈25" in after, after
        assert "reason=顺序扫描基表" in after
    finally:
        db.close()


def test_explain_reports_index_choice_reason(tmp_path):
    db = Database(tmp_path / "access_reason")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, code INT)")
        for i in range(12):
            db.execute_one(f"INSERT INTO t VALUES ({i}, {i % 4})")
        db.execute_one("ANALYZE t")
        db.execute_one("CREATE INDEX ix_code ON t (code)")
        plan = _plan(db, "SELECT id FROM t WHERE code = 1")
        assert "IndexScan" in plan
        assert "reason=等值谓词命中索引" in plan
        assert "SeqScan≈12" in plan and "IndexScan≈3" in plan
    finally:
        db.close()


def test_analyze_histogram_drives_range_estimate(tmp_path):
    db = Database(tmp_path / "range")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        for i in range(100):
            db.execute_one(f"INSERT INTO t VALUES ({i}, {i})")
        before = _plan(db, "SELECT id FROM t WHERE id > 90")
        assert "rows≈33" in before, before
        db.execute_one("ANALYZE t")
        after = _plan(db, "SELECT id FROM t WHERE id > 90")
        assert "rows≈10" in after, after
        assert "范围已由 IndexRange 应用" in after
        assert "SeqScan≈100 → IndexRange≈10" in after
    finally:
        db.close()


def test_string_histogram_drives_lexicographic_range_estimate(tmp_path):
    db = Database(tmp_path / "string_range")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        for i in range(100):
            db.execute_one(f"INSERT INTO t VALUES ({i}, '{i:03d}')")
        before = _plan(db, "SELECT id FROM t WHERE s > '090'")
        assert "rows≈33" in before, before
        db.execute_one("ANALYZE t")
        stats = db.catalog.get_table("t").column_stats["s"]
        assert stats.min_value == "000"
        assert stats.max_value == "099"
        assert len(stats.histogram) == 16
        assert sum(count for _lo, _hi, count in stats.histogram) == 100
        after = _plan(db, "SELECT id FROM t WHERE s > '090'")
        assert "rows≈13" in after, after
        # 字符串边界不能按数值转换：'090' 在字典序里仍小于 '1'。
        assert stats.range_selectivity("090", None) == pytest.approx(0.13)
    finally:
        db.close()


def test_show_statistics_exposes_analyzed_catalog(tmp_path):
    db = Database(tmp_path / "show_stats")
    try:
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        for i in range(20):
            db.execute_one(f"INSERT INTO t VALUES ({i}, '{i:02d}')")
        db.execute_one("ANALYZE t")
        result = db.execute_one("SHOW STATISTICS FROM t")
        assert [col.name for col in result.columns] == [
            "table",
            "column",
            "rows",
            "n_distinct",
            "null_frac",
            "min",
            "max",
            "buckets",
        ]
        rows = {row[1]: row for row in result.rows}
        assert rows["id"][2:] == (20, 20, 0.0, "0.0", "19.0", 16)
        assert rows["s"][2:] == (20, 20, 0.0, "00", "19", 16)
        assert result.tag == "STATISTICS 2"
    finally:
        db.close()
