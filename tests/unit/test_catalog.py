"""目录的单元测试。"""

import pytest

from lumendb.catalog.catalog import Catalog
from lumendb.catalog.schema import Column, IndexDef, TableDef
from lumendb.catalog.store import CatalogStore
from lumendb.datatypes import DataType, TypeSpec
from lumendb.errors import (
    ColumnNotFoundError,
    IndexAlreadyExistsError,
    TableAlreadyExistsError,
    TableNotFoundError,
)
from lumendb.storage.bufferpool import BufferPoolManager
from lumendb.storage.pager import DiskManager
from lumendb.storage.txn import TransactionManager
from lumendb.storage.wal import WAL


@pytest.fixture
def env(tmp_path):
    dm = DiskManager(tmp_path / "data.db")
    wal = WAL(tmp_path / "wal.log")
    bp = BufferPoolManager(dm, wal, pool_size=16)
    txns = TransactionManager(dm, wal)
    cat = Catalog(CatalogStore(bp))
    cat.load()
    yield cat, txns, bp, dm, wal
    bp.close()
    wal.close()
    dm.close()


def users_columns():
    return [
        Column("id", TypeSpec(DataType.INT), nullable=False, primary_key=True),
        Column("name", TypeSpec(DataType.VARCHAR, 64), nullable=False),
        Column("age", TypeSpec(DataType.INT)),
    ]


class TestSchemaPersistence:
    def test_empty_catalog(self, env):
        cat, txns, bp, dm, wal = env
        assert cat.list_tables() == []

    def test_create_and_reload(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "users", users_columns())
        txns.commit(txn)
        bp.flush_all()

        cat2 = Catalog(CatalogStore(bp))
        cat2.load()
        assert cat2.table_names() == ["users"]
        table = cat2.get_table("users")
        assert table.column_names == ["id", "name", "age"]
        assert table.column("id").primary_key is True
        assert table.column("name").type == TypeSpec(DataType.VARCHAR, 64)

    def test_table_names_are_case_insensitive(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "Users", users_columns())
        txns.commit(txn)
        assert cat.get_table("USERS").name == "users"

    def test_duplicate_table_rejected(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "users", users_columns())
        with pytest.raises(TableAlreadyExistsError):
            cat.create_table(txn, "users", users_columns())
        txns.abort(txn)

    def test_duplicate_index_name_rejected(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        indexes = [
            IndexDef(name="idx_dup", table="users", columns=("name",), unique=True),
            IndexDef(name="idx_dup", table="users", columns=("age",), unique=True),
        ]
        with pytest.raises(IndexAlreadyExistsError):
            cat.create_table(txn, "users", users_columns(), indexes)
        txns.abort(txn)

    def test_drop_table(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "temp", users_columns())
        cat.drop_table(txn, "temp")
        txns.commit(txn)
        with pytest.raises(TableNotFoundError):
            cat.get_table("temp")

    def test_unknown_table_lists_available(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "alpha", users_columns())
        txns.commit(txn)
        with pytest.raises(TableNotFoundError) as exc:
            cat.get_table("beta")
        assert "alpha" in str(exc.value)

    def test_persist_survives_reopen_of_process(self, tmp_path):
        dm = DiskManager(tmp_path / "x.db")
        wal = WAL(tmp_path / "x.wal")
        bp = BufferPoolManager(dm, wal, pool_size=8)
        txns = TransactionManager(dm, wal)
        cat = Catalog(CatalogStore(bp))
        cat.load()
        txn = txns.begin()
        cat.create_table(txn, "t1", users_columns())
        txns.commit(txn)
        bp.flush_all()
        bp.close()
        wal.close()
        dm.close()

        dm2 = DiskManager(tmp_path / "x.db")
        wal2 = WAL(tmp_path / "x.wal")
        bp2 = BufferPoolManager(dm2, wal2, pool_size=8)
        cat2 = Catalog(CatalogStore(bp2))
        cat2.load()
        assert cat2.table_names() == ["t1"]
        bp2.close()
        wal2.close()
        dm2.close()


class TestIndexes:
    def test_create_index(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "users", users_columns())
        idx = cat.create_index(
            txn, "users", IndexDef(name="idx_age", table="users", columns=("age",))
        )
        txns.commit(txn)
        assert idx.table == "users"
        assert cat.get_table("users").indexes["idx_age"].columns == ("age",)

    def test_duplicate_index_rejected(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "users", users_columns())
        cat.create_index(
            txn, "users", IndexDef(name="i1", table="users", columns=("age",))
        )
        with pytest.raises(IndexAlreadyExistsError):
            cat.create_index(
                txn, "users", IndexDef(name="i1", table="users", columns=("age",))
            )
        txns.abort(txn)

    def test_index_on_missing_column(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "users", users_columns())
        with pytest.raises(ColumnNotFoundError):
            cat.create_index(
                txn, "users", IndexDef(name="bad", table="users", columns=("nope",))
            )
        txns.abort(txn)

    def test_drop_index(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cat.create_table(txn, "users", users_columns())
        cat.create_index(
            txn, "users", IndexDef(name="i1", table="users", columns=("age",))
        )
        assert cat.drop_index(txn, "i1") == "users"
        assert "i1" not in cat.get_table("users").indexes
        txns.commit(txn)


class TestValidation:
    def test_primary_key_must_be_not_null(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cols = [
            Column("id", TypeSpec(DataType.INT), nullable=True, primary_key=True),
        ]
        with pytest.raises(Exception):
            cat.create_table(txn, "bad", cols)
        txns.abort(txn)

    def test_duplicate_column_names(self, env):
        cat, txns, bp, dm, wal = env
        txn = txns.begin()
        cols = [
            Column("a", TypeSpec(DataType.INT)),
            Column("a", TypeSpec(DataType.INT)),
        ]
        with pytest.raises(Exception):
            cat.create_table(txn, "bad", cols)
        txns.abort(txn)
