"""目录管理：表的增删查与序列化。"""

from __future__ import annotations

import json
from typing import Optional

from ..errors import (
    IndexAlreadyExistsError,
    IndexNotFoundError,
    TableAlreadyExistsError,
    TableNotFoundError,
    NotSupportedError,
)
from ..storage.page import INVALID_PAGE_ID
from ..storage.txn import Transaction
from .schema import Column, IndexDef, TableDef, validate_table_def
from .store import CatalogStore

CATALOG_VERSION = 1


class Catalog:
    def __init__(self, store: CatalogStore) -> None:
        self._store = store
        self.tables: dict[str, TableDef] = {}
        self._loaded = False
        #: DML 可能改动表/索引的根页，由执行器置位，提交前统一落盘
        self.dirty = False

    def mark_dirty(self) -> None:
        self.dirty = True

    # ------------------------------------------------------------------
    # 加载 / 持久化
    # ------------------------------------------------------------------

    def load(self) -> None:
        raw = self._store.load()
        self.tables.clear()
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise NotSupportedError(
                    "目录内容无法解析",
                    detail=str(exc),
                    hint="数据文件可能损坏，或来自不兼容的版本",
                ) from exc
            version = data.get("version")
            if version != CATALOG_VERSION:
                raise NotSupportedError(
                    f"目录版本 {version} 不受支持",
                    detail=f"当前仅支持版本 {CATALOG_VERSION}",
                )
            for tdata in data.get("tables", []):
                table = TableDef.from_json(tdata)
                self.tables[table.name] = table
        self._loaded = True

    def persist(self, txn: Transaction) -> None:
        payload = json.dumps(
            {
                "version": CATALOG_VERSION,
                "tables": [t.to_json() for t in self.tables.values()],
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self._store.save(txn, payload)
        self.dirty = False

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_table(self, name: str) -> TableDef:
        table = self.tables.get(name.lower())
        if table is None:
            raise TableNotFoundError(
                f"表 {name!r} 不存在",
                hint="可用表：" + (", ".join(sorted(self.tables)) or "（空）"),
            )
        return table

    def has_table(self, name: str) -> bool:
        return name.lower() in self.tables

    def list_tables(self) -> list[TableDef]:
        return [self.tables[k] for k in sorted(self.tables)]

    def table_names(self) -> list[str]:
        return sorted(self.tables)

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def create_table(
        self,
        txn: Transaction,
        name: str,
        columns: list[Column],
        indexes: Optional[list[IndexDef]] = None,
    ) -> TableDef:
        key = name.lower()
        if key in self.tables:
            raise TableAlreadyExistsError(
                f"表 {name!r} 已存在",
                hint="使用 DROP TABLE 后再建，或换一个名字",
            )
        table = TableDef(name=key, columns=columns)
        for idx in indexes or []:
            idx.table = key
            table.indexes[idx.name] = idx
        validate_table_def(table)
        self.tables[key] = table
        self.persist(txn)
        return table

    def drop_table(self, txn: Transaction, name: str) -> None:
        key = name.lower()
        if key not in self.tables:
            raise TableNotFoundError(f"表 {name!r} 不存在")
        del self.tables[key]
        self.persist(txn)

    def create_index(
        self,
        txn: Transaction,
        table_name: str,
        index: IndexDef,
    ) -> IndexDef:
        table = self.get_table(table_name)
        if index.name in table.indexes:
            raise IndexAlreadyExistsError(f"索引 {index.name!r} 已存在")
        for cname in index.columns:
            table.column(cname)  # 借它的报错检查列是否存在
        index.table = table.name
        index.root_page = INVALID_PAGE_ID
        table.indexes[index.name] = index
        self.persist(txn)
        return index

    def drop_index(self, txn: Transaction, index_name: str) -> str:
        for table in self.tables.values():
            if index_name in table.indexes:
                del table.indexes[index_name]
                self.persist(txn)
                return table.name
        raise IndexNotFoundError(f"索引 {index_name!r} 不存在")


__all__ = ["Catalog", "CATALOG_VERSION"]
