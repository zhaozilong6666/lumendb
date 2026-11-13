"""目录管理：表的增删查与序列化。"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Optional

from ..errors import (
    BinderError,
    ColumnNotFoundError,
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
        #: 视图名 -> ``AS`` 之后的词法 token 列表；只读、不落数据页。
        self.views: dict[str, list[tuple[str, str]]] = {}
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
        self.views.clear()
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
            self.views = {
                vdata["name"].lower(): [
                    (kind, value) for kind, value in vdata.get("tokens", [])
                ]
                for vdata in data.get("views", [])
            }
        self._loaded = True

    def persist(self, txn: Transaction) -> None:
        payload = json.dumps(
            {
                "version": CATALOG_VERSION,
                "tables": [t.to_json() for t in self.tables.values()],
                "views": [
                    {
                        "name": name,
                        "tokens": [[kind, value] for kind, value in tokens],
                    }
                    for name, tokens in self.views.items()
                ],
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

    def view_names(self) -> list[str]:
        return sorted(self.views)

    def has_view(self, name: str) -> bool:
        return name.lower() in self.views

    def get_view(self, name: str) -> list[tuple[str, str]]:
        key = name.lower()
        tokens = self.views.get(key)
        if tokens is None:
            raise TableNotFoundError(
                f"视图 {name!r} 不存在",
                hint="可用视图：" + (", ".join(self.view_names()) or "（空）"),
            )
        return tokens

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
        if key in self.tables or key in self.views:
            raise TableAlreadyExistsError(
                f"表 {name!r} 已存在",
                hint="使用 DROP TABLE 后再建，或换一个名字",
            )
        table = TableDef(name=key, columns=columns)
        for idx in indexes or []:
            if idx.name in table.indexes:
                raise IndexAlreadyExistsError(
                    f"索引/约束名重复：{idx.name}",
                    hint="同一张表内索引名必须唯一",
                )
            idx.table = key
            table.indexes[idx.name] = idx
        validate_table_def(table)
        self.tables[key] = table
        txn.undo_log.append(lambda: self.tables.pop(key, None))
        self.persist(txn)
        return table

    def create_view(
        self,
        txn: Transaction,
        name: str,
        tokens: list[tuple[str, str]],
    ) -> None:
        key = name.lower()
        if key in self.tables or key in self.views:
            raise TableAlreadyExistsError(
                f"对象 {name!r} 已存在",
                hint="表名和视图名共享同一个命名空间",
            )
        self.views[key] = list(tokens)

        def undo() -> None:
            self.views.pop(key, None)

        txn.undo_log.append(undo)
        self.persist(txn)

    def drop_view(self, txn: Transaction, name: str) -> None:
        key = name.lower()
        if key not in self.views:
            raise TableNotFoundError(f"视图 {name!r} 不存在")
        tokens = self.views.pop(key)

        def undo() -> None:
            self.views[key] = tokens

        txn.undo_log.append(undo)
        self.persist(txn)

    def drop_table(self, txn: Transaction, name: str) -> None:
        key = name.lower()
        if key not in self.tables:
            raise TableNotFoundError(f"表 {name!r} 不存在")
        table = self.tables.pop(key)

        def undo() -> None:
            self.tables[key] = table

        txn.undo_log.append(undo)
        self.persist(txn)

    def rename_table(self, txn: Transaction, old_name: str, new_name: str) -> TableDef:
        old_key = old_name.lower()
        new_key = new_name.lower()
        if old_key not in self.tables:
            raise TableNotFoundError(f"表 {old_name!r} 不存在")
        if new_key in self.tables:
            raise TableAlreadyExistsError(
                f"表 {new_name!r} 已存在",
                hint="换一个表名，或先 DROP TABLE 旧表",
            )
        table = self.tables.pop(old_key)
        table.name = new_key
        for index in table.indexes.values():
            index.table = new_key
        self.tables[new_key] = table

        def undo() -> None:
            self.tables.pop(new_key, None)
            table.name = old_key
            for index in table.indexes.values():
                index.table = old_key
            self.tables[old_key] = table

        txn.undo_log.append(undo)
        validate_table_def(table)
        self.persist(txn)
        return table

    def rename_column(
        self, txn: Transaction, table_name: str, old_name: str, new_name: str
    ) -> TableDef:
        table = self.get_table(table_name)
        old_key = old_name.lower()
        new_key = new_name.lower()
        if not table.has_column(old_key):
            raise ColumnNotFoundError(
                f"表 {table.name} 没有列 {old_name!r}",
                hint=f"可用列：{', '.join(table.column_names)}",
            )
        if table.has_column(new_key):
            raise BinderError(
                f"列 {new_name!r} 已存在",
                hint="换一个列名",
            )
        index = table.column_index(old_key)
        old_column = table.columns[index]
        old_index_columns = {
            idx.name: idx.columns for idx in table.indexes.values()
        }
        table.columns[index] = replace(table.columns[index], name=new_key)
        for idx in table.indexes.values():
            idx.columns = tuple(
                new_key if cname == old_key else cname for cname in idx.columns
            )

        def undo() -> None:
            table.columns[index] = old_column
            for idx in table.indexes.values():
                idx.columns = old_index_columns.get(idx.name, idx.columns)

        txn.undo_log.append(undo)
        validate_table_def(table)
        self.persist(txn)
        return table

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

        def undo() -> None:
            table.indexes.pop(index.name, None)

        txn.undo_log.append(undo)
        self.persist(txn)
        return index

    def drop_index(self, txn: Transaction, index_name: str) -> str:
        for table in self.tables.values():
            if index_name in table.indexes:
                index = table.indexes.pop(index_name)

                def undo() -> None:
                    table.indexes[index_name] = index

                txn.undo_log.append(undo)
                self.persist(txn)
                return table.name
        raise IndexNotFoundError(f"索引 {index_name!r} 不存在")


__all__ = ["Catalog", "CATALOG_VERSION"]
