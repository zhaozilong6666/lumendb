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
from .schema import (
    CheckDef,
    Column,
    ForeignKeyDef,
    IndexDef,
    TableDef,
    validate_table_def,
)
from .store import CatalogStore

CATALOG_VERSION = 1


def _rewrite_check_tokens(
    checks: dict[str, CheckDef],
    old_name: str,
    new_name: str,
    *,
    qualifier_only: bool,
) -> dict[str, CheckDef]:
    """重命名表/列时同步 CHECK token 里的标识符引用。

    列重命名跳过 ``old.col`` 里的表限定符；表重命名只改 ``old.col`` 里的
    表限定符。这样不会把同名的列引用误当成表名。
    """
    rewritten: dict[str, CheckDef] = {}
    old_key = old_name.lower()
    for key, check in checks.items():
        tokens = list(check.tokens)
        for i, (kind, value) in enumerate(tokens):
            if kind != "ident" or value.lower() != old_key:
                continue
            next_is_dot = (
                i + 1 < len(tokens)
                and tokens[i + 1][0] == "dot"
            )
            if qualifier_only and not next_is_dot:
                continue
            if not qualifier_only and next_is_dot:
                continue
            tokens[i] = (kind, new_name.lower())
        rewritten[key] = CheckDef(
            name=check.name,
            tokens=tuple(tokens),
            validated=check.validated,
        )
    return rewritten


class Catalog:
    def __init__(self, store: CatalogStore) -> None:
        self._store = store
        self.tables: dict[str, TableDef] = {}
        #: 视图名 -> ``AS`` 之后的词法 token 列表；只读、不落数据页。
        self.views: dict[str, list[tuple[str, str]]] = {}
        #: 视图名 -> 显式列名（可选）；空列表表示沿用查询输出的列名。
        self.view_columns: dict[str, list[str]] = {}
        #: 视图名 -> 实际引用到的表/视图名；用于拒绝悬空的 DROP。
        self.view_dependencies: dict[str, set[str]] = {}
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
        self.view_columns.clear()
        self.view_dependencies.clear()
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
            for vdata in data.get("views", []):
                name = vdata["name"].lower()
                self.views[name] = [
                    (kind, value) for kind, value in vdata.get("tokens", [])
                ]
                self.view_columns[name] = list(vdata.get("columns", []))
                self.view_dependencies[name] = set(vdata.get("dependencies", []))
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
                        "columns": self.view_columns.get(name, []),
                        "dependencies": sorted(
                            self.view_dependencies.get(name, set())
                        ),
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

    def get_view_columns(self, name: str) -> list[str]:
        return list(self.view_columns.get(name.lower(), []))

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def create_table(
        self,
        txn: Transaction,
        name: str,
        columns: list[Column],
        indexes: Optional[list[IndexDef]] = None,
        checks: Optional[dict[str, CheckDef]] = None,
        foreign_keys: Optional[dict[str, ForeignKeyDef]] = None,
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
        table.checks = dict(checks or {})
        table.foreign_keys = dict(foreign_keys or {})
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
        columns: Optional[list[str]] = None,
        dependencies: Optional[set[str]] = None,
    ) -> None:
        key = name.lower()
        if key in self.tables or key in self.views:
            raise TableAlreadyExistsError(
                f"对象 {name!r} 已存在",
                hint="表名和视图名共享同一个命名空间",
            )
        self.views[key] = list(tokens)
        self.view_columns[key] = list(columns or [])
        self.view_dependencies[key] = set(dependencies or ())

        def undo() -> None:
            self.views.pop(key, None)
            self.view_columns.pop(key, None)
            self.view_dependencies.pop(key, None)

        txn.undo_log.append(undo)
        self.persist(txn)

    def replace_view(
        self,
        txn: Transaction,
        name: str,
        tokens: list[tuple[str, str]],
        columns: Optional[list[str]] = None,
        dependencies: Optional[set[str]] = None,
    ) -> None:
        key = name.lower()
        if key not in self.views:
            raise TableNotFoundError(f"视图 {name!r} 不存在")
        old = self.views[key]
        old_columns = self.view_columns.get(key, [])
        old_dependencies = self.view_dependencies.get(key, set())
        self.views[key] = list(tokens)
        self.view_columns[key] = list(columns or [])
        self.view_dependencies[key] = set(dependencies or ())

        def undo() -> None:
            self.views[key] = old
            self.view_columns[key] = old_columns
            self.view_dependencies[key] = old_dependencies

        txn.undo_log.append(undo)
        self.persist(txn)

    def update_view_dependencies(
        self, txn: Transaction, updates: dict[str, set[str]]
    ) -> None:
        """刷新多个视图的依赖快照，并登记事务 undo。"""
        old = {
            key: set(self.view_dependencies.get(key, set()))
            for key in updates
        }
        for key, deps in updates.items():
            self.view_dependencies[key] = set(deps)

        def undo() -> None:
            for key, deps in old.items():
                self.view_dependencies[key] = deps

        txn.undo_log.append(undo)
        self.persist(txn)

    def drop_view(
        self, txn: Transaction, name: str, cascade: bool = False
    ) -> None:
        key = name.lower()
        if key not in self.views:
            raise TableNotFoundError(f"视图 {name!r} 不存在")
        dependents = self._view_dependents(key)
        if dependents:
            if not cascade:
                raise BinderError(
                    f"视图 {name!r} 被其他视图依赖：{', '.join(dependents)}",
                    hint="使用 DROP VIEW ... CASCADE，或先删除依赖视图",
                )
            for dependent in dependents:
                if dependent in self.views:
                    self.drop_view(txn, dependent, cascade=True)
        tokens = self.views.pop(key)
        columns = self.view_columns.pop(key, [])
        dependencies = self.view_dependencies.pop(key, set())

        def undo() -> None:
            self.views[key] = tokens
            self.view_columns[key] = columns
            self.view_dependencies[key] = dependencies

        txn.undo_log.append(undo)
        self.persist(txn)

    def _view_dependents(self, key: str) -> list[str]:
        return sorted(
            name
            for name, deps in self.view_dependencies.items()
            if key in deps
        )

    def drop_table(
        self, txn: Transaction, name: str, cascade: bool = False
    ) -> None:
        key = name.lower()
        if key not in self.tables:
            raise TableNotFoundError(f"表 {name!r} 不存在")
        dependents = self._view_dependents(key)
        if dependents:
            if not cascade:
                raise BinderError(
                    f"表 {name!r} 被视图依赖：{', '.join(dependents)}",
                    hint="使用 DROP TABLE ... CASCADE，或先 DROP VIEW 依赖它的视图",
                )
            for dependent in dependents:
                if dependent in self.views:
                    self.drop_view(txn, dependent, cascade=True)
        fk_dependents = [
            (other, fk.name)
            for other in self.tables.values()
            if other.name != key
            for fk in other.foreign_keys.values()
            if fk.ref_table == key
        ]
        if fk_dependents:
            if not cascade:
                names = ", ".join(
                    f"{other.name}.{fk_name}"
                    for other, fk_name in fk_dependents
                )
                raise BinderError(
                    f"表 {name!r} 被外键引用：{names}",
                    hint="使用 DROP TABLE ... CASCADE，或先删除引用它的外键",
                )
            old_fk_maps: dict[str, dict] = {}
            for other, _fk_name in fk_dependents:
                if other.name in old_fk_maps:
                    continue
                old_fk_maps[other.name] = dict(other.foreign_keys)
                other.foreign_keys = {
                    n: fk
                    for n, fk in other.foreign_keys.items()
                    if fk.ref_table != key
                }

            def undo_fks() -> None:
                for tname, fks in old_fk_maps.items():
                    if tname in self.tables:
                        self.tables[tname].foreign_keys = fks

            txn.undo_log.append(undo_fks)
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
        dependents = self._view_dependents(old_key)
        if dependents:
            raise BinderError(
                f"表 {old_name!r} 被视图依赖：{', '.join(dependents)}",
                hint="先 DROP VIEW 依赖它的视图，再重命名表",
            )
        if new_key in self.tables:
            raise TableAlreadyExistsError(
                f"表 {new_name!r} 已存在",
                hint="换一个表名，或先 DROP TABLE 旧表",
            )
        table = self.tables[old_key]
        old_checks = dict(table.checks)
        self.tables.pop(old_key)
        table.name = new_key
        table.checks = _rewrite_check_tokens(
            old_checks, old_key, new_key, qualifier_only=True
        )
        for index in table.indexes.values():
            index.table = new_key
        old_fk_refs: dict[str, dict[str, ForeignKeyDef]] = {}
        holders = [table] + [
            t for t in self.tables.values() if t is not table
        ]
        for holder in holders:
            targets = {
                name: fk
                for name, fk in holder.foreign_keys.items()
                if fk.ref_table == old_key
            }
            if not targets:
                continue
            old_fk_refs[old_key if holder is table else holder.name] = dict(
                holder.foreign_keys
            )
            for name, fk in targets.items():
                holder.foreign_keys[name] = replace(fk, ref_table=new_key)
        self.tables[new_key] = table

        def undo() -> None:
            self.tables.pop(new_key, None)
            table.name = old_key
            table.checks = old_checks
            for index in table.indexes.values():
                index.table = old_key
            for holder_name, fks in old_fk_refs.items():
                holder = table if holder_name == old_key else self.tables.get(holder_name)
                if holder is not None:
                    holder.foreign_keys = fks
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
        old_checks = dict(table.checks)
        old_index_columns = {
            idx.name: idx.columns for idx in table.indexes.values()
        }
        table.columns[index] = replace(table.columns[index], name=new_key)
        table.checks = _rewrite_check_tokens(
            old_checks, old_key, new_key, qualifier_only=False
        )
        for idx in table.indexes.values():
            idx.columns = tuple(
                new_key if cname == old_key else cname for cname in idx.columns
            )
        old_fk_maps: dict[str, dict[str, ForeignKeyDef]] = {}
        holders = [table] + [
            t for t in self.tables.values() if t is not table
        ]
        for holder in holders:
            fks = dict(holder.foreign_keys)
            changed = False
            for name, fk in list(fks.items()):
                columns = tuple(
                    new_key if cname == old_key else cname
                    for cname in fk.columns
                ) if holder is table else fk.columns
                ref_columns = tuple(
                    new_key if cname == old_key else cname
                    for cname in fk.ref_columns
                ) if fk.ref_table == table.name else fk.ref_columns
                if (columns, ref_columns) != (fk.columns, fk.ref_columns):
                    fks[name] = replace(
                        fk, columns=columns, ref_columns=ref_columns
                    )
                    changed = True
            if changed:
                old_fk_maps[holder.name] = dict(holder.foreign_keys)
                holder.foreign_keys = fks

        def undo() -> None:
            table.columns[index] = old_column
            table.checks = old_checks
            for idx in table.indexes.values():
                idx.columns = old_index_columns.get(idx.name, idx.columns)
            for holder_name, fks in old_fk_maps.items():
                holder = self.tables.get(holder_name)
                if holder is not None:
                    holder.foreign_keys = fks

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
