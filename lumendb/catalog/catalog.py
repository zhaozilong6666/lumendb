"""目录管理：表的增删查与序列化。"""

from __future__ import annotations

import functools
import json
from datetime import datetime, timezone
import threading
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


def _locked(method):
    """目录的读/写方法互斥。

    线协议的服务端并发（docs/concurrency.md 第 1.8 步）会让多个连接的
    SELECT 在各自线程里同时读目录；而 DDL 线程会 ``insert``/``del``
    ``self.tables``。CPython 保证单次 dict 操作原子，但**迭代**期间被改会抛
    ``RuntimeError: dictionary changed size during iteration``——所以凡是
    遍历 dict 的方法（以及所有会改 dict 的方法）都进同一把 RLock。
    锁是叶子锁：持有 ``Database._write_lock`` 的写路径可以继续拿它，
    反向（目录锁内去拿写锁）不存在。
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


def _utc_now_iso() -> str:
    """目录版本时间戳；用微秒避免同一秒内多次 DDL 无法按时间区分。"""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class Catalog:
    def __init__(self, store: CatalogStore) -> None:
        self._store = store
        self._lock = threading.RLock()
        self.tables: dict[str, TableDef] = {}
        #: 逻辑 schema 版本序号；只在 schema 形状变化时递增，纯页根更新不增。
        self.catalog_version = 0
        #: 上一个 schema 版本的目录根页；用于版本链回溯。
        self._previous_root = 0
        #: 当前 schema 版本的创建时间（UTC ISO-8601），用于按时间定位目录版本。
        self.catalog_created_at = ""
        #: 最近一次落盘时的形状版本，用来识别“这次 persist 是否改形状”。
        self._last_shape_version = 0
        #: 只在列/表/索引/视图形状变化时递增；TRUNCATE/REINDEX 只换根页不增。
        self._shape_version = 0
        #: 视图名 -> ``AS`` 之后的词法 token 列表；只读、不落数据页。
        self.views: dict[str, list[tuple[str, str]]] = {}
        #: 视图名 -> 显式列名（可选）；空列表表示沿用查询输出的列名。
        self.view_columns: dict[str, list[str]] = {}
        #: 视图名 -> 实际引用到的表/视图名；用于拒绝悬空的 DROP。
        self.view_dependencies: dict[str, set[str]] = {}
        #: 视图名 -> 实际引用到的基表列 (表名, 列名)；用于列级依赖检查。
        self.view_column_dependencies: dict[str, set[tuple[str, str]]] = {}
        self._loaded = False
        #: DML 可能改动表/索引的根页，由执行器置位，提交前统一落盘
        self.dirty = False
        #: schema 形状版本：DDL 与副本重载目录时递增，供绑定计划缓存失效。
        self.schema_version = 0
        #: (表名, 索引名) -> 根页锁。B+ 树是每语句新建的短命对象，根分裂的
        #: 串行化必须挂在目录上才能跨实例共享；否则两个线程各拿一把实例锁，
        #: 同时建新根并各自回写，后写者覆盖前者，子树失联。
        self._index_root_locks: dict[tuple[str, str], threading.RLock] = {}

    @_locked
    def bump_schema(self, *, shape: bool = True) -> None:
        """失效计划缓存；``shape=True`` 时同时推进持久 catalog 版本。

        TRUNCATE/REINDEX 只换根页、不改列/索引形状，调用方传 ``shape=False``：
        schema_version 仍递增（旧计划里的页对象可能过期），但不会为每次
        TRUNCATE 累积一个可回溯的目录版本页。
        """
        self.schema_version += 1
        if shape:
            self._shape_version += 1

    @_locked
    def mark_dirty(self) -> None:
        self.dirty = True

    @_locked
    def index_root_lock(self, table_name: str, index_name: str) -> threading.RLock:
        """返回某索引的根页锁（同库同名共享一把）。"""
        key = (table_name, index_name)
        lock = self._index_root_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            self._index_root_locks[key] = lock
        return lock

    # ------------------------------------------------------------------
    # 加载 / 持久化
    # ------------------------------------------------------------------

    @_locked
    def load(self) -> None:
        raw = self._store.load()
        self.tables.clear()
        self.views.clear()
        self.view_columns.clear()
        self.view_dependencies.clear()
        self.view_column_dependencies.clear()
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
            self.catalog_version = int(data.get("catalog_version", 1))
            self._previous_root = int(data.get("previous_root", 0))
            self.catalog_created_at = str(data.get("created_at", ""))
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
                self.view_column_dependencies[name] = {
                    (table, column)
                    for table, column in vdata.get("column_dependencies", [])
                }
        self._loaded = True
        # 副本收到主库的目录页后会重载：旧计划引用的表/列对象已经过期。
        self.schema_version += 1
        # schema_version 是进程内计划缓存代际，不是持久值；重开库后从当前
        # 代际起算，避免把一次普通 DML 的目录保存误判成 schema 变化。
        self._last_shape_version = self._shape_version = self.catalog_version

    @_locked
    def persist(self, txn: Transaction) -> None:
        txn.wrote = True
        previous_root = self._store.current_root()
        schema_changed = self._shape_version != self._last_shape_version
        if schema_changed:
            self.catalog_version += 1
            before_version = self.catalog_version - 1
            before_previous = self._previous_root
            before_created = self.catalog_created_at
            self.catalog_created_at = _utc_now_iso()

            def undo_version() -> None:
                # 回滚/失败的 DDL 不能留下一个“从未提交过”的 catalog 版本号。
                # schema_version 不回滚（它只负责失效计划缓存），所以这里
                # 只恢复版本序号、上一版根页和创建时间。
                self.catalog_version = before_version
                self._previous_root = before_previous
                self.catalog_created_at = before_created

            # 在写盘之前登记：save 中途 EIO 时事务 abort 也要把版本号退回去。
            txn.undo_log.append(undo_version)
        history_root = previous_root if schema_changed else self._previous_root
        payload = json.dumps(
            {
                "version": CATALOG_VERSION,
                "catalog_version": self.catalog_version,
                "previous_root": history_root,
                "created_at": self.catalog_created_at,
                "tables": [t.to_json() for t in self.tables.values()],
                "views": [
                    {
                        "name": name,
                        "tokens": [[kind, value] for kind, value in tokens],
                        "columns": self.view_columns.get(name, []),
                        "dependencies": sorted(
                            self.view_dependencies.get(name, set())
                        ),
                        "column_dependencies": sorted(
                            self.view_column_dependencies.get(name, set())
                        ),
                    }
                    for name, tokens in self.views.items()
                ],
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self._store.save(txn, payload, retain_old=schema_changed)
        if schema_changed:
            self._previous_root = previous_root
            self._last_shape_version = self._shape_version
        self.dirty = False

    # ------------------------------------------------------------------
    # 版本历史
    # ------------------------------------------------------------------

    @_locked
    def history(self, limit: int = 1000) -> list[dict]:
        """沿 ``previous_root`` 回溯 schema 版本链，返回当前在前的快照元数据。"""
        result: list[dict] = []
        root = self._store.current_root()
        seen: set[int] = set()
        while root and root not in seen and len(result) < limit:
            seen.add(root)
            raw = self._store.load_at_root(root)
            if not raw:
                break
            data = json.loads(raw.decode("utf-8"))
            tables = data.get("tables", [])
            views = data.get("views", [])
            result.append(
                {
                    "catalog_version": int(data.get("catalog_version", 1)),
                    "created_at": str(data.get("created_at", "")),
                    "root_page": root,
                    "tables": [
                        {
                            "name": table.get("name", ""),
                            "columns": [
                                column.get("name", "")
                                for column in table.get("columns", [])
                            ],
                            "indexes": sorted(table.get("indexes", {})),
                        }
                        for table in tables
                    ],
                    "views": [
                        {
                            "name": view.get("name", ""),
                            "columns": list(view.get("columns", [])),
                            "dependencies": sorted(view.get("dependencies", [])),
                        }
                        for view in views
                    ],
                }
            )
            root = int(data.get("previous_root", 0))
        return result

    def _view_record(self, vdata: dict) -> dict:
        return {
            "name": vdata.get("name", ""),
            "tokens": [(kind, value) for kind, value in vdata.get("tokens", [])],
            "columns": list(vdata.get("columns", [])),
            "dependencies": sorted(vdata.get("dependencies", [])),
        }

    @_locked
    def view_at(self, catalog_version: int, name: str) -> Optional[dict]:
        """读某个历史 catalog 版本里的视图定义；不存在返回 None。"""
        data = self._root_data(catalog_version)
        if data is None:
            return None
        key = name.lower()
        for vdata in data.get("views", []):
            if vdata.get("name", "").lower() == key:
                return self._view_record(vdata)
        return None

    @_locked
    def views_at(self, catalog_version: int) -> list[dict]:
        """读某个历史 catalog 版本里的全部视图定义；版本不存在返回空列表。"""
        data = self._root_data(catalog_version)
        if data is None:
            return []
        return [self._view_record(vdata) for vdata in data.get("views", [])]

    @_locked
    def version_at_or_before(self, timestamp: str) -> Optional[int]:
        """返回 ``timestamp`` 当时或之前最近的 catalog 版本号。

        时间只用于**目录**定位，不代表数据页可回到该时刻；数据时间旅行还需要
        版本保留水位和按提交时间过滤的可见性。
        """
        for snapshot in self.history():
            created = snapshot.get("created_at", "")
            if created and created <= timestamp:
                return int(snapshot["catalog_version"])
        return None

    @_locked
    def version_exists(self, catalog_version: int) -> bool:
        return any(
            snapshot["catalog_version"] == catalog_version
            for snapshot in self.history()
        )

    def _root_data(self, catalog_version: int) -> Optional[dict]:
        """读某个历史版本的目录 JSON；版本不在链上返回 None。"""
        for snapshot in self.history():
            if snapshot["catalog_version"] != catalog_version:
                continue
            raw = self._store.load_at_root(snapshot["root_page"])
            if not raw:
                return None
            return json.loads(raw.decode("utf-8"))
        return None

    @_locked
    def table_at(self, catalog_version: int, name: str) -> Optional[TableDef]:
        """读某个历史 catalog 版本里的表定义；不存在返回 None。"""
        data = self._root_data(catalog_version)
        if data is None:
            return None
        for tdata in data.get("tables", []):
            table = TableDef.from_json(tdata)
            if table.name == name.lower():
                return table
        return None

    @_locked
    def tables_at(self, catalog_version: int) -> list[TableDef]:
        """读某个历史 catalog 版本里的全部表定义；版本不存在返回空列表。"""
        data = self._root_data(catalog_version)
        if data is None:
            return []
        return [TableDef.from_json(tdata) for tdata in data.get("tables", [])]

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    @_locked
    def get_table(self, name: str) -> TableDef:
        table = self.tables.get(name.lower())
        if table is None:
            raise TableNotFoundError(
                f"表 {name!r} 不存在",
                hint="可用表：" + (", ".join(sorted(self.tables)) or "（空）"),
            )
        return table

    @_locked
    def has_table(self, name: str) -> bool:
        return name.lower() in self.tables

    @_locked
    def list_tables(self) -> list[TableDef]:
        return [self.tables[k] for k in sorted(self.tables)]

    @_locked
    def table_names(self) -> list[str]:
        return sorted(self.tables)

    @_locked
    def view_names(self) -> list[str]:
        return sorted(self.views)

    @_locked
    def has_view(self, name: str) -> bool:
        return name.lower() in self.views

    @_locked
    def get_view(self, name: str) -> list[tuple[str, str]]:
        key = name.lower()
        tokens = self.views.get(key)
        if tokens is None:
            raise TableNotFoundError(
                f"视图 {name!r} 不存在",
                hint="可用视图：" + (", ".join(self.view_names()) or "（空）"),
            )
        return tokens

    @_locked
    def get_view_columns(self, name: str) -> list[str]:
        return list(self.view_columns.get(name.lower(), []))

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    @_locked
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

    @_locked
    def create_view(
        self,
        txn: Transaction,
        name: str,
        tokens: list[tuple[str, str]],
        columns: Optional[list[str]] = None,
        dependencies: Optional[set[str]] = None,
        column_dependencies: Optional[set[tuple[str, str]]] = None,
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
        self.view_column_dependencies[key] = set(column_dependencies or ())

        def undo() -> None:
            self.views.pop(key, None)
            self.view_columns.pop(key, None)
            self.view_dependencies.pop(key, None)
            self.view_column_dependencies.pop(key, None)

        txn.undo_log.append(undo)
        self.persist(txn)

    @_locked
    def replace_view(
        self,
        txn: Transaction,
        name: str,
        tokens: list[tuple[str, str]],
        columns: Optional[list[str]] = None,
        dependencies: Optional[set[str]] = None,
        column_dependencies: Optional[set[tuple[str, str]]] = None,
    ) -> None:
        key = name.lower()
        if key not in self.views:
            raise TableNotFoundError(f"视图 {name!r} 不存在")
        old = self.views[key]
        old_columns = self.view_columns.get(key, [])
        old_dependencies = self.view_dependencies.get(key, set())
        old_column_dependencies = self.view_column_dependencies.get(key, set())
        self.views[key] = list(tokens)
        self.view_columns[key] = list(columns or [])
        self.view_dependencies[key] = set(dependencies or ())
        self.view_column_dependencies[key] = set(column_dependencies or ())

        def undo() -> None:
            self.views[key] = old
            self.view_columns[key] = old_columns
            self.view_dependencies[key] = old_dependencies
            self.view_column_dependencies[key] = old_column_dependencies

        txn.undo_log.append(undo)
        self.persist(txn)

    @_locked
    def update_view_dependencies(
        self,
        txn: Transaction,
        updates: dict[str, set[str]],
        column_updates: Optional[dict[str, set[tuple[str, str]]]] = None,
    ) -> None:
        """刷新多个视图的依赖快照，并登记事务 undo。"""
        column_updates = column_updates or {}
        old = {
            key: set(self.view_dependencies.get(key, set()))
            for key in updates
        }
        old_columns = {
            key: set(self.view_column_dependencies.get(key, set()))
            for key in column_updates
        }
        for key, deps in updates.items():
            self.view_dependencies[key] = set(deps)
        for key, deps in column_updates.items():
            self.view_column_dependencies[key] = set(deps)

        def undo() -> None:
            for key, deps in old.items():
                self.view_dependencies[key] = deps
            for key, deps in old_columns.items():
                self.view_column_dependencies[key] = deps

        txn.undo_log.append(undo)
        self.persist(txn)

    @_locked
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
        column_dependencies = self.view_column_dependencies.pop(key, set())

        def undo() -> None:
            self.views[key] = tokens
            self.view_columns[key] = columns
            self.view_dependencies[key] = dependencies
            self.view_column_dependencies[key] = column_dependencies

        txn.undo_log.append(undo)
        self.persist(txn)

    @_locked
    def _view_dependents(self, key: str) -> list[str]:
        return sorted(
            name
            for name, deps in self.view_dependencies.items()
            if key in deps
        )

    @_locked
    def _view_column_dependents(self, table: str, column: str) -> list[str]:
        return sorted(
            name
            for name, deps in self.view_column_dependencies.items()
            if (table, column) in deps
        )

    @_locked
    def drop_table(
        self, txn: Transaction, name: str, cascade: bool = False
    ) -> tuple[int, int]:
        """删除表；返回 (级联删除的视图数, 级联删除的外键数)。"""
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
        return len(dependents), len(fk_dependents)

    @_locked
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

    @_locked
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
        view_dependents = self._view_column_dependents(table.name, old_key)
        if view_dependents:
            raise BinderError(
                f"列 {old_name!r} 被视图依赖：{', '.join(view_dependents)}",
                hint="先 DROP VIEW 依赖它的视图，再重命名列",
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
                on_delete_columns = tuple(
                    new_key if cname == old_key else cname
                    for cname in fk.on_delete_columns
                ) if holder is table else fk.on_delete_columns
                on_update_columns = tuple(
                    new_key if cname == old_key else cname
                    for cname in fk.on_update_columns
                ) if holder is table else fk.on_update_columns
                if (
                    columns,
                    ref_columns,
                    on_delete_columns,
                    on_update_columns,
                ) != (
                    fk.columns,
                    fk.ref_columns,
                    fk.on_delete_columns,
                    fk.on_update_columns,
                ):
                    fks[name] = replace(
                        fk,
                        columns=columns,
                        ref_columns=ref_columns,
                        on_delete_columns=on_delete_columns,
                        on_update_columns=on_update_columns,
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

    @_locked
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

    @_locked
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
