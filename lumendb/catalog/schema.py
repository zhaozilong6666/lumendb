"""表的逻辑结构定义。

这一层只描述"表长什么样"，不负责持久化（那是 catalog 的事），也不负责
读写（那是 heap 的事）。保持纯数据结构的目的是：解析器、优化器、执行器
都可以自由地持有它，而不用担心副作用。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from ..datatypes import DataType, TypeSpec
from ..errors import (
    AmbiguousColumnError,
    ColumnNotFoundError,
    NotSupportedError,
    TableNotFoundError,
)
from ..storage.page import INVALID_PAGE_ID


@dataclass(frozen=True)
class Column:
    name: str
    type: TypeSpec
    nullable: bool = True
    primary_key: bool = False
    default: Any = None

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "type": self.type.name,
            "nullable": self.nullable,
            "primary_key": self.primary_key,
            "default": self.default,
        }

    @classmethod
    def from_json(cls, data: dict) -> "Column":
        return cls(
            name=data["name"],
            type=TypeSpec.parse(data["type"]),
            nullable=data.get("nullable", True),
            primary_key=data.get("primary_key", False),
            default=data.get("default"),
        )


@dataclass
class IndexDef:
    """一个索引。

    只支持单列与多列（复合）B+ 树索引；``columns`` 的顺序决定键的拼装顺序。
    """

    name: str
    table: str
    columns: tuple[str, ...]
    unique: bool = False
    root_page: int = INVALID_PAGE_ID
    #: "PRIMARY" / "UNIQUE" 表示由表约束创建的索引；普通 CREATE INDEX 为 None。
    constraint_kind: Optional[str] = None

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "table": self.table,
            "columns": list(self.columns),
            "unique": self.unique,
            "root_page": self.root_page,
            "constraint_kind": self.constraint_kind,
        }

    @classmethod
    def from_json(cls, data: dict) -> "IndexDef":
        return cls(
            name=data["name"],
            table=data["table"],
            columns=tuple(data["columns"]),
            unique=data.get("unique", False),
            root_page=data.get("root_page", INVALID_PAGE_ID),
            constraint_kind=data.get("constraint_kind"),
        )


@dataclass(frozen=True)
class CheckDef:
    """一条 CHECK 约束；表达式以 token 切片持久化，加载后重新解析绑定。"""

    name: str
    tokens: tuple[tuple[str, str], ...]

    def to_json(self) -> dict:
        return {"name": self.name, "tokens": [list(t) for t in self.tokens]}

    @classmethod
    def from_json(cls, data: dict) -> "CheckDef":
        return cls(
            name=data["name"],
            tokens=tuple((kind, value) for kind, value in data.get("tokens", [])),
        )


@dataclass(frozen=True)
class ForeignKeyDef:
    """引用另一张表（或自身）的外键约束；删除动作支持 RESTRICT / CASCADE。"""

    name: str
    columns: tuple[str, ...]
    ref_table: str
    ref_columns: tuple[str, ...]
    on_delete: str = "RESTRICT"
    on_update: str = "RESTRICT"
    match_type: str = "SIMPLE"
    deferrable: bool = False
    initially_deferred: bool = False

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "columns": list(self.columns),
            "ref_table": self.ref_table,
            "ref_columns": list(self.ref_columns),
            "on_delete": self.on_delete,
            "on_update": self.on_update,
            "match_type": self.match_type,
            "deferrable": self.deferrable,
            "initially_deferred": self.initially_deferred,
        }

    @classmethod
    def from_json(cls, data: dict) -> "ForeignKeyDef":
        return cls(
            name=data["name"],
            columns=tuple(data["columns"]),
            ref_table=data["ref_table"],
            ref_columns=tuple(data["ref_columns"]),
            on_delete=data.get("on_delete", "RESTRICT"),
            on_update=data.get("on_update", "RESTRICT"),
            match_type=data.get("match_type", "SIMPLE"),
            deferrable=bool(data.get("deferrable", False)),
            initially_deferred=bool(data.get("initially_deferred", False)),
        )


@dataclass
class TableDef:
    name: str
    columns: list[Column]
    root_page: int = INVALID_PAGE_ID
    last_page: int = INVALID_PAGE_ID
    row_count_hint: int = 0
    indexes: dict[str, IndexDef] = field(default_factory=dict)
    checks: dict[str, CheckDef] = field(default_factory=dict)
    foreign_keys: dict[str, ForeignKeyDef] = field(default_factory=dict)

    # -- 列查找 -------------------------------------------------------------

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column_index(self, name: str) -> int:
        for i, col in enumerate(self.columns):
            if col.name == name:
                return i
        raise ColumnNotFoundError(
            f"表 {self.name} 没有列 {name!r}",
            hint=f"可用列：{', '.join(self.column_names)}",
        )

    def column(self, name: str) -> Column:
        return self.columns[self.column_index(name)]

    def has_column(self, name: str) -> bool:
        return any(c.name == name for c in self.columns)

    def index_for_columns(self, columns: Iterable[str]) -> Optional[IndexDef]:
        target = tuple(columns)
        for idx in self.indexes.values():
            if idx.columns == target:
                return idx
        return None

    @property
    def primary_key_column(self) -> Optional[Column]:
        for col in self.columns:
            if col.primary_key:
                return col
        return None

    @property
    def primary_index(self) -> Optional[IndexDef]:
        pk = self.primary_key_column
        if pk is None:
            return None
        for idx in self.indexes.values():
            if idx.unique and idx.columns == (pk.name,):
                return idx
        return None

    @property
    def key_specs(self) -> list[TypeSpec]:
        """B+ 树键的类型列表（复合索引时为多列）。"""
        return [self.column(c).type for c in self.columns]

    # -- 序列化 -------------------------------------------------------------

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "columns": [c.to_json() for c in self.columns],
            "root_page": self.root_page,
            "last_page": self.last_page,
            "row_count_hint": self.row_count_hint,
            "indexes": {k: v.to_json() for k, v in self.indexes.items()},
            "checks": {k: v.to_json() for k, v in self.checks.items()},
            "foreign_keys": {
                k: v.to_json() for k, v in self.foreign_keys.items()
            },
        }

    @classmethod
    def from_json(cls, data: dict) -> "TableDef":
        return cls(
            name=data["name"],
            columns=[Column.from_json(c) for c in data["columns"]],
            root_page=data.get("root_page", INVALID_PAGE_ID),
            last_page=data.get("last_page", INVALID_PAGE_ID),
            row_count_hint=data.get("row_count_hint", 0),
            indexes={
                k: IndexDef.from_json(v) for k, v in data.get("indexes", {}).items()
            },
            checks={
                k: CheckDef.from_json(v) for k, v in data.get("checks", {}).items()
            },
            foreign_keys={
                k: ForeignKeyDef.from_json(v)
                for k, v in data.get("foreign_keys", {}).items()
            },
        )


def validate_table_def(table: TableDef) -> None:
    """建表时的结构校验。宁可建表失败，也不要留下一个语义模糊的表。"""
    if not table.columns:
        raise NotSupportedError("表必须至少有一列")

    seen = set()
    for col in table.columns:
        if col.name in seen:
            raise NotSupportedError(f"列名重复：{col.name}")
        seen.add(col.name)

    pk_cols = [c for c in table.columns if c.primary_key]
    for col in pk_cols:
        if col.nullable:
            # 主键隐含 NOT NULL。这里直接报错，而不是悄悄改成非空——
            # 用户应该知道自己的 DDL 与实际语义有差别。
            raise NotSupportedError(
                f"主键列 {col.name} 不能声明为可空",
                hint="去掉 NULL，或删掉 PRIMARY KEY",
            )

    for idx in table.indexes.values():
        for cname in idx.columns:
            if not table.has_column(cname):
                raise ColumnNotFoundError(
                    f"索引 {idx.name} 引用了不存在的列 {cname!r}"
                )
        if idx.constraint_kind not in (None, "PRIMARY", "UNIQUE"):
            raise NotSupportedError(
                f"索引 {idx.name} 的约束类型非法：{idx.constraint_kind!r}"
            )
        if idx.constraint_kind is not None and not idx.unique:
            raise NotSupportedError(
                f"约束索引 {idx.name} 必须是唯一索引"
            )

    for name in table.checks:
        if name in table.indexes:
            raise NotSupportedError(
                f"CHECK 约束名 {name!r} 与索引/约束名冲突",
                hint="给 CHECK 换一个名字",
            )

    for name, fk in table.foreign_keys.items():
        if name != fk.name:
            raise NotSupportedError(f"外键 {name!r} 的目录键与名字不一致")
        if name in table.checks or name in table.indexes:
            raise NotSupportedError(
                f"外键名 {name!r} 与 CHECK/索引名冲突",
                hint="给外键换一个名字",
            )
        if len(fk.columns) != len(fk.ref_columns):
            raise NotSupportedError(
                f"外键 {name!r} 的列数与被引用列数不一致"
            )
        if not fk.ref_columns:
            raise NotSupportedError(f"外键 {name!r} 没有被引用列")
        if fk.on_delete not in (
            "RESTRICT",
            "NO ACTION",
            "CASCADE",
            "SET NULL",
            "SET DEFAULT",
        ):
            raise NotSupportedError(
                f"外键 {name!r} 的删除动作非法：{fk.on_delete!r}"
            )
        if fk.on_update not in (
            "RESTRICT",
            "NO ACTION",
            "CASCADE",
            "SET NULL",
            "SET DEFAULT",
        ):
            raise NotSupportedError(
                f"外键 {name!r} 的更新动作非法：{fk.on_update!r}"
            )
        if fk.match_type not in ("SIMPLE", "FULL"):
            raise NotSupportedError(
                f"外键 {name!r} 的 MATCH 类型非法：{fk.match_type!r}"
            )
        if fk.initially_deferred and not fk.deferrable:
            raise NotSupportedError(
                f"外键 {name!r} 的 INITIALLY DEFERRED 需要 DEFERRABLE"
            )
        for cname in fk.columns:
            if not table.has_column(cname):
                raise ColumnNotFoundError(
                    f"外键 {name!r} 引用了本表不存在的列 {cname!r}"
                )
        if not fk.ref_table:
            raise NotSupportedError(f"外键 {name!r} 缺少被引用表名")


__all__ = [
    "CheckDef",
    "Column",
    "ForeignKeyDef",
    "IndexDef",
    "TableDef",
    "validate_table_def",
]
