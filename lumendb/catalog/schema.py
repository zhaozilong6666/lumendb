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

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "table": self.table,
            "columns": list(self.columns),
            "unique": self.unique,
            "root_page": self.root_page,
        }

    @classmethod
    def from_json(cls, data: dict) -> "IndexDef":
        return cls(
            name=data["name"],
            table=data["table"],
            columns=tuple(data["columns"]),
            unique=data.get("unique", False),
            root_page=data.get("root_page", INVALID_PAGE_ID),
        )


@dataclass
class TableDef:
    name: str
    columns: list[Column]
    root_page: int = INVALID_PAGE_ID
    last_page: int = INVALID_PAGE_ID
    row_count_hint: int = 0
    indexes: dict[str, IndexDef] = field(default_factory=dict)

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
    if len(pk_cols) > 1:
        raise NotSupportedError(
            "暂不支持复合主键",
            hint="使用多列唯一索引替代",
        )
    if pk_cols and pk_cols[0].nullable:
        # 主键隐含 NOT NULL。这里直接报错，而不是悄悄改成非空——
        # 用户应该知道自己的 DDL 与实际语义有差别。
        raise NotSupportedError(
            f"主键列 {pk_cols[0].name} 不能声明为可空",
            hint="去掉 NULL，或删掉 PRIMARY KEY",
        )

    for idx in table.indexes.values():
        for cname in idx.columns:
            if not table.has_column(cname):
                raise ColumnNotFoundError(
                    f"索引 {idx.name} 引用了不存在的列 {cname!r}"
                )


__all__ = ["Column", "IndexDef", "TableDef", "validate_table_def"]
