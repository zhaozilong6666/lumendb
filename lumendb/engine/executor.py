"""执行引擎：把绑定后的计划跑成结果，以及 DDL / DML 的实际落地。

火山模型（Volcano / iterator model）：每个计划节点是一个生成器，一次产出
一行，上游 `next()` 驱动下游。优点是实现简单、内存占用与数据量无关；缺点
是每行有一次函数调用开销。对这个规模的项目，简单可读比几微秒的常数更重要。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Optional

from ..catalog.catalog import Catalog
from ..catalog.schema import IndexDef, TableDef
from ..datatypes import DataType, TypeSpec
from ..errors import (
    BinderError,
    DuplicateKeyError,
    IndexAlreadyExistsError,
    NotNullViolationError,
    NotSupportedError,
    TableAlreadyExistsError,
    TableNotFoundError,
    TypeMismatchError,
)
from ..sql.ast import (
    CreateIndex,
    CreateTable,
    Delete,
    DropIndex,
    DropTable,
    Explain,
    Insert,
    Query,
    ShowTables,
    Statement,
    Update,
    render_expr,
)
from ..storage.btree import BPlusTree, KeyCodec
from ..storage.bufferpool import BufferPoolManager
from ..storage.heap import HeapFile
from ..storage.page import INVALID_PAGE_ID
from ..storage.recovery import RecoveryManager
from ..storage.txn import Transaction, TransactionManager
from ..storage.wal import WAL
from ..plan.binder import AggregateAccumulatorGroup, Binder, Schema, SchemaColumn
from ..plan.nodes import (
    Aggregate,
    Distinct,
    EvalContext,
    Filter,
    IndexLookup,
    Join,
    Limit,
    PlanNode,
    Project,
    Scan,
    Sort,
    Values,
)


@dataclass
class ColumnMeta:
    name: str
    type: TypeSpec


@dataclass
class Result:
    columns: list[ColumnMeta] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    affected: int = 0
    tag: str = ""
    notice: Optional[str] = None


class Executor:
    def __init__(
        self,
        buffer_pool: BufferPoolManager,
        catalog: Catalog,
        txns: TransactionManager,
        wal: WAL,
        disk=None,
    ) -> None:
        self._bp = buffer_pool
        self._catalog = catalog
        self._txns = txns
        self._wal = wal
        self._disk = disk if disk is not None else getattr(buffer_pool, "disk", None)
        self._txn: Optional[Transaction] = None
        self.binder = self._new_binder()

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def run(self, stmt: Statement, txn: Transaction) -> Result:
        self._txn = txn
        try:
            if isinstance(stmt, Query):
                return self._select(stmt, txn)
            if isinstance(stmt, Insert):
                return self._insert(stmt, txn)
            if isinstance(stmt, Update):
                return self._update(stmt, txn)
            if isinstance(stmt, Delete):
                return self._delete(stmt, txn)
            if isinstance(stmt, CreateTable):
                return self._create_table(stmt, txn)
            if isinstance(stmt, DropTable):
                return self._drop_table(stmt, txn)
            if isinstance(stmt, CreateIndex):
                return self._create_index(stmt, txn)
            if isinstance(stmt, DropIndex):
                return self._drop_index(stmt, txn)
            if isinstance(stmt, ShowTables):
                return self._show_tables()
            if isinstance(stmt, Explain):
                return self._explain(stmt)
            if isinstance(stmt, Statement) and type(stmt).__name__ == "Checkpoint":
                RecoveryManager(
                    self._disk, self._wal, self._bp, self._txns
                ).checkpoint()
                return Result(tag="CHECKPOINT")
            raise NotSupportedError(
                f"语句 {type(stmt).__name__} 尚未实现",
                hint="BEGIN / COMMIT / ROLLBACK 由会话层处理",
            )
        finally:
            self._txn = None

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def _select(self, stmt: Query, txn: Transaction) -> Result:
        plan, out_cols = self.binder.bind_select(stmt.select)
        rows = self._execute(plan, txn)
        width = len(out_cols)
        materialized = [tuple(r)[:width] for r in rows]
        return Result(
            columns=[ColumnMeta(c.name, c.type) for c in out_cols],
            rows=materialized,
            tag=f"SELECT {len(materialized)}",
        )

    def _new_binder(self) -> Binder:
        binder = Binder(self._catalog)
        binder.subquery_runner = self._run_subquery
        return binder

    def _run_subquery(self, select, outer_ctx: Optional[EvalContext]):
        # 子查询在"没有外层列可用"的 schema 下绑定：如果它引用了外层列，
        # binder 会报 ColumnNotFoundError，这就是"相关子查询不支持"的明确表现。
        binder = self._new_binder()
        plan, _ = binder.bind_select(select)
        return list(self._execute(plan, self._require_txn()))

    def _require_txn(self) -> Transaction:
        if self._txn is None:
            raise BinderError("没有活动事务")
        return self._txn

    # -- 计划执行 -------------------------------------------------------

    def _execute(self, node: PlanNode, txn: Transaction) -> Iterator[tuple]:
        if isinstance(node, Values):
            yield from node.rows
            return
        if isinstance(node, Scan):
            yield from self._scan(node, txn)
            return
        if isinstance(node, IndexLookup):
            yield from self._index_lookup(node, txn)
            return
        if isinstance(node, Filter):
            assert node.child is not None and node.predicate is not None
            for row in self._execute(node.child, txn):
                ctx = EvalContext(row)
                if _truthy(node.predicate(ctx)):
                    yield row
            return
        if isinstance(node, Join):
            yield from self._join(node, txn)
            return
        if isinstance(node, Aggregate):
            yield from self._aggregate(node, txn)
            return
        if isinstance(node, Project):
            assert node.child is not None
            for row in self._execute(node.child, txn):
                ctx = EvalContext(row)
                yield tuple(fn(ctx) for fn in node.exprs)
            return
        if isinstance(node, Distinct):
            assert node.child is not None
            width = node.limit_columns or len(node.columns)
            seen: set = set()
            for row in self._execute(node.child, txn):
                key = tuple(row[:width])
                if key in seen:
                    continue
                seen.add(key)
                yield row
            return
        if isinstance(node, Sort):
            yield from self._sort(node, txn)
            return
        if isinstance(node, Limit):
            assert node.child is not None
            skipped = 0
            emitted = 0
            for row in self._execute(node.child, txn):
                if skipped < node.offset:
                    skipped += 1
                    continue
                if node.limit is not None and emitted >= node.limit:
                    return
                emitted += 1
                yield row
            return
        raise NotSupportedError(f"计划节点 {type(node).__name__} 无法执行")

    def _index_lookup(self, node: IndexLookup, txn: Transaction) -> Iterator[tuple]:
        table = self._catalog.get_table(node.table)
        index = table.indexes.get(node.index_name)
        if index is None:
            # 索引在计划生成之后被删掉了：退化成全表扫描，结果仍然正确
            yield from self._scan(Scan(columns=node.columns, table=node.table, alias=node.alias), txn)
            return
        tree = self._tree_for(table, index)
        heap = HeapFile(self._bp, table, self._txns)
        for rid in tree.lookup(txn, list(node.key)):
            values = heap.get(txn, rid)
            if values is not None:
                yield tuple(values)

    def _scan(self, node: Scan, txn: Transaction) -> Iterator[tuple]:
        table = self._catalog.get_table(node.table)
        heap = HeapFile(self._bp, table, self._txns)
        for _rid, values in heap.scan(txn):
            yield tuple(values)

    def _join(self, node: Join, txn: Transaction) -> Iterator[tuple]:
        assert node.left is not None and node.right is not None
        right_rows = list(self._execute(node.right, txn))
        right_width = len(node.right.columns)
        for left in self._execute(node.left, txn):
            matched = False
            for right in right_rows:
                combined = tuple(left) + tuple(right)
                if node.on is None or _truthy(node.on(EvalContext(combined))):
                    matched = True
                    yield combined
            if node.kind == "LEFT" and not matched:
                yield tuple(left) + (None,) * right_width

    def _aggregate(self, node: Aggregate, txn: Transaction) -> Iterator[tuple]:
        assert node.child is not None
        group_count = len(node.group_exprs)

        if group_count == 0:
            group = AggregateAccumulatorGroup(node.aggs)
            for row in self._execute(node.child, txn):
                group.update(_ctx_for_aggregate(row))
            yield group.results()
            return

        groups: dict[tuple, AggregateAccumulatorGroup] = {}
        for row in self._execute(node.child, txn):
            ctx = EvalContext(row)
            key = tuple(fn(ctx) for fn in node.group_exprs)
            acc = groups.get(key)
            if acc is None:
                acc = AggregateAccumulatorGroup(node.aggs)
                groups[key] = acc
            acc.update(ctx)
        for key, acc in groups.items():
            yield tuple(key) + acc.results()

    def _sort(self, node: Sort, txn: Transaction) -> Iterator[tuple]:
        assert node.child is not None
        rows = list(self._execute(node.child, txn))

        # 稳定排序：从最后一列往前排，天然支持多键
        for fn, descending in reversed(node.keys):
            rows.sort(
                key=lambda r, f=fn: _sort_key(f(EvalContext(r))),
                reverse=descending,
            )
        yield from rows

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def _create_table(self, stmt: CreateTable, txn: Transaction) -> Result:
        if self._catalog.has_table(stmt.name):
            if stmt.if_not_exists:
                return Result(tag="CREATE TABLE", notice="表已存在，已跳过")
            raise TableAlreadyExistsError(
                f"表 {stmt.name!r} 已存在",
                hint="使用 DROP TABLE 后再建",
            )

        columns = [_to_column(c) for c in stmt.columns]
        primary = [c for c in columns if c.primary_key]
        if len(primary) > 1:
            raise NotSupportedError(
                "只支持单列主键",
                hint="复合主键请改用 UNIQUE INDEX",
            )

        indexes: list[IndexDef] = []
        if primary:
            indexes.append(
                IndexDef(
                    name=f"{stmt.name.lower()}_pkey",
                    table=stmt.name.lower(),
                    columns=(primary[0].name,),
                    unique=True,
                )
            )
        self._catalog.create_table(txn, stmt.name, columns, indexes)
        return Result(tag="CREATE TABLE")

    def _drop_table(self, stmt: DropTable, txn: Transaction) -> Result:
        for name in stmt.names:
            if not self._catalog.has_table(name):
                if stmt.if_exists:
                    continue
                raise TableNotFoundError(f"表 {name!r} 不存在")
            self._catalog.drop_table(txn, name)
        return Result(tag="DROP TABLE")

    def _create_index(self, stmt: CreateIndex, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        name = stmt.name.lower()
        if name in table.indexes:
            raise IndexAlreadyExistsError(f"索引 {stmt.name!r} 已存在")
        for cname in stmt.columns:
            table.column(cname)

        idx = IndexDef(
            name=name,
            table=table.name,
            columns=tuple(c.lower() for c in stmt.columns),
            unique=stmt.unique,
        )
        self._catalog.create_index(txn, table.name, idx)

        # 已有数据必须回填，否则索引是空的——查询会漏行，这比报错更糟
        heap = HeapFile(self._bp, table, self._txns)
        tree = self._tree_for(table, idx)
        for rid, values in heap.scan(txn):
            key = self._key_for(table, idx, values)
            if key is None:
                continue
            if idx.unique and tree.lookup_one(txn, key) is not None:
                raise DuplicateKeyError(
                    f"无法创建唯一索引 {stmt.name!r}",
                    detail=f"列 {', '.join(idx.columns)} 上存在重复值",
                )
            tree.insert(txn, key, rid)
        idx.root_page = tree.root_page
        self._catalog.persist(txn)
        return Result(tag="CREATE INDEX")

    def _drop_index(self, stmt: DropIndex, txn: Transaction) -> Result:
        try:
            self._catalog.drop_index(txn, stmt.name.lower())
        except Exception:
            if stmt.if_exists:
                return Result(tag="DROP INDEX", notice="索引不存在，已跳过")
            raise
        return Result(tag="DROP INDEX")

    def _show_tables(self) -> Result:
        rows = [
            (t.name, len(t.columns), t.row_count_hint)
            for t in self._catalog.list_tables()
        ]
        return Result(
            columns=[
                ColumnMeta("table", TypeSpec(DataType.TEXT)),
                ColumnMeta("columns", TypeSpec(DataType.INT)),
                ColumnMeta("rows", TypeSpec(DataType.BIGINT)),
            ],
            rows=rows,
            tag=f"SHOW {len(rows)}",
        )

    def _explain(self, stmt: Explain) -> Result:
        inner = stmt.statement
        if isinstance(inner, Query):
            plan, cols = self.binder.bind_select(inner.select)
            lines = list(_describe_plan(plan))
            text = "\n".join(lines)
            return Result(
                columns=[ColumnMeta("plan", TypeSpec(DataType.TEXT))],
                rows=[(line,) for line in lines],
                tag="EXPLAIN",
                notice=text,
            )
        return Result(
            columns=[ColumnMeta("plan", TypeSpec(DataType.TEXT))],
            rows=[(f"{type(inner).__name__}（该语句没有可展示的查询计划）",)],
            tag="EXPLAIN",
        )

    # ------------------------------------------------------------------
    # DML
    # ------------------------------------------------------------------

    def _insert(self, stmt: Insert, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        heap = HeapFile(self._bp, table, self._txns)
        empty_schema = Schema([])
        count = 0

        for row_exprs in stmt.rows:
            values: list[Any] = []
            for expr in row_exprs:
                fn, _ = self.binder.compile(expr, empty_schema, allow_agg=False)
                values.append(fn(EvalContext(())))

            prepared = self._prepare_values(table, stmt.columns, values)
            self._check_constraints(table, prepared, txn)

            rid = heap.insert(txn, prepared)
            for idx in table.indexes.values():
                tree = self._tree_for(table, idx)
                key = self._key_for(table, idx, prepared)
                if key is None:
                    continue
                tree.insert(txn, key, rid)
                idx.root_page = tree.root_page
            count += 1

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return Result(affected=count, tag=f"INSERT {count}")

    def _update(self, stmt: Update, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        heap = HeapFile(self._bp, table, self._txns)

        assignment_fns: list[tuple[int, Any]] = []
        schema = Schema(
            [SchemaColumn(table.name, c.name, c.type) for c in table.columns]
        )
        for name, expr in stmt.assignments:
            index = table.column_index(name)
            fn, _ = self.binder.compile(expr, schema, allow_agg=False)
            assignment_fns.append((index, fn))

        where_fn = None
        if stmt.where is not None:
            where_fn, _ = self.binder.compile(stmt.where, schema, allow_agg=False)

        updated = 0
        # 必须先物化目标行：更新会向堆里追加新版本，边扫边写会让扫描器
        # 不断看到刚写进去的新版本（root RID 不变），从而反复更新同一行。
        targets = list(heap.scan(txn))
        for rid, old_values in targets:
            old_tuple = tuple(old_values)
            ctx = EvalContext(old_tuple)
            if where_fn is not None and not _truthy(where_fn(ctx)):
                continue
            new_values = list(old_tuple)
            for index, fn in assignment_fns:
                new_values[index] = fn(ctx)
            prepared = self._prepare_values(table, None, new_values)

            changed_indexes = [
                idx
                for idx in table.indexes.values()
                if self._key_for(table, idx, old_tuple) != self._key_for(table, idx, prepared)
            ]
            for idx in changed_indexes:
                old_key = self._key_for(table, idx, old_tuple)
                if old_key is not None:
                    self._tree_for(table, idx).delete(txn, old_key, rid)

            heap.update(txn, rid, prepared)

            for idx in changed_indexes:
                tree = self._tree_for(table, idx)
                new_key = self._key_for(table, idx, prepared)
                if new_key is not None:
                    tree.insert(txn, new_key, rid)
                idx.root_page = tree.root_page
            updated += 1

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return Result(affected=updated, tag=f"UPDATE {updated}")

    def _delete(self, stmt: Delete, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        heap = HeapFile(self._bp, table, self._txns)

        where_fn = None
        if stmt.where is not None:
            schema = Schema(
                [SchemaColumn(table.name, c.name, c.type) for c in table.columns]
            )
            where_fn, _ = self.binder.compile(stmt.where, schema, allow_agg=False)

        deleted = 0
        for rid, values in heap.scan(txn):
            ctx = EvalContext(tuple(values))
            if where_fn is not None and not _truthy(where_fn(ctx)):
                continue
            for idx in table.indexes.values():
                key = self._key_for(table, idx, values)
                if key is not None:
                    self._tree_for(table, idx).delete(txn, key, rid)
            heap.delete(txn, rid)
            deleted += 1

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return Result(affected=deleted, tag=f"DELETE {deleted}")

    # ------------------------------------------------------------------
    # 约束与索引辅助
    # ------------------------------------------------------------------

    def _prepare_values(
        self, table: TableDef, columns: Optional[list[str]], values: list[Any]
    ) -> list[Any]:
        if columns is None:
            if len(values) != len(table.columns):
                raise BinderError(
                    f"列数不匹配：表 {table.name} 有 {len(table.columns)} 列，"
                    f"提供了 {len(values)} 个值"
                )
            supplied = list(values)
        else:
            if len(values) != len(columns):
                raise BinderError("VALUES 里的值个数与列清单不一致")
            supplied = [None] * len(table.columns)
            filled = [False] * len(table.columns)
            for name, value in zip(columns, values):
                index = table.column_index(name)
                supplied[index] = value
                filled[index] = True
            for i, col in enumerate(table.columns):
                if not filled[i]:
                    if col.default is not None:
                        supplied[i] = col.default
                    elif not col.nullable:
                        raise NotNullViolationError(
                            f"列 {col.name} 不允许 NULL 且没有默认值"
                        )

        prepared: list[Any] = []
        for col, value in zip(table.columns, supplied):
            if value is None:
                if not col.nullable:
                    raise NotNullViolationError(
                        f"列 {col.name} 是 NOT NULL，不能插入 NULL",
                        hint="给一个值，或在建表时声明 DEFAULT",
                    )
                prepared.append(None)
                continue
            coerced = col.type.coerce(value)
            prepared.append(coerced)
        return prepared

    def _check_constraints(
        self, table: TableDef, values: list[Any], txn: Transaction
    ) -> None:
        for idx in table.indexes.values():
            if not idx.unique:
                continue
            key = self._key_for(table, idx, values)
            if key is None:
                continue
            if self._tree_for(table, idx).lookup_one(txn, key) is not None:
                raise DuplicateKeyError(
                    f"唯一约束冲突：索引 {idx.name}",
                    detail=f"列 {', '.join(idx.columns)} 上已存在相同的值",
                )

    def _tree_for(self, table: TableDef, idx: IndexDef) -> BPlusTree:
        # KeyCodec 会自动在末尾追加 RID 两列，这里只给索引列的类型
        specs = [table.column(c).type for c in idx.columns]
        return BPlusTree(
            self._bp,
            KeyCodec(specs),
            idx.root_page,
            unique=idx.unique,
            name=idx.name,
        )

    def _key_for(
        self, table: TableDef, idx: IndexDef, values: Iterable[Any]
    ) -> Optional[list]:
        """索引的"列值部分"。RID 由 B+ 树自己在插入/删除时拼在后面。"""
        values = list(values)
        key: list = []
        for cname in idx.columns:
            value = values[table.column_index(cname)]
            if value is None:
                return None  # NULL 不进索引
            key.append(value)
        return key


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


def _ctx_for_aggregate(row: tuple) -> EvalContext:
    return EvalContext(row)


def _truthy(value: Any) -> bool:
    return bool(value) if value is not None else False


def _sort_key(value: Any):
    # NULL 排在最后：先按"是否为空"，再按值本身
    return (1, None) if value is None else (0, value)


def _describe_plan(node: PlanNode, depth: int = 0) -> Iterator[str]:
    prefix = "  " * depth
    yield prefix + node.describe()
    for child in node.children():
        yield from _describe_plan(child, depth + 1)


def _to_column(col_def):
    from ..catalog.schema import Column

    return Column(
        name=col_def.name.lower(),
        type=col_def.type,
        nullable=col_def.nullable,
        primary_key=col_def.primary_key,
        default=None,
    )


__all__ = ["Executor", "Result", "ColumnMeta"]
