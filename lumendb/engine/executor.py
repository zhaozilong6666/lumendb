"""执行引擎：把绑定后的计划跑成结果，以及 DDL / DML 的实际落地。

火山模型（Volcano / iterator model）：每个计划节点是一个生成器，一次产出
一行，上游 `next()` 驱动下游。优点是实现简单、内存占用与数据量无关；缺点
是每行有一次函数调用开销。对这个规模的项目，简单可读比几微秒的常数更重要。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import re
import time
from collections import Counter
from typing import Any, Iterable, Iterator, Optional

from ..catalog.catalog import Catalog
from ..catalog.schema import (
    CheckDef,
    Column,
    ForeignKeyDef,
    IndexDef,
    TableDef,
)
from ..datatypes import DataType, TypeSpec
from ..errors import (
    BinderError,
    CheckViolationError,
    DuplicateKeyError,
    ForeignKeyViolationError,
    IndexAlreadyExistsError,
    NotNullViolationError,
    NotSupportedError,
    TableAlreadyExistsError,
    TableNotFoundError,
    TypeMismatchError,
)
from ..sql.ast import (
    AlterTableAddColumn,
    AlterTableAddConstraint,
    AlterTableAlterDefault,
    AlterTableAlterNullability,
    AlterTableAlterType,
    AlterTableDropColumn,
    AlterTableDropConstraint,
    AlterTableRenameColumn,
    AlterTableRenameConstraint,
    AlterTableRenameTable,
    CreateIndex,
    CreateTable,
    CreateTableAs,
    CreateView,
    DefaultValue,
    Delete,
    DropIndex,
    DropTable,
    DropView,
    Expr,
    Explain,
    ForeignKeySpec,
    Insert,
    Query,
    Reindex,
    SetConstraints,
    Vacuum,
    Truncate,
    ShowTables,
    ShowViews,
    Statement,
    TableConstraint,
    Update,
    render_expr,
)
from ..storage.btree import BPlusTree, KeyCodec
from ..storage.bufferpool import BufferPoolManager
from ..storage.heap import HeapFile
from ..storage.page import INVALID_PAGE_ID
from ..storage.record import RID
from ..storage.recovery import RecoveryManager
from ..storage.txn import Transaction, TransactionManager
from ..storage.wal import WAL

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
from ..plan.binder import AggregateAccumulatorGroup, Binder, Schema, SchemaColumn
from ..plan.nodes import (
    Aggregate,
    CteScan,
    Distinct,
    EvalContext,
    Filter,
    IndexLookup,
    IndexRange,
    Join,
    Limit,
    PlanNode,
    Project,
    RecursiveCte,
    Scan,
    SetOp,
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
        #: EXPLAIN ANALYZE 期间按节点累计的执行统计
        self._profile: Optional[dict[int, list]] = None
        #: 当前语句的 ``$n`` 绑定值；run() 每次覆盖，子查询 binder 共享
        self._params: list[Any] = []
        #: 当前语句的 CTE 作用域；子查询执行时复用
        self._active_ctes: dict[str, Any] = {}
        #: 递归 CTE 当前轮的工作表，只在执行递归节点期间有值
        self._recursive_rows: dict[str, list[tuple]] = {}
        #: CHECK 约束 token -> 编译后谓词；DDL 改约束时键会变化，无需主动失效
        self._check_cache: dict[tuple, Any] = {}
        self.binder = self._new_binder()

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def run(
        self,
        stmt: Statement,
        txn: Transaction,
        params: Optional[list[Any]] = None,
    ) -> Result:
        self._txn = txn
        self._params = list(params) if params else []
        self._recursive_rows = {}
        # Binder 保存 CTE 作用域和参数；每条语句必须从干净的作用域开始，
        # 否则上一条 WITH 的 CTE 会泄漏到下一句（曾经确实泄漏过）。
        self.binder = self._new_binder()
        self.binder.params = self._params
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
            if isinstance(stmt, CreateTableAs):
                return self._create_table_as(stmt, txn)
            if isinstance(stmt, CreateView):
                return self._create_view(stmt, txn)
            if isinstance(stmt, DropView):
                return self._drop_view(stmt, txn)
            if isinstance(stmt, AlterTableAddColumn):
                return self._alter_add_column(stmt, txn)
            if isinstance(stmt, AlterTableAddConstraint):
                return self._alter_add_constraint(stmt, txn)
            if isinstance(stmt, AlterTableDropConstraint):
                return self._alter_drop_constraint(stmt, txn)
            if isinstance(stmt, AlterTableAlterDefault):
                return self._alter_default(stmt, txn)
            if isinstance(stmt, AlterTableAlterNullability):
                return self._alter_nullability(stmt, txn)
            if isinstance(stmt, AlterTableAlterType):
                return self._alter_type(stmt, txn)
            if isinstance(stmt, AlterTableDropColumn):
                return self._alter_drop_column(stmt, txn)
            if isinstance(stmt, AlterTableRenameTable):
                self._catalog.rename_table(txn, stmt.table, stmt.new_name)
                return Result(
                    tag="ALTER TABLE",
                    notice=f"表已重命名为 {stmt.new_name.lower()}",
                )
            if isinstance(stmt, AlterTableRenameColumn):
                self._catalog.rename_column(
                    txn, stmt.table, stmt.column, stmt.new_name
                )
                return Result(
                    tag="ALTER TABLE",
                    notice=(
                        f"列 {stmt.column.lower()} 已重命名为 "
                        f"{stmt.new_name.lower()}"
                    ),
                )
            if isinstance(stmt, AlterTableRenameConstraint):
                return self._alter_rename_constraint(stmt, txn)
            if isinstance(stmt, DropTable):
                return self._drop_table(stmt, txn)
            if isinstance(stmt, CreateIndex):
                return self._create_index(stmt, txn)
            if isinstance(stmt, DropIndex):
                return self._drop_index(stmt, txn)
            if isinstance(stmt, Reindex):
                return self._reindex(stmt, txn)
            if isinstance(stmt, Vacuum):
                return self._vacuum(stmt, txn)
            if isinstance(stmt, Truncate):
                return self._truncate(stmt, txn)
            if isinstance(stmt, SetConstraints):
                return self._set_constraints(stmt, txn)
            if isinstance(stmt, ShowTables):
                return self._show_tables()
            if isinstance(stmt, ShowViews):
                return self._show_views()
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

    def describe(self, stmt: Statement, params: Optional[list[Any]] = None) -> list[ColumnMeta]:
        """只绑定、不执行，返回语句的结果列。

        扩展协议的 Describe(Portal) 需要 RowDescription，但绝不能为了拿列
        信息去执行一条 INSERT/UPDATE/DELETE——那会让同一条 DML 在
        Describe + Execute 中被执行两次。这里复用 Binder，只做静态绑定。
        """
        self._txn = None
        self._params = list(params) if params else []
        self.binder = self._new_binder()
        try:
            if isinstance(stmt, Query):
                _, cols = self.binder.bind_query(stmt)
                return [ColumnMeta(c.name, c.type) for c in cols]
            if isinstance(stmt, Insert):
                table = self._catalog.get_table(stmt.table)
                if stmt.query is not None:
                    # bind_query 自己会绑定 CTE；这里不能再先 bind_ctes，
                    # 否则同一个 CTE 会被注册两次并报重名。
                    if stmt.ctes:
                        self.binder.bind_query(
                            Query(select=stmt.query, ctes=stmt.ctes)
                        )
                    else:
                        self.binder.bind_select(stmt.query)
                elif stmt.ctes:
                    self.binder.bind_ctes(stmt.ctes)
                schema = Schema(
                    [SchemaColumn(table.name, c.name, c.type) for c in table.columns]
                )
                _, cols = self.binder.bind_returning(stmt.returning, schema)
                return [ColumnMeta(c.name, c.type) for c in cols]
            if isinstance(stmt, (Update, Delete)):
                table = self._catalog.get_table(stmt.table)
                if stmt.ctes:
                    self.binder.bind_ctes(stmt.ctes)
                schema = Schema(
                    [SchemaColumn(table.name, c.name, c.type) for c in table.columns]
                )
                _, cols = self.binder.bind_returning(stmt.returning, schema)
                return [ColumnMeta(c.name, c.type) for c in cols]
            if isinstance(stmt, Explain):
                return [ColumnMeta("plan", TypeSpec(DataType.TEXT))]
            if isinstance(stmt, ShowViews):
                return [
                    ColumnMeta("view", TypeSpec(DataType.TEXT)),
                    ColumnMeta("definition", TypeSpec(DataType.TEXT)),
                ]
            if isinstance(stmt, ShowTables):
                return [
                    ColumnMeta("table", TypeSpec(DataType.TEXT)),
                    ColumnMeta("columns", TypeSpec(DataType.INT)),
                    ColumnMeta("rows", TypeSpec(DataType.BIGINT)),
                ]
            return []
        finally:
            self._txn = None

    def _select(self, stmt: Query, txn: Transaction) -> Result:
        plan, out_cols = self.binder.bind_query(stmt)
        width = len(out_cols)
        self._active_ctes = dict(self.binder.ctes)
        try:
            materialized = [tuple(r)[:width] for r in self._execute(plan, txn)]
        finally:
            self._active_ctes = {}
        return Result(
            columns=[ColumnMeta(c.name, c.type) for c in out_cols],
            rows=materialized,
            tag=f"SELECT {len(materialized)}",
        )

    def _new_binder(self) -> Binder:
        binder = Binder(self._catalog, self._params)
        binder.subquery_runner = self._run_subquery
        return binder

    def _run_subquery(self, select, outer_ctx: Optional[EvalContext]):
        # 子查询在"没有外层列可用"的 schema 下绑定：如果它引用了外层列，
        # binder 会报 ColumnNotFoundError，这就是"相关子查询不支持"的明确表现。
        binder = self._new_binder()
        binder.ctes = dict(self._active_ctes or self.binder.ctes)
        plan, _ = binder.bind_select(select)
        return list(self._execute(plan, self._require_txn()))

    def _require_txn(self) -> Transaction:
        if self._txn is None:
            raise BinderError("没有活动事务")
        return self._txn

    # -- 计划执行 -------------------------------------------------------

    def _execute(self, node: PlanNode, txn: Transaction) -> Iterator[tuple]:
        if self._profile is None:
            yield from self._execute_inner(node, txn)
            return
        start = time.perf_counter()
        rows = 0
        try:
            for row in self._execute_inner(node, txn):
                rows += 1
                yield row
        finally:
            entry = self._profile.get(id(node))
            if entry is None:
                self._profile[id(node)] = [
                    time.perf_counter() - start,
                    1,
                    rows,
                ]
            else:
                entry[0] += time.perf_counter() - start
                entry[1] += 1
                entry[2] += rows

    def _execute_inner(self, node: PlanNode, txn: Transaction) -> Iterator[tuple]:
        if isinstance(node, Values):
            yield from node.rows
            return
        if isinstance(node, CteScan):
            if node.recursive:
                yield from self._recursive_rows.get(node.name, [])
            else:
                assert node.child is not None
                yield from self._execute(node.child, txn)
            return
        if isinstance(node, RecursiveCte):
            yield from self._recursive_cte(node, txn)
            return
        if isinstance(node, Scan):
            yield from self._scan(node, txn)
            return
        if isinstance(node, IndexLookup):
            yield from self._index_lookup(node, txn)
            return
        if isinstance(node, IndexRange):
            yield from self._index_range(node, txn)
            return
        if isinstance(node, SetOp):
            yield from self._set_op(node, txn)
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
            yield from self._limit(node, txn)
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

    def _index_range(self, node: IndexRange, txn: Transaction) -> Iterator[tuple]:
        table = self._catalog.get_table(node.table)
        index = table.indexes.get(node.index_name)
        if index is None:
            yield from self._scan(
                Scan(columns=node.columns, table=node.table, alias=node.alias), txn
            )
            return
        tree = self._tree_for(table, index)
        heap = HeapFile(self._bp, table, self._txns)
        for _key, rid in tree.scan_range(
            len(node.index_columns),
            low=node.low,
            high=node.high,
            low_inclusive=node.low_inclusive,
            high_inclusive=node.high_inclusive,
        ):
            values = heap.get(txn, rid)
            if values is not None:
                yield tuple(values)

    def _scan(self, node: Scan, txn: Transaction) -> Iterator[tuple]:
        table = self._catalog.get_table(node.table)
        heap = HeapFile(self._bp, table, self._txns)
        for _rid, values in heap.scan(txn):
            yield tuple(values)

    def _recursive_cte(
        self, node: RecursiveCte, txn: Transaction
    ) -> Iterator[tuple]:
        """执行 seed，然后反复执行 recursive 项，直到没有新行。"""
        assert node.seed is not None and node.recursive is not None
        saved = self._recursive_rows.get(node.name)
        seen: Optional[set] = None if node.union_all else set()
        output: list[tuple] = []
        working: list[tuple] = []
        try:
            for row in self._execute(node.seed, txn):
                key = tuple(row)
                if seen is not None:
                    if key in seen:
                        continue
                    seen.add(key)
                output.append(row)
                working.append(row)
            rounds = 0
            while working:
                rounds += 1
                if rounds > 10000:
                    raise NotSupportedError(
                        f"递归 CTE {node.name!r} 超过 10000 轮",
                        hint="检查递归终止条件，或改用显式迭代",
                    )
                self._recursive_rows[node.name] = working
                next_rows: list[tuple] = []
                for row in self._execute(node.recursive, txn):
                    key = tuple(row)
                    if seen is not None:
                        if key in seen:
                            continue
                        seen.add(key)
                    output.append(row)
                    next_rows.append(row)
                working = next_rows
        finally:
            if saved is None:
                self._recursive_rows.pop(node.name, None)
            else:
                self._recursive_rows[node.name] = saved
        yield from output

    def _set_op(self, node: SetOp, txn: Transaction) -> Iterator[tuple]:
        assert node.left is not None and node.right is not None
        if node.op == "UNION":
            if node.all:
                yield from self._execute(node.left, txn)
                yield from self._execute(node.right, txn)
                return
            seen: set[tuple] = set()
            for child in (node.left, node.right):
                for row in self._execute(child, txn):
                    row = tuple(row)
                    if row in seen:
                        continue
                    seen.add(row)
                    yield row
            return

        if node.op == "EXCEPT":
            if node.all:
                # EXCEPT ALL 是多重集差：左侧每命中右侧一次，消耗一份右侧重复行。
                right_counts = Counter(
                    tuple(row) for row in self._execute(node.right, txn)
                )
                for row in self._execute(node.left, txn):
                    row = tuple(row)
                    if right_counts.get(row, 0) > 0:
                        right_counts[row] -= 1
                        continue
                    yield row
                return
            right_rows = {tuple(row) for row in self._execute(node.right, txn)}
            seen: set[tuple] = set()
            for row in self._execute(node.left, txn):
                row = tuple(row)
                if row in right_rows or row in seen:
                    continue
                seen.add(row)
                yield row
            return

        if node.op == "INTERSECT":
            if node.all:
                # INTERSECT ALL 是多重集交：右侧每有一份，左侧就产出一份。
                right_counts = Counter(
                    tuple(row) for row in self._execute(node.right, txn)
                )
                for row in self._execute(node.left, txn):
                    row = tuple(row)
                    if right_counts.get(row, 0) <= 0:
                        continue
                    right_counts[row] -= 1
                    yield row
                return
            right_rows = {tuple(row) for row in self._execute(node.right, txn)}
            seen: set[tuple] = set()
            for row in self._execute(node.left, txn):
                row = tuple(row)
                if row not in right_rows or row in seen:
                    continue
                seen.add(row)
                yield row
            return

        raise NotSupportedError(
            f"集合运算 {node.op} 尚未实现",
            hint="支持 UNION / UNION ALL / EXCEPT / EXCEPT ALL / INTERSECT / INTERSECT ALL",
        )

    def _join(self, node: Join, txn: Transaction) -> Iterator[tuple]:
        assert node.left is not None and node.right is not None
        if node.kind in ("RIGHT", "FULL"):
            yield from self._outer_join(node, txn)
            return
        if node.hash_keys and node.kind in ("INNER", "LEFT"):
            yield from self._hash_join(node, txn)
            return
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

    def _outer_join(self, node: Join, txn: Transaction) -> Iterator[tuple]:
        """RIGHT / FULL JOIN 的通用实现。

        RIGHT 以右表为外层；FULL 还要记录右表中哪些行被匹配过，最后补出
        未匹配的右行。两者都按 left+right 的列顺序输出。这里不建哈希表，
        因为外连接的“未匹配行”需要逐行判断，nested loop 的语义最直接。
        """
        assert node.left is not None and node.right is not None
        left_rows = [tuple(row) for row in self._execute(node.left, txn)]
        right_rows = [tuple(row) for row in self._execute(node.right, txn)]
        left_width = len(node.left.columns)
        right_width = len(node.right.columns)

        if node.kind == "RIGHT":
            for right in right_rows:
                matched = False
                for left in left_rows:
                    combined = left + right
                    if node.on is None or _truthy(node.on(EvalContext(combined))):
                        matched = True
                        yield combined
                if not matched:
                    yield (None,) * left_width + right
            return

        right_matched = [False] * len(right_rows)
        for left in left_rows:
            matched = False
            for j, right in enumerate(right_rows):
                combined = left + right
                if node.on is None or _truthy(node.on(EvalContext(combined))):
                    matched = True
                    right_matched[j] = True
                    yield combined
            if not matched:
                yield left + (None,) * right_width
        for j, right in enumerate(right_rows):
            if not right_matched[j]:
                yield (None,) * left_width + right

    def _hash_join(self, node: Join, txn: Transaction) -> Iterator[tuple]:
        assert node.left is not None and node.right is not None
        right_rows = list(self._execute(node.right, txn))
        right_width = len(node.right.columns)
        left_keys = [pair[0] for pair in node.hash_keys]
        right_keys = [pair[1] for pair in node.hash_keys]

        buckets: dict[tuple, list[tuple]] = {}
        for right in right_rows:
            key = tuple(right[i] for i in right_keys)
            if any(value is None for value in key):
                # SQL 语义：NULL 不参与等值匹配
                continue
            buckets.setdefault(key, []).append(tuple(right))

        for left in self._execute(node.left, txn):
            key = tuple(left[i] for i in left_keys)
            matches = () if any(v is None for v in key) else buckets.get(key, ())
            matched = False
            for right in matches:
                combined = tuple(left) + right
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
        for fn, descending, nulls_first in reversed(node.keys):
            rows.sort(
                key=lambda r, f=fn, d=descending, n=nulls_first: _sort_key(
                    f(EvalContext(r)), d, n
                ),
                reverse=descending,
            )
        yield from rows

    def _limit(self, node: Limit, txn: Transaction) -> Iterator[tuple]:
        """LIMIT/OFFSET；``WITH TIES`` 时继续输出与最后一条排序键相同的行。"""
        assert node.child is not None
        if node.limit == 0:
            return
        skipped = 0
        emitted = 0
        boundary: Optional[tuple] = None
        for row in self._execute(node.child, txn):
            if skipped < node.offset:
                skipped += 1
                continue
            if node.limit is not None and emitted >= node.limit:
                if not node.with_ties:
                    return
                # 没有 ORDER BY 时所有行互为并列；有排序键时只放行同键行。
                if node.tie_keys:
                    key = tuple(fn(EvalContext(row)) for fn in node.tie_keys)
                    if key != boundary:
                        return
                yield row
                continue
            emitted += 1
            yield row
            if node.limit is not None and emitted == node.limit and node.with_ties:
                boundary = tuple(fn(EvalContext(row)) for fn in node.tie_keys)

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
        by_name = {c.name: c for c in columns}

        table_primary: list[tuple[tuple[str, ...], Optional[str]]] = []
        for constraint in stmt.constraints:
            if constraint.kind != "PRIMARY KEY":
                continue
            if table_primary:
                raise NotSupportedError(
                    "只支持一个主键",
                    hint="一个表只能声明一个 PRIMARY KEY",
                )
            names = tuple(name.lower() for name in constraint.columns)
            for name in names:
                if name not in by_name:
                    raise BinderError(
                        f"表级主键引用了不存在的列 {name!r}"
                    )
            table_primary.append(
                (names, constraint.name.lower() if constraint.name else None)
            )

        column_primary = [c for c in columns if c.primary_key]
        if table_primary and column_primary:
            raise NotSupportedError(
                "不能同时声明列级和表级主键",
                hint="把 PRIMARY KEY 统一写在列上或表级约束里",
            )
        if len(column_primary) > 1:
            raise NotSupportedError(
                "只支持一个主键",
                hint="复合主键请用表级 PRIMARY KEY (col, ...)",
            )

        primary_name: Optional[str] = None
        if table_primary:
            primary_columns, primary_name = table_primary[0]
            # 表级复合主键把参与列标记为 NOT NULL + primary_key，
            # 列级读取和目录展示仍然看到完整的主键信息。
            columns = [
                replace(
                    c,
                    primary_key=c.name in primary_columns,
                    nullable=False if c.name in primary_columns else c.nullable,
                )
                if c.name in primary_columns
                else c
                for c in columns
            ]
        elif column_primary:
            primary_columns = (column_primary[0].name,)
        else:
            primary_columns = ()

        indexes: list[IndexDef] = []
        if primary_columns:
            indexes.append(
                IndexDef(
                    name=primary_name or f"{stmt.name.lower()}_pkey",
                    table=stmt.name.lower(),
                    columns=primary_columns,
                    unique=True,
                    constraint_kind="PRIMARY",
                )
            )

        # 列级 UNIQUE 不能只解析不落地：否则表看起来有约束，插入重复值
        # 却会静默成功。这里为每个非主键的 UNIQUE 列建一个唯一索引。
        for col_def, col in zip(stmt.columns, columns):
            if not col_def.unique or col.name in primary_columns:
                continue
            indexes.append(
                IndexDef(
                    name=f"{stmt.name.lower()}_{col.name}_key",
                    table=stmt.name.lower(),
                    columns=(col.name,),
                    unique=True,
                    constraint_kind="UNIQUE",
                )
            )

        # 表级 UNIQUE (a, b) 生成一个复合唯一索引；ON CONFLICT ON CONSTRAINT
        # 可以直接引用这个索引名。
        for constraint in stmt.constraints:
            if constraint.kind != "UNIQUE":
                continue
            names = tuple(name.lower() for name in constraint.columns)
            for name in names:
                if name not in by_name:
                    raise BinderError(
                        f"表级 UNIQUE 引用了不存在的列 {name!r}"
                    )
            if constraint.name is not None:
                index_name = constraint.name.lower()
            else:
                index_name = (
                    f"{stmt.name.lower()}_"
                    + "_".join(names)
                    + "_key"
                )
            indexes.append(
                IndexDef(
                    name=index_name,
                    table=stmt.name.lower(),
                    columns=names,
                    unique=True,
                    constraint_kind="UNIQUE",
                )
            )

        seen_index_names: set[str] = set()
        for idx in indexes:
            if idx.name in seen_index_names:
                raise BinderError(
                    f"索引/约束名重复：{idx.name}",
                    hint="显式命名表级约束时避免与自动生成的索引名冲突",
                )
            seen_index_names.add(idx.name)

        checks = self._collect_create_checks(stmt, columns, indexes)
        self._validate_check_defs(stmt.name.lower(), columns, checks)
        foreign_keys = self._collect_create_foreign_keys(
            stmt, columns, indexes, checks, primary_columns
        )
        self._catalog.create_table(
            txn, stmt.name, columns, indexes, checks, foreign_keys
        )
        return Result(tag="CREATE TABLE")

    def _collect_create_checks(
        self,
        stmt: CreateTable,
        columns: list[Column],
        indexes: list[IndexDef],
    ) -> dict[str, CheckDef]:
        """把列级/表级 CHECK 收集成目录对象，并生成稳定的默认约束名。"""
        table_name = stmt.name.lower()
        used = {idx.name for idx in indexes}
        checks: dict[str, CheckDef] = {}

        def add(spec, base_name: str, explicit_name: Optional[str] = None) -> None:
            explicit = (
                explicit_name.lower()
                if explicit_name is not None
                else (spec.name.lower() if spec.name is not None else None)
            )
            name = explicit or base_name
            if explicit is None:
                while name in used or name in checks:
                    name += "_2"
            if name in used or name in checks:
                raise BinderError(
                    f"CHECK 约束名重复：{name}",
                    hint="给 CHECK 指定唯一名称，或避免与索引名冲突",
                )
            checks[name] = CheckDef(name=name, tokens=tuple(spec.tokens))
            used.add(name)

        for col_def, col in zip(stmt.columns, columns):
            for spec in col_def.checks:
                add(spec, f"{table_name}_{col.name}_check")
        for constraint in stmt.constraints:
            if constraint.kind != "CHECK":
                continue
            assert constraint.check is not None
            add(
                constraint.check,
                f"{table_name}_check_{len(checks) + 1}",
                constraint.name,
            )
        return checks

    def _collect_create_foreign_keys(
        self,
        stmt: CreateTable,
        columns: list[Column],
        indexes: list[IndexDef],
        checks: dict[str, CheckDef],
        primary_columns: tuple[str, ...],
    ) -> dict[str, ForeignKeyDef]:
        table_name = stmt.name.lower()
        used = set(checks) | {idx.name for idx in indexes}
        result: dict[str, ForeignKeyDef] = {}
        by_name = {c.name: c for c in columns}

        def add(spec: ForeignKeySpec, fallback_columns: list[str]) -> None:
            local_columns = tuple(
                name.lower() for name in (spec.columns or fallback_columns)
            )
            if len(set(local_columns)) != len(local_columns):
                raise BinderError("外键的本地列不能重复")
            for name in local_columns:
                if name not in by_name:
                    raise BinderError(
                        f"外键引用了本表不存在的列 {name!r}"
                    )
            ref_table = spec.ref_table.lower()
            if ref_table == table_name:
                ref_columns = tuple(
                    name.lower()
                    for name in (spec.ref_columns or primary_columns)
                )
                if not ref_columns:
                    raise BinderError(
                        "自引用外键需要显式指定被引用列，或先声明主键"
                    )
                ref_indexes = list(indexes)
                ref_by_name = by_name
            else:
                ref = self._catalog.get_table(ref_table)
                if spec.ref_columns:
                    ref_columns = tuple(
                        name.lower() for name in spec.ref_columns
                    )
                else:
                    primary = next(
                        (
                            idx
                            for idx in ref.indexes.values()
                            if idx.constraint_kind == "PRIMARY"
                        ),
                        None,
                    )
                    if primary is None:
                        raise BinderError(
                            f"被引用表 {ref_table!r} 没有主键，"
                            "REFERENCES 必须显式给出列"
                        )
                    ref_columns = primary.columns
                ref_indexes = list(ref.indexes.values())
                ref_by_name = {c.name: c for c in ref.columns}

            if len(local_columns) != len(ref_columns):
                raise BinderError(
                    "外键本地列数与被引用列数不一致",
                    detail=f"{local_columns} -> {ref_columns}",
                )
            for name in ref_columns:
                if name not in ref_by_name:
                    raise BinderError(
                        f"被引用表 {ref_table!r} 没有列 {name!r}"
                    )
            if not any(
                idx.unique and tuple(idx.columns) == ref_columns
                for idx in ref_indexes
            ):
                raise BinderError(
                    "被引用列必须上有 UNIQUE 或 PRIMARY KEY 约束",
                    detail=f"{ref_table}({', '.join(ref_columns)})",
                    hint="先给被引用列建唯一约束/唯一索引",
                )
            for local, referenced in zip(local_columns, ref_columns):
                if by_name[local].type != ref_by_name[referenced].type:
                    raise TypeMismatchError(
                        f"外键列 {local} 与被引用列 {referenced} 类型不一致"
                    )

            explicit = spec.name.lower() if spec.name is not None else None
            base = (
                f"{table_name}_" + "_".join(local_columns) + "_fkey"
            )
            name = explicit or base
            if explicit is None:
                while name in used or name in result:
                    name += "_2"
            if name in used or name in result:
                raise BinderError(
                    f"外键约束名重复：{name}",
                    hint="给 FOREIGN KEY 指定唯一名称",
                )
            result[name] = ForeignKeyDef(
                name=name,
                columns=local_columns,
                ref_table=ref_table,
                ref_columns=ref_columns,
                on_delete=spec.on_delete,
                on_update=spec.on_update,
                match_type=spec.match_type,
                deferrable=spec.deferrable,
                initially_deferred=spec.initially_deferred,
            )
            used.add(name)

        for col_def, col in zip(stmt.columns, columns):
            if col_def.references is not None:
                add(col_def.references, [col.name])
        for constraint in stmt.constraints:
            if constraint.kind != "FOREIGN KEY":
                continue
            add(
                ForeignKeySpec(
                    columns=list(constraint.columns),
                    ref_table=constraint.ref_table or "",
                    ref_columns=list(constraint.ref_columns),
                    name=constraint.name,
                    on_delete=constraint.on_delete,
                    on_update=constraint.on_update,
                    match_type=constraint.match_type,
                    deferrable=constraint.deferrable,
                    initially_deferred=constraint.initially_deferred,
                ),
                list(constraint.columns),
            )
        return result

    def _collect_column_checks(
        self,
        table: TableDef,
        column_name: str,
        specs,
    ) -> dict[str, CheckDef]:
        """为 ADD COLUMN 的列级 CHECK 生成目录对象，复用现有命名空间。"""
        used = set(table.indexes) | set(table.checks)
        checks: dict[str, CheckDef] = {}
        for spec in specs:
            if not spec.tokens:
                raise BinderError(
                    "CHECK 约束缺少表达式 token",
                    hint="使用 SQL 文本声明 CHECK，而不是手工构造 AST",
                )
            explicit = spec.name.lower() if spec.name is not None else None
            name = explicit or f"{table.name}_{column_name}_check"
            if explicit is None:
                while name in used or name in checks:
                    name += "_2"
            if name in used or name in checks:
                raise BinderError(
                    f"CHECK 约束名重复：{name}",
                    hint="给 CHECK 指定唯一名称，或避免与索引名冲突",
                )
            checks[name] = CheckDef(name=name, tokens=tuple(spec.tokens))
            used.add(name)
        return checks

    def _validate_check_defs(
        self,
        table_name: str,
        columns: list[Column],
        checks: dict[str, CheckDef],
    ) -> None:
        if not checks:
            return
        table = TableDef(name=table_name, columns=columns, checks=checks)
        for check in checks.values():
            self._compile_check(table, check)

    def _compile_check(self, table: TableDef, check: CheckDef) -> Any:
        """从持久化 token 重建 CHECK 谓词；结果按 (表, 名, token, 列类型) 缓存。"""
        key = (
            table.name,
            check.name,
            check.tokens,
            tuple((col.name, col.type) for col in table.columns),
        )
        cached = self._check_cache.get(key)
        if cached is not None:
            return cached
        from ..sql.lexer import Token
        from ..sql.parser import Parser

        if any(
            kind == "keyword" and value.upper() in ("SELECT", "WITH")
            for kind, value in check.tokens
        ):
            raise NotSupportedError(
                f"CHECK 约束 {check.name} 不支持子查询",
                hint="把约束条件写成只引用当前行列的表达式",
            )
        tokens = [
            Token(kind, value, 0, 0) for kind, value in check.tokens
        ]
        tokens.append(Token("eof", "", 0, 0))
        expr = Parser(tokens).parse_expression()
        schema = Schema(
            [SchemaColumn(table.name, c.name, c.type) for c in table.columns]
        )
        fn, result_type = self.binder.compile(
            expr, schema, allow_agg=False
        )
        if not result_type.unknown and result_type.kind is not DataType.BOOLEAN:
            raise TypeMismatchError(
                f"CHECK 约束 {check.name} 的表达式必须返回 BOOLEAN",
                detail=f"实际类型 {result_type.name}",
                hint="改写比较表达式，或使用布尔列",
            )
        self._check_cache[key] = fn
        return fn

    def _check_checks(self, table: TableDef, values: list[Any]) -> None:
        for check in table.checks.values():
            fn = self._compile_check(table, check)
            result = fn(EvalContext(tuple(values)))
            # SQL CHECK 只在结果为 FALSE 时拒绝；NULL / UNKNOWN 视为通过。
            if result is not None and not bool(result):
                raise CheckViolationError(
                    f"违反 CHECK 约束 {check.name!r}",
                    detail=f"表 {table.name} 的行不满足约束条件",
                )

    def _refresh_view_dependencies(self, txn: Transaction) -> None:
        updates: dict[str, set[str]] = {}
        for name in self._catalog.view_names():
            binder = self._new_binder()
            updates[name.lower()] = binder.collect_view_dependencies(name)
        self._catalog.update_view_dependencies(txn, updates)

    def _create_view(self, stmt: CreateView, txn: Transaction) -> Result:
        if self._catalog.has_table(stmt.name):
            raise TableAlreadyExistsError(
                f"对象 {stmt.name!r} 已存在",
                hint="表名和视图名共享同一个命名空间",
            )
        if self._catalog.has_view(stmt.name) and not stmt.or_replace:
            raise TableAlreadyExistsError(
                f"视图 {stmt.name!r} 已存在",
                hint="使用 CREATE OR REPLACE VIEW 覆盖，或先 DROP VIEW",
            )
        # 建视图时先完整绑定一次，提前暴露未知表/列和类型错误；
        # 视图不物化数据，SELECT 时再按保存的 token 重新绑定。
        binder = self._new_binder()
        if stmt.or_replace:
            # 防止 OR REPLACE 把视图替换成依赖自身的递归定义。
            binder.view_stack = frozenset({stmt.name.lower()})
        if isinstance(stmt.query, Query):
            bound_cols = binder.bind_query(stmt.query)
        else:
            bound_cols = binder.bind_select(stmt.query)
        if stmt.columns:
            if len(stmt.columns) != len(bound_cols):
                raise BinderError(
                    f"视图 {stmt.name!r} 的列清单有 {len(stmt.columns)} 列，"
                    f"但查询输出 {len(bound_cols)} 列",
                    hint="视图列名数量必须与查询输出的列数一致",
                )
            names = [name.lower() for name in stmt.columns]
            if len(set(names)) != len(names):
                raise BinderError(
                    f"视图 {stmt.name!r} 的列名重复",
                    hint="视图列名必须唯一",
                )
        if stmt.or_replace and self._catalog.has_view(stmt.name):
            self._catalog.replace_view(
                txn, stmt.name, stmt.tokens, stmt.columns,
                binder.source_dependencies,
            )
            # 替换可能改变了间接依赖：重算所有视图，避免依赖快照里残留旧表，
            # 也避免新表没有被依赖视图记录而允许 DROP TABLE。
            self._refresh_view_dependencies(txn)
            notice = f"已替换视图 {stmt.name.lower()}"
        else:
            self._catalog.create_view(
                txn, stmt.name, stmt.tokens, stmt.columns,
                binder.source_dependencies,
            )
            notice = f"已创建视图 {stmt.name.lower()}"
        return Result(tag="CREATE VIEW", notice=notice)

    def _drop_view(self, stmt: DropView, txn: Transaction) -> Result:
        try:
            self._catalog.drop_view(txn, stmt.name, stmt.cascade)
        except TableNotFoundError:
            if stmt.if_exists:
                return Result(tag="DROP VIEW", notice="视图不存在，已跳过")
            raise
        return Result(tag="DROP VIEW")

    def _create_table_as(
        self, stmt: CreateTableAs, txn: Transaction
    ) -> Result:
        if self._catalog.has_table(stmt.name):
            if stmt.if_not_exists:
                return Result(tag="CREATE TABLE", notice="表已存在，已跳过")
            raise TableAlreadyExistsError(
                f"表 {stmt.name!r} 已存在",
                hint="使用 DROP TABLE 后再建",
            )

        plan, out_cols = self.binder.bind_select(stmt.query)
        columns: list[Column] = []
        seen: set[str] = set()
        for i, out in enumerate(out_cols, start=1):
            name = out.name.lower()
            if not _IDENT_RE.match(name) or name in seen:
                raise BinderError(
                    "CREATE TABLE AS 的列名必须是合法且唯一的标识符",
                    detail=f"第 {i} 列是 {out.name!r}",
                    hint="给表达式加一个 AS 别名，例如 COUNT(*) AS n",
                )
            seen.add(name)
            # CTAS 必须把 unknown（SELECT NULL）落成具体可存储类型；
            # PostgreSQL 也把这种列推成 text。
            col_type = TypeSpec(DataType.TEXT) if out.type.unknown else out.type
            columns.append(Column(name=name, type=col_type, nullable=True))

        self._catalog.create_table(txn, stmt.name, columns, [])
        table = self._catalog.get_table(stmt.name)
        heap = HeapFile(self._bp, table, self._txns)
        # 先物化查询结果：如果来源恰好读了同名的旧表（IF NOT EXISTS 不适用），
        # 建表之后再读会改变语义。这里统一在写入前固定结果集。
        source_rows = [tuple(row) for row in self._execute(plan, txn)]
        for row in source_rows:
            heap.insert(txn, list(row))

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return Result(
            affected=len(source_rows),
            tag=f"CREATE TABLE AS SELECT ({len(source_rows)} rows)",
        )

    def _alter_add_column(self, stmt: AlterTableAddColumn, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        name = stmt.column.name.lower()
        if table.has_column(name):
            if stmt.if_not_exists:
                return Result(
                    tag="ALTER TABLE",
                    notice=f"列 {name} 已存在，已跳过",
                )
            raise BinderError(
                f"列 {name!r} 已存在",
                hint="换一个列名，或先 DROP TABLE 重建",
            )
        column = _to_column(stmt.column)
        if not column.nullable and column.default is None:
            raise NotSupportedError(
                "ADD COLUMN NOT NULL 需要提供 DEFAULT",
                hint="已有行没有这个列的值；先加可空列，再 UPDATE 后改约束",
            )
        new_checks = self._collect_column_checks(
            table, column.name, stmt.column.checks
        )
        # 先用“旧列 + 新列默认值”拼出存量行的完整形状，验证新 CHECK；
        # 校验失败时不修改目录，避免加列成功但约束没落地的半状态。
        provisional = TableDef(
            name=table.name,
            columns=list(table.columns) + [column],
            indexes=table.indexes,
            checks=dict(table.checks, **new_checks),
        )
        for check in new_checks.values():
            self._compile_check(provisional, check)
        if new_checks:
            heap = HeapFile(self._bp, table, self._txns)
            for _rid, values in heap.scan(txn):
                padded = list(values)
                while len(padded) < len(provisional.columns):
                    padded.append(column.default)
                for check in new_checks.values():
                    fn = self._compile_check(provisional, check)
                    result = fn(EvalContext(tuple(padded)))
                    if result is not None and not bool(result):
                        raise CheckViolationError(
                            f"已有行违反 CHECK 约束 {check.name!r}",
                            detail=f"表 {table.name} 的存量数据不满足新列约束",
                            hint="换一个默认值，或先 UPDATE 修正存量行",
                        )

        old_columns = list(table.columns)
        old_checks = dict(table.checks)
        table.columns.append(column)
        table.checks.update(new_checks)

        def undo() -> None:
            table.columns = old_columns
            table.checks = old_checks

        txn.undo_log.append(undo)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        notice = (
            f"已添加列 {name}；旧记录在读取时按默认值补齐，"
            "不重写堆页"
        )
        ref = stmt.column.references
        if ref is not None:
            fk_result = self._alter_add_constraint(
                AlterTableAddConstraint(
                    table=stmt.table,
                    constraint=TableConstraint(
                        kind="FOREIGN KEY",
                        columns=[name],
                        name=ref.name,
                        ref_table=ref.ref_table,
                        ref_columns=list(ref.ref_columns),
                        on_delete=ref.on_delete,
                        on_update=ref.on_update,
                        match_type=ref.match_type,
                        deferrable=ref.deferrable,
                        initially_deferred=ref.initially_deferred,
                    ),
                ),
                txn,
            )
            notice += f"；{fk_result.notice}"
        return Result(tag="ALTER TABLE", notice=notice)

    def _alter_default(
        self, stmt: AlterTableAlterDefault, txn: Transaction
    ) -> Result:
        from ..sql.ast import Literal

        table = self._catalog.get_table(stmt.table)
        index = table.column_index(stmt.column)
        column = table.columns[index]
        if stmt.default is not None:
            if not isinstance(stmt.default, Literal):
                raise NotSupportedError(
                    "ALTER COLUMN SET DEFAULT 目前只支持字面常量",
                    hint="默认值不能引用列或函数",
                )
            value = stmt.default.value
            if value is not None:
                try:
                    value = column.type.coerce(value)
                except ValueError as exc:
                    raise TypeMismatchError(
                        f"列 {column.name} 的默认值类型不匹配",
                        detail=str(exc),
                    ) from exc
        else:
            value = None
        old = column
        table.columns[index] = replace(column, default=value)

        def undo() -> None:
            table.columns[index] = old

        txn.undo_log.append(undo)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        action = "设置" if stmt.default is not None else "删除"
        return Result(
            tag="ALTER TABLE",
            notice=f"已{action}列 {column.name} 的默认值",
        )

    def _alter_nullability(
        self, stmt: AlterTableAlterNullability, txn: Transaction
    ) -> Result:
        """SET/DROP NOT NULL：只改目录约束，不重写任何已有记录。

        SET NOT NULL 必须先扫描堆表确认没有 NULL；校验失败时目录保持原样。
        主键列天然 NOT NULL，拒绝 DROP NOT NULL，避免破坏主键索引的不变量。
        """
        table = self._catalog.get_table(stmt.table)
        index = table.column_index(stmt.column)
        column = table.columns[index]
        if stmt.nullable and column.primary_key:
            raise BinderError(
                f"列 {column.name!r} 属于主键，不能 DROP NOT NULL",
                hint="主键列必须保持 NOT NULL",
            )

        checked = 0
        if not stmt.nullable:
            heap = HeapFile(self._bp, table, self._txns)
            for _rid, values in heap.scan(txn):
                checked += 1
                if values[index] is None:
                    raise NotNullViolationError(
                        f"列 {column.name!r} 存在 NULL，无法 SET NOT NULL",
                        detail=f"已扫描 {checked} 行，首次发现 NULL",
                        hint="先 UPDATE 或 DELETE 含 NULL 的行",
                    )

        old = column
        table.columns[index] = replace(column, nullable=stmt.nullable)

        def undo() -> None:
            table.columns[index] = old

        txn.undo_log.append(undo)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        action = "删除" if stmt.nullable else "设置"
        notice = f"已{action}列 {column.name} 的 NOT NULL 约束"
        if not stmt.nullable:
            notice += f"；已校验 {checked} 行"
        return Result(tag="ALTER TABLE", notice=notice)

    def _alter_type(
        self, stmt: AlterTableAlterType, txn: Transaction
    ) -> Result:
        table = self._catalog.get_table(stmt.table)
        index = table.column_index(stmt.column)
        old_column = table.columns[index]
        new_type = stmt.type
        if new_type == old_column.type:
            return Result(
                tag="ALTER TABLE",
                notice=f"列 {old_column.name} 的类型已经是 {new_type.name}",
            )
        for fk in table.foreign_keys.values():
            if old_column.name in fk.columns:
                raise NotSupportedError(
                    f"列 {old_column.name!r} 被外键约束 {fk.name!r} 引用",
                    hint="先删除外键约束，再修改列类型",
                )
        for other in self._catalog.list_tables():
            for fk in other.foreign_keys.values():
                if (
                    fk.ref_table == table.name
                    and old_column.name in fk.ref_columns
                ):
                    raise NotSupportedError(
                        f"列 {old_column.name!r} 被表 {other.name} 的外键 "
                        f"{fk.name!r} 引用",
                        hint="先删除外键约束，再修改列类型",
                    )
        # 先物化旧行：类型一旦改变，后续 scan 会按新类型解码旧记录。
        heap = HeapFile(self._bp, table, self._txns)
        source_rows = [
            (rid, list(values)) for rid, values in heap.scan(txn)
        ]
        using_fn = None
        if stmt.using is not None:
            schema = Schema(
                [
                    SchemaColumn(table.name, col.name, col.type)
                    for col in table.columns
                ]
            )
            using_fn, _result_type = self.binder.compile(
                stmt.using, schema, allow_agg=False
            )
        converted: list[tuple[Any, list[Any], list[Any]]] = []
        for rid, values in source_rows:
            new_values = list(values)
            source_value = values[index]
            if using_fn is not None:
                source_value = using_fn(EvalContext(tuple(values)))
            try:
                new_values[index] = new_type.coerce(source_value)
            except TypeMismatchError as exc:
                raise TypeMismatchError(
                    f"列 {old_column.name} 无法从 {old_column.type.name} "
                    f"转换为 {new_type.name}",
                    detail=f"行 {rid}：{exc}",
                    hint="先 UPDATE 修正不兼容的值，或改用可转换的目标类型",
                ) from exc
            if new_values[index] is None and not old_column.nullable:
                raise NotNullViolationError(
                    f"列 {old_column.name} 的 USING 表达式产生了 NULL",
                    detail=f"行 {rid}",
                    hint="保证转换表达式对每一行都返回非 NULL，或先 DROP NOT NULL",
                )
            converted.append((rid, values, new_values))

        new_default = old_column.default
        if new_default is not None:
            try:
                new_default = new_type.coerce(new_default)
            except TypeMismatchError as exc:
                raise TypeMismatchError(
                    f"列 {old_column.name} 的默认值无法转换为 {new_type.name}",
                    detail=str(exc),
                    hint="先用 ALTER COLUMN ... DROP DEFAULT 清除默认值",
                ) from exc

        new_columns = list(table.columns)
        new_columns[index] = replace(
            old_column, type=new_type, default=new_default
        )
        provisional = TableDef(
            name=table.name,
            columns=new_columns,
            indexes=table.indexes,
            checks=table.checks,
        )
        for check in table.checks.values():
            self._compile_check(provisional, check)
        for _rid, _old_values, new_values in converted:
            self._check_checks(provisional, new_values)

        old_columns = list(table.columns)
        old_indexes = dict(table.indexes)
        self._snapshot_table_state(table, txn)

        # 旧版本仍按旧列类型编码；不能在原堆页上 UPDATE，否则后续扫描
        # 会用新 schema 解码旧版本。这里把可见行写入一棵新的堆页链，
        # 然后一次性切换目录里的 root/last_page，旧页链不再被引用。
        shadow = TableDef(name=table.name, columns=new_columns)
        shadow_heap = HeapFile(self._bp, shadow, self._txns)
        for _rid, _old_values, new_values in converted:
            shadow_heap.insert(txn, new_values)

        table.columns = new_columns
        table.root_page = shadow.root_page
        table.last_page = shadow.last_page
        table.row_count_hint = shadow.row_count_hint

        def undo() -> None:
            table.columns = old_columns

        txn.undo_log.append(undo)

        rebuilt = 0
        if old_indexes:
            rebuilt, _entries = self._rebuild_indexes(table, txn)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        index_note = (
            f"；重建 {rebuilt} 个索引" if rebuilt else ""
        )
        return Result(
            tag="ALTER TABLE",
            notice=(
                f"已把列 {old_column.name} 的类型从 {old_column.type.name} "
                f"改为 {new_type.name}；重写 {len(converted)} 行"
                f"{index_note}"
            ),
        )

    def _alter_drop_column(
        self, stmt: AlterTableDropColumn, txn: Transaction
    ) -> Result:
        table = self._catalog.get_table(stmt.table)
        if not table.has_column(stmt.column):
            if stmt.if_exists:
                return Result(
                    tag="ALTER TABLE",
                    notice=f"列 {stmt.column} 不存在，已跳过",
                )
            raise BinderError(
                f"列 {stmt.column!r} 不存在",
                hint=f"可用列：{', '.join(table.column_names)}",
            )
        index = table.column_index(stmt.column)
        column = table.columns[index]
        if len(table.columns) == 1:
            raise NotSupportedError(
                "表必须至少保留一列，不能删除最后一列",
                hint="可以先 DROP TABLE 再重建空表",
            )
        for fk in table.foreign_keys.values():
            if column.name in fk.columns:
                raise NotSupportedError(
                    f"列 {column.name!r} 被外键约束 {fk.name!r} 引用",
                    hint="先 ALTER TABLE ... DROP CONSTRAINT 删除外键",
                )
        for other in self._catalog.list_tables():
            for fk in other.foreign_keys.values():
                if fk.ref_table == table.name and column.name in fk.ref_columns:
                    raise NotSupportedError(
                        f"列 {column.name!r} 被表 {other.name} 的外键 "
                        f"{fk.name!r} 引用",
                        hint="先删除引用它的外键约束",
                    )
        for idx in table.indexes.values():
            if column.name in idx.columns:
                raise NotSupportedError(
                    f"列 {column.name!r} 被索引/约束 {idx.name!r} 引用",
                    hint="先 DROP INDEX 或 ALTER TABLE ... DROP CONSTRAINT",
                )
        for check in table.checks.values():
            tokens = list(check.tokens)
            for pos, (kind, value) in enumerate(tokens):
                if kind not in ("ident", "keyword"):
                    continue
                if value.lower() != column.name:
                    continue
                # ``t.a`` 里的 ``t`` 是表限定符，不是列引用。
                if pos + 1 < len(tokens) and tokens[pos + 1][0] == "dot":
                    continue
                raise BinderError(
                    f"CHECK 约束 {check.name!r} 引用了列 {column.name!r}",
                    hint="先删除引用该列的 CHECK 约束",
                )

        heap = HeapFile(self._bp, table, self._txns)
        source_rows = [
            (rid, list(values)) for rid, values in heap.scan(txn)
        ]
        new_columns = [
            col for pos, col in enumerate(table.columns) if pos != index
        ]
        new_rows: list[list[Any]] = []
        for _rid, values in source_rows:
            new_values = values[:index] + values[index + 1 :]
            new_rows.append(new_values)

        provisional = TableDef(
            name=table.name,
            columns=new_columns,
            checks=table.checks,
        )
        for check in table.checks.values():
            self._compile_check(provisional, check)
        for values in new_rows:
            self._check_checks(provisional, values)

        old_columns = list(table.columns)
        old_indexes = dict(table.indexes)
        self._snapshot_table_state(table, txn)

        shadow = TableDef(name=table.name, columns=new_columns)
        shadow_heap = HeapFile(self._bp, shadow, self._txns)
        for values in new_rows:
            shadow_heap.insert(txn, values)

        table.columns = new_columns
        table.root_page = shadow.root_page
        table.last_page = shadow.last_page
        table.row_count_hint = shadow.row_count_hint

        def undo() -> None:
            table.columns = old_columns

        txn.undo_log.append(undo)

        rebuilt = 0
        if old_indexes:
            rebuilt, _entries = self._rebuild_indexes(table, txn)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        index_note = f"；重建 {rebuilt} 个索引" if rebuilt else ""
        return Result(
            tag="ALTER TABLE",
            notice=(
                f"已删除列 {column.name}；重写 {len(new_rows)} 行"
                f"{index_note}"
            ),
        )

    def _alter_rename_constraint(
        self, stmt: AlterTableRenameConstraint, txn: Transaction
    ) -> Result:
        table = self._catalog.get_table(stmt.table)
        old_key = stmt.old_name.lower()
        new_key = stmt.new_name.lower()
        if (
            old_key not in table.checks
            and old_key not in table.indexes
            and old_key not in table.foreign_keys
        ):
            raise BinderError(
                f"表 {table.name} 没有约束 {stmt.old_name!r}",
                hint="约束名可由 SHOW CREATE TABLE 或 \\d 查看",
            )
        if (
            new_key in table.checks
            or new_key in table.indexes
            or new_key in table.foreign_keys
        ):
            if new_key != old_key:
                raise BinderError(
                    f"约束名 {stmt.new_name!r} 已被占用",
                    hint="同一张表内约束/索引名必须唯一",
                )
        if old_key == new_key:
            return Result(
                tag="ALTER TABLE",
                notice=f"约束名已经是 {new_key}",
            )

        if old_key in table.checks:
            check = table.checks[old_key]
            new_check = replace(check, name=new_key)
            table.checks.pop(old_key)
            table.checks[new_key] = new_check

            def undo() -> None:
                table.checks.pop(new_key, None)
                table.checks[old_key] = check

            txn.undo_log.append(undo)
        elif old_key in table.foreign_keys:
            fk = table.foreign_keys[old_key]
            new_fk = replace(fk, name=new_key)
            table.foreign_keys.pop(old_key)
            table.foreign_keys[new_key] = new_fk

            def undo() -> None:
                table.foreign_keys.pop(new_key, None)
                table.foreign_keys[old_key] = fk

            txn.undo_log.append(undo)
        else:
            idx = table.indexes[old_key]
            if idx.constraint_kind is None:
                raise NotSupportedError(
                    f"{old_key!r} 是普通索引，不是表约束",
                    hint="普通索引请使用 CREATE INDEX / DROP INDEX 管理",
                )
            new_idx = replace(idx, name=new_key)
            table.indexes.pop(old_key)
            table.indexes[new_key] = new_idx

            def undo() -> None:
                table.indexes.pop(new_key, None)
                table.indexes[old_key] = idx

            txn.undo_log.append(undo)

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return Result(
            tag="ALTER TABLE",
            notice=f"约束 {old_key} 已重命名为 {new_key}",
        )

    def _alter_add_constraint(
        self, stmt: AlterTableAddConstraint, txn: Transaction
    ) -> Result:
        table = self._catalog.get_table(stmt.table)
        constraint = stmt.constraint
        names = tuple(name.lower() for name in constraint.columns)
        for name in names:
            table.column(name)
        if constraint.kind == "CHECK":
            assert constraint.check is not None
            spec = constraint.check
            explicit = (
                constraint.name.lower()
                if constraint.name is not None
                else (spec.name.lower() if spec.name is not None else None)
            )
            name = explicit or f"{table.name}_check_{len(table.checks) + 1}"
            if explicit is None:
                while name in table.checks or name in table.indexes:
                    name += "_2"
            if name in table.checks or name in table.indexes:
                raise BinderError(
                    f"CHECK 约束名重复：{name}",
                    hint="给 CHECK 指定唯一名称，或避免与索引名冲突",
                )
            check = CheckDef(name=name, tokens=tuple(spec.tokens))
            fn = self._compile_check(table, check)
            heap = HeapFile(self._bp, table, self._txns)
            for _rid, values in heap.scan(txn):
                result = fn(EvalContext(tuple(values)))
                if result is not None and not bool(result):
                    raise CheckViolationError(
                        f"已有行违反 CHECK 约束 {name!r}",
                        detail=f"表 {table.name} 的存量数据不满足约束条件",
                        hint="先 UPDATE 修正存量行，再添加约束",
                    )
            old_checks = dict(table.checks)
            table.checks[name] = check

            def undo_check() -> None:
                table.checks = old_checks

            txn.undo_log.append(undo_check)
            self._catalog.mark_dirty()
            self._catalog.persist(txn)
            return Result(
                tag="ALTER TABLE",
                notice=f"已添加 CHECK 约束 {name}",
            )
        if constraint.kind == "FOREIGN KEY":
            if constraint.ref_table is None:
                raise BinderError("FOREIGN KEY 缺少 REFERENCES 目标")
            ref_name = constraint.ref_table.lower()
            ref_table = self._catalog.get_table(ref_name)
            if constraint.ref_columns:
                ref_columns = tuple(
                    name.lower() for name in constraint.ref_columns
                )
            else:
                primary = next(
                    (
                        idx
                        for idx in ref_table.indexes.values()
                        if idx.constraint_kind == "PRIMARY"
                    ),
                    None,
                )
                if primary is None:
                    raise BinderError(
                        f"被引用表 {ref_name!r} 没有主键，"
                        "REFERENCES 必须显式给出列"
                    )
                ref_columns = primary.columns
            if len(names) != len(ref_columns):
                raise BinderError(
                    "外键本地列数与被引用列数不一致",
                    detail=f"{names} -> {ref_columns}",
                )
            for ref_col in ref_columns:
                ref_table.column(ref_col)
            if not any(
                idx.unique and tuple(idx.columns) == ref_columns
                for idx in ref_table.indexes.values()
            ):
                raise BinderError(
                    "被引用列必须上有 UNIQUE 或 PRIMARY KEY 约束",
                    detail=f"{ref_name}({', '.join(ref_columns)})",
                )
            for local, referenced in zip(names, ref_columns):
                if table.column(local).type != ref_table.column(referenced).type:
                    raise TypeMismatchError(
                        f"外键列 {local} 与被引用列 {referenced} 类型不一致"
                    )
            explicit = (
                constraint.name.lower()
                if constraint.name is not None
                else None
            )
            fk_name = explicit or (
                f"{table.name}_" + "_".join(names) + "_fkey"
            )
            used = set(table.checks) | set(table.indexes) | set(table.foreign_keys)
            if explicit is None:
                while fk_name in used:
                    fk_name += "_2"
            if fk_name in used:
                raise BinderError(
                    f"外键约束名重复：{fk_name}",
                    hint="给 FOREIGN KEY 指定唯一名称",
                )
            fk = ForeignKeyDef(
                name=fk_name,
                columns=names,
                ref_table=ref_name,
                ref_columns=ref_columns,
                on_delete=constraint.on_delete,
                on_update=constraint.on_update,
                match_type=constraint.match_type,
                deferrable=constraint.deferrable,
                initially_deferred=constraint.initially_deferred,
            )
            provisional = TableDef(
                name=table.name,
                columns=table.columns,
                indexes=table.indexes,
                checks=table.checks,
                foreign_keys=dict(table.foreign_keys, **{fk_name: fk}),
            )
            heap = HeapFile(self._bp, table, self._txns)
            for _rid, values in heap.scan(txn):
                self._check_foreign_keys(provisional, values, txn, force=True)
            old_fks = dict(table.foreign_keys)
            table.foreign_keys[fk_name] = fk

            def undo_fk() -> None:
                table.foreign_keys = old_fks

            txn.undo_log.append(undo_fk)
            self._catalog.mark_dirty()
            self._catalog.persist(txn)
            return Result(
                tag="ALTER TABLE",
                notice=f"已添加外键约束 {fk_name}",
            )
        if constraint.kind == "UNIQUE":
            index_name = (
                constraint.name.lower()
                if constraint.name is not None
                else f"{table.name}_{'_'.join(names)}_key"
            )
            # 复用 CREATE UNIQUE INDEX 的回填逻辑：先建目录索引，再扫描堆表
            # 填入键；已有重复值会直接报 DuplicateKeyError，并随事务回滚。
            self._create_index(
                CreateIndex(
                    name=index_name,
                    table=table.name,
                    columns=list(names),
                    unique=True,
                ),
                txn,
                constraint_kind="UNIQUE",
            )
            return Result(
                tag="ALTER TABLE",
                notice=f"已添加唯一约束 {index_name}",
            )

        if constraint.kind == "PRIMARY KEY":
            if any(col.primary_key for col in table.columns):
                raise NotSupportedError(
                    "表已有主键",
                    hint="先删除旧主键，再添加新主键",
                )
            # 主键隐含 NOT NULL；已有数据里的 NULL 必须在建索引前拒绝，
            # 否则唯一索引会跳过 NULL，看起来成功却不符合主键语义。
            heap = HeapFile(self._bp, table, self._txns)
            positions = [table.column_index(name) for name in names]
            for _rid, values in heap.scan(txn):
                for pos in positions:
                    if values[pos] is None:
                        raise NotNullViolationError(
                            f"主键列 {table.columns[pos].name} 存在 NULL",
                            hint="先 UPDATE 填充 NULL，再添加主键",
                        )
            index_name = (
                constraint.name.lower()
                if constraint.name is not None
                else f"{table.name}_pkey"
            )
            self._create_index(
                CreateIndex(
                    name=index_name,
                    table=table.name,
                    columns=list(names),
                    unique=True,
                ),
                txn,
                constraint_kind="PRIMARY",
            )
            old_columns = list(table.columns)
            table.columns = [
                replace(
                    col,
                    primary_key=col.name in names,
                    nullable=False if col.name in names else col.nullable,
                )
                if col.name in names
                else col
                for col in table.columns
            ]

            def undo_columns() -> None:
                table.columns = old_columns

            txn.undo_log.append(undo_columns)
            self._catalog.persist(txn)
            return Result(
                tag="ALTER TABLE",
                notice=f"已添加主键 {index_name}",
            )

        raise NotSupportedError(
            f"不支持的表级约束：{constraint.kind}"
        )

    def _alter_drop_constraint(
        self, stmt: AlterTableDropConstraint, txn: Transaction
    ) -> Result:
        table = self._catalog.get_table(stmt.table)
        name = stmt.name.lower()
        if name in table.foreign_keys:
            old_fks = dict(table.foreign_keys)
            del table.foreign_keys[name]

            def undo_fk() -> None:
                table.foreign_keys = old_fks

            txn.undo_log.append(undo_fk)
            self._catalog.mark_dirty()
            self._catalog.persist(txn)
            return Result(
                tag="ALTER TABLE",
                notice=f"已删除外键约束 {name}",
            )
        if name not in table.checks:
            index = table.indexes.get(name)
            if index is not None:
                if index.constraint_kind is None:
                    raise NotSupportedError(
                        f"{name!r} 是普通索引，不是约束",
                        hint="使用 DROP INDEX 删除普通索引",
                    )
                if index.constraint_kind not in ("PRIMARY", "UNIQUE"):
                    raise NotSupportedError(
                        f"不支持的约束索引类型：{index.constraint_kind!r}"
                    )
                for other in self._catalog.list_tables():
                    for fk in other.foreign_keys.values():
                        if (
                            fk.ref_table == table.name
                            and tuple(fk.ref_columns) == tuple(index.columns)
                        ):
                            raise NotSupportedError(
                                f"约束 {name!r} 仍被外键 {fk.name!r} 依赖",
                                hint="先删除引用该唯一键的外键约束",
                            )
                old_columns = list(table.columns)
                self._catalog.drop_index(txn, name)
                if index.constraint_kind == "PRIMARY":
                    table.columns = [
                        replace(col, primary_key=False)
                        if col.name in index.columns
                        else col
                        for col in table.columns
                    ]

                    def undo_columns() -> None:
                        table.columns = old_columns

                    txn.undo_log.append(undo_columns)
                    self._catalog.mark_dirty()
                    self._catalog.persist(txn)
                    return Result(
                        tag="ALTER TABLE",
                        notice=f"已删除主键约束 {name}",
                    )
                return Result(
                    tag="ALTER TABLE",
                    notice=f"已删除唯一约束 {name}",
                )
            if stmt.if_exists:
                return Result(
                    tag="ALTER TABLE",
                    notice=f"约束 {name} 不存在，已跳过",
                )
            raise BinderError(
                f"约束 {name!r} 不存在",
                hint=(
                    f"可用 CHECK 约束：{', '.join(table.checks) or '无'}；"
                    f"可用外键约束：{', '.join(table.foreign_keys) or '无'}；"
                    f"可用唯一/主键索引："
                    f"{', '.join(n for n, i in table.indexes.items() if i.constraint_kind) or '无'}"
                ),
            )
        old_checks = dict(table.checks)
        del table.checks[name]

        def undo_check() -> None:
            table.checks = old_checks

        txn.undo_log.append(undo_check)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return Result(
            tag="ALTER TABLE",
            notice=f"已删除 CHECK 约束 {name}",
        )

    def _drop_table(self, stmt: DropTable, txn: Transaction) -> Result:
        for name in stmt.names:
            if not self._catalog.has_table(name):
                if stmt.if_exists:
                    continue
                raise TableNotFoundError(f"表 {name!r} 不存在")
            self._catalog.drop_table(txn, name, stmt.cascade)
        return Result(tag="DROP TABLE")

    def _create_index(
        self,
        stmt: CreateIndex,
        txn: Transaction,
        *,
        constraint_kind: Optional[str] = None,
    ) -> Result:
        table = self._catalog.get_table(stmt.table)
        name = stmt.name.lower()
        if name in table.indexes:
            if stmt.if_not_exists:
                return Result(
                    tag="CREATE INDEX",
                    notice=f"索引 {stmt.name.lower()} 已存在，已跳过",
                )
            raise IndexAlreadyExistsError(f"索引 {stmt.name!r} 已存在")
        for cname in stmt.columns:
            table.column(cname)

        idx = IndexDef(
            name=name,
            table=table.name,
            columns=tuple(c.lower() for c in stmt.columns),
            unique=stmt.unique,
            constraint_kind=constraint_kind,
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
        name = stmt.name.lower()
        for table in self._catalog.list_tables():
            idx = table.indexes.get(name)
            if idx is not None and idx.constraint_kind is not None:
                raise NotSupportedError(
                    f"索引 {name!r} 由表约束创建，不能直接 DROP INDEX",
                    hint="使用 ALTER TABLE ... DROP CONSTRAINT 删除约束",
                )
            if idx is not None and idx.unique:
                for other in self._catalog.list_tables():
                    for fk in other.foreign_keys.values():
                        if (
                            fk.ref_table == table.name
                            and tuple(fk.ref_columns) == tuple(idx.columns)
                        ):
                            raise NotSupportedError(
                                f"索引 {name!r} 仍被外键 {fk.name!r} 依赖",
                                hint="先删除引用该唯一键的外键约束",
                            )
        try:
            self._catalog.drop_index(txn, name)
        except Exception:
            if stmt.if_exists:
                return Result(tag="DROP INDEX", notice="索引不存在，已跳过")
            raise
        return Result(tag="DROP INDEX")

    def _snapshot_table_state(self, table, txn: Transaction) -> None:
        """登记 DML 对表/索引根页号的逆操作。

        堆表插入可能分配新页并修改 ``root_page``/``last_page``，索引插入可能
        更换 B+ 树根页；这些字段在 Catalog 对象上原地变化，如果事务回滚只
        撤销页内容而不撤销这些指针，内存目录就会指向已经恢复成空页的页。
        """
        state = (
            table.root_page,
            table.last_page,
            table.row_count_hint,
            {name: idx.root_page for name, idx in table.indexes.items()},
        )

        def undo() -> None:
            table.root_page = state[0]
            table.last_page = state[1]
            table.row_count_hint = state[2]
            for name, root_page in state[3].items():
                idx = table.indexes.get(name)
                if idx is not None:
                    idx.root_page = root_page

        txn.undo_log.append(undo)

    def _reindex(self, stmt: Reindex, txn: Transaction) -> Result:
        """按堆表当前内容重建索引：只保留当前可见的版本，回收稀疏页。"""
        table = self._catalog.get_table(stmt.table)
        self._snapshot_table_state(table, txn)
        rebuilt, entries_total = self._rebuild_indexes(table, txn)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        if rebuilt == 0:
            return Result(tag="REINDEX 0 indexes", notice="该表没有索引")
        return Result(
            affected=entries_total,
            tag=f"REINDEX {rebuilt} indexes ({entries_total} entries)",
        )

    def _rebuild_indexes(self, table, txn: Transaction) -> tuple[int, int]:
        """从堆表当前可见行重建该表的所有索引，返回 (索引数, 条目数)。"""
        heap = HeapFile(self._bp, table, self._txns)
        snapshot = list(heap.scan(txn))
        rebuilt = 0
        entries_total = 0
        for idx in table.indexes.values():
            entries: list[tuple[list, tuple[int, int]]] = []
            for rid, values in snapshot:
                key = self._key_for(table, idx, values)
                if key is not None:
                    entries.append((key, rid))
            tree = self._tree_for(table, idx)
            tree.rebuild(txn, entries)
            idx.root_page = tree.root_page
            rebuilt += 1
            entries_total += len(entries)
        return rebuilt, entries_total

    def _vacuum(self, stmt: Vacuum, txn: Transaction) -> Result:
        """回收死版本。堆页重排会改变 RID，所以随后必须重建索引。"""
        tables = (
            [self._catalog.get_table(stmt.table)]
            if stmt.table
            else self._catalog.list_tables()
        )
        kept = dead = pages = 0
        for table in tables:
            self._snapshot_table_state(table, txn)
            heap = HeapFile(self._bp, table, self._txns)
            k, d, p = heap.vacuum(txn)
            kept += k
            dead += d
            pages += p
            self._rebuild_indexes(table, txn)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return Result(
            affected=dead,
            tag=(
                f"VACUUM {len(tables)} tables ({kept} rows kept, "
                f"{dead} versions reclaimed, {pages} pages rewritten)"
            ),
        )

    def _truncate(self, stmt: Truncate, txn: Transaction) -> Result:
        """清空整表：页链保留但每一页重置为空页，索引随之重建为空。

        ``CASCADE`` 会递归清空所有直接/间接引用该表的子表，避免只清父表后
        留下悬空行。``RESTRICT``（默认）仍拒绝清空被外键引用的表。
        """
        table = self._catalog.get_table(stmt.table)
        if not stmt.cascade:
            for other in self._catalog.list_tables():
                for fk in other.foreign_keys.values():
                    if fk.ref_table == table.name:
                        raise BinderError(
                            f"表 {table.name!r} 被外键 {fk.name!r} 引用，不能直接 TRUNCATE",
                            hint="使用 TRUNCATE ... CASCADE，或先删除引用它的外键",
                        )
            removed = self._truncate_one(table, txn)
            return Result(
                affected=removed,
                tag=f"TRUNCATE TABLE ({removed} rows removed)",
            )

        tables = self._truncate_cascade_plan(table)
        total = sum(self._truncate_one(t, txn) for t in tables)
        return Result(
            affected=total,
            tag=f"TRUNCATE TABLE ({total} rows removed)",
        )

    def _truncate_cascade_plan(self, table: TableDef) -> list[TableDef]:
        """按外键反向依赖收集需要一起清空的表，打破自引用环。"""
        seen = {table.name: table}
        pending = [table]
        while pending:
            parent = pending.pop()
            for other in self._catalog.list_tables():
                if other.name in seen:
                    continue
                if any(
                    fk.ref_table == parent.name
                    for fk in other.foreign_keys.values()
                ):
                    seen[other.name] = other
                    pending.append(other)
        return list(seen.values())

    def _truncate_one(self, table: TableDef, txn: Transaction) -> int:
        self._snapshot_table_state(table, txn)
        heap = HeapFile(self._bp, table, self._txns)
        removed = heap.truncate(txn)
        self._rebuild_indexes(table, txn)
        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        return removed

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

    def _show_views(self) -> Result:
        rows = [
            (name, " ".join(value for _kind, value in tokens))
            for name in self._catalog.view_names()
            for tokens in [self._catalog.get_view(name)]
        ]
        return Result(
            columns=[
                ColumnMeta("view", TypeSpec(DataType.TEXT)),
                ColumnMeta("definition", TypeSpec(DataType.TEXT)),
            ],
            rows=rows,
            tag=f"SHOW {len(rows)}",
        )

    def _explain(self, stmt: Explain) -> Result:
        inner = stmt.statement
        if isinstance(inner, Query):
            plan, cols = self.binder.bind_query(inner)
            if stmt.analyze:
                assert self._txn is not None
                profile: dict[int, list] = {}
                self._profile = profile
                self._active_ctes = dict(self.binder.ctes)
                started = time.perf_counter()
                count = 0
                try:
                    for _row in self._execute(plan, self._txn):
                        count += 1
                finally:
                    self._profile = None
                    self._active_ctes = {}
                elapsed = (time.perf_counter() - started) * 1000
                lines = list(_describe_plan(plan, profile=profile))
                lines.append(
                    f"Execution: {elapsed:.2f} ms, {count} rows"
                )
            else:
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
        self._snapshot_table_state(table, txn)
        heap = HeapFile(self._bp, table, self._txns)
        if stmt.query is not None:
            return self._insert_select(stmt, table, heap, txn)
        if stmt.ctes:
            # 即使 VALUES 不读表，CTE 里的 SQL 也必须被解析/绑定，不能静默跳过
            self.binder.bind_ctes(stmt.ctes)
            self._active_ctes = dict(self.binder.ctes)
        else:
            self._active_ctes = {}

        empty_schema = Schema([])
        schema = Schema(
            [SchemaColumn(table.name, c.name, c.type) for c in table.columns]
        )
        ret_fns, ret_cols = self.binder.bind_returning(stmt.returning, schema)
        conflict_index = self._resolve_conflict_index(table, stmt)
        assignment_fns = (
            self._compile_conflict_assignments(table, stmt.on_conflict_update)
            if stmt.on_conflict_update
            else []
        )
        conflict_where_fn = None
        if stmt.on_conflict_where is not None:
            conflict_where_fn, _ = self.binder.compile(
                stmt.on_conflict_where,
                self._conflict_schema(table),
                allow_agg=False,
            )
        returned: list[tuple] = []
        count = 0

        if stmt.default_values:
            prepared = self._prepare_values(
                table, None, [column.default for column in table.columns]
            )
            delta, returning_row = self._insert_row(
                table,
                heap,
                txn,
                prepared,
                do_nothing=stmt.on_conflict_nothing,
                on_conflict_update=stmt.on_conflict_update,
                conflict_index=conflict_index,
                assignment_fns=assignment_fns,
                conflict_where_fn=conflict_where_fn,
                ret_fns=ret_fns,
            )
            count += delta
            if returning_row is not None:
                returned.append(returning_row)
        else:
            for row_exprs in stmt.rows:
                values: list[Any] = []
                for expr in row_exprs:
                    fn, _ = self.binder.compile(expr, empty_schema, allow_agg=False)
                    values.append(fn(EvalContext(())))

                prepared = self._prepare_values(table, stmt.columns, values)
                delta, returning_row = self._insert_row(
                    table,
                    heap,
                    txn,
                    prepared,
                    do_nothing=stmt.on_conflict_nothing,
                    on_conflict_update=stmt.on_conflict_update,
                    conflict_index=conflict_index,
                    assignment_fns=assignment_fns,
                    conflict_where_fn=conflict_where_fn,
                    ret_fns=ret_fns,
                )
                count += delta
                if returning_row is not None:
                    returned.append(returning_row)

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        self._active_ctes = {}
        return Result(
            columns=ret_cols,
            rows=returned,
            affected=count,
            tag=f"INSERT {count}",
        )

    def _conflict_index_for(
        self, table: TableDef, columns: Optional[list[str]]
    ) -> Optional[IndexDef]:
        """把 ``ON CONFLICT (col, ...)`` 映射到唯一索引。"""
        if columns is None:
            return None
        wanted = list(columns)
        for idx in table.indexes.values():
            if idx.unique and list(idx.columns) == wanted:
                for name in wanted:
                    table.column_index(name)
                return idx
        raise BinderError(
            "ON CONFLICT 的目标没有匹配的唯一索引",
            detail=f"目标列：{', '.join(wanted)}",
            hint="先在该列组合上建 UNIQUE 索引，或省略冲突目标",
        )

    def _conflict_index_for_constraint(
        self, table: TableDef, name: str
    ) -> IndexDef:
        """把 ``ON CONFLICT ON CONSTRAINT name`` 映射到唯一索引。"""
        wanted = name.lower()
        for idx in table.indexes.values():
            if idx.unique and idx.name.lower() == wanted:
                return idx
        available = sorted(idx.name for idx in table.indexes.values() if idx.unique)
        raise BinderError(
            "ON CONFLICT ON CONSTRAINT 没找到唯一约束",
            detail=f"约束名：{name}",
            hint=(
                "可用唯一约束：" + ", ".join(available)
                if available
                else "该表没有唯一约束"
            ),
        )

    def _resolve_conflict_index(
        self, table: TableDef, stmt: Insert
    ) -> Optional[IndexDef]:
        """统一解析三种冲突目标：无目标 / 列组合 / 约束名。"""
        if stmt.conflict_constraint is not None:
            return self._conflict_index_for_constraint(
                table, stmt.conflict_constraint
            )
        if stmt.conflict_columns is not None:
            return self._conflict_index_for(table, stmt.conflict_columns)
        return None

    def _find_conflict_rid(
        self,
        table: TableDef,
        values: list[Any],
        txn: Transaction,
        conflict_index: Optional[IndexDef] = None,
    ) -> Optional[RID]:
        """返回与插入值冲突的已存在根 RID。"""
        indexes = (
            [conflict_index]
            if conflict_index is not None
            else list(table.indexes.values())
        )
        conflict = self._find_unique_conflict(
            table, values, txn, indexes=indexes
        )
        return conflict[1] if conflict is not None else None

    def _conflict_schema(self, table: TableDef) -> Schema:
        """旧行 + ``excluded`` 行的复合 schema，用于 upsert 赋值/WHERE。

        两套列名相同，因此给 Schema 一个默认 qualifier：未限定列名落到
        目标表（旧行），只有 ``excluded.col`` 才访问待插入行。
        """
        old_cols = [
            SchemaColumn(table.name, c.name, c.type) for c in table.columns
        ]
        excluded_cols = [
            SchemaColumn("excluded", c.name, c.type) for c in table.columns
        ]
        return Schema(old_cols + excluded_cols, default_qualifier=table.name)

    def _compile_assignment(
        self, table: TableDef, index: int, expr: Expr, schema: Schema
    ) -> Any:
        """编译 UPDATE / ON CONFLICT 的赋值；DEFAULT 直接取目录中的列默认值。"""
        if isinstance(expr, DefaultValue):
            value = table.columns[index].default
            return lambda _ctx, captured=value: captured
        fn, _ = self.binder.compile(expr, schema, allow_agg=False)
        return fn

    def _compile_conflict_assignments(
        self,
        table: TableDef,
        assignments: Optional[list[tuple[str, Expr]]],
    ) -> list[tuple[int, Any]]:
        if assignments is None:
            return []
        schema = self._conflict_schema(table)
        compiled: list[tuple[int, Any]] = []
        for name, expr in assignments:
            index = table.column_index(name)
            compiled.append(
                (index, self._compile_assignment(table, index, expr, schema))
            )
        return compiled

    def _apply_conflict_update(
        self,
        table: TableDef,
        heap: HeapFile,
        txn: Transaction,
        rid: RID,
        old_values: list[Any],
        excluded_values: list[Any],
        assignment_fns: list[tuple[int, Any]],
    ) -> list[Any]:
        old_tuple = tuple(old_values)
        # 赋值表达式可以引用旧行（默认 qualifier）和 excluded.col；
        # 两者在同一个 tuple 里连续排放，列下标由 _conflict_schema 决定。
        ctx = EvalContext(old_tuple + tuple(excluded_values))
        new_values = list(old_tuple)
        for index, fn in assignment_fns:
            new_values[index] = fn(ctx)
        return self._rewrite_row(table, heap, txn, rid, old_tuple, new_values)

    def _rewrite_row(
        self,
        table: TableDef,
        heap: HeapFile,
        txn: Transaction,
        rid: RID,
        old_values: Iterable[Any],
        new_values: list[Any],
        _updating: Optional[set[tuple[str, RID]]] = None,
    ) -> list[Any]:
        """UPDATE 与 upsert 共用的行重写入口：校验、维护索引、写堆。"""
        old_tuple = tuple(old_values)
        prepared = self._prepare_values(table, None, new_values)
        self._check_checks(table, prepared)
        self._check_foreign_keys(table, prepared, txn)
        updating = _updating if _updating is not None else set()
        ident = (table.name, rid)
        if ident in updating:
            return prepared
        updating.add(ident)
        pending = self._plan_referenced_updates(
            table, old_tuple, prepared, txn, rid, updating
        )

        changed_indexes = [
            idx
            for idx in table.indexes.values()
            if self._key_for(table, idx, old_tuple)
            != self._key_for(table, idx, prepared)
        ]
        for idx in changed_indexes:
            self._check_update_unique(table, idx, old_tuple, prepared, txn, rid)
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
        self._track_deferred_fk_row(table, rid, txn)
        for child, child_rid, child_old, child_new in pending:
            child_heap = HeapFile(self._bp, child, self._txns)
            self._rewrite_row(
                child,
                child_heap,
                txn,
                child_rid,
                child_old,
                child_new,
                _updating=updating,
            )
        return prepared

    def _plan_referenced_updates(
        self,
        table: TableDef,
        old_values: tuple[Any, ...],
        new_values: list[Any],
        txn: Transaction,
        rid: RID,
        updating: set[tuple[str, RID]],
    ) -> list[tuple[TableDef, RID, list[Any], list[Any]]]:
        """收集被引用键变化影响的行；RESTRICT 立即报错，CASCADE 延后重写。"""
        pending: dict[tuple[str, RID], tuple[TableDef, RID, list[Any], list[Any]]] = {}
        for child in self._catalog.list_tables():
            for fk in child.foreign_keys.values():
                if fk.ref_table != table.name:
                    continue
                if all(
                    old_values[table.column_index(name)]
                    == new_values[table.column_index(name)]
                    for name in fk.ref_columns
                ):
                    continue
                old_key = tuple(
                    old_values[table.column_index(name)]
                    for name in fk.ref_columns
                )
                if any(value is None for value in old_key):
                    continue
                child_heap = HeapFile(self._bp, child, self._txns)
                for child_rid, child_values in self._fk_child_rows(
                    child, fk, old_key, txn, child_heap
                ):
                    child_ident = (child.name, child_rid)
                    if child_ident in updating:
                        continue
                    if fk.on_update not in ("CASCADE", "SET NULL", "SET DEFAULT"):
                        if (
                            fk.on_update == "NO ACTION"
                            and self._fk_is_deferred(fk, txn)
                        ):
                            txn.deferred_ref_checks.add(
                                (child.name, fk.name, old_key)
                            )
                            continue
                        raise ForeignKeyViolationError(
                            f"外键约束 {fk.name!r} 阻止更新被引用行",
                            detail=(
                                f"表 {child.name} 中仍有行引用 "
                                f"{table.name}.{fk.ref_columns} = {old_key}"
                            ),
                            hint="先 DELETE/UPDATE 引用行，或使用 ON UPDATE CASCADE/SET NULL/SET DEFAULT",
                        )
                    entry = pending.get(child_ident)
                    if entry is None:
                        child_new = list(child_values)
                        entry = (child, child_rid, child_values, child_new)
                        pending[child_ident] = entry
                    else:
                        child_new = entry[3]
                    for local_name, ref_name in zip(fk.columns, fk.ref_columns):
                        index = child.column_index(local_name)
                        if fk.on_update == "SET NULL":
                            child_new[index] = None
                        elif fk.on_update == "SET DEFAULT":
                            child_new[index] = child.columns[index].default
                        else:
                            child_new[index] = new_values[
                                table.column_index(ref_name)
                            ]
        return list(pending.values())

    def _insert_select(self, stmt: Insert, table, heap, txn: Transaction) -> Result:
        if stmt.ctes:
            plan, out_cols = self.binder.bind_query(
                Query(select=stmt.query, ctes=stmt.ctes)
            )
        else:
            plan, out_cols = self.binder.bind_select(stmt.query)
        target_width = (
            len(stmt.columns) if stmt.columns is not None else len(table.columns)
        )
        if len(out_cols) != target_width:
            raise BinderError(
                "INSERT ... SELECT 的列数与目标列不一致",
                detail=f"SELECT 返回 {len(out_cols)} 列，目标需要 {target_width} 列",
            )

        # 先物化来源：INSERT INTO t SELECT * FROM t 不能边读边写，
        # 否则扫描器会把刚插入的新行也读出来，形成无限增长。
        self._active_ctes = dict(self.binder.ctes) if stmt.ctes else {}
        source_rows = [tuple(row) for row in self._execute(plan, txn)]
        schema = Schema(
            [SchemaColumn(table.name, c.name, c.type) for c in table.columns]
        )
        ret_fns, ret_cols = self.binder.bind_returning(stmt.returning, schema)
        conflict_index = self._resolve_conflict_index(table, stmt)
        assignment_fns = (
            self._compile_conflict_assignments(table, stmt.on_conflict_update)
            if stmt.on_conflict_update
            else []
        )
        conflict_where_fn = None
        if stmt.on_conflict_where is not None:
            conflict_where_fn, _ = self.binder.compile(
                stmt.on_conflict_where,
                self._conflict_schema(table),
                allow_agg=False,
            )
        returned: list[tuple] = []
        count = 0
        for values in source_rows:
            prepared = self._prepare_values(table, stmt.columns, list(values))
            delta, returning_row = self._insert_row(
                table,
                heap,
                txn,
                prepared,
                do_nothing=stmt.on_conflict_nothing,
                on_conflict_update=stmt.on_conflict_update,
                conflict_index=conflict_index,
                assignment_fns=assignment_fns,
                conflict_where_fn=conflict_where_fn,
                ret_fns=ret_fns,
            )
            count += delta
            if returning_row is not None:
                returned.append(returning_row)

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        self._active_ctes = {}
        return Result(
            columns=ret_cols,
            rows=returned,
            affected=count,
            tag=f"INSERT {count}",
        )

    def _insert_row(
        self,
        table: TableDef,
        heap: HeapFile,
        txn: Transaction,
        prepared: list[Any],
        *,
        do_nothing: bool,
        on_conflict_update: bool,
        conflict_index: Optional[IndexDef],
        assignment_fns: list[tuple[int, Any]],
        conflict_where_fn: Optional[Any],
        ret_fns: list[Any],
    ) -> tuple[int, Optional[tuple]]:
        """统一写入一条 INSERT 候选行。

        所有 INSERT（VALUES / SELECT）都必须经过这里，避免两条路径在
        NOT NULL、唯一索引和 ON CONFLICT 处理上各自漂移。
        返回 (affected_delta, returning_row)；DO NOTHING 跳过时返回 (0, None)。
        """
        if do_nothing:
            # 有冲突目标时只跳过该索引上的冲突；其它唯一索引仍必须报错。
            # 没有目标时保持 PostgreSQL 的"任意唯一冲突都跳过"语义。
            if conflict_index is not None:
                conflict = self._find_unique_conflict(
                    table, prepared, txn, indexes=(conflict_index,)
                )
                if conflict is not None:
                    return 0, None
                self._check_constraints(table, prepared, txn)
            else:
                # 唯一冲突是预期内的跳过，因此先跳过唯一检查；NOT NULL 仍要检查。
                self._check_constraints(table, prepared, txn, skip_unique=True)
                if self._has_unique_conflict(table, prepared, txn):
                    return 0, None
        elif on_conflict_update:
            rid = self._find_conflict_rid(table, prepared, txn, conflict_index)
            old_values = heap.get(txn, rid) if rid is not None else None
            if old_values is not None:
                if conflict_where_fn is not None and not _truthy(
                    conflict_where_fn(
                        EvalContext(tuple(old_values) + tuple(prepared))
                    )
                ):
                    return 0, None
                prepared = self._apply_conflict_update(
                    table, heap, txn, rid, old_values, prepared, assignment_fns
                )
                returning_row = (
                    tuple(fn(EvalContext(tuple(prepared))) for fn in ret_fns)
                    if ret_fns
                    else None
                )
                return 1, returning_row
            # 冲突目标上没有可更新行：按普通插入处理，其它唯一索引冲突
            # 会在 _check_constraints 中抛出来。
            self._check_constraints(table, prepared, txn)
        else:
            self._check_constraints(table, prepared, txn)

        self._check_foreign_keys(table, prepared, txn)
        rid = heap.insert(txn, prepared)
        for idx in table.indexes.values():
            tree = self._tree_for(table, idx)
            key = self._key_for(table, idx, prepared)
            if key is None:
                continue
            tree.insert(txn, key, rid)
            idx.root_page = tree.root_page
        self._track_deferred_fk_row(table, rid, txn)
        returning_row = (
            tuple(fn(EvalContext(tuple(prepared))) for fn in ret_fns)
            if ret_fns
            else None
        )
        return 1, returning_row

    def _has_unique_conflict(self, table, values, txn: Transaction) -> bool:
        """ON CONFLICT DO NOTHING 的前置检查：任一唯一索引上已有同样的键。"""
        return self._find_unique_conflict(table, values, txn) is not None

    def _update(self, stmt: Update, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        self._snapshot_table_state(table, txn)
        heap = HeapFile(self._bp, table, self._txns)
        if stmt.ctes:
            self.binder.bind_ctes(stmt.ctes)
            self._active_ctes = dict(self.binder.ctes)
        else:
            self._active_ctes = {}

        assignment_fns: list[tuple[int, Any]] = []
        target_schema = Schema(
            [SchemaColumn(table.name, c.name, c.type) for c in table.columns],
            default_qualifier=table.name,
        )
        schema_columns = list(target_schema.columns)
        source_table = None
        source_rows: list[list[Any]] = []
        if stmt.from_table is not None:
            if stmt.from_table.lower() == table.name:
                raise BinderError(
                    "UPDATE ... FROM 的源表不能与目标表同名",
                    hint="给源表使用不同的表名或先物化到临时表",
                )
            source_table = self._catalog.get_table(stmt.from_table)
            schema_columns.extend(
                SchemaColumn(source_table.name, c.name, c.type)
                for c in source_table.columns
            )
            source_rows = [
                list(values)
                for _rid, values in HeapFile(
                    self._bp, source_table, self._txns
                ).scan(txn)
            ]
        schema = Schema(schema_columns, default_qualifier=table.name)
        for name, expr in stmt.assignments:
            index = table.column_index(name)
            assignment_fns.append(
                (index, self._compile_assignment(table, index, expr, schema))
            )

        where_fn = None
        if stmt.where is not None:
            where_fn, _ = self.binder.compile(stmt.where, schema, allow_agg=False)

        ret_fns, ret_cols = self.binder.bind_returning(
            stmt.returning, target_schema
        )
        returned: list[tuple] = []
        updated = 0
        # 必须先物化目标行：更新会向堆里追加新版本，边扫边写会让扫描器
        # 不断看到刚写进去的新版本（root RID 不变），从而反复更新同一行。
        targets = list(heap.scan(txn))
        for rid, old_values in targets:
            old_tuple = tuple(old_values)
            if source_table is None:
                ctx = EvalContext(old_tuple)
                if where_fn is not None and not _truthy(where_fn(ctx)):
                    continue
            else:
                ctx = None
                for source_values in source_rows:
                    candidate = EvalContext(old_tuple + tuple(source_values))
                    if where_fn is None or _truthy(where_fn(candidate)):
                        ctx = candidate
                        break
                if ctx is None:
                    continue
            new_values = list(old_tuple)
            for index, fn in assignment_fns:
                new_values[index] = fn(ctx)
            prepared = self._rewrite_row(
                table, heap, txn, rid, old_tuple, new_values
            )
            if ret_fns:
                returned.append(
                    tuple(fn(EvalContext(tuple(prepared))) for fn in ret_fns)
                )
            updated += 1

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        self._active_ctes = {}
        return Result(
            columns=ret_cols,
            rows=returned,
            affected=updated,
            tag=f"UPDATE {updated}",
        )

    def _delete(self, stmt: Delete, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        self._snapshot_table_state(table, txn)
        heap = HeapFile(self._bp, table, self._txns)
        if stmt.ctes:
            self.binder.bind_ctes(stmt.ctes)
            self._active_ctes = dict(self.binder.ctes)
        else:
            self._active_ctes = {}

        where_fn = None
        target_schema = Schema(
            [SchemaColumn(table.name, c.name, c.type) for c in table.columns],
            default_qualifier=table.name,
        )
        schema_columns = list(target_schema.columns)
        source_table = None
        source_rows: list[list[Any]] = []
        if stmt.using_table is not None:
            if stmt.using_table.lower() == table.name:
                raise BinderError(
                    "DELETE ... USING 的源表不能与目标表同名",
                    hint="给源表使用不同的表名或先物化到临时表",
                )
            source_table = self._catalog.get_table(stmt.using_table)
            schema_columns.extend(
                SchemaColumn(source_table.name, c.name, c.type)
                for c in source_table.columns
            )
            source_rows = [
                list(values)
                for _rid, values in HeapFile(
                    self._bp, source_table, self._txns
                ).scan(txn)
            ]
        schema = Schema(schema_columns, default_qualifier=table.name)
        if stmt.where is not None:
            where_fn, _ = self.binder.compile(stmt.where, schema, allow_agg=False)

        ret_fns, ret_cols = self.binder.bind_returning(
            stmt.returning, target_schema
        )
        returned: list[tuple] = []
        deleted = 0
        deleted_rids: set[tuple[str, RID]] = set()
        processing: set[tuple[str, RID]] = set()
        deferred: dict[
            tuple[str, RID], tuple[TableDef, RID, list[Any], list[Any]]
        ] = {}
        for rid, values in heap.scan(txn):
            old_tuple = tuple(values)
            if source_table is None:
                ctx = EvalContext(old_tuple)
                if where_fn is not None and not _truthy(where_fn(ctx)):
                    continue
            else:
                matched = False
                for source_values in source_rows:
                    candidate = EvalContext(old_tuple + tuple(source_values))
                    if where_fn is None or _truthy(where_fn(candidate)):
                        matched = True
                        break
                if not matched:
                    continue
            ident = (table.name, rid)
            if ident in deleted_rids or ident in processing:
                continue
            processing.add(ident)
            self._cascade_delete_children(
                table, values, txn, deleted_rids, processing, deferred
            )
            self._delete_index_entries(table, values, txn, rid)
            if ret_fns:
                returned.append(
                    tuple(fn(EvalContext(old_tuple)) for fn in ret_fns)
                )
            heap.delete(txn, rid)
            txn.deferred_fk_rows.discard((table.name, rid))
            processing.discard(ident)
            deleted_rids.add(ident)
            deleted += 1

        for ident, (child, child_rid, child_old, child_new) in deferred.items():
            if ident in deleted_rids:
                continue
            self._snapshot_table_state(child, txn)
            child_heap = HeapFile(self._bp, child, self._txns)
            self._rewrite_row(
                child,
                child_heap,
                txn,
                child_rid,
                child_old,
                child_new,
            )

        self._catalog.mark_dirty()
        self._catalog.persist(txn)
        self._active_ctes = {}
        return Result(
            columns=ret_cols,
            rows=returned,
            affected=deleted,
            tag=f"DELETE {deleted}",
        )

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

    def _find_unique_conflict(
        self,
        table: TableDef,
        values: Iterable[Any],
        txn: Transaction,
        *,
        indexes: Optional[Iterable[IndexDef]] = None,
        ignore_rid: Optional[RID] = None,
    ) -> Optional[tuple[IndexDef, RID]]:
        """返回第一个违反唯一约束的索引和占用者 RID；没有冲突返回 None。

        INSERT、UPDATE、upsert 和 DO NOTHING 都走这一个查询入口，
        避免各路径对"NULL 不进索引""忽略当前行"的理解出现分歧。
        """
        candidates = indexes if indexes is not None else table.indexes.values()
        for idx in candidates:
            if not idx.unique:
                continue
            key = self._key_for(table, idx, values)
            if key is None:
                continue
            rid = self._tree_for(table, idx).lookup_one(txn, key)
            if rid is not None and rid != ignore_rid:
                return idx, rid
        return None

    def _duplicate_key_error(self, idx: IndexDef) -> DuplicateKeyError:
        return DuplicateKeyError(
            f"唯一约束冲突：索引 {idx.name}",
            detail=f"列 {', '.join(idx.columns)} 上已存在相同的值",
        )

    def _check_update_unique(
        self,
        table: TableDef,
        idx: IndexDef,
        old_values: Iterable[Any],
        new_values: Iterable[Any],
        txn: Transaction,
        rid: RID,
    ) -> None:
        """UPDATE/upsert 改变唯一索引键时，先查新键是否被别的行占用。"""
        if not idx.unique:
            return
        old_key = self._key_for(table, idx, old_values)
        new_key = self._key_for(table, idx, new_values)
        if new_key is None or old_key == new_key:
            return
        conflict = self._find_unique_conflict(
            table,
            new_values,
            txn,
            indexes=(idx,),
            ignore_rid=rid,
        )
        if conflict is not None:
            raise self._duplicate_key_error(conflict[0])

    def _fk_is_deferred(self, fk: ForeignKeyDef, txn: Transaction) -> bool:
        if not fk.deferrable:
            return False
        mode = txn.constraint_modes.get(fk.name)
        if mode is not None:
            return mode == "DEFERRED"
        return fk.initially_deferred

    def _set_constraints(
        self, stmt: SetConstraints, txn: Transaction
    ) -> Result:
        all_fks = [
            fk
            for table in self._catalog.tables.values()
            for fk in table.foreign_keys.values()
        ]
        if stmt.all_constraints:
            targets = [fk for fk in all_fks if fk.deferrable]
            names = {fk.name for fk in targets}
            only_names = None
        else:
            names = {name.lower() for name in stmt.names}
            targets = [fk for fk in all_fks if fk.name in names]
            missing = names - {fk.name for fk in targets}
            if missing:
                raise BinderError(
                    f"约束 {sorted(missing)[0]!r} 不存在",
                    hint="可用外键约束："
                    + (", ".join(sorted(fk.name for fk in all_fks)) or "（空）"),
                )
            non_deferrable = [fk for fk in targets if not fk.deferrable]
            if non_deferrable:
                raise NotSupportedError(
                    f"约束 {non_deferrable[0].name!r} 不是 DEFERRABLE",
                    hint="只有声明 DEFERRABLE 的外键可以切换检查时机",
                )
            only_names = names

        if not stmt.deferred:
            self.validate_deferred_foreign_keys(
                txn, only_names=only_names
            )

        mode = "DEFERRED" if stmt.deferred else "IMMEDIATE"
        for fk in targets:
            txn.constraint_modes[fk.name] = mode
        return Result(
            tag="SET CONSTRAINTS",
            notice=f"已将 {len(targets)} 个外键约束设为 {mode}",
        )

    def _track_deferred_fk_row(
        self, table: TableDef, rid: RID, txn: Transaction
    ) -> None:
        if any(
            self._fk_is_deferred(fk, txn)
            for fk in table.foreign_keys.values()
        ):
            txn.deferred_fk_rows.add((table.name, rid))

    def _parent_key_exists(
        self,
        table: TableDef,
        columns: tuple[str, ...],
        key: tuple[Any, ...],
        txn: Transaction,
    ) -> bool:
        """提交时判断被引用键是否仍存在；优先走唯一索引。"""
        heap = HeapFile(self._bp, table, self._txns)
        positions = [table.column_index(name) for name in columns]
        index = next(
            (
                idx
                for idx in table.indexes.values()
                if idx.unique and tuple(idx.columns) == tuple(columns)
            ),
            None,
        )
        if index is not None:
            tree = self._tree_for(table, index)
            for rid in tree.lookup(txn, list(key)):
                values = heap.get(txn, rid)
                if values is not None and all(
                    values[pos] == value
                    for pos, value in zip(positions, key)
                ):
                    return True
        for _rid, values in heap.scan(txn):
            if all(
                values[pos] == value
                for pos, value in zip(positions, key)
            ):
                return True
        return False

    def validate_deferred_foreign_keys(
        self, txn: Transaction, *, only_names: Optional[set[str]] = None
    ) -> None:
        """提交前补检延迟外键，失败由事务层 abort。

        ``only_names`` 用于 ``SET CONSTRAINTS ... IMMEDIATE``：只强制检查指定
        约束；提交时传 ``None``，检查所有延迟外键。
        """
        for table_name, rid in list(txn.deferred_fk_rows):
            try:
                table = self._catalog.get_table(table_name)
            except TableNotFoundError:
                continue
            values = HeapFile(self._bp, table, self._txns).get(txn, rid)
            if values is None:
                continue
            self._check_foreign_keys(
                table, list(values), txn, force=True, only_names=only_names
            )

        for child_name, fk_name, key in list(txn.deferred_ref_checks):
            if only_names is not None and fk_name not in only_names:
                continue
            try:
                child = self._catalog.get_table(child_name)
            except TableNotFoundError:
                continue
            fk = child.foreign_keys.get(fk_name)
            if fk is None:
                continue
            try:
                ref_table = self._catalog.get_table(fk.ref_table)
            except TableNotFoundError:
                continue
            if self._parent_key_exists(
                ref_table, fk.ref_columns, key, txn
            ):
                continue
            heap = HeapFile(self._bp, child, self._txns)
            for _rid, _values in self._fk_child_rows(
                child, fk, key, txn, heap
            ):
                raise ForeignKeyViolationError(
                    f"外键约束 {fk.name!r} 在提交时仍被引用",
                    detail=(
                        f"表 {child.name} 中仍有行引用 "
                        f"{ref_table.name}.{fk.ref_columns} = {key}"
                    ),
                    hint="先删除/更新引用行，或在同一事务中恢复被引用键",
                )

    def _check_foreign_keys(
        self,
        table: TableDef,
        values: list[Any],
        txn: Transaction,
        *,
        force: bool = False,
        only_names: Optional[set[str]] = None,
    ) -> None:
        """MATCH SIMPLE：任一本地列为 NULL 就跳过；MATCH FULL 要求全空或全非空。

        ``DEFERRABLE INITIALLY DEFERRED``（或当前事务被 ``SET CONSTRAINTS``
        改成延迟）的外键在 DML 路径跳过检查，由提交前的
        ``validate_deferred_foreign_keys`` 用 ``force=True`` 统一补检。
        """
        for fk in table.foreign_keys.values():
            if only_names is not None and fk.name not in only_names:
                continue
            if not force and self._fk_is_deferred(fk, txn):
                continue
            local_key = tuple(
                values[table.column_index(name)] for name in fk.columns
            )
            if fk.match_type == "FULL":
                if all(value is None for value in local_key):
                    continue
                if any(value is None for value in local_key):
                    raise ForeignKeyViolationError(
                        f"外键约束 {fk.name!r} 的 MATCH FULL 不允许部分 NULL",
                        detail=(
                            f"表 {table.name} 的 {fk.columns} 要么全为 NULL，"
                            "要么全部非 NULL"
                        ),
                        hint="补齐所有外键列，或将它们全部置为 NULL",
                    )
            elif any(value is None for value in local_key):
                continue
            ref_table = self._catalog.get_table(fk.ref_table)
            ref_positions = [ref_table.column_index(name) for name in fk.ref_columns]
            found = False
            if ref_table.name == table.name:
                # 自引用：候选行本身也参与匹配，允许 ``(id, parent_id) = (1, 1)``
                # 这种 PostgreSQL 接受的自引用行。
                candidate_key = tuple(
                    values[pos] for pos in ref_positions
                )
                found = candidate_key == local_key
            ref_heap = HeapFile(self._bp, ref_table, self._txns)
            # 被引用列一定有 UNIQUE / PRIMARY 索引；优先用索引点查，
            # 避免每次写入都扫描整张被引用表。索引项可能指向已不可见的
            # 旧版本，所以仍以 heap.get 的可见性结果为准；点查未命中时
            # 再退回全表扫描，保证索引异常时也不会漏判。
            ref_index = next(
                (
                    idx
                    for idx in ref_table.indexes.values()
                    if idx.unique
                    and tuple(idx.columns) == tuple(fk.ref_columns)
                ),
                None,
            )
            if ref_index is not None:
                tree = self._tree_for(ref_table, ref_index)
                for rid in tree.lookup(txn, list(local_key)):
                    ref_values = ref_heap.get(txn, rid)
                    if ref_values is not None and all(
                        ref_values[pos] == value
                        for pos, value in zip(ref_positions, local_key)
                    ):
                        found = True
                        break
            if not found:
                for _rid, ref_values in ref_heap.scan(txn):
                    if all(
                        ref_values[pos] == value
                        for pos, value in zip(ref_positions, local_key)
                    ):
                        found = True
                        break
            if not found:
                raise ForeignKeyViolationError(
                    f"外键约束 {fk.name!r} 违反",
                    detail=(
                        f"表 {table.name} 的 {fk.columns} 找不到 "
                        f"{ref_table.name}.{fk.ref_columns} 中的对应行"
                    ),
                    hint="先插入/更新被引用行，或检查外键列的值",
                )

    def _delete_index_entries(
        self, table: TableDef, values: list[Any], txn: Transaction, rid: RID
    ) -> None:
        for idx in table.indexes.values():
            key = self._key_for(table, idx, values)
            if key is not None:
                self._tree_for(table, idx).delete(txn, key, rid)

    def _fk_child_rows(
        self,
        child: TableDef,
        fk: ForeignKeyDef,
        key: tuple[Any, ...],
        txn: Transaction,
        child_heap: HeapFile,
    ) -> Iterator[tuple[RID, list[Any]]]:
        """按本地外键列查找子行；有精确匹配索引时走点查，否则全表扫描。"""
        child_index = next(
            (
                idx
                for idx in child.indexes.values()
                if tuple(idx.columns) == tuple(fk.columns)
            ),
            None,
        )
        if child_index is not None:
            tree = self._tree_for(child, child_index)
            for rid in tree.lookup(txn, list(key)):
                child_values = child_heap.get(txn, rid)
                if child_values is None:
                    continue
                if all(
                    child_values[child.column_index(name)] == value
                    for name, value in zip(fk.columns, key)
                ):
                    yield rid, child_values
            return
        for rid, child_values in child_heap.scan(txn):
            child_key = tuple(
                child_values[child.column_index(name)] for name in fk.columns
            )
            if any(value is None for value in child_key):
                continue
            if child_key == key:
                yield rid, child_values

    def _cascade_delete_children(
        self,
        table: TableDef,
        values: list[Any],
        txn: Transaction,
        deleted: set[tuple[str, RID]],
        processing: set[tuple[str, RID]],
        deferred: dict[tuple[str, RID], tuple[TableDef, RID, list[Any], list[Any]]],
    ) -> None:
        """按 ON DELETE 动作递归处理子行；SET DEFAULT 延后到父行删除后执行。"""
        for child in self._catalog.list_tables():
            for fk in child.foreign_keys.values():
                if fk.ref_table != table.name:
                    continue
                key = tuple(
                    values[table.column_index(name)] for name in fk.ref_columns
                )
                if any(value is None for value in key):
                    continue
                child_heap = HeapFile(self._bp, child, self._txns)
                matches: list[tuple[RID, list[Any]]] = []
                for rid, child_values in self._fk_child_rows(
                    child, fk, key, txn, child_heap
                ):
                    ident = (child.name, rid)
                    if ident in deleted or ident in processing:
                        continue
                    matches.append((rid, child_values))
                if not matches:
                    continue
                if fk.on_delete == "SET NULL":
                    self._snapshot_table_state(child, txn)
                    for child_rid, child_values in matches:
                        ident = (child.name, child_rid)
                        if ident in deleted or ident in processing:
                            continue
                        child_new = list(child_values)
                        for name in fk.columns:
                            child_new[child.column_index(name)] = None
                        self._rewrite_row(
                            child,
                            child_heap,
                            txn,
                            child_rid,
                            child_values,
                            child_new,
                            _updating=set(processing),
                        )
                    continue
                if fk.on_delete == "SET DEFAULT":
                    for child_rid, child_values in matches:
                        ident = (child.name, child_rid)
                        if ident in deleted or ident in processing:
                            continue
                        entry = deferred.get(ident)
                        if entry is None:
                            child_new = list(child_values)
                            entry = (child, child_rid, child_values, child_new)
                            deferred[ident] = entry
                        else:
                            child_new = entry[3]
                        for name in fk.columns:
                            index = child.column_index(name)
                            child_new[index] = child.columns[index].default
                    continue
                if fk.on_delete != "CASCADE":
                    if (
                        fk.on_delete == "NO ACTION"
                        and self._fk_is_deferred(fk, txn)
                    ):
                        txn.deferred_ref_checks.add(
                            (child.name, fk.name, key)
                        )
                        continue
                    raise ForeignKeyViolationError(
                        f"外键约束 {fk.name!r} 阻止删除被引用行",
                        detail=(
                            f"表 {child.name} 中仍有行引用 "
                            f"{table.name}.{fk.ref_columns} = {key}"
                        ),
                        hint="先 DELETE/UPDATE 引用行，或使用 ON DELETE CASCADE/SET NULL/SET DEFAULT",
                    )
                for child_rid, child_values in matches:
                    ident = (child.name, child_rid)
                    if ident in deleted or ident in processing:
                        continue
                    processing.add(ident)
                    self._snapshot_table_state(child, txn)
                    self._cascade_delete_children(
                        child, child_values, txn, deleted, processing, deferred
                    )
                    self._delete_index_entries(child, child_values, txn, child_rid)
                    child_heap.delete(txn, child_rid)
                    txn.deferred_fk_rows.discard((child.name, child_rid))
                    processing.discard(ident)
                    deleted.add(ident)

    def _check_referenced_by(
        self,
        table: TableDef,
        values: list[Any],
        txn: Transaction,
        *,
        skip_rid: Optional[RID] = None,
    ) -> None:
        """RESTRICT：被其他表（或自身）的外键引用的键不能被删除/改掉。"""
        for ref_table in self._catalog.list_tables():
            for fk in ref_table.foreign_keys.values():
                if fk.ref_table != table.name:
                    continue
                key = tuple(
                    values[table.column_index(name)] for name in fk.ref_columns
                )
                if any(value is None for value in key):
                    continue
                ref_heap = HeapFile(self._bp, ref_table, self._txns)
                for rid, child_values in ref_heap.scan(txn):
                    if (
                        ref_table.name == table.name
                        and skip_rid is not None
                        and rid == skip_rid
                    ):
                        continue
                    child_key = tuple(
                        child_values[ref_table.column_index(name)]
                        for name in fk.columns
                    )
                    if any(value is None for value in child_key):
                        continue
                    if child_key == key:
                        raise ForeignKeyViolationError(
                            f"外键约束 {fk.name!r} 阻止修改/删除被引用行",
                            detail=(
                                f"表 {ref_table.name} 中仍有行引用 "
                                f"{table.name}.{fk.ref_columns} = {key}"
                            ),
                            hint="先 DELETE/UPDATE 引用行，或不要改被引用键",
                        )

    def _check_constraints(
        self,
        table: TableDef,
        values: list[Any],
        txn: Transaction,
        *,
        skip_unique: bool = False,
    ) -> None:
        self._check_checks(table, values)
        if skip_unique:
            # ON CONFLICT DO NOTHING：唯一冲突由调用方判断并跳过，
            # 但 NOT NULL / CHECK 之类的约束仍然必须报错。
            return
        conflict = self._find_unique_conflict(table, values, txn)
        if conflict is not None:
            raise self._duplicate_key_error(conflict[0])

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


def _sort_key(value: Any, descending: bool, nulls_first: Optional[bool]):
    # None 使用 PostgreSQL 默认：ASC 时 NULL 最后，DESC 时 NULL 最前。
    if nulls_first is None:
        nulls_first = descending
    null_rank = 1 if nulls_first == descending else 0
    if value is None:
        return (null_rank, None)
    return (1 - null_rank, value)


def _describe_plan(
    node: PlanNode,
    depth: int = 0,
    profile: Optional[dict[int, list]] = None,
) -> Iterator[str]:
    prefix = "  " * depth
    line = prefix + node.describe()
    if profile is not None:
        entry = profile.get(id(node))
        if entry is None:
            line += " (never executed)"
        else:
            total_ms, calls, rows = entry
            line += (
                f" (actual time={total_ms / calls:.2f} ms rows={rows} "
                f"loops={calls})"
            )
    yield line
    for child in node.children():
        yield from _describe_plan(child, depth + 1, profile)


def _to_column(col_def):
    from ..catalog.schema import Column
    from ..sql.ast import Literal

    default = None
    if col_def.default is not None:
        if not isinstance(col_def.default, Literal):
            raise NotSupportedError(
                "DEFAULT 目前只支持字面常量",
                hint="默认值不能引用列或函数",
            )
        value = col_def.default.value
        if value is not None:
            try:
                value = col_def.type.coerce(value)
            except ValueError as exc:
                raise TypeMismatchError(
                    f"列 {col_def.name} 的默认值类型不匹配",
                    detail=str(exc),
                ) from exc
        default = value

    return Column(
        name=col_def.name.lower(),
        type=col_def.type,
        nullable=col_def.nullable,
        primary_key=col_def.primary_key,
        default=default,
    )


__all__ = ["Executor", "Result", "ColumnMeta"]
