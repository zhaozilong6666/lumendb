"""执行引擎：把绑定后的计划跑成结果，以及 DDL / DML 的实际落地。

火山模型（Volcano / iterator model）：每个计划节点是一个生成器，一次产出
一行，上游 `next()` 驱动下游。优点是实现简单、内存占用与数据量无关；缺点
是每行有一次函数调用开销。对这个规模的项目，简单可读比几微秒的常数更重要。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import random
import re
import time
import weakref
from collections import Counter
from typing import Any, Iterable, Iterator, Optional

from ..catalog.catalog import Catalog
from ..catalog.schema import (
    CheckDef,
    Column,
    ColumnStats,
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
    AlterTableAlterConstraint,
    AlterTableAlterDefault,
    AlterTableAlterNullability,
    AlterTableAlterType,
    AlterTableDropColumn,
    AlterTableDropConstraint,
    AlterTableRenameColumn,
    AlterTableRenameConstraint,
    AlterTableRenameTable,
    AlterTableValidateConstraint,
    Analyze,
    BinaryOp,
    ColumnRef,
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
    Literal,
    Query,
    Reindex,
    SetConstraints,
    Vacuum,
    Truncate,
    ShowCatalogVersions,
    ShowColumns,
    ShowCommitLog,
    ShowHistoryWindow,
    ShowStatistics,
    ShowTables,
    ShowViews,
    Statement,
    TableConstraint,
    Update,
    render_expr,
)
from ..storage.btree import BPlusTree, KeyCodec
from ..storage.bufferpool import BufferPoolManager
from ..storage.heap import HeapFile, VACUUM_ONLINE_MAX_VERSIONS
from ..storage.history import HistoryWindow
from ..storage.page import INVALID_PAGE_ID
from ..storage.record import RID
from ..storage.recovery import RecoveryManager
from ..storage.lock import (
    LockManager,
    LockMode,
    catalog_resource,
    ddl_resource,
    key_resource,
    row_resource,
    writer_resource,
)
from ..storage.txn import Transaction, TransactionManager
from ..storage.wal import WAL

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: 不需要写者锁的语句：纯读、事务/会话控制，以及只读目录查询。
#: ``SET CONSTRAINTS`` 只改事务内存状态，``CHECKPOINT`` 由显式调用方负责。
_NON_WRITING_STATEMENTS = frozenset(
    {
        "Query",
        "Select",
        "Explain",
        "ShowTables",
        "ShowViews",
        "ShowCatalogVersions",
        "ShowColumns",
        "ShowCommitLog",
        "ShowHistoryWindow",
        "ShowStatistics",
        "SetConstraints",
        "Checkpoint",
    }
)
from ..plan.binder import AggregateAccumulatorGroup, Binder, Schema, SchemaColumn
from ..plan.nodes import (
    _CURRENT_OUTER,
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


#: 会改变 schema 形状（列、索引、视图、约束）的语句。执行前统一抬高
#: ``Catalog.schema_version``，让所有已绑定计划失效。宁可让失败的 DDL 也
#: 多清一次缓存，也不能让旧计划活到新 schema 上。
_SCHEMA_CHANGING_STATEMENTS = {
    CreateTable,
    CreateTableAs,
    CreateView,
    DropView,
    DropTable,
    CreateIndex,
    DropIndex,
    Reindex,
    Truncate,
    AlterTableAddColumn,
    AlterTableDropColumn,
    AlterTableAlterType,
    AlterTableAlterNullability,
    AlterTableAlterDefault,
    AlterTableAddConstraint,
    AlterTableDropConstraint,
    AlterTableAlterConstraint,
    AlterTableValidateConstraint,
    AlterTableRenameTable,
    AlterTableRenameColumn,
    AlterTableRenameConstraint,
}


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
        locks: Optional[LockManager] = None,
        read_only: bool = False,
        concurrent_dml: bool = False,
        vacuum_online_max_versions: int = VACUUM_ONLINE_MAX_VERSIONS,
        history: Optional[HistoryWindow] = None,
    ) -> None:
        self._bp = buffer_pool
        self._catalog = catalog
        self._txns = txns
        self._wal = wal
        #: 历史读水位；直接构造 Executor 的旧测试可以传 None，此时 AS OF
        #: 会明确报"未接入历史窗口"，而不是退回当前读。
        self._history = history
        self._locks = locks if locks is not None else LockManager()
        #: 写并发模式：DML 不拿数据库写者锁，由行/键锁 + 页 latch 保护。
        self._concurrent_dml = concurrent_dml
        #: 在线 VACUUM 的物理版本上限；由 Database 下传，便于按内存预算配置。
        self._vacuum_online_max_versions = max(1, int(vacuum_online_max_versions))
        #: 当前语句是否是 DDL/schema 变更（目录锁要持有到事务结束、页回滚
        #: 必须走物理路径）。run() 每条语句都重设。
        self._schema_statement = False
        #: 只读副本标记。必须在执行器入口再拦一次：线协议会话直接调用
        #: ``Executor.run``，绕过了 ``Database._run`` 里的只读检查。
        self._read_only = read_only
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
        #: 已绑定计划缓存：``(id(stmt), schema_version)`` -> (弱引用, 计划, 列, CTE)。
        #: 只缓存无参数语句：``$n`` 在绑定阶段被折成字面量，键里必须含参数，
        #: 而参数每次都变，缓存没有意义。弱引用保证 AST 被解析缓存淘汰后不会
        #: 把新对象的 ``id`` 误当成旧计划。
        self._plan_cache: dict[tuple[int, int], tuple] = {}
        self._plan_cache_limit = 256
        #: B+ 树内部页解码缓存（跨语句共享）：(页号, pageLSN, 页版本号) -> (keys, children)。
        #: DDL 改变 schema_version 时整体清空：页号可能被新索引复用。
        self._internal_page_cache: dict[tuple[int, int, int], tuple] = {}
        self._internal_cache_version = -1
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
        self._schema_statement = type(stmt) in _SCHEMA_CHANGING_STATEMENTS
        if self._schema_statement and self._history is not None:
            # 当前 schema 只能安全解码"DDL 之后"的数据页；把历史下界抬到
            # DDL 开始前的 WAL 位置，AS OF 不会拿新 schema 解旧页。
            self._history.advance(self._wal.next_lsn)
        if txn.capture_row_undo and self._schema_statement:
            raise NotSupportedError(
                "逻辑保存点事务未升级，不能执行 DDL/TRUNCATE",
                detail="结构变更依赖页原像回滚，绕过 Database 直接执行会破坏行 undo 的 RID/槽位假设",
                hint="通过 Database/线协议执行以升级成物理保存点，或改用 --physical-savepoints",
            )
        if self._read_only and type(stmt).__name__ not in _NON_WRITING_STATEMENTS:
            raise NotSupportedError(
                "只读副本拒绝写入",
                detail=f"语句 {type(stmt).__name__} 不是只读语句",
                hint="SELECT / EXPLAIN / SHOW 可以执行，写操作需要在主库完成",
            )
        self._acquire_writer_lock(stmt, txn)
        if type(stmt) in _SCHEMA_CHANGING_STATEMENTS:
            # DDL 可能替换表/索引对象、改列形状；绑定计划缓存按版本号失效。
            self._catalog.bump_schema()
        self._params = list(params) if params else []
        self._recursive_rows = {}
        # Binder 保存 CTE 作用域和参数；每条语句必须从干净的作用域开始，
        # 否则上一条 WITH 的 CTE 会泄漏到下一句（曾经确实泄漏过）。
        self.binder = self._new_binder()
        self.binder.params = self._params
        # DDL 必须能真正撤回（TRUNCATE/DROP 会在原地清空堆页），所以在这条
        # schema 语句执行期间强制关闭"逻辑回滚"；DML 仍走逻辑路径。
        _physical_undo = self._concurrent_dml and self._schema_statement
        if _physical_undo:
            self._bp._undo_override.value = False
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
            if isinstance(stmt, AlterTableAlterConstraint):
                return self._alter_alter_constraint(stmt, txn)
            if isinstance(stmt, AlterTableDropConstraint):
                return self._alter_drop_constraint(stmt, txn)
            if isinstance(stmt, AlterTableValidateConstraint):
                return self._alter_validate_constraint(stmt, txn)
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
            if isinstance(stmt, Analyze):
                return self._analyze(stmt, txn)
            if isinstance(stmt, Reindex):
                return self._reindex(stmt, txn)
            if isinstance(stmt, Vacuum):
                return self._vacuum(stmt, txn)
            if isinstance(stmt, Truncate):
                return self._truncate(stmt, txn)
            if isinstance(stmt, SetConstraints):
                return self._set_constraints(stmt, txn)
            if isinstance(stmt, ShowTables):
                return self._show_tables(stmt)
            if isinstance(stmt, ShowViews):
                return self._show_views(stmt)
            if isinstance(stmt, ShowCatalogVersions):
                return self._show_catalog_versions()
            if isinstance(stmt, ShowCommitLog):
                return self._show_commit_log(stmt)
            if isinstance(stmt, ShowHistoryWindow):
                return self._show_history_window(stmt)
            if isinstance(stmt, ShowColumns):
                return self._show_columns(stmt)
            if isinstance(stmt, ShowStatistics):
                return self._show_statistics(stmt)
            if isinstance(stmt, Explain):
                return self._explain(stmt)
            if isinstance(stmt, Statement) and type(stmt).__name__ == "Checkpoint":
                RecoveryManager(
                    self._disk, self._wal, self._bp, self._txns, self._history
                ).checkpoint()
                return Result(tag="CHECKPOINT")
            raise NotSupportedError(
                f"语句 {type(stmt).__name__} 尚未实现",
                hint="BEGIN / COMMIT / ROLLBACK 由会话层处理",
            )
        finally:
            if _physical_undo:
                self._bp._undo_override.value = None
            self._txn = None
            if type(stmt) in _SCHEMA_CHANGING_STATEMENTS:
                # DDL 结束（无论成败）后页号可能被复用或替换，内部页解码缓存
                # 整体作废。只靠 `_tree_for` 里的版本比对不够：索引构建过程中
                # 缓存已经被喂进新条目、版本号也被拉到新值，事后就不会再清。
                self._internal_page_cache.clear()
                self._internal_cache_version = self._catalog.schema_version

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def _acquire_writer_lock(self, stmt: Statement, txn: Transaction) -> None:
        """写并发的入口：DML 只由行/键锁保护，其它写语句拿 DDL 锁。

        分配与顺序：

        * **DML**（INSERT/UPDATE/DELETE）：**不再**拿全库写者锁。它们要保护的
          冲突（同一行、同一唯一键、父子行的引用）由 `row_resource` /
          `key_resource` 在真正写之前逐条申请；
        * **DDL 与维护语句**（建删表/索引/视图、ALTER、VACUUM、TRUNCATE、
          REINDEX）：拿 `ddl_resource()` 的 X 锁，持到事务结束——schema 形状
          变化不能与另一个 DDL 并发，也不能与"正在跑的 DML"并发；
        * 目录保存另有 `catalog_resource()` 的短锁（见 `_persist_catalog`）。

        全部走 ``try_acquire`` 快速失败（40001）：语句级串行化还在，阻塞等锁
        会造出"持语句锁等行锁、持行锁等语句锁"的环，而等待图看不见这种环。
        """
        if type(stmt).__name__ in _NON_WRITING_STATEMENTS:
            return
        if isinstance(stmt, (Insert, Update, Delete)):
            return
        self._acquire_or_fail(
            txn,
            ddl_resource(),
            LockMode.EXCLUSIVE,
            "另一个事务正在执行 DDL / 维护语句",
            "等它 COMMIT / ROLLBACK 后重试；schema 变更不能并发",
        )

    def _acquire_or_fail(
        self,
        txn: Transaction,
        resource: tuple,
        mode: LockMode,
        message: str,
        hint: str,
    ) -> None:
        """申请一把锁；拿不到就 40001（不等待）。"""
        if self._locks.try_acquire(txn.xid, resource, mode):
            return
        holder = next(
            (x for x in self._locks.holders(resource) if x != txn.xid), None
        )
        from ..errors import SerializationError

        raise SerializationError(
            message,
            detail=(
                f"resource={resource[0]} holder_xid={holder} "
                f"waiter_xid={txn.xid}"
            ),
            hint=hint,
        )

    def _lock_row(
        self,
        table: TableDef,
        rid: RID,
        txn: Transaction,
        *,
        wait: bool = False,
    ) -> None:
        """给一行拿 X 锁（改/删之前）。

        ``wait=True`` 只给并发 DML 的自动提交 UPDATE 用：语句没有语句级写
        锁，行锁是唯一冲突点，阻塞等到前一个写者结束才能做锁后重读。显式
        事务仍然快速失败，避免把事务快照冻结在等锁前的旧版本上。
        """
        resource = row_resource(table.name, rid)
        if wait:
            self._locks.acquire(txn.xid, resource, LockMode.EXCLUSIVE)
            return
        self._acquire_or_fail(
            txn,
            resource,
            LockMode.EXCLUSIVE,
            f"行 {table.name}{tuple(rid)} 正被另一个事务修改",
            "重试事务；并发改同一行在快照隔离下必须失败",
        )

    def _lock_parent_row(self, table: TableDef, rid: RID, txn: Transaction) -> None:
        """外键校验命中父行后拿 S 锁（等价 PostgreSQL 的 FOR KEY SHARE）。

        没有它：T1 删父行、T2 插子行可以"都成功"，提交后就是悬空引用。
        """
        self._acquire_or_fail(
            txn,
            row_resource(table.name, rid),
            LockMode.SHARED,
            f"被引用行 {table.name}{tuple(rid)} 正被另一个事务修改",
            "重试事务；外键校验期间父行不能改/删",
        )

    def _lock_key(
        self, table: TableDef, idx: IndexDef, key: list[Any], txn: Transaction
    ) -> None:
        """给唯一索引的一个键拿 X 锁（插/删索引项之前）。"""
        if not idx.unique or key is None:
            return
        self._acquire_or_fail(
            txn,
            key_resource(table.name, idx.name, tuple(key)),
            LockMode.EXCLUSIVE,
            f"唯一键 {table.name}.{idx.name}{tuple(key)} 正被另一个事务修改",
            "重试事务；唯一性判断必须等对方提交/回滚",
        )

    def _persist_catalog(self, txn: Transaction) -> None:
        """目录保存：短锁 + 整份落盘（目录是单根指针 + 页链）。

        并发写模式下 DML 的保存用**阻塞获取、保存完立即释放**：否则每个
        发生过分裂的写事务会把目录锁握到提交，别的写者只能收 40001。
        DDL 仍然持锁到事务结束。
        """
        hold = not self._concurrent_dml or txn.catalog_hold
        if self._concurrent_dml and not hold:
            self._locks.acquire(txn.xid, catalog_resource(), LockMode.EXCLUSIVE)
        else:
            self._acquire_or_fail(
                txn,
                catalog_resource(),
                LockMode.EXCLUSIVE,
                "另一个事务正在保存目录",
                "重试事务；目录保存是整链重写，必须独占",
            )
        if self._schema_statement:
            txn.catalog_hold = True
            hold = True
        try:
            # 并发 DML 的目录页不能物理回滚：A 保存目录后回滚，旧镜像会把
            # B 随后保存的新根一起抹掉，重启后整棵新子树失联。
            if (
                self._concurrent_dml
                and not self._schema_statement
                and not txn.physical_savepoint
            ):
                with self._bp.logical_undo_scope():
                    self._catalog.mark_dirty()
                    self._catalog.persist(txn)
            else:
                self._catalog.mark_dirty()
                self._catalog.persist(txn)
        finally:
            if not hold:
                self._locks.release(txn.xid, catalog_resource())

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
        cached = self._plan_from_cache(stmt)
        if cached is None:
            plan, out_cols = self.binder.bind_query(stmt)
            ctes = dict(self.binder.ctes)
            if not self._params:
                self._remember_plan(stmt, plan, out_cols, ctes)
        else:
            plan, out_cols, ctes = cached
        width = len(out_cols)
        self._active_ctes = dict(ctes)
        try:
            materialized = [tuple(r)[:width] for r in self._execute(plan, txn)]
        finally:
            self._active_ctes = {}
        return Result(
            columns=[ColumnMeta(c.name, c.type) for c in out_cols],
            rows=materialized,
            tag=f"SELECT {len(materialized)}",
        )

    def _plan_from_cache(self, stmt: Query):
        key = (id(stmt), self._catalog.schema_version)
        entry = self._plan_cache.get(key)
        if entry is None:
            return None
        ref, plan, cols, ctes = entry
        if ref() is not stmt:
            # AST 被解析缓存淘汰后又释放，id 可能被新对象复用：绝不认错。
            self._plan_cache.pop(key, None)
            return None
        return plan, cols, ctes

    def _remember_plan(self, stmt: Query, plan, cols, ctes) -> None:
        try:
            ref = weakref.ref(stmt)
        except TypeError:  # pragma: no cover - AST 目前都支持弱引用
            return
        if len(self._plan_cache) >= self._plan_cache_limit:
            # 按插入顺序淘汰一条，而不是 clear() 全清：常量各不相同的点查
            # 会轮询几百条 SQL，全清会让每一轮都重新绑定所有语句。
            self._plan_cache.pop(next(iter(self._plan_cache)))
        self._plan_cache[(id(stmt), self._catalog.schema_version)] = (
            ref, plan, cols, ctes,
        )

    def _new_binder(self) -> Binder:
        binder = Binder(self._catalog, self._params, history=self._history)
        binder.subquery_runner = self._run_subquery
        return binder

    def _run_subquery(
        self,
        select,
        outer_ctx: Optional[EvalContext],
        outer_schemas: list[Schema],
    ):
        # 子查询可以引用外层列。绑定阶段把外层 schema 链交给新 Binder；
        # 执行阶段把外层行挂到 contextvar，计划内所有 EvalContext 都会
        # 继承它，从而沿 parent 链读取对应层级的列。
        binder = self._new_binder()
        binder.outer_schemas = list(outer_schemas)
        binder.ctes = dict(self._active_ctes or self.binder.ctes)
        plan, _ = binder.bind_select(select)
        token = _CURRENT_OUTER.set(outer_ctx)
        try:
            return list(self._execute(plan, self._require_txn()))
        finally:
            _CURRENT_OUTER.reset(token)

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
            if values is None:
                continue
            # 索引项可能陈旧（删除/更新后不摘项，等 VACUUM 回收）：以堆里的
            # 当前版本为准复核键，否则会把已经改走键的行当成命中返回。
            if self._key_for(table, index, values) != list(node.key):
                continue
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
        prefix = len(node.index_columns)
        for key, rid in tree.scan_range(
            prefix,
            low=node.low,
            high=node.high,
            low_inclusive=node.low_inclusive,
            high_inclusive=node.high_inclusive,
        ):
            values = heap.get(txn, rid)
            if values is None:
                continue
            row_key = self._key_for(table, index, values)
            if row_key is None or tuple(row_key) != tuple(key[:prefix]):
                continue  # 陈旧索引项：这一行的当前键已不在该范围内
            yield tuple(values)

    def _scan(self, node: Scan, txn: Transaction) -> Iterator[tuple]:
        table = self._catalog.get_table(node.table)
        heap = HeapFile(self._bp, table, self._txns)
        if node.as_of_lsn is not None:
            yield from self._scan_as_of(node, table, heap)
            return
        for _rid, values in heap.scan(txn):
            yield tuple(values)

    def _scan_as_of(
        self, node: Scan, table, heap: HeapFile
    ) -> Iterator[tuple]:
        """执行一次历史堆扫描；窗口不足时明确拒绝，不退回当前读。"""
        assert node.as_of_lsn is not None
        if self._history is None:
            raise NotSupportedError(
                "当前执行器没有接入历史读窗口",
                detail="AS OF 需要 checkpoint / VACUUM / DDL 水位",
                hint="通过 Database 打开数据库后再使用 AS OF",
            )
        target = int(node.as_of_lsn)
        floor = self._history.floor_lsn
        if target < floor:
            raise BinderError(
                "AS OF 目标早于可用的历史窗口",
                detail=(
                    f"目标 LSN={target}，历史下界 LSN={floor}；"
                    "checkpoint/VACUUM/DDL 之后更早的版本不再保证可读"
                ),
                hint="用 SHOW HISTORY WINDOW 查看下界，换一个不早于下界的目标点",
            )
        entries = dict(self._txns.commit_log())
        if not entries:
            raise NotSupportedError(
                "当前 WAL 窗口还没有可用的提交点",
                detail="提交账本为空，无法定位任何历史目标",
                hint="先执行一条写语句并提交，再用 SHOW COMMIT LOG 选择 LSN",
            )
        latest = max(entries.values())
        if target > latest:
            raise BinderError(
                "AS OF 目标晚于最近一次提交",
                detail=f"目标 LSN={target}，最近提交 LSN={latest}",
                hint="历史读只能读到已提交的点；先提交事务或选择更早的 LSN",
            )
        for _rid, values in heap.scan_at(
            entries,
            target,
            assume_committed_before=floor,
            strict_unknown_xmax=True,
        ):
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
        if node.build_left and node.kind == "INNER":
            yield from self._hash_join_build_left(node, txn)
            return
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

    def _hash_join_build_left(
        self, node: Join, txn: Transaction
    ) -> Iterator[tuple]:
        """INNER 哈希连接的反向形态：左侧建表，右侧流式探测。"""
        assert node.left is not None and node.right is not None
        left_rows = list(self._execute(node.left, txn))
        left_keys = [pair[0] for pair in node.hash_keys]
        right_keys = [pair[1] for pair in node.hash_keys]

        buckets: dict[tuple, list[tuple]] = {}
        for left in left_rows:
            key = tuple(left[i] for i in left_keys)
            if any(value is None for value in key):
                continue
            buckets.setdefault(key, []).append(tuple(left))

        for right in self._execute(node.right, txn):
            key = tuple(right[i] for i in right_keys)
            matches = () if any(v is None for v in key) else buckets.get(key, ())
            for left in matches:
                combined = left + tuple(right)
                if node.on is None or _truthy(node.on(EvalContext(combined))):
                    yield combined

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

        table_primary: list[
            tuple[tuple[str, ...], Optional[str], bool, bool]
        ] = []
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
                (
                    names,
                    constraint.name.lower() if constraint.name else None,
                    constraint.deferrable,
                    constraint.initially_deferred,
                )
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
        primary_deferrable = False
        primary_initially_deferred = False
        if table_primary:
            (
                primary_columns,
                primary_name,
                primary_deferrable,
                primary_initially_deferred,
            ) = table_primary[0]
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
            primary_def = next(
                col_def
                for col_def in stmt.columns
                if col_def.name.lower() == primary_columns[0]
            )
            primary_deferrable = primary_def.primary_deferrable
            primary_initially_deferred = (
                primary_def.primary_initially_deferred
            )
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
                    deferrable=primary_deferrable,
                    initially_deferred=primary_initially_deferred,
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
                    deferrable=col_def.unique_deferrable,
                    initially_deferred=col_def.unique_initially_deferred,
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
                    deferrable=constraint.deferrable,
                    initially_deferred=constraint.initially_deferred,
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
            if constraint.not_valid:
                raise NotSupportedError(
                    "CREATE TABLE 不支持 NOT VALID CHECK",
                    hint="先建表，再 ALTER TABLE ... ADD CONSTRAINT ... NOT VALID",
                )
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
            if spec.not_valid:
                raise NotSupportedError(
                    "CREATE TABLE 不支持 NOT VALID 外键",
                    hint="先建表，再 ALTER TABLE ... ADD CONSTRAINT ... NOT VALID",
                )
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
                on_delete_columns=tuple(spec.on_delete_columns),
                on_update_columns=tuple(spec.on_update_columns),
                match_type=spec.match_type,
                deferrable=spec.deferrable,
                initially_deferred=spec.initially_deferred,
                validated=True,
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
                    on_delete_columns=list(constraint.on_delete_columns),
                    on_update_columns=list(constraint.on_update_columns),
                    match_type=constraint.match_type,
                    deferrable=constraint.deferrable,
                    initially_deferred=constraint.initially_deferred,
                    not_valid=constraint.not_valid,
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
        column_updates: dict[str, set[tuple[str, str]]] = {}
        for name in self._catalog.view_names():
            binder = self._new_binder()
            deps, column_deps = binder.collect_view_dependencies(name)
            updates[name.lower()] = deps
            column_updates[name.lower()] = column_deps
        self._catalog.update_view_dependencies(txn, updates, column_updates)

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
                binder.column_dependencies,
            )
            # 替换可能改变了间接依赖：重算所有视图，避免依赖快照里残留旧表，
            # 也避免新表没有被依赖视图记录而允许 DROP TABLE。
            self._refresh_view_dependencies(txn)
            notice = f"已替换视图 {stmt.name.lower()}"
        else:
            self._catalog.create_view(
                txn, stmt.name, stmt.tokens, stmt.columns,
                binder.source_dependencies,
                binder.column_dependencies,
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

        self._persist_catalog(txn)
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
        self._persist_catalog(txn)
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
        self._persist_catalog(txn)
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
        self._persist_catalog(txn)
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
        own_fk_dependents = [
            fk for fk in table.foreign_keys.values()
            if old_column.name in fk.columns
        ]
        ext_fk_dependents = [
            (other, fk)
            for other in self._catalog.list_tables()
            if other is not table
            for fk in other.foreign_keys.values()
            if (
                fk.ref_table == table.name
                and old_column.name in fk.ref_columns
            )
        ]
        removed_constraints = (
            {fk.name for fk in own_fk_dependents}
            | {fk.name for _other, fk in ext_fk_dependents}
        )
        if not stmt.cascade:
            if own_fk_dependents:
                fk = own_fk_dependents[0]
                raise NotSupportedError(
                    f"列 {old_column.name!r} 被外键约束 {fk.name!r} 引用",
                    hint="先删除外键约束，再修改列类型",
                )
            if ext_fk_dependents:
                other, fk = ext_fk_dependents[0]
                raise NotSupportedError(
                    f"列 {old_column.name!r} 被表 {other.name} 的外键 "
                    f"{fk.name!r} 引用",
                    hint="先删除外键约束，再修改列类型",
                )
        else:
            fk_holders: dict[str, tuple[TableDef, dict[str, ForeignKeyDef]]] = {}
            if own_fk_dependents:
                fk_holders[table.name] = (table, dict(table.foreign_keys))
            for other, _fk in ext_fk_dependents:
                if other.name not in fk_holders:
                    fk_holders[other.name] = (
                        other,
                        dict(other.foreign_keys),
                    )
            for holder, old_map in fk_holders.values():
                holder.foreign_keys = {
                    name: fk
                    for name, fk in holder.foreign_keys.items()
                    if not (
                        (holder is table and old_column.name in fk.columns)
                        or (
                            fk.ref_table == table.name
                            and old_column.name in fk.ref_columns
                        )
                    )
                }

            if fk_holders:

                def undo_cascade_fks() -> None:
                    for holder, old_map in fk_holders.values():
                        holder.foreign_keys = old_map

                txn.undo_log.append(undo_cascade_fks)
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
        self._persist_catalog(txn)
        old_modes = {
            name: txn.constraint_modes[name]
            for name in removed_constraints
            if name in txn.constraint_modes
        }
        for name in old_modes:
            txn.constraint_modes.pop(name, None)
        if old_modes:

            def undo_modes() -> None:
                txn.constraint_modes.update(old_modes)

            txn.undo_log.append(undo_modes)
        index_note = (
            f"；重建 {rebuilt} 个索引" if rebuilt else ""
        )
        cascade_note = ""
        fk_count = len(own_fk_dependents) + len(ext_fk_dependents)
        if stmt.cascade and fk_count:
            cascade_note = f"；级联删除 {fk_count} 个外键"
        return Result(
            tag="ALTER TABLE",
            notice=(
                f"已把列 {old_column.name} 的类型从 {old_column.type.name} "
                f"改为 {new_type.name}；重写 {len(converted)} 行"
                f"{index_note}{cascade_note}"
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
        view_dependents = self._catalog._view_column_dependents(
            table.name, column.name
        )
        if view_dependents and not stmt.cascade:
            raise BinderError(
                f"列 {column.name!r} 被视图依赖："
                f"{', '.join(view_dependents)}",
                hint="使用 DROP COLUMN ... CASCADE，或先 DROP VIEW 依赖它的视图",
            )
        if len(table.columns) == 1:
            raise NotSupportedError(
                "表必须至少保留一列，不能删除最后一列",
                hint="可以先 DROP TABLE 再重建空表",
            )
        own_fk_dependents = [
            fk for fk in table.foreign_keys.values()
            if column.name in fk.columns
        ]
        ext_fk_dependents = [
            (other, fk)
            for other in self._catalog.list_tables()
            if other is not table
            for fk in other.foreign_keys.values()
            if fk.ref_table == table.name and column.name in fk.ref_columns
        ]
        index_dependents = [
            idx for idx in table.indexes.values()
            if column.name in idx.columns
        ]
        removed_constraints = (
            {fk.name for fk in own_fk_dependents}
            | {fk.name for _other, fk in ext_fk_dependents}
            | {idx.name for idx in index_dependents}
        )

        def check_uses_column(check: CheckDef) -> bool:
            tokens = list(check.tokens)
            for pos, (kind, value) in enumerate(tokens):
                if kind not in ("ident", "keyword"):
                    continue
                if value.lower() != column.name:
                    continue
                # ``t.a`` 里的 ``t`` 是表限定符，不是列引用。
                if pos + 1 < len(tokens) and tokens[pos + 1][0] == "dot":
                    continue
                return True
            return False

        check_dependents = [
            check for check in table.checks.values()
            if check_uses_column(check)
        ]

        if not stmt.cascade:
            if own_fk_dependents:
                fk = own_fk_dependents[0]
                raise NotSupportedError(
                    f"列 {column.name!r} 被外键约束 {fk.name!r} 引用",
                    hint="先 ALTER TABLE ... DROP CONSTRAINT 删除外键",
                )
            if ext_fk_dependents:
                other, fk = ext_fk_dependents[0]
                raise NotSupportedError(
                    f"列 {column.name!r} 被表 {other.name} 的外键 "
                    f"{fk.name!r} 引用",
                    hint="先删除引用它的外键约束",
                )
            if index_dependents:
                idx = index_dependents[0]
                raise NotSupportedError(
                    f"列 {column.name!r} 被索引/约束 {idx.name!r} 引用",
                    hint="先 DROP INDEX 或 ALTER TABLE ... DROP CONSTRAINT",
                )
            if check_dependents:
                check = check_dependents[0]
                raise BinderError(
                    f"CHECK 约束 {check.name!r} 引用了列 {column.name!r}",
                    hint="先删除引用该列的 CHECK 约束",
                )
        else:
            fk_holders: dict[str, tuple[TableDef, dict[str, ForeignKeyDef]]] = {}
            if own_fk_dependents:
                fk_holders[table.name] = (table, dict(table.foreign_keys))
            for other, _fk in ext_fk_dependents:
                if other.name not in fk_holders:
                    fk_holders[other.name] = (
                        other,
                        dict(other.foreign_keys),
                    )
            for holder, old_map in fk_holders.values():
                holder.foreign_keys = {
                    name: fk
                    for name, fk in holder.foreign_keys.items()
                    if not (
                        (holder is table and column.name in fk.columns)
                        or (
                            fk.ref_table == table.name
                            and column.name in fk.ref_columns
                        )
                    )
                }

            if fk_holders:

                def undo_cascade_fks() -> None:
                    for holder, old_map in fk_holders.values():
                        holder.foreign_keys = old_map

                txn.undo_log.append(undo_cascade_fks)

            old_checks = dict(table.checks)
            for check in check_dependents:
                del table.checks[check.name]
            if check_dependents:

                def undo_cascade_checks() -> None:
                    table.checks = old_checks

                txn.undo_log.append(undo_cascade_checks)

            for idx in index_dependents:
                self._catalog.drop_index(txn, idx.name)

        for view_name in view_dependents:
            self._catalog.drop_view(txn, view_name, cascade=True)

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
        self._persist_catalog(txn)
        old_modes = {
            name: txn.constraint_modes[name]
            for name in removed_constraints
            if name in txn.constraint_modes
        }
        for name in old_modes:
            txn.constraint_modes.pop(name, None)
        if old_modes:

            def undo_modes() -> None:
                txn.constraint_modes.update(old_modes)

            txn.undo_log.append(undo_modes)
        index_note = f"；重建 {rebuilt} 个索引" if rebuilt else ""
        cascade_parts: list[str] = []
        if stmt.cascade:
            fk_count = len(own_fk_dependents) + len(ext_fk_dependents)
            if fk_count:
                cascade_parts.append(f"{fk_count} 个外键")
            if index_dependents:
                cascade_parts.append(f"{len(index_dependents)} 个索引")
            if check_dependents:
                cascade_parts.append(f"{len(check_dependents)} 个 CHECK 约束")
            if view_dependents:
                cascade_parts.append(f"{len(view_dependents)} 个视图")
        cascade_note = (
            f"；级联删除 {'、'.join(cascade_parts)}"
            if cascade_parts
            else ""
        )
        return Result(
            tag="ALTER TABLE",
            notice=(
                f"已删除列 {column.name}；重写 {len(new_rows)} 行"
                f"{index_note}{cascade_note}"
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
            old_mode = txn.constraint_modes.pop(old_key, None)

            def undo() -> None:
                table.checks.pop(new_key, None)
                table.checks[old_key] = check
                if old_mode is not None:
                    txn.constraint_modes[old_key] = old_mode

            txn.undo_log.append(undo)
        elif old_key in table.foreign_keys:
            fk = table.foreign_keys[old_key]
            new_fk = replace(fk, name=new_key)
            table.foreign_keys.pop(old_key)
            table.foreign_keys[new_key] = new_fk
            old_mode = txn.constraint_modes.pop(old_key, None)
            if old_mode is not None:
                txn.constraint_modes[new_key] = old_mode

            def undo() -> None:
                table.foreign_keys.pop(new_key, None)
                table.foreign_keys[old_key] = fk
                if old_mode is not None:
                    txn.constraint_modes.pop(new_key, None)
                    txn.constraint_modes[old_key] = old_mode

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
            old_mode = txn.constraint_modes.pop(old_key, None)
            if old_mode is not None:
                txn.constraint_modes[new_key] = old_mode

            def undo() -> None:
                table.indexes.pop(new_key, None)
                table.indexes[old_key] = idx
                if old_mode is not None:
                    txn.constraint_modes.pop(new_key, None)
                    txn.constraint_modes[old_key] = old_mode

            txn.undo_log.append(undo)

        self._persist_catalog(txn)
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
            check = CheckDef(
                name=name,
                tokens=tuple(spec.tokens),
                validated=not constraint.not_valid,
            )
            fn = self._compile_check(table, check)
            if not constraint.not_valid:
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
            self._persist_catalog(txn)
            return Result(
                tag="ALTER TABLE",
                notice=(
                    f"已添加 CHECK 约束 {name}（NOT VALID）"
                    if constraint.not_valid
                    else f"已添加 CHECK 约束 {name}"
                ),
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
                on_delete_columns=tuple(constraint.on_delete_columns),
                on_update_columns=tuple(constraint.on_update_columns),
                match_type=constraint.match_type,
                deferrable=constraint.deferrable,
                initially_deferred=constraint.initially_deferred,
                validated=not constraint.not_valid,
            )
            provisional = TableDef(
                name=table.name,
                columns=table.columns,
                indexes=table.indexes,
                checks=table.checks,
                foreign_keys=dict(table.foreign_keys, **{fk_name: fk}),
            )
            heap = HeapFile(self._bp, table, self._txns)
            if not constraint.not_valid:
                for _rid, values in heap.scan(txn):
                    self._check_foreign_keys(provisional, values, txn, force=True)
            old_fks = dict(table.foreign_keys)
            table.foreign_keys[fk_name] = fk

            def undo_fk() -> None:
                table.foreign_keys = old_fks

            txn.undo_log.append(undo_fk)
            self._persist_catalog(txn)
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
                deferrable=constraint.deferrable,
                initially_deferred=constraint.initially_deferred,
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
                deferrable=constraint.deferrable,
                initially_deferred=constraint.initially_deferred,
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
            self._persist_catalog(txn)
            return Result(
                tag="ALTER TABLE",
                notice=f"已添加主键 {index_name}",
            )

        raise NotSupportedError(
            f"不支持的表级约束：{constraint.kind}"
        )

    def _alter_alter_constraint(
        self, stmt: AlterTableAlterConstraint, txn: Transaction
    ) -> Result:
        """修改外键的 DEFERRABLE / INITIALLY 属性并立即持久化目录。"""
        table = self._catalog.get_table(stmt.table)
        name = stmt.name.lower()
        fk = table.foreign_keys.get(name)
        if fk is None:
            available = sorted(table.foreign_keys)
            raise BinderError(
                f"外键约束 {name!r} 不存在",
                hint="可用外键：" + (", ".join(available) or "（空）"),
            )
        if (
            stmt.initially_deferred is True
            and stmt.deferrable is None
            and not fk.deferrable
        ):
            raise NotSupportedError(
                f"外键 {name!r} 不是 DEFERRABLE，不能直接 INITIALLY DEFERRED",
                hint="同时声明 DEFERRABLE INITIALLY DEFERRED",
            )
        deferrable = (
            fk.deferrable if stmt.deferrable is None else stmt.deferrable
        )
        initially_deferred = (
            fk.initially_deferred
            if stmt.initially_deferred is None
            else stmt.initially_deferred
        )
        if not deferrable:
            initially_deferred = False
        if initially_deferred and not deferrable:
            raise NotSupportedError(
                f"外键 {name!r} 的 INITIALLY DEFERRED 需要 DEFERRABLE",
                hint="先声明 DEFERRABLE，再设置 INITIALLY DEFERRED",
            )
        if (
            deferrable == fk.deferrable
            and initially_deferred == fk.initially_deferred
        ):
            return Result(
                tag="ALTER TABLE",
                notice=f"外键 {name} 的延迟属性未改变",
            )

        # 目标模式是立即检查时，先把当前事务已积压的违反行补检掉；
        # 失败则不修改目录，事务可以继续修复后重试。
        if not (deferrable and initially_deferred):
            self.validate_deferred_foreign_keys(txn, only_names={name})

        old_mode = txn.constraint_modes.get(name)
        new_fk = replace(
            fk, deferrable=deferrable, initially_deferred=initially_deferred
        )
        table.foreign_keys[name] = new_fk
        txn.constraint_modes.pop(name, None)

        def undo() -> None:
            table.foreign_keys[name] = fk
            if old_mode is None:
                txn.constraint_modes.pop(name, None)
            else:
                txn.constraint_modes[name] = old_mode

        txn.undo_log.append(undo)
        self._persist_catalog(txn)
        mode = (
            "INITIALLY DEFERRED"
            if deferrable and initially_deferred
            else "INITIALLY IMMEDIATE"
            if deferrable
            else "NOT DEFERRABLE"
        )
        return Result(
            tag="ALTER TABLE",
            notice=f"已修改外键 {name} 为 {mode}",
        )

    def _alter_validate_constraint(
        self, stmt: AlterTableValidateConstraint, txn: Transaction
    ) -> Result:
        """校验 NOT VALID 外键或 CHECK 的存量行，成功后才置 validated=True。

        PostgreSQL 的 VALIDATE CONSTRAINT 只读扫描、不等待延迟队列；这里同样
        用 force=True 立即检查，扫描失败时约束保持 NOT VALID，事务可继续修复
        数据后重试。
        """
        table = self._catalog.get_table(stmt.table)
        name = stmt.name.lower()
        fk = table.foreign_keys.get(name)
        check = table.checks.get(name)
        if fk is None and check is None:
            available = sorted(table.foreign_keys) + sorted(table.checks)
            raise BinderError(
                f"约束 {name!r} 不存在",
                hint="可用约束："
                + (", ".join(available) or "（空）"),
            )

        if check is not None:
            if check.validated:
                return Result(
                    tag="ALTER TABLE",
                    notice=f"CHECK 约束 {name} 已处于校验状态",
                )
            fn = self._compile_check(table, check)
            heap = HeapFile(self._bp, table, self._txns)
            for _rid, values in heap.scan(txn):
                result = fn(EvalContext(tuple(values)))
                if result is not None and not bool(result):
                    raise CheckViolationError(
                        f"已有行违反 CHECK 约束 {name!r}",
                        detail=f"表 {table.name} 的存量数据不满足约束条件",
                        hint="先 UPDATE 修正存量行，再执行 VALIDATE CONSTRAINT",
                    )
            old_check = check
            table.checks[name] = replace(check, validated=True)

            def undo_check_validated() -> None:
                table.checks[name] = old_check

            txn.undo_log.append(undo_check_validated)
            self._persist_catalog(txn)
            return Result(
                tag="ALTER TABLE",
                notice=f"已校验 CHECK 约束 {name}",
            )

        assert fk is not None
        if fk.validated:
            return Result(
                tag="ALTER TABLE",
                notice=f"外键约束 {name} 已处于校验状态",
            )

        heap = HeapFile(self._bp, table, self._txns)
        for _rid, values in heap.scan(txn):
            self._check_foreign_keys(
                table, values, txn, force=True, only_names={name}
            )
        old_fk = fk
        table.foreign_keys[name] = replace(fk, validated=True)

        def undo_validated() -> None:
            table.foreign_keys[name] = old_fk

        txn.undo_log.append(undo_validated)
        self._persist_catalog(txn)
        return Result(
            tag="ALTER TABLE",
            notice=f"已校验外键约束 {name}",
        )

    def _alter_drop_constraint(
        self, stmt: AlterTableDropConstraint, txn: Transaction
    ) -> Result:
        table = self._catalog.get_table(stmt.table)
        name = stmt.name.lower()
        if name in table.foreign_keys:
            old_fks = dict(table.foreign_keys)
            old_mode = txn.constraint_modes.pop(name, None)
            del table.foreign_keys[name]

            def undo_fk() -> None:
                table.foreign_keys = old_fks
                if old_mode is not None:
                    txn.constraint_modes[name] = old_mode

            txn.undo_log.append(undo_fk)
            self._persist_catalog(txn)
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
                dependents: list[tuple[TableDef, str]] = []
                for other in self._catalog.list_tables():
                    for fk in other.foreign_keys.values():
                        if (
                            fk.ref_table == table.name
                            and tuple(fk.ref_columns) == tuple(index.columns)
                        ):
                            dependents.append((other, fk.name))
                if dependents and not stmt.cascade:
                    raise NotSupportedError(
                        f"约束 {name!r} 仍被外键 {dependents[0][1]!r} 依赖",
                        hint="先删除引用该唯一键的外键约束，或使用 DROP CONSTRAINT ... CASCADE",
                    )
                old_fk_maps = {
                    other.name: (other, dict(other.foreign_keys))
                    for other, _fk_name in dependents
                }
                for other, fk_name in dependents:
                    del other.foreign_keys[fk_name]
                if dependents:
                    cascade_note = f"（级联删除 {len(dependents)} 个外键）"

                    def undo_dependent_fks() -> None:
                        for _table, old_map in old_fk_maps.values():
                            _table.foreign_keys = old_map

                    txn.undo_log.append(undo_dependent_fks)
                else:
                    cascade_note = ""
                old_columns = list(table.columns)
                dropped_names = {name} | {
                    fk_name for _other, fk_name in dependents
                }
                old_modes = {
                    n: txn.constraint_modes[n]
                    for n in dropped_names
                    if n in txn.constraint_modes
                }
                self._catalog.drop_index(txn, name)
                for n in old_modes:
                    txn.constraint_modes.pop(n, None)

                def undo_mode() -> None:
                    txn.constraint_modes.update(old_modes)

                txn.undo_log.append(undo_mode)
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
                    self._persist_catalog(txn)
                    return Result(
                        tag="ALTER TABLE",
                        notice=f"已删除主键约束 {name}{cascade_note}",
                    )
                return Result(
                    tag="ALTER TABLE",
                    notice=f"已删除唯一约束 {name}{cascade_note}",
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
        old_mode = txn.constraint_modes.pop(name, None)
        del table.checks[name]

        def undo_check() -> None:
            table.checks = old_checks
            if old_mode is not None:
                txn.constraint_modes[name] = old_mode

        txn.undo_log.append(undo_check)
        self._persist_catalog(txn)
        return Result(
            tag="ALTER TABLE",
            notice=f"已删除 CHECK 约束 {name}",
        )

    def _drop_table(self, stmt: DropTable, txn: Transaction) -> Result:
        views = 0
        foreign_keys = 0
        removed_constraints: set[str] = set()
        for name in stmt.names:
            if not self._catalog.has_table(name):
                if stmt.if_exists:
                    continue
                raise TableNotFoundError(f"表 {name!r} 不存在")
            target = self._catalog.get_table(name)
            removed_constraints.update(target.foreign_keys)
            removed_constraints.update(target.indexes)
            # CASCADE 会删除其他表上引用该表的外键；这些约束的
            # SET CONSTRAINTS 覆盖也必须一起清掉，避免同名重建后继承旧模式。
            for other in self._catalog.list_tables():
                if other.name == target.name:
                    continue
                for fk in other.foreign_keys.values():
                    if fk.ref_table == target.name:
                        removed_constraints.add(fk.name)
            v, f = self._catalog.drop_table(txn, name, stmt.cascade)
            views += v
            foreign_keys += f
        old_modes = {
            name: txn.constraint_modes[name]
            for name in removed_constraints
            if name in txn.constraint_modes
        }
        for name in old_modes:
            txn.constraint_modes.pop(name, None)
        if old_modes:

            def undo_modes() -> None:
                txn.constraint_modes.update(old_modes)

            txn.undo_log.append(undo_modes)
        notice = ""
        if stmt.cascade and (views or foreign_keys):
            notice = f"级联删除 {views} 个视图、{foreign_keys} 个外键"
        return Result(tag="DROP TABLE", notice=notice)

    def _create_index(
        self,
        stmt: CreateIndex,
        txn: Transaction,
        *,
        constraint_kind: Optional[str] = None,
        deferrable: bool = False,
        initially_deferred: bool = False,
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
            deferrable=deferrable,
            initially_deferred=initially_deferred,
        )
        self._catalog.create_index(txn, table.name, idx)

        # 已有数据必须回填，否则索引是空的——查询会漏行，这比报错更糟
        heap = HeapFile(self._bp, table, self._txns)
        tree = self._tree_for(table, idx)
        for rid, values in heap.scan(txn):
            key = self._key_for(table, idx, values)
            if key is None:
                continue
            if idx.unique and tree.lookup(txn, key):
                raise DuplicateKeyError(
                    f"无法创建唯一索引 {stmt.name!r}",
                    detail=f"列 {', '.join(idx.columns)} 上存在重复值",
                )
            tree.insert(txn, key, rid)
        idx.root_page = tree.root_page
        self._persist_catalog(txn)
        return Result(tag="CREATE INDEX")

    def _drop_index(self, stmt: DropIndex, txn: Transaction) -> Result:
        name = stmt.name.lower()
        table: Optional[TableDef] = None
        idx: Optional[IndexDef] = None
        for candidate in self._catalog.list_tables():
            found = candidate.indexes.get(name)
            if found is not None:
                table = candidate
                idx = found
                break
        if idx is not None and idx.constraint_kind is not None:
            raise NotSupportedError(
                f"索引 {name!r} 由表约束创建，不能直接 DROP INDEX",
                hint="使用 ALTER TABLE ... DROP CONSTRAINT 删除约束",
            )

        dependents: list[tuple[TableDef, str]] = []
        if idx is not None and idx.unique:
            assert table is not None
            for other in self._catalog.list_tables():
                for fk in other.foreign_keys.values():
                    if (
                        fk.ref_table == table.name
                        and tuple(fk.ref_columns) == tuple(idx.columns)
                    ):
                        dependents.append((other, fk.name))
            if dependents and not stmt.cascade:
                raise NotSupportedError(
                    f"索引 {name!r} 仍被外键 {dependents[0][1]!r} 依赖",
                    hint="先删除引用该唯一键的外键约束，或使用 DROP INDEX ... CASCADE",
                )

        old_fk_maps = {
            other.name: (other, dict(other.foreign_keys))
            for other, _fk_name in dependents
        }
        for other, fk_name in dependents:
            del other.foreign_keys[fk_name]
        if dependents:

            def undo_dependent_fks() -> None:
                for _table, old_map in old_fk_maps.values():
                    _table.foreign_keys = old_map

            txn.undo_log.append(undo_dependent_fks)

        try:
            self._catalog.drop_index(txn, name)
        except Exception:
            if dependents:
                for _table, old_map in old_fk_maps.values():
                    _table.foreign_keys = old_map
            if stmt.if_exists:
                return Result(tag="DROP INDEX", notice="索引不存在，已跳过")
            raise
        dropped_names = {name} | {fk_name for _other, fk_name in dependents}
        old_modes = {
            n: txn.constraint_modes[n]
            for n in dropped_names
            if n in txn.constraint_modes
        }
        for n in old_modes:
            txn.constraint_modes.pop(n, None)

        def undo_modes() -> None:
            txn.constraint_modes.update(old_modes)

        txn.undo_log.append(undo_modes)
        notice = (
            f"已级联删除 {len(dependents)} 个外键"
            if dependents
            else ""
        )
        return Result(tag="DROP INDEX", notice=notice)

    def _snapshot_table_state(self, table, txn: Transaction):
        """登记 DML 对表/索引根页号的逆操作，并返回 DML 前的结构状态。

        堆表插入可能分配新页并修改 ``root_page``/``last_page``，索引插入可能
        更换 B+ 树根页；这些字段在 Catalog 对象上原地变化，如果事务回滚只
        撤销页内容而不撤销这些指针，内存目录就会指向已经恢复成空页的页。

        返回值 ``(root_page, last_page, row_count_hint, {索引: 根页})``
        供 :meth:`_persist_if_structure_changed` 判断本次 DML 是否真的改了
        结构，从而决定要不要把目录落盘。
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

        if not (
            self._concurrent_dml
            and not self._schema_statement
            and not txn.physical_savepoint
        ):
            # 并发 DML 走逻辑回滚：页内容不回滚，根页指针也不能退回旧值，
            # 否则会覆盖别的事务刚建好的新根。DDL / 保存点事务仍然要恢复。
            txn.undo_log.append(undo)
        return state

    def _catalog_structure_fingerprint(self) -> tuple:
        """整个目录的**结构性字段**指纹（不含 row_count_hint 等统计 hint）。

        用整目录而不是单表：级联操作（``ON UPDATE CASCADE`` / ``SET NULL``）
        会改子表，子表也可能分配新页。目录的 ``persist`` 本来就是全量保存，
        指纹对上"全量"才是同一个粒度。
        """
        return tuple(
            (
                name,
                table.root_page,
                table.last_page,
                tuple(
                    (iname, idx.root_page)
                    for iname, idx in table.indexes.items()
                ),
            )
            for name, table in sorted(self._catalog.tables.items())
        )

    def _persist_if_structure_changed(self, before, txn: Transaction) -> None:
        """DML 只在**结构**变化时持久化目录（写并发第 1 步的落地）。

        目录里的 ``row_count_hint`` 是统计 hint：DML 更新它在内存里供
        planner 用，但不逐笔落盘——重启后回退到上次结构性写入时的值，
        查询结果不变，代价只是优化器的行数估计暂时偏保守。

        ``root_page`` / ``last_page`` / 索引 ``root_page`` 是结构性的：
        ``HeapFile._append_page`` 用 ``last_page`` 把新页链到链尾，崩
        溃恢复后如果 ``last_page`` 是旧值，下一次分配会把新页链到旧的
        链尾上，覆盖它已有的 ``next`` 指针。所以这些字段一变就必须
        ``persist``。目录的整链保存（一条新页链 + 每页全页 WAL 镜像）
        因此从"每行一次"降到"每分配一页一次"。
        """
        if self._catalog_structure_fingerprint() != before:
            self._persist_catalog(txn)

    def _analyze(self, stmt: Analyze, txn: Transaction) -> Result:
        """扫描表并记录列级统计；统计是优化器输入，不改变查询结果。"""
        tables = (
            [self._catalog.get_table(stmt.table)]
            if stmt.table
            else self._catalog.list_tables()
        )
        if stmt.sample is not None and stmt.sample < 1:
            raise BinderError(
                "ANALYZE SAMPLE 必须大于 0",
                hint="SAMPLE 行数上限至少为 1；不要抽样时省略 SAMPLE",
            )
        total_rows = 0
        analyzed_columns = 0
        sampled_rows = 0
        for table in tables:
            heap = HeapFile(self._bp, table, self._txns)
            if stmt.sample is None:
                rows = [values for _rid, values in heap.scan(txn)]
                exact_rows = len(rows)
            else:
                rows = []
                exact_rows = 0
                rng = random.Random(f"analyze:{table.name}:{stmt.sample}")
                for _rid, values in heap.scan(txn):
                    exact_rows += 1
                    if len(rows) < stmt.sample:
                        rows.append(values)
                    else:
                        slot = rng.randrange(exact_rows)
                        if slot < stmt.sample:
                            rows[slot] = values
                sampled_rows += len(rows)
            total_rows += exact_rows
            table.row_count_hint = exact_rows
            table.stats_rows = exact_rows
            table.stats_sample_rows = len(rows) if stmt.sample is not None else 0
            table.column_stats = {}
            for index, column in enumerate(table.columns):
                nulls = 0
                distinct = set()
                for values in rows:
                    value = values[index]
                    if value is None:
                        nulls += 1
                        continue
                    try:
                        distinct.add(value)
                    except TypeError:
                        distinct.add(repr(value))
                n_distinct = len(distinct)
                null_frac = (nulls / len(rows)) if rows else 0.0
                stats = ColumnStats(
                    n_distinct=n_distinct,
                    null_frac=null_frac,
                )
                if column.type.is_numeric and n_distinct > 1:
                    numbers = [
                        float(values[index])
                        for values in rows
                        if values[index] is not None
                    ]
                    minimum, maximum = min(numbers), max(numbers)
                    stats.min_value = minimum
                    stats.max_value = maximum
                    if maximum > minimum:
                        buckets = 16
                        width = (maximum - minimum) / buckets
                        counts = [0] * buckets
                        for number in numbers:
                            slot = min(
                                buckets - 1,
                                int((number - minimum) / width),
                            )
                            counts[slot] += 1
                        stats.histogram = [
                            (
                                minimum + width * slot,
                                minimum + width * (slot + 1),
                                counts[slot],
                            )
                            for slot in range(buckets)
                        ]
                elif column.type.is_string and n_distinct > 1:
                    # 字符串没有“等宽”概念；用排序后的等频分桶。桶边界取每桶
                    # 首尾实际值，桶内计数相同，范围估算按整桶命中。
                    strings = sorted(
                        values[index]
                        for values in rows
                        if values[index] is not None
                    )
                    stats.min_value = strings[0]
                    stats.max_value = strings[-1]
                    if strings[-1] > strings[0]:
                        buckets = min(16, len(strings))
                        stats.histogram = []
                        for slot in range(buckets):
                            start = slot * len(strings) // buckets
                            end = (slot + 1) * len(strings) // buckets
                            if end <= start:
                                continue
                            stats.histogram.append(
                                (strings[start], strings[end - 1], end - start)
                            )
                table.column_stats[column.name] = stats
                analyzed_columns += 1
        self._persist_catalog(txn)
        return Result(
            affected=total_rows,
            tag=f"ANALYZE {len(tables)} table(s), {analyzed_columns} column(s)",
            notice=(
                f"已分析 {len(tables)} 张表、{analyzed_columns} 列，"
                f"可见行 {total_rows}；"
                + (
                    f"抽样 {sampled_rows} 行，统计为近似值；"
                    if stmt.sample is not None
                    else ""
                )
                + "统计只影响估算，不改变结果"
            ),
        )

    def _reindex(self, stmt: Reindex, txn: Transaction) -> Result:
        """按堆表当前内容重建索引：只保留当前可见的版本，回收稀疏页。"""
        table = self._catalog.get_table(stmt.table)
        self._snapshot_table_state(table, txn)
        rebuilt, entries_total = self._rebuild_indexes(table, txn)
        self._persist_catalog(txn)
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
            seen: Optional[set[tuple]] = set() if idx.unique else None
            for rid, values in snapshot:
                key = self._key_for(table, idx, values)
                if key is None:
                    continue
                if seen is not None:
                    # 树级唯一检查已关掉（索引项延迟回收后它分不清陈旧键），
                    # 这里从"当前可见行"重建，重复就是真冲突（ALTER TYPE
                    # 把 '1'/'01' 都转成 1 的用例靠这一句报错）。
                    normalized = tuple(key)
                    if normalized in seen:
                        raise self._duplicate_key_error(idx)
                    seen.add(normalized)
                entries.append((key, rid))
            tree = self._tree_for(table, idx)
            tree.rebuild(txn, entries)
            idx.root_page = tree.root_page
            rebuilt += 1
            entries_total += len(entries)
        return rebuilt, entries_total

    def _vacuum(self, stmt: Vacuum, txn: Transaction) -> Result:
        """回收死版本。堆页重排会改变 RID，所以随后必须重建索引。

        并发模式下如果还有空闲的活跃事务，走保 RID 的 ``vacuum_online``：
        只回收对所有活跃快照都不可见的版本，槽位与索引项保持不变。
        """
        if txn.capture_row_undo:
            raise NotSupportedError(
                "逻辑保存点事务内不能执行 VACUUM",
                detail="VACUUM 会物理回收行 undo 仍依赖的旧版本，逻辑回滚无法还原",
                hint="先 COMMIT 或 ROLLBACK 整个事务；需要事务内 VACUUM 时改用 --physical-savepoints",
            )
        tables = (
            [self._catalog.get_table(stmt.table)]
            if stmt.table
            else self._catalog.list_tables()
        )
        if self._history is not None:
            if stmt.retain_lsn is None:
                # VACUUM 开始之后回收掉的旧版本，任何不早于这个位置的目标点都
                # 不再需要（删除/覆盖都发生在这个位置之前）。保守抬到 WAL 的
                # 下一个 LSN，避免把正好在 VACUUM 里提交的版本算进可读窗口。
                self._history.advance(self._wal.next_lsn)
        retain_entries: Optional[dict[int, int]] = None
        retain = stmt.retain_lsn
        if retain is not None:
            if self._history is None:
                raise NotSupportedError(
                    "当前执行器没有接入历史读窗口",
                    hint="通过 Database 打开数据库后再使用 VACUUM RETAIN",
                )
            entries = dict(self._txns.commit_log())
            if not entries:
                raise NotSupportedError(
                    "当前 WAL 窗口还没有可用的提交点",
                    hint="先执行一条写语句并提交，再用 VACUUM RETAIN 指定保留点",
                )
            latest = max(entries.values())
            floor = self._history.floor_lsn
            if retain < floor:
                raise BinderError(
                    "VACUUM RETAIN 的目标早于可用的历史窗口",
                    detail=f"目标 LSN={retain}，历史下界 LSN={floor}",
                    hint="用 SHOW HISTORY WINDOW 查看下界，换一个不早于下界的目标点",
                )
            if retain > latest:
                raise BinderError(
                    "VACUUM RETAIN 的目标晚于最近一次提交",
                    detail=f"目标 LSN={retain}，最近提交 LSN={latest}",
                    hint="先提交事务，或选择更早的 LSN",
                )
            # 物理回收前先把结构下界持久化到 retain；VACUUM 会保留该目标点
            # 仍可见的版本。先推进后回收，崩溃也不会让下界落后于已删数据。
            self._history.advance(retain)
            retain_entries = entries
        active_others = [
            t for t in self._txns.active_transactions() if t.xid != txn.xid
        ]
        # 只对“没有持有 DDL 目录锁/物理 undo”的空闲事务走在线模式；
        # DDL 的 abort 会按页面原像回滚，和在线回收整页写日志不能交错。
        # RETAIN 必须走保 RID 的在线路径：离线 vacuum 会把表重排成每行只剩
        # 当前根元组，历史目标点需要的旧版本会被直接丢掉。
        online = retain is not None or (
            self._concurrent_dml
            and bool(active_others)
            and all(not t.catalog_hold and not t.undo_log for t in active_others)
        )
        before = self._catalog_structure_fingerprint()
        kept = dead = pages = 0
        online_tables = 0
        for table in tables:
            self._snapshot_table_state(table, txn)
            heap = HeapFile(self._bp, table, self._txns)
            if online:
                k, d, p = heap.vacuum_online(
                    txn,
                    active_others,
                    max_versions=self._vacuum_online_max_versions,
                    retain_lsn=retain or 0,
                    commit_lsns=retain_entries,
                )
                online_tables += 1
            else:
                k, d, p = heap.vacuum(txn)
                self._rebuild_indexes(table, txn)
            kept += k
            dead += d
            pages += p
        # 在线 VACUUM 可能只清死版本/索引陈旧项，不动 root_page/last_page/
        # 索引根；这时不必每次重写整份目录（否则每次 VACUUM 都会新建一条
        # 目录页链，文件随维护次数增长）。离线 VACUUM 会换 RID 和根页，
        # 指纹变化后仍会落盘。
        self._persist_if_structure_changed(before, txn)
        suffix = ", online" if online_tables else ""
        if retain is not None:
            suffix += f", retain LSN {retain}"
        return Result(
            affected=dead,
            tag=(
                f"VACUUM {len(tables)} tables ({kept} rows kept, "
                f"{dead} versions reclaimed, {pages} pages rewritten{suffix})"
            ),
        )

    def _truncate(self, stmt: Truncate, txn: Transaction) -> Result:
        """清空一到多张表：页链保留但每一页重置为空页，索引随之重建为空。

        ``TRUNCATE a, b`` 在默认 ``RESTRICT`` 下允许一起清空有外键依赖的表，
        但只要某张被引用表的子表没有出现在同一列表中，就仍然拒绝。``CASCADE``
        会递归清空所有直接/间接引用这些表的子表，避免只清父表后留下悬空行。
        """
        tables = self._truncate_target_tables(stmt)
        if not stmt.cascade:
            self._truncate_restrict_check(tables)
            total = sum(self._truncate_one(table, txn) for table in tables)
        else:
            plan: dict[str, TableDef] = {}
            for table in tables:
                for dependent in self._truncate_cascade_plan(table):
                    plan[dependent.name] = dependent
            total = sum(self._truncate_one(table, txn) for table in plan.values())
        return Result(
            affected=total,
            tag=f"TRUNCATE TABLE ({total} rows removed)",
        )

    def _truncate_target_tables(self, stmt: Truncate) -> list[TableDef]:
        """解析并去重 ``TRUNCATE`` 的目标表；重复表名视为语法错误。"""
        names = list(stmt.tables) or [stmt.table]
        tables: list[TableDef] = []
        seen: set[str] = set()
        for name in names:
            table = self._catalog.get_table(name)
            if table.name in seen:
                raise BinderError(
                    f"表 {table.name!r} 在 TRUNCATE 中被重复指定",
                    hint="每张表只能出现一次",
                )
            seen.add(table.name)
            tables.append(table)
        return tables

    def _truncate_restrict_check(self, tables: list[TableDef]) -> None:
        """默认 RESTRICT：所有引用目标表的子表都必须同时出现在目标列表。"""
        targets = {table.name for table in tables}
        for target in tables:
            for other in self._catalog.list_tables():
                for fk in other.foreign_keys.values():
                    if fk.ref_table != target.name:
                        continue
                    # 自引用表不能只靠“自己在列表里”绕过：TRUNCATE 单表时
                    # 仍然需要 CASCADE，避免删掉父行后子行悬空。
                    if other.name == target.name or other.name not in targets:
                        raise BinderError(
                            f"表 {target.name!r} 被外键 {fk.name!r} 引用，"
                            "不能直接 TRUNCATE",
                            hint=(
                                "把这些引用表加入同一条 TRUNCATE，"
                                "或使用 TRUNCATE ... CASCADE"
                            ),
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
        self._persist_catalog(txn)
        return removed

    def _catalog_version_at_time(self, timestamp: str) -> Optional[int]:
        try:
            moment = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            raise BinderError(
                f"无法解析 catalog 时间 {timestamp!r}",
                hint="要求 ISO-8601，例如 '2026-11-26T08:57:07+00:00'",
            )
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        normalized = moment.astimezone(timezone.utc).isoformat(timespec="microseconds")
        return self._catalog.version_at_or_before(normalized)

    def _history_version(self, catalog_version: Optional[int], catalog_time: Optional[str]) -> Optional[int]:
        if catalog_time is not None:
            return self._catalog_version_at_time(catalog_time)
        if catalog_version is not None and not self._catalog.version_exists(catalog_version):
            raise BinderError(
                f"catalog 版本 {catalog_version} 不在当前版本链上",
                hint="用 SHOW CATALOG VERSIONS 查可用版本；历史链默认保留最近 1000 个版本",
            )
        return catalog_version

    def _show_tables(self, stmt: ShowTables) -> Result:
        has_history = stmt.catalog_version is not None or stmt.catalog_time is not None
        version = self._history_version(stmt.catalog_version, stmt.catalog_time)
        if not has_history:
            rows = [
                (t.name, len(t.columns), t.row_count_hint)
                for t in self._catalog.list_tables()
            ]
            notice = ""
        elif version is None:
            rows = []
            notice = f"{stmt.catalog_time!r} 早于第一个 catalog 版本，没有表"
        else:
            rows = [
                (t.name, len(t.columns), t.row_count_hint)
                for t in self._catalog.tables_at(version)
            ]
            notice = (
                f"按目录版本 {version} 列出表；行数是该版本的 hint，不是旧数据页"
            )
        return Result(
            columns=[
                ColumnMeta("table", TypeSpec(DataType.TEXT)),
                ColumnMeta("columns", TypeSpec(DataType.INT)),
                ColumnMeta("rows", TypeSpec(DataType.BIGINT)),
            ],
            rows=rows,
            tag=f"SHOW {len(rows)}",
            notice=notice,
        )

    def _show_views(self, stmt: ShowViews) -> Result:
        has_history = stmt.catalog_version is not None or stmt.catalog_time is not None
        version = self._history_version(stmt.catalog_version, stmt.catalog_time)
        if not has_history:
            rows = [
                (name, " ".join(value for _kind, value in tokens))
                for name in self._catalog.view_names()
                for tokens in [self._catalog.get_view(name)]
            ]
            notice = ""
        elif version is None:
            rows = []
            notice = f"{stmt.catalog_time!r} 早于第一个 catalog 版本，没有视图"
        else:
            rows = [
                (view["name"], " ".join(value for _kind, value in view["tokens"]))
                for view in self._catalog.views_at(version)
            ]
            notice = f"按目录版本 {version} 列出视图；不读取旧数据页"
        return Result(
            columns=[
                ColumnMeta("view", TypeSpec(DataType.TEXT)),
                ColumnMeta("definition", TypeSpec(DataType.TEXT)),
            ],
            rows=rows,
            tag=f"SHOW {len(rows)}",
            notice=notice,
        )

    def _show_statistics(self, stmt: ShowStatistics) -> Result:
        has_history = stmt.catalog_version is not None or stmt.catalog_time is not None
        version = self._history_version(stmt.catalog_version, stmt.catalog_time)
        notice = ""
        if not has_history:
            if stmt.table is None:
                tables = sorted(self._catalog.list_tables(), key=lambda t: t.name)
            else:
                tables = [self._catalog.get_table(stmt.table)]
        elif version is None:
            if stmt.table is not None:
                raise TableNotFoundError(
                    f"表 {stmt.table!r} 在 {stmt.catalog_time!r} 及之前不存在",
                    hint="用 SHOW CATALOG VERSIONS 查 created_at",
                )
            tables = []
            notice = f"{stmt.catalog_time!r} 早于第一个 catalog 版本，没有统计"
        else:
            if stmt.table is None:
                tables = self._catalog.tables_at(version)
            else:
                found = self._catalog.table_at(version, stmt.table)
                if found is None:
                    raise TableNotFoundError(
                        f"表 {stmt.table!r} 在 catalog 版本 {version} 不存在",
                        hint="用 SHOW TABLES AT CATALOG VERSION n 查该版本的表",
                    )
                tables = [found]
            notice = f"按目录版本 {version} 读取列统计；不代表旧数据页的当前内容"
        rows = []
        for table in tables:
            for column in table.columns:
                stats = table.column_stats.get(column.name)
                rows.append(
                    (
                        table.name,
                        column.name,
                        table.stats_rows,
                        stats.n_distinct if stats is not None else 0,
                        stats.null_frac if stats is not None else 0.0,
                        "" if stats is None or stats.min_value is None else str(stats.min_value),
                        "" if stats is None or stats.max_value is None else str(stats.max_value),
                        len(stats.histogram) if stats is not None else 0,
                        table.stats_sample_rows,
                    )
                )
        return Result(
            columns=[
                ColumnMeta("table", TypeSpec(DataType.TEXT)),
                ColumnMeta("column", TypeSpec(DataType.TEXT)),
                ColumnMeta("rows", TypeSpec(DataType.BIGINT)),
                ColumnMeta("n_distinct", TypeSpec(DataType.BIGINT)),
                ColumnMeta("null_frac", TypeSpec(DataType.FLOAT)),
                ColumnMeta("min", TypeSpec(DataType.TEXT)),
                ColumnMeta("max", TypeSpec(DataType.TEXT)),
                ColumnMeta("buckets", TypeSpec(DataType.BIGINT)),
                ColumnMeta("sample_rows", TypeSpec(DataType.BIGINT)),
            ],
            rows=rows,
            tag=f"STATISTICS {len(rows)}",
            notice=notice,
        )

    def _show_commit_log(self, stmt: ShowCommitLog) -> Result:
        from ..errors import BinderError

        if stmt.limit is not None and stmt.limit < 0:
            raise BinderError(
                "SHOW COMMIT LOG LIMIT 必须是非负整数",
                hint="LIMIT 0 返回空结果；省略 LIMIT 查看全部",
            )
        rows = self._txns.commit_log(stmt.limit)
        return Result(
            columns=[
                ColumnMeta("xid", TypeSpec(DataType.BIGINT)),
                ColumnMeta("commit_lsn", TypeSpec(DataType.BIGINT)),
            ],
            rows=rows,
            tag=f"COMMITS {len(rows)}",
            notice="进程内新增提交 + 启动时从当前 WAL 窗口重建；更早的持久提交历史尚未保留",
        )

    def _show_history_window(self, stmt: ShowHistoryWindow) -> Result:
        if self._history is None:
            raise NotSupportedError(
                "当前执行器没有接入历史读窗口",
                detail="SHOW HISTORY WINDOW 需要 checkpoint / VACUUM / DDL 水位",
                hint="通过 Database 打开数据库后再查询",
            )
        entries = self._txns.commit_log()
        latest = max((lsn for _xid, lsn in entries), default=None)
        checkpoint = self._history.checkpoint_lsn
        floor = self._history.floor_lsn
        rows = [(checkpoint, floor, latest, len(entries))]
        return Result(
            columns=[
                ColumnMeta("checkpoint_lsn", TypeSpec(DataType.BIGINT)),
                ColumnMeta("floor_lsn", TypeSpec(DataType.BIGINT)),
                ColumnMeta("latest_commit_lsn", TypeSpec(DataType.BIGINT)),
                ColumnMeta("commits", TypeSpec(DataType.BIGINT)),
            ],
            rows=rows,
            tag="HISTORY WINDOW",
            notice=(
                "AS OF SYSTEM TIME LSN 的可用窗口是 [floor_lsn, latest_commit_lsn]；"
                "checkpoint/VACUUM/DDL 会把 floor 抬高"
            ),
        )

    def _show_columns(self, stmt: ShowColumns) -> Result:
        if stmt.catalog_time is not None:
            version = self._catalog_version_at_time(stmt.catalog_time)
            if version is None:
                raise TableNotFoundError(
                    f"表 {stmt.table!r} 在 {stmt.catalog_time!r} 及之前不存在",
                    hint="时间不早于本库第一个 catalog 版本；用 SHOW CATALOG VERSIONS 查 created_at",
                )
            table = self._catalog.table_at(version, stmt.table)
            if table is None:
                raise TableNotFoundError(
                    f"表 {stmt.table!r} 在 {stmt.catalog_time!r} 对应的版本 {version} 不存在",
                    hint="该时间点是其他表的 schema 版本；用 SHOW CATALOG VERSIONS 查版本历史",
                )
        elif stmt.catalog_version is None:
            table = self._catalog.get_table(stmt.table)
        else:
            table = self._catalog.table_at(stmt.catalog_version, stmt.table)
            if table is None:
                raise TableNotFoundError(
                    f"表 {stmt.table!r} 在 catalog 版本 {stmt.catalog_version} 不存在",
                    hint="用 SHOW CATALOG VERSIONS 查可用版本；版本号从 1 开始",
                )
        rows = [
            (
                column.name,
                column.type.name,
                "YES" if column.nullable else "NO",
                "YES" if column.primary_key else "NO",
                "" if column.default is None else repr(column.default),
            )
            for column in table.columns
        ]
        return Result(
            columns=[
                ColumnMeta("column", TypeSpec(DataType.TEXT)),
                ColumnMeta("type", TypeSpec(DataType.TEXT)),
                ColumnMeta("nullable", TypeSpec(DataType.TEXT)),
                ColumnMeta("primary_key", TypeSpec(DataType.TEXT)),
                ColumnMeta("default", TypeSpec(DataType.TEXT)),
            ],
            rows=rows,
            tag=f"COLUMNS {len(rows)}",
        )

    def _show_catalog_versions(self) -> Result:
        history = self._catalog.history()
        rows = [
            (
                item["catalog_version"],
                ", ".join(table["name"] for table in item["tables"]),
                ", ".join(view["name"] for view in item["views"]),
                len(item["tables"]) + len(item["views"]),
                item.get("created_at", ""),
            )
            for item in history
        ]
        return Result(
            columns=[
                ColumnMeta("catalog_version", TypeSpec(DataType.INT)),
                ColumnMeta("tables", TypeSpec(DataType.TEXT)),
                ColumnMeta("views", TypeSpec(DataType.TEXT)),
                ColumnMeta("objects", TypeSpec(DataType.INT)),
                ColumnMeta("created_at", TypeSpec(DataType.TEXT)),
            ],
            rows=rows,
            tag=f"CATALOG VERSIONS {len(rows)}",
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
        before = self._catalog_structure_fingerprint()
        heap = HeapFile(self._bp, table, self._txns)
        if stmt.query is not None:
            return self._insert_select(stmt, table, heap, txn, before)
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

        self._persist_if_structure_changed(before, txn)
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
                if idx.deferrable:
                    raise NotSupportedError(
                        "可延迟唯一约束不能作为 ON CONFLICT 的仲裁者",
                        hint="PostgreSQL 要求冲突仲裁索引必须是非延迟唯一索引",
                    )
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
                if idx.deferrable:
                    raise NotSupportedError(
                        "可延迟唯一约束不能作为 ON CONFLICT 的仲裁者",
                        hint="PostgreSQL 要求冲突仲裁索引必须是非延迟唯一索引",
                    )
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
        if not stmt.on_conflict_nothing and stmt.on_conflict_update is None:
            return None
        unique = [idx for idx in table.indexes.values() if idx.unique]
        if unique and all(idx.deferrable for idx in unique):
            raise NotSupportedError(
                "可延迟唯一约束不能作为 ON CONFLICT 的仲裁者",
                hint="为该表建立非延迟唯一索引，或改用显式冲突目标",
            )
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
        *,
        latest: bool = False,
    ) -> list[Any]:
        """UPDATE 与 upsert 共用的行重写入口：校验、维护索引、写堆。

        ``latest=True`` 用于并发 DML 的自动提交 UPDATE：调用方已经在行锁上
        等待并重读过最新已提交版本，写堆也必须按同一版本落地。
        """
        old_tuple = tuple(old_values)
        txn.touched_tables.add(table.name)
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
        # 行锁：改这一行之前独占；随后 `heap.update` 里的版本检查才可靠。
        self._lock_row(table, rid, txn, wait=latest)
        # 键锁：改唯一键时，新旧两个键都要独占（别人可能正插/删同一个键）。
        for idx in changed_indexes:
            self._lock_key(table, idx, self._key_for(table, idx, old_tuple), txn)
            self._lock_key(table, idx, self._key_for(table, idx, prepared), txn)
        for idx in changed_indexes:
            self._check_update_unique(table, idx, old_tuple, prepared, txn, rid)

        heap.update(txn, rid, prepared, ri=latest)

        for idx in changed_indexes:
            tree = self._tree_for(table, idx)
            new_key = self._key_for(table, idx, prepared)
            if new_key is not None:
                # 旧键的索引项不摘（等 VACUUM 回收），但同一 rid 可能已经有一条
                # 同键的陈旧项（键改回原值），重复插会造出 (key, rid) 重复项，
                # 让索引扫描把同一行返回两次。
                if rid not in tree.lookup(txn, new_key):
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
                    action_columns = (
                        fk.on_update_columns
                        if fk.on_update in ("SET NULL", "SET DEFAULT")
                        else ()
                    ) or fk.columns
                    for local_name, ref_name in zip(fk.columns, fk.ref_columns):
                        index = child.column_index(local_name)
                        if (
                            fk.on_update == "SET NULL"
                            and local_name in action_columns
                        ):
                            child_new[index] = None
                        elif (
                            fk.on_update == "SET DEFAULT"
                            and local_name in action_columns
                        ):
                            child_new[index] = child.columns[index].default
                        else:
                            child_new[index] = new_values[
                                table.column_index(ref_name)
                            ]
        return list(pending.values())

    def _insert_select(
        self, stmt: Insert, table, heap, txn: Transaction, before
    ) -> Result:
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

        self._persist_if_structure_changed(before, txn)
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
        txn.touched_tables.add(table.name)
        rid = heap.insert(txn, prepared)
        for idx in table.indexes.values():
            tree = self._tree_for(table, idx)
            key = self._key_for(table, idx, prepared)
            if key is None:
                continue
            # 键锁：唯一性判断依赖"物理索引项"，而并发的 DELETE 会立刻摘掉它。
            self._lock_key(table, idx, key, txn)
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

    def _indexed_dml_targets(
        self,
        table: TableDef,
        where: Optional[Expr],
        txn: Transaction,
        heap: HeapFile,
        *,
        ri: bool,
    ) -> Optional[list[tuple[RID, Optional[list[Any]]]]]:
        """把单列 ``col = 常量`` 的 DML 目标改走索引，避免全表扫描。

        返回 ``None`` 表示这条 WHERE 不适合索引下推（调用方继续全扫）；
        返回列表（可能为空）表示已经用索引物化完目标，调用方不要再扫堆。
        ``UPDATE/DELETE`` 仍会在物化后重新执行 ``where_fn``，所以这里只负责
        缩小候选集合，不改变谓词语义。
        """
        if not isinstance(where, BinaryOp) or where.op != "=":
            return None
        left, right = where.left, where.right
        if isinstance(right, ColumnRef) and isinstance(left, Literal):
            left, right = right, left
        if not isinstance(left, ColumnRef) or not isinstance(right, Literal):
            return None
        if left.table is not None and left.table.lower() != table.name:
            return None
        column_name = left.name.lower()
        if not table.has_column(column_name):
            return None
        column = table.column(column_name)
        index = table.index_for_columns((column_name,))
        if index is None:
            return None
        if right.value is None:
            return None
        try:
            key_value = column.type.coerce(right.value)
        except (TypeError, ValueError):
            return None

        targets: list[tuple[RID, Optional[list[Any]]]] = []
        seen_roots: set[RID] = set()
        for rid in self._tree_for(table, index).lookup(txn, [key_value]):
            if rid in seen_roots:
                continue
            seen_roots.add(rid)
            values = heap.get(txn, rid, ri=ri)
            if values is None:
                # 自动提交 UPDATE 的 ri=True 目标物化发生在行锁之前：此刻
                # 版本链头可能正被另一个事务改写（xmin 尚未 COMMIT），
                # get 会返回 None。不能据此把目标丢掉——否则 _update 拿到
                # 空列表、affected=0，热点自增直接少一次（实测 800→799）。
                # 保留 RID 作为“待复核目标”，_update 会在等行锁后用 fresh
                # 值再判可见性和完整 WHERE；显式快照事务保持原语义跳过。
                if ri:
                    targets.append((rid, None))
                continue
            # 索引项可能陈旧（DELETE/UPDATE 不立即摘项，等 VACUUM 回收）：
            # 以堆里的当前版本复核键，避免把已经改走键的行当成命中。
            if self._key_for(table, index, values) != [key_value]:
                continue
            targets.append((rid, values))
        return targets

    def _update(self, stmt: Update, txn: Transaction) -> Result:
        table = self._catalog.get_table(stmt.table)
        self._snapshot_table_state(table, txn)
        before = self._catalog_structure_fingerprint()
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
        # 并发 DML 的自动提交 UPDATE：行锁上等待后必须重读"最新已提交版本"。
        # 否则语句快照仍停在等锁前，SET v = v + 1 会把对方刚提交的增量覆盖掉，
        # 或者被 heap.update 的 first-updater-wins 再判成一次 40001。
        latest = self._concurrent_dml and txn.statement_scoped
        # 必须先物化目标行：更新会向堆里追加新版本，边扫边写会让扫描器
        # 不断看到刚写进去的新版本（root RID 不变），从而反复更新同一行。
        # 同一个 root RID 只能作为一次 UPDATE 目标。并发更新时，旧版本页
        # 与新版本页不是同一份快照：ri=True 的扫描可能先看到旧页（xmax 尚未
        # 写），再看到新页（xmin 已提交），把同一逻辑行的两个物理版本都算
        # 成可见。若不去重，SET v=v+1 会对同一行执行两次，热点基准出现
        # “800 次更新最后 v=801”。
        #
        # 单列等值条件优先走索引：UPDATE ... WHERE id = ? 若仍全表扫描，
        # 热点行更新会被 8000 行的堆扫描拖成几十毫秒，行锁争用会被放大。
        indexed = (
            self._indexed_dml_targets(
                table, stmt.where, txn, heap, ri=latest
            )
            if source_table is None
            else None
        )
        if indexed is not None:
            targets = indexed
        else:
            targets = []
            seen_roots: set[RID] = set()
            for rid, values in heap.scan(txn, ri=latest):
                if rid in seen_roots:
                    continue
                seen_roots.add(rid)
                targets.append((rid, values))
        for rid, old_values in targets:
            # 索引物化时若版本链头正被并发写者改到一半，old_values 会是
            # None。此时不做预过滤，等行锁后拿到 fresh 值再判 WHERE。
            old_tuple = tuple(old_values) if old_values is not None else None
            ctx = None
            if old_tuple is not None:
                if source_table is None:
                    ctx = EvalContext(old_tuple)
                    if where_fn is not None and not _truthy(where_fn(ctx)):
                        continue
                else:
                    for source_values in source_rows:
                        candidate = EvalContext(old_tuple + tuple(source_values))
                        if where_fn is None or _truthy(where_fn(candidate)):
                            ctx = candidate
                            break
                    if ctx is None:
                        continue
            if latest:
                # 先等到这一行的写锁，再从版本链头取最新已提交版本重算。
                self._lock_row(table, rid, txn, wait=True)
                fresh = heap.get(txn, rid, ri=True)
                if fresh is None:
                    continue
                old_tuple = tuple(fresh)
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
            elif old_tuple is None:  # pragma: no cover - 非 latest 不会产生空目标
                continue
            assert ctx is not None
            new_values = list(old_tuple)
            for index, fn in assignment_fns:
                new_values[index] = fn(ctx)
            prepared = self._rewrite_row(
                table, heap, txn, rid, old_tuple, new_values, latest=latest
            )
            if ret_fns:
                returned.append(
                    tuple(fn(EvalContext(tuple(prepared))) for fn in ret_fns)
                )
            updated += 1

        self._persist_if_structure_changed(before, txn)
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
        before = self._catalog_structure_fingerprint()
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
        indexed = (
            self._indexed_dml_targets(table, stmt.where, txn, heap, ri=False)
            if source_table is None
            else None
        )
        targets = indexed if indexed is not None else list(heap.scan(txn))
        for rid, values in targets:
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
            self._lock_row(table, rid, txn)
            self._lock_index_keys(table, values, txn, rid)
            txn.touched_tables.add(table.name)
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

        self._persist_if_structure_changed(before, txn)
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

        索引项可能陈旧（DELETE/UPDATE 不再当场摘项，等 VACUUM 回收），所以
        命中后必须回堆里看**当前可见版本**：行已删/本事务已删 → 不算占用；
        行还在但键已改走 → 也是陈旧项。唯一性只能按 heap 可见性判，不能只看
        "索引里还有没有这个键"。
        """
        candidates = indexes if indexes is not None else table.indexes.values()
        heap = HeapFile(self._bp, table, self._txns)
        for idx in candidates:
            if not idx.unique:
                continue
            if self._index_is_deferred(idx, txn):
                continue
            key = self._key_for(table, idx, values)
            if key is None:
                continue
            tree = self._tree_for(table, idx)
            for rid in tree.lookup(txn, key):
                if rid == ignore_rid:
                    continue
                occupant = heap.get(txn, rid)
                if occupant is None:
                    continue
                if self._key_for(table, idx, occupant) != key:
                    continue
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

    def _index_is_deferred(self, idx: IndexDef, txn: Transaction) -> bool:
        if not idx.deferrable:
            return False
        mode = txn.constraint_modes.get(idx.name)
        if mode is not None:
            return mode == "DEFERRED"
        return idx.initially_deferred


    def _set_constraints(
        self, stmt: SetConstraints, txn: Transaction
    ) -> Result:
        all_fks = [
            fk
            for table in self._catalog.tables.values()
            for fk in table.foreign_keys.values()
        ]
        all_indexes = [
            idx
            for table in self._catalog.tables.values()
            for idx in table.indexes.values()
            if idx.constraint_kind is not None
        ]
        if stmt.all_constraints:
            fk_targets = [fk for fk in all_fks if fk.deferrable]
            index_targets = [idx for idx in all_indexes if idx.deferrable]
            names = {fk.name for fk in fk_targets} | {
                idx.name for idx in index_targets
            }
            only_names = None
        else:
            names = {name.lower() for name in stmt.names}
            fk_targets = [fk for fk in all_fks if fk.name in names]
            index_targets = [
                idx for idx in all_indexes if idx.name in names
            ]
            found = {fk.name for fk in fk_targets} | {
                idx.name for idx in index_targets
            }
            missing = names - found
            if missing:
                available = sorted(
                    {fk.name for fk in all_fks}
                    | {idx.name for idx in all_indexes}
                )
                raise BinderError(
                    f"约束 {sorted(missing)[0]!r} 不存在",
                    hint="可用约束："
                    + (", ".join(available) or "（空）"),
                )
            non_deferrable = [
                fk for fk in fk_targets if not fk.deferrable
            ] + [
                idx for idx in index_targets if not idx.deferrable
            ]
            if non_deferrable:
                raise NotSupportedError(
                    f"约束 {non_deferrable[0].name!r} 不是 DEFERRABLE",
                    hint="只有声明 DEFERRABLE 的约束可以切换检查时机",
                )
            only_names = names

        if not stmt.deferred:
            self.validate_deferred_foreign_keys(
                txn, only_names=only_names
            )
            self.validate_deferred_unique_constraints(
                txn, only_names=only_names
            )

        mode = "DEFERRED" if stmt.deferred else "IMMEDIATE"
        for fk in fk_targets:
            txn.constraint_modes[fk.name] = mode
        for idx in index_targets:
            txn.constraint_modes[idx.name] = mode
        return Result(
            tag="SET CONSTRAINTS",
            notice=(
                f"已将 {len(fk_targets) + len(index_targets)} 个约束"
                f"设为 {mode}"
            ),
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
                values = heap.get(txn, rid, ri=True)
                if values is not None and all(
                    values[pos] == value
                    for pos, value in zip(positions, key)
                ):
                    return True
        for _rid, values in heap.scan(txn, ri=True):
            if all(
                values[pos] == value
                for pos, value in zip(positions, key)
            ):
                return True
        return False

    def validate_deferred_unique_constraints(
        self, txn: Transaction, *, only_names: Optional[set[str]] = None
    ) -> None:
        """提交前补检延迟 UNIQUE / PRIMARY KEY，失败由事务层 abort。

        [Review-Fix] 只扫本事务写过的表：没有写过的表不可能被本事务弄成重复。
        之前的实现是"每张表都全扫一遍"，并且被自动提交路径对**每条语句**
        （包括 SELECT）调用，读操作因此变成了 O(全库行数) 的全表扫描。
        """
        if not txn.touched_tables:
            return
        for table in self._catalog.list_tables():
            if table.name not in txn.touched_tables:
                continue
            deferred = [
                idx
                for idx in table.indexes.values()
                if idx.deferrable
                and self._index_is_deferred(idx, txn)
                and (only_names is None or idx.name in only_names)
            ]
            if not deferred:
                continue
            heap = HeapFile(self._bp, table, self._txns)
            seen: dict[str, set[tuple]] = {
                idx.name: set() for idx in deferred
            }
            for _rid, values in heap.scan(txn):
                for idx in deferred:
                    key = self._key_for(table, idx, values)
                    if key is None:
                        continue
                    normalized = tuple(key)
                    if normalized in seen[idx.name]:
                        raise DuplicateKeyError(
                            f"延迟唯一约束 {idx.name!r} 在提交时仍有重复值",
                            detail=f"列 {', '.join(idx.columns)}",
                        )
                    seen[idx.name].add(normalized)

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
            values = HeapFile(self._bp, table, self._txns).get(txn, rid, ri=True)
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
                    ref_values = ref_heap.get(txn, rid, ri=True)
                    if ref_values is not None and all(
                        ref_values[pos] == value
                        for pos, value in zip(ref_positions, local_key)
                    ):
                        # FOR KEY SHARE：校验期间父行不能被改/删，否则会出现
                        # "子行插入成功、父行同时被删"的悬空引用。
                        self._lock_parent_row(ref_table, rid, txn)
                        found = True
                        break
            if not found:
                for _rid, ref_values in ref_heap.scan(txn, ri=True):
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

    def _lock_index_keys(
        self, table: TableDef, values: list[Any], txn: Transaction, rid: RID
    ) -> None:
        """DELETE/UPDATE 时只**锁键**，不物理摘索引项。

        为什么不能当场摘：MVCC 里删除/更新在提交前，别的快照仍应看到旧版本。
        索引项一摘，并发读者走索引扫描就找不到这一行（键锁只防重复键，不解决
        读可见性）。所以索引项与 heap 里的死版本一起，等到 VACUUM 重建索引时
        再回收；这期间唯一性判断改按 heap 可见性做（见 ``_find_unique_conflict``），
        索引扫描也要用堆里的当前版本复核键（见 ``_index_lookup`` / ``_index_range``）。
        """
        for idx in table.indexes.values():
            key = self._key_for(table, idx, values)
            if key is not None:
                # 删/改索引键前锁键：否则并发 INSERT 看不到这次未提交的删除，
                # 删除方一回滚，索引里就出现两个相同的键。
                self._lock_key(table, idx, key, txn)

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
                child_values = child_heap.get(txn, rid, ri=True)
                if child_values is None:
                    continue
                if all(
                    child_values[child.column_index(name)] == value
                    for name, value in zip(fk.columns, key)
                ):
                    yield rid, child_values
            return
        for rid, child_values in child_heap.scan(txn, ri=True):
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
                        action_columns = fk.on_delete_columns or fk.columns
                        for name in action_columns:
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
                        action_columns = fk.on_delete_columns or fk.columns
                        for name in action_columns:
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
                    self._lock_row(child, child_rid, txn)
                    self._lock_index_keys(child, child_values, txn, child_rid)
                    txn.touched_tables.add(child.name)
                    child_heap.delete(txn, child_rid)
                    txn.deferred_fk_rows.discard((child.name, child_rid))
                    processing.discard(ident)
                    deleted.add(ident)

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
        # 唯一性判断之前先锁键：唯一性读的是"物理索引项"，而并发事务可能正在
        # 插/删同一个键（未提交）。不加锁时会把"别人还没提交的键"当成真冲突
        # 报 23505——对方一旦回滚，这个 23505 就是错的。加了键锁，未提交冲突
        # 变成 40001（重试），已提交冲突才是 23505。
        for idx in table.indexes.values():
            if idx.unique:
                self._lock_key(table, idx, self._key_for(table, idx, values), txn)
        conflict = self._find_unique_conflict(table, values, txn)
        if conflict is not None:
            raise self._duplicate_key_error(conflict[0])

    def _tree_for(self, table: TableDef, idx: IndexDef) -> BPlusTree:
        # KeyCodec 会自动在末尾追加 RID 两列，这里只给索引列的类型
        if self._internal_cache_version != self._catalog.schema_version:
            self._internal_page_cache.clear()
            self._internal_cache_version = self._catalog.schema_version
        specs = [table.column(c).type for c in idx.columns]
        return BPlusTree(
            self._bp,
            KeyCodec(specs),
            idx.root_page,
            # 不再让 B+ 树自己判唯一：索引项现在延迟到 VACUUM 才回收，
            # 树里可能存在"已删除/键已改走"的陈旧键，树级 lookup 分不清
            # 它是否还该算占用。唯一性统一由 `_find_unique_conflict` 按
            # heap 可见性判定（键锁负责并发窗口）。
            unique=False,
            name=idx.name,
            internal_cache=self._internal_page_cache,
            # 根页号跨语句共享：索引定义对象就是那个"根页单元"，根锁由目录
            # 按索引共享；否则 A 分裂根后 B 用旧实例回写会覆盖新根。
            root_cell=idx,
            root_lock=self._catalog.index_root_lock(table.name, idx.name),
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
    if node.estimated is not None:
        est = node.estimated
        line += (
            f" (rows≈{est.rows:.0f} cpu≈{est.cpu:.0f} "
            f"confidence={est.confidence}"
        )
        if est.reason:
            line += f"; reason={est.reason}"
        line += ")"
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
