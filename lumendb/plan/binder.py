"""名字绑定、类型检查与表达式编译。

这里做的事情是数据库教科书上 "semantic analysis + binding" 的部分：

1. 把列名（可能带表前缀）解析成"这一行里的第几列"；
2. 推导每个表达式的类型，提前发现不可能成立的运算；
3. 把 AST 编译成闭包，执行时不需要再走一遍语法树。

编译而不是"边遍历边求值"是有理由的：树遍历每行都要做类型判断和字典查找，
而闭包把名字解析的结果固化成下标访问。
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Optional

from ..catalog.catalog import Catalog
from ..catalog.schema import Column, TableDef
from ..datatypes import DataType, TypeSpec, common_type
from ..errors import (
    AmbiguousColumnError,
    BinderError,
    ColumnNotFoundError,
    NotSupportedError,
    TableNotFoundError,
    TypeMismatchError,
    UndefinedParameterError,
)
from ..sql.ast import (
    Between,
    BinaryOp,
    Cast,
    CaseWhen,
    ColumnRef,
    CompoundSelect,
    Cte,
    Exists,
    Expr,
    FunctionCall,
    InList,
    InSubquery,
    IsNull,
    Like,
    Literal,
    ParamRef,
    ScalarSubquery,
    Star,
    render_expr,
    UnaryOp,
)
from .nodes import (
    Aggregate,
    CteScan,
    Distinct,
    EvalContext,
    Filter,
    IndexLookup,
    IndexRange,
    Join,
    Limit,
    OutputColumn,
    PlanNode,
    Project,
    Scan,
    SetOp,
    Sort,
)
from .nodes import Values

AGGREGATE_FUNCTIONS = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SchemaColumn:
    qualifier: str
    name: str
    type: TypeSpec


class Schema:
    """一行数据的列布局，用于把名字解析成下标。"""

    def __init__(
        self,
        columns: Iterable[SchemaColumn],
        *,
        default_qualifier: Optional[str] = None,
    ) -> None:
        self.columns = list(columns)
        # 冲突更新时 schema 同时含旧行和 excluded 行；未限定的列名
        # 必须优先落到目标表，而不是因为两个 qualifier 同名而报歧义。
        self.default_qualifier = (
            default_qualifier.lower() if default_qualifier else None
        )

    def __len__(self) -> int:
        return len(self.columns)

    def resolve(self, table: Optional[str], name: str) -> int:
        lowered = name.lower()
        qualifier = table.lower() if table else self.default_qualifier
        matches = [
            i
            for i, col in enumerate(self.columns)
            if col.name == lowered and (qualifier is None or col.qualifier == qualifier)
        ]
        if not matches:
            hint = ""
            if qualifier:
                tables = {c.qualifier for c in self.columns}
                if qualifier not in tables:
                    hint = f"可用表：{', '.join(sorted(tables))}"
                else:
                    cols = [c.name for c in self.columns if c.qualifier == qualifier]
                    hint = f"表 {qualifier} 有列：{', '.join(cols)}"
            else:
                hint = f"可用列：{', '.join(c.name for c in self.columns)}"
            raise ColumnNotFoundError(
                f"列 {name!r} 不存在" + (f"（表 {table}）" if table else ""),
                hint=hint,
            )
        if len(matches) > 1:
            owners = ", ".join(self.columns[i].qualifier for i in matches)
            raise AmbiguousColumnError(
                f"列名 {name!r} 有歧义",
                detail=f"多张表都有该列：{owners}",
                hint="用表名限定，例如 t.name",
            )
        return matches[0]

    def type_of(self, index: int) -> TypeSpec:
        return self.columns[index].type

    def concat(self, other: "Schema") -> "Schema":
        return Schema(self.columns + other.columns)


# ---------------------------------------------------------------------------
# 聚合
# ---------------------------------------------------------------------------


@dataclass
class AggSpec:
    kind: str
    arg: Optional[Callable[[EvalContext], Any]]
    distinct: bool
    result_type: TypeSpec


class _Accumulator:
    __slots__ = ("spec", "count", "total", "best", "seen")

    def __init__(self, spec: AggSpec) -> None:
        self.spec = spec
        self.count = 0
        self.total: Any = None
        self.best: Any = None
        self.seen: set[Any] = set()

    def update(self, ctx: EvalContext) -> None:
        kind = self.spec.kind
        if kind == "COUNT" and self.spec.arg is None:
            self.count += 1
            return
        value = self.spec.arg(ctx)
        if value is None:
            return
        if self.spec.distinct:
            # MIN/MAX 对重复值不敏感，但仍然去重一次也无害；主要收益在
            # COUNT(DISTINCT x) / SUM(DISTINCT x) / AVG(DISTINCT x)。
            if value in self.seen:
                return
            self.seen.add(value)
        if kind == "COUNT":
            self.count += 1
        elif kind == "SUM":
            self.total = value if self.total is None else self.total + value
            self.count += 1
        elif kind == "AVG":
            self.total = value if self.total is None else self.total + value
            self.count += 1
        elif kind == "MIN":
            self.best = value if self.best is None else min(self.best, value)
        elif kind == "MAX":
            self.best = value if self.best is None else max(self.best, value)

    def result(self) -> Any:
        kind = self.spec.kind
        if kind == "COUNT":
            return self.count
        if kind == "SUM":
            return self.total
        if kind == "AVG":
            return (self.total / self.count) if self.count else None
        if kind in ("MIN", "MAX"):
            return self.best
        raise NotSupportedError(f"未知聚合函数 {kind}")


class AggregateAccumulatorGroup:
    """一组聚合累积器，用一次遍历完成所有聚合。"""

    def __init__(self, specs: list[AggSpec]) -> None:
        self.specs = specs
        self.accumulators = [_Accumulator(s) for s in specs]

    def update(self, ctx: EvalContext) -> None:
        for acc in self.accumulators:
            acc.update(ctx)

    def results(self) -> tuple:
        return tuple(acc.result() for acc in self.accumulators)


# ---------------------------------------------------------------------------
# 表达式编译
# ---------------------------------------------------------------------------


class Binder:
    def __init__(self, catalog: Catalog, params: Optional[list[Any]] = None) -> None:
        self.catalog = catalog
        #: ``$n`` 的绑定值。绑定阶段直接折成字面量，执行计划里不存在参数数组。
        self.params: list[Any] = list(params) if params else []
        #: 聚合节点 id -> 在聚合输出行里的下标
        self._agg_slots: dict[int, int] = {}
        self._agg_specs: list[AggSpec] = []
        self._agg_base: int = 0
        #: 需要执行子查询时由执行器注入的钩子
        self.subquery_runner: Optional[Callable[[Any, EvalContext], Any]] = None
        #: 当前查询可见的 CTE：name -> (已绑定计划, 输出列)
        self.ctes: dict[str, tuple[PlanNode, list[OutputColumn]]] = {}

    # -- 表达式 ---------------------------------------------------------

    def compile(
        self,
        expr: Expr,
        schema: Schema,
        *,
        allow_agg: bool = True,
    ) -> tuple[Callable[[EvalContext], Any], TypeSpec]:
        method = _DISPATCH.get(type(expr))
        if method is None:
            raise NotSupportedError(
                f"表达式 {type(expr).__name__} 尚不支持",
                hint="检查 SELECT / WHERE 里是否用了未实现的语法",
            )
        return method(self, expr, schema, allow_agg)

    def _compile_children(
        self, expr: Expr, schema: Schema, allow_agg: bool
    ) -> list[tuple[Callable, TypeSpec]]:
        return [self.compile(c, schema, allow_agg=allow_agg) for c in _children(expr)]

    # -- 各节点 ---------------------------------------------------------

    def _literal(self, expr: Literal, schema, allow_agg):
        value = expr.value
        if value is None:
            # SQL NULL 在 PostgreSQL 里是 unknown 类型，而不是 TEXT；
            # 让它在 UNION / CASE / 比较里被另一侧的真实类型吸收。
            return (lambda ctx: None), TypeSpec(DataType.TEXT, unknown=True)
        if isinstance(value, bool):
            return (lambda ctx: value), TypeSpec(DataType.BOOLEAN)
        if isinstance(value, int):
            return (lambda ctx: value), TypeSpec(DataType.INT)
        if isinstance(value, float):
            return (lambda ctx: value), TypeSpec(DataType.FLOAT)
        return (lambda ctx: value), TypeSpec(DataType.TEXT)

    def _param(self, expr: ParamRef, schema, allow_agg):
        index = expr.index - 1
        if index < 0 or index >= len(self.params):
            raise UndefinedParameterError(
                f"参数 ${expr.index} 没有绑定值",
                detail=f"本次调用提供了 {len(self.params)} 个参数",
                hint="扩展协议要在 Bind 里给参数赋值；简单查询里不支持 $n",
            )
        return self._literal(Literal(self.params[index]), schema, allow_agg)

    def _column(self, expr: ColumnRef, schema, allow_agg):
        index = schema.resolve(expr.table, expr.name)
        return (lambda ctx, i=index: ctx.row[i]), schema.type_of(index)

    def _star(self, expr: Star, schema, allow_agg):
        raise TypeMismatchError(
            "* 不能出现在这里",
            hint="* 只能用在 SELECT 列表或 COUNT(*) 中",
        )

    def _unary(self, expr: UnaryOp, schema, allow_agg):
        operand, t = self.compile(expr.operand, schema, allow_agg=allow_agg)
        if expr.op == "-":
            if not t.is_numeric:
                raise TypeMismatchError(
                    "一元负号只能作用于数值",
                    detail=f"实际类型 {t.sql}",
                )
            return (lambda ctx: None if operand(ctx) is None else -operand(ctx)), t
        if expr.op == "NOT":
            fn = operand

            def _not(ctx):
                v = fn(ctx)
                return None if v is None else (not v)

            return _not, TypeSpec(DataType.BOOLEAN)
        raise NotSupportedError(f"未知一元运算符 {expr.op}")

    def _binary(self, expr: BinaryOp, schema, allow_agg):
        left_fn, lt = self.compile(expr.left, schema, allow_agg=allow_agg)
        right_fn, rt = self.compile(expr.right, schema, allow_agg=allow_agg)
        op = expr.op

        if op in ("AND", "OR"):
            if op == "AND":

                def _and(ctx):
                    l = left_fn(ctx)
                    if l is False:
                        return False
                    r = right_fn(ctx)
                    if r is False:
                        return False
                    if l is None or r is None:
                        return None
                    return bool(l) and bool(r)

                return _and, TypeSpec(DataType.BOOLEAN)

            def _or(ctx):
                l = left_fn(ctx)
                if l is True:
                    return True
                r = right_fn(ctx)
                if r is True:
                    return True
                if l is None or r is None:
                    return None
                return bool(l) or bool(r)

            return _or, TypeSpec(DataType.BOOLEAN)

        if op in ("=", "<>", "<", "<=", ">", ">="):
            if not _comparable_types(lt, rt):
                raise TypeMismatchError(
                    f"无法比较 {lt.sql} 与 {rt.sql}",
                    detail=f"运算符 {op} 两侧的类型不兼容",
                    hint="用 CAST 显式转换后再比较",
                )
            return _make_comparison(op, left_fn, right_fn, lt, rt), TypeSpec(DataType.BOOLEAN)

        if op == "||":
            # 字符串拼接：SQL 的 || 对 NULL 是"传染"的——任一侧为 NULL 结果就是 NULL
            def _concat(ctx):
                a, b = left_fn(ctx), right_fn(ctx)
                if a is None or b is None:
                    return None
                return str(a) + str(b)

            return _concat, TypeSpec(DataType.TEXT)

        if op in ("+", "-", "*", "/", "%"):
            if lt.unknown or rt.unknown:
                other = rt if lt.unknown else lt
                if other.unknown:
                    return (lambda ctx: None), TypeSpec(DataType.TEXT, unknown=True)
                if not other.is_numeric:
                    raise TypeMismatchError(
                        f"运算符 {op} 需要数值操作数",
                        detail=f"未知类型 NULL 与 {other.sql}",
                    )
                # NULL + 1 的结果类型由另一侧决定，运行时直接传播 NULL。
                return (lambda ctx: None), other
            if not lt.is_numeric or not rt.is_numeric:
                raise TypeMismatchError(
                    f"运算符 {op} 需要数值操作数",
                    detail=f"左边 {lt.sql}，右边 {rt.sql}",
                )
            result_type = (
                TypeSpec(DataType.FLOAT)
                if lt.kind is DataType.FLOAT or rt.kind is DataType.FLOAT or op == "/"
                else TypeSpec(DataType.INT)
            )

            def _arith(ctx):
                a, b = left_fn(ctx), right_fn(ctx)
                if a is None or b is None:
                    return None
                if op == "+":
                    return a + b
                if op == "-":
                    return a - b
                if op == "*":
                    return a * b
                if op == "/":
                    if b == 0:
                        raise TypeMismatchError(
                            "除数为 0",
                            hint="SQL 语义里除以 0 是错误，不是返回 NULL",
                        )
                    return a / b
                if b == 0:
                    raise TypeMismatchError("取模的除数为 0")
                return a % b

            return _arith, result_type

        raise NotSupportedError(f"未知二元运算符 {op}")

    def _is_null(self, expr: IsNull, schema, allow_agg):
        fn, _ = self.compile(expr.operand, schema, allow_agg=allow_agg)
        negated = expr.negated
        return (lambda ctx: (fn(ctx) is not None) if negated else (fn(ctx) is None)), TypeSpec(
            DataType.BOOLEAN
        )

    def _in_list(self, expr: InList, schema, allow_agg):
        fn, _ = self.compile(expr.operand, schema, allow_agg=allow_agg)
        items = [self.compile(i, schema, allow_agg=allow_agg)[0] for i in expr.items]
        negated = expr.negated

        def _in(ctx):
            value = fn(ctx)
            if value is None:
                return None
            hit = any(value == i(ctx) for i in items)
            return (not hit) if negated else hit

        return _in, TypeSpec(DataType.BOOLEAN)

    def _between(self, expr: Between, schema, allow_agg):
        fn, _ = self.compile(expr.operand, schema, allow_agg=allow_agg)
        low, _ = self.compile(expr.low, schema, allow_agg=allow_agg)
        high, _ = self.compile(expr.high, schema, allow_agg=allow_agg)
        negated = expr.negated

        def _between(ctx):
            v = fn(ctx)
            lo, hi = low(ctx), high(ctx)
            if v is None or lo is None or hi is None:
                return None
            inside = lo <= v <= hi
            return (not inside) if negated else inside

        return _between, TypeSpec(DataType.BOOLEAN)

    def _like(self, expr: Like, schema, allow_agg):
        fn, _ = self.compile(expr.operand, schema, allow_agg=allow_agg)
        pat, _ = self.compile(expr.pattern, schema, allow_agg=allow_agg)
        negated = expr.negated
        import re

        cache: dict[str, Any] = {}

        def _like(ctx):
            value = fn(ctx)
            pattern = pat(ctx)
            if value is None or pattern is None:
                return None
            rx = cache.get(pattern)
            if rx is None:
                rx = re.compile(_like_to_regex(pattern), re.DOTALL)
                if len(cache) < 256:
                    cache[pattern] = rx
            hit = rx.fullmatch(str(value)) is not None
            return (not hit) if negated else hit

        return _like, TypeSpec(DataType.BOOLEAN)

    def _function(self, expr: FunctionCall, schema, allow_agg):
        name = expr.name.upper()
        if name in AGGREGATE_FUNCTIONS:
            if not allow_agg:
                raise NotSupportedError(
                    "聚合函数不能出现在这里",
                    detail="WHERE / GROUP BY 里不允许聚合",
                    hint="把聚合放到 SELECT 或 HAVING",
                )
            return self._aggregate_ref(expr, schema)

        args = [self.compile(a, schema, allow_agg=allow_agg) for a in expr.args]
        build = _SCALAR_FUNCTIONS.get(name)
        if build is None:
            raise NotSupportedError(
                f"函数 {name} 尚未实现",
                hint="支持 COUNT/SUM/AVG/MIN/MAX/LENGTH/UPPER/LOWER/ABS/COALESCE",
            )
        return build(args)

    def _aggregate_ref(self, expr: FunctionCall, schema):
        key = id(expr)
        slot = self._agg_slots.get(key)
        if slot is None:
            spec = self._make_agg_spec(expr, schema)
            slot = len(self._agg_specs)
            self._agg_specs.append(spec)
            self._agg_slots[key] = slot

        # 聚合输出行 = 分组列 + 聚合结果，因此聚合结果的物理下标是
        # ``_agg_base + slot``。这里直接按 row 下标读，避免再引入一层上下文。
        def _ref(ctx, i=self._agg_base + slot):
            return ctx.row[i]

        return _ref, self._agg_specs[slot].result_type

    def _make_agg_spec(self, expr: FunctionCall, schema) -> AggSpec:
        kind = expr.name.upper()
        if kind == "COUNT" and len(expr.args) == 1 and isinstance(expr.args[0], Star):
            return AggSpec(kind, None, expr.distinct, TypeSpec(DataType.BIGINT))
        if len(expr.args) != 1:
            raise TypeMismatchError(f"{kind} 需要且只需要 1 个参数")
        arg_fn, arg_type = self.compile(expr.args[0], schema, allow_agg=False)
        if kind in ("SUM", "AVG") and arg_type.unknown:
            # SUM(NULL) / AVG(NULL) 的结果是 NULL；类型由各自聚合语义决定。
            result = (
                TypeSpec(DataType.TEXT, unknown=True)
                if kind == "SUM"
                else TypeSpec(DataType.FLOAT)
            )
            return AggSpec(kind, arg_fn, expr.distinct, result)
        if kind in ("SUM", "AVG") and not arg_type.is_numeric:
            raise TypeMismatchError(
                f"{kind} 需要数值参数",
                detail=f"实际类型 {arg_type.sql}",
            )
        result = {
            "COUNT": TypeSpec(DataType.BIGINT),
            "SUM": arg_type,
            "AVG": TypeSpec(DataType.FLOAT),
            "MIN": arg_type,
            "MAX": arg_type,
        }[kind]
        return AggSpec(kind, arg_fn, expr.distinct, result)

    def _cast(self, expr: Cast, schema, allow_agg):
        fn, _ = self.compile(expr.operand, schema, allow_agg=allow_agg)
        target = expr.target

        def _cast(ctx):
            value = fn(ctx)
            if value is None:
                return None
            try:
                return target.coerce(value)
            except ValueError as exc:
                raise TypeMismatchError(
                    f"无法把 {value!r} 转换成 {target.sql}",
                    detail=str(exc),
                ) from exc

        return _cast, target

    def _case(self, expr: CaseWhen, schema, allow_agg):
        operand_fn = None
        if expr.operand is not None:
            operand_fn, _ = self.compile(expr.operand, schema, allow_agg=allow_agg)

        tests: list[tuple[Callable[[EvalContext], bool], Callable[[EvalContext], Any]]] = []
        result_type: Optional[TypeSpec] = None

        for cond, value in expr.branches:
            cond_fn, cond_type = self.compile(cond, schema, allow_agg=allow_agg)
            if operand_fn is None:
                if cond_type.kind != DataType.BOOLEAN:
                    raise TypeMismatchError(
                        "CASE WHEN 的条件必须是布尔值",
                        detail=f"实际类型 {cond_type.sql}",
                        hint="写成 WHEN age > 18 THEN ... 这种能产生布尔值的比较",
                    )
                test = _bool_test(cond_fn)
            else:
                test = _equality_test(operand_fn, cond_fn)

            value_fn, value_type = self.compile(value, schema, allow_agg=allow_agg)
            result_type = _merge_branch_types(result_type, value_type)
            tests.append((test, value_fn))

        otherwise_fn = None
        if expr.otherwise is not None:
            otherwise_fn, other_type = self.compile(expr.otherwise, schema, allow_agg=allow_agg)
            result_type = _merge_branch_types(result_type, other_type)

        if result_type is None:
            result_type = TypeSpec(DataType.TEXT)

        def _case(ctx):
            for test, value in tests:
                if test(ctx):
                    return value(ctx)
            return otherwise_fn(ctx) if otherwise_fn is not None else None

        return _case, result_type

    def _scalar_subquery(self, expr: ScalarSubquery, schema, allow_agg):
        runner = self.subquery_runner
        if runner is None:
            raise NotSupportedError("子查询在当前上下文不可用")

        def _sub(ctx):
            rows = runner(expr.query, ctx) or []
            if not rows:
                return None
            if len(rows) > 1:
                raise TypeMismatchError(
                    "标量子查询返回了多行",
                    detail=f"实际返回 {len(rows)} 行",
                    hint="用 LIMIT 1，或改用 IN / EXISTS",
                )
            return rows[0][0]

        type_spec = self._scalar_subquery_type(expr.query)
        return _sub, type_spec

    def _scalar_subquery_type(self, query) -> TypeSpec:
        """只绑定、不执行，用输出列推断标量子查询的静态类型。

        之前这里固定返回 TEXT，导致 ``id = (SELECT 2)`` 被误判为
        INT 与 TEXT 比较；类型必须来自子查询的真实输出列。
        """
        return self._subquery_column_type(query, "标量子查询")

    def _subquery_column_type(self, query, label: str) -> TypeSpec:
        binder = Binder(self.catalog, self.params)
        binder.subquery_runner = self.subquery_runner
        binder.ctes = dict(self.ctes)
        _, cols = binder.bind_select(query)
        if len(cols) != 1:
            raise BinderError(
                f"{label}只能返回一列",
                detail=f"实际返回 {len(cols)} 列",
            )
        return cols[0].type

    def _in_subquery(self, expr: InSubquery, schema, allow_agg):
        fn, operand_type = self.compile(expr.operand, schema, allow_agg=allow_agg)
        # IN (SELECT ...) 的输出列类型必须和左侧操作数兼容；只做运行时
        # 比较会让 INT IN (SELECT '1') 静默返回空结果，也会让多列子查询
        # 悄悄忽略后面的列。
        sub_type = self._subquery_column_type(expr.query, "IN 子查询")
        if not _comparable_types(operand_type, sub_type):
            raise TypeMismatchError(
                "IN 左侧与子查询列类型不兼容",
                detail=(
                    f"左侧 {operand_type.sql}，"
                    f"子查询 {sub_type.sql}"
                ),
                hint="用 CAST 显式转换，或改写成 JOIN",
            )
        runner = self.subquery_runner
        if runner is None:
            raise NotSupportedError("子查询在当前上下文不可用")
        negated = expr.negated

        def _in(ctx):
            value = fn(ctx)
            if value is None:
                return None
            rows = runner(expr.query, ctx) or []
            hit = any(r[0] == value for r in rows)
            return (not hit) if negated else hit

        return _in, TypeSpec(DataType.BOOLEAN)

    def _exists(self, expr: Exists, schema, allow_agg):
        runner = self.subquery_runner
        if runner is None:
            raise NotSupportedError("子查询在当前上下文不可用")
        negated = expr.negated

        def _exists(ctx):
            rows = runner(expr.query, ctx)
            found = bool(rows) if rows is not None else False
            return (not found) if negated else found

        return _exists, TypeSpec(DataType.BOOLEAN)

    # -- 表达式里的聚合收集 ---------------------------------------------

    def reset_aggregates(self) -> None:
        self._agg_slots.clear()
        self._agg_specs.clear()
        self._agg_base = 0

    @property
    def agg_specs(self) -> list[AggSpec]:
        return self._agg_specs

    # -- 语句绑定 -------------------------------------------------------

    def bind_table_schema(self, refs) -> tuple[Schema, list[Scan]]:
        schema_cols: list[SchemaColumn] = []
        scans: list[Scan] = []
        for ref in refs:
            table = self.catalog.get_table(ref.name)
            alias = (ref.alias or table.name).lower()
            columns = [
                OutputColumn(col.name, col.type) for col in table.columns
            ]
            scans.append(Scan(columns=columns, table=table.name, alias=alias))
            for col in table.columns:
                schema_cols.append(SchemaColumn(alias, col.name, col.type))
        return Schema(schema_cols), scans

    # -- SELECT 绑定 -----------------------------------------------------

    def bind_ctes(self, ctes) -> None:
        """把一个 ``WITH`` 列表按书写顺序绑进当前作用域。"""
        saved = self.ctes
        self.ctes = dict(self.ctes)
        try:
            for cte in ctes:
                name = cte.name.lower()
                if name in self.ctes:
                    raise BinderError(f"CTE {cte.name!r} 重复定义")
                node, cols = self.bind_select(cte.query)
                self.ctes[name] = (node, cols)
        except Exception:
            self.ctes = saved
            raise

    def bind_query(self, query) -> tuple[PlanNode, list[OutputColumn]]:
        """绑定一条带 ``WITH`` 的顶层查询。

        CTE 注册表在成功后保留，供执行阶段的子查询复用同一个作用域；
        执行器每次执行语句都会新建 Binder，所以不会泄漏到下一条语句。
        """
        self.bind_ctes(query.ctes)
        return self.bind_select(query.select)

    def bind_returning(self, items, schema) -> tuple[list, list[OutputColumn]]:
        """把 ``RETURNING`` 列表编译成 (表达式函数, 输出列)。

        表达式在 DML 的行上求值：INSERT/UPDATE 用新行，DELETE 用旧行。
        ``*`` 直接展开成表的全部列，顺序与表定义一致。
        """
        if not items:
            return [], []
        fns: list = []
        cols: list[OutputColumn] = []
        for item in items:
            if isinstance(item.expr, Star):
                if item.expr.table:
                    wanted = item.expr.table.lower()
                    picked = [c for c in schema.columns if c.qualifier == wanted]
                    if not picked:
                        raise TableNotFoundError(
                            f"没有名为 {item.expr.table} 的表"
                        )
                else:
                    picked = list(schema.columns)
                for col in picked:
                    index = schema.resolve(col.qualifier, col.name)
                    fns.append(lambda ctx, i=index: ctx.row[i])
                    cols.append(OutputColumn(col.name, col.type))
                continue

            fn, type_spec = self.compile(item.expr, schema, allow_agg=False)
            fns.append(fn)
            name = item.alias
            if name is None:
                if isinstance(item.expr, ColumnRef):
                    name = item.expr.name
                else:
                    name = render_expr(item.expr)
            cols.append(
                OutputColumn(name.lower() if item.alias else name, type_spec)
            )
        return fns, cols

    def bind_select(self, select) -> tuple[PlanNode, list[OutputColumn]]:
        if isinstance(select, CompoundSelect):
            return self._bind_compound(select)
        self.reset_aggregates()

        node, schema = self._bind_from(select.from_)

        if (
            select.where is not None
            and len(select.from_) == 1
            and not select.from_[0].joins
        ):
            lookup = self._try_index_lookup(select.where, schema, node)
            if lookup is not None:
                node = lookup
            else:
                range_node = self._try_index_range(select.where, schema, node)
                predicate, _ = self.compile(select.where, schema, allow_agg=False)
                child = range_node if range_node is not None else node
                node = Filter(
                    child=child, predicate=predicate, columns=child.columns
                )
        elif select.where is not None:
            predicate, _ = self.compile(select.where, schema, allow_agg=False)
            node = Filter(child=node, predicate=predicate, columns=node.columns)

        has_agg = (
            bool(select.group_by)
            or any(_contains_aggregate(item.expr) for item in select.items)
            or (select.having is not None and _contains_aggregate(select.having))
            or any(_contains_aggregate(item.expr) for item in select.order_by)
        )

        column_group: dict[int, str] = {}
        expr_group: dict[str, str] = {}
        if has_agg:
            node, out_schema, (column_group, expr_group) = self._bind_aggregate(
                node, schema, select
            )
            if select.having is not None:
                having_expr = self._rewrite_group_refs(
                    select.having, schema, column_group, expr_group
                )
                having_fn, _ = self.compile(having_expr, out_schema, allow_agg=True)
                node = Filter(
                    child=node, predicate=having_fn, columns=node.columns
                )
        else:
            out_schema = schema

        proj_fns: list[Callable[[EvalContext], Any]] = []
        proj_cols: list[OutputColumn] = []
        for item in select.items:
            if isinstance(item.expr, Star):
                if item.expr.table:
                    wanted = item.expr.table.lower()
                    picked = [c for c in out_schema.columns if c.qualifier == wanted]
                    if not picked:
                        raise TableNotFoundError(f"没有名为 {item.expr.table} 的表")
                else:
                    picked = list(out_schema.columns)
                for col in picked:
                    index = out_schema.resolve(col.qualifier, col.name)
                    proj_fns.append(lambda ctx, i=index: ctx.row[i])
                    proj_cols.append(OutputColumn(col.name, col.type))
                continue

            fn, type_spec = self.compile(
                self._rewrite_group_refs(item.expr, schema, column_group, expr_group)
                if has_agg
                else item.expr,
                out_schema,
                allow_agg=has_agg,
            )
            proj_fns.append(fn)
            name = item.alias
            if name is None:
                if isinstance(item.expr, ColumnRef):
                    name = item.expr.name
                else:
                    from ..sql.ast import render_expr

                    name = render_expr(item.expr)
            proj_cols.append(OutputColumn(name.lower() if item.alias else name, type_spec))

        # ORDER BY：先尝试按输出别名解析，否则编译成隐藏列
        extra_fns: list[Callable[[EvalContext], Any]] = []
        sort_keys: list[tuple[Callable[[EvalContext], Any], bool]] = []
        out_names = [c.name.lower() for c in proj_cols]
        for item in select.order_by:
            index: Optional[int] = None
            if isinstance(item.expr, ColumnRef) and item.expr.table is None:
                lowered = item.expr.name.lower()
                if lowered in out_names:
                    index = out_names.index(lowered)
            if index is None:
                sort_expr = (
                    self._rewrite_group_refs(
                        item.expr, schema, column_group, expr_group
                    )
                    if has_agg
                    else item.expr
                )
                fn, _ = self.compile(sort_expr, out_schema, allow_agg=has_agg)
                extra_fns.append(fn)
                index = len(proj_fns) + len(extra_fns) - 1
            sort_keys.append((lambda ctx, i=index: ctx.row[i], item.descending))

        project = Project(
            child=node,
            exprs=proj_fns + extra_fns,
            columns=proj_cols
            + [OutputColumn(f"$sort{i}", TypeSpec(DataType.TEXT)) for i in range(len(extra_fns))],
        )
        node = project

        if select.distinct:
            node = Distinct(child=node, columns=proj_cols, limit_columns=len(proj_cols))

        if sort_keys:
            node = Sort(child=node, keys=sort_keys, columns=node.columns)

        limit_value = self._limit_value(select.limit, "LIMIT")
        offset_value = self._limit_value(select.offset, "OFFSET")
        if limit_value is not None or offset_value:
            node = Limit(
                child=node,
                limit=limit_value,
                offset=offset_value or 0,
                columns=node.columns,
            )

        return node, proj_cols

    def _bind_compound(self, select: CompoundSelect) -> tuple[PlanNode, list[OutputColumn]]:
        parts = select.parts
        node, out_cols = self.bind_select(parts[0])
        for i, part in enumerate(parts[1:]):
            op = select.ops[i] if i < len(select.ops) else "UNION"
            right_node, right_cols = self.bind_select(part)
            if len(right_cols) != len(out_cols):
                raise TypeMismatchError(
                    f"{op} 两侧的列数不同",
                    detail=f"左侧 {len(out_cols)} 列，右侧 {len(right_cols)} 列",
                )
            merged_cols: list[OutputColumn] = []
            for left_col, right_col in zip(out_cols, right_cols):
                try:
                    merged_type = common_type(left_col.type, right_col.type)
                except TypeMismatchError as exc:
                    raise TypeMismatchError(
                        f"{op} 两侧的列类型不兼容",
                        detail=(
                            f"{left_col.name}: {left_col.type.name} 与 "
                            f"{right_col.type.name}"
                        ),
                    ) from exc
                merged_cols.append(OutputColumn(left_col.name, merged_type))
            out_cols = merged_cols
            node = SetOp(
                left=node,
                right=right_node,
                op=op,
                all=select.all_flags[i],
                columns=list(out_cols),
            )

        if select.order_by:
            sort_keys = []
            names = [c.name.lower() for c in out_cols]
            for item in select.order_by:
                index: Optional[int] = None
                if isinstance(item.expr, ColumnRef) and item.expr.table is None:
                    lowered = item.expr.name.lower()
                    if lowered in names:
                        index = names.index(lowered)
                elif isinstance(item.expr, Literal) and isinstance(item.expr.value, int):
                    ordinal = item.expr.value
                    if 1 <= ordinal <= len(out_cols):
                        index = ordinal - 1
                if index is None:
                    raise NotSupportedError(
                        "集合查询的 ORDER BY 只支持输出列名或列序号",
                        hint="先给 SELECT 的输出列起别名，再按别名排序",
                    )
                sort_keys.append((lambda ctx, i=index: ctx.row[i], item.descending))
            node = Sort(child=node, keys=sort_keys, columns=node.columns)

        limit_value = self._limit_value(select.limit, "LIMIT")
        offset_value = self._limit_value(select.offset, "OFFSET")
        if limit_value is not None or offset_value:
            node = Limit(
                child=node,
                limit=limit_value,
                offset=offset_value or 0,
                columns=node.columns,
            )
        return node, out_cols

    def _limit_value(self, value, clause: str) -> Optional[int]:
        """把 ``LIMIT`` / ``OFFSET`` 的整数或 ``$n`` 统一成 int（None 表示不限）。"""
        if value is None:
            return None
        if isinstance(value, ParamRef):
            index = value.index - 1
            if index < 0 or index >= len(self.params):
                raise UndefinedParameterError(
                    f"参数 ${value.index} 没有绑定值",
                    detail=f"{clause} 引用了这个参数",
                )
            bound = self.params[index]
            if bound is None:
                return None
            if isinstance(bound, bool) or not isinstance(bound, int):
                raise TypeMismatchError(
                    f"{clause} 需要整数",
                    detail=f"实际是 {type(bound).__name__}",
                )
            if bound < 0:
                raise TypeMismatchError(
                    f"{clause} 不能为负数",
                    detail=f"实际是 {bound}",
                )
            return bound
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeMismatchError(f"{clause} 需要整数")
        return value

    def _try_index_lookup(
        self, where, schema: Schema, scan: PlanNode
    ) -> Optional[PlanNode]:
        """识别 ``col = 常量`` 这种能走索引的等值条件。

        只处理单列等值，且要求该列上确实有索引。更复杂的条件留给全表扫描 +
        Filter —— 与其做一个半吊子的索引匹配，不如明确只支持一种模式。
        """
        if not isinstance(scan, Scan):
            return None
        if not isinstance(where, BinaryOp) or where.op != "=":
            return None

        left, right = where.left, where.right
        if isinstance(right, ColumnRef) and isinstance(left, Literal):
            left, right = right, left
        if not isinstance(left, ColumnRef) or not isinstance(right, Literal):
            return None
        if right.value is None:
            return None

        table = self.catalog.get_table(scan.table)
        index = table.index_for_columns((left.name.lower(),))
        if index is None:
            return None

        column = table.column(left.name.lower())
        try:
            value = column.type.coerce(right.value)
        except ValueError:
            return None

        return IndexLookup(
            columns=scan.columns,
            table=scan.table,
            alias=scan.alias,
            index_name=index.name,
            index_columns=index.columns,
            key=(value,),
        )

    def _try_index_range(
        self, where, schema: Schema, scan: PlanNode
    ) -> Optional[PlanNode]:
        """识别单列范围条件：``>`` / ``>=`` / ``<`` / ``<=`` / ``BETWEEN``。

        只在整条 WHERE 就是这一个范围条件时生效；``a > 1 AND b = 2`` 这种
        留给全表扫描 + Filter。原因和等值查找一样：只做能保证正确的匹配。
        """
        if not isinstance(scan, Scan):
            return None

        low = high = None
        low_inclusive = high_inclusive = True
        column_ref: Optional[ColumnRef] = None

        if isinstance(where, Between):
            if where.negated or not isinstance(where.operand, ColumnRef):
                return None
            if not isinstance(where.low, Literal) or not isinstance(where.high, Literal):
                return None
            if where.low.value is None or where.high.value is None:
                return None
            column_ref = where.operand
            low, high = where.low.value, where.high.value
        elif isinstance(where, BinaryOp) and where.op in (">", ">=", "<", "<="):
            left, right = where.left, where.right
            op = where.op
            if isinstance(left, Literal) and isinstance(right, ColumnRef):
                left, right = right, left
                op = {"<": ">", ">": "<", "<=": ">=", ">=": "<="}[op]
            if not isinstance(left, ColumnRef) or not isinstance(right, Literal):
                return None
            if right.value is None:
                return None
            column_ref = left
            if op == ">":
                low, low_inclusive = right.value, False
            elif op == ">=":
                low = right.value
            elif op == "<":
                high, high_inclusive = right.value, False
            else:
                high = right.value
        else:
            return None

        table = self.catalog.get_table(scan.table)
        if not table.indexes:
            return None
        index = table.index_for_columns((column_ref.name.lower(),))
        if index is None or len(index.columns) != 1:
            return None
        column = table.column(column_ref.name.lower())
        try:
            low_key = (column.type.coerce(low),) if low is not None else None
            high_key = (column.type.coerce(high),) if high is not None else None
        except ValueError:
            return None

        return IndexRange(
            columns=scan.columns,
            table=scan.table,
            alias=scan.alias,
            index_name=index.name,
            index_columns=index.columns,
            low=low_key,
            high=high_key,
            low_inclusive=low_inclusive,
            high_inclusive=high_inclusive,
        )

    def _equi_keys(
        self, on, merged: Schema, left_width: int
    ) -> tuple[tuple[tuple[int, int], ...], tuple[str, ...]]:
        """从 ``ON`` 里提取左右两侧的等值列对，供哈希连接使用。

        只提取顶层 ``AND`` 的连接项；``OR`` 里的等值不能单独当哈希键，
        否则会把本应匹配的行漏掉。剩余谓词仍由完整 ``on`` 复核。
        """
        if on is None:
            return (), ()
        keys: list[tuple[int, int]] = []
        labels: list[str] = []
        for expr in _conjuncts(on):
            if not isinstance(expr, BinaryOp) or expr.op != "=":
                continue
            left, right = expr.left, expr.right
            if not isinstance(left, ColumnRef) or not isinstance(right, ColumnRef):
                continue
            li = merged.resolve(left.table, left.name)
            ri = merged.resolve(right.table, right.name)
            if li >= left_width > ri:
                li, ri = ri, li
            if not (li < left_width <= ri):
                continue
            lcol, rcol = merged.columns[li], merged.columns[ri]
            keys.append((li, ri - left_width))
            labels.append(f"{lcol.qualifier}.{lcol.name} = {rcol.qualifier}.{rcol.name}")
        return tuple(keys), tuple(labels)

    def _bind_from(self, refs) -> tuple[PlanNode, Schema]:
        if not refs:
            return Values(rows=[()], columns=[]), Schema([])

        node: Optional[PlanNode] = None
        schema = Schema([])
        for ref in refs:
            sub_node, sub_schema = self._bind_table_ref(ref)
            if node is None:
                node, schema = sub_node, sub_schema
            else:
                merged = schema.concat(sub_schema)
                node = Join(
                    left=node,
                    right=sub_node,
                    kind="INNER",
                    on=None,
                    columns=[OutputColumn(c.name, c.type) for c in merged.columns],
                )
                schema = merged
        assert node is not None
        return node, schema

    def _source_for(
        self, name: str, alias: Optional[str]
    ) -> tuple[PlanNode, Schema, list[OutputColumn]]:
        """解析 ``FROM`` 里的一个数据源：优先查 CTE，再查目录表。"""
        entry = self.ctes.get(name.lower())
        if entry is not None:
            child, cols = entry
            effective = (alias or name).lower()
            node = CteScan(child=child, columns=list(cols))
            schema = Schema(
                [SchemaColumn(effective, col.name, col.type) for col in cols]
            )
            return node, schema, list(cols)

        table = self.catalog.get_table(name)
        effective = (alias or table.name).lower()
        columns = [OutputColumn(col.name, col.type) for col in table.columns]
        node = Scan(columns=columns, table=table.name, alias=effective)
        schema = Schema(
            [SchemaColumn(effective, col.name, col.type) for col in table.columns]
        )
        return node, schema, columns

    def _bind_table_ref(self, ref) -> tuple[PlanNode, Schema]:
        node, schema, columns = self._source_for(ref.name, ref.alias)

        for join in ref.joins:
            right_node, right_schema, right_cols = self._source_for(
                join.table.name, join.table.alias
            )
            merged = schema.concat(right_schema)
            on_fn = None
            if join.on is not None:
                on_fn, _ = self.compile(join.on, merged, allow_agg=False)
            hash_keys, hash_labels = ((), ())
            if join.kind in ("INNER", "LEFT"):
                hash_keys, hash_labels = self._equi_keys(
                    join.on, merged, len(schema)
                )
            node = Join(
                left=node,
                right=right_node,
                kind=join.kind,
                on=on_fn,
                columns=[OutputColumn(c.name, c.type) for c in merged.columns],
                hash_keys=hash_keys,
                hash_labels=hash_labels,
            )
            schema = merged
        return node, schema

    def _bind_aggregate(
        self, child: PlanNode, schema: Schema, select
    ) -> tuple[PlanNode, Schema, tuple[dict[int, str], dict[str, str]]]:
        group_fns: list[Callable[[EvalContext], Any]] = []
        group_cols: list[SchemaColumn] = []
        group_labels: list[str] = []
        # 分组的列 -> 聚合输出行里的物理下标；表达式 -> 合成列名。
        # SELECT/HAVING/ORDER BY 里的同形表达式会被改写成对合成列的引用。
        column_group: dict[int, str] = {}
        expr_group: dict[str, str] = {}
        for i, gexpr in enumerate(select.group_by):
            synthetic = f"$g{i}"
            if isinstance(gexpr, ColumnRef):
                index = schema.resolve(gexpr.table, gexpr.name)
                col = schema.columns[index]
                group_cols.append(SchemaColumn(None, synthetic, col.type))
                group_fns.append(lambda ctx, i=index: ctx.row[i])
                group_labels.append(col.name)
                column_group[index] = synthetic
                expr_group.setdefault(col.name.lower(), synthetic)
                expr_group.setdefault(render_expr(gexpr).lower(), synthetic)
                continue
            fn, spec = self.compile(gexpr, schema, allow_agg=False)
            group_cols.append(SchemaColumn(None, synthetic, spec))
            group_fns.append(fn)
            group_labels.append(render_expr(gexpr))
            expr_group[render_expr(gexpr).lower()] = synthetic

        # 第一遍：用原 schema 编译，触发聚合函数注册（结果丢弃）。
        # ORDER BY 里如果是 SELECT 的输出别名，在这里解析会失败（输入 schema
        # 里没有这一列），留给输出层处理即可。
        aliases = {item.alias.lower() for item in select.items if item.alias}
        for item in select.items:
            if not isinstance(item.expr, Star):
                self.compile(item.expr, schema, allow_agg=True)
        if select.having is not None:
            self.compile(select.having, schema, allow_agg=True)
        for item in select.order_by:
            expr = item.expr
            if (
                isinstance(expr, ColumnRef)
                and expr.table is None
                and expr.name.lower() in aliases
            ):
                continue
            self.compile(expr, schema, allow_agg=True)

        agg_schema = Schema(
            group_cols
            + [
                SchemaColumn("$agg", f"$agg{i}", spec.result_type)
                for i, spec in enumerate(self.agg_specs)
            ]
        )
        self._agg_base = len(group_cols)
        agg_node = Aggregate(
            child=child,
            group_exprs=group_fns,
            group_labels=group_labels,
            aggs=list(self.agg_specs),
            columns=[OutputColumn(c.name, c.type) for c in group_cols]
            + [OutputColumn(f"$agg{i}", s.result_type) for i, s in enumerate(self.agg_specs)],
        )
        return agg_node, agg_schema, (column_group, expr_group)

    def _rewrite_group_refs(
        self,
        expr: Expr,
        base_schema: Schema,
        column_group: dict[int, str],
        expr_group: dict[str, str],
    ) -> Expr:
        """把 SELECT/HAVING/ORDER BY 里与 GROUP BY 同形的表达式换成合成列。

        聚合输出行里只有分组键和聚合结果，原表的列已经不存在了；这一步
        让 ``SELECT a % 2 ... GROUP BY a % 2`` 这类查询能按分组键读结果。
        没有发生替换的子树原样返回，保证聚合函数对象还是同一个（聚合槽位
        是按对象身份登记的）。
        """
        if isinstance(expr, ColumnRef):
            try:
                index = base_schema.resolve(expr.table, expr.name)
            except (ColumnNotFoundError, AmbiguousColumnError):
                index = None
            if index is not None and index in column_group:
                return ColumnRef(None, column_group[index])
        key = render_expr(expr).lower()
        if key in expr_group:
            return ColumnRef(None, expr_group[key])
        if not dataclasses.is_dataclass(expr):
            return expr

        changed = False
        updates: dict[str, Any] = {}
        for field in dataclasses.fields(expr):
            value = getattr(expr, field.name)
            if isinstance(value, Expr):
                new = self._rewrite_group_refs(
                    value, base_schema, column_group, expr_group
                )
                if new is not value:
                    updates[field.name] = new
                    changed = True
            elif isinstance(value, list):
                items = []
                for item in value:
                    if isinstance(item, Expr):
                        new_item = self._rewrite_group_refs(
                            item, base_schema, column_group, expr_group
                        )
                        if new_item is not item:
                            changed = True
                        items.append(new_item)
                    else:
                        items.append(item)
                if changed:
                    updates[field.name] = items
        if not changed:
            return expr
        return dataclasses.replace(expr, **updates)


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


def _bool_test(fn: Callable[[Any], Any]) -> Callable[[Any], bool]:
    """把"可能返回 NULL 的表达式"包装成严格布尔判断：NULL 视为假。"""

    def _test(ctx):
        value = fn(ctx)
        return False if value is None else bool(value)

    return _test


def _equality_test(left_fn, right_fn):
    """简单式 CASE 的比较：``CASE x WHEN v`` 等价于 ``x = v``，NULL 不匹配。"""

    def _test(ctx):
        a = left_fn(ctx)
        b = right_fn(ctx)
        if a is None or b is None:
            return False
        return a == b

    return _test


def _merge_branch_types(current, candidate):
    if current is None:
        return candidate
    if current.unknown:
        return candidate
    if candidate.unknown:
        return current
    if current.kind == candidate.kind:
        return current
    if current.is_numeric and candidate.is_numeric:
        return TypeSpec(DataType.FLOAT)
    raise TypeMismatchError(
        "CASE 各分支的返回类型必须一致",
        detail=f"{current.sql} 与 {candidate.sql}",
        hint="用 CAST 把各分支统一到同一个类型",
    )


def _conjuncts(expr: Expr) -> Iterator[Expr]:
    """展开顶层 ``AND``；不进入 ``OR`` / ``NOT`` 内部。"""
    if isinstance(expr, BinaryOp) and expr.op == "AND":
        yield from _conjuncts(expr.left)
        yield from _conjuncts(expr.right)
    else:
        yield expr


def _contains_aggregate(expr: Expr) -> bool:
    if isinstance(expr, FunctionCall) and expr.name.upper() in AGGREGATE_FUNCTIONS:
        return True
    return any(_contains_aggregate(c) for c in _children(expr))


def _children(expr: Expr) -> list[Expr]:
    if isinstance(expr, UnaryOp):
        return [expr.operand]
    if isinstance(expr, BinaryOp):
        return [expr.left, expr.right]
    if isinstance(expr, IsNull):
        return [expr.operand]
    if isinstance(expr, InList):
        return [expr.operand, *expr.items]
    if isinstance(expr, Between):
        return [expr.operand, expr.low, expr.high]
    if isinstance(expr, Like):
        return [expr.operand, expr.pattern]
    if isinstance(expr, FunctionCall):
        return list(expr.args)
    if isinstance(expr, Cast):
        return [expr.operand]
    if isinstance(expr, CaseWhen):
        kids: list[Expr] = []
        if expr.operand is not None:
            kids.append(expr.operand)
        for cond, value in expr.branches:
            kids.append(cond)
            kids.append(value)
        if expr.otherwise is not None:
            kids.append(expr.otherwise)
        return kids
    return []


def _like_to_regex(pattern: str) -> str:
    out = []
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re_escape(ch))
    return "".join(out)


def re_escape(ch: str) -> str:
    import re

    return re.escape(ch)


_NUMERIC_KINDS = frozenset({DataType.INT, DataType.BIGINT, DataType.FLOAT})
_TEXT_KINDS = frozenset({DataType.TEXT, DataType.VARCHAR})


def _comparable_types(left: TypeSpec, right: TypeSpec) -> bool:
    """比较运算两侧的类型是否兼容。

    数值之间可以互比（INT/BIGINT/FLOAT）；TEXT 与 VARCHAR 可以互比；
    ``unknown``（SQL NULL）与任何类型兼容。其它情况要求类型完全相同。
    ``WHERE id = 'oops'`` 这类错误应该在绑定阶段报出来，而不是等执行到某一行
    才抛异常或者悄悄返回 False。
    """
    if left.unknown or right.unknown:
        return True
    if left.kind is right.kind:
        return True
    if left.kind in _NUMERIC_KINDS and right.kind in _NUMERIC_KINDS:
        return True
    if left.kind in _TEXT_KINDS and right.kind in _TEXT_KINDS:
        return True
    return False


def _make_comparison(op: str, left_fn, right_fn, lt: TypeSpec, rt: TypeSpec):
    def _cmp(a, b):
        if op == "=":
            return a == b
        if op == "<>":
            return a != b
        if op == "<":
            return a < b
        if op == "<=":
            return a <= b
        if op == ">":
            return a > b
        return a >= b

    def _compare(ctx):
        a, b = left_fn(ctx), right_fn(ctx)
        if a is None or b is None:
            return None
        try:
            return _cmp(a, b)
        except TypeError as exc:
            raise TypeMismatchError(
                f"无法比较 {lt.sql} 与 {rt.sql}",
                detail=str(exc),
                hint="用 CAST 显式转换后再比较",
            ) from exc

    return _compare


def _one_arg(name: str, fn, result_type: TypeSpec, *, require_str: bool = False, require_num: bool = False):
    def build(args):
        if len(args) != 1:
            raise TypeMismatchError(f"{name} 需要 1 个参数")
        arg_fn, arg_type = args[0]
        if require_str and arg_type.kind not in (DataType.TEXT, DataType.VARCHAR):
            raise TypeMismatchError(f"{name} 需要字符串参数", detail=f"实际 {arg_type.sql}")
        if require_num and not arg_type.is_numeric:
            raise TypeMismatchError(f"{name} 需要数值参数", detail=f"实际 {arg_type.sql}")

        def _call(ctx):
            value = arg_fn(ctx)
            if value is None:
                return None
            return fn(value)

        return _call, result_type

    return build


_SCALAR_FUNCTIONS = {
    "LENGTH": _one_arg("LENGTH", lambda v: len(str(v)), TypeSpec(DataType.INT), require_str=True),
    "UPPER": _one_arg("UPPER", lambda v: str(v).upper(), TypeSpec(DataType.TEXT), require_str=True),
    "LOWER": _one_arg("LOWER", lambda v: str(v).lower(), TypeSpec(DataType.TEXT), require_str=True),
    "ABS": _one_arg("ABS", abs, TypeSpec(DataType.INT), require_num=True),
}


def _coalesce(args):
    if not args:
        raise TypeMismatchError("COALESCE 至少需要 1 个参数")
    fns = [a[0] for a in args]
    result_type = args[0][1]
    for _, candidate_type in args[1:]:
        try:
            result_type = common_type(result_type, candidate_type)
        except TypeMismatchError as exc:
            raise TypeMismatchError(
                "COALESCE 各参数的类型必须兼容",
                detail=f"{result_type.sql} 与 {candidate_type.sql}",
                hint="用 CAST 把参数统一到同一个类型",
            ) from exc

    def _call(ctx):
        for fn in fns:
            value = fn(ctx)
            if value is not None:
                return value
        return None

    return _call, result_type


_SCALAR_FUNCTIONS["COALESCE"] = _coalesce


_DISPATCH = {
    Literal: Binder._literal,
    ParamRef: Binder._param,
    ColumnRef: Binder._column,
    Star: Binder._star,
    UnaryOp: Binder._unary,
    BinaryOp: Binder._binary,
    IsNull: Binder._is_null,
    InList: Binder._in_list,
    InSubquery: Binder._in_subquery,
    Between: Binder._between,
    Like: Binder._like,
    FunctionCall: Binder._function,
    Cast: Binder._cast,
    CaseWhen: Binder._case,
    ScalarSubquery: Binder._scalar_subquery,
    Exists: Binder._exists,
}


__all__ = [
    "Binder",
    "Schema",
    "SchemaColumn",
    "AggSpec",
    "AggregateAccumulatorGroup",
    "AGGREGATE_FUNCTIONS",
]
