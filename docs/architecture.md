# LumenDB 架构设计

本文描述 LumenDB 的整体分层、每层的职责边界、模块之间的契约，以及关键设计取舍。存储格式的字节级细节见 `storage-format.md`。

## 1. 分层与依赖方向

依赖是严格单向的，下层不知道上层的存在：

```
server  →  plan  →  engine  →  storage  →  pager (OS file)
             ↑                    ↑
             └──── sql ───────────┘ (sql 只被 plan 依赖)
```

| 层 | 职责 | 不允许做的事 |
|---|---|---|
| `server/` | 协议编解码、连接生命周期、会话事务状态 | 直接碰页或文件 |
| `sql/` | 文本 → token → AST | 名字解析、类型推断（留给 binder） |
| `plan/` | AST → 绑定后的逻辑计划 → 物理计划 | 执行、I/O |
| `engine/` | 火山模型算子，驱动 storage 的读写 | 解析 SQL 文本 |
| `storage/` | 页、B+ 树、堆表、WAL、事务、锁 | 理解 SQL 语义 |

## 2. 存储层

### 2.1 页

数据库文件被切成 4096 字节的页。页号从 0 开始，页 0 保留为元数据页（魔数、版本、页大小、根页指针）。

堆表使用 **slotted page**：页头 + 槽目录（向下增长）+ 记录区（自页尾向上增长）。槽目录的偏移在删除后保留为 tombstone（`length = 0`），因此 RID 永不失效——这是 MVCC 版本链能够用 RID 寻址的前提。

### 2.2 Buffer Pool

固定容量的帧池，被 `pin` 的页不会被淘汰。淘汰策略为 LRU，脏页淘汰时先写 WAL 再写数据页（WAL 规则）。每个页记录 `pageLSN`，恢复时用来跳过已落盘的日志。

### 2.3 WAL

日志是**物理页日志**（physiological 的简化版）：记录"某页某偏移的旧值 → 新值"。选择物理日志而非逻辑日志的原因：

- redo 幂等，崩溃后可以无条件重放；
- undo 不需要反向推导 SQL 语义；
- 恢复正确性可以用 before/after image 直接断言。

代价是日志体积大，组提交与分段回收留作后续优化（见 roadmap）。

### 2.4 恢复

启动时：

1. **analysis**：从最近 checkpoint 扫到日志尾，重建活跃事务表与脏页表；
2. **redo**：从 checkpoint 的 `redo_lsn` 起重放所有日志，`pageLSN >= lsn` 的页跳过；
3. **undo**：对活跃事务（loser）按 LSN 逆序应用 before image，并写 CLR 防止二次回滚。

恢复结束后写一个 checkpoint，截断旧日志。

### 2.5 事务与并发

- 单调递增的 `xid`，`xmin/xmax` 写在每行头部；
- 快照 = (自己的 xid, 活跃事务列表)。可见性：`xmin` 已提交且不在快照活跃集，且 `xmax` 未提交或已回滚；
- 写写冲突用行级锁（`S`/`X`），等待图 DFS 检测环，成环时中止发起等待的事务；
- 隔离级别固定为**快照隔离**，不提供 `SET TRANSACTION ISOLATION LEVEL`
  切换（会明确报 `NotSupportedError`）。既然只有一种语义，就不假装有多种。

### 2.6 回收（VACUUM）

`UPDATE` 写新版本、`DELETE` 只打 `xmax`，所以表会持续膨胀。`VACUUM [table]`
负责回收：

1. 在**没有并发事务**的前提下（本引擎同一时刻只允许一个活跃事务，这条
   前提在进程内天然成立）扫描整表，保留当前快照可见的那一版；
2. 把存活行重排成"每行只有一条根元组"：`root/newest` 指向自己、`prev` 为空；
   页内顺序紧凑，死版本的空间被真正收回；
3. 重排会改变所有 RID，所以紧接着**重建该表的所有索引**；
4. 每一页的改写都写一条**整页 before/after image**，崩溃后恢复能重放，
   不会出现"VACUUM 之后行凭空消失/复活"。

文件本身不会缩小（和 PostgreSQL 的普通 `VACUUM` 一样）；空页留在页链上，
扫描会跳过。没有 autovacuum、没有增量回收、没有 oldest-xmin 水位——
一旦放开并发事务，这些就是下一件必须做的事，写在 `docs/sql-support.md` 里。

## 3. SQL 与计划

### 3.1 解析

手写词法分析器（不引入 parser generator，避免隐藏的语义）。递归下降，优先级用绑定强度表处理二元表达式。语法错误带位置信息。

### 3.2 绑定

绑定层做三件事，都不在"执行时"做：

1. **名字解析**：把 `ColumnRef`（可能带表前缀）解析成"这一行元组里的第几列"。
   重复列名报 `AmbiguousColumnError`，未知列报 `ColumnNotFoundError` 并列出可用列；
2. **类型推导**：每个表达式都有一个 `TypeSpec`。算术要求数值、比较要求可比的
   类型、聚合参数要求数值——错在编译期报出来，而不是执行到一半才炸；
3. **编译成闭包**：`compile(expr, schema)` 返回 `f(ctx) -> value`。名字解析的
   结果被固化成下标，执行时不再走语法树、不再查字典。

聚合的处理值得一提：聚合函数被注册到 `AggSpec` 列表并分配一个槽位，
`Aggregate` 节点的输出行 = `分组键... + 聚合结果...`，因此上层 `Project`
里对 `COUNT(*)` 的引用就是一次下标读取。`GROUP BY` 目前只接受列引用
（表达式分组会报 `NotSupportedError`），这是刻意的边界——与其支持一半，
不如明确拒绝。

### 3.3 计划与优化

老实说：**当前没有代价模型**。规则只有两条能被 `EXPLAIN` 看见：

- 单表查询、`WHERE` 形如 `col = 常量`、且该列上有索引 → `IndexLookup`；
- `WHERE` 是单列范围条件（`>` / `>=` / `<` / `<=` / `BETWEEN`）→ `IndexRangeScan`，
  范围谓词仍会在上层 `Filter` 再判一次，保证边界与剩余条件正确；
- 其余一律 `SeqScan + Filter`。

范围扫描按"索引列前缀"比较，忽略 `KeyCodec` 追加的 RID；否则
`col > 10` 会把 RID 更大的 `(10, ...)` 当成大于边界，结果里混进 10 本身。

`INNER JOIN` / `LEFT JOIN` 的顶层 `AND` 等值条件走 **hash join**：右侧建哈希表，
左侧流式探测；没有等值键（逗号连接、`OR` 条件）时退回 nested loop。`NULL` 键
按 SQL 语义不参与匹配，重复键会展开成多行，剩余谓词仍由完整的 `ON` 复核。
连接顺序枚举与代价模型仍未实现——`EXPLAIN` 输出的是真实执行的计划，不是理想计划。

## 4. 执行

火山模型，每个算子是生成器，一次产出一行：

- `Values`：常量行（`SELECT 1`、`INSERT ... VALUES` 的表达式求值）；
- `Scan` / `IndexLookup`：堆表顺序扫描 / 索引点查；
- `Filter`：谓词为真才放行（`NULL` 视为假）；
- `Join`：有等值键时 `HashJoin`（右侧物化建哈希表，左侧流式探测）；否则
  nested loop（右侧物化，左侧流式）。`LEFT JOIN` 无匹配时补 NULL；
- `Aggregate`：无 `GROUP BY` 时输出一行（空表也输出 `COUNT=0`）；
- `Project`：求值输出列，可附加 ORDER BY 需要的隐藏列；
- `Distinct`：按前 N 列去重（隐藏列不参与）；
- `SetOp`：`UNION` / `UNION ALL`，逐行拼接；`UNION` 用集合去重，
  `UNION ALL` 保留重复，`ORDER BY` / `LIMIT` 由外层节点处理；
- `Sort`：从最后一个键往前依次稳定排序，支持多键；
- `Limit`：跳 `offset`，取 `limit`。

`NULL` 的排序位置固定为"最后"（ASC 与 DESC 都是），不做 `NULLS FIRST/LAST` 语法。

## 5. 协议

实现 PostgreSQL v3：

- 启动：`StartupMessage` → `AuthenticationOk`（trust，不校验密码）→
  `ParameterStatus*` → `BackendKeyData` → `ReadyForQuery`；客户端发
  `SSLRequest` 时回 `N`（不做 TLS），让它退回明文；
- 简单查询：`Query` → `RowDescription` / `DataRow*` / `CommandComplete` / `ReadyForQuery`；
- 扩展查询：`Parse` / `Bind` / `Describe` / `Execute` / `Sync`。`Parse` 记录客户端
  声明的参数类型 OID 并**在这里就做语法检查**；`Bind` 按格式码（文本 0 / 二进制 1）
  把参数解码成 Python 值；绑定阶段把 `$n` **折成字面量**，所以执行计划里没有
  "参数数组"，代价是没有计划缓存；
- 参数：支持 `INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/TEXT/VARCHAR` 的文本与二进制格式；
  OID 0（客户端未声明）时按字面量推断。参数个数与 `Bind` 不一致报 `08P01`，
  `$n` 没有值报 `42P02`；
- `Describe` 语句返回 `ParameterDescription`（真实 OID 列表）；结果列要等 `Bind`
  之后 `Describe` 门户才能确定，此时回 `RowDescription`；
- 错误：`ErrorResponse` 带 `SQLSTATE`、`MESSAGE`、`DETAIL`、`HINT`，
  客户端（psql / JDBC）能按标准字段展示；
- **会话事务**：`BEGIN` 创建的事务挂在 `Session` 上，而不是共用 `Database`
  的单个 `_txn`。否则 A 连接 `BEGIN` 后，B 连接的查询会直接加入 A 的事务，
  读到未提交数据。`ReadyForQuery` 按状态回 `I` / `T` / `E`；事务内出错后
  只能 `ROLLBACK`，此时 `COMMIT` 按 PostgreSQL 语义等价于回滚；连接断开时
  未提交事务自动中止。

多连接共享同一个 `Database`，服务端用一把 asyncio 锁把**单条语句**串行化；
事务之间的隔离由每个会话自己的 `Transaction` 与 MVCC 提供。
这不是"高并发数据库"，而是"协议正确、语义正确"的单机引擎。

## 6. 为什么这样切分

- **协议层与引擎解耦**：可以先用进程内 REPL 开发引擎，等引擎稳定再接协议，调试成本低一个数量级。
- **物理日志**：让"崩溃恢复"这件事从"希望正确"变成"可以断言"。
- **不做静默降级**：任何未实现的能力抛 `NotSupportedError` 并附带原因。一个返回错误答案的数据库比一个明确报错的数据库危险得多。
