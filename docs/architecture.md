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

DDL 不止改磁盘页，还会原地修改内存里的 `Catalog` 对象。因此 `Catalog` 的
建表/删表/建索引/删索引/重命名/加列都会把一个逆操作登记到 `txn.undo_log`；
DML 也会在首次接触表时登记 `root_page`/`last_page`/`row_count_hint` 与索引
根页的快照。Buffer Pool 另外按 `xid` 记录每次页写的 before image，`abort`
时先逆序恢复页内容，再逆序恢复目录对象，最后由 WAL 负责崩溃后的页级 undo。
页恢复会写 CLR 并推进 `pageLSN`，避免下次 redo 把回滚结果覆盖回去。
`CatalogStore` 还会把 page 0 的 `catalog_root` 指回旧链，否则显式 `ROLLBACK`
之后关闭再打开，会重新加载到未提交的新目录链。

`SAVEPOINT` 记录当时 Buffer Pool 的页 undo 深度和 `txn.undo_log` 深度。
`ROLLBACK TO SAVEPOINT` 只应用这个深度之后的页原像（同样写 CLR），再逆序
执行这个深度之后的目录对象 undo；更早的原像保留给后续事务级 abort。
`RELEASE` 只丢弃保存点标记，不撤销任何写入。保存点是事务内状态，不写 WAL。

### 2.6 回收（VACUUM）

`UPDATE` 写新版本、`DELETE` 只打 `xmax`，所以表会持续膨胀。`VACUUM [table]`
负责回收：

1. 要求调用时没有其它活跃事务；服务端多连接下这个前提不是天然成立的，
   执行器会检查 `active_xids`，否则拒绝 `VACUUM`。扫描整表，保留当前
   快照可见的那一版；
2. 把存活行重排成"每行只有一条根元组"：`root/newest` 指向自己、`prev` 为空；
   页内顺序紧凑，死版本的空间被真正收回；
3. 重排会改变所有 RID，所以紧接着**重建该表的所有索引**；
4. 每一页的改写都写一条**整页 before/after image**，崩溃后恢复能重放，
   不会出现"VACUUM 之后行凭空消失/复活"。

文件本身不会缩小（和 PostgreSQL 的普通 `VACUUM` 一样）；空页留在页链上，
扫描会跳过。没有 autovacuum、没有增量回收、没有 oldest-xmin 水位——
一旦放开并发事务，这些就是下一件必须做的事，写在 `docs/sql-support.md` 里。

### 2.7 加列与旧记录

`ALTER TABLE ... ADD COLUMN` **不重写堆页**：目录里追加列定义并持久化，旧记录的
`ncols` 仍是旧值。`ADD COLUMN IF NOT EXISTS` 遇到同名列只返回跳过 notice，不改目录。`decode_record(..., allow_missing=True)` 在读取时按新列的
`DEFAULT`（没有默认值就是 `NULL`）补齐尾部；`UPDATE` / `VACUUM` 把该行重新编码时
才会写成包含新列的完整格式。

`ALTER TABLE ... ALTER [COLUMN] ... SET/DROP DEFAULT` 同样只修改目录里的列默认值，
不扫描或重写已有行；默认值在后续 `INSERT` 省略该列时读取，并沿用与建表相同的
字面常量/类型转换校验；`UPDATE ... SET col = DEFAULT` 与 `ON CONFLICT DO UPDATE`
也读取同一份目录默认值，再走正常约束检查。`SET NOT NULL` 会先扫描堆表验证存量数据，校验通过后只改
目录约束；`DROP NOT NULL` 直接解除约束，两者都登记事务 undo，不重写堆页。默认值
修改与 `DROP` 也都登记事务 undo。

这样做的好处是 DDL 是 O(1)，不需要停机搬表；代价是"同一张表里新旧两种行格式
共存"，解码路径必须显式知道缺列是合法的。`RENAME TO` / `RENAME COLUMN` 只改目录
里的名字并同步索引列引用与 CHECK token，不搬页也不重写记录；真正需要行格式迁移的
`ALTER TABLE ... ADD COLUMN` 的新列在旧记录缺失时按默认值/`NULL` 补齐；`DROP COLUMN` 与
`ALTER [COLUMN] ... TYPE` / `SET DATA TYPE` 都需要行格式迁移。`DROP COLUMN` 先物化
全部可见行、裁掉目标列的值并编译校验剩余 CHECK，然后写入新的 shadow heap，切换
目录里的 `root_page`/`last_page` 并重建全部索引。若目标列被索引/约束引用或 CHECK
引用该列，语句直接拒绝，要求先删除依赖；表至少保留一列。`ALTER TYPE` 先物化旧行，
没有 `USING` 时用目标类型的 `coerce` 逐行预校验，有 `USING` 时用旧表 schema 绑定
表达式求值；再转换默认值、用临时 `TableDef` 编译所有 CHECK 并验证转换后的行。全部
通过后同样写入 shadow heap、切换根页、从新堆表重建全部索引。这样扫描不会再用新
schema 解码旧版本字节；旧页链不再被引用，页空间暂不自动回收。任一步失败都由事务
undo 恢复目录、堆根页与索引根页，不会留下半转换/半删列的表。

### 2.8 表级约束

列级 `PRIMARY KEY` / `UNIQUE` 之外，`CREATE TABLE` 也接受表级
`PRIMARY KEY (a, b)` 和 `UNIQUE (a, b)`，可带 `CONSTRAINT name`。执行器把
表级约束落成**一个复合唯一 B+ 索引**：复合主键参与列在目录里被标记为
`primary_key=True, nullable=False`，索引名默认 `<table>_pkey`；复合唯一约束默认
`<table>_<col...>_key`。约束名不是独立目录对象，而是索引名，因此
`ON CONFLICT ON CONSTRAINT name` 与 `CREATE UNIQUE INDEX name` 共用同一条解析
路径，不会出现“约束存在但冲突检测看不到”的分裂状态。

复合主键的 `_check_constraints` 仍走统一唯一键入口；`NULL` 不进入唯一索引，
主键列的 `NOT NULL` 由写路径先行拒绝。多个主键、列级与表级主键混用、以及
自动索引名与显式约束名碰撞都会在 DDL 阶段报错，避免 `Catalog` 静默覆盖索引。

`CHECK` 不建索引，而是 `TableDef.checks` 里的独立 `CheckDef` 对象：表达式以
token 切片持久化，重开数据库后重新解析 AST、再经 Binder 绑定成谓词，避免把
内存中的闭包写进目录。写路径在 `_check_constraints` 和 `_rewrite_row` 统一调用
`_check_checks`，因此普通 INSERT、`INSERT ... SELECT`、`DEFAULT VALUES`、upsert
以及 UPDATE 都会经过同一套校验；表达式必须返回 `BOOLEAN`（`NULL` 字面量的
`UNKNOWN` 可通过），只有结果为 `FALSE` 才拒绝，`NULL`/`UNKNOWN` 按 SQL 语义视为通过。`ALTER TABLE ... ADD [CONSTRAINT name] CHECK (...)` 会先扫描
整个堆表，发现存量违反行就整体失败；重命名表/列会同步重写 CHECK token 里的表限定符
或列引用，不会留下绑定旧名字的约束。`DROP CONSTRAINT` 支持 CHECK、UNIQUE 和
PRIMARY KEY：CHECK 直接删除目录对象；唯一/主键通过 `IndexDef.constraint_kind`
区分，删除时同时移除底层索引，主键还会清除 `primary_key` 标记但保留 `NOT NULL`。
普通索引与约束索引的删除入口分开：`DROP CONSTRAINT` 拒绝普通索引，`DROP INDEX`
拒绝约束索引，避免删掉索引后目录里留下“主键存在但没有索引”的半状态。
`ADD COLUMN ... CHECK` 会先用“旧列 + 新列默认值”拼出完整存量行，验证通过后才把
列和约束一起写入目录，因此不会出现列已加上但约束没落地的半状态。

### 2.9 外键

外键是 `TableDef.foreign_keys` 里独立的 `ForeignKeyDef`：`name`、本地 `columns`、
`ref_table`、被引用 `ref_columns`。它不建索引，随目录 JSON 持久化，事务 undo / 恢复
都会恢复整个字典。建表时执行器先把列级 `REFERENCES` 与表级 `FOREIGN KEY` 收集成
目录对象，并解析省略的被引用列：自引用取当前表主键，其它表取目标表的 `PRIMARY`
索引列；随后校验被引用列存在、上有 UNIQUE/PRIMARY 索引、本地列与被引用列类型完全
一致，并生成 `<table>_<cols>_fkey` 默认名（冲突追加 `_2`）。被引用唯一索引可以来自
主键、UNIQUE 约束或普通 `CREATE UNIQUE INDEX`，只要索引键完全匹配。

写入路径在 `_check_foreign_keys` 中按外键的 `MATCH` 模式检查：`SIMPLE` 下本地键
任一列为 `NULL` 就跳过；`FULL` 下要么全为 `NULL`，要么全部非 `NULL`，部分 `NULL`
直接抛 `ForeignKeyViolationError`（SQLSTATE `23503`）；否则优先用被引用列上的
`UNIQUE` / `PRIMARY` B+ 树索引做点查，索引项不可见或缺失时再回落到被引用表扫描；
两步都找不到完整匹配行时抛 `ForeignKeyViolationError`。因此每次写入不必扫描整张
父表，同时保留索引异常时的正确性兜底。`INSERT`、`INSERT ... SELECT`、`DEFAULT VALUES`、upsert 和
`UPDATE` 都走同一入口；`UPDATE` 改被引用键时由 `_plan_referenced_updates` 收集所有
引用表（自引用时跳过当前 RID）：子表外键列有精确索引时走 B+ 树点查，否则扫描整张
子表。`RESTRICT` 直接报错，`CASCADE` 收集待更新子行，
`SET NULL` 收集并将匹配外键列置空，`SET DEFAULT` 收集并写入列默认值，在父行写堆后
通过同一 `_rewrite_row` 递归更新子行，并维护其索引和下游外键。
`ON DELETE CASCADE` / `SET NULL` / `SET DEFAULT` 则由 `_cascade_delete_children`
处理：先按子表外键索引（如有）或全表扫描收集匹配子行，`RESTRICT` 子行立即报错，`CASCADE` 子行递归删除其子行后再
删索引项和堆版本，`SET NULL` 子行通过 `_rewrite_row` 将外键列置空，`SET DEFAULT`
子行延后到父行删除完成后再写入默认值（避免默认值恰好等于旧父键时留下悬空引用）；
临时 `processing` / `deleted` 集合用于打断自引用环，事务 abort 会恢复所有页和目录。
子表外键列有精确索引时走点查，没有索引时退回逐表扫描，规模上最坏仍是 O(子表行数)。

`ForeignKeyDef` 还保存 `deferrable` / `initially_deferred` / `validated`。`DEFERRABLE INITIALLY
DEFERRED` 的 `INSERT` / `UPDATE` 存在性检查在 DML 路径跳过，执行器把 `(表, RID)`
放进 `Transaction.deferred_fk_rows`；`NO ACTION` 删除/更新被引用键时把旧键放进
`Transaction.deferred_ref_checks`。显式事务内可执行
`SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }`，覆盖写在
`Transaction.constraint_modes`，后续写入按覆盖后的时机决定是否立即检查；
`IMMEDIATE` 会先对已积压的行/父键执行一次强制补检，失败则不改变模式。
`SAVEPOINT` 记录这三个延迟状态并在 `ROLLBACK TO` 时一并恢复，避免已撤销的检查
或模式泄漏到提交阶段。`Database` 在 `COMMIT`（以及自动提交语句的提交点）调用
`executor.validate_deferred_foreign_keys`：前者按当前行重跑外键检查，后者确认
被引用键是否已恢复或子行是否已清理；失败则 `abort`，WAL/页 undo 恢复。`RESTRICT`
始终立即，`CASCADE` / `SET NULL` / `SET DEFAULT` 动作也立即执行。

`ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... NOT VALID` 只跳过存量行扫描，
新写入仍走 `_check_foreign_keys`；CHECK 的 `NOT VALID` 同样跳过存量扫描，新写入仍走
行级 CHECK 谓词。`VALIDATE CONSTRAINT` 按名称分派：外键用 `force=True` 重跑引用检查，
CHECK 重新编译谓词后扫描全部可见行，成功后才把 `validated` 从 `False` 改为 `True`。
因此大表可以先上线约束、再择机校验，失败时约束仍保持 `NOT VALID`。

DDL 侧的保护不是“补丁”而是同一份目录语义：`DROP COLUMN` / `ALTER COLUMN TYPE`（均可 `CASCADE` 清理依赖外键；`DROP COLUMN CASCADE` 还会删除列级依赖视图）
拒绝仍被外键使用的列；`DROP INDEX` / `DROP CONSTRAINT` 默认拒绝删除支撑外键的唯一键，
`DROP CONSTRAINT ... CASCADE` / `DROP INDEX ... CASCADE` 会显式删除依赖外键；`DROP TABLE` 默认拒绝被引用的表，`CASCADE` 时删除引用表上的外键约束；`TRUNCATE`
默认拒绝被引用的表；`TRUNCATE a, b` 允许同一列表内的相互依赖，但漏掉任一引用表
仍拒绝，`TRUNCATE ... CASCADE` 递归清空直接/间接引用它的子表。`RENAME TO` / `RENAME COLUMN` 会同步改外键里的表名/列名；
`ALTER TABLE ADD CONSTRAINT FOREIGN KEY` 默认先扫描存量行，再原子写入目录（即使约束
声明为延迟，也先校验存量数据）；声明 `NOT VALID` 时跳过扫描并等待
`VALIDATE CONSTRAINT`。当前
`ON DELETE` / `ON UPDATE` 均支持 `RESTRICT` / `CASCADE` / `SET NULL` / `SET DEFAULT`（后两者可带 `(col...)` 列级动作） /
`NO ACTION`（显式 `NO ACTION` 保留；`DEFERRABLE` 可让它提交时检查），`MATCH` 支持
`SIMPLE` / `FULL`，不支持 `PARTIAL`；`ADD COLUMN ... REFERENCES`
复用 `ADD CONSTRAINT FOREIGN KEY` 的存量行校验路径，先加列再原子建立外键，失败时
整条语句回滚。`SET CONSTRAINTS` 不持久化，只影响当前事务。

### 2.10 视图

`CREATE VIEW` 只保存视图定义，不复制基表数据：目录里存的是 `AS` 之后的
**词法 token 切片**，而不是 AST 或某次查询的物化结果。这样做的好处是视图定义
能直接随目录 JSON 持久化，不依赖 AST 的序列化格式；代价是每次使用视图都要重新
解析一次（只在绑定阶段发生，不重复执行基表扫描）。

Binder 在 `_source_for` 中把视图展开成一个 `CteScan(child=...)`：先解析 token，
再用独立 Binder 绑定基表查询，最后把结果列挂到视图名/别名下。视图定义可以带
`WITH`（普通 CTE 或递归 CTE），此时 token 重新解析为 `Query` 并走 `bind_query`；
普通定义仍走 `bind_select`。
若视图带显式列名清单，
目录会在 `view_columns` 里保存别名，Binder 用别名覆盖输出列名并校验列数/重复名。
`view_stack` 记录正在展开的视图，检测 `A -> B -> A` 的循环引用；Binder 同时把
实际解析到的表/视图记进 `source_dependencies`，目录持久化为每个视图的依赖集合，
`DROP TABLE` / `DROP VIEW` / `ALTER TABLE ... RENAME` 据此拒绝会留下悬空定义的删除或
重命名；显式 `CASCADE` 会先递归删除依赖视图。视图不占堆页，因此不参与
`VACUUM`/`TRUNCATE`；它读到的始终是基表当前已提交的 MVCC 版本。
`CREATE OR REPLACE VIEW` 先绑定新定义，再替换目录里的 token 切片，因此新定义
非法时旧视图仍然可用；替换本身也在事务 undo 范围内。替换成功后会重新绑定并刷新
所有视图的依赖快照，避免依赖视图继续引用已经被移除的旧表，或漏掉新表依赖。

当前限制：没有物化视图，视图不能作为 `INSERT`/`UPDATE`/`DELETE` 的目标；
`SHOW TABLES` 只列基表，`SHOW VIEWS` 单独列出视图。

## 3. SQL 与计划

### 3.1 解析

手写词法分析器（不引入 parser generator，避免隐藏的语义）。递归下降，优先级用绑定强度表处理二元表达式。语法错误带位置信息。

### 3.2 绑定

绑定层做三件事，都不在"执行时"做：

1. **名字解析**：把 `ColumnRef`（可能带表前缀）解析成"这一行元组里的第几列"。
   重复列名报 `AmbiguousColumnError`，未知列报 `ColumnNotFoundError` 并列出可用列；
2. **类型推导**：每个表达式都有一个 `TypeSpec`。算术要求数值、比较要求可比的
   类型、聚合参数要求数值——错在编译期报出来，而不是执行到一半才炸。`NULL`
   字面量是 `unknown` 类型，会被 `UNION` / `CASE` / `COALESCE` 的另一侧吸收，
   不会因为固定推断成 `TEXT` 而误报类型不兼容；
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
`RIGHT JOIN` / `FULL JOIN` 统一用 nested loop：右表做外层，`FULL` 额外记录右表
哪些行被匹配过，最后补出未匹配的右行；两者都按 left+right 的列顺序输出，未匹配
的一侧填 `NULL`。`CROSS JOIN` 是 `ON` 为真的 `INNER JOIN`。
集合运算用 `SetOp`：`UNION ALL` 直接拼接，`UNION` 流式去重，`EXCEPT` 先物化右表
再筛左表，`INTERSECT` 同理；`INTERSECT` 在解析时被收紧成更高的优先级。
连接顺序枚举与代价模型仍未实现——`EXPLAIN` 输出的是真实执行的计划，不是理想计划。

## 4. 执行

火山模型，每个算子是生成器，一次产出一行：

- `Values`：常量行（`SELECT 1`、`INSERT ... VALUES` 的表达式求值）；
- `Scan` / `IndexLookup`：堆表顺序扫描 / 索引点查；
- `Filter`：谓词为真才放行（`NULL` 视为假）；
- `Join`：有等值键时 `HashJoin`（右侧物化建哈希表，左侧流式探测）；否则
  nested loop（右侧物化，左侧流式）。`LEFT JOIN` 无匹配时补 NULL；`RIGHT` /
  `FULL` 走 nested loop，`FULL` 两侧未匹配行都补 NULL；
- `Aggregate`：无 `GROUP BY` 时输出一行（空表也输出 `COUNT=0`）；
- `Project`：求值输出列，可附加 ORDER BY 需要的隐藏列；
- `Distinct`：按前 N 列去重（隐藏列不参与）；
- `SetOp`：集合运算，逐行拼接 `UNION` / `UNION ALL`，`EXCEPT [ALL]` /
  `INTERSECT [ALL]` 按行多重集处理；`ORDER BY` / `LIMIT` 由外层节点处理；
- `CteScan`：普通 `WITH` 的命名子查询。绑定阶段先按书写顺序把每个 CTE
  绑成独立计划并放进作用域，主查询里的 `FROM cte_name` 直接引用它；
  `WITH c(a, b) AS (...)` 的显式列名会覆盖输出列名并校验列数/重名；
  `WITH ... INSERT ... SELECT` 也走同一套作用域；同一个 CTE 被引用两次会
  执行两次（没有做物化缓存）；
- `RecursiveCte`：`seed UNION [ALL] recursive` 的递归 CTE。`CteScan(recursive=True)`
  在递归项里读取执行器保存的“上一轮工作表”，每一轮把新行加入结果并作为下一轮输入，
  直到没有新行；`UNION` 用哈希集合去重，`UNION ALL` 保留重复，超过 10000 轮报错；
- `Sort`：从最后一个键往前依次稳定排序，支持多键；每键独立处理
  `ASC/DESC` 与 `NULLS FIRST/LAST`（默认 ASC 时 NULL 最后、DESC 时 NULL 最前）；
- `Limit`：跳 `offset`，取 `limit`；`WITH TIES` 时用排序键比较边界行，
  继续放行与最后一条同键的行。

默认排序位置与 PostgreSQL 对齐：`ASC` 时 `NULL` 最后、`DESC` 时 `NULL` 最前；
`NULLS FIRST/LAST` 可以显式覆盖，每个排序键独立生效。

`INSERT ... SELECT` 不经过 `Values`：绑定来源查询并核对列数后，先把结果完整物化，
再逐行写堆和索引。物化不只是为了简单——`INSERT INTO t SELECT * FROM t` 如果边读
边写，扫描器会不断看到刚插入的新版本，表会无限增长。

`CREATE TABLE AS SELECT` 复用同一条链路：先绑定查询拿到输出列名和类型，建一张
全是可空列的空表，再物化结果并逐行插入。它不复制来源表的索引、主键或约束——
这些需要显式 `CREATE INDEX`。

DML 的 `RETURNING` 不另建计划：绑定阶段把表达式编译成闭包，写入循环里对
新行（INSERT/UPDATE）或旧行（DELETE）求值。绑定失败发生在任何写入之前，
所以未知列不会留下半条记录；`ON CONFLICT DO NOTHING` 跳过的行自然不产出。
`WITH ... UPDATE/DELETE` 把 CTE 注册表挂到当前语句的 Binder 上，执行器在
DML 求值期间把同一作用域放进 `_active_ctes`，语句结束即丢弃。

所有 `INSERT`（`VALUES` 与 `INSERT ... SELECT`）共用 `_insert_row`：
NOT NULL、CHECK、唯一索引检查、`ON CONFLICT` 分派和索引维护都在写堆之前完成。
唯一性查询本身收敛在 `_find_unique_conflict`，INSERT、UPDATE、upsert 和
`DO NOTHING` 共用同一个"NULL 不进索引 / 忽略当前行 / 目标索引"语义。
新增插入路径必须复用这个入口，不能绕过去直接写 `HeapFile`，否则会重现
"先写行、后建索引"或静默重复唯一键的问题。

`ON CONFLICT DO UPDATE` 的赋值和 `WHERE` 在“旧行 + excluded 行”的复合
Schema 上绑定：默认 qualifier 是目标表，因此未限定列名访问旧行，
`excluded.col` 访问待插入行。两者在同一个 `EvalContext` 元组里连续排放；
执行器只把赋值结果写回堆，excluded 行本身不会被写入。

冲突目标有三种写法：不写目标、`(col, ...)` 列组合、`ON CONSTRAINT name`；
三者都由 `_resolve_conflict_index` 映射到唯一 `IndexDef`，后续写入路径不再
关心目标写法。

`UPDATE` 和 upsert 的最终写回共用 `_rewrite_row`：类型/约束校验、唯一键
冲突检查、旧索引删除、堆更新和新索引插入都在一个入口完成，避免两条
路径对“先删索引还是先查重”的顺序产生分歧。

Binder 每执行一条语句重建一次。CTE 注册表是语句级作用域，不能跨语句复用；
曾经复用 Binder 导致上一条 `WITH` 的 CTE 在下一句仍然可见，现已修复并加测试。

## 5. 协议

实现 PostgreSQL v3：

- 启动：`StartupMessage` → `AuthenticationOk`（trust，不校验密码）→
  `ParameterStatus*` → `BackendKeyData` → `ReadyForQuery`；客户端发
  `SSLRequest` 时回 `N`（不做 TLS），让它退回明文；
- 简单查询：`Query` → `RowDescription` / `DataRow*` / `CommandComplete` / `ReadyForQuery`；
- 扩展查询：`Parse` / `Bind` / `Describe` / `Execute` / `Sync`。`Parse` 记录客户端
  声明的参数类型 OID、**在这里做语法检查并缓存 AST**；`Execute` 直接复用该 AST，
  不再重新 parse；`Bind` 按格式码（文本 0 / 二进制 1）把参数解码成 Python 值；
  绑定阶段把 `$n` **折成字面量**，所以执行计划里没有"参数数组"，代价是没有
  计划缓存；
- 参数：支持 `INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/TEXT/VARCHAR` 的文本与二进制格式；
  OID 0（客户端未声明）时按字面量推断。参数个数与 `Bind` 不一致报 `08P01`，
  `$n` 没有值报 `42P02`；
- `Describe` 语句先回 `ParameterDescription`（真实 OID 列表）。没有 `$n` 的语句
  可以直接静态绑定；带 `$n` 的语句用 Parse 里声明的参数 OID 构造 NULL 占位值做
  绑定，同样返回 `RowDescription`（只绑定、不执行）；只有缺少类型声明、无法
  静态推断时才退回 `NoData`。门户的 `Describe` **只做静态绑定**，绝不执行语句——否则同一条
  `INSERT/UPDATE/DELETE` 会在 `Describe` 和 `Execute` 中各执行一次；
- `Close` 支持关闭预处理语句（`S`）和门户（`P`）；关闭语句时连带关闭由它
  创建的门户，名字可以重新 `Parse`/`Bind`。每个连接对预处理语句和门户各保留
  1024 个上限，超过时返回明确错误，避免长连接无界增长。每次事务提交/回滚后
  关闭该事务的门户；同名重新 `Parse` 也会让旧语句创建的门户失效。
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
