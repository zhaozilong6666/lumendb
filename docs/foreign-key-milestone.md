# 外键完整性与级联动作阶段报告

- 日期：2026-11-14
- 范围：LumenDB 外键约束、删除/更新动作、`ALTER TABLE` 外键 DDL、测试与文档
- 基线：`8da5a63`（`ON DELETE CASCADE`）
- 本轮提交：`9cc420f`、`640f38d`、`d27fea0`、`42627d8`、`9039add`、`5c3dd00`、`35b702e`、`2c20173`、`7d809b7`、`326edbb`、`8f37f76`、`02e9c68`、`852641b`、`bd979bd`

## 1. 成果清单

| 能力 | 状态 | 说明 |
|---|---|---|
| 列级 `REFERENCES` / 表级 `FOREIGN KEY` | 完成 | 支持复合键、自引用、省略被引用列（取主键） |
| 写入引用完整性 | 完成 | `INSERT`、`INSERT ... SELECT`、`DEFAULT VALUES`、upsert、`UPDATE` 统一校验；SQLSTATE `23503` |
| `MATCH SIMPLE` | 完成 | 本地键任一列为 `NULL` 时跳过检查 |
| `MATCH FULL` | 完成 | 复合本地键必须全 `NULL` 或全非 `NULL`，部分 `NULL` 报错 |
| `ON DELETE RESTRICT` | 完成 | 默认动作，阻止删除仍被引用的父行 |
| `ON DELETE CASCADE` | 完成 | 递归删除子行，维护子表索引，支持多级链与自引用链 |
| `ON DELETE SET NULL` | 完成 | 将匹配子行的外键列置空（支持 `(col...)` 列级动作）；`NOT NULL` 子列会报错并回滚 |
| `ON DELETE SET DEFAULT` | 完成 | 父行删除后把匹配子行写回列默认值（支持 `(col...)` 列级动作）；默认值不存在时回滚 |
| `ON UPDATE RESTRICT` | 完成 | 默认动作，阻止更新仍被引用的父键 |
| `ON UPDATE CASCADE` | 完成 | 父键更新后递归同步子行，支持多级链与自引用链 |
| `ON UPDATE SET NULL` | 完成 | 父键更新后将匹配子行的外键列置空（支持 `(col...)` 列级动作）；`NOT NULL` 子列会报错并回滚 |
| `ON UPDATE SET DEFAULT` | 完成 | 父键更新后将匹配子行写回列默认值（支持 `(col...)` 列级动作）；默认值不存在时回滚 |
| `NO ACTION` | 完成 | 独立动作；配合 `DEFERRABLE` 可提交时检查，否则立即拒绝 |
| `DEFERRABLE INITIALLY DEFERRED` | 完成 | `INSERT`/`UPDATE` 存在性检查与 `NO ACTION` 父行动作延迟到 `COMMIT`；`SET CONSTRAINTS` 支持事务内切换 |
| `ALTER TABLE ADD CONSTRAINT FOREIGN KEY` | 完成 | 默认先校验存量行，再原子写入目录；支持 `ON DELETE/UPDATE CASCADE/RESTRICT/SET NULL/SET DEFAULT`、`NOT VALID` |
| `ALTER TABLE ADD COLUMN ... REFERENCES` | 完成 | 复用 `ADD CONSTRAINT` 的存量行回填/校验路径，失败整条语句回滚 |
| `NOT VALID` / `VALIDATE CONSTRAINT` | 完成 | 外键与 CHECK 均支持：`NOT VALID` 只跳过存量行、新写入仍立即检查；`VALIDATE` 只读扫描全部可见行，成功后才把 `validated` 置为真 |
| 保存点与延迟状态 | 完成 | `SAVEPOINT` 同时快照 `deferred_fk_rows` / `deferred_ref_checks` / `constraint_modes`，`ROLLBACK TO` 一并恢复 |
| 外键 DDL 依赖保护 | 完成 | `DROP COLUMN`、`ALTER COLUMN TYPE`、`DROP INDEX`、`DROP CONSTRAINT`、`DROP TABLE`、`TRUNCATE` 均有保护；`DROP COLUMN ... CASCADE` / `ALTER COLUMN TYPE ... CASCADE` / `DROP CONSTRAINT ... CASCADE` / `DROP INDEX ... CASCADE` / `DROP TABLE ... CASCADE` 可显式删除依赖对象 |
| 元数据持久化 | 完成 | `ForeignKeyDef` 的列、引用表/列、`on_delete`、`on_update`、`deferrable`、`validated` 全部写入目录 JSON，兼容旧目录缺省值 |
| 事务回滚 | 完成 | 删除级联、更新级联、加列建外键失败均可回滚到语句前状态 |

## 2. 关键实现

### 2.1 目录对象

`lumendb/catalog/schema.py` 的 `ForeignKeyDef` 增加：

```python
on_delete: str = "RESTRICT"
on_update: str = "RESTRICT"
match_type: str = "SIMPLE"  # SIMPLE / FULL
```

`from_json` 对旧目录缺省为 `RESTRICT` / `SIMPLE`；`validate_table_def` 允许
`RESTRICT` / `NO ACTION` / `CASCADE` / `SET NULL` / `SET DEFAULT`（后两者支持 `(col...)` 列级动作）和
`SIMPLE` / `FULL`，非法动作或 MATCH 类型直接拒绝。`NO ACTION` 保留为独立动作，
`DEFERRABLE` / `INITIALLY DEFERRED` 随目录持久化；`INITIALLY DEFERRED` 必须同时
声明 `DEFERRABLE`。

### 2.2 插入与更新校验

`_check_foreign_keys` 按外键的 `MATCH` 模式检查，并优先使用被引用列上的唯一索引点查：

- `MATCH SIMPLE`：本地键任一列为 `NULL`：跳过；
- `MATCH FULL`：本地键全为 `NULL`：跳过；部分 `NULL`：直接报错；全非 `NULL`：
  继续查找完整匹配行；
- 否则必须在被引用表中找到完整匹配行；
- 找不到抛 `ForeignKeyViolationError`。

`INSERT`、`INSERT ... SELECT`、`DEFAULT VALUES`、upsert 和 `UPDATE` 都走这一入口。
外键引用列要求存在 `UNIQUE` 或 `PRIMARY KEY` 索引，本地列与被引用列类型必须完全一致。

### 2.3 延迟约束

`ForeignKeyDef` 的 `deferrable` / `initially_deferred` 为真时，DML 路径跳过
`INSERT` / `UPDATE` 的本地存在性检查，并把 `(table, RID)` 加入
`Transaction.deferred_fk_rows`。`NO ACTION` 的父行删除/更新把旧键加入
`Transaction.deferred_ref_checks`，不会立即报错。`Database` 在 `COMMIT` 前调用
`validate_deferred_foreign_keys`：

- 按当前 RID 重新读取行并强制检查所有外键；
- 对父行待检查项，先确认被引用键是否已被同事务恢复；否则按子表索引/扫描查找
  仍存在的引用行；
- 失败时 `abort` 事务，页 undo 与 WAL 恢复保证子行、父行都不留半状态。

`DEFERRABLE INITIALLY IMMEDIATE` 仍走语句内检查；`RESTRICT`、`CASCADE`、
`SET NULL`、`SET DEFAULT` 动作始终立即执行。显式事务内可用
`SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }` 覆盖检查时机：
`DEFERRED` 影响后续 DML，`IMMEDIATE` 先补检已积压项；`ALL` 只作用于所有
`DEFERRABLE` 外键，按名称指定非 `DEFERRABLE` 约束会报错。

### 2.4 删除级联

`_delete` 在删除目标行前调用 `_cascade_delete_children`：

- 扫描所有引用该表的子外键；
- `RESTRICT` 子行立即报错；
- `SET NULL` 子行通过 `_rewrite_row` 将外键列置空，保留子行；
- `SET DEFAULT` 子行先收集，等父行删除完成后再通过 `_rewrite_row` 写回列默认值，
  避免默认值恰好等于旧父键时留下悬空引用；
- `CASCADE` 子行递归删除其子行，再删除子表索引项和堆版本；
- `processing` / `deleted` 集合打断自引用环和多级链；
- 删除每个子行前调用 `_snapshot_table_state`，事务回滚可恢复页和索引根页；
- `affected` 只统计直接目标行，不统计级联子行。

### 2.5 更新级联

`_rewrite_row` 在父行写堆前调用 `_plan_referenced_updates`：

- 扫描引用该表的外键，按被引用列旧值/新值判断键是否变化；
- `RESTRICT` 且存在匹配子行：立即抛 `ForeignKeyViolationError`；
- `SET NULL`：收集待更新子行，将外键列置空；
- `SET DEFAULT`：收集待更新子行，写入列默认值；
- `CASCADE`：收集待更新子行，合并同一子行的多个外键动作；
- 父行完成索引删除、堆更新、索引重建后，递归调用 `_rewrite_row` 更新子行；
- 子行更新继续走自己的 CHECK、外键、唯一索引和下游级联路径；
- `_updating` 集合用于打断循环引用。

### 2.6 `ALTER TABLE`

- `ADD CONSTRAINT FOREIGN KEY`：先扫描存量行，再写入目录；失败不留下半状态。
- `ADD COLUMN ... REFERENCES`：先加列，再复用 `ADD CONSTRAINT` 路径建立外键；
  任一步失败整条语句回滚（测试覆盖“列已加、外键校验失败”场景）。
- `DROP TABLE ... CASCADE`：删除其他表上引用该表的外键约束，不删除子表数据。
- `DROP COLUMN` / `ALTER COLUMN TYPE`：目标列仍被本地或外部外键使用时拒绝；`DROP COLUMN ... CASCADE` 删除依赖外键、索引与 CHECK；`ALTER COLUMN TYPE ... CASCADE` 删除依赖外键。
- `DROP INDEX` / `DROP CONSTRAINT`：删除支撑外键的唯一键时保守拒绝；两者的 `CASCADE` 可显式删除依赖外键。
- `RENAME TABLE` / `RENAME COLUMN` / `RENAME CONSTRAINT`：同步外键元数据。
- `TRUNCATE`：默认拒绝被外键引用的表；`TRUNCATE a, b` 在同一列表中允许一起清空相互依赖的表，未列入的引用表仍拒绝；`TRUNCATE ... CASCADE` 递归清空直接/间接子表。

## 3. 测试与验证

- 全量测试：**909 passed**（`python -m pytest tests -q`，约 35 秒）
- 编译检查：`python -m compileall -q lumendb tests` 通过
- 空白检查：`git diff --check` 通过
- 测试覆盖重点：
  - 基础外键、复合键、自引用、省略被引用列；
  - `ON DELETE CASCADE` 单级/多级/自引用链、索引维护、事务回滚；
  - `ON UPDATE CASCADE` 单级/多级/自引用链、事务回滚；
  - `ON DELETE SET NULL` / `ON UPDATE SET NULL`、`NOT NULL` 冲突回滚；
  - `MATCH FULL` 全空/全非空/部分 NULL 拒绝；
  - `NO ACTION` 保留为独立动作；
  - `DEFERRABLE INITIALLY DEFERRED`：先子后父提交成功、缺父提交失败、更新后补父、删除后恢复父键；
  - `SET CONSTRAINTS ALL/named DEFERRED`、`ALL IMMEDIATE` 立即补检失败、未知约束名、非 DEFERRABLE 约束、事务外执行；
- `TRUNCATE ... CASCADE` 递归清空父/子/孙表并支持事务回滚；
  - `TRUNCATE a, b` 多表清空、遗漏依赖拒绝、CASCADE 合并与重复表名拒绝；
  - `ADD CONSTRAINT ... NOT VALID`（外键与 CHECK）跳过存量行、新写入仍检查、`VALIDATE` 失败保持未校验并可重试、持久化；
  - `SAVEPOINT` / `ROLLBACK TO` 恢复延迟外键队列与 `SET CONSTRAINTS` 覆盖模式；
  - `ON DELETE SET DEFAULT` / `ON UPDATE SET DEFAULT`、列级默认值动作、默认值缺失回滚；
  - `ALTER ADD CONSTRAINT` 的 `SET NULL` 动作；
  - `RESTRICT` 阻止删除/更新；
  - `ADD COLUMN ... REFERENCES` 成功与失败回滚；
  - `ALTER ADD CONSTRAINT` 的 `ON DELETE/UPDATE CASCADE`；
  - 外键持久化；
  - DDL 依赖保护、`DROP CONSTRAINT ... CASCADE` 删除依赖外键与重命名同步。

CI：

- `e1a0552`：success
- `8da5a63`：success
- `9cc420f`：success
- `640f38d`：success
- `b0985ae`：success
- `d27fea0`：success
- `42627d8`：success
- `9039add`：success
- `5c3dd00`：success
- `7d809b7`：success
- `326edbb`：success（其代码树由 `8f37f76` 覆盖）
- `8f37f76`：success
- `02e9c68`：success
- `4b6b13c`：success
- `852641b`：success
- `1f4337f`：success（TRUNCATE CASCADE 的远端代码树）

## 4. 基准（2000 行）

本机实测，结果在 `bench/results/benchmark.md`：

| 场景 | 最好 (s) | 中位 (s) | 吞吐 (行/秒) |
|---|---:|---:|---:|
| 批量插入（含主键索引维护） | 0.404 | 0.404 | 4955 |
| 批量插入（无索引） | 0.119 | 0.120 | 16802 |
| 全表扫描 COUNT(*) | 0.045 | 0.054 | 44634 |
| GROUP BY（100 个分组） | 0.055 | 0.056 | 36659 |
| 更新 500 行 | 0.312 | 0.317 | 1601 |
| VACUUM 压实 2000 行 | 0.350 | 0.623 | 5722 |
| TRUNCATE 清空 2000 行 | 0.002 | 0.003 | 902772 |
| 索引点查（200 次） | 0.234 | 0.245 | 855 |
| 无索引点查（20 次） | 0.109 | 0.131 | 184 |
| 索引范围查询（50 次） | 0.205 | 0.210 | 244 |
| 无索引范围查询（20 次） | 0.913 | 0.991 | 22 |
| 等值连接（2000 × 1000，哈希） | 0.201 | 0.230 | 9950 |
| 外键子表插入（每行唯一索引点查） | 2.147 | 3.135 | 931 |

## 5. 遗留风险与未实现项

1. **动作子集仍有限**：未实现 `MATCH PARTIAL`；当前支持
   `RESTRICT` / `CASCADE` / `SET NULL` / `SET DEFAULT` / `NO ACTION` 和
   `DEFERRABLE` / `SET CONSTRAINTS`（`MATCH SIMPLE` / `MATCH FULL`）。
2. **级联收集不自动建子表索引**：写入侧 `_check_foreign_keys` 已优先走被引用唯一索引，
   `_plan_referenced_updates` / `_cascade_delete_children` 在子表外键列存在精确索引时
   也走点查，但外键不会自动创建子表索引；没有索引时仍逐表扫描，复杂度与子表行数
   线性相关，数据量大时应显式 `CREATE INDEX`。
3. **复杂循环图**：`_updating` / `processing` 能打断常见自引用环，但菱形依赖、
   同一子行被多条级联路径同时命中的精确 SQL 顺序语义没有单独建模；
   并发事务下也没有等锁语义，当前更接近单写者语义。`SET CONSTRAINTS` 只覆盖当前
   事务，同名约束按全局匹配一并切换。
4. **`DROP TABLE ... CASCADE` 不删子表数据**：只删除引用表上的外键约束，
   与部分数据库的“级联删表数据”语义不同；`TRUNCATE ... CASCADE` 会递归清空引用子表。
5. **保守拒绝**：`DROP INDEX` / `DROP CONSTRAINT` 只要还被外键引用就拒绝，
   即使表中还有另一条可支撑的唯一索引。
6. **`ADD COLUMN ... REFERENCES` 依赖语句级回滚**：先追加列，再由
   `ADD CONSTRAINT` 校验；失败时靠事务 undo 恢复，未做“先完整预检再加列”的
   两阶段优化。
7. **基准是合成负载**：只覆盖单机、小规模、固定行数，不能代表真实并发和
   大数据量表现。

## 6. 验收结论

本轮外键能力已形成完整闭环：DDL 创建/变更、DML 写入校验、删除/更新级联、
索引点查、事务回滚、持久化和文档均已落地；全量 909 个测试通过，最新已推送代码提交
`02e9c68` 的 CI 成功。上述遗留项属于后续扩展，不影响当前已声明支持的
`RESTRICT` / `CASCADE` / `SET NULL` / `SET DEFAULT` / `NO ACTION`、
`DEFERRABLE INITIALLY DEFERRED` / `SET CONSTRAINTS` 与 `MATCH SIMPLE` / `MATCH FULL` 子集。
