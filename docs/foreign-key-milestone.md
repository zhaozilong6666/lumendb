# 外键完整性与级联动作阶段报告

- 日期：2026-11-13
- 范围：LumenDB 外键约束、删除/更新动作、`ALTER TABLE` 外键 DDL、测试与文档
- 基线：`8da5a63`（`ON DELETE CASCADE`）
- 本轮提交：`9cc420f`、`640f38d`、`d27fea0`、`42627d8`、`9039add`、`5c3dd00`、`35b702e`、`2c20173`、`8a88bad`

## 1. 成果清单

| 能力 | 状态 | 说明 |
|---|---|---|
| 列级 `REFERENCES` / 表级 `FOREIGN KEY` | 完成 | 支持复合键、自引用、省略被引用列（取主键） |
| 写入引用完整性 | 完成 | `INSERT`、`INSERT ... SELECT`、`DEFAULT VALUES`、upsert、`UPDATE` 统一校验；SQLSTATE `23503` |
| `MATCH SIMPLE` | 完成 | 本地键任一列为 `NULL` 时跳过检查 |
| `MATCH FULL` | 完成 | 复合本地键必须全 `NULL` 或全非 `NULL`，部分 `NULL` 报错 |
| `ON DELETE RESTRICT` | 完成 | 默认动作，阻止删除仍被引用的父行 |
| `ON DELETE CASCADE` | 完成 | 递归删除子行，维护子表索引，支持多级链与自引用链 |
| `ON DELETE SET NULL` | 完成 | 将匹配子行的外键列置空；`NOT NULL` 子列会报错并回滚 |
| `ON DELETE SET DEFAULT` | 完成 | 父行删除后把匹配子行写回列默认值；默认值不存在时回滚 |
| `ON UPDATE RESTRICT` | 完成 | 默认动作，阻止更新仍被引用的父键 |
| `ON UPDATE CASCADE` | 完成 | 父键更新后递归同步子行，支持多级链与自引用链 |
| `ON UPDATE SET NULL` | 完成 | 父键更新后将匹配子行的外键列置空；`NOT NULL` 子列会报错并回滚 |
| `ON UPDATE SET DEFAULT` | 完成 | 父键更新后将匹配子行写回列默认值；默认值不存在时回滚 |
| `NO ACTION` | 完成 | 接受该语法；无 `DEFERRABLE` 时按立即 `RESTRICT` 处理 |
| `ALTER TABLE ADD CONSTRAINT FOREIGN KEY` | 完成 | 先校验存量行，再原子写入目录；支持 `ON DELETE/UPDATE CASCADE/RESTRICT/SET NULL/SET DEFAULT` |
| `ALTER TABLE ADD COLUMN ... REFERENCES` | 完成 | 复用 `ADD CONSTRAINT` 的存量行回填/校验路径，失败整条语句回滚 |
| 外键 DDL 依赖保护 | 完成 | `DROP COLUMN`、`ALTER COLUMN TYPE`、`DROP INDEX`、`DROP CONSTRAINT`、`DROP TABLE`、`TRUNCATE` 均有保护 |
| 元数据持久化 | 完成 | `ForeignKeyDef` 的列、引用表/列、`on_delete`、`on_update` 全部写入目录 JSON，兼容旧目录缺省值 |
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
`RESTRICT` / `CASCADE` / `SET NULL` / `SET DEFAULT` 和 `SIMPLE` / `FULL`，非法动作或
MATCH 类型直接拒绝。`NO ACTION` 在解析层归一化为立即 `RESTRICT`，因此目录里不会
留下未定义的延迟动作。

### 2.2 插入与更新校验

`_check_foreign_keys` 按外键的 `MATCH` 模式检查，并优先使用被引用列上的唯一索引点查：

- `MATCH SIMPLE`：本地键任一列为 `NULL`：跳过；
- `MATCH FULL`：本地键全为 `NULL`：跳过；部分 `NULL`：直接报错；全非 `NULL`：
  继续查找完整匹配行；
- 否则必须在被引用表中找到完整匹配行；
- 找不到抛 `ForeignKeyViolationError`。

`INSERT`、`INSERT ... SELECT`、`DEFAULT VALUES`、upsert 和 `UPDATE` 都走这一入口。
外键引用列要求存在 `UNIQUE` 或 `PRIMARY KEY` 索引，本地列与被引用列类型必须完全一致。

### 2.3 删除级联

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

### 2.4 更新级联

`_rewrite_row` 在父行写堆前调用 `_plan_referenced_updates`：

- 扫描引用该表的外键，按被引用列旧值/新值判断键是否变化；
- `RESTRICT` 且存在匹配子行：立即抛 `ForeignKeyViolationError`；
- `SET NULL`：收集待更新子行，将外键列置空；
- `SET DEFAULT`：收集待更新子行，写入列默认值；
- `CASCADE`：收集待更新子行，合并同一子行的多个外键动作；
- 父行完成索引删除、堆更新、索引重建后，递归调用 `_rewrite_row` 更新子行；
- 子行更新继续走自己的 CHECK、外键、唯一索引和下游级联路径；
- `_updating` 集合用于打断循环引用。

### 2.5 `ALTER TABLE`

- `ADD CONSTRAINT FOREIGN KEY`：先扫描存量行，再写入目录；失败不留下半状态。
- `ADD COLUMN ... REFERENCES`：先加列，再复用 `ADD CONSTRAINT` 路径建立外键；
  任一步失败整条语句回滚（测试覆盖“列已加、外键校验失败”场景）。
- `DROP TABLE ... CASCADE`：删除其他表上引用该表的外键约束，不删除子表数据。
- `DROP COLUMN` / `ALTER COLUMN TYPE`：目标列仍被本地或外部外键使用时拒绝。
- `DROP INDEX` / `DROP CONSTRAINT`：删除支撑外键的唯一键时保守拒绝。
- `RENAME TABLE` / `RENAME COLUMN` / `RENAME CONSTRAINT`：同步外键元数据。
- `TRUNCATE`：被外键引用的表直接拒绝。

## 3. 测试与验证

- 全量测试：**839 passed**（`python -m pytest tests -q`，约 35 秒）
- 编译检查：`python -m compileall -q lumendb tests` 通过
- 空白检查：`git diff --check` 通过
- 测试覆盖重点：
  - 基础外键、复合键、自引用、省略被引用列；
  - `ON DELETE CASCADE` 单级/多级/自引用链、索引维护、事务回滚；
  - `ON UPDATE CASCADE` 单级/多级/自引用链、事务回滚；
  - `ON DELETE SET NULL` / `ON UPDATE SET NULL`、`NOT NULL` 冲突回滚；
  - `MATCH FULL` 全空/全非空/部分 NULL 拒绝；
  - `NO ACTION` 解析为立即 `RESTRICT`；
  - `ON DELETE SET DEFAULT` / `ON UPDATE SET DEFAULT`、默认值缺失回滚；
  - `ALTER ADD CONSTRAINT` 的 `SET NULL` 动作；
  - `RESTRICT` 阻止删除/更新；
  - `ADD COLUMN ... REFERENCES` 成功与失败回滚；
  - `ALTER ADD CONSTRAINT` 的 `ON DELETE/UPDATE CASCADE`；
  - 外键持久化；
  - DDL 依赖保护与重命名同步。

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

1. **动作子集仍有限**：未实现 `DEFERRABLE` / `MATCH PARTIAL`；当前支持
   `RESTRICT` / `CASCADE` / `SET NULL` / `SET DEFAULT` / `NO ACTION` 和
   `MATCH SIMPLE` / `MATCH FULL`。
2. **级联收集不自动建子表索引**：写入侧 `_check_foreign_keys` 已优先走被引用唯一索引，
   `_plan_referenced_updates` / `_cascade_delete_children` 在子表外键列存在精确索引时
   也走点查，但外键不会自动创建子表索引；没有索引时仍逐表扫描，复杂度与子表行数
   线性相关，数据量大时应显式 `CREATE INDEX`。
3. **复杂循环图**：`_updating` / `processing` 能打断常见自引用环，但菱形依赖、
   同一子行被多条级联路径同时命中的精确 SQL 顺序语义没有单独建模；
   并发事务下也没有等锁/延迟约束，当前更接近单写者语义。
4. **`DROP TABLE ... CASCADE` 不删子表数据**：只删除引用表上的外键约束，
   与部分数据库的“级联删表数据”语义不同；`TRUNCATE` 没有 `CASCADE`。
5. **保守拒绝**：`DROP INDEX` / `DROP CONSTRAINT` 只要还被外键引用就拒绝，
   即使表中还有另一条可支撑的唯一索引。
6. **`ADD COLUMN ... REFERENCES` 依赖语句级回滚**：先追加列，再由
   `ADD CONSTRAINT` 校验；失败时靠事务 undo 恢复，未做“先完整预检再加列”的
   两阶段优化。
7. **基准是合成负载**：只覆盖单机、小规模、固定行数，不能代表真实并发和
   大数据量表现。

## 6. 验收结论

本轮外键能力已形成完整闭环：DDL 创建/变更、DML 写入校验、删除/更新级联、
索引点查、事务回滚、持久化和文档均已落地；全量 839 个测试通过，已推送代码提交
`5c3dd00` 的 CI 成功。上述遗留项属于后续扩展，不影响当前已声明支持的
`RESTRICT` / `CASCADE` / `SET NULL` / `SET DEFAULT` / `NO ACTION` 与
`MATCH SIMPLE` / `MATCH FULL` 子集。
