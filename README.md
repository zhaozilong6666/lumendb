# LumenDB

[![CI](https://github.com/zhaozilong6666/lumendb/actions/workflows/ci.yml/badge.svg)](https://github.com/zhaozilong6666/lumendb/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> A relational database engine written from scratch in Python —— slotted pages, B+tree indexes, WAL crash recovery, MVCC snapshot isolation, rule-based index selection, and a PostgreSQL wire-compatible server you can connect to with `psql`.

LumenDB 是一个**从零手写**的关系型数据库系统。它不依赖任何第三方数据库组件，也没有把工作外包给 SQLite——SQL 解析、查询优化、执行算子、页式存储、B+ 树索引、预写日志、崩溃恢复、并发控制，全部是项目自己实现的代码。

写这个项目的目的是把"数据库"这门课里被跳过的部分真正走一遍：一条 `SELECT` 从网络字节流进来，到最终从磁盘页里读出一行，中间究竟发生了什么。

---

## 为什么值得看

大多数"数据库项目"最后是一层 ORM 或者一个 KV 包装。LumenDB 的每一层都可以独立压测与验证：

| 能力 | 实现方式 | 如何验证 |
|---|---|---|
| 存储格式 | 4096B slotted page，行内变长编码，RID 稳定寻址 | `tests/unit/test_page.py` 覆盖碎片、回收、compact |
| 索引 | B+ 树：分裂 + 范围扫描 + 惰性删除（空节点塌陷） | 随机插入/删除与 `dict` 对照，`check_invariants()` 校验全树 |
| 崩溃恢复 | WAL + ARIES 风格 redo/undo，pageLSN 幂等重放 | `tests/crash/test_recovery.py`：数据页不落盘直接"崩溃"，重开后校验 |
| 并发控制 | MVCC 快照隔离（读并发）+ 单写者事务（写冲突 `40001` 快速失败）+ first-updater-wins 版本冲突检测；行级 `S`/`X` 锁与等待图死锁检测已在 `storage/lock.py` 实现，但只被单元测试使用，尚未接入写路径 | `tests/unit/test_concurrency.py`：并发写、唯一键回滚、外键并发、并发 DDL |
| 查询执行 | 火山模型算子 + 哈希连接 + 等值/范围索引选择 | `EXPLAIN` 输出与实际执行路径对照 |
| 协议 | PostgreSQL v3 wire protocol（simple + extended query，支持 `$1` 参数绑定） | `tests/unit/test_pgwire.py` 走真实 socket；可用 `psql` 直连 |
| 复制 | TCP 物理 WAL 流、分块基准备份、token 认证、zlib 压缩响应、可选 TLS、多副本槽位、页级增量基准备份、重定向、只读副本、LSN 延迟、手动/自动提升、多数派投票、心跳 RPC/独立心跳循环/多数派续租、TTL 写租约、租约锁/后台续租、副本提升自动续租/租约写围栏/租约丢失自动降级/自动提升租约 | `tests/unit/test_replication.py`：本地与真实 TCP 两条路径；生命周期与围栏见 `tests/unit/test_replication_lifecycle.py`、`tests/unit/test_write_fence.py` |
| 进程模型 | 数据目录独占锁（OS 级字节锁），拒绝双开 | `tests/unit/test_filelock.py`：第二个打开者必须失败 |

## 架构

```
        psql / DBeaver / 任意 PG 客户端
                     │  PostgreSQL wire protocol v3
        ┌────────────▼─────────────┐
        │  server/pgwire.py        │  启动握手、简单查询、Parse/Bind/Execute
        │  engine/database.py      │  门面：组装存储 + 目录 + 执行器 + 事务
        └────────────┬─────────────┘
        ┌────────────▼─────────────┐
        │  sql/lexer.py → parser   │  手写词法 + 递归下降语法分析 → AST
        │  plan/binder.py          │  名字解析、类型检查、表达式编译成闭包
        │  plan/nodes.py           │  计划节点：Scan/IndexLookup/IndexRange/Filter/Join/…
        └────────────┬─────────────┘
        ┌────────────▼─────────────┐
        │  engine/executor.py      │  火山模型：一次一行的迭代器
        └────────────┬─────────────┘
        ┌────────────▼─────────────┐
        │  storage/heap.py         │  堆文件、可见性判断、版本链
        │  storage/btree.py        │  B+ 树索引（键 = 列值 + RID）
        │  storage/bufferpool.py   │  LRU 淘汰、pin/unpin、脏页
        │  storage/pager.py        │  页分配、读写、元数据页
        │  storage/wal.py          │  预写日志、LSN、fsync
        │  storage/wal_segments.py │  分段 WAL、轮转与安全回收
        │  storage/recovery.py     │  analysis / redo / undo / checkpoint
        │  storage/txn.py, lock.py │  事务状态、快照隔离、行锁与死锁检测
        │  storage/filelock.py     │  数据目录独占锁（拒绝双开）
        │  replication/*           │  WAL TCP 流、只读副本、手动提升
        │  catalog/*               │  表定义持久化（存进数据文件页链）
        └────────────┬─────────────┘
                     │
          lumendb.data / lumendb.wal
```

详细设计见 [`docs/architecture.md`](docs/architecture.md)、[`docs/storage-format.md`](docs/storage-format.md)；
部署与排障（租约/fence/水位/故障剧本）见 [`docs/operations.md`](docs/operations.md)；
并发模型（三层串行化、为什么写并发度是 1、放开它的三步）见 [`docs/concurrency.md`](docs/concurrency.md)。

长周期开发的事实账本见 [`docs/memory/README.md`](docs/memory/README.md)：每条结论都挂
commit / 文件锚点 / 测试作为证据，`python -m tools.project_memory verify` 可一键复查，
并会自动检测"同一主题下互相矛盾的数字"——避免把过期的测量当成当前事实。

## 快速开始

无需任何第三方依赖，只需要 Python 3.12+。

```bash
git clone https://github.com/zhaozilong6666/lumendb.git
cd lumendb

# 交互式 shell
python -m lumendb shell --path ./mydb

# 或者直接跑一条 SQL
python -m lumendb exec --path ./mydb "CREATE TABLE t (a INT)"

# 启动 PostgreSQL 协议服务端
python -m lumendb serve --path ./mydb --port 5432

# 另开一个终端，用原生 psql 直连
psql -h 127.0.0.1 -p 5432 -U lumendb -d lumendb
```

```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);
CREATE INDEX idx_users_age ON users (age);

INSERT INTO users VALUES (1, 'Ada', 36), (2, 'Linus', 55), (3, 'Grace', 45);

SELECT name, age FROM users WHERE age > 40 ORDER BY age DESC;
--  name   | age
--  Linus  | 55
--  Grace  | 45

BEGIN;
UPDATE users SET age = 46 WHERE name = 'Grace';
ROLLBACK;
```

保存点让事务可以只撤销一部分：

```sql
BEGIN;
SAVEPOINT before_cleanup;
DELETE FROM users WHERE age > 50;
ROLLBACK TO SAVEPOINT before_cleanup;  -- 只撤销 DELETE，事务继续
INSERT INTO users VALUES (4, 'Margaret', 85);
COMMIT;
```

也可以用内置 REPL：

```bash
python -m lumendb shell --path ./mydb
```

想一次性看完整个链路（建表、索引、连接、聚合、更新、重开校验），跑：

```bash
python examples/demo.py
```

它会打印每一步的真实输出，包括 `EXPLAIN` 里命中的索引名。

## 真实客户端

协议不是只有自带的测试客户端在跑：`examples/psycopg2_smoke.py` 用 **libpq**
（psycopg2）连服务端，覆盖 DDL、参数化查询、事务隔离与 SQLSTATE `23505`。

```bash
python -m pip install psycopg2-binary
python -m lumendb serve --path ./mydb     # 另开一个终端
python examples/psycopg2_smoke.py
```

`psql` 用的是同一个 libpq，本机没装 psql，所以用同源驱动做互操作验证：
握手、`RowDescription`、`DataRow`、`CommandComplete` 任何一处不合协议，
psycopg2 都不会正常返回结果。

## SQL 支持

| 类别 | 支持 |
|---|---|
| DDL | `CREATE TABLE`（列级/表级 `PRIMARY KEY` / `UNIQUE` / `CHECK` / `REFERENCES` / `FOREIGN KEY`，支持复合主键、复合唯一约束、复合外键、命名或自动命名 CHECK/外键、`MATCH FULL/SIMPLE`、`ON DELETE/UPDATE CASCADE/RESTRICT/SET NULL/SET DEFAULT/NO ACTION`（`SET NULL`/`SET DEFAULT` 支持 `(col...)` 列级动作）、`DEFERRABLE INITIALLY DEFERRED`，UNIQUE/PRIMARY KEY 也支持 `DEFERRABLE`）、`CREATE TABLE AS SELECT`、`CREATE [OR REPLACE] VIEW [ (col...) ] AS SELECT`、`CREATE [UNIQUE] INDEX [IF NOT EXISTS]`、`ALTER TABLE ... ADD COLUMN` / `ADD COLUMN IF NOT EXISTS` / `ADD COLUMN ... CHECK` / `ADD COLUMN ... REFERENCES` / `DROP [COLUMN] [IF EXISTS] name [CASCADE]` / `ADD [CONSTRAINT name] UNIQUE (col...)` / `ADD [CONSTRAINT name] PRIMARY KEY (col...)` / `ADD [CONSTRAINT name] CHECK (expr) [NOT VALID]` / `ADD [CONSTRAINT name] FOREIGN KEY (col...) REFERENCES ... [ON DELETE/UPDATE CASCADE/SET NULL/SET DEFAULT/NO ACTION] [NOT VALID]` / `VALIDATE CONSTRAINT name` / `ALTER CONSTRAINT name [DEFERRABLE|NOT DEFERRABLE] [INITIALLY DEFERRED|IMMEDIATE]` / `DROP CONSTRAINT [IF EXISTS] name [CASCADE]` / `ALTER [COLUMN] ... SET/DROP DEFAULT` / `SET/DROP NOT NULL` / `TYPE [USING expr] [CASCADE]` / `SET DATA TYPE [USING expr] [CASCADE]` / `RENAME TO` / `RENAME COLUMN` / `RENAME CONSTRAINT`、`DROP TABLE [CASCADE]`、`DROP INDEX [IF EXISTS] name [CASCADE]`、`DROP VIEW`、`SHOW VIEWS` |
| DML | `INSERT`（多行、`INSERT ... DEFAULT VALUES`、`INSERT ... SELECT`、`ON CONFLICT DO NOTHING` / `DO UPDATE` / `DO UPDATE ... WHERE` / `excluded.col` / `ON CONSTRAINT`）、`UPDATE`（支持 `SET col = DEFAULT`、单源表 `UPDATE ... FROM`）、`DELETE`（支持单源表 `DELETE ... USING`）、`RETURNING` |
| 查询 | `SELECT`、`WHERE`、`ORDER BY [NULLS FIRST/LAST]`、`LIMIT`/`LIMIT ALL`/`OFFSET`/`FETCH FIRST|NEXT ... ROWS ONLY|WITH TIES`、`DISTINCT`、`UNION`/`UNION ALL`/`EXCEPT`/`EXCEPT ALL`/`INTERSECT`/`INTERSECT ALL`、`WITH`（非递归 CTE，支持 `WITH c(a,b) AS ...`，含 `WITH ... UPDATE/DELETE`）、`WITH RECURSIVE`（`seed UNION [ALL] recursive`，支持显式列名） |
| 连接 | `INNER JOIN`、`LEFT JOIN`、`RIGHT JOIN`、`FULL JOIN`、`CROSS JOIN`、多表连接；等值 INNER/LEFT 走哈希，外连接补 NULL |
| 聚合 | `GROUP BY`（列或表达式）、`HAVING`、`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`、`COUNT(DISTINCT x)` |
| 表达式 | 算术、`\|\|` 拼接、比较、`AND`/`OR`/`NOT`、`IN`、`BETWEEN`、`LIKE`、`CAST`、`CASE WHEN`、`IS [NOT] DISTINCT FROM` |
| 子查询 | 标量子查询、`IN (子查询)`、`EXISTS`；支持单层/多层相关子查询（子查询可引用外层 `FROM` 列）；`FROM (SELECT ...) AS alias` 派生表（含 JOIN 右侧、列别名列表 `AS x(a, b)`、内部 `WITH`） |
| 事务 | `BEGIN`/`COMMIT`/`ROLLBACK`，`SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE`，`SET CONSTRAINTS ... DEFERRED/IMMEDIATE`，MVCC 快照隔离 |
| 运维 | `EXPLAIN`、`EXPLAIN ANALYZE`、`SHOW TABLES`、`CHECKPOINT`、`REINDEX [TABLE]`、`VACUUM [FULL]`、`TRUNCATE [TABLE] t [, ...] [CASCADE | RESTRICT]` |

未支持的语法会明确报错并给出原因，而不是静默返回错误结果——见 [`docs/sql-support.md`](docs/sql-support.md)。
外键与级联动作的实现、测试矩阵和遗留风险见
[`docs/foreign-key-milestone.md`](docs/foreign-key-milestone.md)。

## 基准测试

`bench/run.py` 是可复现的脚本，所有数字由它实测产生（不是手写）。

```bash
python -m bench.run --rows 2000
```

<!-- BENCH_TABLE_START -->
数据由 `python -m bench.run --rows 2000 --repeat 3` 在 Windows 11 / Python 3.12.4 / 本机 SSD 上实测：
连跑 3 轮，每轮内再重复 3 次，表里取各场景**最好的那一次**（本机是开发机，后台负载会造成数倍波动，
这里的数字是量级参考而不是 SLA）。

口径分两类，**不要混着看**：

* **写场景**（插入 / 更新 / VACUUM / TRUNCATE / 外键）：含建表与打开数据库，每条语句一个事务。
* **读场景**（扫描 / 聚合 / 点查 / 范围 / 连接）：**打开数据库一次，重复查询 N 次**，表里给的是单次
  查询的均摊耗时。此前每个场景都重开数据库，把 ~0.16–0.3 s 的冷启动摊进了单次查询，于是"点查
  855 行/秒"这类数字描述的其实是打开速度。冷启动现在单独列成一行。
* 读场景的"吞吐"列在点查/范围/连接行里是**次/秒**（每次查询返回的行数不同，用次更直观）。

| 场景 | 行数 | 最好 (s) | 中位 (s) | 吞吐 |
|---|---:|---:|---:|---:|
| 批量插入（含主键索引维护） | 2000 | 0.087 | 0.093 | 23096 行/秒 |
| 批量插入（无索引） | 2000 | 0.086 | 0.093 | 23262 行/秒 |
| 全表扫描 `COUNT(*)`（每次，热缓存） | 2000 | 0.010 | 0.011 | 191259 行/秒 |
| `GROUP BY`（100 个分组，每次） | 2000 | 0.013 | 0.014 | 151891 行/秒 |
| 更新 500 行（含打开） | 500 | 0.095 | 0.117 | 5240 行/秒 |
| `VACUUM` 压实 2000 行（回收 500 个死版本，含打开） | 2000 | 0.034 | 0.034 | 58811 行/秒 |
| `TRUNCATE` 清空 2000 行（含打开） | 2000 | 0.005 | 0.005 | 419331 行/秒 |
| 索引点查（200 次，**每次 SQL 文本都不同**） | 200 | 0.000 | 0.000 | 13146 次/秒 |
| 无索引点查（20 次，全表扫描，每次） | 20 | 0.011 | 0.012 | 90 次/秒 |
| 索引范围查询（`score >= 99`，50 次，每次） | 50 | 0.000 | 0.000 | 2862 次/秒 |
| 无索引范围查询（`score >= 99`，20 次，全表扫描，每次） | 20 | 0.010 | 0.011 | 95 次/秒 |
| 等值连接（2000 订单 × 1000 客户，哈希，每次） | 2000 | 0.021 | 0.024 | 93899 行/秒 |
| 冷启动（打开 2000 行库 + 1 次 `COUNT`） | 1 | 0.049 | 0.050 | 21 次/秒 |
| 外键子表插入（每行唯一索引点查，含建表） | 2000 | 1.669 | 2.838 | 1199 行/秒 |

**这一轮（2026-11-17）整机负载明显低于上一轮**：同一套基准的绝对值整体快
3–6 倍（全量测试也从 619s 降到 144s），所以**跨轮绝对值不能直接比较**。
本轮新增的性能改动只看同进程内交错 A/B 的相对收益：绑定计划缓存让同一 SQL
点查 307.6→189.2 us（−38.5%），200 个不同常量轮询 97.6→70.2 us/次（+28.1%）；
`BPlusTree.lookup` 前缀二分 35.2→28.0 us（−20.5%，轻载）/ 150→128 us
（−14.6%，重载）。
|---|---:|---:|---:|---:|
| 批量插入（含主键索引维护） | 2000 | 0.404 | 0.404 | 4955 |
| 批量插入（无索引） | 2000 | 0.119 | 0.120 | 16802 |
| 全表扫描 `COUNT(*)` | 2000 | 0.045 | 0.054 | 44634 |
| `GROUP BY`（100 个分组） | 2000 | 0.055 | 0.056 | 36659 |
| 更新 500 行 | 500 | 0.312 | 0.317 | 1601 |
| `VACUUM` 压实 2000 行（回收 500 个死版本） | 2000 | 0.350 | 0.623 | 5722 |
| `TRUNCATE` 清空 2000 行 | 2000 | 0.002 | 0.003 | 902772 |
| 索引点查（200 次） | 200 | 0.234 | 0.245 | 855 |
| 无索引点查（20 次，全表扫描） | 20 | 0.109 | 0.131 | 184 |
| 索引范围查询（`score >= 99`，50 次） | 50 | 0.205 | 0.210 | 244 |
| 无索引范围查询（`score >= 99`，20 次，全表扫描） | 20 | 0.913 | 0.991 | 22 |
| 等值连接（2000 订单 × 1000 客户，哈希） | 2000 | 0.201 | 0.230 | 9950 |
| 外键子表插入（每行唯一索引点查） | 2000 | 2.147 | 3.135 | 931 |

怎么读这张表：

- **点查 0.076ms vs 全表 11ms，约 146 倍。** 这是索引存在的最强证据：`id = ?` 在
  二级索引上只走树高（2000 行约 3 层），全表扫描每次要翻 2000 行、解码每个槽位。
  表再大差距只会更大，因为树高只以对数增长。
- **解析缓存对"重复同一条 SQL"有效，对字面量随机变化的查询无效。** 这一栏的
  点查每次 SQL 文本都不同（`WHERE id = 123` / `457` …），所以 200 次里有 200 次
  缓存未命中；同进程 A/B 实测（同一条 SQL 反复执行）点查从 0.149ms 降到
  **0.096ms（−35.6%）**，解析占比从 21.4% 降到 0.6%。真实应用的参数化查询
  （扩展协议 `$1`）走的正是"同一文本、不同参数"，所以收益落在它身上。
- **计划缓存再把"重复同一条 SQL"往下压 38.5%。** 绑定产物（计划/输出列/CTE）
  按 `(AST id, schema_version)` 缓存，DDL 抬高版本号整体失效；实测同一 SQL
  点查 307.6→189.2 us。字面量随机的查询也能命中（上限 256、逐条淘汰），
  200 个不同常量轮询 97.6→70.2 us/次（+28.1%）。
- **范围查询 0.35ms vs 全表 10.5ms，约 30 倍。** `score >= 99` 在索引上只读约 20
  个条目；全表扫描每次都要扫完整张表。
- **插入 23.1 千行/秒，含主键索引与不含索引几乎没有差别（23096 vs 23262）。**
  这条是"叶子槽位目录 + 页内二分"的收益：索引维护不再需要整页重编码，成本降到
  噪声级。改造成槽位目录之前，同一份基准只有 498 行/秒。
- **等值连接 2000 行结果 21ms。** 2000 个订单 × 1000 个客户，嵌套循环要做两百万次
  比较；哈希连接只建一次 1000 行哈希表、再 2000 次探测，差距随数据量线性拉大而不是
  平方。
- **`TRUNCATE` 5ms 清空 2000 行**：成本与页数相关，不逐行处理。`VACUUM` 要重写存活
  行，34ms / 2000 行（含 500 个死版本），属于停机维护的成本量级。
- **外键子表插入 1.2 千行/秒**：每行都要做一次父表唯一索引点查和堆可见性校验，
  比无外键的普通插入慢一个数量级，但避免了逐行扫描父表。
- **冷启动 49ms**：打开一个 2000 行的库并做一次 `COUNT(*)`，含 WAL 恢复与目录加载。
  这个成本是**一次性**的，之前它被摊进每一次查询里，才让读吞吐看起来低了一个量级。
- 扫描与聚合是纯内存操作，十几万行/秒量级，符合 Python 的预期。这个数字不是数据库
  引擎的卖点，正确性和可验证性才是。
<!-- BENCH_TABLE_END -->

复制路径另有一个脚本：

```bash
python -m bench.replication --rows 2000 --repeat 3
```

它量化基准备份、WAL 追平、`lag_from_server` 字节延迟和副本查询；实测与分析见
[`docs/replication-bench.md`](docs/replication-bench.md)。同一 2000 行场景下，
多区间页 diff 把写入后 WAL 延迟从 13.9 MB 降到 1.1 MB；基准备份已分块传输，
不再整库读进内存。这些数字是相对基线，不作为 SLA。

## 怎么验证它真的能跑

- **1285 个自动化测试**（`python -m pytest tests -q`，约 20–120 秒，视机器与平台而定），覆盖页格式、WAL 与恢复、
  B+ 树不变量、词法/语法、绑定与执行、协议收发、锁与死锁；
- **崩溃恢复用例是"真拔电"**：绕过 buffer pool 的刷盘路径直接写数据页，模拟进程被杀，
  重启后分别校验"已提交必须存活、未提交必须回滚"；
- **随机崩溃一致性测试**：20 个固定种子，随机执行插入/更新/删除并随机提交、回滚或
  直接崩溃，每次重开后与测试侧维护的"已提交模型"逐行比对；失败信息带 seed，可复现；
- **真实客户端互操作**：`examples/psycopg2_smoke.py` 用 libpq（psycopg2）走一遍
  握手、DDL、参数化查询、两连接事务隔离与 SQLSTATE `23505`；
- **基准数字由脚本产出**，README 里没有手写性能；
- CI 在 Ubuntu / Windows × Python 3.12 / 3.13 上跑全量测试 + CLI 冒烟。

## 开发

```bash
python -m pytest tests -q          # 全部测试
python -m pytest tests/unit -q     # 单元测试
python -m pytest tests/crash -q    # 崩溃恢复注入测试
python -m pytest tests/stress -q   # 随机崩溃一致性测试（20 seeds）
```

CI 见 `.github/workflows/ci.yml`：Ubuntu/Windows × Python 3.12/3.13 矩阵，
跑全量测试 + CLI 冒烟。

## 路线图

- [x] 页式存储、Buffer Pool、WAL
- [x] B+ 树索引（分裂 / 范围扫描 / 惰性删除）
- [x] 堆表 MVCC 版本链与快照隔离
- [x] SQL 词法、语法、AST
- [x] 绑定、类型检查、表达式编译
- [x] 火山模型执行器（Scan/Filter/Project/Join/Aggregate/Sort/Limit）
- [x] 等值与范围的索引选择（`EXPLAIN` 可见）
- [x] PostgreSQL wire protocol（simple + extended query，支持 `$1` 参数绑定）
- [x] `VACUUM`：回收死版本、压实堆页、重建索引（要求无并发事务）
- [x] 代价模型基础设施：`plan/cost.py` 提供扫描/过滤/连接/投影/聚合/排序/集合/`LIMIT` 基数与 CPU 估算，`EXPLAIN` 输出 `rows≈/cpu≈` 标注，并作为左深连接顺序与哈希构建侧选择的输入
- [x] 哈希连接构建侧选择：INNER 连接按估算选择较小的一侧建表，`EXPLAIN` 显示 `[build=left/right]`
- [x] 左深动态规划连接顺序：3–8 个源按子集枚举选择 `rows + cpu` 最小的顺序；逗号连接和纯 INNER 显式 `JOIN` 链都会重排，外连接/混合 `JOIN`/超过 8 源保持原序；输出列顺序由投影还原；`EXPLAIN` 标记 `[order=dp] [candidates=N]`
- [x] 唯一/主键约束 `DEFERRABLE`：事务内暂存重复键，`COMMIT` 或 `SET CONSTRAINTS ... IMMEDIATE` 时统一校验
- [x] 外键：`MATCH SIMPLE/FULL`、`RESTRICT/CASCADE/SET NULL/SET DEFAULT/NO ACTION`、`DEFERRABLE INITIALLY DEFERRED` 提交时检查、`SET CONSTRAINTS` 事务内切换、`ALTER CONSTRAINT` 修改延迟属性、`ALTER TABLE` 外键 DDL、`SET NULL/SET DEFAULT (col...)` 列级动作、`ADD CONSTRAINT ... NOT VALID`（外键/CHECK）与 `VALIDATE CONSTRAINT`
- 外键 `MATCH PARTIAL`：不做（PostgreSQL 本身不支持，见 `docs/roadmap.md`「明确不做」）
- [x] 基于代价的连接顺序枚举（左深 DP，`rows + cpu`）
- [x] 单源谓词下推：多源查询中只引用一个数据源的 `WHERE` 合取项下推到扫描之上，最终 `WHERE` 仍保留兜底 `Filter`
- [x] 相关子查询
- [x] 组提交：WAL 追加/刷盘线程安全，多个并发提交由一个 leader 合并刷盘，提交记录持久化后才标记 `COMMITTED`
- [x] 自动 CHECKPOINT：WAL 自上次 checkpoint 增长超过阈值时触发，保留手动 `CHECKPOINT`
- [x] WAL 分段回收：新库使用 `wal-<first_lsn>.log` 分段，checkpoint 后按安全 LSN 删除旧段，活跃事务 BEGIN 之前的历史保留
- [x] 按提交数触发的 checkpoint 策略
- [x] 复制 / 只读副本（MVP）：TCP 物理 WAL 流、基准备份写冻结、只读拒绝写入、LSN 延迟、手动提升
- [x] 复制槽：副本 ack 的 `confirmed_lsn` 原子持久化，`Database(wal_retain_lsn=...)` 在启动恢复前恢复保留点
- [x] 手动提升 fencing：副本提升时原子写 marker，旧主通过写回调拒绝写入
- [x] 自动提升 MVP：`ReplicationMonitor` 连续网络失败后 `promote`
- [x] 客户端重定向：`WALServer(redirect_to=...)`、`WALClient(follow_redirects=True)`、监控器自动跟随
- [x] 多数派投票 MVP：`QuorumNode.campaign()` 需要多数票才写提升 fence；投票持久化任期并拒绝落后候选
- [x] 写租约 MVP：`Lease` 以 TTL 约束旧主库写入，`campaign(lease_path=...)` 在多数票后获取租约
- [x] 租约串行化与续租：`Lease` 用 `<lease>.lock` 串行化 acquire/renew/release，`LeaseKeeper` 后台续租
- [x] 心跳 RPC / 多数派续租 MVP：`QuorumNode.heartbeat_round()` 统计心跳，`renew_lease()` 只有多数派确认才续租
- [x] 提升后写围栏：`promote_with_quorum(lease_path=...)` 把 `LeaseFence` 装到新主库，租约丢失或观察到更高任期即拒绝写入/续租
- [x] 租约丢失自动降级：`lease_loss_stepdown=True` 时 keeper 续租失败自动 `step_down()`，主库切回只读
- [x] 任期感知续租：`Lease.renew(term=...)` 与 `QuorumNode.renew_lease()` 拒绝用更高任期节点续旧任期租约
- [x] 写围栏唯一入口：语句入口（`_write_lock` 内）+ 提交点复检 + 线协议路径都走 `Database.assert_writable()`，只读事务不受影响
- [x] 提升后的后台生命周期：`promote_with_quorum` 自动起续租线程（默认 `ttl/3`）并可启用心跳循环，`close()`/`step_down()` 先停心跳再停 keeper
- [x] 跟随器状态机：`MonitorState.following/promoting/promoted/read_only/needs_attention`，租约丢失降级后不再报 `promoted`
- [x] 心跳并发扇出：`heartbeat_round` 用线程池发送，总耗时约等于单个 timeout 而不是 `N × timeout`
- [x] 多数派提交水位（轻量版）：副本上报 `match_lsn`，leader 维护 `quorum_commit_lsn`，`campaign()` 拒绝落后于水位的候选（`tests/unit/test_quorum_commit.py`）
- [x] 仲裁节点 `status` RPC：网络可读 `term` / `match_lsn` / `commit_lsn` / 最近心跳来源，用于排查"水位为什么不前进"
- [x] 预投票（pre-vote）：`campaign()` 先探询意向，拿不到多数派就失败且**不推进任期**，避免分区节点把任期越推越高
- [x] 自动提升租约：`ReplicationMonitor(quorum=..., lease_path=...)` 把投票、租约、心跳续租和写围栏串到自动提升路径
- [x] 独立心跳循环：`HeartbeatLoop` 在后台持续向 peers 发心跳，观察到更高任期时回调上层停写，回调异常记录到 `last_callback_error` 且不会终止后台线程
- [x] 心跳超时自动竞选：`QuorumNode(election_timeout=...)` 看门狗在收不到 leader 心跳后自动发起竞选，双候选同窗竞争收敛到唯一主库（`test_two_candidates_pick_exactly_one_leader`）
- [x] 逐条任期与拼接点校验：`VoteState.log_terms` 按 LSN 持久化任期分段，`append_entries(entry_terms=...)` 重放后逐条落段，拼接点 `(prev_lsn, prev_term)` 分歧拒绝并要求重建
- [x] 分叉/缺口自动重建：leader 下发 `rebuild_from` 端点，follower 后台重新基准备份并对齐进度；提升后空 WAL 的缺口由 leader 主动发 `rebuild_hint`（`test_lagging_follower_gap_triggers_auto_rebuild`）
- [x] 重启双写防护：`Database(fence_path=/lease_path=)` 打开即检共享 marker 并持续接进写路径，堵住"旧主库崩溃重启后双写"
- [x] 完整 commit index 协商与分歧尾巴截断：分歧尾巴采用「重建替代截断」（更保守、无截断误删窗口）；commit index 由 leader 按多数派 `match_lsn` 算出 `verified_commit_lsn` 自证，同步屏障/提升闸门只信它，follower 侧口头 `commit_lsn` 属标准 Raft 行为、不做 match 表广播（见 [`docs/replication-commit-index.md`](docs/replication-commit-index.md)）
- [x] 同步复制（可选）：`sync_replication=True` 时 COMMIT 要等多数派水位追平，超时给 `40003`（结果未知、不回滚），只读事务不受影响
- [x] 复制流二进制记录帧：WAL 记录走 `B` + 23 B 定长头 + 原始 payload，8,124 条记录的流体积 1,634,288 B → 863,144 B（1.89x，省 47.2%）；非记录消息仍是 JSON 行，按首字节分派，老客户端不受影响，默认关闭
- [x] 副本跟随读等待：`Replica.wait_until(lsn, host, port, timeout)` 阻塞到副本应用目标 LSN，用于"写入后重定向到副本"时避免读到旧前缀
- [x] 副本一致快照读：`Replica.snapshot_read()` 把多条只读语句固定在同一个已应用前缀上；默认读路径也由"事务级应用单元"保护——apply 只在 COMMIT/ABORT 之后放出记录，**单个事务超过 `max_records` 时先在副本侧暂存**，等边界到达再整体应用，不再出现"表页已应用、索引页未应用"的中间态（`tests/unit/test_replica_partial_transaction.py`），详见 [`docs/replication-correctness.md`](docs/replication-correctness.md)
- [x] 复制流长连接复用：`WALClient(keepalive=True)` / `Replica.replicate_from_server(keepalive=True)` 在一条 TCP 连接上发多个请求，服务端按请求循环并支持压缩与二进制帧混用，空闲超时回收线程，断连自动重连
- [x] 重建失败自愈：页级增量重建失败（本地页损坏 / 主库已回收旧页）时自动回退到全量基准备份，旧数据文件保留到 `broken-<时间戳>/` 供取证
- [x] Raft 级日志协商（任期/前缀一致性检查）：`append_entries` 带逐条任期做前缀校验，任期分歧拒绝；commit index 由 leader 按多数派 `match_lsn` 算出 `verified_commit_lsn` 自证（见 [`docs/replication-commit-index.md`](docs/replication-commit-index.md)）

长期分层路线、阶段依赖与“不青黄不接”的完成标准见
[`docs/roadmap.md`](docs/roadmap.md)。

## 想把它讲出来

面试前想快速回忆"哪些点值得深挖、被追问怎么答"，见
[`docs/portfolio.md`](docs/portfolio.md)——里面记了四个真实的调试故事
（无限自更新的 UPDATE、键里多塞 RID 的索引、目录尾部的填充零字节、
被测试放过的 HAVING 静默失效），以及"为什么不做代价模型"这类问题的回答。

## 设计取舍

- **单文件页式存储 + 物理日志**：牺牲了压缩率，换来恢复逻辑的可验证性。
- **读并发、写单写者**：读走各自的 MVCC 快照，互不阻塞；写事务用一把事务级写者锁串行，
  冲突直接 `40001` 快速失败。换来的是"目录页、唯一索引项、版本链不会被两个写事务同时踩"；
  代价是写并发度是 1。行级锁与死锁检测（`storage/lock.py`）已经实现，但接入写路径需要
  先把语句级锁收窄到物理写，否则会引入等待图看不见的等待环——留给下一版。
- **索引不做借位与合并**：删除走惰性标记，空节点从父节点摘除并修复叶子链。
  少了一套最容易出错的分支，代价是稀疏页要靠 `REINDEX table` 重建来回收。
- **`VACUUM` 是停机维护，不是在线回收**：重写堆页会改变 RID，随后必须重建索引；
  没有 autovacuum、没有 oldest-xmin 水位、文件不会缩小。换来的是回收逻辑
  可以被一条 SQL 触发、也可以被崩溃恢复测试断言。
- **目录存进数据文件的页链**：不用独立的 `catalog.json`，让表定义和用户数据
  共享同一份 WAL——否则"表建好了但数据没落盘"这类中间状态要另立机制处理。
- **Python**：这个项目的目标是"把机制写清楚、可被测出来"，不是吞吐量竞赛。
- **不做静默降级**：任何未实现的能力都抛 `NotSupportedError`，避免出现"看着像支持、结果算错"。

## License

MIT
