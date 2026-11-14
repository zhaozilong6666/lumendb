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
| 并发控制 | MVCC 快照隔离 + 行级写锁 + 等待图死锁检测 | 快照隔离用例 + 死锁检测单元测试 |
| 查询执行 | 火山模型算子 + 哈希连接 + 等值/范围索引选择 | `EXPLAIN` 输出与实际执行路径对照 |
| 协议 | PostgreSQL v3 wire protocol（simple + extended query，支持 `$1` 参数绑定） | `tests/unit/test_pgwire.py` 走真实 socket；可用 `psql` 直连 |
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
        │  storage/recovery.py     │  analysis / redo / undo / checkpoint
        │  storage/txn.py, lock.py │  事务状态、快照隔离、行锁与死锁检测
        │  storage/filelock.py     │  数据目录独占锁（拒绝双开）
        │  catalog/*               │  表定义持久化（存进数据文件页链）
        └────────────┬─────────────┘
                     │
          lumendb.data / lumendb.wal
```

详细设计见 [`docs/architecture.md`](docs/architecture.md)、[`docs/storage-format.md`](docs/storage-format.md)。

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
| DDL | `CREATE TABLE`（列级/表级 `PRIMARY KEY` / `UNIQUE` / `CHECK` / `REFERENCES` / `FOREIGN KEY`，支持复合主键、复合唯一约束、复合外键、命名或自动命名 CHECK/外键、`MATCH FULL/SIMPLE`、`ON DELETE/UPDATE CASCADE/RESTRICT/SET NULL/SET DEFAULT/NO ACTION`（`SET NULL`/`SET DEFAULT` 支持 `(col...)` 列级动作）、`DEFERRABLE INITIALLY DEFERRED`）、`CREATE TABLE AS SELECT`、`CREATE [OR REPLACE] VIEW [ (col...) ] AS SELECT`、`CREATE [UNIQUE] INDEX [IF NOT EXISTS]`、`ALTER TABLE ... ADD COLUMN` / `ADD COLUMN IF NOT EXISTS` / `ADD COLUMN ... CHECK` / `ADD COLUMN ... REFERENCES` / `DROP [COLUMN] [IF EXISTS] name [CASCADE]` / `ADD [CONSTRAINT name] UNIQUE (col...)` / `ADD [CONSTRAINT name] PRIMARY KEY (col...)` / `ADD [CONSTRAINT name] CHECK (expr) [NOT VALID]` / `ADD [CONSTRAINT name] FOREIGN KEY (col...) REFERENCES ... [ON DELETE/UPDATE CASCADE/SET NULL/SET DEFAULT/NO ACTION] [NOT VALID]` / `VALIDATE CONSTRAINT name` / `DROP CONSTRAINT [IF EXISTS] name [CASCADE]` / `ALTER [COLUMN] ... SET/DROP DEFAULT` / `SET/DROP NOT NULL` / `TYPE [USING expr] [CASCADE]` / `SET DATA TYPE [USING expr] [CASCADE]` / `RENAME TO` / `RENAME COLUMN` / `RENAME CONSTRAINT`、`DROP TABLE [CASCADE]`、`DROP INDEX [IF EXISTS] name [CASCADE]`、`DROP VIEW`、`SHOW VIEWS` |
| DML | `INSERT`（多行、`INSERT ... DEFAULT VALUES`、`INSERT ... SELECT`、`ON CONFLICT DO NOTHING` / `DO UPDATE` / `DO UPDATE ... WHERE` / `excluded.col` / `ON CONSTRAINT`）、`UPDATE`（支持 `SET col = DEFAULT`、单源表 `UPDATE ... FROM`）、`DELETE`（支持单源表 `DELETE ... USING`）、`RETURNING` |
| 查询 | `SELECT`、`WHERE`、`ORDER BY [NULLS FIRST/LAST]`、`LIMIT`/`LIMIT ALL`/`OFFSET`/`FETCH FIRST|NEXT ... ROWS ONLY|WITH TIES`、`DISTINCT`、`UNION`/`UNION ALL`/`EXCEPT`/`EXCEPT ALL`/`INTERSECT`/`INTERSECT ALL`、`WITH`（非递归 CTE，支持 `WITH c(a,b) AS ...`，含 `WITH ... UPDATE/DELETE`）、`WITH RECURSIVE`（`seed UNION [ALL] recursive`，支持显式列名） |
| 连接 | `INNER JOIN`、`LEFT JOIN`、`RIGHT JOIN`、`FULL JOIN`、`CROSS JOIN`、多表连接；等值 INNER/LEFT 走哈希，外连接补 NULL |
| 聚合 | `GROUP BY`（列或表达式）、`HAVING`、`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`、`COUNT(DISTINCT x)` |
| 表达式 | 算术、`\|\|` 拼接、比较、`AND`/`OR`/`NOT`、`IN`、`BETWEEN`、`LIKE`、`CAST`、`CASE WHEN`、`IS [NOT] DISTINCT FROM` |
| 子查询 | 标量子查询、`IN (子查询)`、`EXISTS`（不支持相关子查询，会明确报错） |
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
数据由 `python -m bench.run --rows 2000 --repeat 3` 在 Windows 11 / Python 3.12.4 / 本机 SSD 上实测。
每个场景重复 3 次，表中同时给出最好成绩与中位数；插入、扫描类场景每条语句一个事务（`fsync` 提交），
点查、范围和连接探针场景把查询包在**同一个显式事务**里，否则测到的是每条查询的 fsync 延迟而不是查找速度。

| 场景 | 行数 | 最好 (s) | 中位 (s) | 吞吐 (行/秒) |
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

- **索引插入约 5.0 千行/秒，是无索引的 1/3.4。** 这个差距是索引的固有成本
  （多写一页 B+ 树 + 一次二分定位），不是实现浪费；叶子改成槽位目录前同一份
  基准只有 498 行/秒，改完提升到数千行/秒。
- **点查约 1.2ms vs 全表 5.5ms，约 4–5 倍。** 索引省掉 2000 行扫描；表再大一点差距
  还会拉大，因为树高只以对数增长。
- **范围查询约 4.1ms vs 全表 45ms，约 11 倍。** `score >= 99` 在索引上只读约 20 个
  条目，全表扫描每次要翻 2000 行。这是范围索引存在的全部理由。
- **等值连接 2000 行结果约 0.20s。** 2000 个订单 × 1000 个客户，嵌套循环要做
  两百万次比较；哈希连接只建一次 1000 行哈希表、再 2000 次探测，差距随数据量
  线性拉大，而不是平方。
- **`TRUNCATE` 2ms 清空 2000 行**：成本与页数相关，不逐行处理；`VACUUM` 则要重写
  存活行，约 0.35–0.62s / 2000 行（含 500 个死版本），属于停机维护的成本量级。
- **外键子表插入约 0.93 千行/秒**：每行都要做一次父表唯一索引点查和堆可见性校验，
  比无外键的索引插入慢，但避免了逐行扫描 1000 行父表。
- 扫描与聚合是纯内存操作，3 万行/秒量级，符合 Python 的预期。这个数字不是数据库
  引擎的卖点，正确性和可验证性才是。
<!-- BENCH_TABLE_END -->

## 怎么验证它真的能跑

- **898 个自动化测试**（`python -m pytest tests -q`，约 25–40 秒，视机器负载而定），覆盖页格式、WAL 与恢复、
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
- [x] 外键：`MATCH SIMPLE/FULL`、`RESTRICT/CASCADE/SET NULL/SET DEFAULT/NO ACTION`、`DEFERRABLE INITIALLY DEFERRED` 提交时检查、`SET CONSTRAINTS` 事务内切换、`ALTER TABLE` 外键 DDL、`SET NULL/SET DEFAULT (col...)` 列级动作、`ADD CONSTRAINT ... NOT VALID`（外键/CHECK）与 `VALIDATE CONSTRAINT`
- [ ] 外键 `MATCH PARTIAL`
- [ ] 基于代价的连接顺序枚举（当前为规则化）
- [ ] 相关子查询
- [ ] 组提交与 WAL 分段回收
- [ ] 复制 / 只读副本

## 想把它讲出来

面试前想快速回忆"哪些点值得深挖、被追问怎么答"，见
[`docs/portfolio.md`](docs/portfolio.md)——里面记了四个真实的调试故事
（无限自更新的 UPDATE、键里多塞 RID 的索引、目录尾部的填充零字节、
被测试放过的 HAVING 静默失效），以及"为什么不做代价模型"这类问题的回答。

## 设计取舍

- **单文件页式存储 + 物理日志**：牺牲了压缩率，换来恢复逻辑的可验证性。
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
