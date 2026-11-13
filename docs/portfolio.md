# 怎么把这个项目写进简历、怎么在面试里讲

这个文件不是给"读者"看的，是给你自己用的：面试前十分钟翻一遍，确保每个
数字和每个技术点都能落到具体代码上。

## 一句话定位

> 用纯 Python（零第三方依赖）从零实现了一个单机关系型数据库引擎：4096B
> slotted page 存储、B+ 树索引、WAL + ARIES 风格崩溃恢复、MVCC 快照隔离、
> 手写 SQL 词法/语法/绑定/执行器，并实现了 PostgreSQL v3 wire protocol，
> 可以直接用 `psql` 连上去查询。

这句话里每个名词都对应仓库里一段可运行的代码和一个能跑的测试，不是概念堆砌。

## 简历写法（中文）

> **LumenDB — 从零实现的关系型数据库引擎**（个人项目，Python 3.12，零第三方依赖）
>
> - 实现 4096B slotted page 存储格式与 38B 堆记录头，支持行内变长编码、
>   RID 稳定寻址、MVCC 版本链与惰性删除；
> - 实现 B+ 树索引（分裂、范围扫描、空节点塌陷），随机插入/删除与内存
>   `dict` 对照校验，并用全树不变量检查兜底；
> - 实现 WAL 与 ARIES 风格 redo/undo 恢复，以 pageLSN 保证重放幂等；
>   用"绕过缓冲池直接写数据页"的方式模拟断电，验证已提交数据存活、
>   未提交数据回滚；
> - 显式 `ROLLBACK` 不只写 abort 日志：Buffer Pool 按事务记录页的 before image，
>   逆序恢复并写 CLR 推进 pageLSN；同时恢复 Catalog 的表/索引根页与目录根指针，
>   使 `TRUNCATE`、`VACUUM`、DDL 在事务中真正可回滚（含重启回归测试）；
> - 实现事务内 `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE`：保存点记录
>   页 undo 与目录 undo 的深度，回滚只撤销保存点之后的部分，失败语句可用保存点
>   恢复后继续事务；
> - 手写 SQL 词法分析器与递归下降解析器，把表达式编译成闭包以消除执行期的
>   名字解析开销；等值和范围条件命中索引时生成索引计划，
>   等值连接生成哈希连接（NULL 键不匹配、重复键展开、剩余谓词复核），
>   支持 `UNION` / `UNION ALL` 集合算子与整体排序/限量，非递归 `WITH`（CTE）
>   按书写顺序绑定并可在 `FROM` 中当作表引用，也支持 `WITH ... INSERT ... SELECT`，
>   `WITH RECURSIVE` 用 seed + 递归项逐轮迭代（`UNION ALL` 保留重复、`UNION` 去重），
>   `INSERT ... SELECT` 先物化来源再批量写入（避免自插入无限增长），
>   `CREATE TABLE AS SELECT` 从查询输出推断列名/类型并落表，
>   `INSERT` / `UPDATE` / `DELETE` 支持 `RETURNING`（新行/旧行语义、`*` 展开），
>   `INSERT ... ON CONFLICT DO UPDATE` 支持冲突目标匹配唯一索引并在旧行上
>   计算赋值、同步索引，
>   `EXCEPT` / `INTERSECT` 支持集合差与交集（`INTERSECT` 优先级更高），
>   `ALTER TABLE ADD COLUMN` 走目录升级 + 旧记录读时补齐（不重写堆页），
>   `ALTER TABLE RENAME TO` / `RENAME COLUMN` 只改目录并同步索引列引用；列级/表级
>   `CHECK` 约束以 token 切片持久化，写入统一校验并支持存量回填；`RENAME` 会同步
>   重写 token 里的列/表引用；`ALTER COLUMN
>   SET/DROP NOT NULL` 先校验存量行，再只改目录约束；`DROP CONSTRAINT` 可移除
>   CHECK、UNIQUE 或 PRIMARY KEY（删除主键保留 NOT NULL），`DROP INDEX`
>   拒绝约束索引；`ADD COLUMN ... CHECK` 会先按默认值补齐旧行再验证；`ALTER COLUMN
>   TYPE / SET DATA TYPE` 走全表物化、类型预校验、逐行重写，并在类型变化后
>   从堆表重建受影响索引；
>   `CREATE VIEW` 保存 `AS` 之后的词法切片，不物化数据，查询时展开为 `CteScan`，
>   支持 `OR REPLACE`、显式列名和 `SHOW VIEWS`，建视图时即校验绑定，
>   并把真实表/视图依赖落进目录，`DROP TABLE` / `DROP VIEW` 会拒绝悬空删除；
>   `EXPLAIN` 输出真实执行路径，`EXPLAIN ANALYZE` 标注实际耗时与行数；
> - 实现 PostgreSQL v3 wire protocol（启动握手、simple/extended query、
>   Parse/Bind/Describe/Execute、`$n` 文本/二进制参数、Describe Statement/Portal
>   静态列描述、Close 生命周期、带 SQLSTATE 的错误响应），可直接用 `psql` 连接；
> - 772 个自动化测试（含崩溃注入），CI 覆盖 Ubuntu/Windows × Python 3.12/3.13。

## 简历写法（English）

> **LumenDB — a relational database engine built from scratch** (Python 3.12,
> standard library only)
>
> - Designed a 4096-byte slotted-page storage format with a 38-byte tuple header,
>   in-place variable-length encoding, stable RID addressing, MVCC version chains
>   and lazy deletion.
> - Implemented a B+ tree index (node splits, range scans, empty-node collapse),
>   cross-validated against an in-memory `dict` plus whole-tree invariant checks.
> - Built a WAL with ARIES-style redo/undo recovery; pageLSN makes replay
>   idempotent. Crash safety is verified by bypassing the buffer pool and writing
>   pages directly, then reopening and asserting committed rows survive while
>   uncommitted ones roll back.
> - Explicit ROLLBACK restores page before-images in reverse order and writes
>   CLRs to advance pageLSN; catalog table/index root pointers and catalog_root
>   are restored too, so TRUNCATE, VACUUM and DDL are rollback-safe across reopen.
> - Implemented transactional SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE:
>   savepoints capture page-undo and catalog-undo depths, so a failed statement
>   can be rolled back while the transaction continues.
> - Wrote the SQL lexer and recursive-descent parser by hand, and compile
>   expressions into closures so name resolution is paid once at bind time.
>   Equality and range predicates on indexed columns become index scans;
>   equi-joins use a hash join (NULL keys never match, duplicate keys expand,
>   residual predicates are rechecked); `UNION` / `UNION ALL` are supported
>   with compound-level ORDER BY and LIMIT; non-recursive `WITH` (CTEs) are
>   bound in declaration order and can be referenced as tables in FROM;
>   `WITH ... INSERT ... SELECT` uses the same scope;
>   `INSERT ... SELECT` materializes its source before writing;
>   `CREATE TABLE AS SELECT` infers column names/types and creates a plain table;
>   `INSERT` / `UPDATE` / `DELETE` support `RETURNING` (new-row/old-row semantics
>   and `*` expansion);
>   `INSERT ... ON CONFLICT DO UPDATE` matches unique-index conflict targets,
>   evaluates assignments on the old row and maintains indexes;
>   `EXCEPT` / `INTERSECT` implement set difference and intersection;
>   `WITH RECURSIVE` iterates seed + recursive term round by round
>   (`UNION ALL` keeps duplicates, `UNION` deduplicates);
>   `ALTER TABLE ADD COLUMN` upgrades
>   the catalog and pads old records at read time instead of rewriting the heap;
>   `ALTER TABLE RENAME TO` / `RENAME COLUMN` update catalog metadata and
>   index column references without rewriting heap records; column- and table-level
>   `CHECK` constraints persist token slices, enforce on all writes and backfill
>   existing rows; `RENAME` rewrites table/column references inside the tokens;
>   `DROP CONSTRAINT` removes CHECK, UNIQUE or PRIMARY KEY constraints (dropping
>   a primary key keeps `NOT NULL`), while `DROP INDEX` rejects constraint-owned
>   indexes; `ADD COLUMN ... CHECK`
>   pads old rows with the default before validating; `ALTER COLUMN
>   SET/DROP NOT NULL` validates existing rows and updates only the catalog;
>   `ALTER COLUMN TYPE` / `SET DATA TYPE` materializes rows, pre-validates
>   conversions/defaults/CHECKs, rewrites each row and rebuilds affected
>   indexes from the heap because index key encoding is type-dependent;
>   `CREATE VIEW` persists the token slice after `AS` instead of materializing
>   data, expands to a `CteScan` at bind time, and supports `OR REPLACE`,
>   explicit column names and `SHOW VIEWS` with bind-time validation; resolved
>   table/view dependencies are persisted so `DROP TABLE` / `DROP VIEW` reject
>   dangling deletions;
>   `EXPLAIN` reports the plan that actually runs, and `EXPLAIN ANALYZE`
>   annotates each operator with actual time, rows and loop count.
> - Implemented the PostgreSQL v3 wire protocol (startup handshake,
>   simple/extended query, Parse/Bind/Describe/Execute, `$n` text/binary
>   parameters, static Describe metadata, Close lifecycle, SQLSTATE-bearing
>   error responses), so `psql` can connect directly.
> - 772 automated tests including crash-injection and a 20-seed randomized
>   crash-consistency suite; CI matrix covers Ubuntu/Windows × Python 3.12/3.13.

## 七个可深挖的点（面试主力）

### 1. 为什么用物理日志（page-level redo）而不是逻辑日志

物理日志记录"某页某偏移的旧值/新值"，redo 时不需要重新推导执行计划，
只要把字节写回去，天然幂等——用 `pageLSN` 判断"这页是否已经重放过这条
日志"即可。代价是日志体积大（整页 4096B），但对这个规模的项目，正确性
远比日志压缩重要。逻辑日志（记录"插入了哪一行"）体积小，但 redo 时要
重放执行逻辑，一旦执行器有不确定行为（比如依赖当前时间）就会重放出不同
结果。

可以顺手提一句你知道优化方向：把整页日志改成"页内 delta"或做组提交。

### 2. 一个真实的 B+ 树坑：键里多塞了 RID

最初的实现里，调用方传 `values = 列值 + RID`，树内部又自己拼了一次 RID，
导致键长度是 5 而不是 3。索引能写进去，但删除时按 3 个值找不到，删除失效、
重插报重复键。教训是"复合键的边界必须只有一个地方负责拼装"。
现在 `KeyCodec` 负责在列类型后追加 RID，`insert/delete` 只接收列值。

### 3. UPDATE 的"自反馈"bug（最值得讲的一个）

`UPDATE t SET v = v + 1` 在数据跨页之后会**无限执行下去**：执行器一边
顺序扫描堆表，一边把新版本追加到堆里；扫描器走到下一页时又看到了刚写进去
的新版本，于是继续更新，循环不终止。

问题的本质是"边遍历边修改"违反了迭代器的不变量。修复方式是先把目标行
物化成列表再修改（`targets = list(heap.scan(txn))`），并把这条写成了回归
测试：插入 500 行（跨 6 页）后更新，断言行数仍是 500。

怎么发现的：基准脚本跑 `UPDATE 500 行` 时卡住两分钟不出结果，`cProfile`
显示 scan 调用次数远超行数——这个 bug 只在跨页时出现，小数据量的单测
永远测不到。

### 5. 一个"看起来支持、其实没接线"的 HAVING bug

最危险的一类 bug 不是崩溃，而是**功能静默失效**：解析器认识 `HAVING`，绑定器
会编译它，`docs/sql-support.md` 也写着支持——但执行计划里根本没有这个过滤节点，
`HAVING SUM(v) > 30` 会把所有分组原样返回。

它之所以长期没被发现，是因为原来的测试数据让每个分组都满足条件：
`(1,10),(1,20),(2,30)` 两组 `SUM(v)` 都是 30，`> 25` 恒真。测试通过，功能是假的。
修的时候把数据改成 `(2,30),(2,1)`，让一组满足、一组不满足，测试才真正有判别力。

现在 `HAVING` 被编译成 `Aggregate` 之上的 `Filter`；同时 `GROUP BY` 支持任意
表达式，SELECT 里的同形表达式会被改写成对分组键的引用。这件事的教训是：
**测试用例必须能让错误实现失败**，否则它只是给"已经写了代码"盖章。

### 6. 为什么系统目录存在数据文件里，而不是单独的 JSON 文件

如果目录是外部文件，它和数据页就无法"一起"崩溃或一起回滚：DDL 提交到
一半进程被杀，可能出现"目录里有这张表，但数据页没建"。把目录编码成 JSON
存进数据文件自己的页链（`PageType.CATALOG`），目录的更新就自动走同一套
WAL 与事务机制，原子性由已有的基础设施保证。

代价是每次 DDL 都要重写整条目录链（目录小，可接受），而且目录加载要
处理"页尾填充零字节"这类细节（踩过：直接 `json.loads` 会报 `Extra data`）。

### 7. 为什么选 PostgreSQL 协议而不是自己写个 HTTP 接口

因为"能直接用 `psql` 连上"是一个无法伪造的验收标准：握手字节、类型 OID、
`RowDescription` 的字段顺序、`ReadyForQuery` 的事务状态，任何一处不对，
客户端就会报协议错误。相比之下，自定义 HTTP 接口只有自己写的客户端能跑，
说服力弱得多。

讲的时候可以补一句：我知道自己没做 TLS（`SSLRequest` 回 `N`），但参数绑定是做了的——
`Parse` 记下客户端声明的类型 OID，`Bind` 把文本/二进制参数解码，`Describe` 回
`ParameterDescription`。JDBC 风格的"预处理语句 + 参数"能跑通，这条边界是我
主动划的，不是"忘了"。

多连接隔离还踩过一个坑：`Database._txn` 是单连接时代的字段，而 wire 服务端
所有连接共用一个 `Database`。结果 A 连接 `BEGIN` 之后，B 连接的查询会直接
加入 A 的事务，**读到未提交数据**。修法是把事务挂到 `Session` 上，`Database`
退回纯执行门面；连接断开时回滚未提交事务，失败事务的 `COMMIT` 按 PostgreSQL
语义变成 `ROLLBACK`。这条行为由两个真实 socket 连接的测试覆盖，而不是只测
单连接的 SQL 语义。

互操作也不是"自己测自己"：本机没有 psql，我用同源的 libpq 驱动 psycopg2
跑通了 `examples/psycopg2_smoke.py`——DDL、参数化查询、两连接隔离、唯一键
冲突的 SQLSTATE `23505` 都按客户端预期返回。协议字段只要错一个，libpq 就
不会给出正常结果。

## 常见追问（准备好了就答得出来）

**"你的 MVCC 和 PostgreSQL 差在哪？"**
PostgreSQL 有 tuple 的 `xmin/xmax`、clog（commit log）、快照 xip 列表；
我这里用同样的可见性规则，但没有 clog（用事务表在内存里维护状态）。
`VACUUM` 是有的：它要求没有并发事务，扫描出当前快照可见的每一版，把
"根元组 + 版本链"重排成每行一条记录，再重建索引。差的是 autovacuum、
oldest-xmin 水位和 `VACUUM FULL`——所以我的回收是"停机维护"级别的，
不是 PostgreSQL 那种可以在线跑的。这个边界写在 `docs/architecture.md` 里。
还差一件：`ROLLBACK` 只写中止记录，不做进程内 undo（重启恢复时才真正回滚）。
所以 `UPDATE` 必须从"快照可见的版本"出发，不能盲信 newest 指针——否则会
从已中止的版本继续，根记录的 `xmax` 永远不会被设置，同一行会同时可见两次。
这个 bug 是 20 个种子的随机崩溃测试抓出来的：固定用例恰好没有覆盖
"abort 之后又 update 同一行"。

**"为什么不做代价模型？"**
因为没有表统计信息（不支持独立的 `ANALYZE` 统计），估算基数只能靠猜，做一个假的代价模型
不如不做。当前只有两条规则：单表等值 + 有索引 → 索引查找；单列范围条件
（`>`/`>=`/`<`/`<=`/`BETWEEN`）→ B+ 树范围扫描。`EXPLAIN` 输出的是真实计划，
不是理想计划；`EXPLAIN ANALYZE` 再把每个算子的实际耗时、行数、循环次数打出来。
范围扫描按索引列前缀比较，忽略键尾自动追加的 RID——否则
`age > 30` 会把 `(30, rid)` 当成大于边界，把 30 本身也扫出来。

**"哈希连接怎么处理 NULL 和重复键？"**
哈希键里有 `NULL` 的右侧行不进哈希表，左侧探测键含 `NULL` 时直接视为无匹配——
这不是优化，是 SQL 语义：`NULL = NULL` 的结果是 `NULL`，不是真。重复键在哈希桶里
存一个列表，探测时全部展开，所以一对多连接不会丢行。连接条件里的非等值部分
（`ON a.id = b.id AND a.x < b.y`）不进哈希键，但在候选行上仍用完整 `ON` 复核，
避免"哈希只保证候选、不保证谓词"的漏判。没有等值键（逗号连接、`OR`）时退回
嵌套循环，`EXPLAIN` 会显示实际用的是哪一种。

**"`ALTER TABLE ADD COLUMN` 为什么不重写表？"**
重写全表在数据量上来后是停机操作，而且会改变所有 RID、必须重建全部索引。
这里的做法是目录里追加列定义，旧记录保留原来的 `ncols`；解码时发现记录列数
少于表定义，就按新列的 `DEFAULT`（没有默认值则 `NULL`）补齐。于是 DDL 是 O(1)，
旧行可以继续被扫描、更新、建索引；`UPDATE` 或 `VACUUM` 把该行重新编码时，
才写成包含新列的完整格式。代价是同一张表里新旧行格式共存，解码路径必须显式
知道"缺列是合法的"——这也解释了为什么 `decode_record` 有一个 `allow_missing`
开关，而不是默默吞掉所有列数不一致。

**"CTE 在计划里长什么样？"**
`WITH` 不是语法糖：解析器把每个 CTE 存成独立的 `SELECT` 树，绑定阶段按书写
顺序把它们绑成独立计划放进当前作用域；`FROM cte_name` 生成 `CteScan`，它的
child 就是那份计划。后面的 CTE 能引用前面的 CTE，同名的 CTE 遮蔽真实表；
`WITH c(a, b) AS (...)` 可以覆盖输出列名并校验列数与重名。递归 `WITH RECURSIVE`
由 `RecursiveCte` 逐轮执行 seed + 递归项。没有做“同一个 CTE 被引用多次只算一次”的
物化缓存——现在每次引用都会重新执行一次，这是明确的取舍。

**"`INSERT ... SELECT` 为什么要先物化？"**
如果边扫来源边写目标，而来源就是目标表本身，扫描器会读到刚插入的新版本，
表会无限增长。实现里先把来源查询完整物化成元组列表，再逐行写堆和索引；
列数在绑定阶段就与目标列核对，所以空结果也不会绕过 schema 检查。

**"性能怎么样？"**
`bench/results/benchmark.md` 是实测（每场景 3 次，取最好与中位）：扫描/聚合约 3 万行/秒；
带索引插入 3.4 千行/秒，不带索引 1.2 万行/秒；索引点查约 1.0ms/次，全表扫描 4.6ms/次；
范围查询（`score >= 99`）走索引约 3.1ms/次，全表约 45ms/次，约 15 倍；
等值连接 2000 订单 × 1000 客户，哈希连接 0.18s 出 2000 行，约 1.1 万行/秒；
`TRUNCATE` 2000 行 4ms，`VACUUM` 压实 2000 行（回收 500 个死版本）0.90s。

这里有一个我主动做的优化，也是最能讲的一段：最初带索引插入只有 498 行/秒，
cProfile 显示每次插入都调用 `codec.decode` 十几万次——因为叶子页是按
"整页解码成 Python 对象、改完再整页重新编码"的方式实现的。我把叶子改成
**槽位目录 + 页内二分**：记录区从页头向上长，槽位目录从页尾向下长，
插入只搬一段 u16 再把记录追加到页尾，查找只解码命中的几条记录。
同一台机器同一参数，插入从 4.0s 降到 0.47s，9 倍。

改造过程还顺带抓出两个真 bug：叶子链的 next/prev 字段写反了，以及摘除
最左/最右叶时边界邻居的指针没更新。它们之前一直藏着，因为"删空整棵树"
的用例最后只剩根叶，链断了也看不出来。现在 `check_invariants()` 会校验
"叶子链顺序 == 树的中序顺序"，这类错误不可能再溜过去。

**"这个项目最难的部分是什么？"**
恢复。因为它的错误不会在写的时候暴露，只会在"崩溃之后"暴露。我的做法是
把崩溃变成可测试的：绕过缓冲池落盘路径，直接把数据页写到磁盘，模拟
"日志已落盘、数据页没落盘"，然后重开数据库断言结果。这个过程逼出了两个
真实 bug：新页初始化没有写 WAL、目录根指针被元数据页覆盖。

## 演示顺序（如果有人要看现场）

1. `python examples/demo.py` —— 30 秒看完 DDL/DML/索引/聚合/重开校验；
2. `python -m lumendb serve --path ./mydb --port 5432`，另一个窗口
   `psql -h 127.0.0.1 -p 5432 -U lumendb -d lumendb` —— 用原生客户端连；
3. `python -m pytest tests/crash -q` —— 崩溃注入测试；
4. `python -m bench.run --rows 2000` —— 现场跑基准，数字不是抄的。

## 提交前的两件事（只有你能做）

1. `git config user.name` / `user.email` 现在是占位的
   `LumenDB <dev@lumendb.local>`，改成你自己的 GitHub 身份；
2. 在 GitHub 上建一个空仓库，然后
   `git remote add origin <url> && git push -u origin main`。
