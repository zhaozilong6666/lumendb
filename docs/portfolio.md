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
> - 手写 SQL 词法分析器与递归下降解析器，把表达式编译成闭包以消除执行期的
>   名字解析开销；实现等值条件索引选择，`EXPLAIN` 输出真实执行路径；
> - 实现 PostgreSQL v3 wire protocol（启动握手、simple/extended query、
>   带 SQLSTATE 的错误响应），可直接用 `psql` 连接；
> - 406 个自动化测试（含崩溃注入），CI 覆盖 Ubuntu/Windows × Python 3.12/3.13。

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
> - Wrote the SQL lexer and recursive-descent parser by hand, and compile
>   expressions into closures so name resolution is paid once at bind time.
>   Equality predicates on indexed columns become index scans; `EXPLAIN` reports
>   the plan that actually runs.
> - Implemented the PostgreSQL v3 wire protocol (startup handshake,
>   simple/extended query, SQLSTATE-bearing error responses), so `psql` can
>   connect directly.
> - 406 automated tests including crash-injection; CI matrix covers
>   Ubuntu/Windows × Python 3.12/3.13.

## 五个可深挖的点（面试主力）

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

### 4. 为什么系统目录存在数据文件里，而不是单独的 JSON 文件

如果目录是外部文件，它和数据页就无法"一起"崩溃或一起回滚：DDL 提交到
一半进程被杀，可能出现"目录里有这张表，但数据页没建"。把目录编码成 JSON
存进数据文件自己的页链（`PageType.CATALOG`），目录的更新就自动走同一套
WAL 与事务机制，原子性由已有的基础设施保证。

代价是每次 DDL 都要重写整条目录链（目录小，可接受），而且目录加载要
处理"页尾填充零字节"这类细节（踩过：直接 `json.loads` 会报 `Extra data`）。

### 5. 为什么选 PostgreSQL 协议而不是自己写个 HTTP 接口

因为"能直接用 `psql` 连上"是一个无法伪造的验收标准：握手字节、类型 OID、
`RowDescription` 的字段顺序、`ReadyForQuery` 的事务状态，任何一处不对，
客户端就会报协议错误。相比之下，自定义 HTTP 接口只有自己写的客户端能跑，
说服力弱得多。

讲的时候可以补一句：我知道自己没做 TLS（`SSLRequest` 回 `N`）和参数绑定
（`$1` 明确报 `NotSupportedError`），这两个是刻意的边界，不是"忘了"。

## 常见追问（准备好了就答得出来）

**"你的 MVCC 和 PostgreSQL 差在哪？"**
PostgreSQL 有 tuple 的 `xmin/xmax`、clog（commit log）、快照 xip 列表；
我这里用同样的可见性规则，但没有 clog（用事务表在内存里维护状态），
也没有 `VACUUM`（旧版本只标记不回收）。所以我的表在大量更新后会持续膨胀，
这是已知取舍，写在 `docs/architecture.md` 里。

**"为什么不做代价模型？"**
因为没有统计信息（没有 `ANALYZE`），估算基数只能靠猜，做一个假的代价模型
不如不做。当前只有一条规则：单表等值 + 有索引 → 索引扫描。`EXPLAIN` 输出
的是真实计划，不是理想计划。

**"性能怎么样？"**
`bench/results/benchmark.md` 是实测：扫描/聚合 4.5 万行/秒；带索引插入
约 500 行/秒，不带索引约 2 万行/秒。40 倍的差距来自 B+ 树每次插入都要
整页解码再重编码，不是"数据库就该慢"。优化方向是叶内槽位目录 + 就地插入。

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
