# 并发模型与"写并发度 1"的真实原因

这份文档回答两个问题：**现在到底有几层串行化**、**要放开写并发需要动什么**。
它不是路线图口号，每一条都对应代码里的具体位置和一条能失败的测试。

---

## 1. 现状：三层串行化，缺一不可

| 层 | 位置 | 保护什么 | 去掉会怎样 |
|---|---|---|---|
| **L1 语句锁** | `Database._statement_lock`（`engine/database.py`） | `Executor` 的**每条语句状态**：`binder`、`$n` 参数、CTE 作用域、递归工作表 | 两个线程同时 SELECT 会互相覆盖 binder：带 CTE 的查询丢作用域、参数化查询拿到别人的参数。实测：去掉后 `tests/unit/test_concurrent_statements.py` 的并发 CTE 用例 **30/30 轮失败** |
| **L2 事务写者锁** | `Executor._acquire_writer_lock` + `storage/lock.py:WRITER_RESOURCE` | 让"写事务"之间互斥：目录页、唯一索引项的物理删除、版本链 | 两个写事务同时改同一行 → 一个逻辑行 `SELECT` 出两行；回滚复活被删的唯一键；并发 DDL 互相踩目录页（都是实测过的真实故障） |
| **L3 quiesce 锁** | `Database._write_lock` | `close()` / `snapshot()` / `checkpoint()` 与写语句互斥 | 备份/降级期间数据页被改，基准备份拿到混合状态；`step_down()` 把页写到已关闭的句柄上 |

外加 `PGWireServer` 的 asyncio 锁（`server/pgwire.py`）：它把"单条语句"串行化，
所以**在线协议下** L1 一直是隐式成立的——这也解释了为什么 L1 的缺口长期没被发现：
只有直接用 `Database` API 的多线程调用者会踩到（`Replica.execute_one` 靠
`Replica._lock` 躲过一劫）。

### 为什么 L2 不能用行锁直接替掉

`storage/lock.py` 的 `S`/`X` + 等待图 DFS 早就写好了，但**接不上**，原因不是锁本身：

1. `Executor` 把语句状态放在实例字段上（L1 的成因）。想并行跑两条语句，就得把
   这些状态改成按调用传递的上下文——那是 4400 行执行器的结构性改动；
2. 目录（`catalog/*`）是"单根指针 + 页链"，**每条 DML 提交前都会 `persist` 一次**，
   两个写事务必然写同一批目录字节；
3. 唯一索引项在 `DELETE` 时被**物理删除**，并发 `INSERT` 看不到它 —— 只有键级
   互斥（或索引项 MVCC）能修；
4. 物理 undo 不按事务区分字节，条件 undo（`storage/recovery.py`）只能止血，
   它的正确性前提恰好是"没有两个事务写同一字节区间"。

所以顺序必须是：**先拆掉语句状态 → 再接行/键/目录锁 → 最后才能拿掉 L2**。
反过来做（先拿掉写者锁）会立刻回到实测过的那批故障。

---

## 2. 放开写并发的三步

### 第 1 步：执行器按会话隔离（已完成，`9922875` / 本轮）

两条路可以做到"语句状态不被共享"，选的是成本低的那条：

| 方案 | 内容 | 成本 |
|---|---|---|
| A | 把状态放进 `StatementContext`，`Executor.run` 逐层传递 | 要改 4400 行执行器里所有 `self.binder/self._params/self._active_ctes/self._txn` 的读写点 |
| **B（已采用）** | `Executor` 保持"实例即语句状态"，但**每个会话一个执行器**：`Database.make_executor()`，`PGWireServer.Session.engine()` 懒创建，共享 `bp/catalog/txns/wal/locks` | 新增 12 行 + 一处 `session.engine()` 替换 |

B 的代价是"并发度 = 执行器个数"，而不是"任意并发"；`Database.execute` 仍然只有
一个执行器，所以 L1 保留。B 的收益是不动执行器内部就拿到了隔离，且为第 2 步
（行锁）准备好"多会话并行"的骨架。

**验收（已满足）**：`tests/unit/test_concurrent_statements.py::TestPerSessionExecutor`
直接绕过 `Database._run` 调两个执行器并发跑 CTE 查询与参数化查询，结果互不影响——
等价于"L1 不存在时仍然正确"。

风险：`Binder` 的 `outer_schemas` / 视图展开缓存、`_check_cache` 也要一起搬；
`_profile` 是 `EXPLAIN ANALYZE` 用的，必须按调用隔离，否则两个分析互相污染。

### 第 2 步：行锁 + 键锁 + 目录锁（替掉 L2）

| 资源 | 何时加 | 模式 | 释放 |
|---|---|---|---|
| `row_resource(table, root_rid)` | `_rewrite_row` / `_delete` 改一行之前 | X | 提交/回滚 |
| 同上 | `_check_foreign_keys` 命中父行后（`FOR KEY SHARE` 语义） | S | 提交/回滚 |
| `("ukey", table, index, key)` | 唯一索引插/删之前 | X | 提交/回滚 |
| `("ddl", table)` | 任何 DDL | X | 提交/回滚 |

**关键细节**：拿到行 X 锁之后必须**重新检查版本**——等待期间别人可能已经提交，
旧快照继续写会覆盖对方的 `newest` 指针。规则是"等锁 → 看链头是否仍是我快照
看到的那一版 → 不是就 40001"。这就是 PostgreSQL 在 REPEATABLE READ 下的行为：
先等，再报序列化失败。现在 `HeapFile._raise_if_concurrent_write` 里的检查原样
保留，只是从"立即失败"变成"等锁之后再判"。

目录仍然是瓶颈：第 2 步要让 DML **不再无条件 `persist`**（只有真正改了目录对象
才写），否则"目录锁"会把所有写者重新串起来，等于白做。

### 第 3 步：收掉 L2，重跑不变量

- 删 `_acquire_writer_lock`，`WRITER_RESOURCE` 只留给"需要全库互斥的维护操作"
  （VACUUM 已经要求无活跃事务，可以直接用）；
- 必跑的回归：`tests/integration/test_cross_layer_invariants.py`（6 条）、
  `tests/unit/test_concurrency.py`（14 条）、`tests/unit/test_write_fence.py`、
  以及 `tests/crash` 全套；
- 新增必跑：两个写事务改**不同行**必须并行（用屏障证明它们同时在语句内部）、
  改**同一行**必须 40001、父子表交叉更新必须靠等待图检出死锁（40P01）。

---

### 第 1.5 步：DML 不再无条件 persist 目录（已完成，2026-11-17）

这是"写并发三步"里真正拿性能收益的一步，落地方式比"改事务模型"便宜：

- `Catalog.persist` 是**全量**保存：每次新建一整条目录页链，并把每页的
  全页 before/after 写成 WAL 记录。此前**每条 DML** 都会调用它，即使
  INSERT/UPDATE 只是把 `row_count_hint` 加一。
- 现在执行器在 DML 开始前记录**整目录结构指纹**（`root_page` / `last_page`
  / 每个索引的 `root_page`，见 `_catalog_structure_fingerprint`），结束时
  只有指纹变化才 persist（`_persist_if_structure_changed`）。
- `row_count_hint` 留在内存里供 planner 用；重启后回退到上次结构性写入
  时的值——查询结果不变，只是行数估计暂时偏保守。
- 结构性字段不能降级：`_append_page` 用 `last_page` 把新页链到链尾，
  崩溃恢复后若它是旧值，下一次分配会覆盖链尾已有的 `next` 指针。
- 整目录而非单表：级联操作（`ON UPDATE CASCADE` / `SET NULL`）会改子表，
  子表也会分配新页；目录 persist 本来就是全量粒度，指纹必须同粒度。

实测（同机、1000 行逐行 INSERT、各跑 3 次）：

| 版本 | WAL 体积 | 每行 | 耗时 |
|---|---:|---:|---:|
| 每行 persist | 13,005,600 B | 13,006 B | 2.0–2.5 s |
| 结构变化才 persist | 793,851 B | 794 B | 1.7–1.8 s |

WAL 体积降到 **1/16.4（−93.9%）**。注意：批量 INSERT（一条 SQL 插 200 行）
本来就只有一次 persist，所以 `bench.replication` 的批量场景看不出差异；
受益的是逐行写入的真实路径（CLI 单条执行、pgwire 单条 INSERT）。

### 第 1.7 步：只读语句不再写 WAL（已完成，2026-11-17）

自动提交的 `SELECT` / `EXPLAIN` / `SHOW` 此前也走 `txns.begin()` + `txns.commit()`，
会在 WAL 里写一对 BEGIN/COMMIT 记录，而组提交的 leader 必然调 `wal.flush()`
——**每一次 SELECT 都要 fsync 一次**。实测（同机，100 次 `SELECT COUNT(*)`）：

| 版本 | WAL 增量 | 每次 SELECT |
|---|---:|---:|
| begin/commit 照写 | 4,200 B | 1.45–1.52 ms |
| 只读事务不写记录 | **0 B** | **0.18 ms** |

延迟降到 **1/8.3（−88%）**。做法：

- `TransactionManager.begin(read_only=True)` 跳过 `wal.begin()`，只把当前
  `next_lsn` 记成快照起点（仍登记进 `_active_starts`，长读事务要阻止
  checkpoint 回收它还要读的旧版本页）；
- `commit()` / `abort()` 对只读事务直接改内存结局表，不写记录、不触发
  fsync（只读事务没有页写，崩溃恢复不需要认识它）；
- `Database._run` 与 pgwire 的自动提交路径都按语句类型选 `read_only`；
  pgwire 里只读语句走 `read_guard()`，不拿写者锁。
- 显式 `BEGIN ... SELECT ... COMMIT` 仍是普通事务（入口无法预知后续语句）。

### 第 1.8 步：服务端并发（已完成，2026-11-17）

`PGWireServer` 此前用一把 asyncio 锁把**所有连接的所有语句**串起来，而且语句是
同步阻塞调用，留在事件循环线程里会把所有连接的网络 IO 一起卡住。现在：

- 语句执行（`_run_statements` / `_run_statement` / `Describe`）放进
  `ThreadPoolExecutor`（`min(32, cpu+4)` 个工作线程），用
  `loop.run_in_executor` 等待；网络读写与协议编解码仍在事件循环线程；
- 原来那把全局 asyncio 锁**删掉**了：写语句的互斥由 `Database._write_lock`
  （threading RLock）继续保证，只读语句走 `read_guard()`，两者都不依赖事件循环；
- 每个会话一个执行器（第 1 步）保证 `Executor` 的 binder / `$n` 参数 / CTE 作用域
  不跨连接串味；
- `Catalog` 加了一把 RLock（`@_locked`，23 个读/写方法）：CPython 保证单次 dict
  操作原子，但**迭代**期间被 DDL 改会抛 `RuntimeError: dictionary changed size
  during iteration`。目录锁是叶子锁（持有 `_write_lock` 的写路径可以继续拿它，
  反向不存在），所以不引入 ABBA；
- `serve_forever` 退出时 `shutdown(wait=True)` 关池。

正确性边界（写在这里，避免下一轮误读）：

1. **写语句仍然完全串行**（`_write_lock`），这是第 2 步没做完之前的刻意选择；
2. 读语句与写语句可以真正并行：MVCC 快照保证读不到未提交的行，读到的页链即使
   被并发追加新页也只会"多看到一页而已"，可见性由 xid 过滤；
3. 备份/降级（`snapshot()` / `close()`）持有 `_write_lock` 时仍是写静止点，
   读可以继续，但新事务不能开始写。

测试（`tests/unit/test_pgwire.py::TestWireConcurrency`）：两个连接各跑一条
`sleep(0.12)` 的 SELECT，总耗时 < 0.228 s（串行应 ≥ 0.24 s）；另一条用例证明
慢查询进行中，新连接的握手不被阻塞。

顺带效果：全量测试套件 118.9 s → **75.0 s**（套件里大量 `SELECT` 走只读事务，
不再每次 fsync）。

### 第 1.9 步：结构性写与在途读者互斥（已完成，2026-11-17）

第 1.8 步把读路径放开之后，`HeapFile.vacuum` 出现了一个 TOCTOU：

1. `vacuum` 检查 `t.active_xids` 为空（没有并发事务）；
2. 检查通过之后、重排页之前，**新的只读事务**可以开起来（它不再拿
   `_write_lock`），扫到正在重排的页——槽位目录与记录位置都在变。

`TRUNCATE` 与 shadow heap 重写（`ALTER TYPE` / `DROP COLUMN`）是同一类问题。
于是引入 `Database.exclusive_guard()`：拿 `_write_lock` 之后等 `_readers`
归零，并在此期间挡住新读者（`read_guard()` 先检查 `_no_readers` 标志）。
`_readers_cv` 本来就绑在 `_write_lock` 上，条件变量与写者锁天然同序，
不引入 ABBA。

`Database._run_locked` 与 pgwire 的 `_run_statement` 两处入口按语句类型选
guard：`Vacuum` / `Truncate` / `AlterTableAlterType` / `AlterTableDropColumn`
走 `exclusive_guard()`，其余写语句仍只拿 `_write_lock`。

顺带修掉一个可用性问题：此前 VACUUM 碰到并发读**直接报错**（"VACUUM 需要表上
没有并发事务"），读密集型系统里永远跑不起来；现在它等读者退出（毫秒级）再
独占执行。测试 `tests/unit/test_exclusive_guard.py`（4 例）：VACUUM 不再失败、
独占期间新读者被挡、在途读者挡住结构性写、四类语句都被正确标记。

注意：这是**结构性写与读者**的互斥，不是行级并发控制；第 2 步（行/键锁）
仍没开工，写语句之间依旧靠 `_write_lock` 完全串行。
### 第 2 步的评估结论（2026-11-17）

第 1 步（"DML 不再无条件 persist 目录"）**已落地**（见上面第 1.5 步），
所以这里原来的阻塞项（"每条 DML 都写同一份目录字节"）已经解除：DML 只在
结构变化时才碰目录。

第 2 步（行/键锁）仍暂不落地，剩下的真实阻塞是三条：

- 目录 persist 的**写入时机**仍是"结构变化时在事务内写"：两个同时分配新页的
  写事务仍可能写同一批目录字节（频率从每行一次降到每页一次，但没有消失）；
- 唯一索引项在 `DELETE` 时被**物理删除**，并发 `INSERT` 看不到它——只有键级
  互斥或索引项 MVCC 能修；
- 物理 undo 不按事务区分字节，正确性前提是"没有两个事务写同一字节区间"。

所以顺序不变：
1. ~~DML 不再无条件 persist 目录~~（已完成）；
2. 再做行/键锁（含"等锁后重新校验版本"，见上文）；
3. 最后收掉事务写者锁。

## 3. 已知的取舍（写在这里，避免下一轮再讨论一遍）

- **`VACUUM` 仍是停机维护**：它重排 RID、重建索引，需要"没有其它活跃事务"。
  行锁放开后这个前提不再天然成立，执行器已经在检查 `active_xids`。
- **witness 节点不参与 durability**：不存数据的投票节点算进多数派会让
  "多数派持有"比数据副本宽松；`docs/operations.md` 有提示，语义上应排除。
- **单写者不是 bug，是声明过的边界**：`README.md`、`docs/architecture.md` §2.5
  都写明"读并发、写单写者"。这份文档给出的是**放开它的正确顺序**，不是
  "现在就是坏的"。
