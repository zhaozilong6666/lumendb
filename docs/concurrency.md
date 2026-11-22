# 并发模型与"写并发度 1"的真实原因

这份文档回答两个问题：**现在到底有几层串行化**、**要放开写并发需要动什么**。
它不是路线图口号，每一条都对应代码里的具体位置和一条能失败的测试。

---

## 1. 现状：三层串行化，缺一不可

| 层 | 位置 | 保护什么 | 去掉会怎样 |
|---|---|---|---|
| **L1 语句锁** | `Database._statement_lock`（`engine/database.py`） | `Executor` 的**每条语句状态**：`binder`、`$n` 参数、CTE 作用域、递归工作表 | 两个线程同时 SELECT 会互相覆盖 binder：带 CTE 的查询丢作用域、参数化查询拿到别人的参数。实测：去掉后 `tests/unit/test_concurrent_statements.py` 的并发 CTE 用例 **30/30 轮失败** |
| **L2 行/键/DDL 锁** | `Executor._lock_row` / `_lock_key` / `_acquire_writer_lock`（DDL 分支）+ `storage/lock.py` | 写写冲突：同一行、同一唯一键、DDL 互斥、目录保存 | 两个写事务同时改同一行 → 一个逻辑行 `SELECT` 出两行；回滚复活被删的唯一键；并发 DDL 互相踩目录页（都是实测过的真实故障） |
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
3. 唯一索引项曾在 `DELETE` 时被物理删除，并发 `INSERT` 看不到它——已改为键级
   互斥 + 索引项延迟回收（VACUUM 重建时清理），唯一性按 heap 可见性判，
   见下文「唯一索引的 DELETE/UPDATE 可见性已落地」；
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

### 第 2 步的前置：页级 latch（实测证据，2026-11-17）

`BufferPoolManager` 只有一把全局 RLock（`fetch_page` / `unpin` 在锁内取放），
**页内容的读写不在任何锁之下**：`HeapPage` 改 slot 目录、`free_space` 和记录
偏移时，安全性完全依赖 `Database._write_lock` 把所有写语句串行。

把这个前提绕开验证一次（2 线程 × 300 行、同一张表、各自独立事务、直接调
`HeapFile.insert`）：

| 指标 | 结果 |
|---|---|
| 请求插入 | 600 行 |
| 返回 RID | 600 个，去重后 **513** |
| RID 重复（同一 slot 被分配两次） | **87** |
| 表里实际扫到 | **513 行（丢 87 行，14.5%）** |
| 页链页数 vs `last_page` | **6 vs 7（链断了）** |

结论：放开写并发**不能从行锁/键锁开始**，第一件事必须是**页级 latch**——
每页一个读写 latch，`HeapPage` / `BTreePage` 的每个结构修改都在 latch 内；
B+ 树分裂/合并要同时持有父页与子页，必须按 `page_id` 升序加锁防死锁。
没有它，行锁只能保证"不同行不冲突"，挡不住"同一页的两行并发插入把 slot
目录写坏"——上面的 87 次重复分配就是这么来的。

探针：`D:\projects\lumendb-review\_probe_page_race.py`（`PYTHONPATH` 指向仓库
后运行）。

**已落地（2026-11-17，基础设施，尚未放开 `_write_lock`）**：

- `Page.latch`（`threading.RLock`，进 `__slots__`）：`BufferPool` 对每个
  `page_id` 只缓存一个 `Page` 对象，所以对象上的这把锁就是那一页的锁；
  `Page.view()` 共享同一块 `buf`，也共享同一把 latch；
- `HeapFile._apply_and_log` 与 `_insert_raw` **自锁**：读旧值 → 写日志 →
  写页三步在 `page.latch` 内完成，交错时不会出现"后者的 before image 已经
  含了前者的改动"；`_insert_raw` 在 latch 内**重新核算空间**，不够就返回
  `None` 让调用方重选页（`insert` 最多重试 4 次）；
- `BufferPoolManager.shape_lock(table_name)`：跨 `HeapFile` 实例共享的表级
  "形状锁"，覆盖"读 `last_page` → 判空间 → 必要时追加新页 → 更新链尾指针"。
  这一把必须挂在 BufferPool 上而不是 `HeapFile` 上——`HeapFile` 是每次执行
  新建的短命对象，挂它身上两个写者各拿一把锁等于没锁；
- 实测（同一探针）：RID 重复 **87 → 0**，表里扫到的行数 **513 → 600**。

**仍未覆盖**：`bp.new_page()` 拿到 page_id 与把新页链上链尾之间的窗口、
目录 `last_page`/`root_page` 的持久化与 catalog 重载之间的一致性。这两段要
等"行/键锁 + 目录锁"一起做；在此之前 `Database._write_lock` 仍然串行所有写
语句，页 latch 只是为放开设好的基础设施（本切片不改变对外并发语义）。
### 为什么索引页 latch 暂不做（架构判断，不是遗漏）

堆表的 latch 可以"一页一锁、自包含"：一次 insert 只碰一页。B+ 树不行 ——
分裂要同时改**子页**和**父页**，合并（`_detach_if_empty`）则从叶子往父页反
向改。要让索引 latch 正确，必须有确定的加锁方向：

* 下行（查找/插入）天然"父 → 子"，可以 latch crabbing；
* 上行（删除合并）必须"子 → 父"，方向相反 —— 两条路径互撞就是死锁。

真实数据库的解法是 latch crabbing（乐观下行 + 冲突重试，或悲观父锁预判），
不是"给每个方法套一把 with page.latch"。在没有第 2 步行锁、`_write_lock`
仍然串行所有写语句的前提下，索引页 latch **没有收益**：它唯一的作用是让
两个写线程并发分裂同一棵树，而那两个写线程现在根本进不来。

结论：**索引页 latch 与行锁一起做**。等第 2 步放开 `_write_lock` 时按 latch
crabbing 一次设计完（含删除合并的反向路径），而不是现在先加一半。

对应的实测边界（仍然没有的东西）：`bp.new_page()` 拿到 page_id 与"把新页
链上链尾"之间、目录 `last_page` / `root_page` 的持久化与 catalog 重载之间
仍无锁，并发写探针里表现为"页链 7 页 vs last_page=8"。这两处属于目录级
（而非页级）竞争，修法是把目录页也纳入同一套 latch，同样安排在第 2 步。

### B+ 树叶页 latch 已落地（2026-11-19，Step 4 前半段）

第 2 步（行/键锁）落地后，按上文"索引页 latch 与行锁一起做"的顺序补上 B+ 树的
**单页** latch：

- `_commit_page`：读旧值 → 写变化区间日志 → 换页三步在同一 `page.latch` 内
  （与 `HeapFile._apply_and_log` 同款语义）；
- `_leaf_insert_at`：把"二分定位 + 幂等判断 + 紧凑 + 插入"整段搬进 latch。
  此前定位下标在 latch 外算，两个线程各自基于同一份旧页算出下标，后写者把
  先写者槽位顶乱（探针实测：`槽位目录未保持键序 (1050) 之后出现 (1044)`）；
- `_leaf_delete_at` / `_leaf_compact` / 叶子链修复 `_set_left_next` /
  `_set_right_prev` / `_clear_links` / 分裂时的后继页 prev 回指，各自单页 latch。

锁序只有 `page.latch → bp._lock` 一个方向（与 heap 一致），无 ABBA。

**叶分裂 crabbing 已落地（2026-11-19 追加，Step 4 后半段①的前半）**：

- 新增 `_leaf_insert_or_split`：把"发现满 → 读页 → 分裂"整段放进同一个
  `page.latch`（`_leaf_insert_at` 仍只做单页插入、页满返回插入位置，语义不变）。
  此前 `_insert_recursive` 在 `_leaf_insert_at` 返回后**释放 latch 再分裂**，两个
  线程各自基于同一份旧页分裂、各建一个右页并重写本叶 → 丢行/叶子链断裂；
  现在第二个线程进入 latch 时叶已分裂成前半（不再满），走普通插入而非重复分裂。
- `_insert_recursive` 内部页分支整段持 `page.latch`（读键 → 递归插孩子 → 插
  分隔键 / `_split_internal`），两个线程不再各自基于同一份旧内部页覆盖分隔键。
- `insert` 根分裂改成**乐观 + 重试**：普通插入不碰 `_root_lock`，只有触发根
  分裂才拿锁，锁内重读 `root_page`，发现别的线程已分裂过根就重试；初始建根
  也做双重检查。`_collapse_root` 拆出 `_root_lock` 包装。
- `_split_leaf` 改后继页 prev 回指时加 `expected` 校验（`nxt.prev` 必须仍指向
  本叶），与 `_set_left_next`/`_set_right_prev` 对齐。

锁序保持单向：`父页 latch → 子页 latch → 叶 latch → 后继 latch`，`_root_lock`
只在页 latch 全部释放后获取（`_root_lock → 新根 latch`），无 ABBA。

**内部页 / 叶子 B-link（high key）已落地（Step 4 后半段①的后半）**：

- 页头加 high key 标志：内部页用偏移 4、叶子页用偏移 6（叶子偏移 4 已被
  `LEAF_DATA_END_OFF` 占用），high key（右兄弟最小键）编码在记录区开头。
- `_split_internal` / `_split_leaf`：左页 high key = sep、next 指向右页；右页
  继承旧 high key、next 指向旧后继；旧后继 prev 回指（expected 校验）。
- `_descend` / `_insert_recursive`：读页后先判 `key >= high key` 就沿 next 右移
  （B-link 路由），再 `_child_index` 二分。此前只有查询路径 `_descend` 有路由，
  插入路径仍按旧 keys 二分，key 会被错插进分裂后的左半。
- 修两个真实 bug：①初始建根双重检查里，后到的线程发现根已被并发建好时仍
  `return`，静默丢弃自己的 key（最左叶最小键丢 3 条）；②`_split_leaf` /
  `_split_internal` 用 `new_page(Page).page_id` 后再 `fetch_page`，每次分裂
  泄漏 1 个 pin，长事务最终把缓冲池 pin 满（`pool_size=64 pinned=64`）。
- `check_invariants` 叶子/内部页的最右孩子上界改用 high key 校验。

**删除路径递归塌陷已落地（Step 4 后半段①的另一半）**：

- `_delete_recursive` 改为返回 `(deleted, page_now_empty)`：叶子键删空、或内部页
  「0 个孩子」才算空，需要上层把它摘掉并继续向上塌陷。
- **「只剩 1 个孩子」的退化内部页保留、不摘**：把它的唯一孩子（一片叶子）提到
  内部页层，会让这条路径的叶子深度少一层，`check_invariants` 的「叶子深度一致」
  当场失败（实测删掉前 1000 条后深度变成 {3,4}）。
- `_remove_child` 摘除空孩子；孩子是内部页时用 `_purge_empty_tree` 把它整棵空子树的
  叶子逐片从叶子链上摘掉——只摘内部页会留下孤儿叶（最左叶 prev 仍指向已删页）。
- 修三个真实 bug（顺序删除 2000 条长键值时暴露）：①退化内部页被当空页直接摘除，
  唯一孩子成孤儿、整片叶丢失（delete 失败 700 次，失败键每 30 个里 5 个）；②只摘
  内部页不清理空子树，叶子链断裂；③把叶子提到内部页层，叶子深度 {3,4}。
  正解：自底向上递归塌陷——空叶先摘并修链，只有 0 个孩子的内部页才跟着塌。
- 根塌陷 `_collapse_root` 加「0 个孩子」防御（正常删除路径不会走到，纯防御）。

**删除 crabbing 已落地（2026-11-20，Step 4 后半段①的收口）**：

- `_delete_recursive` 的内部页分支改成真 crabbing：父锁内**只做"选孩子"**，
  立刻释放父锁；下探子页时只持子页自己的 latch；只有孩子确实变空、需要摘除
  时才重新取父锁，且**按页号重新定位**（`child_id not in children` 说明别的线程
  已摘走，直接让位）。并发度不再被父/根 latch 串行。
- 由此引出两个必须同时修的连锁 bug（都是实测抓到、不是推演）：
  1. `_scan_head` 的兜底原本靠"写者在内部页整段持父锁，所以持根锁时结构静止"。
     crabbing 后面提不成立了。改为：重试**直到拿到非空链首或 `_tree_is_empty()`
     确认空树**；只有预算耗尽且树仍非空才抛 `BTreeInvariantError`。绝不能把
     "重试耗尽"静默当空——实测会让读者扫出 `min=0` 条（3000 条树、两读者）。
  2. 叶子链指针手术加树级 `_link_lock`：分裂要改写左叶 next / 右叶 prev / 旧后继
     prev，摘叶要改写前驱 next / 后继 prev 并清零自身。两组操作与相邻页交错会
     留下"跳过一片还在树上的叶"的断链（实测 4 线程删 12000 条后：页 197 的
     next 是 224，但中序下一叶是 203）。锁序固定 **链锁 → 页 latch**，所以
     `_leaf_insert_or_split` 要分裂时先**释放叶 latch** 再取链锁（否则与"持链锁
     摘叶、正等这片叶 latch"的删除线程成 ABBA）。
- 实测：4 线程乱序删除 + 1 并发读者，120 字节 TEXT 键造 `depth 3 / 797 页`
  的树、12000 条全删，**6/6 轮** 无异常、读者每次都键序正确无重复、终态
  `{depth:0, entries:0, pages:1}` 且 `check_invariants` 通过。
- 回归用例 `test_concurrent_adjacent_deletes_keep_leaf_chain`（4000 条长键、
  4 线程、断言叶子链与 depth>=2）连跑 3/3；全量 **1386 passed**（删除路径落地时
  的基线，后来 1395）。
- 当时仍未做：唯一索引项 DELETE 物理删除导致的并发可见性问题（已在下一节
  之后的「唯一索引的 DELETE/UPDATE 可见性」收口）；`Database._write_lock` 未放开。

**插入 crabbing 已落地（2026-11-20，Step 4 后半段①的收口）**：

`insert` 不再用"返回值栈"（父页 latch 内递归孩子、孩子把 `(sep,right)` 返给还持
着父页的调用者），改为**重试式上推**：

- 下行：`_insert_owned` 持父页 latch 选孩子 → `fetch + latch` 子页 → 释放父页，
  递归进子页；叶子层在叶 latch 内判断 high key，越界就释放、沿 `next` 右移。
- 叶分裂：`_leaf_insert_or_split` 返回 `(left_id, sep, right_id)`。分裂需要叶链
  指针手术时**先释放叶 latch 再取 `_link_lock`**（锁序链锁 → 页 latch），重新
  取锁后**再判一次 high key**：若本键已越过本页 high key（说明这页在让锁窗口里
  被别人分裂成了左半），返回哨兵 `_REDIRECT`，调用方沿链右移重试。少了这一判，
  本应进右页的键会写进左半（实测现场：`L488 n=7 [11/4.11..13/2.13] high=13/0.13`）。
- 上推：`_install_sep` 不信任下行时的旧下标，`_locate_parent(left_id, sep)` 从
  **当前** `root_page` 重新下探找 `children` 含 `left_id` 的内部页；latch 后若
  `left_id not in children`（父页已被并发分裂）就重新定位；插入位置按
  `bisect_left(keys, sep)` 定，**不用 `children.index(left_id)`**——左半页在
  "装分隔键"之前可能又被分裂过一次，旧下标会指向旧边界。装不下就
  `_split_internal` 后把 `left_id/sep/right_id` 换成父页自己的 `(P, sep2, right2)`
  继续上推；`root_page == left_id` 时在 `_root_lock` 内建新根。
- 修掉的三个实测 bug：①surplus 不带分裂页身份，父页按旧 `child_id` 装错位置
  （报"页 553 的键不小于上界"）；②用旧下标定位（报"页 494 的键不小于上界"）；
  ③重新取锁后不判 high key（上述 `_REDIRECT` 场景）。

**顺带修掉的缓存失效 bug（内部页解码缓存）**：

- 现象：`test_internal_cache_*` / `test_plan_cache` / 三例 lookup 报
  "索引 idx_id 找不到页 32 的父页"，而父页 31 实际已收下 32。
- 根因：缓存键是 `(页号, pageLSN)`，页被换出再换入后 `page_lsn` 归零，缓存里
  `(31, 0)` 的旧条目**永久命中**，按旧 `children` 路由（插桩证据：`kids=[29,30]`
  vs `fresh_kids=[29,30,32]`）。改"LSN=0 不缓存"也不行——该场景页 LSN 长期为 0，
  缓存彻底不填（`assert db.executor._internal_page_cache` 得 `{}`，也丢掉 +29% 收益）。
- 正解：`BufferPoolManager` 维护**单调页版本号** `page_version(page_id)`，在
  `log_page_update`、`fetch_page` miss（换入）、`new_page` 三处 bump；缓存键改为
  `(页号, pageLSN, 页版本号)`。页号复用不会撞旧条目，因为新页一分配版本就变。
- DDL 收口：`Executor.run` 的 `finally` 里对 schema 变更语句整体清空缓存。只靠
  `_tree_for` 的版本比对会漏——索引构建过程已把缓存喂满、版本号也拉到新值，
  事后不再清（`test_internal_cache_cleared_on_ddl` 钉住）。

- 实测：`test_btree.py + test_btree_page_latch.py` 22 passed、
  `test_btree_split_crab.py` 11 passed；压测探针（2000 长键 + 4 线程 × 200 插入）
  5/5 轮 `errors []`、不变量 `depth 4 / entries 2800`；全量 **1386 passed**（4m40s）。

**并发门禁（`scripts/concurrency_gate.py`，2026-11-20 落地）**：

把本轮手跑的探针固化成仓库内可重复执行的判据，开 `_write_lock` 之前每次都跑：

```
python scripts/concurrency_gate.py --rounds 3     # 默认 3 轮，约 6~10 分钟
python scripts/concurrency_gate.py --quick        # 1 轮，冒烟用
```

五项门禁与它们各自对应的真实故障：

| 门禁 | 做法 | 钉住的故障 |
|---|---|---|
| `btree_page_latch` | 同一片叶双线程并发插 200 条 | 定位/读键在 latch 外时基于旧页算 idx，丢行/乱序 |
| `btree_split_chain` | 分裂 + 删除 + 并发读者的全部 btree 用例 | 重复分裂、B-link 路由错、叶子链断 |
| `heap_insert_race` | 2 线程 × 300 行绕过写锁直插 `HeapFile` | 600 请求只拿到 513 个唯一 RID（重复分配） |
| `btree_insert_crab` | 2000 长键预置 `depth>=2` 树 + 4 线程 × 200 插入 | 上推时用旧下标装错分隔键、丢条目、键序越界 |
| `btree_delete_race` | 4 线程乱序删完 `depth>=2` 的 12000 条 + 并发读者 | 摘链交错跳过活叶、读者扫到 0 条、树不能完全塌陷 |

判据是"可失败的"：RID 数/行数不等、读者扫到空、条目数不等、终态 `entries != 0`
或 `depth != 0` 都直接判 FAIL 并返回非 0 退出码。实测 `--rounds 3`：**5/5 项 PASS**。

**唯一索引的 DELETE/UPDATE 可见性已落地（2026-11-20）**：

- 问题：删除/更新原先当场物理摘索引项。MVCC 下旧版本在提交前对别的快照仍
  可见，索引项一摘，并发读者走索引扫描就看不到这一行——键锁只防重复键。
- 修法：`_lock_index_keys` 只锁键、不摘项；索引项与堆里的死版本一起等
  VACUUM（`_rebuild_indexes`）重建时回收。代价是索引里会累积陈旧项，但换来
  并发读可见性正确。
- 唯一性改按 **heap 可见性**判：`_find_unique_conflict` 遍历键的所有命中 RID，
  回堆里取当前可见版本；行已删（含本事务刚删）→ 不算占用；行还在但键已改走
  → 陈旧项也不算。`_tree_for` 不再把 `unique=True` 交给 B+ 树（树级检查分不清
  陈旧键），唯一性只走这一个入口；`_rebuild_indexes` 从可见行重建时用 Python
  集合补检重复（守住 ALTER TYPE 把 `'1'`/`'01'` 都转成 `1` 的场景）。
- 索引扫描复核键：`_index_lookup` 命中后要求堆里当前版本的键等于查询键；
  `_index_range` 要求当前键仍落在范围内、且等于返回的树键前缀。否则陈旧项会
  把"已经改走键的行"当命中返回。
- UPDATE 改键时不再摘旧项；同一 rid 的同键陈旧项已存在时跳过重复插入
  （否则键改回原值会让索引扫描把同一行返回两次）。
- 回归用例 `tests/unit/test_mvcc_index_visibility.py`（9 个）：未提交删除/更新
  下的读者可见性、同事务删后重插同键、活行重复键仍报 23505、未提交删除时插
  同键 40001、陈旧键点查/范围扫描、键改回原值不重复、VACUUM 回收后条目数回落。

### 并发 DML 实验模式（`concurrent_dml=True`，默认关闭）

默认模式仍是 `_write_lock` 串行所有写语句；`Database(concurrent_dml=True)`（或
`LUMEN_CONCURRENT_DML=1`，或 CLI 的 `--concurrent-dml`）切到真正的写并发路径，
Step 4 的前置项在这条路径上落地：

- **DML 不拿 `_write_lock`**：`Database._run_locked` / `PGWireServer._run_statement`
  的普通写语句改走 `read_guard()`，只与 DDL 的 `exclusive_guard`、`close()` 互斥；
  行/键锁与页 latch 负责写冲突。
- **目录保存短锁**：DML 的 `_persist_catalog` 阻塞获取目录锁、保存完立即释放；
  DDL 仍 `txn.catalog_hold=True` 持锁到事务结束，避免未提交 schema 被别的快照落盘。
- **索引根页跨实例共享**：每语句新建的 `BPlusTree` 通过 `root_cell`（共享 `IndexDef`）
  直读/直写根页号，`Catalog.index_root_lock()` 提供全库同索引根锁；否则 A 分裂根后
  B 用旧实例回写会覆盖新根，整棵子树失联（实测 800 行只落 122/48）。
- **逻辑回滚**：并发 DML 的 HEAP / BTREE 页不记物理 undo，死行/陈旧索引项靠
  xmin/xmax 可见性与 VACUUM 回收；WAL `PAGE_UPDATE` 用 `old_len=0xFFFF` 哨兵省略
  before image，恢复时跳过。目录页在 DML 保存时也走逻辑（线程局部
  `BufferPool.logical_undo_scope()`），避免 A 回滚把 B 随后保存的新根抹掉；
  DDL 用 `_undo_override=False` 强制物理原像，保证 TRUNCATE/DROP 能真正撤回。
- **读者计数锁独立**：`read_guard()` 不再借 `_write_lock` 做计数（此前每条
  并发 DML 都要在这把全局锁上进/出一次，等于把语句重新串行）。现在读者计数用
  独立的 `_readers_lock`，`exclusive_guard` / `close()` 仍持 `_write_lock` 挡默认
  写者，再通过读者条件变量等在途读者退出。
- **保存点**：逻辑回滚无法撤回"保存点之后的 DML"，所以并发模式下第一次
  `SAVEPOINT` 会先置 `_no_readers`、排空在途语句，再拿 `_write_lock` 并把事务
  切到物理回滚（`Transaction.physical_savepoint`）。已经有活跃事务不再报
  `0A000`，但它们在保存点事务结束前不能进入下一条语句；保存点事务自己的后续
  语句走无 guard 的独占路径。正确性优先，代价仍是保存点事务存续期间写串行。
  写锁改成可跨线程释放的 `_CrossThreadRLock`：线协议语句跑在线程池，保存点可能
  在一个 worker 建、COMMIT 在另一个 worker 释放，`threading.RLock` 会报
  "cannot release a lock owned by another thread"。

回归：
- `tests/unit/test_concurrent_dml.py`（11 例：并发插入全提交、混合 DML + 半数回滚、
  逻辑回滚不可见 + VACUUM 回收、并发删除与读者、保存点等待独占并回滚、
  保存点跨线程释放、保存点事务内关闭、VACUUM 等待活跃事务、VACUUM 超时转在线、
  活跃 DDL 时拒绝在线回收、热点行无丢失更新）；
- `tests/crash/test_concurrent_dml_recovery.py`（崩溃后 loser 的 `no_undo` 记录不参与
  undo，已提交行完整、VACUUM 回收逻辑垃圾）；
- `tests/crash/test_online_vacuum_recovery.py`（在线 VACUUM 的整页回收落盘后崩溃：
  恢复可重放或撤销，但可见行/索引扫描/后续离线 VACUUM 必须一致）；
- `scripts/concurrency_gate.py` 新增 `sql_concurrent_dml`（4 会话 × 150 行，半提交
  半回滚，校验行数/去重/索引不变量）、`sql_savepoint_concurrent`（A 未提交时
  B 建保存点、插入、回滚到保存点、提交；终态只保留 A 的行，校验不再 `0A000`）
  与 `sql_vacuum_online`（模拟 5s 超时，活跃快照存在时 VACUUM 仍完成，快照看旧版本）。

**仍未做 / 切默认模式前必须解决**：
- 默认模式仍是串行；`LUMEN_CONCURRENT_DML=1` 全量已进 CI（Linux 3.12），默认/
  并发各 **1416 passed**，门禁脚本 3 轮 PASS（9 项，前两项 pytest 探针固定
  1 轮，已含 VACUUM 等待与在线回收探针）。
  HeapPage / 目录页的锁纪律还缺
  一份逐路径审计清单——现有并发回归覆盖插入/删除/扫描/回滚，但不等于审计完成。
- 保存点已经能并发使用：不再因为"有别的活跃事务"直接 `0A000`，而是排空在途
  语句后独占写者锁。它仍然是"功能正确但并发度低"的兜底路径——保存点事务存续
  期间其它写者排队。要恢复高并发保存点，仍需要行级逻辑 undo log。
- `VACUUM` 不再因为活跃事务失败：入口先等最多 5s；等不到空窗口就转“保 RID 的
  在线回收”（`HeapFile.vacuum_online`）。在线模式按 VACUUM 事务与所有活跃事务的
  快照逐版本判可见性，保留根记录、链头到最老可见版本的整条前缀链；只删对所有
  快照都不可见的版本，槽位不重排、索引不重建。持有 DDL 目录锁/物理 undo 的活跃
  事务仍会让 VACUUM 按原门禁报错——DDL abort 的页原像不能和在线整页日志交错。
  剩余边界是根记录/空洞槽位不回收，文件不缩小；离线模式（无活跃事务）仍重排
  RID 并重建索引。

**实测（2026-11-21，Windows / Python 3.13，单机）**

`benchmarks/concurrency_throughput.py` 用同一条 SQL 路径跑两种模式，每个
INSERT 是自动提交的短事务：

- 4 线程 × 300 行插入：默认 `_write_lock` 约 436 ops/s，`concurrent_dml`
  约 269 ops/s；
- 1 线程 × 300 行插入：两种模式都在 490 ops/s 左右，说明并发路径没有额外的
  单线程开销，多线程反而被共享锁/页 latch 拖慢；
- 4 线程热点行更新：默认约 43 ops/s；并发模式约 10 ops/s，并出现 504 次
  `40001` 重试（同一行只能串行，重试成本高于默认模式的直接排队）。

结论：`concurrent_dml` 目前只在"多事务、写不同行"的正确性路径上成立，
**短事务吞吐还不优于默认模式**，因此继续保持 opt-in，不切默认。下一步要
先定位 B+ 根页锁 / 页 latch 的争用，再谈吞吐；这份基准先作为负结果留档。

**2026-11-22 缓冲池锁拆分（Linux / Python 3.12，单机）**

缓冲池探针显示 `BufferPoolManager._lock` 是并发插入的主要争用点之一：
4 线程 × 300 行时累计等待 2.11s，占 wall 115.9%（跨线程累计可超过 wall）。
本轮把 `log_page_update` 的临界区拆开：

- `_lock` 内只做 fresh / dirty / undo / version 簿记，WAL append 与
  `page.page_lsn` 推进放到 `page.latch` 内、`_lock` 外，避免不同页的写者在
  WAL 磁盘等待上排成一队；
- `undo_xid` / `undo_to` 在 `_lock` 内只弹出/切片撤销记录，页原像的应用移到
  锁外，固定「页 latch → 缓冲池锁」的锁序，避免与 `log_page_update` 形成 ABBA；
- `HeapFile.update` 的"选页 → `_insert_raw`"窗口补了最多 4 轮重选页：并发写者
  可能在两步之间把页占满，不能把竞争导致的空间变化当成页损坏（新增
  `tests/unit/test_heap.py::test_update_retries_when_chosen_page_fills_before_insert`）。

探针复测：4 线程 × 300 行 `_lock` 累计等待从 2.11s 降到 0.118s，占 wall 从
115.9% 降到 6.8%，wall 1.729s。但短事务吞吐没有实质变化：4 线程 × 300 行三轮
默认 709 / 798 / 775 ops/s，并发 697 / 760 / 780；8 线程 × 200 行两轮默认
789 / 787，并发 799 / 734。热点行更新仍显著退化（4 线程并发 37 ops/s vs 默认
140，443 次 `40001` 重试；8 线程 15/12 vs 106/103，约 2000 次重试）。说明
剩余瓶颈主要在每事务提交的 WAL fsync 与同行冲突重试，而不是缓冲池全局锁。

回归：默认模式 1403 passed，`LUMEN_CONCURRENT_DML=1` 1403 passed；门禁 6 项
× 3 轮全 PASS。结论不变：`concurrent_dml` 保持 opt-in，暂不切默认。

**2026-11-22 WAL 分段刷盘锁拆分（Linux / Python 3.12，单机）**

组提交本身已经在 `TransactionManager._run_commit_group` 里，但
`SegmentedWAL.flush` 仍把 `_pending` 交换、分段轮转、文件写入和 fsync 全包在
`_lock` 里：leader 刷盘时所有提交线程都无法追加 COMMIT，批次被压到约 1 个
事务，测到 1200 次插入对应 930 次 flush。

本轮把文件 I/O 拆到独立的 `_flush_lock`：

- `flush` 只在 `_lock` 内交换 `_pending`，随后在 `_flush_lock` 下做轮转、
  `write`、`fsync`，完成后只回写 `_size`；
- 写失败时把这一批放回 `_pending` 队首，行为与旧实现“pending 不清空”一致；
- `iter_records` / `truncate_before` / `close` 按 `_flush_lock → _lock` 的
  固定锁序执行；`flush_upto` 先查 `_size`，不在持 `_lock` 时调用 `flush`。

实测（4 线程 × 300 行自动提交插入，基线来自 HEAD 的独立 worktree）：
基线默认 786 / 817 ops/s、并发 785 / 788；改动后默认 782 / 799 / 803、
并发 867 / 870 / 872。8 线程 × 200 行：默认 792 / 794，并发 884 / 852。
并发插入首次稳定超过默认约 8%–10%；热点行更新仍由同行冲突主导（4 线程
36 vs 默认 140，460 次 `40001`），没有改善，继续保留 opt-in。

回归：默认 1404 passed，`LUMEN_CONCURRENT_DML=1` 1404 passed；门禁 6 项
× 3 轮全 PASS；新增 `test_append_during_flush_is_not_blocked_or_lost` 用可控
慢 fsync 验证刷盘期间 append 不阻塞且下一轮不丢记录。

### 插入 crabbing 的协议（已按此实现，保留作契约）

现在的插入是"返回值栈"：`_insert_recursive` 在父页 latch 内递归孩子，孩子分裂
就把 `(sep, right_id)` 返回给这一层，由**还持着父页**的调用者插分隔键。要让
父锁在下探时释放，这个栈就断了——父页可能已被别的线程分裂，不能再拿旧下标插
键。改用"**重新从根定位父页**"的重试式上推（B-link 风格，不引入父指针）：

1. **下行（latch crabbing）**：持父页 latch → 读 `keys/children` 并选孩子 →
   `fetch + latch 子页`（**先拿到子页 latch 才释放父页**，保证孩子此刻还在树上）
   → 释放父页。叶子层照旧在叶 latch 内"发现满即分裂"。
2. **叶分裂后**：得到 `(sep, right_id, left_id=叶号)`。进入上推循环。
3. **上推循环**：用 `_locate_parent(left_id)` 从根下探，找 `children` 里含
   `left_id` 的内部页 P（分裂才走这条路径，O(height×fanout) 可接受）。
   - latch P 后重读 children：`left_id` 不在 → 说明 P 已被并发分裂，重新定位；
   - 在 → 在 `children.index(left_id)+1` 处插 `(sep, right_id)`；装得下就结束；
   - 装不下 → `_split_internal(P)` 得到 `(sep2, right2)`，令
     `left_id = P.page_id` 继续上推；
   - P 是根且装不下 → 在 `_root_lock` 内建新根（沿用现有根分裂路径）。
4. **必须守住的不变量**：①任何"写某页内容"的动作都在该页 latch 内完成；
   ②分隔键的插入位置一律按 `left_id` **重定位**，绝不用下探时的旧下标；
   ③`_link_lock` 只在叶链指针手术时取，且必须先释放叶 latch（锁序
   链锁 → 页 latch）；④上推循环内的每一次重定位都从**当前** `root_page` 起，
   因为根也可能已换。
5. **要先写的测试**（照现成的并发用例长）：
   - 4 线程并发插相交键触发叶分裂 + 内部页分裂 + 根分裂，断言键序/叶子链/条目数；
   - 单线程"父页被并发分裂"的强行交错（用探针在 `_locate_parent` 前后插桩）；
   - 与删除 crabbing 混合的读写并发（插入 + 删除 + 两读者），断言不截断、不重复；
   - `concurrency_gate.py` 增加 `btree_insert_crab` 一项，判据与删除项对齐。

- 读者侧：`lookup`/`scan` 目前不拿 latch，靠 GIL 的"一次换页 memcpy 原子" +
  `_write_lock` 串行写保证读到一致快照。**已实测出缺口**（2026-11-19）：
  3000 条树、写线程删 0..1499、2 个读者循环 `scan_all` 共 640 次扫描——
  无异常、无键序错乱，但扫描条数区间 **0..3000（最小 0 条，最终实体 1500）**，
  读者会瞬时读到整棵空树 / 截断结果，来源是空叶被摘时叶子链的断点与根塌陷窗口，
  与插入分裂时的表现完全不同（那条路径读者是安全的）。结论：放开 `_write_lock`
  前，读者必须加共享 latch 或**乐观读重检**（每页读前记 `(nkeys, next_page)`，
  读后用页版本号/`pageLSN` 校验未变，变了就重试，且起始页每次从根重新下探，
  不能沿用缓存的最左叶）。

**读者侧本轮已落地的两件事（2026-11-19）**：

1. **整页快照在页 latch 内一次读完**（`_snapshot_leaf` / `_snapshot_internal` /
   `_leaf_page_view`）：`_read_leaf` 是"先读 nkeys 再按槽位逐个解键"，中途撞上
   写者换页就会拿"旧 nkeys + 新槽位表"去取偏移——实测抛
   `叶子页 2 的槽位 150 偏移越界：0`。现在 `scan_all` / `scan_range` /
   `_scan_range` / `lookup` 全部改走快照。修复后同一探针 **661 次扫描 0 异常、
   0 次少于 1500 条（最小 1500）**；修复前 382 次里有 1 次撕裂异常 + 1 次扫到 0。
2. **链首校验**（`_leftmost_leaf_verified`）：下探可能落进正在被 purge 的子树，
   拿到一片已摘除的空叶（`_clear_links` 把它的 `prev` 和 `next` 都清零，看起来
   就是"链首"）。现在要求链首同时满足 `prev == INVALID` 且（`nkeys > 0` 或
   `next != INVALID`），唯一例外是"根自己就是这片空叶"（整棵树为空）；不满足
   就沿 `prev` 回溯 / 重新下探。

**仍未解决（实测边界，别当已完成）**：跨页跳转与"摘空叶"之间还缺页锁耦合。
全量负载下链首校验会偶发耗尽 32 次重试（抛"无法稳定读到链首"），并且约
1/600 次扫描仍会截断到 0 条。根除方案是**双页嵌套**：持当前页 latch 去
fetch 下一页并在同一临界区里校验 `next.prev == 当前页号`，这样"前驱重写先于
被摘页清零"的顺序保证就不会被读到撕裂的 next。因为没有稳定通过，**没有把
这条严格保证写成测试**（避免提交 flaky 用例）；留到 Step 4 的 crabbing 一起做。

**上述残余已在 2026-11-20 收口（扫描路径）**，改动与实测：

1. `_scan_head`：把"校验链首"和"读首页内容"合并重试——链首必须有键
   （整棵空树除外）。空页不可能是扫描起点，它要么正被摘除（next 可能还没清
   零，读下去就 0 条），要么是刚建出来还没落键的过渡态。重试耗尽时**不再**指望
   "持根锁即结构静止"（插入/删除两条写路径都已 crabbing，父锁在下探时就放了），
   改为重试到拿到非空链首、或 `_tree_is_empty()` 确认整棵树为空；只有预算耗尽
   且树仍非空才抛 `BTreeInvariantError`。
2. `_advance_leaf(page, next_id, prev_expected)`：跳页前用**期望前驱**校验
   `next.prev == 当前页号`；不一致说明 next 已陈旧，重读当前页 next 改道；
   若当前页自己已被摘空（prev/next 双清零），退到 `prev_expected` 读它**当前**
   的后继把扫描接回链上。判"真链尾"也用它，而不是只看 `prev == INVALID`
   （清零发生在读完 next 之后）。
3. **按已产出键去重**：B-link 分裂会把一页的上半搬到右页；读者先读到旧页
   快照（含上半），再从右页读到同一批键，就出现"787 → 633"倒序/重复。
   扫描器记住 `last_key`，进入新页时 `bisect_right(keys, last_key)` 跳过已产出
   的键——有界、O(log n)，不牺牲顺序。
4. 代价与实测：删并发探针 683 次扫描 **0 截断、0 异常、最小行数 1500**；插入
   并发 + 两读者测试连跑 **3/3 通过**；全量 **1386 passed**（crabbing 删除落地
   后基线，新增 1 个相邻叶并发删除回归用例）。
5. 点查 `lookup` 也补了跨页去重：页被分裂时同一 RID 不再被重复计入
   （否则唯一索引会把并发分裂误判成"重复键"）。

**剩余（Step 4 后半段）**：删除与插入两条写路径的真 crabbing 都已落地（选完
孩子即释放父锁 + 叶链 `_link_lock` + 重试式上推）；唯一索引项的可见性也已改为
延迟回收（见本文件前面的「唯一索引的 DELETE/UPDATE 可见性已落地」）；放开
`Database._write_lock` 之前还要给 HeapPage / 目录页做同样的锁纪律（见上文 L2
的四条原因），以及把物理 undo 按事务分区。

验证：
- `tests/unit/test_btree_page_latch.py::test_concurrent_leaf_inserts_keep_order_and_count`
  双线程绕过 `_write_lock` 对同一片叶并发插 200 条，断言不变量 + 条目数（修复前
  稳定丢行/乱序）。
- `tests/unit/test_btree_split_crab.py::test_concurrent_leaf_splits_keep_invariants`
  预置多层叶后双线程向同一片叶并发插 300 条触发叶分裂，断言键序/叶子链/条目数。
- `tests/unit/test_btree_split_crab.py::test_concurrent_inserts_from_empty_trigger_root_split`
  4 线程从空树并发插相交键，钉住建根竞态（丢最小键）+ 根分裂 B-link 路由。
- `tests/unit/test_btree_split_crab.py::test_concurrent_inserts_trigger_internal_splits`
  预置 depth≥2 的多层树后 4 线程并发插相交键触发内部页分裂，断言不变量 + 条目数。
- `tests/unit/test_btree_split_crab.py::test_sequential_delete_large_tree_no_orphan`
  2000 条长键值顺序全删：每次 delete 必须为 True、最终 entries=0（钉住退化内部页
  摘除丢叶）。
- `tests/unit/test_btree_split_crab.py::test_delete_half_keeps_leaf_depth_uniform`
  删前 1000 条再插回 1000 条：数据完整 + 叶子深度唯一（钉住孤儿叶 / 深度 {3,4}）。
- `tests/unit/test_btree_split_crab.py::test_concurrent_reader_scans_during_insert_splits`
  写线程插入触发分裂 + 两个读者并发全表扫：读者结果无异常、键序单调、条目数完整。
  读者路径不拿 latch，一致依靠「分裂是一次整页提交」+「沿叶子链 next」（B-link）。
- `tests/unit/test_btree_split_crab.py::test_concurrent_deletes_same_leaf_no_lost_delete`
  双线程并发删同一片叶的不同键：`delete` 全部返回 True、叶内剩 100 条不丢。
  回归：定位/读键在 latch 外时两线程基于旧页各算 idx，后写者覆盖先写者的删除
  （实测 300 条叶丢 27 条）；修法是把定位/读/删整段放进同一个 `page.latch`。
- `tests/unit/test_btree_split_crab.py::test_concurrent_delete_across_leaves_no_lost_delete`
  双线程各删一半键跨多片叶删完整棵树：不丢删除、最终为空、不变量成立。
  覆盖内部页摘孩子、叶子链前后互接、根塌陷三条路径的并发组合。

**删除路径的锁纪律（2026-11-20 更新为真 crabbing）**：叶子分支「定位 → 读键 →
删」仍在同一个页 latch 内；内部页分支不再「读孩子下标 → 递归 → 摘空孩子」全程
持父锁，而是**选完孩子就释放父锁**，下探只持子页 latch，孩子变空后再重取父锁
并按页号重新定位（页号已不在 children 里就让位）。摘链指针手术（前驱 next、
后继 prev、自身清零）与分裂的邻居 prev 回指统一由 `_link_lock` 保护，锁序固定
「链锁 → 页 latch」，所以分裂路径会先释放叶 latch 再取链锁。这样既没有
「子 → 父」路径成环，也不再让删并集被根 latch 串行。

### 第 2 步：行锁 + 键锁 + 目录锁（已完成，2026-11-18）

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

### 第 3 步：重跑不变量（2026-11-18 已随第 2 步验收）

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

频率护栏（`tests/unit/test_catalog_persist_frequency.py`，2026-11-18 补）：
40 行小插入只触发 **1** 次目录保存；20 行大插入（每行 ~1KB，强制分页）触发
**5** 次——即"按页"而不是"按行"。同组用例还钉住了两个容易回退的点：重开数据库后
`last_page` 必须仍是可用的链尾（否则下一次插入会覆盖链尾的 `next`），以及
`row_count_hint` 不落盘也不影响查询结果。

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

注意：这是**结构性写与读者**的互斥，不是行级并发控制；写这段时第 2 步
（行/键锁）还没开工，写语句之间靠 `_write_lock` 完全串行（第 2 步已于
2026-11-18 落地，见下文，但 `_write_lock` 仍串行直到第 4 步放开）。
### 第 2 步的评估结论（2026-11-17）

第 1 步（"DML 不再无条件 persist 目录"）**已落地**（见上面第 1.5 步），
所以这里原来的阻塞项（"每条 DML 都写同一份目录字节"）已经解除：DML 只在
结构变化时才碰目录。

第 2 步（行/键锁）已于 2026-11-18 落地（见下文第 2 步）；本节写于 2026-11-17，
当时的真实阻塞是三条：

- 目录 persist 的**写入时机**仍是"结构变化时在事务内写"：两个同时分配新页的
  写事务仍可能写同一批目录字节（频率从每行一次降到每页一次，但没有消失）；
- 唯一索引项曾在 `DELETE` 时被**物理删除**，并发 `INSERT` 看不到它——已改为
  键级互斥 + 索引项延迟回收（VACUUM 重建时清理），唯一性按 heap 可见性判；
- 物理 undo 不按事务区分字节，正确性前提是"没有两个事务写同一字节区间"。

所以顺序不变：
1. ~~DML 不再无条件 persist 目录~~（已完成）；
2. 再做行/键锁（含"等锁后重新校验版本"，见上文）；
3. 最后收掉事务写者锁。

### 磁盘满（ENOSPC）时的事务收尾（2026-11-17）

写 WAL 的 COMMIT 记录可能因为磁盘满而失败，这条路径此前有三个洞，现在
都补了，并各有测试（`tests/unit/test_wal_failure.py` 3 例）：

1. **COMMIT 记录写失败后事务没有回滚**。`Database._run_unlocked` 的两处
   `txns.commit(txn)` 抛错时直接向上冒，事务留在 ACTIVE、页已改、写者锁
   未释放 —— 单写者模型下后续每个写事务都拿 40001，整台库变成永久只读。
   现在抛错时按 `txn.commit_record_written` 判断：没写过就 abort（页 undo
   + 交锁），写过了（例如同步复制屏障超时）就不 undo —— 那时事务事实上
   已提交，调用方拿到的是"结果未知"，回滚反而是错的。
2. **不能用 `last_lsn` 当"是否已提交"的判据**。事务在 `begin()` 里就把
   `last_lsn` 置成 `first_lsn`（非 0），WAL 写失败时它不会变回 0。所以
   新增了显式标记 `Transaction.commit_record_written`。
3. **abort 自己也会失败**。磁盘满时连 ABORT 记录都写不进去，`abort` 中途
   抛错 → 锁照样泄漏。`TxnManager.abort` 现在用 `try/finally` 收尾：
   无论 undo / WAL 能否写成功，事务状态一定落 ABORTED、簿记清干净、锁一定
   交回去（页可能没完全撤销，但锁和状态机不会卡死）。

**已知边界（不修，写清楚）**：abort 的 `undo_xid` 逐个页 flush，中途 ENOSPC
会让一部分页已撤销落盘、后面的页只撤销在内存里。此时**本进程内**继续读那
张表可能抛 `struct.error`（页内 slot 目录与记录不一致）。这是"进程内页已
半损坏"，不是持久化数据坏了 —— 失败页仍是脏页、从未落盘，**重启后从磁盘读
到的是事务开始前的版本**（测试里断言了这一点）。正确处置是重启，或直接从
副本 / 基准备份重建。这正是第 4 层副本存在的意义：单机磁盘坏掉时，读一致
性由副本兜底。

### 第 4 步：热点行更新改为“锁后重读”（2026-11-22）

只把 `_lock_row` 的 `try_acquire` 换成 `acquire` 并不够。第一次实测反而更差
（4 线程热点更新 22.8 vs 默认 32.3 ops/s，174 次 40001）：语句先在快照里读出旧
`v`、算好 `v + 1`，然后才排队等行锁；等前一个写者提交后，`heap.update` 的
first-updater-wins 仍会发现链头已变，于是继续报冲突。等待只是把“立即失败”换成
“等一会儿再失败”。

现在的做法分三层：

1. **只对自动提交的单语句 UPDATE 等待**。`Transaction.statement_scoped` 由
   `Database._run_unlocked` 与 pgwire 隐式事务路径置位；显式 `BEGIN` 的事务仍
   快速失败，避免把事务快照冻结在等锁前的旧版本上。
2. **拿锁后重读最新已提交版本**。`HeapFile.get(..., ri=True)` 走引用完整性可见性
   （“已提交即可见”，不看 `snapshot_xid`），拿到链头最新的已提交版本；WHERE 与
   SET 都用这份新值重算。`HeapFile.update(..., ri=True)` 也按同一版本落地，避免
   再撞一次版本检查。
3. **其它锁不等待**。索引键锁、外键父行锁、DDL 锁、目录锁仍是 `try_acquire`
   快失败，因此等待图只包含行锁，不会重新引入“持语句锁等行锁”的不可见环。

实测（Linux / Python 3.12，4 线程 × 300 行插入 + 40 次/线程热点自增）：

- 默认模式：插入 617.7 ops/s，热点更新 116.1 ops/s，0 次重试；
- `concurrent_dml`：插入 606.6 ops/s，热点更新 **122.0 ops/s**，**0 次重试**；
- 热点行最终值 160（4×40），没有丢失更新。

对比改动前并发热点更新 22.8–36 ops/s、170–530 次 `40001`，这是数量级上的改善，
也是并发模式第一次在热点写上不低于默认模式。新增回归
`tests/unit/test_concurrent_dml.py::test_hot_row_increments_serialize_without_lost_updates`
（4 线程各 25 次自增，最终值必须恰好 100）。默认与并发全量各 **1405 passed**，
并发门禁 6 项 × 3 轮全 PASS，GitHub Actions run **279**（Python 3.12 / 3.13 ×
Linux / Windows 四矩阵）全绿。

前一个 docs 提交 `4fbb6df` 的 CI #278 失败与本次改动无关：Windows 3.13 上
`test_quorum.py` 两条时间敏感用例偶发（看门狗心跳重置竞态、`append_entries`
连接超时），同树在 run 279 重跑即绿。已作为 flake 记进账本，后续单独硬化。

run 282（仅 docs 改动）又在 Windows 3.13 撞到另一条时间敏感用例：
`test_sync_replication.py::test_end_to_end_commit_waits_for_the_majority` 在
`CREATE TABLE` 后只等 `quorum_commit_lsn > 0` 就插入，但此时水位只追到 12k、
WAL 尾已经是 41k，0.15s 同步屏障必然超时。这是测试的等待条件不完整，不是屏障
实现的问题；已改为等水位 >= leader 自己的 `state.last_lsn`，并断言目标 >0。
目标用例连续跑 20 次、`test_sync_replication.py` 13 例均通过。

并发全量本地还碰到 `test_write_fence.py::TestLeasePathOnOpen::
test_lease_expiry_after_open_blocks_the_next_write` 的时间敏感失败：租约 TTL
只有 0.15s，而 Windows 慢盘上“打开 Database + CREATE TABLE”本身就可能超过
0.15s，构造时直接判租约过期。改为先用 10s TTL 打开建表，再把 `lease.ttl`
缩短并 `renew`，随后 sleep 0.25 断言下一次写被拒；默认/并发各连续 10 次通过。

run 286（`a9785a3`）Ubuntu 3.12 又撞到 `test_promote_with_quorum_starts_lease_keeper`
的调度抖动：keeper 间隔 0.05s，测试固定 sleep 0.15s 后就断言 `expires_at`
变大，慢 runner 上一次都没续租。改为轮询到租约真正续上（上限 2s），默认/并发
各连续 10 次通过；287/288 是修复前的四矩阵基线，本轮修复由 run 289/290 验证。顺带把
`test_lease_keeper_uses_renew_callback` / `test_lease_keeper_calls_on_failure`
的固定 `sleep(0.12)` 断言、`test_open_after_expiry...` 的固定 `sleep(0.2)` 和
`test_promote_with_quorum_steps_down_on_lease_loss` 的固定 `sleep(0.3)` 都改成
轮询到条件成立；`test_lease.py` + `test_write_fence.py` 默认/并发各 5 轮通过。

**边界写清楚**：这条路径让并发 UPDATE 的目标选择与 SET 求值带上了 READ COMMITTED
色彩（能看到语句开始后别的自动提交事务写下的行），不是严格的可重复快照；显式事务
仍是快照隔离 + 冲突即 40001。`concurrent_dml` 因此仍保持 opt-in，不切默认：下一步
要先把显式事务的等待/重读语义与保存点、VACUUM 水位一起定下来。

### 切默认的判据（2026-11-22，结论：仍不切）

- **正确性：过关。** 默认与 `LUMEN_CONCURRENT_DML=1` 全量各 1416 passed；门禁
  脚本 3 轮 PASS（9 项，含保存点并发、VACUUM 等待与在线回收探针）；CI run
  279/280/281 四矩阵全绿（含看门狗竞态修复后重跑）。
- **热点行写：过关。** Linux 4 线程 122.0 vs 默认 116.1 ops/s；Windows 8 线程
  28.0 vs 默认 29.2 ops/s，二者均 0 次 `40001`，最终值等于总自增次数。
- **短事务插入：不再作为阻塞项。** Linux 8 线程 × 200 行：并发 694.7–730.1 vs
  默认 407.3–452.3 ops/s，稳定领先 60%–70%；4 线程此前 606.6 vs 617.7，基本持平。
  Windows 曾见 262.0 vs 708.8，但交替复测 8 线程为 278.1 vs 272.4，160 行轮次
  257/246/277 vs 353/193/187，受 fsync 抖动主导，2.7× 退化未复现；单线程并发
  198.4 vs 默认 277.4，固定开销约 28%，是“吞吐换并发能力”的代价。平台差异记录，
  不作为切默认门禁。
- **保存点：部分过关。** 并发模式下第一次 `SAVEPOINT` 不再要求全库没有其它活跃
  事务（不再 `0A000`）：它排空在途语句后独占写者锁，事务自己的后续语句继续执行，
  其它写者排队到事务结束。功能可用、跨线程 COMMIT/ROLLBACK 已回归；但空闲保存点
  事务仍会长期占住写者锁，切默认前要么接受"保存点即串行"，要么补行级逻辑 undo。
- **VACUUM：在线 MVP 过关。** 活跃事务存在时不再报错：等 5s 后自动转保 RID
  在线回收，保留所有活跃快照需要的版本与根 RID，不重建索引。剩余是空间回收
  效率：根记录和空洞槽位不回收、文件不缩小；离线模式仍可做完整重排。
- **隔离级别：需要发布说明。** 自动提交 UPDATE 是 READ COMMITTED 色彩，显式事务
  才是快照隔离；这跟“默认模式”原来的单写者语义不同，不能静默切换。

所以下一步优先级是：保存点行级逻辑 undo → 自动提交 UPDATE 的隔离级别说明；
VACUUM 在线回收已不再作为硬阻塞项。

### 锁表的规模（已处理）

批量插入会为每行拿一个唯一键锁（长事务可能上千个）。`release_all` 早期是"扫全系统
锁表"，多会话下这一步是 O(总锁数)。现在 `LockManager` 维护 `_held[xid]` 集合，
`release_all` 只看自己持有的（`tests/unit/test_lock.py` 有数量断言）。
`held_count(xid)` 可用于观测单事务的锁表规模。

## 3. 已知的取舍（写在这里，避免下一轮再讨论一遍）

- **`VACUUM` 有离线/在线两模式**：没有活跃事务时重排 RID、重建索引；有活跃
  事务时走保 RID 在线回收，只删对所有人不可见的版本，根记录与空洞槽位不回收。
- **witness 节点不参与 durability**：不存数据的投票节点**不计入**"多数派真正持有"
  的提交水位。水位由 `match_lsn`（真正 apply 到）算出，`last_lsn`（听到过）只
  用于投票安全性比较；心跳与投票响应都带两个字段、`campaign()` 与 `heartbeat_round`
  都用 `match_lsn`，所以 witness（`match_lsn=0`）听再多心跳也抬不高水位。
- **单写者不是 bug，是声明过的边界**：`README.md`、`docs/architecture.md` §2.5
  都写明"读并发、写单写者"。这份文档给出的是**放开它的正确顺序**，不是
  "现在就是坏的"。
