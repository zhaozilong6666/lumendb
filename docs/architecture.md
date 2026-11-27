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

代价是日志体积大。新库默认使用**分段 WAL**：LSN 仍是逻辑字节偏移 + 1，
但日志拆成 `wal-<first_lsn>.log` 多个段文件，文件名记录该段第一条记录的
LSN。checkpoint 之后，`RecoveryManager` 只删除“最后一条记录 LSN 小于安全
LSN”的整段；安全 LSN 取活跃事务最老的 BEGIN LSN，没有活跃事务时取
`checkpoint_lsn`。已有单文件 `lumendb.wal` 的旧库继续走单文件路径，避免
在没有迁移逻辑时误读旧数据。

**记录校验**：新 WAL 记录的 `rtype` 最高位标记"尾部带 4 字节 CRC32"，
CRC 覆盖 17 字节头和 payload。旧记录没有该标志，解码后 `has_crc=False`，
重新编码/恢复 `next_lsn` 时保持原长度。完整记录 CRC 不匹配会抛
`WALCorruptionError` 并拒绝恢复；只有真正写到一半的残尾才会被当成崩溃残尾
静默丢弃，并在打开日志时截掉——否则下一次 append 会写在残尾之后，重启后
恢复在残尾处停下，静默丢掉崩溃后写入的全部记录。

**页镜像**：页分配本身不写日志，只有页内容修改才写 `UPDATE`。为了让物理
副本能从较旧的基准备份重建主库新分配的页，Buffer Pool 在“新分配页第一次
被修改”时额外写一条 `PAGE_IMAGE`（`u32 page_id + 4096B 镜像`），随后才写
普通的页内 `UPDATE`。恢复与副本都先按 `PAGE_IMAGE` 初始化页、再按 pageLSN
应用后续 diff；这条记录同时补上了“恢复时新页可能尚未落盘”的缺口。

**组提交**：`WAL` 的追加与刷盘都持同一把锁；`TransactionManager.commit` 先把
COMMIT 记录入队，由一个 leader 在锁外统一 `flush`，一次刷盘可以覆盖多个
目标 LSN。其余提交在条件变量上等待，只有自己的 COMMIT 记录持久化后才被标记
为 `COMMITTED`；因此“已提交”仍然等于“崩溃后可见”。单提交路径语义不变，
并发提交测试会断言刷盘次数少于提交次数。

`TransactionManager` 同时维护 `xid -> COMMIT LSN` 的提交账本：只有 COMMIT
记录持久化、事务真正标记 `COMMITTED` 后才登记，成功恢复时从 WAL 的 COMMIT
记录重建。账本另外原子持久化到数据目录的 `commit-ledger.bin`，默认保留最近
100000 条提交；`SHOW COMMIT LOG` 暴露跨 checkpoint 的 xid/commit_lsn（按 LSN
升序）。checkpoint 的顺序固定为“写账本 → 推进 `checkpoint_lsn` → 裁 WAL”，
账本写失败就不推进，WAL 仍可重建。

`AS OF SYSTEM TIME LSN n` 在这个账本之上提供只读历史读，可读窗口是
`[max(structural_floor_lsn, ledger_floor_lsn), latest_commit_lsn]`，由
`SHOW HISTORY WINDOW` 暴露。结构下界取最近 VACUUM / DDL 开始位置，并持久化
到元数据页；账本下界是最老保留提交。checkpoint 本身不再限制历史读，因此
目标点可以早于最近 checkpoint。基表顺序扫描与索引扫描都可用：索引项只提供
候选根 RID，执行器必须回堆按目标 LSN 解析版本并复核索引键，不能把当前索引
直接当历史真值。同一查询里的多表必须使用同一 LSN。

### 2.4 恢复

启动时：

1. **analysis**：从最近 checkpoint 扫到日志尾，重建活跃事务表与脏页表；
2. **redo**：从 checkpoint 的 `redo_lsn` 起重放所有日志，`pageLSN >= lsn` 的页跳过；
3. **undo**：对活跃事务（loser）按 LSN 逆序应用 before image，并写 CLR 防止二次回滚。

undo 是**条件式的**：只有当页面上仍是该记录的 after image 时，才写回 before image。
ARIES 靠写锁保证"两个事务不会写同一字节区间"，而本引擎的并发控制并不总能提供这个
前提；不加这个条件，loser 的 before image 会把赢家**已提交**的写入抹掉（实测：已提交
的 `UPDATE` 和已提交的 `CREATE TABLE` 都会在崩溃恢复后消失）。代价与收益写在
`tests/crash/test_recovery.py::TestConditionalUndo` 里：自己的写入会被撤销，被更高
LSN 覆盖的字节保留赢家的值。

恢复结束后写一个 checkpoint，把 `checkpoint_lsn` 记入元数据页。分段 WAL 会在
checkpoint 落盘后回收旧段；恢复只读取保留下来的段，仍然从
`checkpoint_lsn` 之后开始 redo。

**自动 checkpoint**：`Database(checkpoint_wal_bytes=...)` 在每次提交后比较
`WAL.next_lsn` 与元数据中的 `checkpoint_lsn`，超过阈值就调用
`RecoveryManager.checkpoint`；`Database(checkpoint_every_commits=...)` 则按
累计提交数触发，两个阈值可以单独或同时使用（0 表示关闭）。checkpoint 先刷
所有脏页，再写 CHECKPOINT 日志并更新元数据；因为 `checkpoint_lsn` 随之前移，
后续提交不会在同一阈值上重复触发。手动 `CHECKPOINT` 语句保留。

### 2.5 事务与并发

- 单调递增的 `xid`，`xmin/xmax` 写在每行头部；
- 快照 = (自己的 xid, 活跃事务列表)。可见性：`xmin` 已提交且不在快照活跃集，且 `xmax` 未提交或已回滚；
- 历史读第一切片：`HeapFile.scan_at(commit_lsns, target_lsn)` 不伪造事务，
  只按提交 LSN 截止点判断版本；目标点后才提交的 `xmin` 不可见，目标点后才
  提交的 `xmax` 不隐藏行。它目前只覆盖基表顺序扫描，未接 SQL、索引或 JOIN；
- 隔离级别固定为**快照隔离**，不提供 `SET TRANSACTION ISOLATION LEVEL`
  切换（会明确报 `NotSupportedError`）。既然只有一种语义，就不假装有多种。

**并发模型：读并发 + 行/键粒度的写互斥。** 读语句不取锁，各走自己的 MVCC 快照。
写语句在真正改数据之前申请**不等待**的锁，冲突直接 `40001` 让应用重试：

| 资源 | 何时申请 | 模式 |
|---|---|---|
| `row`（表名 + 根 RID） | `UPDATE` / `DELETE` 改这一行之前 | X |
| 同上 | 外键校验命中父行之后（等价 `FOR KEY SHARE`） | S |
| `ukey`（表名 + 索引名 + 键） | 唯一索引插/删索引项之前（含唯一性判断之前） | X |
| `ddl` | 任何 DDL / `VACUUM` / `TRUNCATE` / `REINDEX` | X（持到事务结束） |
| `catalog` | 目录整份保存时（短锁） | X |

为什么**不等待**：语句级还有两把锁（`Database._statement_lock` 与线协议的 asyncio 锁），
在语句内部阻塞等锁会造出"持语句锁等行锁、持行锁等语句锁"的环，而等待图看不见它。
所以冲突一律 `40001`，由应用重试；换来的好处是不同行/不同键/不同表的写入真正并行。

拿锁之后**还要重查版本**：等待（或失败重试）之间别人可能已经提交，旧快照继续写会覆盖
根记录的 `newest` 指针，让一个逻辑行出现两个可见版本。`HeapFile.update/delete` 里的
`_raise_if_concurrent_write` 负责这一步，语义等同 PostgreSQL 在 REPEATABLE READ 下的
"并发更新报序列化失败"。

唯一性判断必须在**拿键锁之后**做：唯一性读的是"物理索引项"，而 `DELETE` 会立刻把它
摘掉。不加锁时，并发的未提交删除会让另一个事务报出假的 `23505`；加锁后未提交冲突是
`40001`（重试），已提交冲突才是 `23505`。

**引用完整性用自己的可见性。** `TransactionManager.is_visible(..., ri=True)` 对应
PostgreSQL RI 触发器的 `SnapshotDirty`：外键校验与级联收集把"已提交"当作可见，
不看本事务的 `snapshot_xid`。否则父行被并发事务提交后，合法的子行插入会被误判成
`23503`；删除父行时也会漏掉并发提交的子行，留下悬空引用。

**当前边界（诚实版）**：`storage/lock.py` 已经实现了行级 `S`/`X` 锁与等待图 DFS
死锁检测，但只被 `tests/unit/test_lock.py` 使用，**没有接入写路径**；
所以本版本的写并发度是 1，而不是"行级并发"。接入它需要同时把语句级的
`Database._write_lock` 收窄到物理写，否则两者叠加会产生等待图看不见的等待环。

DDL 不止改磁盘页，还会原地修改内存里的 `Catalog` 对象。因此 `Catalog` 的
建表/删表/建索引/删索引/重命名/加列都会把一个逆操作登记到 `txn.undo_log`；
DML 也会在首次接触表时登记 `root_page`/`last_page`/`row_count_hint` 与索引
根页的快照。Buffer Pool 另外按 `xid` 记录每次页写的 before image，`abort`
时先逆序恢复页内容，再逆序恢复目录对象，最后由 WAL 负责崩溃后的页级 undo。
页恢复会写 CLR 并推进 `pageLSN`，避免下次 redo 把回滚结果覆盖回去。
`CatalogStore` 还会把 page 0 的 `catalog_root` 指回旧链，否则显式 `ROLLBACK`
之后关闭再打开，会重新加载到未提交的新目录链。

**目录版本链（C 的第一切片）**：每次 schema 形状变化（建表/删表/加列/索引/
重命名/视图）才把 `catalog_version` 加一，并在新目录 JSON 里写 `previous_root`
指向上一条 schema 链；普通 DML 只更新页根，仍写新链但沿用同一
`catalog_version`，不会伪造 schema 版本。**schema 根链**从不覆盖：旧
schema 根页必须保留，`Catalog.history()` 沿 `previous_root` 回溯历史表定义。
普通 DML 产生的中间目录根不属于这条历史链，提交后直接进入空闲页表复用；
这正是"目录只在 DDL 时变 schema、却在每次翻页时新建整链"不会把文件无界
撑大的原因。
`Catalog.table_at(version, name)` 读出当时的列；`Catalog.view_at(version, name)` 读出当时的视图 token/依赖；每个版本还记录
UTC 微秒级 `created_at`，`Catalog.version_at_or_before(ts)` 可定位某时刻之前的最近版本。
`SHOW CATALOG VERSIONS` 暴露版本、表名、视图名、对象数和创建时间，
`SHOW COLUMNS FROM t AT CATALOG VERSION n` 或 `AT TIME '…'` 直接读出历史列定义；
时间会先经 `version_at_or_before` 解析到目录版本，再取该版本的 `TableDef`。回滚的 DDL 通过
`txn.undo_log` 把版本号和上一版根页恢复，不留下从未提交的版本。
这仍是**目录历史**，不是并发 DDL：当前只有一个可写根指针，DDL 依旧由目录锁
串行化；历史链用于目录回溯。数据页的历史读走 `AS OF SYSTEM TIME LSN`，但它
只覆盖最近 DDL / VACUUM / checkpoint 之后的窗口，不复用目录版本链做历史
schema 解码。

`SAVEPOINT` 记录当时 Buffer Pool 的页 undo 深度和 `txn.undo_log` 深度。
`ROLLBACK TO SAVEPOINT` 只应用这个深度之后的页原像（同样写 CLR），再逆序
执行这个深度之后的目录对象 undo；更早的原像保留给后续事务级 abort。
`RELEASE` 只丢弃保存点标记，不撤销任何写入。保存点是事务内状态，不写 WAL。

并发模式下行级逻辑保存点默认开启（`logical_savepoints=True`；显式
`--physical-savepoints` 或 `logical_savepoints=False` 可关闭）：第一次
`SAVEPOINT` 只打开 `txn.capture_row_undo`，不拿全局写者锁；堆的 INSERT/UPDATE/
DELETE 成功后把逆操作闭包压入 `txn.row_undo_log`。`ROLLBACK TO` 在页/目录 undo
之后按保存点记下的深度逆序执行闭包，直接修版本链的 `xmax/newest/flags`，而不是
恢复整页。该模式是 DML-only：保存点后一旦有行 undo，事务内 `DDL/TRUNCATE`
会被拒绝（结构变更的页原像回滚会和行 undo 的 RID/槽位假设冲突）；如果还没有
行 DML，遇到 `DDL/TRUNCATE` 会先把事务升级成物理保存点，拿写者锁、排空读者、
改回页原像，之后的结构变更与 DML 都能整页撤回。其它会话的 `DDL/TRUNCATE`
在执行前会检查活跃事务的行 undo；仍有则返回 `40001`，因为 `ALTER DROP COLUMN`
改行布局后，行 undo 再按新 schema 解码旧版本会报列数不符。有保存点时也拒绝
`VACUUM`，避免物理回收删掉回滚仍依赖的旧版本。所有保存点 `RELEASE` 后关闭
行捕获，DDL 恢复物理路径。

### 2.6 回收（VACUUM）

`UPDATE` 写新版本、`DELETE` 只打 `xmax`，所以表会持续膨胀。`VACUUM [table]`
负责回收，分两种模式：

1. **离线重排**：没有其它活跃事务时，扫描整表保留当前快照可见的版本，
   把存活行重排成"每行只有一条根元组"（`root/newest` 指向自己、`prev` 为空），
   死版本空间真正收回；重排改变所有 RID，所以紧接着重建该表的所有索引。
2. **在线保 RID**：并发模式下入口先等最多 5s；还有活跃事务且它们都没有
   持有 DDL 目录锁/物理 undo 时转 `HeapFile.vacuum_online`。它保留根记录、
   从 `newest` 到最老活跃快照可见版本的整条前缀链，只删对所有快照都不可见
   的版本；槽位不重排，RID 与索引项保持有效，因此不重建索引。空洞槽位与
   根记录不回收，页链不缩短。活跃 DDL 事务仍按原 `active_xids` 门禁报错，
   因为它的 abort 要按页面原像回滚，不能和在线整页回收交错。

`VACUUM [table] RETAIN LSN n` 强制走在线模式，并额外保留在历史目标 `n`
仍可见的版本；结构下界持久化到 `n`，因此 VACUUM 后 `AS OF ... LSN n` 仍能
读回。它保留的是从目标点根记录到最新版本的整条链，目标越早占用越多；
`RETAIN` 不是自动保留策略。

两种模式都按页写 **before/after image**，崩溃后恢复能重放，不会出现
"VACUUM 之后行凭空消失/复活"。

并发写模式（`BufferPool.logical_abort=True`）下，VACUUM 只允许自动提交事务。
显式 `BEGIN` 事务内的 VACUUM 会直接返回 `0A000`：整事务回滚走逻辑可见性，
堆页/B+ 树页没有物理原像；VACUUM 已经重排页链、回收死版本后，`ROLLBACK`
无法把旧版本变回来。默认串行模式仍允许事务内 VACUUM，按页 undo 回滚。

离线 VACUUM 还会摘掉链尾连续的空页，并把它们交给文件级空闲页表；在线
模式保持 RID 不变，不摘页链。空闲页表存在元数据页尾部（`free_count` +
最多 1008 个 u32 页号），分配新页时优先复用；表满或崩溃丢掉空闲表只会
泄漏空间，不会把仍被引用的页交给新对象。提交点后 `Database` 才把该事务
登记的 `freed_pages` 交给空闲表（仍被 pin 的页跳过），回滚则直接丢弃这批
登记，靠 WAL undo 指回旧页链。`ALTER TYPE` / `DROP COLUMN` 的 shadow heap
切换根页后，旧堆页链也会登记到 `txn.freed_pages`：DDL 开始时历史下界已经
前移，旧堆页不再供 `AS OF` 读取；提交后这些页进入空闲表复用，回滚仍由
页原像恢复旧根。释放时把页头重写成 `FREE`；恢复 redo 结束后
`validate_free_list` 逐项复核页头，持久表里"已被重新分配但仍记为空闲"的
陈旧项会被剔除，宁可泄漏也不复用已提交页。`Database.close()` 在关闭 Buffer Pool、且
先做一次 checkpoint 之后，才从文件尾截掉连续空闲页；checkpoint 保证下次
恢复不会用旧 WAL 记录把已截掉的页 `ensure_page` 回来。page 0 被 Buffer
Pool 缓存时，整页刷盘/淘汰不能直接写旧快照：`DiskManager.write_meta_page`
先用内存里的 `next_page_id`、`next_xid`、`checkpoint_lsn`、历史下界和
空闲表覆盖快照，只保留快照里的 `catalog_root`，避免一次 LRU 刷盘把刚持久化
的空闲表/高水位写回旧值。中间空页留在空闲表
里等复用，因此文件缩容是"尾部回收"，不是在线整理。没有 autovacuum、
没有增量回收；在线模式的根记录/空洞槽位回收是下一件要做的事。

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
父表，同时保留索引异常时的正确性兜底。这条路径上的所有 heap 读取（父行点查、
父行兜底扫描、子行收集 `_fk_child_rows`、提交前补检）都必须带 `ri=True`，即
引用完整性可见性（"已提交即可见"，等同 PostgreSQL RI 触发器的 `SnapshotDirty`）。
用普通快照可见性会让并发提交的父/子行"不存在"：合法插入误报 `23503`，删除父行
时漏掉子行而留下悬空引用。

`INSERT`、`INSERT ... SELECT`、`DEFAULT VALUES`、upsert 和
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
或模式泄漏到提交阶段。`Database` 与 `PGWireServer` 在 `COMMIT`（以及自动提交语句的
提交点）都调用 `executor.validate_deferred_foreign_keys` / `validate_deferred_unique_constraints`：
前者按当前行重跑外键检查，后者确认被引用键是否已恢复或子行是否已清理；失败则
`abort`，WAL/页 undo 恢复。**两条提交路径必须同时维护**：曾经线协议路径漏了这两个
调用，导致 `DEFERRABLE INITIALLY DEFERRED` 在 psql 上完全失效（单条 INSERT 就能
写入没有父行的引用，`tests/unit/test_concurrency.py` 里有回归用例）。纯读语句不进入
这两个补检，否则一次 `SELECT` 会变成全库扫描，并可能返回 `23505`。`RESTRICT`
始终立即，`CASCADE` / `SET NULL` / `SET DEFAULT` 动作也立即执行。
`DROP TABLE` / `ALTER TABLE ... DROP COLUMN ... CASCADE` / `ALTER COLUMN TYPE ... CASCADE`
在删除外键或约束索引时同步清理 `constraint_modes`，避免同一事务内同名重建的约束继承旧覆盖；事务回滚
时 undo 恢复这些模式。`ALTER TABLE ... ALTER CONSTRAINT` 修改外键延迟属性时
也会清理该约束的旧覆盖。

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
`SHOW TABLES` 只列基表，`SHOW VIEWS` 单独列出视图；两者也接受
`AT CATALOG VERSION n` / `AT TIME '…'`，从历史目录重建当时的表/视图清单。
行数列来自该版本的 `row_count_hint`，不是旧数据页。

### 2.11 只读副本与物理 WAL 流

复制层不引入第二套日志：副本消费主库同一份物理 WAL。`Replica` 打开一个
`Database(read_only=True)`，按 LSN 递增应用：

- `PAGE_IMAGE`：先 `DiskManager.ensure_page` 补齐页号，再写完整页镜像；
- `UPDATE` / `CLR`：按 pageLSN 判断是否需要写 after image；
- `BEGIN` / `COMMIT` / `ABORT`：更新副本的 `TransactionManager` 状态，
  让快照读看到与主库一致的事务可见性；`COMMIT` 还把该记录的 LSN 登记为
  副本的提交点并重写 `commit-ledger.bin`，副本不参与本机提交队列。

每批应用结束后先 `flush_all` 页、再原子写 `replica.lsn` marker，崩溃后
从 marker 重放；pageLSN 保证重复应用幂等。副本的 `Database(read_only=True)`
在语句入口拒绝写操作，只允许 `SELECT`/`EXPLAIN`/`SHOW`/事务控制。

传输层是 `WALServer` + `WALClient`：换行分隔 JSON，二进制内容 base64；
`WALClient(compress=True)` 会把响应行变成 `z<base64(zlib(json))>`，客户端按
首字节区分压缩与明文，请求本身仍保持明文。`backup` 返回主库 checkpoint
后的数据文件，`replicate` 从请求的 `start_lsn` 起按上限返回记录。主库
`SegmentedWAL.retain_lsn` 按副本请求的起点保留 WAL；`WALServer` 可配置一个
`slot_path`，客户端 `ack` 后把 `confirmed_lsn` 原子写盘，服务重启时重新装载
并恢复保留点。若起点已经落在已回收段之前，服务端返回明确错误，要求重新
基准备份，避免静默丢段。

基准备份不只传数据文件：`backup` / `backup_stream` 在 `backup_end` 之前用
`ledger_chunk` 分块发送 `commit-ledger.bin`，只包含 `commit_lsn` 不晚于本次
checkpoint 的条目（避免账本领先于已拷贝的数据文件），客户端整包 CRC 校验
后原子写入目标目录；页级增量备份在同一流里带上主库最新账本，副本
`rebase_from_server` 在替换数据文件后替换账本。副本应用 `COMMIT` 时把 LSN
登记进内存账本，并在每批应用后重写账本；它不等副本 checkpoint，因此副本侧
的 `SHOW COMMIT LOG` / AS OF 不会停在基准备份的 checkpoint LSN。账本文件
损坏时副本打开数据库会明确报错；缺失时账本为空，历史读会显式拒绝，而不是
猜一个错误的历史窗口。

复制流的每个 WAL 记录额外携带 `has_crc` / `crc`：CRC32 覆盖逻辑帧头和
payload（不覆盖帧尾自身的 CRC 字段），接收端在反序列化时校验；不带 `crc`
的旧服务端仍可通信。更老的服务端连 `has_crc` 都不发送，接收端必须按
`has_crc=False` 解码，否则本地重编码会多出 4 字节，破坏后续记录的 LSN
连续性。`Replica.apply` 还会用上一帧的 `lsn + len(record.encode())` 计算
下一条记录应有的字节边界，发现批次跳过记录时抛 `StorageError`，不把
`replica.lsn` 静默推过缺口；基准备份和提升会重置这个进程内边界。

提升时的另一个边界：基准备份拷贝数据文件与提交账本、不拷贝 WAL，副本
提升后本地 WAL 是空的。`Replica` 在 `replica.lsn` 里同时记录“最后 apply 的记录 LSN”
和“下一条记录边界”；`promote()` 在 WAL 仍为空时把分段 WAL 的 `next_lsn`
接到这个边界，避免新主库从 LSN 1 重新开始、让已经追到高位的副本拒绝后续记录。
旧版单整数 marker 仍可读，但只按 `applied_lsn + 1` 估算边界，精确边界从新
marker 开始生效。

多副本用 `WALServer(slot_dir=...)`：每个请求携带 `replica_id`，确认 LSN 写到
`<id>.confirmed`；服务端把保留点刷新为所有已知槽位的最小值，慢副本不会被
快副本的 ack 提前截断。`Replica.bootstrap_from_server(replica_id=...)` 在拉完
基准备份后立即 ack 基准备份 LSN；服务端收到带 `replica_id` 的基准备份请求时
也会自动 ack 该 base LSN。新副本不会以 0 确认把整段 WAL 钉住；
`replicate_from_server` / `lag_from_server` / `ReplicationMonitor` 都透传
`replica_id`；监控器遇到 WAL 断档会先尝试页级增量重建。`WALServer(redirect_to=...)`
对请求返回 `redirect`；`WALClient` 抛 `RedirectError`，`ReplicationMonitor`
会更新目标并最多跟随 3 次，不做 DNS 发现或自动选举。槽位目录仍只属于一个主库数据目录。

页级增量基准备份走 `incremental` 请求：主库在写锁内 checkpoint 后扫描
pageLSN 大于 `base_lsn` 的页（页 0 总是包含），只发送这些 4096B 页；副本
关闭只读句柄、逐页写回数据文件再重开，从新的 checkpoint 继续追 WAL。
它能覆盖“副本有旧基准备份但 WAL 已回收”的窗口，不替代完整 bootstrap；
页文件损坏时仍应重新全量备份。

`status` 请求返回 `last_lsn` / `base_lsn` / `confirmed_lsn`；`last_lsn` 是最后
一条记录自身的 LSN，不是 `next_lsn` 字节偏移，副本的 `lag_from_server` 用它
计算真实落后量。`WALServer(token=...)` 开启后，所有请求必须携带同一个
token；这是共享密钥认证，TLS 需要另配 `ssl_context`。`WALServer` 接受连接后
先做 TLS 握手再读取请求；`WALClient(ssl_context=...)` 在 TCP 连接上做客户端
握手，`server_hostname` 默认取主机名。测试用自签名证书覆盖握手、基准备份和
WAL 流，但证书分发、轮换、双向认证仍属于部署层。
主库重启时应在打开 `Database` 前读槽位，并用
`Database(wal_retain_lsn=read_slot(slot_path))` 把保留点放回 WAL；这比“先开库、
再启动 WALServer”更安全。

**基准备份写冻结**：`Database.snapshot()` 用 `_write_lock` 阻止新的写语句
进入，要求 `txns.active_xids` 为空，然后先 checkpoint、再读取数据文件。读
查询不走写锁，可以在备份期间继续；副本的基准备份因此不会包含未提交版本
或 checkpoint 之后的混合页。`snapshot_file()` 用同一条写锁路径把数据文件
复制到目标，而不是读成 `bytes`；`WALServer.backup_stream` 再把临时文件按
块发送，`WALClient.backup_to` 分块落盘，避免大库占用 Python 内存。

**手动提升 fencing**：`PromotionFence` 是一个共享文件 marker，副本提升时
原子写入 `promoted_node` 和 `lsn`；旧主库通过 `Database(write_fence=...)`
在每笔写语句前检查 marker，发现别人已提升就拒绝写入。它单独使用时不做自动选举，
也没有租约/超时仲裁，只覆盖“两个节点能看到同一个文件”的部署。

**多数派投票 MVP**：`QuorumNode` 在持久化 `term/voted_for/last_lsn/last_term`
的状态下响应 `vote_request`；`campaign()` 需要拿到集群多数票，投票节点按
Raft 的 `(last_term, last_lsn)` 字典序拒绝落后候选（同 LSN 但最后日志任期
更旧的候选也会被拒）。`observe_applied(lsn, term=...)` 在记录已复制进度时
一并记录日志任期；心跳只更新"听到过"的 `last_lsn`，**不会**把 leader 的当前
任期写成日志任期，否则日志相同的节点会互相拒绝。`Replica.promote_with_quorum()`
先拿票再切换读写。它已有 leader 心跳 RPC、独立 `HeartbeatLoop` 和 TTL 租约，
以及 prefix-checked `append_entries`；提升后的心跳循环每轮自动追推多批
entries，拼接点任期分歧与 WAL 缺口都会触发自动重建（leader 侧还会主动
下发 `rebuild_hint`），逐条任期由 `VoteState.log_terms` 按 LSN 分段持久化；
没有分歧尾巴截断（重建替代）和完整 commit index，也不能单独处理
“主库还活着但网络分区”的脑裂；提升后仍依赖共享 fence 挡住旧主库。

**写租约 MVP**：`Lease(path, node_id, ttl=...)` 把持有者、任期和到期时间
原子写进共享文件。主库每次写语句前调用 `lease.assert_writable`，超过 TTL
没有 `renew` 就自行拒绝写入；候选节点必须等旧租约过期才能 `acquire`，
不会在旧主库仍持租约时抢占。`QuorumNode.campaign(lease_path=...)` 在拿到
多数票后同时获取租约，`Replica.promote_with_quorum(lease_path=...)` 把它
接到提升路径。`QuorumNode.heartbeat_round()` 向 peers 发送 `heartbeat`
RPC，`renew_lease()` 只有拿到多数派确认才调用 `Lease.renew()`；
`LeaseKeeper(renew=...)` 可把后台续租切到 quorum 路径。它仍依赖共享文件
和大致同步的墙钟，不是完整的 Raft leader lease，时钟漂移会改变安全窗口。
`Lease` 的 acquire/renew/release 用 `<lease>.lock` 串行化，`LeaseKeeper`
在后台续租；`Replica.promote_with_quorum(...)` 在提升后自动启动 keeper
（默认周期 `ttl / 3`，可用 `lease_keeper_interval` 覆盖、`lease_keeper_auto=False`
关掉）；`Replica.close()` / `step_down()` 先停心跳、再停 keeper；提升后的新主库
用 `LeaseFence`（租约 + 当前任期）作为 `write_fence`，租约丢失或观察到
更高任期即拒绝写；旧主库收到更高任期心跳后也会立即停写；keeper 续租失败可回调 `Replica.step_down()`
自动切回只读；`Lease.renew(term=...)` 在节点任期高于租约任期时拒绝续租。

**重启双写防护（打开即检）**：运行中的写围栏只挡"还活着的旧主库"——进程
崩溃后重启、用裸 `Database(path)` 打开时，不知道共享 marker 的存在，恢复
写入即成双主。`Database(fence_path=..., node_id=...)` 在非只读打开时立即
校验 fence（指向别的节点就拒绝打开）；`Database(lease_path=...)` 校验租约
（无主/他人持有/已过期一律拒，旧主库必须显式重新 `acquire` 才能恢复写）。
两个检查都持续接进写路径（打开之后别人才提升的场景在下一笔写上被拦）；
只读打开放行，被废黜的旧主库仍可当只读观察者。`Replica.promote()` 重开
可写库时透传两个路径，提升后的新主库重启同样受自己写的 marker 保护。

**写围栏的三个检查点（缺一不可）**：

1. **语句入口、且在 `Database._write_lock` 之内**（`Database._run`）——在锁外检查的话，
   等锁期间租约可能已过期，这一笔写还会落盘；
2. **提交点**（`TransactionManager.commit`，追加 COMMIT 记录之前）——一条长语句
   （如百万行 `INSERT ... SELECT`）可以跨过租约到期，入口检查拦不住它；
   只读事务（`txn.wrote == False`）跳过检查，旧主库降级后仍要能当只读观察者；
3. **线协议路径**（`server/pgwire.py` 的 DML 与 COMMIT 分支）——会话直接调
   `Executor.run`，**不经过** `Database._run`；漏掉这里，被 fence 的旧主库挂上
   pgwire 后 psql 仍然能写。`Database.assert_writable()` 是这三个入口的统一实现。

**心跳接线**：`promote_with_quorum(heartbeat_interval=...)` 在提升后启动
`HeartbeatLoop`，观察到的更高任期会推进本节点任期并回调 `on_higher_term`；
不提供回调时，`lease_loss_stepdown=True` 会直接 `step_down()`。没有心跳循环时，
新主库的任期视图永远不动，`LeaseFence` 的"观察到更高任期就停写"实际上不会触发。

**心跳扇出**：`heartbeat_round()` 用线程池并发向 peers 发 `heartbeat`，总耗时约等于
单个 `timeout` 而不是 `N × timeout`（串行扇出会让一个挂死的 peer 把租约推到过期）；
结果按 peer 声明顺序回收，`last_acks` / `last_errors` 语义不变；
`abort_on_higher_term=True` 时看到更高任期就取消还没发出的请求。

**锁文件与租约文件是两件事**：`<lease>.lock` 是 OS advisory lock
（`storage/filelock.py`，进程退出/崩溃即释放，不会"残留"）；真正会残留的是
**租约 JSON**（`holder` / `term` / `expires_at`），这是有意的 TTL 语义：崩溃节点的
租约必须等到 `expires_at` 之后才能被抢占，否则会出现两个写者。租约里的时间是
**墙钟**，跨主机时钟漂移会直接改变安全窗口——部署要求各节点时钟偏移小于 `ttl / 3`，
并且不应把租约文件放在不可靠的跨网络文件系统上。

**自动提升 MVP**：`ReplicationMonitor.poll()` 先 `status`、再拉一批 WAL；
连续 `max_failures` 次网络错误后调用 `Replica.promote(fence_path=...)`。
传入 `lease_path` 等参数时，自动提升也走 `promote_with_quorum`，把租约、
心跳续租和写围栏一并装到新主库。

`ReplicationMonitor` 有显式状态机（`MonitorState.following / promoting / promoted /
read_only / needs_attention`）：租约丢失触发的 `Replica.step_down()` 会通过
`add_step_down_listener` 把 monitor 的状态扣回 `needs_attention`
（`poll()` 也随即返回 `needs_attention`），不会继续对外报 `promoted`。

**多数派提交水位（轻量版）**：只有 WAL 流 + 槽位 ACK 时，提升一个落后副本会
静默丢掉主库已提交、但还没发出去的尾段。这里用一层最小协议补上这个判断：

- 每个节点把**真正复制到**的 LSN 记在 `VoteState.match_lsn`，心跳响应会带上它；
- leader 每轮心跳收集“自己 + 各 peer 的 `match_lsn`”，取第 `majority` 大的值作为
  `quorum_commit_lsn`（三节点即“第二大的那个”），并持久化（只前进不后退）；
  append_entries 推送成功后 `push_entries_to_peers` 会立即把 peer 的新
  `match_lsn` 计入水位，同步复制的屏障不用多等一个心跳周期；心跳请求同时
  携带 leader 的 `commit_lsn`，follower 也会保存，换主时新候选
  不必从 0 开始猜水位；
- `campaign()` 在投票前先比一次：`candidate_lsn < quorum_commit_lsn` 直接拒绝提升，
  提示先追平 WAL（或做一次增量基准备份）再发起。

`match_lsn` 必须与 `last_lsn` 分开，这是这条协议唯一的坑：`last_lsn` 是"听到过
的最高 LSN"，一个只投票不带数据的 witness 节点收到 leader 心跳后也会把它抬到
leader 的位置——如果拿它算水位，两个 witness 就能伪造"多数派已持有"，闸门形同虚设。
`heartbeat()` 里的 `observe_lsn(leader_lsn)` 只用于投票安全（拒绝落后候选），
不参与水位计算；投票响应里 `last_lsn` 与 `match_lsn` 都带，`campaign()` 当选后
算水位也只用 `match_lsn`，避免 witness 用"听到过"的进度虚抬 `verified_commit_lsn`。

水位默认只解决"**提升谁**"的问题：写入仍然只要求本地 WAL fsync。
`Replica.promote_with_quorum(sync_replication=True, heartbeat_interval=...)` 可以把它
升级成"**少数派都不算提交**"：

- 屏障 `QuorumCommitBarrier` 挂在 `TransactionManager` 上，在本地 COMMIT 记录
  fsync 之后、返回成功之前被调用，等 `quorum_commit_lsn >= 本事务的 commit_lsn`；
- 超时抛 `CommitOutcomeUnknown`（SQLSTATE `40003`），**不回滚**：数据已经本地落盘，
  把它当成"没写进去"才是真正的错误。调用方应重新读一次确认，或交给幂等重试；
- 只读事务（包括自动提交的 `SELECT`）不经过屏障，否则每次查询都要等水位；
- 必须配心跳循环：水位由 `heartbeat_round` 推进，没有心跳就等于每笔写都等超时，
  所以 `sync_replication=True` 而 `heartbeat_interval is None` 会在提升前被直接拒绝。

它仍然**不是完整的 Raft 日志协商**：现在有 prefix-checked `append_entries`
MVP（`QuorumClient` / `QuorumNode`，`prev_lsn/prev_term` 前缀检查，entries 走
现有 CRC 解码并交给绑定的 `Replica.apply`）；`Replica.push_entries_to_peers`
可以在一次显式调用中把本地 WAL 的下一批记录推给 peers，`promote_with_quorum`
启动的 `HeartbeatLoop` 会把它作为 push callback 每轮心跳后自动推，一轮最多
`max_batches_per_round` 批（默认 10），且推送前先过
`db.assert_writable()`。逐条日志的 per-entry term 已做
（`VoteState.log_terms` 按 LSN 分段持久化，拼接点任期分歧拒绝并要求重建，
leader 还主动下发 `rebuild_hint` 处理 WAL 缺口）。但仍有分歧尾巴截断/回填，
也没有完整的 commit index 协商。长期失联的旧
leader 复辟后仍可能写进一些“多数派没收到”的记录（共享 fence + 租约负责
挡住这类写入）。当前的 `(last_term, last_lsn)` 投票只能防止“同 LSN 但更旧
任期”的候选直接当选，`last_term` 来自 `observe_applied` 上报时携带的任期，
是近似值。要做到 Raft 级别的正确性，仍需要完整日志协商 + commit index。

客户端重定向已做 MVP（`WALServer(redirect_to=...)` / `WALClient(follow_redirects=True)`），
但“主库其实还活着但网络分区”的脑裂仍只能靠共享 fence + 多数派续租兜底。

当前边界（也是下一步）：

- 复制槽支持多个副本（`slot_dir` + `replica_id`，保留点取最小确认 LSN），
  但槽位目录只属于一个主库数据目录，没有跨节点槽位复制；主库重启时用
  `Database(wal_retain_lsn=min_slot(...))` 在恢复和自动 checkpoint 之前
  恢复保留点，避免启动顺序造成误回收；
- 基准备份已分块传输，且数据文件与 `commit-ledger.bin` 同流携带，但仍是
  完整全量拷贝；页级增量基准备份已做 MVP
  （`snapshot_pages(base_lsn)` 只传 pageLSN 大于基础 LSN 的页），基准备份
  内容本身的压缩仍未做；
- `_write_lock` 只覆盖 `Database._run` 的写语句，跨进程或直接写磁盘不受
  保护；备份期间主库写语句被串行化，读查询不受影响；
- 自动提升已有连续失败阈值、多数派投票、`(last_term,last_lsn)` 投票比较、TTL
  写租约、heartbeat RPC 和多数派续租，并有轻量版提交水位（`quorum_commit_lsn`）
  挡住"提升落后副本"，可选开同步复制（多数派追上才回 COMMIT，超时给 `40003`
  结果未知）；已有 prefix-checked `append_entries`、逐条 per-entry term 与
  gap→rebuild 自动闭环，提升后的心跳循环可每轮推一批 entries，但仍缺分歧
  尾巴截断和完整 commit index，网络分区时仍可能双写。fencing 依赖共享
  marker 和租约文件（重启双写防护：`fence_path`/`lease_path` 打开即检）；
  客户端重定向已做 MVP；
- 复制连接支持可选共享 token 校验和可选 TLS；服务端把 `SSLContext.verify_mode`
  设为 `CERT_REQUIRED` 并加载客户端 CA 即可双向认证。TLS 不配置时 token 在明文
  JSON 中传输，默认只应监听本机或可信网络；证书分发和轮换仍需部署层补齐；
- 副本单线程 apply，查询与 apply 不应并发；提升是手动操作，fencing 覆盖
  共享 marker + TTL 租约 + 多数派心跳续租。

`Replica.promote()` 关闭只读 `Database`，以读写模式重新打开同一数据目录，
返回可写 `Database`。副本只保留已应用的 WAL 前缀；主库尚未发送的尾段会在
提升时丢失，调用方应先确认 `lag_lsn` 或接受该窗口。

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

计划节点带有 `estimated` 字段（`plan/cost.py` 的 `CostEstimate`）：扫描用堆表
维护的 `row_count_hint`，过滤按等值/范围/其他谓词的选择率估算，连接按等值键
数量估算并区分哈希与嵌套循环，投影/聚合/排序/去重/集合/`LIMIT` 继续向上
传播估算。`EXPLAIN` 会把 `rows≈N cpu≈M confidence=...; reason=...` 标在节点
后面；`reason` 说明为什么选这个访问路径或放弃别的路径。`EXPLAIN ANALYZE`
再追加实际耗时、行数与循环次数。

`ANALYZE [TABLE] t [SAMPLE n]` 会扫描当前可见行，把 `row_count_hint` 对齐到
实际行数，并为每一列记录 `n_distinct` 与 `null_frac`，随目录 JSON 持久化。
`SAMPLE n` 用固定种子的蓄水池抽样只保留 n 行参与列统计，行数仍精确计数；
此时 `n_distinct`、`min/max` 和直方图都是近似值，`SHOW STATISTICS` 的
`rows` 仍是总行数，`sample_rows` 记录实际参与统计的样本行数（0 表示全量）。
等值谓词
（`col = 常量`）不再固定使用 0.1，而是优先用
`(1 - null_frac) / n_distinct`；没有 ANALYZE 统计时仍回退默认选择率。

数值列记录 `min_value` / `max_value` 和一个 16 桶等宽直方图；字符串列记录
首尾值和 16 桶等频直方图（桶边界取排序后的实际值）。范围谓词
（`>` / `>=` / `<` / `<=` / `BETWEEN`）数值按桶覆盖比例、字符串按整桶命中
估算，索引范围扫描和普通过滤共用同一份统计；`IndexRange` 已经估过范围，
上层 Filter 只复核边界，不会把同一谓词的选择率乘第二遍。字符串边界按字典序
比较，不做数值转换（`'090' < '1'`）。

`SHOW STATISTICS [FROM t]` 把这些列统计暴露成只读目录视图（行数、n_distinct、
null_frac、min/max、桶数、sample_rows），不复制 PostgreSQL 的
`pg_statistic` 二进制布局。加 `AT CATALOG VERSION n` / `AT TIME '...'`
可读取历史版本的列统计（列集合与统计值都来自该版本目录快照）；数据页本身
不会回到过去，notice 会明确这一点。

这些估算目前只用于观测，不改变执行顺序。索引选择仍是两条规则：

- 单表查询、`WHERE` 形如 `col = 常量`、且该列上有索引 → `IndexLookup`；
- `WHERE` 是单列范围条件（`>` / `>=` / `<` / `<=` / `BETWEEN`）→ `IndexRangeScan`，
  范围谓词仍会在上层 `Filter` 再判一次，保证边界与剩余条件正确；
- 其余一律 `SeqScan + Filter`。

范围扫描按"索引列前缀"比较，忽略 `KeyCodec` 追加的 RID；否则
`col > 10` 会把 RID 更大的 `(10, ...)` 当成大于边界，结果里混进 10 本身。

`INNER JOIN` / `LEFT JOIN` 的顶层 `AND` 等值条件走 **hash join**：默认右侧
建哈希表、左侧流式探测；`INNER` 连接若估算左侧更小，则反向建左表、右侧
探测，`EXPLAIN` 用 `[build=left/right]` 标出实际选择。没有等值键（`OR`
条件、`WHERE` 里没有跨数据源等值）时退回 nested loop。逗号连接会从 `WHERE`
顶层 `AND` 中提取跨源 `列 = 列`，附加到对应的 `Join` 作为哈希键；完整
`WHERE` 仍保留为 `Filter`，所以残余谓词和非等值条件不会被漏判。`NULL` 键按
SQL 语义不参与匹配，重复键会展开成多行，剩余谓词仍由完整的 `ON` 复核。
`RIGHT JOIN` / `FULL JOIN` 统一用 nested loop：右表做外层，`FULL` 额外记录右表
哪些行被匹配过，最后补出未匹配的右行；两者都按 left+right 的列顺序输出，未匹配
的一侧填 `NULL`。`CROSS JOIN` 是 `ON` 为真的 `INNER JOIN`。
集合运算用 `SetOp`：`UNION ALL` 直接拼接，`UNION` 流式去重，`EXCEPT` 先物化右表
再筛左表，`INTERSECT` 同理；`INTERSECT` 在解析时被收紧成更高的优先级。
连接顺序：对 3–8 个没有显式 `JOIN` 的 `FROM` 源（逗号连接），绑定器用
`WHERE` 顶层的跨源等值边做**左深动态规划**——状态是已选源集合，枚举最后
接入的源，取 `rows + cpu` 最小的顺序。重排后追加一个顶层 `Project`，把输出
列映射回 SQL 书写顺序；显式 `JOIN` 链、外连接和超过 8 个源保持原序。
`EXPLAIN` 的哈希连接行会标 `[order=dp]` 与 `[candidates=N]`，估算的
`reason` 写明“几个等值键、build 是哪一侧”；没有等值键时写明“嵌套循环：
没有等值连接键”。候选的逐项代价仍不展开。

### 3.4 计划缓存

`Binder.bind_query` 的产物（计划节点、输出列、CTE 表）按
`(id(AST), Catalog.schema_version)` 缓存在执行器里，上限 256 条、按插入顺序
逐条淘汰。三个约束决定了这个设计：

1. **只缓存无参数语句。** `$n` 在绑定阶段就被折成字面量；缓存带参数的计划
   等于把第一个参数值冻结进去。参数化查询每次重绑，是正确性的代价。
2. **DDL 必须让缓存整体失效。** `CREATE/DROP/ALTER TABLE`、`CREATE/DROP INDEX`、
   `REINDEX`、`TRUNCATE` 执行前抬高 `Catalog.schema_version`；副本收到目录页
   重载 `Catalog.load()` 时同样抬高。版本号进键，旧计划自然取不到。
3. **AST 被解析缓存淘汰后不能认错。** 键里是 `id(AST)`，但对象释放后 `id`
   会被复用；所以条目里存一个弱引用，命中时要求 `ref() is AST`，否则丢弃。

实测（2000 行、单列索引、同进程 A/B）：同一 SQL 反复执行 307.6 → 189.2 us
（−38.5%）；200 个不同常量轮询（模拟基准里的"每次 SQL 文本都不同"）97.6 →
70.2 us/次（+28.1%）。后者能命中，靠的是"逐条淘汰 + 256 条上限"，全清会让
每一轮都把几百条语句重新绑定一遍。

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
- FROM 派生表：`TableRef.query` 保存 `(SELECT ...) AS alias` 的子查询；
  `_bind_derived` 用独立 Binder 绑定并把依赖合并回外层，再包成 `CteScan`。
  派生表按非 LATERAL 语义处理——只能看到外层查询的 schema，不能引用同一
  FROM 里的兄弟数据源；普通表与派生表共用同一套 JOIN 绑定路径。
  `TableRef.column_aliases` 由 `_apply_column_aliases` 统一改写 Schema 与
  输出列名，普通表别名也走同一路径；派生表内部是 `WITH ... SELECT` 时
  走 `bind_query`，CTE 注册表只属于该派生表 Binder；
- `RecursiveCte`：`seed UNION [ALL] recursive` 的递归 CTE。`CteScan(recursive=True)`
  在递归项里读取执行器保存的“上一轮工作表”，每一轮把新行加入结果并作为下一轮输入，
  直到没有新行；`UNION` 用哈希集合去重，`UNION ALL` 保留重复，超过 10000 轮报错；
- 相关子查询：`Binder.outer_schemas` 保存由近到远的外层 schema 链，列解析先查
  当前查询、再逐个外层查找；编译后的外层列闭包沿 `EvalContext.parent` 取值。
  `_run_subquery` 在执行子查询计划前把外层行写入上下文变量，计划内新建的
  `EvalContext` 自动继承它，支持多层嵌套；当前按外层行逐次执行，未做去相关优化；
  `_bind_subquery` 会把子查询 Binder 的表/列依赖合并回外层，保证带相关子查询的
  视图仍能被 `DROP COLUMN ... CASCADE` 正确识别；
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

所有写语句（简单查询与扩展查询、显式事务与隐式事务）都先过
`Database.assert_writable()`：写围栏不是"主库自己记得检查"，而是每个入口都必须过的一道闸。

多连接共享同一个 `Database`，服务端用一把 asyncio 锁把**单条语句**串行化；
事务之间的隔离由每个会话自己的 `Transaction` 与 MVCC 提供。
这不是"高并发数据库"，而是"协议正确、语义正确"的单机引擎。

## 6. 为什么这样切分

- **协议层与引擎解耦**：可以先用进程内 REPL 开发引擎，等引擎稳定再接协议，调试成本低一个数量级。
- **物理日志**：让"崩溃恢复"这件事从"希望正确"变成"可以断言"。
- **不做静默降级**：任何未实现的能力抛 `NotSupportedError` 并附带原因。一个返回错误答案的数据库比一个明确报错的数据库危险得多。
