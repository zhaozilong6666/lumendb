# AS OF SYSTEM TIME：设计与已交付边界

`AS OF SYSTEM TIME LSN n` 已经接到 SQL 表面。它不是“无限历史”：可读窗口是
`[max(structural_floor_lsn, ledger_floor_lsn), latest_commit_lsn]`，超出窗口
必须报错，不会静默退化成当前读。

```sql
SHOW HISTORY WINDOW;
SELECT * FROM t AS OF SYSTEM TIME LSN 123456;
EXPLAIN SELECT * FROM t AS OF SYSTEM TIME LSN 123456;
```

`SHOW HISTORY WINDOW` 输出 `checkpoint_lsn / floor_lsn / latest_commit_lsn / commits`。
`latest_commit_lsn` 在账本为空时为 `NULL`。

## 已交付的 P0 / P1 / P3 切片

### 提交账本

- `TransactionManager` 维护 `xid -> COMMIT LSN`，只在事务真正标记 `COMMITTED`
  后登记；回滚、未提交事务不进入账本。
- 账本跨 checkpoint 持久化到数据目录的 `commit-ledger.bin`：定长二进制、
  `os.replace` 原子重写、fsync 文件与目录。默认保留最近
  `commit_ledger_max_entries=100000` 条提交。
- checkpoint 的顺序是“先写账本，再推进元数据页里的 `checkpoint_lsn`，最后
  裁 WAL”。账本写入失败时 checkpoint 不推进，WAL 也不裁剪，恢复仍能重建。
- 重开时把持久账本与 WAL 中所有 COMMIT 记录按 LSN 合并；不再只收
  `checkpoint_lsn` 之后的记录，避免“元数据已推进、WAL 尚未裁剪”时丢提交。
- `SHOW COMMIT LOG [LIMIT n]` 暴露 `xid / commit_lsn`，按 LSN 升序，供用户
  选目标点；checkpoint 不再把它清空。

### 历史可见性

- `HeapFile.scan_at(..., assume_committed_before=..., strict_unknown_xmax=...)`
  按提交 LSN 截止点判断版本，不伪造 `Transaction`；默认 `scan` 路径不变。
- 目标点之前提交的 `xmin` 可见；目标点之后才提交的 `xmax` 不会提前隐藏旧行。
- 账本最老 LSN 之前的 `xmin` 视为“水位前已提交”：该物理版本仍留在堆里，
  且目标点不早于水位；只要 `xmax` 没有在水位后提交，它在目标点就应可见。
- 非零 `xmax` 不在账本、也没有已知结局时，SQL 路径严格报错，不猜“未提交”，
  避免复活已经删除的旧版本。持久账本覆盖 checkpoint 之前的删除后，这个
  报错窗口只剩“目标点早于账本保留窗口”的情况。

### SQL 表面

- 语法只接受 `LSN <非负整数>`；时间字面量明确报 `NotSupportedError`，提示先用
  `SHOW COMMIT LOG` 选 LSN。暂不支持 `AS OF SYSTEM TIME '...'`。
- 基表顺序扫描与索引扫描都支持：索引项只提供候选根 RID，`HeapFile.get_at`
  从根沿版本链找到目标点可见的版本，再按该版本的键复核索引条件。索引里的
  旧键项是历史候选，不会因为当前键已改走而直接返回。
- 索引重建（`CREATE INDEX` / `REINDEX`）会推进结构下界，丢掉更早的旧键项；
  因此索引历史读与堆表历史读共享同一个可读窗口。
- 同一查询内所有 `AS OF` 数据源必须停在同一个 LSN；JOIN、CTE、派生表、标量
  子查询共用目标点集合，出现多个目标直接报 `BinderError`。
- `AS OF` 只能直接作用于基表；视图、CTE、派生表作为数据源时拒绝，视图定义里也不允许
  藏 `AS OF`。
- 显式事务内拒绝 `AS OF`，避免和事务自己的快照混在一起。
- 只允许独立的只读 `SELECT` / `EXPLAIN`；`INSERT ... SELECT`、`CREATE TABLE AS
  SELECT` 等写语句里出现 `AS OF` 直接报 `NotSupportedError`，不把历史行当作
  当前写入输入。
- `EXPLAIN` 的 `SeqScan` / `IndexScan` 都会带 `[AS OF LSN n]`；索引计划只说明
  候选入口，历史版本仍要回堆解析。

### 历史窗口

`floor_lsn = max(structural_floor_lsn, ledger_floor_lsn)`：

- `structural_floor_lsn` 是 VACUUM / DDL 开始前的 WAL 位置，持久化在元数据
  页 `history_floor` 字段（offset 48）。推进发生在任何页回收或 schema 变更
  之前，崩溃重启后不会放宽。
- `ledger_floor_lsn` 是持久提交账本里最老一条提交的 LSN。账本保留窗口裁短
  时它会抬高；首次把 WAL 中的旧提交写入账本时它会降低。
- checkpoint 不再推进下界：它只落盘和裁 WAL，旧版本仍在页里，提交账本也已
  持久化。因此目标点可以落在 checkpoint 之前，只要不早于上述两个下界。
- 默认 VACUUM 推进结构下界：开始回收旧版本之前的点不再保证能读回。
- `VACUUM [table] RETAIN LSN n` 是显式保留水位：强制走保 RID 的在线回收，
  额外保留在 `n` 仍可见的版本，结构下界只推进到 `n`。目标必须落在
  `[floor_lsn, latest_commit_lsn]` 内；VACUUM 结束后仍可 `AS OF ... LSN n`。
  这是“保留一个目标点”，不是自动保留最近 N 小时/N 条。
- DDL 推进结构下界：当前 schema 不能安全解码 DDL 之前的旧页。于是所有被
  允许的目标点都在最近 DDL 之后，基表列布局与当前 schema 一致；不需要
  历史 schema 解码。
- 目标点早于下界报 `BinderError`；晚于 `latest_commit_lsn` 也报 `BinderError`。

## 仍未交付

### P0 剩余风险

持久账本解决了“checkpoint 后提交历史消失”，但它仍是**有界保留**：超过
`commit_ledger_max_entries` 的更老提交会被裁掉，目标点随之下界抬高。账本
文件也还没有纳入基准备份/增量备份的传输清单；副本侧的历史读需要单独设计。

### P2 保留水位（已交付显式目标点）

VACUUM 默认会推进结构下界；`VACUUM ... RETAIN LSN n` 可以把一个目标点
保留下来，但仍没有“保留最近 N 个 LSN / N 小时”的自动策略。RETAIN 会保留
该目标点到最新版本之间的整条版本链，目标越早，磁盘占用越大；文件本身仍
不缩盘。

### P1 剩余面

- 索引历史读是“索引候选 + 回堆解析”，没有覆盖索引或索引-only 历史扫描；
  索引重建会推进结构下界，丢掉更早的旧键项。
- JOIN 已强制同一目标点，但还没有“查询级共享快照对象”；每个 Scan 各自调用
  `scan_at`，目标点相同即可。
- 不承诺超出账本保留窗口的完整历史，也不承诺历史 schema 解码。
- 写入、DDL、序列/自增回滚、外部副作用的时间倒流都不支持。

## 验收现状

`tests/unit/test_as_of.py` 覆盖：

- INSERT / UPDATE / DELETE 三个提交点的读数；
- checkpoint 前后都能读历史，目标点可以早于 checkpoint；
- checkpoint 之前删除、未 VACUUM 的旧版本，重开后由持久账本正确判为不可见；
- VACUUM / DDL 推进的结构下界跨重启仍然生效；
- `VACUUM ... RETAIN LSN n` 保留目标点版本，目标越界报错，重开后仍可读；
- 等值/范围索引扫描在目标点解析旧版本，旧键项不会混入；删除后仍可点查；
- `CREATE INDEX` / `REINDEX` 推进下界，索引重建前的目标点明确拒绝；
- `EXPLAIN` 的 SeqScan / IndexScan 都标注 AS OF 目标；
- 早于 floor、晚于 latest、显式事务、视图数据源、时间字面量的稳定报错；
- JOIN / 子查询的多目标点拒绝。

`tests/unit/test_commit_log.py` 与 `tests/unit/test_commit_ledger.py` 覆盖
账本跨 checkpoint、重开、保留窗口裁剪、文件损坏和 checkpoint 写入失败保护。

全量基线见 [`docs/roadmap.md`](roadmap.md)。

## 后续设计问题

1. 账本保留默认多长？当前按条数（100000）；按 LSN 距离或小时更贴近“历史
   窗口”的语义，但需要同时考虑写入速率。
2. 是否允许 `AS OF SYSTEM TIME '...'` 由时间映射到最近提交 LSN？内部仍以
   LSN 为准，避免时钟回拨。
3. 索引历史读已落地为“候选 + 回堆解析”；要不要进一步做覆盖索引的历史
   索引-only 扫描，需要单独保留历史键值或可见性元数据。
4. 账本文件如何进入备份/复制链路：基准备份要原子携带它，增量备份要处理
   checkpoint 之间的追加。
