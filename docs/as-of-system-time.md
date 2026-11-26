# AS OF SYSTEM TIME：设计与已交付边界

`AS OF SYSTEM TIME LSN n` 已经接到 SQL 表面。它不是“无限历史”：可读窗口是
`[floor_lsn, latest_commit_lsn]`，超出窗口必须报错，不会静默退化成当前读。

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
- 重开时从 WAL 当前窗口重建；`SHOW COMMIT LOG [LIMIT n]` 暴露
  `xid / commit_lsn`，按 LSN 升序，供用户选目标点。
- checkpoint 仍会裁剪 WAL 窗口内的旧 COMMIT 记录。更早的提交账本尚未持久化
  到 checkpoint 之外，这是当前最大的 P0 缺口。

### 历史可见性

- `HeapFile.scan_at(..., assume_committed_before=..., strict_unknown_xmax=...)`
  按提交 LSN 截止点判断版本，不伪造 `Transaction`；默认 `scan` 路径不变。
- 目标点之前提交的 `xmin` 可见；目标点之后才提交的 `xmax` 不会提前隐藏旧行。
- 账本里没有的 `xmin` 视为“水位前已提交”：该物理版本仍留在堆里，且目标点
  不早于水位；只要 `xmax` 没有在水位后提交，它在目标点就应可见。这样
  checkpoint 之前的存量行不会因为账本裁剪而整表读空。
- 非零 `xmax` 不在账本、也没有已知结局时，SQL 路径严格报错，不猜“未提交”，
  避免复活已经删除的旧版本。

### SQL 表面

- 语法只接受 `LSN <非负整数>`；时间字面量明确报 `NotSupportedError`，提示先用
  `SHOW COMMIT LOG` 选 LSN。暂不支持 `AS OF SYSTEM TIME '...'`。
- 只允许基表顺序扫描：索引项只保存当前 RID，`AS OF` 时禁用 `IndexLookup` /
  `IndexRange`，统一走堆表扫描后按提交点复核。
- 同一查询内所有 `AS OF` 数据源必须停在同一个 LSN；JOIN、CTE、派生表、标量
  子查询共用目标点集合，出现多个目标直接报 `BinderError`。
- `AS OF` 只能直接作用于基表；视图、CTE、派生表作为数据源时拒绝，视图定义里也不允许
  藏 `AS OF`。
- 显式事务内拒绝 `AS OF`，避免和事务自己的快照混在一起。
- 只允许独立的只读 `SELECT` / `EXPLAIN`；`INSERT ... SELECT`、`CREATE TABLE AS
  SELECT` 等写语句里出现 `AS OF` 直接报 `NotSupportedError`，不把历史行当作
  当前写入输入。
- `EXPLAIN` 的 `SeqScan` 会带 `[AS OF LSN n]`，不会出现 `IndexScan`。

### 历史窗口

`floor_lsn = max(latest checkpoint LSN, 最近 VACUUM 开始 LSN, 最近 DDL 开始 LSN)`。

- checkpoint 推进下界：更早的 COMMIT 账本被裁剪，不保证还能精确判断。
- VACUUM 推进下界：开始回收旧版本之前的点不再保证能读回。
- DDL 推进下界：当前 schema 不能安全解码 DDL 之前的旧页。于是所有被允许的
  目标点都在最近 DDL 之后，基表列布局与当前 schema 一致；不需要历史 schema
  解码。
- 目标点早于下界报 `BinderError`；晚于 `latest_commit_lsn` 也报 `BinderError`。

## 仍未交付

### P0 持久提交账本

`SHOW COMMIT LOG` 仍只覆盖当前 WAL 窗口；checkpoint 后更早的 COMMIT 记录被
裁剪。AS OF 用“水位前已提交”假设补存量行，但没有跨 checkpoint 的完整账本。
因此 checkpoint 之前被删除、又还没被 VACUUM 回收的旧版本，遇到未知 `xmax`
会严格报错，而不是猜测。

### P2 保留水位

VACUUM 会推进 floor，但没有“保留 N 个 LSN / N 小时”的显式参数。VACUUM 之后
更早的目标点直接不可用。在线 VACUUM 能复用空页，但文件仍不缩盘。

### P1 剩余面

- 索引路径禁用；覆盖索引、索引真值没有历史读。
- JOIN 已强制同一目标点，但还没有“查询级共享快照对象”；每个 Scan 各自调用
  `scan_at`，目标点相同即可。
- 不承诺跨 checkpoint 的完整历史，也不承诺历史 schema 解码。
- 写入、DDL、序列/自增回滚、外部副作用的时间倒流都不支持。

## 验收现状

`tests/unit/test_as_of.py` 覆盖：

- INSERT / UPDATE / DELETE 三个提交点的读数；
- checkpoint 后仍能读 checkpoint 前的存量行；
- `EXPLAIN` 标注 AS OF 且禁用索引；
- 早于 floor、晚于 latest、显式事务、视图数据源、时间字面量的稳定报错；
- VACUUM 推进 floor；
- JOIN / 子查询的多目标点拒绝。

全量基线见 [`docs/roadmap.md`](roadmap.md)。

## 后续设计问题

1. 持久提交账本放 meta 页、目录 JSON 还是独立文件？独立文件更容易原子替换，
   但复制/备份要一起带走。
2. 保留水位默认多长？按 LSN 数、WAL 大小还是小时？
3. 是否允许 `AS OF SYSTEM TIME '...'` 由时间映射到最近提交 LSN？内部仍以
   LSN 为准，避免时钟回拨。
4. 索引历史读是否值得做：要么保留历史索引项，要么只在目标点等于 latest 时
   允许索引并回堆复核。
