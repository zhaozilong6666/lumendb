# AS OF SYSTEM TIME 设计（未实现）

本文是 ① 的设计边界，不是已交付能力。P0 的第一块只读提交账本已经落地：
`SHOW COMMIT LOG` 暴露当前 WAL 窗口的 `xid / commit_lsn`，但它还不是完整
历史，也没有驱动任何 `AS OF` 读。当前只有目录版本链：
`SHOW ... AT CATALOG VERSION n` / `AT TIME '...'` 能读历史 **schema 与列统计**，
但数据页仍只有当前版本；VACUUM 也会回收旧行版本。因此不能把
`SHOW ... AT TIME` 当成 `AS OF SYSTEM TIME`。

## 目标语法（候选）

```sql
SELECT * FROM t AS OF SYSTEM TIME '2026-11-26T08:00:00+00:00';
SELECT * FROM t AS OF SYSTEM TIME 123456;  -- commit LSN，二选一后再定
```

只允许只读查询。`INSERT/UPDATE/DELETE/DDL`、显式事务内的 `AS OF`、
`AS OF` 与 `FOR UPDATE` 组合都应报错，而不是静默退化成当前读。

## 必须先成立的不变量

1. **提交点可定位**：每个提交事务都有持久、单调的提交标识（UTC 时间或
   COMMIT 记录的 LSN）。仅靠 xid 不够：xid 分配顺序不等于提交顺序。
2. **旧版本有保留期**：VACUUM 只能回收“保留水位之前就已删除”的版本；
   否则 `AS OF` 只能碰运气读到还没被清理的旧行。
3. **可见性按提交点判断**：`xmin` 在目标点之前提交才可见；`xmax` 在目标点
   之前提交才表示删除。回滚/未提交/目标点之后才提交的 `xmax` 都不能隐藏行。
4. **索引不作历史真值**：B+ 树项只指向 RID，历史可见性必须回到堆页复核；
   不能因为索引里还有键就认为该行在目标时刻存在。
5. **schema 与数据同一快照**：目标时刻的目录版本给出列布局；数据页必须按
   该版本的 `TableDef` 解码，不能拿当前 schema 解旧页。

## 分阶段落地

### P0 提交账本

- 在 COMMIT 记录里带上提交时间（或在 WAL 头部维护单调 commit LSN 映射）。
- `TransactionManager` 维护 `xid -> commit_lsn/commit_time`，恢复时从 WAL
  重建；checkpoint 截断 WAL 后仍要保留最近窗口的映射。
- 暴露最小只读视图，例如 `SHOW COMMIT LOG`，用于验证账本与测试断言。
- 验收：重启前后同一 xid 的提交点一致；未提交/回滚事务不在账本里。

### P1 历史可见性

- 已落地第一切片：`HeapFile.scan_at(commit_lsns, target_lsn)` 按提交 LSN
  截止点判断版本，不伪造 `Transaction`；默认 `scan` 路径不变。
- 账本里没有的 xid（未提交/回滚/已裁剪）按不可见处理；目标点后才提交的
  `xmax` 不会提前隐藏旧行。
- 仍未接 SQL 语法、索引扫描和 JOIN；覆盖索引暂不支持历史读。
- 验收：同一行 INSERT → UPDATE → DELETE 的三个时间点分别读到不存在、
  旧值、新值、不存在；已由 `tests/unit/test_heap.py` 钉住。

### P2 保留水位

- 维护 `history_retention_floor`（时间或 LSN），VACUUM 以它为下限。
- `AS OF` 早于水位时报 `BinderError`/`FeatureNotSupported`，并提示可用窗口；
  不允许悄悄返回不完整结果。
- `VACUUM` 仍可在线执行，但只能清理 `xmax` 提交点早于水位的版本。
- 验收：水位前/后各一个删除版本；VACUUM 后水位内仍可读，水位外明确报错。

### P3 SQL 表面

- parser 支持 `AS OF SYSTEM TIME <literal>`，只挂在 `SELECT`/`EXPLAIN` 上。
- binder 解析目标点、选目录版本、构造历史快照；写语句与显式事务拒绝。
- `EXPLAIN` 标注 `as_of=...` 与使用的目录版本，避免用户误以为当前读。
- 验收：非法时间、未来时间、早于水位、写语句组合各有稳定错误码。

### P4 测试与重放

- 确定性重放增加 `asof` 场景：固定种子下交替 INSERT/UPDATE/DELETE，
  在多个目标点读，轨迹必须可复现。
- 崩溃恢复测试：提交账本落盘后重启，目标点读数不变。
- 并发测试：`AS OF` 读与在线 VACUUM/DDL 的交错必须有明确门禁，不能靠
  “读得快”碰运气。

## 明确不承诺

- 不复制 PostgreSQL 的 `pg_xact` / `txid_current_snapshot` 语义。
- 不支持写入、DDL、序列/自增回滚、外部副作用的时间倒流。
- 不承诺无限制历史；窗口由保留水位决定，超出必须报错。
- 时钟回拨时以持久提交标识为准，不以墙上时钟排序。

## 开放问题

1. 目标点用 UTC 时间还是 commit LSN？时间对用户友好，LSN 更抗时钟回拨；
   可以内部用 LSN、对外同时接受时间并映射。
2. 保留水位默认多长？按时间（例如 1 小时）还是按 WAL 大小/版本数？
3. checkpoint 截断 WAL 后，提交账本放 meta、目录 JSON 还是独立文件？
4. 历史读是否允许 `JOIN` 多表？允许，但所有表必须绑定同一目标点与同一
   目录版本，不能一张表当前、一张表历史。
