# 副本读一致性：事务级应用单元 + `snapshot_read`（2026-11-17）

## 结论先说

副本的**原子应用单元是事务**：`apply` 的组边界只落在 COMMIT/ABORT 之后，
服务端 `fetch` 返回的批也截断在事务边界上；单个事务超出 `max_records` 时，
副本把这段"半个事务"暂存起来，等边界到达再整体应用。所以同一个已应用前缀内，
顺序扫描与索引扫描看到的是同一份数据，不会再出现"表页已应用、索引页未应用"。

需要**跨语句**一致视图时用 `snapshot_read()`（下面的 API）。

## 曾经的缺口（已修）

一条 SQL 的 DML 会拆成**多条 WAL 记录**（表页一条、每个索引页各一条）。
`Replica.apply` 早期为了不让长批饿死读者，按 `_apply_chunk_size`（默认 512）
**按记录数**分组、组间释放数据锁，于是组边界可能落在"表页已应用、索引页还没
应用"的位置：

- 顺序扫描能看到新行、索引扫描看不到 → 同一个前缀里两条访问路径对不上；
- 极端时读者读到页头计数与槽位数组打架的半个页（`BTreeInvariantError`）。

### 复现（修复前的对照实验）

`replica._apply_chunk_size = 1`、400 行插入 + 一个二级索引、一线程 apply、
一线程用 `snapshot_read` 固定前缀反复对比两种 COUNT：

```
修复前（eb3489d）：不一致 7 次，样本 [(51, 50), (51, 50), ...]
                  BTreeInvariantError: 叶子页 6 的槽位 50 偏移越界：0
修复后：          不一致 0 次
```

样本 `(a, b)` 里 `a` 是顺序扫描、`b` 是索引扫描：**`a > b` 才是真 bug**
（索引落后于表页）；`a < b` 只是两次独立查询之间前缀合法前进，不算错位。
早期用两次独立 `execute_one` 对比时统计出的 197 次，绝大部分属于后者这种
假阳性——所以判据必须固定在**同一个前缀**上。

探针：`D:\projects\lumendb-review\_probe_snapshot.py`（`PYTHONPATH` 指向仓库后运行）。
回归测试：`tests/unit/test_replica_txn_boundary.py`。

## 修法（两侧都要对齐，只对齐一侧无效）

只在一侧做"6 找最近的 COMMIT"不成立：组起点由上一组决定，且**一个事务可能
跨两个 fetch 批**，批尾本身就会切在事务中间。

1. **服务端**（`wal_stream.py::_handle_replicate`）：先取 `max_records` 条，
   再回退到批内**最后一个** COMMIT/ABORT 之后发出。批因此总以事务边界结尾。
2. **副本**（`replica.py::apply` → `_transaction_cut`）：组切点优先落在
   `chunk` 窗口内最后一个 COMMIT/ABORT 之后；窗口内没有边界就**向后延伸**到
   最近的边界（绝不把一个事务切成两半）；整批都没有边界时退回批尾。

### 超长事务：副本侧暂存，不降级

`max_records` 是有界的，但一个事务可以更大（一次 300 行插入带二级索引就有
几百条"表页 + 各索引页"记录）。服务端截不到不存在的边界，只能整批发出去；
如果副本照着批尾切组，就会重新出现撕裂窗口。

现在副本侧有**半事务暂存区**（`Replica._pending_records`）：

- 一批里没有 COMMIT/ABORT → **一条都不应用**，全部暂存；
- 下一批到达后与暂存区拼接，找到边界再一起应用；
- 拉取游标用 `max(applied_lsn, pending_end_lsn)`（`_fetch_cursor`）——
  否则第一批全在事务中间、`applied_lsn` 不动，会永远重拉同一段；
- 暂存上限 `_max_pending_records`（默认 10 万条）。超过就抛 `StorageError`：
  宁可让复制停住等人处理，也不退化成"应用半个事务"。

代价是暂存期间副本的可见状态停在事务前（这是正确性要求的），内存占用等于
该事务的 WAL 记录数。验收：`tests/unit/test_replica_partial_transaction.py`
用一个 300 行、带二级索引的显式事务 + `max_records=40` 跑完全程，断言读者
看到的 `COUNT` **只能是 0 或 300**，并检查索引不变量与条目数；测试里同时
确认出现过"整批都在事务中间"的批次（否则说明没覆盖到这条路径）。

## `snapshot_read()`：跨语句一致视图

```python
with replica.snapshot_read():
    a = replica.execute_one("SELECT COUNT(*) FROM a")
    b = replica.execute_one("SELECT COUNT(*) FROM b")
```

上下文期间持有数据锁，`apply` 把剩余记录一次应用完，两次查询看到同一个前缀。
代价是这段时间副本 apply 停摆——只用于确实需要跨语句一致性的短读。
不可重入计数（同一线程可嵌套，退出要一一对应），异常退出也会把计数还回去。
测试：`tests/unit/test_replica_snapshot_read.py`。

## 仍然没有的东西

**索引项 MVCC**。副本侧的事务级 apply 解决了"读者看到半个事务"，但写并发
第 2 步（行/键锁）的三条阻塞还在（见 `docs/concurrency.md`）：
结构变化时仍写同一批目录字节、唯一索引项在 DELETE 时被物理删除、
物理 undo 不按事务区分字节。这三条与"索引结构与表数据不是一个原子单元"
同源，靠索引项 MVCC 或逻辑 undo 才能消。
