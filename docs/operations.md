# LumenDB 运维手册（单写者 + fence/租约 部署）

这份文档只讲**部署与排障**，不讲实现。实现细节见 `architecture.md`，
验证方式见 `README.md` 的"怎么验证它真的能跑"。

先说清边界：LumenDB 现在的复制是"物理 WAL 流 + 多数派水位 + 可选同步复制"，
**没有 Raft 级日志协商**。它适合"单写者 + 一个共享目录（fence/租约文件）"的部署，
不适合跨公网、跨不可靠文件系统的多写场景。

---

## 1. 部署前必须满足的四条

1. **同一时刻只有一个可写主库**。写并发由单写者事务模型保证；跨节点靠 fence 挡住。
2. **所有节点能看到同一个 fence 文件与租约文件**（同一台机器、挂载盘、或语义正确的
   共享卷）。放在 NFS/SMB 上要确认 `rename` 是原子的——本项目用的是
   `os.replace` + `fsync`。
3. **各节点时钟偏移 < `lease_ttl / 3`**。租约用墙钟比较 `expires_at`，偏移会直接
   缩小安全窗口。没有 NTP 的机器不要开租约。
4. **副本的 `replica.lsn` 目录可写**，主库数据目录独占锁可用（第二个进程打开会被拒）。

---

## 2. 首次拉起（顺序很重要）

```
# 1) 主库：先读槽位，再开库（保留点必须在恢复和自动 checkpoint 之前生效）
slot = read_slot(slot_path)                       # 可选的复制槽
db   = Database(path, wal_retain_lsn=slot)
# 2) 主库：装上 fence（旧主库靠它停写）
db   = Database(path, write_fence=PromotionFence(fence_path, "primary").assert_writable)
# 3) 副本：基准备份
Replica.bootstrap_from_server(host, port, replica_dir)
# 4) 副本：把自己的复制进度登记到仲裁节点（水位靠它算）
replica.attach_quorum(quorum_node)
```

`attach_quorum` 之后，副本每次 apply 推进都会 `observe_applied(lsn)`；leader 的
心跳响应会带上这个值，水位才能前进。**漏掉这一步 → 水位永远是 0 → 提升会被闸门
拒绝，同步复制会一直超时。**

---

## 3. 提升（failover）

### 3.1 手动提升

```python
replica.promote_with_quorum(
    quorum,                     # 候选自己的 QuorumNode
    fence_path=fence_path,      # 共享 fence：写下去旧主库立刻拒写
    lease_path=lease_path,      # 写租约：TTL 内约束旧主
    lease_ttl=10.0,
    lease_keeper_interval=None, # 默认自动用 ttl/3
    heartbeat_interval=0.5,     # 任期视图 + 水位推进
    sync_replication=True,      # 可选：多数派追上才回 COMMIT
    sync_timeout=2.0,
)
```

提升前会依次检查：多数派投票 → 候选 `last_lsn >= quorum_commit_lsn`（水位闸门）
→ 拿租约 → 写 fence → 切读写 → 起 keeper/心跳（可选装提交屏障）。

### 3.2 自动提升

```python
ReplicationMonitor(
    replica, host, port,
    quorum=node, fence_path=fence, lease_path=lease,
    heartbeat_interval=0.5, max_failures=3,
).start()
```

监控器有显式状态机：`following / promoting / promoted / read_only / needs_attention`。
`lease_loss_stepdown=True` 时，租约丢失触发的 `step_down()` 会把状态扣回
`needs_attention`——**看到 `needs_attention` 就不要再往这个节点写**。

---

## 4. 故障剧本

### 4.1 写语句报 `40001`（另一个事务正在写）

单写者模型下这是**正常背压**，不是错误：另一个事务持有写者锁。
处理：等它 COMMIT/ROLLBACK 后重试。持续出现说明有长事务（检查 `txns.active_xids`）。

### 4.2 写语句报 `40003`（CommitOutcomeUnknown）

只会在开了 `sync_replication` 时出现。含义是**结果未知**：

- 数据已经本地 fsync，可能已生效；
- 不要当成失败回滚，先 `SELECT` 确认，再决定是否重试（重试要幂等）；
- 大概率原因：副本掉线 / 心跳循环没跑 / 水位停滞。按 §5 的顺序查。

### 4.3 写语句报 "节点已被提升副本 fence"

旧主库发现了新主的 fence marker。它是**不可自动恢复**的：旧主库必须人工确认
拓扑、把数据目录当只读观察者或重新 bootstrap。不要删 marker 了事。

### 4.4 写语句报 "写租约已过期 / 没有有效写租约"

主库的租约 TTL 过了没人续。检查：

1. `LeaseKeeper` 是否在跑（`replica._lease_keeper` 非空）；
2. 给了 `lease_path` 但没给 `lease_keeper_interval` 时，默认会自动起 keeper；
   如果显式 `lease_keeper_auto=False`，那这就是预期的护栏；
3. 续租走 quorum 时（`lease_renew_with_quorum=True`），多数派心跳是否够数。

### 4.5 提升被拒："候选落后于多数派已提交水位"

副本没追上。先让它追平（`replicate_from_server` 循环到 `lag_lsn == 0`），
或做一次页级增量基准备份（`rebase_from_server`）。**不要绕过闸门**：
它挡的正是"提升落后副本导致丢已提交数据"。

### 4.6 提升被拒："多数派投票失败"

达不到 `majority`。查 `QuorumClient.status()` 看各节点的 `term` / `voted_for`；
常见原因是某节点任期更高（说明发生过一次未完成的选举），或网络不通。

---

## 5. 排障顺序（水位为什么不前进）

1. 对每个仲裁节点调 `QuorumClient.status(host, port)`：
   - `match_lsn` 是否在推进？停在某个值 → 副本没在 apply；
   - `commit_lsn` 是水位结果，落后于 `match_lsn` 说明多数派没凑齐（有节点掉线）；
   - `last_heartbeat_from` 是否还是旧主？→ 旧主还在发心跳，说明它没意识到自己该停。
2. 副本侧看 `replica.lsn` 文件是否增长；不增长就看 `replicate_from_server` 是否报错
   （槽位被回收会报错，需要 `rebase_from_server`）。
3. 主库侧看 WAL 是否在涨（`wal.next_lsn`）——主库不写，水位当然不动。
4. 最后看租约文件 `expires_at` 与 fence marker 的 `promoted_node`。

---

## 6. 已知限制（交付时必须说明）

| 限制 | 影响 | 缓解 |
|---|---|---|
| 没有 Raft 日志协商 | 长期失联的旧 leader 复辟后仍可能写入"多数派没有"的记录 | 共享 fence + 租约；不要让旧主库重新可写 |
| 写并发度为 1 | 写事务串行；读不受影响 | 需要更高写并发要接行级锁（见 `architecture.md` §2.5） |
| 提升会丢掉主库未发送的尾段 | 未同步复制的写入可能丢失 | 开 `sync_replication`，或提升前确认 `lag_lsn == 0` |
| 租约用墙钟 | 时钟漂移缩小安全窗口 | 强制 NTP，偏移 < `ttl/3` |
| 旧 WAL 记录没有 CRC | 升级前写入的历史记录仍可能把介质位翻转当成合法记录 | 新写入记录自带 CRC32；完整记录校验失败会拒绝恢复而不是静默截尾 |
| 基准备份是全量拷贝 | 大库首次同步慢 | 页级增量备份（`rebase_from_server`） |
