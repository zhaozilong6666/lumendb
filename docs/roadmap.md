# LumenDB 路线图

LumenDB 是一个长期项目。路线图按“基础设施 → 优化器 → 存储/复制”分层，
每一层都必须先具备可验证的输入、明确的完成标准和回退路径，避免出现功能
已经宣布完成、但下游依赖仍然悬空的情况。

## 当前基线

- 全量测试：1685 passed（默认串行模式）；`LUMEN_CONCURRENT_DML=1` 为
  1683 通过 + 2 skipped（并发写模式的显式事务 VACUUM 回滚被明确拒绝，
  对应用例按模式跳过）。故障注入覆盖 fsync/WAL 追加/页写/torn/ENOSPC，见
  [`docs/faults.md`](faults.md)。
- DML 单列等值条件已索引下推：8 线程 ×1000 行 + 800 次热点自增，默认热点
  34.40s → 0.81s（23.3 → 987.8 ops/s），并发 49.97s → 0.82s（16.0 → 976.1
  ops/s），最终 `final_rows=8001`、`hot_value=800`。
- 质量门禁：`python scripts/gates.py` 跑全量测试、变异清单有效性、账本 drift
  与账本证据校验。证据校验用只读的 `verify --check`：`failed` 才红；
  `unchecked/manual/external` 记为 partial，CI 页面 URL 不会因为“这台机器
  读不到 GitHub”被误判成假事实。
- 并发 DML 实验模式已落地（`Database(concurrent_dml=True)`，默认关闭）：
  DML 走 `read_guard` + 行/键锁 + 页 latch，目录保存短锁，索引根页跨语句
  共享（`root_cell` + `Catalog.index_root_lock`），回滚走逻辑路径（WAL
  `no_undo` 哨兵 + VACUUM 回收），DDL 仍物理可回滚；保存点事务在并发模式下
  默认走行级逻辑 undo，堆 INSERT/UPDATE/DELETE 改记逆操作闭包，保存点存续期间
  其它写者可提交；`--physical-savepoints` 退回排空在途语句后独占写者锁。
  跨会话 DDL 会先拒绝仍有行 undo、未提交写入或旧快照的事务；逻辑保存点的
  晚升级路径也走同一套检查，避免绕过 DDL 门。
  详见 `docs/concurrency.md`。
- 代价模型基础设施：`plan/cost.py` 已提供扫描/过滤/连接/投影/聚合/排序/
  集合/`LIMIT` 的基数与 CPU 估算，并已接入 `EXPLAIN` 的 `rows≈/cpu≈`
  标注；已接入 3–8 个逗号连接源的左深动态规划。
- 组提交已落地：WAL 追加/刷盘线程安全，提交 leader 合并刷盘，单提交语义
  不变。
- 自动 checkpoint 已落地：WAL 增长超过 `checkpoint_wal_bytes` 或累计提交数
  达到 `checkpoint_every_commits` 时刷脏页、写 CHECKPOINT 并前移
  `checkpoint_lsn`；手动 `CHECKPOINT` 保留。
- WAL 分段回收已落地：新库使用分段 WAL，checkpoint 后按安全 LSN 删除旧段，
  活跃事务的 BEGIN 之前的历史不会被回收。
- 物理复制垂直切片已落地：新页首次修改时写 `PAGE_IMAGE`，主库通过 TCP
  `WALServer` 提供基准备份和物理 WAL 流，`Replica` 按 LSN 重放页更新与事务
  状态，拒绝写入，并支持手动提升。
- 外键主线已闭环：引用完整性、级联动作、DDL、延迟约束、`NOT VALID` /
  `VALIDATE`、`TRUNCATE` 依赖、依赖删除 `CASCADE`。
- SQL 查询：相关子查询（单层/多层）、FROM 派生表、列别名列表、派生表内部
  `WITH ... SELECT`。
- 优化器仍按 SQL 书写顺序构造连接，`WHERE` 在连接之后过滤；没有基于代价的
  连接顺序枚举。

## 分层路线

### 第 1 层：优化器基础设施（完成）

目标：在不改变结果语义和输出顺序的前提下，先建立统计与代价模型。

1. **基数估算**
   - 基表：使用 `TableDef.row_count_hint`；空表或统计缺失时回退到默认值。
   - 过滤：等值、范围、其他谓词分别使用可配置选择率。
   - 连接：按等值键数量计算选择率，结果下限为 1 行。
   - 派生表/CTE：暂时使用默认基数，后续可接入 `ANALYZE` 统计。
2. **代价模型**
   - 嵌套循环：`left_rows * right_rows`。
   - 哈希连接：`left_rows + right_rows`，并区分 build side。
   - 输出 `rows / cpu / total` 三个量，供 EXPLAIN 和后续枚举使用。
3. **回归护栏**
   - 代价模型独立单元测试，不依赖执行器。
   - 任何改变连接顺序的提交必须同时验证：结果集（含重复行）、`ORDER BY`
     稳定性、`NULL` 语义、外连接语义。
   - 暂不改变现有执行顺序，避免在缺少护栏时引入结果顺序回归。

完成标准：`plan/cost.py` 有稳定 API 和测试；EXPLAIN 已输出估算行数、CPU
与置信度（本次完成）。

### 第 2 层：连接顺序与谓词下推（MVP 已完成）

前置条件：第 1 层完成，且存在结果顺序回归测试。

1. 只对纯 `INNER JOIN` / 逗号连接枚举顺序；`LEFT` / `RIGHT` / `FULL` 保持
   原顺序。
   - **已完成（逗号连接 + 纯 INNER 显式链）**：3–8 个源用左深动态规划
     选择顺序；纯 `INNER` 的显式 `JOIN` 链会先展开为源集合再枚举，外连接/
     混合 `JOIN` 仍保持原序。
2. 从 `WHERE` 中提取跨数据源的等值谓词，转成连接键；其余谓词下推或保留在
   `Filter`。
   - **已完成（等值键 + 单源下推）**：跨源 `列 = 列` 附加到 `Join` 的哈希键；
     只引用一个数据源的合取项会下推到该扫描之上（`Filter`），完整 `WHERE`
     仍保留为最终兜底 `Filter`，残余谓词不会漏判。显式外连接/混合 JOIN 不做
     下推。
3. 用动态规划枚举左深连接树，代价取 `rows + cpu` 加权和；源数量超过阈值
   （例如 8）时回退原序，避免指数爆炸。
   - **已完成（DP 版）**：在 3–8 个源内对“已选源集合”做左深动态规划，
     代价取 `rows + cpu`；每一步优先选择有连接键的源。INNER 哈希连接会按
     估算选择较小的 build side；显式 `JOIN`、外连接和超过 8 源回退原序。
4. 输出顺序变化必须由测试显式接受；需要稳定顺序时由 `ORDER BY` 保证。
   - **已完成**：重排后用顶层 `Project` 还原 SQL 书写顺序的输出列；无
     `ORDER BY` 时行顺序仍不承诺稳定。

完成标准：TPC-H 风格的小型固定数据集上，连接顺序与代价符合预期；全量测试
通过；EXPLAIN 能显示估算行数与选择理由。

**验收补充**：`tests/unit/test_optimizer_acceptance.py` 用三表固定数据集锁定
“SQL 书写顺序不是最优时重排、输出列还原、`NULL` 行保留”；EXPLAIN 现在同时
输出 DP 搜索过的状态数 `[candidates=N]`。逗号连接与纯 INNER 显式链均覆盖，
单源谓词下推已实现；外连接仍不下推。

### 第 3 层：存储耐久性

1. **组提交**：把同一刷盘窗口内的多个提交合并为一次 `fsync`；单提交语义
   不变，崩溃后仍只能看到完整提交。
   - **已完成**：`WAL` 追加/刷盘加锁；`TransactionManager` 用提交队列和
     单 leader，leader 一次刷盘覆盖多个目标 LSN，其余提交等到自己的
     COMMIT 记录持久化后才变为 `COMMITTED`。并发提交测试断言刷盘次数少于
     提交数，崩溃恢复测试仍通过。
2. **WAL 分段回收**：checkpoint 之后轮转 WAL，保留基础 LSN，恢复从
   `checkpoint_lsn` 之后开始；旧段在确认无引用后删除。
   - **已完成**：新库默认使用 `wal-<first_lsn>.log` 分段 WAL；checkpoint 后
     只删除“最后一条记录 LSN 小于安全 LSN”的整段。安全 LSN 取活跃事务的
     最老 BEGIN LSN；没有活跃事务时取 `checkpoint_lsn`。旧单文件库继续走
     兼容路径。
3. **CHECKPOINT 策略**：按 WAL 大小或提交数触发，保留手动 `CHECKPOINT`。
   - **已完成**：`Database(checkpoint_wal_bytes=...)` 在每次提交后检查 WAL
     增长，超过阈值自动调用 `RecoveryManager.checkpoint`；
     `Database(checkpoint_every_commits=...)` 按累计提交数触发，二者可单独
     或同时启用，0 表示关闭，手动 `CHECKPOINT` 保留。

完成标准：崩溃注入测试覆盖“提交后、checkpoint 中、WAL 轮转后”三个时点；
恢复结果与未崩溃基线一致。

- **已完成**：崩溃测试覆盖已提交事务、checkpoint 刷脏页后崩溃、checkpoint
  元数据写入失败、活跃事务 checkpoint 后崩溃、WAL 轮转后崩溃；恢复结果与
  未崩溃基线一致。第 3 层可进入第 4 层。

### 第 4 层：复制与只读副本（进行中）

前置条件：第 3 层完成，WAL 具备稳定的段边界与基础 LSN。

1. 物理日志流：主库发送 WAL 记录，副本按 LSN 重放。
   - **已完成（垂直切片）**：`PAGE_IMAGE` 记录在新页第一次修改时携带完整
     页镜像，解决“页分配/初始化不单独写 WAL”导致的副本缺页；`WALServer`
     用换行 JSON（二进制 base64）提供 `backup` 与 `replicate` 请求，`Replica`
     按 LSN 递增重放 `PAGE_IMAGE` / `UPDATE` / `CLR` / `BEGIN` / `COMMIT` /
     `ABORT`。主库 WAL 的 `retain_lsn` 会按副本请求起点保留记录；`WALServer`
     可配置 `slot_path`，把副本 ack 的 `confirmed_lsn` 原子持久化，并在服务
     重启时恢复保留点。若副本落后于已回收段，明确报错要求重新基准备份。
     `status` 请求返回主库 `last_lsn`、WAL `base_lsn` 和槽位 `confirmed_lsn`，
     `Replica.lag_from_server` 用它计算网络延迟。
2. 只读副本：副本拒绝写入，提供快照读；延迟以 LSN 衡量。
   - **已完成（垂直切片）**：`Database(read_only=True)` 在 `_run` 入口拒绝
     非白名单语句；`Replica.lag_lsn` 返回待应用字节数；副本 LSN 以
     `replica.lsn` sidecar 原子落盘，重启从 marker 继续。
3. 故障切换：至少支持手动提升，明确数据丢失窗口。
   - **已完成（手动提升）**：`Replica.promote()` 关闭只读句柄并以读写模式
     重开；测试覆盖“主库关闭后副本提升并继续写入”。未复制的主库尾段会
     丢失，`test_promote_keeps_only_replicated_prefix` 明确了这一前缀语义。

**基准备份写冻结**：`Database.snapshot()` 持有写路径的 `RLock`，要求无活跃
事务，先 checkpoint 再读数据文件；新写语句被挡在锁外，读查询仍可继续。
`WALServer.backup` 和 `Replica.bootstrap` 都走这条路径。

**手动提升 fencing**：`Replica.promote(fence_path=..., node_id="replica")`
先原子写提升 marker；旧主库用 `Database(write_fence=PromotionFence(...).assert_writable)`
配置写回调，在 marker 指向别的节点时拒绝写语句。它依赖共享文件，不是自动
选举或网络租约。

**自动提升 MVP**：`ReplicationMonitor` 每轮先检查 `status` 再拉一批 WAL；
连续网络失败达到 `max_failures` 后调用同一个 `promote(fence_path=...)`。
逻辑错误（槽位已回收）先尝试 `rebase_from_server` 页级增量重建，成功
返回 `rebuilt`，失败才返回 `needs_attention`，不会自动提升。
`ReplicationMonitor.start()` 可在后台线程按 `poll_interval` 周期跟随，
`stop()` 停止；`promoted` / `needs_attention` 会让后台循环退出。heartbeat RPC、
多数派续租和 `LeaseKeeper` 已做 MVP；客户端重定向已做 MVP
（`WALServer(redirect_to=...)` / `WALClient(follow_redirects=True)`），
网络分区下仍要求共享 fence。

**心跳超时驱动选举**：`ReplicationMonitor(election_timeout=...)` 在副本挂
quorum 时启动 `QuorumNode` 选举看门狗——超过 `election_timeout` 收不到
leader 心跳（或从未收到）就自动 campaign，覆盖"复制流连接半开挂住、
status 不失败但 leader 已死"的场景。看门狗每轮等待带 ±35% 抖动，
多个 follower 不会永远同票瓜分；提升成功后立刻停看门狗（leader 收不到
别人的心跳，不能自我逼停）；未显式配置 `heartbeat_interval` 时用选举
超时的一半自动维持心跳。竞选失败分两类：票不够 / fence 已有更高提升
LSN 回 FOLLOWING、下个超时重试（输家被赢家心跳喂住自然退出）；持久化
/闸门类失败才 NEEDS_ATTENTION。

**多数派投票 MVP**：`QuorumNode.campaign()` 递增任期，向 `peers` 发送
`vote_request`，只有拿到集群多数票才写提升 fence；投票节点持久化
`term/voted_for/last_lsn`，会拒绝落后于自己 LSN 的候选，也会拒绝旧任期。
`Replica.promote_with_quorum()` 把投票和切换读写串起来，投票失败时副本保持
只读。`ReplicationMonitor(quorum=...)` 的自动提升也走这条路径；没有多数票时
返回 `needs_attention`，不会退化成单机提升。传入 `lease_path` / `lease_ttl` /
`lease_keeper_interval` 时，监控器会把租约和写围栏一并带到自动提升后的主库，
避免“自动提升成功但新主库没有租约围栏”的接线缺口。这不是完整 Raft：已有
基础 heartbeat RPC、独立 `HeartbeatLoop` 与多数派续租，也有 prefix-checked
`append_entries` MVP；`promote_with_quorum` 的心跳循环每轮会通过
`push_entries_to_peers` 推一批记录（一轮最多 10 批加速追平）。逐条
per-entry term 已做：`VoteState.log_terms` 按 LSN 记录任期分段，
`append_entries(entry_terms=...)` 重放成功后逐条落段，拼接点
`(prev_lsn, prev_term)` 与本地任期不一致时拒绝并要求重建
（`rebuild_required`，原因记入 `Replica.last_push_rejections`），
基准备份流不带任期、follower 无任期知识时首次接触信任 leader。
自动重建闭环已做：leader 把 `rebuild_from` 基准备份端点随请求下发，
follower 分叉时经 `QuorumNode.rebuild_callback` 后台重新基准备份并
`reset_progress` 对齐 match/清空旧任期分段，重建后下一轮心跳自动
恢复推送；拒绝原因变化会记 warning，`ReplicationMonitor` 可读
`last_push_rejections` 快照。预投票不再检查 `voted_for`（那是当前任期的
真实投票，检查会让故障切换永远过不了预投票）；leader 心跳携带
`last_term`，基准备份接入的 follower 靠它补任期知识，否则竞选时
`last_term=0` 会被有任期历史的节点拒绝、永远选不上。双候选同窗竞选
已收敛：心跳轮检测到本节点的仲裁节点改投别人（`voted_for != 自己`）
时旧主库立即降级，不留双主窗口；`fence_path` 在无租约时也默认把
`assert_writable` 接进写路径，新主库提升后旧主库下一笔写即被拒。
仍没有分歧尾巴截断（重建是唯一修复手段）和完整 commit index 协商；
重建路径已是**增量优先 + 全量回退**：`rebase_from_server` 页级增量失败
（本地页损坏 / 主库已回收旧页）时自动 `rebuild_from_scratch` 全量重拉，
旧数据文件改名到 `broken-<时间戳>/` 保留取证、不删除，重建后仍是只读副本。
网络分区时多数派不可达会明确失败，双写防护靠共享 fence + TTL 租约
两层兜底，不是完整的多数派写确认。

**重启双写防护（fence/lease 打开即检）**：fence 与租约此前只接在运行中
的写路径上，旧主库被 fence 后若进程崩溃重启，重开的裸 `Database` 不知道
marker 的存在，恢复写入即成双主。现在 `Database(fence_path=..., node_id=...)`
/ `Database(lease_path=...)` 在非只读打开时立即校验共享 marker（fence 指向
别的节点拒绝打开；租约无主/他人持有/已过期一律拒绝，旧主库必须重新
`acquire` 才能恢复写），并在调用方未提供更强围栏时把检查持续接进写路径
（打开后别人才提升的场景在下一笔写上被拦）。只读打开放行——被废黜的旧
主库仍可当只读观察者。`Replica.promote` 重开可写库时透传两个路径，
提升后的新主库重启同样受自己写的 marker 保护。follower 落后到 leader
WAL 起点之前时，leader 的 `push_entries_to_peers` 会主动下发
`rebuild_hint`（提升后新 WAL 从 applied 边界开始，`base_lsn` 不可用，
用 leader 的 applied 位置判断缺口），follower 收到后跳过 prefix 校验
直接走基准备份自愈，落后副本不再停在原地干等（端到端验收
`test_lagging_follower_gap_triggers_auto_rebuild`）。

**写租约 MVP**：`Lease(path, node_id, ttl=...)` 把持有者、任期和到期时间
原子写入共享文件。主库每笔写语句前通过 `Database(write_fence=lease.assert_writable)`
检查租约，超过 TTL 没有 `renew` 就自行停写；候选节点必须等旧租约过期才能
`acquire`。`QuorumNode.campaign(lease_path=...)` 在多数票之后获取租约，
`Replica.promote_with_quorum(lease_path=...)` 把租约接到提升路径。它依赖
共享文件和大致同步的墙钟，不是完整的 quorum heartbeat lease；时钟漂移会
改变安全窗口。`QuorumNode.heartbeat_round()` 向 peers 发送 `heartbeat`
RPC，`renew_lease()` 只有拿到多数派确认才续租；`LeaseKeeper(renew=...)`
可把续租切换到 quorum 路径。`Lease` 用 `<lease>.lock` 串行化 acquire/renew/release，
`LeaseKeeper` 在后台按 TTL 比例续租；进程崩溃留下的锁文件按失败关闭处理。
`Replica.promote_with_quorum(..., lease_keeper_interval=...)` 可在提升后
自动启动 keeper，`Replica.close()` 会先停止续租再关闭数据库句柄。
提升时还会把 `LeaseFence` 装到新主库的 `write_fence`，租约被释放、过期，
或本节点观察到更高任期后写语句立即失败；`lease_loss_callback` 可接收
keeper 续租失败通知；`lease_loss_stepdown=True` 时 keeper 失败会直接
`step_down()` 切回只读；keeper 的本地续租也把当前任期传给 `Lease.renew()`，
任期落后时续租失败，不会继续持有旧任期租约。`heartbeat_round()` 收到更高任期响应会推进本地
任期并清空投票，为旧主库自动停写提供任期依据；独立 `HeartbeatLoop` 可在后台
周期发送心跳并在观察到更高任期时回调上层停写；回调异常只记录到 `last_callback_error`，不会终止心跳线程。

**尚未完成，不能把第 4 层宣布为整体完成**：

- 自动故障切换的 **MVP 已做**（`ReplicationMonitor` 连续网络失败后自动
  `promote` + 共享 fence），客户端重定向 MVP 已做（`WALServer(redirect_to=...)` + `RedirectError`，
  `ReplicationMonitor` 会跟随最多 3 次），多数派投票和 TTL 写租约 MVP 已做
  （`QuorumNode.campaign()` / `Replica.promote_with_quorum()` / `Lease`），但
  完整 leader 选举、日志复制协商和网络分区下的双写防护仍未完成。
- 复制流认证/TLS、压缩、二进制帧、长连接、断点续传与真实网络故障注入。
  - **长连接复用已做**：`WALClient(keepalive=True)` /
    `Replica.replicate_from_server(keepalive=True)`；服务端 `_handle` 按请求
    循环，同一连接可混用压缩与二进制帧，空闲超时回收，断连由客户端丢弃重连。
    本机回环收益可忽略，跨网络追平时省掉每批一个 RTT。**应用层心跳已做**：
    `WALClient.ping()` 发一条真实往返（`ping`/`pong`，pong 带当前 LSN），
    对端没响应抛 `StorageError` 而不是像 `status()` 那样返回全 0。写这条测试
    时撞出并修掉一个真实缺口：`WALServer.stop()` 此前只关 listen socket 和
    join 处理线程，**已建立的长连接仍在应答**——"服务停了但还能连上"，探活
    会被骗；现在服务端记录活跃连接并在 `stop()` 里逐个 shutdown+close。
  - **二进制记录帧已做**（`binary_records=True`）：WAL 记录走 `B` + 定长头 +
    原始 payload，8,124 条记录的流体积 1,634,288 B → 863,144 B（1.89x，省
    47.2%）；非记录消息仍是 JSON 行，按首字节分派，老客户端不受影响。
    端到端 apply 时间在本机差异落在噪声内（apply 由页操作主导），收益体现在
    流字节与省掉的 base64 解码。
  - **认证/压缩/TLS 已做 MVP**：`WALServer(token=...)` / `WALClient(token=...)`
    在每条请求上校验共享 token；`WALClient(compress=True)` 让响应行使用
    zlib 压缩；`WALServer(ssl_context=...)` / `WALClient(ssl_context=...)`
    可以走标准库 TLS；服务端 `SSLContext.verify_mode = CERT_REQUIRED` + 客户端
    证书即双向认证，已有测试覆盖。证书分发和轮换仍需部署层补齐；主机名校验由客户端 `server_hostname`
  控制，已有“错误主机名必须握手失败”的测试。
- 副本查询与 apply 的并发调度**已做，原子单元是事务**：`Replica.apply`
  仍按 `_apply_chunk_size`（默认 512）分组、组间释放数据锁，但**切点只落在
  COMMIT/ABORT 之后**，服务端 `fetch` 也回退到批内最后一个事务边界——读者在
  任何组边界看到的都是"完整事务前缀"，顺序扫描与索引扫描不会再对不上
  （按记录数硬切时能看到"表页已应用、索引页未应用"：400 行 + 二级索引 +
  `_apply_chunk_size=1` 的对照实验从 7 次错位降到 0，且修复前伴随
  `BTreeInvariantError`）。`_pending_reads` 启发式只决定"要不要让出"，不承担
  正确性；拉取循环的串行化拆到 `_fetch_lock` 上。跨语句一致视图用
  `Replica.snapshot_read()`（上下文期间 apply 停摆，只用于短读）。单事务记录
  数超过 `max_records` 时无法对齐，窗口仍在（降级路径，见
  `docs/replication-correctness.md`）。
- 多副本槽位已做 MVP：`WALServer(slot_dir=...)` 每个 `replica_id` 一个
  `<id>.confirmed`，主库保留点取所有槽位的最小确认 LSN；`Replica` 在
  bootstrap 后先 ack 基准备份 LSN，避免新副本把保留点钉在 0。槽位仍保存在
  单个主库数据目录里，没有跨节点槽位目录或槽位复制；`WALServer.list_slots()`
  和 `drop_slot(replica_id)` 支持副本退役后回收保留点。
- `Database(wal_retain_lsn=read_slot(...))` 覆盖启动顺序：在恢复和自动
  checkpoint 之前恢复保留点；多副本场景需要把最小确认 LSN 传给这个参数。
- 基准备份已改为 `snapshot_file` + `backup_stream` 分块传输，不再把整个
  数据文件读成 bytes；页级增量基准备份已做：`snapshot_pages(base_lsn)`
  只导出 pageLSN 大于基础 LSN 的页，副本可在 WAL 已回收时重建文件。
  基准备份的压缩已做：`WALClient(compress=True)` 会让 backup_stream /
  incremental 的分块响应行同样走 zlib（压缩 roundtrip 有测试覆盖）。
- 基准备份的写冻结依赖 `Database._write_lock`；它把主库写语句串行化，
  读查询仍可并发。跨进程/绕过 Database 的直接磁盘写不受保护。

**旧主库重新加入**：`Replica.step_down()` 只把自己切回只读，并不等于"能安全
加入新集群"——旧主库可能留下**分叉尾巴**（崩溃前已提交、但从未复制出去的
事务），而页级增量重建（`rebase_from_server`）只覆盖"新主库 pageLSN 更新过的
页"，分叉页不会被覆盖，副本会带着新主库不认的行继续服务。因此新增
`Replica.rejoin_from_server(host, port, path)`：把旧目录整体归档为
`<name>.stale-<YYYYMMDD-HHMMSS>`（可指定 `archive_dir`，**归档不删除**，
保留现场供审计），再走全量基准备份；返回值带归档路径。
`tests/unit/test_rejoin_as_replica.py` 用"旧主库多提交 5 行、副本提升为新主库"的
场景验证：重新加入后只看到集群真实前缀的 10 行（分叉 5 行按预期丢弃），
节点只读、且能追平提升后的新写入。命令行同样可达：
`lumendb rejoin --path <旧目录> --host <新主库> --port <复制端口>`（可选
`--archive-dir` / `--token` / `--replica-id` / `--compress` / `--binary`），
打印归档位置与重建后的 `applied_lsn`。
完成标准（整体第 4 层）：主库写入、副本追平、主库崩溃后副本可提升；有延迟
与丢失窗口的量化报告；上述未完成项至少补齐复制槽、写冻结基准备份和提升
fencing。

**当前状态**：上述三条已补齐；延迟/吞吐与丢失窗口的实测见
[`docs/replication-bench.md`](replication-bench.md)，多区间页 diff 已把同一
场景的副本延迟从 13.9 MB 降到 1.1 MB，响应行压缩和标准库 TLS 已作为可选
传输层落地。多数派投票、客户端重定向、增量基准备份、TTL 写租约和
heartbeat RPC / quorum 续租已做 MVP；投票比较已升级为
`(last_term,last_lsn)` 字典序，心跳会把多数派提交水位带给 follower，
更高任期会让候选放弃本轮提升；关闭/降级与在途 SQL 的竞争也已收口
（close 等待只读语句、回滚线上事务）。日志协商已到 prefix-checked
`append_entries` + 逐条任期 + 分叉/缺口自动重建（重建替代尾巴截断）；
心跳超时自动竞选与双候选收敛已做；双写防护为共享 fence + TTL 租约 +
重启打开即检（`fence_path`/`lease_path`）三层兜底，不是完整的多数派写
确认。副本查询与 apply 的并发调度已做 MVP（批内分组让出）；提交水位
已拆成"leader 口头告知"与"多数派自证"（`verified_commit_lsn`，同步复制
屏障只信后者），且**当选即算**（campaign 用本轮赞成票的真实进度立刻推进
自证水位，第一笔写不必等心跳）。

写/读路径在 2026-11-17 拿到三个可量化收益（见 `docs/concurrency.md`
第 1.5 / 1.7 / 1.8 步）：

- DML 只在**目录结构指纹变化**时持久化目录，逐行 INSERT 的 WAL
  13,006 B/行 → **794 B/行（1/16.4）**；
- 自动提交的只读语句不再写 BEGIN/COMMIT 记录（此前每条 SELECT 都要
  fsync），单次 `SELECT COUNT(*)` 1.45–1.52 ms → **0.18 ms（1/8.3）**；
- 服务端语句执行进线程池、只读语句走 `read_guard()`、目录访问加叶子
  RLock，两条连接的慢 SELECT 真正并行（写语句仍由 `_write_lock` 串行）；
- 结构性写（VACUUM / TRUNCATE / ALTER TYPE / DROP COLUMN）改走
  `exclusive_guard()`：读并发放开后，VACUUM 的"查 `active_xids` → 重排页"
  之间有 TOCTOU 窗口，现在等读者归零并挡住新读者再执行，顺带把 VACUUM
  从"遇并发读直接报错"变成"等读者退出（毫秒级）后独占执行"。

剩余差距：follower 的 commit_lsn 仍是 leader 口头告知（标准 Raft 行为，
见 `docs/replication-commit-index.md`，同步屏障/提升闸门已用
`verified_commit_lsn` 独立验证，不做 match 表广播）；单事务超过一个复制批
时 apply 的原子性（现为降级路径，彻底解决要索引项 MVCC）；写并发已降到
"行/键/DDL 三级锁"（`09376cb`），并新增 opt-in `concurrent_dml` 模式：DML
不再拿 `_write_lock`，B+ 树插入/删除均已 crabbing，索引根页跨语句共享，回滚
走逻辑路径（堆页/B+ 树页 `no_undo`，VACUUM 回收），并发模式保存点默认走
行级逻辑 undo，堆 DML 逐条记录逆操作，保存点存续期间其它写者可提交；
`--physical-savepoints` 退回独占写者锁。逻辑路径是 DML-only，保存点后一旦有行
undo，事务内 DDL/TRUNCATE/VACUUM 会被拒绝；尚无行 DML 时 DDL/TRUNCATE 先升级成
物理保存点。撤销的插入只标
`FLAG_ABORTED`。`concurrent_dml` 仍保持 opt-in：不是正确性没底，而是显式事务
仍是快照隔离（不是 PostgreSQL READ COMMITTED），静默切换会改变用户可见语义；
默认串行短事务吞吐也更好。文件级空间回收已补 MVP：离线 VACUUM 摘掉链尾
空页、REINDEX/VACUUM 旧索引页、普通 DML 的旧目录中间根页在提交后进入
元数据页空闲表（最多 1008 项），分配优先复用；关闭时先 checkpoint 再从
文件尾截掉连续空闲页。中间空页仍只复用不搬移，在线 VACUUM 仍不摘页链；
空闲表溢出只泄漏空间，不会误用活页。`VACUUM` 在线模式只保 RID、不重排
（有活跃事务时等 5s 后自动转在线，不再报错；版本索引只保留 38 字节记录头，
物理版本数默认上限 250,000，可由 Database/CLI 配置，超限失败关闭）。自动提交
UPDATE 的 READ COMMITTED 色彩与显式事务的快照隔离已写成契约并由测试钉住。
2026-11-26 D 连接淘汰原因可见：`EXPLAIN` 的连接行估算 `reason` 写明
“几个等值键、build 哪一侧”，无等值键时写明嵌套循环；DP 候选数继续由
`[candidates=N]` 暴露。候选逐项代价仍不展开。

2026-11-26 D 统计目录视图：`SHOW STATISTICS [FROM table]` 只读输出
表/列/行数/n_distinct/null_frac/min/max/桶数/sample_rows，支持
`AT CATALOG VERSION n` / `AT TIME` 读取历史版本统计；ANALYZE 结果可直接核查。
`sample_rows=0` 表示全量，>0 表示抽样，解决“抽样后看不出是近似值”的问题。

2026-11-26 D ANALYZE 抽样：`ANALYZE [TABLE] t SAMPLE n` 用固定种子蓄水池抽样
只保留 n 行参与列统计，内存不再随全表 distinct 增长；行数仍精确计数，
`n_distinct`/直方图为近似值并在 notice 中注明。全量基线 1622。

2026-11-26 D 字符串范围统计：`ANALYZE` 为字符串列生成 16 桶等频直方图，
桶边界取排序后的实际值；范围谓词按整桶命中估算，边界按字典序比较而不是
数值转换。字符串范围从固定 0.33 变为统计驱动，`EXPLAIN` 可观测。

2026-11-26 A 仲裁节点生命周期收口：`QuorumNode` 的 serve 与 election-watchdog
改由内部 `ServiceRegistry` 管理，start 幂等、stop 反向、可重新 start；
`services_status()` 可观测。线程泄漏与多数派/租约/同步复制用例全绿。

2026-11-26 A 复制监控生命周期收口：`ReplicationMonitor` 的 poll 与
election-watchdog 改由内部 `ServiceRegistry` 管理，启动按注册顺序、停止反向，
`services_status()` 可观测；stop 后可重新 start。复制/故障切换/线程泄漏用例全绿。

2026-11-26 C 历史 schema 可查：`SHOW COLUMNS FROM table AT CATALOG VERSION n`
从目录历史根重建 `TableDef`，输出列名/类型/可空/主键/默认值；未知版本或表报
`42P01`。它只读目录，不读旧数据页，仍不是 `AS OF`。全量基线 1613。

2026-11-26 C 版本化 catalog 第一切片：目录 JSON 增加 `catalog_version` 与
`previous_root`，只有 schema 形状变化才递增；`Catalog.history()` /
`table_at()` 可回溯旧列，`SHOW CATALOG VERSIONS` 暴露版本链；回滚 DDL 恢复
版本号。普通 DML 的页根更新不再伪造新 schema 版本。
每个 schema 版本写入 UTC `created_at`，`Catalog.version_at_or_before(ts)` 可按
时间定位最近目录版本，`SHOW CATALOG VERSIONS` 增加第五列 `created_at`。
`SHOW COLUMNS FROM t AT TIME '…'` 把时间参数经 `version_at_or_before` 解析成
目录版本后再读历史列定义；`SHOW TABLES` / `SHOW VIEWS` 也接受版本号或时间，
从历史目录重建当时的表/视图清单。这只定位目录，不保证数据页可回到该时刻；
并发 DDL 仍是单写者，但历史链已经为 `AS OF SYSTEM TIME` 提供了索引骨架。
全量基线 1620。

2026-11-26 C 历史统计与样本可见性：`SHOW STATISTICS [FROM t]` 增加
`AT CATALOG VERSION n` / `AT TIME`，读取该目录版本的列集合与统计快照；
输出增加 `sample_rows`（0=全量，>0=抽样参与行数），抽样统计不再只能靠
notice 判断。全量基线 1622。

2026-11-26 ① AS OF SYSTEM TIME 只完成设计边界，见
[`docs/as-of-system-time.md`](as-of-system-time.md)：目录 `AT TIME` 不是数据
时间旅行；必须先有持久提交账本、历史可见性和 VACUUM 保留水位。
P0 第一切片已落地：`SHOW COMMIT LOG [LIMIT n]` 输出当前 WAL 窗口内
`xid / commit_lsn`，事务标记 COMMITTED 时才登记，重开从 WAL 重建；checkpoint
截断前的持久历史尚未保留。未实现 `AS OF` 前不宣称时间旅行。全量基线 1625。

2026-11-26 ① P1 第一切片：`HeapFile.scan_at(commit_lsns, target_lsn)` 按提交
LSN 截止点做历史顺序扫描，INSERT/UPDATE/DELETE 三个边界各有测试；未提交或
账本缺失的 xid 一律不可见。仍未接 SQL、索引、JOIN 与 VACUUM 保留水位，
所以它只是 AS OF 的存储层地基，不是用户可用的时间旅行。全量基线 1627。

2026-11-26 ① P3 SQL 表面：新增 `HistoryWindow`、`SHOW HISTORY WINDOW`、
`SELECT ... FROM t AS OF SYSTEM TIME LSN n` 与 `Scan.as_of_lsn`。窗口为
`[floor_lsn, latest_commit_lsn]`，floor 取最近 checkpoint / VACUUM 开始 /
DDL 开始的最大值；超出窗口报 `BinderError`。只允许基表顺序扫描，索引路径
禁用；同一查询里的 JOIN / 子查询 / CTE 必须共享同一 LSN；视图数据源、视图
定义内、显式事务内使用都拒绝。checkpoint 前的存量行按“水位前已提交”假设
保留，未知 `xmax` 严格报错。`tests/unit/test_as_of.py` 12 例（新增重开用例单文件 12 例通过）；全量基线 1638。
持久提交账本、VACUUM 保留水位、索引历史读仍未完成。

2026-11-26 并发 B+ 树删除路由修复：500 删 + 500 并发插时，叶分裂与父分隔键
更新之间有一个窗口，`delete` 会按旧路由落到左半、返回 `False` 并静默留下
`old0019` 这类残键（约 1/30）。`delete` 现在最多从当前根重试 8 次；隔离用例
连续 80 次 0 失败，默认/并发全量各 1638。

2026-11-27 ① P1 索引历史读：`IndexLookup` / `IndexRange` 带上目标 LSN；
索引项只作为候选根 RID，`HeapFile.get_at` 沿版本链解析目标点可见版本，再用
该版本的键复核谓词。索引里的旧键项不会把当前值冒充历史值；`CREATE INDEX` /
`REINDEX` 已推进结构下界，重建前的目标点明确拒绝。等值/范围/删除后点查各有
用例，`EXPLAIN` 会显示 `IndexScan ... [AS OF LSN n]`。

2026-11-27 ① P2 VACUUM 保留水位：`VACUUM [table] RETAIN LSN n` 强制走保
RID 的在线回收，额外保留目标点仍可见的版本；结构下界持久化到 `n`，不再
无条件抬到 `wal.next_lsn`。目标必须落在 `[floor_lsn, latest_commit_lsn]`，
越界分别报“早于/晚于”；VACUUM 结束后仍可 `AS OF ... LSN n`，重开也保持。
当时默认全量 1651 例。

2026-11-27 ① P0 持久提交账本：新增 `commit-ledger.bin`（定长二进制、原子替换、
fsync），默认保留最近 100000 条提交；checkpoint 固定为“写账本 → 推进
`checkpoint_lsn` → 裁 WAL”，写账本失败则不推进、WAL 保留可恢复记录。恢复时
合并账本与 WAL 中全部 COMMIT，`SHOW COMMIT LOG` 不再因 checkpoint 清空。
历史窗口拆成持久化的结构下界（VACUUM/DDL，meta offset 48）与账本下界
（最老保留提交），checkpoint 不再阻断历史读；目标点可以早于 checkpoint。
VACUUM/DDL 下界跨重启保持。当时默认/并发全量各 1647 例。

2026-11-27 ① P0 账本纳入基准备份与副本：`backup` / `backup_stream` 在
`backup_end` 前用 `ledger_chunk` 分块发送 `commit-ledger.bin`，客户端 CRC
校验后原子落盘；页级增量备份携带最新账本，`rebase_from_server` 替换数据
文件时同步替换账本。副本应用 `COMMIT` 时登记提交 LSN，每批应用后重写
账本，因此副本的 `SHOW COMMIT LOG` / AS OF 覆盖基准备份之后的新提交。
新增基准备份历史读与增量重建账本 2 例；默认全量 1656。

2026-11-27 Step 4 文件级空间回收：`DiskManager` 在元数据页 offset 56 起维护
空闲页表（u32 count + 最多 1008 个 u32），`allocate_page` 优先复用、
`release_pages` 提交后登记、`truncate_free_tail` 从文件尾截连续空页。
离线 VACUUM 把重排后的链尾空页摘链并登记，`BPlusTree.collect_pages` 收集
旧索引树、`CatalogStore.save` 释放非 schema 版本的旧目录中间根页，统一进
`Transaction.freed_pages`；COMMIT 成功后才 `BufferPool.drop_pages` +
`release_pages`，ROLLBACK 直接丢弃登记。`Database.close()` 先 checkpoint
再截尾，避免恢复用旧 WAL 把已截页扩回。空闲表是提示性结构：释放时把页头写成 FREE，恢复 redo 后
`validate_free_list` 按页头剔除陈旧项；崩溃丢失或 1008 项溢出只泄漏空间，
不会把活页交给分配器。新增 12 例空间回收测试；
默认全量 1668。

2026-11-27 元数据页缓存一致性：page 0 被 BufferPool 缓存后，整页刷盘/淘汰
会把 `DiskManager._persist_meta` 刚写下的 `next_page_id`、`checkpoint_lsn`、
历史下界和空闲表覆盖回旧快照（实测 flush 后 next_page_id 从 3 回退到 1）。
新增 `write_meta_page`：写 page 0 前用内存 meta 字段叠加快照，只保留
`catalog_root`；`flush_page` 与 LRU 淘汰两条路径都走它。新增 1 例回归，
默认全量 1669；并发 1668 + 1 skipped。

2026-11-27 并发写模式显式事务 VACUUM 门禁：整事务回滚走逻辑可见性、堆页/
B+ 树页不记物理原像；事务内 VACUUM 一旦重排页链、回收死版本，`ROLLBACK`
无法还原（实测 40 行表 `BEGIN→DELETE→VACUUM→ROLLBACK` 只剩 1 行）。
入口在 `logical_abort` 且非自动提交事务时返回 `0A000`，先 COMMIT/ROLLBACK
再单独 VACUUM；自动提交 VACUUM 仍走在线/离线路径。新增 1 例门禁回归，
并把在线 VACUUM 崩溃用例改为自动提交。默认全量 1671；并发 1669 + 2 skipped。

2026-11-27 Windows 提交账本替换重试：副本读账本与 checkpoint 重写并发时，
`os.replace` 可能被杀毒/索引短暂锁住目标文件（`WinError 5`）。账本写入改为
8 次线性退避重试；一次瞬时锁失败不再让整库回归红。新增 1 例重试回归，
默认全量 1671。

2026-11-27 ALTER shadow heap 旧堆页回收：`ALTER COLUMN TYPE` / `DROP COLUMN`
把可见行写入新堆页链并切换根页后，旧链不再被目录引用；此前旧索引页会进
`freed_pages`，旧堆页却永久泄漏。新增 `HeapFile.collect_pages()` 只读遍历
`root_page` 的 `next_page` 链（带环检测）；两条 shadow 路径在切换根页后把
旧页登记进 `txn.freed_pages`。DDL 开始时历史下界已前移，旧页不再供 `AS OF`；
回滚丢弃登记并由 WAL 原像恢复旧根。实测 160 行 900B 表 `DROP COLUMN` 释放
41 页，后续 160 行插入复用空闲页、文件不再增长。新增 DROP/TYPE 各串行+
并发 4 例回归，默认全量 1675；并发 1673 + 2 skipped。

2026-11-27 DROP TABLE/INDEX 旧页回收：删除目录项后，表的堆页链与索引树页
不再被目录引用，却未登记 `freed_pages`，提交后永久泄漏（实测 160 行 900B
表带索引：`DROP INDEX` 前 126 页、空闲 1，删除后空闲仍只有 0；`DROP TABLE`
同样）。新增 `_collect_table_pages` 收集堆页与全部索引树页，`DROP TABLE`
删除目录项前、`DROP INDEX` 删除索引前登记；提交后复用，回滚随目录撤销
丢弃登记。修复后 `DROP INDEX` 释放 81 页、`DROP TABLE` 释放 122 页，新建
表优先复用旧页。新增 DROP TABLE/DROP INDEX 各串行+并发 4 例回归（含回滚
不释放），默认全量 1679；并发 1677 + 2 skipped。

2026-11-27 回滚事务新分配页回收：事务内 INSERT 新开的堆页、B+ 树分裂页与
目录新链页，ROLLBACK 后没有任何结构引用，却从未登记释放；实测 160 行 900B
表回滚后文件仍停在 102 页、空闲 0。`BufferPool.new_page` 接 `owner` 事务，
把页号记进 `txn.allocated_pages`；`abort` 仅在“本事务开始后没有别的提交、
且回滚时没有别的活跃事务”时把这些页写回空闲表。任何重叠都保守跳过：别的
写事务可能已把这些页挂进自己的链，提前复用会让已提交结构指向被覆盖页。
并发写模式的逻辑回滚不恢复页链，因此不启用；手动重叠用例覆盖两条保守门禁，
串行回滚 160 行后空闲 100 页、重插不再增长。默认全量 1685；并发 1683 + 2
skipped。

2026-11-27 DROP CONSTRAINT 索引页回收：`ALTER TABLE ... DROP CONSTRAINT`
删除 PRIMARY/UNIQUE 约束时先 `drop_index` 再改表结构；旧索引树页没有登记
`freed_pages`，提交后永久泄漏（实测 160 行 900B 唯一约束删除后空闲页仍为 0，
文件反而多 1 页）。在 `drop_index` 前用 `_tree_for(...).collect_pages()` 收集，
成功后登记；回滚撤销目录项时丢弃。修复后同一场景释放 114 页。新增串行+
并发 2 例回归（含回滚不释放），默认全量 1681；并发 1679 + 2 skipped。

2026-11-25 修掉两个真实并发缺口：行锁前物化目标在链头未提交时误丢 RID，导致
`affected=0`；checkpoint 在锁外 fsync 可能漏掉晚到的页写；屏障最终 fsync 失败
会泄漏 BufferPool 锁。并发全量全绿、
门禁脚本 3 轮（11 项，含混合 INSERT/UPDATE/DELETE 与保存点、VACUUM 探针）、
8×1000+8×100 基准顺序 20 轮/低切换 10 轮/四进程 20 轮均 `8001/800`；
`mutation_check.py` 4/4（3 条必须变红，行锁 1 条已知冗余保持绿）。HeapPage/目录页
锁纪律还缺逐路径审计清单，因此第 4 层仍只能标 "MVP 验收"，不能标 "生产就绪"。

## 明确不做

- `MATCH PARTIAL` 外键匹配：PostgreSQL 本身不支持，不实现。
- 递归 CTE 的 DML 形态：当前只支持递归查询。
- 分布式事务：不在本项目范围内。

## 阶段纪律

- 每个阶段先写“输入/输出/完成标准/回退”，再动代码。
- 新能力必须同时补：正向测试、错误路径测试、文档、测试数更新。
- 推送后核对 CI；CI 未绿不进入下一阶段。
- 不为追求提交数量跳过前置层；若前置层缺失，先补基础设施。
