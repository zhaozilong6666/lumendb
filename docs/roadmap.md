# LumenDB 路线图

LumenDB 是一个长期项目。路线图按“基础设施 → 优化器 → 存储/复制”分层，
每一层都必须先具备可验证的输入、明确的完成标准和回退路径，避免出现功能
已经宣布完成、但下游依赖仍然悬空的情况。

## 当前基线

- 全量测试：1036 passed。
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
`stop()` 停止；`promoted` / `needs_attention` 会让后台循环退出。没有心跳
租约和客户端重定向，网络分区下仍要求共享 fence。

**尚未完成，不能把第 4 层宣布为整体完成**：

- 自动故障切换的 **MVP 已做**（`ReplicationMonitor` 连续网络失败后自动
  `promote` + 共享 fence），客户端重定向 MVP 已做（`WALServer(redirect_to=...)` + `RedirectError`，
  `ReplicationMonitor` 会跟随最多 3 次），租约/仲裁和网络分区下的双写防护
  仍未完成。
- 复制流认证/TLS、压缩、二进制帧、断点续传与真实网络故障注入。
  - **认证/压缩/TLS 已做 MVP**：`WALServer(token=...)` / `WALClient(token=...)`
    在每条请求上校验共享 token；`WALClient(compress=True)` 让响应行使用
    zlib 压缩；`WALServer(ssl_context=...)` / `WALClient(ssl_context=...)`
    可以走标准库 TLS；服务端 `SSLContext.verify_mode = CERT_REQUIRED` + 客户端
    证书即双向认证，已有测试覆盖。证书分发和轮换仍需部署层补齐；主机名校验由客户端 `server_hostname`
  控制，已有“错误主机名必须握手失败”的测试。
- 副本查询与 apply 的并发调度；当前是单线程应用。
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
  基准备份内容本身的压缩仍未做。
- 基准备份的写冻结依赖 `Database._write_lock`；它把主库写语句串行化，
  读查询仍可并发。跨进程/绕过 Database 的直接磁盘写不受保护。

完成标准（整体第 4 层）：主库写入、副本追平、主库崩溃后副本可提升；有延迟
与丢失窗口的量化报告；上述未完成项至少补齐复制槽、写冻结基准备份和提升
fencing。

**当前状态**：上述三条已补齐；延迟/吞吐与丢失窗口的实测见
[`docs/replication-bench.md`](replication-bench.md)，多区间页 diff 已把同一
场景的副本延迟从 13.9 MB 降到 1.1 MB，响应行压缩和标准库 TLS 已作为可选
传输层落地。租约/仲裁和副本查询/apply 并发仍未完成（客户端重定向、增量基准备份
已做 MVP），因此第 4 层只能标“MVP 验收”，不能标“生产就绪”。

## 明确不做

- `MATCH PARTIAL` 外键匹配：PostgreSQL 本身不支持，不实现。
- 递归 CTE 的 DML 形态：当前只支持递归查询。
- 分布式事务：不在本项目范围内。

## 阶段纪律

- 每个阶段先写“输入/输出/完成标准/回退”，再动代码。
- 新能力必须同时补：正向测试、错误路径测试、文档、测试数更新。
- 推送后核对 CI；CI 未绿不进入下一阶段。
- 不为追求提交数量跳过前置层；若前置层缺失，先补基础设施。
