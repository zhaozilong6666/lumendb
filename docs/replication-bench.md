# 复制路径基准（第 4 层）

> 实测日期：2026-11-14，Windows 本地开发机，Python 3.12。
> 跑法：`python -m bench.replication --rows 2000 --repeat 3 --out bench/results/replication.md`
> 原始表格见 `bench/results/replication.md`。数字会随机器、磁盘负载变化。

## 场景

- 主库 `checkpoint_wal_bytes=0`，避免自动 checkpoint 干扰观察。
- 先建 2000 行表，基准备份在 200 行时创建；其余 1800 行在副本启动后写入。
- 副本通过 `WALServer` 的 TCP `backup` 拉基准备份，再以 `max_records=1000`
  分批 `replicate`，每批 apply 后 `ack`。
- `lag_from_server` 读取主库 `status` 的 `last_lsn` 与副本 `applied_lsn` 之差；
  单位是 WAL 逻辑字节，不是表数据字节。

## 实测

| 指标 | 数值 |
|---|---:|
| 数据行数 | 2000 |
| 基准备份（最好 / 中位） | 0.020 s / 0.034 s |
| 主库写入 1800 行（中位） | 0.400 s |
| 副本追平（最好 / 中位） | 1.275 s / 3.215 s |
| 应用记录数 | 7376 |
| 应用吞吐 | 约 5,783 记录/秒（最好样本） |
| 写入后 LSN 延迟 | 13,896,882 字节 |
| 追平后 LSN 延迟 | 0 字节 |
| 副本 `COUNT(*)` | 16.91 ms |
| 主库数据文件 | 212,992 B |
| 最后记录 LSN / base LSN | 15,566,304 / 1,046,313 |

## 解读

1. **延迟的量化单位是 WAL 字节**。2000 行数据文件只有 213 KB，但这一轮
   WAL 到 15.5 MB（约 7.7 KB/行）。主要来源不是 `PAGE_IMAGE`（新页镜像
   约 100 KB），而是 B+ 树叶页的页内 diff：每次插入会把 key/slot 区间
   往后挪，物理 before/after 切片经常接近半页。因此“WAL 字节/行”很高，
   但副本按 `pageLSN` 幂等重放，正确性不依赖 WAL 大小。
2. **副本吞吐约 5.8k 记录/秒（最好样本）**。中位追平 3.2 s 明显受 Windows
   文件刷盘影响；同一台机器重复运行会波动，不能把最好样本当成 SLA。
3. **丢失窗口**：主库在写入后立即崩溃、副本尚未 apply 时，最多丢失
   13.9 MB WAL 对应的已提交事务。`Replica.promote` 只保留已复制前缀；
   这不是自动 failover，也没有 RPO=0 承诺。
4. **手动提升**：`promote(fence_path=...)` 会写 `PromotionFence` marker；
   配置了同一 marker 的旧主库会在下一笔写上拒绝。共享 marker 不存在时，
   旧主仍可能继续写，因此部署必须把 fence 文件放在两个节点都能看到的
   位置。

## 下一步（按优先级）

- 降低物理 WAL 放大：把 B+ 树叶页的“槽位目录 + key”改成更小的逻辑/记录级
  diff，或对连续插入使用批量页构造；当前先保留物理语义，不牺牲恢复正确性。
- 分块基准备份，避免 `Database.snapshot()` 把整个数据文件读进内存。
- 复制流认证/TLS、压缩、二进制帧、长连接/心跳；当前是换行 JSON + base64。
- 自动故障切换需要心跳、租约/仲裁和客户端重定向；目前只有手动提升 +
  共享文件 fencing。
- 副本查询与 apply 的并发调度：当前 `Replica._lock` 串行化 apply 与查询，
  先保证不读到半批状态。
