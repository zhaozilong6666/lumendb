# 故障注入

默认关闭、进程内生效。目标不是“随机把库打坏”，而是把某一次 I/O 精确地卡在
危险窗口里，验证事务状态机与恢复路径的承诺是否真的成立。

## API

```python
from lumendb import faults

faults.arm("wal_segment.fsync@1")        # 第一次 fsync 抛 EIO
faults.arm("disk.write_page@2:diskfull") # 第二次页写抛 ENOSPC
faults.arm("disk.write_page@0.1:torn")   # 10% 概率只写前半页
faults.disarm()                          # 全部撤掉
```

规格是 `point@n`、`point@p` 或 `point@n:kind`：

- `n`：第 n 次命中；`p`：每次调用按概率命中（0–1）。
- `kind`：`io`（默认，EIO）、`diskfull`（ENOSPC）、`torn`（只写前半页）。
- `faults.disabled()` 是上下文管理器，可嵌套；只有最外层退出才重新放行。
- `faults.hits(point)` 返回调用次数，`faults.armed_points()` 返回当前挂点。

## 注入点与位置

| 点 | 位置 | 语义 |
|---|---|---|
| `wal.append` | 单文件 WAL 追加入口 | 整条记录未追加，事务可干净 abort |
| `wal.fsync` | **写完 pending、`os.fsync` 之前** | 记录进了 buffer 但持久性未知 |
| `wal_segment.append` | 分段 WAL 追加入口 | 整条记录未进入 pending |
| `wal_segment.fsync` | **写完 pending、`os.fsync` 之前** | 同上，且会先尝试段轮转 |
| `disk.write_page` | 页写入口 / 写前半页 | `io` 整页失败；`torn` 半页新半页旧 |
| `page.fetch` | 缓冲池未命中、`DiskManager.read_page` 前 | 读盘 EIO；不能把空页装进缓存 |
| `disk.sync` | `fflush` 后、`fsync` 前 | 元数据可能只在 OS 页缓存 |
| `disk.allocate_page` | 分配入口 | 不改变页数，避免“注入失败反而分配了一页” |

`fsync` 点必须放在真正写完之后，否则测试只验证了“入口抛错”，没有覆盖
“pending 已清空 / 页已写但未 durable”的危险窗口。

## 已钉住的不变量

- 调用方拿到错误的事务不能变成已提交：`tests/unit/test_faults.py` 的
  `test_fsync_failure_does_not_mark_the_write_committed` 会关闭并重开数据库，
  失败的那一行必须不存在。
- 一次 fsync 失败不能污染后续提交：`test_fsync_failure_does_not_poison_the_next_transaction`
  在失败后继续插一行，重开后只能看到失败前和失败后的行。
- 提交失败必须立刻 abort，而不是因为“COMMIT 记录已写进 buffer”就保留 undo。
  否则后续事务改同一页后再回滚，会把后来已提交的页改动一起覆盖。
- 单文件 WAL 的 `flush` 失败要把 pending 放回队首，并把文件截回上一个持久
  边界，避免重试丢记录或追加重复记录。
- 分段 WAL 失败同理；若失败跨段轮转，删除未完成的新段并回到旧段边界。
- `torn` 必须真的只写前半页：`test_torn_page_write_writes_prefix_then_reports_error`
  直接检查数据文件字节，后半页保持旧内容。
- checkpoint 屏障内的最终 `fsync` 失败必须放掉 `BufferPool._lock`：否则一次
  可重试的 I/O 错误会变成永久死锁。`test_clean_barrier_releases_lock_when_final_sync_fails`
  用第二个线程抢锁来验证失败路径确实还锁。

## 边界

- 注入状态是进程全局的，测试并行跑时必须用 `faults.disabled()` 隔离。
- 没有跨进程时钟/调度注入；只覆盖本进程的系统调用边界。
- `torn` 目前只接在 `disk.write_page`。页格式没有校验和，所以 torn 页的
  检测依赖页头/结构校验；真正做端到端 torn 恢复还需要页校验和或 WAL 全页镜像。
- 分段 WAL 在跨段失败时的回滚已实现，但尚未做“段文件被外部删除”的注入。
