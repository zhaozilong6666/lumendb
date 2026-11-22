# WAL 分段刷盘锁拆分（2026-11-22）

`SegmentedWAL.flush` 原先把 `_pending` 交换、分段轮转、文件写入和 fsync 全包在
`_lock` 里：组提交 leader 刷盘时其它提交线程无法追加 COMMIT，批次被压到约 1 个
事务（1200 次插入对应 930 次 flush）。

现在文件 I/O 拆到独立的 `_flush_lock`：flush 只在 `_lock` 内交换 pending，随后
在 `_flush_lock` 下做轮转、write、fsync；写失败时把这一批放回 pending 队首。
`iter_records` / `truncate_before` / `close` 按 `_flush_lock → _lock` 固定锁序执行，
`flush_upto` 也改为不在持 `_lock` 时调用 flush。

实测（Linux / Python 3.12，4 线程 × 300 行自动提交插入）：基线默认 786 / 817
ops/s、并发 785 / 788；改动后默认 782 / 799 / 803、并发 867 / 870 / 872。
8 线程 × 200 行：默认 792 / 794，并发 884 / 852。并发插入首次稳定超过默认约
8%–10%；热点行更新仍由同行冲突主导（4 线程 36 vs 默认 140，460 次 `40001`），
`concurrent_dml` 继续保持 opt-in。
