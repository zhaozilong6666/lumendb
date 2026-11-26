# 确定性重放（② 骨架的四个场景）

这不是概率性并发压测。它把真实引擎里的关键交错点变成可命名的闸门，用固定
种子决定参数，用事件轨迹断言因果顺序；失败时按同一 seed 重跑会走到同一个点。

## 场景一：叶分裂结构窗口

`scenario_split.py` 钉住刚修好的 B+ 树结构窗口：

1. 9 条长键占满一片叶；
2. `splitter` 插入第 10 条（同值不同 RID），触发叶分裂；
3. 闸门停在“叶已分裂、父键还没装”的那一刻；
4. `follower` 去填满新右页并尝试第二次分裂；
5. 新实现让它停在 `_split_lock`，等 `splitter` 装完父键再继续；
6. 断言 `install.end(splitter)` 的序号小于 `split.start(follower)`。

旧实现（没有 `_split_lock`）在 10 个种子上全部失败；新实现 10/10 通过。

## 场景二：两个事务写同一行

`scenario_row_lock.py` 固定：

1. A 显式事务已拿到行锁、改完值，停在 `COMMIT` 前；
2. B 的自动提交 `UPDATE` 进入 `LockManager.acquire` 并阻塞；
3. 断言 B 的 `acquire.end` 在 A 的 `COMMIT` 之前没有发生；
4. 放行 A 提交，B 唤醒后锁后重读，最终值是两次增量，不是 40001。

## 场景三：未提交写挡住并发 DDL

`scenario_ddl.py` 固定：

1. A 显式事务已插入一行，停在 `COMMIT` 前；
2. B 在 A 提交前尝试 `ALTER TABLE ... ADD COLUMN`；
3. 断言 DDL 被 40001 拒绝，而不是悄悄重写只复制可见行的 shadow heap；
4. 放行 A 提交，B 重试 DDL 成功；
5. 最终两行都在，且都能按新列形状读出。

若门失效，第一版 DDL 会成功，未提交行之后落到已被替换的旧 heap，查询静默少行。

## 场景四：副本 apply 中途换主

`scenario_replica.py` 固定：

1. 副本应用完第一个事务，在组边界把数据锁让给读者；
2. 闸门停在这里，主线程执行 `promote`，把只读句柄换成读写句柄；
3. 放行后旧 apply 循环发现句柄已换，把剩余记录放回暂存区；
4. 新主库从暂存区幂等重放，最终所有行都在。

若旧 apply 继续写旧句柄，最后会静默少行，而不是崩溃。

## 运行

```bash
python -m pytest tests/unit/test_deterministic_replay.py -q
python -m tests.deterministic.replay --seeds 10
python -m tests.deterministic.replay --seed 3   # 单种子复现
python -m tests.deterministic.replay --seed 3 --scenario row
python -m tests.deterministic.replay --seed 3 --scenario ddl
python -m tests.deterministic.replay --seed 3 --scenario replica
python -m tests.deterministic.replay --seed 3 --scenario split
```

## 边界

这是 ② 的基础设施，覆盖了 Cherry 要求的三个场景（行锁等待、并发 DDL、
副本 apply 中途换主），外加一个 B+ 树分裂结构窗口。四场景各 1000 个种子
已实测 **4000/4000 通过**；`fsync` / `page.fetch` 仍只有结构窗口这一处闸门，
是下一轮要补的故障点。
