# 确定性重放（② 骨架的第一块）

这不是概率性并发压测。它把真实引擎里的关键交错点变成可命名的闸门，用固定
种子决定参数，用事件轨迹断言因果顺序；失败时按同一 seed 重跑会走到同一个点。

## 第一个场景

`scenario_split.py` 钉住刚修好的 B+ 树结构窗口：

1. 9 条长键占满一片叶；
2. `splitter` 插入第 10 条（同值不同 RID），触发叶分裂；
3. 闸门停在“叶已分裂、父键还没装”的那一刻；
4. `follower` 去填满新右页并尝试第二次分裂；
5. 新实现让它停在 `_split_lock`，等 `splitter` 装完父键再继续；
6. 断言 `install.end(splitter)` 的序号小于 `split.start(follower)`。

旧实现（没有 `_split_lock`）在 10 个种子上全部失败；新实现 10/10 通过。

## 运行

```bash
python -m pytest tests/unit/test_deterministic_replay.py -q
python -m tests.deterministic.replay --seeds 10
python -m tests.deterministic.replay --seed 3   # 单种子复现
```

## 边界

这是 ② 的第一块基础设施，不是 Cherry 评审里“三个场景各 1000 个种子”的全部。
下一步应把闸门插到 `lock.acquire` / `fsync` / `page.fetch`，再覆盖“两个事务
写同一行”“并发 DDL”“副本 apply 中途换主”。目前只覆盖 B+ 树分裂结构窗口。
