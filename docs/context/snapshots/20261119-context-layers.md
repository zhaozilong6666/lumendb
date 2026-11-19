---
task: T-20261119-1
title: 三层上下文与可校验预算
date: 2026-11-19
commit: 9ed3917
modules: [docs/context, docs/memory, .github, docs, root, tests/unit, tools]
keywords: [context, snapshot, budget]
facts: [f-20261119-703b68, f-20261119-b54e98, f-20261119-22fea7, f-20261119-511f64]
status: sealed
---

# 三层上下文与可校验预算

## 结论
上下文从"每轮重新塞历史"改成三层：base 常驻 450 token（估算值，偏保守）、task 单任务、
snapshots 按需；索引、预算、封存状态由 `context_pack check` 校验，不靠人记得。

## 改动
- `tools/context_pack.py`：check/index/new/seal/next/pack/budget/status 八个子命令；
  预算用偏大的估算（ASCII 3 字符/token），索引由快照 front matter 生成。
- `docs/context/`：base 与 task 刻意不写测试数、不写 commit，只写"用哪条命令查"；
  README 说明三层边界与检查项。
- CI 增加 `tools.context_pack check`；checkout 改为 `fetch-depth: 0`，否则快照里的
  commit 在浅克隆里无法验证。
- 新增 16 条测试：预算、重复章节、索引漂移、done 无快照、占位符、引用不存在的账本 id。

## 证据
- 全量 1360 passed in 153.90s（f-20261119-22fea7）；`context_pack check` 450/500。
- `project_memory drift`：文档与账本测试数一致。
- 账本：f-20261119-703b68（三层决策）、f-20261119-b54e98（70% 阈值的坑）。

## 遗留
- 下一件事用 `context_pack next` 开；路线图当前没有未完成项，方向需要人定。
- 顺手发现一处漂移：roadmap 仍写"行/键锁未开工"，但 09376cb 已落地；
  已记为 open_question（f-20261119-511f64），等确认是表述没跟上还是指
  `_write_lock` 尚未放开。
- 70% 阈值只能手动传 `--used`：脚本读不到宿主 token 计数。这是有意的，
  不做成自动触发，避免得到一个恒为 0 的指标。
