# 三层上下文

长周期项目的上下文膨胀，通常不是因为信息太多，而是因为信息没有落脚点：
每轮都要把"项目是什么、上次做到哪、为什么那么选"重新塞回对话。这个目录
把上下文拆成三层，每层只回答一个问题，并且**尽量让机器检查代替自觉**。

| 层 | 文件 | 进上下文 | 回答什么 |
|---|---|---|---|
| 常驻 | `base.md` | 默认 | 项目是什么、哪些规则不能破 |
| 任务 | `task.md` | 默认 | 这一轮只做哪一件、怎么算做完 |
| 快照 | `snapshots/*.md` | 按需单份 | 当时为什么那么做、证据在哪 |

常驻层有硬预算：`base.md` + `task.md` 的估算 token 超过 `RESIDENT_BUDGET`
（`tools/context_pack.py` 顶部，500）就检查失败。估算整体偏保守：ASCII 按
3 字符/token（真实分词器约 4），中文按 1 字 1 token；中英混排时通常高于实际。

## 一条命令

```bash
python -m tools.context_pack status   # 常驻占用、当前任务、快照数、索引是否过期
python -m tools.context_pack check    # 全量校验；CI 跑的就是这条
python -m tools.context_pack query 行锁 崩溃恢复   # 按关键词召回快照，多个词按命中数排序
python -m tools.context_pack pack     # 交接包：base + task + 前置快照
```

## 为什么索引是生成物

`snapshots/index.md` 由脚本从各快照的 front matter 重写，`check` 会比对
文件内容。手写索引一定会落后于文件列表——这不是纪律问题，是结构问题：
新增快照的人和维护索引的人不是同一时刻的同一个人。

同理，快照的 `commit` 和 `modules` 由 `new` 从 git 自动填，不靠手抄。

## 一份快照长什么样

```markdown
---
task: T-20261119-1
title: 用行/键/DDL 锁替换单写者锁
date: 2026-11-19
commit: 09376cb
modules: [lumendb/storage, lumendb/engine, tests/unit]
keywords: [lock, row, key, ddl]
facts: [f-20261119-abc123]
supersedes: [20261115-single-writer.md]
status: sealed
---
# ...

## 结论
一句话说清这轮改变了什么判断。

## 改动
改了哪些文件，为什么是这些文件。

## 证据
测试、基准、账本 id。没有测量的数字不要写。

## 遗留
没做完的、以及下一步该谁接手。
```

`supersedes` 声明"这份快照推翻了哪几份旧快照的结论"。索引和 `query` 会把
被替代的快照标成"已过时"，避免下一个 agent 把旧结论当真。账本用
`--supersedes` 管事实，快照用 front matter 的 `supersedes` 管叙述——同一件事
的两个层面，都靠机器核对引用存在，而不是靠自觉。

`status` 只有 `draft` / `sealed`。草稿可以有占位符，封存不行——`seal`
会拒绝 `（待填）`、`TODO`、`TBD`，因为"封存了但没写"比"没封存"更糟：
它会让下一个 agent 以为这里有答案。

## 生命周期

```bash
# 1. 干活，改代码，跑测试
# 2. 封存这一轮
python -m tools.context_pack new --slug row-lock-granularity --title "行/键/DDL 锁"
#    填 结论/改动/证据/遗留
python -m tools.context_pack seal
# 3. 开下一个任务（没有 sealed 快照会被拒绝）
python -m tools.context_pack next --to "……" --accept "……" --accept "……" --not-do "……"
# 4. 提交
```

`next` 拒绝在未封存时开新任务，这是"任务完成必触发快照"的落地点；
`check` 另外堵住"手动把 status 改成 done 却没有快照"。

## 检查项

| 代码 | 规则 |
|---|---|
| CTX001 | base / task / 索引缺失 |
| CTX002 | task front matter 不合法（id 格式、status 取值） |
| CTX004 | 章节缺失、为空或重复（单任务约束） |
| CTX005 | 前置快照不存在、未封存或未进索引 |
| CTX006 | 常驻 token 超预算 |
| CTX007 | 常驻层出现会过期的数字（`N passed`、commit 哈希） |
| CTX008 | 索引与快照实际内容不一致 |
| CTX009 | 快照命名、front matter 或封存章节不合法 |
| CTX010 | 快照引用的 commit 不存在（浅克隆时跳过并告警） |
| CTX011 | 快照引用的账本 id 不存在 |
| CTX012 | 任务标 done 但没有 sealed 快照 |
| CTX013 | 同一任务有多份 sealed 快照 |
| CTX014 | 封存快照仍含占位符 |
| CTX015 | 封存快照没有 modules 或 keywords |
| CTX016 | 封存快照正文超 `SNAPSHOT_BUDGET`（快照只写结论和证据，细节拆到下一份） |
| CTX017 | `supersedes` 引用的快照不存在，或指向自己 |

## 70% 阈值：显式命令，不是自动触发

脚本读不到宿主对话的 token 计数。假装能读，只会得到一个恒为 0 的指标——
那种"看起来在工作"的检查比没有更糟。所以强制封存做成显式命令：

```bash
python -m tools.context_pack budget --used 140000 --window 200000
# 140000/200000 = 70%  -> 退出码 2，提示先封存
```

`--used` 从界面读一次传进来。真正自动的是另外两条：常驻超预算、done 无快照，
它们只依赖仓库里的文件，CI 每次都能跑。

## 边界：什么时候别用

- **多任务并行**：task.md 只有一个当前任务；同时推三个模块会频繁切换文件。
  这种情况按任务拆分支，而不是往一份 task.md 里塞三件事。
- **跨模块深度调试**：需要连续十几轮推理链时，别在中途封存——快照是给
  下一个接手的人看的，不是给自己打断思路用的。
- **没有文件权限的环境**：这套方案完全依赖本地文件，退化回对话内摘要。

## 和账本的分工

`docs/memory/ledger.jsonl` 是**事实**：带证据、可校验、可检索。
快照是**叙述**：这一轮为什么这么改、当时排除了什么。快照里的数字和结论
必须引用账本 id，`check` 会核对 id 真的存在——这样快照不会长成第二份
没人核对的真相。
