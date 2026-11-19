---
id: T-20261119-1
status: done
opened: 2026-11-19
base: 459d7c3
---
# 当前任务

## 目标
上下文压成三层（常驻 base、单任务 task、按需快照），预算、索引、完成状态可一条命令检查。

## 验收
- `python -m tools.context_pack check` 退出码 0。
- 常驻超预算、索引被手改、标 done 无 sealed 快照，三者都让 check 失败。

## 不做
- 不引向量库；检索复用账本 BM25。
- 不改数据库实现代码。

## 前置快照
无
