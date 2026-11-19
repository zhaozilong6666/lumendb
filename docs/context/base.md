# LumenDB 常驻上下文

从零写的单机关系数据库：B+ 树、WAL、MVCC、物理复制、PostgreSQL 线协议。

## 规则
- 零运行时依赖（Python ≥3.11，只用标准库）；不引新依赖、不改公开 SQL 语义，除非任务写明。
- 改实现必须带测试，提交前 `python -m pytest tests -q` 全绿。
- 数字只信账本 `docs/memory/ledger.jsonl`：文档不抄数字，要查用
  `python -m tools.project_memory status`，查不到先测再写；结论冲突以带证据的记录为准。
- 一个任务只动声明范围，别顺手改别的模块。

## 开场与交接
1. `python -m tools.project_memory status` 看现状，再读 `docs/context/task.md`。
2. 旧细节按 `snapshots/index.md` 取单份，用完即弃。
3. 交接给下一个 agent：`python -m tools.context_pack pack`。
