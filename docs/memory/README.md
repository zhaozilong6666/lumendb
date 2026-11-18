# 项目记忆（Project Memory）

长周期无人值守项目里，最贵的错误不是"忘了做"，而是**把记得的数字当成事实**：
三个月后的我（或另一个 agent）会把 `0.13ms` 和 `0.47ms` 同时当成"当前点查延迟"，
然后据此做出错误的优化判断。这个目录就是为此存在的：**每条结论都挂机器可核对的
证据，并且能一键复查**。

账本文件：`ledger.jsonl`（逐行 JSON，append-only，便于 diff 与 grep）。
工具：`python -m tools.project_memory`，纯标准库，无外部依赖。

## 一条记录长什么样

```json
{"id":"f-20261117-a1b2c3","kind":"measurement",
 "claim":"SQL 解析缓存把参数化点查从 0.149ms 降到 0.096ms（−35.6%）",
 "created":"2026-11-17T12:00:00+09:00","confidence":"measured",
 "tags":["perf","parser","cache"],
 "evidence":[{"type":"commit","value":"dcab1b6","anchor":""},
             {"type":"file","value":"lumendb/sql/parser.py","anchor":"_parse_sql_cached"},
             {"type":"test","value":"tests/unit/test_parse_cache.py","anchor":""},
             {"type":"command","value":"python D:/projects/lumendb-review/_probe_parse_cost.py","anchor":""}],
 "supersedes":null,"verify":{"status":"ok","checked_at":"...","detail":[...]}}
```

`kind` 的含义：

| kind | 记什么 | 典型例子 |
|---|---|---|
| `measurement` | 测出来的数字 + 怎么测的 | 点查 0.47ms；二进制帧省 47.2% |
| `decision` | 取舍及理由（**为什么不做**同样重要） | 索引 latch 暂不做，先做行锁 |
| `bug` | 真 bug 的根因与修法 | ENOSPC 让整库永久只读 |
| `pitfall` | 走过的死路，别再试 | `last_lsn` 不能判断 COMMIT 是否落盘 |
| `open_question` | 还没定、等拍板的事 | 160 个提交要不要 push |
| `contract` | 对使用者的承诺/不变量 | 副本原子应用单元 = 一个事务 |
| `commit` | 由 git 自动灌入，最硬的锚点 | `perf(parser): 解析加 LRU 缓存` |

## 三条使用规则

0. **每轮先 `status`。** 它把"现在在哪、还剩什么"压成十几行：HEAD、ahead、
   账本覆盖、最近一次测试、未决问题、校验失败的记录。避免开场就凭记忆开工。
1. **写文档/总结前先 `query`。** 任何要写进 README、portfolio、提交说明的数字，
   都必须能从这里查到一条 `measurement` 记录；查不到就先测再写。
2. **修正旧结论用 `--supersedes` 追加新记录，不要改旧行。** 历史可以被推翻，
   但不该被抹掉——否则以后无法解释"为什么当时会那么判断"。
3. **每次提交后 `ingest-git`。** 提交记录由 git 生成，不经过我的记忆，是成本最低、
   最不容易出错的一类锚点。（账本自身的维护提交除外，否则会无穷递归。）

## 命令

```powershell
# 一页看清现在在哪：HEAD / 分支 / 领先多少提交 / 账本覆盖 / 最近测试 / 未决问题
python -m tools.project_memory status

# 续作交接视图：未决问题 + 校验失败/无证据 + 数字冲突 + 各主题最新结论
# （和 status 的区别：status 说"现在在哪"，digest 说"接手先信哪几条、先处理哪几条"）
python -m tools.project_memory digest

# 路线图对账：README「路线图」清单 × 账本证据——还剩什么、每条上次讨论
python -m tools.project_memory roadmap

# 追加一条带证据的事实
python -m tools.project_memory add --kind measurement `
  --claim "索引点查 0.47ms/次，全表 48ms/次（约 100 倍）" `
  --tags bench,point-lookup --confidence measured `
  --evidence commit:cf22311 `
  --evidence file:bench/results/benchmark.md:量级参考

# 按问题检索（BM25 + 120 天半衰期；--kind/--tag 可过滤）
python -m tools.project_memory query "点查延迟为什么变慢"

# 复查所有证据（--run-tests 会真的跑测试文件；--only-stale 只查有问题的）
python -m tools.project_memory verify --run-tests

# 把 git 提交灌成锚点记录（自动跳过已灌过的）
python -m tools.project_memory ingest-git --rev-range "d21da96..HEAD"

# 体检：重复、数字矛盾、校验覆盖率
python -m tools.project_memory dups
python -m tools.project_memory conflicts
python -m tools.project_memory stats
python -m tools.project_memory show f-20261117-a1b2c3
```

## 用了哪些算法，为什么

| 问题 | 算法 | 选择理由 |
|---|---|---|
| 按问题找记录 | **Okapi BM25**（k1=1.2, b=0.75） | 语料里全是标识符——commit 哈希、文件路径、错误码、函数名。精确 token 匹配正是 BM25 的强项；嵌入模型反而容易把 `dcab1b6` 这种稀有 token 稀释掉，而且分数不可解释 |
| 旧事实不该压过新事实 | **指数半衰期**（默认 120 天，`0.5^(age/hl)`） | 上周测的应排在三个月前测的前面，但不该被丢弃；原始 BM25 分与衰减系数都原样输出，排序可解释 |
| 同一件事记了两遍 | **SimHash(64bit) + Jaccard** | 换个说法重复记录，以后会被当成两条独立证据；SimHash 抓"措辞不同、内容相同"，Jaccard 兜底 |
| 数字互相矛盾 | **单位归一 + 同主题数值比对（>25% 报警）** | `0.13ms` 和 `0.47ms` 同时躺在账本里早晚被误引；宁可在写入时就吵起来 |
| 中文分词 | **字符 + 二元组，ASCII 按标识符切** | 无词典依赖；中文语序变化靠 bigram 兜住；路径既保留整串也保留片段 |

为什么不用嵌入/向量库：语料只有几百条、查询以标识符为主、要求离线可跑且分数可解释，
BM25 在这个规模上更准也更便宜。等账本上千条、出现大量同义改写时再换。

## 证据类型与校验行为

| 类型 | 校验做什么 | 备注 |
|---|---|---|
| `commit` | `git rev-parse --verify` | 抓 rebase 后锚点消失 |
| `file` | 文件存在 + 锚点字面串存在 | 抓"函数被改名/删除"导致的过期结论 |
| `test` | 文件存在；`--run-tests` 时真的 `pytest` | 默认只查存在，避免每次 verify 都跑几分钟 |
| `command` | 只记录，标记 `manual` | **不默认执行**外部命令：账本里的命令可能带副作用 |

聚合状态：任一条 `failed` → `failed`；有 `unchecked`/`manual` → `partial`；全 `ok` → `ok`。

## 和"多写日志"的区别

日志回答"我做了什么"，账本回答"**什么仍然为真、凭什么**"。前者是流水，后者是可
反驳的断言集合——这就是它抗幻觉的地方：如果我记错了，`verify` 会让它红；如果记录
本身过期了，`verify` 也会让它红。
