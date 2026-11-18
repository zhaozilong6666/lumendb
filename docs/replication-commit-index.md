# follower 侧 commit index：现状与结论（2026-11-18）

> 起因：`docs/roadmap.md` §第4层把"follower 侧独立推演 commit index"列为
> 剩余差距。本文用当前代码核实这条差距到底有多大、是否真的需要补。

## 现状（代码实测）

两个水位字段分开存在：

- `VoteState.commit_lsn` —— **leader 口头告知**的高水位。follower 在心跳里把
  leader 下发的值直接 `observe_commit(lsn)`（`verified=False`）抄进来，用途是
  "我还落后多少、能不能竞选"，不代表本地验证过多数派。
- `VoteState.verified_commit_lsn` —— 仅由 `_advance_commit_lsn` 算出：把
  `[self_lsn] + 各 peer match_lsn` 降序后取第 `majority` 大值，然后
  `observe_commit(watermark, verified=True)`。代表"多数派确实持有"。

关键接线点：

1. **同步复制屏障只信 `verified_commit_lsn`**（`QuorumCommitBarrier`），
   防止"手动提升一个抄来高水位的 follower"后屏障一上来就放行。
2. **提升即算**：`campaign` 用本轮赞成票附带的真实进度
   `_advance_commit_lsn(candidate_lsn, granted_lsns)`，新 leader 的
   verified 水位不滞后。
3. **follower 读可见性基于 `Replica._applied_lsn`**（真正 apply 到的 LSN），
   不基于口头 `commit_lsn`。

## 结论：这条"缺口"大部分是标准 Raft，不是 bug

标准 Raft 里，follower 的 commitIndex 就是 AppendEntries 里的 leaderCommit
（口头），follower **不会**独立重算多数派。本项目真正要保证的是两条，而这两条
都已经满足：

1. leader 按多数派 `match_lsn` 正确算 commit —— `_advance_commit_lsn` 做对了；
2. 手动提升的节点不得把"抄来的高水位"当作已提交 —— `verified_commit_lsn`
   已解决（`d9c1a89`）。

因此 roadmap 把"follower 独立推演 commit index"列为"剩余差距"措辞偏重，容易让
接手的人以为这是必须补的正确性缺口，从而去实现一个非标准、且对可信 leader
零收益的协议变更。

## 剩余的真实风险（比 roadmap 措辞窄）

follower 的 `quorum_commit_lsn`（口头）仍被竞选闸门 `_known_commit_lsn` 当作
"集群已知水位"参与取 max。这是**保守方向**：取大只会让候选更严格，不会放行
落后候选，风险低。

## 建议：保持 leader-authoritative，不做 match 表广播

要"follower 完全独立推演"，唯一办法是让 heartbeat 每轮携带 leader 的 match
表（各 peer 的 `match_lsn`），follower 自行取第 majority 大值去校验 leader 的
`commit_lsn`。这属于协议变更，额外收益仅在于防御"leader 错误/恶意虚报
commit"，对可信 leader 无收益。当前不建议做。

## 若未来决定做（输入/验收/回退）

- **输入**：heartbeat response 增加 `matches` 字段；follower 校验
  `leader.commit_lsn <= 自己算出的第 majority 大值`，否则拒绝采纳并告警。
- **验收**：手动提升一个"抄来高水位"的 follower，同步屏障仍不提前放行
  （已有测试兜底）；新增"follower 独立算出的多数派水位 ≥ 口头水位"约束测试。
- **回退**：去掉 `matches` 字段即可，无 WAL 格式变更。
