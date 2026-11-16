"""复制与故障切换的端到端演示（第 4 层）。

跑法：
    python -m examples.replication_demo

按顺序演示：物理复制追平 → 断点续传 → 共享 fence 手动提升 →
旧主库停写 → 三节点多数派投票提升（含 TTL 写租约）。

所有输出都来自本机临时目录，跑完自动清理。
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

from lumendb.engine.database import Database
from lumendb.replication import (
    Lease,
    PromotionFence,
    Replica,
    WALServer,
)
from lumendb.replication.quorum import QuorumNode


def _section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="lumendb-repl-demo-"))
    try:
        primary = Database(root / "primary", checkpoint_wal_bytes=0)
        primary.execute_one(
            "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32), score INT)"
        )
        for i in range(20):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'u{i}', {i % 100})")
        server = WALServer(primary).start()

        # 1) 基准备份 + WAL 追平
        _section("1. 副本基准备份与追平")
        replica = Replica.bootstrap_from_server(
            server.host, server.port, root / "replica"
        )
        for i in range(20, 200):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'u{i}', {i % 100})")
        lag_before = replica.lag_from_server(server.host, server.port)
        print(f"主库写入后 LSN 延迟: {lag_before} 字节")
        applied = 0
        while True:
            n = replica.replicate_from_server(server.host, server.port)
            if n == 0:
                break
            applied += n
        print(f"副本追平: {applied} 条 WAL 记录, lag 现在 "
              f"{replica.lag_from_server(server.host, server.port)}")
        print("副本查询:", replica.execute_one(
            "SELECT COUNT(*), MAX(score) FROM t").rows)

        # 2) 断点续传：副本只拉一半，服务端"重启"，再接着拉
        _section("2. 断点续传（服务端重启后接着追）")
        for i in range(200, 240):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'u{i}', {i % 100})")
        first_batch = replica.replicate_from_server(
            server.host, server.port, max_records=50
        )
        print(f"第一批拉了 {first_batch} 条后服务端停止")
        server.stop()
        server = WALServer(primary).start()
        rest = replica.replicate_from_server(server.host, server.port)
        print(f"重启后接着拉 {rest} 条, 副本行数 = "
              f"{replica.execute_one('SELECT COUNT(*) FROM t').rows[0][0]}")

        # 3) 共享 fence：副本手动提升，旧主库重启后打开即被拒（重启防护）
        _section("3. 共享 fence 手动提升 + 旧主库重启防护")
        fence_path = root / "fence.json"
        promoted = replica.promote(fence_path=fence_path, node_id="replica-1")
        promoted.execute_one("INSERT INTO t VALUES (999, 'from-replica', 99)")
        print("提升后的新主库可写 ✓")
        try:
            old_primary = Database(
                root / "primary",
                read_only=False,
                fence_path=fence_path,
                node_id="old-primary",
            )
            print("旧主库还能打开 ✗（不应发生）")
            old_primary.close()
        except Exception as exc:  # noqa: BLE001 - 演示预期的拒绝
            print(f"旧主库重启打开被 fence 拒绝 ✓ ({type(exc).__name__})")
        promoted.close()

        # 4) 三节点多数派投票 + TTL 租约
        _section("4. 多数派投票提升（含 TTL 租约）")
        nodes = []
        for i in range(3):
            nodes.append(
                QuorumNode(f"n{i}", state_path=root / f"n{i}.vote.json").start()
            )
        peers = {n.node_id: (n.host, n.port) for n in nodes}
        for n in nodes:
            n.peers = {k: v for k, v in peers.items() if k != n.node_id}
        lease_path = root / "lease.json"
        candidate = Replica.bootstrap_from_server(
            server.host, server.port, root / "replica2"
        )
        candidate.attach_quorum(nodes[2])
        new_leader = candidate.promote_with_quorum(
            nodes[2], lease_path=lease_path, lease_ttl=2.0
        )
        new_leader.execute_one("INSERT INTO t VALUES (1001, 'quorum', 1)")
        lease = Lease(lease_path, nodes[2].node_id, ttl=2.0).read()
        print(f"租约持有者: {lease['holder']} (任期 {lease['term']})")
        new_leader.close()
        for n in nodes:
            n.stop()

        # 5) 只读打开放行：被废黜的旧主库"重启"后仍可当只读观察者
        _section("5. 旧主库只读观察者")
        server.stop()
        primary.close()
        observer = Database(root / "primary", read_only=True)
        print("只读观察者仍可查:",
              observer.execute_one("SELECT COUNT(*) FROM t").rows)
        observer.close()
        print("（primary 句柄已关闭，finally 中不再重复关闭）")
        return
    finally:
        try:
            server.stop()
        except Exception:  # noqa: BLE001 - 可能已停
            pass
        try:
            primary.close()
        except Exception:  # noqa: BLE001 - 可能已关
            pass
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
