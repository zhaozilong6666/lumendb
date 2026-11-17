"""多副本演练：1 主 2 副、单个副本停更、槽位保留点、断点续传。

这是第 4 层"多副本槽位"的端到端验收：不是测单个 API，而是走真实的
bootstrap → 追平 → 掉队 → 恢复 链路，任何一环接线断了都会暴露。
"""

from __future__ import annotations

from lumendb.engine.database import Database
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import WALServer


def _catch_up(replica, server, replica_id: str, rounds: int = 60) -> None:
    for _ in range(rounds):
        if replica.replicate_from_server(
            server.host, server.port, replica_id=replica_id
        ) == 0:
            return
    raise AssertionError(f"副本 {replica_id} 在 {rounds} 轮内没追平")


def test_two_replicas_one_stalls_then_catches_up(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    for i in range(20):
        primary.execute_one(f"INSERT INTO t VALUES ({i})")

    server = WALServer(primary, slot_dir=tmp_path / "slots").start()
    a = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "a", replica_id="a"
    )
    b = Replica.bootstrap_from_server(
        server.host, server.port, tmp_path / "b", replica_id="b"
    )
    try:
        _catch_up(a, server, "a")
        _catch_up(b, server, "b")
        assert a.execute_one("SELECT COUNT(*) FROM t").rows == [(20,)]
        assert b.execute_one("SELECT COUNT(*) FROM t").rows == [(20,)]

        # 主库再写 10 行；只让 a 追平，b 停在原地（模拟 b 的进程/网络挂掉）。
        for i in range(20, 30):
            primary.execute_one(f"INSERT INTO t VALUES ({i})")
        _catch_up(a, server, "a")
        assert a.execute_one("SELECT COUNT(*) FROM t").rows == [(30,)]
        assert b.execute_one("SELECT COUNT(*) FROM t").rows == [(20,)]

        slots = server.list_slots()
        assert set(slots) == {"a", "b"}
        assert slots["a"] > slots["b"], "追平的那个槽位确认点必须更靠前"
        # 保留点不能越过最落后副本的确认点，否则它恢复时已经没有 WAL 可续。
        assert primary.wal.retain_lsn <= slots["b"]

        # b 恢复：从自己的断点续传（不重放已应用的记录）。
        _catch_up(b, server, "b")
        assert b.execute_one("SELECT COUNT(*) FROM t").rows == [(30,)]
        assert a.execute_one("SELECT COUNT(*) FROM t").rows == [(30,)]
        assert primary.execute_one("SELECT COUNT(*) FROM t").rows == [(30,)]
        assert server.list_slots()["b"] == server.list_slots()["a"]
    finally:
        a.close()
        b.close()
        server.stop()
        primary.close()

# 注：副本落后到 WAL 已被回收时的行为（服务端回 error → 副本走
# `rebuild_from_scratch` 重新基准备份）已由 tests/unit/test_replica_rebuild_fallback.py
# 覆盖，这里不重复造一个需要伪造段回收的脆弱场景。