"""复制流的二进制记录帧：与 JSON 行逐字段等价，且体积明显更小。"""

from __future__ import annotations

from lumendb.engine.database import Database
from lumendb.replication.replica import Replica
from lumendb.replication.wal_stream import (
    WALClient,
    WALServer,
    _encode_binary_record,
    _encode_line,
    _record_to_json,
)


def test_binary_record_frame_is_smaller_than_the_json_line(tmp_path):
    """同一批记录：二进制帧的总字节数必须显著小于 JSON 行。"""
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
    for i in range(20):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'user{i}')")
    records = list(db.wal.iter_records(1))
    assert records, "WAL 里应该有记录"

    json_bytes = sum(len(_encode_line(_record_to_json(r))) for r in records)
    bin_bytes = sum(len(_encode_binary_record(_record_to_json(r))) for r in records)
    assert bin_bytes < json_bytes
    ratio = json_bytes / bin_bytes
    # 记录很小的时候 JSON 字段名占大头；即使如此也至少要省 20%。
    assert ratio > 1.2, f"二进制帧收益不足：json={json_bytes} bin={bin_bytes}"
    db.close()


def test_binary_stream_returns_the_same_records_as_json(tmp_path):
    """同一条主库分别用 JSON / 二进制拉取：记录逐字段完全一致。"""
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
    for i in range(30):
        db.execute_one(f"INSERT INTO t VALUES ({i}, 'user{i}')")
    server = WALServer(db).start()
    try:
        json_records = WALClient(server.host, server.port).fetch(0, max_records=500)
        bin_records = WALClient(
            server.host, server.port, binary_records=True
        ).fetch(0, max_records=500)
    finally:
        server.stop()

    assert len(json_records) == len(bin_records) > 0
    for a, b in zip(json_records, bin_records):
        assert (a.lsn, a.xid, int(a.rtype), a.payload, a.has_crc) == (
            b.lsn,
            b.xid,
            int(b.rtype),
            b.payload,
            b.has_crc,
        ), f"LSN {a.lsn} 的记录在两种编码下不一致"
    db.close()


def test_binary_stream_applies_and_is_readable(tmp_path):
    """二进制流真正应用到一个副本：行数与内容都要对。"""
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
    for i in range(40):
        primary.execute_one(f"INSERT INTO t VALUES ({i}, 'user{i}')")
    server = WALServer(primary).start()
    replica = None
    try:
        replica = Replica.bootstrap_from_server(
            server.host, server.port, tmp_path / "replica", binary_records=True
        )
        # 基准备份已经把 40 行带过去了；再写 10 行，验证增量流（二进制帧）。
        for i in range(40, 50):
            primary.execute_one(f"INSERT INTO t VALUES ({i}, 'user{i}')")
        applied = 0
        while True:
            n = replica.replicate_from_server(
                server.host, server.port, max_records=200, binary_records=True
            )
            if n == 0:
                break
            applied += n
        assert applied > 0
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(50,)]
        assert replica.execute_one("SELECT name FROM t WHERE id = 49").rows == [
            ("user49",)
        ]
        # 只读副本继续拒绝写入。
        try:
            replica.execute_one("INSERT INTO t VALUES (99, 'no')")
        except Exception as exc:  # 只读副本必须拒绝
            assert "只读" in str(exc) or "read-only" in str(exc).lower()
        else:  # pragma: no cover
            raise AssertionError("只读副本接受了写入")
    finally:
        if replica is not None:
            replica.close()
        server.stop()
        primary.close()
