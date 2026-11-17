"""CLI 的 fence/只读参数，以及"已提升节点拒绝被重建"的守卫。

两条都是"能力写进了库、从入口够不着"的典型：

* `Database(fence_path=...)` 在打开时校验共享 fence，重启后的旧主库才能
  发现自己已被替代。但 `shell`/`exec`/`serve` 之前都不接受这个参数，
  用 CLI 拉起的旧主库重启后照样双写——保护只对写代码的人可见。
* 分叉自愈会在后台用 leader 的基准备份替换数据目录。若本节点已经提升为
  可写主库，这条路径会把"已经对外确认过的写入"直接抹掉。
"""

from __future__ import annotations

import json

import pytest

from lumendb.cli import build_parser, main
from lumendb.engine.database import Database
from lumendb.errors import NotSupportedError
from lumendb.replication import Replica


def _args(*argv):
    return build_parser().parse_args(list(argv))


class TestCliFence:
    def test_serve_accepts_fence_and_read_only_flags(self, tmp_path):
        args = _args(
            "serve",
            "--path", str(tmp_path / "db"),
            "--fence-path", str(tmp_path / "fence.json"),
            "--node-id", "node-b",
            "--read-only",
        )
        assert args.fence_path == tmp_path / "fence.json"
        assert args.node_id == "node-b"
        assert args.read_only is True

    def test_all_commands_share_the_same_flags(self, tmp_path):
        for command in (["exec", "SELECT 1"], ["shell"]):
            args = _args(command[0], *command[1:], "--fence-path", str(tmp_path / "f.json"))
            assert args.fence_path == tmp_path / "f.json"

    def test_exec_refuses_to_write_when_another_node_is_promoted(self, tmp_path):
        path = tmp_path / "db"
        fence = tmp_path / "fence.json"
        db = Database(path, checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.close()

        # 另一个节点已经提升：marker 指向 node-b
        fence.write_text(
            json.dumps({"promoted_node": "node-b", "lsn": 10}), encoding="utf-8"
        )
        code = main([
            "exec",
            "--path", str(path),
            "--fence-path", str(fence),
            "--node-id", "node-a",
            "INSERT INTO t VALUES (1)",
        ])
        assert code == 1, "被 fence 的节点必须拒绝写入"

        # 同一个库，不带 fence 就没有保护（这正是之前 CLI 的默认行为）
        assert main([
            "exec", "--path", str(path), "INSERT INTO t VALUES (1)",
        ]) == 0

    def test_exec_opens_read_only_when_asked(self, tmp_path):
        path = tmp_path / "db"
        db = Database(path, checkpoint_wal_bytes=0)
        db.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        db.close()
        assert main([
            "exec", "--path", str(path), "--read-only", "INSERT INTO t VALUES (1)",
        ]) == 1


class TestPromotedReplicaRefusesRebuild:
    def test_rebuild_is_refused_while_the_node_is_writable(self, tmp_path):
        primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        try:
            replica = Replica.bootstrap(primary, tmp_path / "replica")
            try:
                replica.promote(node_id="replica")  # 变成可写主库
                assert replica.db._read_only is False
                row = replica.execute_one("INSERT INTO t VALUES (1)")
                assert row.affected == 1

                replica._schedule_rebuild("127.0.0.1", 1, 0)
                assert replica.last_rebuild is None, "已提升节点不能被后台重建"
                assert "promoted" in (replica.last_rebuild_error or "")
                # 数据必须还在（重建没有动过目录）
                assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
            finally:
                replica.close()
        finally:
            primary.close()

    def test_rebuild_still_runs_for_a_read_only_replica(self, tmp_path, monkeypatch):
        primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
        primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
        try:
            replica = Replica.bootstrap(primary, tmp_path / "replica")
            calls: list[tuple] = []

            def fake_rebase(host, port, **kwargs):
                calls.append((host, port))
                return 0

            monkeypatch.setattr(replica, "rebase_from_server", fake_rebase)
            try:
                replica._schedule_rebuild("127.0.0.1", 1, 0)
                for _ in range(100):
                    if calls:
                        break
                    import time

                    time.sleep(0.02)
                assert calls == [("127.0.0.1", 1)]
                assert replica.last_rebuild is not None
            finally:
                replica.close()
        finally:
            primary.close()
