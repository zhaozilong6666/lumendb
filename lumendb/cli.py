"""命令行入口：交互式 shell、单条 SQL 执行、启动协议服务端。

    python -m lumendb shell  --path data/mydb
    python -m lumendb exec   --path data/mydb "SELECT 1"
    python -m lumendb serve  --path data/mydb --port 5432
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Optional

from .engine.database import Database
from .errors import LumenError
from .replication.replica import Replica
from .server.pgwire import PGWireServer


def _format_table(columns, rows) -> str:
    if not columns:
        return ""
    headers = [c.name for c in columns]
    widths = [len(h) for h in headers]
    rendered: list[list[str]] = []
    for row in rows:
        cells = [("NULL" if v is None else str(v)) for v in row]
        rendered.append(cells)
        for i, cell in enumerate(cells):
            widths[i] = max(widths[i], len(cell))

    line = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out = [line, "|" + "|".join(f" {h.ljust(w)} " for h, w in zip(headers, widths)) + "|", line]
    for cells in rendered:
        out.append("|" + "|".join(f" {c.ljust(w)} " for c, w in zip(cells, widths)) + "|")
    out.append(line)
    return "\n".join(out)


def _print_result(result, *, show_tag: bool = True) -> None:
    if result.columns:
        print(_format_table(result.columns, result.rows))
    if show_tag and result.tag:
        print(result.tag)


def _open_database(args: argparse.Namespace) -> Database:
    """按命令行参数打开数据库（三个子命令共用同一套 fence/只读语义）。"""
    return Database(
        args.path,
        pool_size=args.pool_size,
        read_only=bool(getattr(args, "read_only", False)),
        fence_path=getattr(args, "fence_path", None),
        node_id=getattr(args, "node_id", "primary"),
        concurrent_dml=bool(getattr(args, "concurrent_dml", False)),
    )


def cmd_exec(args: argparse.Namespace) -> int:
    db = _open_database(args)
    try:
        code = 0
        for statement_sql in _split_input(args.sql):
            try:
                results = db.execute(statement_sql)
                for result in results:
                    _print_result(result)
            except LumenError as exc:
                _report(exc)
                code = 1
        return code
    finally:
        db.close()


def cmd_shell(args: argparse.Namespace) -> int:
    db = _open_database(args)
    print("LumenDB 交互式 shell。输入 SQL 以分号结束；\\q 退出，\\dt 查看表。")
    buffer: list[str] = []
    try:
        while True:
            prompt = "lumendb> " if not buffer else "      ..> "
            try:
                line = input(prompt)
            except (EOFError, KeyboardInterrupt):
                print()
                break

            stripped = line.strip()
            if not buffer:
                if stripped in ("\\q", "quit", "exit"):
                    break
                if stripped == "\\dt":
                    try:
                        result = db.execute_one("SHOW TABLES")
                        _print_result(result)
                    except LumenError as exc:
                        print(f"错误：{exc.message}")
                    continue
                if stripped in ("\\help", "\\h"):
                    print("\\dt 列出表；\\q 退出。SQL 以分号结束即可执行。")
                    continue
                if not stripped:
                    continue

            buffer.append(line)
            sql = "\n".join(buffer)
            if not sql.rstrip().endswith(";"):
                continue
            buffer.clear()

            try:
                for result in db.execute(sql):
                    _print_result(result)
            except LumenError as exc:
                print(f"错误：{exc.message}")
                if exc.detail:
                    print(f"DETAIL: {exc.detail}")
                if exc.hint:
                    print(f"HINT: {exc.hint}")
    finally:
        db.close()
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    db = _open_database(args)
    server = PGWireServer(db, host=args.host, port=args.port)
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        print("\n已停止")
    finally:
        db.close()
    return 0


def _split_input(sql: str) -> list[str]:
    """按分号切成语句，但跳过字符串字面量里的分号。"""
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    for ch in sql:
        if ch == "'":
            in_string = not in_string
            current.append(ch)
            continue
        if ch == ";" and not in_string:
            text = "".join(current).strip()
            if text:
                statements.append(text)
            current = []
            continue
        current.append(ch)
    text = "".join(current).strip()
    if text:
        statements.append(text)
    return statements


def cmd_rejoin(args: argparse.Namespace) -> int:
    """把本地目录（曾是主库/旧副本）归档，再从新主库全量重建为只读副本。

    为什么必须全量：旧主库可能留下"分叉尾巴"（崩溃前已提交、从未复制出去
    的事务），页级增量重建只覆盖新主库 pageLSN 更新过的页，分叉页不会被覆盖。
    旧目录整体归档（默认同级 ``<name>.stale-<时间戳>``），不删除。
    """
    replica, archived = Replica.rejoin_from_server(
        args.host,
        args.port,
        args.path,
        archive_dir=args.archive_dir,
        token=args.token,
        compress=args.compress,
        binary_records=args.binary,
        replica_id=args.replica_id,
        timeout=args.timeout,
    )
    try:
        if archived is not None:
            print(f"旧目录已归档到：{archived}")
        else:
            print("目标目录原本为空，无需归档")
        print(
            f"已从 {args.host}:{args.port} 全量重建为只读副本，"
            f"applied_lsn={replica.applied_lsn}"
        )
    finally:
        replica.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lumendb", description="LumenDB 命令行工具")
    sub = parser.add_subparsers(dest="command", required=True)

    for name, handler, help_text in (
        ("shell", cmd_shell, "进入交互式 shell"),
        ("exec", cmd_exec, "执行一条或多条 SQL"),
        ("serve", cmd_serve, "启动 PostgreSQL 协议服务端"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--path", type=Path, default=Path("lumendb_data"), help="数据目录")
        p.add_argument("--pool-size", type=int, default=64, help="缓冲池页数")
        # 共享 fencing marker：给了就必须能挡住"重启后的旧主库"。
        # 之前这三个命令都不接受它，`Database(fence_path=...)` 的保护
        # 只有写代码的人才能用到，用 CLI 拉起的旧主库重启后照样能双写。
        p.add_argument(
            "--fence-path",
            type=Path,
            default=None,
            help="共享提升 marker（fence）路径；旧主库会据此拒绝写入",
        )
        p.add_argument(
            "--node-id",
            default="primary",
            help="本节点在 fence 里的身份（默认 primary）",
        )
        p.add_argument(
            "--read-only",
            action="store_true",
            help="以只读方式打开（副本/观察者）",
        )
        p.add_argument(
            "--concurrent-dml",
            action="store_true",
            help="开启并发 DML（opt-in；保存点仍要求独占，VACUUM 仍要求无活跃事务）",
        )
        if name == "exec":
            p.add_argument("sql", help="要执行的 SQL")
        if name == "serve":
            p.add_argument("--host", default="127.0.0.1")
            p.add_argument("--port", type=int, default=5432)
        p.set_defaults(handler=handler)

    # 重新加入集群：不与上面三个共用 --fence-path / --node-id / --read-only
    # （重建之后本来就是一个只读副本，fence 由新主库持有）。
    p = sub.add_parser(
        "rejoin", help="归档本地目录并从新主库全量重建为只读副本"
    )
    p.add_argument(
        "--path", type=Path, default=Path("lumendb_data"), help="本地数据目录（会被归档）"
    )
    p.add_argument("--host", required=True, help="新主库地址")
    p.add_argument("--port", type=int, required=True, help="新主库复制流端口")
    p.add_argument(
        "--archive-dir",
        type=Path,
        default=None,
        help="旧目录归档位置（默认同级 <name>.stale-<时间戳>）",
    )
    p.add_argument("--token", default=None, help="复制流共享 token")
    p.add_argument("--replica-id", default=None, help="槽位 id")
    p.add_argument("--compress", action="store_true", help="复制流走 zlib")
    p.add_argument("--binary", action="store_true", help="记录走二进制帧")
    p.add_argument("--timeout", type=float, default=10.0)
    p.set_defaults(handler=cmd_rejoin)
    return parser


def _report(exc: LumenError) -> None:
    print(f"错误：{exc.message}", file=sys.stderr)
    if exc.detail:
        print(f"DETAIL: {exc.detail}", file=sys.stderr)
    if exc.hint:
        print(f"HINT: {exc.hint}", file=sys.stderr)


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except LumenError as exc:
        # 打开数据库本身也可能被拒（被 fence 的旧主库、目录被另一个进程占用、
        # 只读库的写语句……）。这些是预期内的用户错误，不应该甩 traceback。
        _report(exc)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
