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


def cmd_exec(args: argparse.Namespace) -> int:
    db = Database(args.path, pool_size=args.pool_size)
    try:
        code = 0
        for statement_sql in _split_input(args.sql):
            try:
                results = db.execute(statement_sql)
                for result in results:
                    _print_result(result)
            except LumenError as exc:
                print(f"错误：{exc.message}", file=sys.stderr)
                if exc.detail:
                    print(f"DETAIL: {exc.detail}", file=sys.stderr)
                if exc.hint:
                    print(f"HINT: {exc.hint}", file=sys.stderr)
                code = 1
        return code
    finally:
        db.close()


def cmd_shell(args: argparse.Namespace) -> int:
    db = Database(args.path, pool_size=args.pool_size)
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
    db = Database(args.path, pool_size=args.pool_size)
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
        if name == "exec":
            p.add_argument("sql", help="要执行的 SQL")
        if name == "serve":
            p.add_argument("--host", default="127.0.0.1")
            p.add_argument("--port", type=int, default=5432)
        p.set_defaults(handler=handler)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
