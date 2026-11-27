"""scripts/ 下的门禁脚本自身也要有护栏。

变异脚本最常见的失效方式是"清单腐烂"：源码改了，`original` 片段再也匹配不上，
脚本就静默跳过那条变异——门禁仍然是绿的，但什么都没检查。这里把清单有效性
钉成用例，并确认门禁脚本能被子进程正常调用。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
        # 子进程把中文写进管道时，Windows 默认用 locale 编码（cp936）；
        # 我们不显式指定的话，父进程按 utf-8 解码会得到乱码，
        # 于是"断言输出里含某个中文短语"会**只在整库跑时**假红。
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )


def test_mutation_manifest_is_still_valid():
    result = _run(str(SCRIPTS / "mutation_check.py"), "--check")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "条变异全部有效" in result.stdout


def test_gates_help_lists_the_documented_flags():
    result = _run(str(SCRIPTS / "gates.py"), "--help")
    assert result.returncode == 0
    for flag in ("--skip-tests", "--full"):
        assert flag in result.stdout
