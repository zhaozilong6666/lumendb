"""``atomic_replace`` 的瞬时锁重试语义。"""

from __future__ import annotations

import pytest

from lumendb.replication._atomic import atomic_replace


def test_atomic_replace_succeeds_after_transient_permission_error(
    tmp_path, monkeypatch
):
    src = tmp_path / "a.tmp"
    dst = tmp_path / "a.json"
    src.write_text("1", encoding="utf-8")
    calls = []

    import os as _os

    real_replace = _os.replace

    def flaky_replace(s, d):
        calls.append((str(s), str(d)))
        if len(calls) == 1:
            raise PermissionError("transient lock")
        return real_replace(s, d)

    monkeypatch.setattr("os.replace", flaky_replace)
    atomic_replace(src, dst, tries=3, delay=0.0)
    assert len(calls) == 2
    assert dst.read_text(encoding="utf-8") == "1"


def test_atomic_replace_raises_after_retries_exhausted(tmp_path, monkeypatch):
    src = tmp_path / "a.tmp"
    dst = tmp_path / "a.json"
    src.write_text("1", encoding="utf-8")

    def always_fail(s, d):
        raise PermissionError("locked forever")

    monkeypatch.setattr("os.replace", always_fail)
    with pytest.raises(PermissionError):
        atomic_replace(src, dst, tries=3, delay=0.0)
    # 失败路径不吞错：目标文件未被改写。
    assert not dst.exists()
    assert src.exists()
