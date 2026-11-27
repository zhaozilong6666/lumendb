"""持久提交账本的格式、保留窗口与损坏检测。"""

from __future__ import annotations

import pytest

from lumendb.errors import StorageError
from lumendb.storage.commit_ledger import CommitLedger


def test_commit_ledger_roundtrip_and_retention(tmp_path):
    path = tmp_path / "commit-ledger.bin"
    ledger = CommitLedger(path, max_entries=3)
    retained = ledger.write([(xid, xid * 10) for xid in range(1, 6)])
    assert retained == [(3, 30), (4, 40), (5, 50)]

    reopened = CommitLedger(path, max_entries=3)
    assert reopened.entries() == retained

    # dict 入口与 list 等价；重复 xid 取更大的 LSN。
    retained = reopened.write({1: 5, 3: 33, 6: 60, 7: 70})
    assert retained == [(3, 33), (6, 60), (7, 70)]
    assert CommitLedger(path, max_entries=3).entries() == retained


def test_commit_ledger_rejects_corrupted_file(tmp_path):
    path = tmp_path / "commit-ledger.bin"
    path.write_bytes(b"not-a-lumen-ledger")
    with pytest.raises(StorageError, match="被截断|魔数"):
        CommitLedger(path)

    ledger = CommitLedger(tmp_path / "empty.bin", max_entries=2)
    ledger.write([(1, 10)])
    data = (tmp_path / "empty.bin").read_bytes()
    (tmp_path / "empty.bin").write_bytes(data + b"\x00")
    with pytest.raises(StorageError, match="长度不符"):
        CommitLedger(tmp_path / "empty.bin")
