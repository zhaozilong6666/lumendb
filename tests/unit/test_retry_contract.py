"""重试契约：每个 SQLSTATE 有且只有一种策略，且必须被显式登记。

为什么需要这条用例：应用侧最常问"这个错能不能重试"。如果答案靠读源码猜，
结果就是"有人重试了确定性错误（白跑）"或"有人把结果未知当成失败（丢数据）"。
这里遍历**全部**异常类，任何新增错误码忘了登记 `RETRY_POLICY` 都会红。
"""

from __future__ import annotations

import pytest

from lumendb import errors
from lumendb.errors import (
    NO_RETRY,
    RETRY_POLICIES,
    RETRY_POLICY,
    RETRY_RECHECK,
    RETRY_TRANSACTION,
    LumenError,
    retry_policy,
)
from lumendb.storage.lock import LockTimeoutError


def _all_error_classes() -> list[type]:
    classes: list[type] = [LumenError, LockTimeoutError]
    pending: list[type] = [LumenError]
    while pending:
        cls = pending.pop()
        for sub in cls.__subclasses__():
            if sub not in classes:
                classes.append(sub)
                pending.append(sub)
    return classes


def test_every_error_code_has_a_declared_policy():
    missing = [
        f"{cls.__name__}({getattr(cls, 'sqlstate', None)})"
        for cls in _all_error_classes()
        if isinstance(getattr(cls, "sqlstate", None), str)
        and cls.sqlstate not in RETRY_POLICY
    ]
    assert not missing, (
        "这些错误码没有重试策略，新增错误时必须登记：" + ", ".join(sorted(missing))
    )


def test_every_class_declares_a_sqlstate():
    """没有 sqlstate 的类会静默退化成 no_retry，等于契约漏了一条。"""
    missing = [
        cls.__name__
        for cls in _all_error_classes()
        if cls is not LumenError
        and not isinstance(getattr(cls, "sqlstate", None), str)
    ]
    assert not missing, missing


def test_policy_values_are_from_the_closed_set():
    for code, policy in RETRY_POLICY.items():
        assert policy in RETRY_POLICIES, f"{code} 的策略 {policy!r} 不在允许集合里"


def test_unknown_code_defaults_to_no_retry():
    class Weird(LumenError):
        sqlstate = "ZZ999"

    assert retry_policy(Weird("x")) == NO_RETRY


def test_non_lumen_exception_defaults_to_no_retry():
    assert retry_policy(ValueError("boom")) == NO_RETRY


@pytest.mark.parametrize(
    ("cls", "expected"),
    [
        (errors.SerializationError, RETRY_TRANSACTION),
        (errors.DeadlockError, RETRY_TRANSACTION),
        (errors.CommitOutcomeUnknown, RETRY_RECHECK),
        (errors.DuplicateKeyError, NO_RETRY),
        (errors.NotNullViolationError, NO_RETRY),
        (errors.ForeignKeyViolationError, NO_RETRY),
        (errors.ColumnNotFoundError, NO_RETRY),
        (errors.NotSupportedError, NO_RETRY),
        (errors.CorruptedPageError, NO_RETRY),
        (LockTimeoutError, RETRY_TRANSACTION),
    ],
)
def test_documented_policies(cls, expected):
    assert retry_policy(cls("x")) == expected


def test_policy_is_derived_from_the_code_not_the_class_name():
    """重试策略只认 SQLSTATE：子类/改名都不该改变语义。"""

    class Renamed(errors.SerializationError):
        pass

    assert retry_policy(Renamed("x")) == RETRY_TRANSACTION
