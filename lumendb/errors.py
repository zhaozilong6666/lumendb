"""LumenDB 的错误体系。

设计原则：**不静默降级**。任何未实现的语法、任何类型不匹配、任何越界写入，
都必须抛出一个携带 SQLSTATE 的错误，而不是返回近似结果。

SQLSTATE 取值参考 PostgreSQL，以便客户端（psql / DBeaver / JDBC 驱动）
能够正确识别错误类别。
"""

from __future__ import annotations

from typing import Optional


class LumenError(Exception):
    """所有 LumenDB 错误的基类。"""

    sqlstate: str = "XX000"
    #: 面向用户的简短分类，出现在协议层的 ``S`` 字段
    severity: str = "ERROR"

    def __init__(
        self,
        message: str,
        *,
        detail: Optional[str] = None,
        hint: Optional[str] = None,
        position: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail
        self.hint = hint
        self.position = position

    def __str__(self) -> str:  # pragma: no cover - 仅用于日志
        text = self.message
        if self.detail:
            text += f"\nDETAIL: {self.detail}"
        if self.hint:
            text += f"\nHINT: {self.hint}"
        return text


# ---------------------------------------------------------------------------
# SQL 语法与语义
# ---------------------------------------------------------------------------


class ParserError(LumenError):
    """词法/语法错误。"""

    sqlstate = "42601"


class SQLSyntaxError(ParserError):
    """词法或语法分析失败（``ParserError`` 的具名子类，便于上层捕获）。"""

    sqlstate = "42601"


class BinderError(LumenError):
    """名字解析或类型检查失败。"""

    sqlstate = "42000"


class ColumnNotFoundError(BinderError):
    sqlstate = "42703"


class AmbiguousColumnError(BinderError):
    sqlstate = "42702"


class TypeMismatchError(BinderError):
    sqlstate = "42804"


class UndefinedParameterError(BinderError):
    """``$n`` 没有对应的绑定值。"""

    sqlstate = "42P02"


class NotSupportedError(LumenError):
    """语法本身合法，但本引擎尚未实现。"""

    sqlstate = "0A000"


# ---------------------------------------------------------------------------
# 目录（catalog）
# ---------------------------------------------------------------------------


class CatalogError(LumenError):
    sqlstate = "XX000"


class TableNotFoundError(CatalogError):
    sqlstate = "42P01"


class IndexNotFoundError(CatalogError):
    sqlstate = "42704"


class TableAlreadyExistsError(CatalogError):
    sqlstate = "42P07"


class IndexAlreadyExistsError(CatalogError):
    sqlstate = "42P07"


# ---------------------------------------------------------------------------
# 约束
# ---------------------------------------------------------------------------


class ConstraintViolationError(LumenError):
    sqlstate = "23000"


class DuplicateKeyError(ConstraintViolationError):
    sqlstate = "23505"


class NotNullViolationError(ConstraintViolationError):
    sqlstate = "23502"


class CheckViolationError(ConstraintViolationError):
    sqlstate = "23514"


class ForeignKeyViolationError(ConstraintViolationError):
    sqlstate = "23503"


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------


class StorageError(LumenError):
    sqlstate = "58000"


class RedirectError(StorageError):
    """复制流目标已迁移；调用方应连接新的 host/port。"""

    def __init__(self, host: str, port: int) -> None:
        super().__init__(f"复制流已重定向到 {host}:{port}")
        self.host = host
        self.port = int(port)


class PageNotFoundError(StorageError):
    sqlstate = "58030"


class CorruptedPageError(StorageError):
    """页自校验失败（page_id 与实际位置不符、魔数错误等）。"""

    sqlstate = "XX002"


class RowTooLargeError(StorageError):
    """单行超出页容量。我们选择明确拒绝，而不是截断数据。"""

    sqlstate = "54000"


class BTreeInvariantError(StorageError):
    """B+ 树结构不变量被破坏——出现即说明有 bug，测试里会断言它不出现。"""

    sqlstate = "XX001"


# ---------------------------------------------------------------------------
# 事务
# ---------------------------------------------------------------------------


class TransactionError(LumenError):
    sqlstate = "40000"


class DeadlockError(TransactionError):
    sqlstate = "40P01"


class SerializationError(TransactionError):
    sqlstate = "40001"


class CommitOutcomeUnknown(TransactionError):
    """本地已经提交（COMMIT 记录已 fsync），但多数派水位没在超时内追平。

    调用方不能把它当成"没写进去"：数据可能已经在本地持久化、并且随后会被
    复制出去。正确处理是重新读一次确认，或把结果交给幂等重试逻辑。
    """

    sqlstate = "40003"  # statement_completion_unknown


class TransactionAbortedError(TransactionError):
    """事务已处于 aborted 状态，只允许 ROLLBACK。"""

    sqlstate = "25P02"


# ---------------------------------------------------------------------------
# 协议
# ---------------------------------------------------------------------------


class ProtocolError(LumenError):
    sqlstate = "08P01"


class ClientDisconnected(ProtocolError):
    """客户端正常或异常断开，用于跳出读循环，不属于错误上报。"""

    sqlstate = "08006"


# ---------------------------------------------------------------------------
# 重试契约
# ---------------------------------------------------------------------------
#
# 应用侧最常问的一句是"这个错能不能重试"。答案不能靠读源码猜，必须集中声明：
# 每个 SQLSTATE 有且只有一种策略，`tests/unit/test_retry_contract.py` 会遍历
# 全部异常类来保证没有遗漏（新增错误码忘了登记就会红）。

#: 重放整条语句 (事务仍然有效，仅当错误发生在写入之前时使用)
RETRY_STATEMENT = "retry_statement"
#: 重放整个事务 (事务已 abort，必须重来)
RETRY_TRANSACTION = "retry_transaction"
#: 结果未知：先读一次确认，再决定是否重放 (绝不能当成"没写进去")
RETRY_RECHECK = "recheck"
#: 不重试：确定性错误或需要人工介入
NO_RETRY = "no_retry"

RETRY_POLICIES = (RETRY_STATEMENT, RETRY_TRANSACTION, RETRY_RECHECK, NO_RETRY)

#: SQLSTATE -> 策略。'55P03' 来自 storage/lock.py 的 LockTimeoutError。
RETRY_POLICY: dict[str, str] = {
    # 并发冲突：事务已 abort，整事务重来
    "40001": RETRY_TRANSACTION,   # serialization_failure
    "40P01": RETRY_TRANSACTION,   # deadlock_detected
    "55P03": RETRY_TRANSACTION,   # lock_not_available（锁等待超时）
    "55006": RETRY_TRANSACTION,   # object_in_use（数据目录被占用）
    # 提交结果未知：先读确认，再决定
    "40003": RETRY_RECHECK,       # statement_completion_unknown
    # 语句写错了：确定性，重试无用
    "42601": NO_RETRY,            # syntax_error
    "42000": NO_RETRY,            # syntax_error_or_access_rule_violation
    "42703": NO_RETRY,            # undefined_column
    "42702": NO_RETRY,            # ambiguous_column
    "42804": NO_RETRY,            # datatype_mismatch
    "42P02": NO_RETRY,            # undefined_parameter
    "42P01": NO_RETRY,            # undefined_table
    "42704": NO_RETRY,            # undefined_object
    "42P07": NO_RETRY,            # duplicate_table
    "0A000": NO_RETRY,            # feature_not_supported
    # 约束冲突：重试只会得到同一个错
    "23000": NO_RETRY,
    "23505": NO_RETRY,            # unique_violation
    "23502": NO_RETRY,            # not_null_violation
    "23514": NO_RETRY,            # check_violation
    "23503": NO_RETRY,            # foreign_key_violation
    # 事务状态错：必须先 ROLLBACK
    "25P02": NO_RETRY,            # in_failed_sql_transaction
    "40000": NO_RETRY,            # transaction_rollback
    # 存储/数据损坏：需要人工介入
    "58000": NO_RETRY,
    "58030": NO_RETRY,
    "54000": NO_RETRY,            # program_limit_exceeded（行过大）
    "XX000": NO_RETRY,
    "XX001": NO_RETRY,            # data_corrupted
    "XX002": NO_RETRY,
    # 协议/连接：重连而不是重试语句
    "08P01": NO_RETRY,
    "08006": NO_RETRY,
}


def retry_policy(error: BaseException) -> str:
    """返回该异常的重试策略；未登记的 SQLSTATE 一律 ``NO_RETRY``。"""
    code = getattr(error, "sqlstate", None)
    if not isinstance(code, str):
        return NO_RETRY
    return RETRY_POLICY.get(code, NO_RETRY)


__all__ = [
    "LumenError",
    "ParserError",
    "SQLSyntaxError",
    "BinderError",
    "ColumnNotFoundError",
    "AmbiguousColumnError",
    "TypeMismatchError",
    "NotSupportedError",
    "CatalogError",
    "TableNotFoundError",
    "IndexNotFoundError",
    "TableAlreadyExistsError",
    "IndexAlreadyExistsError",
    "ConstraintViolationError",
    "DuplicateKeyError",
    "NotNullViolationError",
    "CheckViolationError",
    "ForeignKeyViolationError",
    "StorageError",
    "PageNotFoundError",
    "CorruptedPageError",
    "RowTooLargeError",
    "BTreeInvariantError",
    "TransactionError",
    "DeadlockError",
    "CommitOutcomeUnknown",
    "SerializationError",
    "TransactionAbortedError",
    "ProtocolError",
    "ClientDisconnected",
    "NO_RETRY",
    "RETRY_POLICY",
    "RETRY_POLICIES",
    "RETRY_RECHECK",
    "RETRY_STATEMENT",
    "RETRY_TRANSACTION",
    "retry_policy",
]
