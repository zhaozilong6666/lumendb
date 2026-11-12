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


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------


class StorageError(LumenError):
    sqlstate = "58000"


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
    "StorageError",
    "PageNotFoundError",
    "CorruptedPageError",
    "RowTooLargeError",
    "BTreeInvariantError",
    "TransactionError",
    "DeadlockError",
    "SerializationError",
    "TransactionAbortedError",
    "ProtocolError",
    "ClientDisconnected",
]
