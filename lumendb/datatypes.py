"""类型系统与行内编码。

LumenDB 的类型集合刻意保持小：INT / BIGINT / FLOAT / BOOLEAN / VARCHAR / TEXT。
小集合的好处是每一种编码都可以被穷举测试。

编码约定（与 docs/storage-format.md 一致）：

* ``INT`` / ``BIGINT``   —— zigzag + 7 位延续位的变长整数
* ``FLOAT``              —— IEEE754 double，8 字节定长
* ``BOOLEAN``            —— 1 字节
* ``VARCHAR`` / ``TEXT`` —— 无符号 varint 长度前缀 + UTF-8 字节

``NULL`` 不参与编码，由记录头部的位图表示。
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import Enum
from typing import Any, Tuple

from .errors import TypeMismatchError, NotSupportedError

INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


class DataType(str, Enum):
    INT = "INT"
    BIGINT = "BIGINT"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"

    @property
    def is_numeric(self) -> bool:
        return self in (DataType.INT, DataType.BIGINT, DataType.FLOAT)

    @property
    def is_string(self) -> bool:
        return self in (DataType.VARCHAR, DataType.TEXT)


#: 类型提升顺序：越靠后"越宽"
_NUMERIC_RANK = {DataType.INT: 0, DataType.BIGINT: 1, DataType.FLOAT: 2}


@dataclass(frozen=True)
class TypeSpec:
    """一个列的具体类型，包含 VARCHAR 的长度上限。

    ``unknown`` 是 SQL 的 NULL 字面量类型：它只在绑定阶段存在，表示
    “还不知道具体类型”，需要在集合运算、CASE 分支等位置被另一侧的类型
    吸收。它不会进入表定义，也不会参与存储编码。
    """

    kind: DataType
    length: int | None = None
    unknown: bool = False

    def __post_init__(self) -> None:
        if self.unknown:
            if self.kind is not DataType.TEXT or self.length is not None:
                raise NotSupportedError(
                    "NULL 未知类型必须是 TEXT 且不接受长度",
                    detail=f"收到 {self.kind.value} / length={self.length}",
                )
            return
        if self.kind is DataType.VARCHAR:
            if self.length is None:
                raise NotSupportedError(
                    "VARCHAR 必须声明长度",
                    hint="使用 VARCHAR(n)，或改用 TEXT 表示不限长度字符串",
                )
            if self.length <= 0:
                raise NotSupportedError(
                    f"VARCHAR 长度必须为正数，收到 {self.length}",
                )
        elif self.length is not None:
            raise NotSupportedError(
                f"{self.kind.value} 不接受长度参数",
                detail=f"收到 length={self.length}",
            )

    # -- 展示 ---------------------------------------------------------------

    @property
    def name(self) -> str:
        if self.unknown:
            return "NULL"
        if self.kind is DataType.VARCHAR:
            return f"VARCHAR({self.length})"
        return self.kind.value

    def __str__(self) -> str:  # pragma: no cover - 便捷
        return self.name

    @property
    def sql(self) -> str:
        """SQL 里书写这个类型时的文本（用于报错与 EXPLAIN）。"""
        return self.name

    @property
    def is_numeric(self) -> bool:
        return self.kind.is_numeric

    @property
    def is_string(self) -> bool:
        return self.kind.is_string

    # -- 解析 ---------------------------------------------------------------

    @classmethod
    def parse(cls, text: str) -> "TypeSpec":
        """把 ``INT`` / ``VARCHAR(64)`` 这样的类型文本解析成 TypeSpec。"""
        raw = text.strip()
        upper = raw.upper()

        base, _, tail = upper.partition("(")
        base = base.strip()
        args = tail.rstrip(")").strip() if tail else ""

        if base in ("INT", "INTEGER"):
            return cls(DataType.INT)
        if base in ("BIGINT", "LONG"):
            return cls(DataType.BIGINT)
        if base in ("FLOAT", "DOUBLE", "REAL", "DOUBLE PRECISION"):
            return cls(DataType.FLOAT)
        if base in ("BOOL", "BOOLEAN"):
            return cls(DataType.BOOLEAN)
        if base == "TEXT":
            return cls(DataType.TEXT)
        if base in ("VARCHAR", "CHAR", "CHARACTER VARYING", "STRING"):
            if not args:
                raise NotSupportedError(
                    f"{base} 必须声明长度",
                    hint="例如 VARCHAR(64)，或使用 TEXT",
                )
            try:
                n = int(args)
            except ValueError as exc:
                raise NotSupportedError(f"VARCHAR 长度不是整数：{args!r}") from exc
            return cls(DataType.VARCHAR, n)

        raise NotSupportedError(
            f"不支持的类型：{text!r}",
            hint="支持 INT / BIGINT / FLOAT / BOOLEAN / VARCHAR(n) / TEXT",
        )

    # -- 值规整与校验 --------------------------------------------------------

    def coerce(self, value: Any) -> Any:
        """把 Python 值规整成该类型的规范表示，越界则报错。

        注意：这里不做"静默截断"。``VARCHAR(10)`` 收到 11 个字符时报错，
        而不是悄悄切成 10 个字符——后者会让用户以为数据存进去了。
        """
        if value is None:
            return None

        kind = self.kind
        if self.unknown:
            raise TypeMismatchError(
                "NULL 未知类型只能表示 SQL NULL",
                detail=f"收到非 NULL 值 {value!r}",
            )

        if kind is DataType.INT:
            if isinstance(value, bool):
                raise TypeMismatchError("BOOLEAN 不能隐式转换为 INT")
            if isinstance(value, float):
                raise TypeMismatchError(
                    "FLOAT 不能隐式转换为 INT",
                    hint="使用 CAST(x AS INT) 显式转换",
                )
            try:
                ivalue = int(value)
            except (TypeError, ValueError) as exc:
                raise TypeMismatchError(f"无法把 {value!r} 解释为 INT") from exc
            if not (INT32_MIN <= ivalue <= INT32_MAX):
                raise TypeMismatchError(
                    f"{ivalue} 超出 INT 范围 [{INT32_MIN}, {INT32_MAX}]",
                    hint="改用 BIGINT",
                )
            return ivalue

        if kind is DataType.BIGINT:
            if isinstance(value, bool):
                raise TypeMismatchError("BOOLEAN 不能隐式转换为 BIGINT")
            if isinstance(value, float):
                raise TypeMismatchError("FLOAT 不能隐式转换为 BIGINT")
            try:
                ivalue = int(value)
            except (TypeError, ValueError) as exc:
                raise TypeMismatchError(f"无法把 {value!r} 解释为 BIGINT") from exc
            if not (INT64_MIN <= ivalue <= INT64_MAX):
                raise TypeMismatchError(f"{ivalue} 超出 BIGINT 范围")
            return ivalue

        if kind is DataType.FLOAT:
            if isinstance(value, bool):
                raise TypeMismatchError("BOOLEAN 不能隐式转换为 FLOAT")
            try:
                return float(value)
            except (TypeError, ValueError) as exc:
                raise TypeMismatchError(f"无法把 {value!r} 解释为 FLOAT") from exc

        if kind is DataType.BOOLEAN:
            if isinstance(value, bool):
                return value
            if isinstance(value, int) and value in (0, 1):
                return bool(value)
            raise TypeMismatchError(
                f"{value!r} 不是合法的 BOOLEAN",
                hint="使用 TRUE / FALSE",
            )

        if kind in (DataType.VARCHAR, DataType.TEXT):
            if not isinstance(value, str):
                raise TypeMismatchError(f"{value!r} 不是字符串")
            if kind is DataType.VARCHAR and len(value) > (self.length or 0):
                raise TypeMismatchError(
                    f"字符串长度 {len(value)} 超过 {self.name} 的上限",
                    hint="缩短内容，或把列类型改为 TEXT",
                )
            return value

        raise NotSupportedError(f"未实现的类型：{kind!r}")  # pragma: no cover

    def storage_size_upper_bound(self) -> int:
        """编码后的最大字节数，用于行大小预估。"""
        if self.kind in (DataType.INT, DataType.BIGINT):
            return 10
        if self.kind is DataType.FLOAT:
            return 8
        if self.kind is DataType.BOOLEAN:
            return 1
        if self.kind is DataType.TEXT:
            return 4096  # 实际由页容量最终把关
        return (self.length or 0) * 4 + 4  # UTF-8 最坏 4 字节/字符

    def sort_key(self, value: Any):
        """排序用的键。NULL 由调用方决定放前还是放后。"""
        return value


def common_type(a: TypeSpec, b: TypeSpec) -> TypeSpec:
    """二元运算的类型提升规则。

    只允许无损提升：INT -> BIGINT -> FLOAT。字符串与数值之间必须显式 CAST。
    ``unknown``（SQL NULL 字面量）会被另一侧吸收；两侧都是 unknown 时保持
    unknown，交给更外层的表达式继续推断。
    """
    if a.unknown:
        return b
    if b.unknown:
        return a
    if a.kind is b.kind and a.kind is not DataType.VARCHAR:
        return a
    if a.kind is DataType.VARCHAR and b.kind is DataType.VARCHAR:
        # 取更宽的一侧
        la, lb = a.length or 0, b.length or 0
        return TypeSpec(DataType.VARCHAR, max(la, lb))
    if a.kind in (DataType.TEXT, DataType.VARCHAR) and b.kind in (
        DataType.TEXT,
        DataType.VARCHAR,
    ):
        # VARCHAR 与 TEXT 可以互转；结果取不限长度的 TEXT，避免集合运算
        # 因一侧是 TEXT 就直接报不兼容。
        return TypeSpec(DataType.TEXT)
    if a.kind in (DataType.TEXT, DataType.VARCHAR) or b.kind in (
        DataType.TEXT,
        DataType.VARCHAR,
    ):
        raise TypeMismatchError(
            f"无法在 {a.name} 与 {b.name} 之间做隐式类型提升",
            hint="使用 CAST 显式转换",
        )
    if a.kind is DataType.BOOLEAN or b.kind is DataType.BOOLEAN:
        raise TypeMismatchError(f"BOOLEAN 不参与数值提升（{a.name} vs {b.name}）")
    rank = max(_NUMERIC_RANK[a.kind], _NUMERIC_RANK[b.kind])
    for kind, r in _NUMERIC_RANK.items():
        if r == rank:
            return TypeSpec(kind)
    raise NotSupportedError(f"无法提升 {a.name} 与 {b.name}")  # pragma: no cover


# ---------------------------------------------------------------------------
# varint
# ---------------------------------------------------------------------------


def zigzag_encode(n: int) -> int:
    """有符号 -> 无符号。正数 2n，负数 2|n|-1。"""
    return n * 2 if n >= 0 else -n * 2 - 1


def zigzag_decode(z: int) -> int:
    return (z >> 1) ^ -(z & 1)


def encode_varint(n: int) -> bytes:
    z = zigzag_encode(n)
    out = bytearray()
    while z >= 0x80:
        out.append((z & 0x7F) | 0x80)
        z >>= 7
    out.append(z & 0x7F)
    return bytes(out)


def decode_varint(buf: bytes, pos: int) -> Tuple[int, int]:
    """返回 ``(值, 新位置)``。"""
    z = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise ValueError("varint 截断")
        b = buf[pos]
        pos += 1
        z |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
        if shift > 70:
            raise ValueError("varint 过长")
    return zigzag_decode(z), pos


def encode_uvarint(n: int) -> bytes:
    if n < 0:
        raise ValueError("无符号 varint 不能编码负数")
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n & 0x7F)
    return bytes(out)


def decode_uvarint(buf: bytes, pos: int) -> Tuple[int, int]:
    n = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise ValueError("uvarint 截断")
        b = buf[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
        if shift > 70:
            raise ValueError("uvarint 过长")
    return n, pos


# ---------------------------------------------------------------------------
# 值编码
# ---------------------------------------------------------------------------

_F64 = struct.Struct("<d")


def encode_value(spec: TypeSpec, value: Any) -> bytes:
    """编码一个非 NULL 值。"""
    kind = spec.kind

    if kind is DataType.INT:
        return encode_varint(int(value))
    if kind is DataType.BIGINT:
        return encode_varint(int(value))
    if kind is DataType.FLOAT:
        return _F64.pack(float(value))
    if kind is DataType.BOOLEAN:
        return b"\x01" if value else b"\x00"
    if kind in (DataType.VARCHAR, DataType.TEXT):
        raw = value.encode("utf-8")
        return encode_uvarint(len(raw)) + raw

    raise NotSupportedError(f"未实现的编码类型：{kind!r}")  # pragma: no cover


def decode_value(spec: TypeSpec, buf: bytes, pos: int) -> Tuple[Any, int]:
    """解码一个非 NULL 值，返回 ``(值, 新位置)``。"""
    kind = spec.kind

    if kind in (DataType.INT, DataType.BIGINT):
        n, pos = decode_varint(buf, pos)
        return n, pos
    if kind is DataType.FLOAT:
        if pos + 8 > len(buf):
            raise ValueError("FLOAT 截断")
        return _F64.unpack_from(buf, pos)[0], pos + 8
    if kind is DataType.BOOLEAN:
        if pos >= len(buf):
            raise ValueError("BOOLEAN 截断")
        return bool(buf[pos]), pos + 1
    if kind in (DataType.VARCHAR, DataType.TEXT):
        n, pos = decode_uvarint(buf, pos)
        if pos + n > len(buf):
            raise ValueError("字符串截断")
        return buf[pos : pos + n].decode("utf-8"), pos + n

    raise NotSupportedError(f"未实现的解码类型：{kind!r}")  # pragma: no cover


# ---------------------------------------------------------------------------
# 比较
# ---------------------------------------------------------------------------


def compare_values(a: Any, b: Any, spec: TypeSpec) -> int:
    """返回 -1 / 0 / 1。NULL 的比较由调用方决定，这里拒绝 NULL。"""
    if a is None or b is None:
        raise ValueError("compare_values 不接受 NULL，请先处理空值语义")

    if spec.kind in (DataType.INT, DataType.BIGINT):
        ai, bi = int(a), int(b)
        return (ai > bi) - (ai < bi)
    if spec.kind is DataType.FLOAT:
        af, bf = float(a), float(b)
        return (af > bf) - (af < bf)
    if spec.kind is DataType.BOOLEAN:
        ai, bi = int(bool(a)), int(bool(b))
        return (ai > bi) - (ai < bi)
    if spec.kind in (DataType.VARCHAR, DataType.TEXT):
        return (a > b) - (a < b)

    raise NotSupportedError(f"未实现的比较类型：{spec.kind!r}")  # pragma: no cover


def is_truthy(value: Any) -> bool:
    """SQL 三值逻辑里的 "真"。NULL 视为不真。"""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value != ""
    return bool(value)


__all__ = [
    "DataType",
    "TypeSpec",
    "common_type",
    "zigzag_encode",
    "zigzag_decode",
    "encode_varint",
    "decode_varint",
    "encode_uvarint",
    "decode_uvarint",
    "encode_value",
    "decode_value",
    "compare_values",
    "is_truthy",
    "INT32_MIN",
    "INT32_MAX",
    "INT64_MIN",
    "INT64_MAX",
]
