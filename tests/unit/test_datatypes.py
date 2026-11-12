"""类型系统与编码的单元测试。"""

import pytest

from lumendb.datatypes import (
    DataType,
    TypeSpec,
    common_type,
    compare_values,
    decode_uvarint,
    decode_value,
    decode_varint,
    encode_uvarint,
    encode_value,
    encode_varint,
    zigzag_decode,
    zigzag_encode,
)
from lumendb.errors import NotSupportedError, TypeMismatchError


class TestTypeSpecParse:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("INT", TypeSpec(DataType.INT)),
            ("int", TypeSpec(DataType.INT)),
            ("INTEGER", TypeSpec(DataType.INT)),
            ("BIGINT", TypeSpec(DataType.BIGINT)),
            ("FLOAT", TypeSpec(DataType.FLOAT)),
            ("DOUBLE", TypeSpec(DataType.FLOAT)),
            ("BOOLEAN", TypeSpec(DataType.BOOLEAN)),
            ("BOOL", TypeSpec(DataType.BOOLEAN)),
            ("TEXT", TypeSpec(DataType.TEXT)),
            ("VARCHAR(64)", TypeSpec(DataType.VARCHAR, 64)),
            ("varchar ( 8 )", TypeSpec(DataType.VARCHAR, 8)),
        ],
    )
    def test_parse(self, text, expected):
        assert TypeSpec.parse(text) == expected

    def test_varchar_requires_length(self):
        with pytest.raises(NotSupportedError):
            TypeSpec.parse("VARCHAR")

    def test_unknown_type(self):
        with pytest.raises(NotSupportedError):
            TypeSpec.parse("DATETIME")

    def test_varchar_zero_length(self):
        with pytest.raises(NotSupportedError):
            TypeSpec.parse("VARCHAR(0)")

    def test_name_roundtrip(self):
        assert TypeSpec(DataType.VARCHAR, 64).name == "VARCHAR(64)"
        assert TypeSpec(DataType.INT).name == "INT"


class TestCoerce:
    def test_int_range(self):
        spec = TypeSpec(DataType.INT)
        assert spec.coerce(7) == 7
        with pytest.raises(TypeMismatchError):
            spec.coerce(2**31)
        with pytest.raises(TypeMismatchError):
            spec.coerce(-(2**31) - 1)

    def test_bigint_range(self):
        spec = TypeSpec(DataType.BIGINT)
        assert spec.coerce(2**31) == 2**31
        with pytest.raises(TypeMismatchError):
            spec.coerce(2**63)

    def test_float_rejects_bool(self):
        with pytest.raises(TypeMismatchError):
            TypeSpec(DataType.FLOAT).coerce(True)

    def test_int_rejects_bool(self):
        with pytest.raises(TypeMismatchError):
            TypeSpec(DataType.INT).coerce(True)

    def test_varchar_length_is_not_silently_truncated(self):
        """超长必须报错。静默截断会让用户以为数据存进去了。"""
        spec = TypeSpec(DataType.VARCHAR, 5)
        assert spec.coerce("abcde") == "abcde"
        with pytest.raises(TypeMismatchError) as exc:
            spec.coerce("abcdef")
        assert "超过" in str(exc.value)

    def test_null_passes_through(self):
        assert TypeSpec(DataType.INT).coerce(None) is None

    def test_boolean_from_int(self):
        assert TypeSpec(DataType.BOOLEAN).coerce(1) is True
        assert TypeSpec(DataType.BOOLEAN).coerce(0) is False
        with pytest.raises(TypeMismatchError):
            TypeSpec(DataType.BOOLEAN).coerce(2)


class TestCommonType:
    def test_int_bigint(self):
        assert common_type(TypeSpec(DataType.INT), TypeSpec(DataType.BIGINT)) == TypeSpec(
            DataType.BIGINT
        )

    def test_int_float(self):
        assert common_type(TypeSpec(DataType.INT), TypeSpec(DataType.FLOAT)) == TypeSpec(
            DataType.FLOAT
        )

    def test_string_numeric_rejected(self):
        with pytest.raises(TypeMismatchError):
            common_type(TypeSpec(DataType.VARCHAR, 4), TypeSpec(DataType.INT))

    def test_varchar_widens(self):
        assert common_type(
            TypeSpec(DataType.VARCHAR, 10), TypeSpec(DataType.VARCHAR, 20)
        ) == TypeSpec(DataType.VARCHAR, 20)

    def test_boolean_rejected(self):
        with pytest.raises(TypeMismatchError):
            common_type(TypeSpec(DataType.BOOLEAN), TypeSpec(DataType.INT))


class TestVarint:
    @pytest.mark.parametrize("n", [0, 1, -1, 2, -2, 127, 128, -128, 2**31, -(2**31), 2**62])
    def test_zigzag_roundtrip(self, n):
        assert zigzag_decode(zigzag_encode(n)) == n

    @pytest.mark.parametrize("n", [0, 1, -1, 42, -42, 2**40, -(2**40)])
    def test_varint_roundtrip(self, n):
        enc = encode_varint(n)
        val, pos = decode_varint(enc, 0)
        assert val == n
        assert pos == len(enc)

    @pytest.mark.parametrize("n", [0, 1, 300, 2**20])
    def test_uvarint_roundtrip(self, n):
        enc = encode_uvarint(n)
        val, pos = decode_uvarint(enc, 0)
        assert val == n
        assert pos == len(enc)

    def test_varint_size_bounds(self):
        # 小整数必须真的小——否则页里全是白占的空间
        assert len(encode_varint(127)) == 2  # zigzag(127)=254 -> 2 字节
        assert len(encode_varint(63)) == 1  # zigzag(63)=126 -> 1 字节

    def test_truncated_varint_is_an_error(self):
        with pytest.raises(ValueError):
            decode_varint(b"\x80", 0)


class TestValueCodec:
    @pytest.mark.parametrize(
        "spec,value",
        [
            (TypeSpec(DataType.INT), 0),
            (TypeSpec(DataType.INT), -12345),
            (TypeSpec(DataType.INT), 2**31 - 1),
            (TypeSpec(DataType.BIGINT), -(2**40)),
            (TypeSpec(DataType.FLOAT), 3.14159),
            (TypeSpec(DataType.FLOAT), -0.0),
            (TypeSpec(DataType.BOOLEAN), True),
            (TypeSpec(DataType.BOOLEAN), False),
            (TypeSpec(DataType.VARCHAR, 64), "hello 世界"),
            (TypeSpec(DataType.TEXT), ""),
        ],
    )
    def test_roundtrip(self, spec, value):
        buf = encode_value(spec, value)
        got, pos = decode_value(spec, buf, 0)
        assert pos == len(buf)
        if spec.kind is DataType.FLOAT and value == 0.0:
            assert got == 0.0
        else:
            assert got == value

    def test_multibyte_string_uses_utf8_bytes(self):
        spec = TypeSpec(DataType.VARCHAR, 10)
        val = "中文"
        enc = encode_value(spec, val)
        assert len(enc) == 1 + 2 * 3  # uvarint(6) + 6 字节

    def test_compare_values(self):
        assert compare_values(1, 2, TypeSpec(DataType.INT)) == -1
        assert compare_values(2, 2, TypeSpec(DataType.INT)) == 0
        assert compare_values(3, 2, TypeSpec(DataType.INT)) == 1
        assert compare_values("a", "b", TypeSpec(DataType.TEXT)) == -1
        assert compare_values(False, True, TypeSpec(DataType.BOOLEAN)) == -1

    def test_compare_rejects_null(self):
        with pytest.raises(ValueError):
            compare_values(None, 1, TypeSpec(DataType.INT))
