"""词法分析器的测试。"""

import pytest

from lumendb.errors import SQLSyntaxError
from lumendb.sql.lexer import Token, tokenize


def kinds(sql):
    return [t.kind for t in tokenize(sql) if t.kind != "eof"]


def values(sql):
    return [t.value for t in tokenize(sql) if t.kind != "eof"]


class TestBasics:
    def test_empty_input(self):
        toks = tokenize("")
        assert len(toks) == 1
        assert toks[0].kind == "eof"

    def test_only_whitespace(self):
        assert kinds("   \n\t  ") == []

    def test_select_statement(self):
        toks = tokenize("SELECT id FROM users")
        assert [t.kind for t in toks] == [
            "keyword",
            "ident",
            "keyword",
            "ident",
            "eof",
        ]
        assert toks[0].keyword == "SELECT"
        assert toks[1].value == "id"

    def test_keywords_are_case_insensitive(self):
        lower = tokenize("select")
        upper = tokenize("SELECT")
        assert lower[0].keyword == upper[0].keyword == "SELECT"

    def test_identifiers_keep_case(self):
        toks = tokenize("MyTable")
        assert toks[0].kind == "ident"
        assert toks[0].value == "MyTable"

    def test_keyword_word_usable_as_column(self):
        """``name`` / ``value`` 这类词不在关键字表里，应当被当成标识符。"""
        toks = tokenize("name value user")
        assert all(t.kind == "ident" for t in toks[:-1])


class TestNumbers:
    def test_integer(self):
        toks = tokenize("42")
        assert toks[0].kind == "int"
        assert toks[0].value == "42"

    def test_float(self):
        toks = tokenize("3.14")
        assert toks[0].kind == "float"

    def test_leading_dot_float(self):
        toks = tokenize(".5")
        assert toks[0].kind == "float"
        assert toks[0].value == ".5"

    def test_scientific(self):
        toks = tokenize("1e10")
        assert toks[0].kind == "float"
        assert toks[0].value == "1e10"

    def test_scientific_with_sign(self):
        toks = tokenize("2.5E-3")
        assert toks[0].kind == "float"

    def test_broken_exponent(self):
        with pytest.raises(SQLSyntaxError):
            tokenize("1e")


class TestStrings:
    def test_simple(self):
        toks = tokenize("'hello'")
        assert toks[0].kind == "string"
        assert toks[0].value == "hello"

    def test_empty_string(self):
        assert tokenize("''")[0].value == ""

    def test_escaped_quote(self):
        assert tokenize("'it''s'")[0].value == "it's"

    def test_semicolon_inside_string(self):
        assert tokenize("'a;b'")[0].value == "a;b"

    def test_unclosed_string(self):
        with pytest.raises(SQLSyntaxError) as exc:
            tokenize("'abc")
        assert "闭合" in str(exc.value)

    def test_quoted_identifier(self):
        toks = tokenize('"my col"')
        assert toks[0].kind == "ident"
        assert toks[0].value == "my col"


class TestOperators:
    def test_two_char_before_one_char(self):
        toks = tokenize("a <= b")
        ops = [t.value for t in toks if t.kind == "op"]
        assert ops == ["<="]

    def test_not_equal_variants(self):
        assert tokenize("a <> b")[1].value == "<>"
        assert tokenize("a != b")[1].value == "!="

    def test_arithmetic(self):
        ops = [t.value for t in tokenize("1 + 2 - 3 * 4 / 5 % 6") if t.kind == "op"]
        assert ops == ["+", "-", "*", "/", "%"]

    def test_punctuation(self):
        toks = tokenize("(a, b); .")
        kinds_ = [t.kind for t in toks[:-1]]
        assert kinds_ == ["lparen", "ident", "comma", "ident", "rparen", "semicolon", "dot"]

    def test_unrecognized_character(self):
        with pytest.raises(SQLSyntaxError) as exc:
            tokenize("a @ b")
        assert "@" in str(exc.value)


class TestComments:
    def test_line_comment(self):
        assert values("1 -- comment\n2") == ["1", "2"]

    def test_block_comment(self):
        assert values("1 /* x */ 2") == ["1", "2"]

    def test_multiline_block_comment(self):
        assert values("1 /* a\nb\nc */ 2") == ["1", "2"]

    def test_unclosed_block_comment(self):
        with pytest.raises(SQLSyntaxError) as exc:
            tokenize("1 /* oops")
        assert "*/" in str(exc.value)


class TestParameters:
    def test_param_token(self):
        toks = tokenize("$1 $23")
        assert [(t.kind, t.value) for t in toks[:2]] == [
            ("param", "1"),
            ("param", "23"),
        ]

    def test_dollar_without_digits(self):
        with pytest.raises(SQLSyntaxError) as exc:
            tokenize("SELECT $ FROM t")
        assert "$" in str(exc.value)


class TestPositions:
    def test_line_and_column(self):
        toks = tokenize("a\n  b")
        assert (toks[0].line, toks[0].col) == (1, 1)
        assert (toks[1].line, toks[1].col) == (2, 3)

    def test_error_reports_position(self):
        with pytest.raises(SQLSyntaxError) as exc:
            tokenize("a\n  @")
        assert "第 2 行" in str(exc.value)


class TestTokenHelpers:
    def test_is_keyword(self):
        tok = tokenize("SELECT")[0]
        assert tok.is_keyword("SELECT")
        assert not tok.is_keyword("INSERT")

    def test_is_op(self):
        tok = tokenize("+")[0]
        assert tok.is_op("+")
        assert not tok.is_op("-")

    def test_str_repr(self):
        assert "第 1 行" in str(Token("ident", "x", 1, 1))
