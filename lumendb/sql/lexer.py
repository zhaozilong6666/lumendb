"""SQL 词法分析。

设计取舍：手写扫描器而不是正则表达式整段匹配。原因有两个：

1. 报错要精确——用户写错一个引号，必须能指出是哪一行哪一列；
2. SQL 的字符串字面量里可以出现任何字符（包括 ``;``），用一条大正则
   很容易在边界上出错。

关键字集合是固定的，但**类型名不是关键字**：``INT`` / ``VARCHAR`` 这些
在语法上出现在"类型位置"，由 parser 按名字识别即可，这样 ``CAST(x AS int)``
这种大小写混写也能自然工作。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..errors import SQLSyntaxError

KEYWORDS = frozenset(
    """
    SELECT FROM WHERE GROUP BY HAVING ORDER ASC DESC LIMIT OFFSET
    INSERT INTO VALUES UPDATE SET DELETE
    CREATE TABLE INDEX UNIQUE ON DROP IF EXISTS PRIMARY KEY NOT NULL DEFAULT
    AND OR IS IN BETWEEN LIKE AS
    JOIN INNER LEFT RIGHT FULL OUTER CROSS
    DISTINCT
    BEGIN START TRANSACTION COMMIT ROLLBACK
    EXPLAIN SHOW TABLES CHECKPOINT REINDEX
    TRUE FALSE UNKNOWN CAST EXISTS
    ALTER WITH CASE WHEN THEN ELSE END
    VIEW TRIGGER PROCEDURE REFERENCES CHECK COLLATE
    GENERATED AUTOINCREMENT AUTO_INCREMENT
    READ ONLY SAVEPOINT RECURSIVE WINDOW OVER PARTITION RETURNING
    TRUNCATE VACUUM ANALYZE GRANT REVOKE COPY
    UNION EXCEPT INTERSECT
    """.split()
)

#: 这些关键字在"名字位置"也不允许出现，否则 ``SELECT FROM`` 这类笔误会被
#: 当成"列名叫 FROM"而静默通过。
RESERVED = frozenset(
    """
    SELECT FROM WHERE GROUP BY HAVING ORDER LIMIT OFFSET
    INSERT INTO VALUES UPDATE SET DELETE
    CREATE TABLE INDEX DROP JOIN ON
    AND OR NOT IS IN BETWEEN LIKE AS
    DISTINCT BEGIN START COMMIT ROLLBACK TRANSACTION
    EXPLAIN SHOW CHECKPOINT REINDEX VACUUM
    TRUE FALSE NULL CAST EXISTS
    UNIQUE PRIMARY KEY IF DEFAULT
    LEFT INNER OUTER ASC DESC
    WITH UNION EXCEPT INTERSECT
    ALTER CASE WHEN THEN ELSE END
    REFERENCES CHECK COLLATE CROSS
    """.split()
)

# 双字符运算符必须排在单字符前面，否则 "<=" 会被切成 "<" 和 "="。
_OPERATORS = ("<>", "!=", "<=", ">=", "||", "=", "<", ">", "+", "-", "*", "/", "%")

_PUNCT = {
    "(": "lparen",
    ")": "rparen",
    ",": "comma",
    ";": "semicolon",
    ".": "dot",
}

_IDENT_START = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
_IDENT_BODY = _IDENT_START | set("0123456789")
_DIGITS = set("0123456789")


@dataclass(frozen=True)
class Token:
    kind: str
    value: str
    line: int
    col: int

    @property
    def keyword(self) -> Optional[str]:
        return self.value.upper() if self.kind == "keyword" else None

    def is_keyword(self, *names: str) -> bool:
        return self.kind == "keyword" and self.value.upper() in names

    def is_punct(self, name: str) -> bool:
        return self.kind == name

    def is_op(self, *ops: str) -> bool:
        return self.kind == "op" and self.value in ops

    def __str__(self) -> str:  # pragma: no cover - 仅用于报错
        return f"{self.value!r}（第 {self.line} 行第 {self.col} 列）"


class Lexer:
    def __init__(self, text: str) -> None:
        self._text = text
        self._pos = 0
        self._line = 1
        self._col = 1

    # ------------------------------------------------------------------

    def tokenize(self) -> list[Token]:
        tokens: list[Token] = []
        while True:
            tok = self.next_token()
            tokens.append(tok)
            if tok.kind == "eof":
                return tokens

    def next_token(self) -> Token:
        self._skip_space_and_comments()
        if self._pos >= len(self._text):
            return Token("eof", "", self._line, self._col)

        ch = self._text[self._pos]
        line, col = self._line, self._col

        if ch == "'":
            return self._read_string()
        if ch == "$":
            return self._read_param()
        if ch in _DIGITS or (ch == "." and self._peek(1) in _DIGITS):
            return self._read_number()
        if ch == '"':
            return self._read_quoted_ident()
        if ch in _IDENT_START:
            return self._read_ident()
        if ch in _PUNCT:
            self._advance()
            return Token(_PUNCT[ch], ch, line, col)

        for op in _OPERATORS:
            if self._text.startswith(op, self._pos):
                for _ in op:
                    self._advance()
                return Token("op", op, line, col)

        raise SQLSyntaxError(
            f"无法识别的字符 {ch!r}",
            detail=f"第 {line} 行第 {col} 列",
        )

    # ------------------------------------------------------------------
    # 内部扫描
    # ------------------------------------------------------------------

    def _peek(self, offset: int = 0) -> str:
        i = self._pos + offset
        return self._text[i] if i < len(self._text) else ""

    def _advance(self) -> None:
        if self._text[self._pos] == "\n":
            self._line += 1
            self._col = 1
        else:
            self._col += 1
        self._pos += 1

    def _skip_space_and_comments(self) -> None:
        while self._pos < len(self._text):
            ch = self._text[self._pos]
            if ch in " \t\r\n":
                self._advance()
            elif ch == "-" and self._peek(1) == "-":
                while self._pos < len(self._text) and self._text[self._pos] != "\n":
                    self._advance()
            elif ch == "/" and self._peek(1) == "*":
                self._advance()
                self._advance()
                closed = False
                while self._pos < len(self._text):
                    if self._text[self._pos] == "*" and self._peek(1) == "/":
                        self._advance()
                        self._advance()
                        closed = True
                        break
                    self._advance()
                if not closed:
                    raise SQLSyntaxError("块注释没有闭合（缺少 */）")
            else:
                return

    def _read_param(self) -> Token:
        """``$1`` / ``$12``：PostgreSQL 风格的参数占位符。"""
        line, col = self._line, self._col
        self._advance()  # $
        if self._pos >= len(self._text) or self._text[self._pos] not in _DIGITS:
            raise SQLSyntaxError(
                "参数占位符 $ 后面必须跟数字",
                detail=f"第 {line} 行第 {col} 列",
            )
        digits = []
        while self._pos < len(self._text) and self._text[self._pos] in _DIGITS:
            digits.append(self._text[self._pos])
            self._advance()
        return Token("param", "".join(digits), line, col)

    def _read_string(self) -> Token:
        line, col = self._line, self._col
        self._advance()  # 开头的引号
        parts: list[str] = []
        while True:
            if self._pos >= len(self._text):
                raise SQLSyntaxError(
                    "字符串字面量没有闭合（缺少 '）",
                    detail=f"第 {line} 行第 {col} 列",
                )
            ch = self._text[self._pos]
            if ch == "'":
                # SQL 标准：两个连续单引号表示一个单引号
                if self._peek(1) == "'":
                    self._advance()
                    self._advance()
                    parts.append("'")
                    continue
                self._advance()
                return Token("string", "".join(parts), line, col)
            parts.append(ch)
            self._advance()

    def _read_quoted_ident(self) -> Token:
        line, col = self._line, self._col
        self._advance()
        parts: list[str] = []
        while True:
            if self._pos >= len(self._text):
                raise SQLSyntaxError(
                    "引号标识符没有闭合（缺少 \"）",
                    detail=f"第 {line} 行第 {col} 列",
                )
            ch = self._text[self._pos]
            if ch == '"':
                self._advance()
                return Token("ident", "".join(parts), line, col)
            parts.append(ch)
            self._advance()

    def _read_number(self) -> Token:
        line, col = self._line, self._col
        start = self._pos
        while self._pos < len(self._text) and self._text[self._pos] in _DIGITS:
            self._advance()
        is_float = False
        if self._pos < len(self._text) and self._text[self._pos] == ".":
            is_float = True
            self._advance()
            while self._pos < len(self._text) and self._text[self._pos] in _DIGITS:
                self._advance()
        if self._pos < len(self._text) and self._text[self._pos] in "eE":
            is_float = True
            self._advance()
            if self._pos < len(self._text) and self._text[self._pos] in "+-":
                self._advance()
            seen = False
            while self._pos < len(self._text) and self._text[self._pos] in _DIGITS:
                seen = True
                self._advance()
            if not seen:
                raise SQLSyntaxError(
                    "科学计数法缺少指数部分",
                    detail=f"第 {line} 行第 {col} 列",
                )
        text = self._text[start : self._pos]
        return Token("float" if is_float else "int", text, line, col)

    def _read_ident(self) -> Token:
        line, col = self._line, self._col
        start = self._pos
        while self._pos < len(self._text) and self._text[self._pos] in _IDENT_BODY:
            self._advance()
        text = self._text[start : self._pos]
        kind = "keyword" if text.upper() in KEYWORDS else "ident"
        return Token(kind, text, line, col)


def tokenize(sql: str) -> list[Token]:
    return Lexer(sql).tokenize()


__all__ = ["Lexer", "Token", "tokenize", "KEYWORDS", "RESERVED"]
