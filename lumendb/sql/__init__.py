"""SQL 词法 / 语法层。

对外只暴露三个入口：``tokenize``（调试用）、``parse_sql``（多语句）、
``parse_statement``（单语句）。
"""

from .lexer import Lexer, Token, tokenize
from .parser import Parser, parse_sql, parse_statement

__all__ = [
    "Lexer",
    "Token",
    "tokenize",
    "Parser",
    "parse_sql",
    "parse_statement",
]
