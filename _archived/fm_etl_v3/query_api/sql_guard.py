"""SQL 白名单守卫

策略（四层过滤）：
  1. 长度限制：<= 20000 字符（Pydantic 层）
  2. 首关键字白名单：只允许 SELECT / SHOW / DESCRIBE / EXPLAIN / WITH
  3. 危险关键字黑名单：任何出现 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/ATTACH/COPY/PRAGMA/SET/LOAD/INSTALL 均拒绝
  4. 禁止多语句：不允许 `;` 后还有非空白非注释内容

注意：此守卫配合 DuckDB 的 read_only=True 连接使用，即使守卫被绕过也无法写入。
"""

from __future__ import annotations

import re

ALLOWED_FIRST_KEYWORDS = frozenset({
    "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH", "PRAGMA_TABLE_INFO",
})

FORBIDDEN_KEYWORDS = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
    "ATTACH", "DETACH", "COPY", "EXPORT", "IMPORT",
    "PRAGMA", "SET", "RESET", "LOAD", "INSTALL", "CALL",
    "GRANT", "REVOKE", "BEGIN", "COMMIT", "ROLLBACK",
    "VACUUM", "ANALYZE", "CHECKPOINT",
})


class SQLGuardError(ValueError):
    """SQL 守卫拒绝的异常"""


def _strip_comments(sql: str) -> str:
    """去除 SQL 注释（-- 单行 和 /* ... */ 多行），便于关键字检测。"""
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def _tokenize_keywords(sql: str) -> list[str]:
    """提取所有 SQL 关键字（大写单词），忽略引号内字符串。"""
    no_strings = re.sub(r"'(?:[^']|'')*'", "''", sql)
    no_strings = re.sub(r'"(?:[^"]|"")*"', '""', no_strings)
    words = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", no_strings)
    return [w.upper() for w in words]


def validate_sql(sql: str) -> str:
    """校验 SQL 是否允许执行，返回清理后的 SQL。违规抛 SQLGuardError。"""
    cleaned = sql.strip()
    if not cleaned:
        raise SQLGuardError("Empty SQL")

    if cleaned.endswith(";"):
        cleaned = cleaned.rstrip(";").strip()

    bare = _strip_comments(cleaned).strip()

    if ";" in bare:
        after = bare.split(";", 1)[1].strip()
        if after:
            raise SQLGuardError("Multiple statements are not allowed")

    tokens = _tokenize_keywords(bare)
    if not tokens:
        raise SQLGuardError("No SQL keywords found")

    first = tokens[0]
    if first not in ALLOWED_FIRST_KEYWORDS:
        raise SQLGuardError(
            f"Only {sorted(ALLOWED_FIRST_KEYWORDS)} queries are allowed, got '{first}'"
        )

    for kw in tokens:
        if kw in FORBIDDEN_KEYWORDS:
            raise SQLGuardError(f"Forbidden keyword '{kw}' in SQL")

    return cleaned
