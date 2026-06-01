"""Bearer Token 鉴权

Token 存储格式（环境变量 FM_TOKENS）：
    alice:64位随机串,bob:64位随机串,charlie:64位随机串

请求头：
    Authorization: Bearer <64位随机串>
"""

from __future__ import annotations

import os
import secrets
from typing import Optional

from fastapi import Header, HTTPException, status


def _parse_tokens() -> dict[str, str]:
    """解析 FM_TOKENS 环境变量，返回 {token: user} 映射。"""
    raw = os.getenv("FM_TOKENS", "").strip()
    if not raw:
        return {}
    out: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        user, token = pair.split(":", 1)
        user, token = user.strip(), token.strip()
        if user and token:
            out[token] = user
    return out


_TOKENS: dict[str, str] = _parse_tokens()


def reload_tokens() -> int:
    """重新从环境变量加载 tokens（用于热更新）。"""
    global _TOKENS
    _TOKENS = _parse_tokens()
    return len(_TOKENS)


def verify_token(authorization: Optional[str] = Header(None)) -> str:
    """FastAPI 依赖：从 Authorization 头提取并校验 Bearer Token。

    校验通过返回用户名；否则抛 401。
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization must use Bearer scheme",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization[7:].strip()

    # 恒定时间比对防止时序攻击
    for valid_token, user in _TOKENS.items():
        if secrets.compare_digest(token, valid_token):
            return user

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid token",
        headers={"WWW-Authenticate": "Bearer"},
    )
