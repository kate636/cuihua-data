"""
QDM BI API 连接器（只读）

封装 bdapp.qdama.cn HTTP API，提供与原 StarRocksConnector.query() 相同的接口。
签名算法、分页、WAF 规避逻辑参考 qdm-bi-api skill。

WAF 注意事项：
  - SQL 中禁止使用 CASE WHEN，改用 IF(condition, true_val, false_val)
  - IN (...) 列表过大时分批查询
"""

from __future__ import annotations

import hashlib
import json
import random
import string
import time
from typing import Any, Optional

import pandas as pd
import requests

from ..config import get_settings
from ..utils import get_logger, with_retry

_log = get_logger("api_connector")


class ApiConnector:
    """QDM BI API 轻量封装，只支持 SELECT 查询。"""

    def __init__(self, settings=None):
        cfg = (settings or get_settings()).api
        self._host       = cfg.host
        self._api_id     = cfg.api_id
        self._access_key = cfg.access_key
        self._secret_key = cfg.secret_key
        self._version    = cfg.version

    # ── 公开接口（与原 StarRocksConnector.query 签名一致）───────────────────
    @with_retry(max_attempts=3, delay=5.0)
    def query(self, sql: str, params=None) -> pd.DataFrame:
        """执行 SELECT SQL，返回 DataFrame。自动处理分页，最多重试 3 次。"""
        _log.debug(f"query: {sql[:120].strip()} ...")
        rows = self._fetch_all(sql)
        df = pd.DataFrame(rows)
        _log.debug(f"query returned {len(df)} rows")
        return df

    # ── 内部实现 ─────────────────────────────────────────────────────────────
    def _build_request(self, sql: str) -> tuple[str, str]:
        """构建带签名的请求 URL 和 body。每次调用生成新 nonce/timestamp。"""
        body = {
            "apiId": self._api_id,
            "paramMap": {"apiId": self._api_id, "sql": sql},
        }
        body_str = json.dumps(body, ensure_ascii=False)

        nonce     = "".join(random.choices(string.ascii_letters + string.digits, k=6))
        timestamp = int(time.time() * 1000)
        encrypt   = 0

        sign = self._generate_sign(timestamp, nonce, encrypt, body_str)

        query_params = {
            "AccessKey": self._access_key,
            "timestamp": timestamp,
            "nonce":     nonce,
            "encrypt":   encrypt,
            "version":   self._version,
            "sign":      sign,
        }
        url = (
            f"{self._host}/api/v1/executeApi/{self._api_id}?"
            + "&".join(f"{k}={v}" for k, v in query_params.items())
        )
        return url, body_str

    def _generate_sign(self, timestamp: int, nonce: str, encrypt: int, body_str: str) -> str:
        sign_params: dict[str, Any] = {
            "AccessKey": self._access_key,
            "encrypt":   encrypt,
            "nonce":     nonce,
            "timestamp": timestamp,
            "version":   self._version,
            "bodyStr":   body_str,
        }
        keys = sorted(k for k, v in sign_params.items() if v not in (None, ""))
        param_str = "&".join(f"{k}={sign_params[k]}" for k in keys)
        param_str += f"&SecretKey={self._secret_key}"
        return hashlib.md5(param_str.encode("utf-8")).hexdigest().upper()

    def _fetch_all(self, sql: str) -> list[dict]:
        """执行 SQL 并自动翻页，返回所有行。"""
        headers = {"Content-Type": "application/json"}

        url, body_str = self._build_request(sql)
        resp = requests.post(url, data=body_str.encode("utf-8"), headers=headers, timeout=600)
        resp.raise_for_status()
        result = resp.json()

        if result.get("code") != 0:
            raise RuntimeError(
                f"API error: code={result.get('code')}, msg={result.get('msg')}"
            )

        data = result["data"]

        if isinstance(data, dict) and "pageData" in data:
            rows: list = data["pageData"]
            total_page = data.get("pageInfo", {}).get("totalPage", 1)
            for _ in range(2, total_page + 1):
                url2, body2 = self._build_request(sql)
                r2 = requests.post(url2, data=body2.encode("utf-8"), headers=headers, timeout=600)
                r2.raise_for_status()
                rows.extend(r2.json().get("data", {}).get("pageData", []))
            return rows

        if isinstance(data, list):
            return data

        return []
