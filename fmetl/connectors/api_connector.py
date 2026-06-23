"""
QDM BI API 连接器（只读）

封装 bdapp.qdama.cn HTTP API，提供与原 StarRocksConnector.query() 相同的接口。
签名算法、分页、WAF 规避逻辑参考 qdm-bi-api skill。

WAF 注意事项：
  - SQL 中禁止使用 CASE WHEN，改用 IF(condition, true_val, false_val)
  - IN (...) 列表过大时分批查询
  - SQL 中禁止使用 LIMIT / OFFSET（API 侧 SQL 解析器不支持）

分页机制：
  - API 默认 pageSize=10，最大允许 20000
  - pageSize 必须作为 body 顶层参数（非 paramMap 内部）
  - 超过 pageSize 的行会被静默截断，API 不支持 LIMIT/OFFSET
  - _fetch_all 自动处理三种路径：
    1. pageData 格式 → 标准翻页
    2. list 且 < 20000 行 → 直接返回
    3. list 且 = 20000 行 → 自动键集分页（以第一列为键循环拉取）
"""

from __future__ import annotations

import hashlib
import json
import random
import re
import string
import time
from typing import Any, Optional

import pandas as pd
import requests

from ..config import get_settings
from ..utils import get_logger, retry_on_exception

_log = get_logger("api_connector")

# 把 QDM API 返回的 camelCase 列名（如 businessDate / abiArticleId）恢复为
# SQL 里写的 snake_case（business_date / abi_article_id）。
# QDM 网关会强制把所有下划线列名/别名改写成 camelCase，下游 DuckDB 合并层写的都是
# snake_case，所以必须在 query() 出口统一归一化。
_CAMEL_SPLIT = re.compile(r"(?<!^)(?=[A-Z])")


def _camel_to_snake(name: str) -> str:
    """驼峰 → 下划线。对全小写/带数字的列名不产生影响。"""
    return _CAMEL_SPLIT.sub("_", name).lower()


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
    @retry_on_exception(max_attempts=3, wait_seconds=5.0)
    def query(self, sql: str, params=None, normalize_columns: bool = True) -> pd.DataFrame:
        """执行 SELECT SQL，返回 DataFrame。自动处理分页，最多重试 3 次。

        normalize_columns=True（默认）会把 API 返回的 camelCase 列名统一转回
        snake_case，以便 DuckDB 下游 SQL（全部走 snake_case）能正确引用。
        """
        _log.debug(f"query: {sql[:120].strip()} ...")
        rows = self._fetch_all(sql)
        df = pd.DataFrame(rows)
        if normalize_columns and not df.empty:
            df.columns = [_camel_to_snake(str(c)) for c in df.columns]
        _log.debug(f"query returned {len(df)} rows")
        return df

    def query_with_keyset(self, sql: str, order_by: str) -> pd.DataFrame:
        """带显式排序键的查询，避免自动检测第一列可能不准确的问题。

        调用方明确指定排序键（StarRocks snake_case 列名），_fetch_all
        会用此键做分页。用于已知可能超过 PAGE_SIZE 的大表查询。
        """
        _log.debug(f"query_with_keyset: order_by={order_by}  {sql[:100].strip()} ...")
        rows = self._fetch_all(sql, keyset_col=order_by)
        df = pd.DataFrame(rows)
        if not df.empty:
            df.columns = [_camel_to_snake(str(c)) for c in df.columns]
        _log.debug(f"query_with_keyset returned {len(df)} rows")
        return df

    PAGE_SIZE = 20_000  # API 允许的最大 pageSize，超过会报错

    # ── 内部实现 ─────────────────────────────────────────────────────────────
    def _build_request(self, sql: str) -> tuple[str, str]:
        """构建带签名的请求 URL 和 body。每次调用生成新 nonce/timestamp。"""
        body = {
            "apiId": self._api_id,
            "pageSize": self.PAGE_SIZE,
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

    def _fetch_all(self, sql: str, keyset_col: str = "") -> list[dict]:
        """执行 SQL 并自动翻页，返回所有行。

        分页策略（按优先级）：
        1. API 返回 pageData 格式 → 循环拉取所有页（标准分页路径）
        2. API 返回 list 且行数 < PAGE_SIZE → 数据完整，直接返回
        3. API 返回 list 且行数 = PAGE_SIZE → 自动键集分页（keyset pagination）
           keyset_col 为空时自动取结果集第一列；调用方也可显式指定 StarRocks
           列名（snake_case）。
           键集分页要求排序列为可排序的唯一键（如 article_id / store_id），
           否则可能漏数据或死循环。
        """
        headers = {"Content-Type": "application/json"}

        # 如果预先知道排序键，首轮查询也加 ORDER BY 保证分页一致性
        first_sql = sql
        if keyset_col:
            first_sql = f"SELECT * FROM ({sql}) _ks_base ORDER BY {keyset_col}"

        first_page = self._fetch_single_page(first_sql, headers)

        if len(first_page) < self.PAGE_SIZE:
            return first_page

        # 确定排序键：优先用调用方指定的，否则取结果集第一列
        if keyset_col:
            sort_col_snake = keyset_col
        else:
            first_col_camel = list(first_page[0].keys())[0]
            sort_col_snake = _camel_to_snake(first_col_camel)

        # 转换为 API 返回的 camelCase 列名，用于从 dict 取值
        sort_col_camel = sort_col_snake.title().replace("_", "")
        # title() 不完美，直接用首列名
        if not keyset_col:
            sort_col_camel = list(first_page[0].keys())[0]
        else:
            # 把 snake_case 转 camelCase: article_id → articleId
            parts = sort_col_snake.split("_")
            sort_col_camel = parts[0] + "".join(p.title() for p in parts[1:])

        all_rows = list(first_page)
        page = first_page
        page_num = 1

        while len(page) >= self.PAGE_SIZE:
            page_num += 1
            last_val = page[-1][sort_col_camel]
            page_sql = (
                f"SELECT * FROM ({sql}) _ks_page "
                f"WHERE {sort_col_snake} > '{last_val}' "
                f"ORDER BY {sort_col_snake}"
            )
            page = self._fetch_single_page(page_sql, headers)
            if not page:
                break
            all_rows.extend(page)
            _log.info(
                f"keyset page {page_num}: +{len(page)} rows "
                f"(key={sort_col_snake} > '{str(last_val)[:40]}')"
            )

        _log.info(f"keyset pagination done: {len(all_rows)} total rows in {page_num} pages")
        return all_rows

    def _fetch_single_page(self, sql: str, headers: dict) -> list[dict]:
        """发送单次 API 请求，返回原始行列表或 (rows, total_pages) 元组。"""
        url, body_str = self._build_request(sql)
        resp = requests.post(url, data=body_str.encode("utf-8"), headers=headers, timeout=600)
        resp.raise_for_status()
        result = resp.json()

        if result.get("code") != 0:
            raise RuntimeError(
                f"API error: code={result.get('code')}, msg={result.get('msg')}"
            )

        data = result["data"]

        # pageData 格式：返回 (rows, total_page) 供外层循环
        if isinstance(data, dict) and "pageData" in data:
            rows: list = data["pageData"]
            total_page = data.get("pageInfo", {}).get("totalPage", 1)
            all_rows = list(rows)
            for _ in range(2, total_page + 1):
                url2, body2 = self._build_request(sql)
                r2 = requests.post(url2, data=body2.encode("utf-8"), headers=headers, timeout=600)
                r2.raise_for_status()
                all_rows.extend(r2.json().get("data", {}).get("pageData", []))
            return all_rows

        if isinstance(data, list):
            return data

        return []
