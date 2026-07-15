from __future__ import annotations

import hashlib
import json
import random
import re
import string
import time
from typing import Any

import pandas as pd
import requests

from fmetl.config import Settings, get_settings


_CAMEL_SPLIT = re.compile(r"(?<!^)(?=[A-Z])")


class PaginationContractError(RuntimeError):
    pass


def _snake(name: object) -> str:
    return _CAMEL_SPLIT.sub("_", str(name)).lower()


class QdmApi:
    """Read-only QDM API client.

    The old client silently guessed a keyset column and repeated pageData calls
    without a proven page cursor. v0.12 refuses ambiguous multi-page results.
    Extractors must shard a mirror query until each shard is below page_size.
    """

    def __init__(self, settings: Settings | None = None, session: requests.Session | None = None):
        self.settings = settings or get_settings()
        self.session = session or requests.Session()

    def _request(self, sql: str) -> tuple[str, str]:
        body = {
            "apiId": self.settings.qdm_api_id,
            "pageSize": self.settings.page_size,
            "paramMap": {"apiId": self.settings.qdm_api_id, "sql": sql},
        }
        body_str = json.dumps(body, ensure_ascii=False)
        nonce = "".join(random.choices(string.ascii_letters + string.digits, k=12))
        timestamp = int(time.time() * 1000)
        params: dict[str, Any] = {
            "AccessKey": self.settings.qdm_access_key,
            "encrypt": 0,
            "nonce": nonce,
            "timestamp": timestamp,
            "version": self.settings.qdm_version,
            "bodyStr": body_str,
        }
        keys = sorted(key for key, value in params.items() if value not in (None, ""))
        plain = "&".join(f"{key}={params[key]}" for key in keys)
        plain += f"&SecretKey={self.settings.qdm_secret_key}"
        sign = hashlib.md5(plain.encode("utf-8")).hexdigest().upper()
        query = {
            "AccessKey": self.settings.qdm_access_key,
            "timestamp": timestamp,
            "nonce": nonce,
            "encrypt": 0,
            "version": self.settings.qdm_version,
            "sign": sign,
        }
        url = (
            f"{self.settings.qdm_host}/api/v1/executeApi/{self.settings.qdm_api_id}?"
            + "&".join(f"{key}={value}" for key, value in query.items())
        )
        return url, body_str

    def query(self, sql: str) -> pd.DataFrame:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                return self._query_once(sql)
            except (requests.RequestException, RuntimeError) as exc:
                last_error = exc
                if attempt == 3 or isinstance(exc, PaginationContractError):
                    raise
                time.sleep(5)
        raise RuntimeError(str(last_error))

    def _query_once(self, sql: str) -> pd.DataFrame:
        url, body = self._request(sql)
        response = self.session.post(
            url,
            data=body.encode("utf-8"),
            headers={"Content-Type": "application/json"},
            timeout=600,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"QDM API code={payload.get('code')}: {payload.get('msg')}")
        data = payload.get("data", [])
        if isinstance(data, dict) and "pageData" in data:
            page_info = data.get("pageInfo", {})
            total_pages = int(page_info.get("totalPage", 1) or 1)
            if total_pages > 1:
                raise PaginationContractError(
                    f"query returned {total_pages} pages without a proven cursor; shard the source query"
                )
            rows = data.get("pageData", [])
        elif isinstance(data, list):
            rows = data
        else:
            raise RuntimeError(f"unexpected QDM response data type: {type(data).__name__}")
        if len(rows) >= self.settings.page_size:
            raise PaginationContractError(
                f"query returned page_size={self.settings.page_size}; completeness is ambiguous"
            )
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame.columns = [_snake(column) for column in frame.columns]
        return frame
