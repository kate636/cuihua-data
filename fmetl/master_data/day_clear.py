from __future__ import annotations

import pandas as pd


def resolve_day_clear(chdj_day_clear: object) -> str:
    if pd.isna(chdj_day_clear):
        raise ValueError("A3XV day_clear is missing; no fallback is authorized")
    value = str(chdj_day_clear)
    if value not in {"0", "1"}:
        raise ValueError(f"unexpected day_clear={value!r}")
    return value
