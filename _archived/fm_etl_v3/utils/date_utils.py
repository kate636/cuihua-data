from datetime import datetime, timedelta
from typing import List, Tuple


def split_date_range(start: str, end: str, interval: int = 7) -> List[Tuple[str, str]]:
    """将 [start, end] 日期范围按 interval 天切分成若干段。"""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end,   "%Y-%m-%d")
    segments: List[Tuple[str, str]] = []
    cur = s
    while cur <= e:
        seg_end = min(cur + timedelta(days=interval - 1), e)
        segments.append((cur.strftime("%Y-%m-%d"), seg_end.strftime("%Y-%m-%d")))
        cur = seg_end + timedelta(days=1)
    return segments


def yesterday(ref: str) -> str:
    """返回 ref 日期的前一天（YYYY-MM-DD）。"""
    d = datetime.strptime(ref, "%Y-%m-%d")
    return (d - timedelta(days=1)).strftime("%Y-%m-%d")


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")
