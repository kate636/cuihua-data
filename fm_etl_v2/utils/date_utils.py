"""
日期处理工具

提供日期范围生成、格式转换等工具函数
"""

from datetime import datetime, timedelta, date
from typing import List, Tuple, Optional, Iterator
import pandas as pd


class DateUtils:
    """日期处理工具类"""

    @staticmethod
    def parse_date(date_str: str, fmt: str = "%Y-%m-%d") -> date:
        """
        解析日期字符串

        Args:
            date_str: 日期字符串
            fmt: 日期格式

        Returns:
            date 对象
        """
        return datetime.strptime(date_str, fmt).date()

    @staticmethod
    def format_date(d: date, fmt: str = "%Y-%m-%d") -> str:
        """
        格式化日期

        Args:
            d: date 对象
            fmt: 输出格式

        Returns:
            格式化后的日期字符串
        """
        return d.strftime(fmt)

    @staticmethod
    def date_range(
        start_date: str,
        end_date: str,
        fmt: str = "%Y-%m-%d",
    ) -> List[date]:
        """
        生成日期范围

        Args:
            start_date: 开始日期字符串
            end_date: 结束日期字符串
            fmt: 日期格式

        Returns:
            日期列表
        """
        start = DateUtils.parse_date(start_date, fmt)
        end = DateUtils.parse_date(end_date, fmt)

        dates = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)

        return dates

    @staticmethod
    def date_range_str(
        start_date: str,
        end_date: str,
        fmt: str = "%Y-%m-%d",
    ) -> List[str]:
        """
        生成日期字符串范围

        Args:
            start_date: 开始日期字符串
            end_date: 结束日期字符串
            fmt: 日期格式

        Returns:
            日期字符串列表
        """
        dates = DateUtils.date_range(start_date, end_date, fmt)
        return [DateUtils.format_date(d, fmt) for d in dates]

    @staticmethod
    def iter_dates(
        start_date: str,
        end_date: str,
        fmt: str = "%Y-%m-%d",
    ) -> Iterator[str]:
        """
        迭代日期字符串

        Args:
            start_date: 开始日期字符串
            end_date: 结束日期字符串
            fmt: 日期格式

        Yields:
            日期字符串
        """
        start = DateUtils.parse_date(start_date, fmt)
        end = DateUtils.parse_date(end_date, fmt)

        current = start
        while current <= end:
            yield DateUtils.format_date(current, fmt)
            current += timedelta(days=1)

    @staticmethod
    def get_week_range(d: date) -> Tuple[date, date]:
        """
        获取日期所在周的范围 (周一到周日)

        Args:
            d: 日期

        Returns:
            (周一, 周日) 元组
        """
        weekday = d.weekday()  # 0=周一, 6=周日
        week_start = d - timedelta(days=weekday)
        week_end = week_start + timedelta(days=6)
        return week_start, week_end

    @staticmethod
    def get_month_range(d: date) -> Tuple[date, date]:
        """
        获取日期所在月的范围

        Args:
            d: 日期

        Returns:
            (月初, 月末) 元组
        """
        month_start = d.replace(day=1)
        if d.month == 12:
            month_end = d.replace(day=31)
        else:
            next_month = d.replace(month=d.month + 1, day=1)
            month_end = next_month - timedelta(days=1)
        return month_start, month_end

    @staticmethod
    def get_previous_date(d: date) -> date:
        """
        获取前一天的日期

        Args:
            d: 日期

        Returns:
            前一天的日期
        """
        return d - timedelta(days=1)

    @staticmethod
    def get_next_date(d: date) -> date:
        """
        获取后一天的日期

        Args:
            d: 日期

        Returns:
            后一天的日期
        """
        return d + timedelta(days=1)

    @staticmethod
    def to_pandas_period(dates: List[str], freq: str = "D") -> pd.PeriodIndex:
        """
        将日期字符串列表转换为 Pandas PeriodIndex

        Args:
            dates: 日期字符串列表
            freq: 频率 (D=日, W=周, M=月)

        Returns:
            PeriodIndex
        """
        return pd.PeriodIndex(dates, freq=freq)

    @staticmethod
    def is_business_day(d: date) -> bool:
        """
        判断是否为工作日 (周一到周五)

        Args:
            d: 日期

        Returns:
            是否为工作日
        """
        return d.weekday() < 5

    @staticmethod
    def get_quarter(d: date) -> int:
        """
        获取日期所在季度

        Args:
            d: 日期

        Returns:
            季度 (1-4)
        """
        return (d.month - 1) // 3 + 1

    @staticmethod
    def get_year_week(d: date) -> Tuple[int, int]:
        """
        获取日期的年份和周次

        Args:
            d: 日期

        Returns:
            (年份, 周次) 元组
        """
        iso_year, iso_week, _ = d.isocalendar()
        return iso_year, iso_week
