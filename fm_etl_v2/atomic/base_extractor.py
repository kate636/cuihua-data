"""
原子层取数器基类

定义原子层取数器的抽象接口和通用方法
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import logging

from ..config.settings import Settings, get_settings
from ..utils.parquet_handler import ParquetHandler
from ..utils.logger import get_logger


class BaseExtractor(ABC):
    """
    原子层取数器基类

    所有域取数器继承此类，实现具体的 SQL 生成和数据处理逻辑
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        初始化取数器

        Args:
            settings: 配置对象 (可选，默认使用全局配置)
        """
        self.settings = settings or get_settings()
        self.logger = get_logger(f"extractor.{self.domain_name}")
        self.parquet_handler = ParquetHandler(self.settings.paths.atomic_dir)
        self._conn = None

    @property
    @abstractmethod
    def domain_name(self) -> str:
        """
        域名称

        Returns:
            域名称字符串 (如 "sale", "purchase" 等)
        """
        pass

    @property
    @abstractmethod
    def atomic_fields(self) -> List[str]:
        """
        原子字段列表

        只包含不可由其他字段推导的独立观测量

        Returns:
            原子字段名称列表
        """
        pass

    @property
    def key_fields(self) -> List[str]:
        """
        主键字段列表

        默认为门店、日期、商品

        Returns:
            主键字段名称列表
        """
        return ["store_id", "business_date", "article_id"]

    @abstractmethod
    def get_sql(self, start_date: str, end_date: str) -> str:
        """
        生成取数 SQL

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            SQL 查询字符串
        """
        pass

    def get_connection(self):
        """
        获取数据库连接

        使用 MySQL 协议连接 StarRocks

        Returns:
            数据库连接对象
        """
        if self._conn is None:
            try:
                from sqlalchemy import create_engine
                engine = create_engine(self.settings.starrocks.connection_string)
                self._conn = engine.connect()
                self.logger.debug("Database connection established")
            except Exception as e:
                self.logger.error(f"Failed to establish database connection: {e}")
                raise

        return self._conn

    def close_connection(self) -> None:
        """关闭数据库连接"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self.logger.debug("Database connection closed")

    def extract(
        self,
        start_date: str,
        end_date: str,
        chunk_size: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        执行取数并返回 DataFrame

        Args:
            start_date: 开始日期
            end_date: 结束日期
            chunk_size: 分块大小 (可选，用于大数据量)

        Returns:
            取数结果 DataFrame
        """
        sql = self.get_sql(start_date, end_date)
        self.logger.info(f"Executing SQL for date range: {start_date} to {end_date}")
        self.logger.debug(f"SQL: {sql[:500]}...")  # 只打印前 500 字符

        try:
            conn = self.get_connection()

            if chunk_size:
                # 分块读取
                chunks = []
                for chunk in pd.read_sql(sql, conn, chunksize=chunk_size):
                    chunks.append(self._post_process(chunk))
                df = pd.concat(chunks, ignore_index=True)
            else:
                # 一次性读取
                df = pd.read_sql(sql, conn)
                df = self._post_process(df)

            self.logger.info(f"Extracted {len(df)} rows, columns: {list(df.columns)}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to extract data: {e}")
            raise

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        后处理钩子

        子类可重写此方法进行数据清洗、类型转换等

        Args:
            df: 原始 DataFrame

        Returns:
            处理后的 DataFrame
        """
        # 默认不做处理
        return df

    def save_to_parquet(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
    ) -> Path:
        """
        保存到 Parquet 文件

        Args:
            df: 要保存的 DataFrame
            filename: 文件名 (可选，默认使用域名称)

        Returns:
            保存的文件路径
        """
        if filename is None:
            filename = self.domain_name

        output_path = self.parquet_handler.save(df, filename)
        self.logger.info(f"Saved {len(df)} rows to {output_path}")
        return output_path

    def load_from_parquet(
        self,
        filename: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        从 Parquet 文件加载

        Args:
            filename: 文件名 (可选，默认使用域名称)
            columns: 要加载的列 (可选)

        Returns:
            加载的 DataFrame
        """
        if filename is None:
            filename = self.domain_name

        df = self.parquet_handler.load(filename, columns=columns)
        self.logger.info(f"Loaded {len(df)} rows from Parquet")
        return df

    def extract_and_save(
        self,
        start_date: str,
        end_date: str,
        filename: Optional[str] = None,
    ) -> Path:
        """
        取数并保存到 Parquet

        Args:
            start_date: 开始日期
            end_date: 结束日期
            filename: 文件名 (可选)

        Returns:
            保存的文件路径
        """
        df = self.extract(start_date, end_date)
        return self.save_to_parquet(df, filename)

    def validate_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        验证数据质量

        Args:
            df: 要验证的 DataFrame

        Returns:
            验证结果字典
        """
        result = {
            "domain": self.domain_name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "null_counts": df.isnull().sum().to_dict(),
            "duplicate_keys": 0,
        }

        # 检查主键重复
        if all(k in df.columns for k in self.key_fields):
            key_duplicates = df.duplicated(subset=self.key_fields).sum()
            result["duplicate_keys"] = int(key_duplicates)

        # 检查必需字段
        missing_fields = [f for f in self.atomic_fields if f not in df.columns]
        result["missing_fields"] = missing_fields

        return result

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_connection()
        return False
