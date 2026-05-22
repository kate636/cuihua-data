"""
Parquet 读写工具

处理原子层、计算层、输出层的 Parquet 文件读写
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class ParquetHandler:
    """
    Parquet 文件处理器

    提供读写 Parquet 文件的统一接口
    """

    def __init__(self, base_dir: Path):
        """
        初始化处理器

        Args:
            base_dir: 基础目录路径
        """
        self.base_dir = Path(base_dir)

    def save(
        self,
        df: pd.DataFrame,
        filename: str,
        subdir: Optional[str] = None,
        partition_cols: Optional[List[str]] = None,
        compression: str = "snappy",
    ) -> Path:
        """
        保存 DataFrame 到 Parquet 文件

        Args:
            df: 要保存的 DataFrame
            filename: 文件名 (不含扩展名)
            subdir: 子目录名称
            partition_cols: 分区列
            compression: 压缩算法

        Returns:
            保存的文件路径
        """
        # 构建输出路径
        output_dir = self.base_dir
        if subdir:
            output_dir = output_dir / subdir
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f"{filename}.parquet"

        # 保存文件
        if partition_cols:
            # 分区保存
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=str(output_dir),
                partition_cols=partition_cols,
                compression=compression,
            )
            logger.info(f"Saved partitioned Parquet to {output_dir}")
            return output_dir
        else:
            # 单文件保存
            df.to_parquet(output_path, compression=compression, index=False)
            logger.info(f"Saved Parquet to {output_path}")
            return output_path

    def load(
        self,
        filename: str,
        subdir: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List] = None,
    ) -> pd.DataFrame:
        """
        从 Parquet 文件加载 DataFrame

        Args:
            filename: 文件名 (不含扩展名)
            subdir: 子目录名称
            columns: 要加载的列
            filters: 过滤条件

        Returns:
            加载的 DataFrame
        """
        # 构建文件路径
        file_path = self.base_dir
        if subdir:
            file_path = file_path / subdir
        file_path = file_path / f"{filename}.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")

        # 加载文件
        df = pd.read_parquet(file_path, columns=columns, filters=filters)
        logger.info(f"Loaded Parquet from {file_path}, shape: {df.shape}")
        return df

    def load_partitioned(
        self,
        subdir: str,
        columns: Optional[List[str]] = None,
        filters: Optional[List] = None,
    ) -> pd.DataFrame:
        """
        从分区 Parquet 数据集加载 DataFrame

        Args:
            subdir: 子目录名称 (分区数据集根目录)
            columns: 要加载的列
            filters: 过滤条件

        Returns:
            加载的 DataFrame
        """
        dataset_path = self.base_dir / subdir

        if not dataset_path.exists():
            raise FileNotFoundError(f"Partitioned dataset not found: {dataset_path}")

        # 加载分区数据集
        df = pd.read_parquet(dataset_path, columns=columns, filters=filters)
        logger.info(f"Loaded partitioned Parquet from {dataset_path}, shape: {df.shape}")
        return df

    def exists(self, filename: str, subdir: Optional[str] = None) -> bool:
        """
        检查 Parquet 文件是否存在

        Args:
            filename: 文件名
            subdir: 子目录名称

        Returns:
            文件是否存在
        """
        file_path = self.base_dir
        if subdir:
            file_path = file_path / subdir
        file_path = file_path / f"{filename}.parquet"
        return file_path.exists()

    def list_files(self, subdir: Optional[str] = None) -> List[Path]:
        """
        列出目录中的所有 Parquet 文件

        Args:
            subdir: 子目录名称

        Returns:
            Parquet 文件路径列表
        """
        dir_path = self.base_dir
        if subdir:
            dir_path = dir_path / subdir

        if not dir_path.exists():
            return []

        return list(dir_path.glob("*.parquet"))

    def merge_files(
        self,
        subdir: str,
        output_filename: str,
        output_subdir: Optional[str] = None,
    ) -> Path:
        """
        合并目录中的所有 Parquet 文件

        Args:
            subdir: 源子目录
            output_filename: 输出文件名
            output_subdir: 输出子目录

        Returns:
            合并后的文件路径
        """
        files = self.list_files(subdir)
        if not files:
            raise FileNotFoundError(f"No Parquet files found in {subdir}")

        dfs = [pd.read_parquet(f) for f in files]
        merged_df = pd.concat(dfs, ignore_index=True)

        return self.save(merged_df, output_filename, output_subdir)
