"""
FM ETL v2.0

基于指标字典 v3 的 ETL 管道
- 原子层: SQL 取数 → Parquet 存储
- 计算层: Python 本地计算
- 输出层: 写入 StarRocks 新表
"""

__version__ = "2.0.0"
