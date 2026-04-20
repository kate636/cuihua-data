#!/usr/bin/env python3
"""
FM ETL v2.0 主入口

用法:
    python -m fm_etl_v2.main --start-date 2025-01-01 --end-date 2025-01-31
    python -m fm_etl_v2.main --start-date 2025-01-15 --end-date 2025-01-15  # 单日执行

参数:
    --start-date: 开始日期 (YYYY-MM-DD)
    --end-date: 结束日期 (YYYY-MM-DD)
    --skip-atomic: 跳过原子层取数 (使用已有 Parquet)
    --skip-calculation: 跳过计算层 (使用已有 Parquet)
    --use-bom: 启用 BOM 成本分摊
    --output-only: 只输出到 Parquet，不写入数据库
    --config: 配置文件路径 (可选)
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime

from .config.settings import Settings, get_settings, set_settings
from .utils.logger import get_logger, TaskLogger
from .atomic import extract_all_domains
from .calculated import run_calculations
from .output import build_output_tables


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="FM ETL v2.0")
    parser.add_argument("--start-date", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--skip-atomic", action="store_true", help="跳过原子层取数")
    parser.add_argument("--skip-calculation", action="store_true", help="跳过计算层")
    parser.add_argument("--use-bom", action="store_true", help="启用 BOM 成本分摊")
    parser.add_argument("--output-only", action="store_true", help="只输出到 Parquet")
    parser.add_argument("--config", type=str, help="配置文件路径")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    parser.add_argument("--log-file", type=str, help="日志文件路径")
    args = parser.parse_args()

    # 设置日志
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    log_file = Path(args.log_file) if args.log_file else None
    logger = get_logger("fm_etl_v2", level=log_level, log_file=log_file)

    # 加载配置
    if args.config:
        settings = Settings.from_file(Path(args.config))
        set_settings(settings)
    else:
        settings = get_settings()

    # 记录开始
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"FM ETL v2.0 started at {start_time}")
    logger.info(f"Date range: {args.start_date} to {args.end_date}")
    logger.info(f"Settings: skip_atomic={args.skip_atomic}, use_bom={args.use_bom}")
    logger.info("=" * 60)

    try:
        # Step 1: 原子层取数
        if not args.skip_atomic:
            with TaskLogger("Atomic Layer Extraction", logger):
                atomic_results = extract_all_domains(settings, args.start_date, args.end_date)
                logger.info(f"Atomic extraction completed: {len(atomic_results)} domains")
        else:
            logger.info("Skipping atomic layer extraction")

        # Step 2: 计算层处理
        if not args.skip_calculation:
            with TaskLogger("Calculation Layer", logger):
                calculated_df = run_calculations(
                    settings,
                    args.start_date,
                    args.end_date,
                    use_bom=args.use_bom,
                )
                logger.info(f"Calculation completed: {len(calculated_df)} rows")
        else:
            logger.info("Skipping calculation layer")

        # Step 3: 输出层构建
        with TaskLogger("Output Layer", logger):
            output_results = build_output_tables(settings, args.start_date, args.end_date)
            logger.info(f"Output completed: {output_results}")

        # Step 4: 写入数据库 (可选)
        if not args.output_only:
            with TaskLogger("Database Write", logger):
                # TODO: 实现 StarRocks 写入
                logger.info("Database write not implemented yet")

        # 记录完成
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info(f"FM ETL v2.0 completed at {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        raise


def run_single_date(date_str: str, use_bom: bool = False) -> None:
    """
    运行单日 ETL

    Args:
        date_str: 日期字符串 (YYYY-MM-DD)
        use_bom: 是否使用 BOM 成本分摊
    """
    settings = get_settings()
    logger = get_logger("fm_etl_v2")

    logger.info(f"Running ETL for {date_str}")

    # 原子层
    extract_all_domains(settings, date_str, date_str)

    # 计算层
    run_calculations(settings, date_str, date_str, use_bom=use_bom)

    # 输出层
    build_output_tables(settings, date_str, date_str)

    logger.info(f"ETL completed for {date_str}")


if __name__ == "__main__":
    main()
