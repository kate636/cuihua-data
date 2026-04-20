"""
日志工具

提供统一的日志配置和获取接口
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def get_logger(
    name: str = "fm_etl_v2",
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """
    获取配置好的 Logger 实例

    Args:
        name: Logger 名称
        level: 日志级别
        log_file: 日志文件路径 (可选)

    Returns:
        配置好的 Logger 实例
    """
    logger = logging.getLogger(name)

    # 避免重复配置
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # 日志格式
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器 (可选)
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


class TaskLogger:
    """
    任务日志记录器

    用于记录 ETL 任务执行过程中的关键步骤和耗时
    """

    def __init__(self, task_name: str, logger: Optional[logging.Logger] = None):
        """
        初始化任务日志记录器

        Args:
            task_name: 任务名称
            logger: Logger 实例 (可选)
        """
        self.task_name = task_name
        self.logger = logger or get_logger()
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        """进入上下文"""
        import time
        self.start_time = time.time()
        self.logger.info(f"[{self.task_name}] Started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文"""
        import time
        self.end_time = time.time()
        duration = self.end_time - self.start_time

        if exc_type:
            self.logger.error(f"[{self.task_name}] Failed after {duration:.2f}s: {exc_val}")
        else:
            self.logger.info(f"[{self.task_name}] Completed in {duration:.2f}s")

        return False  # 不抑制异常

    def info(self, message: str) -> None:
        """记录信息日志"""
        self.logger.info(f"[{self.task_name}] {message}")

    def warning(self, message: str) -> None:
        """记录警告日志"""
        self.logger.warning(f"[{self.task_name}] {message}")

    def error(self, message: str) -> None:
        """记录错误日志"""
        self.logger.error(f"[{self.task_name}] {message}")

    def debug(self, message: str) -> None:
        """记录调试日志"""
        self.logger.debug(f"[{self.task_name}] {message}")
