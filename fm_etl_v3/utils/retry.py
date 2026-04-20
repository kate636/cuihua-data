import time
import logging
from functools import wraps
from typing import Callable, Type, Tuple

logger = logging.getLogger("fm_etl_v3.retry")


def retry_on_exception(
    max_attempts: int = 3,
    wait_seconds: float = 5.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    """通用重试装饰器。"""
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        logger.error(f"{fn.__name__} 失败（第{attempt}次），不再重试: {exc}")
                        raise
                    logger.warning(f"{fn.__name__} 失败（第{attempt}次），{wait_seconds}s 后重试: {exc}")
                    time.sleep(wait_seconds)
        return wrapper
    return decorator
