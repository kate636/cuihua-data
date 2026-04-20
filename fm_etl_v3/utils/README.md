# utils — 通用工具

三个轻量工具模块，被 `connectors/`、`atomic/`、`calculated/`、`fm_tables/` 各层共用。

## 文件

### `logger.py` — 结构化日志

```python
from fm_etl_v3.utils import get_logger
_log = get_logger("my_module")
_log.info("done")   # [2025-01-01 12:00:00] INFO [my_module] done
```

- 输出格式：`[时间戳] 级别 [模块名] 消息`，打到 stdout。
- 同名 logger 只注册一次 handler，避免重复输出。

### `date_utils.py` — 日期分段

```python
from fm_etl_v3.utils import split_date_range, yesterday

# 将长区间切成 7 天一段，用于原子域分批提取
segments = split_date_range("2025-01-01", "2025-01-31", chunk=7)
# → [('2025-01-01','2025-01-07'), ('2025-01-08','2025-01-14'), ...]

yesterday("2025-01-31")  # → '2025-01-30'
yesterday()              # → 今天前一天
```

`split_date_range` 返回首尾均包含的闭区间列表，最后一段自动截断到 `end`。

### `retry.py` — 指数退避重试

```python
from fm_etl_v3.utils import with_retry

@with_retry(max_attempts=3, delay=5.0, backoff=2.0)
def unstable_query():
    ...
```

- 首次失败等 `delay` 秒，之后每次乘以 `backoff`（默认 5s → 10s → 20s）。
- 耗尽重试次数后抛出原始异常。
- `StarRocksConnector.query()` 和 `.execute()` 默认使用 `@with_retry(max_attempts=3, delay=5.0)`。
