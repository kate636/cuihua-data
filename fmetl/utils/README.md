# utils — 通用工具

三个零业务依赖的轻量模块，被 `connectors/`、`atomic/`、`calculated/`、`fm_tables/`、`query_api/` 共用。

## 公共入口

```python
from fmetl.utils import get_logger, split_date_range, retry_on_exception
```

`__init__.py` 只暴露这三个名字。

---

## `logger.py` — 结构化日志

```python
_log = get_logger("my_module")
_log.info("done")
# [2026-04-20 12:00:00] INFO [my_module] done
```

**特性**：
- 输出到 `stdout`，格式 `[时间戳] LEVEL [name] message`
- 同名 logger 只注册一次 handler（避免重复输出）
- 默认 level = `INFO`，可通过 `logging.getLogger("my_module").setLevel(logging.DEBUG)` 临时调高
- **pipeline 每个步骤都有独立 logger**（名字见各模块源码），云端日志文件会把所有步骤合并

**为什么不用 structlog / loguru**：依赖最小化，标准 `logging` 够用，部署不折腾。

---

## `date_utils.py` — 日期分段

```python
from fm_etl_v3.utils import split_date_range

# 把长区间切成 7 天一段，闭区间
segments = split_date_range("2026-01-01", "2026-01-31", chunk=7)
# [('2026-01-01', '2026-01-07'),
#  ('2026-01-08', '2026-01-14'),
#  ('2026-01-15', '2026-01-21'),
#  ('2026-01-22', '2026-01-28'),
#  ('2026-01-29', '2026-01-31')]   ← 最后一段自动截断
```

**为什么切 7 天**：QDM BI API 单次查询有行数和超时限制，分片既能避开 WAF，也能让重试粒度更小（某一片挂掉只重跑那一片）。

**`BaseExtractor.extract()` 内部就是用这个函数做分片。**

---

## `retry.py` — 指数退避重试

```python
from fm_etl_v3.utils import retry_on_exception

@retry_on_exception(max_attempts=3, wait_seconds=5.0, backoff=2.0)
def unstable_call():
    ...
```

**行为**：
1. 首次失败等 `wait_seconds` 秒后重试
2. 每次失败等待时间 ×`backoff`（默认 5s → 10s → 20s）
3. 达到 `max_attempts` 后抛出**原始异常**（带完整 traceback）
4. 只捕获 `Exception`，不包括 `KeyboardInterrupt` / `SystemExit`

**当前使用点**：
- `ApiConnector.query()` 每次查询默认 `max_attempts=3, wait_seconds=5.0`
- 如需关闭重试，直接调内部方法或传 `max_attempts=1`

---

## 扩展指南

**什么场景放这里**：
- 被 2 个以上模块共用
- 无业务逻辑（纯粹工具）
- 零外部依赖（或只依赖标准库）

**什么场景不放这里**：
- 和 DuckDB / API 强耦合的 → 放 `connectors/`
- 和具体业务表有关的 → 放对应 `atomic/` `calculated/` `fm_tables/`
