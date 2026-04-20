# connectors — 数据库连接封装

Pipeline 的 I/O 层。数据读取走 `ApiConnector`（HTTP），中间结果和最终结果统一落 `DuckDBStore`（本地文件或 MotherDuck 云）。

## ApiConnector (`api_connector.py`)

封装 `bdapp.qdama.cn` HTTP API，提供与原 StarRocks 直连相同的 `query()` 接口，**只支持读取**。

```python
api = ApiConnector(cfg)

df = api.query("SELECT ...")   # → pandas DataFrame，自动分页，自动 3 次重试
```

内部实现：每次调用生成新的 `nonce + timestamp + MD5 签名`，通过 POST 请求发送 SQL，自动翻页拼接完整结果集。超时设置 600s。

**WAF 限制（重要）：**
- SQL 中禁止使用 `CASE WHEN`，改用 `IF(condition, true_val, false_val)`
- 多分支判断用嵌套 `IF()`
- `IN (...)` 列表不要超过 300 个值

`ApiConnector` 只有 `query()` 方法，没有写入能力。

## DuckDBStore (`duckdb_store.py`)

进程内单 connection，所有步骤共享同一实例，支持本地文件和 MotherDuck 两种模式。

```python
duck = DuckDBStore()   # 自动读取 cfg.duckdb_conn_str 决定连接方式

duck.load_df(df, "atomic_sales",
             date_col="business_date",
             start="2025-01-01", end="2025-01-07",
             mode="replace_partition")

df   = duck.query("SELECT * FROM atomic_sales WHERE ...")
duck.execute("DROP TABLE IF EXISTS t_calc_profit")
duck.table_exists("atomic_sales")    # → bool
duck.row_count("atomic_sales")       # → int
duck.to_df("t_fm_levels_result")     # SELECT * 转 DataFrame
duck.close()                         # executor finally 块中调用
```

**连接模式：**

| 情况 | 连接串 |
|------|--------|
| `MOTHERDUCK_TOKEN` 非空 | `md:fm_etl_v3?motherduck_token=<token>` |
| `MOTHERDUCK_TOKEN` 为空 | 本地文件路径（`FM_DUCKDB_PATH` 或默认 `data/fm_etl_v3.duckdb`） |

**`load_df` 三种写入模式：**

| mode | 行为 |
|------|------|
| `replace_partition` | 按 `[start, end]` 删除旧行再追加，表不存在时直接 CREATE。幂等，支持重跑。 |
| `replace` | DROP 整表再建，全量覆盖。 |
| `append` | 直接追加，不删除任何旧数据。 |
