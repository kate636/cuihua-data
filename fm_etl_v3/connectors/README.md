# connectors — 数据 I/O 层

Pipeline 的两个外部接口：**读** 走 `ApiConnector`（HTTP），**写+读中间结果** 走 `DuckDBStore`（本地单文件）。

```
QDM BI API  ────ApiConnector.query()────▶  pandas.DataFrame
                                                │
                                                ▼
                                        DuckDBStore.load_df()
                                                │
                                                ▼
                                        /opt/fm/data/fm.duckdb
```

---

## `ApiConnector` (`api_connector.py`)

封装 `bdapp.qdama.cn` HTTP API，**只读**（没有写入方法），对外接口和旧 `StarRocksConnector.query()` 完全一致。

```python
from fm_etl_v3.connectors import ApiConnector
from fm_etl_v3.config import get_settings

api = ApiConnector(get_settings())
df  = api.query("SELECT store_id, SUM(sale_amt) FROM ... GROUP BY 1")
```

### 内部机制

| 环节 | 实现 |
|------|------|
| **签名** | 每次调用生成新的 `nonce`（6 位随机串）+ `timestamp` + MD5 签名，防止重放 |
| **POST body** | `{"apiId":..., "paramMap":{"apiId":..., "sql":"..."}}`，UTF-8 编码 |
| **分页** | 响应包含 `pageInfo.totalPage` 时自动 POST 剩余页并拼接 `pageData` |
| **超时** | 单次请求 600s（10 分钟），足够应付大查询 |
| **重试** | `@retry_on_exception(max_attempts=3, wait_seconds=5.0)`，指数退避 |

签名算法与 `qdm-bi-api` skill 里写的完全一致，如果 API 侧改算法，同步改 `_generate_sign`。

### WAF 限制（**极其重要**）

所有发往 API 的 SQL 必须遵守：

1. **禁止 `CASE WHEN`** — 必须改用 `IF(condition, true_val, false_val)` 或嵌套 `IF()`
2. **禁止 `SELECT *` 直接跑**（WAF 会拦），必须显式列出字段
3. **`IN (...)` 列表 ≤ 300 个值**，超了要分批
4. **分号 `;` 不能出现在 SQL 中间**（哪怕注释里），API 只接受单条语句

`atomic/sales_extractor.py`、`atomic/scm_extractor.py`、`atomic/promo_extractor.py`、`fm_tables/cust.py` 里已经把所有 `CASE WHEN` 转成嵌套 `IF()`。写新的 SQL 时遵循这个模式。

### WAF 踩坑案例

| 错误写法 | 正确写法 |
|---------|----------|
| `CASE WHEN a>0 THEN 1 ELSE 0 END` | `IF(a>0, 1, 0)` |
| `CASE WHEN a=1 THEN 'A' WHEN a=2 THEN 'B' ELSE 'C' END` | `IF(a=1, 'A', IF(a=2, 'B', 'C'))` |
| `WHERE id IN (select ... from big_table)` | 先 `api.query` 取出 id 列表，再分批 `IN (...)` |
| `-- 注释\nSELECT ...;\n-- 另一句` | 只保留一句 SELECT，注释也别带 `;` |

---

## `DuckDBStore` (`duckdb_store.py`)

进程内单 `connection`，pipeline 所有步骤共用同一实例（DuckDB 同一文件不支持多连接并发写）。

```python
from fm_etl_v3.connectors import DuckDBStore

duck = DuckDBStore()                  # 连接路径由 cfg.duckdb_conn_str 决定
# ... pipeline 运行 ...
duck.close()                          # executor finally 块中调用
```

### 方法清单

| 方法 | 用途 |
|------|------|
| `load_df(df, table, date_col, start, end, mode)` | 将 DataFrame 写入表，支持三种模式（见下） |
| `query(sql) -> DataFrame` | 执行查询返回 DataFrame（`conn.execute(sql).df()`） |
| `execute(sql) -> None` | 执行非查询 SQL（CREATE / DELETE 等） |
| `table_exists(table) -> bool` | 检查表是否存在（支持 `schema.table` 语法） |
| `row_count(table) -> int` | 返回 `COUNT(*)`，表不存在返回 0 |
| `to_df(table) -> DataFrame` | `SELECT * FROM table` 的简写 |
| `close()` | 关闭连接（`finally` 块必须调用） |

### `load_df` 的三种写入模式

| mode | 行为 | 典型场景 |
|------|------|---------|
| `replace_partition`（默认） | `DELETE FROM table WHERE date_col BETWEEN start AND end` 然后 `INSERT`；表不存在时直接 `CREATE TABLE AS SELECT * FROM df` | 原子层、计算层、FM 底表增量写入，**幂等重跑** |
| `replace` | `DROP TABLE IF EXISTS` 然后 `CREATE TABLE AS` | 维度表全量替换（`DimsExtractor` 用） |
| `append` | 直接 `INSERT`（表不存在时先 `CREATE`） | 特殊场景（目前 pipeline 未使用） |

### 为什么用 DuckDB 不用 SQLite/PostgreSQL

- **分析型 OLAP 引擎**，列存 + 向量化，JOIN / 聚合比 SQLite 快 10~100 倍
- **零运维**：单文件 `.duckdb`，备份就是 `cp`，迁移就是 `scp`
- **和 pandas 无缝互通**：`conn.execute(sql).df()` 直接返回 DataFrame，`load_df` 通过 `FROM df` 视图写回
- **支持复杂 SQL**：窗口函数、递归 CTE、MAP/LIST 类型，够覆盖所有 `fm_tables/*.py` 的逻辑

### 锁与并发

- DuckDB 同一文件**只允许一个读写连接**
- 可以有任意多个 `read_only=True` 的并发读连接
- 所以 **ETL 写入时**（02:00~02:15 附近），`query_api` 的只读查询可能偶发 `IO Error`
- **约定**：用户端查询统一在 03:00 之后

---

## 错误处理惯例

- `ApiConnector.query()` 失败 3 次后**原样抛出**，不 swallow
- `DuckDBStore.load_df()` 的 `df.empty` 直接跳过（debug 日志），不报错
- executor 捕获所有异常后仍然 `duck.close()`（`finally` 块），避免留下 `.duckdb.wal` 残留文件
