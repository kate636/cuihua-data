# query_api — DuckDB 只读 HTTP 查询服务

FastAPI 微服务，云端 `systemd` 托管，监听 `127.0.0.1:5003`，通过 nginx `location /api/` 反代对外（`http://47.115.213.115:8080/api/`）。让 5 人团队不登服务器也能查 `fm.duckdb`。

---

## 四重安全防护

| 防护层 | 实现 |
|-------|------|
| **Bearer Token 鉴权** | `auth.py` 解析 `FM_TOKENS` 环境变量，请求头 `Authorization: Bearer <token>` |
| **SQL 白名单守卫** | `sql_guard.py` 只允许 SELECT/SHOW/DESCRIBE/EXPLAIN/WITH 开头 |
| **只读 DuckDB 连接** | `duckdb.connect(path, read_only=True)`（即使守卫被绕过也写不进去） |
| **单查询超时** | 默认 60 秒（`FM_QUERY_TIMEOUT_SEC`，通过 `SET statement_timeout` 硬限） |

---

## 文件职责

| 文件 | 内容 |
|------|------|
| `app.py` | FastAPI 实例、路由、只读连接上下文管理、CORS 中间件 |
| `auth.py` | `verify_token` 依赖，Token 恒定时间比对（`secrets.compare_digest` 防时序攻击） |
| `sql_guard.py` | 四层过滤：长度 / 首关键字 / 危险关键字 / 禁止多语句 |
| `models.py` | Pydantic 请求/响应模型（`QueryRequest`、`QueryResponse`、`TableInfo` 等） |
| `__init__.py` | `__version__` |

---

## 端点

### `GET /api/health`

**不需要鉴权**。用于监控、探活。

```json
{"status": "ok", "db_path": "/opt/fm/data/fm.duckdb"}
```

### `GET /api/tables`

列出所有表（排除 `information_schema` 和 `pg_catalog`），附带行数。

```bash
curl http://47.115.213.115:8080/api/tables \
  -H "Authorization: Bearer <TOKEN>"
```

响应：
```json
{
  "tables": [
    {"name": "atomic_sales", "row_count": 12345678, "schema_name": "main"},
    {"name": "t_fm_levels_result", "row_count": 456789, "schema_name": "main"}
  ],
  "database_path": "/opt/fm/data/fm.duckdb"
}
```

### `GET /api/schema/{table}`

看某张表的字段列表和行数。表名必须是合法 identifier（`^[A-Za-z_][A-Za-z0-9_]*$`），防 SQL 注入。

```bash
curl http://47.115.213.115:8080/api/schema/t_fm_levels_result \
  -H "Authorization: Bearer <TOKEN>"
```

### `POST /api/query`

执行 SELECT SQL 返回 JSON 行。

**请求体**：
```json
{
  "sql": "SELECT store_no, SUM(sale_amt) FROM t_fm_sku_dim WHERE business_date='2026-04-19' GROUP BY 1",
  "limit": 10000
}
```

- `sql`：最长 20000 字符，SQL 守卫校验通过才执行
- `limit`：1~50000，默认 10000（服务端再用 `LIMIT limit+1` 包一层来探测是否被截断）

**响应**：
```json
{
  "rows": [{"store_no":"GZ001","SUM(sale_amt)":12345.6}, ...],
  "row_count": 500,
  "elapsed_ms": 123,
  "truncated": false
}
```

`truncated: true` 表示结果被 `limit` 截断（实际有更多数据），需要改 SQL 加 `LIMIT` / `WHERE` 缩小范围。

---

## SQL 守卫规则

### 允许（首关键字白名单）

```
SELECT, SHOW, DESCRIBE, DESC, EXPLAIN, WITH, PRAGMA_TABLE_INFO
```

### 禁止（黑名单关键字出现任何位置都拒绝）

```
INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE,
ATTACH, DETACH, COPY, EXPORT, IMPORT,
PRAGMA, SET, RESET, LOAD, INSTALL, CALL,
GRANT, REVOKE, BEGIN, COMMIT, ROLLBACK,
VACUUM, ANALYZE, CHECKPOINT
```

### 其他约束

- 禁止多语句（`;` 后有非空白非注释内容）
- 末尾可以有 `;`（会 strip）
- `--` 单行注释和 `/* ... */` 多行注释都会先去除后再检测关键字
- 字符串字面量内的关键字被忽略（`'DROP'` 在 `'...'` 里不算）

### 被拒绝的返回

```json
{"detail": "SQL rejected by guard: Forbidden keyword 'DROP' in SQL"}
```

HTTP 400，响应体是 `ErrorResponse`。

---

## 启动方式

### 云端生产（systemd）

```bash
systemctl start fm-query-api
systemctl status fm-query-api
journalctl -u fm-query-api -f
```

单元文件 `deploy/fm-query-api.service`：用 `uvicorn` 启动 2 个 worker，只监听 `127.0.0.1:5003`，通过 `EnvironmentFile=.env` 注入 Token 和 DB 路径。

### 本地开发调试

```bash
# 先确保本地 data/fm_etl_v3.duckdb 有数据
export FM_DUCKDB_PATH=data/fm_etl_v3.duckdb
export FM_TOKENS=me:test123

uvicorn fm_etl_v3.query_api.app:app --host 127.0.0.1 --port 5003 --reload
```

然后：
```bash
curl http://127.0.0.1:5003/api/health
curl http://127.0.0.1:5003/api/tables -H "Authorization: Bearer test123"
```

---

## 环境变量

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `FM_DUCKDB_PATH` | ❌ | `/opt/fm/data/fm.duckdb` | DuckDB 文件路径 |
| `FM_TOKENS` | ✅ | — | `user1:token1,user2:token2` 格式 |
| `FM_QUERY_TIMEOUT_SEC` | ❌ | `60` | 单查询超时 |

**Token 生成**：
```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

**Token 轮换**：编辑 `.env` 里 `FM_TOKENS` 后 `systemctl restart fm-query-api` 即生效。

---

## 设计决策 FAQ

**为什么每请求新建 DuckDB 连接，不用连接池？**  
DuckDB 单文件在同一进程内共享连接是最快，但 `read_only=True` 可以无限并发。为简化错误隔离（某查询异常不影响其他），选择每请求一次 `connect` / `close`。延迟可忽略（<5ms）。

**为什么 SQL 守卫要正则而不是 SQL 解析器？**  
守卫是**第二层防护**，只读连接是终极保障。DuckDB 没有成熟的 Python SQL 解析器，维护成本高；正则 + 只读连接双保险足够。

**为什么 CORS 开放 `*`？**  
内网 nginx:8080 前置，真实访问控制在 nginx 层（IP 白名单）和 Bearer Token，CORS 不承担安全责任。

**为什么不支持 write？**  
写入只能通过 ETL，走 git push → cron 02:00 自动生效。Query API 是纯消费者，这样所有写入都有 git 留痕，审计方便。

**分析师查大数据量超时怎么办？**  
两条路：
1. 改 SQL 加 `WHERE` 缩小范围 + `LIMIT`
2. 用 DBeaver + SSH 隧道直连 DuckDB，没有 60s 超时限制（见 [../docs/TEAM_ACCESS.md](../docs/TEAM_ACCESS.md)）
