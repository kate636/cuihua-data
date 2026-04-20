# 翠花数据 · 团队接入指南

> 5 人团队远程查询 `/opt/fm/data/fm.duckdb` 的三种方式。按角色对号入座。

## 服务器信息

- 地址：`47.115.213.115`（阿里云广州）
- 对外端口：`8080`（nginx，现有）
- DuckDB 文件：`/opt/fm/data/fm.duckdb`（云服务器本地，**不上 GitHub**）

---

## 角色 1：开发者（改代码）

### Cursor Remote-SSH

**Step 1** — 本地 `~/.ssh/config` 追加：
```
Host fm-prod
    HostName 47.115.213.115
    User root
    IdentityFile ~/.ssh/id_rsa
    ServerAliveInterval 60
    ServerAliveCountMax 3
```

**Step 2** — 找管理员把你的公钥（`~/.ssh/id_rsa.pub`）加到服务器 `~/.ssh/authorized_keys`。

**Step 3** — 验证：
```bash
ssh fm-prod 'echo ok'
```

**Step 4** — Cursor 底部状态栏 → 远程窗口图标 → Connect to Host → `fm-prod` → 打开 `/opt/fm/etl/cuihua-data`。

---

## 角色 2：分析师（写 SQL 查数据）

### 方式 A：DBeaver + SSH 隧道（推荐，支持大数据量）

**Step 1** — 装 DBeaver Community（免费）：https://dbeaver.io/

**Step 2** — 新建连接 → DuckDB

**Step 3** — 主连接：
- Path: `/opt/fm/data/fm.duckdb`
- URL（手动编辑）：`jdbc:duckdb:/opt/fm/data/fm.duckdb?access_mode=read_only`

> `access_mode=read_only` **必须**加，避免和 ETL 抢写锁。

**Step 4** — SSH 选项卡 → Use SSH Tunnel：
- Host: `47.115.213.115`
- Port: `22`
- User: 你的 Linux 账号
- Authentication: Public Key
- Private Key: `~/.ssh/id_rsa`

**Step 5** — Test Connection → 看到 `Connected` 就成功。

### 方式 B：HTTP API（轻量，Postman/curl/浏览器）

找管理员领你的 Token，然后直接调：

```bash
# 列出所有表
curl http://47.115.213.115:8080/api/tables \
  -H "Authorization: Bearer YOUR_TOKEN"

# 查看表结构
curl http://47.115.213.115:8080/api/schema/atomic_sales \
  -H "Authorization: Bearer YOUR_TOKEN"

# 执行 SELECT
curl -X POST http://47.115.213.115:8080/api/query \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT sku_id, SUM(sales) FROM atomic_sales WHERE biz_date = '\''2026-04-19'\'' GROUP BY 1 ORDER BY 2 DESC LIMIT 20",
    "limit": 20
  }'
```

**限制**：
- 只允许 `SELECT` / `SHOW` / `DESCRIBE` / `EXPLAIN` / `WITH` 开头
- 禁止多语句（`;` 后不能再跟东西）
- 单查询 60 秒超时
- 默认返回 10000 行，最多 50000 行（`limit` 参数）

### 方式 C：Python / pandas（本地脚本）

```python
import requests

TOKEN = "YOUR_TOKEN"
API = "http://47.115.213.115:8080/api/query"

resp = requests.post(API, json={
    "sql": "SELECT * FROM atomic_sales WHERE biz_date = '2026-04-19' LIMIT 1000",
    "limit": 1000,
}, headers={"Authorization": f"Bearer {TOKEN}"})

data = resp.json()
print(f"{data['row_count']} rows in {data['elapsed_ms']}ms")

import pandas as pd
df = pd.DataFrame(data["rows"])
```

---

## 角色 3：AI 分析（Cursor Claude）

零开发，两种姿势：

**姿势 1 — Cursor Remote-SSH 窗口里问**：
在连了 `fm-prod` 的 Cursor 窗口里直接问 AI，AI 会用 `duckdb /opt/fm/data/fm.duckdb "SELECT ..."` 查表，把结果翻译成人话。

**姿势 2 — 本地 Cursor 调 HTTP API**：
问 AI 时让它 `curl` `http://47.115.213.115:8080/api/query`，AI 自己构造 SQL、解析 JSON、分析。

---

## 角色 4：老板（看看板）

无任何变化：继续打开 `http://47.115.213.115:8080/reports/index.html`。

---

## Token 分发（管理员维护）

| 用户 | Token | 发放 | 备注 |
|---|---|---|---|
| （待填） | `xxx` | 2026-04-20 | 生成后从 `.env` 复制 |

### 新增用户

1. SSH 上服务器：`ssh fm-prod`
2. 生成 Token：
   ```bash
   python3 -c "import secrets; print(secrets.token_urlsafe(48))"
   ```
3. 编辑 `/opt/fm/etl/cuihua-data/.env` → 追加到 `FM_TOKENS` 末尾（格式 `用户名:token`，逗号分隔）
4. `systemctl restart fm-query-api`
5. 发 Token 给用户（走私信/钉钉加密消息）

### 作废 Token

编辑 `.env` 移除对应条目 → `systemctl restart fm-query-api`。

---

## 常用 SQL 速查

```sql
-- 查看所有表
SHOW TABLES;

-- 某表结构
DESCRIBE atomic_sales;

-- 行数统计
SELECT COUNT(*) FROM atomic_sales;

-- 昨天大盘
SELECT biz_date, SUM(sales) AS total_sales
FROM atomic_sales
WHERE biz_date = CURRENT_DATE - INTERVAL 1 DAY
GROUP BY biz_date;

-- 品类 TOP 10
SELECT category, SUM(sales) AS s
FROM atomic_sales
WHERE biz_date BETWEEN '2026-04-01' AND '2026-04-19'
GROUP BY category
ORDER BY s DESC
LIMIT 10;
```
