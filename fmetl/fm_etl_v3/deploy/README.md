# deploy — 云端部署产物

这个目录只包含 3 个文件，**原样** `cp` 到服务器对应位置即可。整体部署流程请看 [../DEPLOY.md](../DEPLOY.md)。

---

## 文件清单

| 文件 | 服务器目标位置 | 触发方式 |
|------|---------------|---------|
| `daily_run.sh` | `/opt/fm/etl/daily_run.sh` | cron 每日 02:00 调用 |
| `fm-query-api.service` | `/etc/systemd/system/fm-query-api.service` | systemd 开机/按需启动 |
| `nginx-api.conf` | 追加到现有 nginx :8080 的 server block | nginx reload 后生效 |

---

## `daily_run.sh` — 每日 ETL

**位置**：`/opt/fm/etl/daily_run.sh`（注意：不是 `/opt/fm/etl/cuihua-data/` 下，放在 cuihua-data 外边是为了 **git pull 冲突时脚本本身不会消失**）

**流程**：
1. `cd /opt/fm/etl/cuihua-data`
2. `git pull --ff-only origin main` — 拉最新代码，遇冲突**不合并**直接报错退出（ETL 不跑，数据不污染）
3. `source .venv/bin/activate`
4. `YESTERDAY=$(date -d 'yesterday' +%Y-%m-%d)` — 兼容 GNU `date` 和 BSD `date`
5. `python -m fm_etl_v3.executor ${YESTERDAY} ${YESTERDAY}` — 跑昨天一天
6. 输出双写：`tee -a /opt/fm/logs/etl-YYYY-MM.log`（按月聚合）

**cron 注册**（一次性）：

```bash
( crontab -l 2>/dev/null; echo "0 2 * * * /opt/fm/etl/daily_run.sh >> /opt/fm/logs/cron.log 2>&1" ) | crontab -
```

**排查**：
- `/opt/fm/logs/cron.log` — 看 cron 本身和 `git pull` 是否成功
- `/opt/fm/logs/etl-$(date +%Y-%m).log` — 看 ETL 各 Step 日志

---

## `fm-query-api.service` — systemd 单元

**位置**：`/etc/systemd/system/fm-query-api.service`

**关键配置**：

| 指令 | 值 | 说明 |
|------|-----|------|
| `WorkingDirectory` | `/opt/fm/etl/cuihua-data` | 模块导入需要在 repo 根 |
| `EnvironmentFile` | `/opt/fm/etl/cuihua-data/.env` | 注入 `FM_DUCKDB_PATH` / `FM_TOKENS` |
| `ExecStart` | `uvicorn fm_etl_v3.query_api.app:app --host 127.0.0.1 --port 5003 --workers 2` | **仅监听 127.0.0.1**，不暴露公网 |
| `Restart` | `on-failure` | 崩溃自动重启 |
| `RestartSec` | `5` | 5 秒后再起，防止抖动 |
| `NoNewPrivileges` | `true` | 安全加固 |
| `ProtectSystem` | `full` | 禁止写系统目录 |
| `ReadWritePaths` | `/opt/fm/logs` | 只允许写日志目录 |

**管理命令**：

```bash
# 首次安装
cp /opt/fm/etl/cuihua-data/fm_etl_v3/deploy/fm-query-api.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable fm-query-api
systemctl start fm-query-api

# 日常
systemctl status fm-query-api
systemctl restart fm-query-api        # 修改 .env 后重启生效
journalctl -u fm-query-api -f
journalctl -u fm-query-api -n 100     # 最后 100 条
```

**日志位置**：`/opt/fm/logs/query-api.log`（stdout + stderr append 到同一文件）

---

## `nginx-api.conf` — nginx 反代片段

**不是独立文件**，是一段 `location /api/` 配置，需要**追加**到现有监听 8080 的 server block 内。

**安装步骤**：
1. 找到现有 nginx 配置文件（通常是 `/etc/nginx/conf.d/reports.conf` 或 `/etc/nginx/nginx.conf`）
2. 定位到 `server { listen 8080; ... }` 的闭合 `}` 前
3. 粘贴本文件的整段 `location /api/ { ... }`
4. `nginx -t && systemctl reload nginx`

**配置要点**：

| 指令 | 值 | 说明 |
|------|-----|------|
| `proxy_pass` | `http://127.0.0.1:5003` | 后端 uvicorn |
| `proxy_pass_request_headers on` | — | 让 `Authorization: Bearer` 透传给 FastAPI |
| `proxy_connect_timeout` | `10s` | 建立连接超时 |
| `proxy_send_timeout` | `120s` | 发请求体超时 |
| `proxy_read_timeout` | `120s` | 读响应超时（比 FastAPI 内部 60s 长，留点缓冲） |
| `proxy_buffering off` | — | 大结果集直接流式回客户端，不占 nginx 内存 |

**为什么这样做**：
- 现有 nginx 已在 :8080 跑 `/reports/` 静态看板（绝对不动）
- 本配置只**追加**一个 `location /api/`，完全不影响 `/reports/`
- `/api/` 前缀避免和未来可能的 `/auth/`、`/static/` 等路径冲突
- 后端绑 127.0.0.1 意味着外界只能通过 nginx 8080 访问，攻击面最小化

---

## 验证清单（部署后逐项跑）

```bash
# 1. ETL 手动跑一次（不等 cron）
/opt/fm/etl/daily_run.sh
tail /opt/fm/logs/etl-$(date +%Y-%m).log

# 2. 查 DuckDB 是否有数据
duckdb /opt/fm/data/fm.duckdb "SELECT COUNT(*) FROM atomic_sales"

# 3. Query API 健康检查
systemctl status fm-query-api
ss -tlnp | grep 5003           # 只能看到 127.0.0.1:5003，不能是 0.0.0.0:5003
curl http://127.0.0.1:5003/api/health

# 4. nginx 反代
curl http://47.115.213.115:8080/api/health
curl http://47.115.213.115:8080/api/tables -H "Authorization: Bearer <TOKEN>"

# 5. SQL 守卫（应返回 400）
curl -X POST http://47.115.213.115:8080/api/query \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"sql":"DROP TABLE atomic_sales"}'
```

全部通过就表示部署成功。逐项失败的排查见 [../DEPLOY.md](../DEPLOY.md) 最后的 "故障排查" 章节。
