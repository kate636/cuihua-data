# 翠花数据 · 云端部署与运维手册

> 目标：把 `fm_etl_v3` ETL 系统部署在阿里云 ECS `47.115.213.115`（广州），代码托管 GitHub 私有仓库 `cuihua-data`，云端每天 02:00 自动 `git pull` 后跑增量 ETL，数据落 `/opt/fm/data/fm.duckdb`。对外通过 FastAPI 只读查询层 + nginx 反代供 5 人团队远程查询。

## 硬性边界

| 红线 | 实现 |
|---|---|
| **数据库绝不上 GitHub** | `.gitignore` 排除 `data/` / `*.duckdb` / `*.duckdb.wal` |
| **凭证绝不上 GitHub** | `.gitignore` 排除 `.env`；只留 `.env.example` 模板 |
| **现有看板一律不动** | `/opt/fm/reports/`、`/opt/fm/duitou/`、Flask:5002、Mac 本地 09:20 cron、`fm_tables/*.py` 保持现状；nginx 只**追加** `location /api/` 不改现有规则 |

新增东西只落在三个独立目录（与现有看板互不干扰）：

```
/opt/fm/
├── etl/cuihua-data/    # 新增：ETL 代码（git clone）
├── data/               # 新增：fm.duckdb 唯一存放地
├── logs/               # 新增：ETL + API 日志
├── reports/            # 现有：nginx 静态看板（不动）
└── duitou/             # 现有：Flask:5002（不动）
```

---

## 架构

```
本地 Mac Cursor  ──git push──▶  GitHub private/cuihua-data
                                    │
                         每日 02:00 │ git pull --ff-only
                                    ▼
┌─────────────── 阿里云 47.115.213.115 ─────────────────┐
│                                                       │
│  cron 02:00 → daily_run.sh → python -m fm_etl_v3     │
│                                    │                  │
│                                    ▼ 独占写           │
│                    ┌──── /opt/fm/data/fm.duckdb ────┐ │
│                    │                                │ │
│                    │ read_only=True                 │ │
│                    ▼                                │ │
│  FastAPI :5003 (systemd fm-query-api.service)      │ │
│              ▲                                      │ │
│              │ location /api/ 反代                  │ │
│  nginx :8080 (现有) + /reports/ (现有) + /api/ (新) │ │
└──────────────┬──────────────────────────────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
  分析师 DBeaver    浏览器/Cursor
  (SSH Tunnel)      (Bearer Token)
```

---

## 首次部署步骤

### Phase 1 本地 → GitHub（约 10 分钟）

前置：本地 git 已 init + 首次 commit 已完成（`57898f6 initial: 翠花数据 ETL pipeline`）。

1. 在 GitHub 建**私有**仓库 `cuihua-data`（Visibility: Private；不要勾选 README/.gitignore/license）
2. 推送：
   ```bash
   cd /path/to/翠花数据
   git remote add origin git@github.com:<你的GitHub用户名>/cuihua-data.git
   git branch -M main
   git push -u origin main
   ```

### Phase 2 云端 ETL（约 30 分钟）

```bash
ssh root@47.115.213.115
```

#### 2.1 装 git + 建目录

```bash
yum install -y git
mkdir -p /opt/fm/{etl,data,logs}
```

#### 2.2 生成 Deploy Key 并加到 GitHub

```bash
ssh-keygen -t ed25519 -C "fm-prod-deploy" -f ~/.ssh/github_deploy -N ""
cat ~/.ssh/github_deploy.pub
```

把上面输出的 public key 贴到 GitHub → Settings → **Deploy keys** → Add deploy key：
- Title: `fm-prod ECS`
- Key: 粘贴
- **Allow write access: 不勾**（只读）

配 SSH config：
```bash
cat >> ~/.ssh/config <<'EOF'
Host github.com
    IdentityFile ~/.ssh/github_deploy
    IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config
```

验证：`ssh -T git@github.com` 应返回 `Hi <username>/cuihua-data! You've successfully authenticated...`

#### 2.3 clone + venv + 依赖

```bash
cd /opt/fm/etl
git clone git@github.com:<你的GitHub用户名>/cuihua-data.git
cd cuihua-data

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r fm_etl_v3/requirements.txt
```

#### 2.4 写 `.env`（不入库）

> **重要**：QDM Secret Key 在过往 skill 文档里出现过明文，部署前**先在 bdapp 后台重新生成**，原 Key 视为已泄漏。

```bash
cat > /opt/fm/etl/cuihua-data/.env <<'EOF'
QDM_ACCESS_KEY=<在 bdapp 后台新生成的 key>
QDM_SECRET_KEY=<在 bdapp 后台新生成的 secret>
QDM_API_ID=i_fjl10g687-790
QDM_HOST=https://bdapp.qdama.cn
QDM_VERSION=1.0

FM_DUCKDB_PATH=/opt/fm/data/fm.duckdb

# 生成 Token：python -c "import secrets; print(secrets.token_urlsafe(48))"
FM_TOKENS=alice:<64位随机串>,bob:<64位随机串>
FM_QUERY_TIMEOUT_SEC=60
EOF
chmod 600 /opt/fm/etl/cuihua-data/.env
```

#### 2.5 首次手动跑（补历史 + 验证）

```bash
cd /opt/fm/etl/cuihua-data
source .venv/bin/activate
python -m fm_etl_v3.executor 2026-04-01 2026-04-19

# 验证
duckdb /opt/fm/data/fm.duckdb "SHOW TABLES"
duckdb /opt/fm/data/fm.duckdb "SELECT COUNT(*) FROM atomic_sales"
```

#### 2.6 安装 daily_run.sh + crontab

```bash
cp /opt/fm/etl/cuihua-data/deploy/daily_run.sh /opt/fm/etl/daily_run.sh
chmod +x /opt/fm/etl/daily_run.sh

# 追加 cron（不动现有行）
( crontab -l 2>/dev/null; echo "0 2 * * * /opt/fm/etl/daily_run.sh >> /opt/fm/logs/cron.log 2>&1" ) | crontab -
crontab -l | grep daily_run
```

> **cron 验证技巧**（见 `mac-auto-task` skill 2.3 节）：临时插一条 5 分钟后的测试任务，确认生效后再删除。

### Phase 3 云端 Query API（约 20 分钟）

#### 3.1 装 systemd 单元

```bash
cp /opt/fm/etl/cuihua-data/deploy/fm-query-api.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable fm-query-api
systemctl start fm-query-api
systemctl status fm-query-api         # 确认 active (running)
ss -tlnp | grep 5003                   # 确认只监听 127.0.0.1:5003
```

#### 3.2 nginx 追加反代

找到现有 nginx 监听 8080 的 server block（常见位置 `/etc/nginx/conf.d/reports.conf` 或 `/etc/nginx/nginx.conf`），在 `server { listen 8080; ... }` 块内追加一整段：

```nginx
# 从 /opt/fm/etl/cuihua-data/deploy/nginx-api.conf 复制
location /api/ {
    proxy_pass http://127.0.0.1:5003;
    proxy_http_version 1.1;
    proxy_set_header Host              $host;
    proxy_set_header X-Real-IP         $remote_addr;
    proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_pass_request_headers on;
    proxy_connect_timeout 10s;
    proxy_send_timeout    120s;
    proxy_read_timeout    120s;
    proxy_buffering off;
}
```

```bash
nginx -t
systemctl reload nginx
```

#### 3.3 冒烟测试

把 `<TOKEN>` 替换成 `.env` 里 `FM_TOKENS` 的某个值：

```bash
# 健康检查（不需要鉴权）
curl http://47.115.213.115:8080/api/health

# 列表
curl http://47.115.213.115:8080/api/tables \
  -H "Authorization: Bearer <TOKEN>"

# 表结构
curl http://47.115.213.115:8080/api/schema/atomic_sales \
  -H "Authorization: Bearer <TOKEN>"

# SELECT
curl -X POST http://47.115.213.115:8080/api/query \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) AS n FROM atomic_sales"}'

# 守卫验证（应返回 400）
curl -X POST http://47.115.213.115:8080/api/query \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"sql":"DROP TABLE atomic_sales"}'
```

### Phase 4 多人接入

#### 4.1 Cursor Remote-SSH（开发者）

本地 `~/.ssh/config` 追加：
```
Host fm-prod
    HostName 47.115.213.115
    User root
    IdentityFile ~/.ssh/id_rsa
```

Cursor → Remote-SSH → Connect to Host → `fm-prod` → 打开 `/opt/fm/etl/cuihua-data`。

#### 4.2 分析师 DBeaver + SSH 隧道（直连 DuckDB 只读）

- 新建连接：DuckDB
- Path: `/opt/fm/data/fm.duckdb`
- URL: 追加 `?access_mode=read_only`（**必须**，避免抢写锁）
- **SSH 隧道**：
  - Host: `47.115.213.115`
  - Port: 22
  - User: `analyst`（预先建 Linux 账号，不给 sudo）
  - Auth: Public Key

#### 4.3 HTTP API（轻量查询）

分析师也可以直接调 `http://47.115.213.115:8080/api/query`（Bearer Token）——不需要装任何东西，浏览器/Postman/curl 都行。

---

## 日常运维

### 改代码 → 上线

1. 本地 Cursor 改代码
2. `git add . && git commit -m "..."`
3. `git push`
4. **第二天 02:00 自动生效**（daily_run.sh 会先 `git pull --ff-only` 再跑 ETL）

### 紧急热更新（改了 Query API，要立即生效）

```bash
ssh root@47.115.213.115
cd /opt/fm/etl/cuihua-data
git pull --ff-only origin main
systemctl restart fm-query-api
```

### 补历史数据

```bash
ssh root@47.115.213.115
cd /opt/fm/etl/cuihua-data
source .venv/bin/activate
python -m fm_etl_v3.executor 2026-04-01 2026-04-19
```

### 查看日志

```bash
# ETL 日志
tail -f /opt/fm/logs/etl-$(date +%Y-%m).log
tail -f /opt/fm/logs/cron.log

# Query API 日志
tail -f /opt/fm/logs/query-api.log
journalctl -u fm-query-api -f
```

### 撤销昨天 ETL 的写入（极少用）

DuckDB 本身不支持细粒度回滚，只能：
1. 停 API：`systemctl stop fm-query-api`
2. 从 OSS 备份恢复 `fm.duckdb`（Phase 7 备份机制上线后）
3. 重跑 ETL
4. 启 API：`systemctl start fm-query-api`

---

## 故障排查

### cron 没跑

1. `grep CRON /var/log/cron | tail`（CentOS）或 `/var/log/syslog | grep CRON`（Ubuntu）
2. `tail /opt/fm/logs/cron.log`
3. 手动跑验证：`/opt/fm/etl/daily_run.sh`

### git pull 冲突

daily_run.sh 用 `--ff-only`，冲突不会合并，只会在 cron.log 里留下错误行并退出（ETL 不跑，数据不污染）。

修复：
```bash
ssh root@47.115.213.115
cd /opt/fm/etl/cuihua-data
git fetch origin
git reset --hard origin/main       # 放弃云端本地改动（云端不应该有改动）
```

### Query API 502 Bad Gateway

```bash
systemctl status fm-query-api
journalctl -u fm-query-api -n 50
```

常见原因：
- `.env` 里 `FM_DUCKDB_PATH` 指向不存在的文件
- `FM_TOKENS` 格式错误
- 依赖缺失：`source .venv/bin/activate && pip install -r fm_etl_v3/requirements.txt`

### DuckDB `IO Error: Cannot open file` / 锁冲突

ETL 正在跑（02:00 附近）时 API 读不到是正常，因为 DuckDB 对 WAL 有短暂锁。
- 用户端查询应在 03:00 之后
- 或者把 ETL 时间改到更早

### SQL 被守卫拒绝

查 `sql_guard.py`：只允许 SELECT / SHOW / DESCRIBE / EXPLAIN / WITH 开头；禁止多语句；禁止 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/ATTACH/COPY/PRAGMA/SET/LOAD/INSTALL 等。

---

## Token 分发表模板

| 用户 | Token（64 位随机串） | 发放日期 | 备注 |
|---|---|---|---|
| alice | `...` | 2026-04-20 | 分析师 |
| bob | `...` | 2026-04-20 | 运营 |
| ... | ... | ... | ... |

生成命令：
```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Token 作废/轮换：编辑 `.env` 里 `FM_TOKENS` 后 `systemctl restart fm-query-api`。

---

## 后续（本期不做）

| Phase | 事项 | 备注 |
|---|---|---|
| 6 | 搭 DuckDB MCP Server | Claude/Cursor 原生对话直连 |
| 7 | `fm.duckdb` 每日快照备份到 OSS | ¥5/月，10 分钟可搞定 |
| 8 | 改 `fm_tables/*.py` 从 API 改读本地 DuckDB | **等 DuckDB 数据稳定验证后**，看板提速 10-100 倍 |
| 9 | GitHub Actions 自动 SSH 部署 | push → 立即生效（不等 02:00） |
| 10 | ETL 失败飞书/企业微信告警 | webhook |
