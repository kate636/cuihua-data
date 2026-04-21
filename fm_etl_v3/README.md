# FM ETL v3 · 翠花门店商品全链路数据管道

---

## 目录

1. [项目背景与整体方案](#1-项目背景与整体方案)
2. [系统架构与硬性红线](#2-系统架构与硬性红线)
3. [完整数据流与表清单](#3-完整数据流与表清单)
4. [目录结构详解](#4-目录结构详解)
5. [Pipeline 11 个步骤](#5-pipeline-11-个步骤)
6. [本地开发环境搭建](#6-本地开发环境搭建)
7. [云端部署与运维](#7-云端部署与运维)
8. [查询数据的三种方式](#8-查询数据的三种方式)
9. [常见故障排查](#9-常见故障排查)
10. [相关文档索引](#10-相关文档索引)

---

## 1. 项目背景与整体方案

### 1.1 业务目标

对广州 Food Mart 门店做**商品日粒度 × 全链路指标**的数据生产，覆盖销售 / 进货 / 库存 / 损耗 / 加工 / 促销 / 补贴 / 供应链 / 价格九大业务域，支撑 FM 平台经营看板、毛利分析、损耗归因、AI SKU 诊断等场景。

最终产物是 4 张底表：

| 底表 | 内容 |
|---|---|
| `t_fm_sku_dim` | SKU 级完整宽表（原子 + 计算指标） |
| `t_fm_cust` | 按 6 个层级聚合的客数 |
| `t_fm_levels_sum` | 7 层分类展开的数量/金额汇总 |
| `t_fm_levels_result` | **平台对接表**，中文列名 + 比率型 KPI |

### 1.2 核心设计原则（沿用 v2 的"原子层 / 计算层"分层）

底层架构详见 [docs/底表架构设计_指标字典.md](docs/底表架构设计_指标字典.md)。最关键的一条：

- **Layer -2 原子层**：只放**不可再分解的独立观测量**（数量、单价、标识、POS 交易金额、SAP 让利金额）
- **Layer -1 计算层**：放**所有可由公式推导的指标**（金额、库存余额、差异、毛利）
- **效果**：底层改一个值，上层自动算对；毛利 / 损耗 / 全链路指标的口径不会在多处拷贝漂移

v3 把这套分层**完整落到 DuckDB 表里**，`atomic_*` 对应 Layer -2，`t_calc_*` 对应 Layer -1，`t_fm_*` 是最终对外的 Layer 0。

### 1.3 从 v2 到 v3 的迭代原因

| 方面 | v2（旧） | v3（现在） |
|------|---------|------------|
| 数据源 | StarRocks / Hive 直连 | **只调 QDM BI API**（`bdapp.qdama.cn`） |
| 计算引擎 | StarRocks 多层 SQL + Python 辅助 | **单进程 DuckDB 单文件** |
| 最终落地 | 回写到 StarRocks `ads_business_analysis.*` | **留在 DuckDB**，不回写任何外部库 |
| 对外查询 | StarRocks 连接池 | **HTTP 只读 API + DBeaver SSH 隧道** |
| 补数/回刷 | 需要 StarRocks 写权限 | `python -m fm_etl_v3.executor 2026-04-01 2026-04-19`，**幂等分区覆盖** |
| 部署 | 多机多服务 | **1 台阿里云 ECS + 1 个 .duckdb 文件 + 1 个 FastAPI** |

触发迭代的直接原因：
- 数据库侧凭证收紧，只允许 HTTP API 访问（StarRocks 直连逐步关闭）
- 业务范围小（5 人团队 / 广州 Food Mart 几十家门店 / 单日百万行量级），分布式 OLAP 杀鸡用牛刀
- DuckDB 对 OLAP 负载足够快（JOIN / 窗口 / 聚合毫秒~秒级），且单文件意味着**备份 = `cp`，迁移 = `scp`**

### 1.4 整体方案一句话总结

> **QDM BI API 是唯一数据来源 → 在云服务器本地 DuckDB 里做 11 个 Step 的计算 → 结果留在同一个 `fm.duckdb` 文件里 → 通过 FastAPI 只读服务 + DBeaver SSH 隧道对 5 人团队提供查询。**

### 1.5 范围边界（本期不做）

| 不做项 | 原因 |
|-------|------|
| `fm_tables/*.py` 改为读本地 DuckDB 回写看板 | 等 DuckDB 数据稳定验证后再做（现有看板继续走各自 API） |
| MotherDuck / 其他云数据库 | 数据不出服务器，降低合规风险 |
| 分布式调度（Airflow 等） | 一个 cron 已经够 |
| 流式增量 | 日粒度已满足业务需求 |
| DuckDB MCP Server、OSS 快照备份、飞书告警 | 在后续 Phase 里（见 DEPLOY.md 末尾） |

---

## 2. 系统架构与硬性红线

```
┌─── 本地 Mac (开发) ────────────┐
│ Cursor 改代码                  │
│   │                            │
│   │ git push                   │
│   ▼                            │
└─── GitHub 私有仓库 cuihua-data ┘
            │
            │  每日 02:00 git pull --ff-only
            ▼
┌─── 阿里云 ECS 47.115.213.115 (广州) ─────────────────────┐
│                                                         │
│  /opt/fm/etl/cuihua-data/    代码（git clone）          │
│  /opt/fm/data/fm.duckdb      唯一数据文件（不上 GitHub）│
│  /opt/fm/logs/               ETL + API 日志             │
│                                                         │
│   cron 02:00                                            │
│     ↓                                                   │
│   daily_run.sh                                          │
│     ↓                                                   │
│   python -m fm_etl_v3.executor 昨天 昨天                │
│     │                                                   │
│     │ 独占写                                            │
│     ▼                                                   │
│   /opt/fm/data/fm.duckdb  ←─── 读 (read_only=True) ──┐  │
│                                                      │  │
│   fm-query-api.service (systemd)                     │  │
│     └── uvicorn 127.0.0.1:5003  (FastAPI)            │  │
│                    ▲                                 │  │
│                    │ 反代 location /api/             │  │
│   nginx :8080 (现有，仅追加 location)                │  │
│                    ▲                                 │  │
└────────────────────┼─────────────────────────────────┘  │
                     │                                    │
        ┌────────────┼────────────┐                       │
        │            │            │                       │
  浏览器/Postman  DBeaver+SSH   Cursor Remote-SSH ────────┘
  (Bearer Token)  (只读隧道)    (开发/改代码)
```

### 两条硬性红线（上线时人工核查）

| 红线 | 强制手段 |
|---|---|
| **数据库绝不上 GitHub** | `.gitignore` 排除 `data/` / `*.duckdb` / `*.duckdb.wal`；首次 commit 前 `git status` 人工核查；云端 `.env` 也不入库（`.env.example` 是模板） |
| **现有看板一律不动** | `/opt/fm/reports/`、`/opt/fm/duitou/`、Flask:5002、Mac 本地 09:20 cron、`fm_tables/*.py` 保持现状；nginx 只**追加** `location /api/`，不改现有 `/reports/` 规则 |

---

## 3. 完整数据流与表清单

三层命名约定：`atomic_*`（原子域） → `t_calc_*`（中间计算） → `t_fm_*`（最终底表） + `dim_*`（维度快照）。

### 3.1 原子域表（9 张，来自 QDM BI API）

所有原子表都是 **门店 × 业务日期 × article_id** 粒度。

| DuckDB 表 | 来源 Hive 表 | 内容 |
|---|---|---|
| `atomic_sales` | `hive.dsl.dsl_transaction_non_daily_store_order_details_di` | 销售流水（含原价额、补贴额、渠道） |
| `atomic_inventory` | `hive.dsl.dsl_transaction_non_daily_store_article_purchase_di` | 进货验收流水 |
| `atomic_scm` | `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di` | 供应链出入库（SAP 财务口径） |
| `atomic_loss` | `hive.dal.dal_transaction_store_article_lost_di` | 已知损耗 |
| `atomic_compose` | `hive.dsl.dsl_transaction_sotre_article_compose_info_di` | 加工转换（一个 SKU 拆/合为另一个 SKU） |
| `atomic_allowance` | `hive.dal.dal_activity_article_order_sale_info_di` | 订单级补贴额 |
| `atomic_promo` | `hive.dsl.dsl_promotion_order_item_article_sale_info_di` | 促销活动明细 |
| `atomic_cost_price` | `hive.ods_sc_db.t_shop_inventory_sku_pool` | 成本价底表 |
| `atomic_price` | `hive.dim.dim_store_article_price_info_da` | 商品售价 / 原价 / DC 原价 |

### 3.2 维度表（7 张，每次跑一次全量替换）

| DuckDB 表 | 来源 | 用途 |
|---|---|---|
| `dim_store_list` | `hive.dim.dim_chdj_store_list_di` | **翠花门店白名单**（merge 阶段 INNER JOIN 过滤用） |
| `dim_day_clear` | `hive.dim.dim_day_clear_article_list_di` | 商品日清 / 非日清标签（后备） |
| `dim_goods` | `hive.dim.dim_goods_information_have_pt` | 商品主数据（品类层级、单重、规格） |
| `dim_store_profile` | `hive.dim.dim_store_profile` | 门店属性（区域、城市） |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` | 翠花门店编号、标签 |
| `dim_calendar` | `hive.dim.dim_calendar` | 日历（周 / 月 / 年） |
| `dim_saleable` | `hive.ods_sc_db.t_purchase_order_item_tmp` | 门店可售商品列表 |

### 3.3 中间计算表（5 张，DuckDB 内部产物）

| DuckDB 表 | 构建器 | 粒度 |
|---|---|---|
| `t_atomic_wide` | `AtomicMerger` | 门店 × 业务日期 × article_id × day_clear |
| `t_calc_inventory` | `InventoryCalculator` | 同上，修正库存方程后的数量字段 |
| `t_calc_avg_price` | `AvgPriceCalculator` | 同上，加权平均进货价 |
| `t_calc_amounts` | `AmountsCalculator` | 同上，数量 × 单价 → 金额字段 |
| `t_calc_profit` | `ProfitCalculator` | 同上，9 个毛利与销售成本指标 |

### 3.4 FM 底表（4 张，对外产出）

| DuckDB 表 | 构建器 | 粒度 | 用途 |
|---|---|---|---|
| `t_fm_sku_dim` | `SkuDimBuilder` | 门店 × 日期 × article_id × day_clear | SKU 级完整宽表（类别重映射、近 7 天日均销量、售罄标识） |
| `t_fm_cust` | `CustBuilder` | 门店 × 日期 × day_clear × level | 按 6 个层级聚合的客数（唯一仍调 API 的 builder） |
| `t_fm_levels_sum` | `LevelsSumBuilder` | 门店 × 日期 × 分类等级 × day_clear | 7 层分类（门店 / 大类 / 中类 / 小类 / SPU / 黑白猪 / SKU）展开 UNION ALL |
| `t_fm_levels_result` | `LevelsResultBuilder` | 同上 | **最终对接 FM 平台看板**，输出中文列名的比率型 KPI |

详细字段字典：[docs/底表架构设计_指标字典.md](docs/底表架构设计_指标字典.md)

---

## 4. 目录结构详解

```
fm_etl_v3/
├── README.md                    ← 本文件
├── DEPLOY.md                    ← 云端部署完整手册（Phase 1~4，含故障排查）
├── executor.py                  ← 主入口，串联 Step 1~11
├── requirements.txt             ← pip 依赖（duckdb / pandas / fastapi / ...）
├── .env.example                 ← 凭证模板，复制为 .env 后填值
│
├── config/                      ← 全局配置
│   ├── README.md
│   └── settings.py              ← ApiConfig + Settings dataclass + get_settings()
│
├── utils/                       ← 通用工具（无业务逻辑）
│   ├── README.md
│   ├── logger.py                ← get_logger(name) 结构化日志
│   ├── date_utils.py            ← split_date_range 按 7 天切片
│   └── retry.py                 ← @retry_on_exception 指数退避
│
├── connectors/                  ← I/O 层
│   ├── README.md
│   ├── api_connector.py         ← QDM BI API 只读客户端（MD5 签名 + 自动翻页）
│   └── duckdb_store.py          ← DuckDB 单 connection 封装 + 3 种写入模式
│
├── atomic/                      ← Step 1 & 2：原子域提取
│   ├── README.md
│   ├── _base.py                 ← BaseExtractor 基类（子类只需写 SQL）
│   ├── dims_extractor.py        ← 7 张维度表一次性提取
│   ├── sales_extractor.py       ← atomic_sales
│   ├── inventory_extractor.py   ← atomic_inventory
│   ├── scm_extractor.py         ← atomic_scm
│   ├── loss_extractor.py        ← atomic_loss
│   ├── compose_extractor.py     ← atomic_compose
│   ├── allowance_extractor.py   ← atomic_allowance
│   ├── promo_extractor.py       ← atomic_promo
│   ├── cost_price_extractor.py  ← atomic_cost_price
│   └── price_extractor.py       ← atomic_price
│
├── calculated/                  ← Step 3~7：DuckDB 内部计算
│   ├── README.md
│   ├── merge.py                 ← AtomicMerger → t_atomic_wide
│   ├── inventory.py             ← InventoryCalculator → t_calc_inventory
│   ├── avg_price.py             ← AvgPriceCalculator → t_calc_avg_price
│   ├── amounts.py               ← AmountsCalculator → t_calc_amounts
│   └── profit.py                ← ProfitCalculator → t_calc_profit
│
├── fm_tables/                   ← Step 8~11：FM 底表构建
│   ├── README.md
│   ├── sku_dim.py               ← SkuDimBuilder → t_fm_sku_dim
│   ├── cust.py                  ← CustBuilder → t_fm_cust（唯一仍调 API）
│   ├── levels_sum.py            ← LevelsSumBuilder → t_fm_levels_sum
│   └── levels_result.py         ← LevelsResultBuilder → t_fm_levels_result
│
├── query_api/                   ← 只读 HTTP 查询服务（云端 systemd 托管）
│   ├── README.md
│   ├── app.py                   ← FastAPI 路由：/query /tables /schema /health
│   ├── auth.py                  ← Bearer Token 鉴权（FM_TOKENS 环境变量）
│   ├── sql_guard.py             ← SQL 白名单守卫（只允许 SELECT/SHOW/...）
│   └── models.py                ← Pydantic 请求/响应模型
│
├── deploy/                      ← 云端部署产物（cp 到服务器对应位置）
│   ├── README.md
│   ├── daily_run.sh             ← 每日 cron 调用的 shell 脚本
│   ├── fm-query-api.service     ← systemd 单元文件
│   └── nginx-api.conf           ← nginx location /api/ 反代片段
│
├── scripts/                     ← 独立工具脚本（非 pipeline 组件）
│   └── probe_tables.py          ← 探测 Hive 源表是否可达 / schema 是否变化
│
├── output/                      ← （本地运行输出占位，实际产物在 data/*.duckdb）
│
└── docs/
    ├── TEAM_ACCESS.md           ← 5 人团队接入指南（Cursor/DBeaver/API）
    └── 底表架构设计_指标字典.md   ← 710 行完整字段字典与口径说明
```

> **注意**：`data/` 目录在仓库根（`翠花数据/data/`），被 `.gitignore` 排除。本地第一次跑时会自动创建 `data/fm_etl_v3.duckdb`。

---

## 5. Pipeline 11 个步骤

`executor.py` 按顺序跑完下面 11 步，每步都可独立重跑（幂等，按日期分区覆盖写入）。

| 步骤 | 模块 | 输出 | 数据来源 | 典型耗时（单日） |
|---|---|---|---|---|
| 1 | `DimsExtractor` | 7 张 `dim_*` 全量替换 | QDM API | 10-30s |
| 2 | 9 个 `*Extractor`（原子域） | 9 张 `atomic_*` 分区追加 | QDM API | 3-8 分钟 |
| 3 | `AtomicMerger` | `t_atomic_wide` | DuckDB JOIN | 5-10s |
| 4 | `InventoryCalculator` | `t_calc_inventory` | DuckDB 计算 | <5s |
| 5 | `AvgPriceCalculator` | `t_calc_avg_price` | DuckDB 计算 | <5s |
| 6 | `AmountsCalculator` | `t_calc_amounts` | DuckDB 计算 | <5s |
| 7 | `ProfitCalculator` | `t_calc_profit` | DuckDB 计算 | <5s |
| 8 | `SkuDimBuilder` | `t_fm_sku_dim` | DuckDB JOIN + 窗口 | 10-30s |
| 9 | `CustBuilder` | `t_fm_cust` | QDM API + DuckDB | 1-2 分钟 |
| 10 | `LevelsSumBuilder` | `t_fm_levels_sum` | DuckDB UNION ALL | 10-20s |
| 11 | `LevelsResultBuilder` | `t_fm_levels_result` | DuckDB GROUP BY | 5-15s |

**总耗时参考**：单日增量跑完约 **5~12 分钟**（取决于 API 响应速度）。

### 步骤间依赖

```
Step 1 (dims)     ──────────┐
Step 2 (atomic)   ──┬───▶ Step 3 (merge) ───▶ Step 4 (inventory)
                    │                                 │
                    │                                 ▼
                    └─────────────────────────▶ Step 5 (avg_price)
                                                      │
                                                      ▼
                                              Step 6 (amounts)
                                                      │
                                                      ▼
                                              Step 7 (profit)
                                                      │
                                                      ▼
                                              Step 8 (sku_dim) ──┐
                                                                 │
                                              Step 9 (cust)  ────┤
                                                                 │
                                                      ┌──────────┤
                                                      ▼          │
                                              Step 10 (levels_sum)
                                                      │
                                                      ▼
                                              Step 11 (levels_result)
```

---

## 6. 本地开发环境搭建

### 6.1 前置要求

- Python **3.10+**（macOS 推荐用 pyenv 或官网安装包，别用系统自带的）
- git
- 能访问 `bdapp.qdama.cn`（QDM BI API）

### 6.2 首次配置

```bash
# 1. 克隆仓库（假设已有 GitHub 访问权限）
git clone git@github.com:<你的组织>/cuihua-data.git
cd cuihua-data

# 2. 建 venv（强烈推荐，别污染系统 Python）
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

# 3. 装依赖
pip install -r fm_etl_v3/requirements.txt

# 4. 配置 .env（复制模板后改值）
cp fm_etl_v3/.env.example .env
# 用编辑器打开 .env，填入 QDM_ACCESS_KEY / QDM_SECRET_KEY（找组长要）
# FM_DUCKDB_PATH 本地可留空或注释掉 → 默认写到 data/fm_etl_v3.duckdb
```

### 6.3 跑一天数据（冒烟测试）

```bash
# 跑昨天一天（最快的验证方式）
python -m fm_etl_v3.executor 2026-04-19 2026-04-19

# 跑一周
python -m fm_etl_v3.executor 2026-04-13 2026-04-19

# 看结果
python -c "
import duckdb
con = duckdb.connect('data/fm_etl_v3.duckdb', read_only=True)
for (name,) in con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1\").fetchall():
    cnt = con.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]
    print(f'{name:30s} {cnt:>12,}')
"
```

### 6.4 日志与调试

- 所有模块用 `utils.get_logger(name)` 打 info 日志，输出到 stdout
- 子步骤失败时会抛异常并退出，**不会继续跑后面的步骤**
- 临时 debug 某一步：直接 `python -c "from fm_etl_v3.xxx import Builder; Builder(duck).build(...)"`

### 6.5 IDE / 编辑器建议

- **Cursor** 首选（项目根已有 `.cursor/` 配置）
- 格式化：不强制，但保持 4 空格缩进 + 类型标注
- 测试：目前无单测框架，改动后跑一遍 executor 一天数据就是最好的集成测试

---

## 7. 云端部署与运维

完整手册见 [DEPLOY.md](DEPLOY.md)（385 行，含 Phase 1~4 首次部署、日常运维、故障排查）。

### 7.1 服务器信息

- **IP**：`47.115.213.115`（阿里云广州）
- **对外端口**：`8080`（nginx，现有）
- **SSH**：`ssh root@47.115.213.115`（也可用 `ssh fm-prod` 别名）
- **目录约定**：
  - `/opt/fm/etl/cuihua-data/` ← ETL 代码（git clone 而来）
  - `/opt/fm/data/fm.duckdb` ← 唯一数据文件
  - `/opt/fm/logs/` ← ETL + API 日志

### 7.2 三个自动运行的组件

| 组件 | 触发方式 | 文件 |
|---|---|---|
| ETL 每日跑 | cron `0 2 * * *` | `/opt/fm/etl/daily_run.sh` → `python -m fm_etl_v3.executor 昨天 昨天` |
| Query API | systemd `fm-query-api.service` | uvicorn 127.0.0.1:5003 |
| nginx 反代 | nginx 现有服务 | `location /api/` → 127.0.0.1:5003 |

### 7.3 改代码 → 生效

**常规路径**（第二天 02:00 自动生效）：

```bash
# 本地
git add . && git commit -m "..." && git push

# 服务器会在 02:00 自动 git pull --ff-only 再跑 ETL
```

**紧急热更新**（改了 Query API 要立即生效）：

```bash
ssh root@47.115.213.115
cd /opt/fm/etl/cuihua-data
git pull --ff-only origin main
systemctl restart fm-query-api
```

### 7.4 补历史数据

```bash
ssh root@47.115.213.115
cd /opt/fm/etl/cuihua-data
source .venv/bin/activate
python -m fm_etl_v3.executor 2026-04-01 2026-04-19
```

pipeline 是 **按日期分区幂等**的，同一区间重跑会先删旧分区再写，不会产生重复行。

### 7.5 看日志

```bash
# ETL 月度聚合日志（推荐）
tail -f /opt/fm/logs/etl-$(date +%Y-%m).log

# cron 本身的日志（看 git pull 是否成功）
tail -f /opt/fm/logs/cron.log

# Query API 日志
tail -f /opt/fm/logs/query-api.log
journalctl -u fm-query-api -f
```

---

## 8. 查询数据的三种方式

面向 5 人团队，按角色选择：

### 8.1 HTTP API（最轻量，curl/Postman/浏览器都能用）

```bash
# 健康检查（不需要 Token）
curl http://47.115.213.115:8080/api/health

# 列出所有表 + 行数
curl http://47.115.213.115:8080/api/tables \
  -H "Authorization: Bearer <你的 Token>"

# 看表结构
curl http://47.115.213.115:8080/api/schema/t_fm_levels_result \
  -H "Authorization: Bearer <你的 Token>"

# 执行 SELECT
curl -X POST http://47.115.213.115:8080/api/query \
  -H "Authorization: Bearer <你的 Token>" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) FROM t_fm_levels_result WHERE business_date = '\''2026-04-19'\''"}'
```

**限制**：单查询 60s 超时 + 默认 10000 行上限（可请求参数指定 `limit` 最大 50000）。

### 8.2 DBeaver + SSH 隧道（分析师，大数据量）

见 [docs/TEAM_ACCESS.md](docs/TEAM_ACCESS.md) 角色 2 小节。配好后 DBeaver 直接像查本地数据库一样查 `fm.duckdb`。

**关键参数**：连接字符串必须追加 `?access_mode=read_only`，否则会和 ETL 抢写锁。

### 8.3 Cursor Remote-SSH（开发者）

本地 `~/.ssh/config` 加 `Host fm-prod` 别名后，Cursor → Remote-SSH → Connect to Host → `fm-prod` → 打开 `/opt/fm/etl/cuihua-data`，改代码像改本地一样。

---

## 9. 常见故障排查

### 9.1 ETL 失败

**症状**：`/opt/fm/logs/etl-YYYY-MM.log` 有 ERROR，或者 `/api/tables` 昨天的行数没涨。

```bash
# 1. 看当日日志
tail -100 /opt/fm/logs/etl-$(date +%Y-%m).log

# 2. 最常见的问题：git pull 冲突（用了 --ff-only，不会合并）
tail /opt/fm/logs/cron.log
# → 看到 "git pull failed" → SSH 上去手动解决：
#   cd /opt/fm/etl/cuihua-data
#   git fetch origin && git reset --hard origin/main

# 3. QDM API 429 或超时
#   → 重跑失败的那天（idempotent）
#   → 如果系统性失败，查 QDM 运维状态
```

### 9.2 Query API 502 Bad Gateway

```bash
systemctl status fm-query-api
journalctl -u fm-query-api -n 100

# 常见原因：
# - .env 里 FM_DUCKDB_PATH 指向不存在的文件
# - FM_TOKENS 格式错
# - 依赖缺失：source .venv/bin/activate && pip install -r fm_etl_v3/requirements.txt
```

### 9.3 SQL 被守卫拒绝（400）

Query API 只允许 `SELECT / SHOW / DESCRIBE / EXPLAIN / WITH` 开头的 SQL，禁止任何 DDL/DML。详见 [query_api/sql_guard.py](query_api/sql_guard.py) 的黑白名单。

需要写入？直接改 ETL 代码，让 `fm_tables/*.py` 或者新增步骤生成你要的表，走 git push → 02:00 生效。

### 9.4 DuckDB 锁冲突 / 读不到数

ETL 跑的时候（02:00 ~ 02:15 附近）DuckDB 会对文件短暂独占锁，Query API 的 `read_only=True` 连接可能会偶发 `IO Error`。

**处理方式**：
- 用户端查询统一约定在 03:00 之后（一般不会冲突）
- 或者把 ETL 跑完的时间往前挪（改 cron）

### 9.5 数据对不上 / 字段含义不清

先查 [docs/底表架构设计_指标字典.md](docs/底表架构设计_指标字典.md)，710 行完整字段字典含口径说明（来源字段、计算公式、边界情况）。

还是不清楚 → 看对应 builder 的源码（`fm_tables/levels_result.py` 等），里面的 SQL 就是最权威的口径定义。

---

## 10. 相关文档索引

| 文档 | 内容 |
|---|---|
| [DEPLOY.md](DEPLOY.md) | 云端部署完整手册（Phase 1~4 + 日常运维） |
| [docs/TEAM_ACCESS.md](docs/TEAM_ACCESS.md) | 5 人团队接入指南（按角色：开发/分析师/管理员） |
| [docs/底表架构设计_指标字典.md](docs/底表架构设计_指标字典.md) | 710 行字段字典与口径说明 |
| [config/README.md](config/README.md) | 配置项说明 + 环境变量表 |
| [utils/README.md](utils/README.md) | 日志/日期/重试工具 |
| [connectors/README.md](connectors/README.md) | API + DuckDB 封装 |
| [atomic/README.md](atomic/README.md) | 9 个原子域提取器 + WAF 限制 |
| [calculated/README.md](calculated/README.md) | 5 个计算步骤的公式 |
| [fm_tables/README.md](fm_tables/README.md) | 4 张最终底表 |
| [query_api/README.md](query_api/README.md) | HTTP 查询服务 |
| [deploy/README.md](deploy/README.md) | 部署产物使用说明 |

---

## 附：环境变量完整清单

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `QDM_ACCESS_KEY` | ✅ | — | QDM BI API access key（找组长要） |
| `QDM_SECRET_KEY` | ✅ | — | QDM BI API secret key |
| `QDM_API_ID` | ❌ | `i_fjl10g687-790` | API 路由 ID |
| `QDM_HOST` | ❌ | `https://bdapp.qdama.cn` | API 主机 |
| `QDM_VERSION` | ❌ | `1.0` | API 协议版本 |
| `FM_DUCKDB_PATH` | ❌ | `data/fm_etl_v3.duckdb`（本地）<br>`/opt/fm/data/fm.duckdb`（云端） | DuckDB 文件路径 |
| `FM_TOKENS` | ❌（仅云端） | — | Query API 鉴权 Token 列表，格式 `user1:xxx,user2:yyy` |
| `FM_QUERY_TIMEOUT_SEC` | ❌ | `60` | 单查询超时秒数 |

**安全提醒**：`.env` 在 `.gitignore` 里，`git status` 看不到。任何时候都不要 `git add .env`。如果 `.env` 不小心被 add 了，立刻 `git reset -- .env`。
