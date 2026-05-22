# FM ETL v9 · 翠花门店商品全链路数据管道

> **v9.0（2026-04-30）**：BOM 分摊 v9 Σ总权重 + Python 共享组识别，
> SKU 成本改为加权平均（含期初库存，跨日链式传递），
> 库存 v9 跨日滚动（今日期初 = 昨日期末），
> 销售成本全面切换为 effective_unit_cost，双口径毛利强制对齐。
> cost_price 不再参与任何成本计算。
>
> 详见 [docs/测试期注意事项.md](docs/测试期注意事项.md) v9.0.0 章节。

---

## 目录

1. [项目背景与整体方案](#1-项目背景与整体方案)
2. [系统架构与硬性红线](#2-系统架构与硬性红线)
3. [完整数据流与表清单](#3-完整数据流与表清单)
4. [目录结构详解](#4-目录结构详解)
5. [Pipeline 16 个步骤](#5-pipeline-16-个步骤-v50)
6. [核心算法详解](#6-核心算法详解-v50)
7. [本地开发环境搭建](#7-本地开发环境搭建)
8. [云端部署与运维](#8-云端部署与运维)
9. [查询数据的三种方式](#9-查询数据的三种方式)
10. [常见故障排查](#10-常见故障排查)
11. [相关文档索引](#11-相关文档索引)

---

## 1. 项目背景与整体方案

### 1.1 业务目标

对广州 Food Mart 门店做**商品日粒度 × 全链路指标**的数据生产，覆盖销售 / 进货 / 库存 / 损耗 / 加工 / 促销 / 补贴 / 供应链 / 价格九大业务域，支撑 FM 平台经营看板、毛利分析、损耗归因、AI SKU 诊断等场景。

最终产物是 6 张底表：

| 底表 | 内容 |
|---|---|
| `t_fm_sku_dim` | SKU 级完整宽表（原子 + 计算指标） |
| `t_fm_cust` | 按 6 个层级聚合的客数 |
| `t_fm_levels_sum` | 7 层分类展开的数量/金额汇总 |
| `t_fm_levels_result` | **平台对接表**，中文列名 + 比率型 KPI（含双口径毛利） |
| `t_fm_bom_breakdown` | **v4 新增** · sub × parent 的 BOM 分摊溯源表（AI 友好） |
| `t_fm_stock_roll` | **v4 新增** · SKU 库存八要素滚动展开（AI 友好） |

### 1.2 核心设计原则（沿用"原子层 / 计算层"分层）

底层架构详见 [docs/底表架构设计_指标字典.md](docs/底表架构设计_指标字典.md)。最关键的一条：

- **Layer -2 原子层**：只放**不可再分解的独立观测量**（数量、单价、标识、POS 交易金额、SAP 让利金额）
- **Layer -1 计算层**：放**所有可由公式推导的指标**（金额、库存余额、差异、毛利）
- **效果**：底层改一个值，上层自动算对；毛利 / 损耗 / 全链路指标的口径不会在多处拷贝漂移

这套分层完整落到 DuckDB 表里，`atomic_*` 对应 Layer -2，`t_calc_*` 对应 Layer -1，`t_fm_*` 是最终对外的 Layer 0。

### 1.3 从 v2 到 v9 的迭代历程

| 版本 | 关键变化 |
|------|---------|
| v2 | StarRocks / Hive 直连，多机多服务 |
| v3 | 切到 QDM BI API + 单进程 DuckDB，不回写外部库 |
| v4 | 引入 `atomic_receive_sale` 作为 BOM 拆分主源，新增双口径毛利 + AI 溯源表 |
| v5 | BOM v9 Σ总权重算法 + Python 共享组识别；SKU 成本并行叠加（废弃 cost_price）；库存 LAG 跨日滚动 |
| v9 | SKU 成本改为加权平均（含期初库存，跨日链式传递）；销售成本切到 effective_unit_cost；双口径毛利强制对齐；receive_amt 全用计算值 |

### 1.4 整体方案一句话总结

> **QDM BI API 是唯一数据来源 → 在云服务器本地 DuckDB 里做 16 个 Step 的计算 → 结果留在同一个 `fm.duckdb` 文件里 → 通过 FastAPI 只读服务 + DBeaver SSH 隧道对 5 人团队提供查询。**

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

### 3.1 原子域表（12 张活跃 + 2 张骨架，来自 QDM BI API）

所有原子表都是 **门店 × 业务日期 × article_id** 粒度（BOM 关系表多 parent/sub 两维）。
全部切换到商分口径的 `strategy_fm_*` 源表。

| DuckDB 表 | 来源商分表 | 内容 | 活跃 |
|---|---|---|---|
| `atomic_sales` | `strategy_fm_sales_di` | 销售流水（含原价额、补贴额、渠道） | ✅ |
| `atomic_inventory` | `strategy_fm_purchase_di` | 进货验收 + 期初/期末库存 | ✅ |
| `atomic_scm` | `strategy_fm_scm_di` | 供应链出入库（SAP 财务口径） | ✅ |
| `atomic_scm_adjust` | `strategy_fm_scm_adjust_di` | 差异调整（当前 0 行预留） | ✅ |
| `atomic_loss` | `strategy_fm_loss_di` | 已知/未知损耗（含上游预计算金额） | ✅ |
| `atomic_compose` | `strategy_fm_compose_di` | 加工转换（一个 SKU 拆/合为另一个 SKU） | ✅ |
| `atomic_allowance` | `strategy_fm_allowance_di` | 订单级补贴额 | ✅ |
| `atomic_promo` | `strategy_fm_promo_di` | 促销活动明细 | ✅ |
| `atomic_cost_price` | `strategy_fm_inventory_pool_di` | 成本价底表（**v5 降级为观测**，不再参与 effective_unit_cost） | ✅ |
| `atomic_price` | `strategy_fm_price_da` | 商品售价 / 原价 / DC 原价 | ✅ |
| `atomic_bom_relation` | `strategy_dim_store_article_bom_relation` | parent→sub 出肉率 / 成本比例（**v5 降级为观测**，BOM 分摊不再使用） | ✅ |
| `atomic_receive_sale` | `strategy_fm_receive_sale_di` | **v4 新增 · v5 BOM 核心源** · parent × sub 当天进货分摊事实 | ✅ |
| `atomic_order_receive` | `strategy_fm_order_receive_di` | 订验关系（空骨架预留） | ❌ |
| `atomic_article_convert` | `strategy_fm_dim_article_convert` | 单位互转（空骨架预留） | ❌ |

### 3.2 维度表（7 张，每次跑一次全量替换）

| DuckDB 表 | 来源 | 用途 |
|---|---|---|
| `dim_store_list` | `ads_business_analysis.chdj_store_info` | **翠花门店白名单**（merge 阶段 INNER JOIN 过滤用） |
| `dim_day_clear` | `strategy_fm_dim_day_clear` | 商品日清 / 非日清标签（后备） |
| `dim_goods` | `strategy_fm_dim_goods` | 商品主数据（品类层级、单重、规格） |
| `dim_store_profile` | `strategy_fm_dim_store_profile` | 门店属性（区域、城市） |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` | 翠花门店编号、标签 |
| `dim_calendar` | `strategy_fm_dim_calendar` | 日历（周 / 月 / 年） |
| `dim_saleable` | `strategy_fm_dim_saleable` | 门店可售商品列表 |

### 3.3 中间计算表（7 张，DuckDB 内部产物 · v9 重构）

| DuckDB 表 | 构建器 | 粒度 | v9 变化 |
|---|---|---|---|
| `t_atomic_wide` | `AtomicMerger` | 门店 × 日期 × article_id × day_clear | 不变 |
| `t_calc_inventory` | `InventoryCalculator` | 同上 | legacy 库存方程（观测用，v9 权威源为 t_calc_stock） |
| `t_calc_avg_price` | `AvgPriceCalculator` | 同上 | 观测表（v9 profit 不再使用，切到 effective_unit_cost） |
| `t_calc_bom_alloc` | `BomAllocCalculator` | 门店 × 日期 × parent × sub | **v9 重写** · Σ总权重 + Python 共享组识别 + 8 列新增（bom_breakdown 用） |
| `t_calc_sku_cost` | `SkuCostCalculator` | 门店 × 日期 × article_id | **v9 重写** · 加权平均含期初库存（读昨天 t_calc_stock，跨日链式传递） |
| `t_calc_stock` | `StockCalculator` | 门店 × 日期 × article_id × day_clear | **v9 重写** · 跨日滚动（LAG 昨日期末 → 今日期初），供 sku_cost 明天使用 |
| `t_calc_amounts` | `AmountsCalculator` | 门店 × 日期 × article_id × day_clear | **v9 修正** · receive_amt 全用计算值 + lost_amt = know + unknow |
| `t_calc_profit` | `ProfitCalculator` | 门店 × 日期 × article_id × day_clear | **v9 修正** · sale_cost_amt 切到 effective_unit_cost，双口径强制对齐 |

### 3.4 FM 底表（6 张，对外产出）

| DuckDB 表 | 构建器 | 粒度 | 用途 |
|---|---|---|---|
| `t_fm_sku_dim` | `SkuDimBuilder` | 门店 × 日期 × article_id × day_clear | SKU 级完整宽表（含双口径毛利 + cost_source + effective_unit_cost） |
| `t_fm_cust` | `CustBuilder` | 门店 × 日期 × day_clear × level | 按 6 个层级聚合的客数（唯一仍调 API 的步骤） |
| `t_fm_levels_sum` | `LevelsSumBuilder` | 门店 × 日期 × 分类等级 × day_clear | 7 层分类 UNION ALL |
| `t_fm_levels_result` | `LevelsResultBuilder` | 同上 | **平台看板对接表**（含"门店毛利额_销售方程 / 库存方程 / 口径差异"） |
| `t_fm_bom_breakdown` | `BomBreakdownBuilder` | 门店 × 日期 × parent × sub | AI 溯源 · BOM 分摊明细 |
| `t_fm_stock_roll` | `StockRollBuilder` | 门店 × 日期 × sku × day_clear | AI 溯源 · 库存八要素滚动展开 |

详细字段字典：[docs/底表架构设计_指标字典.md](docs/底表架构设计_指标字典.md)

### 3.5 ⚠️ `day_clear` 字段业务语义（重要）

所有带 `day_clear` 字段的表都遵循以下口径：

| 值 | 含义 | 来源 |
|---|---|---|
| `'0'` | **日清** — 当日必须清空（生鲜、日配品） | 业务记录 |
| `'1'` | **非日清** — 可跨日留存（标品、冷藏加工类） | 业务记录 |
| `'2'` | **合计** — `day_clear=0` + `day_clear=1` 的汇总 | **由 ETL 额外 UNION 生成** |

**查询时的注意事项**：

- 查"全天合计"数据 → **必须** `WHERE day_clear = '2'`
- 按 `day_clear` 分组求 `SUM` 会**天然翻倍**（0+1+2 = 2×真实值），这是设计如此，不是 bug
- 客数字段单独在 `CustBuilder` 链路按订单级去重生成，**不要在 SKU 维度做 SUM**
- 当日如果没有日清 SKU（全是非日清），`t_fm_levels_sum` 只会看到 `day_clear='1'` 和 `='2'`

---

## 4. 目录结构详解

```
fm_etl_v3/
├── README.md                    ← 本文件
├── DEPLOY.md                    ← 云端部署完整手册
├── executor.py                  ← 主入口，串联 Step 1~16
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
│   ├── scm_adjust_extractor.py  ← atomic_scm_adjust
│   ├── loss_extractor.py        ← atomic_loss
│   ├── compose_extractor.py     ← atomic_compose
│   ├── allowance_extractor.py   ← atomic_allowance
│   ├── promo_extractor.py       ← atomic_promo
│   ├── cost_price_extractor.py  ← atomic_cost_price
│   ├── price_extractor.py       ← atomic_price
│   ├── bom_relation_extractor.py ← atomic_bom_relation（v5 降级为观测）
│   ├── receive_sale_extractor.py ← atomic_receive_sale（v5 BOM 核心源）
│   ├── order_receive_extractor.py ← atomic_order_receive（空骨架）
│   └── article_convert_extractor.py ← atomic_article_convert（空骨架）
│
├── calculated/                  ← Step 3~10：DuckDB 内部计算
│   ├── README.md
│   ├── merge.py                 ← AtomicMerger → t_atomic_wide
│   ├── inventory.py             ← InventoryCalculator → t_calc_inventory（legacy 观测）
│   ├── avg_price.py             ← AvgPriceCalculator → t_calc_avg_price（观测）
│   ├── bom_alloc.py             ← BomAllocCalculator → t_calc_bom_alloc（v9 Σ总权重算法）
│   ├── sku_cost.py              ← SkuCostCalculator → t_calc_sku_cost（v9 并行叠加）
│   ├── stock.py                 ← StockCalculator → t_calc_stock（v9 库存滚动）
│   ├── amounts.py               ← AmountsCalculator → t_calc_amounts（v9 compose 修正）
│   └── profit.py                ← ProfitCalculator → t_calc_profit（双口径毛利）
│
├── fm_tables/                   ← Step 11~16：FM 底表构建
│   ├── README.md
│   ├── sku_dim.py               ← SkuDimBuilder → t_fm_sku_dim
│   ├── cust.py                  ← CustBuilder → t_fm_cust（唯一仍调 API）
│   ├── levels_sum.py            ← LevelsSumBuilder → t_fm_levels_sum
│   ├── levels_result.py         ← LevelsResultBuilder → t_fm_levels_result
│   ├── bom_breakdown.py         ← BomBreakdownBuilder → t_fm_bom_breakdown
│   └── stock_roll.py            ← StockRollBuilder → t_fm_stock_roll
│
├── query_api/                   ← 只读 HTTP 查询服务（云端 systemd 托管）
│   ├── README.md
│   ├── app.py                   ← FastAPI 路由：/query /tables /schema /health
│   ├── auth.py                  ← Bearer Token 鉴权
│   ├── sql_guard.py             ← SQL 白名单守卫
│   └── models.py                ← Pydantic 请求/响应模型
│
├── deploy/                      ← 云端部署产物
│   ├── README.md
│   ├── daily_run.sh             ← 每日 cron 调用的 shell 脚本
│   ├── fm-query-api.service     ← systemd 单元文件
│   └── nginx-api.conf           ← nginx location /api/ 反代片段
│
├── scripts/                     ← 独立工具脚本
│   └── probe_tables.py          ← 探测源表是否可达 / schema 是否变化
│
└── docs/
    ├── TEAM_ACCESS.md           ← 5 人团队接入指南
    └── 底表架构设计_指标字典.md   ← 完整字段字典与口径说明
```

---

## 5. Pipeline 16 个步骤（v9.0）

`executor.py` 按顺序跑完 16 步，每步都可独立重跑（幂等，按日期分区覆盖写入）。
支持子命令：`--atomic-only` / `--calc-only` / `--fm-only`。

| 步骤 | 模块 | 输出 | 典型耗时（单日） |
|---|---|---|---|
| 1 | `DimsExtractor` | 7 张 `dim_*` 全量替换 | 10-30s |
| 2 | 12 个 `*Extractor` | 12 张 `atomic_*` 分区追加 + 2 张空骨架 | 3-8 分钟 |
| 3 | `AtomicMerger` | `t_atomic_wide` | 5-10s |
| 4 | `InventoryCalculator` | `t_calc_inventory`（legacy 观测） | <5s |
| 5 | `AvgPriceCalculator` | `t_calc_avg_price`（v9 观测，profit 不再使用） | <5s |
| 6 | `BomAllocCalculator` | `t_calc_bom_alloc` **v9 Σ总权重 + 共享组识别** | <5s |
| 7 | `SkuCostCalculator` | `t_calc_sku_cost` **v9 加权平均含期初库存** | <5s |
| 8 | `StockCalculator` | `t_calc_stock` **v9 跨日滚动（供明天 Step 7）** | <5s |
| 9 | `AmountsCalculator` | `t_calc_amounts` **v9 receive_amt 全计算 + lost_amt 简化** | <5s |
| 10 | `ProfitCalculator` | `t_calc_profit` **v9 双口径对齐（effective_unit_cost）** | <5s |
| 11 | `SkuDimBuilder` | `t_fm_sku_dim` | 10-30s |
| 12 | `CustBuilder` | `t_fm_cust`（唯一仍调 API） | 1-2 分钟 |
| 13 | `LevelsSumBuilder` | `t_fm_levels_sum` | 10-20s |
| 14 | `LevelsResultBuilder` | `t_fm_levels_result`（含双口径毛利） | 5-15s |
| 15 | `BomBreakdownBuilder` | `t_fm_bom_breakdown` AI 溯源 | <5s |
| 16 | `StockRollBuilder` | `t_fm_stock_roll` AI 溯源 | <5s |

**总耗时参考**：单日增量跑完约 **5~12 分钟**（取决于 API 响应速度）。

### 步骤间依赖

```
Step 1 (dims)     ──────────┐
Step 2 (atomic)   ──┬───▶ Step 3 (merge) ───▶ Step 4 (inventory, legacy观测)
                    │                                 │
                    │                                 ├──▶ Step 5 (avg_price, 观测)
                    │                                 │
                    │                                 ├──▶ Step 6 (bom_alloc, v9核心)
                    │                                 │         │
                    │                                 │         ▼
                    │                                 │     Step 7 (sku_cost, v9核心)
                    │                                 │         │    ↖ 读昨天 t_calc_stock
                    │                                 ├─────────┤
                    │                                 │         ▼
                    │                                 │     Step 8 (stock, v9核心)
                    │                                 │         │    ──→ 明天 Step 7
                    │                                 ▼         ▼
                    │                             Step 9 (amounts)
                    │                                 │
                    │                                 ▼
                    │                             Step 10 (profit)
                    │                                 │
                    │                                 ▼
                    └─────────────────────────▶ Step 11 (sku_dim) ──┐
                                                                     │
                                                  Step 12 (cust)  ──┤
                                                                     │
                                                          ┌──────────┤
                                                          ▼          │
                                                  Step 13 (levels_sum)
                                                          │
                                                          ▼
                                                  Step 14 (levels_result)

Step 6 (bom_alloc) ──────────────────────▶ Step 15 (bom_breakdown)
Step 8 (stock) ──────────────────────────▶ Step 16 (stock_roll)
```

> **跨日链式依赖**：Step 7 读**昨天**的 `t_calc_stock`（期初库存），Step 8 产出**今天**的 `t_calc_stock`（期末库存）→ 明天 Step 7 使用。同一天内无循环依赖。

---

## 6. 核心算法详解（v9.0）

### 6.1 BOM 分摊：v9 Σ总权重算法 + Python 共享组识别

**文件**: [calculated/bom_alloc.py](calculated/bom_alloc.py)

**数据源**: 唯一使用 `atomic_receive_sale`（不再使用 `atomic_bom_relation` 的 cost_rate/dressing_rate）。

**核心公式**:

```
① 消耗量   consume_qty = sale_qty + know_lost_qty
② 消耗权重 consume_weight = consume_qty × original_price（销售原价）
③ 自己进货权重 self_inbound_weight = self_inbound_qty × original_price

④ 拆分需求权重 split_need_weight:
   - Type A（自己有进货）: consume_weight - self_inbound_weight
   - Type B/C（无自己进货）: consume_weight

⑤ 共享组识别（Python）:
   如果 parent_B 的 subs ⊆ parent_A 的 subs，则合并为一个共享组
   组内 Σ总权重 和 parent 组进货额 统一计算

⑥ 分配占比 alloc_ratio = split_need_weight / Σ总权重
⑦ BOM分摊金额 bom_alloc_amt = alloc_ratio × 组总进货额

⑧ 拆分需求量 split_need_qty（v9 新增）:
   - Type A: consume_qty - self_inbound_qty
   - Type B/C: consume_qty
⑨ BOM分摊量 bom_alloc_qty = split_need_qty（供 sku_cost 作为成本量）
```

**输出表 `t_calc_bom_alloc` 关键字段**:

| 字段 | 含义 |
|---|---|
| `parent_article_id` / `sub_article_id` | BOM 父子关系 |
| `parent_inbound_qty` / `parent_inbound_amount` | parent 当天进货量/额 |
| `parent_unit_price` | parent 进货均价 |
| `consume_qty` / `consume_weight` | sub 消耗量/消耗权重 |
| `self_inbound_qty` / `self_inbound_amt` / `self_inbound_weight` | sub 自己进货量/额/权重 |
| `is_type_a` | 1=有自己进货（Type A），0=无（Type B/C） |
| `split_need_weight` | 拆分需求权重 |
| `split_need_qty` | **v9 新增** — 拆分需求量（供 sku_cost） |
| `group_total_weight` | 所在共享组的 Σ总权重 |
| `alloc_ratio` | 分配占比 |
| `bom_alloc_amt` | BOM 分摊金额 |
| `bom_alloc_qty` | **v9 新增** — BOM 分摊量（= split_need_qty） |
| `dressing_rate` | **v9 新增** — = alloc_ratio（别名，供 bom_breakdown） |
| `cost_rate_effective` | **v9 新增** — 占位 0（v9 不使用 cost_rate） |
| `sub_qty_actual` / `sub_qty_source` | **v9 新增** — 实际消耗量 + 来源标识（供 bom_breakdown） |
| `sub_alloc_amt` / `sub_unit_cost` | **v9 新增** — 分摊金额 + 单位成本（供 bom_breakdown） |
| `cost_rate_source` | 固定为 `'V9_WEIGHT_SHARED'` |

### 6.2 SKU 成本：v9 加权平均（含期初库存）

**文件**: [calculated/sku_cost.py](calculated/sku_cost.py)

**核心变化（v5 → v9）**: 分母从单纯消耗量改为**含期初库存的加权平均**。cost_price 不参与计算。

**核心公式**:

```
成本额 cost_amt = init_stock_amt       ← 期初库存额（昨天 t_calc_stock.end_stock_amt）
                + self_inbound_amt     ← 自己进货额
                + compose_net_amt      ← 加工净额（compose_in - compose_out）
                + bom_alloc_amt        ← BOM 分摊额

成本量 cost_qty = init_stock_qty       ← 期初库存量（昨天 t_calc_stock.end_stock_qty）
                + self_inbound_qty     ← 自己进货量
                + compose_net_qty      ← 加工净量
                + bom_alloc_qty        ← BOM 分摊量（= split_need_qty）

effective_unit_cost = cost_amt / cost_qty  （cost_qty > 0 时）
                    = 0                     （cost_qty = 0 时）

cost_source = 'V9_WEIGHTED_AVG'
```

**跨日链式传递**:

```
昨天 t_calc_stock.end_stock  ──→  今天 sku_cost.init_stock  ──→  effective_unit_cost
                                                                        │
                                                                今天 stock.end_stock  ──→  明天 sku_cost.init_stock ...
```

- Step 7 读**昨天**的 `t_calc_stock` 获取 `end_stock_qty / end_stock_amt` 作为今天的期初库存
- Step 8 用今天刚算出的 `effective_unit_cost` 计算今天 `end_stock_qty / end_stock_amt`
- 明天 Step 7 再读今天 Step 8 的产出 → 链式传递
- 首日（无昨天数据）fallback：`init_stock_amt = init_stock_qty_src × avg_inbound_price`

**三条成本源的单价逻辑**:

| 成本源 | 数量来源 | 单价来源 |
|---|---|---|
| 期初库存 | 昨天 `t_calc_stock.end_stock` | 昨天 effective_unit_cost 已隐含在金额中 |
| 自己进货 | `atomic_receive_sale` (article_id = sale_article_id) | 源表直接提供金额 |
| compose 加工 | `atomic_compose` | `avg_inbound_price`（来自 atomic_inventory，**不是** effective_unit_cost） |
| BOM 分摊 | `t_calc_bom_alloc` (按 sub 聚合) | `bom_alloc_amt / split_need_qty` |

### 6.3 库存：v9 跨日滚动

**文件**: [calculated/stock.py](calculated/stock.py)

**核心逻辑**: 今日期初库存 = 昨日期末库存（LAG 窗口函数），与 sku_cost 形成跨日链式闭环。

**三支分支逻辑**:

```
① day_clear = '0'（日清商品）:
    end_stock_qty   = 0
    unknow_lost_qty = init + receive + compose_in - compose_out - sale - know_lost

② know_lost_amt > 0（视为当天有盘点动作）:
    end_stock_qty   = 方程计算值
    unknow_lost_qty = 0

③ 否则（非日清 day_clear='1' 且无盘点）:
    end_stock_qty   = 方程计算值
    unknow_lost_qty = 0
```

**金额侧**: 统一使用 `effective_unit_cost`（来自 `t_calc_sku_cost`）计算自算金额，并与源表官方值做 diff 校验。

### 6.4 金额派生

**文件**: [calculated/amounts.py](calculated/amounts.py)

**v9 关键修正**:

| 字段 | v5 逻辑 | v9 逻辑 |
|---|---|---|
| `receive_amt` | `COALESCE(源表, qty × cost)` | `receive_qty × effective_unit_cost`（全计算） |
| `compose_in_amt` / `compose_out_amt` | `qty × avg_inbound_price` | 不变 |
| `know_lost_amt` | `COALESCE(源表, qty × cost)` | 不变（兜底仍用 effective_unit_cost） |
| `unknow_lost_amt` | `COALESCE(源表, stk自算, 0)` | 不变 |
| `lost_amt` | `know_lost_amt + unknow_lost_amt`（各自 COALESCE） | 不变（语义一致） |
| `init_stock_amt` | `COALESCE(源表, qty × cost)` | 不变（兜底仍用 effective_unit_cost） |

### 6.5 门店毛利：v9 双口径强制对齐

**文件**: [calculated/profit.py](calculated/profit.py)

**核心变化（v5 → v9）**: 销售成本（非日清）从 `avg_purchase_price` 切换为 `effective_unit_cost`，双口径数学恒等。

**两条毛利口径同时落盘**:

| 口径 | 字段名 | 公式 |
|---|---|---|
| **销售方程** | `store_profit_sales` | `sale_amt - sale_qty × effective_unit_cost - lost_amt` |
| **库存方程** | `store_profit_stock` | `sale_amt - (receive_amt + compose_in_amt - compose_out_amt) + (end_stock_amt - init_stock_amt)` |
| **口径差异** | `store_profit_diff` | `store_profit_sales - store_profit_stock` |

**双口径对齐原理**：当 receive_amt、lost_amt、end_stock_amt、init_stock_amt 全部使用同一 `effective_unit_cost` 计算时，库存方程和销售方程在数学上恒等（`store_profit_diff → 0`）。

**销售成本 `sale_cost_amt` 计算**:

```
日清 (day_clear='0'):
    sale_cost_amt = receive_amt + compose_in_amt - compose_out_amt - lost_amt
    （日清用金额流水，保持不变）

非日清 (day_clear='1'):
    sale_cost_amt = sale_qty × effective_unit_cost
    （v9 切换：不再使用 avg_purchase_price）
```

**运营毛利 `profit_amt`**（库存方程）:

```
profit_amt = sale_amt - (receive_amt + compose_in_amt - compose_out_amt)
           + (end_stock_amt - init_stock_amt)
```

### 6.6 day_clear='2' 合计行的生成

合计行在 `t_fm_levels_sum` 构建时通过 `UNION ALL` 生成（[levels_sum.py:143-183](fm_tables/levels_sum.py#L143-L183)）。每个层级都生成两遍：
- `exact` 模式：保留原始 `day_clear`（0 或 1）
- `total` 模式：固定输出 `day_clear='2'`，聚合日清+非日清

---

## 7. 本地开发环境搭建

### 7.1 前置要求

- Python **3.10+**
- git
- 能访问 `bdapp.qdama.cn`（QDM BI API）

### 7.2 首次配置

```bash
# 1. 克隆仓库
git clone git@github.com:<你的组织>/cuihua-data.git
cd cuihua-data

# 2. 建 venv
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

# 3. 装依赖
pip install -r fm_etl_v3/requirements.txt

# 4. 配置 .env
cp fm_etl_v3/.env.example .env
# 填入 QDM_ACCESS_KEY / QDM_SECRET_KEY
```

### 7.3 跑一天数据（冒烟测试）

```bash
# 跑昨天一天
python -m fm_etl_v3.executor 2026-04-29 2026-04-29

# 跑一周
python -m fm_etl_v3.executor 2026-04-23 2026-04-29

# 只跑计算层（需 atomic 已存在）
python -m fm_etl_v3.executor 2026-04-29 2026-04-29 --calc-only

# 看结果
python -c "
import duckdb
con = duckdb.connect('data/fm_etl_v3.duckdb', read_only=True)
for (name,) in con.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1\").fetchall():
    cnt = con.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]
    print(f'{name:30s} {cnt:>12,}')
"
```

---

## 8. 云端部署与运维

完整手册见 [DEPLOY.md](DEPLOY.md)。

### 8.1 服务器信息

- **IP**：`47.115.213.115`（阿里云广州）
- **对外端口**：`8080`（nginx）
- **SSH**：`ssh root@47.115.213.115`
- **目录约定**：
  - `/opt/fm/etl/cuihua-data/` ← ETL 代码
  - `/opt/fm/data/fm.duckdb` ← 唯一数据文件
  - `/opt/fm/logs/` ← ETL + API 日志

### 8.2 三个自动运行的组件

| 组件 | 触发方式 | 文件 |
|---|---|---|
| ETL 每日跑 | cron `0 2 * * *` | `/opt/fm/etl/daily_run.sh` → `python -m fm_etl_v3.executor 昨天 昨天` |
| Query API | systemd `fm-query-api.service` | uvicorn 127.0.0.1:5003 |
| nginx 反代 | nginx 现有服务 | `location /api/` → 127.0.0.1:5003 |

### 8.3 改代码 → 生效

**常规路径**：本地 `git push` → 服务器 02:00 自动 `git pull --ff-only` + 跑 ETL

**紧急热更新**（改了 Query API）：
```bash
ssh root@47.115.213.115
cd /opt/fm/etl/cuihua-data
git pull --ff-only origin main
systemctl restart fm-query-api
```

### 8.4 补历史数据

```bash
ssh root@47.115.213.115
cd /opt/fm/etl/cuihua-data
source .venv/bin/activate
python -m fm_etl_v3.executor 2026-04-01 2026-04-19
```

pipeline 按日期分区幂等，同一区间重跑会先删旧分区再写。

### 8.5 看日志

```bash
# ETL 月度聚合日志
tail -f /opt/fm/logs/etl-$(date +%Y-%m).log

# cron 日志
tail -f /opt/fm/logs/cron.log

# Query API 日志
tail -f /opt/fm/logs/query-api.log
journalctl -u fm-query-api -f
```

---

## 9. 查询数据的三种方式

### 9.1 HTTP API（最轻量）

```bash
# 健康检查
curl http://47.115.213.115:8080/api/health

# 列出所有表 + 行数
curl http://47.115.213.115:8080/api/tables \
  -H "Authorization: Bearer <你的 Token>"

# 执行 SELECT
curl -X POST http://47.115.213.115:8080/api/query \
  -H "Authorization: Bearer <你的 Token>" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT store_id, SUM(store_profit_sales) FROM t_fm_sku_dim WHERE business_date = '\''2026-04-29'\'' AND day_clear = '\''2'\'' GROUP BY store_id"}'
```

**限制**：单查询 60s 超时 + 默认 10000 行上限（最大 50000）。

### 9.2 DBeaver + SSH 隧道（分析师）

见 [docs/TEAM_ACCESS.md](docs/TEAM_ACCESS.md)。关键：连接字符串必须追加 `?access_mode=read_only`。

### 9.3 Cursor Remote-SSH（开发者）

`~/.ssh/config` 加 `Host fm-prod` → Cursor Remote-SSH → 打开 `/opt/fm/etl/cuihua-data`。

---

## 10. 常见故障排查

### 10.1 ETL 失败

```bash
# 看当日日志
tail -100 /opt/fm/logs/etl-$(date +%Y-%m).log

# 最常见：git pull 冲突
tail /opt/fm/logs/cron.log
# → "git pull failed" → SSH 上去：
#   cd /opt/fm/etl/cuihua-data
#   git fetch origin && git reset --hard origin/main

# QDM API 429 或超时 → 重跑失败的那天
```

### 10.2 Query API 502 Bad Gateway

```bash
systemctl status fm-query-api
journalctl -u fm-query-api -n 100
```

### 10.3 SQL 被守卫拒绝（400）

只允许 `SELECT / SHOW / DESCRIBE / EXPLAIN / WITH` 开头的 SQL。

### 10.4 DuckDB 锁冲突

ETL 跑的时候（02:00 ~ 02:15）Query API 可能偶发 `IO Error`。用户端查询建议在 03:00 之后。

---

## 11. 相关文档索引

| 文档 | 内容 |
|---|---|
| [DEPLOY.md](DEPLOY.md) | 云端部署完整手册 |
| [docs/TEAM_ACCESS.md](docs/TEAM_ACCESS.md) | 5 人团队接入指南 |
| [docs/底表架构设计_指标字典.md](docs/底表架构设计_指标字典.md) | 完整字段字典与口径说明 |
| [docs/strategy_fm_mapping.md](docs/strategy_fm_mapping.md) | 源表→目标表字段映射 |
| [docs/BOM毛利计算问题分析.md](docs/BOM毛利计算问题分析.md) | BOM 毛利算法演进分析 |
| [config/README.md](config/README.md) | 配置项说明 |
| [connectors/README.md](connectors/README.md) | API + DuckDB 封装 |
| [atomic/README.md](atomic/README.md) | 原子域提取器 |
| [calculated/README.md](calculated/README.md) | 计算层公式 |
| [fm_tables/README.md](fm_tables/README.md) | FM 底表 |
| [query_api/README.md](query_api/README.md) | HTTP 查询服务 |

---

## 附：环境变量完整清单

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `QDM_ACCESS_KEY` | ✅ | — | QDM BI API access key |
| `QDM_SECRET_KEY` | ✅ | — | QDM BI API secret key |
| `QDM_API_ID` | ❌ | `i_fjl10g687-790` | API 路由 ID |
| `QDM_HOST` | ❌ | `https://bdapp.qdama.cn` | API 主机 |
| `QDM_VERSION` | ❌ | `1.0` | API 协议版本 |
| `FM_DUCKDB_PATH` | ❌ | `data/fm_etl_v3.duckdb`（本地）<br>`/opt/fm/data/fm.duckdb`（云端） | DuckDB 文件路径 |
| `FM_TOKENS` | ❌（仅云端） | — | Query API 鉴权 Token 列表 |
| `FM_QUERY_TIMEOUT_SEC` | ❌ | `60` | 单查询超时秒数 |
