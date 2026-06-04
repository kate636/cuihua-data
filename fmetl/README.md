# fmetl v0.10 — 翠花当家数据管道

## 目录

1. [项目背景与整体方案](#1-项目背景与整体方案)
2. [系统架构与硬性红线](#2-系统架构与硬性红线)
3. [快速开始](#3-快速开始)
4. [架构设计](#4-架构设计)
5. [完整计算逻辑流程](#5-完整计算逻辑流程)
6. [13步流程详解](#6-13步流程详解)
7. [QDM源表→DuckDB表映射](#7-源表映射)
8. [v0.10修复对照表](#8-v0.10修复对照表)
9. [day_clear字段说明](#9-day_clear字段说明)
10. [验证方案](#10-验证方案)
11. [环境配置](#11-环境配置)

---

## 1. 项目背景与整体方案

### 1.1 业务目标

对广州 Food Mart 门店做**商品日粒度 × 全链路指标**的数据生产，覆盖销售 / 进货 / 库存 / 损耗 / 加工 / BOM拆分 / 促销 / 补贴 / 供应链 / 价格十大业务域，支撑 FM 平台经营看板、毛利分析、损耗归因、AI SKU 诊断等场景。

最终产物是 6 张底表：

| 底表 | 内容 |
|---|---|
| `t_fm_sku_dim` | SKU 级完整宽表（原子 + 计算指标，80字段） |
| `t_fm_cust` | 按 6 个层级聚合的客数 |
| `t_fm_levels_sum` | 7 层分类展开的数量/金额汇总 |
| `t_fm_levels_result` | **平台对接表**，中文列名 + 比率型 KPI |
| `t_fm_bom_breakdown` | BOM 分摊溯源表（parent × sub 明细，AI 友好） |
| `t_fm_stock_roll` | 库存八要素滚动展开（含 balance_qty 校验，AI 友好） |

### 1.2 核心设计原则

**原子层 / 计算层分层**：

- **Layer -2 原子层**：只放**不可再分解的独立观测量**（数量、单价、标识、POS 交易金额、SAP 让利金额）
- **Layer -1 计算层**：放**所有可由公式推导的指标**（金额、库存余额、差异、毛利）
- **效果**：底层改一个值，上层自动算对；毛利 / 损耗 / 全链路指标的口径不会在多处拷贝漂移

这组分层完整落到 DuckDB 表里：`atomic_*` 对应 Layer -2，`t_calc_*` 对应 Layer -1，`t_fm_*` 是最终对外的 Layer 0。

**v0.10 新增设计原则**：

| 原则 | 说明 |
|------|------|
| **简单SQL，复杂逻辑Python** | SQL 仅做 SELECT / JOIN / GROUP BY / WHERE。计算、分支、窗口函数全部在 Python（pandas + NumPy）中完成 |
| **每个数字只算一次** | 上游结果直接复用，下游不重复计算。例如 euc 只在 sku_cost.py 计算一次，stock/profit 只引用不重算 |
| **四流分离** | 进货(receive)、拆分入(bom_in)、拆分出(bom_out)、加工入/出(compose_in/out) 各自独立列，一目了然 |
| **跨日链式传递** | 今日期初库存 = 昨日期末库存，通过 `t_calc_stock` 逐日滚动 |

### 1.3 从 v9 到 v0.10 的迭代

| 方面 | v9（旧） | v0.10（现在） |
|------|---------|------------|
| 计算引擎 | SQL 内嵌复杂 CASE/窗口函数 | **Python pandas + NumPy** 计算，SQL 仅拉数据 |
| 计算模块 | 7 张 calc 表 (inventory + avg_price + amounts + ...) | **4 张 calc 表** (删除 3 个冗余模块，合并到 stock.py) |
| 库存方程 | 三流 (receive + compose + sale) | **四流** (+bom_in -bom_out) |
| 毛利公式 | 不含 BOM | **含 BOM 流入/流出** |
| BOM 分摊 | SQL 中算 | **Python 8 步计算** + 共享组分拆 + 单位归一化 |
| 负库存 | 不处理 | **eq<0 → end=0, unknow=-eq**（负库存保护） |
| 盘点判断 | `know_lost_amt > 0`（金额） | **`know_lost_qty > 0`**（数量，更可靠） |

触发迭代的直接原因：
- v9 的 SQL 内嵌计算越来越复杂，BOM 分摊在多父品共享子品场景下偏差大
- 负库存导致库存方程无法闭合，需要保护性转未知损耗
- 毛利公式缺失 BOM 流入/流出项，猪肉类毛利偏差 > 20%
- Python 重写后每个计算步骤可独立调试、打印中间结果

### 1.4 整体方案一句话总结

> **QDM BI API 是唯一数据来源 → 在本地 DuckDB 里做 13 个 Step 的 Python 计算 → 结果留在同一个 `fm.duckdb` 文件里 → 通过 nginx :8080 静态报告 + DuckDB 直连提供查询。**

### 1.5 范围边界（本期不做）

| 不做项 | 原因 |
|-------|------|
| MotherDuck / 其他云数据库 | 数据不出服务器，降低合规风险 |
| 分布式调度（Airflow 等） | 一个 cron 已够用 |
| 流式增量 | 日粒度满足业务需求 |
| DuckDB MCP Server、OSS 快照备份、飞书告警 | 后续 Phase |

---

## 2. 系统架构与硬性红线

### 2.1 部署架构

```
                         ┌─── 翠花经营监控平台 (FM Platform) ───┐
                         │  http://47.115.213.115:8080/reports/  │
                         │                                       │
                         │  ┌─ 报表看板                         │
                         │  │  经营日报 / 毛利分析 / 损耗归因    │
                         │  │  AI SKU 诊断                      │
                         │  ├─ 加工关系管理 (网页端)             │
                         │  │  业务填写原料→成品配方            │
                         │  │  /reports/processing-relation.html │
                         │  └─ 加工关系 API (Flask, :5003)       │
                         │     SQLite 存储, systemd 保活         │
                         └──────────┬────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
              BI API 查源表    nginx 反向代理    读取底表
              (StarRocks)     /api/proc-rel/   (DuckDB)
                    │               │               │
    ┌───────────────┼───────────────┼───────────────┼──────────────┐
    │                                         阿里云 ECS (广州)    │
    │                                         47.115.213.115       │
    │                                                              │
    │  ┌───────────────────────────────────────────────────────┐  │
    │  │              fmetl v0.10 ETL 管道                     │  │
    │  │                                                       │  │
    │  │  sync_strategy_fm.sql (手动)                          │  │
    │  │    Hive → StarRocks 21张源表同步                      │  │
    │  │            │                                          │  │
    │  │            ▼                                          │  │
    │  │  ① atomic_* (15张) ← BI API 拉取                     │  │
    │  │  ② t_atomic_wide (82字段) ← merge                     │  │
    │  │  ③ t_calc_bom_alloc (BOM分摊, Python 8步)            │  │
    │  │  ④ t_calc_sku_cost (加权成本, 跨日链)                │  │
    │  │  ⑤ t_calc_stock (四流合一, 库存方程, 中枢)           │  │
    │  │  ⑥ t_calc_profit (门店毛利, 含BOM+SCM)               │  │
    │  │  ⑦ t_fm_* (6张底表) → /opt/fm/data/fm.duckdb         │  │
    │  │                                                       │  │
    │  │  验证: 负库存/BOM对称/方程平衡/QDM对比                │  │
    │  └───────────────────────────────────────────────────────┘  │
    │                                                              │
    │  /opt/fm/etl/cuihua-data/   代码 (git clone)                │
    │  /opt/fm/data/fm.duckdb     数据文件 (不上 GitHub)          │
    │  /opt/fm/logs/              ETL 日志                        │
    │                                                              │
    │  cron 08:50: cd /opt/fm/etl && git pull && 跑ETL            │
    └──────────────────────────────────────────────────────────────┘
                    │
                    │ git push
                    ▼
    ┌──────────────────────────────┐
    │  GitHub 私有仓库              │
    │  kate636/cuihua-data          │
    └──────────────────────────────┘
                    │
                    │ git push / pull
                    ▼
    ┌──────────────────────────────┐
    │  本地 Mac (开发)              │
    │  Cursor + Claude Code         │
    │  三 agent 流程:               │
    │    设计 → 编码 → 审查         │
    │  持久记忆 + Skill 体系        │
    └──────────────────────────────┘
```

### 2.2 系统互动关系

| 组件 | 如何互动 | 方向 |
|------|---------|:---:|
| StarRocks 源表 → ETL | BI API 拉取 21 张 strategy_fm_* 表，7天chunk分区写入 | 拉 |
| Hive → StarRocks | sync_strategy_fm.sql 每日手动执行，从 Hive 同步到商分库 | 推 |
| 加工关系 → ETL | API 优先 (nginx :8080/api/proc-rel/)，失败回退本地 JSON 缓存，成功后自动写回缓存 | 拉 |
| ETL → DuckDB | 13 步计算产出 6 张底表，写入 /opt/fm/data/fm.duckdb | 写 |
| DuckDB → FM 看板 | nginx :8080 静态报告 + DuckDB 直连查询 | 读 |
| 本地 → 服务器 | git push → GitHub → cron pull，全量重刷后 scp DuckDB | 推 |
| 持久记忆/Skill → Agent | 新 session 启动时自动加载 13 条记忆 + 22+ Skill | 自动 |

### 2.3 迭代路径

```
v3 (旧版) ──→ v4 (BOM重写) ──→ v0.10 (架构重构)
  SQL计算        scripts/下9版     三层分离
  7张calc表      临时脚本验证       4张calc表
                                  三agent流程
                                  修复文档体系
```

BOM 分摊逻辑在 `fm_etl_v3/scripts/` 下迭代了 9 版临时脚本，每版输出详细中间计算结果逐行验证，确认正确后才集成到 ETL 主流程。

### 2.4 两条硬性红线

| 红线 | 强制手段 |
|---|---|
| **数据库绝不上 GitHub** | `.gitignore` 排除 `data/` / `*.duckdb` / `*.duckdb.wal`；`.env` 也不入库 |
| **现有看板一律不动** | `/opt/fm/reports/` 保持现状；nginx :8080 已配置 `/reports/` 和 `/api/proc-rel/` |

---

## 3. 快速开始

### 安装依赖

```bash
pip install duckdb pandas numpy python-dotenv requests
```

### 配置凭证

创建 `.env` 文件：

```
QDM_ACCESS_KEY=你的QDM_AK
QDM_SECRET_KEY=你的QDM_SK
FM_DUCKDB_PATH=data/fm.duckdb
```

### 运行

```bash
# 单日执行（首日）
python -m fmetl.executor 2026-04-23 2026-04-23

# 日期范围
python -m fmetl.executor 2026-04-01 2026-04-30

# 分阶段执行（调试用）
python -m fmetl.executor 2026-04-23 2026-04-23 --atomic-only   # 仅提取+合并
python -m fmetl.executor 2026-04-23 2026-04-23 --calc-only     # 仅计算层
python -m fmetl.executor 2026-04-23 2026-04-23 --fm-only       # 仅FM底表
```

### 验证

```bash
# 查询 DuckDB 结果
python3 -c "
import duckdb
conn = duckdb.connect('data/fm.duckdb', read_only=True)
print(conn.execute(\"SELECT * FROM t_fm_levels_result WHERE 分类等级 = 'SKU' LIMIT 5\").df())
"
```

---

## 4. 架构设计

### 三层结构

```
┌──────────────────────────────────────────────────────────────────┐
│ Layer -2: atomic_* (原子层) — 不可分解的独立观测量              │
│   13张活跃 + 2张骨架表                                           │
│   粒度: store_id × business_date × article_id × day_clear       │
│   从 QDM BI API 分批拉取，DuckDB 本地存储                        │
└──────────────────────────────────────────────────────────────────┘
                              ↓ merge → t_atomic_wide (82字段)
┌──────────────────────────────────────────────────────────────────┐
│ Layer -1: t_calc_* (计算层) — Python 推导的指标                 │
│   t_calc_bom_alloc  → BOM分摊 (Σ总权重 + 共享组 + 单位归一化)   │
│   t_calc_sku_cost   → 有效单位成本 (加权平均含期初库存)         │
│   t_calc_stock      → 库存与金额 (四流合一 + 跨日滚动, 中枢)    │
│   t_calc_profit     → 门店毛利 (含BOM + SCM + 全链路)           │
└──────────────────────────────────────────────────────────────────┘
                              ↓ build
┌──────────────────────────────────────────────────────────────────┐
│ Layer 0: t_fm_* (FM底表) — 最终产出                             │
│   t_fm_sku_dim       → SKU级完整宽表 (80字段)                   │
│   t_fm_cust          → 客数聚合                                  │
│   t_fm_levels_sum    → 分类汇总 (数量/金额)                      │
│   t_fm_levels_result → 平台对接表 (中文列名 + 比率KPI)           │
│   t_fm_bom_breakdown → BOM分摊溯源 (parent × sub 明细)          │
│   t_fm_stock_roll    → 库存八要素滚动 (含 balance_qty 校验)     │
└──────────────────────────────────────────────────────────────────┘
```

### 数据流全景

```
                  ┌─────────────────────┐
                  │  QDM BI API (19张源表)│
                  └──┬──────┬──────┬───┘
                     │      │      │
         ┌───────────┘      │      └───────────┐
         ↓                  ↓                  ↓
  维度表(7张)         原子域(13张)        receive_sale
  dim_goods           atomic_sales         (BOM核心)
  dim_store_list      atomic_inventory          │
  dim_day_clear       atomic_scm               ↓
  dim_store_profile   atomic_scm_adjust   bom_alloc.py
  dim_calendar        atomic_loss         (per parent×sub)
  dim_saleable        atomic_compose           │
  dim_chdj_store_info atomic_allowance    ┌────┴────┐
                      atomic_promo        ↓         ↓
                      atomic_cost_price  bom_in   bom_out
                      atomic_price      (sub)   (parent)
                      atomic_bom_relation   │    │
                      atomic_receive_sale   │    │
                             │              │    │
                             ↓              ↓    ↓
                        merge.py ────→ t_atomic_wide
                             │
                             ↓
                        sku_cost.py → t_calc_sku_cost (euc)
                             │
                             ↓
                        stock.py → t_calc_stock (四流合一)
                             │
                             ↓
                        profit.py → t_calc_profit
                             │
                             ↓
                        fm_tables/ (6张底表)
```

### 目录结构

```
fmetl/
├── executor.py               # 主入口，14步调度（13步主流程 + Step 14 加工候选同步）
├── config/                   # API 凭证 (.env → Pydantic Settings)
├── connectors/               # ApiConnector (重试) + DuckDBStore (本地)
├── utils/                    # get_logger 日志工具
│
├── atomic/                   # 15个提取器 (13原子域 + 2骨架)
│   ├── _base.py              # BaseExtractor: 分区写入 + 重试
│   └── *_extractor.py        # 每个只需实现 _fetch_sql()
│
├── calculated/               # 5个计算模块 (Python 核心)
│   ├── merge.py              # 原子宽表合并 + 父品补全
│   ├── bom_alloc.py          # BOM 分摊 (8步Python计算)
│   ├── sku_cost.py           # 有效单位成本 (加权平均含期初)
│   ├── stock.py              # 库存与金额 (四流合一中枢)
│   └── profit.py             # 门店毛利 (含BOM + SCM)
│
├── fm_tables/                # 6张FM底表产出
│   ├── sku_dim.py            # SKU 维度完整宽表
│   ├── cust.py               # 客数聚合
│   ├── levels_sum.py         # 分类汇总
│   ├── levels_result.py      # 平台对接表
│   ├── bom_breakdown.py      # BOM 分摊溯源
│   └── stock_roll.py         # 库存滚动展开
│
└── data/                     # DuckDB 数据文件 (gitignore)
```

---

## 5. 完整计算逻辑流程

### 数据总览：从 QDM API 到 FM 底表（13 步全链路）

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 1-2: 数据提取 (DimsExtractor + 14个*Extractor)                     │
│                                                                         │
│ QDM BI API ──→ 7张 dim_* (全量替换)                                     │
│ QDM BI API ──→ 13张 atomic_* (分区replace_partition) + 2张空骨架         │
│   粒度: store × date × article_id × day_clear                          │
│   品类过滤: cat_level1 NOT IN ('70'-'77')                               │
│   门店过滤: INNER JOIN dim_store_list                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 3: AtomicMerger → t_atomic_wide (82字段)                           │
│                                                                         │
│ ① FULL OUTER JOIN atomic_sales + atomic_inventory (基表)                │
│ ② 从 atomic_receive_sale 提取两路进货:                                   │
│    - 自购行 (article_id = sale_article_id)  → SUM聚合                   │
│    - BOM父品行 (article_id ≠ sale_article_id) → MAX去重                  │
│ ③ LEFT JOIN 10张其他原子表 (scm/loss/compose/allowance/promo/...)       │
│ ④ LEFT JOIN dim_day_clear → 日清/非日清标签                             │
│ ⑤ INNER JOIN dim_store_list → 翠花门店过滤                              │
│ ⑥ INSERT BOM父品补全行 (父品不在sales/inventory中但有进货额)             │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 4: BomAllocCalculator → t_calc_bom_alloc (Python 8步)              │
│                                                                         │
│ ① 从 atomic_receive_sale 加载 BOM 关系 (parent≠sub 的行)                │
│ ② 构建 parent→{subs} 映射 + parent进货额/量                             │
│ ③ 识别 parent 共享组 (parent_B.subs ⊆ parent_A.subs → 合并为一组)        │
│ ④ 从 atomic_sales + atomic_price + atomic_loss 加载 sub 消耗数据         │
│ ⑤ 从 atomic_receive_sale 加载 sub 自己进货数据 (article_id=sale_article) │
│ ⑥ 计算每个 sub 的拆分需求权重:                                           │
│    consume_qty = sale_qty + know_lost_qty                               │
│    consume_weight = consume_qty × list_price                            │
│    self_inbound_weight = self_inbound_qty × list_price                  │
│    split_need_weight = consume_weight - self_inbound_weight (Type A)     │
│                      = consume_weight (Type B/C, 无自己进货)             │
│ ⑦ 分配计算:                                                             │
│    Σ总权重 = 组内所有sub的split_need_weight之和                         │
│    alloc_ratio = split_need_weight / Σ总权重                            │
│    bom_alloc_amt = alloc_ratio × 组总parent进货额                        │
│    【共享组】共享子品按parent进货额比例分拆给各parent                     │
│    【单位归一化】bom_alloc_qty = split_need_qty × (parent_qty/sum_sub_qty)│
│ ⑧ 写入 t_calc_bom_alloc (parent×sub 粒度，27字段)                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 5: SkuCostCalculator → t_calc_sku_cost (Python 加权平均)           │
│                                                                         │
│ ① 从 t_atomic_wide 取基础数据 (self_receive/compose/init_stock_src)      │
│ ② 从 t_calc_bom_alloc 取 BOM 分摊额 (按 sub 聚合 SUM)                   │
│ ③ 从 t_calc_stock (昨天) 取期初库存 → 今天 init_stock                    │
│    【跨日链式】昨日 end → 今日 init → LAG(+1天)                          │
│    【首日】无昨天数据 → 直接用 atomic_inventory 源表值                    │
│ ④ compose 净额: 配方推算（成品=Σ(raw_qty/yield_qty×raw_euc), 原料=价值守恒）│
│ ⑤ 加权平均:                                                             │
│    cost_amt = init_stock_amt + self_receive_amt + compose_net_amt        │
│             + bom_alloc_amt                                              │
│    cost_qty = init_stock_qty + self_receive_qty + compose_net_qty        │
│             + bom_alloc_qty                                              │
│    euc = cost_amt / cost_qty       (cost_qty > 0)                        │
│    cost_source = 'V10_WEIGHTED_AVG'                                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 6: StockCalculator → t_calc_stock (Python 四流合一, 中枢模块)       │
│                                                                         │
│ ① 加载 t_atomic_wide (含 self_receive/compose/sale/lost/SCM)            │
│ ② 加载 t_calc_sku_cost (euc) + t_calc_bom_alloc (bom_in按sub, bom_out按parent)│
│ ③ 加载 昨天 t_calc_stock → shift(+1天) → today.init_stock                │
│    【首日】fallback: init_stock_qty_src / init_stock_amt_src              │
│ ④ 【父品补全】BOM 父品不在 wide 中但有 bom_out → 创建空行补入            │
│ ⑤ 四流合一库存方程:                                                     │
│    eq = init + receive + bom_in - bom_out + compose_in - compose_out     │
│        - sale - know_lost                                               │
│ ⑥ 分支判断 (逐个SKU，Python loop):                                      │
│    ├─ is_counted (人工盘点) → end=actual,    unknow=max(0,eq-actual)     │
│    ├─ day_clear='0'(软日清)→ 清新供给,      init可部分消耗              │
│    ├─ eq < 0 (负库存保护)  → end=0,         unknow=-eq                  │
│    ├─ know_lost_qty > 0    → end=eq,        unknow=0                    │
│    └─ 其他                  → end=eq,        unknow=0                    │
│ ⑦ 金额统一 euc:                                                         │
│    end_stock_amt = end_qty × euc                                        │
│    unknow_lost_amt = unknow_qty × euc                                   │
│    know_lost_amt = know_qty × euc                                       │
│    lost_amt = know_lost_amt + unknow_lost_amt                           │
│ ⑧ 写出 t_calc_stock (全量库存 + 金额 + SCM，30+字段)                     │
│                                                                         │
│ 跨日依赖: 今天 end_stock ──→ 明天 Step 5 (sku_cost).init_stock ──→ ...   │
│          同一天内无循环依赖 (Step5读昨天stock, Step6写今天stock)          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 7: ProfitCalculator → t_calc_profit (Python)                       │
│                                                                         │
│ ① 从 t_calc_stock 取库存+金额 (四流已含BOM)                              │
│ ② 新毛利公式 (含BOM):                                                    │
│    profit = sale - receive - bom_in + bom_out - compose_in + compose_out │
│            + end_stock - init_stock                                      │
│    【注意】损耗已通过库存方程反映在 end_stock 中，不再额外扣减            │
│ ③ 销售成本 (统一公式):                                                   │
│    sale_cost_amt = sale_qty × euc                                       │
│ ④ SCM 金融毛利: income(丨out丨-丨return丨_notax) - cost(丨out丨-丨return丨_cb)│
│ ⑤ 全链路毛利 = profit + scm_income - scm_cost                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ Step 8-13: FM底表构建                                                    │
│                                                                         │
│ Step 8  → t_fm_sku_dim (80字段, Python类别重映射, 合并所有域+维度)       │
│ Step 9  → t_fm_cust (客数聚合, Python JOIN dim_goods 过滤品类70-77)      │
│ Step 10 → t_fm_levels_sum (7级分类汇总 UNION ALL)                       │
│ Step 11 → t_fm_levels_result (中文列名 + 比率KPI)                       │
│ Step 12 → t_fm_bom_breakdown (BOM分摊溯源, parent×sub明细)              │
│ Step 13 → t_fm_stock_roll (库存八要素滚动 + balance_qty校验)            │
└─────────────────────────────────────────────────────────────────────────┘
```

### BOM 分摊完整流程（Σ总权重 + 共享组识别 + 单位归一化）

BOM (Bill of Materials) 是 v0.10 最复杂的计算模块。核心问题：当一个父品（如"大白猪A级"）拆分为多个子品（五花肉、前腿肉...）销售时，如何将父品的进货成本合理分摊到各子品？

**数据源**：唯一使用 `atomic_receive_sale`（`strategy_fm_receive_sale_di`），不再使用 `atomic_bom_relation`。

#### BOM 关系加载与分类

从 `atomic_receive_sale` 中分离两类数据：
- **BOM 关系行** (`article_id ≠ sale_article_id`)：parent→sub 的进货事实，一 parent 对多 sub
- **自购行** (`article_id = sale_article_id`)：sub 自己也有独立进货

#### 三种 SKU 类型

| 类型 | 特征 | 拆分需求权重 |
|------|------|-------------|
| **Type A** | sub 自己有进货 (self_inbound_qty > 0) | consume_weight - self_inbound_weight |
| **Type B** | sub 无自己进货，但在 parent 的 subs 中 | consume_weight |
| **Type C** | sub 无自己进货，且不在任何 parent 中 | — (不参与 BOM) |

#### Python 8 步计算详解

```
步骤①: 加载 BOM 关系
  SELECT FROM atomic_receive_sale WHERE article_id != sale_article_id
  → 获取 parent_article_id, sub_article_id, parent_inbound_qty, parent_inbound_amount

步骤②: 构建 parent→subs 映射
  parent_subs[(store, date)][parent_id] = {sub_id_1, sub_id_2, ...}
  例: 大白猪A级 → {五花肉, 前腿肉, 后腿肉, 排骨}
      黑猪A级   → {五花肉, 前腿肉}

步骤③: 识别 parent 共享组
  如果 parent_B 的 subs ⊆ parent_A 的 subs → 合并为一个共享组
  例: 黑猪A级.subs ⊆ 大白猪A级.subs → 共享组 {大白猪A级, 黑猪A级}
  → 共享子品 "五花肉" 按大白猪:黑猪进货额比例分配 bom_out

步骤④: 加载 sub 消耗数据
  FROM atomic_sales (sale_qty) + atomic_price (list_price) + atomic_loss (know_lost_qty)

步骤⑤: 加载 sub 自己进货数据
  FROM atomic_receive_sale WHERE article_id = sale_article_id
  → self_inbound_qty, self_inbound_amt

步骤⑥: 计算拆分需求权重
  consume_qty = sale_qty + know_lost_qty
  consume_weight = consume_qty × list_price
  self_inbound_weight = self_inbound_qty × list_price

  IF self_inbound_qty > 0 (Type A):
      split_need_weight = max(0, consume_weight - self_inbound_weight)
      split_need_qty   = max(0, consume_qty - self_inbound_qty)     ← v0.10 A14: 防负
  ELSE (Type B/C):
      split_need_weight = consume_weight
      split_need_qty   = consume_qty

  为什么减去自己进货？→ sub 自己进货的部分不需要 parent 分摊

步骤⑦: 分配计算

  ⑦a. 共享组 parent:
  Σ总权重 = Σ 组内所有 sub 的 split_need_weight
  alloc_ratio = split_need_weight / Σ总权重
  bom_alloc_amt = alloc_ratio × 组总 parent_inbound_amount

  【共享子品分拆】(v0.10 A18):
    共享子品（同时属于两个parent）按各 parent 进货额比例分拆:
    p0_ratio = parent_A.amt / (parent_A.amt + parent_B.amt)
    p1_ratio = parent_B.amt / (parent_A.amt + parent_B.amt)
    → 生成两行: (parent_A, sub, bom_alloc_amt×p0_ratio) + (parent_B, sub, bom_alloc_amt×p1_ratio)

  【单位归一化】(v0.10 A19):
    bom_alloc_qty = split_need_qty × (parent_qty / sum_sub_qty) × qty_split_ratio
    例: 海大虾 1箱=12kg, 子品消耗10kg → bom_out = 10 × (1/12) = 0.833箱
    目的: 保证库存方程各列单位一致 (全部用父品单位)

  ⑦b. 单独 parent (不在任何共享组):
  Σ总权重 = Σ 该 parent 所有 sub 的 split_need_weight
  alloc_ratio = split_need_weight / Σ总权重
  bom_alloc_amt = alloc_ratio × parent_inbound_amount
  bom_alloc_qty = split_need_qty × (parent_qty / sum_sub_qty)

步骤⑧: 写入 t_calc_bom_alloc (parent×sub 粒度, 27字段)
  包含: bom_alloc_amt, bom_alloc_qty, alloc_ratio (=dressing_rate),
        split_need_weight, parent_inbound_qty/amt 等溯源字段
```

#### BOM 数据在后续模块的流向

```
t_calc_bom_alloc
    │
    ├── 按 sub 聚合 → bom_in_qty/amt  → sku_cost.py (Step 5: 成本量/额)
    │                                 → stock.py    (Step 6: bom_in项)
    │
    ├── 按 parent 聚合 → bom_out_qty/amt → stock.py (Step 6: bom_out项)
    │
    └── 原始明细 → bom_breakdown.py (Step 12: AI溯源表)
```

### 库存方程分支逻辑（完整决策树）

```
输入: eq_end_qty, day_clear, know_lost_qty, is_counted, actual_stock_qty

                    ┌─────────────────┐
                    │ is_counted ?    │  ← created_by != '系统' (人工盘点)
                    └────────┬────────┘
                             │ YES
                    ┌────────▼────────┐
                    │ end = actual    │
                    │ unknow =        │
                    │   max(0,eq-act) │
                    └─────────────────┘

                             │ NO
                    ┌────────▼────────┐
                    │ day_clear='0' ? │
                    └────────┬────────┘
                             │ YES
                    ┌────────▼────────┐
                    │ 软日清:         │  ← 清新供给, init可部分消耗
                    │ end=max(0,init  │
                    │  -已消耗部分)    │
                    │ unknow=max(0,   │
                    │  新供给-sale-kl)│
                    └─────────────────┘

                             │ NO
                    ┌────────▼────────┐
                    │   eq < 0 ?      │
                    └────────┬────────┘
                             │ YES
                    ┌────────▼────────┐
                    │ end = 0         │  ← 负库存保护
                    │ unknow = -eq    │
                    └─────────────────┘

                             │ NO
                    ┌────────▼────────┐
                    │know_lost_qty>0? │
                    └────────┬────────┘
                             │ YES
                    ┌────────▼────────┐
                    │ end = eq        │  ← 已知损耗日:
                    │ unknow = 0      │     信任盘点结果
                    └─────────────────┘

                             │ NO
                    ┌────────▼────────┐
                    │ end = eq        │  ← 正常
                    │ unknow = 0      │
                    └─────────────────┘
```

**金额统一规则**：
- receive_amt = 源表值 (receive_sale_di.inbound_amount)，不用 euc
- compose_amt = 加工关系配方推算（v0.10 不再使用 avg_inbound_price）
- know_lost_amt / unknow_lost_amt / end_stock_amt = qty × euc

### 跨日链式依赖

```
Day T-1:
  stock.py → end_stock_qty, end_stock_amt → 写入 t_calc_stock

Day T:
  sku_cost.py:
    读 t_calc_stock (Day T-1) → shift(+1天) → init_stock
    cost_amt += init_stock_amt
    cost_qty += init_stock_qty
    → 算出 effective_unit_cost

  stock.py:
    用 effective_unit_cost 算今天 end_stock
    → 写入 t_calc_stock (Day T)

Day T+1:
  sku_cost.py: 读 t_calc_stock (Day T) → ... (链式延续)
```

首日无昨天数据时，init_stock 直接从 `atomic_inventory` 源表值取。

---

## 6. 13步流程详解

### Step 1: 维度表快照 (DimsExtractor)

从 QDM 拉取 7 张维度表，写入 DuckDB。

| DuckDB表 | QDM源表 | 提取模式 | 行数(典型) |
|----------|---------|---------|-----------|
| `dim_goods` | `strategy_fm_dim_goods` | replace | ~86K |
| `dim_store_list` | `ads_business_analysis.chdj_store_info` | replace | ~8 |
| `dim_day_clear` | `strategy_fm_dim_day_clear` | replace_partition | ~92K |
| `dim_store_profile` | `strategy_fm_dim_store_profile` | replace | ~2 |
| `dim_calendar` | `strategy_fm_dim_calendar` | replace | 按日期范围 |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` | replace | ~8 |
| `dim_saleable` | `strategy_fm_dim_saleable` | replace | ~1.4K |

### Step 2: 原子域提取 (14个 Extractor)

从 QDM 拉取 14 张业务明细表，分区写入 DuckDB。

**粒度**: `store_id × business_date × article_id × day_clear`
**品类过滤**: `category_level1_id NOT IN ('70','71','72','73','74','75','76','77')` 排除物料类

| # | DuckDB表 | QDM源表 | 域 | 关键输出列 |
|---|----------|---------|---|-----------|
| 2.1 | `atomic_sales` | `strategy_fm_sales_di` | ①销售 | sale_qty, sale_amt, bf19_sale_qty, sales_weight |
| 2.2 | `atomic_inventory` | `strategy_fm_purchase_di` | ②库存 | init_stock_qty/amt, avg_inbound_price, purchase_receive |
| 2.3 | `atomic_inventory_detail` | `strategy_fm_store_article_inventory_detail_di` | ②附 盘点 | actual_stock_qty, created_by (人工/系统) |
| 2.4 | `atomic_scm` | `strategy_fm_scm_di` | ③供应链 | original_outstock_qty, outstock_unit_price |
| 2.5 | `atomic_scm_adjust` | `strategy_fm_scm_adjust_di` | ③附 | adjustment_amt |
| 2.6 | `atomic_loss` | `strategy_fm_loss_di` | ④损耗 | know_lost_qty, unknow_lost_qty_src |
| 2.7 | `atomic_compose` | `strategy_fm_compose_di` | ⑤加工 | compose_in_qty/amt, compose_out_qty/amt |
| 2.8 | `atomic_allowance` | `strategy_fm_allowance_di` | ⑥补贴 | allowance_amt |
| 2.9 | `atomic_promo` | `strategy_fm_promo_di` | ⑦促销 | member_coupon_shop_amt, shop_promo_amt |
| 2.10 | `atomic_cost_price` | `strategy_fm_inventory_pool_di` | ⑧成本价 | cost_price (观测用，不参与euc计算) |
| 2.11 | `atomic_price` | `strategy_fm_price_da` | ⑨价格 | current_price, original_price |
| 2.12 | `atomic_bom_relation` | `strategy_dim_store_article_bom_relation` | BOM关系 | parent_article_id, sub_article_id, dressing_rate |
| 2.13 | `atomic_receive_sale` | `strategy_fm_receive_sale_di` | ⑪进货销售 | article_id, sale_article_id, inbound_qty, inbound_amount |
| 2.14 | `atomic_order_receive` | (空骨架) | — | — |
| 2.15 | `atomic_article_convert` | (空骨架) | — | — |

### Step 3: 原子宽表合并 (AtomicMerger → t_atomic_wide, 82字段)

将 12 张活跃原子表合并为一张宽表。

**合并方式**:
- **基表**: `atomic_sales` FULL OUTER JOIN `atomic_inventory`
- **自购+父品进货提取**: 从 `atomic_receive_sale` 提取两路进货:
  - 自购行 (`article_id = sale_article_id`) → 聚合 SUM
  - BOM父品行 (`article_id != sale_article_id`) → 去重取 MAX (同一父品多行)
- **父品补全**: INSERT 不在 sales/inventory 中的 BOM 父品 (带 self_receive > 0)
- **维度JOIN**: LEFT JOIN dim_day_clear 获取 day_clear 标签
- **门店过滤**: INNER JOIN dim_store_list
- **其他域**: LEFT JOIN 其余 10 张原子表

### Steps 4-7: 计算层 — 详见[第 5 节完整计算逻辑流程](#5-完整计算逻辑流程)

### Steps 8-13: FM 底表产出

| Step | 模块 | 产出 | 说明 |
|------|------|------|------|
| 8 | SkuDimBuilder | `t_fm_sku_dim` | Python类别重映射，合并所有域+维度，80字段 |
| 9 | CustBuilder | `t_fm_cust` | 客数聚合，Python端JOIN dim_goods过滤品类70-77 |
| 10 | LevelsSumBuilder | `t_fm_levels_sum` | 7级分类汇总 (门店/大类/中类/小类/SPU/黑白猪/SKU) |
| 11 | LevelsResultBuilder | `t_fm_levels_result` | 中文列名 + 比率KPI (毛利率/损耗率/售罄率等) |
| 12 | BomBreakdownBuilder | `t_fm_bom_breakdown` | BOM分摊溯源 (parent×sub明细) |
| 13 | StockRollBuilder | `t_fm_stock_roll` | 库存八要素滚动 + balance_qty校验列 |

---

## 7. 源表映射

### QDM → DuckDB 原子表

| QDM 商分源表 | DuckDB 原子表 | 域 | 关键过滤 |
|-------------|--------------|---|---------|
| `strategy_fm_sales_di` | `atomic_sales` | ①销售 | inc_day BETWEEN, 排品类91, 在线下时排70-77 |
| `strategy_fm_purchase_di` | `atomic_inventory` | ②库存 | inc_day BETWEEN, LEFT JOIN dim_goods排70-77 |
| `strategy_fm_store_article_inventory_detail_di` | `atomic_inventory_detail` | ②附 盘点 | inventory_date BETWEEN |
| `strategy_fm_scm_di` | `atomic_scm` | ③供应链 | inc_day BETWEEN |
| `strategy_fm_scm_adjust_di` | `atomic_scm_adjust` | ③附 | inc_day BETWEEN |
| `strategy_fm_loss_di` | `atomic_loss` | ④损耗 | inc_day BETWEEN |
| `strategy_fm_compose_di` | `atomic_compose` | ⑤加工 | inc_day BETWEEN |
| `strategy_fm_allowance_di` | `atomic_allowance` | ⑥补贴 | inc_day BETWEEN |
| `strategy_fm_promo_di` | `atomic_promo` | ⑦促销 | inc_day BETWEEN, INNER JOIN dim_goods排70-77 |
| `strategy_fm_inventory_pool_di` | `atomic_cost_price` | ⑧成本价 | inventory_date BETWEEN |
| `strategy_fm_price_da` | `atomic_price` | ⑨价格 | inc_day BETWEEN |
| `strategy_dim_store_article_bom_relation` | `atomic_bom_relation` | BOM关系 | inc_day BETWEEN, bom_type IN(1,2,3) |
| `strategy_fm_receive_sale_di` | `atomic_receive_sale` | ⑪进货销售 | inc_day BETWEEN |

### QDM → DuckDB 维度表

| QDM 商分源表 | DuckDB 维度表 | 提取条件 |
|-------------|-------------|---------|
| `strategy_fm_dim_goods` | `dim_goods` | inc_day = yesterday, 排品类70-77 |
| `ads_business_analysis.chdj_store_info` | `dim_store_list` | 全量 |
| `strategy_fm_dim_day_clear` | `dim_day_clear` | business_date BETWEEN start AND end |
| `strategy_fm_dim_store_profile` | `dim_store_profile` | 全量 |
| `strategy_fm_dim_calendar` | `dim_calendar` | day_date BETWEEN start AND end |
| `strategy_fm_dim_saleable` | `dim_saleable` | 全量 |
| `ads_business_analysis.chdj_store_info` | `dim_chdj_store_info` | 全量 |

### 源表字段名映射

| QDM 源表 | QDM 字段 | DuckDB 字段 |
|----------|---------|------------|
| `strategy_fm_promo_di` | `shop_id` | `store_id` |
| `strategy_fm_promo_di` | `sku_code` | `article_id` |
| `strategy_fm_inventory_pool_di` | `shop_id` | `store_id` |
| `strategy_fm_inventory_pool_di` | `sku_code` | `article_id` |
| `strategy_fm_inventory_pool_di` | `inventory_date` | `business_date` |
| `strategy_fm_dim_store_profile` | `sp_store_id` | `store_id` |
| `strategy_fm_dim_calendar` | `day_date` | `business_date` |
| `strategy_fm_dim_saleable` | `shop_id` | `store_id` |
| `strategy_fm_dim_saleable` | `sku_code` | `article_id` |
| `strategy_fm_sales_di` | `abi_article_id` | `article_id` |

---

## 8. v0.10修复对照表

| # | 异常描述 | 影响 | 修复位置 | 修复方案 |
|---|---------|------|---------|---------|
| A1 | receive_qty/amt 从 purchase_di 取 | 进货额错误 | merge.py + inventory_extractor | 切为 receive_sale_di 自购行 (article_id = sale_article_id) |
| A2 | 负库存不转 unknow_lost | 库存方程不闭合 | stock.py | eq<0 → end=0, unknow=-eq (含盘点场景) |
| A3 | 库存方程缺少 BOM 项 | 方程不完整 | stock.py | 新增 +bom_in -bom_out |
| A4 | receive_amt 用 euc 计算 | 进货额不准确 | merge.py | 直接用 receive_sale_di 源表 inbound_amount |
| A5 | init_stock 每天重算 | 库存不连续 | sku_cost.py | 取昨天 t_calc_stock.end_stock |
| A6 | compose 两套定价 (cost_price vs avg_inbound_price) | 不一致 | sku_cost.py | 改用加工关系配方推算，不再用 cost_price 或 avg_inbound_price |
| A7 | 盘点分支用 know_lost_amt > 0 | 零损耗时误判为盘点 | stock.py | 改用 know_lost_qty > 0 |
| A8 | 首日 init_amt = qty × price | 金额误差 | sku_cost.py | 直接用源表 init_stock_amt_src |
| A9 | prev_end JOIN 限制 day_clear | 跨日匹配遗漏 | sku_cost.py | Python merge 不过滤 day_clear |
| A10 | 毛利公式缺 BOM | 毛利偏差 | profit.py | 新公式含 bom_in/bom_out |
| A11 | profit_amt 双口径冗余 | 数据冗余 | profit.py + levels_sum | 只保留一个 profit_amt |
| A12 | 日清 sale_cost 缺 BOM | 销售成本偏差 | profit.py | 新公式含 BOM 项 |
| A13 | 共享组 parent qty 只取第一个 parent | 分摊基数小 | bom_alloc.py | 组总 qty = 所有 parent qty 之和 |
| A14 | split_need_qty 出现负值 | 子品负采购 | bom_alloc.py | max(0, consume_qty - self_inbound_qty) |
| A15 | inbound 来源不一致 | 不同底表同一字段值不同 | sku_dim.py | 统一从 t_calc_stock 取 |
| A16 | cust 未过滤品类 70-77 | 物料类混入客数 | cust.py | Python JOIN dim_goods 过滤 |
| A17 | BOM父品(大白猪/黑猪)进货条码无进货额 | 父品 receive=0, 库存方程失衡 | merge.py | self_receive 扩展为自购+父品两路; 父品补入 t_atomic_wide |
| A18 | 共享组 bom_out 全归第一个父品 | 大白猪A级 bom_out超分 | bom_alloc.py | 共享子品按进货额比例分拆为两行, 各父品仅承担自身份额 |
| A19 | bom_out_qty 子品单位 ≠ receive 父品单位 | 海大虾 1箱 receive vs 10kg bom_out | bom_alloc.py | 单位归一化: qty × (parent_qty / sum_sub_qty) |

---

## 9. day_clear 字段说明

`day_clear` 是库存计算的核心分类标签，标识商品是否可以跨日留存。

| 值 | 含义 | 典型商品 | 库存规则 |
|----|------|---------|---------|
| `'0'` | **日清** — 当日必须清空 | 生鲜、熟食 | end=0, 残差全部转 unknow_lost |
| `'1'` | **非日清** — 可跨日留存 | 标品、包装食品 | 正常库存方程, end 可非零 |
| `'2'` | **合计** — '0' + '1' 汇总 | — | ETL 自动 UNION 生成，不是源表值 |

**注意**: 查询"全天合计"必须用 `WHERE day_clear = '2'`。如果 `GROUP BY day_clear` 再 `SUM`，会天然翻倍（合计已含日清+非日清），这是设计行为而非 bug。

---

## 10. 验证方案

### 快速验证

```bash
# 1. 全管道执行
python -m fmetl.executor 2026-04-23 2026-04-23

# 2. 检查负库存（应为 0）
python3 -c "
import duckdb
conn = duckdb.connect('data/fm.duckdb', read_only=True)
print(conn.execute('SELECT COUNT(*) FROM t_calc_stock WHERE end_stock_qty < 0').fetchone())
"

# 3. 检查 BOM 对称（应相等）
python3 -c "
import duckdb
conn = duckdb.connect('data/fm.duckdb', read_only=True)
r = conn.execute('SELECT SUM(bom_in_amt)-SUM(bom_out_amt) FROM t_calc_stock').fetchone()
print(f'BOM diff: {r[0]:.2f}')
"

# 4. 检查 euc 有效性
python3 -c "
import duckdb
conn = duckdb.connect('data/fm.duckdb', read_only=True)
print(conn.execute('SELECT COUNT(*) FROM t_calc_sku_cost WHERE effective_unit_cost <= 0').fetchone())
"
```

### 与 QDM 对比

选取 3 个 SKU (20003470, 20015855, 21292699) 对比 v0.10 输出 vs QDM `strategy_fm_levels_result`:

| 指标 | 期望差异 | 原因 |
|------|---------|------|
| 销售额 (sale_amt) | 完全一致 | 源表相同 |
| 进货额 (inbound_amount) | 完全一致 | 源表相同 |
| 期初库存额 | 完全一致 | 首日源表相同 |
| 期末库存额 | < 0.5% | 浮点精度差异 |
| 门店毛利额 | 有偏差 | v0.10 新公式含 BOM (A10/A12 修复) |

### 2026-04-23 验证结果

- ✅ 13步全通 (80.9s)
- ✅ neg_end_stock = 0 (A2修复)
- ✅ 黑猪A级: receive=23.76/508.70, bom_out=20.65/508.70, end=3.11
- ✅ 大白猪A级: receive=57.40/851.82, 共享组21153037分获bom_out=2.79/90.13 (A18)
- ✅ 海大虾: bom_out=0.84箱 (归一化, A19), end=0.16箱
- ✅ BOM 对称: Σbom_in = Σbom_out
- ✅ 核心金额 (sale/inbound/init/end) 匹配 QDM，差异 < 0.3%
- ⚠️ 猪肉类毛利 v4=431 vs QDM=545 (差距-114, 来自 euc=0 的BOM加工品和共享组权重计算差异)
- ⚠️ 毛利偏离 QDM — 预期内 (v0.10 纳入 BOM 毛利, A10/A12/A17-A19)

---

## 11. 环境配置

### .env 文件

| 变量 | 必填 | 说明 | 示例 |
|------|------|------|------|
| `QDM_ACCESS_KEY` | ✅ | QDM BI API 访问密钥 | — |
| `QDM_SECRET_KEY` | ✅ | QDM BI API 秘密密钥 | — |
| `FM_DUCKDB_PATH` | ❌ | DuckDB 数据文件路径 | `data/fm.duckdb` |

### Python 版本

- Python 3.9+
- duckdb >= 0.9.0
- pandas >= 1.3.0
- numpy >= 1.21.0
