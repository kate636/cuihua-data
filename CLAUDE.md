# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**fmetl** — 翠花当家 (Cuihua Dangjia) 零售数据分析 ETL 管道。当前活跃版本为 **v0.10**，从 QDM BI API 提取数据，经 Python 计算处理后在 DuckDB 中产出 FM 底表。

## Project Skills

项目专属 skill 位于 `.cursor/skills/`，覆盖常用操作：

| Skill | 用途 |
|-------|------|
| `master-data` | **品类映射唯一权威来源**，所有涉及分类的改动必读 |
| `qdm-compare` | FM ETL vs QDM 基准表全指标对比验证 |
| `etl-check` | ETL 健康检查（负库存/BOM/损耗/零成本/盘盈） |
| `server-query` | 查询服务器 DuckDB 数据 |
| `fm-data-query` | Food Mart 取数查询 |
| `fm-platform` | FM 平台相关操作 |
| `qdm-bi-api` | BI 平台 API 调用 |
| `monthly-report` | 月度经营日报 |
| `xlsx` | Excel 文件处理 |
| `webapp-testing` | Web 应用测试 |
| `skill-creator` | 创建新 skill |

### 品类分类规则

**以 `.cursor/skills/master-data/SKILL.md` 为唯一权威来源。** ETL `sku_dim.py` 中的分类重映射是旧版（v1），仅用于 FM 底表的历史兼容。做品类分析、对比验证、报表时，一律用 master-data 的中分类映射规则。

## Key Commands

所有命令在仓库根目录 (`翠花数据/`) 下执行。

### 环境初始化
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb pandas numpy python-dotenv requests
cp fmetl/fm_etl_v3/.env.example .env   # 然后填入 QDM_ACCESS_KEY / QDM_SECRET_KEY
```

### Run ETL Pipeline
```bash
# 单日执行
python -m fmetl.executor 2026-04-23 2026-04-23

# 日期范围
python -m fmetl.executor 2026-04-01 2026-04-30

# 分阶段执行（调试用）
python -m fmetl.executor 2026-04-23 2026-04-23 --atomic-only  # 提取+合并
python -m fmetl.executor 2026-04-23 2026-04-23 --calc-only    # 仅计算层
python -m fmetl.executor 2026-04-23 2026-04-23 --fm-only      # 仅FM底表
```

### 验证
```bash
# 检查负库存（应为 0）
python3 -c "
import duckdb
conn = duckdb.connect('data/fm.duckdb', read_only=True)
print(conn.execute('SELECT COUNT(*) FROM t_calc_stock WHERE end_stock_qty < 0').fetchone())
"

# 检查 BOM 对称（bom_in ≈ bom_out）
python3 -c "
import duckdb
conn = duckdb.connect('data/fm.duckdb', read_only=True)
r = conn.execute('SELECT SUM(bom_in_amt)-SUM(bom_out_amt) FROM t_calc_stock').fetchone()
print(f'BOM diff: {r[0]:.2f}')
"

# 查询结果
python3 -c "
import duckdb
conn = duckdb.connect('data/fm.duckdb', read_only=True)
print(conn.execute(\"SELECT * FROM t_fm_levels_result WHERE 分类等级 = 'SKU' LIMIT 5\").df())
"
```

## Multi-Agent Workflow

本项目的改动遵循 **设计 → 编码 → 审查** 三段式流程，每阶段由一个独立 agent 负责。用户可单独调用任一 agent，也可串联执行。

### 项目目标
- **目的**: 用 fmetl 替换现有 QDM ETL 链路
- **验收标准**: 门店 + 大分类维度的门店毛利额与 QDM 数据基本一致（SKU 级允许差异，因计算方式不同）
- **新 ETL 核心价值**: BOM 正确性、重刷数据便捷性、指标灵活性
- **QDM 基准表**: `default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di`

### Agent 1: 设计 (Design Architect)

**职责**: 分析需求，设计计算逻辑与工程框架。**不写代码**。

**输入**: 用户需求描述、现有代码、QDM 基准表数据、已知问题清单

**输出**:
- 公式推导（从业务语义到数学表达式，注明每一步的假设和边界）
- 模块/文件拆分方案（新增/修改哪些文件，数据流如何连接）
- 对下游模块的影响评估（尤其是 stock → sku_cost 跨日依赖链）
- 验收标准（哪些指标在什么粒度上应与 QDM 一致，允许差异范围）

**约束**:
- 遵循现有设计原则（简单SQL复杂Python、每个数字只算一次、四流分离）
- BOM 分摊必须尊重共享组语义和单位归一化规则
- 存量模块修改时必须说明对跨日滚动的影响

**触发方式**: `用设计agent分析XX` / `先出方案再写代码`

### Agent 2: 编码 (Code Implementer)

**职责**: 严格按设计文档实现代码。**不自行修改设计**。

**输入**: Agent 1 的设计文档（或用户直接给出的明确技术方案）

**输出**:
- 修改/新增的代码文件
- 如有设计疑点，标注 `# TODO-DESIGN: <问题描述>` 回抛给设计 agent

**约束**:
- 新 extractor 继承 `BaseExtractor`，只实现 `_fetch_sql()`
- 新 calculator 遵循 Calculator 模式（DuckDB→DataFrame→计算→写回）
- SQL 仅做 SELECT/JOIN/GROUP BY/WHERE，计算逻辑在 Python
- 不引入新依赖，不改变公共接口签名

**触发方式**: `按方案实现` / `写代码agent`

### Agent 3: 审查与校对 (Data Reviewer)

**职责**: 核对代码是否与设计一致，运行数据验证，对比 QDM 基准。

**输入**: Agent 2 的代码变更 + Agent 1 的设计文档

**审查清单**:

**A. 代码与设计一致性**
- [ ] 实现是否完整覆盖设计文档中的所有公式和分支
- [ ] BOM 字段引用是否正确（`bom_alloc_qty` vs `bom_alloc_qty_sub` 不可混用）
- [ ] 日清覆盖逻辑是否正确应用
- [ ] 是否有断裂的跨日依赖链

**B. 内部一致性校验**（跑 DuckDB）
- [ ] 负库存检查: `SELECT COUNT(*) FROM t_calc_stock WHERE end_stock_qty < 0`
- [ ] BOM 对称: `SUM(bom_in_amt) - SUM(bom_out_amt)` 全局 ≈ 0
- [ ] 库存方程平衡: `init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost - end - unknow` = 0
- [ ] 毛利自洽: 毛利公式各分量符号正确，全链路 profit 与拆解加总一致
- [ ] 父品 profit = 0（BOM 父品不应产生独立毛利）

**C. QDM 对比校验**（核心验收）
- [ ] 以 `dal_transaction_chdj_store_sale_article_sale_info_di` 为基准
- [ ] **门店 × 大分类** 粒度: 毛利额偏差在可接受范围（默认 ±5%，具体以设计文档为准）
- [ ] **门店 × 日期** 粒度: 总毛利额偏差
- [ ] 差异较大的门店/分类需要标注并分析原因
- [ ] SKU 粒度差异: 记录但**不阻塞**（因计算方式不同允许差异）

**对比 SQL 模板**:
```sql
-- 门店 × 大分类 毛利额对比
WITH fmetl AS (
  SELECT store_id, category_level1_id, SUM(profit_amt) as fmetl_profit
  FROM t_calc_profit GROUP BY store_id, category_level1_id
),
qdm AS (
  SELECT shop_id as store_id, category_level1_id,
         SUM(<毛利字段>) as qdm_profit
  FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
  WHERE business_date = '<日期>'
  GROUP BY shop_id, category_level1_id
)
SELECT f.store_id, f.category_level1_id,
       f.fmetl_profit, q.qdm_profit,
       f.fmetl_profit - q.qdm_profit as diff,
       CASE WHEN q.qdm_profit != 0 THEN (f.fmetl_profit - q.qdm_profit)/ABS(q.qdm_profit) END as diff_pct
FROM fmetl f
FULL OUTER JOIN qdm q ON f.store_id = q.store_id AND f.category_level1_id = q.category_level1_id
WHERE ABS(f.fmetl_profit - q.qdm_profit) / NULLIF(ABS(q.qdm_profit), 0) > 0.05
ORDER BY ABS(diff) DESC
```

**输出**:
- 审查报告（通过/阻塞/需澄清）
- 差异明细表（门店、分类、差异额、差异率、原因分析）
- 对设计或编码的回退建议

**触发方式**: `审查代码` / `校对数据` / `验证agent`

---

## Architecture: 3-Layer ETL (v0.10)

### 设计原则
- **简单SQL，复杂逻辑Python**: SQL 仅做 SELECT/JOIN/GROUP BY/WHERE。计算、分支、窗口函数在 Python（pandas + NumPy）完成
- **每个数字只算一次**: 上游结果直接复用，下游不重复计算
- **四流分离**: receive / bom_in / bom_out / compose_in/out 各自独立列
- **跨日链式传递**: 今日期初库存 = 昨日期末库存

```
┌──────────────────────────────────────────────────────────────────┐
│ Layer -2: atomic_* (原子层) — 不可分解的独立观测量              │
│   13张活跃 + 2张骨架表，从 strategy_fm_* 源表 API 拉取          │
│   粒度: store_id × business_date × article_id × day_clear       │
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
│   t_fm_sku_dim       → SKU级完整宽表 (~60字段)                  │
│   t_fm_cust          → 客数聚合                                  │
│   t_fm_levels_sum    → 7级分类汇总 (数量/金额)                   │
│   t_fm_levels_result → 平台对接表 (中文列名 + 比率KPI)           │
│   t_fm_bom_breakdown → BOM分摊溯源 (parent×sub 明细)            │
│   t_fm_stock_roll    → 库存八要素滚动 (含 balance_qty 校验)     │
└──────────────────────────────────────────────────────────────────┘
```

### 与 v9 的核心变化
- 删除 3 个冗余计算模块: `inventory.py`, `avg_price.py`, `amounts.py`（功能合并到 `stock.py`）
- 所有复杂计算从 SQL 移到 Python（pandas + NumPy）
- 新增 BOM 父品补全、负库存保护、共享组父品分拆、单位归一化
- 毛利公式纳入 BOM 流入/流出
- 新增 `atomic_inventory_detail` 源表（人工盘点判定）

## Pipeline 13 Steps

| Step | Module | Output |
|---|---|---|
| 1 | `DimsExtractor` | 7张 `dim_*` 全量替换 |
| 2 | 15个 `*Extractor` | 13张 `atomic_*` 分区追加 + 2张空骨架 |
| 3 | `AtomicMerger` | `t_atomic_wide` (82字段, 含父品补全 + 日清覆盖) |
| 4 | `BomAllocCalculator` | `t_calc_bom_alloc` (Σ总权重+共享组分拆+单位归一化) |
| 5 | `SkuCostCalculator` | `t_calc_sku_cost` (加权平均含期初库存) |
| 6 | `StockCalculator` | `t_calc_stock` (四流合一+跨日滚动, 中枢模块) |
| 7 | `ProfitCalculator` | `t_calc_profit` (含BOM+SCM+全链路) |
| 8 | `SkuDimBuilder` | `t_fm_sku_dim` |
| 9 | `CustBuilder` | `t_fm_cust` (Python端JOIN dim_goods过滤品类70-77) |
| 10 | `LevelsSumBuilder` | `t_fm_levels_sum` |
| 11 | `LevelsResultBuilder` | `t_fm_levels_result` |
| 12 | `BomBreakdownBuilder` | `t_fm_bom_breakdown` |
| 13 | `StockRollBuilder` | `t_fm_stock_roll` |

### 步骤间依赖
```
Step 1 (dims) ─────────────────────────────────────────────────┐
Step 2 (atomic) ──→ Step 3 (merge) ──→ Step 4 (bom_alloc) ──┐ │
                                           │                  │ │
                                           ↓                  │ │
                                      Step 5 (sku_cost)       │ │
                                       ↖ 读昨天 t_calc_stock   │ │
                                           │                  │ │
                                           ↓                  │ │
                                      Step 6 (stock) ──→ 明天Step5
                                           │                  │ │
                                           ↓                  │ │
                                      Step 7 (profit)         │ │
                                           │                  │ │
                                           ↓                  ▼ ▼
                                      Step 8-13 (fm_tables, 6张底表)

Step 4 (bom_alloc) ────────────────────→ Step 12 (bom_breakdown)
Step 6 (stock) ────────────────────────→ Step 13 (stock_roll)
```

## Core Business Logic

### effective_unit_cost (euc): 加权平均含期初库存

`cost_price` 不参与计算。唯一权威成本来源为 `t_calc_sku_cost`。

```
cost_amt = init_stock_amt + self_receive_amt + compose_net_amt + bom_alloc_amt
cost_qty = init_stock_qty + self_receive_qty + compose_net_qty + bom_alloc_qty_sub  ← 注意: 子品单位!
euc = cost_amt / cost_qty  (cost_qty > 0 时)

cost_source = 'V10_WEIGHTED_AVG'
```

**关键**: `t_calc_bom_alloc` 有两套 qty 字段:
- `bom_alloc_qty` — 父品单位（归一化，用于父品 stock 方程 bom_out）
- `bom_alloc_qty_sub` — 子品单位（原始，用于子品 euc 计算 bom_in）

混用会导致 euc 异常（如海大虾 200.84 → 54.47）。首日 init_stock 直接用源表值，次日开始用昨天 `t_calc_stock.end_stock`。

### 库存方程 (stock.py — 四流合一)

```
eq = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost

分支 (优先级从高到低):
  1. is_counted (人工盘点, created_by != '系统')
     → end=actual_stock_qty, unknow=max(0, eq-actual)
  2. day_clear='0' (软日清)
     → 新供给 = receive + bom_in - bom_out + compose_in - compose_out
     → end = max(0, init - max(0, (sale+klost) - 新供给))
     → unknow = max(0, 新供给 - sale - klost)
  3. eq < 0 (负库存保护)
     → end=0, unknow=-eq
  4. know_lost_qty > 0 (有已知损耗)
     → end=eq, unknow=0
  5. 其他 (正常)
     → end=eq, unknow=0
```

金额统一用 euc 计算: `end_stock_amt = end_qty × euc`, `unknow_lost_amt = unknow_qty × euc`, `know_lost_amt = know_qty × euc`

### 门店毛利 (profit.py — 含BOM)

```
profit = sale - receive - bom_in + bom_out - compose_in + compose_out
       + end_stock - init_stock

注: 损耗已通过库存方程反映在 end_stock 中，不再额外扣减 lost_amt（A20）。
```

### 销售成本
`sale_cost_amt = sale_qty × euc`（日清/非日清统一公式，差异在 stock.py 端体现）

### BOM 分摊 — 共享组与父品归集

**共享组**：当 parent_B.subs ⊆ parent_A.subs 时，合并为共享组。分配逻辑：
- **所有子品**（含独有）用组总额 `group_total_amt` 做基数分配，保证子品成本公允
- 共享子品按进货额比例（如 76%/24%）分拆到两个父品

**父品 bom_out 归集**：在 bom_alloc.py 中按 `alloc_ratio × parent_inbound_amount` 分配，各父品仅承担自身份额。

### 库存转移（stock.py）

BOM 父品剩余库存（sale=0, bom_out>0, end>0）通过 `stock_transfer_out` 按 bom_alloc 比例转移给子品 `stock_transfer_in`，确保父品 end=0, profit=0。转移直接修改 end_stock，不在毛利公式中额外加减。

### day_clear 字段语义
| 值 | 含义 | 库存规则 |
|---|---|---|
| `'0'` | 日清 (生鲜) | 软日清: 只清新供给, init 存量可部分消耗 |
| `'1'` | 非日清 (标品) | 正常库存方程 |
| `'2'` | 合计 (ETL UNION生成) | 查询全天合计必须 `WHERE day_clear='2'` |

**日清覆盖**（merge.py 强制设置 day_clear='0'，通过 `dim_day_clear_override` 辅助表）:
- 猪肉类 (`category_level1_description = '猪肉类'`)
- 熟食类 (`category_level3_description LIKE '%熟食'`)
- 烘焙类 (24个现烤面包/点心 SKU，详见 Deployment > 日清覆盖规则)
- 鲜牛肉**已移除**（purchase_di 的 init_stock ≠ 0，日清会导致巨额虚假亏损）

## Data Source Tables

### 原子域表 (13张活跃 + 2张骨架)
| DuckDB 表 | QDM 商分表 | 域 |
|---|---|---|
| `atomic_sales` | `strategy_fm_sales_di` | ①销售 |
| `atomic_inventory` | `strategy_fm_purchase_di` | ②库存 |
| `atomic_inventory_detail` | `strategy_fm_store_article_inventory_detail_di` | ②附 盘点 |
| `atomic_scm` | `strategy_fm_scm_di` | ③供应链 |
| `atomic_scm_adjust` | `strategy_fm_scm_adjust_di` | ③附 |
| `atomic_loss` | `strategy_fm_loss_di` | ④损耗 |
| `atomic_compose` | `strategy_fm_compose_di` | ⑤加工 |
| `atomic_allowance` | `strategy_fm_allowance_di` | ⑥补贴 |
| `atomic_promo` | `strategy_fm_promo_di` | ⑦促销 |
| `atomic_cost_price` | `strategy_fm_inventory_pool_di` | ⑧成本价(观测) |
| `atomic_price` | `strategy_fm_price_da` | ⑨价格 |
| `atomic_bom_relation` | `strategy_dim_store_article_bom_relation` | BOM关系(观测) |
| `atomic_receive_sale` | `strategy_fm_receive_sale_di` | BOM核心源 |
| `atomic_order_receive` | (空骨架) | — |
| `atomic_article_convert` | (空骨架) | — |

### 维度表 (7张)
| DuckDB 表 | QDM 源表 |
|---|---|
| `dim_goods` | `strategy_fm_dim_goods` | ⚠️ 见下方 |
| `dim_store_list` | `ads_business_analysis.chdj_store_info` |
| `dim_day_clear` | `strategy_fm_dim_day_clear` |
| `dim_store_profile` | `strategy_fm_dim_store_profile` |
| `dim_calendar` | `strategy_fm_dim_calendar` |
| `dim_saleable` | `strategy_fm_dim_saleable` |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` |

### dim_goods 注意事项

`strategy_fm_dim_goods` 在 StarRocks 中**只存最新一天数据**（从 `hive.dim.dim_goods_information_have_pt` 每日全量覆盖）。补数据 SQL：
```sql
DELETE FROM default_catalog.ads_business_analysis.strategy_fm_dim_goods;
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_goods
SELECT * FROM hive.dim.dim_goods_information_have_pt
WHERE inc_day = '${date}';
```

DuckDB 端 `dim_goods` 每次从最新 `inc_day` 全量替换，**表中没有 inc_day 列**。所有历史日期的数据在 FM 底表层（sku_dim.py）统一 JOIN 最新 dim_goods，下游只需 `ON article_id`。

### 计算层表 (5张)
| DuckDB 表 | 算法 |
|---|---|
| `t_atomic_wide` | FULL OUTER JOIN + 父品补全 + 日清覆盖 (82字段) |
| `t_calc_bom_alloc` | Σ总权重 + Python共享组识别 + 单位归一化 |
| `t_calc_sku_cost` | 加权平均含期初库存 |
| `t_calc_stock` | 四流合一 + 跨日滚动 (中枢, 合并了inventory/avg_price/amounts) |
| `t_calc_profit` | 含BOM + SCM金融 + 全链路毛利 |

### FM 底表 (6张)
| DuckDB 表 | 粒度 | 说明 |
|---|---|---|
| `t_fm_sku_dim` | store×date×article_id×day_clear | SKU级完整宽表，含分类重映射、7日均量 |
| `t_fm_cust` | store×date×day_clear×level | 客数聚合（6层级） |
| `t_fm_levels_sum` | store×date×day_clear×level | 7级分类汇总 |
| `t_fm_levels_result` | store×date×day_clear×level | 平台对接表（中文列名+KPI） |
| `t_fm_bom_breakdown` | store×date×parent×sub | BOM分摊溯源 |
| `t_fm_stock_roll` | store×date×article_id×day_clear | 库存八要素滚动 + balance_qty |

## Field Mapping Notes

| 源表 | 源字段 | 目标字段 |
|---|---|---|
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
| `strategy_fm_store_article_inventory_detail_di` | `shop_id` | `store_id` |
| `strategy_fm_store_article_inventory_detail_di` | `sku_code` | `article_id` |
| `strategy_fm_store_article_inventory_detail_di` | `inventory_date` | `business_date` |

## Code Patterns

### API 列名自动归一化
`ApiConnector.query()` 自动将 QDM API 返回的 `camelCase` 列名转回 `snake_case`（如 `businessDate` → `business_date`）。

### WAF 规避: CASE WHEN → IF()
API SQL 中所有 `CASE WHEN x THEN y ELSE z END` 必须写成 `IF(x, y, z)`。DuckDB 内部 SQL 和 Python 代码无此限制。

### dim_goods 注意事项

`dim_goods` 是**日快照表**（API 侧已按 `inc_day = '{end}'` 过滤），DuckDB 中**没有 inc_day 列**。下游 JOIN dim_goods 时只需 `ON article_id`，不需要额外日期过滤。

### QDM 对比表

对比 v4 输出时使用 QDM 表 `default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di`（不是 `strategy_fm_levels_result`）。该表字段与 v4 一一对应，可做 SKU 级逐字段对比。

### 门店过滤 & 品类过滤
所有原子表 INNER JOIN `dim_store_list`（翠花门店白名单）。品类过滤: `category_level1_id NOT IN ('70','71','72','73','74','75','76','77')` 排除物料类。

### BaseExtractor 子类只需实现 SQL
```python
class MyExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_my_table"
    def _fetch_sql(self, start, end, yesterday):
        return f"SELECT ... FROM strategy_fm_my_di WHERE inc_day BETWEEN '{start}' AND '{end}'"
```

### Calculator 模式 (Python 计算)
v0.10 中所有复杂计算在 Python 完成，SQL 仅做数据拉取和 JOIN。Calculator 读取 DuckDB 数据为 DataFrame → pandas/NumPy 计算 → 写回 DuckDB。

### 日期分片 & 写入模式
`BaseExtractor.extract()` 按 7 天 chunk 拆分，逐片 API 请求后用 `replace_partition` 模式写入（先删后插，幂等）。`DuckDBStore.load_df()` 支持 `replace_partition`（默认）、`replace`、`append` 三种模式。

## Project Structure

```
翠花数据/
├── fmetl/                    # 主 ETL Pipeline v0.10
│   ├── executor.py          # 主入口 (13步)
│   ├── config/              # API凭证配置
│   ├── connectors/          # ApiConnector + DuckDBStore
│   ├── atomic/              # Step 1-2: 原子域提取 (16个extractor文件)
│   ├── calculated/          # Step 3-7: 计算层 (5个模块, Python核心)
│   ├── fm_tables/           # Step 8-13: FM底表 (6张)
│   ├── utils/               # 日志/日期/重试工具
│   ├── docs/                # 架构/参考/审查/修复 (4个子目录)
│   └── fm_etl_v3/           # 旧版 v3.0 (历史版本, 保留参考)
│
├── _archived/               # 历史归档 (deploy/tests/docs/旧版脚本等)
├── legacy_scripts/          # 旧版独立脚本 (参考/备份)
├── data/                    # DuckDB数据文件 (不上GitHub)
└── .claude/                 # Claude Code 配置
```

## Deployment Architecture

### 系统架构

```
┌─── 本地 Mac (开发) ────────────┐
│ Cursor 改代码                  │
│   │                            │
│   │ git push                   │
│   ▼                            │
└─── GitHub 私有仓库 kate636/cuihua-data ┘
            │
            │  每日 08:50 git pull --ff-only
            ▼
┌─── 阿里云 ECS 47.115.213.115 (广州) ─────────────────────┐
│                                                         │
│  /opt/fm/etl/cuihua-data/    代码（git clone）          │
│  /opt/fm/data/fm.duckdb      唯一数据文件（不上 GitHub）│
│  /opt/fm/logs/               ETL 日志                   │
│  /opt/fm/etl/daily_run.sh    ETL 入口脚本               │
│  /opt/fm/proc-rel/           加工关系管理 API            │
│  /opt/fm/reports/            前端页面（nginx :8080）     │
│                                                         │
│   cron 08:50                                            │
│     ↓                                                   │
│   daily_run.sh                                          │
│     ├─ git pull --ff-only (从 GitHub 拉最新代码)        │
│     └─ python -m fmetl.executor 昨天 昨天               │
│                                                         │
│   前置依赖: 源表同步 (sync_strategy_fm.sh, 公司IDE手动) │
│   加工关系: /opt/fm/proc-rel/cloud_api.py (systemd)     │
└─────────────────────────────────────────────────────────┘
```

### 关键路径

| 路径 | 用途 |
|------|------|
| `/opt/fm/etl/cuihua-data/` | git clone 代码 |
| `/opt/fm/data/fm.duckdb` | 唯一数据文件（111MB+） |
| `/opt/fm/logs/` | ETL 日志 |
| `/opt/fm/etl/daily_run.sh` | 每日 ETL 入口 |
| `/opt/fm/proc-rel/` | 加工关系 API + SQLite DB |
| `/opt/fm/reports/` | 前端 HTML 页面 |

### 部署后注意事项

- **代码更新**: 本地 `git push` → 次日 08:50 服务器自动 `git pull --ff-only`
- **DuckDB 不要提交**: `data/` 目录在 .gitignore 中，fm.duckdb 仅存在于服务器
- **加工关系**: 远程服务器 `/opt/fm/proc-rel/processing_relation.db` 是权威数据源，
  本地 `data/processing_relations.json` 仅作缓存。API 通过 systemd service `proc-rel` 保活
- **源表同步**: `data/sync_strategy_fm.sh` 在 StarRocks 上执行（公司IDE手动），
  从 Hive 同步 21 张表到 `strategy_fm_*`，必须在 ETL 之前跑完
- **dim_goods 日期**: 仅存最新 inc_day 快照（INSERT OVERWRITE），
  ETL 会从 `end` 日期向前 7 天 + 向后 3 天扫描找到最新数据

### Cron 任务 (服务器)

```
50 8 * * * /bin/sh /opt/fm/etl/daily_run.sh    # ETL (git pull + 运行)
```

### Systemd 服务 (服务器)

```
proc-rel.service    # 加工关系管理 API (端口 5003)
```

---

## QDM 对比基准

### 对比结果 (2026-05-28 修复后)

5月1-24日全月日均对比，**总差异 +3.7%，进入 ±5% 目标范围**：

| 大分类 | 差异 | 评估 |
|--------|:---:|:---:|
| 猪肉类 | +0.7% | ✅ |
| 蛋类 | +3.8% | ✅ |
| 标品类 | +1.1% | ✅ |
| 乳制品及水饮类 | -1.1% | ✅ |
| 蔬菜类 | -5.5% | ✅ |
| 水果类 | 0.0% | ✅ |
| 水产类 | +13.9% | 小基数波动 |
| 熟食类 | -6.5% | 待补充加工关系 |
| 烘焙类 | +25.5% | 日清覆盖后大幅改善（修复前+170%） |
| 冷藏加工类 | +9.6% | 修复前+98% |

### QDM 对比表

`default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di`。
该表 `day_clear` 只有 `'1'`（不含 `'2'`），FM 对比时需合并 `'0'+'1'` 全口径。

### QDM 数据查询

通过 `ApiConnector` 从 StarRocks 查询。QDM 表不包含 category 字段，
需 JOIN `strategy_fm_dim_goods` 获取分类。

---

## 加工关系 (Processing Relation)

### 业务逻辑

加工关系管理原料→成品的转化率，**与 BOM 逻辑不同**：
- **BOM**: 1个父品 → 多个子品（拆分，如整猪→排骨+五花肉）
- **加工关系**: 多个原料 → 1个成品（组合，如蛋挞液+蛋挞皮→葡式蛋挞）

### 成本计算公式

```
成品 compose_in_amt = compose_in_qty × Σ(raw_qty / yield_qty × raw_base_euc)
原料 compose_out_amt = compose_out_qty × base_euc (价值守恒)
```

- `raw_qty`: 原料用量
- `yield_qty`: 产出成品数量
- `raw_base_euc`: 原料的 base EUC（不含 compose 的加权平均成本）

### 数据管理

| 位置 | 说明 |
|------|------|
| 远程 DB `/opt/fm/proc-rel/processing_relation.db` | **权威数据源** |
| 本地 `data/processing_relations.json` | 缓存（ETL 优先读本地，失败则调 API） |
| Web 管理 `http://47.115.213.115:8080/reports/processing-relation.html` | 前端页面 |
| API `http://47.115.213.115:5003/api/proc-rel/` | Flask REST API |

### 当前覆盖

22 条活跃关系，18 个成品（全部为烘焙类）。熟食类加工关系待补充。

### 加工关系修改后

需在服务器上重启 ETL 或等次日自动跑。本地修改 JSON 不影响服务器 ETL——
服务器 ETL 优先读服务器上的 JSON 缓存，回退到 API。

---

## 日清覆盖规则 (day_clear override)

### 当前三组覆盖（merge.py + dims_extractor.py）

| override_type | 规则 | 来源 |
|:---|------|------|
| 猪肉类 | `category_level1_description = '猪肉类'` | dim_goods 派生 |
| 熟食类 | `category_level3_description LIKE '%熟食'` | dim_goods 派生 |
| 烘焙类 | 24 个 SKU 硬编码列表 | 业务临时补充 |

### 烘焙日清 SKU 清单

```
21333774 愤怒的小章鱼(C)    21333798 核桃马里奥(C)
21334108 丹麦芝士金枪鱼(C)  21334115 南瓜软欧(C)
21334146 茶香果物(C)        21334153 凤梨鸡扒三文治(C)
21334160 伯爵红茶(C)        21334177 今生挚爱(C)
21334184 岩烧榴莲(C)        21334191 芋泥麻薯软欧(C)
21334207 招牌榴莲软欧(C)    21334221 原味可颂(C)
21336645 巴伐利亚碱水结(C)  21346026 半个核桃马里奥(C)
21346033 半个伯爵红茶(C)    21346040 半个今生挚爱(C)
21346057 半个招牌榴莲软欧(C) 21346064 半个茶香果物(C)
21346583 现烤老婆饼(C)      21346590 丹麦菠萝包(C)
21346705 现烤榴莲酥(C)      21346729 原味麻花(C)
21346736 北海道吐司(C)      21346743 椰蓉古法奶油包(C)
```

> 注意: 鲜牛肉**不在**日清覆盖中（purchase_di 的 init_stock ≠ 0，日清会导致虚假亏损）
> 烘焙日清清单为业务临时补充，后续需产品侧建立持续维护机制

---

## 盘盈（负损耗）处理

### 问题

QDM 的 `lost_amt` 可以为负（盘盈/inventory gain），FM 的库存方程在"正常"分支
不产生负损耗。QDM 盘盈是运营记录（净库存调整），FM 损耗是方程残差。

### v0.10 修复

1. **is_counted 扩展**: 不仅 `created_by != '系统'` 触发，系统快照中 `actual_stock_qty > 0`
   的记录也会触发盘点逻辑（`is_counted = True`）
2. **盘盈检测**: 正常分支中，如果 `actual_stock_qty > eq + 0.001`，
   用实盘数覆盖 end_stock，差额记为负 unknow_lost
3. **日清分支**: `unknow = 新供给 - sale - kl`，已经允许负值

### 局限性

盘盈仅在有人工盘点或有实盘数据的日期触发。大多数日期（无盘点数据），
正常 SKU 的 unknow 仍为 0。这是结构性差异，短期无法完全对齐。

---

## 采购价定义

FM 输出表 `t_fm_levels_result` 中的"采购价"字段：

```
采购价 = SUM(out_stock_amt_cb) / SUM(purchase_weight)

其中:
  out_stock_amt_cb = outstock_cost_price × original_outstock_qty (出库成本金额)
  purchase_weight  = order_qty_payean × outstock_unit_price       (订货权重)
```

**全部字段来自 `strategy_fm_scm_di`** (Hive: `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di`)

采购价是 **SCM SAP 出库成本单价（含税）**——DC 配送到门店的出库成本，不是门店验收价。

### 采购价 ≠ 进货价

| | 采购价(FM输出) | dc_original_price | self_receive_amt/qty |
|---|---|---|---|
| **含义** | SCM出库成本单价(含税) | DC标准出库原价 | 门店实际验收价 |
| **来源** | SCM表 | 价格表 | 验收表 |
| **EUC使用** | ❌ | ❌ | ✅ | 

## Environment Variables (.env)

| 变量 | 必填 | 说明 |
|---|---|---|
| `QDM_ACCESS_KEY` | ✅ | QDM BI API access key |
| `QDM_SECRET_KEY` | ✅ | QDM BI API secret key |
| `FM_DUCKDB_PATH` | ❌ | DuckDB路径 (默认 `data/fm.duckdb`，服务器设为 `/opt/fm/data/fm.duckdb`) |

## Key Documents

- [fmetl/README.md](fmetl/README.md) — v0.10 完整pipeline说明 (13步, 核心公式, 修复对照表)
- [fmetl/docs/architecture/ETL_v0.10_完整处理逻辑.md](fmetl/docs/architecture/ETL_v0.10_完整处理逻辑.md) — 价格体系 + 13步详解 + 分类重映射
- [fmetl/docs/references/strategy_fm_字段手册_完整版.md](fmetl/docs/references/strategy_fm_字段手册_完整版.md) — 唯一权威源表字段手册
- [fmetl/docs/references/strategy_fm_字段手册_BOM版.md](fmetl/docs/references/strategy_fm_字段手册_BOM版.md) — BOM专用字段手册
- [fmetl/docs/reviews/差异问题与待办事项_v0.10.md](fmetl/docs/reviews/差异问题与待办事项_v0.10.md) — QDM对比差异分析 + 行动计划
- [fmetl/docs/fixes/README.md](fmetl/docs/fixes/README.md) — 修复记录索引 + 依赖关系图
- [fmetl/docs/README.md](fmetl/docs/README.md) — 文档导航索引

## Documentation Conventions

- **CLAUDE.md** (this file): AI 操作手册 — 核心规则、快速索引、代码模式
- **子目录 README.md**: 给人看的详细文档 — 保留所有细节，方便同事理解每个模块
- **docs/**: 按类型分目录 — `architecture/` (架构) / `references/` (参考手册) / `reviews/` (审查) / `fixes/` (修复记录)
- **字段手册**: `references/strategy_fm_字段手册_完整版.md` 是唯一权威，不建多版本
- **修复记录**: 每次 bug fix 写 `docs/fixes/FIX-NNN-short-name.md`，更新 `fixes/README.md` 索引
