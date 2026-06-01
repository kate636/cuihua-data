# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

**fmetl** — 翠花当家 (Cuihua Dangjia) 零售数据分析 ETL 管道。当前活跃版本为 **0.10**（v10.0，预发布），从 QDM BI API 提取数据，经 Python 计算处理后在 DuckDB 中产出 FM 底表。

## Key Commands

所有命令在仓库根目录 (`翠花数据/`) 下执行。

### 环境初始化
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb pandas numpy python-dotenv requests
cp fm_etl_v3/.env.example .env   # 然后填入 QDM_ACCESS_KEY / QDM_SECRET_KEY
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

# 检查 BOM 对称（bom_in = bom_out）
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

## Architecture: 3-Layer ETL (v10.0)

### 设计原则
- **简单SQL，复杂逻辑Python**: SQL 仅做 SELECT/JOIN/GROUP BY/WHERE。计算、分支、窗口函数在 Python（pandas + NumPy）完成
- **每个数字只算一次**: 上游结果直接复用，下游不重复计算
- **四流分离**: receive / bom_in / bom_out / compose_in/out 各自独立列
- **跨日链式传递**: 今日期初库存 = 昨日期末库存

```
┌──────────────────────────────────────────────────────────────────┐
│ Layer -2: atomic_* (原子层) — 不可分解的独立观测量              │
│   12张活跃 + 2张骨架表，从 strategy_fm_* 源表 API 拉取          │
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
│   t_fm_sku_dim       → SKU级完整宽表 (80字段)                   │
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

## Pipeline 13 Steps

| Step | Module | Output |
|---|---|---|
| 1 | `DimsExtractor` | 7张 `dim_*` 全量替换 |
| 2 | 14个 `*Extractor` | 12张 `atomic_*` 分区追加 + 2张空骨架 |
| 3 | `AtomicMerger` | `t_atomic_wide` (82字段, 含父品补全) |
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

cost_price 不参与计算。唯一权威成本来源为 `t_calc_sku_cost`。

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

分支:
  day_clear='0' (日清) → end=0, unknow=eq
  eq < 0 (任何场景)    → end=0, unknow=-eq (负库存保护)
  know_lost_qty > 0    → end=eq, unknow=0 (盘点)
  其他                  → end=eq, unknow=0
```

金额统一用 euc 计算: `end_stock_amt = end_qty × euc`, `unknow_lost_amt = unknow_qty × euc`

### 门店毛利 (profit.py — 含BOM)

```
profit = sale - receive - bom_in + bom_out - compose_in + compose_out
       + end_stock - init_stock

注: 损耗已通过库存方程反映在 end_stock 中，不再额外扣减 lost_amt（A20）。

### BOM 分摊 — 共享组与父品归集

**共享组**：当 parent_B.subs ⊆ parent_A.subs 时，合并为共享组。分配遵循 v9 逻辑：
- **所有子品**（含独有）用组总额 `group_total_amt` 做基数分配，保证子品成本公允
- 共享子品按进货额比例（如 76%/24%）分拆到两个父品

**父品 bom_out 归集**（stock.py 5b）：组内所有子品的 bom_out 汇总后可能超过单个父品的 receive_amt，因此按 `receive_amt / group_total_amt` 比例缩放各父品的 `bom_out_amt`，使其精确等于各自的 receive_amt，父品利润归零。

### 库存转移（stock.py 12步）

BOM 父品剩余库存（sale=0, bom_out>0, end>0）通过 `stock_transfer_out` 按 bom_alloc 比例转移给子品 `stock_transfer_in`，确保父品 end=0, profit=0。转移直接修改 end_stock，不在毛利公式中额外加减。
```

### 销售成本
```
日清: sale_cost = receive + bom_in - bom_out + compose_in - compose_out
非日清: sale_cost = sale_qty × euc
```

### day_clear 字段语义
| 值 | 含义 | 库存规则 |
|---|---|---|
| `'0'` | 日清 (生鲜) | end=0, 残差→unknow_lost |
| `'1'` | 非日清 (标品) | 正常库存方程 |
| `'2'` | 合计 (ETL UNION生成) | 查询全天合计必须 `WHERE day_clear='2'` |

## Data Source Tables

### 原子域表 (12张活跃 + 2张骨架)
| DuckDB 表 | QDM 商分表 | 域 |
|---|---|---|
| `atomic_sales` | `strategy_fm_sales_di` | ①销售 |
| `atomic_inventory` | `strategy_fm_purchase_di` | ②库存 |
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
| `dim_goods` | `strategy_fm_dim_goods` |
| `dim_store_list` | `ads_business_analysis.chdj_store_info` |
| `dim_day_clear` | `strategy_fm_dim_day_clear` |
| `dim_store_profile` | `strategy_fm_dim_store_profile` |
| `dim_calendar` | `strategy_fm_dim_calendar` |
| `dim_saleable` | `strategy_fm_dim_saleable` |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` |

### 计算层表 (4张, v10)
| DuckDB 表 | 算法 |
|---|---|
| `t_atomic_wide` | FULL OUTER JOIN + 父品补全 (82字段) |
| `t_calc_bom_alloc` | Σ总权重 + Python共享组识别 + 单位归一化 |
| `t_calc_sku_cost` | 加权平均含期初库存 |
| `t_calc_stock` | 四流合一 + 跨日滚动 (中枢, 合并了inventory/avg_price/amounts) |
| `t_calc_profit` | 含BOM + SCM金融 + 全链路毛利 |

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

## Code Patterns

### API 列名自动归一化
`ApiConnector.query()` 自动将 QDM API 返回的 `camelCase` 列名转回 `snake_case`（如 `businessDate` → `business_date`）。

### WAF 规避: CASE WHEN → IF()
API SQL 中所有 `CASE WHEN x THEN y ELSE z END` 必须写成 `IF(x, y, z)`。DuckDB 内部 SQL 和 Python 代码无此限制。

### dim_goods 注意事项

`dim_goods` 是**日快照表**（API 侧已按 `inc_day = '{end}'` 过滤），DuckDB 中**没有 inc_day 列**。下游 JOIN dim_goods 时只需 `ON article_id`，不需要额外日期过滤。用错日期会导致分类映射错误。

### QDM 对比表

对比 v4 输出时使用 QDM 表 `default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di`（不是 `strategy_fm_levels_result`）。该表字段与 v4 一一对应（receive_amt, compose_in/out, know_lost/unknow_lost, init/end_stock 等），可做 SKU 级逐字段对比。

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
v10 中所有复杂计算在 Python 完成，SQL 仅做数据拉取和 JOIN。Calculator 读取 DuckDB 数据为 DataFrame → pandas/NumPy 计算 → 写回 DuckDB。

### 日期分片 & 写入模式
`BaseExtractor.extract()` 按 7 天 chunk 拆分，逐片 API 请求后用 `replace_partition` 模式写入（先删后插，幂等）。`DuckDBStore.load_df()` 支持 `replace_partition`（默认）、`replace`、`append` 三种模式。

## Project Structure

```
翠花数据/
├── fmetl/                    # 主 ETL Pipeline v10.0 (预发布)
│   ├── executor.py          # 主入口 (13步)
│   ├── config/              # API凭证配置
│   ├── connectors/          # ApiConnector + DuckDBStore
│   ├── atomic/              # Step 1-2: 原子域提取 (14个extractor)
│   ├── calculated/          # Step 3-7: 计算层 (5个模块, Python核心)
│   ├── fm_tables/           # Step 8-13: FM底表 (6张)
│   └── utils/               # 日志/工具
│
├── fm_etl_v3/               # 旧版 v3.0 (历史版本, 保留参考)
├── legacy_scripts/          # 旧版脚本 (参考/备份)
├── _archived/               # 历史归档 (tests/deploy/docs等)
│
└── data/                    # DuckDB数据文件 (不上GitHub)
```

## Environment Variables (.env)

| 变量 | 必填 | 说明 |
|---|---|---|
| `QDM_ACCESS_KEY` | ✅ | QDM BI API access key |
| `QDM_SECRET_KEY` | ✅ | QDM BI API secret key |
| `FM_DUCKDB_PATH` | ❌ | DuckDB路径 (默认 `data/fm.duckdb`) |

## Key Documents

- [fmetl/README.md](fmetl/README.md) — v10.0 完整pipeline说明 (13步, 核心公式, 修复对照表)
- [fmetl/docs/](fmetl/docs/) — 指标字典 + 字段映射
- [fm_etl_v3/DEPLOY.md](fm_etl_v3/DEPLOY.md) — 云端部署手册
