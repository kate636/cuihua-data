# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an ETL (Extract-Transform-Load) data pipeline for **翠花当家 (Cuihua Dangjia)** retail business analytics. All scripts execute SQL against a **StarRocks** database (MySQL-compatible OLAP) to build layered data marts for store-level sales, profit, loss, and supply chain analysis.

## Project Structure

```
翠花数据/
├── fm_etl_pipeline/      # 新版 ETL Pipeline (Python 模块化)
│   ├── config/           # 配置管理
│   ├── connectors/       # 数据库连接器
│   ├── extraction/       # 数据提取（10大域取数器）
│   ├── models/           # 数据模型（原子层、计算层、BOM处理）
│   ├── layers/           # 处理层（商品汇总、分类汇总、结果层）
│   └── executor.py       # 主执行器
│
├── legacy_scripts/       # 旧版脚本（Layer 0-4）
│   ├── layer0/           # 上游数据收集
│   ├── layer1/           # 全渠道门店仓商品销售信息
│   ├── layer2/           # 非日清门店商品汇总宽表
│   ├── layer3/           # 翠花门店商品全链路指标
│   └── layer4/           # 翠花门店商品销售条码宽表
│
├── upstream/             # 上游数据同步脚本
├── monitoring/           # 数据监控脚本
├── docs/                 # 文档和笔记
└── 底表/                 # 底表设计和SQL
```

## Architecture: 5-Layer ETL Pipeline

### 新版 Pipeline (fm_etl_pipeline/)

```
原子层 (Layer -4) → 计算层 (Layer -3) → 商品汇总层 (Layer -2) → 分类汇总层 (Layer -1) → 结果层 (Layer 0)
```

**运行方式：**
```bash
# 基础运行
python -m fm_etl_pipeline.executor 2025-01-01 2025-01-31

# 使用 BOM 毛利计算
python -m fm_etl_pipeline.executor 2025-01-01 2025-01-31 --use-bom
```

### 旧版 Pipeline (legacy_scripts/)

Layer 0 → Layer 1 → Layer 2 → Layer 3 → Layer 4（严格顺序执行）

## Key Business Logic

### Day Clear (日清) vs Non-Day-Clear (非日清)
- `day_clear='1'` = non-perishable (long shelf life) → cost = `sale_qty × avg_purchase_price`
- `day_clear='0'` = perishable (must sell same day) → cost includes inventory changes, processing transforms, and losses

### Loss (损耗) Data Lineage
- **lost_amt** = `know_lost_amt + unknow_lost_amt`
- Source: `dal_transaction_store_article_lost_di` (produced by Layer 0 `门店商品损耗表.py`)
- Three UNION ALL sources with a date split at **2025-07-22**

### Processing Transform (加工转换) Data Lineage
- Source: `dsl_transaction_sotre_article_compose_info_di` (produced by Layer 0 Hive script)
- Two UNION ALL sources:
  - `ddl.ddl_compose_in_info_di` → raw materials consumed → mapped to `compose_out_*`
  - `ddl.ddl_compose_out_info_di` → finished products created → mapped to `compose_in_*`

### Critical Calculated Fields
- **lost_amt (损耗额)** = `know_lost_amt + unknow_lost_amt`
- **profit_amt (毛利额)** = `sale_amt - (receive_amt + compose_in_amt - compose_out_amt) + (end_stock_amt - init_stock_amt)`
- **sale_cost_amt (销售成本)** = differs by day_clear type
- **avg_purchase_price (平均进货价)** = weighted average calculation

## Data Source Tables

### ODS/STG Layer (raw operational data)
- `ods_rt_dws.dws_transaction_store_article_unknowlost_rts_di` — real-time unknown loss snapshots
- `ods_sc_db.t_purchase_wastage` — store-reported wastage records
- `ods_sc_db.t_sc_settlement_detail_logs` — historical settlement detail logs
- `ods_sc_db.t_shop_inventory_sku_pool` — cost prices

### DDL Layer (compose/BOM definitions)
- `ddl.ddl_compose_in_info_di` — raw materials consumed in processing
- `ddl.ddl_compose_out_info_di` — finished products created in processing

### DSL Layer (transaction logs)
- `dsl_transaction_non_daily_store_order_details_di` — order-level transaction details
- `dsl_transaction_non_daily_store_article_purchase_di` — purchase/inventory
- `dsl_transaction_sotre_article_compose_info_di` — processing transforms
- `dsl_promotion_order_item_article_sale_info_di` — promotion discount details

### DAL Layer (aggregated data marts)
- `dal_transaction_store_article_lost_di` — known/unknown losses
- `dal_activity_article_order_sale_info_di` — subsidies
- `dal_transaction_cbstore_cust_num_info_di` — customer traffic counts

### DAL_FULL_LINK Layer (supply chain)
- `dal_manage_full_link_dc_store_article_scm_di` — supply chain metrics
- `dal_manage_full_link_store_dc_article_info_di` — merged full-link wide table

### DIM Layer (dimensions)
- `dim_chdj_store_list_di` — Cuihua store list
- `dim_day_clear_article_list_di` — day-clear article labels
- `dim_goods_information_have_pt` — product master data
- `dim_store_article_price_info_da` — price info

## Code Patterns

- Python scripts embed SQL as multiline strings
- UNION ALL pattern: each source provides its own metrics and sets all other metrics to 0, then grouped and summed
- Category filter: `category_level1_id not in ('70','71','72','73','74','75','76','77')` excludes materials
- Store filter: `dim_chdj_store_list_di` INNER JOIN restricts to Cuihua stores only
- Loss data has a date-based source split: `>= 2025-07-22` uses new real-time sources

## Development Guidelines

### 新功能开发
1. 在 `fm_etl_pipeline/` 目录下开发
2. 遵循模块化设计
3. 添加对应的 README 文档

### 旧脚本维护
1. 旧脚本位于 `legacy_scripts/` 目录
2. 仅作参考和备份
3. 新需求优先使用新版 Pipeline
