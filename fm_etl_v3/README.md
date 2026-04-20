# FM ETL v3.0 — 翠花数据本地计算管道

## 概览

v3 的核心设计：**唯一数据入口是 QDM BI API（只读），所有中间计算在本地 DuckDB 完成，最终结果存入 MotherDuck 云数据库（或本地 DuckDB 文件）供团队查询。**

```
QDM BI API → DuckDB/MotherDuck（计算+存储）
```

```bash
python -m fm_etl_v3.executor 2025-01-01 2025-01-31
```

## Pipeline 执行顺序

```
Step 1   维度表提取 (DimsExtractor)         → DuckDB dim_*
Step 2   9 个原子域提取 (atomic/)            → DuckDB atomic_*
Step 3   原子宽表合并 (AtomicMerger)         → DuckDB t_atomic_wide
Step 4   库存方程计算 (InventoryCalculator)  → DuckDB t_calc_inventory
Step 5   均价计算 (AvgPriceCalculator)       → DuckDB t_calc_avg_price
Step 6   金额计算 (AmountsCalculator)        → DuckDB t_calc_amounts
Step 7   毛利计算 (ProfitCalculator)         → DuckDB t_calc_profit
Step 8   FM 商品维度底表 (SkuDimBuilder)     → DuckDB/MotherDuck t_fm_sku_dim
Step 9   FM 客数底表 (CustBuilder)           → DuckDB/MotherDuck t_fm_cust
Step 10  FM 分类汇总 (LevelsSumBuilder)      → DuckDB/MotherDuck t_fm_levels_sum
Step 11  FM 结果层 (LevelsResultBuilder)     → DuckDB/MotherDuck t_fm_levels_result
```

## 目录结构

```
fm_etl_v3/
├── executor.py          # 主入口，串联全部 11 个步骤
├── requirements.txt
├── .env.example         # 凭证配置模板（复制为 .env 填入真实值）
├── config/              # API 凭证 + MotherDuck 配置 + 业务过滤常量
├── utils/               # 日志、日期分段、重试装饰器
├── connectors/          # API 连接器（只读）+ DuckDB/MotherDuck 连接封装
├── atomic/              # 9 个原子域提取器 + 维度提取器（调用 QDM API）
├── calculated/          # DuckDB 内部计算层（合并→库存→均价→金额→毛利）
└── fm_tables/           # FM 底表构建器，结果写入 DuckDB/MotherDuck
```

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置凭证
cp .env.example .env
# 编辑 .env，填入 QDM_ACCESS_KEY、QDM_SECRET_KEY、MOTHERDUCK_TOKEN

# 3. 运行
python -m fm_etl_v3.executor 2025-01-01 2025-01-31
```

## 环境变量

| 变量 | 必填 | 说明 |
|------|------|------|
| `QDM_ACCESS_KEY` | ✅ | QDM BI API access key |
| `QDM_SECRET_KEY` | ✅ | QDM BI API secret key |
| `QDM_API_ID` | 否 | API ID（默认 `i_fjl10g687-790`） |
| `QDM_HOST` | 否 | API 主机（默认 `https://bdapp.qdama.cn`） |
| `MOTHERDUCK_TOKEN` | 否 | MotherDuck token（空则使用本地 DuckDB） |
| `MOTHERDUCK_DB` | 否 | MotherDuck 数据库名（默认 `fm_etl_v3`） |
| `FM_DUCKDB_PATH` | 否 | 本地 DuckDB 路径（`MOTHERDUCK_TOKEN` 为空时生效） |

## 依赖安装

```bash
pip install -r requirements.txt
```

主要依赖：`duckdb>=0.10.0`、`requests>=2.28.0`、`pandas>=2.0.0`、`python-dotenv>=1.0.0`
