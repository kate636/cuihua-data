# FM ETL v2.0

基于指标字典 v3 的 ETL 管道，作为现有商品维度底表 (`strategy_fm_flag_sku_di`) 的上游数据源。

## 核心原则

- **原子层 (Layer -2)**: SQL 取数 → Parquet 存储
- **计算层 (Layer -1)**: Python 本地计算
- **输出层**: 写入 StarRocks 新表

## 项目结构

```
fm_etl_v2/
├── config/           # 配置管理
├── atomic/           # 原子层取数器 (10大域)
├── calculated/       # 计算层 (库存方程、金额、毛利)
├── bom/              # BOM 拆分管道
├── output/           # 输出层 (底表构建、数据库写入)
├── utils/            # 工具函数
└── main.py           # 主入口
```

## 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

### 运行 ETL

```bash
# 基础运行
python -m fm_etl_v2.main --start-date 2025-01-01 --end-date 2025-01-31

# 使用 BOM 成本分摊
python -m fm_etl_v2.main --start-date 2025-01-01 --end-date 2025-01-31 --use-bom

# 跳过原子层 (使用已有 Parquet)
python -m fm_etl_v2.main --start-date 2025-01-01 --end-date 2025-01-31 --skip-atomic
```

## 数据流

```
StarRocks 源表 → 原子层 (SQL) → Parquet → 计算层 (Python) → 输出层 → StarRocks 新表
```

## 10 大原子域

| 域 | 说明 | 主要字段 |
|----|------|---------|
| 域① 销售域 | POS 交易数据 | sale_qty, sale_amt, discount_amt |
| 域② 进货库存域 | 进货和库存数量 | receive_qty, init_stock_qty, end_stock_qty |
| 域③ 供应链域 | SAP 交付数据 | outstock_qty, scm_promotion_amt |
| 域④ 损耗域 | 已知损耗 | know_lost_qty |
| 域⑤ 加工转换域 | BOM 加工 | compose_in_qty, compose_out_qty |
| 域⑥ 补贴域 | 补贴金额 | allowance_amt |
| 域⑦ 促销优惠域 | 促销优惠 | member_coupon_shop_amt |
| 域⑧ 成本价域 | 成本价 | cost_price |
| 域⑨ 价格域 | 销售价格 | current_price, original_price |
| 域⑩ 盘点域 | 盘点数据 | physical_count_qty |

## 计算层处理流程

1. **日切处理**: init_stock_qty = 前日期末库存
2. **库存方程求解**: 推算 end_stock_qty 和 unknow_lost_qty
3. **均价计算**: 区分日清/非日清
4. **金额计算**: 数量 × 单价
5. **毛利计算**: profit_amt, sale_cost_amt

## 配置

可通过环境变量或配置文件配置：

```bash
# 环境变量
export STARROCKS_HOST=localhost
export STARROCKS_PORT=9030
export STARROCKS_USER=root
export FM_ETL_DATA_DIR=/path/to/data
```

或使用 JSON 配置文件：

```bash
python -m fm_etl_v2.main --config config.json --start-date 2025-01-01 --end-date 2025-01-31
```

## 输出表

- `strategy_fm_flag_sku_di_v2`: 商品维度底表
- `strategy_fm_cust_v2`: 客数底表

## 待确认事项

- [ ] 盘点数据源表
- [ ] BOM 关系表结构
