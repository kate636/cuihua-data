# fm_tables/ — FM 底表层

最终产出层。6 张表供商分平台消费。

## 表概览

| 表 | 粒度 | 说明 |
|----|------|------|
| `t_fm_sku_dim` | store×date×article_id×day_clear | SKU 级完整宽表 |
| `t_fm_cust` | store×date×day_clear×level | 客数聚合 |
| `t_fm_levels_sum` | store×date×level_id×day_clear | 分类汇总（数量/金额） |
| `t_fm_levels_result` | store_flag×store_no×date×level_id×day_clear | 平台对接表（中文列名+比率KPI） |
| `t_fm_bom_breakdown` | store×date×parent×sub | AI 溯源：BOM 分摊明细 |
| `t_fm_stock_roll` | store×date×article_id×day_clear | AI 溯源：库存八要素滚动 |

## t_fm_sku_dim

SKU 维度全量宽表，合并 t_atomic_wide + t_calc_stock + t_calc_profit + 7 张维度表。

**v0.10 变更:**
- 类别重映射从 SQL CASE WHEN 移到 Python `_remap_category()`
- inbound 统一从 t_calc_stock 取（A15 修复）

**关键字段:**
- 销售: total_sale_qty, total_sale_amt, bf19_sale_qty, bf19_sale_amt, lp_sale_amt
- 库存: init_stock_qty/amt, end_stock_qty/amt
- 进货: inbound_qty, inbound_amount
- 损耗: store_lost_qty/amt, store_know_lost_amt, store_unknow_lost_amt
- 毛利: article_profit_amt, full_link_article_profit, scm_fin_article_profit
- 成本: effective_unit_cost, cost_source
- 分类: category_level1/2/3 (remapped)
- KPI: sales_weight, is_stock_sku, is_soldout_16/20, avg_7d_sale_qty

## t_fm_cust

客数聚合表。**v0.10 修复 A16**: Python 端 JOIN dim_goods 排除品类 70-77（物料类）。

## t_fm_levels_sum

多级分类汇总：门店 / 大类 / 中类 / 小类 / SPU / 黑白猪 / SKU。

**v0.10**: 移除 store_profit_sales / store_profit_stock / store_profit_diff（不再区分双口径）。

## t_fm_levels_result

平台对接表，中文列名。KPI 包括:

| KPI | 公式 |
|-----|------|
| 门店毛利额 | AVG(article_profit_amt) |
| 全链路毛利额 | AVG(full_link_article_profit) |
| 全链路毛利率 | SUM(full_link_article_profit) / SUM(total_sale_amt) |
| 门店毛利率 | SUM(article_profit_amt) / SUM(total_sale_amt) |
| 供应链毛利率 | SUM(scm_fin_article_profit) / SUM(out_stock_pay_amt_notax + return_stock_pay_amt_notax) |
| 损耗率 | SUM(store_lost_amt) / SUM(lost_denominator) |
| 售罄率16/20 | AVG(is_soldout_16/20) |
| 采购价 | SUM(out_stock_amt_cb) / SUM(purchase_weight) |
| 平均售价 | SUM(total_sale_amt) / SUM(sales_weight) |

**v0.10**: 移除门店毛利额_销售方程 / 门店毛利额_库存方程 / 门店毛利口径差异。

## t_fm_bom_breakdown

BOM 分摊溯源表，粒度 (store, date, parent, sub)。

**v0.10**: 适配新字段 cost_rate_source。

## t_fm_stock_roll

库存八要素滚动展示：期初 → 四流入 → 三流出 → 销售 → 损耗 → 期末，含 balance_qty 校验列。

**v0.10**: 四流分离展示（receive / bom_in / bom_out / compose_in / compose_out 各自独立列）。
