#!/usr/bin/env python3
"""5月 fmetl vs QDM 全量对比 — 门店×大分类 → 逐日 → SKU"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fmetl.config import get_settings
from fmetl.connectors import ApiConnector
import duckdb
import pandas as pd
import numpy as np

cfg = get_settings()
api = ApiConnector(cfg)
conn = duckdb.connect('data/fm.duckdb', read_only=True)
START, END = '2026-05-01', '2026-05-27'

# ============================================================
# 1. 门店 × 大分类 全月汇总对比
# ============================================================
print("=" * 100)
print("1. 门店 × 大分类 — 全月汇总")
print("=" * 100)

fmetl = conn.execute(f"""
WITH cat_map AS (
    SELECT DISTINCT
        article_id,
        CASE
            WHEN category_level2_description = '烘焙类' THEN '烘焙类'
            WHEN category_level3_description LIKE '%熟食' THEN '熟食类'
            WHEN category_level1_id = '24' THEN
                CASE WHEN category_level2_description = '蛋品类' THEN '蛋类'
                     WHEN category_level2_description = '肉禽类' THEN '肉禽类'
                     ELSE '肉禽蛋类' END
            WHEN category_level1_id IN ('25','26') THEN '冷藏加工及预制菜类'
            WHEN category_level2_description = '乳制品及水饮' THEN '乳制品及水饮类'
            ELSE category_level1_description
        END AS cat1_name
    FROM t_fm_sku_dim
)
SELECT
    s.store_id,
    COALESCE(cm.cat1_name, '其他') AS cat1_name,
    SUM(s.sale_amt)            AS fmetl_sale_amt,
    SUM(s.sale_qty)            AS fmetl_sale_qty,
    SUM(p.profit_amt)          AS fmetl_profit,
    SUM(s.init_stock_amt)      AS fmetl_init_stock_amt,
    SUM(s.end_stock_amt)       AS fmetl_end_stock_amt,
    SUM(s.init_stock_qty)      AS fmetl_init_stock_qty,
    SUM(s.end_stock_qty)       AS fmetl_end_stock_qty,
    SUM(s.receive_amt)         AS fmetl_receive_amt,
    SUM(s.know_lost_amt)       AS fmetl_know_lost_amt,
    SUM(s.unknow_lost_amt)     AS fmetl_unknow_lost_amt,
    SUM(s.compose_in_amt)      AS fmetl_compose_in_amt,
    SUM(s.compose_out_amt)     AS fmetl_compose_out_amt,
    SUM(s.bom_in_amt)          AS fmetl_bom_in_amt,
    SUM(s.bom_out_amt)         AS fmetl_bom_out_amt,
    SUM(p.sale_cost_amt)       AS fmetl_sale_cost_amt,
    SUM(p.allowance_amt_profit) AS fmetl_allowance_amt
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date
    AND s.article_id = p.article_id
    AND s.day_clear = p.day_clear
LEFT JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
GROUP BY s.store_id, cm.cat1_name
""").df()
print(f"fmetl: {len(fmetl)} rows, stores={fmetl['store_id'].nunique()}, cats={fmetl['cat1_name'].nunique()}")

# QDM: 不用表别名，直接列名
# 先取 dim_goods 映射
print("获取 QDM dim_goods ...")
goods_df = api.query("""
SELECT DISTINCT article_id, category_level1_description, category_level2_description, category_level3_description
FROM strategy_fm_dim_goods
WHERE inc_day = (SELECT MAX(inc_day) FROM strategy_fm_dim_goods)
""")
print(f"  dim_goods: {len(goods_df)} rows")

# QDM SKU 级数据 (不分批，一次性拿）
print("获取 QDM SKU 级数据 (5月全月)...")
qdm_sku = api.query(f"""
SELECT
    store_id,
    business_date,
    article_id,
    SUM(sale_amt)       AS qdm_sale_amt,
    SUM(sale_qty)       AS qdm_sale_qty,
    SUM(profit_amt)     AS qdm_profit,
    SUM(init_stock_amt) AS qdm_init_stock_amt,
    SUM(end_stock_amt)  AS qdm_end_stock_amt,
    SUM(init_stock_qty) AS qdm_init_stock_qty,
    SUM(end_stock_qty)  AS qdm_end_stock_qty,
    SUM(receive_amt)    AS qdm_receive_amt,
    SUM(know_lost_amt)  AS qdm_know_lost_amt,
    SUM(unknow_lost_amt) AS qdm_unknow_lost_amt,
    SUM(compose_in_amt) AS qdm_compose_in_amt,
    SUM(compose_out_amt) AS qdm_compose_out_amt
FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
WHERE business_date BETWEEN '{START}' AND '{END}'
GROUP BY store_id, business_date, article_id
""")
print(f"  QDM SKU rows: {len(qdm_sku)}")

# QDM JOIN dim_goods
qdm_with_cat = qdm_sku.merge(goods_df, on='article_id', how='left')

def remap_cat(c2, c3, c1):
    c2 = str(c2) if pd.notna(c2) else ''
    c3 = str(c3) if pd.notna(c3) else ''
    c1 = str(c1) if pd.notna(c1) else ''
    if c2 == '烘焙类':
        return '烘焙类'
    if '熟食' in c3:
        return '熟食类'
    if c1 == '肉禽蛋类':
        if '蛋品' in c2:
            return '蛋类'
        if '肉禽' in c2:
            return '肉禽类'
        return '肉禽蛋类'
    if c1 in ('预制菜', '冷藏及加工类'):
        return '冷藏加工及预制菜类'
    if c2 == '乳制品及水饮':
        return '乳制品及水饮类'
    return c1

qdm_with_cat['cat1_name'] = qdm_with_cat.apply(
    lambda r: remap_cat(r.get('category_level2_description'), r.get('category_level3_description'), r.get('category_level1_description')), axis=1)

qdm_by_cat = qdm_with_cat.groupby(['store_id', 'cat1_name']).agg(
    qdm_sale_amt=('qdm_sale_amt', 'sum'),
    qdm_sale_qty=('qdm_sale_qty', 'sum'),
    qdm_profit=('qdm_profit', 'sum'),
    qdm_init_stock_amt=('qdm_init_stock_amt', 'sum'),
    qdm_end_stock_amt=('qdm_end_stock_amt', 'sum'),
    qdm_init_stock_qty=('qdm_init_stock_qty', 'sum'),
    qdm_end_stock_qty=('qdm_end_stock_qty', 'sum'),
    qdm_receive_amt=('qdm_receive_amt', 'sum'),
    qdm_know_lost_amt=('qdm_know_lost_amt', 'sum'),
    qdm_unknow_lost_amt=('qdm_unknow_lost_amt', 'sum'),
    qdm_compose_in_amt=('qdm_compose_in_amt', 'sum'),
    qdm_compose_out_amt=('qdm_compose_out_amt', 'sum'),
).reset_index()

# ---- 合并 ----
m = fmetl.merge(qdm_by_cat, on=['store_id', 'cat1_name'], how='outer')
for c in m.columns:
    if m[c].dtype in ('float64', 'int64'):
        m[c] = m[c].fillna(0)
m['profit_diff'] = m['fmetl_profit'] - m['qdm_profit']
m['sale_diff'] = m['fmetl_sale_amt'] - m['qdm_sale_amt']

cat_s = m.groupby('cat1_name').agg(
    store_cnt=('store_id', 'nunique'),
    fmetl_sale=('fmetl_sale_amt', 'sum'),
    qdm_sale=('qdm_sale_amt', 'sum'),
    fmetl_profit=('fmetl_profit', 'sum'),
    qdm_profit=('qdm_profit', 'sum'),
    fmetl_init=('fmetl_init_stock_amt', 'sum'),
    qdm_init=('qdm_init_stock_amt', 'sum'),
    fmetl_end=('fmetl_end_stock_amt', 'sum'),
    qdm_end=('qdm_end_stock_amt', 'sum'),
    fmetl_receive=('fmetl_receive_amt', 'sum'),
    qdm_receive=('qdm_receive_amt', 'sum'),
    fmetl_klost=('fmetl_know_lost_amt', 'sum'),
    qdm_klost=('qdm_know_lost_amt', 'sum'),
    fmetl_ulost=('fmetl_unknow_lost_amt', 'sum'),
    qdm_ulost=('qdm_unknow_lost_amt', 'sum'),
    fmetl_compose_in=('fmetl_compose_in_amt', 'sum'),
    qdm_compose_in=('qdm_compose_in_amt', 'sum'),
    fmetl_bom_in=('fmetl_bom_in_amt', 'sum'),
    fmetl_bom_out=('fmetl_bom_out_amt', 'sum'),
    fmetl_sale_cost=('fmetl_sale_cost_amt', 'sum'),
    fmetl_allowance=('fmetl_allowance_amt', 'sum'),
).reset_index()
cat_s['profit_diff'] = cat_s['fmetl_profit'] - cat_s['qdm_profit']
cat_s['profit_pct'] = (cat_s['profit_diff'] / cat_s['qdm_profit'].abs().replace(0, np.nan) * 100).round(1)
cat_s['sale_diff'] = cat_s['fmetl_sale'] - cat_s['qdm_sale']
cat_s['sale_pct'] = (cat_s['sale_diff'] / cat_s['qdm_sale'].abs().replace(0, np.nan) * 100).round(1)
cat_s['init_diff'] = cat_s['fmetl_init'] - cat_s['qdm_init']
cat_s['end_diff'] = cat_s['fmetl_end'] - cat_s['qdm_end']
cat_s = cat_s.sort_values('profit_diff', key=abs, ascending=False)

print("\n" + "=" * 100)
print("【大分类汇总 — 5月全月】")
print("=" * 100)
for _, r in cat_s.iterrows():
    print(f"\n{'─'*90}")
    print(f"【{r['cat1_name']}】({int(r['store_cnt'])}门店)")
    print(f"  毛利额:    fmetl={r['fmetl_profit']:>12,.0f}  QDM={r['qdm_profit']:>12,.0f}  diff={r['profit_diff']:>10,.0f} ({r['profit_pct']:+.1f}%)")
    print(f"  销售额:    fmetl={r['fmetl_sale']:>12,.0f}  QDM={r['qdm_sale']:>12,.0f}  diff={r['sale_diff']:>10,.0f} ({r['sale_pct']:+.1f}%)")
    print(f"  销售成本:  fmetl={r['fmetl_sale_cost']:>12,.0f}")
    print(f"  期初库存:  fmetl={r['fmetl_init']:>12,.0f}  QDM={r['qdm_init']:>12,.0f}  diff={r['init_diff']:>10,.0f}")
    print(f"  期末库存:  fmetl={r['fmetl_end']:>12,.0f}  QDM={r['qdm_end']:>12,.0f}  diff={r['end_diff']:>10,.0f}")
    print(f"  进货额:    fmetl={r['fmetl_receive']:>12,.0f}  QDM={r['qdm_receive']:>12,.0f}  diff={r['fmetl_receive']-r['qdm_receive']:>10,.0f}")
    print(f"  加工转入:  fmetl={r['fmetl_compose_in']:>12,.0f}  QDM={r['qdm_compose_in']:>12,.0f}")
    print(f"  BOM流入:   fmetl={r['fmetl_bom_in']:>12,.0f}")
    print(f"  BOM流出:   fmetl={r['fmetl_bom_out']:>12,.0f}")
    print(f"  折让收入:  fmetl={r['fmetl_allowance']:>12,.0f}")
    print(f"  已知损耗:  fmetl={r['fmetl_klost']:>12,.0f}  QDM={r['qdm_klost']:>12,.0f}")
    print(f"  未知损耗:  fmetl={r['fmetl_ulost']:>12,.0f}  QDM={r['qdm_ulost']:>12,.0f}")

tot = cat_s.sum(numeric_only=True)
print(f"\n{'='*90}")
print(f"【全月总计】")
print(f"  毛利额:    fmetl={tot['fmetl_profit']:,.0f}  QDM={tot['qdm_profit']:,.0f}  diff={tot['fmetl_profit']-tot['qdm_profit']:,.0f}")
print(f"  销售额:    fmetl={tot['fmetl_sale']:,.0f}  QDM={tot['qdm_sale']:,.0f}  diff={tot['fmetl_sale']-tot['qdm_sale']:,.0f}")
if tot['qdm_profit'] != 0:
    print(f"  毛利偏差率: {(tot['fmetl_profit']-tot['qdm_profit'])/abs(tot['qdm_profit'])*100:+.1f}%")

# ============================================================
# 2. 逐日对比
# ============================================================
print("\n\n" + "=" * 100)
print("2. 逐日下钻 — 差异最大的 3 个分类")
print("=" * 100)

top3cats = cat_s.head(3)['cat1_name'].tolist()
print(f"目标分类: {top3cats}")
cat_list = ', '.join(repr(c) for c in top3cats)

# fmetl 逐日
fmetl_daily = conn.execute(f"""
WITH cat_map AS (
    SELECT DISTINCT
        article_id,
        CASE
            WHEN category_level2_description = '烘焙类' THEN '烘焙类'
            WHEN category_level3_description LIKE '%熟食' THEN '熟食类'
            WHEN category_level1_id = '24' THEN
                CASE WHEN category_level2_description = '蛋品类' THEN '蛋类'
                     WHEN category_level2_description = '肉禽类' THEN '肉禽类'
                     ELSE '肉禽蛋类' END
            WHEN category_level1_id IN ('25','26') THEN '冷藏加工及预制菜类'
            WHEN category_level2_description = '乳制品及水饮' THEN '乳制品及水饮类'
            ELSE category_level1_description
        END AS cat1_name
    FROM t_fm_sku_dim
)
SELECT
    s.business_date,
    COALESCE(cm.cat1_name, '其他') AS cat1_name,
    SUM(s.sale_amt)       AS fmetl_sale,
    SUM(p.profit_amt)     AS fmetl_profit,
    SUM(s.init_stock_amt) AS fmetl_init,
    SUM(s.end_stock_amt)  AS fmetl_end,
    SUM(s.receive_amt)    AS fmetl_receive,
    SUM(s.know_lost_amt)  AS fmetl_klost,
    SUM(s.unknow_lost_amt) AS fmetl_ulost,
    SUM(s.compose_in_amt) AS fmetl_compose_in,
    SUM(s.bom_in_amt)     AS fmetl_bom_in,
    SUM(s.bom_out_amt)    AS fmetl_bom_out,
    COUNT(DISTINCT s.article_id) AS sku_cnt
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date
    AND s.article_id = p.article_id
    AND s.day_clear = p.day_clear
LEFT JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
  AND cm.cat1_name IN ({cat_list})
GROUP BY s.business_date, cm.cat1_name
ORDER BY s.business_date, cm.cat1_name
""").df()

# QDM 逐日
qdm_daily = qdm_with_cat[qdm_with_cat['cat1_name'].isin(top3cats)].groupby(
    ['business_date', 'cat1_name']
).agg(
    qdm_sale=('qdm_sale_amt', 'sum'),
    qdm_profit=('qdm_profit', 'sum'),
    qdm_init=('qdm_init_stock_amt', 'sum'),
    qdm_end=('qdm_end_stock_amt', 'sum'),
    qdm_receive=('qdm_receive_amt', 'sum'),
    qdm_klost=('qdm_know_lost_amt', 'sum'),
    qdm_ulost=('qdm_unknow_lost_amt', 'sum'),
    qdm_compose_in=('qdm_compose_in_amt', 'sum'),
    sku_cnt=('article_id', 'nunique'),
).reset_index()

dm = fmetl_daily.merge(qdm_daily, on=['business_date', 'cat1_name'], how='outer')
for c in dm.columns:
    if dm[c].dtype in ('float64', 'int64'):
        dm[c] = dm[c].fillna(0)
dm['profit_diff'] = dm['fmetl_profit'] - dm['qdm_profit']

print("\n逐日明细：")
for cat in top3cats:
    dcat = dm[dm['cat1_name'] == cat].sort_values('business_date')
    if dcat.empty:
        print(f"\n  {cat}: 无数据")
        continue
    print(f"\n{'─'*110}")
    print(f"【{cat}】")
    header = f"{'日期':>12} {'fmetl毛利':>10} {'QDM毛利':>10} {'差异':>10} {'fmetl销售':>10} {'QDM销售':>10} {'fmetl期初':>10} {'QDM期初':>10} {'fmetl期末':>10} {'QDM期末':>10} {'SKU':>5}"
    print(header)
    print('-'*110)
    for _, r in dcat.iterrows():
        print(f"{r['business_date']:>12} {r['fmetl_profit']:>10,.0f} {r['qdm_profit']:>10,.0f} {r['profit_diff']:>10,.0f} {r['fmetl_sale']:>10,.0f} {r['qdm_sale']:>10,.0f} {r['fmetl_init']:>10,.0f} {r['qdm_init']:>10,.0f} {r['fmetl_end']:>10,.0f} {r['qdm_end']:>10,.0f} {int(r['sku_cnt_x']):>5}")

# 保存中间结果供后续 SKU 下钻
conn.close()
print("\n\n分类汇总 + 逐日对比完成。")
print("接下来: SKU 级下钻...")
