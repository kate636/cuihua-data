#!/usr/bin/env python3
"""QDM 按 fmetl 分类重映射 → 逐日+SKU 详细对比"""
import sys, os, time
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
# Step 0: 从本地 DuckDB 取 dim_goods 映射
# ============================================================
print("=" * 100)
print("Step 0: 获取数据")
print("=" * 100)

# DuckDB 的 dim_goods 是 strategy_fm_dim_goods 的完整副本
goods_df = conn.execute("""
SELECT DISTINCT article_id, category_level1_id, category_level1_description,
       category_level2_description, category_level3_description
FROM dim_goods
""").df()
print(f"  dim_goods (本地): {len(goods_df)} rows")

# QDM 分批查询，避免 API 超时
print("  获取 QDM 数据 (逐日分批避免超时)...")
import datetime
dates = []
d = datetime.date(2026, 5, 1)
end_d = datetime.date(2026, 5, 27)
while d <= end_d:
    dates.append(d.isoformat())
    d += datetime.timedelta(days=1)

all_qdm = []
for i, dt in enumerate(dates):
    print(f"    fetching {dt} ({i+1}/{len(dates)})...", end=' ', flush=True)
    try:
        batch = api.query(f"""
        SELECT
            store_id, business_date, article_id,
            SUM(profit_amt)     AS qdm_profit,
            SUM(sale_amt)       AS qdm_sale_amt,
            SUM(sale_qty)       AS qdm_sale_qty,
            SUM(init_stock_amt) AS qdm_init,
            SUM(end_stock_amt)  AS qdm_end,
            SUM(receive_amt)    AS qdm_receive,
            SUM(know_lost_amt)  AS qdm_klost,
            SUM(unknow_lost_amt) AS qdm_ulost,
            SUM(compose_in_amt) AS qdm_compose_in,
            SUM(compose_out_amt) AS qdm_compose_out
        FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
        WHERE business_date = '{dt}'
        GROUP BY store_id, business_date, article_id
        """)
        all_qdm.append(batch)
        print(f"{len(batch)} rows")
    except Exception as e:
        print(f"FAILED: {e}")
        # retry once
        time.sleep(10)
        print(f"    retrying {dt}...", end=' ', flush=True)
        batch = api.query(f"""
        SELECT store_id, business_date, article_id,
            SUM(profit_amt) AS qdm_profit, SUM(sale_amt) AS qdm_sale_amt,
            SUM(sale_qty) AS qdm_sale_qty, SUM(init_stock_amt) AS qdm_init,
            SUM(end_stock_amt) AS qdm_end, SUM(receive_amt) AS qdm_receive,
            SUM(know_lost_amt) AS qdm_klost, SUM(unknow_lost_amt) AS qdm_ulost,
            SUM(compose_in_amt) AS qdm_compose_in, SUM(compose_out_amt) AS qdm_compose_out
        FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
        WHERE business_date = '{dt}'
        GROUP BY store_id, business_date, article_id
        """)
        all_qdm.append(batch)
        print(f"{len(batch)} rows")

qdm = pd.concat(all_qdm, ignore_index=True)
print(f"  QDM total: {len(qdm)} rows, dates={qdm['business_date'].nunique()}")

# JOIN dim_goods
qdm = qdm.merge(goods_df, on='article_id', how='left')

# ============================================================
# Step 1: 对 QDM 应用 fmetl 分类重映射
# ============================================================
def remap_fmetl(row):
    c1 = str(row.get('category_level1_description', ''))
    c2 = str(row.get('category_level2_description', ''))
    c3 = str(row.get('category_level3_description', ''))
    c1_id = str(row.get('category_level1_id', ''))
    if c2 == '烘焙类':
        return '烘焙类'
    if '熟食' in c3:
        return '熟食类'
    if c1_id == '24':
        if '蛋品' in c2:
            return '蛋类'
        if '肉禽' in c2:
            return '肉禽类'
        return '肉禽蛋类'
    if c1_id in ('25', '26'):
        return '冷藏加工及预制菜类'
    if c2 == '乳制品及水饮':
        return '乳制品及水饮类'
    return c1

qdm['cat1'] = qdm.apply(remap_fmetl, axis=1)

print(f"\n  QDM 按 fmetl 分类重映射后的分布:")
cats = qdm.groupby('cat1').agg(
    sku_cnt=('article_id', 'nunique'),
    qdm_profit=('qdm_profit', 'sum'),
    qdm_sale=('qdm_sale_amt', 'sum'),
).sort_values('qdm_profit', ascending=False)
for _, r in cats.iterrows():
    print(f"    {r.name:<20}  SKU={int(r['sku_cnt']):>5}  profit={r['qdm_profit']:>12,.0f}  sale={r['qdm_sale']:>12,.0f}")

# ============================================================
# Step 2: fmetl vs QDM(重映射) — 按分类汇总
# ============================================================
print("\n" + "=" * 100)
print("Step 2: fmetl vs QDM(重映射) — 按分类汇总（5月全月）")
print("=" * 100)

qdm_cat = qdm.groupby('cat1').agg(
    qdm_profit=('qdm_profit', 'sum'),
    qdm_sale=('qdm_sale_amt', 'sum'),
    qdm_sale_qty=('qdm_sale_qty', 'sum'),
    qdm_init=('qdm_init', 'sum'),
    qdm_end=('qdm_end', 'sum'),
    qdm_receive=('qdm_receive', 'sum'),
    qdm_klost=('qdm_klost', 'sum'),
    qdm_ulost=('qdm_ulost', 'sum'),
    qdm_compose_in=('qdm_compose_in', 'sum'),
    qdm_compose_out=('qdm_compose_out', 'sum'),
    qdm_sku=('article_id', 'nunique'),
).reset_index()

fmetl_cat = conn.execute(f"""
WITH cat_map AS (
    SELECT DISTINCT article_id,
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
    COALESCE(cm.cat1_name, '其他') AS cat1,
    SUM(s.sale_amt)       AS fmetl_sale,
    SUM(s.sale_qty)       AS fmetl_sale_qty,
    SUM(p.profit_amt)     AS fmetl_profit,
    SUM(s.init_stock_amt) AS fmetl_init,
    SUM(s.end_stock_amt)  AS fmetl_end,
    SUM(s.receive_amt)    AS fmetl_receive,
    SUM(s.know_lost_amt)  AS fmetl_klost,
    SUM(s.unknow_lost_amt) AS fmetl_ulost,
    SUM(s.compose_in_amt) AS fmetl_compose_in,
    SUM(s.compose_out_amt) AS fmetl_compose_out,
    SUM(p.sale_cost_amt)  AS fmetl_sale_cost,
    SUM(p.allowance_amt_profit) AS fmetl_allowance,
    SUM(s.bom_in_amt)     AS fmetl_bom_in,
    SUM(s.bom_out_amt)    AS fmetl_bom_out,
    COUNT(DISTINCT s.article_id) AS fmetl_sku
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date AND s.article_id = p.article_id AND s.day_clear = p.day_clear
LEFT JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
GROUP BY cm.cat1_name
""").df()

# 合并
m = fmetl_cat.merge(qdm_cat, on='cat1', how='outer')
for c in m.columns:
    if m[c].dtype in ('float64', 'int64'):
        m[c] = m[c].fillna(0)
m['profit_diff'] = m['fmetl_profit'] - m['qdm_profit']
m['profit_pct'] = (m['profit_diff'] / m['qdm_profit'].abs().replace(0, np.nan) * 100).round(1)
m['sale_diff'] = m['fmetl_sale'] - m['qdm_sale']
m['sale_pct'] = (m['sale_diff'] / m['qdm_sale'].abs().replace(0, np.nan) * 100).round(1)
m['init_diff'] = m['fmetl_init'] - m['qdm_init']
m['end_diff'] = m['fmetl_end'] - m['qdm_end']
m = m.sort_values('profit_diff', key=abs, ascending=False)

print(f"\n{'分类':<20} {'fmetl利润':>10} {'QDM利润':>10} {'利润差':>10} {'偏差%':>8} {'fmetl销售':>10} {'QDM销售':>10} {'销售差%':>8} {'fmetl期初':>10} {'QDM期初':>10} {'fmetl期末':>10} {'QDM期末':>10} {'fmetl进货':>10} {'QDM进货':>10}")
print('-'*175)
for _, r in m.iterrows():
    print(f"{r['cat1']:<20} {r['fmetl_profit']:>10,.0f} {r['qdm_profit']:>10,.0f} {r['profit_diff']:>10,.0f} {r['profit_pct']:>+7.1f}% {r['fmetl_sale']:>10,.0f} {r['qdm_sale']:>10,.0f} {r['sale_pct']:>+7.1f}% {r['fmetl_init']:>10,.0f} {r['qdm_init']:>10,.0f} {r['fmetl_end']:>10,.0f} {r['qdm_end']:>10,.0f} {r['fmetl_receive']:>10,.0f} {r['qdm_receive']:>10,.0f}")

# 总计
tot = m.sum(numeric_only=True)
pct = (tot['fmetl_profit']-tot['qdm_profit'])/abs(tot['qdm_profit'])*100 if tot['qdm_profit']!=0 else 0
print(f"\n{'─'*175}")
print(f"{'全月总计':<20} {tot['fmetl_profit']:>10,.0f} {tot['qdm_profit']:>10,.0f} {tot['fmetl_profit']-tot['qdm_profit']:>10,.0f} {pct:>+7.1f}% {tot['fmetl_sale']:>10,.0f} {tot['qdm_sale']:>10,.0f} {(tot['fmetl_sale']-tot['qdm_sale'])/abs(tot['qdm_sale'])*100 if tot['qdm_sale']!=0 else 0:>+7.1f}%")

# ============================================================
# Step 3: 按日期和 SKU 查看差异明细
# ============================================================
print("\n\n" + "=" * 100)
print("Step 3: 逐日趋势 + 差异构成分析")
print("=" * 100)

# 找出差异 TOP 分类
top_cats = m[m['qdm_profit'].abs() > 0].head(3)['cat1'].tolist()
print(f"重点分析分类: {top_cats}")

# fmetl 逐日
fmetl_daily = conn.execute(f"""
WITH cat_map AS (
    SELECT DISTINCT article_id,
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
    COALESCE(cm.cat1_name, '其他') AS cat1,
    SUM(s.sale_amt)       AS fmetl_sale,
    SUM(p.profit_amt)     AS fmetl_profit,
    SUM(s.init_stock_amt) AS fmetl_init,
    SUM(s.end_stock_amt)  AS fmetl_end,
    SUM(s.receive_amt)    AS fmetl_receive,
    SUM(s.compose_in_amt) AS fmetl_compose_in,
    SUM(s.compose_out_amt) AS fmetl_compose_out,
    SUM(s.know_lost_amt)  AS fmetl_klost,
    SUM(s.unknow_lost_amt) AS fmetl_ulost,
    SUM(p.sale_cost_amt)  AS fmetl_sale_cost,
    SUM(p.allowance_amt_profit) AS fmetl_allowance,
    COUNT(DISTINCT s.article_id) AS sku_cnt
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date AND s.article_id = p.article_id AND s.day_clear = p.day_clear
LEFT JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
  AND cm.cat1_name IN ({','.join(repr(c) for c in top_cats)})
GROUP BY s.business_date, cm.cat1_name
ORDER BY s.business_date, cm.cat1_name
""").df()

# QDM 逐日 (已重映射)
qdm_daily = qdm[qdm['cat1'].isin(top_cats)].groupby(['business_date', 'cat1']).agg(
    qdm_sale=('qdm_sale_amt', 'sum'),
    qdm_profit=('qdm_profit', 'sum'),
    qdm_init=('qdm_init', 'sum'),
    qdm_end=('qdm_end', 'sum'),
    qdm_receive=('qdm_receive', 'sum'),
    qdm_klost=('qdm_klost', 'sum'),
    qdm_ulost=('qdm_ulost', 'sum'),
    qdm_compose_in=('qdm_compose_in', 'sum'),
    sku_cnt=('article_id', 'nunique'),
).reset_index()

dm = fmetl_daily.merge(qdm_daily, on=['business_date', 'cat1'], how='outer').fillna(0)
dm['profit_diff'] = dm['fmetl_profit'] - dm['qdm_profit']
dm['sale_diff'] = dm['fmetl_sale'] - dm['qdm_sale']
dm['init_diff'] = dm['fmetl_init'] - dm['qdm_init']

for cat in top_cats:
    dcat = dm[dm['cat1'] == cat].sort_values('business_date')
    if dcat.empty:
        continue
    print(f"\n{'─'*140}")
    print(f"【{cat}】逐日利润 + 关键指标对比")
    hdr = (f"{'日期':>12} {'fmetl利润':>10} {'QDM利润':>10} {'差':>10} "
           f"{'fmetl期初':>10} {'QDM期初':>10} {'fmetl期末':>10} {'QDM期末':>10} "
           f"{'fmetl进货':>10} {'QDM进货':>10} {'fmetl加工入':>10} {'QDM加工入':>10} "
           f"{'fmetl未知损':>10} {'QDM未知损':>10} {'fmetl折让':>10}")
    print(hdr)
    print('-'*175)
    for _, r in dcat.iterrows():
        print(f"{r['business_date']:>12} {r['fmetl_profit']:>10,.0f} {r['qdm_profit']:>10,.0f} {r['profit_diff']:>10,.0f} "
              f"{r['fmetl_init']:>10,.0f} {r['qdm_init']:>10,.0f} {r['fmetl_end']:>10,.0f} {r['qdm_end']:>10,.0f} "
              f"{r['fmetl_receive']:>10,.0f} {r['qdm_receive']:>10,.0f} {r['fmetl_compose_in']:>10,.0f} {r['qdm_compose_in']:>10,.0f} "
              f"{r['fmetl_ulost']:>10,.0f} {r['qdm_ulost']:>10,.0f} {r['fmetl_allowance']:>10,.0f}")

# ============================================================
# Step 4: SKU 级差异 TOP 20
# ============================================================
print("\n\n" + "=" * 100)
print("Step 4: SKU 级 — 差异最大 20 个 SKU 的完整画像")
print("=" * 100)

qdm_sku = qdm.groupby(['article_id', 'cat1']).agg(
    qdm_profit=('qdm_profit', 'sum'),
    qdm_sale=('qdm_sale_amt', 'sum'),
    qdm_init=('qdm_init', 'sum'),
    qdm_end=('qdm_end', 'sum'),
    qdm_receive=('qdm_receive', 'sum'),
    qdm_klost=('qdm_klost', 'sum'),
    qdm_ulost=('qdm_ulost', 'sum'),
).reset_index()

fmetl_sku = conn.execute(f"""
WITH cat_map AS (
    SELECT DISTINCT article_id,
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
    s.article_id, g.article_name,
    COALESCE(cm.cat1_name, '其他') AS cat1,
    SUM(p.profit_amt)     AS fmetl_profit,
    SUM(s.sale_amt)       AS fmetl_sale,
    SUM(s.init_stock_amt) AS fmetl_init,
    SUM(s.end_stock_amt)  AS fmetl_end,
    SUM(s.receive_amt)    AS fmetl_receive,
    SUM(s.compose_in_amt) AS fmetl_compose_in,
    SUM(s.know_lost_amt)  AS fmetl_klost,
    SUM(s.unknow_lost_amt) AS fmetl_ulost,
    SUM(p.allowance_amt_profit) AS fmetl_allowance,
    AVG(p.effective_unit_cost) AS avg_euc,
    MAX(p.cost_source) AS cost_source,
    COUNT(DISTINCT s.business_date) AS ndays
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date AND s.article_id = p.article_id AND s.day_clear = p.day_clear
JOIN dim_goods g ON s.article_id = g.article_id
LEFT JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
GROUP BY s.article_id, g.article_name, cm.cat1_name
""").df()

sku_c = fmetl_sku.merge(qdm_sku, on=['article_id', 'cat1'], how='outer').fillna(0)
sku_c['profit_diff'] = sku_c['fmetl_profit'] - sku_c['qdm_profit']
sku_c['sale_diff'] = sku_c['fmetl_sale'] - sku_c['qdm_sale']
sku_c = sku_c.sort_values('profit_diff', key=abs, ascending=False)

print(f"\n{'分类':<18} {'SKU':>10} {'商品名':<26} {'fmetl利润':>10} {'QDM利润':>10} {'利润差':>10} {'fmetl销售':>10} {'QDM销售':>10} {'fmetl期初':>10} {'QDM期初':>10} {'fmetl期末':>10} {'QDM期末':>10} {'cost_src':<22} {'EUC':>7}")
print('-'*185)
for _, r in sku_c.head(20).iterrows():
    name = str(r['article_name'])[:25]
    cs = str(r['cost_source'])[:21] if pd.notna(r['cost_source']) else 'N/A'
    print(f"{r['cat1']:<18} {r['article_id']:>10} {name:<26} {r['fmetl_profit']:>10,.0f} {r['qdm_profit']:>10,.0f} {r['profit_diff']:>10,.0f} {r['fmetl_sale']:>10,.0f} {r['qdm_sale']:>10,.0f} {r['fmetl_init']:>10,.0f} {r['qdm_init']:>10,.0f} {r['fmetl_end']:>10,.0f} {r['qdm_end']:>10,.0f} {cs:<22} {r['avg_euc']:>7.2f}")

# ============================================================
# Step 5: 逐品类差异根因总结
# ============================================================
print("\n\n" + "=" * 100)
print("Step 5: 差异根因总结（按品类）")
print("=" * 100)

# 对于每个有差异的分类，分析差异来源
for _, cr in m.iterrows():
    if abs(cr['profit_diff']) < 100:
        continue
    cat = cr['cat1']
    print(f"\n【{cat}】利润差={cr['profit_diff']:,.0f} ({cr['profit_pct']:+.1f}%)")
    print(f"  销售额对比: fmetl={cr['fmetl_sale']:,.0f}  QDM={cr['qdm_sale']:,.0f}  差={cr['sale_diff']:,.0f} ({cr['sale_pct']:+.1f}%)")
    print(f"  进货额对比: fmetl={cr['fmetl_receive']:,.0f}  QDM={cr['qdm_receive']:,.0f}  差={cr['fmetl_receive']-cr['qdm_receive']:,.0f}")
    print(f"  期初库存差: {cr['init_diff']:,.0f}  |  期末库存差: {cr['fmetl_end']-cr['qdm_end']:,.0f}")
    print(f"  加工转入差: {cr['fmetl_compose_in']-cr['qdm_compose_in']:,.0f}  |  折让(allowance): {cr['fmetl_allowance']:,.0f}")

    # 毛利差异 = (销售差) - (进货差) + (期末差) - (期初差) + (加工入差) + 折让 + 损耗差
    profit_recon = (cr['sale_diff']
                    - (cr['fmetl_receive'] - cr['qdm_receive'])
                    + (cr['fmetl_end'] - cr['qdm_end']) - cr['init_diff']
                    + (cr['fmetl_compose_in'] - cr['qdm_compose_in'])
                    - (cr['fmetl_compose_out'] - cr['qdm_compose_out'])
                    + cr['fmetl_allowance']
                    - (cr['fmetl_klost'] - cr['qdm_klost'])
                    - (cr['fmetl_ulost'] - cr['qdm_ulost']))
    print(f"  毛利差异构成 (近似):")
    print(f"    销售差 ({cr['sale_diff']:,.0f}) + 进货差 ({-(cr['fmetl_receive']-cr['qdm_receive']):,.0f}) + 库存变动差 ({cr['fmetl_end']-cr['qdm_end']-cr['init_diff']:,.0f})")
    print(f"    + 加工入差 ({(cr['fmetl_compose_in']-cr['qdm_compose_in']):,.0f}) + 折让 ({cr['fmetl_allowance']:,.0f})")
    print(f"    + 损耗差 ({-(cr['fmetl_klost']-cr['qdm_klost']-cr['fmetl_ulost']+cr['qdm_ulost']):,.0f})")

    # 库存差异的逐日累计
    if abs(cr['init_diff']) > 1000:
        sub = dm[dm['cat1'] == cat].sort_values('business_date')
        if len(sub) > 0:
            first_day_init_diff = sub.iloc[0]['init_diff'] if len(sub) > 0 else 0
            last_day_end_diff = sub.iloc[-1]['fmetl_end'] - sub.iloc[-1]['qdm_end'] if len(sub) > 0 else 0
            print(f"  首日期初差: {first_day_init_diff:,.0f}  |  末日期末差: {last_day_end_diff:,.0f}")

conn.close()
print("\n\nDone.")
