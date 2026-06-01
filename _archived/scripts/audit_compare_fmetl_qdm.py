"""
fmetl vs QDM 对比审查脚本
日期范围: 2026-05-01 ~ 2026-05-20
"""
import sys
sys.path.insert(0, '.')

import duckdb
import pandas as pd
import numpy as np
from fmetl.connectors import ApiConnector
from fmetl.config import get_settings

START = '2026-05-01'
END = '2026-05-20'

# ============================================================
# 类别重映射函数 (与 sku_dim.py 的 _remap_category 完全一致)
# ============================================================
def remap_category(df):
    """向量化类别重映射。returns df with two new columns:
    category_level1_id_remap, category_level1_description_remap
    """
    c2 = df['category_level2_description'].fillna('')
    c1_desc = df['category_level1_description'].fillna('')
    c3 = df['category_level3_description'].fillna('')
    c1_id = df['category_level1_id'].fillna('')

    is_egg_bake = c2.isin(['蛋类', '烘焙类'])
    is_dairy_drink = c2.isin(['冷藏奶制品类', '饮料类'])
    is_meat = (c1_desc == '肉禽蛋类') & (c2 != '蛋类')
    is_cooked = c3.str.endswith('熟食').fillna(False)
    is_cold_prep = c1_desc.isin(['冷藏及加工类', '预制菜'])
    is_remapped = is_egg_bake | is_dairy_drink | is_meat | is_cooked | is_cold_prep

    df['category_level1_id_remap'] = c1_id.where(~is_remapped, '')
    df['category_level1_description_remap'] = (
        c2.where(is_egg_bake,
        np.where(is_dairy_drink, '乳制品及水饮类',
        np.where(is_meat, '肉禽类',
        np.where(is_cooked, '熟食类',
        np.where(is_cold_prep, '冷藏加工及预制菜类',
                 c1_desc))))))
    return df


# ============================================================
# Part 1: 加载 fmetl 数据 (DuckDB)
# ============================================================
print("=" * 80)
print("Part 1: Loading fmetl data from DuckDB")
print("=" * 80)

conn = duckdb.connect('data/fm.duckdb', read_only=True)

# 1a. t_calc_stock with DISTINCT dedup
print("Loading t_calc_stock (DISTINCT dedup)...")
stock_df = conn.execute(f"""
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY store_id, business_date, article_id, day_clear
            ) AS rn
        FROM t_calc_stock
        WHERE business_date BETWEEN '{START}' AND '{END}'
    ) WHERE rn = 1
""").df()
print(f"  t_calc_stock: {len(stock_df):,} rows")

# 1b. t_calc_profit with DISTINCT dedup
print("Loading t_calc_profit (DISTINCT dedup)...")
profit_df = conn.execute(f"""
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY store_id, business_date, article_id, day_clear
            ) AS rn
        FROM t_calc_profit
        WHERE business_date BETWEEN '{START}' AND '{END}'
    ) WHERE rn = 1
""").df()
print(f"  t_calc_profit: {len(profit_df):,} rows")

# 1c. dim_goods for category info
print("Loading dim_goods...")
goods_df = conn.execute("""
    SELECT DISTINCT article_id, article_name,
        category_level1_id, category_level1_description,
        category_level2_id, category_level2_description,
        category_level3_id, category_level3_description
    FROM dim_goods
""").df()
print(f"  dim_goods: {len(goods_df):,} rows")

# 1d. Merge stock + profit
print("Merging fmetl stock + profit...")
fmetl = stock_df.merge(
    profit_df[['store_id', 'business_date', 'article_id', 'day_clear',
               'profit_amt', 'sale_cost_amt', 'pre_profit_amt',
               'allowance_amt_profit', 'scm_fin_article_profit',
               'full_link_article_profit', 'pre_sale_amt', 'pre_inbound_amount']],
    on=['store_id', 'business_date', 'article_id', 'day_clear'],
    how='left'
)

# Fill NaN numeric with 0
for c in fmetl.columns:
    if fmetl[c].dtype in ('float64', 'float32'):
        fmetl[c] = fmetl[c].fillna(0)

print(f"  Merged fmetl: {len(fmetl):,} rows")

# 1e. Aggregate dc=0+1 by store×date×article
print("Aggregating dc=0+1...")
agg_cols = {
    'sale_qty': 'sum', 'sale_amt': 'sum',
    'receive_qty': 'sum', 'receive_amt': 'sum',
    'init_stock_qty': 'sum', 'init_stock_amt': 'sum',
    'end_stock_qty': 'sum', 'end_stock_amt': 'sum',
    'know_lost_qty': 'sum', 'know_lost_amt': 'sum',
    'unknow_lost_qty': 'sum', 'unknow_lost_amt': 'sum',
    'lost_qty': 'sum', 'lost_amt': 'sum',
    'bom_in_qty': 'sum', 'bom_in_amt': 'sum',
    'bom_out_qty': 'sum', 'bom_out_amt': 'sum',
    'compose_in_qty': 'sum', 'compose_in_amt': 'sum',
    'compose_out_qty': 'sum', 'compose_out_amt': 'sum',
    'profit_amt': 'sum', 'sale_cost_amt': 'sum', 'pre_profit_amt': 'sum',
    'allowance_amt_profit': 'sum', 'scm_fin_article_profit': 'sum',
    'full_link_article_profit': 'sum', 'pre_sale_amt': 'sum',
    'pre_inbound_amount': 'sum',
    'out_stock_pay_amt': 'sum', 'out_stock_pay_amt_notax': 'sum',
    'out_stock_amt_cb': 'sum', 'return_stock_pay_amt_notax': 'sum',
    'scm_promotion_amt_total': 'sum', 'expect_outstock_amt': 'sum',
    'purchase_weight': 'sum',
    'stock_transfer_out_qty': 'sum', 'stock_transfer_out_amt': 'sum',
    'stock_transfer_in_qty': 'sum', 'stock_transfer_in_amt': 'sum',
}
fmetl_agg = fmetl.groupby(['store_id', 'business_date', 'article_id']).agg(agg_cols).reset_index()
print(f"  Aggregated fmetl (dc=0+1): {len(fmetl_agg):,} rows")

# 1f. Join dim_goods for category info
print("Joining dim_goods for fmetl categories...")
fmetl_agg = fmetl_agg.merge(goods_df, on='article_id', how='left')
for c in ['category_level1_id', 'category_level1_description',
          'category_level2_id', 'category_level2_description',
          'category_level3_id', 'category_level3_description', 'article_name']:
    fmetl_agg[c] = fmetl_agg[c].fillna('')

# 1g. Apply category remapping
print("Applying category remapping to fmetl...")
fmetl_agg = remap_category(fmetl_agg)
fmetl_agg['cat_id'] = fmetl_agg['category_level1_id_remap']
fmetl_agg['cat_desc'] = fmetl_agg['category_level1_description_remap']

conn.close()

# ============================================================
# Part 2: 加载 QDM 基准数据 (API)
# ============================================================
print()
print("=" * 80)
print("Part 2: Loading QDM benchmark data via API")
print("=" * 80)

api = ApiConnector(get_settings())

# QDM 基准表: day_clear='1'
# 注意: API 中必须用 IF() 替代 CASE WHEN, IN 列表过大时分批
qdm_sql = f"""
SELECT
    store_id,
    business_date,
    article_id,
    sale_amt, sale_qty,
    receive_amt, receive_qty,
    profit_amt,
    init_stock_amt, end_stock_amt,
    know_lost_amt, unknow_lost_amt,
    compose_in_amt, compose_out_amt
FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di
WHERE business_date BETWEEN '{START}' AND '{END}'
    AND day_clear = '1'
"""

print(f"Querying QDM API ({START} ~ {END}, day_clear='1')...")
print(f"  SQL: {qdm_sql[:200]}...")
qdm_df = api.query(qdm_sql)
print(f"  QDM returned: {len(qdm_df):,} rows")

# Normalize types
for c in ['store_id', 'business_date', 'article_id']:
    if c in qdm_df.columns:
        qdm_df[c] = qdm_df[c].astype(str)

for c in qdm_df.columns:
    if qdm_df[c].dtype in ('float64', 'float32', 'int64', 'int32'):
        qdm_df[c] = qdm_df[c].fillna(0)

# QDM 表无 category 字段, 通过 dim_goods (DuckDB) 补全
print("Joining dim_goods for QDM categories...")
qdm_df = qdm_df.merge(goods_df, on='article_id', how='left')
for c in ['category_level1_id', 'category_level1_description',
          'category_level2_id', 'category_level2_description',
          'category_level3_id', 'category_level3_description', 'article_name']:
    qdm_df[c] = qdm_df[c].fillna('')

# Apply category remapping to QDM
print("Applying category remapping to QDM...")
qdm_df = remap_category(qdm_df)
qdm_df['cat_id'] = qdm_df['category_level1_id_remap']
qdm_df['cat_desc'] = qdm_df['category_level1_description_remap']

print(f"  QDM unique store×date×article: {qdm_df.groupby(['store_id','business_date','article_id']).ngroups:,}")
print(f"  QDM columns: {list(qdm_df.columns)}")

# ============================================================
# Part 3: 合并 fmetl vs QDM
# ============================================================
print()
print("=" * 80)
print("Part 3: Merging fmetl vs QDM by store×article×date")
print("=" * 80)

# Rename fmetl columns to avoid clash
fmetl_renamed = fmetl_agg.rename(columns={
    # Core metrics
    'sale_amt': 'fmetl_sale_amt', 'sale_qty': 'fmetl_sale_qty',
    'receive_amt': 'fmetl_receive_amt', 'receive_qty': 'fmetl_receive_qty',
    'profit_amt': 'fmetl_profit_amt',
    'init_stock_amt': 'fmetl_init_stock_amt',
    'init_stock_qty': 'fmetl_init_stock_qty',
    'end_stock_amt': 'fmetl_end_stock_amt',
    'end_stock_qty': 'fmetl_end_stock_qty',
    'know_lost_amt': 'fmetl_know_lost_amt',
    'know_lost_qty': 'fmetl_know_lost_qty',
    'unknow_lost_amt': 'fmetl_unknow_lost_amt',
    'unknow_lost_qty': 'fmetl_unknow_lost_qty',
    'lost_amt': 'fmetl_lost_amt', 'lost_qty': 'fmetl_lost_qty',
    'bom_in_amt': 'fmetl_bom_in_amt', 'bom_in_qty': 'fmetl_bom_in_qty',
    'bom_out_amt': 'fmetl_bom_out_amt', 'bom_out_qty': 'fmetl_bom_out_qty',
    'compose_in_amt': 'fmetl_compose_in_amt', 'compose_in_qty': 'fmetl_compose_in_qty',
    'compose_out_amt': 'fmetl_compose_out_amt', 'compose_out_qty': 'fmetl_compose_out_qty',
    'sale_cost_amt': 'fmetl_sale_cost_amt',
    'pre_profit_amt': 'fmetl_pre_profit_amt',
    'allowance_amt_profit': 'fmetl_allowance_amt',
    'pre_sale_amt': 'fmetl_pre_sale_amt',
    'pre_inbound_amount': 'fmetl_pre_inbound_amount',
})

qdm_renamed = qdm_df.rename(columns={
    'sale_amt': 'qdm_sale_amt', 'sale_qty': 'qdm_sale_qty',
    'receive_amt': 'qdm_receive_amt', 'receive_qty': 'qdm_receive_qty',
    'profit_amt': 'qdm_profit_amt',
    'init_stock_amt': 'qdm_init_stock_amt',
    'end_stock_amt': 'qdm_end_stock_amt',
    'know_lost_amt': 'qdm_know_lost_amt',
    'unknow_lost_amt': 'qdm_unknow_lost_amt',
    'compose_in_amt': 'qdm_compose_in_amt',
    'compose_out_amt': 'qdm_compose_out_amt',
})

# Select only needed columns from fmetl
fmetl_cols = ['store_id', 'business_date', 'article_id',
              'fmetl_sale_amt', 'fmetl_sale_qty',
              'fmetl_receive_amt', 'fmetl_receive_qty',
              'fmetl_profit_amt',
              'fmetl_init_stock_amt', 'fmetl_init_stock_qty',
              'fmetl_end_stock_amt', 'fmetl_end_stock_qty',
              'fmetl_know_lost_amt', 'fmetl_know_lost_qty',
              'fmetl_unknow_lost_amt', 'fmetl_unknow_lost_qty',
              'fmetl_lost_amt', 'fmetl_lost_qty',
              'fmetl_bom_in_amt', 'fmetl_bom_out_amt',
              'fmetl_compose_in_amt', 'fmetl_compose_out_amt',
              'fmetl_sale_cost_amt', 'fmetl_pre_profit_amt',
              'fmetl_allowance_amt', 'fmetl_pre_sale_amt',
              'fmetl_pre_inbound_amount',
              'cat_id', 'cat_desc']

qdm_cols = ['store_id', 'business_date', 'article_id',
            'qdm_sale_amt', 'qdm_sale_qty',
            'qdm_receive_amt', 'qdm_receive_qty',
            'qdm_profit_amt',
            'qdm_init_stock_amt',
            'qdm_end_stock_amt',
            'qdm_know_lost_amt', 'qdm_unknow_lost_amt',
            'qdm_compose_in_amt', 'qdm_compose_out_amt',
            'cat_id', 'cat_desc']

merged = fmetl_renamed[fmetl_cols].merge(
    qdm_renamed[qdm_cols],
    on=['store_id', 'business_date', 'article_id'],
    how='outer',
    indicator=True
)

# Merge category from whichever side has it
merged['cat_id'] = merged['cat_id_x'].fillna(merged['cat_id_y'])
merged['cat_desc'] = merged['cat_desc_x'].fillna(merged['cat_desc_y'])
merged['cat_id'] = merged['cat_id'].fillna('')
merged['cat_desc'] = merged['cat_desc'].fillna('')
merged.drop(columns=['cat_id_x', 'cat_id_y', 'cat_desc_x', 'cat_desc_y'], inplace=True)

total_merged = len(merged)
both = (merged['_merge'] == 'both').sum()
fmetl_only = (merged['_merge'] == 'left_only').sum()
qdm_only = (merged['_merge'] == 'right_only').sum()

print(f"Total merged rows: {total_merged:,}")
print(f"  Both (common):  {both:,} ({both/total_merged*100:.1f}%)")
print(f"  fmetl only:     {fmetl_only:,} ({fmetl_only/total_merged*100:.1f}%)")
print(f"  QDM only:       {qdm_only:,} ({qdm_only/total_merged*100:.1f}%)")

# Compute differences
both_mask = merged['_merge'] == 'both'
for metric in ['sale_amt', 'sale_qty', 'receive_amt', 'receive_qty',
               'profit_amt', 'init_stock_amt', 'end_stock_amt',
               'know_lost_amt', 'unknow_lost_amt',
               'compose_in_amt', 'compose_out_amt']:
    f_col = f'fmetl_{metric}'
    q_col = f'qdm_{metric}'
    if f_col in merged.columns and q_col in merged.columns:
        merged[f'diff_{metric}'] = np.where(
            both_mask, merged[f_col].fillna(0) - merged[q_col].fillna(0), np.nan
        )

# ============================================================
# Part 4: 按大分类汇总对比
# ============================================================
print()
print("=" * 80)
print("Part 4: Category-level comparison (remapped category_level1)")
print("=" * 80)

# Filter to both sides only for fair comparison
both_df = merged[merged['_merge'] == 'both'].copy()

# Aggregate by category
cat_agg = both_df.groupby(['cat_id', 'cat_desc']).agg(
    fmetl_sale_amt=('fmetl_sale_amt', 'sum'),
    qdm_sale_amt=('qdm_sale_amt', 'sum'),
    fmetl_receive_amt=('fmetl_receive_amt', 'sum'),
    qdm_receive_amt=('qdm_receive_amt', 'sum'),
    fmetl_profit_amt=('fmetl_profit_amt', 'sum'),
    qdm_profit_amt=('qdm_profit_amt', 'sum'),
    fmetl_init_stock_amt=('fmetl_init_stock_amt', 'sum'),
    qdm_init_stock_amt=('qdm_init_stock_amt', 'sum'),
    fmetl_end_stock_amt=('fmetl_end_stock_amt', 'sum'),
    qdm_end_stock_amt=('qdm_end_stock_amt', 'sum'),
    fmetl_know_lost_amt=('fmetl_know_lost_amt', 'sum'),
    qdm_know_lost_amt=('qdm_know_lost_amt', 'sum'),
    fmetl_unknow_lost_amt=('fmetl_unknow_lost_amt', 'sum'),
    qdm_unknow_lost_amt=('qdm_unknow_lost_amt', 'sum'),
    fmetl_compose_in_amt=('fmetl_compose_in_amt', 'sum'),
    qdm_compose_in_amt=('qdm_compose_in_amt', 'sum'),
    fmetl_compose_out_amt=('fmetl_compose_out_amt', 'sum'),
    qdm_compose_out_amt=('qdm_compose_out_amt', 'sum'),
    article_count=('article_id', 'nunique'),
    row_count=('business_date', 'count'),
).reset_index()

# Compute diffs and pct
for metric in ['sale_amt', 'receive_amt', 'profit_amt', 'init_stock_amt',
               'end_stock_amt', 'know_lost_amt', 'unknow_lost_amt',
               'compose_in_amt', 'compose_out_amt']:
    cat_agg[f'diff_{metric}'] = cat_agg[f'fmetl_{metric}'] - cat_agg[f'qdm_{metric}']
    cat_agg[f'pct_{metric}'] = np.where(
        cat_agg[f'qdm_{metric}'].abs() > 1,
        (cat_agg[f'diff_{metric}'] / cat_agg[f'qdm_{metric}'].abs()) * 100,
        np.nan
    )

# Print category comparison table
print()
print(f"{'Category':30s} | {'Sale fmetl':>14s} | {'Sale QDM':>14s} | {'Sale Diff':>14s} | {'Sale Diff%':>10s} | {'Profit fmetl':>14s} | {'Profit QDM':>14s} | {'Profit Diff':>14s} | {'Profit Diff%':>10s} | {'Receive fmetl':>14s} | {'Receive QDM':>14s} | {'Receive Diff':>14s}")
print("-" * 220)

total_row = None
for _, row in cat_agg.sort_values('cat_desc').iterrows():
    cat_name = row['cat_desc'] if row['cat_desc'] else f"ID:{row['cat_id']}"
    if len(cat_name) > 28:
        cat_name = cat_name[:27] + '.'

    sale_diff_pct = f"{row['pct_sale_amt']:.1f}%" if not np.isnan(row['pct_sale_amt']) else 'N/A'
    profit_diff_pct = f"{row['pct_profit_amt']:.1f}%" if not np.isnan(row['pct_profit_amt']) else 'N/A'

    print(f"{cat_name:30s} | {row['fmetl_sale_amt']:>14,.0f} | {row['qdm_sale_amt']:>14,.0f} | {row['diff_sale_amt']:>14,.0f} | {sale_diff_pct:>10s} | {row['fmetl_profit_amt']:>14,.0f} | {row['qdm_profit_amt']:>14,.0f} | {row['diff_profit_amt']:>14,.0f} | {profit_diff_pct:>10s} | {row['fmetl_receive_amt']:>14,.0f} | {row['qdm_receive_amt']:>14,.0f} | {row['diff_receive_amt']:>14,.0f}")

# Total row
total_row = cat_agg.agg({
    'fmetl_sale_amt': 'sum', 'qdm_sale_amt': 'sum', 'diff_sale_amt': 'sum',
    'fmetl_receive_amt': 'sum', 'qdm_receive_amt': 'sum', 'diff_receive_amt': 'sum',
    'fmetl_profit_amt': 'sum', 'qdm_profit_amt': 'sum', 'diff_profit_amt': 'sum',
    'fmetl_init_stock_amt': 'sum', 'qdm_init_stock_amt': 'sum', 'diff_init_stock_amt': 'sum',
    'fmetl_end_stock_amt': 'sum', 'qdm_end_stock_amt': 'sum', 'diff_end_stock_amt': 'sum',
    'fmetl_know_lost_amt': 'sum', 'qdm_know_lost_amt': 'sum', 'diff_know_lost_amt': 'sum',
    'fmetl_unknow_lost_amt': 'sum', 'qdm_unknow_lost_amt': 'sum', 'diff_unknow_lost_amt': 'sum',
    'fmetl_compose_in_amt': 'sum', 'qdm_compose_in_amt': 'sum', 'diff_compose_in_amt': 'sum',
    'fmetl_compose_out_amt': 'sum', 'qdm_compose_out_amt': 'sum', 'diff_compose_out_amt': 'sum',
})
print("-" * 220)
total_sale_pct = (total_row['diff_sale_amt'] / abs(total_row['qdm_sale_amt']) * 100) if abs(total_row['qdm_sale_amt']) > 1 else float('nan')
total_profit_pct = (total_row['diff_profit_amt'] / abs(total_row['qdm_profit_amt']) * 100) if abs(total_row['qdm_profit_amt']) > 1 else float('nan')
total_recv_pct = (total_row['diff_receive_amt'] / abs(total_row['qdm_receive_amt']) * 100) if abs(total_row['qdm_receive_amt']) > 1 else float('nan')
print(f"{'TOTAL':30s} | {total_row['fmetl_sale_amt']:>14,.0f} | {total_row['qdm_sale_amt']:>14,.0f} | {total_row['diff_sale_amt']:>14,.0f} | {total_sale_pct:>9.1f}% | {total_row['fmetl_profit_amt']:>14,.0f} | {total_row['qdm_profit_amt']:>14,.0f} | {total_row['diff_profit_amt']:>14,.0f} | {total_profit_pct:>9.1f}% | {total_row['fmetl_receive_amt']:>14,.0f} | {total_row['qdm_receive_amt']:>14,.0f} | {total_row['diff_receive_amt']:>14,.0f}")

# ============================================================
# Part 5: 中间指标明细表 (全局汇总)
# ============================================================
print()
print("=" * 80)
print("Part 5: Global intermediate metrics summary")
print("=" * 80)

both_df_r = both_df.copy()

# Compute per-row differences for all metrics
all_metrics = {
    'sale_amt': 'Sales Amount',
    'sale_qty': 'Sales Quantity',
    'receive_amt': 'Receive Amount',
    'receive_qty': 'Receive Quantity',
    'profit_amt': 'Profit Amount',
    'init_stock_amt': 'Init Stock Amount',
    'init_stock_qty': 'Init Stock Quantity',
    'end_stock_amt': 'End Stock Amount',
    'end_stock_qty': 'End Stock Quantity',
    'know_lost_amt': 'Known Lost Amount',
    'know_lost_qty': 'Known Lost Quantity',
    'unknow_lost_amt': 'Unknown Lost Amount',
    'unknow_lost_qty': 'Unknown Lost Quantity',
    'lost_amt': 'Lost Amount (total)',
    'lost_qty': 'Lost Quantity (total)',
    'compose_in_amt': 'Compose In Amount',
    'compose_out_amt': 'Compose Out Amount',
    'sale_cost_amt': 'Sale Cost Amount',
}

print()
print(f"{'Metric':25s} | {'fmetl Total':>16s} | {'QDM Total':>16s} | {'Diff':>16s} | {'Diff %':>10s} | {'fmetl Mean':>16s} | {'QDM Mean':>16s} | {'Corr':>8s}")
print("-" * 180)

for metric_key, metric_name in all_metrics.items():
    f_col = f'fmetl_{metric_key}'
    q_col = f'qdm_{metric_key}'

    if f_col not in both_df_r.columns:
        print(f"{metric_name:25s} | {'N/A (no fmetl column)':>16s}")
        continue

    f_sum = both_df_r[f_col].sum()

    if q_col not in both_df_r.columns:
        print(f"{metric_name:25s} | {f_sum:>16,.0f} | {'N/A (no QDM column)':>16s}")
        continue

    q_sum = both_df_r[q_col].sum()
    diff_sum = f_sum - q_sum
    diff_pct = (diff_sum / abs(q_sum) * 100) if abs(q_sum) > 1 else float('nan')
    f_mean = both_df_r[f_col].mean()
    q_mean = both_df_r[q_col].mean()

    # Correlation
    mask = (both_df_r[f_col].notna()) & (both_df_r[q_col].notna())
    corr = both_df_r.loc[mask, f_col].corr(both_df_r.loc[mask, q_col]) if mask.sum() > 2 else float('nan')

    pct_str = f"{diff_pct:.2f}%" if not np.isnan(diff_pct) else 'N/A'
    corr_str = f"{corr:.4f}" if not np.isnan(corr) else 'N/A'
    print(f"{metric_name:25s} | {f_sum:>16,.0f} | {q_sum:>16,.0f} | {diff_sum:>16,.0f} | {pct_str:>10s} | {f_mean:>16,.2f} | {q_mean:>16,.2f} | {corr_str:>8s}")

# ============================================================
# Part 6: BOM-only metrics (fmetl has bom_in/bom_out, QDM v4 does not)
# ============================================================
print()
print("=" * 80)
print("Part 6: fmetl-only BOM metrics (QDM v4 has no bom_in/bom_out)")
print("=" * 80)

bom_metrics = {
    'bom_in_amt': 'BOM In Amount',
    'bom_in_qty': 'BOM In Quantity',
    'bom_out_amt': 'BOM Out Amount',
    'bom_out_qty': 'BOM Out Quantity',
}

for metric_key, metric_name in bom_metrics.items():
    f_col = f'fmetl_{metric_key}'
    if f_col in both_df_r.columns:
        f_sum = both_df_r[f_col].sum()
        print(f"  {metric_name:25s}: {f_sum:>16,.0f}")

# BOM symmetry check
bom_in_sum = both_df_r['fmetl_bom_in_amt'].sum() if 'fmetl_bom_in_amt' in both_df_r.columns else 0
bom_out_sum = both_df_r['fmetl_bom_out_amt'].sum() if 'fmetl_bom_out_amt' in both_df_r.columns else 0
print(f"  BOM Symmetry (in - out): {bom_in_sum - bom_out_sum:,.0f}")

# ============================================================
# Part 7: Store-level profit comparison
# ============================================================
print()
print("=" * 80)
print("Part 7: Store-level profit comparison (top/bottom 10 by diff)")
print("=" * 80)

store_agg = both_df.groupby('store_id').agg(
    fmetl_profit=('fmetl_profit_amt', 'sum'),
    qdm_profit=('qdm_profit_amt', 'sum'),
).reset_index()
store_agg['diff'] = store_agg['fmetl_profit'] - store_agg['qdm_profit']
store_agg['diff_pct'] = np.where(
    store_agg['qdm_profit'].abs() > 1,
    store_agg['diff'] / store_agg['qdm_profit'].abs() * 100,
    np.nan
)

# Load store names
conn2 = duckdb.connect('data/fm.duckdb', read_only=True)
store_names = conn2.execute("SELECT DISTINCT store_id, store_name FROM dim_store_profile").df()
conn2.close()
store_agg = store_agg.merge(store_names, on='store_id', how='left')
store_agg['store_name'] = store_agg['store_name'].fillna('')

print()
print(f"{'Store':10s} {'Store Name':20s} | {'fmetl Profit':>16s} | {'QDM Profit':>16s} | {'Diff':>16s} | {'Diff %':>10s}")
print("-" * 120)
for _, row in store_agg.sort_values('diff').head(10).iterrows():
    name = row['store_name'][:18] if row['store_name'] else ''
    pct_str = f"{row['diff_pct']:.1f}%" if not np.isnan(row['diff_pct']) else 'N/A'
    print(f"{row['store_id']:10s} {name:20s} | {row['fmetl_profit']:>16,.0f} | {row['qdm_profit']:>16,.0f} | {row['diff']:>16,.0f} | {pct_str:>10s}")

print("...")
for _, row in store_agg.sort_values('diff', ascending=False).head(10).iterrows():
    name = row['store_name'][:18] if row['store_name'] else ''
    pct_str = f"{row['diff_pct']:.1f}%" if not np.isnan(row['diff_pct']) else 'N/A'
    print(f"{row['store_id']:10s} {name:20s} | {row['fmetl_profit']:>16,.0f} | {row['qdm_profit']:>16,.0f} | {row['diff']:>16,.0f} | {pct_str:>10s}")

# ============================================================
# Part 8: Date-level trend
# ============================================================
print()
print("=" * 80)
print("Part 8: Date-level profit trend")
print("=" * 80)

date_agg = both_df.groupby('business_date').agg(
    fmetl_profit=('fmetl_profit_amt', 'sum'),
    qdm_profit=('qdm_profit_amt', 'sum'),
    fmetl_sale=('fmetl_sale_amt', 'sum'),
    qdm_sale=('qdm_sale_amt', 'sum'),
).reset_index()
date_agg['profit_diff'] = date_agg['fmetl_profit'] - date_agg['qdm_profit']
date_agg['profit_diff_pct'] = np.where(
    date_agg['qdm_profit'].abs() > 1,
    date_agg['profit_diff'] / date_agg['qdm_profit'].abs() * 100,
    np.nan
)
date_agg['sale_diff'] = date_agg['fmetl_sale'] - date_agg['qdm_sale']
date_agg['sale_diff_pct'] = np.where(
    date_agg['qdm_sale'].abs() > 1,
    date_agg['sale_diff'] / date_agg['qdm_sale'].abs() * 100,
    np.nan
)

print()
print(f"{'Date':12s} | {'Profit fmetl':>16s} | {'Profit QDM':>16s} | {'Profit Diff':>16s} | {'Profit%':>9s} | {'Sale fmetl':>16s} | {'Sale QDM':>16s} | {'Sale Diff':>16s} | {'Sale%':>9s}")
print("-" * 160)
for _, row in date_agg.sort_values('business_date').iterrows():
    pp = f"{row['profit_diff_pct']:.1f}%" if not np.isnan(row['profit_diff_pct']) else 'N/A'
    sp = f"{row['sale_diff_pct']:.1f}%" if not np.isnan(row['sale_diff_pct']) else 'N/A'
    print(f"{row['business_date']:12s} | {row['fmetl_profit']:>16,.0f} | {row['qdm_profit']:>16,.0f} | {row['profit_diff']:>16,.0f} | {pp:>9s} | {row['fmetl_sale']:>16,.0f} | {row['qdm_sale']:>16,.0f} | {row['sale_diff']:>16,.0f} | {sp:>9s}")

# ============================================================
# Part 9: Summary verdict
# ============================================================
print()
print("=" * 80)
print("Part 9: Review Verdict")
print("=" * 80)

# Check key acceptance criteria
profit_diff_total = total_row['diff_profit_amt']
profit_qdm_total = total_row['qdm_profit_amt']
profit_pct = abs(profit_diff_total / profit_qdm_total * 100) if abs(profit_qdm_total) > 1 else float('nan')

sale_diff_total = total_row['diff_sale_amt']
sale_qdm_total = total_row['qdm_sale_amt']
sale_pct = abs(sale_diff_total / sale_qdm_total * 100) if abs(sale_qdm_total) > 1 else float('nan')

print(f"  Overall Profit: fmetl={total_row['fmetl_profit_amt']:,.0f}, QDM={profit_qdm_total:,.0f}, diff={profit_diff_total:,.0f} ({profit_pct:.2f}%)")
print(f"  Overall Sales:  fmetl={total_row['fmetl_sale_amt']:,.0f}, QDM={sale_qdm_total:,.0f}, diff={sale_diff_total:,.0f} ({sale_pct:.2f}%)")
print(f"  Overall Receive: fmetl={total_row['fmetl_receive_amt']:,.0f}, QDM={total_row['qdm_receive_amt']:,.0f}, diff={total_row['diff_receive_amt']:,.0f}")

# Category-level outlier check (>5% deviation)
print()
print("  Categories with >5% profit deviation:")
outlier_count = 0
for _, row in cat_agg.sort_values('cat_desc').iterrows():
    if not np.isnan(row['pct_profit_amt']) and abs(row['pct_profit_amt']) > 5 and abs(row['qdm_profit_amt']) > 1000:
        cat_name = row['cat_desc'] if row['cat_desc'] else f"ID:{row['cat_id']}"
        print(f"    WARNING: {cat_name:30s} profit diff={row['diff_profit_amt']:>14,.0f} ({row['pct_profit_amt']:>6.1f}%)")
        outlier_count += 1
if outlier_count == 0:
    print("    None found (all within 5% or low absolute amounts)")

# Negative stock check (from DuckDB)
conn3 = duckdb.connect('data/fm.duckdb', read_only=True)
neg_stock = conn3.execute(f"SELECT COUNT(*) FROM t_calc_stock WHERE end_stock_qty < 0 AND business_date BETWEEN '{START}' AND '{END}'").fetchone()[0]
conn3.close()
print()
print(f"  Negative stock check (t_calc_stock): {neg_stock} rows with end_stock_qty < 0")

# Overall verdict
print()
if not np.isnan(profit_pct) and profit_pct <= 5:
    print("  VERDICT: PASS - Overall profit deviation within 5% tolerance")
elif not np.isnan(profit_pct) and profit_pct <= 10:
    print("  VERDICT: WARNING - Overall profit deviation within 5-10%, requires review")
else:
    print(f"  VERDICT: FAIL - Overall profit deviation {profit_pct:.2f}% exceeds 10% tolerance")

print()
print("Done.")
