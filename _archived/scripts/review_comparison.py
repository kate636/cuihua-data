#!/usr/bin/env python3
"""
fmetl v10 vs QDM 基准表 对比审查
日期: 2026-05-01 ~ 2026-05-24
输出: 纯文本结论 (不做HTML)
"""

import duckdb
import pandas as pd
import numpy as np
import sys
from datetime import date, timedelta

from fmetl.connectors.api_connector import ApiConnector
from fmetl.config.settings import get_settings

START = '2026-05-01'
END = '2026-05-24'
DUCKDB_PATH = 'data/fm.duckdb'


# ============================================================
# 分类重映射 (与 sku_dim.py _remap_category 一致)
# ============================================================
def remap_category(c1, c2, c3):
    c1, c2, c3 = str(c1 or ''), str(c2 or ''), str(c3 or '')
    if c2 in ('蛋类', '烘焙类'):
        return c2
    if c2 in ('冷藏奶制品类', '饮料类'):
        return '乳制品及水饮类'
    if c1 == '肉禽蛋类' and c2 != '蛋类':
        return '肉禽类'
    if c3.endswith('熟食'):
        return '熟食类'
    if c1 in ('冷藏及加工类', '预制菜'):
        return '冷藏加工及预制菜类'
    return c1 or '(无分类)'


# ============================================================
# 1. 加载 fmetl 数据
# ============================================================
def load_fmetl():
    print("=" * 70)
    print("1. 加载 fmetl 数据 (DuckDB)")
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)

    # dim_goods
    goods = conn.execute("""
        SELECT DISTINCT
            article_id, article_name,
            category_level1_id, category_level1_description,
            category_level2_id, category_level2_description,
            category_level3_id, category_level3_description
        FROM dim_goods
    """).df()
    print(f"   dim_goods: {len(goods)} articles")

    # t_calc_stock (无需去重, 已确认唯一)
    stock = conn.execute(f"""
        SELECT
            store_id, business_date, article_id, day_clear,
            sale_qty, sale_amt,
            receive_qty, receive_amt,
            bom_in_qty, bom_in_amt,
            bom_out_qty, bom_out_amt,
            compose_in_qty, compose_in_amt,
            compose_out_qty, compose_out_amt,
            know_lost_qty, know_lost_amt,
            unknow_lost_qty, unknow_lost_amt,
            lost_qty, lost_amt,
            init_stock_qty, init_stock_amt,
            end_stock_qty, end_stock_amt,
            stock_transfer_out_qty, stock_transfer_out_amt,
            stock_transfer_in_qty, stock_transfer_in_amt
        FROM t_calc_stock
        WHERE business_date BETWEEN '{START}' AND '{END}'
    """).df()
    print(f"   t_calc_stock: {len(stock)} rows, dc=0:{len(stock[stock.day_clear=='0'])} dc=1:{len(stock[stock.day_clear=='1'])}")

    # t_calc_profit
    profit = conn.execute(f"""
        SELECT
            store_id, business_date, article_id, day_clear,
            profit_amt, sale_cost_amt, pre_profit_amt,
            scm_fin_article_profit, full_link_article_profit
        FROM t_calc_profit
        WHERE business_date BETWEEN '{START}' AND '{END}'
    """).df()
    print(f"   t_calc_profit: {len(profit)} rows")

    conn.close()

    # Merge stock + profit
    keys = ['store_id', 'business_date', 'article_id', 'day_clear']
    df = stock.merge(profit, on=keys, how='left')

    # Merge dim_goods
    df = df.merge(goods, on='article_id', how='left')
    for c in ['article_name', 'category_level1_id', 'category_level1_description',
              'category_level2_description', 'category_level3_description']:
        df[c] = df[c].fillna('')

    # Category remap
    df['remap_cat'] = df.apply(
        lambda r: remap_category(
            r['category_level1_description'],
            r['category_level2_description'],
            r['category_level3_description']
        ), axis=1
    )

    # ---- Aggregate: SUM dc=0+1 by (store_id, article_id, business_date) ----
    agg_spec = {
        'sale_qty': 'sum', 'sale_amt': 'sum',
        'receive_qty': 'sum', 'receive_amt': 'sum',
        'bom_in_qty': 'sum', 'bom_in_amt': 'sum',
        'bom_out_qty': 'sum', 'bom_out_amt': 'sum',
        'compose_in_qty': 'sum', 'compose_in_amt': 'sum',
        'compose_out_qty': 'sum', 'compose_out_amt': 'sum',
        'know_lost_qty': 'sum', 'know_lost_amt': 'sum',
        'unknow_lost_qty': 'sum', 'unknow_lost_amt': 'sum',
        'lost_qty': 'sum', 'lost_amt': 'sum',
        'init_stock_qty': 'sum', 'init_stock_amt': 'sum',
        'end_stock_qty': 'sum', 'end_stock_amt': 'sum',
        'stock_transfer_out_qty': 'sum', 'stock_transfer_out_amt': 'sum',
        'stock_transfer_in_qty': 'sum', 'stock_transfer_in_amt': 'sum',
        'profit_amt': 'sum', 'sale_cost_amt': 'sum',
        'pre_profit_amt': 'sum',
        'scm_fin_article_profit': 'sum', 'full_link_article_profit': 'sum',
    }
    grp_keys = ['store_id', 'business_date', 'article_id']

    # 先取每 article 的 remap_cat 信息
    info_cols = ['article_id', 'article_name', 'remap_cat',
                 'category_level1_id', 'category_level1_description',
                 'category_level2_description', 'category_level3_description']
    info = df[info_cols].drop_duplicates('article_id')

    fm = df.groupby(grp_keys, as_index=False).agg(agg_spec)
    fm = fm.merge(info, on='article_id', how='left')

    print(f"   fmetl aggregated (dc=0+1): {len(fm)} rows, "
          f"{fm['article_id'].nunique()} SKUs, {fm['business_date'].nunique()} days")

    return fm


# ============================================================
# 2. 加载 QDM 数据
# ============================================================
def load_qdm():
    print("\n" + "=" * 70)
    print("2. 加载 QDM 基准数据 (API -> dal_transaction_*)")

    cfg = get_settings()
    api = ApiConnector(cfg)

    qdm_fields = [
        'store_id', 'article_id', 'business_date',
        'sale_amt', 'sale_qty',
        'receive_amt', 'receive_qty',
        'compose_in_amt', 'compose_in_qty',
        'compose_out_amt', 'compose_out_qty',
        'init_stock_amt', 'init_stock_qty',
        'end_stock_amt', 'end_stock_qty',
        'know_lost_amt', 'know_lost_qty',
        'unknow_lost_amt', 'unknow_lost_qty',
        'profit_amt',
    ]
    cols = ', '.join(qdm_fields)

    dates = []
    s = date.fromisoformat(START)
    e = date.fromisoformat(END)
    for n in range((e - s).days + 1):
        dates.append((s + timedelta(days=n)).isoformat())

    all_dfs = []
    failed_days = []

    for i, d in enumerate(dates):
        # WAF-safe SQL: no CASE WHEN, just simple SELECT WHERE
        sql = (
            f"SELECT {cols} "
            f"FROM default_catalog.ads_business_analysis.dal_transaction_chdj_store_sale_article_sale_info_di "
            f"WHERE day_clear = '1' AND inc_day = '{d}'"
        )
        try:
            df = api.query(sql)
            all_dfs.append(df)
            if (i + 1) % 5 == 0 or i == len(dates) - 1:
                print(f"  [{i+1}/{len(dates)}] {d}: {len(df)} rows")
        except Exception as ex:
            failed_days.append(d)
            print(f"  [{i+1}/{len(dates)}] {d}: FAILED - {ex}")

    if not all_dfs:
        print("ERROR: 所有 QDM 查询均失败, 无法继续")
        return None

    if failed_days:
        print(f"WARNING: {len(failed_days)} 天查询失败: {failed_days}")

    qdm = pd.concat(all_dfs, ignore_index=True)
    print(f"   QDM row count: {len(qdm)}, dates: {qdm['business_date'].nunique()}, "
          f"stores: {qdm['store_id'].nunique()}, SKUs: {qdm['article_id'].nunique()}")

    # Join dim_goods for category
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    goods = conn.execute("""
        SELECT DISTINCT
            article_id, article_name,
            category_level1_id, category_level1_description,
            category_level2_id, category_level2_description,
            category_level3_id, category_level3_description
        FROM dim_goods
    """).df()
    conn.close()

    qdm = qdm.merge(goods, on='article_id', how='left')
    for c in ['article_name', 'category_level1_id', 'category_level1_description',
              'category_level2_description', 'category_level3_description']:
        qdm[c] = qdm[c].fillna('')

    qdm['remap_cat'] = qdm.apply(
        lambda r: remap_category(
            r['category_level1_description'],
            r['category_level2_description'],
            r['category_level3_description']
        ), axis=1
    )
    print(f"   QDM with categories: {len(qdm)} rows")

    return qdm


# ============================================================
# 3. 合并对比
# ============================================================
def compare(fm, qdm):
    print("\n" + "=" * 70)
    print("3. 合并对比")

    # fmetl: rename value columns with _f suffix
    info_cols = {'store_id', 'business_date', 'article_id',
                 'article_name', 'remap_cat',
                 'category_level1_id', 'category_level1_description',
                 'category_level2_description', 'category_level3_description'}
    fm_renamed = fm.rename(
        columns={c: c + '_f' for c in fm.columns if c not in info_cols})

    # qdm: rename value columns with _q suffix
    qdm_renamed = qdm.rename(
        columns={c: c + '_q' for c in qdm.columns if c not in info_cols})

    merge_keys = ['store_id', 'business_date', 'article_id']

    merged = fm_renamed.merge(qdm_renamed, on=merge_keys, how='outer', suffixes=('_f', '_q'), indicator=True)
    print(f"   合并后总行数: {len(merged)}")

    common = merged[merged['_merge'] == 'both']
    fm_only = merged[merged['_merge'] == 'left_only']
    qdm_only = merged[merged['_merge'] == 'right_only']

    print(f"   共同: {len(common)} rows ({common['article_id'].nunique()} SKUs)")
    print(f"   fmetl独有: {len(fm_only)} rows ({fm_only['article_id'].nunique()} SKUs)")
    print(f"   QDM独有: {len(qdm_only)} rows ({qdm_only['article_id'].nunique()} SKUs)")

    # 选择 category 来源: fmetl 优先, 否则 QDM
    common['remap_cat'] = common['remap_cat_f'].fillna(common['remap_cat_q'])
    common['article_name'] = common['article_name_f'].fillna(common['article_name_q'])
    fm_only['remap_cat'] = fm_only['remap_cat_f']
    qdm_only['remap_cat'] = qdm_only['remap_cat_q']

    return merged, common, fm_only, qdm_only


# ============================================================
# 4. 大分类汇总对比
# ============================================================
def category_summary(common):
    print("\n" + "=" * 70)
    print("4. 按大分类(remap_cat) 汇总对比")
    print("=" * 70)

    # sale_amt comparison
    print("\n--- 销售额 (sale_amt) ---")
    cats = sorted(common['remap_cat'].dropna().unique())
    rows_data = []
    for cat in cats:
        sub = common[common['remap_cat'] == cat]
        f_val = sub['sale_amt_f'].sum()
        q_val = sub['sale_amt_q'].sum()
        diff = f_val - q_val
        pct = diff / abs(q_val) * 100 if abs(q_val) > 0.01 else None
        rows_data.append([cat, f_val, q_val, diff, pct, len(sub)])
    rows_data.sort(key=lambda x: abs(x[3]), reverse=True)

    print(f"{'分类':<20s} {'fmetl':>14s} {'QDM':>14s} {'差异':>14s} {'差异%':>8s} {'行数':>6s}")
    print("-" * 80)
    total_f, total_q, total_diff = 0, 0, 0
    for r in rows_data:
        cat, fv, qv, d, p, n = r
        total_f += fv; total_q += qv; total_diff += d
        pct_str = f"{p:.2f}%" if p is not None else "N/A"
        print(f"{cat:<20s} {fv:>14.2f} {qv:>14.2f} {d:>14.2f} {pct_str:>8s} {n:>6d}")
    total_pct = total_diff / abs(total_q) * 100 if abs(total_q) > 0.01 else 0
    print("-" * 80)
    print(f"{'TOTAL':<20s} {total_f:>14.2f} {total_q:>14.2f} {total_diff:>14.2f} {total_pct:>7.2f}%")
    print()

    # receive_amt comparison
    print("\n--- 进货 (receive_amt) ---")
    rows_data2 = []
    for cat in cats:
        sub = common[common['remap_cat'] == cat]
        f_val = sub['receive_amt_f'].sum()
        q_val = sub['receive_amt_q'].sum()
        diff = f_val - q_val
        pct = diff / abs(q_val) * 100 if abs(q_val) > 0.01 else None
        rows_data2.append([cat, f_val, q_val, diff, pct, len(sub)])
    rows_data2.sort(key=lambda x: abs(x[3]), reverse=True)

    print(f"{'分类':<20s} {'fmetl':>14s} {'QDM':>14s} {'差异':>14s} {'差异%':>8s} {'行数':>6s}")
    print("-" * 80)
    total_f, total_q, total_diff = 0, 0, 0
    for r in rows_data2:
        cat, fv, qv, d, p, n = r
        total_f += fv; total_q += qv; total_diff += d
        pct_str = f"{p:.2f}%" if p is not None else "N/A"
        print(f"{cat:<20s} {fv:>14.2f} {qv:>14.2f} {d:>14.2f} {pct_str:>8s} {n:>6d}")
    total_pct = total_diff / abs(total_q) * 100 if abs(total_q) > 0.01 else 0
    print("-" * 80)
    print(f"{'TOTAL':<20s} {total_f:>14.2f} {total_q:>14.2f} {total_diff:>14.2f} {total_pct:>7.2f}%")
    print()

    # profit_amt comparison
    print("\n--- 门店毛利 (profit_amt) ---")
    rows_data3 = []
    for cat in cats:
        sub = common[common['remap_cat'] == cat]
        f_val = sub['profit_amt_f'].sum()
        q_val = sub['profit_amt_q'].sum()
        diff = f_val - q_val
        pct = diff / abs(q_val) * 100 if abs(q_val) > 0.01 else None
        rows_data3.append([cat, f_val, q_val, diff, pct, len(sub)])
    rows_data3.sort(key=lambda x: abs(x[3]), reverse=True)

    print(f"{'分类':<20s} {'fmetl':>14s} {'QDM':>14s} {'差异':>14s} {'差异%':>8s} {'行数':>6s}")
    print("-" * 80)
    total_f, total_q, total_diff = 0, 0, 0
    for r in rows_data3:
        cat, fv, qv, d, p, n = r
        total_f += fv; total_q += qv; total_diff += d
        pct_str = f"{p:.2f}%" if p is not None else "N/A"
        print(f"{cat:<20s} {fv:>14.2f} {qv:>14.2f} {d:>14.2f} {pct_str:>8s} {n:>6d}")
    total_pct = total_diff / abs(total_q) * 100 if abs(total_q) > 0.01 else 0
    print("-" * 80)
    print(f"{'TOTAL':<20s} {total_f:>14.2f} {total_q:>14.2f} {total_diff:>14.2f} {total_pct:>7.2f}%")


# ============================================================
# 5. 全局汇总所有中间指标
# ============================================================
def global_summary(common):
    print("\n" + "=" * 70)
    print("5. 全局汇总: 所有中间指标 fmetl vs QDM")
    print("=" * 70)

    pairs = [
        ('销售额',        'sale_amt_f',         'sale_amt_q',         'sale_qty_f',         'sale_qty_q'),
        ('进货',          'receive_amt_f',      'receive_amt_q',      'receive_qty_f',      'receive_qty_q'),
        ('加工入',        'compose_in_amt_f',   'compose_in_amt_q',   'compose_in_qty_f',   'compose_in_qty_q'),
        ('加工出',        'compose_out_amt_f',  'compose_out_amt_q',  'compose_out_qty_f',  'compose_out_qty_q'),
        ('期初库存',      'init_stock_amt_f',   'init_stock_amt_q',   'init_stock_qty_f',   'init_stock_qty_q'),
        ('期末库存',      'end_stock_amt_f',    'end_stock_amt_q',    'end_stock_qty_f',    'end_stock_qty_q'),
        ('已知损耗',      'know_lost_amt_f',    'know_lost_amt_q',    'know_lost_qty_f',    'know_lost_qty_q'),
        ('未知损耗',      'unknow_lost_amt_f',  'unknow_lost_amt_q',  'unknow_lost_qty_f',  'unknow_lost_qty_q'),
        ('BOM入(仅fmetl)', 'bom_in_amt_f',      None,                 'bom_in_qty_f',       None),
        ('BOM出(仅fmetl)', 'bom_out_amt_f',     None,                 'bom_out_qty_f',      None),
        ('库存转出(仅fmetl)', 'stock_transfer_out_amt_f', None,     'stock_transfer_out_qty_f', None),
        ('库存转入(仅fmetl)', 'stock_transfer_in_amt_f',  None,     'stock_transfer_in_qty_f',  None),
        ('门店毛利',      'profit_amt_f',       'profit_amt_q',       None,                  None),
        ('销售成本(仅fmetl)', 'sale_cost_amt_f', None,                None,                  None),
    ]

    print(f"\n{'指标':<20s} {'fmetl金额':>14s} {'QDM金额':>14s} {'差异金额':>14s} {'差异%':>8s}  "
          f"{'fmetl数量':>14s} {'QDM数量':>14s} {'差异数量':>14s} {'差异%':>8s}")
    print("-" * 120)

    for label, fa, qa, fq, qq in pairs:
        f_amt = common[fa].sum() if fa and fa in common.columns else None
        q_amt = common[qa].sum() if qa and qa in common.columns else None
        f_qty = common[fq].sum() if fq and fq in common.columns else None
        q_qty = common[qq].sum() if qq and qq in common.columns else None

        if f_amt is not None and q_amt is not None:
            d_amt = f_amt - q_amt
            p_amt = d_amt / abs(q_amt) * 100 if abs(q_amt) > 0.01 else 0
        else:
            d_amt = None
            p_amt = None

        if f_qty is not None and q_qty is not None:
            d_qty = f_qty - q_qty
            p_qty = d_qty / abs(q_qty) * 100 if abs(q_qty) > 0.001 else 0
        else:
            d_qty = None
            p_qty = None

        fa_str = f"{f_amt:>14.2f}" if f_amt is not None else f"{'N/A':>14s}"
        qa_str = f"{q_amt:>14.2f}" if q_amt is not None else f"{'N/A':>14s}"
        da_str = f"{d_amt:>14.2f}" if d_amt is not None else f"{'N/A':>14s}"
        pa_str = f"{p_amt:>7.2f}%" if p_amt is not None else f"{'N/A':>8s}"

        fq_str = f"{f_qty:>14.2f}" if f_qty is not None else f"{'N/A':>14s}"
        qq_str = f"{q_qty:>14.2f}" if q_qty is not None else f"{'N/A':>14s}"
        dq_str = f"{d_qty:>14.2f}" if d_qty is not None else f"{'N/A':>14s}"
        pq_str = f"{p_qty:>7.2f}%" if p_qty is not None else f"{'N/A':>8s}"

        print(f"{label:<20s} {fa_str} {qa_str} {da_str} {pa_str}  {fq_str} {qq_str} {dq_str} {pq_str}")

    # 库存方程验证 (fmetl侧, 数量)
    print("\n--- fmetl 库存方程验证 (数量) ---")
    eq_qty = (common['init_stock_qty_f'].sum()
              + common['receive_qty_f'].sum()
              + common['bom_in_qty_f'].sum()
              - common['bom_out_qty_f'].sum()
              + common['compose_in_qty_f'].sum()
              - common['compose_out_qty_f'].sum()
              - common['sale_qty_f'].sum()
              - common['know_lost_qty_f'].sum())
    print(f"   init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost = {eq_qty:,.2f}")
    eq_end_qty = eq_qty - common['unknow_lost_qty_f'].sum()
    print(f"   - unknow_lost = {eq_end_qty:,.2f}")
    actual_end_qty = common['end_stock_qty_f'].sum()
    print(f"   end_stock (fmetl) = {actual_end_qty:,.2f}")
    print(f"   数量差异 = {eq_end_qty - actual_end_qty:,.2f}  (负值=实物盘点多于预期/日清库存不足)")

    # 库存方程验证 (fmetl侧, 金额)
    print("\n--- fmetl 库存方程验证 (金额) ---")
    eq_amt = (common['init_stock_amt_f'].sum()
              + common['receive_amt_f'].sum()
              + common['bom_in_amt_f'].sum()
              - common['bom_out_amt_f'].sum()
              + common['compose_in_amt_f'].sum()
              - common['compose_out_amt_f'].sum()
              - common['sale_amt_f'].sum()
              - common['know_lost_amt_f'].sum())
    print(f"   init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost = {eq_amt:,.2f}")
    eq_end_amt = eq_amt - common['unknow_lost_amt_f'].sum()
    actual_end_amt = common['end_stock_amt_f'].sum()
    print(f"   - unknow_lost = {eq_end_amt:,.2f}")
    print(f"   end_stock (fmetl) = {actual_end_amt:,.2f}")
    print(f"   金额差异 = {eq_end_amt - actual_end_amt:,.2f}")
    print(f"   (金额差异来自: 非euc计价的receive/bom金额 + 盘点/日清导致的库存断点)")

    # QDM 库存方程验证 (数量)
    print("\n--- QDM 库存方程验证 (数量) ---")
    eq_qty_q = (common['init_stock_qty_q'].sum()
                + common['receive_qty_q'].sum()
                + common['compose_in_qty_q'].sum()
                - common['compose_out_qty_q'].sum()
                - common['sale_qty_q'].sum()
                - common['know_lost_qty_q'].sum())
    print(f"   init + receive + compose_in - compose_out - sale - know_lost = {eq_qty_q:,.2f}")
    eq_end_qty_q = eq_qty_q - common['unknow_lost_qty_q'].sum()
    actual_end_qty_q = common['end_stock_qty_q'].sum()
    print(f"   - unknow_lost = {eq_end_qty_q:,.2f}")
    print(f"   end_stock (QDM) = {actual_end_qty_q:,.2f}")
    print(f"   数量差异 = {eq_end_qty_q - actual_end_qty_q:,.2f}")

    # QDM 库存方程验证 (金额)
    print("\n--- QDM 库存方程验证 (金额) ---")
    eq_amt_q = (common['init_stock_amt_q'].sum()
                + common['receive_amt_q'].sum()
                + common['compose_in_amt_q'].sum()
                - common['compose_out_amt_q'].sum()
                - common['sale_amt_q'].sum()
                - common['know_lost_amt_q'].sum())
    print(f"   init + receive + compose_in - compose_out - sale - know_lost = {eq_amt_q:,.2f}")
    eq_end_amt_q = eq_amt_q - common['unknow_lost_amt_q'].sum()
    actual_end_amt_q = common['end_stock_amt_q'].sum()
    print(f"   - unknow_lost = {eq_end_amt_q:,.2f}")
    print(f"   end_stock (QDM) = {actual_end_amt_q:,.2f}")
    print(f"   金额差异 = {eq_end_amt_q - actual_end_amt_q:,.2f}")

    # 毛利公式验证 (fmetl)
    print("\n--- fmetl 毛利公式验证 ---")
    profit_calc = (common['sale_amt_f'].sum()
                   - common['receive_amt_f'].sum()
                   - common['bom_in_amt_f'].sum()
                   + common['bom_out_amt_f'].sum()
                   - common['compose_in_amt_f'].sum()
                   + common['compose_out_amt_f'].sum()
                   + common['end_stock_amt_f'].sum()
                   - common['init_stock_amt_f'].sum())
    profit_actual = common['profit_amt_f'].sum()
    print(f"   sale - receive - bom_in + bom_out - compose_in + compose_out + end - init")
    print(f"   = {profit_calc:,.2f}")
    print(f"   profit_amt (fmetl) = {profit_actual:,.2f}")
    print(f"   差异 = {profit_calc - profit_actual:,.2f}")

    # QDM 毛利公式验证
    print("\n--- QDM 毛利公式验证 ---")
    profit_calc_q = (common['sale_amt_q'].sum()
                     - common['receive_amt_q'].sum()
                     - common['compose_in_amt_q'].sum()
                     + common['compose_out_amt_q'].sum()
                     + common['end_stock_amt_q'].sum()
                     - common['init_stock_amt_q'].sum())
    profit_actual_q = common['profit_amt_q'].sum()
    print(f"   sale - receive - compose_in + compose_out + end - init (QDM无BOM)")
    print(f"   = {profit_calc_q:,.2f}")
    print(f"   profit_amt (QDM) = {profit_actual_q:,.2f}")
    print(f"   差异 = {profit_calc_q - profit_actual_q:,.2f}")


# ============================================================
# 6. SKU 级差异 Top N
# ============================================================
def top_diff_skus(common):
    print("\n" + "=" * 70)
    print("6. SKU 级差异 Top 20 (按 |毛利差异| 排序)")
    print("=" * 70)

    # Compute per-SKU totals
    sku_agg = common.groupby(['article_id', 'article_name_f', 'remap_cat'], dropna=False).agg({
        'profit_amt_f': 'sum', 'profit_amt_q': 'sum',
        'sale_amt_f': 'sum', 'sale_amt_q': 'sum',
        'receive_amt_f': 'sum', 'receive_amt_q': 'sum',
        'end_stock_amt_f': 'sum', 'end_stock_amt_q': 'sum',
        'unknow_lost_amt_f': 'sum', 'unknow_lost_amt_q': 'sum',
    }).reset_index()

    sku_agg['profit_diff'] = sku_agg['profit_amt_f'] - sku_agg['profit_amt_q']
    sku_agg['profit_diff_abs'] = sku_agg['profit_diff'].abs()
    sku_agg = sku_agg.sort_values('profit_diff_abs', ascending=False)

    print(f"\n{'Article ID':<14s} {'名称':<20s} {'分类':<14s} "
          f"{'fmetl毛利':>12s} {'QDM毛利':>12s} {'差异':>12s} "
          f"{'fmetl销售额':>12s} {'QDM销售额':>12s}")
    print("-" * 110)
    for _, r in sku_agg.head(20).iterrows():
        aid = str(r['article_id'])
        nm = str(r['article_name_f'])[:18] if pd.notna(r['article_name_f']) else '?'
        cat = str(r['remap_cat'])[:12] if pd.notna(r['remap_cat']) else '?'
        print(f"{aid:<14s} {nm:<20s} {cat:<14s} "
              f"{r['profit_amt_f']:>12.2f} {r['profit_amt_q']:>12.2f} {r['profit_diff']:>12.2f} "
              f"{r['sale_amt_f']:>12.2f} {r['sale_amt_q']:>12.2f}")

    # Summary stats
    print(f"\n--- 毛利差异分布 ---")
    diffs = sku_agg['profit_diff'].dropna()
    print(f"   均值: {diffs.mean():.2f}  中位数: {diffs.median():.2f}")
    print(f"   标准差: {diffs.std():.2f}  最大正差: {diffs.max():.2f}  最大负差: {diffs.min():.2f}")
    p5 = diffs.quantile(0.05); p95 = diffs.quantile(0.95)
    print(f"   P5: {p5:.2f}  P95: {p95:.2f}")
    within_5pct = (abs(diffs) < 5).sum() if 'profit_amt_q' in sku_agg.columns else 0
    print(f"   |diff| < 5元: {within_5pct} SKUs ({within_5pct/len(diffs)*100:.1f}%)")


# ============================================================
# 7. 按天趋势
# ============================================================
def daily_trend(common):
    print("\n" + "=" * 70)
    print("7. 按天趋势: 毛利额差异 (fmetl - QDM)")
    print("=" * 70)

    daily = common.groupby('business_date').agg({
        'profit_amt_f': 'sum', 'profit_amt_q': 'sum',
        'sale_amt_f': 'sum', 'sale_amt_q': 'sum',
    }).reset_index()
    daily['profit_diff'] = daily['profit_amt_f'] - daily['profit_amt_q']
    daily['sale_diff'] = daily['sale_amt_f'] - daily['sale_amt_q']
    daily = daily.sort_values('business_date')

    print(f"\n{'日期':<12s} {'fmetl毛利':>12s} {'QDM毛利':>12s} {'毛利差异':>12s} "
          f"{'fmetl销售':>12s} {'QDM销售':>12s} {'销售差异':>12s}")
    print("-" * 85)
    for _, r in daily.iterrows():
        print(f"{r['business_date']:<12s} {r['profit_amt_f']:>12.2f} {r['profit_amt_q']:>12.2f} "
              f"{r['profit_diff']:>12.2f} {r['sale_amt_f']:>12.2f} "
              f"{r['sale_amt_q']:>12.2f} {r['sale_diff']:>12.2f}")

    total_profit_diff = daily['profit_diff'].sum()
    total_sale_diff = daily['sale_diff'].sum()
    print(f"\n   累计毛利差异: {total_profit_diff:,.2f}")
    print(f"   累计销售差异: {total_sale_diff:,.2f}")


# ============================================================
# main
# ============================================================
if __name__ == '__main__':
    print("=" * 70)
    print("fmetl v10 vs QDM 数据审查")
    print(f"日期范围: {START} ~ {END}")
    print(f"维度: 门店×日期×SKU (fmetl dc=0+1 聚合)")
    print(f"Store: A3XV")
    print("=" * 70)

    # Step 1: fmetl
    fm = load_fmetl()

    # Step 2: QDM
    qdm = load_qdm()
    if qdm is None:
        print("无法继续: QDM数据为空")
        sys.exit(1)

    # Step 3: Merge
    merged, common, fm_only, qdm_only = compare(fm, qdm)

    # Step 4: Category summary
    category_summary(common)

    # Step 5: Global summary
    global_summary(common)

    # Step 6: Top diff SKUs
    top_diff_skus(common)

    # Step 7: Daily trend
    daily_trend(common)

    print("\n" + "=" * 70)
    print("审查结束")
    print("=" * 70)
