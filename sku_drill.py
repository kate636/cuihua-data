#!/usr/bin/env python3
"""SKU 级下钻 — 用本地 DuckDB 数据追踪根因（QDM API 限流后备用方案）"""
import duckdb
import pandas as pd
import numpy as np

conn = duckdb.connect('data/fm.duckdb', read_only=True)
START, END = '2026-05-01', '2026-05-27'

# ============================================================
# 问题1: 烘焙类 — 按 SKU 看 EUC 分布和库存积累
# ============================================================
print("=" * 100)
print("【问题1: 烘焙类 — 哪些 SKU 的 EUC 最高？库存积累最快？】")
print("=" * 100)

bakery_sku = conn.execute(f"""
SELECT
    s.article_id,
    g.article_name,
    COUNT(DISTINCT s.business_date) AS ndays,
    SUM(s.sale_amt)       AS total_sale,
    SUM(s.sale_qty)       AS total_sale_qty,
    SUM(p.profit_amt)     AS total_profit,
    AVG(p.effective_unit_cost) AS avg_euc,
    -- 首日期初 vs 末日期末
    MIN(CASE WHEN s.business_date = '{START}' THEN s.init_stock_amt END) AS day1_init,
    MAX(CASE WHEN s.business_date = '{END}' THEN s.end_stock_amt END) AS lastday_end,
    SUM(s.receive_amt)    AS total_receive,
    SUM(s.know_lost_amt)  AS total_klost,
    SUM(s.unknow_lost_amt) AS total_ulost,
    SUM(s.bom_in_amt)     AS total_bom_in,
    SUM(s.bom_out_amt)    AS total_bom_out,
    SUM(p.allowance_amt_profit) AS total_allowance
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date
    AND s.article_id = p.article_id
    AND s.day_clear = p.day_clear
JOIN dim_goods g ON s.article_id = g.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
  AND g.category_level2_description = '烘焙类'
GROUP BY s.article_id, g.article_name
ORDER BY total_profit DESC
LIMIT 25
""").df()

print(f"\n{'SKU':>10} {'商品名':<30} {'days':>5} {'EUC':>8} {'总利润':>10} {'总销售':>10} {'进货':>10} {'day1期初':>10} {'末日期末':>10} {'折让':>8}")
print("-" * 135)
for _, r in bakery_sku.iterrows():
    name = str(r['article_name'])[:29]
    print(f"{r['article_id']:>10} {name:<30} {int(r['ndays']):>5} {r['avg_euc']:>8.2f} {r['total_profit']:>10,.0f} {r['total_sale']:>10,.0f} {r['total_receive']:>10,.0f} {r['day1_init']:>10,.0f} {r['lastday_end']:>10,.0f} {r['total_allowance']:>8,.0f}")

# ============================================================
# 问题2: 烘焙类 TOP 利润贡献者 — 库存方程详细追踪
# ============================================================
print("\n\n" + "=" * 100)
print("【问题2: 烘焙类 TOP3 利润 SKU — 库存方程逐日展开】")
print("=" * 100)

top3 = bakery_sku.head(3)['article_id'].tolist()
for aid in top3:
    name = conn.execute(f"SELECT article_name FROM dim_goods WHERE article_id = '{aid}'").fetchone()[0]
    d = conn.execute(f"""
    SELECT
        s.business_date, s.day_clear,
        s.init_stock_amt, s.init_stock_qty,
        s.receive_amt, s.receive_qty,
        s.sale_amt, s.sale_qty,
        s.end_stock_amt, s.end_stock_qty,
        s.know_lost_amt, s.unknow_lost_amt,
        s.bom_in_amt, s.bom_out_amt,
        s.compose_in_amt, s.compose_out_amt,
        s.stock_transfer_in_amt, s.stock_transfer_out_amt,
        p.profit_amt, p.effective_unit_cost AS euc,
        p.allowance_amt_profit,
        p.pre_profit_amt
    FROM t_calc_stock s
    JOIN t_calc_profit p ON s.store_id = p.store_id
        AND s.business_date = p.business_date
        AND s.article_id = p.article_id
        AND s.day_clear = p.day_clear
    WHERE s.article_id = '{aid}'
      AND s.business_date BETWEEN '{START}' AND '{END}'
    ORDER BY s.business_date, s.day_clear
    """).df()

    print(f"\n{'─'*130}")
    print(f"【{aid} — {name}】")

    # 库存方程验证: eq = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost
    # end = eq - unknow_lost (+ stock_transfer)
    d['equation'] = (d['init_stock_amt'] + d['receive_amt'] + d['bom_in_amt'] - d['bom_out_amt']
                     + d['compose_in_amt'] - d['compose_out_amt'] - d['sale_amt'] - d['know_lost_amt'])
    d['eq_minus_end'] = d['equation'] - d['end_stock_amt'] - d['unknow_lost_amt'] + d['stock_transfer_in_amt'] - d['stock_transfer_out_amt']

    print(f"{'日期':>12} {'dc':>2} {'期初库存':>10} {'+进货':>10} {'-销售':>10} {'=库存方程':>10} {'期末库存':>10} {'未知损耗':>10} {'方程平衡':>10} {'毛利':>10} {'EUC':>7} {'折让':>8}")
    print('-'*120)
    for _, r in d.iterrows():
        eq_balance = r['eq_minus_end']
        marker = ' <<<' if abs(eq_balance) > 10 else ''
        print(f"{r['business_date']:>12} {r['day_clear']:>2} {r['init_stock_amt']:>10,.0f} {r['receive_amt']:>10,.0f} {r['sale_amt']:>10,.0f} {r['equation']:>10,.0f} {r['end_stock_amt']:>10,.0f} {r['unknow_lost_amt']:>10,.0f} {eq_balance:>10,.0f}{marker} {r['profit_amt']:>10,.0f} {r['euc']:>7.2f} {r['allowance_amt_profit']:>8,.0f}")

# ============================================================
# 问题3: 烘焙类毛利成分分解
# ============================================================
print("\n\n" + "=" * 100)
print("【问题3: 烘焙类 — 毛利公式成分分解 (profit = sale - receive - bom_in + bom_out + end - init)】")
print("=" * 100)

# fmetl 烘焙类汇总毛利成分
bakery_components = conn.execute(f"""
SELECT
    s.business_date,
    SUM(s.sale_amt)       AS sale_amt,
    SUM(s.receive_amt)    AS receive_amt,
    SUM(s.bom_in_amt)     AS bom_in_amt,
    SUM(s.bom_out_amt)    AS bom_out_amt,
    SUM(s.compose_in_amt) AS compose_in_amt,
    SUM(s.compose_out_amt) AS compose_out_amt,
    SUM(s.init_stock_amt) AS init_stock,
    SUM(s.end_stock_amt)  AS end_stock,
    SUM(p.profit_amt)     AS profit_amt,
    SUM(p.allowance_amt_profit) AS allowance_amt,
    -- 验证: profit = sale - receive - bom_in + bom_out - compose_in + compose_out + end - init
    SUM(s.sale_amt - s.receive_amt - s.bom_in_amt + s.bom_out_amt
        - s.compose_in_amt + s.compose_out_amt
        + s.end_stock_amt - s.init_stock_amt) AS profit_check,
    COUNT(DISTINCT s.article_id) AS sku_cnt
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date
    AND s.article_id = p.article_id
    AND s.day_clear = p.day_clear
JOIN dim_goods g ON s.article_id = g.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
  AND g.category_level2_description = '烘焙类'
GROUP BY s.business_date
ORDER BY s.business_date
""").df()

print(f"\n{'日期':>12} {'销售额':>10} {'进货额':>10} {'期初':>10} {'期末':>10} {'折让':>8} {'利润(公式)':>10} {'利润(实际)':>10} {'差异':>10} {'SKU':>5}")
print('-'*115)
for _, r in bakery_components.iterrows():
    diff = r['profit_check'] - r['profit_amt']
    print(f"{r['business_date']:>12} {r['sale_amt']:>10,.0f} {r['receive_amt']:>10,.0f} {r['init_stock']:>10,.0f} {r['end_stock']:>10,.0f} {r['allowance_amt']:>8,.0f} {r['profit_check']:>10,.0f} {r['profit_amt']:>10,.0f} {diff:>10,.0f} {int(r['sku_cnt']):>5}")

# ============================================================
# 问题4: 全局 — 哪些 SKU 贡献了最大的毛利差异（fmetl 内部自洽）
# ============================================================
print("\n\n" + "=" * 100)
print("【问题4: fmetl 全局 — 所有分类 SKU 毛利 TOP + 毛利公式自洽验证】")
print("=" * 100)

all_sku = conn.execute(f"""
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
    cm.cat1_name,
    s.article_id,
    g.article_name,
    SUM(p.profit_amt)     AS total_profit,
    SUM(s.sale_amt)       AS total_sale,
    SUM(s.receive_amt)    AS total_receive,
    SUM(s.init_stock_amt) AS total_init,
    SUM(s.end_stock_amt)  AS total_end,
    AVG(p.effective_unit_cost) AS avg_euc,
    SUM(p.allowance_amt_profit) AS total_allowance,
    COUNT(DISTINCT s.business_date) AS ndays
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date
    AND s.article_id = p.article_id
    AND s.day_clear = p.day_clear
JOIN dim_goods g ON s.article_id = g.article_id
JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
GROUP BY cm.cat1_name, s.article_id, g.article_name
ORDER BY total_profit DESC
LIMIT 40
""").df()

print(f"\n{'分类':<18} {'SKU':>10} {'商品名':<28} {'利润':>10} {'销售':>10} {'进货':>10} {'EUC':>7} {'折让':>8} {'days':>5}")
print('-'*120)
for _, r in all_sku.iterrows():
    name = str(r['article_name'])[:27]
    print(f"{r['cat1_name']:<18} {r['article_id']:>10} {name:<28} {r['total_profit']:>10,.0f} {r['total_sale']:>10,.0f} {r['total_receive']:>10,.0f} {r['avg_euc']:>7.2f} {r['total_allowance']:>8,.0f} {int(r['ndays']):>5}")

# ============================================================
# 问题5: 负毛利 SKU (可能有问题)
# ============================================================
print("\n\n" + "=" * 100)
print("【问题5: 负毛利 SKU — 可能有问题的计算】")
print("=" * 100)

neg_profit = conn.execute(f"""
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
    cm.cat1_name,
    s.article_id,
    g.article_name,
    SUM(p.profit_amt)     AS total_profit,
    SUM(s.sale_amt)       AS total_sale,
    AVG(p.effective_unit_cost) AS avg_euc,
    SUM(s.unknow_lost_amt) AS total_ulost,
    COUNT(DISTINCT s.business_date) AS ndays,
    p.cost_source
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date
    AND s.article_id = p.article_id
    AND s.day_clear = p.day_clear
JOIN dim_goods g ON s.article_id = g.article_id
JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
GROUP BY cm.cat1_name, s.article_id, g.article_name, p.cost_source
HAVING SUM(p.profit_amt) < -100
ORDER BY total_profit
LIMIT 20
""").df()

if neg_profit.empty:
    print("无显著负毛利 SKU (差异 < -100)")
else:
    print(f"\n{'分类':<18} {'SKU':>10} {'商品名':<28} {'利润':>10} {'销售':>10} {'EUC':>7} {'未知损耗':>10} {'cost_source':<25} {'days':>5}")
    print('-'*130)
    for _, r in neg_profit.iterrows():
        name = str(r['article_name'])[:27]
        print(f"{r['cat1_name']:<18} {r['article_id']:>10} {name:<28} {r['total_profit']:>10,.0f} {r['total_sale']:>10,.0f} {r['avg_euc']:>7.2f} {r['total_ulost']:>10,.0f} {str(r['cost_source']):<25} {int(r['ndays']):>5}")

# ============================================================
# 问题6: allowance (折让) 等于 profit 的 SKU（即 BOM 父品？）
# ============================================================
print("\n\n" + "=" * 100)
print("【问题6: 折让=利润 的 SKU — BOM父品/无销售SKU？】")
print("=" * 100)

allowance_skus = conn.execute(f"""
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
    cm.cat1_name,
    s.article_id,
    g.article_name,
    SUM(p.profit_amt)     AS total_profit,
    SUM(p.allowance_amt_profit) AS total_allowance,
    SUM(s.sale_amt)       AS total_sale,
    SUM(s.receive_amt)    AS total_receive,
    SUM(s.bom_in_amt)     AS total_bom_in,
    SUM(s.bom_out_amt)    AS total_bom_out,
    COUNT(DISTINCT s.business_date) AS ndays
FROM t_calc_stock s
JOIN t_calc_profit p ON s.store_id = p.store_id
    AND s.business_date = p.business_date
    AND s.article_id = p.article_id
    AND s.day_clear = p.day_clear
JOIN dim_goods g ON s.article_id = g.article_id
JOIN cat_map cm ON s.article_id = cm.article_id
WHERE s.business_date BETWEEN '{START}' AND '{END}'
  AND p.allowance_amt_profit > 100
  AND ABS(p.profit_amt - p.allowance_amt_profit) < 10
GROUP BY cm.cat1_name, s.article_id, g.article_name
ORDER BY total_allowance DESC
LIMIT 15
""").df()

if allowance_skus.empty:
    print("无显著 折让=利润 的 SKU")
else:
    print(f"\n{'分类':<18} {'SKU':>10} {'商品名':<28} {'利润':>10} {'折让':>10} {'销售':>10} {'BOM_in':>10} {'BOM_out':>10} {'days':>5}")
    print('-'*125)
    for _, r in allowance_skus.iterrows():
        name = str(r['article_name'])[:27]
        print(f"{r['cat1_name']:<18} {r['article_id']:>10} {name:<28} {r['total_profit']:>10,.0f} {r['total_allowance']:>10,.0f} {r['total_sale']:>10,.0f} {r['total_bom_in']:>10,.0f} {r['total_bom_out']:>10,.0f} {int(r['ndays']):>5}")

conn.close()
print("\n\nDone.")
