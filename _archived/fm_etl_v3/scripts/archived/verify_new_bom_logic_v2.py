"""
验证新的 BOM 分摊逻辑（用户提出的方案）- 简化版
用一条 SQL 直接计算

逻辑：
1. sub_sku当日拆分数量 = 销售数量 + 已知损耗数量
2. sub_sku当日合计成本 = (sub_sku当日拆分数量 × 销售原价)
                         / SUM(parent下所有sub的当日拆分数量 × 销售原价)
                         × parent_sku进货额
"""

import hashlib
import json
import random
import string
import time
import requests
import os
from dotenv import load_dotenv

load_dotenv('/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/.env')

HOST = os.getenv('QDM_HOST', 'https://bdapp.qdama.cn')
API_ID = os.getenv('QDM_API_ID')
ACCESS_KEY = os.getenv('QDM_ACCESS_KEY')
SECRET_KEY = os.getenv('QDM_SECRET_KEY')
VERSION = os.getenv('QDM_VERSION', 'v1')

def generate_sign(timestamp, nonce, encrypt, body_str, access_key, secret_key, version):
    sign_params = {
        "AccessKey": access_key,
        "encrypt": encrypt,
        "nonce": nonce,
        "timestamp": timestamp,
        "version": version,
        "bodyStr": body_str,
    }
    keys = sorted(k for k, v in sign_params.items() if v not in (None, ""))
    param_str = "&".join(f"{k}={sign_params[k]}" for k in keys)
    param_str += f"&SecretKey={secret_key}"
    return hashlib.md5(param_str.encode("utf-8")).hexdigest().upper()

def query_sql(sql):
    body = {
        "apiId": API_ID,
        "paramMap": {"apiId": API_ID, "sql": sql},
    }
    body_str = json.dumps(body, ensure_ascii=False)

    nonce = "".join(random.choices(string.ascii_letters + string.digits, k=6))
    timestamp = int(time.time() * 1000)
    encrypt = 0

    sign = generate_sign(timestamp, nonce, encrypt, body_str, ACCESS_KEY, SECRET_KEY, VERSION)

    query_params = {
        "AccessKey": ACCESS_KEY,
        "timestamp": timestamp,
        "nonce": nonce,
        "encrypt": encrypt,
        "version": VERSION,
        "sign": sign,
    }
    url = f"{HOST}/api/v1/executeApi/{API_ID}?" + "&".join(f"{k}={v}" for k, v in query_params.items())

    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, data=body_str.encode("utf-8"), headers=headers, timeout=600)
    resp.raise_for_status()
    result = resp.json()

    if result.get("code") != 0:
        raise RuntimeError(f"API error: code={result.get('code')}, msg={result.get('msg')}")

    data = result.get("data")
    if isinstance(data, dict) and "pageData" in data:
        return data["pageData"]
    if isinstance(data, list):
        return data
    return []

def main():
    # 用一条完整的SQL计算新的BOM分摊逻辑
    # 分析 20045463 优鲜黑猪A级

    print("="*100)
    print("验证新 BOM 分摊逻辑（2026-04-23）")
    print("Parent: 20045463 优鲜黑猪A级")
    print("="*100)

    sql = """
    WITH
    -- 1. parent进货信息（从purchase_di按article_id聚合）
    parent_inbound AS (
        SELECT
            article_id AS parent_article_id,
            SUM(sale_article_purchase_amt) AS parent_inbound_amt,
            AVG(avg_inbound_price) AS parent_avg_price
        FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
        WHERE inc_day = '2026-04-23'
          AND store_id = 'A3XV'
          AND article_id = '20045463'
        GROUP BY article_id
    ),

    -- 2. BOM关系（从purchase_di获取parent→sub映射）
    bom_relation AS (
        SELECT DISTINCT
            article_id AS parent_article_id,
            sale_article_id AS sub_article_id,
            sale_article_name AS sub_name
        FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
        WHERE inc_day = '2026-04-23'
          AND store_id = 'A3XV'
          AND article_id = '20045463'
          AND article_id != sale_article_id
    ),

    -- 3. sub销售数量（从sales_di）
    sub_sales AS (
        SELECT
            abi_article_id AS sub_article_id,
            SUM(qty_spec) AS sale_qty,
            SUM(sales_amt) AS sale_amt
        FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
        WHERE inc_day = '2026-04-23'
          AND store_id = 'A3XV'
        GROUP BY abi_article_id
    ),

    -- 4. sub已知损耗（从loss_di）
    sub_loss AS (
        SELECT
            article_id AS sub_article_id,
            SUM(know_lost_qty) AS know_lost_qty,
            SUM(know_lost_amt) AS know_lost_amt
        FROM default_catalog.ads_business_analysis.strategy_fm_loss_di
        WHERE inc_day = '2026-04-23'
          AND store_id = 'A3XV'
        GROUP BY article_id
    ),

    -- 5. sub销售原价（从price_da）
    sub_price AS (
        SELECT
            sku_code AS sub_article_id,
            original_price
        FROM default_catalog.ads_business_analysis.strategy_fm_price_da
        WHERE inc_day = '2026-04-23'
          AND shop_id = 'A3XV'
    ),

    -- 6. 官方分摊数据（从purchase_di）
    official_alloc AS (
        SELECT
            sale_article_id AS sub_article_id,
            sale_article_name AS sub_name,
            sale_article_qty AS official_qty,
            sale_article_purchase_amt AS official_amt
        FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
        WHERE inc_day = '2026-04-23'
          AND store_id = 'A3XV'
          AND article_id = '20045463'
          AND article_id != sale_article_id
    ),

    -- 7. 计算每个sub的拆分数量和权重
    sub_calc AS (
        SELECT
            b.parent_article_id,
            b.sub_article_id,
            b.sub_name,
            COALESCE(s.sale_qty, 0) AS sale_qty,
            COALESCE(l.know_lost_qty, 0) AS know_lost_qty,
            COALESCE(s.sale_qty, 0) + COALESCE(l.know_lost_qty, 0) AS split_qty_new,
            COALESCE(p.original_price, 0) AS original_price,
            (COALESCE(s.sale_qty, 0) + COALESCE(l.know_lost_qty, 0)) * COALESCE(p.original_price, 0) AS weight
        FROM bom_relation b
        LEFT JOIN sub_sales s ON s.sub_article_id = b.sub_article_id
        LEFT JOIN sub_loss l ON l.sub_article_id = b.sub_article_id
        LEFT JOIN sub_price p ON p.sub_article_id = b.sub_article_id
    ),

    -- 8. 计算总权重（用于归一化）
    total_weight AS (
        SELECT SUM(weight) AS total_weight
        FROM sub_calc
    )

    -- 9. 最终输出：对比新旧逻辑
    SELECT
        sc.parent_article_id,
        sc.sub_article_id,
        sc.sub_name,
        sc.sale_qty,
        sc.know_lost_qty,
        sc.split_qty_new,
        sc.original_price,
        sc.weight,
        tw.total_weight,
        p.parent_inbound_amt,
        -- 新逻辑分摊金额（使用 IF 替代 CASE WHEN，并用 COALESCE 兜底）
        COALESCE(IF(tw.total_weight > 0, sc.weight / tw.total_weight * p.parent_inbound_amt, 0), 0) AS alloc_amt_new,
        -- 新逻辑单位成本
        COALESCE(IF(tw.total_weight > 0 AND sc.split_qty_new > 0, sc.weight / tw.total_weight * p.parent_inbound_amt / sc.split_qty_new, 0), 0) AS unit_cost_new,
        -- 官方分摊数量
        o.official_qty,
        -- 官方分摊金额
        o.official_amt,
        -- 官方单位成本
        COALESCE(IF(o.official_qty > 0, o.official_amt / o.official_qty, 0), 0) AS unit_cost_official,
        -- parent均价
        p.parent_avg_price
    FROM sub_calc sc
    CROSS JOIN total_weight tw
    LEFT JOIN parent_inbound p ON p.parent_article_id = sc.parent_article_id
    LEFT JOIN official_alloc o ON o.sub_article_id = sc.sub_article_id
    ORDER BY sc.sub_article_id
    """

    print("\n执行SQL查询...")
    rows = query_sql(sql)

    if not rows:
        print("无数据")
        return

    print(f"\n共 {len(rows)} 条记录")
    print("\n" + "="*140)

    def safe_float(val, default=0.0):
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    # 先打印第一条原始数据看看字段名
    print("\n第一条原始数据（调试）：")
    print(rows[0])

    # 打印有正权重的行的原始数据
    print("\n有正权重的行（调试）：")
    for r in rows:
        w = safe_float(r.get('weight'))
        if w > 0:
            print(f"sub={r.get('subArticleId')}, weight={w}, allocAmtNew={r.get('allocAmtNew')}, saleQty={r.get('saleQty')}")
    print()

    # 打印结果
    print(f"\n{'SubID':<12} {'名称':<20} {'销售量':<8} {'损耗量':<8} {'拆分量(新)':<10} {'原价':<8} {'权重':<10}")
    print("-"*140)

    total_weight = 0
    total_alloc_new = 0
    total_official = 0

    for r in rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id', ''))
        sub_name = str(r.get('subName', r.get('sub_name', '')))[:18]
        sale_qty = safe_float(r.get('saleQty', r.get('sale_qty')))
        know_lost_qty = safe_float(r.get('knowLostQty', r.get('know_lost_qty')))
        split_qty_new = safe_float(r.get('splitQtyNew', r.get('split_qty_new')))
        original_price = safe_float(r.get('originalPrice', r.get('original_price')))
        weight = safe_float(r.get('weight'))
        total_w = safe_float(r.get('totalWeight', r.get('total_weight')))
        parent_inbound_amt = safe_float(r.get('parentInboundAmt', r.get('parent_inbound_amt')))
        alloc_amt_new = safe_float(r.get('allocAmtNew', r.get('alloc_amt_new')))
        unit_cost_new = safe_float(r.get('unitCostNew', r.get('unit_cost_new')))
        official_qty = safe_float(r.get('officialQty', r.get('official_qty')))
        official_amt = safe_float(r.get('officialAmt', r.get('official_amt')))
        unit_cost_official = safe_float(r.get('unitCostOfficial', r.get('unit_cost_official')))
        parent_avg_price = safe_float(r.get('parentAvgPrice', r.get('parent_avg_price')))

        total_weight = total_w
        total_alloc_new += alloc_amt_new
        total_official += official_amt

        # 只显示有销售或损耗的
        if sale_qty > 0 or know_lost_qty > 0:
            print(f"{sub_id:<12} {sub_name:<20} {sale_qty:<8.3f} {know_lost_qty:<8.3f} {split_qty_new:<10.3f} {original_price:<8.2f} {weight:<10.2f}")

    print("\n" + "="*140)
    print("\n对比结果（只显示有销售+损耗的sub）：")
    print(f"\n{'SubID':<12} {'名称':<20} {'拆分量(新)':<10} {'拆分量(官)':<10} {'分摊额(新)':<12} {'分摊额(官)':<12} {'成本(新)':<10} {'成本(官)':<10} {'parent均价':<10}")
    print("-"*140)

    for r in rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id', ''))
        sub_name = str(r.get('subName', r.get('sub_name', '')))[:18]
        sale_qty = safe_float(r.get('saleQty', r.get('sale_qty')))
        know_lost_qty = safe_float(r.get('knowLostQty', r.get('know_lost_qty')))
        split_qty_new = safe_float(r.get('splitQtyNew', r.get('split_qty_new')))
        alloc_amt_new = safe_float(r.get('allocAmtNew', r.get('alloc_amt_new')))
        unit_cost_new = safe_float(r.get('unitCostNew', r.get('unit_cost_new')))
        official_qty = safe_float(r.get('officialQty', r.get('official_qty')))
        official_amt = safe_float(r.get('officialAmt', r.get('official_amt')))
        unit_cost_official = safe_float(r.get('unitCostOfficial', r.get('unit_cost_official')))
        parent_avg_price = safe_float(r.get('parentAvgPrice', r.get('parent_avg_price')))

        if sale_qty > 0 or know_lost_qty > 0:
            print(f"{sub_id:<12} {sub_name:<20} {split_qty_new:<10.3f} {official_qty:<10.3f} {alloc_amt_new:<12.2f} {official_amt:<12.2f} {unit_cost_new:<10.2f} {unit_cost_official:<10.2f} {parent_avg_price:<10.2f}")

    print("\n" + "="*140)
    print("\n汇总统计：")
    print(f"  Parent进货总额: {rows[0].get('parentInboundAmt', rows[0].get('parent_inbound_amt', 0))} 元")
    print(f"  Parent进货均价: {rows[0].get('parentAvgPrice', rows[0].get('parent_avg_price', 0))} 元/kg")
    print(f"  总权重: {total_weight} 元")
    print(f"  新逻辑分摊合计: {total_alloc_new:.2f} 元")
    print(f"  官方分摊合计: {total_official:.2f} 元")

    # 统计有销售/损耗的sub数量
    active_subs = [r for r in rows if safe_float(r.get('saleQty', r.get('sale_qty'))) > 0 or safe_float(r.get('knowLostQty', r.get('know_lost_qty'))) > 0]
    print(f"  有销售+损耗的sub: {len(active_subs)} 个")
    print(f"  总sub数量: {len(rows)} 个")

    # 统计无销售无损耗的sub（这些在新逻辑下权重=0）
    inactive_subs = len(rows) - len(active_subs)
    print(f"  无销售无损耗的sub: {inactive_subs} 个（新逻辑下权重=0，不参与分摊）")

if __name__ == "__main__":
    main()