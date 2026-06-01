"""
验证新 BOM 分摊逻辑（用户方案）- Python计算版

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

def safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def main():
    print("="*100)
    print("验证新 BOM 分摊逻辑（2026-04-23）")
    print("Parent: 20045463 优鲜黑猪A级")
    print("="*100)

    # 1. 取 purchase_di 数据（parent 进货 + BOM 关系 + 官方分摊）
    sql_purchase = """
    SELECT
        article_id AS parent_article_id,
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name,
        sale_article_qty AS official_qty,
        sale_article_purchase_amt AS official_amt,
        avg_inbound_price AS parent_avg_price,
        init_stock_qty,
        init_stock_amt,
        end_stock_qty,
        end_stock_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '20045463'
    """
    print("\n1. 查询 purchase_di...")
    purchase_rows = query_sql(sql_purchase)
    print(f"   共 {len(purchase_rows)} 条")

    # 计算 parent 进货总额
    parent_inbound_amt = sum(safe_float(r.get('officialAmt', r.get('official_amt'))) for r in purchase_rows)
    parent_avg_price = safe_float(purchase_rows[0].get('parentAvgPrice', purchase_rows[0].get('parent_avg_price'))) if purchase_rows else 0
    print(f"   Parent进货总额: {parent_inbound_amt:.2f} 元")
    print(f"   Parent均价: {parent_avg_price:.2f} 元/kg")

    # 取所有 sub_article_id 列表
    sub_ids = [r.get('subArticleId', r.get('sub_article_id')) for r in purchase_rows]
    print(f"   Sub SKU 数量: {len(sub_ids)} 个")

    # 2. 取 sales_di 数据（销售数量）
    sql_sales = """
    SELECT
        abi_article_id AS sub_article_id,
        SUM(qty_spec) AS sale_qty,
        SUM(sales_amt) AS sale_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
    GROUP BY abi_article_id
    """
    print("\n2. 查询 sales_di...")
    sales_rows = query_sql(sql_sales)
    print(f"   共 {len(sales_rows)} 条")

    # 构建 sale_qty 字典
    sale_qty_dict = {}
    for r in sales_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        qty = safe_float(r.get('saleQty', r.get('sale_qty')))
        sale_qty_dict[sub_id] = qty

    # 检查匹配情况
    matched = [sid for sid in sub_ids if sid in sale_qty_dict]
    unmatched = [sid for sid in sub_ids if sid not in sale_qty_dict and sid != '20045463']
    print(f"   匹配的 sub: {len(matched)} 个")
    print(f"   未匹配的 sub: {len(unmatched)} 个")
    if unmatched:
        print(f"   未匹配的 sub ID: {unmatched[:5]}...")

    # 3. 取 loss_di 数据（已知损耗）
    sql_loss = """
    SELECT
        article_id AS sub_article_id,
        SUM(know_lost_qty) AS know_lost_qty,
        SUM(know_lost_amt) AS know_lost_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_loss_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
    GROUP BY article_id
    """
    print("\n3. 查询 loss_di...")
    loss_rows = query_sql(sql_loss)
    print(f"   共 {len(loss_rows)} 条")

    # 构建 loss 字典
    loss_qty_dict = {}
    for r in loss_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        qty = safe_float(r.get('knowLostQty', r.get('know_lost_qty')))
        loss_qty_dict[sub_id] = qty

    # 4. 取 price_da 数据（销售原价）
    sql_price = """
    SELECT
        sku_code AS sub_article_id,
        original_price
    FROM default_catalog.ads_business_analysis.strategy_fm_price_da
    WHERE inc_day = '2026-04-23'
      AND shop_id = 'A3XV'
    """
    print("\n4. 查询 price_da...")
    price_rows = query_sql(sql_price)
    print(f"   共 {len(price_rows)} 条")

    # 构建 price 字典
    price_dict = {}
    for r in price_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        price = safe_float(r.get('originalPrice', r.get('original_price')))
        price_dict[sub_id] = price

    # 5. Python 计算
    print("\n" + "="*100)
    print("计算新逻辑...")

    results = []
    for r in purchase_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        sub_name = r.get('subName', r.get('sub_name', ''))

        # 取各维度数据
        sale_qty = sale_qty_dict.get(sub_id, 0)
        know_lost_qty = loss_qty_dict.get(sub_id, 0)
        original_price = price_dict.get(sub_id, 0)

        # 官方数据
        official_qty = safe_float(r.get('officialQty', r.get('official_qty')))
        official_amt = safe_float(r.get('officialAmt', r.get('official_amt')))

        # 新逻辑
        split_qty_new = sale_qty + know_lost_qty
        weight = split_qty_new * original_price

        results.append({
            'sub_id': sub_id,
            'sub_name': sub_name,
            'sale_qty': sale_qty,
            'know_lost_qty': know_lost_qty,
            'split_qty_new': split_qty_new,
            'original_price': original_price,
            'weight': weight,
            'official_qty': official_qty,
            'official_amt': official_amt,
        })

    # 计算总权重
    total_weight = sum(r['weight'] for r in results)
    print(f"总权重: {total_weight:.2f}")

    # 计算分摊金额和单位成本
    for r in results:
        if total_weight > 0 and r['weight'] > 0:
            r['alloc_amt_new'] = r['weight'] / total_weight * parent_inbound_amt
        else:
            r['alloc_amt_new'] = 0

        if r['split_qty_new'] > 0:
            r['unit_cost_new'] = r['alloc_amt_new'] / r['split_qty_new']
        else:
            r['unit_cost_new'] = 0

        if r['official_qty'] > 0:
            r['unit_cost_official'] = r['official_amt'] / r['official_qty']
        else:
            r['unit_cost_official'] = 0

    # 6. 打印结果
    print("\n" + "="*140)
    print("\n所有 sub SKU 详情：")
    print(f"\n{'SubID':<12} {'名称':<20} {'销售量':<8} {'损耗量':<8} {'拆分量(新)':<10} {'原价':<8} {'权重':<10}")
    print("-"*140)

    active_subs = [r for r in results if r['sale_qty'] > 0 or r['know_lost_qty'] > 0]
    for r in active_subs:
        print(f"{r['sub_id']:<12} {r['sub_name'][:20]:<20} {r['sale_qty']:<8.3f} {r['know_lost_qty']:<8.3f} {r['split_qty_new']:<10.3f} {r['original_price']:<8.2f} {r['weight']:<10.2f}")

    print("\n" + "="*140)
    print("\n对比结果（新逻辑 vs 官方）：")
    print(f"\n{'SubID':<12} {'名称':<20} {'拆分量(新)':<10} {'拆分量(官)':<10} {'分摊额(新)':<12} {'分摊额(官)':<12} {'成本(新)':<10} {'成本(官)':<10} {'parent均价':<10}")
    print("-"*140)

    for r in active_subs:
        print(f"{r['sub_id']:<12} {r['sub_name'][:20]:<20} {r['split_qty_new']:<10.3f} {r['official_qty']:<10.3f} {r['alloc_amt_new']:<12.2f} {r['official_amt']:<12.2f} {r['unit_cost_new']:<10.2f} {r['unit_cost_official']:<10.2f} {parent_avg_price:<10.2f}")

    print("\n" + "="*140)
    print("\n汇总统计：")
    total_alloc_new = sum(r['alloc_amt_new'] for r in results)
    total_official = sum(r['official_amt'] for r in results)
    print(f"  Parent进货总额: {parent_inbound_amt:.2f} 元")
    print(f"  总权重: {total_weight:.2f} 元")
    print(f"  新逻辑分摊合计: {total_alloc_new:.2f} 元")
    print(f"  官方分摊合计: {total_official:.2f} 元")
    print(f"  有销售+损耗的sub: {len(active_subs)} 个")
    print(f"  总sub数量: {len(results)} 个")
    print(f"  无销售无损耗的sub: {len(results) - len(active_subs)} 个（新逻辑下权重=0）")

    # 7. 计算门店毛利
    print("\n" + "="*140)
    print("\n每个 SKU 门店毛利明细：")

    # 取销售金额（之前已有sale_qty，现在需要sale_amt）
    sale_amt_dict = {}
    for r in sales_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        amt = safe_float(r.get('saleAmt', r.get('sale_amt')))
        sale_amt_dict[sub_id] = amt

    # 取损耗金额
    loss_amt_dict = {}
    for r in loss_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        amt = safe_float(r.get('knowLostAmt', r.get('know_lost_amt')))
        loss_qty = safe_float(r.get('knowLostQty', r.get('know_lost_qty')))
        loss_amt_dict[sub_id] = amt
        # 也要更新损耗量（之前可能没取全）
        if sub_id in loss_qty_dict:
            loss_qty_dict[sub_id] = max(loss_qty_dict[sub_id], loss_qty)

    # 计算毛利
    for r in results:
        sub_id = r['sub_id']
        r['sale_amt'] = sale_amt_dict.get(sub_id, 0)
        r['loss_amt'] = loss_amt_dict.get(sub_id, 0)
        r['loss_qty'] = loss_qty_dict.get(sub_id, 0)

        # 销售成本 = 销售量 × 单位成本
        r['sale_cost_new'] = r['sale_qty'] * r['unit_cost_new']
        r['sale_cost_official'] = r['sale_qty'] * r['unit_cost_official']

        # 门店毛利（销售方程）= 销售额 - 销售成本 - 损耗额
        r['profit_new'] = r['sale_amt'] - r['sale_cost_new'] - r['loss_amt']
        r['profit_official'] = r['sale_amt'] - r['sale_cost_official'] - r['loss_amt']

    # 打印毛利明细
    print(f"\n{'SubID':<12} {'名称':<18} {'销售量':<7} {'销售额':<9} {'损耗量':<7} {'损耗额':<8} {'成本(新)':<8} {'成本(官)':<8} {'毛利(新)':<9} {'毛利(官)':<9} {'毛利率(新)':<8} {'毛利率(官)':<8}")
    print("-"*140)

    for r in active_subs:
        margin_new = (r['profit_new'] / r['sale_amt'] * 100) if r['sale_amt'] > 0 else 0
        margin_official = (r['profit_official'] / r['sale_amt'] * 100) if r['sale_amt'] > 0 else 0
        print(f"{r['sub_id']:<12} {r['sub_name'][:18]:<18} {r['sale_qty']:<7.3f} {r['sale_amt']:<9.2f} {r['loss_qty']:<7.3f} {r['loss_amt']:<8.2f} {r['unit_cost_new']:<8.2f} {r['unit_cost_official']:<8.2f} {r['profit_new']:<9.2f} {r['profit_official']:<9.2f} {margin_new:<8.1f}% {margin_official:<8.1f}%")

    # 汇总毛利
    total_sale_amt = sum(r['sale_amt'] for r in results)
    total_loss_amt = sum(r['loss_amt'] for r in results)
    total_profit_new = sum(r['profit_new'] for r in results)
    total_profit_official = sum(r['profit_official'] for r in results)

    print("\n" + "="*140)
    print("\n汇总：")
    print(f"  总销售额: {total_sale_amt:.2f} 元")
    print(f"  总损耗额: {total_loss_amt:.2f} 元")
    print(f"  总毛利(新逻辑): {total_profit_new:.2f} 元")
    print(f"  总毛利(官方逻辑): {total_profit_official:.2f} 元")
    print(f"  毛利差额: {total_profit_new - total_profit_official:.2f} 元")

    # 差异分析
    print("\n差异分析：")
    for r in active_subs:
        diff_qty = r['split_qty_new'] - r['official_qty']
        diff_amt = r['alloc_amt_new'] - r['official_amt']
        diff_cost = r['unit_cost_new'] - r['unit_cost_official']
        diff_profit = r['profit_new'] - r['profit_official']
        if abs(diff_qty) > 0.01 or abs(diff_amt) > 0.1 or abs(diff_profit) > 0.1:
            print(f"  {r['sub_id']} {r['sub_name'][:15]}: 拆分量差={diff_qty:+.3f}, 分摊额差={diff_amt:+.2f}, 成本差={diff_cost:+.2f}, 毛利差={diff_profit:+.2f}")

    # 8. 排骨全链路计算示例
    print("\n" + "="*140)
    print("\n【排骨 20110673 全链路计算示例】")

    paigu = next((r for r in results if r['sub_id'] == '20110673'), None)
    if paigu:
        print("\n" + "-"*80)
        print("原始数据（从各表取）：")
        print("-"*80)
        print(f"  ① sales_di → 销售量: {paigu['sale_qty']} kg")
        print(f"              → 销售额: {paigu['sale_amt']} 元")
        print(f"  ② loss_di  → 损耗量: {paigu['loss_qty']} kg")
        print(f"              → 损耗额: {paigu['loss_amt']} 元")
        print(f"  ③ price_da → 销售原价: {paigu['original_price']} 元/kg")
        print(f"  ④ purchase_di → 官方拆分量: {paigu['official_qty']} kg")
        print(f"                → 官方分摊额: {paigu['official_amt']} 元")
        print(f"  ⑤ parent → 进货总额: {parent_inbound_amt} 元")

        print("\n" + "-"*80)
        print("【新逻辑计算步骤】")
        print("-"*80)

        # Step 1: 拆分量
        print(f"\n  Step 1: 计算拆分量")
        print(f"    拆分量 = 销售量 + 损耗量")
        print(f"         = {paigu['sale_qty']} + {paigu['loss_qty']}")
        print(f"         = {paigu['split_qty_new']} kg")

        # Step 2: 权重
        print(f"\n  Step 2: 计算权重")
        print(f"    权重 = 拆分量 × 销售原价")
        print(f"        = {paigu['split_qty_new']} × {paigu['original_price']}")
        print(f"        = {paigu['weight']} 元")

        # Step 3: 分摊额
        print(f"\n  Step 3: 计算分摊额")
        print(f"    分摊额 = (权重 / 总权重) × parent进货总额")
        print(f"          = ({paigu['weight']} / {total_weight}) × {parent_inbound_amt}")
        print(f"          = {paigu['weight']/total_weight:.6f} × {parent_inbound_amt}")
        print(f"          = {paigu['alloc_amt_new']} 元")

        # Step 4: 单位成本
        print(f"\n  Step 4: 计算单位成本")
        print(f"    单位成本 = 分摊额 / 拆分量")
        print(f"            = {paigu['alloc_amt_new']} / {paigu['split_qty_new']}")
        print(f"            = {paigu['unit_cost_new']} 元/kg")

        # Step 5: 销售成本
        print(f"\n  Step 5: 计算销售成本")
        print(f"    销售成本 = 销售量 × 单位成本")
        print(f"            = {paigu['sale_qty']} × {paigu['unit_cost_new']}")
        print(f"            = {paigu['sale_qty'] * paigu['unit_cost_new']} 元")

        # Step 6: 毛利
        print(f"\n  Step 6: 计算门店毛利")
        print(f"    毛利 = 销售额 - 销售成本 - 损耗额")
        print(f"        = {paigu['sale_amt']} - {paigu['sale_qty'] * paigu['unit_cost_new']} - {paigu['loss_amt']}")
        print(f"        = {paigu['profit_new']} 元")
        print(f"    毛利率 = 毛利 / 销售额 × 100%")
        print(f"          = {paigu['profit_new']} / {paigu['sale_amt']} × 100%")
        margin_new = (paigu['profit_new'] / paigu['sale_amt'] * 100) if paigu['sale_amt'] > 0 else 0
        print(f"          = {margin_new:.1f}%")

        print("\n" + "-"*80)
        print("【官方逻辑计算步骤】")
        print("-"*80)

        # 官方逻辑：所有sub用parent均价
        print(f"\n  官方单位成本 = parent均价 = {paigu['unit_cost_official']} 元/kg")
        print(f"              （从 purchase_di 的 avg_inbound_price 取）")

        print(f"\n  官方销售成本 = 销售量 × 官方单位成本")
        print(f"              = {paigu['sale_qty']} × {paigu['unit_cost_official']}")
        print(f"              = {paigu['sale_qty'] * paigu['unit_cost_official']} 元")

        print(f"\n  官方毛利 = 销售额 - 官方销售成本 - 损耗额")
        print(f"           = {paigu['sale_amt']} - {paigu['sale_qty'] * paigu['unit_cost_official']} - {paigu['loss_amt']}")
        print(f"           = {paigu['profit_official']} 元")
        margin_official = (paigu['profit_official'] / paigu['sale_amt'] * 100) if paigu['sale_amt'] > 0 else 0
        print(f"    毛利率 = {margin_official:.1f}%")

        print("\n" + "-"*80)
        print("【差异对比】")
        print("-"*80)
        print(f"\n  单位成本: 新逻辑={paigu['unit_cost_new']:.2f} vs 官方={paigu['unit_cost_official']:.2f} → 差={paigu['unit_cost_new'] - paigu['unit_cost_official']:+.2f} 元/kg")
        print(f"  销售成本: 新逻辑={paigu['sale_qty'] * paigu['unit_cost_new']:.2f} vs 官方={paigu['sale_qty'] * paigu['unit_cost_official']:.2f} → 差={paigu['sale_qty'] * paigu['unit_cost_new'] - paigu['sale_qty'] * paigu['unit_cost_official']:+.2f} 元")
        print(f"  毛利:     新逻辑={paigu['profit_new']:.2f} vs 官方={paigu['profit_official']:.2f} → 差={paigu['profit_new'] - paigu['profit_official']:+.2f} 元")
        print(f"  毛利率:   新逻辑={margin_new:.1f}% vs 官方={margin_official:.1f}% → 差={margin_new - margin_official:+.1f}%")

        print("\n" + "-"*80)
        print("【关键差异来源】")
        print("-"*80)
        print(f"\n  官方 purchase_di 给的拆分量: {paigu['official_qty']} kg")
        print(f"  新逻辑计算的拆分量:         {paigu['split_qty_new']} kg")
        print(f"  差异: {paigu['split_qty_new'] - paigu['official_qty']:+.3f} kg")
        print(f"\n  → 官方拆分量远小于实际销售量！")
        print(f"  → 官方逻辑下，排骨只分摊了 {paigu['official_amt']:.2f} 元进货成本")
        print(f"  → 新逻辑下，排骨按权重分摊了 {paigu['alloc_amt_new']:.2f} 元进货成本")
        print(f"  → 高价SKU(排骨59.60元/kg)在新逻辑下承担了更多进货成本")

if __name__ == "__main__":
    main()