"""
验证新的 BOM 分摊逻辑（用户提出的方案）

逻辑：
1. sub_sku当日拆分数量 = 销售数量 + 已知损耗数量
2. sub_sku当日合计成本 = (sub_sku当日拆分数量 × 销售原价)
                         / SUM(parent下所有sub的当日拆分数量 × 销售原价)
                         × parent_sku进货额

数据来源：
- parent进货额：purchase_di（按article_id聚合）
- sub销售数量：sales_di（按abi_article_id聚合）
- sub已知损耗数量：loss_di（按article_id聚合）
- sub销售原价：price_da（original_price）
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
    # 选择一个有BOM拆分的parent进行分析：优鲜黑猪A级 20045463
    print("="*80)
    print("分析 Parent: 20045463 优鲜黑猪A级（2026-04-23）")
    print("="*80)

    # Step 1: 获取parent进货信息
    print("\n【Step 1】获取 parent 进货信息")
    sql1 = """
    SELECT
        article_id AS parent_article_id,
        article_name AS parent_name,
        SUM(sale_article_qty) AS total_sub_qty,
        SUM(sale_article_purchase_amt) AS total_sub_amt,
        AVG(avg_inbound_price) AS parent_avg_price
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND article_id = '20045463'
    GROUP BY article_id, article_name
    """
    parent_data = query_sql(sql1)
    if parent_data:
        p = parent_data[0]
        parent_article_id = p.get('parentArticleId', p.get('parent_article_id'))
        parent_name = p.get('parentName', p.get('parent_name'))
        parent_total_qty = p.get('totalSubQty', p.get('total_sub_qty'))
        parent_total_amt = p.get('totalSubAmt', p.get('total_sub_amt'))
        parent_avg_price = p.get('parentAvgPrice', p.get('parent_avg_price'))
        print(f"  Parent ID: {parent_article_id}")
        print(f"  Parent 名称: {parent_name}")
        print(f"  Parent 进货总额(官方分摊合计): {parent_total_amt} 元")
        print(f"  Parent 进货总量(官方分摊合计): {parent_total_qty} kg")
        print(f"  Parent 进货均价: {parent_avg_price} 元/kg")

    # Step 2: 获取parent下所有sub的销售数量
    print("\n【Step 2】获取 sub 销售数量（从 sales_di）")
    sql2 = """
    SELECT
        abi_article_id AS sub_article_id,
        SUM(qty_spec) AS sale_qty,
        SUM(sales_amt) AS sale_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND abi_article_id IN (
          SELECT sale_article_id
          FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
          WHERE inc_day = '2026-04-23' AND article_id = '20045463'
      )
    GROUP BY abi_article_id
    """
    sales_data = query_sql(sql2)
    sales_dict = {}
    if sales_data:
        for s in sales_data:
            sub_id = s.get('abiArticleId', s.get('sub_article_id'))
            sale_qty = float(s.get('saleQty', s.get('sale_qty', 0)))
            sale_amt = float(s.get('saleAmt', s.get('sale_amt', 0)))
            sales_dict[sub_id] = {'sale_qty': sale_qty, 'sale_amt': sale_amt}

    print(f"  找到 {len(sales_dict)} 个 sub 有销售记录")
    for sub_id, d in sales_dict.items():
        print(f"    {sub_id}: 销售量={d['sale_qty']} kg, 销售额={d['sale_amt']} 元")

    # Step 3: 获取已知损耗数量
    print("\n【Step 3】获取 sub 已知损耗数量（从 loss_di）")
    sql3 = """
    SELECT
        article_id AS sub_article_id,
        know_lost_qty,
        know_lost_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_loss_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id IN (
          SELECT sale_article_id
          FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
          WHERE inc_day = '2026-04-23' AND article_id = '20045463'
      )
    """
    loss_data = query_sql(sql3)
    loss_dict = {}
    if loss_data:
        for l in loss_data:
            sub_id = l.get('articleId', l.get('sub_article_id'))
            know_lost_qty = float(l.get('knowLostQty', l.get('know_lost_qty', 0)))
            know_lost_amt = float(l.get('knowLostAmt', l.get('know_lost_amt', 0)))
            if know_lost_qty > 0:
                loss_dict[sub_id] = {'know_lost_qty': know_lost_qty, 'know_lost_amt': know_lost_amt}

    print(f"  找到 {len(loss_dict)} 个 sub 有已知损耗")
    for sub_id, d in loss_dict.items():
        print(f"    {sub_id}: 已知损耗={d['know_lost_qty']} kg")

    # Step 4: 获取销售原价
    print("\n【Step 4】获取 sub 销售原价（从 price_da）")
    sql4 = """
    SELECT
        sku_code AS sub_article_id,
        original_price
    FROM default_catalog.ads_business_analysis.strategy_fm_price_da
    WHERE inc_day = '2026-04-23'
      AND shop_id = 'A3XV'
      AND sku_code IN (
          SELECT sale_article_id
          FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
          WHERE inc_day = '2026-04-23' AND article_id = '20045463'
      )
    """
    price_data = query_sql(sql4)
    price_dict = {}
    if price_data:
        for p in price_data:
            sub_id = p.get('skuCode', p.get('sub_article_id'))
            original_price = float(p.get('originalPrice', p.get('original_price', 0)))
            price_dict[sub_id] = original_price

    print(f"  找到 {len(price_dict)} 个 sub 有原价信息")
    for sub_id, op in price_dict.items():
        print(f"    {sub_id}: 原价={op} 元/kg")

    # Step 5: 获取BOM关系（parent → sub映射）
    print("\n【Step 5】获取 BOM 关系（从 purchase_di）")
    sql5 = """
    SELECT DISTINCT
        article_id AS parent_article_id,
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND article_id = '20045463'
      AND article_id != sale_article_id
    """
    bom_data = query_sql(sql5)
    bom_list = []
    if bom_data:
        for b in bom_data:
            parent_id = b.get('articleId', b.get('parent_article_id'))
            sub_id = b.get('saleArticleId', b.get('sub_article_id'))
            sub_name = b.get('saleArticleName', b.get('sub_name'))
            bom_list.append({'parent_id': parent_id, 'sub_id': sub_id, 'sub_name': sub_name})

    print(f"  找到 {len(bom_list)} 个 BOM 关系")

    # Step 6: 计算新的分摊逻辑
    print("\n【Step 6】按新逻辑计算 BOM 分摊")
    print("-"*80)

    # 收集所有数据
    results = []
    total_weight = 0  # 用于归一化

    for bom in bom_list:
        sub_id = bom['sub_id']
        sub_name = bom['sub_name']

        # 获取各字段
        sale_qty = sales_dict.get(sub_id, {}).get('sale_qty', 0)
        know_lost_qty = loss_dict.get(sub_id, {}).get('know_lost_qty', 0)
        original_price = price_dict.get(sub_id, 0)

        # 计算拆分数量（新逻辑：销售 + 已知损耗）
        split_qty = sale_qty + know_lost_qty

        # 计算权重（拆分数量 × 原价）
        weight = split_qty * original_price
        total_weight += weight

        results.append({
            'sub_id': sub_id,
            'sub_name': sub_name,
            'sale_qty': sale_qty,
            'know_lost_qty': know_lost_qty,
            'split_qty': split_qty,
            'original_price': original_price,
            'weight': weight
        })

    # 计算分摊金额
    print("\n按新逻辑计算结果：")
    print("="*120)
    total_alloc_amt = 0
    for r in results:
        if total_weight > 0:
            alloc_ratio = r['weight'] / total_weight
            alloc_amt = alloc_ratio * parent_total_amt
        else:
            alloc_ratio = 0
            alloc_amt = 0

        total_alloc_amt += alloc_amt

        # 计算单位成本
        unit_cost = alloc_amt / r['split_qty'] if r['split_qty'] > 0 else 0

        print(f"\n  Sub ID: {r['sub_id']} | {r['sub_name']}")
        print(f"    销售数量: {r['sale_qty']} kg")
        print(f"    已知损耗: {r['know_lost_qty']} kg")
        print(f"    拆分数量(新): {r['split_qty']} kg = 销售 + 损耗")
        print(f"    销售原价: {r['original_price']} 元/kg")
        print(f"    权重: {r['weight']} = 拆分数量 × 原价")
        print(f"    分摊比例: {alloc_ratio:.4f} = 权重 / 总权重({total_weight:.2f})")
        print(f"    分摊金额(新): {alloc_amt:.2f} 元 = 分摊比例 × parent进货额({parent_total_amt:.2f})")
        print(f"    单位成本(新): {unit_cost:.2f} 元/kg = 分摊金额 / 拆分数量")

    print("\n" + "="*120)
    print(f"总权重: {total_weight:.2f}")
    print(f"分摊金额合计: {total_alloc_amt:.2f} 元（vs parent进货额 {parent_total_amt:.2f} 元）")

    # Step 7: 对比官方分摊数据
    print("\n【Step 7】对比官方 purchase_di 分摊数据")
    print("-"*80)

    sql7 = """
    SELECT
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name,
        sale_article_qty AS official_qty,
        sale_article_purchase_amt AS official_amt,
        sale_article_purchase_amt / sale_article_qty AS official_unit_cost
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND article_id = '20045463'
      AND article_id != sale_article_id
    """
    official_data = query_sql(sql7)

    official_dict = {}
    if official_data:
        for o in official_data:
            sub_id = o.get('saleArticleId', o.get('sub_article_id'))
            official_qty = float(o.get('saleArticleQty', o.get('official_qty', 0)))
            official_amt = float(o.get('saleArticlePurchaseAmt', o.get('official_amt', 0)))
            official_unit_cost = float(o.get('officialUnitCost', o.get('official_unit_cost', 0)))
            official_dict[sub_id] = {
                'official_qty': official_qty,
                'official_amt': official_amt,
                'official_unit_cost': official_unit_cost
            }

    # 对比表
    print("\n对比结果：")
    print("="*140)
    print(f"{'Sub ID':<12} {'拆分数量(新)':<12} {'拆分数量(官)':<12} {'分摊金额(新)':<12} {'分摊金额(官)':<12} {'单位成本(新)':<12} {'单位成本(官)':<12} {'差异':<12}")
    print("-"*140)

    for r in results:
        sub_id = r['sub_id']
        new_qty = r['split_qty']
        new_amt = r['weight'] / total_weight * parent_total_amt if total_weight > 0 else 0
        new_unit_cost = new_amt / new_qty if new_qty > 0 else 0

        official_qty = official_dict.get(sub_id, {}).get('official_qty', 0)
        official_amt = official_dict.get(sub_id, {}).get('official_amt', 0)
        official_unit_cost = official_dict.get(sub_id, {}).get('official_unit_cost', 0)

        diff = new_unit_cost - official_unit_cost

        print(f"{sub_id:<12} {new_qty:<12.4f} {official_qty:<12.4f} {new_amt:<12.2f} {official_amt:<12.2f} {new_unit_cost:<12.2f} {official_unit_cost:<12.2f} {diff:<12.2f}")

if __name__ == "__main__":
    main()