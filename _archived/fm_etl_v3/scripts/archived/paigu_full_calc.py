"""
排骨(20003470)完整成本计算链路

计算逻辑：
1. 从每个parent拆分出来的成本（按新逻辑权重分配）
2. 期初库存成本
3. 综合：总成本 = 拆分成本 + 库存成本
4. 平均单位成本 = 总成本 / 总来源量
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
    print("排骨(20003470)完整成本计算链路（2026-04-23）")
    print("="*100)

    # ============================================================
    # 第一部分：收集所有原始数据
    # ============================================================
    print("\n" + "="*80)
    print("【第一部分：收集原始数据】")
    print("="*80)

    # 1. 排骨销售数据
    sql_sale = """
    SELECT
        abi_article_id,
        SUM(qty_spec) AS sale_qty,
        SUM(sales_amt) AS sale_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND abi_article_id = '20003470'
    GROUP BY abi_article_id
    """
    print("\n① 查询 sales_di（排骨销售）...")
    sale_rows = query_sql(sql_sale)
    sale_qty = safe_float(sale_rows[0].get('saleQty', sale_rows[0].get('sale_qty'))) if sale_rows else 0
    sale_amt = safe_float(sale_rows[0].get('saleAmt', sale_rows[0].get('sale_amt'))) if sale_rows else 0
    print(f"   销售量: {sale_qty} kg")
    print(f"   销售额: {sale_amt} 元")

    # 2. 排骨损耗数据
    sql_loss = """
    SELECT
        article_id,
        SUM(know_lost_qty) AS loss_qty,
        SUM(know_lost_amt) AS loss_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_loss_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '20003470'
    GROUP BY article_id
    """
    print("\n② 查询 loss_di（排骨损耗）...")
    loss_rows = query_sql(sql_loss)
    loss_qty = safe_float(loss_rows[0].get('lossQty', loss_rows[0].get('loss_qty'))) if loss_rows else 0
    loss_amt = safe_float(loss_rows[0].get('lossAmt', loss_rows[0].get('loss_amt'))) if loss_rows else 0
    print(f"   损耗量: {loss_qty} kg")
    print(f"   损耗额: {loss_amt} 元")

    # 3. 排骨销售原价
    sql_price = """
    SELECT
        sku_code,
        original_price
    FROM default_catalog.ads_business_analysis.strategy_fm_price_da
    WHERE inc_day = '2026-04-23'
      AND shop_id = 'A3XV'
      AND sku_code = '20003470'
    """
    print("\n③ 查询 price_da（排骨原价）...")
    price_rows = query_sql(sql_price)
    original_price = safe_float(price_rows[0].get('originalPrice', price_rows[0].get('original_price'))) if price_rows else 0
    print(f"   销售原价: {original_price} 元/kg")

    # 4. 排骨期初库存（从purchase_di取，因为库存数据在purchase_di里）
    sql_stock = """
    SELECT
        sale_article_id,
        init_stock_qty,
        init_stock_amt,
        end_stock_qty,
        end_stock_amt
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND sale_article_id = '20003470'
    """
    print("\n④ 查询 purchase_di（排骨库存）...")
    stock_rows = query_sql(sql_stock)
    # 注意：可能有多条记录（来自不同parent），需要看库存是否重复
    print(f"   共 {len(stock_rows)} 条库存记录")

    init_stock_qty = 0
    init_stock_amt = 0
    for r in stock_rows:
        q = safe_float(r.get('initStockQty', r.get('init_stock_qty')))
        a = safe_float(r.get('initStockAmt', r.get('init_stock_amt')))
        print(f"     记录: init_stock_qty={q}, init_stock_amt={a}")
        # 这里需要判断：库存是按sub汇总还是按parent-sub分开
        # 暂时取第一条
        if init_stock_qty == 0:  # 只取第一条
            init_stock_qty = q
            init_stock_amt = a

    print(f"   期初库存量: {init_stock_qty} kg")
    print(f"   期初库存额: {init_stock_amt} 元")
    if init_stock_qty > 0:
        init_unit_cost = init_stock_amt / init_stock_qty
        print(f"   期初单位成本: {init_unit_cost:.2f} 元/kg")
    else:
        init_unit_cost = 0
        print(f"   期初单位成本: 无数据")

    # 5. Parent 20500351（优鲜大白猪A级）的完整数据
    print("\n⑤ 查询 parent 20500351（优鲜大白猪A级）的完整数据...")

    sql_parent1 = """
    SELECT
        article_id AS parent_article_id,
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name,
        sale_article_qty AS split_qty,
        sale_article_purchase_amt AS split_amt,
        avg_inbound_price AS parent_avg_price
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '20500351'
    ORDER BY sale_article_id
    """
    parent1_rows = query_sql(sql_parent1)
    print(f"   共 {len(parent1_rows)} 条拆分记录")

    # 计算parent1进货总额
    parent1_inbound_amt = sum(safe_float(r.get('splitAmt', r.get('split_amt'))) for r in parent1_rows)
    parent1_avg_price = safe_float(parent1_rows[0].get('parentAvgPrice', parent1_rows[0].get('parent_avg_price'))) if parent1_rows else 0
    print(f"   Parent进货总额: {parent1_inbound_amt:.2f} 元")
    print(f"   Parent均价: {parent1_avg_price:.2f} 元/kg")

    # 取parent1中排骨的官方拆分数据
    parent1_paigu = next((r for r in parent1_rows if r.get('subArticleId', r.get('sub_article_id')) == '20003470'), None)
    if parent1_paigu:
        parent1_paigu_split_qty = safe_float(parent1_paigu.get('splitQty', parent1_paigu.get('split_qty')))
        parent1_paigu_split_amt = safe_float(parent1_paigu.get('splitAmt', parent1_paigu.get('split_amt')))
        print(f"   官方给排骨的拆分量: {parent1_paigu_split_qty:.4f} kg")
        print(f"   官方给排骨的拆分额: {parent1_paigu_split_amt:.2f} 元")
    else:
        parent1_paigu_split_qty = 0
        parent1_paigu_split_amt = 0

    # 6. Parent 21153037（优鲜带颈通排）的完整数据
    print("\n⑥ 查询 parent 21153037（优鲜带颈通排）的完整数据...")

    sql_parent2 = """
    SELECT
        article_id AS parent_article_id,
        sale_article_id AS sub_article_id,
        sale_article_name AS sub_name,
        sale_article_qty AS split_qty,
        sale_article_purchase_amt AS split_amt,
        avg_inbound_price AS parent_avg_price
    FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
      AND article_id = '21153037'
    ORDER BY sale_article_id
    """
    parent2_rows = query_sql(sql_parent2)
    print(f"   共 {len(parent2_rows)} 条拆分记录")

    # 计算parent2进货总额
    parent2_inbound_amt = sum(safe_float(r.get('splitAmt', r.get('split_amt'))) for r in parent2_rows)
    parent2_avg_price = safe_float(parent2_rows[0].get('parentAvgPrice', parent2_rows[0].get('parent_avg_price'))) if parent2_rows else 0
    print(f"   Parent进货总额: {parent2_inbound_amt:.2f} 元")
    print(f"   Parent均价: {parent2_avg_price:.2f} 元/kg")

    # 取parent2中排骨的官方拆分数据
    parent2_paigu = next((r for r in parent2_rows if r.get('subArticleId', r.get('sub_article_id')) == '20003470'), None)
    if parent2_paigu:
        parent2_paigu_split_qty = safe_float(parent2_paigu.get('splitQty', parent2_paigu.get('split_qty')))
        parent2_paigu_split_amt = safe_float(parent2_paigu.get('splitAmt', parent2_paigu.get('split_amt')))
        print(f"   官方给排骨的拆分量: {parent2_paigu_split_qty:.4f} kg")
        print(f"   官方给排骨的拆分额: {parent2_paigu_split_amt:.2f} 元")
    else:
        parent2_paigu_split_qty = 0
        parent2_paigu_split_amt = 0

    # ============================================================
    # 第二部分：新逻辑计算每个parent对排骨的成本分摊
    # ============================================================
    print("\n" + "="*80)
    print("【第二部分：新逻辑计算 - Parent 20500351 对排骨的成本分摊】")
    print("="*80)

    # 需要获取parent1所有sub的销售+损耗数据来计算权重
    print("\n步骤1: 获取 parent1 所有 sub 的销售量和损耗量...")

    # 获取parent1所有sub的id
    parent1_sub_ids = [r.get('subArticleId', r.get('sub_article_id')) for r in parent1_rows]
    print(f"   Sub数量: {len(parent1_sub_ids)}")

    # 查所有sub的销售量
    sql_all_sales = """
    SELECT
        abi_article_id AS sub_article_id,
        SUM(qty_spec) AS sale_qty
    FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
    GROUP BY abi_article_id
    """
    all_sales_rows = query_sql(sql_all_sales)
    sale_qty_dict = {}
    for r in all_sales_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        qty = safe_float(r.get('saleQty', r.get('sale_qty')))
        sale_qty_dict[sub_id] = qty

    # 查所有sub的损耗量
    sql_all_loss = """
    SELECT
        article_id AS sub_article_id,
        SUM(know_lost_qty) AS loss_qty
    FROM default_catalog.ads_business_analysis.strategy_fm_loss_di
    WHERE inc_day = '2026-04-23'
      AND store_id = 'A3XV'
    GROUP BY article_id
    """
    all_loss_rows = query_sql(sql_all_loss)
    loss_qty_dict = {}
    for r in all_loss_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        qty = safe_float(r.get('lossQty', r.get('loss_qty')))
        loss_qty_dict[sub_id] = qty

    # 查所有sub的原价
    sql_all_price = """
    SELECT
        sku_code AS sub_article_id,
        original_price
    FROM default_catalog.ads_business_analysis.strategy_fm_price_da
    WHERE inc_day = '2026-04-23'
      AND shop_id = 'A3XV'
    """
    all_price_rows = query_sql(sql_all_price)
    price_dict = {}
    for r in all_price_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        price = safe_float(r.get('originalPrice', r.get('original_price')))
        price_dict[sub_id] = price

    print("步骤2: 计算每个 sub 的权重...")

    parent1_weights = []
    for r in parent1_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        sub_name = r.get('subName', r.get('sub_name', ''))
        sale_qty = sale_qty_dict.get(sub_id, 0)
        loss_qty = loss_qty_dict.get(sub_id, 0)
        price = price_dict.get(sub_id, 0)
        split_qty = sale_qty + loss_qty
        weight = split_qty * price
        parent1_weights.append({
            'sub_id': sub_id,
            'sub_name': sub_name,
            'sale_qty': sale_qty,
            'loss_qty': loss_qty,
            'split_qty': split_qty,
            'price': price,
            'weight': weight
        })

    parent1_total_weight = sum(w['weight'] for w in parent1_weights)
    print(f"   总权重: {parent1_total_weight:.2f} 元")

    # 找排骨在parent1中的权重
    paigu_weight1 = next((w for w in parent1_weights if w['sub_id'] == '20003470'), None)
    if paigu_weight1:
        print(f"\n步骤3: 排骨在 parent1 中的权重计算")
        print(f"   排骨销售量: {paigu_weight1['sale_qty']} kg")
        print(f"   排骨损耗量: {paigu_weight1['loss_qty']} kg")
        print(f"   排骨拆分量: {paigu_weight1['split_qty']} kg")
        print(f"   排骨原价: {paigu_weight1['price']} 元/kg")
        print(f"   排骨权重: {paigu_weight1['weight']} 元")

        print(f"\n步骤4: 排骨从 parent1 分摊的成本")
        if parent1_total_weight > 0 and paigu_weight1['weight'] > 0:
            parent1_paigu_alloc_amt = paigu_weight1['weight'] / parent1_total_weight * parent1_inbound_amt
            print(f"   分摊额 = (排骨权重 / 总权重) × parent进货总额")
            print(f"         = ({paigu_weight1['weight']} / {parent1_total_weight}) × {parent1_inbound_amt:.2f}")
            print(f"         = {paigu_weight1['weight']/parent1_total_weight:.6f} × {parent1_inbound_amt:.2f}")
            print(f"         = {parent1_paigu_alloc_amt:.2f} 元")

            if paigu_weight1['split_qty'] > 0:
                parent1_paigu_unit_cost = parent1_paigu_alloc_amt / paigu_weight1['split_qty']
                print(f"   单位成本 = 分摊额 / 拆分量")
                print(f"           = {parent1_paigu_alloc_amt:.2f} / {paigu_weight1['split_qty']}")
                print(f"           = {parent1_paigu_unit_cost:.2f} 元/kg")
            else:
                parent1_paigu_unit_cost = 0
        else:
            parent1_paigu_alloc_amt = 0
            parent1_paigu_unit_cost = 0
            print(f"   排骨权重为0，不参与分摊")
    else:
        parent1_paigu_alloc_amt = 0
        parent1_paigu_unit_cost = 0
        print("   排骨不在parent1的sub列表中")

    # ============================================================
    # 第三部分：新逻辑计算 Parent 21153037 对排骨的成本分摊
    # ============================================================
    print("\n" + "="*80)
    print("【第三部分：新逻辑计算 - Parent 21153037 对排骨的成本分摊】")
    print("="*80)

    parent2_sub_ids = [r.get('subArticleId', r.get('sub_article_id')) for r in parent2_rows]
    print(f"\n步骤1: Parent2 有 {len(parent2_sub_ids)} 个 sub")

    parent2_weights = []
    for r in parent2_rows:
        sub_id = r.get('subArticleId', r.get('sub_article_id'))
        sub_name = r.get('subName', r.get('sub_name', ''))
        sale_qty = sale_qty_dict.get(sub_id, 0)
        loss_qty = loss_qty_dict.get(sub_id, 0)
        price = price_dict.get(sub_id, 0)
        split_qty = sale_qty + loss_qty
        weight = split_qty * price
        parent2_weights.append({
            'sub_id': sub_id,
            'sub_name': sub_name,
            'sale_qty': sale_qty,
            'loss_qty': loss_qty,
            'split_qty': split_qty,
            'price': price,
            'weight': weight
        })

    parent2_total_weight = sum(w['weight'] for w in parent2_weights)
    print(f"   总权重: {parent2_total_weight:.2f} 元")

    # 找排骨在parent2中的权重
    paigu_weight2 = next((w for w in parent2_weights if w['sub_id'] == '20003470'), None)
    if paigu_weight2:
        print(f"\n步骤2: 排骨在 parent2 中的权重计算")
        print(f"   排骨销售量: {paigu_weight2['sale_qty']} kg")
        print(f"   排骨损耗量: {paigu_weight2['loss_qty']} kg")
        print(f"   排骨拆分量: {paigu_weight2['split_qty']} kg")
        print(f"   排骨原价: {paigu_weight2['price']} 元/kg")
        print(f"   排骨权重: {paigu_weight2['weight']} 元")

        print(f"\n步骤3: 排骨从 parent2 分摊的成本")
        if parent2_total_weight > 0 and paigu_weight2['weight'] > 0:
            parent2_paigu_alloc_amt = paigu_weight2['weight'] / parent2_total_weight * parent2_inbound_amt
            print(f"   分摊额 = (排骨权重 / 总权重) × parent进货总额")
            print(f"         = ({paigu_weight2['weight']} / {parent2_total_weight}) × {parent2_inbound_amt:.2f}")
            print(f"         = {paigu_weight2['weight']/parent2_total_weight:.6f} × {parent2_inbound_amt:.2f}")
            print(f"         = {parent2_paigu_alloc_amt:.2f} 元")

            if paigu_weight2['split_qty'] > 0:
                parent2_paigu_unit_cost = parent2_paigu_alloc_amt / paigu_weight2['split_qty']
                print(f"   单位成本 = 分摊额 / 拆分量")
                print(f"           = {parent2_paigu_alloc_amt:.2f} / {paigu_weight2['split_qty']}")
                print(f"           = {parent2_paigu_unit_cost:.2f} 元/kg")
            else:
                parent2_paigu_unit_cost = 0
        else:
            parent2_paigu_alloc_amt = 0
            parent2_paigu_unit_cost = 0
            print(f"   排骨权重为0，不参与分摊")
    else:
        parent2_paigu_alloc_amt = 0
        parent2_paigu_unit_cost = 0
        print("   排骨不在parent2的sub列表中")

    # ============================================================
    # 第四部分：综合成本计算
    # ============================================================
    print("\n" + "="*80)
    print("【第四部分：综合成本计算】")
    print("="*80)

    print("\n排骨(20003470)的成本来源汇总：")

    # 来源1: 从parent1拆分
    print(f"\n来源①: 从 parent1(20500351) 拆分")
    if paigu_weight1:
        print(f"   拆分量: {paigu_weight1['split_qty']} kg")
        print(f"   分摊成本: {parent1_paigu_alloc_amt:.2f} 元")
        print(f"   单位成本: {parent1_paigu_unit_cost:.2f} 元/kg")

    # 来源2: 从parent2拆分
    print(f"\n来源②: 从 parent2(21153037) 拆分")
    if paigu_weight2:
        print(f"   拆分量: {paigu_weight2['split_qty']} kg")
        print(f"   分摊成本: {parent2_paigu_alloc_amt:.2f} 元")
        print(f"   单位成本: {parent2_paigu_unit_cost:.2f} 元/kg")

    # 来源3: 期初库存
    print(f"\n来源③: 期初库存")
    print(f"   期初量: {init_stock_qty} kg")
    print(f"   期初额: {init_stock_amt} 元")
    if init_stock_qty > 0:
        print(f"   单位成本: {init_unit_cost:.2f} 元/kg")

    # 综合计算
    print("\n" + "-"*60)
    print("综合平均成本计算：")

    total_source_qty = 0
    total_source_amt = 0

    if paigu_weight1 and paigu_weight1['split_qty'] > 0:
        total_source_qty += paigu_weight1['split_qty']
        total_source_amt += parent1_paigu_alloc_amt

    if paigu_weight2 and paigu_weight2['split_qty'] > 0:
        total_source_qty += paigu_weight2['split_qty']
        total_source_amt += parent2_paigu_alloc_amt

    if init_stock_qty > 0:
        total_source_qty += init_stock_qty
        total_source_amt += init_stock_amt

    print(f"  总来源量 = {paigu_weight1['split_qty'] if paigu_weight1 else 0} + {paigu_weight2['split_qty'] if paigu_weight2 else 0} + {init_stock_qty}")
    print(f"           = {total_source_qty} kg")
    print(f"  总来源额 = {parent1_paigu_alloc_amt:.2f} + {parent2_paigu_alloc_amt:.2f} + {init_stock_amt:.2f}")
    print(f"           = {total_source_amt:.2f} 元")

    if total_source_qty > 0:
        avg_unit_cost = total_source_amt / total_source_qty
        print(f"\n  平均单位成本 = 总来源额 / 总来源量")
        print(f"              = {total_source_amt:.2f} / {total_source_qty}")
        print(f"              = {avg_unit_cost:.2f} 元/kg")
    else:
        avg_unit_cost = 0
        print(f"\n  无法计算平均成本（无来源数据）")

    # ============================================================
    # 第五部分：毛利计算
    # ============================================================
    print("\n" + "="*80)
    print("【第五部分：毛利计算】")
    print("="*80)

    print(f"\n排骨当日经营数据：")
    print(f"  销售量: {sale_qty} kg")
    print(f"  销售额: {sale_amt} 元")
    print(f"  损耗量: {loss_qty} kg")
    print(f"  损耗额: {loss_amt} 元")

    # 销售成本 = 销售量 × 平均单位成本
    sale_cost = sale_qty * avg_unit_cost
    print(f"\n销售成本计算：")
    print(f"  销售成本 = 销售量 × 平均单位成本")
    print(f"          = {sale_qty} × {avg_unit_cost:.2f}")
    print(f"          = {sale_cost:.2f} 元")

    # 门店毛利 = 销售额 - 销售成本 - 损耗额
    profit = sale_amt - sale_cost - loss_amt
    margin = (profit / sale_amt * 100) if sale_amt > 0 else 0

    print(f"\n门店毛利计算：")
    print(f"  毛利 = 销售额 - 销售成本 - 损耗额")
    print(f"      = {sale_amt} - {sale_cost:.2f} - {loss_amt}")
    print(f"      = {profit:.2f} 元")
    print(f"  毛利率 = 毛利 / 销售额 × 100%")
    print(f"        = {profit:.2f} / {sale_amt} × 100%")
    print(f"        = {margin:.1f}%")

    # ============================================================
    # 第六部分：与官方逻辑对比
    # ============================================================
    print("\n" + "="*80)
    print("【第六部分：与官方逻辑对比】")
    print("="*80)

    # 官方逻辑：直接用purchase_di的分摊数据
    official_split_qty = parent1_paigu_split_qty + parent2_paigu_split_qty
    official_split_amt = parent1_paigu_split_amt + parent2_paigu_split_amt

    print(f"\n官方 purchase_di 给的数据：")
    print(f"  从 parent1 拆分量: {parent1_paigu_split_qty:.4f} kg, 拆分额: {parent1_paigu_split_amt:.2f} 元")
    print(f"  从 parent2 拆分量: {parent2_paigu_split_qty:.4f} kg, 拆分额: {parent2_paigu_split_amt:.2f} 元")
    print(f"  总拆分量: {official_split_qty:.4f} kg")
    print(f"  总拆分额: {official_split_amt:.2f} 元")

    if official_split_qty > 0:
        official_unit_cost = official_split_amt / official_split_qty
        print(f"  官方单位成本: {official_unit_cost:.2f} 元/kg")
    else:
        official_unit_cost = 0

    # 官方销售成本
    official_sale_cost = sale_qty * official_unit_cost if official_unit_cost > 0 else 0
    official_profit = sale_amt - official_sale_cost - loss_amt
    official_margin = (official_profit / sale_amt * 100) if sale_amt > 0 else 0

    print(f"\n官方逻辑下的毛利：")
    print(f"  销售成本 = {sale_qty} × {official_unit_cost:.2f} = {official_sale_cost:.2f} 元")
    print(f"  毛利 = {sale_amt} - {official_sale_cost:.2f} - {loss_amt} = {official_profit:.2f} 元")
    print(f"  毛利率 = {official_margin:.1f}%")

    print(f"\n差异对比：")
    print(f"  单位成本: 新逻辑={avg_unit_cost:.2f} vs 官方={official_unit_cost:.2f}")
    print(f"  销售成本: 新逻辑={sale_cost:.2f} vs 官方={official_sale_cost:.2f}")
    print(f"  毛利:     新逻辑={profit:.2f} vs 官方={official_profit:.2f}")
    print(f"  毛利率:   新逻辑={margin:.1f}% vs 官方={official_margin:.1f}%")

if __name__ == "__main__":
    main()