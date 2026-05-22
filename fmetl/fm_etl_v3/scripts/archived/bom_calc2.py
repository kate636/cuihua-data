"""
重新实现BOM拆分逻辑 - 正确获取parent的所有sub数据

用户逻辑：
1. subsku当日拆分数量 = 销售数量 + 已知损耗数量
2. subsku当日拆分成本金额 = (sub拆分数量 × 销售原价) / SUM(parent下所有sub拆分数量×销售原价) × parent进货额

关键点：需要获取每个parent的所有sub商品的销售和价格数据
"""

import os
import hashlib
import json
import random
import string
import time
import requests
import re
from dotenv import load_dotenv

load_dotenv()

# API配置
HOST = os.getenv("QDM_HOST", "https://bdapp.qdama.cn")
API_ID = os.getenv("QDM_API_ID", "i_fjl10g687-790")
ACCESS_KEY = os.environ["QDM_ACCESS_KEY"]
SECRET_KEY = os.environ["QDM_SECRET_KEY"]
VERSION = os.getenv("QDM_VERSION", "1.0")

# 测试参数
TEST_DATE = '2026-04-23'
TEST_STORE = 'A3XV'
TARGET_ARTICLES = ['20500351', '21153037', '20003470']


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def query_api(sql):
    body = {"apiId": API_ID, "paramMap": {"apiId": API_ID, "sql": sql}}
    body_str = json.dumps(body, ensure_ascii=False)

    nonce = "".join(random.choices(string.ascii_letters + string.digits, k=6))
    timestamp = int(time.time() * 1000)
    encrypt = 0

    sign_params = {
        "AccessKey": ACCESS_KEY,
        "encrypt": encrypt,
        "nonce": nonce,
        "timestamp": timestamp,
        "version": VERSION,
        "bodyStr": body_str,
    }
    keys = sorted(k for k, v in sign_params.items() if v not in (None, ""))
    param_str = "&".join(f"{k}={sign_params[k]}" for k in keys)
    param_str += f"&SecretKey={SECRET_KEY}"
    sign = hashlib.md5(param_str.encode("utf-8")).hexdigest().upper()

    url = f"{HOST}/api/v1/executeApi/{API_ID}?AccessKey={ACCESS_KEY}&timestamp={timestamp}&nonce={nonce}&encrypt={encrypt}&version={VERSION}&sign={sign}"

    resp = requests.post(url, data=body_str.encode(), headers={"Content-Type": "application/json"}, timeout=120)
    result = resp.json()

    if result.get("code") != 0:
        raise RuntimeError(f"API error: {result.get('msg')}")

    data = result.get("data")
    if isinstance(data, dict) and "pageData" in data:
        rows = data["pageData"]
    elif isinstance(data, list):
        rows = data
    else:
        return []

    result_rows = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            new_row[camel_to_snake(k)] = v
        result_rows.append(new_row)
    return result_rows


def main():
    print("=" * 70)
    print("BOM拆分新逻辑 - 正确计算")
    print(f"日期: {TEST_DATE}  门店: {TEST_STORE}")
    print(f"目标商品: {TARGET_ARTICLES}")
    print("=" * 70)

    # ===== Step 1: 找目标商品的BOM关系 =====
    print("\n【Step 1】找目标商品的BOM关系（作为sub）")

    bom_sql = f"""
    SELECT
        parent_article_id,
        sub_article_id,
        dressing_rate
    FROM strategy_dim_store_article_bom_relation
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND sub_article_id IN ({','.join([f"'{a}'" for a in TARGET_ARTICLES])})
    """
    target_bom = query_api(bom_sql)
    print(f"目标商品的BOM关系: {len(target_bom)} 条")

    if len(target_bom) == 0:
        print("无BOM关系，这些商品是标品")
        return

    # 找到涉及的parent
    target_parents = list(set(r.get('parent_article_id') for r in target_bom))
    print(f"涉及的parent: {target_parents[:10]}... (共{len(target_parents)}个)")

    # ===== Step 2: 找这些parent的所有sub商品 =====
    print("\n【Step 2】获取这些parent的所有sub商品")

    # 分批查询避免IN列表过长
    all_parent_subs = []
    batch_size = 50
    for i in range(0, len(target_parents), batch_size):
        batch_parents = target_parents[i:i+batch_size]
        all_subs_sql = f"""
        SELECT
            parent_article_id,
            sub_article_id,
            dressing_rate
        FROM strategy_dim_store_article_bom_relation
        WHERE inc_day = '{TEST_DATE}'
            AND store_id = '{TEST_STORE}'
            AND parent_article_id IN ({','.join([f"'{p}'" for p in batch_parents])})
        """
        batch_result = query_api(all_subs_sql)
        all_parent_subs.extend(batch_result)

    # 获取所有sub商品ID
    all_sub_ids = list(set(r.get('sub_article_id') for r in all_parent_subs))
    print(f"所有sub商品: {len(all_sub_ids)} 个")

    # ===== Step 3: 获取所有sub的销售数据 =====
    print("\n【Step 3】获取所有sub的销售数据")

    all_sales = []
    for i in range(0, len(all_sub_ids), batch_size):
        batch_subs = all_sub_ids[i:i+batch_size]
        sales_sql = f"""
        SELECT
            abi_article_id,
            SUM(qty_spec) as sale_qty,
            SUM(sales_amt) as sale_amt
        FROM strategy_fm_sales_di
        WHERE inc_day = '{TEST_DATE}'
            AND store_id = '{TEST_STORE}'
            AND abi_article_id IN ({','.join([f"'{s}'" for s in batch_subs])})
        GROUP BY abi_article_id
        """
        batch_result = query_api(sales_sql)
        all_sales.extend(batch_result)

    print(f"销售数据: {len(all_sales)} 条")
    # 打印目标商品的销售数据
    for t in TARGET_ARTICLES:
        s = [r for r in all_sales if r.get('abi_article_id') == t]
        if s:
            print(f"  {t}: 销售 {s[0].get('sale_qty')} kg, 金额 {s[0].get('sale_amt')} 元")

    # ===== Step 4: 获取所有sub的价格数据 =====
    print("\n【Step 4】获取所有sub的价格数据")

    all_prices = []
    for i in range(0, len(all_sub_ids), batch_size):
        batch_subs = all_sub_ids[i:i+batch_size]
        price_sql = f"""
        SELECT
            sku_code,
            original_price
        FROM strategy_fm_price_da
        WHERE inc_day = '{TEST_DATE}'
            AND shop_id = '{TEST_STORE}'
            AND sku_code IN ({','.join([f"'{s}'" for s in batch_subs])})
        """
        batch_result = query_api(price_sql)
        all_prices.extend(batch_result)

    print(f"价格数据: {len(all_prices)} 条")
    for t in TARGET_ARTICLES:
        p = [r for r in all_prices if r.get('sku_code') == t]
        if p:
            print(f"  {t}: 原价 {p[0].get('original_price')} 元/kg")

    # ===== Step 5: 获取所有sub的损耗数据 =====
    print("\n【Step 5】获取所有sub的损耗数据")

    all_losses = []
    for i in range(0, len(all_sub_ids), batch_size):
        batch_subs = all_sub_ids[i:i+batch_size]
        loss_sql = f"""
        SELECT
            article_id,
            know_lost_qty,
            know_lost_amt
        FROM strategy_fm_loss_di
        WHERE inc_day = '{TEST_DATE}'
            AND store_id = '{TEST_STORE}'
            AND article_id IN ({','.join([f"'{s}'" for s in batch_subs])})
        """
        batch_result = query_api(loss_sql)
        all_losses.extend(batch_result)

    print(f"损耗数据: {len(all_losses)} 条")

    # ===== Step 5.5: 获取商品名称 =====
    print("\n【Step 5.5】获取商品名称")

    # 需要获取所有涉及商品的名称：sub商品 + parent商品（target_parents已在Step 1定义）
    all_article_ids = list(set(all_sub_ids + target_parents))
    article_names = {}
    for i in range(0, len(all_article_ids), batch_size):
        batch_articles = all_article_ids[i:i+batch_size]
        goods_sql = f"""
        SELECT
            article_id,
            article_name
        FROM strategy_fm_dim_goods
        WHERE article_id IN ({','.join([f"'{a}'" for a in batch_articles])})
        """
        batch_result = query_api(goods_sql)
        for r in batch_result:
            article_names[r.get('article_id')] = r.get('article_name', '')

    print(f"商品名称: {len(article_names)} 条")
    for t in TARGET_ARTICLES:
        name = article_names.get(t, '(未知)')
        print(f"  {t}: {name}")

    # ===== Step 6: 获取parent进货数据 =====
    print("\n【Step 6】获取parent进货数据（从receive_sale）")

    # 注意：receive_sale表中同一个parent对所有sub都记录相同的inbound_amount（parent总进货额）
    # 所以需要去重，不能直接SUM
    receive_sql = f"""
    SELECT
        article_id as parent_article_id,
        inbound_qty,
        inbound_amount
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND article_id IN ({','.join([f"'{p}'" for p in target_parents[:50]])})
    GROUP BY article_id, inbound_qty, inbound_amount
    """
    receive_data = query_api(receive_sql)
    print(f"进货数据: {len(receive_data)} 条（去重后的parent级别）")

    # 打印parent进货信息
    parents_with_receive = {}
    for r in receive_data:
        parent_id = r.get('parent_article_id')
        # 由于已经GROUP BY去重，每个parent只有一行
        parents_with_receive[parent_id] = {
            'inbound_qty': float(r.get('inbound_qty', 0)),
            'inbound_amount': float(r.get('inbound_amount', 0))
        }

    print(f"有进货的parent: {len(parents_with_receive)} 个")
    for p_id, info in list(parents_with_receive.items())[:10]:
        p_name = article_names.get(p_id, '(未知)')
        print(f"  {p_id} ({p_name}): 进货 {info['inbound_qty']} kg, 金额 {info['inbound_amount']} 元")

    # ===== Step 7: BOM拆分计算 =====
    print("\n" + "=" * 70)
    print("【BOM拆分计算】")
    print("=" * 70)

    # 只处理有进货数据的parent
    all_allocs = []
    for parent_id, parent_info in parents_with_receive.items():
        p_name = article_names.get(parent_id, '(未知)')
        print(f"\n{'='*60}")
        print(f"Parent: {parent_id} ({p_name})")
        print(f"进货量: {parent_info['inbound_qty']:.2f} kg, 进货额: {parent_info['inbound_amount']:.2f} 元")
        print("=" * 60)

        # 该parent的所有sub
        parent_subs = [r for r in all_parent_subs if r.get('parent_article_id') == parent_id]
        print(f"该parent有 {len(parent_subs)} 个sub商品")

        if len(parent_subs) == 0:
            continue

        # 计算每个sub的权重
        sub_weights = []
        for sub_info in parent_subs:
            sub_id = sub_info.get('sub_article_id')

            # 销售数量
            s = [r for r in all_sales if r.get('abi_article_id') == sub_id]
            sale_qty = float(s[0].get('sale_qty', 0)) if s else 0

            # 已知损耗
            l = [r for r in all_losses if r.get('article_id') == sub_id]
            loss_qty = float(l[0].get('know_lost_qty', 0)) if l else 0

            # 拆分数量 = 销售 + 损耗
            split_qty = sale_qty + loss_qty

            # 销售原价
            p = [r for r in all_prices if r.get('sku_code') == sub_id]
            orig_price = float(p[0].get('original_price', 0)) if p else 0

            # 权重 = 拆分数量 × 原价
            weight = split_qty * orig_price

            sub_weights.append({
                'sub_id': sub_id,
                'sale_qty': sale_qty,
                'loss_qty': loss_qty,
                'split_qty': split_qty,
                'orig_price': orig_price,
                'weight': weight
            })

        # 打印前10个sub的权重明细
        print(f"\n各sub权重（前10个）:")
        print(f"{'Sub ID':<12} {'名称':<20} {'销售kg':<10} {'损耗kg':<10} {'拆分kg':<10} {'原价':<10} {'权重':<10}")
        for sw in sub_weights[:10]:
            sub_name = article_names.get(sw['sub_id'], '(未知)')
            sub_name_short = sub_name[:18] if len(sub_name) > 18 else sub_name
            print(f"{sw['sub_id']:<12} {sub_name_short:<20} {sw['sale_qty']:<10.3f} {sw['loss_qty']:<10.3f} {sw['split_qty']:<10.3f} {sw['orig_price']:<10.2f} {sw['weight']:<10.2f}")
        if len(sub_weights) > 10:
            print(f"  ... (共{len(sub_weights)}个sub)")

        # 计算权重总和
        total_weight = sum(sw['weight'] for sw in sub_weights)
        print(f"\n权重总和: {total_weight:.2f}")

        if total_weight == 0:
            print("⚠ 权重总和为0，无法分配")
            continue

        # 计算分配金额
        print("\n分配计算（显示目标商品或权重>0的）:")
        for sw in sub_weights:
            alloc_ratio = sw['weight'] / total_weight
            alloc_amt = alloc_ratio * parent_info['inbound_amount']

            sw['alloc_ratio'] = alloc_ratio
            sw['alloc_amt'] = alloc_amt
            sw['parent_id'] = parent_id

            # 只打印目标商品或权重>0的
            if sw['sub_id'] in TARGET_ARTICLES or sw['weight'] > 0:
                sub_name = article_names.get(sw['sub_id'], '(未知)')
                print(f"  {sw['sub_id']} ({sub_name}): 分配比例 {alloc_ratio:.4f}, 分配金额 {alloc_amt:.2f} 元")

            all_allocs.append(sw)

    # ===== Step 8: 汇总结果 =====
    print("\n" + "=" * 70)
    print("【最终结果】目标商品的BOM拆分汇总")
    print("=" * 70)

    # 按sub汇总
    sub_summary = {}
    for alloc in all_allocs:
        sub_id = alloc['sub_id']
        if sub_id not in sub_summary:
            sub_summary[sub_id] = {
                'sale_qty': 0,
                'sale_amt': 0,
                'split_qty': 0,
                'total_alloc_amt': 0,
                'parents': []
            }

        sub_summary[sub_id]['split_qty'] = alloc['split_qty']
        sub_summary[sub_id]['total_alloc_amt'] += alloc['alloc_amt']
        sub_summary[sub_id]['parents'].append({
            'parent_id': alloc['parent_id'],
            'alloc_amt': alloc['alloc_amt'],
            'alloc_ratio': alloc['alloc_ratio']
        })

    # 加入销售金额
    for s in all_sales:
        sub_id = s.get('abi_article_id')
        if sub_id in sub_summary:
            sub_summary[sub_id]['sale_amt'] = float(s.get('sale_amt', 0))
            sub_summary[sub_id]['sale_qty'] = float(s.get('sale_qty', 0))

    # 打印目标商品的结果
    print(f"\n{'商品ID':<12} {'名称':<20} {'销售金额':<12} {'拆分成本':<12} {'来源parent':<10} {'单位成本':<12} {'毛利':<12}")
    for sub_id in TARGET_ARTICLES:
        sub_name = article_names.get(sub_id, '(未知)')
        sub_name_short = sub_name[:18] if len(sub_name) > 18 else sub_name

        if sub_id not in sub_summary:
            print(f"{sub_id:<12} {sub_name_short:<20} (无BOM拆分数据)")
            continue

        info = sub_summary[sub_id]
        unit_cost = info['total_alloc_amt'] / info['split_qty'] if info['split_qty'] > 0 else 0
        profit = info['sale_amt'] - info['sale_qty'] * unit_cost

        print(f"{sub_id:<12} {sub_name_short:<20} {info['sale_amt']:<12.2f} {info['total_alloc_amt']:<12.2f} {len(info['parents']):<10} {unit_cost:<12.2f} {profit:<12.2f}")

        if len(info['parents']) > 0:
            print(f"  来源parent明细:")
            for p in info['parents']:
                p_name = article_names.get(p['parent_id'], '(未知)')
                print(f"    {p['parent_id']} ({p_name}): 分配金额 {p['alloc_amt']:.2f} 元, 比例 {p['alloc_ratio']:.4f}")

    print("\n" + "=" * 70)
    print("计算完成")
    print("=" * 70)


if __name__ == '__main__':
    main()