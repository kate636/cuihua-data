"""
重新实现BOM拆分逻辑 - 计算门店毛利

用户逻辑：
1. subsku当日拆分数量 = 销售数量 + 已知损耗数量
2. subsku当日拆分成本金额 = (sub拆分数量 × 销售原价) / SUM(所有sub拆分数量×销售原价) × parent进货额

门店毛利额 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存额 - 期初库存额)

测试：排骨 20500351, 21153037, 20003470
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
TEST_DATE = '2026-04-20'
TEST_STORE = 'A3XV'
TEST_ARTICLES = ['20500351', '21153037', '20003470']


def camel_to_snake(name):
    """驼峰转下划线"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def query_api(sql):
    """直接调用API，返回处理好的数据列表"""
    body = {"apiId": API_ID, "paramMap": {"apiId": API_ID, "sql": sql}}
    body_str = json.dumps(body, ensure_ascii=False)

    nonce = "".join(random.choices(string.ascii_letters + string.digits, k=6))
    timestamp = int(time.time() * 1000)
    encrypt = 0

    # 签名参数（过滤空值，排序）
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

    # 转换列名
    result_rows = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            new_row[camel_to_snake(k)] = v
        result_rows.append(new_row)
    return result_rows


def main():
    print("=" * 70)
    print("BOM拆分新逻辑测试")
    print(f"日期: {TEST_DATE}  门店: {TEST_STORE}")
    print(f"测试商品: {TEST_ARTICLES}")
    print("=" * 70)

    # ===== Step 1: 获取销售数据 =====
    print("\n【Step 1】获取销售数据")
    print("-" * 70)

    sales_sql = f"""
    SELECT
        abi_article_id,
        SUM(qty_spec) as sale_qty,
        SUM(sales_amt) as sale_amt
    FROM strategy_fm_sales_di
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND abi_article_id IN ('20500351', '21153037', '20003470')
    GROUP BY abi_article_id
    """
    sales = query_api(sales_sql)
    print(f"返回 {len(sales)} 行:")
    for row in sales:
        print(f"  商品 {row.get('abi_article_id')}: 销售 {row.get('sale_qty')} kg, 金额 {row.get('sale_amt')} 元")

    # ===== Step 2: 获取损耗数据 =====
    print("\n【Step 2】获取损耗数据")
    print("-" * 70)

    loss_sql = f"""
    SELECT
        article_id,
        know_lost_qty,
        know_lost_amt
    FROM strategy_fm_loss_di
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND article_id IN ('20500351', '21153037', '20003470')
    """
    loss = query_api(loss_sql)
    print(f"返回 {len(loss)} 行:")
    for row in loss:
        print(f"  商品 {row.get('article_id')}: 已知损耗 {row.get('know_lost_qty')} kg")

    # ===== Step 3: 获取价格数据 =====
    print("\n【Step 3】获取价格数据（销售原价）")
    print("-" * 70)

    price_sql = f"""
    SELECT
        sku_code,
        original_price
    FROM strategy_fm_price_da
    WHERE inc_day = '{TEST_DATE}'
        AND shop_id = '{TEST_STORE}'
        AND sku_code IN ('20500351', '21153037', '20003470')
    """
    price = query_api(price_sql)
    print(f"返回 {len(price)} 行:")
    for row in price:
        print(f"  商品 {row.get('sku_code')}: 原价 {row.get('original_price')} 元/kg")

    # ===== Step 4: 获取BOM关系 =====
    print("\n【Step 4】获取BOM关系（这些商品作为sub）")
    print("-" * 70)

    bom_sql = f"""
    SELECT
        parent_article_id,
        sub_article_id,
        dressing_rate,
        cost_rate
    FROM strategy_dim_store_article_bom_relation
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND sub_article_id IN ('20500351', '21153037', '20003470')
    """
    bom = query_api(bom_sql)
    print(f"返回 {len(bom)} 行:")
    for row in bom[:10]:  # 只打印前10条
        print(f"  Parent {row.get('parent_article_id')} -> Sub {row.get('sub_article_id')}: 出肉率 {row.get('dressing_rate')}%")
    print(f"  ... (共{len(bom)}条)")

    # ===== Step 4.5: 找到这些parent的所有其他sub =====
    if len(bom) > 0:
        parent_ids = list(set(r.get('parent_article_id') for r in bom))
        print(f"\n【Step 4.5】获取这些parent ({len(parent_ids)}个) 的所有sub商品")

        all_subs_sql = f"""
        SELECT
            parent_article_id,
            sub_article_id,
            dressing_rate,
            cost_rate
        FROM strategy_dim_store_article_bom_relation
        WHERE inc_day = '{TEST_DATE}'
            AND store_id = '{TEST_STORE}'
            AND parent_article_id IN ({','.join([f"'{p}'" for p in parent_ids[:20]])})
        """
        # 注意：如果parent太多，API可能报错，先取前20个有进货的parent
        all_subs = query_api(all_subs_sql)
        print(f"返回 {len(all_subs)} 行")

        # 找出有进货的parent
        parent_with_receive = list(set(r.get('parent_article_id') for r in receive))
        print(f"其中有进货数据的parent: {parent_with_receive}")

        # 只保留有进货的parent的BOM关系
        bom_with_receive = [r for r in bom if r.get('parent_article_id') in parent_with_receive]
        all_subs_with_receive = [r for r in all_subs if r.get('parent_article_id') in parent_with_receive]
        print(f"有进货的parent的BOM关系: {len(bom_with_receive)} 条")
    else:
        all_subs_with_receive = []

    # ===== Step 5: 获取receive_sale数据 =====
    print("\n【Step 5】获取receive_sale数据（parent进货额）")
    print("-" * 70)

    receive_sql = f"""
    SELECT
        article_id as parent_article_id,
        sale_article_id,
        inbound_qty,
        inbound_amount
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND sale_article_id IN ('20500351', '21153037', '20003470')
    """
    receive = query_api(receive_sql)
    print(f"返回 {len(receive)} 行:")
    for row in receive:
        print(f"  Parent {row.get('parent_article_id')} -> Sub {row.get('sale_article_id')}: 进货 {row.get('inbound_qty')} kg, 金额 {row.get('inbound_amount')} 元")

    # ===== 如果没有BOM关系，说明是标品 =====
    if len(bom) == 0:
        print("\n" + "=" * 70)
        print("这些商品无BOM关系（标品），直接计算")
        print("=" * 70)

        for article in TEST_ARTICLES:
            # 销售数据
            s = [r for r in sales if r.get('abi_article_id') == article]
            sale_qty = float(s[0].get('sale_qty', 0)) if s else 0
            sale_amt = float(s[0].get('sale_amt', 0)) if s else 0

            # 损耗数据
            l = [r for r in loss if r.get('article_id') == article]
            loss_qty = float(l[0].get('know_lost_qty', 0)) if l else 0

            # 价格数据
            p = [r for r in price if r.get('sku_code') == article]
            orig_price = float(p[0].get('original_price', 0)) if p else 0

            # 进货数据
            r = [r for r in receive if r.get('sale_article_id') == article]
            inbound_amt = float(r[0].get('inbound_amount', 0)) if r else 0

            # 拆分数量 = 销售 + 损耗
            split_qty = sale_qty + loss_qty

            # 成本金额（标品直接用进货额）
            cost_amt = inbound_amt

            # 单位成本
            unit_cost = cost_amt / split_qty if split_qty > 0 else 0

            # 毛利
            profit = sale_amt - sale_qty * unit_cost

            print(f"\n商品 {article}:")
            print(f"  销售数量: {sale_qty:.3f} kg")
            print(f"  销售金额: {sale_amt:.2f} 元")
            print(f"  已知损耗: {loss_qty:.3f} kg")
            print(f"  销售原价: {orig_price:.2f} 元/kg")
            print(f"  拆分数量: {split_qty:.3f} kg")
            print(f"  进货金额: {inbound_amt:.2f} 元")
            print(f"  单位成本: {unit_cost:.2f} 元/kg")
            print(f"  毛利(简化): {profit:.2f} 元")

        return

    # ===== 有BOM关系，按用户逻辑计算 =====
    print("\n" + "=" * 70)
    print("【BOM拆分计算】按用户逻辑")
    print("=" * 70)

    # 找到所有parent
    parents = list(set(b.get('parent_article_id') for b in bom))
    print(f"\n涉及的parent: {parents}")

    # 对每个parent计算
    all_allocs = []

    for parent_id in parents:
        print(f"\n{'='*60}")
        print(f"Parent: {parent_id}")
        print("=" * 60)

        # 该parent的所有sub
        parent_subs = [b for b in bom if b.get('parent_article_id') == parent_id]
        print(f"该parent有 {len(parent_subs)} 个sub商品")

        # 找parent进货数据
        parent_receives = [r for r in receive if r.get('parent_article_id') == parent_id]
        if not parent_receives:
            print("⚠ 该parent无进货数据")
            continue

        # 取第一条的进货额（parent级别的）
        parent_inbound_amt = float(parent_receives[0].get('inbound_amount', 0))
        print(f"Parent进货金额: {parent_inbound_amt:.2f} 元")

        # 计算每个sub的权重
        print("\n计算各sub权重:")
        sub_weights = []

        for sub_info in parent_subs:
            sub_id = sub_info.get('sub_article_id')

            # 销售数量
            s = [r for r in sales if r.get('abi_article_id') == sub_id]
            sale_qty = float(s[0].get('sale_qty', 0)) if s else 0

            # 已知损耗
            l = [r for r in loss if r.get('article_id') == sub_id]
            loss_qty = float(l[0].get('know_lost_qty', 0)) if l else 0

            # 拆分数量 = 销售 + 损耗
            split_qty = sale_qty + loss_qty

            # 销售原价
            p = [r for r in price if r.get('sku_code') == sub_id]
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

        # 打印权重明细
        print(f"\n{'Sub ID':<12} {'销售kg':<10} {'损耗kg':<10} {'拆分kg':<10} {'原价':<10} {'权重':<10}")
        for sw in sub_weights:
            print(f"{sw['sub_id']:<12} {sw['sale_qty']:<10.3f} {sw['loss_qty']:<10.3f} {sw['split_qty']:<10.3f} {sw['orig_price']:<10.2f} {sw['weight']:<10.2f}")

        # 计算权重总和
        total_weight = sum(sw['weight'] for sw in sub_weights)
        print(f"\n权重总和: {total_weight:.2f}")

        # 计算分配比例和分配金额
        print("\n分配计算:")
        for sw in sub_weights:
            alloc_ratio = sw['weight'] / total_weight if total_weight > 0 else 0
            alloc_amt = alloc_ratio * parent_inbound_amt

            sw['alloc_ratio'] = alloc_ratio
            sw['alloc_amt'] = alloc_amt
            sw['parent_id'] = parent_id

            print(f"  Sub {sw['sub_id']}: 分配比例 {alloc_ratio:.4f}, 分配金额 {alloc_amt:.2f} 元")
            all_allocs.append(sw)

    # ===== 汇总结果 =====
    print("\n" + "=" * 70)
    print("【最终结果】按sub汇总")
    print("=" * 70)

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

    # 加入销售额
    for s in sales:
        sub_id = s.get('abi_article_id')
        if sub_id in sub_summary:
            sub_summary[sub_id]['sale_amt'] = float(s.get('sale_amt', 0))
            sub_summary[sub_id]['sale_qty'] = float(s.get('sale_qty', 0))

    print(f"\n{'Sub ID':<12} {'销售金额':<12} {'拆分成本':<12} {'parent数':<8} {'单位成本':<12} {'毛利':<12}")
    for sub_id, info in sub_summary.items():
        unit_cost = info['total_alloc_amt'] / info['split_qty'] if info['split_qty'] > 0 else 0
        profit = info['sale_amt'] - info['sale_qty'] * unit_cost

        print(f"{sub_id:<12} {info['sale_amt']:<12.2f} {info['total_alloc_amt']:<12.2f} {len(info['parents']):<8} {unit_cost:<12.2f} {profit:<12.2f}")

        if len(info['parents']) > 1:
            print(f"  (多个parent: {info['parents']}")

    print("\n" + "=" * 70)
    print("完成")
    print("=" * 70)


if __name__ == '__main__':
    main()