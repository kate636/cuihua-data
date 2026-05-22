"""
BOM拆分逻辑 - 用户定义的正确分配方式

核心逻辑：
1. 找出哪些sub有多个parent（比如排骨等4个sku都有大白猪和通排2个parent）
2. parent分配优先级：
   - 先满足"只有该parent一个来源的sub"
   - 剩余量分配给"多parent的sub"

3. 大白猪分配给多parent-sub的量 = 大白猪进货 - 大白猪单parent sub需求
4. 通排分配给多parent-sub的量 = 多parent-sub总需求 - 大白猪分配量

5. 成本 = 分配重量 × parent进货单价
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
from collections import defaultdict

load_dotenv()

HOST = os.getenv("QDM_HOST", "https://bdapp.qdama.cn")
API_ID = os.getenv("QDM_API_ID", "i_fjl10g687-790")
ACCESS_KEY = os.environ["QDM_ACCESS_KEY"]
SECRET_KEY = os.environ["QDM_SECRET_KEY"]
VERSION = os.getenv("QDM_VERSION", "1.0")

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
        "AccessKey": ACCESS_KEY, "encrypt": encrypt, "nonce": nonce,
        "timestamp": timestamp, "version": VERSION, "bodyStr": body_str,
    }
    keys = sorted(k for k, v in sign_params.items() if v not in (None, ""))
    param_str = "&".join(f"{k}={sign_params[k]}" for k in keys) + f"&SecretKey={SECRET_KEY}"
    sign = hashlib.md5(param_str.encode()).hexdigest().upper()
    url = f"{HOST}/api/v1/executeApi/{API_ID}?AccessKey={ACCESS_KEY}&timestamp={timestamp}&nonce={nonce}&encrypt={encrypt}&version={VERSION}&sign={sign}"
    resp = requests.post(url, data=body_str.encode(), headers={"Content-Type": "application/json"}, timeout=120)
    result = resp.json()
    if result.get("code") != 0:
        raise RuntimeError(f"API error: {result.get('msg')}")
    data = result.get("data")
    rows = data.get("pageData") if isinstance(data, dict) else data if isinstance(data, list) else []
    return [{camel_to_snake(k): v for k, v in row.items()} for row in rows]


def main():
    print("=" * 70)
    print("BOM拆分计算 - 正确的parent优先级分配")
    print(f"日期: {TEST_DATE}  门店: {TEST_STORE}")
    print("=" * 70)

    batch_size = 50

    # ===== Step 1: 获取目标商品的BOM关系 =====
    print("\n【Step 1】找目标商品的BOM关系")

    bom_sql = f"""
    SELECT
        parent_article_id,
        sub_article_id
    FROM strategy_dim_store_article_bom_relation
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND sub_article_id IN ({','.join([f"'{a}'" for a in TARGET_ARTICLES])})
    """
    target_bom = query_api(bom_sql)
    target_parents = list(set(r.get('parent_article_id') for r in target_bom))
    print(f"目标商品的parent: {target_parents[:10]}... (共{len(target_parents)}个)")

    # ===== Step 2: 获取这些parent的所有sub =====
    print("\n【Step 2】获取parent的所有sub商品及parent来源数")

    all_parent_subs = []
    for i in range(0, len(target_parents), batch_size):
        batch_parents = target_parents[i:i+batch_size]
        all_subs_sql = f"""
        SELECT
            parent_article_id,
            sub_article_id
        FROM strategy_dim_store_article_bom_relation
        WHERE inc_day = '{TEST_DATE}'
            AND store_id = '{TEST_STORE}'
            AND parent_article_id IN ({','.join([f"'{p}'" for p in batch_parents])})
        """
        all_parent_subs.extend(query_api(all_subs_sql))

    # 统计每个sub有多少个parent来源
    sub_parent_count = defaultdict(list)
    for r in all_parent_subs:
        sub_parent_count[r.get('sub_article_id')].append(r.get('parent_article_id'))

    # 分类：单parent vs 多parent
    single_parent_subs = {sub: parents[0] for sub, parents in sub_parent_count.items() if len(parents) == 1}
    multi_parent_subs = {sub: parents for sub, parents in sub_parent_count.items() if len(parents) > 1}

    print(f"所有sub: {len(sub_parent_count)} 个")
    print(f"单parent的sub: {len(single_parent_subs)} 个")
    print(f"多parent的sub: {len(multi_parent_subs)} 个")

    # ===== Step 3: 获取parent进货数据 =====
    print("\n【Step 3】获取parent进货数据")

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

    parents_info = {}
    for r in receive_data:
        parent_id = r.get('parent_article_id')
        inbound_qty = float(r.get('inbound_qty', 0))
        inbound_amt = float(r.get('inbound_amount', 0))
        parents_info[parent_id] = {
            'inbound_qty': inbound_qty,
            'inbound_amount': inbound_amt,
            'unit_price': inbound_amt / inbound_qty if inbound_qty > 0 else 0
        }

    print(f"有进货的parent: {len(parents_info)} 个")
    for p_id, info in parents_info.items():
        print(f"  {p_id}: 进货 {info['inbound_qty']:.2f} kg, 单价 {info['unit_price']:.2f} 元/kg")

    # ===== Step 4: 获取所有sub的销售+损耗（拆分需求） =====
    print("\n【Step 4】获取所有sub的拆分需求")

    all_sub_ids = list(sub_parent_count.keys())

    # 销售数据
    all_sales = {}
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
        for r in query_api(sales_sql):
            all_sales[r.get('abi_article_id')] = {
                'sale_qty': float(r.get('sale_qty', 0)),
                'sale_amt': float(r.get('sale_amt', 0))
            }

    # 损耗数据
    all_losses = {}
    for i in range(0, len(all_sub_ids), batch_size):
        batch_subs = all_sub_ids[i:i+batch_size]
        loss_sql = f"""
        SELECT
            article_id,
            know_lost_qty
        FROM strategy_fm_loss_di
        WHERE inc_day = '{TEST_DATE}'
            AND store_id = '{TEST_STORE}'
            AND article_id IN ({','.join([f"'{s}'" for s in batch_subs])})
        """
        for r in query_api(loss_sql):
            all_losses[r.get('article_id')] = float(r.get('know_lost_qty', 0))

    # 计算拆分需求
    sub_demand = {}
    for sub_id in all_sub_ids:
        sale_qty = all_sales.get(sub_id, {}).get('sale_qty', 0)
        loss_qty = all_losses.get(sub_id, 0)
        sub_demand[sub_id] = sale_qty + loss_qty

    print(f"目标商品拆分需求:")
    for t in TARGET_ARTICLES:
        demand = sub_demand.get(t, 0)
        sale = all_sales.get(t, {}).get('sale_qty', 0)
        loss = all_losses.get(t, 0)
        print(f"  {t}: 销售{sale:.3f}kg + 损耗{loss:.3f}kg = 拆分需求{demand:.3f}kg")

    # ===== Step 5: 获取商品名称 =====
    print("\n【Step 5】获取商品名称")

    all_article_ids = list(set(all_sub_ids + list(parents_info.keys())))
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
        for r in query_api(goods_sql):
            article_names[r.get('article_id')] = r.get('article_name', '')

    for t in TARGET_ARTICLES:
        print(f"  {t}: {article_names.get(t, '(未知)')}")

    # ===== Step 6: 计算parent分配 =====
    print("\n" + "=" * 70)
    print("【分配计算】按用户逻辑")
    print("=" * 70)

    # 对每个有进货的parent计算
    parent_allocation = {}  # parent -> {单parent分配, 多parent分配}

    for parent_id, info in parents_info.items():
        parent_name = article_names.get(parent_id, '(未知)')

        # 该parent的单parent sub需求
        single_subs_demand = sum(
            sub_demand.get(sub, 0)
            for sub, p in single_parent_subs.items()
            if p == parent_id
        )

        # 该parent的多parent sub需求（这些sub也依赖其他parent）
        multi_subs = [sub for sub, parents in multi_parent_subs.items() if parent_id in parents]
        multi_subs_demand = sum(sub_demand.get(sub, 0) for sub in multi_subs)

        # 分配逻辑：
        # 1. 先满足单parent sub（必须由该parent承担）
        # 2. 剩余量分配给多parent sub
        single_alloc = single_subs_demand
        remaining = info['inbound_qty'] - single_alloc
        multi_alloc = min(remaining, multi_subs_demand)

        parent_allocation[parent_id] = {
            'inbound_qty': info['inbound_qty'],
            'unit_price': info['unit_price'],
            'single_subs_demand': single_subs_demand,
            'single_alloc': single_alloc,
            'multi_subs': multi_subs,
            'multi_subs_demand': multi_subs_demand,
            'remaining': remaining,
            'multi_alloc': multi_alloc,
        }

        print(f"\nParent {parent_id} ({parent_name}):")
        print(f"  进货量: {info['inbound_qty']:.2f} kg")
        print(f"  单parent sub需求: {single_subs_demand:.3f} kg")
        print(f"  → 分配给单parent sub: {single_alloc:.3f} kg")
        print(f"  剩余量: {remaining:.3f} kg")
        print(f"  多parent sub ({len(multi_subs)}个): 需求 {multi_subs_demand:.3f} kg")
        print(f"  → 可分配给多parent sub: {multi_alloc:.3f} kg")

    # ===== Step 7: 计算多parent sub的最终分配 =====
    print("\n" + "=" * 70)
    print("【多parent sub的分配明细】")
    print("=" * 70)

    # 对每个多parent的sub，按parent优先级分配
    # 用户逻辑：大白猪先满足单parent，剩余给多parent；通排补差

    multi_sub_allocation = {}  # sub -> {parent分配明细}

    for sub_id, sub_parents in multi_parent_subs.items():
        sub_name = article_names.get(sub_id, '(未知)')
        demand = sub_demand.get(sub_id, 0)

        if demand == 0:
            continue

        # 只考虑有进货的parent
        valid_parents = [p for p in sub_parents if p in parents_info]

        if len(valid_parents) == 0:
            continue

        print(f"\nSub {sub_id} ({sub_name}):")
        print(f"  拆分需求: {demand:.3f} kg")
        print(f"  来源parent: {valid_parents}")

        # 按进货量从小到大排序（小的优先，作为"主要来源")
        valid_parents_sorted = sorted(valid_parents, key=lambda p: parents_info[p]['inbound_qty'])

        allocations = []
        remaining_demand = demand

        for i, parent_id in enumerate(valid_parents_sorted):
            parent_info = parent_allocation[parent_id]
            parent_name = article_names.get(parent_id, '(未知)')

            # 该parent能分配给这个sub的最大量 = parent剩余量 / 多parent sub数量（按比例）
            # 或者按用户逻辑：第一个parent尽量多给，最后一个补差

            if i < len(valid_parents_sorted) - 1:
                # 不是最后一个parent，尽量多给（但有上限）
                # 按比例分配：该sub需求占多parent sub总需求的比例
                share_ratio = demand / parent_info['multi_subs_demand'] if parent_info['multi_subs_demand'] > 0 else 0
                alloc_qty = min(remaining_demand, parent_info['multi_alloc'] * share_ratio)
            else:
                # 最后一个parent，必须承担剩余
                alloc_qty = remaining_demand

            alloc_qty = max(0, min(alloc_qty, parent_info['remaining']))

            alloc_amt = alloc_qty * parent_info['unit_price']
            remaining_demand -= alloc_qty

            allocations.append({
                'parent_id': parent_id,
                'parent_name': parent_name,
                'alloc_qty': alloc_qty,
                'alloc_amt': alloc_amt,
                'unit_price': parent_info['unit_price']
            })

            print(f"  → {parent_id} ({parent_name}): 分配 {alloc_qty:.3f} kg, 成本 {alloc_amt:.2f} 元")

        if remaining_demand > 0:
            print(f"  ⚠ 剩余未分配: {remaining_demand:.3f} kg")

        multi_sub_allocation[sub_id] = {
            'demand': demand,
            'allocations': allocations,
            'total_alloc_qty': sum(a['alloc_qty'] for a in allocations),
            'total_alloc_amt': sum(a['alloc_amt'] for a in allocations),
        }

    # ===== Step 8: 汇总目标商品结果 =====
    print("\n" + "=" * 70)
    print("【目标商品最终结果】")
    print("=" * 70)

    for t in TARGET_ARTICLES:
        t_name = article_names.get(t, '(未知)')
        print(f"\n商品 {t} ({t_name}):")

        if t in single_parent_subs:
            # 单parent的sub，直接由唯一parent承担
            parent_id = single_parent_subs[t]
            parent_name = article_names.get(parent_id, '(未知)')
            demand = sub_demand.get(t, 0)
            unit_price = parents_info.get(parent_id, {}).get('unit_price', 0)
            alloc_amt = demand * unit_price

            print(f"  类型: 单parent sub")
            print(f"  来源: {parent_id} ({parent_name})")
            print(f"  拆分需求: {demand:.3f} kg")
            print(f"  成本金额: {alloc_amt:.2f} 元")

        elif t in multi_sub_allocation:
            info = multi_sub_allocation[t]
            sale_amt = all_sales.get(t, {}).get('sale_amt', 0)
            sale_qty = all_sales.get(t, {}).get('sale_qty', 0)

            avg_unit_cost = info['total_alloc_amt'] / info['total_alloc_qty'] if info['total_alloc_qty'] > 0 else 0
            profit = sale_amt - sale_qty * avg_unit_cost

            print(f"  类型: 多parent sub")
            print(f"  拆分需求: {info['demand']:.3f} kg")
            print(f"  实际分配: {info['total_alloc_qty']:.3f} kg")
            print(f"  成本金额: {info['total_alloc_amt']:.2f} 元")
            print(f"  平均单位成本: {avg_unit_cost:.2f} 元/kg")
            print(f"  销售金额: {sale_amt:.2f} 元")
            print(f"  销售数量: {sale_qty:.3f} kg")
            print(f"  毛利额: {profit:.2f} 元")
            print(f"  来源parent明细:")
            for a in info['allocations']:
                print(f"    {a['parent_id']} ({a['parent_name']}): {a['alloc_qty']:.3f} kg, {a['alloc_amt']:.2f} 元")

        else:
            print(f"  无BOM拆分数据（可能是parent商品）")

    print("\n" + "=" * 70)
    print("计算完成")
    print("=" * 70)


if __name__ == '__main__':
    main()