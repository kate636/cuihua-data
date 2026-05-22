"""
BOM拆分逻辑 - 正确理解用户意图

用户逻辑：
1. subsku当日拆分数量 = 销售数量 + 已知损耗数量（固定值）
2. 一个sub有多个parent时，分配方式：
   - parent分配给该sub的重量 = parent进货 - parent分配给其他sub的重量
   - 如果有多个parent，用补差方式确定各自分配量

例：排骨总需求9.51kg，来自2个parent
   - 通排分配给排骨 = 通排进货 - 通排其他sub需求
   - 大白猪分配给排骨 = 排骨总需求 - 通排分配给排骨

3. 成本金额 = 分配重量 × parent进货单价
4. 如果有多个parent，成本相加，算平均成本
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
    print("BOM拆分计算 - 正确理解用户逻辑")
    print(f"日期: {TEST_DATE}  门店: {TEST_STORE}")
    print("=" * 70)

    # ===== Step 1: 获取目标商品的BOM关系 =====
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
    target_parents = list(set(r.get('parent_article_id') for r in target_bom))
    print(f"目标商品的BOM关系: {len(target_bom)} 条")
    print(f"涉及的parent: {target_parents[:5]}... (共{len(target_parents)}个)")

    # ===== Step 2: 获取这些parent的所有sub =====
    print("\n【Step 2】获取parent的所有sub商品")

    batch_size = 50
    all_parent_subs = []
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
        all_parent_subs.extend(query_api(all_subs_sql))

    all_sub_ids = list(set(r.get('sub_article_id') for r in all_parent_subs))
    print(f"所有sub商品: {len(all_sub_ids)} 个")

    # ===== Step 3: 获取所有sub的销售数据 =====
    print("\n【Step 3】获取所有sub的销售数据")

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

    print(f"销售数据: {len(all_sales)} 条")
    for t in TARGET_ARTICLES:
        if t in all_sales:
            print(f"  {t}: 销售 {all_sales[t]['sale_qty']} kg")

    # ===== Step 4: 获取所有sub的损耗数据 =====
    print("\n【Step 4】获取所有sub的损耗数据")

    all_losses = {}
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
        for r in query_api(loss_sql):
            all_losses[r.get('article_id')] = float(r.get('know_lost_qty', 0))

    print(f"损耗数据: {len(all_losses)} 条")

    # ===== Step 5: 计算每个sub的拆分需求 =====
    print("\n【Step 5】计算每个sub的拆分需求（销售+损耗）")

    sub_demands = {}
    for sub_id in all_sub_ids:
        sale_qty = all_sales.get(sub_id, {}).get('sale_qty', 0)
        loss_qty = all_losses.get(sub_id, 0)
        sub_demands[sub_id] = sale_qty + loss_qty

    print(f"目标商品拆分需求:")
    for t in TARGET_ARTICLES:
        demand = sub_demands.get(t, 0)
        print(f"  {t}: 拆分需求 {demand:.3f} kg")

    # ===== Step 6: 获取parent进货数据 =====
    print("\n【Step 6】获取parent进货数据")

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

    parents_with_receive = {}
    for r in receive_data:
        parent_id = r.get('parent_article_id')
        parents_with_receive[parent_id] = {
            'inbound_qty': float(r.get('inbound_qty', 0)),
            'inbound_amount': float(r.get('inbound_amount', 0)),
            'unit_price': float(r.get('inbound_amount', 0)) / float(r.get('inbound_qty', 0)) if float(r.get('inbound_qty', 0)) > 0 else 0
        }

    print(f"有进货的parent: {len(parents_with_receive)} 个")
    for p_id, info in parents_with_receive.items():
        print(f"  {p_id}: 进货 {info['inbound_qty']:.2f} kg, 金额 {info['inbound_amount']:.2f} 元, 单价 {info['unit_price']:.2f} 元/kg")

    # ===== Step 7: 获取商品名称 =====
    print("\n【Step 7】获取商品名称")

    all_article_ids = list(set(all_sub_ids + list(parents_with_receive.keys())))
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

    # ===== Step 8: 按用户逻辑计算分配 =====
    print("\n" + "=" * 70)
    print("【BOM拆分计算】按用户补差逻辑")
    print("=" * 70)

    # 对每个目标商品计算
    for target_sub in TARGET_ARTICLES:
        target_demand = sub_demands.get(target_sub, 0)
        if target_demand == 0:
            print(f"\n{target_sub}: 无拆分需求")
            continue

        target_name = article_names.get(target_sub, '(未知)')
        print(f"\n{'='*60}")
        print(f"目标商品: {target_sub} ({target_name})")
        print(f"总拆分需求: {target_demand:.3f} kg")
        print("=" * 60)

        # 找该sub的所有parent（有进货的）
        sub_parents = [p for p in target_parents if p in parents_with_receive]
        print(f"涉及的parent（有进货）: {len(sub_parents)} 个")

        if len(sub_parents) == 0:
            print("  无parent进货数据")
            continue

        # 按parent进货量排序（小的先处理，作为"优先分配源"）
        sub_parents_sorted = sorted(sub_parents, key=lambda p: parents_with_receive[p]['inbound_qty'])
        print(f"  Parent排序（按进货量从小到大）:")
        for p in sub_parents_sorted:
            p_name = article_names.get(p, '(未知)')
            print(f"    {p} ({p_name}): 进货 {parents_with_receive[p]['inbound_qty']:.2f} kg")

        # 计算分配
        allocations = []
        remaining_demand = target_demand

        for i, parent_id in enumerate(sub_parents_sorted):
            parent_info = parents_with_receive[parent_id]
            parent_name = article_names.get(parent_id, '(未知)')

            # 该parent的其他sub需求（不含目标sub）
            other_subs = [r for r in all_parent_subs if r.get('parent_article_id') == parent_id and r.get('sub_article_id') != target_sub]
            other_demand = sum(sub_demands.get(s.get('sub_article_id'), 0) for s in other_subs)

            print(f"\n  Parent {parent_id} ({parent_name}):")
            print(f"    进货量: {parent_info['inbound_qty']:.2f} kg")
            print(f"    其他sub需求: {other_demand:.3f} kg")

            # parent能给目标sub的最大量
            max_alloc = max(0, parent_info['inbound_qty'] - other_demand)
            print(f"    最大可分配: {max_alloc:.3f} kg")

            # 实际分配量
            if i < len(sub_parents_sorted) - 1:
                # 不是最后一个parent，尽量多分配
                alloc_qty = min(remaining_demand, max_alloc)
            else:
                # 最后一个parent，必须承担剩余需求
                alloc_qty = remaining_demand

            alloc_qty = max(0, alloc_qty)
            remaining_demand -= alloc_qty

            # 成本金额
            alloc_amt = alloc_qty * parent_info['unit_price']

            print(f"    实际分配: {alloc_qty:.3f} kg")
            print(f"    分配成本: {alloc_amt:.2f} 元")

            allocations.append({
                'parent_id': parent_id,
                'parent_name': parent_name,
                'alloc_qty': alloc_qty,
                'alloc_amt': alloc_amt,
                'unit_price': parent_info['unit_price']
            })

        # 汇总
        total_alloc_qty = sum(a['alloc_qty'] for a in allocations)
        total_alloc_amt = sum(a['alloc_amt'] for a in allocations)

        print(f"\n  汇总:")
        print(f"    总分配重量: {total_alloc_qty:.3f} kg")
        print(f"    总分配成本: {total_alloc_amt:.2f} 元")

        if total_alloc_qty > 0:
            avg_unit_cost = total_alloc_amt / total_alloc_qty
            print(f"    平均单位成本: {avg_unit_cost:.2f} 元/kg")
        else:
            avg_unit_cost = 0

        # 计算毛利
        sale_amt = all_sales.get(target_sub, {}).get('sale_amt', 0)
        sale_qty = all_sales.get(target_sub, {}).get('sale_qty', 0)
        profit = sale_amt - sale_qty * avg_unit_cost

        print(f"\n  毛利计算:")
        print(f"    销售金额: {sale_amt:.2f} 元")
        print(f"    销售数量: {sale_qty:.3f} kg")
        print(f"    销售成本: {sale_qty * avg_unit_cost:.2f} 元")
        print(f"    毛利额: {profit:.2f} 元")

    print("\n" + "=" * 70)
    print("计算完成")
    print("=" * 70)


if __name__ == '__main__':
    main()