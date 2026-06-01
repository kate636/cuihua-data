"""
临时BOM拆分毛利计算脚本 v4（正确分配逻辑）

计算顺序（用户指定）：
    1. 先处理"既是parent又是sub"的sku
       - 计算自己进货能满足多少需求
       - 剩余需求从别的parent分配
    2. 再处理纯sub（只有一个parent）
       - 消耗parent的进货量
    3. 最后处理多parent的sub
       - 按parent剩余量比例分配

核心逻辑：
    - subsku拆分数量 = 销售数量 + 已知损耗数量
    - subsku拆分成本 = (拆分数量 × 销售原价) / Σ(parent下所有subs的拆分数量 × 销售原价) × parent分配给该sub的进货额
    - 门店毛利 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存 - 期初库存)
"""

import sys
from fm_etl_v3.connectors import ApiConnector
from fm_etl_v3.config import get_settings


def fetch_data(api, business_date: str):
    """获取所有需要的数据"""

    # 1. receive_sale_di - 获取BOM关系和parent进货信息
    print("\n1. 获取 receive_sale_di (BOM关系 + parent进货)")
    sql_rs = f"""
    SELECT
        store_id,
        article_id AS parent_id,
        article_name AS parent_name,
        sale_article_id AS sub_id,
        sale_article_name AS sub_name,
        inbound_amount AS parent_inbound_amt,
        inbound_qty AS parent_inbound_qty,
        category_level1_id
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{business_date}'
      AND category_level1_id = '13'
    """
    rs_rows = api._fetch_all(sql_rs)
    print(f"   共 {len(rs_rows)} 条 parent×sub 关系记录")

    # 获取所有涉及的article_id
    all_parent_ids = set(r.get('parentId', '') for r in rs_rows)
    all_sub_ids = set(r.get('subId', '') for r in rs_rows)
    all_article_ids = all_parent_ids | all_sub_ids
    articles_str = ','.join([f"'{a}'" for a in all_article_ids if a])

    # 2. sales_di - 获取销售数据（全局聚合）
    print("\n2. 获取 sales_di (销售数量 + 原价 + 销售额)")
    sql_sales = f"""
    SELECT
        store_id,
        abi_article_id AS article_id,
        SUM(qty_spec) AS sale_qty,
        SUM(sales_amt) AS sale_amt,
        AVG(list_price) AS list_price
    FROM strategy_fm_sales_di
    WHERE inc_day = '{business_date}'
      AND abi_article_id IN ({articles_str})
    GROUP BY store_id, abi_article_id
    """
    sales_rows = api._fetch_all(sql_sales)
    print(f"   共 {len(sales_rows)} 条销售汇总记录")

    # 3. loss_di - 获取已知损耗
    print("\n3. 获取 loss_di (已知损耗)")
    sql_loss = f"""
    SELECT
        store_id,
        article_id,
        know_lost_qty,
        know_lost_amt
    FROM strategy_fm_loss_di
    WHERE inc_day = '{business_date}'
      AND article_id IN ({articles_str})
    """
    loss_rows = api._fetch_all(sql_loss)
    print(f"   共 {len(loss_rows)} 条损耗记录")

    # 4. compose_di - 获取加工转换
    print("\n4. 获取 compose_di (加工入/出)")
    sql_compose = f"""
    SELECT
        store_id,
        article_id,
        compose_in_qty,
        compose_in_amt,
        compose_out_qty,
        compose_out_amt
    FROM strategy_fm_compose_di
    WHERE inc_day = '{business_date}'
      AND article_id IN ({articles_str})
    """
    compose_rows = api._fetch_all(sql_compose)
    print(f"   共 {len(compose_rows)} 条加工记录")

    # 5. purchase_di - 获取库存
    print("\n5. 获取 purchase_di (期初/期末库存)")
    sql_purchase = f"""
    SELECT
        store_id,
        sale_article_id AS article_id,
        SUM(init_stock_amt) AS init_stock_amt,
        SUM(end_stock_amt) AS end_stock_amt,
        SUM(init_stock_qty) AS init_stock_qty,
        SUM(end_stock_qty) AS end_stock_qty
    FROM strategy_fm_purchase_di
    WHERE inc_day = '{business_date}'
      AND sale_article_id IN ({articles_str})
    GROUP BY store_id, sale_article_id
    """
    purchase_rows = api._fetch_all(sql_purchase)
    print(f"   共 {len(purchase_rows)} 条库存记录")

    return rs_rows, sales_rows, loss_rows, compose_rows, purchase_rows


def build_lookup_tables(sales_rows, loss_rows, compose_rows, purchase_rows):
    """构建查找表"""

    # 销售查找表
    sales_lookup = {}
    for r in sales_rows:
        key = (r.get('storeId', ''), r.get('articleId', ''))
        sales_lookup[key] = {
            'sale_qty': r.get('saleQty', 0) or 0,
            'sale_amt': r.get('saleAmt', 0) or 0,
            'list_price': r.get('listPrice', 0) or 0
        }

    # 损耗查找表
    loss_lookup = {}
    for r in loss_rows:
        key = (r.get('storeId', ''), r.get('articleId', ''))
        loss_lookup[key] = {
            'know_lost_qty': r.get('knowLostQty', 0) or 0,
            'know_lost_amt': r.get('knowLostAmt', 0) or 0
        }

    # 加工查找表
    compose_lookup = {}
    for r in compose_rows:
        key = (r.get('storeId', ''), r.get('articleId', ''))
        compose_lookup[key] = {
            'compose_in_qty': r.get('composeInQty', 0) or 0,
            'compose_in_amt': r.get('composeInAmt', 0) or 0,
            'compose_out_qty': r.get('composeOutQty', 0) or 0,
            'compose_out_amt': r.get('composeOutAmt', 0) or 0
        }

    # 库存查找表
    stock_lookup = {}
    for r in purchase_rows:
        key = (r.get('storeId', ''), r.get('articleId', ''))
        stock_lookup[key] = {
            'init_stock_amt': r.get('initStockAmt', 0) or 0,
            'end_stock_amt': r.get('endStockAmt', 0) or 0,
            'init_stock_qty': r.get('initStockQty', 0) or 0,
            'end_stock_qty': r.get('endStockQty', 0) or 0
        }

    return sales_lookup, loss_lookup, compose_lookup, stock_lookup


def classify_skus(rs_rows):
    """
    分类SKU：
    - Type A: 既是parent又是sub（有自己进货 + 需要从别的parent分配）
    - Type B: 纯sub（只有一个parent，没有自己进货）
    - Type C: 多parent的sub（没有自己进货，需要从多个parent分配）
    """

    # 统计每个sub的parents
    sub_parents = {}
    for r in rs_rows:
        sub_id = r.get('subId', '')
        parent_id = r.get('parentId', '')
        parent_inbound_amt = r.get('parentInboundAmt', 0) or 0
        if sub_id not in sub_parents:
            sub_parents[sub_id] = {'parents': [], 'parent_inbounds': {}}
        sub_parents[sub_id]['parents'].append(parent_id)
        sub_parents[sub_id]['parent_inbounds'][parent_id] = parent_inbound_amt

    # 统计每个parent的subs
    parent_subs = {}
    for r in rs_rows:
        parent_id = r.get('parentId', '')
        sub_id = r.get('subId', '')
        parent_inbound_amt = r.get('parentInboundAmt', 0) or 0
        parent_name = r.get('parentName', '')
        if parent_id not in parent_subs:
            parent_subs[parent_id] = {
                'subs': [],
                'inbound_amt': parent_inbound_amt,
                'inbound_qty': r.get('parentInboundQty', 0) or 0,
                'name': parent_name,
                'remaining_amt': parent_inbound_amt  # 剩余可分配量
            }
        # 避免重复
        if sub_id not in parent_subs[parent_id]['subs']:
            parent_subs[parent_id]['subs'].append(sub_id)

    # 分类
    parent_ids = set(parent_subs.keys())
    sub_ids = set(sub_parents.keys())
    both_role = parent_ids & sub_ids  # 既是parent又是sub

    type_a = {}  # 既是parent又是sub
    type_b = {}  # 纯sub（单parent，不是parent）
    type_c = {}  # 多parent的sub

    for sub_id, info in sub_parents.items():
        parents = info['parents']

        if sub_id in both_role:
            # Type A: 既是parent又是sub
            type_a[sub_id] = {
                'parents': parents,
                'parent_inbounds': info['parent_inbounds'],
                'self_inbound_amt': parent_subs[sub_id]['inbound_amt'],  # 自己作为parent的进货
                'self_name': parent_subs[sub_id]['name']
            }
        elif len(parents) > 1:
            # Type C: 多parent的sub
            type_c[sub_id] = {
                'parents': parents,
                'parent_inbounds': info['parent_inbounds']
            }
        else:
            # Type B: 纯sub（单parent）
            type_b[sub_id] = {
                'parent': parents[0],
                'parent_inbound_amt': info['parent_inbounds'].get(parents[0], 0)
            }

    return type_a, type_b, type_c, parent_subs, sub_parents


def calculate_split_requirement(sub_id, sales_lookup, loss_lookup, store_id):
    """
    计算sub的拆分需求（数量和权重）
    拆分数量 = 销售数量 + 已知损耗数量
    权重 = 拆分数量 × 销售原价
    """
    sale_data = sales_lookup.get((store_id, sub_id), {})
    loss_data = loss_lookup.get((store_id, sub_id), {})

    sale_qty = sale_data.get('sale_qty', 0)
    list_price = sale_data.get('list_price', 0)
    know_lost_qty = loss_data.get('know_lost_qty', 0)

    split_qty = sale_qty + know_lost_qty
    weight = split_qty * list_price

    return {
        'sale_qty': sale_qty,
        'sale_amt': sale_data.get('sale_amt', 0),
        'list_price': list_price,
        'know_lost_qty': know_lost_qty,
        'split_qty': split_qty,
        'weight': weight
    }


def calculate_bom_split_ordered(rs_rows, sales_lookup, loss_lookup, store_id):
    """
    按正确顺序计算BOM拆分：
    1. Type A: 既是parent又是sub
    2. Type B: 纯sub
    3. Type C: 多parent的sub
    """

    # 分类SKU
    type_a, type_b, type_c, parent_subs, sub_parents = classify_skus(rs_rows)

    print(f"\n   SKU分类:")
    print(f"   - Type A (既是parent又是sub): {len(type_a)} 个")
    print(f"   - Type B (纯sub): {len(type_b)} 个")
    print(f"   - Type C (多parent的sub): {len(type_c)} 个")

    # 存储每个sub的拆分入额
    sub_split_in = {}  # {sub_id: {'total': xxx, 'details': [{parent_id, amt}]}}

    # ========== Step 1: 处理Type A（既是parent又是sub）==========
    print("\n   Step 1: 处理Type A（既是parent又是sub）")
    for sub_id, info in type_a.items():
        req = calculate_split_requirement(sub_id, sales_lookup, loss_lookup, store_id)

        self_inbound_amt = info['self_inbound_amt']
        parents = info['parents']

        # 自己的进货能满足多少权重
        self_weight = req['weight']
        total_demand_weight = req['weight']

        # 自己进货额先分配给自己（满足自己的需求）
        if self_inbound_amt > 0 and total_demand_weight > 0:
            # 自己进货能满足的比例
            self_ratio = min(self_inbound_amt / (total_demand_weight * (req['list_price'] / req['list_price'])), 1.0) if total_demand_weight > 0 else 0
            # 简化：自己进货先满足自己，剩余需求从别的parent分配
            self_amt_used = min(self_inbound_amt, total_demand_weight * (req['split_qty'] / req['split_qty'] if req['split_qty'] > 0 else 1))
            # 更精确：自己进货直接作为拆分入额的一部分
            self_amt_used = min(self_inbound_amt, self_weight)  # 自己进货最多满足自己的权重

        # 实际逻辑：
        # 1. sub_id作为parent，有自己的进货 self_inbound_amt
        # 2. sub_id作为sub，需要从其他parent（如大白猪20500351）分配
        # 3. 需求权重 = weight，自己进货满足一部分，剩余从别的parent分配

        # 自己进货分配给自己的金额（基于权重）
        # 首先计算该sub作为parent时，它下面有哪些subs（包括自己）
        subs_under_self = parent_subs.get(sub_id, {}).get('subs', [])
        if sub_id in subs_under_self:
            # 自己是自己的sub（自循环）
            # 计算自己parent下的总权重
            total_weight_under_self = 0
            for s in subs_under_self:
                s_req = calculate_split_requirement(s, sales_lookup, loss_lookup, store_id)
                total_weight_under_self += s_req['weight']

            # 自己占的比例
            if total_weight_under_self > 0:
                self_ratio = self_weight / total_weight_under_self
                amt_from_self = self_ratio * self_inbound_amt
            else:
                amt_from_self = self_inbound_amt
        else:
            amt_from_self = 0

        # 剩余需求从别的parent分配
        remaining_demand = self_weight - amt_from_self if self_weight > amt_from_self else 0

        # 从其他parent分配
        amt_from_other_parents = {}
        other_parents = [p for p in parents if p != sub_id]

        for other_parent in other_parents:
            if remaining_demand <= 0:
                break

            # 该parent的剩余可分配量
            parent_remaining = parent_subs[other_parent]['remaining_amt']
            if parent_remaining > 0:
                # 从该parent分配多少（简单按需求比例）
                amt_from_this_parent = min(parent_remaining, remaining_demand)
                amt_from_other_parents[other_parent] = amt_from_this_parent
                parent_subs[other_parent]['remaining_amt'] -= amt_from_this_parent
                remaining_demand -= amt_from_this_parent

        # 记录结果
        sub_split_in[sub_id] = {
            'total': amt_from_self + sum(amt_from_other_parents.values()),
            'details': [{'parent_id': sub_id, 'amt': amt_from_self}] +
                       [{'parent_id': p, 'amt': amt} for p, amt in amt_from_other_parents.items()],
            'req': req
        }

    # ========== Step 2: 处理Type B（纯sub）==========
    print("   Step 2: 处理Type B（纯sub）")
    for sub_id, info in type_b.items():
        if sub_id in sub_split_in:
            continue  # 已处理过

        req = calculate_split_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        parent_id = info['parent']

        # 该parent下的总权重（排除已处理的Type A）
        subs_under_parent = parent_subs[parent_id]['subs']
        total_weight = 0
        unprocessed_subs = []
        for s in subs_under_parent:
            if s not in sub_split_in:  # 未处理的sub
                s_req = calculate_split_requirement(s, sales_lookup, loss_lookup, store_id)
                total_weight += s_req['weight']
                unprocessed_subs.append((s, s_req))

        # 按权重分配parent剩余进货额
        parent_remaining = parent_subs[parent_id]['remaining_amt']

        if total_weight > 0:
            sub_weight = req['weight']
            ratio = sub_weight / total_weight
            amt_from_parent = ratio * parent_remaining
        else:
            amt_from_parent = 0

        # 记录结果
        sub_split_in[sub_id] = {
            'total': amt_from_parent,
            'details': [{'parent_id': parent_id, 'amt': amt_from_parent}],
            'req': req
        }

    # ========== Step 3: 处理Type C（多parent的sub）==========
    print("   Step 3: 处理Type C（多parent的sub）")
    for sub_id, info in type_c.items():
        if sub_id in sub_split_in:
            continue

        req = calculate_split_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        parents = info['parents']

        # 计算每个parent的剩余可分配量
        parent_remaining_amts = {}
        for p in parents:
            parent_remaining_amts[p] = parent_subs[p]['remaining_amt']

        total_parent_remaining = sum(parent_remaining_amts.values())

        # 按parent剩余量比例分配
        amt_from_parents = {}
        if total_parent_remaining > 0:
            for p in parents:
                ratio = parent_remaining_amts[p] / total_parent_remaining
                amt_from_parents[p] = ratio * req['weight']  # 按权重分配
                parent_subs[p]['remaining_amt'] -= amt_from_parents[p]

        # 记录结果
        sub_split_in[sub_id] = {
            'total': sum(amt_from_parents.values()),
            'details': [{'parent_id': p, 'amt': amt} for p, amt in amt_from_parents.items()],
            'req': req
        }

    return sub_split_in, type_a, type_b, type_c


def main():
    business_date = sys.argv[1] if len(sys.argv) > 1 else '2026-04-23'

    print(f"\n{'='*100}")
    print(f"BOM拆分毛利计算（正确分配逻辑 v4） - {business_date}")
    print(f"{'='*100}")

    cfg = get_settings()
    api = ApiConnector(cfg)

    # 获取数据
    rs_rows, sales_rows, loss_rows, compose_rows, purchase_rows = fetch_data(api, business_date)

    # 构建查找表
    sales_lookup, loss_lookup, compose_lookup, stock_lookup = build_lookup_tables(
        sales_rows, loss_rows, compose_rows, purchase_rows
    )

    # 按正确顺序计算BOM拆分（假设单门店）
    store_id = rs_rows[0].get('storeId', '') if rs_rows else ''

    print("\n6. 按正确顺序计算BOM拆分")
    sub_split_in, type_a, type_b, type_c = calculate_bom_split_ordered(
        rs_rows, sales_lookup, loss_lookup, store_id
    )

    # 打印详细计算步骤（排骨举例）
    print("\n" + "="*100)
    print("详细计算步骤 - 排骨 (20003470)")
    print("="*100)

    rib_id = '20003470'
    if rib_id in sub_split_in:
        rib_info = sub_split_in[rib_id]
        req = rib_info['req']
        print(f"\n  商品: {rib_id}")
        print(f"  类型: Type C（多parent的sub）")
        print(f"  Parents: {rib_info['details']}")
        print(f"  拆分数量: {req['split_qty']:.2f} kg")
        print(f"  权重: {req['weight']:.2f}")
        print(f"  拆分入额合计: {rib_info['total']:.2f} 元")

    # 打印Type A详情
    print("\n" + "="*100)
    print("Type A详情（既是parent又是sub）")
    print("="*100)
    for sub_id, info in type_a.items():
        if sub_id in sub_split_in:
            s_info = sub_split_in[sub_id]
            req = s_info['req']
            print(f"\n{sub_id} ({info.get('self_name', '')}):")
            print(f"  自己进货: {info['self_inbound_amt']:.2f} 元")
            print(f"  拆分需求权重: {req['weight']:.2f}")
            print(f"  拆分入额: {s_info['total']:.2f} 元")
            print(f"  来源明细:")
            for d in s_info['details']:
                print(f"    - {d['parent_id']}: {d['amt']:.2f} 元")

    print(f"\n{'='*100}")
    print("计算完成")
    print(f"{'='*100}")


if __name__ == "__main__":
    main()