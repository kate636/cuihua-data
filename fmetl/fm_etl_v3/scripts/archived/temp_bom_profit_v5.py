"""
临时BOM拆分毛利计算脚本 v5（清晰分配逻辑）

计算顺序（用户指定）：
    1. Type A（既是parent又是sub）：如扇骨
       - 计算自己进货能满足多少需求
       - 剩余需求从别的parent分配 → 更新parent剩余量

    2. Type B（纯sub）：只有1个parent
       - 按权重比例消耗parent剩余量 → 更新parent剩余量

    3. Type C（多parent的sub）：如排骨
       - 计算总需求权重
       - 按各parent剩余量比例分配

核心公式：
    拆分数量 = 销售数量 + 已知损耗数量
    权重 = 拆分数量 × 销售原价
    拆分金额 = 权重占比 × parent分配给该sub的进货额
"""

import sys
from fm_etl_v3.connectors import ApiConnector
from fm_etl_v3.config import get_settings


def fetch_data(api, business_date: str):
    """获取所有需要的数据"""

    print("\n1. 获取 receive_sale_di (BOM关系 + parent进货)")
    sql_rs = f"""
    SELECT
        store_id,
        article_id AS parent_id,
        article_name AS parent_name,
        sale_article_id AS sub_id,
        sale_article_name AS sub_name,
        inbound_amount AS parent_inbound_amt,
        inbound_qty AS parent_inbound_qty
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{business_date}'
      AND category_level1_id = '13'
    """
    rs_rows = api._fetch_all(sql_rs)
    print(f"   共 {len(rs_rows)} 条 parent×sub 关系记录")

    all_ids = set()
    for r in rs_rows:
        all_ids.add(r.get('parentId', '') or '')
        all_ids.add(r.get('subId', '') or '')
    articles_str = ','.join([f"'{a}'" for a in all_ids if a])

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

    print("\n3. 获取 loss_di (已知损耗)")
    sql_loss = f"""
    SELECT
        store_id,
        article_id,
        know_lost_qty
    FROM strategy_fm_loss_di
    WHERE inc_day = '{business_date}'
      AND article_id IN ({articles_str})
    """
    loss_rows = api._fetch_all(sql_loss)
    print(f"   共 {len(loss_rows)} 条损耗记录")

    return rs_rows, sales_rows, loss_rows


def build_lookups(sales_rows, loss_rows):
    """构建查找表"""
    sales_lookup = {}
    for r in sales_rows:
        key = (r.get('storeId', ''), r.get('articleId', ''))
        sales_lookup[key] = {
            'sale_qty': r.get('saleQty', 0) or 0,
            'sale_amt': r.get('saleAmt', 0) or 0,
            'list_price': r.get('listPrice', 0) or 0
        }

    loss_lookup = {}
    for r in loss_rows:
        key = (r.get('storeId', ''), r.get('articleId', ''))
        loss_lookup[key] = {
            'know_lost_qty': r.get('knowLostQty', 0) or 0
        }

    return sales_lookup, loss_lookup


def get_sub_requirement(sub_id, sales_lookup, loss_lookup, store_id):
    """计算sub的拆分需求"""
    sale_data = sales_lookup.get((store_id, sub_id), {})
    loss_data = loss_lookup.get((store_id, sub_id), {})

    sale_qty = sale_data.get('sale_qty', 0)
    list_price = sale_data.get('list_price', 0)
    know_lost_qty = loss_data.get('know_lost_qty', 0)
    sale_amt = sale_data.get('sale_amt', 0)

    split_qty = sale_qty + know_lost_qty
    weight = split_qty * list_price

    return {
        'sale_qty': sale_qty,
        'sale_amt': sale_amt,
        'list_price': list_price,
        'know_lost_qty': know_lost_qty,
        'split_qty': split_qty,
        'weight': weight
    }


def classify_and_calculate(rs_rows, sales_lookup, loss_lookup, store_id):
    """
    分类并计算，返回：
    - sub_split_result: {sub_id: {total_amt, details: [{parent, amt}], requirement}}
    - parent_state: {parent_id: {original_amt, remaining_amt, allocated_to}}
    """

    # ========== Step 0: 构建基础结构 ==========

    # 统计每个sub的parents
    sub_parents = {}
    for r in rs_rows:
        sub_id = r.get('subId', '')
        parent_id = r.get('parentId', '')
        if sub_id not in sub_parents:
            sub_parents[sub_id] = []
        if parent_id not in sub_parents[sub_id]:
            sub_parents[sub_id].append(parent_id)

    # 统计每个parent的信息
    parent_state = {}
    for r in rs_rows:
        parent_id = r.get('parentId', '')
        parent_name = r.get('parentName', '')
        parent_inbound_amt = r.get('parentInboundAmt', 0) or 0
        if parent_id not in parent_state:
            parent_state[parent_id] = {
                'name': parent_name,
                'original_amt': parent_inbound_amt,
                'remaining_amt': parent_inbound_amt,
                'subs': [],
                'allocated_to': {}  # {sub_id: amt}
            }
        sub_id = r.get('subId', '')
        if sub_id not in parent_state[parent_id]['subs']:
            parent_state[parent_id]['subs'].append(sub_id)

    # 分类
    parent_ids = set(parent_state.keys())
    sub_ids = set(sub_parents.keys())
    both_role = parent_ids & sub_ids

    type_a = []  # 既是parent又是sub
    type_b = []  # 纯sub
    type_c = []  # 多parent的sub

    for sub_id in sub_ids:
        parents = sub_parents[sub_id]
        if sub_id in both_role:
            type_a.append(sub_id)
        elif len(parents) > 1:
            type_c.append(sub_id)
        else:
            type_b.append(sub_id)

    print(f"\n   SKU分类:")
    print(f"   - Type A (既是parent又是sub): {len(type_a)} 个 - {type_a}")
    print(f"   - Type B (纯sub): {len(type_b)} 个")
    print(f"   - Type C (多parent的sub): {len(type_c)} 个 - {type_c}")

    # 存储结果
    sub_split_result = {}

    # ========== Step 1: 处理Type A ==========

    print("\n   Step 1: 处理Type A（既是parent又是sub）")
    for sub_id in type_a:
        req = get_sub_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        weight = req['weight']

        # 自己作为parent的进货额
        self_inbound = parent_state[sub_id]['original_amt']
        self_name = parent_state[sub_id]['name']

        # 自己进货能满足多少需求？
        # 简化逻辑：自己进货全部用于满足自己的需求（如果权重>0）
        # 如果自己进货 > 权重（按原价计算的成本），则自己进货足够
        # 如果自己进货 < 权重，则剩余需求从别的parent分配

        # 自己进货分配给自己
        amt_from_self = min(self_inbound, weight)  # 自己进货最多等于权重

        # 更新自己的parent状态（自己作为parent，分配给自己）
        parent_state[sub_id]['remaining_amt'] -= amt_from_self
        parent_state[sub_id]['allocated_to'][sub_id] = amt_from_self

        # 剩余需求从别的parent分配
        remaining_weight = weight - amt_from_self
        other_parents = [p for p in sub_parents[sub_id] if p != sub_id]

        amt_from_others = {}
        for other_p in other_parents:
            if remaining_weight <= 0:
                break

            # 从该parent分配（按parent剩余量）
            parent_remaining = parent_state[other_p]['remaining_amt']
            if parent_remaining > 0:
                # 分配多少？按剩余需求的权重比例
                # 但这里需要知道该parent下所有未处理subs的总权重

                # 简化：直接从parent剩余量中分配
                amt = min(parent_remaining, remaining_weight)
                amt_from_others[other_p] = amt
                parent_state[other_p]['remaining_amt'] -= amt
                parent_state[other_p]['allocated_to'][sub_id] = amt
                remaining_weight -= amt

        total_amt = amt_from_self + sum(amt_from_others.values())
        sub_split_result[sub_id] = {
            'total_amt': total_amt,
            'details': [{'parent': sub_id, 'amt': amt_from_self, 'name': self_name}] +
                       [{'parent': p, 'amt': amt, 'name': parent_state[p]['name']} for p, amt in amt_from_others.items()],
            'requirement': req
        }

    # ========== Step 2: 处理Type B ==========

    print("   Step 2: 处理Type B（纯sub）")
    # 按parent分组处理
    for parent_id, p_info in parent_state.items():
        # 找出该parent下的Type B subs
        type_b_subs = [s for s in p_info['subs'] if s in type_b and s not in sub_split_result]

        if not type_b_subs:
            continue

        # 计算这些subs的总权重
        total_weight = 0
        sub_weights = {}
        for s in type_b_subs:
            req = get_sub_requirement(s, sales_lookup, loss_lookup, store_id)
            sub_weights[s] = req['weight']
            total_weight += req['weight']

        # 按权重分配parent剩余量
        parent_remaining = p_info['remaining_amt']

        for s in type_b_subs:
            if total_weight > 0:
                ratio = sub_weights[s] / total_weight
                amt = ratio * parent_remaining
            else:
                amt = 0

            parent_state[parent_id]['remaining_amt'] -= amt
            parent_state[parent_id]['allocated_to'][s] = amt

            req = get_sub_requirement(s, sales_lookup, loss_lookup, store_id)
            sub_split_result[s] = {
                'total_amt': amt,
                'details': [{'parent': parent_id, 'amt': amt, 'name': p_info['name']}],
                'requirement': req
            }

    # ========== Step 3: 处理Type C ==========

    print("   Step 3: 处理Type C（多parent的sub）")

    # 先计算Type C的总需求权重
    type_c_total_weight = 0
    type_c_requirements = {}
    for sub_id in type_c:
        if sub_id in sub_split_result:
            continue
        req = get_sub_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        type_c_requirements[sub_id] = req
        type_c_total_weight += req['weight']

    # 计算各parent的剩余量（只看Type C涉及的parent）
    type_c_parents = set()
    for sub_id in type_c:
        type_c_parents.update(sub_parents[sub_id])

    parent_remaining_amts = {}
    total_parent_remaining = 0
    for p in type_c_parents:
        rem = parent_state[p]['remaining_amt']
        if rem > 0:  # 只考虑有剩余的parent
            parent_remaining_amts[p] = rem
            total_parent_remaining += rem

    print(f"   Type C总需求权重: {type_c_total_weight:.2f}")
    print(f"   Parent剩余进货额合计: {total_parent_remaining:.2f}")
    print(f"   Parent剩余明细: {parent_remaining_amts}")

    # 按权重分配parent剩余进货额
    # 如果总需求权重 > 总剩余进货额，按权重比例缩减
    # 如果总需求权重 <= 总剩余进货额，按parent剩余比例分配

    for sub_id in type_c:
        if sub_id in sub_split_result:
            continue

        req = type_c_requirements[sub_id]
        weight = req['weight']
        parents = sub_parents[sub_id]

        # 该sub的需求权重占比
        weight_ratio = weight / type_c_total_weight if type_c_total_weight > 0 else 0

        # 该sub应分配的总金额 = 需求权重占比 × parent剩余合计
        sub_total_amt = weight_ratio * total_parent_remaining if total_parent_remaining > 0 else 0

        # 按parent剩余量比例分配到各parent
        amt_from_parents = {}
        for p in parents:
            if p in parent_remaining_amts and parent_remaining_amts[p] > 0:
                parent_ratio = parent_remaining_amts[p] / total_parent_remaining if total_parent_remaining > 0 else 0
                amt = parent_ratio * sub_total_amt
                amt_from_parents[p] = amt
                parent_state[p]['remaining_amt'] -= amt
                parent_state[p]['allocated_to'][sub_id] = amt

        sub_split_result[sub_id] = {
            'total_amt': sum(amt_from_parents.values()),
            'details': [{'parent': p, 'amt': amt, 'name': parent_state[p]['name']}
                        for p, amt in amt_from_parents.items()],
            'requirement': req
        }

    return sub_split_result, parent_state, type_a, type_c


def main():
    business_date = sys.argv[1] if len(sys.argv) > 1 else '2026-04-23'

    print(f"\n{'='*100}")
    print(f"BOM拆分毛利计算（清晰分配逻辑 v5） - {business_date}")
    print(f"{'='*100}")

    cfg = get_settings()
    api = ApiConnector(cfg)

    rs_rows, sales_rows, loss_rows = fetch_data(api, business_date)
    sales_lookup, loss_lookup = build_lookups(sales_rows, loss_rows)

    store_id = rs_rows[0].get('storeId', '') if rs_rows else ''

    print("\n6. 按正确顺序计算BOM拆分")
    sub_split_result, parent_state, type_a, type_c = classify_and_calculate(
        rs_rows, sales_lookup, loss_lookup, store_id
    )

    # 打印parent状态
    print("\n" + "="*100)
    print("Parent进货额分配状态")
    print("="*100)
    print(f"{'parent_id':<12} {'name':<20} {'original':>12} {'allocated':>12} {'remaining':>12}")
    print("-"*60)
    for p_id, p_info in parent_state.items():
        allocated_sum = sum(p_info['allocated_to'].values())
        print(f"{p_id:<12} {p_info['name'][:18]:<20} {p_info['original_amt']:>12.2f} {allocated_sum:>12.2f} {p_info['remaining_amt']:>12.2f}")

    # 打印Type A详情
    print("\n" + "="*100)
    print("Type A详情（既是parent又是sub）")
    print("="*100)
    for sub_id in type_a:
        if sub_id in sub_split_result:
            info = sub_split_result[sub_id]
            req = info['requirement']
            print(f"\n{sub_id} ({info['details'][0]['name']}):")
            print(f"  销售数量: {req['sale_qty']:.2f} kg, 销售额: {req['sale_amt']:.2f} 元")
            print(f"  销售原价: {req['list_price']:.2f} 元/kg")
            print(f"  拆分数量: {req['split_qty']:.2f} kg")
            print(f"  需求权重: {req['weight']:.2f}")
            print(f"  拆分入额: {info['total_amt']:.2f} 元")
            print(f"  来源明细:")
            for d in info['details']:
                print(f"    - {d['parent']} ({d['name']}): {d['amt']:.2f} 元")

    # 打印排骨详情
    print("\n" + "="*100)
    print("排骨详情（Type C）")
    print("="*100)
    rib_id = '20003470'
    if rib_id in sub_split_result:
        info = sub_split_result[rib_id]
        req = info['requirement']
        print(f"\n{rib_id} (排骨):")
        print(f"  Parents: {sub_parents if 'sub_parents' in dir() else 'N/A'}")
        print(f"  销售数量: {req['sale_qty']:.2f} kg, 销售额: {req['sale_amt']:.2f} 元")
        print(f"  销售原价: {req['list_price']:.2f} 元/kg")
        print(f"  拆分数量: {req['split_qty']:.2f} kg")
        print(f"  需求权重: {req['weight']:.2f}")
        print(f"  拆分入额: {info['total_amt']:.2f} 元")
        print(f"  来源明细:")
        for d in info['details']:
            print(f"    - {d['parent']} ({d['name']}): {d['amt']:.2f} 元")

    # ========== 输出完整计算结果 ==========

    print("\n" + "="*100)
    print("所有SKU计算结果（完整指标）")
    print("="*100)

    # 输出字段说明
    print("""
字段说明：
  sub_id            商品编码
  sub_name          商品名称
  sale_qty          销售数量(kg)
  sale_amt          销售额(元)
  list_price        销售原价(元/kg)
  know_lost_qty     已知损耗数量(kg)
  split_qty_in      拆分入数量(kg) = 销售数量 + 已知损耗数量
  weight_in         拆分入权重(元) = 拆分入数量 × 销售原价
  split_amt_in      拆分入金额(元) = 自己进货 + 从其他parent分配
  self_inbound      自己进货额(元) - 仅Type A有
  from_other_parent 从其他parent分配金额(元)
  parents           parent来源列表
  avg_cost          平均成本(元/kg) = 拆分入金额 / 拆分入数量
  type              SKU类型(A=既是parent又是sub, B=纯sub, C=多parent的sub)
    """)

    # 表头
    headers = ['sub_id', 'sub_name', 'sale_qty', 'sale_amt', 'list_price', 'know_lost_qty',
               'split_qty_in', 'weight_in', 'split_amt_in', 'self_inbound', 'from_other',
               'avg_cost', 'parents', 'type']
    print(' | '.join(headers))
    print('-'*120)

    # 按销售额排序输出
    sorted_subs = sorted(sub_split_result.items(), key=lambda x: x[1]['requirement']['sale_amt'], reverse=True)

    for sub_id, info in sorted_subs:
        req = info['requirement']
        details = info['details']

        # 自己进货额
        self_inbound = sum(d['amt'] for d in details if d['parent'] == sub_id)

        # 从其他parent分配
        from_other = sum(d['amt'] for d in details if d['parent'] != sub_id)

        # parent列表
        parents_str = ','.join([d['parent'] for d in details])

        # 类型
        if sub_id in type_a:
            sku_type = 'A'
        elif sub_id in type_c:
            sku_type = 'C'
        else:
            sku_type = 'B'

        # 平均成本
        avg_cost = info['total_amt'] / req['split_qty'] if req['split_qty'] > 0 else 0

        # 商品名称（从第一条detail取）
        sub_name = details[0]['name'] if details else ''

        row = [
            sub_id,
            sub_name[:12],
            f"{req['sale_qty']:.2f}",
            f"{req['sale_amt']:.2f}",
            f"{req['list_price']:.2f}",
            f"{req['know_lost_qty']:.2f}",
            f"{req['split_qty']:.2f}",
            f"{req['weight']:.2f}",
            f"{info['total_amt']:.2f}",
            f"{self_inbound:.2f}",
            f"{from_other:.2f}",
            f"{avg_cost:.2f}",
            parents_str[:20],
            sku_type
        ]
        print(' | '.join(row))

    print('-'*120)

    # 汇总
    total_sale_amt = sum(info['requirement']['sale_amt'] for info in sub_split_result.values())
    total_split_qty = sum(info['requirement']['split_qty'] for info in sub_split_result.values())
    total_split_amt = sum(info['total_amt'] for info in sub_split_result.values())
    total_self_inbound = sum(sum(d['amt'] for d in info['details'] if d['parent'] == sub_id)
                             for sub_id, info in sub_split_result.items())
    total_from_other = sum(sum(d['amt'] for d in info['details'] if d['parent'] != sub_id)
                           for sub_id, info in sub_split_result.items())

    print(f"合计: {len(sub_split_result)}个SKU")
    print(f"  销售额合计: {total_sale_amt:.2f} 元")
    print(f"  拆分入数量合计: {total_split_qty:.2f} kg")
    print(f"  拆分入金额合计: {total_split_amt:.2f} 元")
    print(f"  自己进货合计: {total_self_inbound:.2f} 元")
    print(f"  从其他parent分配合计: {total_from_other:.2f} 元")

    # ========== 输出拆分出（Parent视角）==========
    print("\n" + "="*100)
    print("拆分出（Parent视角）")
    print("="*100)

    print("""
字段说明：
  parent_id         Parent编码
  parent_name       Parent名称
  original_amt      原进货额(元)
  allocated_amt     已分配出去的金额(元) = 拆分出金额
  remaining_amt     剩余未分配金额(元)
  allocated_to      分配给了哪些sub
    """)

    headers = ['parent_id', 'parent_name', 'original_amt', 'allocated_amt', 'remaining_amt', 'allocated_to']
    print(' | '.join(headers))
    print('-'*80)

    for p_id, p_info in parent_state.items():
        allocated_sum = sum(p_info['allocated_to'].values())
        allocated_to_str = ','.join([f"{s}:{amt:.1f}" for s, amt in p_info['allocated_to'].items()])
        if len(allocated_to_str) > 40:
            allocated_to_str = allocated_to_str[:40] + '...'

        row = [
            p_id,
            p_info['name'][:12],
            f"{p_info['original_amt']:.2f}",
            f"{allocated_sum:.2f}",
            f"{p_info['remaining_amt']:.2f}",
            allocated_to_str
        ]
        print(' | '.join(row))

    print('-'*80)

    total_original = sum(p['original_amt'] for p in parent_state.values())
    total_allocated = sum(sum(p['allocated_to'].values()) for p in parent_state.values())
    total_remaining = sum(p['remaining_amt'] for p in parent_state.values())

    print(f"合计: {len(parent_state)}个Parent")
    print(f"  原进货额合计: {total_original:.2f} 元")
    print(f"  拆分出金额合计: {total_allocated:.2f} 元")
    print(f"  剩余未分配合计: {total_remaining:.2f} 元")

    # ========== 验证平衡 ==========
    print("\n" + "="*100)
    print("平衡验证")
    print("="*100)
    print(f"  拆分入金额合计 = {total_split_amt:.2f} 元")
    print(f"  拆分出金额合计 = {total_allocated:.2f} 元")
    print(f"  差额 = {total_split_amt - total_allocated:.2f} 元 (应为0)")
    print(f"  Parent原进货额合计 = {total_original:.2f} 元")
    print(f"  拆分出 + 剩余 = {total_allocated + total_remaining:.2f} 元 (应等于原进货额)")

    print(f"\n{'='*100}")
    print("完成")
    print(f"{'='*100}")


if __name__ == "__main__":
    main()