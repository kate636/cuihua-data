"""
BOM拆分成本计算 v6（最终正确逻辑）

计算顺序：
    Step 1: Type A - 既是parent又是sub
        - 自己进货先满足自己需求
        - 剩余需求从别的parent分配
        - 更新parent剩余量

    Step 2: Type B - 纯sub（只有一个parent）
        - 按权重比例分配parent剩余量
        - 更新parent剩余量

    Step 3: Type C - 多parent的sub
        - 第一个parent剩余量分配
        - 剩余未满足由第二个parent补充

核心公式：
    拆分数量 = 销售数量 + 已知损耗数量
    权重 = 拆分数量 × 销售原价
    成本占比 = 权重 / Σ(该parent下未处理subs的权重)
    拆分金额 = 成本占比 × parent剩余进货额
"""

import sys
import csv
from fm_etl_v3.connectors import ApiConnector
from fm_etl_v3.config import get_settings


def fetch_all_data(api, business_date: str):
    """获取所有需要的数据"""

    # 1. receive_sale_di - BOM关系和parent进货
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

    all_ids = set()
    for r in rs_rows:
        all_ids.add(r.get('parentId', '') or '')
        all_ids.add(r.get('subId', '') or '')
    articles_str = ','.join([f"'{a}'" for a in all_ids if a])

    # 2. sales_di
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

    # 3. loss_di
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

    # 4. 商品名称（从dim_goods或receive_sale_di获取）
    # 从receive_sale_di提取sub_name映射
    sub_name_map = {}
    for r in rs_rows:
        sub_id = r.get('subId', '')
        sub_name = r.get('subName', '')
        if sub_id and sub_name:
            sub_name_map[sub_id] = sub_name

    return rs_rows, sales_rows, loss_rows, sub_name_map


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
            'know_lost_qty': r.get('knowLostQty', 0) or 0,
            'know_lost_amt': r.get('knowLostAmt', 0) or 0
        }

    return sales_lookup, loss_lookup


def get_requirement(sub_id, sales_lookup, loss_lookup, store_id):
    """计算sub的需求"""
    sale = sales_lookup.get((store_id, sub_id), {})
    loss = loss_lookup.get((store_id, sub_id), {})

    sale_qty = sale.get('sale_qty', 0)
    sale_amt = sale.get('sale_amt', 0)
    list_price = sale.get('list_price', 0)
    know_lost_qty = loss.get('know_lost_qty', 0)

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


def main():
    business_date = sys.argv[1] if len(sys.argv) > 1 else '2026-04-23'

    print(f"\n{'='*100}")
    print(f"BOM拆分成本计算 v6 - {business_date}")
    print(f"{'='*100}")

    cfg = get_settings()
    api = ApiConnector(cfg)

    rs_rows, sales_rows, loss_rows, sub_name_map = fetch_all_data(api, business_date)
    sales_lookup, loss_lookup = build_lookups(sales_rows, loss_rows)

    store_id = rs_rows[0].get('storeId', '') if rs_rows else ''

    # ========== 构建数据结构 ==========

    # sub -> parents映射
    sub_parents = {}
    for r in rs_rows:
        sub_id = r.get('subId', '')
        parent_id = r.get('parentId', '')
        if sub_id not in sub_parents:
            sub_parents[sub_id] = []
        if parent_id not in sub_parents[sub_id]:
            sub_parents[sub_id].append(parent_id)

    # parent信息
    parent_info = {}
    for r in rs_rows:
        parent_id = r.get('parentId', '')
        parent_name = r.get('parentName', '')
        parent_inbound_amt = r.get('parentInboundAmt', 0) or 0
        parent_inbound_qty = r.get('parentInboundQty', 0) or 0
        if parent_id not in parent_info:
            parent_info[parent_id] = {
                'name': parent_name,
                'original_amt': parent_inbound_amt,
                'original_qty': parent_inbound_qty,  # 保存进货数量
                'remaining_amt': parent_inbound_amt,
                'subs': []
            }
        sub_id = r.get('subId', '')
        if sub_id not in parent_info[parent_id]['subs']:
            parent_info[parent_id]['subs'].append(sub_id)

    # 分类
    parent_ids = set(parent_info.keys())
    sub_ids = set(sub_parents.keys())
    both_role = parent_ids & sub_ids

    type_a = [s for s in sub_ids if s in both_role]  # 既是parent又是sub
    type_b = [s for s in sub_ids if len(sub_parents[s]) == 1 and s not in both_role]  # 纯sub
    type_c = [s for s in sub_ids if len(sub_parents[s]) > 1 and s not in both_role]  # 多parent

    print(f"\n分类统计:")
    print(f"  Type A (既是parent又是sub): {len(type_a)} 个")
    print(f"  Type B (纯sub): {len(type_b)} 个")
    print(f"  Type C (多parent的sub): {len(type_c)} 个")

    # ========== 存储结果 ==========
    results = {}  # {sub_id: {各字段}}

    # ========== Step 1: 处理Type A ==========
    print("\n" + "="*80)
    print("Step 1: 处理Type A（既是parent又是sub）")
    print("="*80)

    for sub_id in type_a:
        req = get_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        weight = req['weight']  # 需求权重 = 消耗数量 × 销售原价
        list_price = req['list_price']

        # 自己作为parent的进货数据
        self_inbound_amt = parent_info[sub_id]['original_amt']  # 自己进货金额
        self_inbound_qty = parent_info[sub_id]['original_qty']  # 自己进货数量
        self_inbound_weight = self_inbound_qty * list_price  # 自己进货权重 = 数量 × 原价

        # 自己能满足的金额（按权重占比）
        # 计算自己作为parent下所有subs的总需求权重
        subs_under_self = parent_info[sub_id]['subs']
        total_weight_self = sum(
            get_requirement(s, sales_lookup, loss_lookup, store_id)['weight']
            for s in subs_under_self
        )

        if total_weight_self > 0:
            self_ratio = weight / total_weight_self  # 自己的需求权重占比
            amt_from_self = self_ratio * self_inbound_amt  # 自己进货分配给自己的金额
        else:
            amt_from_self = self_inbound_amt

        # 更新parent状态
        parent_info[sub_id]['remaining_amt'] -= amt_from_self

        # 剩余需求从别的parent分配
        remaining_weight = weight - self_inbound_weight if weight > self_inbound_weight else 0
        other_parents = [p for p in sub_parents[sub_id] if p != sub_id]

        amt_from_others = {}
        qty_from_others = {}
        weight_from_others = {}

        for other_p in other_parents:
            if remaining_weight <= 0:
                break

            # 计算该other_parent下未处理subs的总权重
            subs_under_other = parent_info[other_p]['subs']
            unprocessed_subs = [s for s in subs_under_other if s not in results]
            total_weight_other = sum(
                get_requirement(s, sales_lookup, loss_lookup, store_id)['weight']
                for s in unprocessed_subs
            )

            if total_weight_other > 0 and parent_info[other_p]['remaining_amt'] > 0:
                ratio = weight / total_weight_other  # 占比
                amt = ratio * parent_info[other_p]['remaining_amt']  # 拆分金额
                amt_from_others[other_p] = amt

                # 从parent拆分入数量（按占比推算）
                qty_from_others[other_p] = req['split_qty'] * ratio if ratio > 0 else 0

                # 从parent拆分入权重 = 需求权重 × 占比（按比例分配权重）
                weight_from_others[other_p] = weight * ratio

                parent_info[other_p]['remaining_amt'] -= amt
                remaining_weight -= weight_from_others[other_p]

        results[sub_id] = {
            'sub_id': sub_id,
            'sub_name': sub_name_map.get(sub_id, ''),
            'sale_qty': req['sale_qty'],
            'sale_amt': req['sale_amt'],
            'list_price': req['list_price'],
            'know_lost_qty': req['know_lost_qty'],
            'split_qty_in': req['split_qty'],  # 消耗数量 = 销售数量 + 已知损耗数量
            'weight_in': req['weight'],  # 需求权重 = 消耗数量 × 销售原价
            'self_inbound_qty': self_inbound_qty,  # 自己进货数量（实际数据）
            'self_inbound_weight': self_inbound_weight,  # 自己进货权重 = 数量 × 原价
            'self_inbound_amt': amt_from_self,  # 自己进货金额（实际分配）
            'split_from_parent_qty': sum(qty_from_others.values()),  # 从parent拆分入数量
            'split_from_parent_weight': sum(weight_from_others.values()),  # 从parent拆分入权重 = 需求权重 × 占比
            'split_from_parent_amt': sum(amt_from_others.values()),  # 从parent拆分入金额
            'split_amt_in': amt_from_self + sum(amt_from_others.values()),  # 拆分入金额合计
            'split_out_qty': 0,
            'split_out_weight': 0,
            'split_out_amt': 0,
            'parents': list(amt_from_others.keys()) + ([sub_id] if amt_from_self > 0 else []),
            'type': 'A',
            'details': [{'parent': sub_id, 'amt': amt_from_self}] +
                       [{'parent': p, 'amt': amt} for p, amt in amt_from_others.items()]
        }

    # ========== Step 2: 处理Type B ==========
    print("\n" + "="*80)
    print("Step 2: 处理Type B（纯sub）")
    print("="*80)

    # 按parent分组处理
    for parent_id, p_info in parent_info.items():
        type_b_subs = [s for s in p_info['subs'] if s in type_b and s not in results]

        if not type_b_subs:
            continue

        # 计算这些subs的总权重
        sub_weights = {}
        total_weight = 0
        for s in type_b_subs:
            req = get_requirement(s, sales_lookup, loss_lookup, store_id)
            sub_weights[s] = req['weight']
            total_weight += req['weight']

        parent_remaining = p_info['remaining_amt']

        for s in type_b_subs:
            req = get_requirement(s, sales_lookup, loss_lookup, store_id)
            weight = req['weight']  # 需求权重 = 消耗数量 × 销售原价
            list_price = req['list_price']

            if total_weight > 0 and parent_remaining > 0:
                ratio = weight / total_weight  # 占比 = 需求权重 / 总权重
                amt = ratio * parent_remaining  # 拆分金额 = 占比 × parent剩余进货额
            else:
                amt = 0
                ratio = 0

            parent_info[parent_id]['remaining_amt'] -= amt

            results[s] = {
                'sub_id': s,
                'sub_name': sub_name_map.get(s, ''),
                'sale_qty': req['sale_qty'],
                'sale_amt': req['sale_amt'],
                'list_price': req['list_price'],
                'know_lost_qty': req['know_lost_qty'],
                'split_qty_in': req['split_qty'],       # 消耗数量
                'weight_in': weight,                    # 需求权重（固定值）
                'self_inbound_amt': 0,
                'self_inbound_qty': 0,
                'self_inbound_weight': 0,
                'split_from_parent_amt': amt,          # 从parent拆分入金额（实际分配）
                'split_from_parent_qty': req['split_qty'] * ratio if ratio > 0 else 0,  # 从parent拆分入数量（按占比推算）
                'split_from_parent_weight': weight,    # 从parent拆分入权重 = 需求权重（固定值）
                'split_amt_in': amt,
                'split_out_qty': 0,
                'split_out_weight': 0,
                'split_out_amt': 0,
                'parents': [parent_id],
                'type': 'B',
                'details': [{'parent': parent_id, 'amt': amt}]
            }

    # ========== Step 3: 处理Type C ==========
    print("\n" + "="*80)
    print("Step 3: 处理Type C（多parent的sub）")
    print("="*80)

    for sub_id in type_c:
        if sub_id in results:
            continue

        req = get_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        weight = req['weight']
        parents = sub_parents[sub_id]

        print(f"\n{sub_id} ({sub_name_map.get(sub_id, '')}):")
        print(f"  需求权重: {weight:.2f}")
        print(f"  Parents: {parents}")

        # 查看各parent剩余
        for p in parents:
            print(f"    {p} ({parent_info[p]['name']}) 剩余: {parent_info[p]['remaining_amt']:.2f}")

        # 按顺序从parents分配（第一个parent优先）
        amt_from_parents = {}
        qty_from_parents = {}
        weight_from_parents = {}
        ratio_from_parents = {}  # 记录各parent下的占比

        remaining_amt_needed = weight  # 剩余需要的金额（用权重表示）
        list_price = req['list_price']

        for p in parents:
            parent_remaining_amt = parent_info[p]['remaining_amt']

            if parent_remaining_amt > 0 and remaining_amt_needed > 0:
                # 计算该parent下未处理subs的总权重
                subs_under = parent_info[p]['subs']
                unprocessed = [s for s in subs_under if s not in results]
                total_weight_under = sum(
                    get_requirement(s, sales_lookup, loss_lookup, store_id)['weight']
                    for s in unprocessed
                )

                if total_weight_under > 0:
                    ratio = weight / total_weight_under  # 在该parent下的占比
                    amt = min(ratio * parent_remaining_amt, remaining_amt_needed)  # 拆分金额
                else:
                    ratio = 1
                    amt = min(parent_remaining_amt, remaining_amt_needed)

                amt_from_parents[p] = amt
                ratio_from_parents[p] = ratio

                # 从parent拆分入数量（按金额推算）
                qty_from_parents[p] = amt / list_price if list_price > 0 else 0

                # 从parent拆分入权重 = 实际分配金额（代表实际满足的需求）
                weight_from_parents[p] = amt

                parent_info[p]['remaining_amt'] -= amt
                remaining_amt_needed -= amt

                print(f"  从 {p} 分配: 金额={amt:.2f}元, 占比={ratio:.4f}, 权重={amt:.2f}元")

            else:
                # parent无剩余或需求已满足，分配0
                amt_from_parents[p] = 0
                ratio_from_parents[p] = 0
                qty_from_parents[p] = 0
                weight_from_parents[p] = 0
                if parent_remaining_amt <= 0:
                    print(f"  从 {p} 分配: 金额=0元 (无剩余)")

        # 未满足的权重 = 需求权重 - 实际分配权重合计
        unmet_weight = weight - sum(weight_from_parents.values())

        results[sub_id] = {
            'sub_id': sub_id,
            'sub_name': sub_name_map.get(sub_id, ''),
            'sale_qty': req['sale_qty'],
            'sale_amt': req['sale_amt'],
            'list_price': req['list_price'],
            'know_lost_qty': req['know_lost_qty'],
            'split_qty_in': req['split_qty'],       # 消耗数量
            'weight_in': weight,                     # 需求权重（固定值）
            'self_inbound_amt': 0,
            'self_inbound_qty': 0,
            'self_inbound_weight': 0,
            'split_from_parent_amt': sum(amt_from_parents.values()),  # 从parent拆分入金额合计
            'split_from_parent_qty': sum(qty_from_parents.values()),  # 从parent拆分入数量合计
            'split_from_parent_weight': sum(weight_from_parents.values()),  # 从parent拆分入权重合计（=金额合计）
            'ratio_from_parents': ratio_from_parents,  # 各parent下的占比
            'unmet_weight': unmet_weight,              # 未满足权重 = 需求权重 - 实际分配权重
            'split_amt_in': sum(amt_from_parents.values()),
            'split_out_qty': 0,
            'split_out_weight': 0,
            'split_out_amt': 0,
            'parents': list(amt_from_parents.keys()),
            'type': 'C',
            'details': [{'parent': p, 'amt': amt, 'ratio': ratio_from_parents.get(p, 0)} for p, amt in amt_from_parents.items()]
        }

    # ========== 计算拆分出（Parent视角）==========
    print("\n" + "="*80)
    print("拆分出（Parent视角）")
    print("="*80)

    # 对于既是parent又是sub的，计算它们拆分出去多少
    for parent_id in parent_ids:
        if parent_id in results:  # Type A
            # 该parent分配给其他subs的金额
            allocated_to_others = parent_info[parent_id]['original_amt'] - parent_info[parent_id]['remaining_amt']
            # 自己用的部分不算拆分出
            self_used = results[parent_id]['self_inbound_amt']
            split_out_amt = allocated_to_others - self_used

            results[parent_id]['split_out_amt'] = split_out_amt
            results[parent_id]['split_out_qty'] = split_out_amt / parent_info[parent_id].get('avg_price', 1) if split_out_amt > 0 else 0
            results[parent_id]['split_out_weight'] = split_out_amt

    # ========== 输出表格 ==========

    # 按销售额排序
    sorted_results = sorted(results.items(), key=lambda x: x[1]['sale_amt'], reverse=True)

    # 输出完整表格
    print("\n" + "="*100)
    print("全量SKU计算结果")
    print("="*100)

    # 字段说明
    print("""
字段说明：
  销售数量           实际销售数量(kg)
  销售额             实际销售额(元)
  销售原价           商品原价(元/kg)
  已知损耗数量       已知损耗(kg)
  消耗数量           = 销售数量 + 已知损耗数量(kg)
  需求权重           = 消耗数量 × 销售原价(元) 【用于计算占比】

  自己进货数量       Type A自己作为parent的进货数量(kg)
  自己进货权重       = 自己进货数量 × 销售原价(元)
  自己进货金额       自己进货实际金额(元)

  从parent拆分入数量 从其他parent分配的数量(kg，按金额推算)
  从parent拆分入权重 = 从parent拆分入数量 × 销售原价(元)
  从parent拆分入金额 从parent分配的实际金额(元)

  拆分入数量合计     = 自己进货数量 + 从parent拆分入数量(kg)
  拆分入金额合计     = 自己进货金额 + 从parent拆分入金额(元)

  占比计算           = 需求权重 / Σ(parent下所有subs的需求权重) × parent进货额
  平均成本           = 拆分入金额合计 / 消耗数量(元/kg)
    """)

    # 表头（中文）
    headers = ['商品编码', '商品名称', '销售数量(kg)', '销售额(元)', '销售原价(元/kg)', '已知损耗数量(kg)',
               '消耗数量(kg)', '需求权重(元)',
               '自己进货数量(kg)', '自己进货权重(元)', '自己进货金额(元)',
               '从parent拆分入数量(kg)', '从parent拆分入权重(元)', '从parent拆分入金额(元)',
               '拆分入数量合计(kg)', '拆分入金额合计(元)',
               '占比', 'parents', '类型',
               '拆分出数量(kg)', '拆分出权重(元)', '拆分出金额(元)',
               '平均成本(元/kg)']
    print('|'.join(headers))
    print('-'*200)

    # 输出每行
    total_sale_qty = 0
    total_sale_amt = 0
    total_know_lost_qty = 0
    total_split_qty_in = 0
    total_weight_in = 0
    total_self_inbound_amt = 0
    total_self_inbound_qty = 0
    total_split_from_parent_amt = 0
    total_split_from_parent_qty = 0
    total_split_amt_in = 0
    total_split_out_amt = 0

    for sub_id, info in sorted_results:
        avg_cost = info['split_amt_in'] / info['split_qty_in'] if info['split_qty_in'] > 0 else 0
        parents_str = ','.join(str(p) for p in info['parents']) if isinstance(info['parents'], list) else str(info['parents'])

        # 拆分入数量合计 = 自己进货数量 + 从parent拆分入数量
        total_split_qty = info['self_inbound_qty'] + info['split_from_parent_qty']

        # 计算占比
        if info['type'] == 'C' and 'ratio_from_parents' in info:
            # Type C: 显示主要parent的占比（有实际分配的）
            main_ratio = max(info['ratio_from_parents'].values()) if info['ratio_from_parents'] else 0
            ratio_str = f"{main_ratio:.4f}"
        elif info['type'] == 'A':
            # Type A: 自己需求权重 / 自己作为parent下subs总权重
            subs_under_self = parent_info[sub_id]['subs']
            total_weight_self = sum(
                get_requirement(s, sales_lookup, loss_lookup, store_id)['weight']
                for s in subs_under_self
            )
            self_ratio = info['weight_in'] / total_weight_self if total_weight_self > 0 else 0
            ratio_str = f"{self_ratio:.4f}"
        elif info['type'] == 'B':
            # Type B: 需求权重 / 该parent下subs总权重
            parent_id = info['parents'][0]
            subs_under = parent_info[parent_id]['subs']
            total_weight = sum(
                get_requirement(s, sales_lookup, loss_lookup, store_id)['weight']
                for s in subs_under if s in type_b
            )
            ratio = info['weight_in'] / total_weight if total_weight > 0 else 0
            ratio_str = f"{ratio:.4f}"
        else:
            ratio_str = "N/A"

        row = [
            str(sub_id),
            str(info['sub_name'])[:12],
            f"{info['sale_qty']:.2f}",
            f"{info['sale_amt']:.2f}",
            f"{info['list_price']:.2f}",
            f"{info['know_lost_qty']:.2f}",
            f"{info['split_qty_in']:.2f}",        # 消耗数量
            f"{info['weight_in']:.2f}",            # 需求权重
            f"{info['self_inbound_qty']:.2f}",     # 自己进货数量
            f"{info['self_inbound_weight']:.2f}",  # 自己进货权重
            f"{info['self_inbound_amt']:.2f}",     # 自己进货金额
            f"{info['split_from_parent_qty']:.2f}",    # 从parent拆分入数量
            f"{info['split_from_parent_weight']:.2f}", # 从parent拆分入权重
            f"{info['split_from_parent_amt']:.2f}",    # 从parent拆分入金额
            f"{total_split_qty:.2f}",              # 拆分入数量合计
            f"{info['split_amt_in']:.2f}",         # 拆分入金额合计
            ratio_str,                              # 占比
            parents_str[:20],
            str(info['type']),
            f"{info['split_out_qty']:.2f}",        # 拆分出数量
            f"{info['split_out_weight']:.2f}",     # 拆分出权重
            f"{info['split_out_amt']:.2f}",        # 拆分出金额
            f"{avg_cost:.2f}"                      # 平均成本
        ]
        print('|'.join(row))

        # 累加汇总
        total_sale_qty += info['sale_qty']
        total_sale_amt += info['sale_amt']
        total_know_lost_qty += info['know_lost_qty']
        total_split_qty_in += info['split_qty_in']
        total_weight_in += info['weight_in']
        total_self_inbound_amt += info['self_inbound_amt']
        total_self_inbound_qty += info['self_inbound_qty']
        total_split_from_parent_amt += info['split_from_parent_amt']
        total_split_from_parent_qty += info['split_from_parent_qty']
        total_split_amt_in += info['split_amt_in']
        total_split_out_amt += info['split_out_amt']

    print('-'*200)

    # 汇总
    print("\n" + "="*100)
    print("汇总")
    print("="*100)
    print(f"  SKU数量: {len(results)}")
    print(f"  销售数量合计: {total_sale_qty:.2f} kg")
    print(f"  已知损耗数量合计: {total_know_lost_qty:.2f} kg")
    print(f"  销售额合计: {total_sale_amt:.2f} 元")
    print(f"  消耗数量合计: {total_split_qty_in:.2f} kg")
    print(f"  需求权重合计: {total_weight_in:.2f} 元")
    print(f"  自己进货数量合计: {total_self_inbound_qty:.2f} kg")
    print(f"  自己进货金额合计: {total_self_inbound_amt:.2f} 元")
    print(f"  从parent拆分入数量合计: {total_split_from_parent_qty:.2f} kg")
    print(f"  从parent拆分入金额合计: {total_split_from_parent_amt:.2f} 元")
    print(f"  拆分入数量合计: {total_self_inbound_qty + total_split_from_parent_qty:.2f} kg")
    print(f"  拆分入金额合计: {total_split_amt_in:.2f} 元")
    print(f"  拆分出金额合计: {total_split_out_amt:.2f} 元")

    # Parent进货额验证
    total_parent_inbound = sum(p['original_amt'] for p in parent_info.values())
    total_parent_remaining = sum(p['remaining_amt'] for p in parent_info.values())
    print(f"\n  Parent原进货额合计: {total_parent_inbound:.2f} 元")
    print(f"  Parent剩余合计: {total_parent_remaining:.2f} 元")
    print(f"  已分配合计: {total_parent_inbound - total_parent_remaining:.2f} 元")

    # 平衡验证
    print(f"\n  平衡验证: 拆分入金额({total_split_amt_in:.2f}) 应等于 Parent已分配({total_parent_inbound - total_parent_remaining:.2f})")

    # ========== 输出Excel ==========
    # 简化Excel输出 - 直接写CSV（UTF-8-sig编码，Excel兼容）
    import csv
    output_path = f'/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/bom_split_result_{business_date}.csv'

    # 公式说明行（第二行）
    formulas = [
        '【公式】',
        '',
        '来自sales_di.qty_spec',
        '来自sales_di.sales_amt',
        '来自sales_di.list_price (AVG)',
        '来自loss_di.know_lost_qty',
        '=销售数量+已知损耗数量',
        '=消耗数量×销售原价',
        '来自receive_sale_di (Type A自己进货)',
        '=自己进货数量×销售原价',
        '=占比×自己进货额 (占比=需求权重/Σsubs权重)',
        '=从parent拆分入金额/销售原价',
        '=从parent拆分入数量×销售原价',
        '=占比×parent剩余进货额',
        '=自己进货数量+从parent拆分入数量',
        '=自己进货金额+从parent拆分入金额',
        '=需求权重/Σ(parent下subs的需求权重)',
        'BOM关系来源',
        'A=既是parent又是sub, B=纯sub, C=多parent',
        '',
        '',
        '',
        '=拆分入金额合计/消耗数量'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerow(formulas)  # 公式说明行
        for sub_id, info in sorted_results:
            avg_cost = info['split_amt_in'] / info['split_qty_in'] if info['split_qty_in'] > 0 else 0
            parents_str = ','.join(str(p) for p in info['parents']) if isinstance(info['parents'], list) else str(info['parents'])
            total_split_qty = info['self_inbound_qty'] + info['split_from_parent_qty']  # 拆分入数量合计

            # 计算占比
            if info['type'] == 'C' and 'ratio_from_parents' in info:
                main_ratio = max(info['ratio_from_parents'].values()) if info['ratio_from_parents'] else 0
            elif info['type'] == 'A':
                subs_under_self = parent_info[sub_id]['subs']
                total_weight_self = sum(
                    get_requirement(s, sales_lookup, loss_lookup, store_id)['weight']
                    for s in subs_under_self
                )
                main_ratio = info['weight_in'] / total_weight_self if total_weight_self > 0 else 0
            elif info['type'] == 'B':
                parent_id = info['parents'][0]
                subs_under = parent_info[parent_id]['subs']
                total_weight = sum(
                    get_requirement(s, sales_lookup, loss_lookup, store_id)['weight']
                    for s in subs_under if s in type_b
                )
                main_ratio = info['weight_in'] / total_weight if total_weight > 0 else 0
            else:
                main_ratio = 0

            writer.writerow([
                sub_id, info['sub_name'], info['sale_qty'], info['sale_amt'], info['list_price'],
                info['know_lost_qty'], info['split_qty_in'], info['weight_in'],
                info['self_inbound_qty'], info['self_inbound_weight'], info['self_inbound_amt'],
                info['split_from_parent_qty'], info['split_from_parent_weight'], info['split_from_parent_amt'],
                total_split_qty, info['split_amt_in'],
                main_ratio, parents_str, info['type'],
                info['split_out_qty'], info['split_out_weight'], info['split_out_amt'],
                avg_cost
            ])
    print(f"\n  CSV已保存: {output_path}")

    # Parent拆分出表格
    parent_output = f'/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/bom_parent_split_{business_date}.csv'
    with open(parent_output, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['parent编码', 'parent名称', '原进货额(元)', '已分配金额(元)', '剩余金额(元)'])
        for p_id, p_info in parent_info.items():
            allocated = p_info['original_amt'] - p_info['remaining_amt']
            writer.writerow([p_id, p_info['name'], p_info['original_amt'], allocated, p_info['remaining_amt']])
    print(f"  Parent拆分CSV: {parent_output}")

    print(f"\n{'='*100}")
    print("完成")
    print(f"{'='*100}")


if __name__ == "__main__":
    main()