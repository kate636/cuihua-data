"""
BOM拆分成本计算 v7（最终版 - 公式修正 + 详细log）

核心修正：
1. Type A占比计算：用拆分需求权重（消耗权重 - 自己进货权重），不是消耗权重
2. 拆分入数量：用拆分需求重量，不是从金额推算
3. 分层处理：Type A → Type B → Type C，上层剩余在处理当前Type之前已确定
4. 详细log输出：每一步计算都生成log

计算顺序（分层处理）：
    Round 1: 处理所有 Type A subs → 得到给 Type B 的 parent 剩余
    Round 2: 处理所有 Type B subs → 得到给 Type C 的 parent 剩余
    Round 3: 处理所有 Type C subs（按parent顺序逐个分配）

核心公式：
    消耗数量 = 销售数量 + 已知损耗数量
    消耗权重 = 消耗数量 × 销售原价

    【Type A】
    自己进货数量 = receive_sale_di.inbound_qty
    自己进货金额 = receive_sale_di.inbound_amount
    拆分需求重量 = 消耗数量 - 自己进货数量
    拆分需求权重 = 消耗权重 - 自己进货权重

    【Type B/C】
    拆分需求重量 = 消耗数量
    拆分需求权重 = 消耗权重

    【占比计算】
    占比 = 拆分需求权重 / Σ(parent下subs的拆分需求权重)

    【拆分入】
    拆分入金额 = 占比 × parent剩余进货额
    拆分入数量 = 拆分需求重量（或按比例缩减）

    【合计】
    合计成本数量 = 自己进货数量 + 拆分入数量
    合计成本金额 = 自己进货金额 + 拆分入金额
    平均成本 = 合计成本金额 / 消耗数量
"""

import sys
import csv
import logging
from datetime import datetime
from fm_etl_v3.connectors import ApiConnector
from fm_etl_v3.config import get_settings


def setup_logger(business_date: str):
    """设置日志"""
    log_path = f'/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/bom_split_log_{business_date}.txt'

    logger = logging.getLogger('bom_profit')
    logger.setLevel(logging.INFO)

    # 文件handler
    fh = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    fh.setLevel(logging.INFO)

    # 控制台handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # 格式
    formatter = logging.Formatter('%(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def fetch_all_data(api, business_date: str, log):
    """获取所有需要的数据"""
    log.info(f"【数据获取】从 strategy_fm_receive_sale_di 获取BOM关系...")

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
    log.info(f"  获取到 {len(rs_rows)} 条BOM关系记录")

    all_ids = set()
    for r in rs_rows:
        all_ids.add(r.get('parentId', '') or '')
        all_ids.add(r.get('subId', '') or '')
    articles_str = ','.join([f"'{a}'" for a in all_ids if a])

    log.info(f"【数据获取】从 strategy_fm_sales_di 获取销售数据...")
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
    log.info(f"  获取到 {len(sales_rows)} 条销售记录")

    log.info(f"【数据获取】从 strategy_fm_loss_di 获取损耗数据...")
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
    log.info(f"  获取到 {len(loss_rows)} 条损耗记录")

    # sub_name映射
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
    """计算sub的消耗需求"""
    sale = sales_lookup.get((store_id, sub_id), {})
    loss = loss_lookup.get((store_id, sub_id), {})

    sale_qty = sale.get('sale_qty', 0)
    sale_amt = sale.get('sale_amt', 0)
    list_price = sale.get('list_price', 0)
    know_lost_qty = loss.get('know_lost_qty', 0)

    consume_qty = sale_qty + know_lost_qty  # 消耗数量
    consume_weight = consume_qty * list_price  # 消耗权重

    return {
        'sale_qty': sale_qty,
        'sale_amt': sale_amt,
        'list_price': list_price,
        'know_lost_qty': know_lost_qty,
        'consume_qty': consume_qty,
        'consume_weight': consume_weight
    }


def get_split_need_weight(sub_id, sales_lookup, loss_lookup, store_id, parent_info, type_a_set):
    """计算拆分需求权重（修正点）"""
    req = get_requirement(sub_id, sales_lookup, loss_lookup, store_id)

    if sub_id in type_a_set:
        # Type A: 拆分需求权重 = 消耗权重 - 自己进货权重
        self_inbound_qty = parent_info[sub_id]['original_qty']
        self_inbound_weight = self_inbound_qty * req['list_price']
        split_need_weight = req['consume_weight'] - self_inbound_weight
        split_need_qty = req['consume_qty'] - self_inbound_qty
    else:
        # Type B/C: 拆分需求权重 = 消耗权重
        split_need_weight = req['consume_weight']
        split_need_qty = req['consume_qty']

    return {
        'split_need_weight': split_need_weight,
        'split_need_qty': split_need_qty,
        'req': req
    }


def main():
    business_date = sys.argv[1] if len(sys.argv) > 1 else '2026-04-23'

    log = setup_logger(business_date)

    log.info(f"\n{'='*100}")
    log.info(f"BOM拆分成本计算 v7 - {business_date}")
    log.info(f"{'='*100}")
    log.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    cfg = get_settings()
    api = ApiConnector(cfg)

    rs_rows, sales_rows, loss_rows, sub_name_map = fetch_all_data(api, business_date, log)
    sales_lookup, loss_lookup = build_lookups(sales_rows, loss_rows)

    store_id = rs_rows[0].get('storeId', '') if rs_rows else ''

    # ========== 构建数据结构 ==========
    log.info(f"\n【数据结构】构建sub->parents映射...")

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
    log.info(f"【数据结构】构建parent信息...")
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
                'original_qty': parent_inbound_qty,
                'remaining_amt': parent_inbound_amt,
                'remaining_qty': parent_inbound_qty,
                'subs': []
            }
        sub_id = r.get('subId', '')
        if sub_id not in parent_info[parent_id]['subs']:
            parent_info[parent_id]['subs'].append(sub_id)

    # 分类
    parent_ids = set(parent_info.keys())
    sub_ids = set(sub_parents.keys())
    both_role = parent_ids & sub_ids

    type_a = [s for s in sub_ids if s in both_role]  # Type A
    type_b = [s for s in sub_ids if len(sub_parents[s]) == 1 and s not in both_role]  # Type B
    type_c = [s for s in sub_ids if len(sub_parents[s]) > 1 and s not in both_role]  # Type C

    type_a_set = set(type_a)  # 用于判断是否Type A

    log.info(f"\n【分类统计】")
    log.info(f"  Type A (既是parent又是sub): {len(type_a)} 个")
    log.info(f"  Type B (纯sub): {len(type_b)} 个")
    log.info(f"  Type C (多parent的sub): {len(type_c)} 个")

    # ========== 存储结果 ==========
    results = {}

    # ========== Round 1: 处理Type A ==========
    log.info(f"\n{'='*80}")
    log.info(f"【Round 1】处理 Type A（既是parent又是sub）")
    log.info(f"{'='*80}")

    # 记录Type A总分配（用于计算给Type B的parent剩余）
    type_a_total_split_amt = {}  # {parent_id: total_amt}
    type_a_total_split_qty = {}  # {parent_id: total_qty}

    for sub_id in type_a:
        sub_name = sub_name_map.get(sub_id, '')
        log.info(f"\n【处理】{sub_id} ({sub_name}) - Type A")

        req = get_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        consume_weight = req['consume_weight']
        consume_qty = req['consume_qty']
        list_price = req['list_price']

        log.info(f"  销售数量: {req['sale_qty']:.2f} kg")
        log.info(f"  已知损耗: {req['know_lost_qty']:.2f} kg")
        log.info(f"  消耗数量: {consume_qty:.2f} kg = 销售数量({req['sale_qty']:.2f}) + 已知损耗({req['know_lost_qty']:.2f})")
        log.info(f"  销售原价: {list_price:.2f} 元/kg")
        log.info(f"  消耗权重: {consume_weight:.2f} 元 = 消耗数量({consume_qty:.2f}) × 销售原价({list_price:.2f})")

        # 自己进货部分
        self_inbound_qty = parent_info[sub_id]['original_qty']
        self_inbound_amt = parent_info[sub_id]['original_amt']
        self_inbound_weight = self_inbound_qty * list_price

        log.info(f"  【自己进货】")
        log.info(f"    自己进货数量: {self_inbound_qty:.2f} kg (来自receive_sale_di)")
        log.info(f"    自己进货金额: {self_inbound_amt:.2f} 元 (来自receive_sale_di)")
        log.info(f"    自己进货权重: {self_inbound_weight:.2f} 元 = 自己进货数量({self_inbound_qty:.2f}) × 销售原价({list_price:.2f})")

        # 拆分需求（修正点）
        split_need_qty = consume_qty - self_inbound_qty
        split_need_weight = consume_weight - self_inbound_weight

        log.info(f"  【拆分需求】")
        log.info(f"    拆分需求重量: {split_need_qty:.2f} kg = 消耗数量({consume_qty:.2f}) - 自己进货数量({self_inbound_qty:.2f})")
        log.info(f"    拆分需求权重: {split_need_weight:.2f} 元 = 消耗权重({consume_weight:.2f}) - 自己进货权重({self_inbound_weight:.2f})")

        # 从其他parent拆分
        other_parents = [p for p in sub_parents[sub_id] if p != sub_id]

        amt_from_others = {}
        qty_from_others = {}

        for other_p in other_parents:
            parent_name = parent_info[other_p]['name']
            parent_remaining_amt = parent_info[other_p]['remaining_amt']
            parent_remaining_qty = parent_info[other_p]['remaining_qty']

            log.info(f"  【从parent({other_p})拆分】")
            log.info(f"    parent: {parent_name}")
            log.info(f"    parent剩余进货额: {parent_remaining_amt:.2f} 元 (上层剩余)")
            log.info(f"    parent剩余进货数量: {parent_remaining_qty:.2f} kg (上层剩余)")

            if parent_remaining_amt <= 0:
                log.info(f"    parent无剩余，分配0")
                amt_from_others[other_p] = 0
                qty_from_others[other_p] = 0
                continue

            # 计算该parent下未处理subs的拆分需求权重合计
            subs_under_other = parent_info[other_p]['subs']
            unprocessed_subs = [s for s in subs_under_other if s not in results]

            total_split_need_weight = 0
            for s in unprocessed_subs:
                s_info = get_split_need_weight(s, sales_lookup, loss_lookup, store_id, parent_info, type_a_set)
                total_split_need_weight += s_info['split_need_weight']

            log.info(f"    Σ(parent下subs拆分需求权重): {total_split_need_weight:.2f} 元 (遍历parent下所有未处理subs)")

            # 占比（修正点：用拆分需求权重）
            if total_split_need_weight > 0:
                ratio = split_need_weight / total_split_need_weight
            else:
                ratio = 0

            log.info(f"    占比: {ratio:.4f} = 拆分需求权重({split_need_weight:.2f}) / Σ(parent下subs拆分需求权重)({total_split_need_weight:.2f})")

            # 拆分入金额
            split_in_amt = ratio * parent_remaining_amt
            log.info(f"    拆分入金额: {split_in_amt:.2f} 元 = 占比({ratio:.4f}) × parent剩余进货额({parent_remaining_amt:.2f})")

            # 拆分入数量（修正点：用拆分需求重量）
            split_in_qty = split_need_qty
            log.info(f"    拆分入数量: {split_in_qty:.2f} kg = 拆分需求重量({split_need_qty:.2f})")

            amt_from_others[other_p] = split_in_amt
            qty_from_others[other_p] = split_in_qty

            # 更新parent剩余（本轮内）
            parent_info[other_p]['remaining_amt'] -= split_in_amt
            parent_info[other_p]['remaining_qty'] -= split_in_qty

            # 记录Type A总分配
            if other_p not in type_a_total_split_amt:
                type_a_total_split_amt[other_p] = 0
                type_a_total_split_qty[other_p] = 0
            type_a_total_split_amt[other_p] += split_in_amt
            type_a_total_split_qty[other_p] += split_in_qty

        # 汇总
        total_split_in_amt = self_inbound_amt + sum(amt_from_others.values())
        total_split_in_qty = self_inbound_qty + sum(qty_from_others.values())
        avg_cost = total_split_in_amt / consume_qty if consume_qty > 0 else 0

        log.info(f"  【汇总】")
        log.info(f"    合计成本数量: {total_split_in_qty:.2f} kg = 自己进货数量({self_inbound_qty:.2f}) + 拆分入数量({sum(qty_from_others.values()):.2f})")
        log.info(f"    合计成本金额: {total_split_in_amt:.2f} 元 = 自己进货金额({self_inbound_amt:.2f}) + 拆分入金额({sum(amt_from_others.values()):.2f})")
        log.info(f"    平均成本: {avg_cost:.2f} 元/kg = 合计成本金额({total_split_in_amt:.2f}) / 消耗数量({consume_qty:.2f})")

        results[sub_id] = {
            'sub_id': sub_id,
            'sub_name': sub_name,
            'sale_qty': req['sale_qty'],
            'sale_amt': req['sale_amt'],
            'list_price': list_price,
            'know_lost_qty': req['know_lost_qty'],
            'consume_qty': consume_qty,
            'consume_weight': consume_weight,
            'self_inbound_qty': self_inbound_qty,
            'self_inbound_weight': self_inbound_weight,
            'self_inbound_amt': self_inbound_amt,
            'split_need_qty': split_need_qty,
            'split_need_weight': split_need_weight,
            'split_from_parent_qty': sum(qty_from_others.values()),
            'split_from_parent_amt': sum(amt_from_others.values()),
            'total_cost_qty': total_split_in_qty,
            'total_cost_amt': total_split_in_amt,
            'avg_cost': avg_cost,
            'parents': list(amt_from_others.keys()),
            'type': 'A'
        }

    # ========== 计算给Type B的parent剩余 ==========
    log.info(f"\n{'='*80}")
    log.info(f"【计算给Type B的parent剩余】")
    log.info(f"{'='*80}")

    for parent_id in parent_info:
        original_amt = parent_info[parent_id]['original_amt']
        original_qty = parent_info[parent_id]['original_qty']
        allocated_amt = type_a_total_split_amt.get(parent_id, 0)
        allocated_qty = type_a_total_split_qty.get(parent_id, 0)
        remaining_for_type_b_amt = original_amt - allocated_amt
        remaining_for_type_b_qty = original_qty - allocated_qty

        parent_info[parent_id]['remaining_amt'] = remaining_for_type_b_amt
        parent_info[parent_id]['remaining_qty'] = remaining_for_type_b_qty

        log.info(f"  parent {parent_id} ({parent_info[parent_id]['name']})")
        log.info(f"    原进货额: {original_amt:.2f} 元")
        log.info(f"    Type A已分配: {allocated_amt:.2f} 元")
        log.info(f"    给Type B剩余: {remaining_for_type_b_amt:.2f} 元")

    # ========== Round 2: 处理Type B ==========
    log.info(f"\n{'='*80}")
    log.info(f"【Round 2】处理 Type B（纯sub）")
    log.info(f"{'='*80}")

    type_b_total_split_amt = {}
    type_b_total_split_qty = {}

    for sub_id in type_b:
        sub_name = sub_name_map.get(sub_id, '')
        log.info(f"\n【处理】{sub_id} ({sub_name}) - Type B")

        req = get_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        consume_weight = req['consume_weight']
        consume_qty = req['consume_qty']
        list_price = req['list_price']

        log.info(f"  消耗数量: {consume_qty:.2f} kg (Type B无自己进货)")
        log.info(f"  消耗权重: {consume_weight:.2f} 元 = 消耗数量({consume_qty:.2f}) × 销售原价({list_price:.2f})")

        # Type B无自己进货
        split_need_qty = consume_qty
        split_need_weight = consume_weight

        log.info(f"  拆分需求重量: {split_need_qty:.2f} kg = 消耗数量({consume_qty:.2f}) (Type B无自己进货)")
        log.info(f"  拆分需求权重: {split_need_weight:.2f} 元 = 消耗权重({consume_weight:.2f}) (Type B无自己进货)")

        # 从唯一parent拆分
        parent_id = sub_parents[sub_id][0]
        parent_name = parent_info[parent_id]['name']
        parent_remaining_amt = parent_info[parent_id]['remaining_amt']
        parent_remaining_qty = parent_info[parent_id]['remaining_qty']

        log.info(f"  【从parent({parent_id})拆分】")
        log.info(f"    parent: {parent_name}")
        log.info(f"    parent剩余进货额: {parent_remaining_amt:.2f} 元 (Type A处理后剩余)")
        log.info(f"    parent剩余进货数量: {parent_remaining_qty:.2f} kg (Type A处理后剩余)")

        if parent_remaining_amt <= 0:
            log.info(f"    parent无剩余，分配0")
            split_in_amt = 0
            split_in_qty = 0
        else:
            # 计算parent下未处理subs的拆分需求权重合计
            subs_under = parent_info[parent_id]['subs']
            unprocessed_subs = [s for s in subs_under if s not in results and s in type_b]

            total_split_need_weight = sum(
                get_split_need_weight(s, sales_lookup, loss_lookup, store_id, parent_info, type_a_set)['split_need_weight']
                for s in unprocessed_subs
            )

            log.info(f"    Σ(parent下subs拆分需求权重): {total_split_need_weight:.2f} 元 (遍历parent下所有未处理Type B subs)")

            ratio = split_need_weight / total_split_need_weight if total_split_need_weight > 0 else 0
            log.info(f"    占比: {ratio:.4f} = 拆分需求权重({split_need_weight:.2f}) / Σ(parent下subs拆分需求权重)({total_split_need_weight:.2f})")

            split_in_amt = ratio * parent_remaining_amt
            split_in_qty = split_need_qty

            log.info(f"    拆分入金额: {split_in_amt:.2f} 元 = 占比({ratio:.4f}) × parent剩余进货额({parent_remaining_amt:.2f})")
            log.info(f"    拆分入数量: {split_in_qty:.2f} kg = 拆分需求重量({split_need_qty:.2f})")

            # 更新parent剩余
            parent_info[parent_id]['remaining_amt'] -= split_in_amt
            parent_info[parent_id]['remaining_qty'] -= split_in_qty

        # 记录Type B总分配
        if parent_id not in type_b_total_split_amt:
            type_b_total_split_amt[parent_id] = 0
            type_b_total_split_qty[parent_id] = 0
        type_b_total_split_amt[parent_id] += split_in_amt
        type_b_total_split_qty[parent_id] += split_in_qty

        # 汇总
        total_cost_qty = split_in_qty
        total_cost_amt = split_in_amt
        avg_cost = total_cost_amt / consume_qty if consume_qty > 0 else 0

        log.info(f"  【汇总】")
        log.info(f"    合计成本数量: {total_cost_qty:.2f} kg = 拆分入数量({split_in_qty:.2f}) (Type B无自己进货)")
        log.info(f"    合计成本金额: {total_cost_amt:.2f} 元 = 拆分入金额({split_in_amt:.2f}) (Type B无自己进货)")
        log.info(f"    平均成本: {avg_cost:.2f} 元/kg = 合计成本金额({total_cost_amt:.2f}) / 消耗数量({consume_qty:.2f})")

        results[sub_id] = {
            'sub_id': sub_id,
            'sub_name': sub_name,
            'sale_qty': req['sale_qty'],
            'sale_amt': req['sale_amt'],
            'list_price': list_price,
            'know_lost_qty': req['know_lost_qty'],
            'consume_qty': consume_qty,
            'consume_weight': consume_weight,
            'self_inbound_qty': 0,
            'self_inbound_weight': 0,
            'self_inbound_amt': 0,
            'split_need_qty': split_need_qty,
            'split_need_weight': split_need_weight,
            'split_from_parent_qty': split_in_qty,
            'split_from_parent_amt': split_in_amt,
            'total_cost_qty': total_cost_qty,
            'total_cost_amt': total_cost_amt,
            'avg_cost': avg_cost,
            'parents': [parent_id],
            'type': 'B'
        }

    # ========== 计算给Type C的parent剩余 ==========
    log.info(f"\n{'='*80}")
    log.info(f"【计算给Type C的parent剩余】")
    log.info(f"{'='*80}")

    for parent_id in parent_info:
        original_amt = parent_info[parent_id]['original_amt']
        original_qty = parent_info[parent_id]['original_qty']
        allocated_amt = type_a_total_split_amt.get(parent_id, 0) + type_b_total_split_amt.get(parent_id, 0)
        allocated_qty = type_a_total_split_qty.get(parent_id, 0) + type_b_total_split_qty.get(parent_id, 0)
        remaining_for_type_c_amt = original_amt - allocated_amt
        remaining_for_type_c_qty = original_qty - allocated_qty

        parent_info[parent_id]['remaining_amt'] = remaining_for_type_c_amt
        parent_info[parent_id]['remaining_qty'] = remaining_for_type_c_qty

        log.info(f"  parent {parent_id} ({parent_info[parent_id]['name']})")
        log.info(f"    原进货额: {original_amt:.2f} 元")
        log.info(f"    Type A+B已分配: {allocated_amt:.2f} 元")
        log.info(f"    给Type C剩余: {remaining_for_type_c_amt:.2f} 元")

    # ========== Round 3: 处理Type C（按parent顺序） ==========
    log.info(f"\n{'='*80}")
    log.info(f"【Round 3】处理 Type C（多parent的sub）")
    log.info(f"{'='*80}")

    # Type C需要按parent顺序处理：先处理从第一个parent的所有subs，再处理从第二个parent的subs
    # 按parent分组Type C subs
    type_c_by_parent = {}
    for sub_id in type_c:
        for parent_id in sub_parents[sub_id]:
            if parent_id not in type_c_by_parent:
                type_c_by_parent[parent_id] = []
            type_c_by_parent[parent_id].append(sub_id)

    # 未满足需求追踪
    unmet_demand = {}  # {sub_id: {'qty': 未满足重量, 'weight': 未满足权重}}

    for parent_id, subs in type_c_by_parent.items():
        log.info(f"\n【本轮】从parent {parent_id} ({parent_info[parent_id]['name']})拆分")
        log.info(f"  parent剩余进货额: {parent_info[parent_id]['remaining_amt']:.2f} 元")
        log.info(f"  parent剩余进货数量: {parent_info[parent_id]['remaining_qty']:.2f} kg")

        for sub_id in subs:
            sub_name = sub_name_map.get(sub_id, '')
            log.info(f"\n  【处理】{sub_id} ({sub_name}) - Type C")

            req = get_requirement(sub_id, sales_lookup, loss_lookup, store_id)
            consume_qty = req['consume_qty']
            consume_weight = req['consume_weight']
            list_price = req['list_price']

            # 如果之前有未满足需求，使用未满足部分
            if sub_id in unmet_demand:
                split_need_qty = unmet_demand[sub_id]['qty']
                split_need_weight = unmet_demand[sub_id]['weight']
                log.info(f"    未满足需求重量: {split_need_qty:.2f} kg (从上一轮parent)")
            else:
                split_need_qty = consume_qty
                split_need_weight = consume_weight

            log.info(f"    拆分需求重量: {split_need_qty:.2f} kg")
            log.info(f"    拆分需求权重: {split_need_weight:.2f} 元 = 拆分需求重量({split_need_qty:.2f}) × 销售原价({list_price:.2f})")

            parent_remaining_amt = parent_info[parent_id]['remaining_amt']
            parent_remaining_qty = parent_info[parent_id]['remaining_qty']

            if parent_remaining_amt <= 0:
                log.info(f"    parent无剩余，本轮分配0")
                split_in_amt = 0
                split_in_qty = 0

                # 记录未满足需求
                if sub_id not in unmet_demand:
                    unmet_demand[sub_id] = {'qty': split_need_qty, 'weight': split_need_weight}
            else:
                # 计算本轮所有subs的拆分需求权重合计
                total_split_need_weight = 0
                for s in subs:
                    if s in unmet_demand:
                        total_split_need_weight += unmet_demand[s]['weight']
                    elif s not in results:
                        s_req = get_requirement(s, sales_lookup, loss_lookup, store_id)
                        total_split_need_weight += s_req['consume_weight']

                log.info(f"    Σ(本轮subs拆分需求权重): {total_split_need_weight:.2f} 元 (本轮从该parent拆分的所有Type C subs)")

                ratio = split_need_weight / total_split_need_weight if total_split_need_weight > 0 else 0
                log.info(f"    占比: {ratio:.4f} = 拆分需求权重({split_need_weight:.2f}) / Σ(本轮subs拆分需求权重)({total_split_need_weight:.2f})")

                split_in_amt = ratio * parent_remaining_amt
                split_in_qty = split_need_qty

                log.info(f"    拆分入金额: {split_in_amt:.2f} 元 = 占比({ratio:.4f}) × parent剩余进货额({parent_remaining_amt:.2f})")
                log.info(f"    拆分入数量: {split_in_qty:.2f} kg = 拆分需求重量({split_need_qty:.2f})")

                # 更新parent剩余
                parent_info[parent_id]['remaining_amt'] -= split_in_amt
                parent_info[parent_id]['remaining_qty'] -= split_in_qty

                # 如果全满足，清除未满足需求
                if sub_id in unmet_demand:
                    del unmet_demand[sub_id]

            # 更新results（Type C可能需要多轮分配）
            if sub_id in results:
                # 累加本轮分配
                results[sub_id]['split_from_parent_qty'] += split_in_qty
                results[sub_id]['split_from_parent_amt'] += split_in_amt
                results[sub_id]['total_cost_qty'] += split_in_qty
                results[sub_id]['total_cost_amt'] += split_in_amt
                results[sub_id]['avg_cost'] = results[sub_id]['total_cost_amt'] / results[sub_id]['consume_qty'] if results[sub_id]['consume_qty'] > 0 else 0
                results[sub_id]['parents'].append(parent_id)
            else:
                # 首次分配
                total_cost_qty = split_in_qty
                total_cost_amt = split_in_amt
                avg_cost = total_cost_amt / consume_qty if consume_qty > 0 else 0

                results[sub_id] = {
                    'sub_id': sub_id,
                    'sub_name': sub_name,
                    'sale_qty': req['sale_qty'],
                    'sale_amt': req['sale_amt'],
                    'list_price': list_price,
                    'know_lost_qty': req['know_lost_qty'],
                    'consume_qty': consume_qty,
                    'consume_weight': consume_weight,
                    'self_inbound_qty': 0,
                    'self_inbound_weight': 0,
                    'self_inbound_amt': 0,
                    'split_need_qty': split_need_qty,
                    'split_need_weight': split_need_weight,
                    'split_from_parent_qty': split_in_qty,
                    'split_from_parent_amt': split_in_amt,
                    'total_cost_qty': total_cost_qty,
                    'total_cost_amt': total_cost_amt,
                    'avg_cost': avg_cost,
                    'parents': [parent_id],
                    'type': 'C'
                }

            log.info(f"    本轮后合计成本数量: {results[sub_id]['total_cost_qty']:.2f} kg")
            log.info(f"    本轮后合计成本金额: {results[sub_id]['total_cost_amt']:.2f} 元")

    # ========== 输出CSV ==========
    log.info(f"\n{'='*80}")
    log.info(f"【输出CSV】")
    log.info(f"{'='*80}")

    # 按销售额排序
    sorted_results = sorted(results.items(), key=lambda x: x[1]['sale_amt'], reverse=True)

    headers = [
        '商品编码', '商品名称', '销售数量(kg)', '销售额(元)', '销售原价(元/kg)', '已知损耗数量(kg)',
        '消耗数量(kg)', '消耗权重(元)',
        '自己进货数量(kg)', '自己进货权重(元)', '自己进货金额(元)',
        '拆分需求重量(kg)', '拆分需求权重(元)',
        '从parent拆分入数量(kg)', '从parent拆分入金额(元)',
        '合计成本数量(kg)', '合计成本金额(元)',
        '平均成本(元/kg)', 'parents', '类型'
    ]

    formulas = [
        '【公式】',
        '', 'sales_di.qty_spec', 'sales_di.sales_amt', 'sales_di.list_price (AVG)', 'loss_di.know_lost_qty',
        '=销售数量+已知损耗', '=消耗数量×销售原价',
        'receive_sale_di (Type A)', '=自己进货数量×销售原价', 'receive_sale_di (Type A)',
        '=消耗数量-自己进货数量 (Type A)', '=消耗权重-自己进货权重 (Type A)',
        '=占比×parent剩余进货额', '拆分需求重量',
        '=自己进货数量+拆分入数量', '=自己进货金额+拆分入金额',
        '=合计成本金额/消耗数量', 'BOM关系', 'A/B/C'
    ]

    output_path = f'/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/bom_split_result_v7_{business_date}.csv'
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerow(formulas)

        for sub_id, info in sorted_results:
            parents_str = ','.join(str(p) for p in info['parents']) if isinstance(info['parents'], list) else str(info['parents'])
            writer.writerow([
                sub_id, info['sub_name'], info['sale_qty'], info['sale_amt'], info['list_price'],
                info['know_lost_qty'], info['consume_qty'], info['consume_weight'],
                info['self_inbound_qty'], info['self_inbound_weight'], info['self_inbound_amt'],
                info['split_need_qty'], info['split_need_weight'],
                info['split_from_parent_qty'], info['split_from_parent_amt'],
                info['total_cost_qty'], info['total_cost_amt'],
                info['avg_cost'], parents_str, info['type']
            ])

    log.info(f"  CSV已保存: {output_path}")

    # ========== 汇总统计 ==========
    log.info(f"\n{'='*80}")
    log.info(f"【汇总统计】")
    log.info(f"{'='*80}")

    total_sale_qty = sum(info['sale_qty'] for info in results.values())
    total_sale_amt = sum(info['sale_amt'] for info in results.values())
    total_consume_qty = sum(info['consume_qty'] for info in results.values())
    total_self_inbound_amt = sum(info['self_inbound_amt'] for info in results.values())
    total_split_from_parent_amt = sum(info['split_from_parent_amt'] for info in results.values())
    total_cost_amt = sum(info['total_cost_amt'] for info in results.values())

    log.info(f"  SKU数量: {len(results)}")
    log.info(f"  销售数量合计: {total_sale_qty:.2f} kg")
    log.info(f"  销售额合计: {total_sale_amt:.2f} 元")
    log.info(f"  消耗数量合计: {total_consume_qty:.2f} kg")
    log.info(f"  自己进货金额合计: {total_self_inbound_amt:.2f} 元")
    log.info(f"  从parent拆分入金额合计: {total_split_from_parent_amt:.2f} 元")
    log.info(f"  合计成本金额合计: {total_cost_amt:.2f} 元")

    # 验证parent分配平衡
    total_parent_original = sum(p['original_amt'] for p in parent_info.values())
    total_parent_remaining = sum(p['remaining_amt'] for p in parent_info.values())
    log.info(f"\n  Parent原进货额合计: {total_parent_original:.2f} 元")
    log.info(f"  Parent剩余合计: {total_parent_remaining:.2f} 元")
    log.info(f"  已分配合计: {total_parent_original - total_parent_remaining:.2f} 元")

    log.info(f"\n  平衡验证: 合计成本金额({total_cost_amt:.2f}) ≈ Parent已分配({total_parent_original - total_parent_remaining:.2f})")

    log.info(f"\n{'='*100}")
    log.info(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'='*100}")


if __name__ == "__main__":
    main()