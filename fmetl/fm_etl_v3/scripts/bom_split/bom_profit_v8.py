"""
BOM拆分成本计算 v8

核心修正：
1. parent固定剩余：使用Round层初始值，不动态更新
2. Σ总权重：预先计算，所有subs共用固定值
3. 占比分母：固定（Σ总权重），不递减
4. Type A+B：一起处理，共用同一层初始值
5. Type C：parents合并，一次处理

处理顺序：
    Round 1: Type A+B一起处理
    Round 2: Type C处理（parents合并）

公式：
    消耗数量 = 销售数量 + 已知损耗数量
    消耗权重 = 消耗数量 × 销售原价

    Type A:
        自己进货数量 = receive_sale_di.inbound_qty
        自己进货金额 = receive_sale_di.inbound_amount
        拆分需求重量 = 消耗数量 - 自己进货数量
        拆分需求权重 = 消耗权重 - 自己进货权重

    Type B/C:
        拆分需求重量 = 消耗数量
        拆分需求权重 = 消耗权重

    占比 = 拆分需求权重 / Σ总权重（固定分母）
    拆分入金额 = 占比 × parent固定剩余
    拆分入数量 = 拆分需求重量

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
    log_path = f'/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/bom_split_log_v8_{business_date}.txt'

    logger = logging.getLogger('bom_profit_v8')
    logger.setLevel(logging.INFO)

    # 清除已有的handlers
    logger.handlers.clear()

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
        inbound_amount AS parent_inbound_amt,
        inbound_qty AS parent_inbound_qty,
        sale_article_id AS sub_id,
        sale_article_name AS sub_name
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{business_date}'
      AND category_level1_id = '13'
    """
    rs_rows = api._fetch_all(sql_rs)
    log.info(f"  获取到 {len(rs_rows)} 条BOM关系记录")

    # 示例记录
    log.info(f"  示例记录:")
    for r in rs_rows[:3]:
        log.info(f"    parent={r.get('parentId')} ({r.get('parentName')}), sub={r.get('subId')} ({r.get('subName')})")

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

    log.info(f"  示例记录:")
    for r in sales_rows[:3]:
        log.info(f"    article_id={r.get('articleId')}, sale_qty={r.get('saleQty', 0):.2f}, sale_amt={r.get('saleAmt', 0):.2f}")

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

    # 损耗明细输出
    log.info(f"\n【损耗明细】:")
    for r in loss_rows:
        log.info(f"  article_id={r.get('articleId')}, know_lost_qty={r.get('knowLostQty', 0):.2f} kg")

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

    consume_qty = sale_qty + know_lost_qty
    consume_weight = consume_qty * list_price

    return {
        'sale_qty': sale_qty,
        'sale_amt': sale_amt,
        'list_price': list_price,
        'know_lost_qty': know_lost_qty,
        'consume_qty': consume_qty,
        'consume_weight': consume_weight
    }


def main():
    business_date = sys.argv[1] if len(sys.argv) > 1 else '2026-04-23'

    log = setup_logger(business_date)

    log.info(f"\n{'='*100}")
    log.info(f"BOM拆分成本计算 v8 - {business_date}")
    log.info(f"{'='*100}")
    log.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    cfg = get_settings()
    api = ApiConnector(cfg)

    rs_rows, sales_rows, loss_rows, sub_name_map = fetch_all_data(api, business_date, log)
    sales_lookup, loss_lookup = build_lookups(sales_rows, loss_rows)

    store_id = rs_rows[0].get('storeId', '') if rs_rows else ''

    # ========== 1.2 构建数据结构 ==========
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

    log.info(f"【数据结构】构建parent信息...")

    # parent信息（排除 parent_id == sub_id 的"自己进货给自己卖"记录）
    parent_info = {}
    # Type A的自己进货信息（parent_id == sub_id 的记录）
    self_inbound_info = {}

    log.info(f"\n【构建数据结构】")
    for r in rs_rows:
        parent_id = r.get('parentId', '')
        sub_id = r.get('subId', '')

        # parent_id == sub_id 的记录：自己进货给自己卖，存储为 Type A 的自己进货信息
        if parent_id == sub_id:
            self_inbound_info[sub_id] = {
                'name': r.get('parentName', ''),
                'self_inbound_qty': r.get('parentInboundQty', 0) or 0,
                'self_inbound_amt': r.get('parentInboundAmt', 0) or 0
            }
            continue

        # 真正的 parent -> sub 关系
        parent_name = r.get('parentName', '')
        parent_inbound_amt = r.get('parentInboundAmt', 0) or 0
        parent_inbound_qty = r.get('parentInboundQty', 0) or 0
        if parent_id not in parent_info:
            parent_info[parent_id] = {
                'name': parent_name,
                'original_amt': parent_inbound_amt,
                'original_qty': parent_inbound_qty,
                'subs': []
            }
        if sub_id not in parent_info[parent_id]['subs']:
            parent_info[parent_id]['subs'].append(sub_id)

    log.info(f"  真正的parent数量: {len(parent_info)} 个")
    log.info(f"  Type A自己进货记录数量: {len(self_inbound_info)} 个")
    if self_inbound_info:
        log.info(f"  Type A自己进货明细:")
        for sub_id, info in self_inbound_info.items():
            log.info(f"    {sub_id} ({info['name']}): {info['self_inbound_qty']:.2f} kg, {info['self_inbound_amt']:.2f} 元")

    # 分类
    # Type A: 有自己进货记录的 sub（在 self_inbound_info 中）
    # Type B: 纯 sub（无自己进货，单 parent）
    # Type C: 多 parent 的 sub（无自己进货）
    sub_ids = set(sub_parents.keys())
    type_a_set = set(self_inbound_info.keys())
    type_b = [s for s in sub_ids if len(sub_parents[s]) == 1 and s not in type_a_set]
    type_c = [s for s in sub_ids if len(sub_parents[s]) > 1 and s not in type_a_set]

    type_a = list(type_a_set)
    type_b_set = set(type_b)
    type_c_set = set(type_c)

    log.info(f"\n【分类统计】")
    log.info(f"  Type A (有自己进货): {len(type_a)} 个")
    log.info(f"  Type B (纯sub, 单parent): {len(type_b)} 个")
    log.info(f"  Type C (多parent的sub): {len(type_c)} 个")

    # ========== 1.3 计算所有subs的拆分需求权重 ==========
    log.info(f"\n【预先计算】所有subs的拆分需求权重...")

    all_split_need = {}  # {sub_id: {'weight': 拆分需求权重, 'qty': 拆分需求重量}}

    for sub_id in sub_ids:
        req = get_requirement(sub_id, sales_lookup, loss_lookup, store_id)
        consume_qty = req['consume_qty']
        consume_weight = req['consume_weight']
        list_price = req['list_price']

        if sub_id in type_a_set:
            # Type A: 有自己进货
            self_inbound_qty = self_inbound_info[sub_id]['self_inbound_qty']
            self_inbound_amt = self_inbound_info[sub_id]['self_inbound_amt']
            self_inbound_weight = self_inbound_qty * list_price

            split_need_qty = consume_qty - self_inbound_qty
            split_need_weight = consume_weight - self_inbound_weight
        else:
            # Type B/C: 无自己进货
            split_need_qty = consume_qty
            split_need_weight = consume_weight

        all_split_need[sub_id] = {
            'weight': split_need_weight,
            'qty': split_need_qty,
            'self_inbound_qty': self_inbound_info[sub_id]['self_inbound_qty'] if sub_id in type_a_set else 0,
            'self_inbound_amt': self_inbound_info[sub_id]['self_inbound_amt'] if sub_id in type_a_set else 0,
            'consume_qty': consume_qty,
            'consume_weight': consume_weight,
            'list_price': list_price,
            'sale_qty': req['sale_qty'],
            'sale_amt': req['sale_amt'],
            'know_lost_qty': req['know_lost_qty']
        }

    # ========== Round 1: Type A+B处理 ==========
    log.info(f"\n{'='*80}")
    log.info(f"【Round 1】处理 Type A+B")
    log.info(f"{'='*80}")

    # 2.1 预先计算（Round开始前）
    log.info(f"\n【预先计算】Round 1层固定值...")

    parent_round1_fixed = {}  # {parent_id: {'fixed_amt': 固定剩余金额, 'total_weight': Σ总权重, 'subs': [Type A+B subs]}}

    for parent_id, info in parent_info.items():
        # 找出parent下所有subs（包括 Type A+B 和 Type C）
        all_subs = info['subs']

        if not all_subs:
            continue

        # 固定剩余金额 = parent原进货额
        fixed_amt = info['original_amt']
        fixed_qty = info['original_qty']

        # Σ总权重 = parent下所有subs的拆分需求权重合计（包括 Type A+B 和 Type C）
        total_weight = sum(all_split_need[s]['weight'] for s in all_subs)
        total_qty = sum(all_split_need[s]['qty'] for s in all_subs)

        parent_round1_fixed[parent_id] = {
            'fixed_amt': fixed_amt,
            'fixed_qty': fixed_qty,
            'total_weight': total_weight,
            'total_qty': total_qty,
            'subs': all_subs
        }

        log.info(f"  parent {parent_id} ({info['name']}):")
        log.info(f"    固定剩余金额: {fixed_amt:.2f} 元 (parent原进货额)")
        log.info(f"    固定剩余数量: {fixed_qty:.2f} kg (parent原进货数量)")
        log.info(f"    所有subs数量: {len(all_subs)} 个 (Type A+B+Type C)")
        log.info(f"    Σ总权重计算过程 (包含所有subs):")
        weight_sum = 0
        qty_sum = 0
        for s in all_subs:
            sn = all_split_need[s]
            s_name = sub_name_map.get(s, '')
            s_type = 'A' if s in type_a_set else ('C' if s in type_c_set else 'B')
            consume_qty = sn['consume_qty']
            consume_weight = sn['consume_weight']
            split_need_weight = sn['weight']
            split_need_qty = sn['qty']
            list_price = sn['list_price']

            if s in type_a_set:
                # Type A: 显示消耗权重 - 自己进货权重 = 拆分需求权重
                self_inbound_qty = sn['self_inbound_qty']
                self_inbound_weight = self_inbound_qty * list_price
                log.info(f"      sub {s} ({s_name}) Type {s_type}:")
                log.info(f"        消耗数量: {consume_qty:.2f} kg, 消耗权重: {consume_weight:.2f} 元")
                log.info(f"        自己进货数量: {self_inbound_qty:.2f} kg, 自己进货权重: {self_inbound_weight:.2f} 元")
                log.info(f"        拆分需求重量 = {consume_qty:.2f} - {self_inbound_qty:.2f} = {split_need_qty:.2f} kg")
                log.info(f"        拆分需求权重 = {consume_weight:.2f} - {self_inbound_weight:.2f} = {split_need_weight:.2f} 元")
            else:
                # Type B/C: 消耗权重 = 拆分需求权重
                log.info(f"      sub {s} ({s_name}) Type {s_type}:")
                log.info(f"        消耗数量: {consume_qty:.2f} kg, 消耗权重: {consume_weight:.2f} 元")
                log.info(f"        拆分需求重量 = {consume_qty:.2f} kg (无自己进货)")
                log.info(f"        拆分需求权重 = {consume_weight:.2f} 元 (无自己进货)")

            weight_sum += split_need_weight
            qty_sum += split_need_qty
        log.info(f"    Σ总权重合计: {weight_sum:.2f} 元")
        log.info(f"    Σ总重量合计: {qty_sum:.2f} kg")

    # 存储结果
    results = {}

    # 累计分配（用于验证）
    total_split_amt = {}  # {parent_id: 累计分配金额}
    total_split_qty = {}  # {parent_id: 累计分配数量}

    # 处理循环（所有 subs 包括 Type A+B+C）
    log.info(f"\n【处理循环】所有 subs (Type A+B+C)...")

    all_subs_order = type_a + type_b + type_c
    for sub_id in all_subs_order:
        sub_name = sub_name_map.get(sub_id, '')
        sub_type = 'A' if sub_id in type_a_set else ('C' if sub_id in type_c_set else 'B')

        log.info(f"\n【处理】{sub_id} ({sub_name}) - Type {sub_type}")

        split_need = all_split_need[sub_id]
        sale_qty = split_need['sale_qty']
        sale_amt = split_need['sale_amt']
        know_lost_qty = split_need['know_lost_qty']
        list_price = split_need['list_price']
        consume_qty = split_need['consume_qty']
        consume_weight = split_need['consume_weight']
        split_need_weight = split_need['weight']
        split_need_qty = split_need['qty']
        self_inbound_qty = split_need['self_inbound_qty']
        self_inbound_amt = split_need['self_inbound_amt']

        # 详细计算过程
        log.info(f"  【基础数据】")
        log.info(f"    销售数量: {sale_qty:.2f} kg")
        log.info(f"    销售额: {sale_amt:.2f} 元")
        log.info(f"    销售原价: {list_price:.2f} 元/kg")
        log.info(f"    已知损耗数量: {know_lost_qty:.2f} kg")

        log.info(f"  【消耗计算】")
        log.info(f"    消耗数量 = 销售数量 + 已知损耗数量")
        log.info(f"    消耗数量 = {sale_qty:.2f} + {know_lost_qty:.2f} = {consume_qty:.2f} kg")
        log.info(f"    消耗权重 = 消耗数量 × 销售原价")
        log.info(f"    消耗权重 = {consume_qty:.2f} × {list_price:.2f} = {consume_weight:.2f} 元")

        if sub_id in type_a_set:
            self_inbound_weight = self_inbound_qty * list_price
            log.info(f"  【自己进货计算】(Type A专属)")
            log.info(f"    自己进货数量: {self_inbound_qty:.2f} kg")
            log.info(f"    自己进货金额: {self_inbound_amt:.2f} 元")
            log.info(f"    自己进货权重 = 自己进货数量 × 销售原价")
            log.info(f"    自己进货权重 = {self_inbound_qty:.2f} × {list_price:.2f} = {self_inbound_weight:.2f} 元")

        log.info(f"  【拆分需求计算】")
        if sub_id in type_a_set:
            log.info(f"    拆分需求重量 = 消耗数量 - 自己进货数量")
            log.info(f"    拆分需求重量 = {consume_qty:.2f} - {self_inbound_qty:.2f} = {split_need_qty:.2f} kg")
            log.info(f"    拆分需求权重 = 消耗权重 - 自己进货权重")
            log.info(f"    拆分需求权重 = {consume_weight:.2f} - {self_inbound_weight:.2f} = {split_need_weight:.2f} 元")
        else:
            log.info(f"    拆分需求重量 = 消耗数量 (Type B无自己进货)")
            log.info(f"    拆分需求重量 = {consume_qty:.2f} kg")
            log.info(f"    拆分需求权重 = 消耗权重 (Type B无自己进货)")
            log.info(f"    拆分需求权重 = {consume_weight:.2f} 元")

        # 从parent拆分
        amt_from_parent = 0
        qty_from_parent = 0

        for parent_id in sub_parents[sub_id]:
            if parent_id == sub_id:  # Type A自己的parent，跳过
                continue

            if parent_id not in parent_round1_fixed:
                continue

            fixed_amt = parent_round1_fixed[parent_id]['fixed_amt']
            fixed_qty = parent_round1_fixed[parent_id]['fixed_qty']
            total_weight = parent_round1_fixed[parent_id]['total_weight']

            log.info(f"  【从parent({parent_id})拆分】")
            log.info(f"    parent固定剩余金额: {fixed_amt:.2f} 元 (Round层初始值，所有subs共用)")
            log.info(f"    parent固定剩余数量: {fixed_qty:.2f} kg (Round层初始值，所有subs共用)")
            log.info(f"    Σ总权重: {total_weight:.2f} 元 (Round层预先计算，固定值)")

            # 占比 = 本sub拆分需求权重 / Σ总权重（固定分母）
            if total_weight > 0:
                ratio = split_need_weight / total_weight
            else:
                ratio = 0

            log.info(f"    占比 = 本sub拆分需求权重 / Σ总权重 (固定分母，不递减)")
            log.info(f"    占比 = {split_need_weight:.2f} / {total_weight:.2f} = {ratio:.4f}")

            # 拆分入金额 = 占比 × parent固定剩余
            split_in_amt = ratio * fixed_amt
            split_in_qty = split_need_qty

            log.info(f"    拆分入金额 = 占比 × parent固定剩余金额")
            log.info(f"    拆分入金额 = {ratio:.4f} × {fixed_amt:.2f} = {split_in_amt:.2f} 元")
            log.info(f"    拆分入数量 = 拆分需求重量")
            log.info(f"    拆分入数量 = {split_in_qty:.2f} kg")

            amt_from_parent += split_in_amt
            qty_from_parent += split_in_qty

            # 更新累计分配（用于验证）
            total_split_amt[parent_id] = total_split_amt.get(parent_id, 0) + split_in_amt
            total_split_qty[parent_id] = total_split_qty.get(parent_id, 0) + split_in_qty

        # 汇总
        total_cost_qty = self_inbound_qty + qty_from_parent
        total_cost_amt = self_inbound_amt + amt_from_parent
        avg_cost = total_cost_amt / consume_qty if consume_qty > 0 else 0

        log.info(f"  【成本汇总】")
        log.info(f"    合计成本数量 = 自己进货数量 + 拆分入数量")
        log.info(f"    合计成本数量 = {self_inbound_qty:.2f} + {qty_from_parent:.2f} = {total_cost_qty:.2f} kg")
        log.info(f"    合计成本金额 = 自己进货金额 + 拆分入金额")
        log.info(f"    合计成本金额 = {self_inbound_amt:.2f} + {amt_from_parent:.2f} = {total_cost_amt:.2f} 元")
        log.info(f"    平均成本 = 合计成本金额 / 消耗数量")
        log.info(f"    平均成本 = {total_cost_amt:.2f} / {consume_qty:.2f} = {avg_cost:.2f} 元/kg")

        results[sub_id] = {
            'sub_id': sub_id,
            'sub_name': sub_name,
            'sale_qty': all_split_need[sub_id]['sale_qty'],
            'sale_amt': all_split_need[sub_id]['sale_amt'],
            'list_price': list_price,
            'know_lost_qty': all_split_need[sub_id]['know_lost_qty'],
            'consume_qty': consume_qty,
            'consume_weight': split_need['consume_weight'],
            'self_inbound_qty': self_inbound_qty,
            'self_inbound_amt': self_inbound_amt,
            'split_need_qty': split_need_qty,
            'split_need_weight': split_need_weight,
            'split_from_parent_qty': qty_from_parent,
            'split_from_parent_amt': amt_from_parent,
            'total_cost_qty': total_cost_qty,
            'total_cost_amt': total_cost_amt,
            'avg_cost': avg_cost,
            'parents': [p for p in sub_parents[sub_id] if p != sub_id],
            'type': sub_type
        }

    # ========== 输出CSV ==========
    log.info(f"\n{'='*80}")
    log.info(f"【输出CSV】")
    log.info(f"{'='*80}")

    sorted_results = sorted(results.items(), key=lambda x: x[1]['sale_amt'], reverse=True)

    headers = [
        '商品编码', '商品名称', '销售数量(kg)', '销售额(元)', '销售原价(元/kg)', '已知损耗数量(kg)',
        '消耗数量(kg)', '消耗权重(元)',
        '自己进货数量(kg)', '自己进货金额(元)',
        '拆分需求重量(kg)', '拆分需求权重(元)',
        '从parent拆分入数量(kg)', '从parent拆分入金额(元)',
        '合计成本数量(kg)', '合计成本金额(元)',
        '平均成本(元/kg)', 'parents', '类型'
    ]

    formulas = [
        '【公式】',
        '', 'sales_di.qty_spec', 'sales_di.sales_amt', 'sales_di.list_price (AVG)', 'loss_di.know_lost_qty',
        '=销售数量+已知损耗', '=消耗数量×销售原价',
        'receive_sale_di (Type A)', 'receive_sale_di (Type A)',
        '=消耗数量-自己进货数量 (Type A)', '=消耗权重-自己进货权重 (Type A)',
        '=占比×parent固定剩余', '拆分需求重量',
        '=自己进货数量+拆分入数量', '=自己进货金额+拆分入金额',
        '=合计成本金额/消耗数量', 'BOM关系', 'A/B/C'
    ]

    output_path = f'/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据/data/bom_split_result_v8_{business_date}.csv'
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerow(formulas)

        for sub_id, info in sorted_results:
            parents_str = ','.join(str(p) for p in info['parents']) if isinstance(info['parents'], list) else str(info['parents'])
            writer.writerow([
                sub_id, info['sub_name'], info['sale_qty'], info['sale_amt'], info['list_price'],
                info['know_lost_qty'], info['consume_qty'], info['consume_weight'],
                info['self_inbound_qty'], info['self_inbound_amt'],
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

    # 验证
    # Parent原进货额合计 + Type A自己进货金额合计 = 合计成本金额合计
    total_parent_original = sum(p['original_amt'] for p in parent_info.values())
    total_self_inbound_original = sum(info['self_inbound_amt'] for info in self_inbound_info.values())
    total_source = total_parent_original + total_self_inbound_original

    balance_check = total_source - total_cost_amt

    log.info(f"\n  Parent原进货额合计: {total_parent_original:.2f} 元 (真正parent的进货)")
    log.info(f"  Type A自己进货金额合计: {total_self_inbound_original:.2f} 元")
    log.info(f"  总进货额合计: {total_source:.2f} 元")
    log.info(f"  合计成本金额合计: {total_cost_amt:.2f} 元")
    log.info(f"  差额(应为0): {balance_check:.2f} 元")

    if abs(balance_check) < 0.01:
        log.info(f"\n  ✓ 平衡验证通过: 合计成本金额({total_cost_amt:.2f}) = Parent原进货额({total_parent_original:.2f}) + Type A自己进货({total_self_inbound_original:.2f})")
    else:
        log.info(f"\n  ✗ 平衡验证失败: 差额 {balance_check:.2f} 元")

    log.info(f"\n{'='*100}")
    log.info(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'='*100}")


if __name__ == "__main__":
    main()