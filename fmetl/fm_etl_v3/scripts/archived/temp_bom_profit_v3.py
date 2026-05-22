"""
临时BOM拆分毛利计算脚本 v3（全链路重算版 - 修正版）

核心逻辑（用户指定）：
    1. subsku当日拆分数量 = 销售数量 + 已知损耗数量（全局取一次）
    2. subsku当日成本 = (拆分数量 × 销售原价) / Σ(该parent下所有subs的拆分数量 × 销售原价) × parent进货额
    3. 多parent场景：各自计算后相加
    4. 门店毛利 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存 - 期初库存)

数据来源：
    - strategy_fm_receive_sale_di: parent-sub关系, inbound_amount, inbound_qty
    - strategy_fm_sales_di: 销售数量(qty_spec), 销售原价(list_price), 销售额(sales_amt)
    - strategy_fm_loss_di: 已知损耗数量(know_lost_qty)
    - strategy_fm_compose_di: 加工入/出
    - strategy_fm_purchase_di: 期初/期末库存

输出：
    - parent视角：bom_split_out_amt (拆分出额)
    - sub视角：bom_split_in_amt (拆分入额)
    - SKU视角：receive_amt, 平均成本, 门店毛利
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
        category_level1_id,
        category_level1_description
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

    # 销售查找表（sub维度全局数据）
    sales_lookup = {}
    for r in sales_rows:
        key = (r.get('storeId', ''), r.get('articleId', ''))
        sales_lookup[key] = {
            'sale_qty': r.get('saleQty', 0) or 0,
            'sale_amt': r.get('saleAmt', 0) or 0,
            'list_price': r.get('listPrice', 0) or 0  # 直接取AVG(list_price)
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


def calculate_bom_split(rs_rows, sales_lookup, loss_lookup):
    """
    核心计算：BOM拆分数量和金额

    逻辑：
        subsku拆分数量 = 销售数量 + 已知损耗数量（全局取一次）
        subsku拆分成本 = (拆分数量 × 销售原价) / Σ(该parent下所有subs的拆分数量 × 销售原价) × parent进货额
    """

    # Step 1: 按 parent分组，收集所有subs
    parent_groups = {}
    for r in rs_rows:
        parent_id = r.get('parentId', '')
        sub_id = r.get('subId', '')
        store_id = r.get('storeId', '')
        parent_inbound_amt = r.get('parentInboundAmt', 0) or 0
        parent_inbound_qty = r.get('parentInboundQty', 0) or 0

        key = (store_id, parent_id)
        if key not in parent_groups:
            parent_groups[key] = {
                'parent_name': r.get('parentName', ''),
                'parent_inbound_amt': parent_inbound_amt,
                'parent_inbound_qty': parent_inbound_qty,
                'subs': []
            }
        # 避免重复添加同一个sub
        if sub_id not in [s['sub_id'] for s in parent_groups[key]['subs']]:
            parent_groups[key]['subs'].append({
                'sub_id': sub_id,
                'sub_name': r.get('subName', '')
            })

    # Step 2: 对每个parent，计算所有subs的拆分数量和成本
    results = []

    for (store_id, parent_id), pdata in parent_groups.items():
        parent_inbound_amt = pdata['parent_inbound_amt']

        # 计算该parent下每个sub的权重，以及总权重
        sub_data_list = []
        total_weight = 0

        for sub_info in pdata['subs']:
            sub_id = sub_info['sub_id']
            sub_name = sub_info['sub_name']

            # 获取sub的全局销售数据（只取一次）
            sale_data = sales_lookup.get((store_id, sub_id), {})
            sale_qty = sale_data.get('sale_qty', 0)
            sale_amt = sale_data.get('sale_amt', 0)
            list_price = sale_data.get('list_price', 0)

            # 获取sub的损耗数据
            loss_data = loss_lookup.get((store_id, sub_id), {})
            know_lost_qty = loss_data.get('know_lost_qty', 0)

            # 拆分数量 = 销售数量 + 已知损耗数量
            split_qty = sale_qty + know_lost_qty

            # 权重 = 拆分数量 × 销售原价
            weight = split_qty * list_price
            total_weight += weight

            sub_data_list.append({
                'sub_id': sub_id,
                'sub_name': sub_name,
                'sale_qty': sale_qty,
                'sale_amt': sale_amt,
                'list_price': list_price,
                'know_lost_qty': know_lost_qty,
                'split_qty': split_qty,
                'weight': weight
            })

        # Step 3: 按权重分配parent进货额
        parent_split_out_amt = 0  # 该parent的拆分出额合计
        for sub_data in sub_data_list:
            if total_weight > 0:
                cost_rate = sub_data['weight'] / total_weight
                split_amt = cost_rate * parent_inbound_amt
            else:
                # 无销售无损耗时，按subs数量平均分配
                split_amt = parent_inbound_amt / len(sub_data_list) if len(sub_data_list) > 0 else 0

            parent_split_out_amt += split_amt

            results.append({
                'store_id': store_id,
                'parent_id': parent_id,
                'parent_name': pdata['parent_name'],
                'parent_inbound_amt': parent_inbound_amt,
                'parent_inbound_qty': pdata['parent_inbound_qty'],
                'sub_id': sub_data['sub_id'],
                'sub_name': sub_data['sub_name'],
                'sale_qty': sub_data['sale_qty'],
                'sale_amt': sub_data['sale_amt'],
                'list_price': sub_data['list_price'],
                'know_lost_qty': sub_data['know_lost_qty'],
                'split_qty': sub_data['split_qty'],
                'weight': sub_data['weight'],
                'total_weight': total_weight,
                'cost_rate': cost_rate if total_weight > 0 else 0,
                'split_amt': split_amt  # 拆分成本金额
            })

    return results


def aggregate_to_sku(results, compose_lookup, stock_lookup):
    """聚合到SKU维度，计算进货额和毛利"""

    # 按sub_id聚合拆分入额
    sub_aggregated = {}
    for r in results:
        sub_id = r['sub_id']
        store_id = r['store_id']
        key = (store_id, sub_id)

        if key not in sub_aggregated:
            sub_aggregated[key] = {
                'sub_id': sub_id,
                'sub_name': r['sub_name'],
                'sale_qty': r['sale_qty'],  # 销售数据相同，直接取
                'sale_amt': r['sale_amt'],
                'bom_split_in_amt': 0,  # 拆分入额合计
                'split_qty': r['split_qty'],  # 拆分数量相同
                'parents': []
            }

        sub_aggregated[key]['bom_split_in_amt'] += r['split_amt']
        sub_aggregated[key]['parents'].append({
            'parent_id': r['parent_id'],
            'split_amt': r['split_amt']
        })

    # 添加加工和库存数据，计算毛利
    final_results = []
    for (store_id, sub_id), data in sub_aggregated.items():
        compose_data = compose_lookup.get((store_id, sub_id), {})
        stock_data = stock_lookup.get((store_id, sub_id), {})

        compose_in_amt = compose_data.get('compose_in_amt', 0)
        compose_out_amt = compose_data.get('compose_out_amt', 0)
        init_stock_amt = stock_data.get('init_stock_amt', 0)
        end_stock_amt = stock_data.get('end_stock_amt', 0)

        # 进货额 = 拆分入额（sub商品成本来自BOM拆分）
        receive_amt = data['bom_split_in_amt']

        # 门店毛利 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存 - 期初库存)
        store_profit = (
            data['sale_amt']
            - (receive_amt + compose_in_amt - compose_out_amt)
            + (end_stock_amt - init_stock_amt)
        )

        # 平均成本 = 拆分入额 / 拆分数量
        avg_cost = data['bom_split_in_amt'] / data['split_qty'] if data['split_qty'] > 0 else 0

        # 拆分出额（parent视角）- 这里用parents的数据
        parents_info = data['parents']

        final_results.append({
            'store_id': store_id,
            'sub_id': sub_id,
            'sub_name': data['sub_name'],
            'sale_qty': data['sale_qty'],
            'sale_amt': data['sale_amt'],
            'bom_split_in_amt': data['bom_split_in_amt'],
            'split_qty': data['split_qty'],
            'avg_cost': avg_cost,
            'parent_count': len(parents_info),
            'parents': ','.join([p['parent_id'] for p in parents_info]),
            'compose_in_amt': compose_in_amt,
            'compose_out_amt': compose_out_amt,
            'init_stock_amt': init_stock_amt,
            'end_stock_amt': end_stock_amt,
            'receive_amt': receive_amt,
            'store_profit': store_profit,
            'parents_detail': parents_info
        })

    return final_results


def print_detailed_calculation(results, final_results, sales_lookup, loss_lookup, example_sub_id='20003470'):
    """打印详细计算步骤，用排骨举例"""

    print("\n" + "="*100)
    print(f"详细计算步骤 - 排骨 ({example_sub_id})")
    print("="*100)

    # 找出排骨在final_results中的记录
    rib_final = None
    for r in final_results:
        if r['sub_id'] == example_sub_id:
            rib_final = r
            break

    if not rib_final:
        print(f"未找到排骨 {example_sub_id} 的数据")
        return

    store_id = rib_final['store_id']

    # 找出排骨相关的所有parent×sub拆分记录
    rib_records = [r for r in results if r['sub_id'] == example_sub_id]

    print("\n【Step 1: 获取排骨当天全局销售和损耗数据】")
    print("-"*80)
    print(f"  商品编码: {example_sub_id}")
    print(f"  商品名称: {rib_final['sub_name']}")
    print(f"  销售数量(sale_qty): {rib_final['sale_qty']:.2f} kg")
    print(f"  销售额(sale_amt): {rib_final['sale_amt']:.2f} 元")

    # 获取原价
    sale_data = sales_lookup.get((store_id, example_sub_id), {})
    list_price = sale_data.get('list_price', 0)
    print(f"  销售原价(list_price): {list_price:.2f} 元/kg")

    loss_data = loss_lookup.get((store_id, example_sub_id), {})
    know_lost_qty = loss_data.get('know_lost_qty', 0)
    print(f"  已知损耗(know_lost_qty): {know_lost_qty:.2f} kg")

    split_qty = rib_final['split_qty']
    print(f"  拆分数量 = 销售数量 + 已知损耗 = {rib_final['sale_qty']:.2f} + {know_lost_qty:.2f} = {split_qty:.2f} kg")
    weight = split_qty * list_price
    print(f"  权重 = 拆分数量 × 销售原价 = {split_qty:.2f} × {list_price:.2f} = {weight:.2f}")

    print("\n【Step 2: 获取排骨所属的Parent进货数据】")
    print("-"*80)
    print(f"  排骨从 {len(rib_records)} 个parent拆入:")
    for r in rib_records:
        print(f"    Parent {r['parent_id']} ({r['parent_name']}): 进货额={r['parent_inbound_amt']:.2f}元")

    print("\n【Step 3: 计算每个Parent下所有subs的权重合计】")
    print("-"*80)

    for r in rib_records:
        print(f"\n  Parent {r['parent_id']} ({r['parent_name']}):")
        print(f"    Parent进货额: {r['parent_inbound_amt']:.2f} 元")
        print(f"    排骨权重: {r['weight']:.2f}")
        print(f"    该parent下所有subs的总权重: {r['total_weight']:.2f}")
        print(f"    排骨成本占比 = 排骨权重 / 总权重 = {r['weight']:.2f} / {r['total_weight']:.2f} = {r['cost_rate']:.4f}")

    print("\n【Step 4: 计算拆分成本金额】")
    print("-"*80)

    total_split_amt = 0
    for r in rib_records:
        print(f"\n  Parent {r['parent_id']} 给排骨的拆分:")
        print(f"    拆分金额 = 成本占比 × Parent进货额")
        print(f"             = {r['cost_rate']:.4f} × {r['parent_inbound_amt']:.2f}")
        print(f"             = {r['split_amt']:.2f} 元")
        total_split_amt += r['split_amt']

    print("\n【Step 5: 排骨合计拆分入额】")
    print("-"*80)
    print(f"  拆分入额合计 = Σ(各parent的拆分金额)")
    for p in rib_final['parents_detail']:
        print(f"    {p['parent_id']}: {p['split_amt']:.2f} 元")
    print(f"  合计: {rib_final['bom_split_in_amt']:.2f} 元")
    print(f"  平均成本 = 拆分入额 / 拆分数量 = {rib_final['bom_split_in_amt']:.2f} / {split_qty:.2f} = {rib_final['avg_cost']:.2f} 元/kg")

    print("\n【Step 6: 计算门店毛利】")
    print("-"*80)
    print(f"  门店毛利 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存 - 期初库存)")
    print(f"           = {rib_final['sale_amt']:.2f} - ({rib_final['receive_amt']:.2f} + {rib_final['compose_in_amt']:.2f} - {rib_final['compose_out_amt']:.2f})")
    print(f"             + ({rib_final['end_stock_amt']:.2f} - {rib_final['init_stock_amt']:.2f})")
    print(f"           = {rib_final['store_profit']:.2f} 元")


def main():
    business_date = sys.argv[1] if len(sys.argv) > 1 else '2026-04-23'

    print(f"\n{'='*100}")
    print(f"BOM拆分毛利计算（全链路重算版 v3） - {business_date}")
    print(f"{'='*100}")

    cfg = get_settings()
    api = ApiConnector(cfg)

    # 获取数据
    rs_rows, sales_rows, loss_rows, compose_rows, purchase_rows = fetch_data(api, business_date)

    # 构建查找表
    sales_lookup, loss_lookup, compose_lookup, stock_lookup = build_lookup_tables(
        sales_rows, loss_rows, compose_rows, purchase_rows
    )

    # 计算BOM拆分
    print("\n6. 计算BOM拆分数量和成本")
    results = calculate_bom_split(rs_rows, sales_lookup, loss_lookup)
    print(f"   共 {len(results)} 条 parent×sub 拆分记录")

    # 聚合到SKU维度
    print("\n7. 聚合到SKU维度，计算毛利")
    final_results = aggregate_to_sku(results, compose_lookup, stock_lookup)
    print(f"   共 {len(final_results)} 个SKU")

    # 打印详细计算步骤（排骨举例）
    print_detailed_calculation(results, final_results, sales_lookup, loss_lookup, '20003470')

    # 打印完整字段列表
    print("\n" + "="*100)
    print("输出字段说明（SKU级汇总）")
    print("="*100)
    print("""
  字段名称               含义说明
  --------------------------------------------------------------------------------
  store_id               门店编码
  sub_id                 商品编码（销售条码）
  sub_name               商品名称
  sale_qty               销售数量(kg)
  sale_amt               销售额(元)
  split_qty              拆分数量 = 销售数量 + 已知损耗数量
  list_price             销售原价(元/kg)
  bom_split_in_amt       拆分入额(元) - 从parents分摊的成本合计
  avg_cost               平均成本(元/kg) = 拆分入额 / 拆分数量
  parent_count           parent数量
  parents                parent编码列表
  compose_in_amt         加工入额(元)
  compose_out_amt        加工出额(元)
  init_stock_amt         期初库存额(元)
  end_stock_amt          期末库存额(元)
  receive_amt            进货额(元) = 拆分入额（sub成本来自BOM拆分）
  store_profit           门店毛利(元) = 销售额 - (进货额 + 加工入 - 加工出) + (期末库存 - 期初库存)
    """)

    # 打印所有猪肉SKU数据（完整字段）
    print("\n" + "="*100)
    print("所有猪肉SKU数据（完整字段）")
    print("="*100)
    sorted_results = sorted(final_results, key=lambda x: x['sale_amt'], reverse=True)

    # 表头
    headers = ['sub_id', 'sub_name', 'sale_qty', 'sale_amt', 'split_qty', 'bom_split_in_amt',
               'avg_cost', 'parent_count', 'compose_in_amt', 'compose_out_amt',
               'init_stock_amt', 'end_stock_amt', 'receive_amt', 'store_profit', 'parents']
    print(' | '.join(headers))
    print('-'*140)

    for r in sorted_results:
        name = (r['sub_name'] or '')[:14]
        parents = (r['parents'] or '')[:20]
        row = [
            r['sub_id'],
            name,
            f"{r['sale_qty']:.2f}",
            f"{r['sale_amt']:.2f}",
            f"{r['split_qty']:.2f}",
            f"{r['bom_split_in_amt']:.2f}",
            f"{r['avg_cost']:.2f}",
            str(r['parent_count']),
            f"{r['compose_in_amt']:.2f}",
            f"{r['compose_out_amt']:.2f}",
            f"{r['init_stock_amt']:.2f}",
            f"{r['end_stock_amt']:.2f}",
            f"{r['receive_amt']:.2f}",
            f"{r['store_profit']:.2f}",
            parents
        ]
        print(' | '.join(row))

    print('-'*140)
    total_sale = sum(r['sale_amt'] for r in final_results)
    total_split_in = sum(r['bom_split_in_amt'] for r in final_results)
    total_profit = sum(r['store_profit'] for r in final_results)
    total_split_qty = sum(r['split_qty'] for r in final_results)
    print(f"合计: {len(final_results)}个SKU, 销售额={total_sale:.2f}, 拆分入额={total_split_in:.2f}, 拆分数量={total_split_qty:.2f}, 毛利={total_profit:.2f}")

    # 验证拆分入额=拆分出额
    print("\n【验证】")
    total_parent_inbound = sum(r['parent_inbound_amt'] for r in results if r['parent_id'] != r['sub_id'])  # 排除标品
    print(f"  Parent进货额合计(排除标品): {total_parent_inbound:.2f}")
    print(f"  Sub拆分入额合计: {total_split_in:.2f}")
    print(f"  差额: {total_parent_inbound - total_split_in:.2f}")

    print(f"\n{'='*100}")
    print("计算完成")
    print(f"{'='*100}")


if __name__ == "__main__":
    main()