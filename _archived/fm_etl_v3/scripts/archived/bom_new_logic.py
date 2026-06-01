"""
重新实现BOM拆分逻辑 - 计算门店毛利

用户逻辑：
1. subsku当日拆分数量 = 销售数量 + 已知损耗数量
2. subsku当日拆分成本金额 = (sub拆分数量 × 销售原价) / SUM(所有sub拆分数量×销售原价) × parent进货额

门店毛利额 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存额 - 期初库存额)

测试：排骨 20500351, 21153037, 20003470
"""

from fm_etl_v3.connectors.api_connector import ApiConnector

# 测试参数
TEST_DATE = '2026-04-20'
TEST_STORE = 'A3XV'
TEST_ARTICLES = ['20500351', '21153037', '20003470']

# API
api = ApiConnector()

# 测试参数
TEST_DATE = '2026-04-20'
TEST_STORE = 'A3XV'
TEST_ARTICLES = ['20500351', '21153037', '20003470']


def fetch_data():
    """获取所有需要的原始数据"""
    print("=" * 70)
    print("【Step 1】获取原始数据")
    print("=" * 70)

    # 1. 销售数据
    print("\n>>> 查询销售数据 strategy_fm_sales_di")
    sales_sql = f"""
    SELECT
        store_id,
        business_date,
        abi_article_id,
        SUM(qty_spec) as sale_qty_kg,
        SUM(sales_amt) as sale_amt
    FROM strategy_fm_sales_di
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND abi_article_id IN ('20500351', '21153037', '20003470')
    GROUP BY store_id, business_date, abi_article_id
    """
    sales_df = api.query(sales_sql)
    sales = sales_df.to_dict('records')
    print(f"返回 {len(sales)} 行:")
    for row in sales:
        print(f"  {row}")

    # 2. 损耗数据
    print("\n>>> 查询损耗数据 strategy_fm_loss_di")
    loss_sql = f"""
    SELECT
        store_id,
        article_id,
        know_lost_qty,
        know_lost_amt
    FROM strategy_fm_loss_di
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND article_id IN ('20500351', '21153037', '20003470')
    """
    loss_df = api.query(loss_sql)
    loss = loss_df.to_dict('records')
    print(f"返回 {len(loss)} 行:")
    for row in loss:
        print(f"  {row}")

    # 3. 价格数据（销售原价）
    print("\n>>> 查询价格数据 strategy_fm_price_da")
    price_sql = f"""
    SELECT
        shop_id,
        sku_code,
        original_price
    FROM strategy_fm_price_da
    WHERE inc_day = '{TEST_DATE}'
        AND shop_id = '{TEST_STORE}'
        AND sku_code IN ('20500351', '21153037', '20003470')
    """
    price_df = api.query(price_sql)
    price = price_df.to_dict('records')
    print(f"返回 {len(price)} 行:")
    for row in price:
        print(f"  {row}")

    # 4. BOM关系（parent→sub）
    print("\n>>> 查询BOM关系 strategy_dim_store_article_bom_relation")
    bom_sql = f"""
    SELECT
        store_id,
        parent_article_id,
        sub_article_id,
        dressing_rate,
        cost_rate
    FROM strategy_dim_store_article_bom_relation
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND sub_article_id IN ('20500351', '21153037', '20003470')
    """
    bom_df = api.query(bom_sql)
    bom = bom_df.to_dict('records')
    print(f"返回 {len(bom)} 行:")
    for row in bom:
        print(f"  {row}")

    # 5. receive_sale数据（parent进货）
    print("\n>>> 查询进货拆分数据 strategy_fm_receive_sale_di")
    receive_sql = f"""
    SELECT
        store_id,
        article_id,
        sale_article_id,
        inbound_qty,
        inbound_amount
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{TEST_DATE}'
        AND store_id = '{TEST_STORE}'
        AND sale_article_id IN ('20500351', '21153037', '20003470')
    """
    receive_df = api.query(receive_sql)
    receive = receive_df.to_dict('records')
    print(f"返回 {len(receive)} 行:")
    for row in receive:
        print(f"  {row}")

    return {
        'sales': sales,
        'loss': loss,
        'price': price,
        'bom': bom,
        'receive': receive
    }


def calculate_bom(data: dict):
    """按用户逻辑计算BOM拆分"""
    print("\n" + "=" * 70)
    print("【Step 2】BOM拆分计算")
    print("=" * 70)

    sales = data['sales']
    loss = data['loss']
    price = data['price']
    bom = data['bom']
    receive = data['receive']

    # 如果没有BOM关系，说明是标品，不需要拆分
    if len(bom) == 0:
        print("⚠ 无BOM关系，这些商品可能是标品，直接用进货数据")

        # 标品逻辑：直接从receive获取
        for sub_id in TEST_ARTICLES:
            print(f"\n--- 商品 {sub_id} ---")

            # 找销售数据
            sale_info = [s for s in sales if s.get('abi_article_id') == sub_id]
            sale_qty = sale_info[0].get('sale_qty_kg', 0) if sale_info else 0
            sale_amt = sale_info[0].get('sale_amt', 0) if sale_info else 0
            print(f"  销售数量: {sale_qty} kg")
            print(f"  销售金额: {sale_amt} 元")

            # 找损耗数据
            loss_info = [l for l in loss if l.get('article_id') == sub_id]
            know_lost_qty = loss_info[0].get('know_lost_qty', 0) if loss_info else 0
            print(f"  已知损耗数量: {know_lost_qty} kg")

            # 找价格数据
            price_info = [p for p in price if p.get('sku_code') == sub_id]
            original_price = price_info[0].get('original_price', 0) if price_info else 0
            print(f"  销售原价: {original_price} 元/kg")

            # 找进货数据
            receive_info = [r for r in receive if r.get('sale_article_id') == sub_id]
            if receive_info:
                inbound_qty = receive_info[0].get('inbound_qty', 0)
                inbound_amt = receive_info[0].get('inbound_amount', 0)
                print(f"  进货数量: {inbound_qty} kg")
                print(f"  进货金额: {inbound_amt} 元")
            else:
                print("  ⚠ 无进货数据")

        return

    # 有BOM关系的情况
    print("\n>>> 找到BOM关系，开始拆分计算")

    # 按用户逻辑：
    # 1. subsku当日拆分数量 = 销售数量 + 已知损耗数量
    # 2. subsku当日拆分成本金额 = (sub拆分数量 × 销售原价) / SUM(所有sub拆分数量×销售原价) × parent进货额

    # 先找到所有parent
    parents = set()
    for b in bom:
        parents.add(b.get('parent_article_id'))

    print(f"\n涉及 {len(parents)} 个parent: {parents}")

    # 对每个parent处理
    for parent_id in parents:
        print(f"\n{'='*60}")
        print(f"Parent: {parent_id}")
        print("="*60)

        # 找到这个parent的所有sub
        parent_subs = [b for b in bom if b.get('parent_article_id') == parent_id]
        print(f"  该parent有 {len(parent_subs)} 个sub商品")

        # 获取parent进货数据
        parent_receive = [r for r in receive if r.get('article_id') == parent_id]
        if parent_receive:
            # 取第一条的inbound_qty和inbound_amount
            inbound_qty = parent_receive[0].get('inbound_qty', 0)
            inbound_amt = parent_receive[0].get('inbound_amount', 0)
        else:
            print("  ⚠ 该parent无进货数据，跳过")
            continue

        print(f"  Parent进货数量: {inbound_qty} kg")
        print(f"  Parent进货金额: {inbound_amt} 元")

        # 计算每个sub的拆分数量和权重
        sub_weights = []
        for sub_info in parent_subs:
            sub_id = sub_info.get('sub_article_id')

            # 销售数量
            sale_info = [s for s in sales if s.get('abi_article_id') == sub_id]
            sale_qty = sale_info[0].get('sale_qty_kg', 0) if sale_info else 0

            # 已知损耗数量
            loss_info = [l for l in loss if l.get('article_id') == sub_id]
            know_lost_qty = loss_info[0].get('know_lost_qty', 0) if loss_info else 0

            # 拆分数量 = 销售数量 + 已知损耗数量
            split_qty = float(sale_qty) + float(know_lost_qty)

            # 销售原价
            price_info = [p for p in price if p.get('sku_code') == sub_id]
            original_price = float(price_info[0].get('original_price', 0)) if price_info else 0

            # 权重 = 拆分数量 × 销售原价
            weight = split_qty * original_price

            sub_weights.append({
                'sub_id': sub_id,
                'sale_qty': float(sale_qty),
                'know_lost_qty': float(know_lost_qty),
                'split_qty': split_qty,
                'original_price': original_price,
                'weight': weight
            })

        # 计算权重总和
        total_weight = sum(s['weight'] for s in sub_weights)
        print(f"\n  各sub权重明细:")
        print(f"  {'sub_id':<12} {'sale_qty':<10} {'loss_qty':<10} {'split_qty':<10} {'price':<10} {'weight':<10}")
        for s in sub_weights:
            print(f"  {s['sub_id']:<12} {s['sale_qty']:<10.3f} {s['know_lost_qty']:<10.3f} {s['split_qty']:<10.3f} {s['original_price']:<10.2f} {s['weight']:<10.2f}")
        print(f"  权重总和: {total_weight:.2f}")

        # 计算分配金额
        print(f"\n  分配计算:")
        print(f"  {'sub_id':<12} {'alloc_ratio':<12} {'alloc_amt':<10}")
        for s in sub_weights:
            if total_weight > 0:
                alloc_ratio = s['weight'] / total_weight
            else:
                alloc_ratio = 0
            alloc_amt = alloc_ratio * float(inbound_amt)

            s['alloc_ratio'] = alloc_ratio
            s['alloc_amt'] = alloc_amt

            print(f"  {s['sub_id']:<12} {alloc_ratio:<12.4f} {alloc_amt:<10.2f}")

            # 存回data供后续使用
            s['parent_id'] = parent_id
            s['parent_inbound_amt'] = float(inbound_amt)

    # 保存计算结果
    data['bom_alloc'] = []
    for parent_id in parents:
        parent_subs = [b for b in bom if b.get('parent_article_id') == parent_id]
        parent_receive = [r for r in receive if r.get('article_id') == parent_id]
        if not parent_receive:
            continue
        inbound_amt = parent_receive[0].get('inbound_amount', 0)

        for sub_info in parent_subs:
            sub_id = sub_info.get('sub_article_id')

            # 从sub_weights找结果
            alloc = None
            for s in sub_weights:
                if s['sub_id'] == sub_id and s.get('parent_id') == parent_id:
                    alloc = s
                    break

            if alloc:
                data['bom_alloc'].append({
                    'parent_id': parent_id,
                    'sub_id': sub_id,
                    'split_qty': alloc['split_qty'],
                    'alloc_amt': alloc['alloc_amt'],
                    'alloc_ratio': alloc['alloc_ratio']
                })


def calculate_profit(data: dict):
    """计算门店毛利"""
    print("\n" + "=" * 70)
    print("【Step 3】门店毛利计算")
    print("=" * 70)

    sales = data['sales']
    bom_alloc = data.get('bom_alloc', [])

    print("\n公式: 门店毛利额 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存额 - 期初库存额)")

    if len(bom_alloc) == 0:
        # 标品逻辑
        print("\n⚠ 无BOM拆分结果，用标品逻辑")

        total_sale_amt = sum(float(s.get('sale_amt', 0)) for s in sales)
        print(f"\n总销售额: {total_sale_amt:.2f} 元")
        print("需要进货额、加工入出额、库存额数据来计算完整毛利")

    else:
        print("\n>>> 按sub汇总拆分结果:")
        sub_summary = {}
        for alloc in bom_alloc:
            sub_id = alloc['sub_id']
            if sub_id not in sub_summary:
                sub_summary[sub_id] = {
                    'total_alloc_amt': 0,
                    'parents': []
                }
            sub_summary[sub_id]['total_alloc_amt'] += alloc['alloc_amt']
            sub_summary[sub_id]['parents'].append({
                'parent_id': alloc['parent_id'],
                'alloc_amt': alloc['alloc_amt'],
                'alloc_ratio': alloc['alloc_ratio']
            })

        print(f"\n{'sub_id':<12} {'总拆分成本':<10} {'来源parent数':<10}")
        for sub_id, info in sub_summary.items():
            print(f"{sub_id:<12} {info['total_alloc_amt']:<10.2f} {len(info['parents']):<10}")

            # 如果有多个parent，计算平均成本
            if len(info['parents']) > 1:
                avg_cost = info['total_alloc_amt'] / len(info['parents'])
                print(f"  有多个parent，平均成本: {avg_cost:.2f}")

        # 计算销售额和毛利
        print("\n>>> 门店毛利计算:")
        for sub_id in sub_summary:
            sale_info = [s for s in sales if s.get('abi_article_id') == sub_id]
            sale_amt = float(sale_info[0].get('sale_amt', 0)) if sale_info else 0
            alloc_amt = sub_summary[sub_id]['total_alloc_amt']

            # 简化计算：销售额 - 拆分成本
            profit = sale_amt - alloc_amt
            print(f"\n商品 {sub_id}:")
            print(f"  销售额: {sale_amt:.2f}")
            print(f"  拆分成本: {alloc_amt:.2f}")
            print(f"  简化毛利: {profit:.2f}")


def main():
    print("BOM拆分新逻辑测试")
    print(f"日期: {TEST_DATE}")
    print(f"门店: {TEST_STORE}")
    print(f"商品: {TEST_ARTICLES}")

    # 1. 获取数据
    data = fetch_data()

    # 2. 计算BOM拆分
    calculate_bom(data)

    # 3. 计算毛利
    calculate_profit(data)

    print("\n" + "=" * 70)
    print("完成")
    print("=" * 70)


if __name__ == '__main__':
    main()