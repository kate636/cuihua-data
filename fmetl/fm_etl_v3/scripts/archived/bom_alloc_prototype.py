"""
BOM拆分原型脚本 - 重新实现BOM逻辑计算门店毛利

核心公式：
门店毛利额 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存额 - 期初库存额)

BOM拆分逻辑（用户自定义）：
1. subsku当日拆分数量 = 销售数量 + 已知损耗数量
2. subsku当日拆分成本金额 = (sub拆分数量 × 销售原价) / SUM(所有sub拆分数量×销售原价) × parent进货额

如果一个sub有多个parent：
- 各parent分别计算分配金额
- 多个parent的成本相加，算平均成本

测试用例：排骨 20500351, 21153037, 20003470
"""

import duckdb
import pandas as pd
from pathlib import Path
from fm_etl_v3.connectors.api_connector import ApiConnector
import os
from dotenv import load_dotenv

load_dotenv()

# 配置
DUCKDB_PATH = os.getenv('FM_DUCKDB_PATH', 'data/fm_etl_v3.duckdb')
QDM_ACCESS_KEY = os.getenv('QDM_ACCESS_KEY')
QDM_SECRET_KEY = os.getenv('QDM_SECRET_KEY')

# 测试参数
TEST_DATE = '2026-04-20'
TEST_STORE = 'A3XV'  # 广州滨江宏岸店
TEST_ARTICLES = ['20500351', '21153037', '20003470']  # 排骨示例


def fetch_raw_data(date: str, store_id: str, articles: list):
    """从API获取需要的原始数据"""
    api = ApiConnector()

    # 1. 销售数据 - 获取销售数量和销售原价
    sales_sql = f"""
    SELECT
        store_id,
        business_date,
        abi_article_id as sale_article_id,
        SUM(qty_spec) as sale_qty_kg,
        SUM(sales_amt) as sale_amt,
        AVG(list_price) as list_price_avg
    FROM strategy_fm_sales_di
    WHERE inc_day = '{date}'
        AND store_id = '{store_id}'
        AND abi_article_id IN ({','.join([f"'{a}'" for a in articles])})
    GROUP BY store_id, business_date, abi_article_id
    """

    # 2. 损耗数据 - 获取已知损耗数量
    loss_sql = f"""
    SELECT
        store_id,
        inc_day,
        article_id as sale_article_id,
        know_lost_qty,
        know_lost_amt
    FROM strategy_fm_loss_di
    WHERE inc_day = '{date}'
        AND store_id = '{store_id}'
        AND article_id IN ({','.join([f"'{a}'" for a in articles])})
    """

    # 3. 价格数据 - 获取销售原价
    price_sql = f"""
    SELECT
        shop_id as store_id,
        sku_code as sale_article_id,
        original_price
    FROM strategy_fm_price_da
    WHERE inc_day = '{date}'
        AND shop_id = '{store_id}'
        AND sku_code IN ({','.join([f"'{a}'" for a in articles])})
    """

    # 4. BOM关系 - 获取parent-sub关系
    bom_sql = f"""
    SELECT
        store_id,
        parent_article_id,
        sub_article_id,
        dressing_rate,
        cost_rate,
        category_level1_description
    FROM strategy_dim_store_article_bom_relation
    WHERE inc_day = '{date}'
        AND store_id = '{store_id}'
        AND sub_article_id IN ({','.join([f"'{a}'" for a in articles])})
    """

    # 5. receive_sale数据 - 获取parent进货量和进货额
    receive_sale_sql = f"""
    SELECT
        store_id,
        business_date,
        article_id as parent_article_id,
        sale_article_id,
        inbound_qty as parent_inbound_qty,
        inbound_amount as parent_inbound_amt,
        purchase_price as parent_purchase_price
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{date}'
        AND store_id = '{store_id}'
        AND sale_article_id IN ({','.join([f"'{a}'" for a in articles])})
    """

    print("Fetching data from API...")
    print("=" * 60)

    sales_df = api.query(sales_sql)
    print(f"销售数据: {len(sales_df)} rows")

    loss_df = api.query(loss_sql)
    print(f"损耗数据: {len(loss_df)} rows")

    price_df = api.query(price_sql)
    print(f"价格数据: {len(price_df)} rows")

    bom_df = api.query(bom_sql)
    print(f"BOM关系: {len(bom_df)} rows")

    receive_sale_df = api.query(receive_sale_sql)
    print(f"Receive_sale数据: {len(receive_sale_df)} rows")

    return {
        'sales': sales_df,
        'loss': loss_df,
        'price': price_df,
        'bom': bom_df,
        'receive_sale': receive_sale_df
    }


def debug_show_data(data: dict):
    """展示原始数据，方便理解"""
    print("\n" + "=" * 60)
    print("原始数据展示")
    print("=" * 60)

    for name, df in data.items():
        print(f"\n【{name}】")
        if len(df) > 0:
            print(df.to_string())
        else:
            print("无数据")


def calculate_bom_alloc_custom(data: dict, date: str, store_id: str):
    """
    用户自定义BOM拆分逻辑

    步骤：
    1. 合并销售+损耗 → subsku拆分数量
    2. 获取销售原价
    3. 找到该sub的所有parent
    4. 对每个parent，计算分配比例和分配金额
    5. 如果有多个parent，汇总计算平均成本
    """
    print("\n" + "=" * 60)
    print("开始BOM拆分计算（用户自定义逻辑）")
    print("=" * 60)

    sales_df = data['sales']
    loss_df = data['loss']
    price_df = data['price']
    bom_df = data['bom']
    receive_sale_df = data['receive_sale']

    # Step 1: 合成sub基础数据（销售+损耗）
    sub_base = sales_df.copy()
    if len(loss_df) > 0:
        sub_base = sub_base.merge(
            loss_df[['sale_article_id', 'know_lost_qty', 'know_lost_amt']],
            on='sale_article_id',
            how='left'
        )
    else:
        sub_base['know_lost_qty'] = 0
        sub_base['know_lost_amt'] = 0

    # 拆分数量 = 销售数量 + 已知损耗数量
    sub_base['split_qty'] = sub_base['sale_qty_kg'] + sub_base['know_lost_qty'].fillna(0)

    print("\n【Step 1】subsku基础数据（拆分数量）")
    print(sub_base.to_string())

    # Step 2: 获取销售原价
    if len(price_df) > 0:
        sub_base = sub_base.merge(
            price_df[['sale_article_id', 'original_price']],
            on='sale_article_id',
            how='left'
        )
    else:
        sub_base['original_price'] = sub_base['list_price_avg']

    print("\n【Step 2】加入销售原价")
    print(sub_base[['sale_article_id', 'split_qty', 'original_price']].to_string())

    # Step 3: 找到每个sub的所有parent
    if len(bom_df) == 0:
        print("警告：无BOM关系数据，无法拆分")
        return None

    sub_parents = bom_df[['parent_article_id', 'sub_article_id', 'dressing_rate', 'cost_rate']]
    sub_base = sub_base.merge(
        sub_parents.rename(columns={'sub_article_id': 'sale_article_id'}),
        on='sale_article_id',
        how='inner'  # 只保留有BOM关系的
    )

    print("\n【Step 3】加入BOM关系（parent）")
    print(sub_base[['sale_article_id', 'parent_article_id', 'dressing_rate', 'cost_rate']].to_string())

    # Step 4: 获取parent进货数据
    if len(receive_sale_df) > 0:
        parent_inbound = receive_sale_df.groupby('parent_article_id').agg({
            'parent_inbound_qty': 'sum',
            'parent_inbound_amt': 'sum',
            'parent_purchase_price': 'mean'
        }).reset_index()

        sub_base = sub_base.merge(parent_inbound, on='parent_article_id', how='left')

    print("\n【Step 4】加入parent进货数据")
    print(sub_base[['sale_article_id', 'parent_article_id', 'parent_inbound_qty', 'parent_inbound_amt']].to_string())

    # Step 5: 计算权重 (拆分数量 × 销售原价)
    sub_base['weight'] = sub_base['split_qty'] * sub_base['original_price']

    print("\n【Step 5】计算权重 (split_qty × original_price)")
    print(sub_base[['sale_article_id', 'parent_article_id', 'split_qty', 'original_price', 'weight']].to_string())

    # Step 6: 计算每个parent下所有sub的权重总和
    parent_weight_sum = sub_base.groupby('parent_article_id')['weight'].sum().reset_index()
    parent_weight_sum.columns = ['parent_article_id', 'parent_weight_sum']

    sub_base = sub_base.merge(parent_weight_sum, on='parent_article_id')

    print("\n【Step 6】parent权重总和")
    print(parent_weight_sum.to_string())

    # Step 7: 计算分配比例
    sub_base['alloc_ratio'] = sub_base['weight'] / sub_base['parent_weight_sum']

    print("\n【Step 7】分配比例 (weight / parent_weight_sum)")
    print(sub_base[['sale_article_id', 'parent_article_id', 'weight', 'parent_weight_sum', 'alloc_ratio']].to_string())

    # Step 8: 计算分配金额
    # 分配金额 = 分配比例 × parent进货额
    sub_base['alloc_amt'] = sub_base['alloc_ratio'] * sub_base['parent_inbound_amt']

    # 分配数量（按比例分配parent进货量）
    sub_base['alloc_qty'] = sub_base['alloc_ratio'] * sub_base['parent_inbound_qty']

    print("\n【Step 8】最终拆分结果")
    print(sub_base[['sale_article_id', 'parent_article_id', 'alloc_ratio', 'parent_inbound_amt', 'alloc_amt', 'alloc_qty']].to_string())

    return sub_base


def calculate_store_profit_custom(bom_alloc_df: pd.DataFrame, data: dict, date: str, store_id: str):
    """
    使用用户自定义BOM拆分结果计算门店毛利

    公式：门店毛利额 = 销售额 - (进货额 + 加工入额 - 加工出额) + (期末库存额 - 期初库存额)
    """
    print("\n" + "=" * 60)
    print("计算门店毛利额（库存方程）")
    print("=" * 60)

    if bom_alloc_df is None or len(bom_alloc_df) == 0:
        print("无BOM拆分数据，无法计算")
        return

    # 汇总sub级别数据
    sub_summary = bom_alloc_df.groupby('sale_article_id').agg({
        'sale_amt': 'sum',  # 销售额
        'alloc_amt': 'sum',  # 拆分成本金额（来自所有parent）
        'alloc_qty': 'sum',  # 拆分数量
        'sale_qty_kg': 'sum',
        'know_lost_amt': 'sum'
    }).reset_index()

    print("\n【sub级别汇总】")
    print(sub_summary.to_string())

    # 计算单位成本
    sub_summary['unit_cost'] = sub_summary['alloc_amt'] / sub_summary['alloc_qty']

    print("\n【单位成本】")
    print(sub_summary[['sale_article_id', 'alloc_amt', 'alloc_qty', 'unit_cost']].to_string())

    # 销售方程毛利
    sub_summary['profit_sales'] = sub_summary['sale_amt'] - sub_summary['sale_qty_kg'] * sub_summary['unit_cost'] - sub_summary['know_lost_amt']

    print("\n【门店毛利（销售方程）】")
    print(f"销售额: {sub_summary['sale_amt'].sum():.2f}")
    print(f"销售成本: {(sub_summary['sale_qty_kg'] * sub_summary['unit_cost']).sum():.2f}")
    print(f"已知损耗: {sub_summary['know_lost_amt'].sum():.2f}")
    print(f"毛利额: {sub_summary['profit_sales'].sum():.2f}")

    return sub_summary


def main():
    """主流程"""
    print("=" * 60)
    print("BOM拆分原型测试")
    print(f"日期: {TEST_DATE}")
    print(f"门店: {TEST_STORE}")
    print(f"测试商品: {TEST_ARTICLES}")
    print("=" * 60)

    # 1. 获取数据
    data = fetch_raw_data(TEST_DATE, TEST_STORE, TEST_ARTICLES)

    # 2. 展示原始数据
    debug_show_data(data)

    # 3. BOM拆分计算
    bom_alloc_df = calculate_bom_alloc_custom(data, TEST_DATE, TEST_STORE)

    # 4. 计算门店毛利
    profit_df = calculate_store_profit_custom(bom_alloc_df, data, TEST_DATE, TEST_STORE)

    print("\n" + "=" * 60)
    print("计算完成")
    print("=" * 60)


if __name__ == '__main__':
    main()