"""
临时BOM拆分毛利计算脚本（简化版 - 直接获取源数据）

用法：
    python -m fm_etl_v3.scripts.temp_bom_calc 2026-04-23

核心计算：
    1. 获取猪肉品类的BOM拆分事实 (receive_sale_di)
    2. 计算拆分入额和拆分出额
    3. 对比原公式和新公式毛利
"""

import sys
import pandas as pd
from fm_etl_v3.connectors import ApiConnector
from fm_etl_v3.config import get_settings


def main():
    business_date = sys.argv[1] if len(sys.argv) > 1 else '2026-04-23'

    print(f"\n{'='*60}")
    print(f"临时BOM拆分毛利计算 - {business_date}")
    print(f"{'='*60}\n")

    # 连接API
    cfg = get_settings()
    api = ApiConnector(cfg)

    # ==================== 1. 获取BOM拆分事实 ====================
    print("1. 获取猪肉品类 BOM拆分事实...")
    sql_receive_sale = f"""
    SELECT
        store_id,
        article_id AS parent_article_id,
        sale_article_id AS sub_article_id,
        spilit_sale_article_amt AS split_amt,
        inbound_amount AS parent_inbound_amt,
        sale_article_qty AS sub_qty,
        category_level1_id
    FROM strategy_fm_receive_sale_di
    WHERE inc_day = '{business_date}'
      AND category_level1_id = '13'
      AND spilit_sale_article_amt > 0
    """
    rs_df = api.query(sql_receive_sale)
    print(f"   共 {len(rs_df)} 条 BOM拆分记录")

    if len(rs_df) == 0:
        print("❌ 无猪肉品类BOM数据")
        return

    # 显示样本数据
    print("\n   BOM拆分样本（前10条）：")
    print(rs_df.head(10).to_string(index=False))

    # ==================== 2. 计算拆分入/出额 ====================
    print("\n2. 计算拆分入/出额...")

    # 拆分入额（sub视角）
    split_in = rs_df.groupby(['store_id', 'sub_article_id']).agg({
        'split_amt': 'sum'
    }).reset_index()
    split_in.columns = ['store_id', 'article_id', 'bom_split_in_amt']

    # 拆分出额（parent视角）
    split_out = rs_df.groupby(['store_id', 'parent_article_id']).agg({
        'split_amt': 'sum'
    }).reset_index()
    split_out.columns = ['store_id', 'article_id', 'bom_split_out_amt']

    # 合并
    bom_split = pd.merge(split_in, split_out, on=['store_id', 'article_id'], how='outer')

    # 显示拆分入额TOP
    print("\n   拆分入额TOP10（sub视角）：")
    print(split_in.nlargest(10, 'bom_split_in_amt').to_string(index=False))

    # 显示拆分出额TOP
    print("\n   拆分出额TOP10（parent视角）：")
    print(split_out.nlargest(10, 'bom_split_out_amt').to_string(index=False))

    # 平衡验证
    total_in = split_in['bom_split_in_amt'].sum()
    total_out = split_out['bom_split_out_amt'].sum()
    print(f"\n   平衡验证: 拆分出={total_out:.2f}, 拆分入={total_in:.2f}, 差额={total_out-total_in:.2f}")

    # ==================== 3. 获取销售+进货数据（简化版） ====================
    print("\n3. 获取销售+进货数据...")

    # 获取所有猪肉品类相关的article_id
    pork_articles = set(rs_df['parent_article_id'].unique()) | set(rs_df['sub_article_id'].unique())
    pork_articles_str = "','".join(pork_articles)

    # 获取进货数据（purchase_di 已经有预拆分的金额）
    sql_purchase = f"""
    SELECT
        store_id,
        sale_article_id AS article_id,
        SUM(init_stock_amt) AS init_stock_amt,
        SUM(end_stock_amt) AS end_stock_amt,
        SUM(sale_article_purchase_amt) AS receive_amt,
        SUM(sale_article_qty) AS receive_qty
    FROM strategy_fm_purchase_di
    WHERE inc_day = '{business_date}'
      AND sale_article_id IN ('{pork_articles_str}')
    GROUP BY store_id, sale_article_id
    """
    purchase_df = api.query(sql_purchase)
    print(f"   进货数据: {len(purchase_df)} 条")

    # 显示进货数据样本
    if len(purchase_df) > 0:
        print("\n   进货数据样本（前10条）：")
        print(purchase_df.head(10).to_string(index=False))

    # ==================== 4. 合并计算毛利 ====================
    print("\n4. 合并数据计算毛利...")

    # 以进货数据为主表（因为包含库存信息）
    if len(purchase_df) > 0:
        result = purchase_df.copy()
    else:
        # 如果没有进货数据，用 bom_split 作为主表
        result = bom_split.copy()
        result['init_stock_amt'] = 0
        result['end_stock_amt'] = 0
        result['receive_amt'] = 0
        result['receive_qty'] = 0

    # 合并BOM拆分
    result = result.merge(bom_split, on=['store_id', 'article_id'], how='outer')
    result = result.fillna(0)

    # 添加销售额（暂时从 receive_sale_di 推算）
    # 实际销售额需要从 sales_di 获取，这里简化处理
    sql_sales = f"""
    SELECT
        store_id,
        abi_article_id AS article_id,
        SUM(sales_amt) AS sale_amt,
        SUM(qty_spec) AS sale_qty
    FROM strategy_fm_sales_di
    WHERE inc_day = '{business_date}'
      AND abi_article_id IN ('{pork_articles_str}')
    GROUP BY store_id, abi_article_id
    """
    sales_df = api.query(sql_sales)
    print(f"   销售数据: {len(sales_df)} 条")

    if len(sales_df) > 0:
        result = result.merge(sales_df, on=['store_id', 'article_id'], how='outer')
        result = result.fillna(0)

    # ==================== 5. 计算毛利对比 ====================
    print("\n5. 计算毛利对比...")

    # 原公式（含损耗扣减）- 损耗暂时为0
    result['store_profit_old'] = (
        result['sale_amt']
        - result['receive_amt']
        + (result['end_stock_amt'] - result['init_stock_amt'])
        - 0  # know_lost_amt 暂时为0
    )

    # 新公式（含BOM拆分，不含损耗扣减）
    result['store_profit_new'] = (
        result['sale_amt']
        - (result['receive_amt'] + result['bom_split_in_amt'] - result['bom_split_out_amt'])
        + (result['end_stock_amt'] - result['init_stock_amt'])
    )

    # 差额（应该 = 损耗额）
    result['profit_diff'] = result['store_profit_old'] - result['store_profit_new']

    # 显示结果
    print("\n   毛利计算结果（前20条）：")
    cols = ['article_id', 'sale_amt', 'receive_amt', 'bom_split_in_amt', 'bom_split_out_amt',
            'init_stock_amt', 'end_stock_amt', 'store_profit_old', 'store_profit_new']
    print(result[cols].head(20).to_string(index=False))

    # ==================== 6. 汇总 ====================
    print("\n6. 猪肉品类毛利汇总：")

    summary = {
        '总销售额': result['sale_amt'].sum(),
        '总进货额': result['receive_amt'].sum(),
        '总拆分入': result['bom_split_in_amt'].sum(),
        '总拆分出': result['bom_split_out_amt'].sum(),
        '总库存变动': (result['end_stock_amt'] - result['init_stock_amt']).sum(),
        '原口径毛利合计': result['store_profit_old'].sum(),
        '新口径毛利合计': result['store_profit_new'].sum(),
        '差额(=损耗)': result['profit_diff'].sum(),
    }
    for k, v in summary.items():
        print(f"   {k}: {v:.2f}")

    # ==================== 7. 验证BOM层级抵消 ====================
    print("\n7. 验证BOM拆分项层级抵消：")
    print(f"   拆分出总额 = 拆分入总额 = {total_out:.2f}")
    print(f"   因此 BOM拆分项在 parent+subs 合计时自动抵消")
    print(f"   新公式毛利合计应与原公式接近（差额来自损耗扣减差异）")

    print(f"\n{'='*60}")
    print("计算完成")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()