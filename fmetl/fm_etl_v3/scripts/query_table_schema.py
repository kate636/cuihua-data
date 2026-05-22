"""
查询 strategy_fm_* 表的完整字段信息
通过 QDM BI API 连接商分数据库，获取每张表的 DESC 结果
"""

import sys
sys.path.insert(0, '/Users/zhukate/Desktop/Projects/qdm/fm/翠花数据')

from fm_etl_v3.connectors.api_connector import ApiConnector
import pandas as pd

# 要查询的表列表（对应 hive表信息 中的序号）
TABLES = [
    # 序号 1: 销售
    ("strategy_fm_sales_di", 1),
    # 序号 2: 进货验收
    ("strategy_fm_purchase_di", 2),
    # 序号 3: SAP出入库
    ("strategy_fm_scm_di", 3),
    # 序号 4: SCM差异调整
    ("strategy_fm_scm_adjust_di", 4),
    # 序号 5: 损耗
    ("strategy_fm_loss_di", 5),
    # 序号 6: 加工转换
    ("strategy_fm_compose_di", 6),
    # 序号 7: 活动让利
    ("strategy_fm_allowance_di", 7),
    # 序号 8: 促销
    ("strategy_fm_promo_di", 8),
    # 序号 9: 成本价池
    ("strategy_fm_inventory_pool_di", 9),
    # 序号 10: 价格
    ("strategy_fm_price_da", 10),
    # 序号 12: 日清商品清单
    ("strategy_fm_dim_day_clear", 12),
    # 序号 13: 门店画像
    ("strategy_fm_dim_store_profile", 13),
    # 序号 14: 可售商品
    ("strategy_fm_dim_saleable", 14),
    # 序号 15: 商品主数据
    ("strategy_fm_dim_goods", 15),
    # 序号 16: 日历
    ("strategy_fm_dim_calendar", 16),
    # 序号 17: BOM收货销售关系
    ("strategy_fm_receive_sale_di", 17),
    # 序号 18: 订验关系
    ("strategy_fm_order_receive_di", 18),
    # 序号 19: 单位转换
    ("strategy_fm_dim_article_convert", 19),
    # 序号 20: BOM关系边
    ("strategy_dim_store_article_bom_relation", 20),
]

def query_table_desc(api: ApiConnector, table_name: str) -> pd.DataFrame:
    """查询表的字段详情"""
    sql = f"DESC default_catalog.ads_business_analysis.{table_name}"
    try:
        df = api.query(sql)
        return df
    except Exception as e:
        print(f"Error querying {table_name}: {e}")
        return pd.DataFrame()

def query_sample_data(api: ApiConnector, table_name: str, date: str = "2026-04-23") -> pd.DataFrame:
    """查询表的样本数据（1条）"""
    # 先检查表是否有 inc_day 分区
    sql = f"SELECT * FROM default_catalog.ads_business_analysis.{table_name} LIMIT 1"
    try:
        df = api.query(sql)
        return df
    except Exception as e:
        print(f"Error sampling {table_name}: {e}")
        return pd.DataFrame()

def main():
    api = ApiConnector()

    results = {}

    for table_name, seq in TABLES:
        print(f"\n{'='*60}")
        print(f"[{seq}] {table_name}")
        print(f"{'='*60}")

        # 查询 DESC
        desc_df = query_table_desc(api, table_name)
        if not desc_df.empty:
            print(f"\n字段数: {len(desc_df)}")
            print("\n字段列表:")
            for i, row in desc_df.iterrows():
                field = row.get('field', row.get('Field', ''))
                type_ = row.get('type', row.get('Type', ''))
                comment = row.get('comment', row.get('Comment', ''))
                print(f"  {i+1}. {field} | {type_} | {comment}")
            results[table_name] = desc_df

        # 查询样本数据
        sample_df = query_sample_data(api, table_name)
        if not sample_df.empty:
            print(f"\n样本数据列: {list(sample_df.columns)}")
            print(f"样本数据行数: {len(sample_df)}")

    return results

if __name__ == "__main__":
    results = main()