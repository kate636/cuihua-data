"""
简单测试 - 获取排骨相关数据
"""
from fm_etl_v3.connectors.api_connector import ApiConnector

api = ApiConnector()

TEST_DATE = '2026-04-20'
TEST_STORE = 'A3XV'
TEST_ARTICLES = ['20500351', '21153037', '20003470']

# 测试1: 查询这些商品的销售数据
print("=" * 60)
print("测试1: 销售数据")
print("=" * 60)

sales_sql = f"""
SELECT
    store_id,
    business_date,
    abi_article_id,
    SUM(qty_spec) as sale_qty,
    SUM(sales_amt) as sale_amt,
    AVG(list_price) as list_price
FROM strategy_fm_sales_di
WHERE inc_day = '{TEST_DATE}'
    AND store_id = '{TEST_STORE}'
    AND abi_article_id IN ('20500351', '21153037', '20003470')
GROUP BY store_id, business_date, abi_article_id
"""

try:
    sales_df = api.query(sales_sql)
    print(f"返回 {len(sales_df)} rows")
    if len(sales_df) > 0:
        print(sales_df.to_string())
except Exception as e:
    print(f"Error: {e}")

# 测试2: BOM关系
print("\n" + "=" * 60)
print("测试2: BOM关系 (这些商品作为sub)")
print("=" * 60)

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

try:
    bom_df = api.query(bom_sql)
    print(f"返回 {len(bom_df)} rows")
    if len(bom_df) > 0:
        print(bom_df.to_string())
except Exception as e:
    print(f"Error: {e}")

# 测试3: receive_sale数据
print("\n" + "=" * 60)
print("测试3: receive_sale数据 (parent进货)")
print("=" * 60)

receive_sql = f"""
SELECT
    store_id,
    article_id,
    sale_article_id,
    inbound_qty,
    inbound_amount,
    purchase_price
FROM strategy_fm_receive_sale_di
WHERE inc_day = '{TEST_DATE}'
    AND store_id = '{TEST_STORE}'
    AND sale_article_id IN ('20500351', '21153037', '20003470')
"""

try:
    receive_df = api.query(receive_sql)
    print(f"返回 {len(receive_df)} rows")
    if len(receive_df) > 0:
        print(receive_df.to_string())
except Exception as e:
    print(f"Error: {e}")

# 测试4: 价格数据
print("\n" + "=" * 60)
print("测试4: 价格数据 (销售原价)")
print("=" * 60)

price_sql = f"""
SELECT
    shop_id,
    sku_code,
    original_price,
    current_price
FROM strategy_fm_price_da
WHERE inc_day = '{TEST_DATE}'
    AND shop_id = '{TEST_STORE}'
    AND sku_code IN ('20500351', '21153037', '20003470')
"""

try:
    price_df = api.query(price_sql)
    print(f"返回 {len(price_df)} rows")
    if len(price_df) > 0:
        print(price_df.to_string())
except Exception as e:
    print(f"Error: {e}")