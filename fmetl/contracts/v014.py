from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RelationType(str, Enum):
    SAME_SKU = "SAME_SKU"
    BOM = "BOM"
    PROCESSING = "PROCESSING"
    EXPLICIT_CONVERT = "EXPLICIT_CONVERT"
    PRODUCT_GROUP_CANDIDATE = "PRODUCT_GROUP_CANDIDATE"
    UNRESOLVED = "UNRESOLVED"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class OutputField:
    name: str
    duckdb_type: str
    label: str


# Frozen from SHOW FULL COLUMNS on the v1.5 result table used by the isolated
# 2026-07-27 product-group experiment.  The first 123 fields are the v1.5
# compatibility contract; the final two are the v0.14 parallel group level.
_CONTRACT_TEXT = """
store_flag|VARCHAR|标签
store_no|VARCHAR|门店号
business_date|VARCHAR|日期
store_name|VARCHAR|门店名称
sku_id|VARCHAR|商品编码
category_name|VARCHAR|分类名称
category_level1_description|VARCHAR|大分类
category_level2_description|VARCHAR|中分类
category_level3_description|VARCHAR|小分类
level_description|VARCHAR|分类等级
day_clear|VARCHAR|day_clear
day_clear_flag|VARCHAR|非日清标识
operating_store_days|INTEGER|营业店日数
operating_store_count|INTEGER|营业店数
full_link_profit_amount|DECIMAL(18,4)|全链路毛利额
supply_chain_profit_amount|DECIMAL(18,4)|供应链毛利额
store_profit_amount|DECIMAL(18,4)|门店毛利额
full_link_profit_rate|DECIMAL(10,4)|全链路毛利率
supply_chain_profit_rate|DECIMAL(10,4)|供应链毛利率
store_profit_rate|DECIMAL(10,4)|门店毛利率
sales_weight|DECIMAL(18,4)|销售重量
sales_weight_before_19|DECIMAL(18,4)|19点前销售重量
total_sale_qty|DECIMAL(18,4)|销售数量
sale_qty_before_19|DECIMAL(18,4)|19点前销售数量
inbound_amount|DECIMAL(18,4)|进货额
total_sale_amount|DECIMAL(18,4)|全天销售额
total_customer_count|DECIMAL(18,4)|全天来客数
total_per_customer_transaction|DECIMAL(18,4)|全天客单价
sale_amount_before_19|DECIMAL(18,2)|19点前销售额
customer_count_before_19|DECIMAL(18,4)|19点前客数
per_customer_transaction_before_19|DECIMAL(18,4)|19点前客单价
per_item_price_before_19|DECIMAL(18,4)|19点前件单价
item_per_customer_before_19|DECIMAL(18,4)|19点前单件数
active_sku_count|DECIMAL(18,4)|动销sku数
supply_chain_expected_profit_rate|DECIMAL(10,4)|供应链预期毛利率
store_expected_profit_rate|DECIMAL(10,4)|门店预期毛利率
store_pricing_profit_rate|DECIMAL(10,4)|门店定价毛利率
purchase_price|DECIMAL(18,4)|采购价
average_selling_price|DECIMAL(18,4)|平均售价
supply_chain_discount_rate|DECIMAL(10,4)|供应链折让率
discount_rate|DECIMAL(10,4)|折扣率
promotional_discount_rate|DECIMAL(10,4)|促销折扣率
time_period_discount_rate|DECIMAL(10,4)|时段折扣率
loss_amount|DECIMAL(18,4)|损耗额
loss_rate|DECIMAL(18,4)|损耗率
return_rate|DECIMAL(10,4)|退货率
product_efficiency|DECIMAL(18,4)|品效
soldout_rate_16|DECIMAL(10,4)|售罄率16
soldout_rate_20|DECIMAL(10,4)|售罄率20
turnover_rate|DECIMAL(10,4)|周转率
sales_proportion_within_group|DECIMAL(10,4)|销售额占比_组内
sales_rank_in_middle_category|VARCHAR|销售额排名_中分类
sales_rank_in_large_category|VARCHAR|销售额排名_大分类
original_sale_amount|DECIMAL(18,4)|理论销售额
initial_inventory_amount|DECIMAL(18,4)|期初库存额
ending_inventory_amount|DECIMAL(18,4)|期末库存额
piece_sales_before_19|DECIMAL(18,4)|19点前销售件数
outbound_cost|DECIMAL(18,4)|出库成本
purchase_weight_amount|DECIMAL(18,4)|进货重量
supply_chain_promotion_amount|DECIMAL(18,4)|供应链促销额
discount_amount|DECIMAL(18,4)|折扣额
time_period_discount_amount|DECIMAL(18,4)|时段折扣额
promotional_discount_amount|DECIMAL(18,4)|促销折扣额
store_expected_profit_amount|DECIMAL(18,4)|门店预期毛利额
supply_chain_profit_rate_denominator|DECIMAL(18,4)|供应链毛利率_分母
supply_chain_expected_profit_rate_numerator|DECIMAL(18,4)|供应链预期毛利率_分子
supply_chain_expected_profit_rate_denominator|DECIMAL(18,4)|供应链预期毛利率_分母
store_pricing_profit_rate_numerator|DECIMAL(18,4)|门店定价毛利率_分子
supply_chain_discount_rate_denominator|DECIMAL(18,4)|供应链折让率_分母
loss_rate_denominator|DECIMAL(18,4)|损耗率_分母
return_rate_numerator|DECIMAL(18,4)|退货率_分子
return_rate_denominator|DECIMAL(18,4)|退货率_分母
lp_sale_amt|DECIMAL(18,4)|原价销售额
init_stock_qty|DECIMAL(18,4)|期初库存量
end_stock_qty|DECIMAL(18,4)|期末库存量
avg_7d_sale_qty|DECIMAL(18,4)|7天日均销售量
inbound_qty|DECIMAL(18,4)|进货数量
loss_rate_sales_amount|DECIMAL(18,4)|损耗率_销售额
loss_qty|DECIMAL(18,4)|损耗数量
loss_rate_qty|DECIMAL(18,4)|损耗率_数量
is_stock_sku|DECIMAL(18,4)|上架sku数
sale_article_num_cate|DECIMAL(18,4)|sku动销率
average_sales_original_price|DECIMAL(18,4)|平均销售原价
inbound_price|DECIMAL(18,4)|进货价
sale_piece_qty|DECIMAL(20,4)|销售件数
store_know_lost_amt|DECIMAL(20,4)|门店已知损耗金额
store_unknow_lost_amt|DECIMAL(20,4)|门店未知损耗金额
online_customer_count|DECIMAL(18,4)|线上客数
offline_customer_count|DECIMAL(18,4)|线下客数
jielong_customer_count|DECIMAL(18,4)|接龙客数
jsd_customer_count|DECIMAL(18,4)|及时达客数
online_sale_amt|DECIMAL(18,4)|线上销售额
offline_sale_amt|DECIMAL(18,4)|线下销售额
jielong_sale_amt|DECIMAL(18,4)|接龙销售额
jsd_sale_amt|DECIMAL(18,4)|及时达销售额
online_sale_qty|DECIMAL(18,4)|线上销售件数
offline_sale_qty|DECIMAL(18,4)|线下销售件数
jielong_sale_qty|DECIMAL(18,4)|接龙销售件数
jsd_sale_qty|DECIMAL(18,4)|及时达销售件数
online_per_customer|DECIMAL(18,4)|线上客单价
offline_per_customer|DECIMAL(18,4)|线下客单价
jielong_per_customer|DECIMAL(18,4)|接龙客单价
jsd_per_customer|DECIMAL(18,4)|及时达客单价
online_per_item|DECIMAL(18,4)|线上件单价
offline_per_item|DECIMAL(18,4)|线下件单价
jielong_per_item|DECIMAL(18,4)|接龙件单价
jsd_per_item|DECIMAL(18,4)|及时达件单价
online_item_per_customer|DECIMAL(18,4)|线上单件数
offline_item_per_customer|DECIMAL(18,4)|线下单件数
jielong_item_per_customer|DECIMAL(18,4)|接龙单件数
jsd_item_per_customer|DECIMAL(18,4)|及时达单件数
new_cust_num|DECIMAL(18,4)|新客客数
old_cust_num|DECIMAL(18,4)|老客客数
new_cust_sale_amt|DECIMAL(18,4)|新客销售额
old_cust_sale_amt|DECIMAL(18,4)|老客销售额
new_cust_sale_qty|DECIMAL(18,4)|新客销售件数
old_cust_sale_qty|DECIMAL(18,4)|老客销售件数
new_cust_per_customer|DECIMAL(18,4)|新客客单价
old_cust_per_customer|DECIMAL(18,4)|老客客单价
new_cust_per_item|DECIMAL(18,4)|新客件单价
old_cust_per_item|DECIMAL(18,4)|老客件单价
new_cust_item_per_customer|DECIMAL(18,4)|新客单件数
old_cust_item_per_customer|DECIMAL(18,4)|老客单件数
article_group_id|VARCHAR|商品集编码
article_group_name|VARCHAR|商品集名称
""".strip()


OUTPUT_CONTRACT = tuple(
    OutputField(*line.split("|", 2)) for line in _CONTRACT_TEXT.splitlines()
)
V15_COMPATIBLE_FIELDS = OUTPUT_CONTRACT[:123]
V014_EXTENSION_FIELDS = OUTPUT_CONTRACT[123:]

if len(V15_COMPATIBLE_FIELDS) != 123 or len(OUTPUT_CONTRACT) != 125:
    raise RuntimeError("v0.14 result contract must be 123 compatible + 2 extension fields")


RELATION_REGISTRY_COLUMNS = (
    "store_id", "business_date", "source_article_id", "target_article_id",
    "relation_type", "quantity_rate", "cost_rate", "posting_mode",
    "effective_from", "effective_to", "evidence_source", "relation_version", "status",
    "relation_id", "formal_flow_allowed",
)
