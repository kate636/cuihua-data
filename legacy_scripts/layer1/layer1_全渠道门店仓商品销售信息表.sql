#!/bin/sh

source /opt/script/password/hive.sh  

# STARROCKS_HOST='10.254.2.249'
# STARROCKS_PORT=39033
# STARROCKS_USER='hive'
# STARROCKS_PASSWD='hive@qdama20240417.'

# 参数1 开始日期 
startdaystr=$1  

# 参数2 结束日期 
enddaystr=$2 

# 参数3 间隔天数
intervalday=$[$3-1]
intervalday2=$[${intervalday}+1]
echo "intervalday2:" $3

# 维度参数 昨天 -1d 
yesterday=$(date -d "1 day ago" +"%Y-%m-%d")  

# 起始日期 转格式 
startdate=`date -d "${startdaystr} +0 day" +%Y%m%d`
enddate=`date -d "${enddaystr} +0 day" +%Y%m%d`

echo "循环开始日期:startdate:"${startdate}
echo "循环结束日期:enddate:"${enddate}
echo "维度表日期:yesterday:"${yesterday}
echo "时间跨度:intervalday:"$3

while [ ${startdate} -le  ${enddate} ] 
do
 
# 开始日期 (循环的开始日期)  字符串 sql第一个参数
start_day=`date -d "${startdate}" +%Y-%m-%d`    

# 加间隔日期 作为第二个参数 
rq2date=`date -d "${startdate} +${intervalday} day" +%Y%m%d`

# 循环的结束日期  转格式
end_day=`date -d "${rq2date}" +%Y-%m-%d` # sq2 参数用

# 日期+1 天  
rq3=`date -d "${end_day}" +%Y%m%d`

echo "参数开始日期:" ${start_day}
echo "参数结束日期:" ${end_day}


if [ ${rq3} -ge ${enddate} ] 

then 
    echo "end_day:" ${end_day}
    echo "最后日期跨度超了限制的范围"
    end_day=`date -d "${enddate} +0 day" +%Y-%m-%d`
    rq3=`date -d "${end_day}" +%Y%m%d`
    echo "最后循环的开始日期为 start_day:" ${start_day}
    echo "最后循环结束日期调整为 end_day:" ${end_day}
    echo "最后循环结束日期调整为rq3:" ${rq3}
else 
    end_day=${end_day}  # 日期不改变
    echo "最后循环的开始日期为 end_day:" ${start_day}
    echo "最后日期满足要求，不发生改变，结束日期为:" ${end_day}
fi

startdate=`date -d "${startdate} +${intervalday2} day" +%Y%m%d`

sqltxt=$(cat << EOF

set catalog hive;


----####################################################################
----# 任务功能说明：全渠道门店仓商品销售信息表
----# 作者：魏彬
----# 修改记录
----# 版本     修改人				修改日期				修改说明
----# v1       魏彬                 2023-03-14             V1 迭代版本
----# v2       魏彬                 2023-04-12             新增 member_coupon_company_amt,promo_amt
----# v3      李娜迷                2024-01-05             修改含税成本逻辑
----# v4      曾维辉                2024-10-29             切换starrocks版本
----# v5      曾维辉                2024-12-05             增加business_flag 是否营业标识字段
----# v6      曾维辉                2024-12-10             增加指标：门店发起促销折扣额
----# v7      曾维辉                2025-01-07             增加指标：供应商承担赠品金额、供应链承担赠品金额
----# v8      曾维辉                2025-02-20             修改return_qty、return_amt字段逻辑,从供应链宽表出
----# v9      曾维辉                2025-03-21             统一business_flag字段逻辑
----# v10     曾维辉                2025-03-25             新增指标：期初库存金额、期末库存金额、期初库存数量、期末库存数量
----# v11     曾维辉                2025-04-08             新增7个字段
----# v12     曾维辉                2025-07-29             新增字段
----# v13     曾维辉                2025-08-06             新增字段
----# v14     曾维辉                2025-09-10             订购数量、金额字段切换门店商品销售促销宽表出数
----# v15     曾维辉                2025-10-27             新增指标:销售件数、19点前销售件数
----# v16     曾维辉                2025-12-09             pur_market_price市调原始价（SRM)字段逻辑调整
----####################################################################


drop table if exists hive.tmp_dal.dal_manage_full_link_dc_store_article_wide_temp_01 Force;
create table hive.tmp_dal.dal_manage_full_link_dc_store_article_wide_temp_01 as
select 
business_date          --业务日期
,new_dc_id             --仓id
,store_id              --门店id
,article_id            --商品id
,sum(p_lp_sub_amt) as p_lp_sub_amt   --原价销售额
,sum(online_lp_sale_amt) as online_lp_sale_amt   --线上_原价销售金额
,sum(offline_lp_sale_amt) as offline_lp_sale_amt   --线下_原价销售金额
,sum(bf19_lp_sale_amt) as bf19_lp_sale_amt   --19点前_原价销售金额
,sum(af19_lp_sale_amt) as af19_lp_sale_amt   --19点后_原价销售金额
,sum(bf19_orig_lp_sale_amt) as bf19_orig_lp_sale_amt   --19点前_按原价销售商品_原价销售金额
,sum(af19_orig_lp_sale_amt) as af19_orig_lp_sale_amt   --19点后_按原价销售商品_原价销售金额
,sum(bf19_non_orig_lp_sale_amt) as bf19_non_orig_lp_sale_amt   --19点前_非原价销售商品_原价销售金额
,sum(af19_non_orig_lp_sale_amt) as af19_non_orig_lp_sale_amt   --19点后_非原价销售商品_原价销售金额
,sum(sale_amt) as sale_amt   --销售金额
,sum(online_sale_amt) as online_sale_amt   --线上_销售金额
,sum(offline_sale_amt) as offline_sale_amt   --线下_销售金额
,sum(bf19_sale_amt) as bf19_sale_amt   --19点前_销售金额
,sum(af19_sale_amt) as af19_sale_amt   --19点后_销售金额
,sum(bf19_orig_sale_amt) as bf19_orig_sale_amt   --19点前_按原价销售商品_销售金额
,sum(af19_orig_sale_amt) as af19_orig_sale_amt   --19点后_按原价销售商品_销售金额
,sum(bf19_non_orig_sale_amt) as bf19_non_orig_sale_amt   --19点前_非原价销售商品_销售金额
,sum(af19_non_orig_sale_amt) as af19_non_orig_sale_amt   --19点后_非原价销售商品_销售金额
,sum(sale_qty) as sale_qty   --销售数量
,sum(online_sale_qty) as online_sale_qty   --线上_销售数量
,sum(offline_sale_qty) as offline_sale_qty   --线下_销售数量
,sum(bf19_sale_qty) as bf19_sale_qty   --19点前_销售数量
,sum(af19_sale_qty) as af19_sale_qty   --19点后_销售数量
,sum(bf19_orig_sale_qty) as bf19_orig_sale_qty   --19点前_按原价销售商品_销售数量
,sum(af19_orig_sale_qty) as af19_orig_sale_qty   --19点后_按原价销售商品_销售数量
,sum(bf19_non_orig_sale_qty) as bf19_non_orig_sale_qty   --19点前_非原价销售商品_销售数量
,sum(af19_non_orig_sale_qty) as af19_non_orig_sale_qty   --19点后_非原价销售商品_销售数量
,sum(sale_return_amt) as sale_return_amt   --退货金额
,sum(online_sale_return_amt) as online_sale_return_amt   --线上_退货金额
,sum(offline_sale_return_amt) as offline_sale_return_amt   --线下_退货金额
,sum(bf19_orig_return_amt) as bf19_orig_return_amt   --19点前_按原价销售商品_退货金额
,sum(af19_orig_return_amt) as af19_orig_return_amt   --19点后_按原价销售商品_退货金额
,sum(bf19_non_orig_return_amt) as bf19_non_orig_return_amt   --19点前_非原价销售商品_退货金额
,sum(af19_non_orig_return_amt) as af19_non_orig_return_amt   --19点后_非原价销售商品_退货金额
,sum(sale_return_qty) as sale_return_qty   --退货数量
,sum(online_sale_return_qty) as online_sale_return_qty   --线上_退货数量
,sum(offline_sale_return_qty) as offline_sale_return_qty   --线下_退货数量
,sum(bf19_orig_return_qty) as bf19_orig_return_qty   --19点前_按原价销售商品_退货数量
,sum(af19_orig_return_qty) as af19_orig_return_qty   --19点后_按原价销售商品_退货数量
,sum(bf19_non_orig_return_qty) as bf19_non_orig_return_qty   --19点前_非原价销售商品_退货数量
,sum(af19_non_orig_return_qty) as af19_non_orig_return_qty   --19点后_非原价销售商品_退货数量
,sum(actual_amount) as actual_amount   --实付金额
,sum(bf19_actual_amount) as bf19_actual_amount   --19点前_实付金额
,sum(af19_actual_amount) as af19_actual_amount   --19点后_实付金额
,sum(bf19_orig_actual_amount) as bf19_orig_actual_amount   --19点前_按原价销售商品_实付金额
,sum(af19_orig_actual_amount) as af19_orig_actual_amount   --19点后_按原价销售商品_实付金额
,sum(bf19_non_orig_actual_amount) as bf19_non_orig_actual_amount   --19点前_非原价销售商品_实付金额
,sum(af19_non_orig_actual_amount) as af19_non_orig_actual_amount   --19点后_非原价销售商品_实付金额
,sum(discount_amt) as discount_amt   --折扣金额
,sum(offline_discount_amt) as offline_discount_amt   --线上_折扣金额
,sum(online_discount_amt) as online_discount_amt   --线下_折扣金额
,sum(bf19_discount_amt) as bf19_discount_amt   --19点前_折扣金额
,sum(af19_discount_amt) as af19_discount_amt   --19点后_折扣金额
,sum(bf19_orig_discount_amt) as bf19_orig_discount_amt   --19点前_按原价销售商品_折扣金额
,sum(af19_orig_discount_amt) as af19_orig_discount_amt   --19点后_按原价销售商品_折扣金额
,sum(bf19_non_orig_discount_amt) as bf19_non_orig_discount_amt   --19点前_非原价销售商品_折扣金额
,sum(af19_non_orig_discount_amt) as af19_non_orig_discount_amt   --19点后_非原价销售商品_折扣金额
,sum(bf19_orig_sale_custs) as bf19_orig_sale_custs   --19点前_按原价销售商品_客流
,sum(bf19_non_orig_sale_custs) as bf19_non_orig_sale_custs   --19点后_非原价销售商品_客流
-- ,sum(store_order_qty) as store_order_qty   --订货数
-- ,sum(order_amt) as order_amt   --订货额
,sum(inbound_qty) as inbound_qty   --进货数
,sum(inbound_amount) as inbound_amount   --进货额
,sum(return_num) as return_num   --退货数量
,sum(return_amt) as return_amt   --退货额
,sum(total_cust_counts) as total_cust_counts   --客数
,max(sale_original_price) as sale_original_price   --销售原价
,sum(article_profit_amt) as article_profit_amt   --毛利额
,sum(member_discount_amt) as member_discount_amt   --会员折扣额
,sum(promotion_discount_amt) as promotion_discount_amt   --促销折扣额
,sum(hour_discount_amt) as hour_discount_amt   --时段折扣额
,sum(store_know_lost_qty) as store_know_lost_qty   --已知损耗数量
,sum(store_know_lost_amt) as store_know_lost_amt   --已知损耗额
,sum(store_unknow_lost_qty) as store_unknow_lost_qty   --未知损耗数量
,sum(store_unknow_lost_amt) as store_unknow_lost_amt   --未知损耗额
,sum(store_lost_amt) as store_lost_amt   --损耗额
,sum(store_lost_qty) as store_lost_qty   --门店总损耗数量
,sum(except_sales_amt) as except_sales_amt   --预期销售额
,sum(except_gross_profit) as except_gross_profit   --预期毛利额
,sum(except_discount_amt) as except_discount_amt   --预期折扣额
,sum(except_lost_amt) as except_lost_amt   --预期损耗额
,sum(gift_qty) as gift_qty   --赠品数量
,sum(allowance_amt) as allowance_amt   --补贴金额
,sum(purchase_weight) as purchase_weight   --按重量单位统计的进货重量
,sum(sales_weight) as sales_weight   --按重量单位统计的销售重量
,sum(sale_custs) as sale_custs   --客数
,sum(bf19_sale_custs) as bf19_sale_custs   --19点前客数
,sum(af19_sale_custs) as af19_sale_custs   --19点后客数
,sum(offline_bf19_sale_custs) as offline_bf19_sale_custs   --19点前线下订单数
,sum(offline_af19_sale_custs) as offline_af19_sale_custs   --19点后线下订单数
,sum(offline_sale_custs) as offline_sale_custs   --线下订单数
,sum(online_sale_custs) as online_sale_custs   --线上订单数
,sum(member_sale_custs) as member_sale_custs   --会员客数
,sum(bf19_member_sale_custs) as bf19_member_sale_custs   --19点后会员客数
,sum(af19_member_sale_custs) as af19_member_sale_custs   --19点前会员客数
,sum(return_custs) as return_custs   --退货客数
,sum(offline_return_custs) as offline_return_custs   --线下退货客数
,sum(online_return_custs) as online_return_custs   --线上退货客数
,sum(p_promo_amt) as p_promo_amt   --商品优惠金额
,sum(f_promo_amt) as f_promo_amt   --运费优惠金额
,sum(store_promotion_cost) as store_promotion_cost   --门店_营销成本
,sum(online_bear_cost) as online_bear_cost   --电商运营_营销成本分摊(113000)
,sum(market_bear_cost) as market_bear_cost   --市场部_营销成本分摊(106005)
,sum(store_bear_cost) as store_bear_cost   --门店_营销成本分摊(shop)
,sum(platform_bear_cost) as platform_bear_cost   --平台_营销成本分摊(platform)
,sum(service_bear_cost) as service_bear_cost   --客服部_营销成本分摊(121002)
,sum(bear_cost_123000) as bear_cost_123000   --品牌中心_营销成本分摊(123000)
,sum(supplier_bear_cost) as supplier_bear_cost   --供应商_营销成本分摊(999998)
,sum(promotion_cost) as promotion_cost   --促销扣款
,sum(other_bear_cost) as other_bear_cost   --其他_营销成本分摊
,sum(member_coupon_shop_amt) as member_coupon_shop_amt   --会员券(门店)
,sum(member_promo_amt) as member_promo_amt   --会员活动促销费
,sum(member_coupon_company_amt) as member_coupon_company_amt   --会员券费用(公司)
,sum(promo_amt) as promo_amt   --总会员券费用
,sum(out_stock_amt_cb_notax) as out_stock_amt_cb_notax   --门店商品维度出库成本
,sum(out_stock_pay_amt) as out_stock_pay_amt   --门店商品维度出库金额
,sum(return_stock_pay_amt) as return_stock_pay_amt   --门店商品维度门店退仓金额
,sum(return_stock_amt_cb_notax) as return_stock_amt_cb_notax   --门店商品维度门店退仓成本(不含税)
,sum(original_outstock_qty) as original_outstock_qty   --原价出库数量
,sum(original_outstock_amt) as original_outstock_amt   --原价出库金额
,sum(promotion_outstock_price) as promotion_outstock_price   --促销出库价
,sum(promotion_outstock_qty) as promotion_outstock_qty   --促销出库数量
,sum(promotion_outstock_amt) as promotion_outstock_amt   --促销出库金额
,sum(gift_outstock_qty) as gift_outstock_qty   --赠品出库数量
,sum(total_outstock_qty) as total_outstock_qty   --总出库数量
,sum(scm_promotion_cost) as scm_promotion_cost   --供应链出库让利金额
,sum(scm_return_promotion_cost) as scm_return_promotion_cost   --供应链让利费用金额
,sum(return_stock_qty) as return_stock_qty   --门店退仓量
,sum(out_stock_zzckj_amt) as out_stock_zzckj_amt   --出库原价金额
,sum(return_stock_original_amt) as return_stock_original_amt   --门店退仓原价金额
,sum(store_order_qty) as store_order_qty   --订购数量（订购单位）
,sum(order_amt) as order_amt   --订购金额
,sum(order_qty_payean) as order_qty_payean   --订购数量（结算单位）
,sum(adjustment_amt) as adjustment_amt   --调整金额
,sum(adjustment_amt_notax) as adjustment_amt_notax   --调整金额(不含税)
,sum(scm_promotion_qty_gift) as scm_promotion_qty_gift   --赠品出库让利数量
,sum(scm_promotion_amt_gift) as scm_promotion_amt_gift   --赠品出库让利金额
,sum(scm_promotion_amt) as scm_promotion_amt   --非赠品出库让利金额
,sum(scm_bear_amt) as scm_bear_amt   --供应链承担非赠品让利金额
,sum(vendor_bear_amt) as vendor_bear_amt   --供应商承担非赠品让利金额
,sum(business_market_bear_amt) as business_market_bear_amt   --运营市场承担非赠品让利金额
,sum(business_bear_amt) as business_bear_amt   --运营承担非赠品让利金额
,sum(market_bear_amt) as market_bear_amt   --市场承担非赠品让利金额
,sum(scm_promotion_amt_total) as scm_promotion_amt_total   --出库让利总额
,sum(miss_stock_qty) as miss_stock_qty   --少货数量
,sum(miss_stock_amt) as miss_stock_amt   --少货金额
,sum(out_stock_pay_amt_notax) as out_stock_pay_amt_notax -- 门店出库额(不含税)
,sum(return_stock_pay_amt_notax) as return_stock_pay_amt_notax --门店退仓额(不含税)
,sum(out_stock_amt_cb) as out_stock_amt_cb -- 门店出库成本
,sum(return_stock_amt_cb) as return_stock_amt_cb --门店退货成本
,sum(no_ordercoupon_company_promotion_amt) as no_ordercoupon_company_promotion_amt -- 公司承担非券的优惠金额    
,sum(ordercoupon_shop_promotion_amt      ) as ordercoupon_shop_promotion_amt       -- 门店承担优惠券的优惠金额(成本费用额(门店))
,sum(ordercoupon_company_promotion_amt   ) as ordercoupon_company_promotion_amt    -- 公司承担优惠券的优惠金额
,sum(shop_promotion_amt) as shop_promotion_amt   --门店发起活动促销额  zengweihui add 2024-12-10
,sum(vender_bear_gift_amt) as vender_bear_gift_amt -- 供应商承担赠品金额 zengweihui add 2025-01-07
,sum(scm_bear_gift_amt) as scm_bear_gift_amt       -- 供应链承担赠品金额 zengweihui add 2025-01-07
,sum(init_stock_amt) as init_stock_amt     -- 期初库存金额
,sum(end_stock_amt) as end_stock_amt       -- 期末库存金额
,sum(init_stock_qty) as init_stock_qty     -- 期初库存数量
,sum(end_stock_qty) as end_stock_qty       -- 期末库存数量
,sum(qdm_bear_negative_amt_total) as qdm_bear_negative_amt_total       -- 公司承担负让利总额 zengweihui 2025-04-08
,sum(qdm_bear_positive_amt_total) as qdm_bear_positive_amt_total       -- 公司承担让利总额 zengweihui 2025-04-08
,sum(qdm_bear_gift_qty) as qdm_bear_gift_qty       -- 公司承担赠品数量 zengweihui 2025-04-08
,sum(qdm_bear_gift_amt) as qdm_bear_gift_amt       -- 公司承担赠品金额 zengweihui 2025-04-08
,sum(qdm_bear_nogift_negative_amt) as qdm_bear_nogift_negative_amt       -- 公司承担非赠品负让利金额 zengweihui 2025-04-08
,sum(qdm_bear_nogift_positive_amt) as qdm_bear_nogift_positive_amt       -- 公司承担非赠品让利金额 zengweihui 2025-04-08
,sum(qdm_bear_promotion_fee) as qdm_bear_promotion_fee       -- 公司承担促销费用 zengweihui 2025-04-08
,sum(cost_price) as cost_price -- 进货成本价 zengweihui 2025-07-29
,sum(sale_cost_amt) as sale_cost_amt -- 销售成本 2025-07-29
,sum(avg_purchase_price) as avg_purchase_price -- 平均进货价 2025-07-29
,sum(pre_profit_amt) as pre_profit_amt -- 门店预期销售毛利额【原价销售额-销售数量*平均进货价】 2025-07-29
,sum(sale_profit_amt) as sale_profit_amt -- 门店毛利额(分析)【销售额-销售成本】 2025-07-29
,sum(vender_bear_gift_qty) as vender_bear_gift_qty -- 供应商承担赠品数量 zengweihui 2025-08-06
,sum(scm_bear_gift_qty) as scm_bear_gift_qty       -- 供应链承担赠品数量 zengweihui 2025-08-06
,sum(sale_piece_qty) as sale_piece_qty -- 销售件数 2025-10-27
,sum(bf19_sale_piece_qty) as bf19_sale_piece_qty -- 19点前销售件数 2025-10-27
,inc_day    --取营业日期为日分区
from (
select 
business_date              --业务日期
,new_dc_id              --仓id
,store_id              --门店id
,article_id              --商品id
,p_lp_sub_amt              --原价销售额
,online_lp_sale_amt              --线上_原价销售金额
,offline_lp_sale_amt              --线下_原价销售金额
,bf19_lp_sale_amt              --19点前_原价销售金额
,af19_lp_sale_amt              --19点后_原价销售金额
,bf19_orig_lp_sale_amt              --19点前_按原价销售商品_原价销售金额
,af19_orig_lp_sale_amt              --19点后_按原价销售商品_原价销售金额
,bf19_non_orig_lp_sale_amt              --19点前_非原价销售商品_原价销售金额
,af19_non_orig_lp_sale_amt              --19点后_非原价销售商品_原价销售金额
,sale_amt              --销售金额
,online_sale_amt              --线上_销售金额
,offline_sale_amt              --线下_销售金额
,bf19_sale_amt              --19点前_销售金额
,af19_sale_amt              --19点后_销售金额
,bf19_orig_sale_amt              --19点前_按原价销售商品_销售金额
,af19_orig_sale_amt              --19点后_按原价销售商品_销售金额
,bf19_non_orig_sale_amt              --19点前_非原价销售商品_销售金额
,af19_non_orig_sale_amt              --19点后_非原价销售商品_销售金额
,sale_qty              --销售数量
,online_sale_qty              --线上_销售数量
,offline_sale_qty              --线下_销售数量
,bf19_sale_qty              --19点前_销售数量
,af19_sale_qty              --19点后_销售数量
,bf19_orig_sale_qty              --19点前_按原价销售商品_销售数量
,af19_orig_sale_qty              --19点后_按原价销售商品_销售数量
,bf19_non_orig_sale_qty              --19点前_非原价销售商品_销售数量
,af19_non_orig_sale_qty              --19点后_非原价销售商品_销售数量
,sale_return_amt              --退货金额
,online_sale_return_amt              --线上_退货金额
,offline_sale_return_amt              --线下_退货金额
,bf19_orig_return_amt              --19点前_按原价销售商品_退货金额
,af19_orig_return_amt              --19点后_按原价销售商品_退货金额
,bf19_non_orig_return_amt              --19点前_非原价销售商品_退货金额
,af19_non_orig_return_amt              --19点后_非原价销售商品_退货金额
,sale_return_qty              --退货数量
,online_sale_return_qty              --线上_退货数量
,offline_sale_return_qty              --线下_退货数量
,bf19_orig_return_qty              --19点前_按原价销售商品_退货数量
,af19_orig_return_qty              --19点后_按原价销售商品_退货数量
,bf19_non_orig_return_qty              --19点前_非原价销售商品_退货数量
,af19_non_orig_return_qty              --19点后_非原价销售商品_退货数量
,actual_amount              --实付金额
,bf19_actual_amount              --19点前_实付金额
,af19_actual_amount              --19点后_实付金额
,bf19_orig_actual_amount              --19点前_按原价销售商品_实付金额
,af19_orig_actual_amount              --19点后_按原价销售商品_实付金额
,bf19_non_orig_actual_amount              --19点前_非原价销售商品_实付金额
,af19_non_orig_actual_amount              --19点后_非原价销售商品_实付金额
,discount_amt              --折扣金额
,offline_discount_amt              --线上_折扣金额
,online_discount_amt              --线下_折扣金额
,bf19_discount_amt              --19点前_折扣金额
,af19_discount_amt              --19点后_折扣金额
,bf19_orig_discount_amt              --19点前_按原价销售商品_折扣金额
,af19_orig_discount_amt              --19点后_按原价销售商品_折扣金额
,bf19_non_orig_discount_amt              --19点前_非原价销售商品_折扣金额
,af19_non_orig_discount_amt              --19点后_非原价销售商品_折扣金额
,bf19_orig_sale_custs              --19点前_按原价销售商品_客流
,bf19_non_orig_sale_custs              --19点后_非原价销售商品_客流
-- ,store_order_qty              --订货数
-- ,order_amt                    --订货额
,inbound_qty              --进货数
,inbound_amount              --进货额
-- ,return_num              --退货数量
-- ,return_amt              --退货额
,0 as return_num              --退货数量 zengweihui add 2025-02-20
,0 as return_amt              --退货额 zengweihui add 2025-02-20
,total_cust_counts              --客数
,sale_original_price              --销售原价
,article_profit_amt              --毛利额
,member_discount_amt              --会员折扣额
,promotion_discount_amt              --促销折扣额
,hour_discount_amt              --时段折扣额
,store_know_lost_qty              --已知损耗数量
,store_know_lost_amt              --已知损耗额
,store_unknow_lost_qty              --未知损耗数量
,store_unknow_lost_amt              --未知损耗额
,store_lost_amt              --损耗额
,store_lost_qty              --门店总损耗数量
,except_sales_amt              --预期销售额
,except_gross_profit              --预期毛利额
,except_discount_amt              --预期折扣额
,except_lost_amt              --预期损耗额
,gift_qty              --赠品数量
,allowance_amt              --补贴金额
,purchase_weight              --按重量单位统计的进货重量
,sales_weight              --按重量单位统计的销售重量
,sale_custs              --客数
,bf19_sale_custs              --19点前客数
,af19_sale_custs              --19点后客数
,offline_bf19_sale_custs              --19点前线下订单数
,offline_af19_sale_custs              --19点后线下订单数
,offline_sale_custs              --线下订单数
,online_sale_custs              --线上订单数
,member_sale_custs              --会员客数
,bf19_member_sale_custs              --19点后会员客数
,af19_member_sale_custs              --19点前会员客数
,return_custs              --退货客数
,offline_return_custs              --线下退货客数
,online_return_custs              --线上退货客数
,p_promo_amt              --商品优惠金额
,f_promo_amt              --运费优惠金额
,store_promotion_cost              --门店_营销成本
,online_bear_cost              --电商运营_营销成本分摊(113000)
,market_bear_cost              --市场部_营销成本分摊(106005)
,store_bear_cost              --门店_营销成本分摊(shop)
,platform_bear_cost              --平台_营销成本分摊(platform)
,service_bear_cost              --客服部_营销成本分摊(121002)
,bear_cost_123000              --品牌中心_营销成本分摊(123000)
,supplier_bear_cost              --供应商_营销成本分摊(999998)
,promotion_cost              --促销扣款
,other_bear_cost              --其他_营销成本分摊
,member_coupon_shop_amt              --会员券(门店)
,member_promo_amt              --会员活动促销费
,member_coupon_company_amt
,promo_amt
,no_ordercoupon_company_promotion_amt  -- 公司承担非券的优惠金额    
,ordercoupon_shop_promotion_amt        -- 门店承担优惠券的优惠金额(成本费用额(门店))
,ordercoupon_company_promotion_amt     -- 公司承担优惠券的优惠金额
,shop_promotion_amt                    -- 门店发起活动促销额  zengweihui add 2024-12-10 
,0 as out_stock_amt_cb_notax              --门店商品维度出库成本
,0 as out_stock_pay_amt              --门店商品维度出库金额
,0 as return_stock_pay_amt              --门店商品维度门店退仓金额
,0 as return_stock_amt_cb_notax              --门店商品维度门店退仓成本(不含税)
,0 as original_outstock_qty              --原价出库数量
,0 as original_outstock_amt              --原价出库金额
,0 as promotion_outstock_price              --促销出库价
,0 as promotion_outstock_qty              --促销出库数量
,0 as promotion_outstock_amt              --促销出库金额
,0 as gift_outstock_qty              --赠品出库数量
,0 as total_outstock_qty              --总出库数量
,0 as scm_promotion_cost              --供应链出库让利金额
,0 as scm_return_promotion_cost              --供应链让利费用金额
,0 as return_stock_qty              --门店退仓量
,0 as out_stock_zzckj_amt              --出库原价金额
,0 as return_stock_original_amt              --门店退仓原价金额
-- ,0 as store_order_qty              --订购数量（订购单位）
-- ,0 as order_amt              --订购金额
,store_order_qty              -- 订购数量（订购单位） zengweihui 2025-09-10
,order_amt              --订购金额 zengweihui 2025-09-10
,0 as order_qty_payean              --订购数量（结算单位）
,0 as adjustment_amt              --调整金额
,0 as adjustment_amt_notax              --调整金额(不含税)
,0 as scm_promotion_qty_gift              --赠品出库让利数量
,0 as scm_promotion_amt_gift              --赠品出库让利金额
,0 as scm_promotion_amt              --非赠品出库让利金额
,0 as scm_bear_amt              --供应链承担非赠品让利金额
,0 as vendor_bear_amt              --供应商承担非赠品让利金额
,0 as business_market_bear_amt              --运营市场承担非赠品让利金额
,0 as business_bear_amt              --运营承担非赠品让利金额
,0 as market_bear_amt              --市场承担非赠品让利金额
,0 as scm_promotion_amt_total              --出库让利总额
,0 as miss_stock_qty              --少货数量
,0 as miss_stock_amt              --少货金额
,0 as out_stock_pay_amt_notax -- 门店出库额(不含税)
,0 as return_stock_pay_amt_notax --门店退仓额(不含税)
,0 as out_stock_amt_cb
,0 as return_stock_amt_cb
,0 as vender_bear_gift_amt -- 供应商承担赠品金额 zengweihui add 2025-01-07
,0 as scm_bear_gift_amt    -- 供应链承担赠品金额 zengweihui add 2025-01-07
,init_stock_amt     -- 期初库存金额
,end_stock_amt      -- 期末库存金额
,init_stock_qty     -- 期初库存数量
,end_stock_qty      -- 期末库存数量
,0 as qdm_bear_negative_amt_total       -- 公司承担负让利总额 zengweihui 2025-04-08
,0 as qdm_bear_positive_amt_total       -- 公司承担让利总额 zengweihui 2025-04-08
,0 as qdm_bear_gift_qty       -- 公司承担赠品数量 zengweihui 2025-04-08
,0 as qdm_bear_gift_amt       -- 公司承担赠品金额 zengweihui 2025-04-08
,0 as qdm_bear_nogift_negative_amt       -- 公司承担非赠品负让利金额 zengweihui 2025-04-08
,0 as qdm_bear_nogift_positive_amt       -- 公司承担非赠品让利金额 zengweihui 2025-04-08
,0 as qdm_bear_promotion_fee       -- 公司承担促销费用 zengweihui 2025-04-08
,cost_price -- 进货成本价 zengweihui 2025-07-29
,sale_cost_amt -- 销售成本 2025-07-29
,avg_purchase_price -- 平均进货价 2025-07-29
,pre_profit_amt -- 门店预期销售毛利额【原价销售额-销售数量*平均进货价】 2025-07-29
,sale_profit_amt -- 门店毛利额(分析)【销售额-销售成本】 2025-07-29
,0 as vender_bear_gift_qty -- 供应商承担赠品数量 zengweihui 2025-08-06
,0 as scm_bear_gift_qty    -- 供应链承担赠品数量 zengweihui 2025-08-06
,sale_piece_qty -- 销售件数 2025-10-27
,bf19_sale_piece_qty -- 19点前销售件数 2025-10-27
,inc_day              --取营业日期为日分区
from dal_full_link.dal_manage_full_link_dc_store_article_sale_promo_info_di
where   inc_day between '$start_day' and '$end_day'

union all

select 
business_date              --业务日期
,new_dc_id              --仓id
,store_id              --门店id
,article_id              --商品id
,0 as p_lp_sub_amt              --原价销售额
,0 as online_lp_sale_amt              --线上_原价销售金额
,0 as offline_lp_sale_amt              --线下_原价销售金额
,0 as bf19_lp_sale_amt              --19点前_原价销售金额
,0 as af19_lp_sale_amt              --19点后_原价销售金额
,0 as bf19_orig_lp_sale_amt              --19点前_按原价销售商品_原价销售金额
,0 as af19_orig_lp_sale_amt              --19点后_按原价销售商品_原价销售金额
,0 as bf19_non_orig_lp_sale_amt              --19点前_非原价销售商品_原价销售金额
,0 as af19_non_orig_lp_sale_amt              --19点后_非原价销售商品_原价销售金额
,0 as sale_amt              --销售金额
,0 as online_sale_amt              --线上_销售金额
,0 as offline_sale_amt              --线下_销售金额
,0 as bf19_sale_amt              --19点前_销售金额
,0 as af19_sale_amt              --19点后_销售金额
,0 as bf19_orig_sale_amt              --19点前_按原价销售商品_销售金额
,0 as af19_orig_sale_amt              --19点后_按原价销售商品_销售金额
,0 as bf19_non_orig_sale_amt              --19点前_非原价销售商品_销售金额
,0 as af19_non_orig_sale_amt              --19点后_非原价销售商品_销售金额
,0 as sale_qty              --销售数量
,0 as online_sale_qty              --线上_销售数量
,0 as offline_sale_qty              --线下_销售数量
,0 as bf19_sale_qty              --19点前_销售数量
,0 as af19_sale_qty              --19点后_销售数量
,0 as bf19_orig_sale_qty              --19点前_按原价销售商品_销售数量
,0 as af19_orig_sale_qty              --19点后_按原价销售商品_销售数量
,0 as bf19_non_orig_sale_qty              --19点前_非原价销售商品_销售数量
,0 as af19_non_orig_sale_qty              --19点后_非原价销售商品_销售数量
,0 as sale_return_amt              --退货金额
,0 as online_sale_return_amt              --线上_退货金额
,0 as offline_sale_return_amt              --线下_退货金额
,0 as bf19_orig_return_amt              --19点前_按原价销售商品_退货金额
,0 as af19_orig_return_amt              --19点后_按原价销售商品_退货金额
,0 as bf19_non_orig_return_amt              --19点前_非原价销售商品_退货金额
,0 as af19_non_orig_return_amt              --19点后_非原价销售商品_退货金额
,0 as sale_return_qty              --退货数量
,0 as online_sale_return_qty              --线上_退货数量
,0 as offline_sale_return_qty              --线下_退货数量
,0 as bf19_orig_return_qty              --19点前_按原价销售商品_退货数量
,0 as af19_orig_return_qty              --19点后_按原价销售商品_退货数量
,0 as bf19_non_orig_return_qty              --19点前_非原价销售商品_退货数量
,0 as af19_non_orig_return_qty              --19点后_非原价销售商品_退货数量
,0 as actual_amount              --实付金额
,0 as bf19_actual_amount              --19点前_实付金额
,0 as af19_actual_amount              --19点后_实付金额
,0 as bf19_orig_actual_amount              --19点前_按原价销售商品_实付金额
,0 as af19_orig_actual_amount              --19点后_按原价销售商品_实付金额
,0 as bf19_non_orig_actual_amount              --19点前_非原价销售商品_实付金额
,0 as af19_non_orig_actual_amount              --19点后_非原价销售商品_实付金额
,0 as discount_amt              --折扣金额
,0 as offline_discount_amt              --线上_折扣金额
,0 as online_discount_amt              --线下_折扣金额
,0 as bf19_discount_amt              --19点前_折扣金额
,0 as af19_discount_amt              --19点后_折扣金额
,0 as bf19_orig_discount_amt              --19点前_按原价销售商品_折扣金额
,0 as af19_orig_discount_amt              --19点后_按原价销售商品_折扣金额
,0 as bf19_non_orig_discount_amt              --19点前_非原价销售商品_折扣金额
,0 as af19_non_orig_discount_amt              --19点后_非原价销售商品_折扣金额
,0 as bf19_orig_sale_custs              --19点前_按原价销售商品_客流
,0 as bf19_non_orig_sale_custs              --19点后_非原价销售商品_客流
-- ,0 as store_order_qty              --订货数
-- ,0 as order_amt              --订货额
,0 as inbound_qty              --进货数
,0 as inbound_amount              --进货额
-- ,0 as return_num              --退货数量
-- ,0 as return_amt              --退货额
,store_return_qty_shop as return_num              --退货数量 zengweihui add 2025-02-20
,store_return_amt_shop as return_amt              --退货额 zengweihui add 2025-02-20
,0 as total_cust_counts              --客数
,0 as sale_original_price              --销售原价
,0 as article_profit_amt              --毛利额
,0 as member_discount_amt              --会员折扣额
,0 as promotion_discount_amt              --促销折扣额
,0 as hour_discount_amt              --时段折扣额
,0 as store_know_lost_qty              --已知损耗数量
,0 as store_know_lost_amt              --已知损耗额
,0 as store_unknow_lost_qty              --未知损耗数量
,0 as store_unknow_lost_amt              --未知损耗额
,0 as store_lost_amt              --损耗额
,0 as store_lost_qty              --门店总损耗数量
,0 as except_sales_amt              --预期销售额
,0 as except_gross_profit              --预期毛利额
,0 as except_discount_amt              --预期折扣额
,0 as except_lost_amt              --预期损耗额
,0 as gift_qty              --赠品数量
,0 as allowance_amt              --补贴金额
,0 as purchase_weight              --按重量单位统计的进货重量
,0 as sales_weight              --按重量单位统计的销售重量
,0 as sale_custs              --客数
,0 as bf19_sale_custs              --19点前客数
,0 as af19_sale_custs              --19点后客数
,0 as offline_bf19_sale_custs              --19点前线下订单数
,0 as offline_af19_sale_custs              --19点后线下订单数
,0 as offline_sale_custs              --线下订单数
,0 as online_sale_custs              --线上订单数
,0 as member_sale_custs              --会员客数
,0 as bf19_member_sale_custs              --19点后会员客数
,0 as af19_member_sale_custs              --19点前会员客数
,0 as return_custs              --退货客数
,0 as offline_return_custs              --线下退货客数
,0 as online_return_custs              --线上退货客数
,0 as p_promo_amt              --商品优惠金额
,0 as f_promo_amt              --运费优惠金额
,0 as store_promotion_cost              --门店_营销成本
,0 as online_bear_cost              --电商运营_营销成本分摊(113000)
,0 as market_bear_cost              --市场部_营销成本分摊(106005)
,0 as store_bear_cost              --门店_营销成本分摊(shop)
,0 as platform_bear_cost              --平台_营销成本分摊(platform)
,0 as service_bear_cost              --客服部_营销成本分摊(121002)
,0 as bear_cost_123000              --品牌中心_营销成本分摊(123000)
,0 as supplier_bear_cost              --供应商_营销成本分摊(999998)
,0 as promotion_cost              --促销扣款
,0 as other_bear_cost              --其他_营销成本分摊
,0 as member_coupon_shop_amt              --会员券(门店)
,0 as member_promo_amt              --会员活动促销费
,0 as member_coupon_company_amt  --会员券费用(公司)
,0 as promo_amt  --总会员券费用
,0 as no_ordercoupon_company_promotion_amt  -- 公司承担非券的优惠金额    
,0 as ordercoupon_shop_promotion_amt        -- 门店承担优惠券的优惠金额(成本费用额(门店))
,0 as ordercoupon_company_promotion_amt     -- 公司承担优惠券的优惠金额
,0 as shop_promotion_amt                    -- 门店发起活动促销额  zengweihui add 2024-12-10 
,out_stock_amt_cb_notax              --门店商品维度出库成本
,out_stock_pay_amt              --门店商品维度出库金额
,return_stock_pay_amt              --门店商品维度门店退仓金额
,return_stock_amt_cb_notax              --门店商品维度门店退仓成本(不含税)
,original_outstock_qty              --原价出库数量
,original_outstock_amt              --原价出库金额
,promotion_outstock_price              --促销出库价
,promotion_outstock_qty              --促销出库数量
,promotion_outstock_amt              --促销出库金额
,gift_outstock_qty              --赠品出库数量
,total_outstock_qty              --总出库数量
,scm_promotion_cost              --供应链出库让利金额
,scm_return_promotion_cost              --供应链让利费用金额
,return_stock_qty              --门店退仓量
,out_stock_zzckj_amt              --出库原价金额
,return_stock_original_amt              --门店退仓原价金额
-- ,store_order_qty              --订购数量（订购单位）
-- ,order_amt              --订购金额
,0 as store_order_qty              --订购数量（订购单位） zengweihui 2025-09-10
,0 as order_amt              --订购金额 zengweihui 2025-09-10
,order_qty_payean              --订购数量（结算单位）
,adjustment_amt              --调整金额
,adjustment_amt_notax              --调整金额(不含税)
,scm_promotion_qty_gift              --赠品出库让利数量
,scm_promotion_amt_gift              --赠品出库让利金额
,scm_promotion_amt              --非赠品出库让利金额
,scm_bear_amt              --供应链承担非赠品让利金额
,vendor_bear_amt              --供应商承担非赠品让利金额
,business_market_bear_amt              --运营市场承担非赠品让利金额
,business_bear_amt              --运营承担非赠品让利金额
,market_bear_amt              --市场承担非赠品让利金额
,scm_promotion_amt_total              --出库让利总额
,miss_stock_qty              --少货数量
,miss_stock_amt              --少货金额
,out_stock_pay_amt_notax -- 门店出库额(不含税)
,return_stock_pay_amt_notax --门店退仓额(不含税)
,out_stock_amt_cb  --出库到店成本
,return_stock_amt_cb  --门店退货成本
,vender_bear_gift_amt -- 供应商承担赠品金额 zengweihui add 2025-01-07
,scm_bear_gift_amt    -- 供应链承担赠品金额 zengweihui add 2025-01-07
,0 as init_stock_amt     -- 期初库存金额
,0 as end_stock_amt      -- 期末库存金额
,0 as init_stock_qty     -- 期初库存数量
,0 as end_stock_qty      -- 期末库存数量
,qdm_bear_negative_amt_total       -- 公司承担负让利总额 zengweihui 2025-04-08
,qdm_bear_positive_amt_total       -- 公司承担让利总额 zengweihui 2025-04-08
,qdm_bear_gift_qty       -- 公司承担赠品数量 zengweihui 2025-04-08
,qdm_bear_gift_amt       -- 公司承担赠品金额 zengweihui 2025-04-08
,qdm_bear_nogift_negative_amt       -- 公司承担非赠品负让利金额 zengweihui 2025-04-08
,qdm_bear_nogift_positive_amt       -- 公司承担非赠品让利金额 zengweihui 2025-04-08
,qdm_bear_promotion_fee       -- 公司承担促销费用 zengweihui 2025-04-08
,0 as cost_price -- 进货成本价 zengweihui 2025-07-29
,0 as sale_cost_amt -- 销售成本 2025-07-29
,0 as avg_purchase_price -- 平均进货价 2025-07-29
,0 as pre_profit_amt -- 门店预期销售毛利额【原价销售额-销售数量*平均进货价】 2025-07-29
,0 as sale_profit_amt -- 门店毛利额(分析)【销售额-销售成本】 2025-07-29
,vender_bear_gift_qty -- 供应商承担赠品数量 zengweihui 2025-08-06
,scm_bear_gift_qty    -- 供应链承担赠品数量 zengweihui 2025-08-06
,0 as sale_piece_qty -- 销售件数 2025-10-27
,0 as bf19_sale_piece_qty -- 19点前销售件数 2025-10-27
,inc_day              --取营业日期为日分区
from dal_full_link.dal_manage_full_link_dc_store_article_scm_di
where   inc_day between '$start_day' and '$end_day'
) a1
group by 
business_date              --业务日期
,new_dc_id              --仓id
,store_id              --门店id
,article_id              --商品id
,inc_day              --取营业日期为日分区
;


drop table if exists hive.tmp_dal.dal_manage_full_link_dc_store_article_wide_temp_02 Force;
create table hive.tmp_dal.dal_manage_full_link_dc_store_article_wide_temp_02 as
select
a1.business_date              --业务日期
,a1.new_dc_id              --仓id
,a1.store_id              --门店id
,a1.article_id              --商品id
-- dim_store_article_price_info_da
,a2.current_price             --今日价格
,a2.yesterday_price             --昨日价格
,a2.dc_original_price             --出库原价格
,a2.dc_price             --出库价格
,a2.yesterday_dc_price             --昨日出库价格
,a2.original_price             --销售原价
,a6.pur_market_price 
-- dal_manage_price_strategy_store_article_info_di
,a3.strategy_no
,a3.strategy_name
,a3.price_level
,a3.price_zone 
,a3.strategy_category_id as strategy_category_level1_id
,a3.strategy_category_name as strategy_category_level1_name
,a1.p_lp_sub_amt              --原价销售额
,a1.online_lp_sale_amt              --线上_原价销售金额
,a1.offline_lp_sale_amt              --线下_原价销售金额
,a1.bf19_lp_sale_amt              --19点前_原价销售金额
,a1.af19_lp_sale_amt              --19点后_原价销售金额
,a1.bf19_orig_lp_sale_amt              --19点前_按原价销售商品_原价销售金额
,a1.af19_orig_lp_sale_amt              --19点后_按原价销售商品_原价销售金额
,a1.bf19_non_orig_lp_sale_amt              --19点前_非原价销售商品_原价销售金额
,a1.af19_non_orig_lp_sale_amt              --19点后_非原价销售商品_原价销售金额
,a1.sale_amt              --销售金额
,a1.online_sale_amt              --线上_销售金额
,a1.offline_sale_amt              --线下_销售金额
,a1.bf19_sale_amt              --19点前_销售金额
,a1.af19_sale_amt              --19点后_销售金额
,a1.bf19_orig_sale_amt              --19点前_按原价销售商品_销售金额
,a1.af19_orig_sale_amt              --19点后_按原价销售商品_销售金额
,a1.bf19_non_orig_sale_amt              --19点前_非原价销售商品_销售金额
,a1.af19_non_orig_sale_amt              --19点后_非原价销售商品_销售金额
,a1.sale_qty              --销售数量
,a1.online_sale_qty              --线上_销售数量
,a1.offline_sale_qty              --线下_销售数量
,a1.bf19_sale_qty              --19点前_销售数量
,a1.af19_sale_qty              --19点后_销售数量
,a1.bf19_orig_sale_qty              --19点前_按原价销售商品_销售数量
,a1.af19_orig_sale_qty              --19点后_按原价销售商品_销售数量
,a1.bf19_non_orig_sale_qty              --19点前_非原价销售商品_销售数量
,a1.af19_non_orig_sale_qty              --19点后_非原价销售商品_销售数量
,a1.sale_return_amt              --退货金额
,a1.online_sale_return_amt              --线上_退货金额
,a1.offline_sale_return_amt              --线下_退货金额
,a1.bf19_orig_return_amt              --19点前_按原价销售商品_退货金额
,a1.af19_orig_return_amt              --19点后_按原价销售商品_退货金额
,a1.bf19_non_orig_return_amt              --19点前_非原价销售商品_退货金额
,a1.af19_non_orig_return_amt              --19点后_非原价销售商品_退货金额
,a1.sale_return_qty              --退货数量
,a1.online_sale_return_qty              --线上_退货数量
,a1.offline_sale_return_qty              --线下_退货数量
,a1.bf19_orig_return_qty              --19点前_按原价销售商品_退货数量
,a1.af19_orig_return_qty              --19点后_按原价销售商品_退货数量
,a1.bf19_non_orig_return_qty              --19点前_非原价销售商品_退货数量
,a1.af19_non_orig_return_qty              --19点后_非原价销售商品_退货数量
,a1.actual_amount              --实付金额
,a1.bf19_actual_amount              --19点前_实付金额
,a1.af19_actual_amount              --19点后_实付金额
,a1.bf19_orig_actual_amount              --19点前_按原价销售商品_实付金额
,a1.af19_orig_actual_amount              --19点后_按原价销售商品_实付金额
,a1.bf19_non_orig_actual_amount              --19点前_非原价销售商品_实付金额
,a1.af19_non_orig_actual_amount              --19点后_非原价销售商品_实付金额
,a1.discount_amt              --折扣金额
,a1.offline_discount_amt              --线上_折扣金额
,a1.online_discount_amt              --线下_折扣金额
,a1.bf19_discount_amt              --19点前_折扣金额
,a1.af19_discount_amt              --19点后_折扣金额
,a1.bf19_orig_discount_amt              --19点前_按原价销售商品_折扣金额
,a1.af19_orig_discount_amt              --19点后_按原价销售商品_折扣金额
,a1.bf19_non_orig_discount_amt              --19点前_非原价销售商品_折扣金额
,a1.af19_non_orig_discount_amt              --19点后_非原价销售商品_折扣金额
,a1.bf19_orig_sale_custs              --19点前_按原价销售商品_客流
,a1.bf19_non_orig_sale_custs              --19点后_非原价销售商品_客流
-- ,a1.store_order_qty              --订货数
-- ,a1.order_amt              --订货额
,a1.inbound_qty              --进货数
,a1.inbound_amount              --进货额
,a1.return_num              --退货数量
,a1.return_amt              --退货额
,a1.total_cust_counts              --客数
,a1.sale_original_price              --销售原价
,a1.article_profit_amt              --毛利额
,a1.member_discount_amt              --会员折扣额
,a1.promotion_discount_amt              --促销折扣额
,a1.hour_discount_amt              --时段折扣额
,a1.store_know_lost_qty              --已知损耗数量
,a1.store_know_lost_amt              --已知损耗额
,a1.store_unknow_lost_qty              --未知损耗数量
,a1.store_unknow_lost_amt              --未知损耗额
,a1.store_lost_amt              --损耗额
,a1.store_lost_qty              --门店总损耗数量
,a1.except_sales_amt              --预期销售额
,a1.except_gross_profit              --预期毛利额
,a1.except_discount_amt              --预期折扣额
,a1.except_lost_amt              --预期损耗额
,a1.gift_qty              --赠品数量
,a1.allowance_amt              --补贴金额
,a1.purchase_weight              --按重量单位统计的进货重量
,a1.sales_weight              --按重量单位统计的销售重量
,a1.sale_custs              --客数
,a1.bf19_sale_custs              --19点前客数
,a1.af19_sale_custs              --19点后客数
,a1.offline_bf19_sale_custs              --19点前线下订单数
,a1.offline_af19_sale_custs              --19点后线下订单数
,a1.offline_sale_custs              --线下订单数
,a1.online_sale_custs              --线上订单数
,a1.member_sale_custs              --会员客数
,a1.bf19_member_sale_custs              --19点后会员客数
,a1.af19_member_sale_custs              --19点前会员客数
,a1.return_custs              --退货客数
,a1.offline_return_custs              --线下退货客数
,a1.online_return_custs              --线上退货客数
,a1.p_promo_amt              --商品优惠金额
,a1.f_promo_amt              --运费优惠金额
,a1.store_promotion_cost              --门店_营销成本
,a1.online_bear_cost              --电商运营_营销成本分摊(113000)
,a1.market_bear_cost              --市场部_营销成本分摊(106005)
,a1.store_bear_cost              --门店_营销成本分摊(shop)
,a1.platform_bear_cost              --平台_营销成本分摊(platform)
,a1.service_bear_cost              --客服部_营销成本分摊(121002)
,a1.bear_cost_123000              --品牌中心_营销成本分摊(123000)
,a1.supplier_bear_cost              --供应商_营销成本分摊(999998)
,a1.promotion_cost              --促销扣款
,a1.other_bear_cost              --其他_营销成本分摊
,a1.member_coupon_shop_amt              --会员券(门店)
,a1.member_promo_amt              --会员活动促销费
,a1.member_coupon_company_amt --会员券费用(公司)
,a1.promo_amt --总会员券费用
,a1.out_stock_amt_cb_notax              --门店商品维度出库成本
,a1.out_stock_pay_amt              --门店商品维度出库金额
,a1.return_stock_pay_amt              --门店商品维度门店退仓金额
,a1.return_stock_amt_cb_notax              --门店商品维度门店退仓成本(不含税)
,a1.original_outstock_qty              --原价出库数量
,a1.original_outstock_amt              --原价出库金额
,a1.promotion_outstock_price              --促销出库价
,a1.promotion_outstock_qty              --促销出库数量
,a1.promotion_outstock_amt              --促销出库金额
,a1.gift_outstock_qty              --赠品出库数量
,a1.total_outstock_qty              --总出库数量
,a1.scm_promotion_cost              --供应链出库让利金额
,a1.scm_return_promotion_cost              --供应链让利费用金额
,a1.return_stock_qty              --门店退仓量
,a1.out_stock_zzckj_amt              --出库原价金额
,a1.return_stock_original_amt              --门店退仓原价金额
,a1.store_order_qty              --订购数量（订购单位）
,a1.order_amt              --订购金额
,a1.order_qty_payean              --订购数量（结算单位）
,a1.adjustment_amt              --调整金额
,a1.adjustment_amt_notax              --调整金额(不含税)
,a1.scm_promotion_qty_gift              --赠品出库让利数量
,a1.scm_promotion_amt_gift              --赠品出库让利金额
,a1.scm_promotion_amt              --非赠品出库让利金额
,a1.scm_bear_amt              --供应链承担非赠品让利金额
,a1.vendor_bear_amt              --供应商承担非赠品让利金额
,a1.business_market_bear_amt              --运营市场承担非赠品让利金额
,a1.business_bear_amt              --运营承担非赠品让利金额
,a1.market_bear_amt              --市场承担非赠品让利金额
,a1.scm_promotion_amt_total              --出库让利总额
,a1.miss_stock_qty              --少货数量
,a1.miss_stock_amt              --少货金额
,a1.sale_amt/a1.sale_qty  as avg_sale_price
,case when a1.inbound_qty=0 then 0 else round( a1.inbound_amount / a1.inbound_qty,4) end as avg_inbound_price
,case when a1.bf19_sale_qty=0 then 0 else round( a1.bf19_sale_amt / a1.bf19_sale_qty,4) end as avg_bf19_sale_price
,case when a1.af19_sale_qty =0 then 0 else round( a1.af19_sale_amt / a1.af19_sale_qty,4) end as avg_af19_sale_price
,case when a1.offline_sale_qty=0 then 0 else round( a1.offline_sale_amt / a1.offline_sale_qty,4) end as avg_offline_sale_price
,case when a1.online_sale_qty=0 then 0 else round( a1.online_sale_amt / a1.online_sale_qty,0) end as avg_online_sale_price
,case when a1.original_outstock_qty=0 then 0 else  a1.original_outstock_amt / a1.original_outstock_qty end as avg_orig_outstock_price --出库原价
,case when a1.bf19_non_orig_sale_qty=0 then 0 else  a1.bf19_non_orig_sale_amt / a1.bf19_non_orig_sale_qty end as avg_bf19_non_orig_price --出库原价
,scm_promotion_amt_gift as gift_outstock_amt       --赠品出库理论金额  ,  --20230628修改
,out_stock_amt_cb as out_stock_amt_cb
,return_stock_amt_cb as return_stock_amt_cb
,a1.sale_qty * coalesce(a2.current_price,0)  as finally_total_sale_amt
,0 as bf19_orig_profit
,0 as bf19_non_orig_profit
,0 as af19_sale_profit
,0 as brand_center_bear_cost
,coalesce(a1.out_stock_pay_amt,0) + coalesce(a1.scm_promotion_amt_total,0) as expect_outstock_amt     -- 预期出库金额  仓毛利未分摊 暂时为0
,coalesce(a1.out_stock_pay_amt,0) + coalesce(a1.scm_promotion_amt_total,0) - coalesce(out_stock_amt_cb,0) 
as dc_article_expect_profit	       -- 供应链商品预期毛利额         总出库原价金额-总出库成本+仓总损耗金额 (总出库原价金额=总出库金额+供应链出库让利金额)     仓损耗金额未分割到门店仓商品维度  暂时置为0
,coalesce(a1.out_stock_pay_amt,0) - abs(coalesce(a1.return_stock_pay_amt,0)) - coalesce(out_stock_amt_cb,0) + abs(coalesce(return_stock_amt_cb,0)) 
as dc_out_profit  --  仓出库毛利额
,a1.p_lp_sub_amt + a1.store_lost_amt - a1.inbound_amount + coalesce(a1.out_stock_pay_amt,0) - abs(coalesce(a1.return_stock_pay_amt,0)) - coalesce(out_stock_amt_cb,0) + abs(coalesce(return_stock_amt_cb,0))
as fulllink_article_expect_profit    -- 全链路商品预期毛利额 全链路商品预期毛利额=门店预期毛利额+供应链商品毛利额+仓总损耗金额 (其中：门店预期毛利额=总商品原价金额+损耗金额-门店进货金额) 仓损耗金额未分割到门店仓商品维度  暂时置为0
,coalesce(a1.article_profit_amt,0) + coalesce(a1.out_stock_pay_amt_notax,0) - abs(coalesce(a1.return_stock_pay_amt_notax,0)) - coalesce(out_stock_amt_cb_notax,0) + abs(coalesce(return_stock_amt_cb_notax,0)) 
as full_link_article_profit  --  全链路商品毛利额
,coalesce(a1.store_lost_qty,0) * coalesce(a2.original_price,0) + a1.p_lp_sub_amt  as pre_sale_amt
,case when coalesce(a4.dc_original_price,0)  <> 0 
then a1.inbound_qty * coalesce(a4.dc_original_price,0) 
when ( (a1.out_stock_pay_amt + a1.scm_promotion_amt_total) / a1.total_outstock_qty ) > (a1.inbound_amount / a1.inbound_qty)
then ( (a1.out_stock_pay_amt + a1.scm_promotion_amt_total) / a1.total_outstock_qty ) * a1.inbound_qty
else a1.inbound_amount
end as pre_inbound_amount
,coalesce(a1.store_lost_qty,0) as pre_lost_qty
,coalesce(a1.store_lost_qty,0) * coalesce(a2.original_price,0) as pre_lost_amt
,case when a5.article_id is not null then 1 else 0 end as is_outstock_promo_article
,coalesce(a4.original_dc_price,0) as pre_original_dc_price
,coalesce(a4.dc_original_price,0) as pre_dc_original_price
,(coalesce(a1.out_stock_pay_amt,0) + coalesce(a1.scm_promotion_amt_total,0)) / total_outstock_qty as expect_outstock_prcie
,(a1.inbound_amount / a1.inbound_qty) as  inbound_price
,case when coalesce(a4.dc_original_price,0)  <> 0 
then a1.inbound_qty * coalesce(a4.dc_original_price,0) 
when ( (a1.out_stock_pay_amt + a1.scm_promotion_amt_total) / a1.total_outstock_qty ) > (a1.inbound_amount / a1.inbound_qty)
then ( (a1.out_stock_pay_amt + a1.scm_promotion_amt_total) / a1.total_outstock_qty )
else (a1.inbound_amount / a1.inbound_qty)
end as pre_inbound_ince
,(a1.p_lp_sub_amt + coalesce(a4.pre_lost_amt,0)) as rev_sale_amt
,a1.out_stock_pay_amt_notax -- 门店出库额(不含税)
,a1.return_stock_pay_amt_notax --门店退仓额(不含税)
,coalesce(a1.out_stock_pay_amt_notax,0) - abs(coalesce(a1.return_stock_pay_amt_notax,0)) as scm_fin_article_income
,coalesce(a1.out_stock_amt_cb_notax,0) - abs(coalesce(a1.return_stock_amt_cb_notax,0)) as scm_fin_article_cost
,( coalesce(a1.out_stock_pay_amt_notax,0) - abs(coalesce(a1.return_stock_pay_amt_notax,0)) ) - (coalesce(a1.out_stock_amt_cb_notax,0) - abs(coalesce(a1.return_stock_amt_cb_notax,0)) )
as scm_fin_article_profit  --  供应链毛利(财务)
,a1.no_ordercoupon_company_promotion_amt  -- 公司承担非券的优惠金额    
,a1.ordercoupon_shop_promotion_amt        -- 门店承担优惠券的优惠金额(成本费用额(门店))
,a1.ordercoupon_company_promotion_amt     -- 公司承担优惠券的优惠金额
,a1.shop_promotion_amt                    -- 门店发起活动促销额  zengweihui add 2024-12-10 
,a1.vender_bear_gift_amt -- 供应商承担赠品金额 zengweihui add 2025-01-07
,a1.scm_bear_gift_amt    -- 供应链承担赠品金额 zengweihui add 2025-01-07
,a1.init_stock_amt     -- 期初库存金额
,a1.end_stock_amt      -- 期末库存金额
,a1.init_stock_qty     -- 期初库存数量
,a1.end_stock_qty      -- 期末库存数量
,a1.qdm_bear_negative_amt_total       -- 公司承担负让利总额 zengweihui 2025-04-08
,a1.qdm_bear_positive_amt_total       -- 公司承担让利总额 zengweihui 2025-04-08
,a1.qdm_bear_gift_qty       -- 公司承担赠品数量 zengweihui 2025-04-08
,a1.qdm_bear_gift_amt       -- 公司承担赠品金额 zengweihui 2025-04-08
,a1.qdm_bear_nogift_negative_amt       -- 公司承担非赠品负让利金额 zengweihui 2025-04-08
,a1.qdm_bear_nogift_positive_amt       -- 公司承担非赠品让利金额 zengweihui 2025-04-08
,a1.qdm_bear_promotion_fee       -- 公司承担促销费用 zengweihui 2025-04-08
,a1.cost_price -- 进货成本价 zengweihui 2025-07-29
,a1.sale_cost_amt -- 销售成本 2025-07-29
,a1.avg_purchase_price -- 平均进货价 2025-07-29
,a1.pre_profit_amt -- 门店预期销售毛利额【原价销售额-销售数量*平均进货价】 2025-07-29
,a1.sale_profit_amt -- 门店毛利额(分析)【销售额-销售成本】 2025-07-29
,a1.vender_bear_gift_qty -- 供应商承担赠品数量 zengweihui 2025-08-06
,a1.scm_bear_gift_qty    -- 供应链承担赠品数量 zengweihui 2025-08-06
,a1.sale_piece_qty -- 销售件数 2025-10-27
,a1.bf19_sale_piece_qty -- 19点前销售件数 2025-10-27
,a1.inc_day              --取营业日期为日分区
from tmp_dal.dal_manage_full_link_dc_store_article_wide_temp_01 a1
left join (
    select	
    inc_day as business_date
    ,shop_id as store_id
    ,sku_code as article_id
    ,max(current_price) as current_price
    ,max(yesterday_price) as yesterday_price
    ,max(dc_original_price) as dc_original_price
    ,max(dc_price) as dc_price
    ,max(yesterday_dc_price) as yesterday_dc_price
    ,max(original_price) as original_price
    from dim.dim_store_article_price_info_da
    where inc_day between '$start_day' and '$end_day'
    group by inc_day,shop_id,sku_code
) a2 on a1.business_date = a2.business_date and a1.store_id = a2.store_id and a1.article_id = a2.article_id
left join (
    select 
    ean11
    ,max(zeanjgcl) as category_id
    ,inc_day
    from ods_sap.zmmt002  
    where inc_day='$yesterday'
    group by 
    ean11
    ,inc_day
) a31 on a1.article_id=a31.ean11
left join (
    select
    store_id               --门店编码
    ,strategy_category_id   --策略品类编码
    ,strategy_category_name --策略品类名称
    ,strategy_no            --策略方案编码
    ,strategy_name          --策略方案名称
    ,ps_ctrl                --是否参与价格策略管控:1是 0否
    ,tag                    --标签
    ,strategy_type          --方案类型
    ,plan_type              --方案类别id
    ,price_level            --价格等级
    ,price_zone             --价格带
    ,strategy_status        --策略状态(0已停用 1使用中)
    ,strategy_short_name    --价格策略简称
    ,update_time            --数据更新时间
    ,inc_day                --取营业日期为日分区
    from dim.dim_store_category_price_strategy_di
    where inc_day = '$yesterday'
) a3 on a1.store_id = a3.store_id and a3.strategy_category_id = a31.category_id
left join (
    select
    inc_day as business_date
    ,article_id
    ,store_id
    --   ,pre_sales_amt as pre_sale_amt
    --   ,purchase_qty_hgift * dc_original_price as pre_inbound_amount
    ,dc_original_price
    ,original_dc_price
    ,(unknow_qty + know_qty) as pre_lost_qty
    ,(unknow_pre_amt + know_pre_amt) as pre_lost_amt
    from dal.dal_article_daily_expect_sales_sap_di
    where inc_day between '$start_day' and '$end_day'
) a4 on a1.business_date = a4.business_date and a1.store_id = a4.store_id and a1.article_id = a4.article_id
left join (
    select 
    business_date
    ,store_id
    ,sale_article_id as article_id
    from dim.dim_outstock_promotion_to_sale_article_label_di
    where inc_day between '$start_day' and '$end_day'
    and is_promotion_article = 1
    group by 
    business_date
    ,store_id
    ,sale_article_id
) a5 on a1.business_date = a5.business_date and a1.store_id = a5.store_id and a1.article_id = a5.article_id
left join (
    select 
    dc_id
    ,article_id
    -- ,inc_day as business_date
    ,date(date_add(inc_day,1)) as business_date -- zengweihui 2025-12-09
    -- ,max(pur_market_price) as pur_market_price
    ,min(pur_market_price) as pur_market_price -- zengweihui 2025-12-09
    from dal_full_link.dal_manage_price_srm_info_di
    -- where inc_day between '$start_day' and '$end_day'
    where inc_day>=date(date_sub('$start_day',1)) and inc_day<= date(date_sub('$end_day',1))   -- 销售日期 -1 zengweihui 2025-12-09
    group by 
    dc_id
    ,article_id
    -- ,inc_day
    ,date(date_add(inc_day,1)) -- zengweihui 2025-12-09
) a6 on a1.article_id = a6.article_id and a1.business_date = a6.business_date and a1.new_dc_id = a6.dc_id
left join (
    select 
    article_id
    ,matnr 
    from dim.dim_goods_information_have_pt 
    where inc_day='$yesterday'
) gs on a1.article_id = gs.article_id
--处理成本_含税不含税问题
-- left join (select dc_id,matnr,tax_rate from dal_bi_rpt.dim_in_dc_tax_na) tax  on gs.matnr = tax.matnr and a1.new_dc_id = tax.dc_id
;


REFRESH EXTERNAL TABLE dal_full_link.dal_manage_full_link_store_dc_article_info_di;
insert overwrite dal_full_link.dal_manage_full_link_store_dc_article_info_di partition(inc_day)
select 
a1.business_date
,st.area_id as operate_id          	      --大区id
,st.area_description as operate_name      --大区名称
,st.area2_id as area_id             	  --运营区域id
,st.area2_name as area_name           	  --运营区域名称
,a1.new_dc_id
,dc.dc_name             	  --仓名称
,a1.store_id
,st.store_name          	               --门店名称
,art.category_level1_id as first_category_id   	  --             	大分类编码
,art.category_level1_description as first_category_name 	  --             	大分类编码
,art.category_level2_id as second_category_id  	  --             	中分类编码
,art.category_level2_description as second_category_name	  --             	中分类编码
,art.category_level3_id as third_category_id   	  --             	小分类编码
,art.category_level3_description as third_category_name 	  --             	小分类编码
,a1.article_id
,art.article_name        	  --             	商品名称
,strategy_no
,strategy_name
,strategy_category_level1_id  as  category_id
,strategy_category_level1_name  as  category_name
,price_level
,price_zone
,store_order_qty
,inbound_qty
,inbound_amount
,avg_inbound_price  as  avg_instock_price
,bf19_orig_sale_qty  as  sale_originalprice_qty
,bf19_orig_lp_sale_amt  as  sale_lp_originalprice_amt
,bf19_orig_sale_amt  as  sale_originalprice_amt
,bf19_orig_sale_custs  as  sale_originalprice_custs
,bf19_orig_discount_amt  as  bf19_discount_amt
,bf19_orig_profit  as  sale_originalprice_profit
,avg_bf19_non_orig_price  as  bf19_promotion_avg_price
,bf19_non_orig_sale_qty  as  bf19_promotion_sale_qty
,bf19_non_orig_lp_sale_amt  as  bf19_lp_promotion_sale_amt
,bf19_non_orig_sale_amt  as  bf19_promotion_sale_amt
,bf19_non_orig_sale_custs  as  bf19_promotion_custs
,bf19_non_orig_profit  as  bf19_promotion_sale_profit
,avg_bf19_sale_price  as  bf19_avg_price
,bf19_sale_qty
,bf19_lp_sale_amt
,bf19_sale_amt
,bf19_sale_custs
,bf19_discount_amt  as  bf19_promotion_amt
,avg_af19_sale_price  as  af19_sale_price
,af19_sale_qty
,af19_lp_sale_amt
,af19_sale_amt
,af19_sale_custs
,af19_sale_profit
,avg_sale_price
,sale_qty  as  total_sale_qty
,p_lp_sub_amt  as  lp_sale_amt
,sale_amt  as  total_sale_amt
,total_cust_counts
,article_profit_amt
,promotion_discount_amt
,member_discount_amt
,hour_discount_amt
,discount_amt
,store_know_lost_qty
,store_know_lost_amt
,store_unknow_lost_qty
,store_unknow_lost_amt
,store_lost_qty
,store_lost_amt
,sale_return_qty
,return_custs  as  sale_return_cust
,sale_return_amt
,avg_offline_sale_price  as  offline_avg_sale_price
,offline_sale_qty
,offline_lp_sale_amt
,offline_sale_amt
,offline_sale_custs  as  offline_cust_num
,offline_sale_return_qty
,offline_return_custs  as  offline_sale_return_cust
,offline_sale_return_amt
,offline_discount_amt
,avg_online_sale_price  as  online_avg_sale_price
,online_sale_qty
,online_lp_sale_amt
,online_sale_amt
,online_sale_custs  as  online_cust_num
,online_sale_return_qty
,online_return_custs  as  online_sale_return_cust
,online_sale_return_amt
,online_discount_amt
,out_stock_amt_cb_notax
,out_stock_pay_amt
,return_stock_pay_amt
,return_stock_amt_cb_notax
,dc_original_price
,original_outstock_qty
,original_outstock_amt
,promotion_outstock_price
,promotion_outstock_qty
,promotion_outstock_amt
,gift_outstock_qty
,gift_outstock_amt
,total_outstock_qty
,full_link_article_profit
,store_promotion_cost
,store_bear_cost
,online_bear_cost
,brand_center_bear_cost
,market_bear_cost
,scm_promotion_cost
,scm_return_promotion_cost
,allowance_amt
,bf19_non_orig_discount_amt  as  bf19_promotion_discount_amt
,order_amt  as  store_order_amt
,dc_article_expect_profit
,fulllink_article_expect_profit
,return_stock_amt_cb
,out_stock_amt_cb
,return_stock_qty
,dc_out_profit
,expect_outstock_amt
,art.matnr
,art.abi_purchase_group as purchase_group_id
,platform_bear_cost
,service_bear_cost
,supplier_bear_cost
,bear_cost_123000
,other_bear_cost
,bf19_actual_amount
,af19_actual_amount
,actual_amount
,'' as settle_unit
,order_qty_payean
,adjustment_amt
,adjustment_amt_notax
,current_timestamp() as update_time    -- 数据更新时间
,current_price
,dc_price
,original_price
,pur_market_price
,finally_total_sale_amt
,st.pro_id
,st.pro_description
,st.city_id
,st.city_description
,return_num
,return_amt
,purchase_weight
,sales_weight
,bf19_member_sale_custs  as  bf19_member_custs
,member_sale_custs  as  member_custs
,scm_promotion_qty_gift
,scm_promotion_amt_gift
,scm_promotion_amt
,scm_bear_amt
,vendor_bear_amt
,business_market_bear_amt
,business_bear_amt
,market_bear_amt
,scm_promotion_amt_total
,case when st.sp_store_status = 30 and st.stop_start_date >= a1.inc_day and st.stop_end_date <= a1.inc_day then 0 else (case when a1.order_qty_payean > 0 and a1.total_outstock_qty <  a1.order_qty_payean then a1.order_qty_payean-a1.total_outstock_qty else 0 end) end as miss_stock_qty
,case when st.sp_store_status = 30 and st.stop_start_date >= a1.inc_day and st.stop_end_date <= a1.inc_day then 0 else (case when a1.order_qty_payean > 0 and a1.total_outstock_qty <  a1.order_qty_payean then (a1.order_qty_payean-a1.total_outstock_qty)*a1.dc_price else 0 end) end as miss_stock_amt
,member_coupon_shop_amt      --会员券(门店)
,member_promo_amt            --会员活动促销费
,pre_sale_amt
,pre_inbound_amount
,pre_lost_qty
,pre_lost_amt
,is_outstock_promo_article
,a1.member_coupon_company_amt --会员券费用(公司)
,a1.promo_amt --总会员券费用
,a1.out_stock_pay_amt_notax -- 门店出库额(不含税)
,a1.return_stock_pay_amt_notax --门店退仓额(不含税)
,a1.scm_fin_article_income
,a1.scm_fin_article_cost
,a1.scm_fin_article_profit  --  供应链毛利(财务)
,a1.no_ordercoupon_company_promotion_amt  -- 公司承担非券的优惠金额    
,a1.ordercoupon_shop_promotion_amt        -- 门店承担优惠券的优惠金额(成本费用额(门店))
,a1.ordercoupon_company_promotion_amt     -- 公司承担优惠券的优惠金额
,f.business_flag -- 是否营业(1：营业 算店日均，0：未营业 算非店日均) zengweihui add 2025-03-21
,a1.shop_promotion_amt -- 门店发起促销折扣额 zengweihui add 2024-12-10
,a1.vender_bear_gift_amt -- 供应商承担赠品金额 zengweihui add 2025-01-07
,a1.scm_bear_gift_amt    -- 供应链承担赠品金额 zengweihui add 2025-01-07
,a1.init_stock_amt     -- 期初库存金额
,a1.end_stock_amt      -- 期末库存金额
,a1.init_stock_qty     -- 期初库存数量
,a1.end_stock_qty      -- 期末库存数量
,a1.qdm_bear_negative_amt_total       -- 公司承担负让利总额 zengweihui 2025-04-08
,a1.qdm_bear_positive_amt_total       -- 公司承担让利总额 zengweihui 2025-04-08
,a1.qdm_bear_gift_qty       -- 公司承担赠品数量 zengweihui 2025-04-08
,a1.qdm_bear_gift_amt       -- 公司承担赠品金额 zengweihui 2025-04-08
,a1.qdm_bear_nogift_negative_amt       -- 公司承担非赠品负让利金额 zengweihui 2025-04-08
,a1.qdm_bear_nogift_positive_amt       -- 公司承担非赠品让利金额 zengweihui 2025-04-08
,a1.qdm_bear_promotion_fee       -- 公司承担促销费用 zengweihui 2025-04-08
,a1.cost_price -- 进货成本价 zengweihui 2025-07-29
,a1.sale_cost_amt -- 销售成本 2025-07-29
,a1.avg_purchase_price -- 平均进货价 2025-07-29
,a1.pre_profit_amt -- 门店预期销售毛利额【原价销售额-销售数量*平均进货价】 2025-07-29
,a1.sale_profit_amt -- 门店毛利额(分析)【销售额-销售成本】 2025-07-29
,a1.vender_bear_gift_qty -- 供应商承担赠品数量 zengweihui 2025-08-06
,a1.scm_bear_gift_qty    -- 供应链承担赠品数量 zengweihui 2025-08-06
,a1.sale_piece_qty -- 销售件数 2025-10-27
,a1.bf19_sale_piece_qty -- 19点前销售件数 2025-10-27
,a1.inc_day
from tmp_dal.dal_manage_full_link_dc_store_article_wide_temp_02 a1
-- 关联门店维度表
left join (
    select  
    sp_store_id as store_id   
    ,sp_store_name as store_name          	               --门店名称
    ,area_id 
    ,area_description 
    ,area2_id            	  --运营区域id
    ,area2_name           	  --运营区域名称
    ,pro_id
    ,pro_description
    ,city_id
    ,city_description
    ,sp_store_status
    ,stop_start_date
    ,stop_end_date
    ,sp_origin_start_date
    from    dim.dim_store_profile 
    where   inc_day='$yesterday'
) st on a1.store_id = st.store_id
left join (
    select  
    dc_id
    ,dc_name
    ,dc_vendor_id
    ,new_dc_id
    from dim.dim_dc_profile 
    where inc_day='$yesterday'
) dc on a1.new_dc_id = dc.new_dc_id
left join (
    select  
    article_id
    ,matnr
    ,article_name
    ,category_level3_id
    ,category_level3_description
    ,category_level2_id
    ,category_level2_description
    ,category_level1_id
    ,category_level1_description
    ,abi_purchase_group
    ,abi_purchase_group_name
    from dim.dim_goods_information_have_pt 
    where inc_day='$yesterday'
) art on a1.article_id = art.article_id
left join (
    select
    inc_day 
    ,store_id
    ,max(business_flag) as  business_flag  -- 是否营业(1：营业中 算店日均、0：未营业 算非店日均)
    from dal.dal_transaction_cbstore_cust_num_info_di
    where inc_day between '$start_day' and '$end_day'
    and categroy_type_id in ('01','04') -- 包含门店、菜吧粒度
    and type_id='00' 
    group by
    inc_day
    ,store_id 
) f on a1.inc_day=f.inc_day and a1.store_id=f.store_id
;

EOF);

echo "$sqltxt";
echo "------开始执行----------"
echo "$sqltxt" | /usr/bin/mysql -h$STARROCKS_HOST -P$STARROCKS_PORT -u$STARROCKS_USER -p$STARROCKS_PASSWD;

cstatus=$? 
echo "------执行状态码如下----------"
echo $cstatus

if [ $cstatus != 0 ];
then 
echo "执行失败"
exit 1
fi 
echo "------执行结束----------"

done 
