-- insert into default_catalog.ads_business_analysis.strategy_fm_levels_sum
with data_range as (
    select '2025-10-20' as startDate, '2025-10-20' as endDate from DUAL
)

--门店-dayclear
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'门店' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,day_clear,
    '' as category_level1_description, '' as category_level1_id,'' as category_level2_description, '' as category_level2_id,'' as category_level3_description, '' as category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '门店'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear

union all
--门店-合计
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'门店' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no, '2' as day_clear,
    '' as category_level1_description, '' as category_level1_id,'' as category_level2_description, '' as category_level2_id,'' as category_level3_description, '' as category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '门店'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear

union all
--大分类-dayclear
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'大分类' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,day_clear,
    category_level1_description,category_level1_id,'' as category_level2_description, '' as category_level2_id,'' as category_level3_description, '' as category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '大分类'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.category_level1_description = b.level_id

union all
--大分类-合计
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'大分类' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,'2' as day_clear,
    category_level1_description,category_level1_id,'' as category_level2_description, '' as category_level2_id,'' as category_level3_description, '' as category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '大分类'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.category_level1_description = b.level_id

union all
--中分类-dayclear
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'中分类' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,day_clear,
    category_level1_description,category_level1_id,category_level2_description,category_level2_id,'' as category_level3_description, '' as category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '中分类'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.category_level2_id = b.level_id

union all
--中分类-合计
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'中分类' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,'2' as day_clear,
    category_level1_description,category_level1_id,category_level2_description,category_level2_id,'' as category_level3_description, '' as category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '中分类'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.category_level2_id = b.level_id

union all
--小分类-dayclear
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'小分类' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,day_clear,
    category_level1_description,category_level1_id,category_level2_description,category_level2_id,category_level3_description,category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '小分类'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.category_level3_id = b.level_id

union all
--小分类-合计
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'小分类' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,'2' as day_clear,
    category_level1_description,category_level1_id,category_level2_description,category_level2_id,category_level3_description,category_level3_id,'' as spu_id,'' as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '小分类'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.category_level3_id = b.level_id

union all
--SPU维度-dayclear
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'spu' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,day_clear,
    category_level1_description,category_level1_id,category_level2_description,category_level2_id,category_level3_description,category_level3_id,spu_id,spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = 'spu'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.spu_id = b.level_id

union all
--SPU维度-合计
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'spu' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,'2' as day_clear,
    category_level1_description,category_level1_id,category_level2_description,category_level2_id,category_level3_description,category_level3_id,spu_id,spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,level_id,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = 'spu'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.spu_id = b.level_id

union all
--黑白猪-dayclear
select a.*,b.cust_num_cate,b.bf19_cust_num_cate,b.sale_article_num_cate,'黑白猪' as level_description from(
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,day_clear,
    category_level1_description,category_level1_id,'' as category_level2_description,'' as category_level2_id,'' as category_level3_description,'' as category_level3_id,'' as spu_id,blackwhite_pig_name as spu_name,'' as article_id,'' as article_name,
    sum(full_link_article_profit) as full_link_article_profit, SUM(scm_fin_article_profit) AS scm_fin_article_profit,
    SUM(article_profit_amt) AS article_profit_amt, sum(pre_profit_amt) as pre_profit_amt,
    sum(sales_weight) as sales_weight, sum(bf19_sales_weight) as bf19_sales_weight,
    SUM(total_sale_qty) AS total_sale_qty, SUM(bf19_sale_qty) AS bf19_sale_qty,
    SUM(inbound_amount) AS inbound_amount, SUM(purchase_weight) AS purchase_weight,
    SUM(total_sale_amt) AS total_sale_amt, SUM(bf19_sale_amt) AS bf19_sale_amt,
    SUM(expect_outstock_amt) AS expect_outstock_amt,
    SUM(out_stock_amt_cb) AS out_stock_amt_cb, SUM(pre_sale_amt) AS pre_sale_amt,
    SUM(pre_inbound_amount) AS pre_inbound_amount, SUM(scm_promotion_amt_total) AS scm_promotion_amt_total,
    SUM(lp_sale_amt) AS lp_sale_amt, SUM(discount_amt) AS discount_amt,
    SUM(hour_discount_amt) AS hour_discount_amt, sum(discount_amt_cate) as discount_amt_cate,
    SUM(store_lost_amt) AS store_lost_amt, SUM(return_amt) AS return_amt,
    SUM(out_stock_pay_amt) AS out_stock_pay_amt, SUM(out_stock_pay_amt_notax) AS out_stock_pay_amt_notax,
    SUM(return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, SUM(bf19_sale_piece_qty) AS bf19_sale_piece_qty,
    SUM(lost_denominator) AS lost_denominator, SUM(is_soldout_16) as is_soldout_16,
    SUM(is_soldout_20) as is_soldout_20, sum(case when is_soldout_16 is not null then 1 end) as is_soldout_16_salesku,
    sum(case when is_soldout_20 is not null then 1 end) as is_soldout_20_salesku, SUM(end_stock_qty) AS end_stock_qty,
    SUM(avg_7d_sale_qty) AS avg_7d_sale_qty, SUM(init_stock_amt) AS init_stock_amt, SUM(end_stock_amt) AS end_stock_amt,
    SUM(init_stock_qty) AS init_stock_qty,SUM(inbound_qty) AS inbound_qty,SUM(is_stock_sku) AS is_stock_sku,
    SUM(store_lost_qty) AS store_lost_qty,avg(sale_piece_qty) AS sale_piece_qty,
    SUM(store_know_lost_amt) AS store_know_lost_amt, SUM(store_unknow_lost_amt) AS store_unknow_lost_amt
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate and category_level1_description = '猪肉类'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
) a
left join (
    select business_date,store_id,day_clear,case when level_id = '01' then '黑猪' when level_id = '02' then '白猪' end as level_name,cust_num_cate,bf19_cust_num_cate,sale_article_num_cate,level_description
    from default_catalog.ads_business_analysis.strategy_fm_cust ,data_range
    where business_date between startDate and endDate
    and level_description = '黑白猪'
) b on a.store_id = b.store_id and a.business_date = b.business_date and a.day_clear = b.day_clear and a.spu_name = b.level_name

union all
--sku维度-dayclear
SELECT 
    business_date, week_no, week_start_date, week_end_date, month_wid, year_wid,
    manage_area_name, sap_area_name, city_description, store_id, store_name,store_flag,store_no,day_clear,
    category_level1_description,category_level1_id,category_level2_description,category_level2_id,category_level3_description,category_level3_id,spu_id,spu_name, article_id,article_name,
    full_link_article_profit, scm_fin_article_profit,article_profit_amt, pre_profit_amt,
    sales_weight, bf19_sales_weight,
    total_sale_qty, bf19_sale_qty,
    inbound_amount, purchase_weight,
    total_sale_amt, bf19_sale_amt,
    expect_outstock_amt,
    out_stock_amt_cb, pre_sale_amt,
    pre_inbound_amount, scm_promotion_amt_total,
    lp_sale_amt, discount_amt,
    hour_discount_amt, discount_amt_cate,
    store_lost_amt, return_amt,
    out_stock_pay_amt, out_stock_pay_amt_notax,
    return_stock_pay_amt_notax, bf19_sale_piece_qty,
    lost_denominator,
    is_soldout_16, is_soldout_20,
    case when is_soldout_16 is not null then 1 end as is_soldout_16_salesku,
    case when is_soldout_20 is not null then 1 end as is_soldout_20_salesku,
    end_stock_qty,
    avg_7d_sale_qty, init_stock_amt, end_stock_amt,
    init_stock_qty,inbound_qty,is_stock_sku,store_lost_qty,sale_piece_qty,
    store_know_lost_amt, store_unknow_lost_amt,
    cust_num as cust_num_cate, bf19_cust_num as bf19_cust_num_cate, '' as sale_article_num_cate,
    'sku' as level_description
FROM default_catalog.ads_business_analysis.strategy_fm_flag_sku_di,data_range
WHERE business_date between startDate and endDate