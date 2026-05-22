-- insert into default_catalog.ads_business_analysis.strategy_fm_levels_result
with data_range as (
    select '2025-11-12' as startDate, '2025-11-25' as endDate from DUAL
)

--门店
SELECT 
    store_flag AS 标签,
    store_no AS 门店号,
    business_date AS 日期,
    case when store_flag is null then '广州' else store_name end as 门店名称,
    sku_id as 商品编码,
    case when level_description = '门店' then ''
        when level_description = '大分类' then category_level1_description
        when level_description = '中分类' then category_level2_description
        when level_description = '小分类' then category_level3_description
        when level_description = 'spu' then spu_name
        when level_description = '黑白猪' then spu_name
        when level_description = 'sku' then sku_name
    end as 分类名称,
    category_level1_description AS 大分类,
    category_level2_description AS 中分类,
    category_level3_description AS 小分类,
    level_description AS 分类等级,
    day_clear,
    case when day_clear = '0' then '日清' when day_clear = '1' then '非日清' when day_clear = '2' then '合计' end as 非日清标识,
    COUNT(store_id) AS 营业店日数, -- 1
    COUNT(DISTINCT store_id) AS 营业店数, -- 1
    AVG(full_link_article_profit) AS 全链路毛利额, -- 1
    AVG(scm_fin_article_profit) AS 供应链毛利额, -- 1
    AVG(article_profit_amt) AS 门店毛利额, -- 1
    SUM(full_link_article_profit)/SUM(total_sale_amt) AS 全链路毛利率, -- 2
    SUM(scm_fin_article_profit)/SUM(out_stock_pay_amt_notax + return_stock_pay_amt_notax) AS 供应链毛利率, -- 2
    SUM(article_profit_amt)/SUM(total_sale_amt) AS 门店毛利率, -- 2
    AVG(sales_weight) AS 销售重量, -- 1
    AVG(bf19_sales_weight) AS "19点前销售重量", -- 1
    AVG(total_sale_qty) AS 销售数量, -- 1
    AVG(bf19_sale_qty) AS "19点前销售数量", -- 1
    AVG(inbound_amount) AS 进货额, -- 1
    AVG(total_sale_amt) AS 全天销售额, -- 1
    AVG(cust_num_cate) AS 全天来客数, -- 1
    AVG(total_sale_amt)/AVG(cust_num_cate) AS 全天客单价, -- 2
    AVG(bf19_sale_amt) AS "19点前销售额", -- 1
    AVG(bf19_cust_num_cate) AS "19点前客数", -- 1
    AVG(bf19_sale_amt)/AVG(bf19_cust_num_cate) AS "19点前客单价", -- 2
    SUM(bf19_sale_amt)/SUM(bf19_sale_piece_qty) AS "19点前件单价", -- 2
    SUM(bf19_sale_piece_qty)/SUM(bf19_cust_num_cate) AS "19点前单件数", -- 2
    -- AVG(bf19_cust_num_cate)/AVG(bf19_cust_num_store) AS 19点前PI
    AVG(sale_article_num_cate) AS 动销sku数, -- 1
    SUM(expect_outstock_amt - out_stock_amt_cb)/SUM(expect_outstock_amt) AS 供应链预期毛利率, -- 2
    SUM(pre_profit_amt)/SUM(lp_sale_amt) AS 门店预期毛利率, -- 2
    SUM(pre_sale_amt-(pre_inbound_amount+coalesce(init_stock_amt,0)-coalesce(end_stock_amt,0)))/SUM(pre_sale_amt) AS 门店定价毛利率, -- 2
    SUM(out_stock_amt_cb)/SUM(purchase_weight) AS 采购价, -- 2
    SUM(total_sale_amt)/SUM(sales_weight) AS 平均售价, -- 2
    SUM(scm_promotion_amt_total)/SUM(scm_promotion_amt_total+out_stock_pay_amt_notax) AS 供应链折让率, -- 2
    SUM(discount_amt)/SUM(lp_sale_amt) AS 折扣率, -- 2
    SUM(discount_amt_cate)/SUM(lp_sale_amt) AS 促销折扣率, -- 2
    SUM(hour_discount_amt)/SUM(lp_sale_amt) AS 时段折扣率, -- 2
    AVG(store_lost_amt) AS 损耗额, -- 1
    SUM(store_lost_amt)/SUM(lost_denominator) AS 损耗率, -- 2
    SUM(return_amt)/SUM(out_stock_pay_amt + return_amt) AS 退货率, -- 2
    SUM(total_sale_amt)/SUM(sale_article_num_cate) AS 品效, -- 2
    SUM(is_soldout_16)/SUM(is_soldout_16_salesku) AS 售罄率16, -- 2
    SUM(is_soldout_20)/SUM(is_soldout_20_salesku) AS 售罄率20, -- 2
    SUM(init_stock_qty+inbound_qty)/SUM(avg_7d_sale_qty) AS 周转率, -- 2
    SUM(total_sale_amt) / SUM(SUM(total_sale_amt)) OVER (
        PARTITION BY
            business_date,
            case when store_flag is null then '广州' else store_name end,
            level_description,
            day_clear,
            CASE WHEN level_description in ('spu','sku') THEN category_level1_description ELSE NULL END
    ) AS 销售额占比_组内, -- 2
    case when level_description in ('spu','sku') then
        row_number() over (partition by business_date,case when store_flag is null then '广州' else store_name end,day_clear,level_description,category_level2_description
                order by avg(total_sale_amt) desc)
        else ''
    end as 销售额排名_中分类, -- 2
    case when level_description in ('spu','sku') then
        row_number() over (partition by business_date,case when store_flag is null then '广州' else store_name end,day_clear,level_description,category_level1_description
                order by avg(total_sale_amt) desc)
        else ''
    end as 销售额排名_大分类, -- 2
    -- 添加分子和分母作为独立指标
    avg(pre_sale_amt) AS "理论销售额", -- 1
    avg(init_stock_amt) AS "期初库存额", -- 1
    avg(end_stock_amt) AS "期末库存额", -- 1
    avg(bf19_sale_piece_qty) AS "19点前销售件数", -- 1
    avg(out_stock_amt_cb) AS "出库成本", -- 1
    avg(purchase_weight) AS "进货重量", -- 1
    avg(scm_promotion_amt_total) AS "供应链促销额", -- 1
    avg(discount_amt) AS "折扣额", -- 1
    avg(hour_discount_amt) AS "时段折扣额", -- 1
    avg(discount_amt_cate) AS "促销折扣额", -- 1
    avg(pre_profit_amt) AS "门店预期毛利额", -- 1
    avg(out_stock_pay_amt_notax + return_stock_pay_amt_notax) AS "供应链毛利率_分母", -- 1
    avg(expect_outstock_amt - out_stock_amt_cb) AS "供应链预期毛利率_分子", -- 1
    avg(expect_outstock_amt) AS "供应链预期毛利率_分母", -- 1
    avg(pre_sale_amt-(pre_inbound_amount+coalesce(init_stock_amt,0)-coalesce(end_stock_amt,0))) AS "门店定价毛利率_分子", -- 1
    avg(scm_promotion_amt_total+out_stock_pay_amt_notax) AS "供应链折让率_分母", -- 1
    avg(lost_denominator) AS "损耗率_分母", -- 1
    avg(return_amt) AS "退货率_分子", -- 1
    avg(out_stock_pay_amt + return_amt) AS "退货率_分母", -- 1
    avg(lp_sale_amt) AS "原价销售额", -- 1
    avg(init_stock_qty) AS "期初库存量", -- 1
    avg(end_stock_qty) AS "期末库存量", -- 1
    avg(avg_7d_sale_qty) AS "7天日均销售量", -- 1
    avg(inbound_qty) AS "进货数量", -- 1
    sum(store_lost_amt)/sum(total_sale_amt) AS "损耗率_销售额", -- 2
    avg(store_lost_qty) AS "损耗数量", -- 1
    sum(store_lost_qty)/sum(init_stock_qty + inbound_qty) AS "损耗率_数量", -- 2
    avg(is_stock_sku) AS "上架sku数", -- 1
    sum(sale_article_num_cate)/sum(is_stock_sku) AS "sku动销率", -- 2
    sum(lp_sale_amt)/sum(sales_weight) AS "平均销售原价", -- 2
    sum(inbound_amount)/sum(purchase_weight) AS "进货价", -- 2
    avg(sale_piece_qty) AS "销售件数", -- 1
    avg(store_know_lost_amt) AS "门店已知损耗额", -- 1
    avg(store_unknow_lost_amt) AS "门店未知损耗额" -- 1
FROM 
    default_catalog.ads_business_analysis.strategy_fm_levels_sum, data_range
WHERE 
    business_date BETWEEN startDate AND endDate
    and not (level_description in ('sku') and is_stock_sku = 0 and total_sale_amt = 0 and inbound_qty = 0 
                and init_stock_qty = 0 and end_stock_qty = 0 and store_lost_qty = 0)
GROUP BY 
    1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12
