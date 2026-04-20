-- insert into default_catalog.ads_business_analysis.strategy_fm_cust 
WITH data_range AS (
    SELECT '2025-10-20' AS startDate, '2025-10-25' AS endDate FROM DUAL
),

store_sum_order as (
    select 
        t1.business_date,
        t1.store_id,
        t4.sp_store_name as store_name,
        t1.order_id,
        t1.pay_at,
        t1.abi_article_id,
        case when t3.category_level2_description in ('蛋类','烘焙类') then t3.category_level2_description 
        when t3.category_level2_description in ('冷藏奶制品类','饮料类') then '乳制品及水饮类'
        when t3.category_level1_description = '肉禽蛋类' and t3.category_level2_description <> '蛋类' then '肉禽类'
        when right(t3.category_level3_description, 2) = '熟食' then '熟食类'
        when t3.category_level1_description in ('冷藏及加工类','预制菜') then '冷藏加工及预制菜类'
        else t3.category_level1_description end as category_level1_id,
        t3.category_level2_id,
        t3.category_level3_id,
        t3.spu_id,
        t3.blackwhite_pig_id,
        h.store_flag,
        case 
            when h.store_flag = '翠花店' then e.day_clear
            else case 
                when t3.category_level1_description in ('水果类','预制菜','冷藏及加工类') 
                or t3.category_level2_description in ('蛋类','冷藏奶制品类','烘焙类') then 1 
                else 0 
            end
        end as day_clear,
        jielong_flag,
        actual_amount
    from (
        select business_date,store_id,order_id,pay_at,abi_article_id,inc_day,order_status,jielong_flag,actual_amount
        from hive.dsl.dsl_transaction_sotre_order_offline_details_di, data_range
        where inc_day between startDate and endDate

        union all
        select business_date,store_id,concat(order_id,'*') as order_id,pay_at,abi_article_id,inc_day,order_status,jielong_flag,actual_amount
        from hive.dsl.dsl_transaction_sotre_order_online_details_di, data_range
        where inc_day between startDate and endDate
    ) t1

    LEFT JOIN (
        -- 翠花商品经营
        SELECT a.inc_day, a.store_id, a.article_id, a.day_clear
        FROM hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di a, data_range
        WHERE a.inc_day BETWEEN startDate AND endDate
    ) e ON t1.store_id = e.store_id AND t1.inc_day = e.inc_day AND t1.abi_article_id = e.article_id
    
    LEFT JOIN (
        -- 翠花商品经营
        SELECT a.inc_day, a.article_id, MAX(a.day_clear) as day_clear
        FROM hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di a, data_range
        WHERE a.inc_day BETWEEN startDate AND endDate
        GROUP BY a.inc_day, a.article_id
    ) i ON t1.inc_day = i.inc_day AND t1.abi_article_id = i.article_id

    left join (--翠花门店信息
        select 
        store_id,
        store_flag,
        store_no,
        store_name
        from default_catalog.ads_business_analysis.chdj_store_info
    ) h on t1.store_id=h.store_id
    
    left join (
        select 
            article_id,
            category_level1_id,
            category_level1_description,
            category_level2_id,
            category_level2_description,
            category_level3_id,
            category_level3_description,
            spu_id,
            spu_name,
            blackwhite_pig_id,
            blackwhite_pig_name
        from hive.dim.dim_goods_information_have_pt, data_range
        where inc_day = DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d')
    ) t3 on t1.abi_article_id = t3.article_id
    left join (
        select 
        city_description,
        sp_store_id,
        sp_store_name
        from hive.dim.dim_store_profile, data_range
        where inc_day = DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d')
    ) t4 on t1.store_id = t4.sp_store_id
    where t4.city_description in ('广州') and t1.order_status = 'os.completed' and store_no = 'food mart'
    and category_level1_description IN ('猪肉类', '预制菜', '水果类', '水产类', '蔬菜类', '肉禽蛋类', '冷藏及加工类', '标品类')
    -- and t1.abi_article_id not in ('21265280')
)

--门店-dayclear客数
select
business_date,
store_id,
store_name,
day_clear,
'门店' as level_description,
'' as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--门店-合计客数
select
business_date,
store_id,
store_name,
'2' as day_clear,
'门店' as level_description,
'' as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--大分类-dayclear客数
select
business_date,
store_id,
store_name,
day_clear,
'大分类' as level_description,
category_level1_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--大分类-合计客数
select
business_date,
store_id,
store_name,
'2' as day_clear,
'大分类' as level_description,
category_level1_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--中分类-dayclear客数
select
business_date,
store_id,
store_name,
day_clear,
'中分类' as level_description,
category_level2_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--中分类-合计客数
select
business_date,
store_id,
store_name,
'2' as day_clear,
'中分类' as level_description,
category_level2_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--小分类-dayclear客数
select
business_date,
store_id,
store_name,
day_clear,
'小分类' as level_description,
category_level3_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--小分类-合计客数
select
business_date,
store_id,
store_name,
'2' as day_clear,
'小分类' as level_description,
category_level3_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--spu-dayclear客数
select
business_date,
store_id,
store_name,
day_clear,
'spu' as level_description,
spu_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--spu-合计客数
select
business_date,
store_id,
store_name,
'2' as day_clear,
'spu' as level_description,
spu_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
group by 1,2,3,4,5,6,store_flag

union all
--猪肉-dayclear客数
select
business_date,
store_id,
store_name,
day_clear,
'黑白猪' as level_description,
blackwhite_pig_id as level_id,
count(distinct case 
    when store_flag = '翠花店' then 
        case when substr(pay_at, 12, 8) < '20:00:00' then order_id end
    else 
        case when substr(pay_at, 12, 8) < '19:00:00' then order_id end
end) as bf19_cust_num_cate,
count(distinct order_id) as cust_num_cate,
count(distinct abi_article_id) as sale_article_num_cate,
count(distinct case when right(order_id, 1) = '*' then order_id end) as online_order_num_cate,
count(distinct case when jielong_flag <> '-' then order_id end) as jielong_cust_num_cate,
sum(case when jielong_flag <> '-' then actual_amount end) as jielong_sale_amt
from store_sum_order
where category_level1_id = 13
group by 1,2,3,4,5,6,store_flag