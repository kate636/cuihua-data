-- insert into default_catalog.ads_business_analysis.strategy_fm_flag_sku_di
with data_range as (
    select '2025-09-22' as startDate, '2025-09-22' as endDate from DUAL
)
SELECT 
    -- 时间维度
    t1.business_date,                                          -- 营业日期   
    t5.week_no,
    t5.week_start_date,
    t5.week_end_date,
    t5.month_wid,
    t5.year_wid,
    
    -- 门店维度
    t2.manage_area_name,                                       -- 管理区域
    t2.sap_area_name,                                          -- 大区
    t2.city_description,                                       -- 城市
    t1.store_id,                                               -- 门店编码
    t2.store_name,                                             -- 门店名称
    h.store_flag,
    h.store_no,
    
    -- 商品分类维度
    case when t3.category_level2_description in ('蛋类','烘焙类') then ''
        when t3.category_level2_description in ('冷藏奶制品类','饮料类') then ''
        when t3.category_level1_description = '肉禽蛋类' and t3.category_level2_description <> '蛋类' then ''
        when right(t3.category_level3_description, 2) = '熟食' then ''
        when t3.category_level1_description in ('冷藏及加工类','预制菜') then ''
        else t3.category_level1_id end as category_level1_id,                                     -- 大类编码
    case 
        when t3.category_level2_description in ('蛋类','烘焙类') then t3.category_level2_description 
        when t3.category_level2_description in ('冷藏奶制品类','饮料类') then '乳制品及水饮类'
        when t3.category_level1_description = '肉禽蛋类' and t3.category_level2_description <> '蛋类' then '肉禽类'
        when right(t3.category_level3_description, 2) = '熟食' then '熟食类'
        when t3.category_level1_description in ('冷藏及加工类','预制菜') then '冷藏加工及预制菜类'
        else t3.category_level1_description end as category_level1_description,                            -- 大类名称
    t3.category_level2_id,                                     -- 中类编码
    t3.category_level2_description,                            -- 中类名称
    t3.category_level3_id,                                     -- 小类编码
    t3.category_level3_description,                            -- 小类名称
    
    -- 商品维度
    t3.spu_id,
    t3.spu_name,
    t3.blackwhite_pig_name,
    t1.article_id,
    t3.article_name,
    
    -- 清场标识
    case 
        when h.store_flag = '翠花店' then e.day_clear
        else case 
            when t3.category_level1_description in ('水果类','预制菜','冷藏及加工类') 
            or t3.category_level2_description in ('蛋类','冷藏奶制品类','烘焙类') then 1 
            else 0 
        end
    end as day_clear,
    
    -- 销售指标(使用CHDJ表字段)
    SUM(f.full_link_profit) AS full_link_article_profit,                           -- 2全链路毛利额(CHDJ)
    SUM(f.scm_fin_article_profit) AS scm_fin_article_profit,                            --2 供应链毛利额(CHDJ)
    SUM(f.profit_amt) AS article_profit_amt,                                 --2 门店毛利额(CHDJ)
    SUM(f.pre_profit_amt) as pre_profit_amt, -- 2门店预期毛利额(CHDJ)
    
    -- 销售数量和重量
    SUM(CASE 
        WHEN t3.sale_unit = '千克' THEN t1.total_sale_qty 
        ELSE t1.total_sale_qty * CASE WHEN t3.unit_weight = 0 THEN 1 ELSE t3.unit_weight END
    END) AS sales_weight,                                      -- 2销售重量
    SUM(CASE
        WHEN t3.sale_unit = '千克' THEN t1.bf19_sale_qty 
        ELSE t1.bf19_sale_qty * CASE WHEN t3.unit_weight = 0 THEN 1 ELSE t3.unit_weight END
    END) AS bf19_sales_weight,                                 -- 2 19点前销售重量
    SUM(t1.total_sale_qty) AS total_sale_qty,                  -- 1销售数量
    SUM(t1.bf19_sale_qty) AS bf19_sale_qty,                    -- 1 19点前销售数量
    
    -- 进销存金额
    SUM(t1.inbound_amount) AS inbound_amount,                  -- 2进货额(=receive_qty×avg_purchase_price)
    SUM(t1.purchase_weight) AS purchase_weight,                -- 2进货重量(=receive_qty×unit_weight)
    SUM(t1.total_sale_amt) AS total_sale_amt,                  -- 1全天销售额
    SUM(t1.bf19_sale_amt) AS bf19_sale_amt,                    -- 1 19点前销售额
    SUM(t1.expect_outstock_amt) AS expect_outstock_amt,        -- 2预期出库金额(=out_stock_pay_amt+scm_promotion_amt_total)
    SUM(t1.out_stock_amt_cb) AS out_stock_amt_cb,              -- 2出库到店成本含税(=outstock_qty×outstock_cost_price)
    SUM(t1.pre_sale_amt) AS pre_sale_amt,                      -- 2理论销售额((实际销售数量+理论损耗数量)*销售原价)
    SUM(t1.pre_inbound_amount) AS pre_inbound_amount,          -- 2理论进货额(bo处理后进货量 * 出库原价)
    
    -- 促销和折扣
    SUM(t1.scm_promotion_amt_total) AS scm_promotion_amt_total, -- 1出库让利总额(SAP直接记录)
    SUM(t1.lp_sale_amt) AS lp_sale_amt,                        -- 1 19点前商品原价金额
    SUM(t1.discount_amt) AS discount_amt,                      -- 2折扣额
    CASE 
        WHEN e.day_clear = 1 THEN 0 
        ELSE SUM(t1.hour_discount_amt) 
    END AS hour_discount_amt,                                  -- 1时段折扣额
    CASE 
        WHEN e.day_clear = 1 THEN SUM(t1.discount_amt) 
        ELSE SUM(t1.discount_amt - t1.hour_discount_amt) 
    END AS discount_amt_cate,                                  -- 2促销折扣额
    
    -- 损耗和退货
    SUM(t1.store_lost_amt) AS store_lost_amt,                  -- 2门店总损耗金额
    SUM(t1.return_amt) AS return_amt,                          -- 1顾客退货金额
    
    -- 出入库金额
    SUM(t1.out_stock_pay_amt) AS out_stock_pay_amt,            -- 2门店商品维度出库金额(=outstock_qty×outstock_unit_price)
    SUM(t1.out_stock_pay_amt_notax) AS out_stock_pay_amt_notax, -- 2门店出库额(不含税,=outstock_qty×outstock_unit_price_notax)
    SUM(t1.return_stock_pay_amt_notax) AS return_stock_pay_amt_notax, -- 2门店退仓额(不含税,=return_qty×return_unit_price_notax)
    
    -- 销售件数和库存
    SUM(c.bf19_sale_piece_qty) AS bf19_sale_piece_qty,         -- 1 19点前销售件数
    SUM(t1.inbound_amount + coalesce(f.init_stock_amt,0)) AS lost_denominator, -- 2 损耗率分母
    
    -- 售罄指标
    AVG(CASE WHEN  saleable = 0 or t3.article_name like '%J%' or t3.article_name like '%ZC%'  THEN null
            ELSE CASE 
                WHEN f.end_stock_qty = 0 AND SUBSTR(f.last_sysdate, 12, 8) < '16:00:00' THEN 1 
                WHEN f.last_sysdate IS NOT NULL or f.end_stock_qty > 0 THEN 0 
            END
        END) AS is_soldout_16,                                 -- 2售罄率16
    AVG(CASE WHEN  saleable = 0 or t3.article_name like '%J%' or t3.article_name like '%ZC%'  THEN null
            ELSE CASE 
                WHEN f.end_stock_qty = 0 AND SUBSTR(f.last_sysdate, 12, 8) < '20:00:00' THEN 1 
                WHEN f.last_sysdate IS NOT NULL or f.end_stock_qty > 0 THEN 0 
            END
        END) AS is_soldout_20,                                 -- 2售罄率20
    
    -- 库存金额
    sum(f.init_stock_amt) as init_stock_amt,                   -- 2期初库存金额(=init_stock_qty×avg_price)
    sum(f.end_stock_amt) as end_stock_amt,                     -- 2期末库存金额(盘点优先为1原子;无盘点时为计算值=end_stock_qty×avg_price)
    SUM(f.end_stock_qty) AS end_stock_qty,                     -- 2期末库存数量(盘点优先为1原子;无盘点时=库存方程推算)
    SUM(f.avg_7d_sale_qty) AS avg_7d_sale_qty,                  -- 2 7天平均销量

    -- 客数
    SUM(t1.total_cust_counts) AS cust_num,                              -- 2客数
    SUM(t1.bf19_sale_custs) AS bf19_cust_num,                   -- 2 19点前客数

    -- 进销存
    SUM(t1.init_stock_qty) AS init_stock_qty,                   -- 1期初库存数量(=昨日end_stock_qty,日切依赖)
    SUM(t1.inbound_qty) AS inbound_qty,                          -- 1进货数量
    round(AVG(CASE WHEN saleable = 1 and (t1.total_sale_amt >0
        or (t1.total_sale_amt = 0 and (f.end_stock_amt <> 0 or t1.store_lost_amt <> 0)))
        and t1.article_id not in (
            '21282294','21282324','21282358','21282362','21282391','21282171','21282188','21282718',
            '21282730','21282717','21282758','21282776','21282715','21282718','21282749','21282777',
            '21282804','21282805','21282812','21282815','21282850','21282935','21282942','21282956',
            '21282959','21282975','21282830','21282904','21282997','21283021','21283041','21281502',
            '21282970','21282416','21282434','21964765','21964730','21964784','21964791','21964798',
            '21974912','21974932','21974925','21974931','21974975','21300745','21300759','21300765',
            '21282669','21282683','21282904','21282928','21283109','20591021'
        ) 
        then 1 else 0 end),0) as is_stock_sku,  --2上架sku
    sum(store_lost_qty) as store_lost_qty,                        -- 2门店损耗数量
    sum(c.sale_piece_qty) as sale_piece_qty,                        -- 1销售件数
    sum(t1.store_know_lost_amt) as store_know_lost_amt,            -- 2门店已知损耗金额(=know_lost_qty×cost_price)
    sum(t1.store_unknow_lost_amt) as store_unknow_lost_amt,        -- 2门店未知损耗金额
    sum(t1.online_cust_num) as online_cust_num -- 2线上客数
FROM 
    (
        SELECT * ,ROW_NUMBER() OVER (PARTITION BY store_id, inc_day, article_id ORDER BY article_id DESC) as rn
        FROM hive.dal_full_link.dal_manage_full_link_store_dc_article_info_di, data_range
        WHERE inc_day BETWEEN startDate AND endDate
    ) t1

    RIGHT JOIN (
        -- 销售经营
        SELECT inc_day, store_id, cust_num, bf19_cust_num, sale_article_num, sale_amt, bf19_sale_amt
        FROM hive.dal.dal_transaction_sale_store_daily_di, data_range
        WHERE inc_day BETWEEN startDate AND endDate
        AND bf19_sale_amt >= 500
    ) b ON t1.store_id = b.store_id AND t1.inc_day = b.inc_day

    LEFT JOIN (
        -- 销售件数
        SELECT inc_day, store_id, article_id, bf19_sale_piece_qty,sale_piece_qty
        FROM hive.dal.dal_transaction_store_article_sale_info_di, data_range
        WHERE inc_day BETWEEN startDate AND endDate
    ) c ON t1.store_id = c.store_id AND t1.inc_day = c.inc_day AND t1.article_id = c.article_id

    LEFT JOIN (
        -- 翠花商品经营
        SELECT a.inc_day, a.store_id, a.article_id, a.day_clear
        FROM hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di a, data_range
        WHERE a.inc_day BETWEEN startDate AND endDate
    ) e ON t1.store_id = e.store_id AND t1.inc_day = e.inc_day AND t1.article_id = e.article_id

    LEFT JOIN (
        SELECT inc_day, store_id, article_id, receive_amt, init_stock_amt, end_stock_amt,
                end_stock_qty, avg_7d_sale_qty, last_sysdate, sale_profit_amt, cust_num, bf19_cust_num,pre_profit_amt,
                init_stock_qty, profit_amt, scm_fin_article_profit, full_link_profit, article_profit_amt
        FROM hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di, data_range
        WHERE inc_day BETWEEN startDate AND endDate
    ) f ON t1.store_id = f.store_id AND t1.inc_day = f.inc_day AND t1.article_id = f.article_id AND rn = 1

    LEFT JOIN (--可订可售sku
        select inc_day,shop_id,sku_code from hive.ods_sc_db.t_purchase_order_item_tmp ,data_range
        where inc_day between startDate and endDate
    ) g on t1.article_id=g.sku_code and t1.inc_day=g.inc_day and t1.store_id=g.shop_id

    left join (--翠花门店信息
        select 
        store_id,
        store_flag,
        store_no,
        store_name
        from default_catalog.ads_business_analysis.chdj_store_info
    ) h on t1.store_id=h.store_id

    LEFT JOIN (
        -- 翠花商品经营
        SELECT a.inc_day, a.article_id, MAX(a.day_clear) as day_clear
        FROM hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di a, data_range
        WHERE a.inc_day BETWEEN startDate AND endDate
        GROUP BY a.inc_day, a.article_id
    ) i ON t1.inc_day = i.inc_day AND t1.article_id = i.article_id

    left join (
        select article_id,inc_day,store_id,is_order,saleable 
        from hive.dim.dim_store_article_order_sale_info_di, data_range
        where inc_day BETWEEN startDate AND endDate
    ) j on t1.article_id = j.article_id and t1.inc_day = j.inc_day and t1.store_id = j.store_id

    LEFT JOIN (
        -- 门店资料表
        SELECT 
            sp_store_id AS store_id,          -- 门店编码
            sp_store_name AS store_name,      -- 门店名称
            inc_day,
            pro_id,                           -- 省份编码            
            pro_description,                  -- 省份名称  
            city_id,                          -- 城市编码            
            city_description,                 -- 城市名称
            dist_description,                 -- 行政区名称                
            manage_area_id,                   -- 管理区域编码
            manage_area_name,                 -- 管理区域名称
            sap_area_id,                      -- 运营区域编码
            sap_area_name,                    -- 运营区域名称
            sap_area2_id,                     -- 区域编码
            sap_area2_name,                   -- 区域名称     
            area2_name,                       -- 运营大区名称
            group_manager,                    -- 督导姓名  
            zone_supper_manager,              -- 运营经理
            mall_supervisor_name,             -- 电商督导名称
            expand_staff_name,                -- 拓展员工姓名
            measuring_area,                   -- 量尺面积 
            sp_type,                          -- 门店类型编码 20加盟10直营
            store_type_name,                  -- 门店类型名称 20加盟10直营
            sp_table_flag,                    -- 门店类型标志编码
            store_flag_name,                  -- 门店类型标志名称 10实体店 20虚拟店 30无人柜 70测试店等
            CAST(sp_origin_start_date AS DATE) AS sp_origin_start_date,  -- 开店日期
            CAST(open_days AS INT) AS open_days,  -- 开业天数
            sp_final_end_date,                -- 闭店日期
            closed_reason_name,               -- 闭店原因
            sp_level,                         -- 门店等级
            sp_store_status
        FROM hive.dim.dim_store_profile, data_range
        WHERE inc_day = DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d') 
    ) t2 ON t1.store_id = t2.store_id 

    LEFT JOIN (
        -- 商品资料表
        SELECT 
            inc_day,
            article_id,                       -- 商品编码
            article_name,                     -- 商品名称
            matnr,                            -- 物料编码
            matnr_name,                       -- 物料名称
            spu_id,                           -- spu编码
            spu_name,                         -- spu名称
            category_level3_id,               -- 小类编码
            category_level3_description,      -- 小类名称
            category_level2_id,               -- 中类编码
            category_level2_description,      -- 中类名称
            category_level1_id,               -- 大类编码
            category_level1_description,      -- 大类名称
            abi_purchase_group,               -- 采购组编号
            abi_purchase_group_name,          -- 采购小组名称
            purchase_department,              -- 采购部门名称
            purchase_department_id,           -- 采购部门编号
            superior_purchase_department_id,  -- 采购商品部门上级编码
            superior_purchase_department_name, -- 采购商品部门上级名称
            sale_unit,                        -- 单位
            norms,                            -- 规格
            unit_weight,
            article_series_id,                -- 商品系列编码                 
            article_series_name,              -- 商品系列名称              
            temperature_layer_id,             -- 温层编码                
            temperature_layer_name,           -- 温层名称                
            import_flag,                      -- 进口标识 1进口 0国产        
            blackwhite_pig_id,                -- 黑白猪编码 01黑猪 02白猪     
            blackwhite_pig_name               -- 黑白猪名称 
        FROM hive.dim.dim_goods_information_have_pt, data_range
        WHERE inc_day = DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d') 
    ) t3 ON t1.article_id = t3.article_id

    LEFT JOIN (
        SELECT 
            day_name,              -- 日期
            day_of_week,           -- 本周第几天 
            week_no,               -- 周序号
            week_start_date,       -- 本周开始日期
            week_end_date,         -- 本周结束日期
            week54_no,             -- 周序号 上周五至本周四为一周
            week54_start_date,     -- 本周开始日期 上周五至本周四为一周
            week54_end_date,       -- 本周结束日期 上周五至本周四为一周
            month_wid,             -- 月ID
            year_wid,              -- 年ID
            is_actual_holiday,     -- 实际节假日标签
            actual_holiday_name,   -- 实际节假日名称
            is_rest_day            -- 休息日标签 包含周末与节假日
        FROM hive.dim.dim_calendar, data_range
        WHERE day_name BETWEEN startDate AND endDate
    ) t5 ON t1.business_date = t5.day_name

WHERE 
    -- 筛选大类
    category_level1_description IN ('猪肉类', '预制菜', '水果类', '水产类', '蔬菜类', '肉禽蛋类', '冷藏及加工类', '标品类')
    -- 筛选门店
    AND t1.city_description in ('广州') and h.store_no = 'food mart'
GROUP BY 
    t1.business_date, t5.week_no, t5.week_start_date, t5.week_end_date, t5.month_wid, t5.year_wid,
    t2.manage_area_name, t2.sap_area_name, t2.city_description, t1.store_id, t2.store_name,
    t3.category_level1_description, t3.category_level1_id, e.day_clear, t3.article_name, t1.article_id,
    t3.category_level2_description, t3.category_level2_id, t3.category_level3_description, t3.category_level3_id,
    t3.spu_id,t3.spu_name,t3.blackwhite_pig_name,h.store_flag,h.store_no,i.day_clear