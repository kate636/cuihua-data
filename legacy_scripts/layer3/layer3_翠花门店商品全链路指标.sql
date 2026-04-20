    set catalog hive;
    -- 翠花当家门店商品全链路指标
    insert overwrite hive.tmp_dal.dal_transaction_chdj_store_sale_article_sale_info_di_01  partition(inc_day)
    select 
     t1.business_date --  日期
    ,t1.store_id               -- 门店编码
    ,t1.article_id             -- 商品编码
    ,case when t5.article_id is not null then '1' else '0' end as day_clear  -- 非日清标识(非日清:1,日清:0)
    ,t1.scm_fin_article_income -- 总销售收入-财务(供应链到店毛利率分母)
    ,t1.scm_fin_article_profit -- 供应链毛利-财务(供应链到店毛利额)
    ,t1.article_profit_amt     -- 门店商品毛利额
    ,t1.full_link_profit       -- 全链路到店毛利额
    ,t1.total_sale_amt         -- 销售总金额(全链路到店毛利率的分母)
    ,t1.scm_promotion_amt_total           -- 出库让利总额(供应链折让率分子)
    ,t1.out_stock_pay_amt                 -- 门店商品维度出库金额
    ,t1.pre_sale_amt                     -- 理论销售额((实际销售数量+理论损耗数量)*销售原价)
    ,t1.pre_inbound_amount         -- 理论进货额(bom处理后进货量*出库原价)

    ,t1.out_stock_amt_cb  -- 出库到店成本含税(出库到店成本)
    ,t1.expect_outstock_amt  -- 预期出库金额(出库原价金额)

    ,t1.business_date as inc_day     -- 取营业日期为日分区
    from 
    (select 
     cast(business_date as string) as business_date --  日期
    ,store_id                          -- 门店编码
    ,article_id                        -- 商品
    
    -- 供应链到店毛利率
    ,sum(scm_fin_article_income) as scm_fin_article_income            -- 总销售收入-财务(供应链到店毛利率分母)
    ,sum(scm_fin_article_profit) as scm_fin_article_profit            -- 供应链毛利-财务(供应链到店毛利额)
    
    ,sum(article_profit_amt) as article_profit_amt                -- 门店商品毛利额
    
    -- 全链路到店毛利率
    ,sum(scm_fin_article_profit+article_profit_amt) as  full_link_profit   -- 全链路到店毛利额
    ,sum(total_sale_amt) as total_sale_amt                    -- 销售总金额(全链路到店毛利率的分母)
    
    -- 供应链折让率
    ,sum(scm_promotion_amt_total) as scm_promotion_amt_total           -- 出库让利总额(供应链折让率分子)
    ,sum(out_stock_pay_amt) as out_stock_pay_amt                 -- 门店商品维度出库金额
    
    -- ,sum(out_stock_pay_amt + scm_promotion_amt_total)    -- 供应链折让率分母
    
    -- 定价毛利率
    ,sum(pre_sale_amt) as pre_sale_amt                     -- 理论销售额((实际销售数量+理论损耗数量)*销售原价)
    ,sum(pre_inbound_amount) as pre_inbound_amount         -- 理论进货额(bom处理后进货量*出库原价)
    -- ,(pre_sale_amt - pre_inbound_amount) /pre_sale_amt    -- 定价毛利率 

    ,sum(out_stock_amt_cb) as out_stock_amt_cb  -- 出库到店成本含税(出库到店成本)
    ,sum(expect_outstock_amt) as expect_outstock_amt  -- 预期出库金额(出库原价金额)
    ,inc_day     -- 取营业日期为日分区
    from dal_full_link.dal_manage_full_link_store_dc_article_info_di
    where inc_day between '{start_day}' and '{end_day}' 
    group by 
     cast(business_date as string)  
    ,store_id                          -- 门店编码
    ,article_id                        -- 商品
    ,inc_day     
    )t1 
    -- inner join (select * from dim.dim_store_profile where inc_day='{yesterday}' and (sp_level='170' or sp_store_id in ('A3N6') ) )t4 
    -- on t1.store_id=t4.sp_store_id  
    inner join (select * from dim.dim_chdj_store_list_di where inc_day  between '{start_day}' and '{end_day}'  )t4 
    on t1.store_id=t4.store_id  and t1.inc_day=t4.inc_day
    left join (  -- 先打上 非日清标签
    select 
     business_date       -- 盘点日期                
    ,store_id              -- 门店编号                
    ,article_id            -- 商品编号                                         
    from dim.dim_day_clear_article_list_di 
    where inc_day between '{start_day}' and '{end_day}'
    )t5 
    on t1.business_date=t5.business_date and t1.store_id=t5.store_id and t1.article_id=t5.article_id