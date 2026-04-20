    set catalog hive;
     
    -- 非日清门店商品汇总宽表
    insert overwrite hive.dal.dal_transaction_non_daily_store_article_sale_info_di partition(inc_day)
 select
     m1.business_date             -- '营业日期'
    ,m1.store_id                  -- '门店编码'
    ,m1.day_clear                 -- 非日清标识(1:不日清,0：日清)
    ,m1.article_id                -- '商品编码'
    ,m1.receive_qty               -- '进货数量'    
    ,m1.receive_amt               -- '进货金额'
    ,m1.sale_amt                  -- '实际销售额'
    ,m1.sale_qty                  -- '销售数量'
    ,m1.online_sale_amt           -- '线上销售额'
    ,m1.offline_sale_amt          -- '线下销售额'
    ,m1.offline_sale_qty          -- '线下销售数量'
    ,m1.online_sale_qty           -- '线上销售数量'
    ,m1.bf19_sale_qty             -- '19点前销售数量'
    ,m1.offline_sale_qty-m1.af19_offline_qty as bf19_offline_qty  -- '19点前线下销售数量'
    ,m1.bf19_offline_sale_amt     -- '线下19点前销售额'
    ,m1.bf19_sale_amt             -- '19点前销售金额'
    ,m1.af19_sale_qty             -- '19点后销售数量'
    ,m1.af19_sale_amt             -- '19点后销售金额'
    ,m1.sale_amt-(m1.receive_amt+ m1.compose_in_amt  - m1.compose_out_amt  )  + (m1.end_stock_amt- m1.init_stock_amt)  as profit_amt   -- '毛利额'
    ,m1.know_lost_amt+m1.unknow_lost_amt as  lost_amt    -- '损耗额'      
    ,m1.know_lost_qty             -- '已知损耗数量'
    ,m1.know_lost_amt             -- '已知损耗额'
    ,m1.unknow_lost_qty           -- '未知损耗数量'
    ,m1.unknow_lost_amt           -- '未知损耗额'
    ,m1.discount_amt              -- '折扣额'
    ,m1.member_discount_amt       -- '会员折扣额'
    ,m1.discount_amt-m1.hour_discount_amt  as promotion_discount_amt -- '促销折扣额'
    ,m1.hour_discount_amt         -- '时段折扣额'
    ,m1.return_sale_qty           -- '顾客退货数量'
    ,m1.return_sale_amt           -- '顾客退货额'
    ,m1.last_sysdate              -- '最后交易时间'
    ,m1.allowance_amt             -- '补贴款'
    ,m1.sale_amt-m1.receive_amt+m1.allowance_amt + (m1.end_stock_amt- m1.init_stock_amt) as allowance_amt_profit      -- '补贴后毛利额'
    ,m6.shop_promo_amt as shop_promotion_amt        -- '门店发起活动促销额' 
    ,m1.original_price_sale_amt   -- '原价销售额' 
    ,m1.actual_amt                -- '实付金额(不含运费)' 
    ,coalesce(m6.member_coupon_shop_amt,0)+m1.store_paylevel_discount as member_coupon_shop_amt    -- '会员券(门店)' 
    ,m6.member_promo_amt as member_promotion_amt      -- '会员活动促销费' 
    ,m6.member_coupon_company_amt -- '会员券费用(公司)' 
    ,m6.promo_amt as promotion_amt             -- '总会员券费用(会员券+会员活动促销)'                                          
    ,m1.discount_amt-m1.hour_discount_amt -m1.member_discount_amt  as nomember_discount_amt -- '非会员折扣额'
    ,m1.member_sale_amt         -- '会员销售额' 
    ,m1.bf19_member_sale_amt    -- '19点前会员销售额' 
    ,m1.sale_weight
    ,m1.init_stock_qty          -- '期初库存数量' 
    ,m1.end_stock_qty           -- '期库末存数量' 
    ,m1.init_stock_amt          -- '期初库存金额' 
    ,m1.end_stock_amt           -- '期初库存金额'
    ,m1.offline_original_amt
    ,case when m1.day_clear='1' and m7.cost_price is not null then m7.cost_price 
      else (case when round((m1.receive_qty+m1.init_stock_qty+m1.compose_in_qty-m1.compose_out_qty),3)=0 then 0
	             else (coalesce(m1.init_stock_amt,0)+m1.receive_amt+m1.compose_in_amt-m1.compose_out_amt)/(m1.receive_qty+m1.init_stock_qty+m1.compose_in_qty-m1.compose_out_qty) end) 
	  end as avg_purchase_price  -- 平均进货价
    ,m1.bf12_sale_qty
    ,m1.bf12_sale_amt
    
    ,coalesce(m6.no_ordercoupon_company_promotion_amt,0)  as no_ordercoupon_company_promotion_amt -- 公司承担非券的优惠金额
    ,coalesce(m6.ordercoupon_shop_promotion_amt,0)+m1.store_paylevel_discount as ordercoupon_shop_promotion_amt              -- 门店承担优惠券的优惠金额(成本费用额(门店))
    ,coalesce(m6.ordercoupon_company_promotion_amt,0)  as ordercoupon_company_promotion_amt       -- 公司承担优惠券的优惠金额
    ,coalesce(m6.no_ordercoupon_company_promotion_amt,0)+coalesce(m6.ordercoupon_company_promotion_amt,0) as company_cost_amt
    ,m1.sale_piece_qty  -- 销售件数
    ,m1.bf19_sale_piece_qty  -- 19点前销售件数
    ,m1.store_paylevel_discount      -- 门店支付级优惠金额
    ,m1.company_paylevel_discount  -- 公司支付级优惠金额
    ,cast(current_timestamp as string) as tj_time --数据统计时间
	,m1.compose_in_qty     --加工转换入数量
	,m1.compose_in_amt     --加工转换入金额
	,m1.compose_out_qty    --加工转换出数量
	,m1.compose_out_amt    --加工转换出金额
    ,m1.inc_day
    from 
    (
    select 
     inc_day
    ,store_id 
    ,business_date            
    ,article_id  
    ,SUM(sales_amount)  as sale_amt
    ,sum(sale_qty) as sale_qty
    ,SUM(discount_amount)     as discount_amt 
    ,SUM(member_discount_amt) as member_discount_amt     
    ,SUM(hour_discount_amt)   as hour_discount_amt   
    ,SUm(bf19_sales_qty) as bf19_sale_qty
    ,SUM(bf19_sales_amt) as bf19_sale_amt
    ,SUM(offline_bf19_sales_amt)  as bf19_offline_sale_amt  
    ,sum(af19_sales_qty)  as af19_sale_qty
    ,sum(af19_offline_qty) as af19_offline_qty
    ,SUM(af19_sales_amt)  as af19_sale_amt
    ,sum(return_sale_qty) as return_sale_qty    -- 02退货数    
    ,SUM(return_sale_amt) as return_sale_amt 
    ,SUM(inbound_amount) as receive_amt
    ,SUM(inbound_qty) as receive_qty
    ,max(last_sysdate) as last_sysdate        -- 时段最后交易时间
    ,sum(online_sales_amt) as online_sale_amt        -- 线上销售额(全天)
    ,sum(sales_amount)-sum(online_sales_amt) as offline_sale_amt -- 线下销售额
    ,sum(online_sale_qty) as online_sale_qty        -- 线上销售数量
    ,sum(sale_qty)-sum(online_sale_qty) as offline_sale_qty  -- 线下销售数量
    ,sum(actual_amount)-(sum( f_sub_amt)+sum(f_promo_sub_amt)) as actual_amt  -- 实付金额(不含运费)
    ,sum(p_lp_sub_amt) as original_price_sale_amt
    ,sum(unknown_wastage_qty) as unknow_lost_qty        -- 未知损耗数量
    ,sum(unknown_wastage_amount) as unknow_lost_amt  -- 未知损耗金额
    ,sum(scrap_amt) as know_lost_amt              -- 已知损耗金额
    ,sum(scrap_qty) as know_lost_qty              -- 已知损耗数量
    ,sum(allowance_amt) as allowance_amt  -- 门店补贴金额 
    
    ,sum(sale_weight) as sale_weight
    ,sum(member_sale_amt) as member_sale_amt  -- '会员销售额' 
    ,sum(bf19_member_sale_amt) as bf19_member_sale_amt  -- '19点前会员销售额' 
    ,sum(init_stock_qty) as init_stock_qty          -- '期初库存数量' 
    ,sum(end_stock_qty) as end_stock_qty          -- '期库末存数量' 
    ,sum(init_stock_amt) as init_stock_amt         -- '期初库存金额' 
    ,sum(end_stock_amt) as end_stock_amt          -- '期初库存金额'
    ,sum(offline_p_lp_sub_amt) as offline_original_amt
    
    ,sum(bf12_sale_qty) as bf12_sale_qty
    ,sum(bf12_sale_amt) as bf12_sale_amt
    
    ,sum(sale_piece_qty) as sale_piece_qty  -- 销售件数
    ,sum(bf19_sale_piece_qty) as bf19_sale_piece_qty  -- 19点前销售件数
    ,sum(store_paylevel_discount) as store_paylevel_discount      -- 门店支付级优惠金额
    ,sum(company_paylevel_discount) as company_paylevel_discount  -- 公司支付级优惠金额
    ,day_clear
    ,sum(compose_in_qty)as  compose_in_qty  --加工转换入数量
    ,sum(compose_in_amt)as  compose_in_amt  --加工转换入金额
    ,sum(compose_out_qty)as compose_out_qty --加工转换出数量
    ,sum(compose_out_amt)as compose_out_amt --加工转换出金额
	
	
    from 
    (
    select 
     m1.inc_day
    ,m1.store_id  
    ,m1.inc_day  as business_date                      
    ,m1.abi_article_id as article_id
    ,sum(sales_amt) sales_amount       -- 销售额
    ,sum(qty_spec) sale_qty
    ,sum(discount_amt) discount_amount    -- 折扣额
    ,sum(vip_discount_amt) member_discount_amt -- 会员折扣额
    ,sum(hour_discount_amt) hour_discount_amt -- 时段折扣额
    ,sum(qty_spec - coalesce(af19_sales_qty*spec_num,0)) as bf19_sales_qty
    ,sum(coalesce(sales_amt,0) - coalesce(af19_sales_amt,0)) as bf19_sales_amt
    ,sum(case when online_flag = 'N' then coalesce(sales_amt,0) - coalesce(af19_sales_amt,0) else 0 end)  as offline_bf19_sales_amt   -- 线下19点前销售额   
    ,sum(af19_sales_qty*spec_num) as af19_sales_qty
    ,sum(case when online_flag = 'N' then af19_sales_qty*spec_num else 0 end) as af19_offline_qty  -- 19点后线下销售数量
    ,sum(af19_sales_amt) af19_sales_amt
    ,sum(case when online_flag = 'N' then af19_sales_amt else 0 end) offline_af19_sales_amt  -- 线下19点后销售额 
    ,sum(case when online_flag = 'Y' then qty*spec_num else 0 end) online_sale_qty  -- 线上销售数量
    ,sum(return_sale_qty) return_sale_qty   -- 02退货数    
    ,sum(return_sale_amt) return_sale_amt   -- 02退货额   
    ,sum(case when online_flag = 'Y' then sales_amt else 0 end) as online_sales_amt    
    ,0 as inbound_amount
    ,0 as inbound_qty
    ,cast(max(pay_at) as string) as last_sysdate     -- 时段最后交易时间
    ,sum(actual_amount) as actual_amount
    ,sum(p_lp_sub_amt) as p_lp_sub_amt
    ,sum(f_sub_amt) as f_sub_amt
    ,sum(f_promo_sub_amt) as f_promo_sub_amt
    ,0 as allowance_amt  -- 门店补贴金额 
    
    ,sum(case when m2.sale_unit='千克' or coalesce(m2.unit_weight,0)=0 then m1.qty_spec  else m1.qty_spec*m2.unit_weight end) as sale_weight
    ,sum(case when customer_phone<>'' and customer_phone is not null then sales_amt else 0 end ) as member_sale_amt  -- '会员销售额' 
    ,sum(case when customer_phone<>'' and customer_phone is not null then coalesce(sales_amt,0) - coalesce(af19_sales_amt,0) else 0 end ) as bf19_member_sale_amt  -- '19点前会员销售额' 
    
    ,0 as init_stock_qty          -- '期初库存数量' 
    ,0 as end_stock_qty           -- '期库末存数量' 
    ,0 as init_stock_amt          -- '期初库存金额' 
    ,0 as end_stock_amt           -- '期初库存金额'
    ,0 as inventory_cost    -- 库存成本
    ,sum(case when online_flag='N' then p_lp_sub_amt else 0 end) as offline_p_lp_sub_amt
    
    ,sum(case when trans_hour<'12' then m1.qty*m1.spec_num else 0 end) as bf12_sale_qty
    ,sum(case when trans_hour<'12' then m1.sales_amt else 0 end) as bf12_sale_amt
    ,sum(qty) as sale_piece_qty  -- 销售件数
    ,sum(qty)-sum(af19_sales_qty) as bf19_sale_piece_qty  -- 19点前销售件数
    ,sum(store_paylevel_discount) as store_paylevel_discount      -- 门店支付级优惠金额
    ,sum(company_paylevel_discount) as company_paylevel_discount  -- 公司支付级优惠金额
    ,0 as unknown_wastage_qty     --  未知损耗数量
    ,0 as unknown_wastage_amount  --  未知损耗金额
    ,0 as scrap_amt               --  已知损耗金额
    ,0 as scrap_qty               --  已知损耗数量
    ,m1.day_clear                             -- 非日清标识(1:不日清,0：日清)
    ,0 as compose_in_qty
    ,0 as compose_in_amt 
    ,0 as compose_out_qty 
    ,0 as compose_out_amt
    from (
    select 
    business_date                          -- 营业日期
    ,store_id                              -- 门店编码
    ,is_hour_promotion                     -- 是否时段促销 1是
    ,abi_article_id                        -- 商品编码
    ,online_flag                           -- 线上标识
    ,spec_num                              -- 实际规格数
    ,customer_id                           -- 会员id
    ,customer_name                         -- 用户名 昵称
    ,customer_phone                        -- 用户注册手机号
    ,sale_unit                             -- 销售单位（千克盒）
    ,order_type                            -- 订单类型
    ,order_sub_type                        -- 订单子类型 delivery:配送 1 selfpick:自提 2 scancode:扫码购 4 pos:收银机 8
    ,channel_id                            -- 渠道号
    ,inc_time                              -- 日结时间
    ,substr(inc_time,12,2) as trans_hour   -- '交易时间（小时）'
    ,case when online_flag='N' then pay_at else null end as pay_at -- 支付时间
    ,refund_at                             -- 订单退款时间
    ,refund_type                           -- 退款类型，整单退，部分退，整单拒收，部分拒收
    ,order_at                              -- 订单生成时间
    ,allrefund_time                        -- 完全退款时间
    ,qty                                   -- 商品数量信息 标品为份数散称为1,规格商品为实际份数
    ,qty_spec                              -- 商品数量信息(规格*份数)
    ,p_lp_sub_amt                          -- 商品总标价 listPrice*qty
    ,p_sp_sub_amt                          -- 商品总售价 salePrice*qty
    ,sales_amt                             -- 日结金额
    ,discount_amt                          -- 折扣额
    ,vip_discount_amt                      -- 会员折扣额
    ,hour_discount_amt                     -- 时段折扣额
    ,return_sale_qty                       -- 退货数量
    ,return_sale_amt                       -- 退货额
    ,member_hour_sales_amt                 -- 会员时段销售额
    ,af19_sales_amt                        -- 19点后销售额
    ,af19_sales_qty                        -- 19点后销售数量
    ,shop_promo_sub_amt                    -- 门店承担优惠-同步
    ,promotion_amt                         -- 线下表示门店与平台优惠金额 线上表示门店承担非券优惠
    ,gift_qty                              -- 赠品数量
    ,promotion_amt_shop                    -- 门店承担促销费用
    ,promotion_amt_platform                -- 平台承担促销费用
    ,actual_amount                         -- 实付金额
    ,gmv                                   -- 应付金额(含运费)
    ,gmv1                                  -- 应付金额(不含运费)
    ,jielong_flag                          -- community线上接龙,shop门店接龙 -非接龙
    ,f_sub_amt
    ,f_promo_sub_amt
    ,gift_gmv                              -- 赠品gmv
    ,postage_shop                          -- 门店承担邮费
    ,postage_platform                      -- 平台承担邮费
    ,business_source                       -- 订单业务来源0默认1团长订单
    ,activity_code                         -- 活动编号
    ,p_mp_sub_amt                          -- 商品中台总价
    ,store_paylevel_discount               -- 门店支付级优惠金额
    ,company_paylevel_discount             -- 公司支付级优惠金额
    ,day_clear                             -- 非日清标识(1:不日清,0：日清)

    ,inc_day
    from dsl.dsl_transaction_non_daily_store_order_details_di
    where inc_day between '{start_day}' and '{end_day}'  -- 筛选营业日期范围
    )m1
    left join (select * from dim.dim_goods_information_have_pt where inc_day='{yesterday}')m2
    on m1.abi_article_id=m2.article_id 
    where coalesce(m2.category_level1_id,'rd') not in('91')   --  过滤掉便捷卡
    and ((m2.category_level1_id not in ('70','71','72','73','74','75','76','77') and m1.online_flag='N') or m1.online_flag='Y')  
    group by 
     m1.inc_day
    ,m1.store_id
    ,m1.abi_article_id
    ,m1.day_clear
    
    union all
    select 
     m1.inc_day
    ,m1.store_id  
    ,m1.inc_day as business_date                      
    ,m1.sale_article_id as article_id
    ,0 as sales_amount       -- 销售额
    ,0 as sale_qty
    ,0 as discount_amount    -- 折扣额
    ,0 as member_discount_amt -- 会员折扣额
    ,0 as hour_discount_amt -- 时段折扣额
    ,0 as bf19_sales_qty
    ,0 as bf19_sales_amt
    ,0 as offline_bf19_sales_amt   -- 线下19点前销售额   
    ,0 as af19_sales_qty
    ,0 as af19_offline_qty  -- 19点后线下销售数量
    ,0 as af19_sales_amt
    ,0 as offline_af19_sales_amt  -- 线下19点后销售额 
    ,0 as online_sale_qty  -- 线上销售数量
    ,0 as return_sale_qty   -- 02退货数    
    ,0 as return_sale_amt   -- 02退货额   
    ,0 as online_sales_amt    
    ,sum(m1.sale_article_purchase_amt) as inbound_amount
    ,sum(m1.sale_article_qty) as inbound_qty
    ,null as last_sysdate     -- 时段最后交易时间
    ,0 as actual_amount
    ,0 as p_lp_sub_amt
    ,0 as f_sub_amt
    ,0 as f_promo_sub_amt
    ,0 as allowance_amt  -- 门店补贴金额 
    
    ,0 as sale_weight
    ,0 as member_sale_amt  -- '会员销售额' 
    ,0 as bf19_member_sale_amt  -- '19点前会员销售额' 
    
    ,sum(m1.init_stock_qty) as init_stock_qty          -- '期初库存数量' 
    ,sum(m1.end_stock_qty ) as end_stock_qty          -- '期库末存数量' 
    ,sum(m1.init_stock_amt) as init_stock_amt          -- '期初库存金额' 
    ,sum(m1.end_stock_amt ) as end_stock_amt          -- '期初库存金额'
    ,sum(m1.inventory_cost) as inventory_cost    -- 库存成本
    ,0 as offline_p_lp_sub_amt
    
    ,0 as bf12_sale_qty
    ,0 as bf12_sale_amt
    ,0 as sale_piece_qty  -- 销售件数
    ,0 as bf19_sale_piece_qty  -- 19点前销售件数
    ,0 as store_paylevel_discount      -- 门店支付级优惠金额
    ,0 as company_paylevel_discount  -- 公司支付级优惠金额
    
    ,0 as unknown_wastage_qty     --  未知损耗数量
    ,0 as unknown_wastage_amount  --  未知损耗金额
    ,0 as scrap_amt               --  已知损耗金额
    ,0 as scrap_qty               --  已知损耗数量
    
    ,m1.day_clear                             -- 非日清标识(1:不日清,0：日清)
    ,0 as compose_in_qty
    ,0 as compose_in_amt 
    ,0 as compose_out_qty 
    ,0 as compose_out_amt
    from (
    select 
    business_date                          -- 营业日期
    ,store_id                              -- 门店编码
    ,day_clear                             -- 非日清标识(1:不日清,0：日清)
    ,sale_article_id                       -- BOM子商品(销售条码)
    ,sale_article_name                     -- 销售商品名称
    ,sale_article_qty                      -- 销售条码转换量
    ,sale_article_purchase_amt             -- 销售条码理论进货金额
    ,init_stock_qty                        -- 期初库存数量
    ,end_stock_qty                         -- 期末库存数量
    ,init_stock_amt                        -- 期初库存金额
    ,end_stock_amt                         -- 期末库存金额
    ,inventory_cost                        -- 库存成本
    ,inc_day                               -- 增量日 (partition key)
    from dsl.dsl_transaction_non_daily_store_article_purchase_di
    where inc_day between '{start_day}' and '{end_day}'  -- 筛选增量日范围
    )m1 
    left join (select * from dim.dim_goods_information_have_pt where inc_day='{yesterday}')m2 
    on m1.sale_article_id=m2.article_id
    where m2.category_level1_id  not in ( '70','71','72','73','74','75','76','77')    --  2019年7月19日 添加 去除物料
    group by 
     m1.inc_day
    ,m1.store_id
    ,m1.sale_article_id
    ,m1.day_clear
    
    union all -- 获取补贴
    select 
     m1.inc_day
    ,m1.store_id  
    ,m1.inc_day as business_date                      
    ,m1.sale_article_id as article_id
    ,0 as sales_amount       -- 销售额
    ,0 as sale_qty
    ,0 as discount_amount    -- 折扣额
    ,0 as member_discount_amt -- 会员折扣额
    ,0 as hour_discount_amt -- 时段折扣额
    ,0 as bf19_sales_qty
    ,0 as bf19_sales_amt
    ,0 as offline_bf19_sales_amt   -- 线下19点前销售额   
    ,0 as af19_sales_qty
    ,0 as af19_offline_qty  -- 19点后线下销售数量
    ,0 as af19_sales_amt
    ,0 as offline_af19_sales_amt  -- 线下19点后销售额 
    ,0 as online_sale_qty  -- 线上销售数量
    ,0 as return_sale_qty   -- 02退货数    
    ,0 as return_sale_amt   -- 02退货额   
    ,0 as online_sales_amt    
    ,0 as inbound_amount
    ,0 as inbound_qty
    ,null as last_sysdate     -- 时段最后交易时间
    ,0 as actual_amount
    ,0 as p_lp_sub_amt
    ,0 as f_sub_amt
    ,0 as f_promo_sub_amt
    ,sum(m1.split_allowance_amt) as allowance_amt  -- 门店补贴金额 
    
    ,0 as sale_weight
    ,0 as member_sale_amt       -- '会员销售额' 
    ,0 as bf19_member_sale_amt  -- '19点前会员销售额' 
    
    ,0 as init_stock_qty          -- '期初库存数量' 
    ,0 as end_stock_qty          -- '期库末存数量' 
    ,0 as init_stock_amt          -- '期初库存金额' 
    ,0 as end_stock_amt          -- '期初库存金额'
    ,0 as inventory_cost    -- 库存成本
    ,0 as offline_p_lp_sub_amt
    
    ,0 as bf12_sale_qty
    ,0 as bf12_sale_amt
    ,0 as sale_piece_qty  -- 销售件数
    ,0 as bf19_sale_piece_qty  -- 19点前销售件数
    ,0 as store_paylevel_discount      -- 门店支付级优惠金额
    ,0 as company_paylevel_discount  -- 公司支付级优惠金额
    
    ,0 as unknown_wastage_qty     --  未知损耗数量
    ,0 as unknown_wastage_amount  --  未知损耗金额
    ,0 as scrap_amt               --  已知损耗金额
    ,0 as scrap_qty               --  已知损耗数量
    
    ,case when m3.article_id is not null then '1' else '0' end as day_clear   -- 非日清标识(1:不日清,0：日清)
    ,0 as compose_in_qty
    ,0 as compose_in_amt 
    ,0 as compose_out_qty 
    ,0 as compose_out_amt
    from
    (select 
     business_date        -- 营业日期
    ,store_id             -- 门店编码
    ,sale_article_id      -- 销售条码
    ,sum(split_allowance_amt) as split_allowance_amt  -- 拆分后补贴额
    ,inc_day              -- 日分区
    from dal.dal_activity_article_order_sale_info_di 
    where inc_day between '{start_day}' and '{end_day}'
    group by 
     business_date        -- 营业日期
    ,store_id             -- 门店编码
    ,sale_article_id      -- 销售条码
    ,inc_day              -- 日分区
    )m1 
    -- inner join (select * from dim.dim_store_profile where inc_day='{yesterday}' and (sp_level='170' or sp_store_id in ('A3N6')))m2 
    -- on m1.store_id=m2.sp_store_id  
    inner join (select * from dim.dim_chdj_store_list_di where inc_day  between '{start_day}' and '{end_day}'  )m2
    on m1.store_id=m2.store_id  and m1.inc_day=m2.inc_day
    left join (  -- 先打上 非日清标签
    select 
     business_date       -- 盘点日期                
    ,store_id              -- 门店编号                
    ,article_id            -- 商品编号                                         
    from dim.dim_day_clear_article_list_di 
    where inc_day between '{start_day}' and '{end_day}'
    )m3 
    on m1.business_date=m3.business_date and m1.store_id=m3.store_id and m1.sale_article_id=m3.article_id
    group by 
     m1.inc_day
    ,m1.store_id  
    ,m1.inc_day               
    ,m1.sale_article_id  
    ,case when m3.article_id is not null then '1' else '0' end  
    
    union all  -- 获取损耗
    select 
     m1.inc_day
    ,m1.shop_id as store_id  
    ,m1.inc_day as business_date                      
    ,m1.sku_code as article_id
    ,0 as sales_amount       -- 销售额
    ,0 as sale_qty
    ,0 as discount_amount    -- 折扣额
    ,0 as member_discount_amt -- 会员折扣额
    ,0 as hour_discount_amt -- 时段折扣额
    ,0 as bf19_sales_qty
    ,0 as bf19_sales_amt
    ,0 as offline_bf19_sales_amt   -- 线下19点前销售额   
    ,0 as af19_sales_qty
    ,0 as af19_offline_qty  -- 19点后线下销售数量
    ,0 as af19_sales_amt
    ,0 as offline_af19_sales_amt  -- 线下19点后销售额 
    ,0 as online_sale_qty  -- 线上销售数量
    ,0 as return_sale_qty   -- 02退货数    
    ,0 as return_sale_amt   -- 02退货额   
    ,0 as online_sales_amt    
    ,0 as inbound_amount
    ,0 as inbound_qty
    ,null as last_sysdate     -- 时段最后交易时间
    ,0 as actual_amount
    ,0 as p_lp_sub_amt
    ,0 as f_sub_amt
    ,0 as f_promo_sub_amt
    ,0 as allowance_amt  -- 门店补贴金额 
    
    ,0 as sale_weight
    ,0 as member_sale_amt       -- '会员销售额' 
    ,0 as bf19_member_sale_amt  -- '19点前会员销售额' 
    
    ,0 as init_stock_qty          -- '期初库存数量' 
    ,0 as end_stock_qty           -- '期库末存数量' 
    ,0 as init_stock_amt          -- '期初库存金额' 
    ,0 as end_stock_amt           -- '期初库存金额'
    ,0 as inventory_cost          -- 库存成本
    ,0 as offline_p_lp_sub_amt
    
    ,0 as bf12_sale_qty
    ,0 as bf12_sale_amt
    ,0 as sale_piece_qty       -- 销售件数
    ,0 as bf19_sale_piece_qty  -- 19点前销售件数
    ,0 as store_paylevel_discount      -- 门店支付级优惠金额
    ,0 as company_paylevel_discount    -- 公司支付级优惠金额
    ,m1.unknown_wastage_qty     --  未知损耗数量
    ,m1.unknown_wastage_amount  --  未知损耗金额
    ,m1.scrap_amt               --  已知损耗金额
    ,m1.scrap_qty               --  已知损耗数量
    ,case when m3.article_id is not null then '1' else '0' end as day_clear   -- 非日清标识(1:不日清,0：日清)
    ,0 as compose_in_qty
    ,0 as compose_in_amt 
    ,0 as compose_out_qty 
    ,0 as compose_out_amt

    from
    (select 
    inc_day
    ,store_id as shop_id 
    ,article_id as sku_code 
    ,sum(unknow_lost_qty) as unknown_wastage_qty        --  未知损耗数量
    ,sum(unknow_lost_amt) as unknown_wastage_amount  --  未知损耗金额
    ,sum(know_lost_amt) as scrap_amt           --  已知损耗金额
    ,sum(know_lost_qty) as scrap_qty              --  已知损耗数量
    from dal.dal_transaction_store_article_lost_di
    where inc_day between '{start_day}' and '{end_day}'
    and category_level1_id not in ('70','71','98','72','73','74','75','76','77')
    group by 
    inc_day
    ,store_id 
    ,article_id 
    )m1 
    -- inner join (select * from dim.dim_store_profile where inc_day='{yesterday}' and (sp_level='170' or sp_store_id in ('A3N6')))m2 
    -- on m1.shop_id=m2.sp_store_id  
    inner join (select * from dim.dim_chdj_store_list_di where inc_day  between '{start_day}' and '{end_day}'  )m2
    on m1.shop_id=m2.store_id  and m1.inc_day=m2.inc_day
    left join (  -- 先打上 非日清标签
    select 
     business_date       -- 盘点日期                
    ,store_id              -- 门店编号                
    ,article_id            -- 商品编号                                         
    from dim.dim_day_clear_article_list_di 
    where inc_day between '{start_day}' and '{end_day}'
    )m3 
    on m1.inc_day=m3.business_date and m1.shop_id=m3.store_id and m1.sku_code=m3.article_id
    
	union all  
	
	    select 
     t1.business_date  as inc_day
    ,t1.store_id  
    ,t1.business_date                      
    ,t1.article_id
    ,0 as sales_amount       -- 销售额
    ,0 as sale_qty
    ,0 as discount_amount    -- 折扣额
    ,0 as member_discount_amt -- 会员折扣额
    ,0 as hour_discount_amt -- 时段折扣额
    ,0 as bf19_sales_qty
    ,0 as bf19_sales_amt
    ,0 as offline_bf19_sales_amt   -- 线下19点前销售额   
    ,0 as af19_sales_qty
    ,0 as af19_offline_qty  -- 19点后线下销售数量
    ,0 as af19_sales_amt
    ,0 as offline_af19_sales_amt  -- 线下19点后销售额 
    ,0 as online_sale_qty  -- 线上销售数量
    ,0 as return_sale_qty   -- 02退货数    
    ,0 as return_sale_amt   -- 02退货额   
    ,0 as online_sales_amt    
    ,0 as inbound_amount
    ,0 as inbound_qty
    ,0 as last_sysdate     -- 时段最后交易时间
    ,0 as actual_amount
    ,0 as p_lp_sub_amt
    ,0 as f_sub_amt
    ,0 as f_promo_sub_amt
    ,0 as allowance_amt  -- 门店补贴金额 
    ,0 as sale_weight
    ,0 as member_sale_amt       -- '会员销售额' 
    ,0 as bf19_member_sale_amt  -- '19点前会员销售额' 
    
    ,0 as init_stock_qty          -- '期初库存数量' 
    ,0 as end_stock_qty           -- '期库末存数量' 
    ,0 as init_stock_amt          -- '期初库存金额' 
    ,0 as end_stock_amt           -- '期初库存金额'
    ,0 as inventory_cost          -- 库存成本
    ,0 as offline_p_lp_sub_amt
    
    ,0 as bf12_sale_qty
    ,0 as bf12_sale_amt
    ,0 as sale_piece_qty       -- 销售件数
    ,0 as bf19_sale_piece_qty  -- 19点前销售件数
    ,0 as store_paylevel_discount      -- 门店支付级优惠金额
    ,0 as company_paylevel_discount    -- 公司支付级优惠金额
    ,0 as unknown_wastage_qty     --  未知损耗数量
    ,0 as unknown_wastage_amount  --  未知损耗金额
    ,0 as scrap_amt               --  已知损耗金额
    ,0 as scrap_qty               --  已知损耗数量
    ,case when t5.article_id is not null then '1' else '0' end as day_clear
    ,t1.compose_in_qty
    ,t1.compose_in_amt 
    ,t1.compose_out_qty 
    ,t1.compose_out_amt

    from (	select business_date ,store_id ,article_id,article_name , compose_in_qty,compose_in_amt ,compose_out_qty ,compose_out_amt   from dsl.dsl_transaction_sotre_article_compose_info_di
	        where  inc_day between '{start_day}' and '{end_day}' )t1
         left join (
        select 
        business_date       -- 盘点日期                
       ,store_id              -- 门店编号                
       ,article_id            -- 商品编号                                         
       from dim.dim_day_clear_article_list_di 
       where inc_day between '{start_day}' and '{end_day}'
       )t5 
    on t1.business_date=t5.business_date and t1.store_id=t5.store_id and t1.article_id=t5.article_id
 
    
    )m0
    group by 
     inc_day
    ,store_id
    ,business_date 
    ,article_id
    ,day_clear
    
    )m1 
    
    left join (select * from dim.dim_goods_information_have_pt where inc_day='{yesterday}') m5
    on m1.article_id=m5.article_id
    left join (
    select 
     t1.business_date
    ,t1.store_id             -- '主键'
    ,t1.article_id            -- '商品原始编辑 如线上场景 goodsi_id对应生成商品idsku_code对应基础库真实商品编码'
    ,sum(t1.p_promo_amt) as p_promo_amt           -- 商品优惠金额
    ,sum(t1.f_promo_amt) as f_promo_amt           -- 运费优惠金额
    ,sum(t1.promotion_cost) as  promotion_cost                   -- 促销扣款
    ,sum(t1.member_coupon_shop_amt) as member_coupon_shop_amt    -- 会员券(门店)   
    ,sum(t1.member_promo_amt) as member_promo_amt                -- 会员活动促销费
    ,sum(t1.member_coupon_company_amt) as member_coupon_company_amt
    ,sum(t1.promo_amt) as promo_amt
    ,sum(case when t2.promotion_code is not null then t1.p_promo_amt+t1.f_promo_amt else 0 end) as shop_promo_amt
    
    ,sum(t1.no_ordercoupon_company_promotion_amt) as no_ordercoupon_company_promotion_amt -- 公司承担非券的优惠金额                              
    ,sum(t1.ordercoupon_shop_promotion_amt) as ordercoupon_shop_promotion_amt       --  门店承担优惠券的优惠金额(成本费用额(门店))
    ,sum(t1.ordercoupon_company_promotion_amt) as ordercoupon_company_promotion_amt    -- 公司承担优惠券的优惠金额 
    ,case when t5.article_id is not null then '1' else '0' end as day_clear
    ,t1.inc_day
    from 
    (select  
    inc_day as business_date
    ,shop_id as store_id-- '主键'
    ,sku_code as article_id -- '商品原始编辑 如线上场景 goodsi_id对应生成商品idsku_code对应基础库真实商品编码'
    ,promotion_code2
    ,sum(coalesce(p_promo_amt,0)  ) as p_promo_amt                                                                                      -- 商品优惠金额
    ,sum(coalesce(f_promo_amt,0)  ) as f_promo_amt                                                                                      -- 运费优惠金额
    ,sum(coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0)) as  store_promotion_cost                                                      -- 门店_营销成本
    ,sum(case when cost_center in ('113000') then coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0) else 0 end) as online_bear_cost       -- 电商运营_营销成本分摊(113000)
    ,sum(case when cost_center in ('106005') then coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0) else 0 end) as market_bear_cost       -- 市场部_营销成本分摊(106005)
    ,sum(case when cost_center in ('shop') then coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0) else 0 end) as store_bear_cost          -- 门店_营销成本分摊(shop)
    ,sum(case when cost_center in ('platform') then coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0) else 0 end) as platform_bear_cost   -- 平台_营销成本分摊(platform)
    ,sum(case when cost_center in ('121002') then coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0) else 0 end) as service_bear_cost      -- 客服部_营销成本分摊(121002)
    ,sum(case when cost_center in ('123000') then coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0) else 0 end) as  bear_cost_123000      -- 品牌中心_营销成本分摊(123000)
    ,sum(case when cost_center in ('999998') then coalesce(p_promo_amt,0)+coalesce(f_promo_amt,0) else 0 end) as supplier_bear_cost     -- 供应商_营销成本分摊(999998)
    ,sum(promotion_cost) as  promotion_cost                                                                                             -- 促销扣款
    
    -- 2023年9月12日 修改 member_coupon_shop_amt/member_promo_amt/member_coupon_company_amt 口径
    ,sum(case when cost_center in('shop') and promotion_category='rule' and promo_type in('OrderCoupon') and  order_type ='normal' and online_flag ='N'
            then p_promo_amt else 0 end) as member_coupon_shop_amt                                                                      --会员券(门店) 
    ,sum(case when cost_center not in('shop','vendor','customer')  
                 and promotion_category='rule' and promo_type in('OrderCoupon')
                 and ( substr(promo_ext_prop,3,2)='01') then p_promo_amt else 0 end)         
    + sum(case when promo_sub_type ='n.fold.point'   
                 and ( substr(coalesce(promo_ext_prop,''),3,2)='01') then p_promo_amt else 0 end) member_promo_amt --会员活动促销费
    ,sum(case when cost_center not  in('shop','vendor','customer') 
               and promotion_category='rule' 
               and promo_type in('OrderCoupon')  
               and  order_type ='normal'
               and online_flag ='N' 
               and ( substr(coalesce(promo_ext_prop,''),1,2)<>'01' and substr(coalesce(promo_ext_prop,''),3,2)<>'01' ) 
               then p_promo_amt else 0 end)  
    +sum(case when promo_sub_type ='n.fold.point' 
               and    order_type ='normal'
               and online_flag ='N'
               and ( substr(coalesce(promo_ext_prop,''),3,2)<>'01')  then p_promo_amt else 0 end) as member_coupon_company_amt  --会员券（公司）
    
    ,sum(case when promotion_category='rule' and promo_type='OrderCoupon' and cost_center  not in ('customer') then p_promo_amt else 0 end) as promo_amt
    
    ,sum(case when online_flag ='N' and coalesce(promotion_category,'') in ('rule')  and coalesce(promo_type,'')  in ('O','I','Exchange') and cost_center not in('shop','customer' )
            then p_promo_amt else 0 end) as no_ordercoupon_company_promotion_amt -- 公司承担非券的优惠金额                              
    ,sum(case when online_flag ='N' and  coalesce(promotion_category,'') in ('rule')  and coalesce(promo_type,'')  in ('OrderCoupon' ) and cost_center  = 'shop'
            then p_promo_amt else 0 end) as ordercoupon_shop_promotion_amt       --  门店承担优惠券的优惠金额(成本费用额(门店))
            
    ,sum(case when online_flag ='N' and  coalesce(promotion_category,'') in ('rule')  and coalesce(promo_type,'')  in ('OrderCoupon' ) and cost_center  not in('customer','shop' )
            then p_promo_amt else 0 end) as ordercoupon_company_promotion_amt    -- 公司承担优惠券的优惠金额   
    ,inc_day
    from dsl.dsl_promotion_order_item_article_sale_info_di
    where  inc_day between '{start_day}' and '{end_day}'
    group by 
    shop_id     -- '主键'
    ,promotion_code2
    ,sku_code    -- '商品原始编辑 如线上场景 goodsi_id对应生成商品idsku_code对应基础库真实商品编码'
    ,inc_day
    )t1 
    left join (
    select promotion_code  from  dim.dim_store_promotion_info_da  where  inc_day = '{yesterday}'     
	and act_type in('coupon','goods','orders','group_price')
    and p_source ='shop'
    )t2
    on t1.promotion_code2 = t2.promotion_code
    inner join (select * from dim.dim_goods_information_have_pt where inc_day='{yesterday}' and category_level1_id not in ('70','71','72','73','74','75','76','77'))t3 
    on t1.article_id=t3.article_id
    -- inner join (select * from dim.dim_store_profile where inc_day='{yesterday}' and (sp_level='170' or sp_store_id in ('A3N6')) )t4 
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
    group by 
     t1.business_date
    ,t1.store_id               -- '主键'
    ,t1.article_id             -- '商品原始编辑 如线上场景 goodsi_id对应生成商品idsku_code对应基础库真实商品编码' 
    ,case when t5.article_id is not null then '1' else '0' end  
    ,t1.inc_day
    
    )m6
    on m1.inc_day=m6.inc_day and m1.store_id=m6.store_id and m1.article_id=m6.article_id and m1.day_clear=m6.day_clear
    left join (
    select 
    shop_id
    ,sku_code
    ,inventory_date
    ,max(cost_price) as cost_price 
    from ods_sc_db.t_shop_inventory_sku_pool  
    where inc_day='{yesterday}' and inventory_date between '{start_day}' and '{end_day}'
    group by 
      shop_id
    ,sku_code
    ,inventory_date
    )m7
    on m1.store_id=m7.shop_id and m1.article_id=m7.sku_code and m1.business_date=m7.inventory_date
 
	
    where m1.inc_day between '{start_day}' and '{end_day}'
   
   
    ;