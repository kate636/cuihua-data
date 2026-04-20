# coding=UTF-8
# Doris数据监控任务
import subprocess
import pandas as pd
import codecs
import sys
import time
import datetime
import threading
import re
import subprocess as sp
from dateutil import parser
from retrying import retry
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# reload(sys)
# sys.setdefaultencoding('utf-8')

# 获取账号密码
def GetPassWord():
    fp_r=codecs.open('/opt/script/password/hive.sh','r',encoding='utf-8')
    content=fp_r.read()
    content_list=content.split("\n")
    print(content_list)
    account={}
    for i in range(4):
        key=content_list[i].split("=")[0]
        value=content_list[i].split("=")[1]
        account[key]=value
    return account;

# 日期分割处理 成集合  效果 [[开始时间1，结束时间1],[开始时间2,结束时间2]...]
def timepand(interval, rq1, rq2):
    interval2 = str(interval) + 'D'
    print('间隔时间为' + str(interval2))
    # 开始日期分割
    times = pd.date_range(start=rq1, end=rq2, freq=interval2)
    # 将结果集 转化为list
    time_list = list(times)
    end_day2 = datetime.strptime(rq2, '%Y-%m-%d')
    print('切割的日期为' + str(time_list))
    day_list = []
    # 如果切割日期>1 段
    if len(time_list) > 1:
        # 遍历 集合
        for i in range(len(time_list)):
            # 日期格式化
            time_format = time_list[i].strftime('%Y-%m-%d')
            # 假如为第一个起始值
            if i == 0:
                start_day = time_list[i]
                start_day_format = start_day.strftime('%Y-%m-%d')
                end_day = time_list[i] + timedelta(days=int(interval) - 1)
                end_day_format = end_day.strftime('%Y-%m-%d')
                day_list.append([start_day_format, end_day_format])
                # print(day_list)
            else:
                start_day = time_list[i]
                start_day_format = start_day.strftime('%Y-%m-%d')
                end_day = time_list[i] + timedelta(days=int(interval) - 1)
                # 分割后+7天 最后一个大于最后一个分割日期 并且大于所传日期
                if end_day >= time_list[-1] and end_day >= end_day2:
                    # 最后的日期大于 集合最后的日期 就取该值
                    end_day = end_day2
                else:
                    pass
                end_day_format = end_day.strftime('%Y-%m-%d')
                day_list.append([start_day_format, end_day_format])
    else:
        # 只有一段直接取
        day_list.append([rq1, rq2])
    print('最终结果集' + str(day_list))
    return day_list

# 损耗汇总2
def sql_format(start_day,end_day,yesterday):

    hive1_sql = """
    set catalog hive;
     -- 翠花当家门店商品销售条码维度数据集
    insert overwrite hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di  partition(inc_day)
    -- 门店毛利率:sale_profit_amt/sale_amt
    -- 预期毛利率:pre_profit_amt/original_price_sale_amt
    -- 定价毛利率:(pre_sale_amt - pre_inbound_amount) /pre_sale_amt 
    -- 时段折扣率:hour_discount_amt/original_price_sale_amt
    -- 促销折扣率:promotion_discount_amt/original_price_sale_amt
    -- 损耗率:lost_amt/receive_amt
    -- 供应链到店毛利率:scm_fin_article_profit/scm_fin_article_income
    -- 全链路到店毛利率:full_link_profit/total_sale_amt
    -- 供应链折让率:scm_promotion_amt_total/(scm_promotion_amt_total+out_stock_pay_amt)
    
    select 
     m1.business_date    -- 营业日期
    ,m1.store_id         -- 门店编码
    ,m1.day_clear        -- 非日清标识(1:不日清,0：日清)
    ,m1.article_id       -- 商品编码
    ,m1.receive_qty      -- 进货数量
    ,m1.receive_amt      -- 进货金额
    ,m1.sale_amt         -- 实际销售额
    -- ,m1.sale_amt-m1.sale_qty*m1.avg_purchase_price as sale_profit_amt  -- 销售毛利额
    ,m1.profit_amt as sale_profit_amt  -- 销售毛利额(2025-03-24 调整了口径)
    ,m1.profit_amt+m1.allowance_amt as sale_allowance_amt_profit  -- 补贴销售毛利额
    ,case when m1.day_clear='1' then m1.sale_qty*m1.avg_purchase_price else m1.receive_amt+m1.compose_in_amt-m1.compose_out_amt-m1.lost_amt end as sale_cost_amt  -- 销售成本金额
    -- ,m1.original_price_sale_amt-m1.receive_amt as pre_profit_amt  -- 预期毛利额
    ,m1.original_price_sale_amt-(case when m1.day_clear='1' then m1.sale_qty*m1.avg_purchase_price else m1.receive_amt-m1.lost_amt end) as pre_profit_amt  -- 预期毛利额(原价销售额-销售成本)
    ,m2.cust_num         -- 来客数/订单数
    ,m2.bf19_cust_num    -- 19点前来客数
    ,m1.sale_qty         -- 销售数量
    ,m1.bf19_sale_qty    -- 19点前销售数量
    ,m1.bf19_sale_amt    -- 19点前销售金额
    ,m1.af19_sale_qty    -- 19点后销售数量
    ,m1.af19_sale_amt    -- 19点后销售金额
    ,m1.profit_amt       -- 毛利额
    ,m1.lost_amt         -- 损耗额
    ,m1.know_lost_qty    -- 已知损耗数量
    ,m1.know_lost_amt    -- 已知损耗额
    ,m1.unknow_lost_qty  -- 未知损耗数量
    ,m1.unknow_lost_amt  -- 未知损耗额
    ,m1.discount_amt     -- 折扣额
    ,m1.promotion_discount_amt -- 促销折扣额
    ,m1.hour_discount_amt      -- 时段折扣额
    ,m1.allowance_amt          -- 补贴款
    ,m1.allowance_amt_profit   -- 补贴后毛利额
    ,m1.shop_promotion_amt -- 门店发起活动促销额
    ,m1.original_price_sale_amt -- 原价销售额
    ,m1.member_coupon_shop_amt -- 会员券(门店)
    ,m1.sale_weight      -- 销售重量
    ,m1.init_stock_qty   -- 期初库存数量
    ,m1.end_stock_qty    -- 期末库存数量
    ,m1.init_stock_amt   -- 期初库存金额
    ,m1.end_stock_amt    -- 期末库存金额
    ,m1.offline_original_amt -- 线下原价销售额
    ,m1.avg_purchase_price   -- 平均进货价(理论进货单价)
    ,m1.sale_piece_qty       -- 销售件数
    ,m1.bf19_sale_piece_qty  -- 19点前销售件数
    ,m1.scm_fin_article_income -- 总销售收入-财务(供应链到店毛利率分母)
    ,m1.scm_fin_article_profit -- 供应链毛利-财务(供应链到店毛利额)
    ,m1.article_profit_amt -- 门店商品毛利额
    ,m1.full_link_profit -- 全链路到店毛利额
    ,m1.total_sale_amt   -- 销售总金额(全链路到店毛利率的分母)
    ,m1.scm_promotion_amt_total -- 出库让利总额(供应链折让率分子)
    ,m1.out_stock_pay_amt -- 门店商品维度出库金额
    ,m1.pre_sale_amt     -- 理论销售额((实际销售数量+理论损耗数量)*销售原价)
    ,m1.pre_inbound_amount -- 理论进货额(bom处理后进货量*出库原价)

    ,m1.out_stock_amt_cb  -- 出库到店成本含税(出库到店成本)
    ,m1.expect_outstock_amt  -- 预期出库金额(出库原价金额)
    ,m3.sum_7d_sale_qty  -- 近7日总销售数量
    ,m3.avg_7d_sale_qty  -- 近7日平均销售数量
    ,m1.last_sysdate     -- 最后交易时间 
	,m1.compose_in_qty     --加工转换入数量
	,m1.compose_in_amt     --加工转换入金额
	,m1.compose_out_qty    --加工转换出数量
	,m1.compose_out_amt    --加工转换出金额
    ,m1.inc_day
    from 
    (select 
     t1.business_date                    -- 营业日期
    ,t1.store_id                         -- 门店编码
    ,t1.day_clear                        -- 非日清标识(1:不日清,0：日清)
    ,t1.article_id                       -- 商品编码
    ,sum(t1.receive_qty)                     as receive_qty      -- 进货数量
    ,sum(t1.receive_amt)                     as receive_amt      -- 进货金额
    ,sum(t1.sale_amt)                        as sale_amt         -- 实际销售额
    ,sum(t1.sale_qty)                        as sale_qty         -- 销售数量
    ,sum(t1.bf19_sale_qty)                   as bf19_sale_qty    -- 19点前销售数量
    ,sum(t1.bf19_sale_amt)                   as bf19_sale_amt    -- 19点前销售金额
    ,sum(t1.af19_sale_qty)                   as af19_sale_qty    -- 19点后销售数量
    ,sum(t1.af19_sale_amt)                   as af19_sale_amt    -- 19点后销售金额
    ,sum(t1.profit_amt)                      as profit_amt       -- 毛利额
    ,sum(t1.lost_amt)                        as lost_amt         -- 损耗额
    ,sum(t1.know_lost_qty)                   as know_lost_qty    -- 已知损耗数量
    ,sum(t1.know_lost_amt)                   as know_lost_amt    -- 已知损耗额
    ,sum(t1.unknow_lost_qty)                 as unknow_lost_qty  -- 未知损耗数量
    ,sum(t1.unknow_lost_amt)                 as unknow_lost_amt  -- 未知损耗额
    ,sum(t1.discount_amt)                    as discount_amt     -- 折扣额
    ,sum(t1.promotion_discount_amt)          as promotion_discount_amt -- 促销折扣额
    ,sum(t1.hour_discount_amt)               as hour_discount_amt-- 时段折扣额
    ,sum(t1.allowance_amt)                   as allowance_amt    -- 补贴款
    ,sum(t1.allowance_amt_profit)            as allowance_amt_profit -- 补贴后毛利额
    ,sum(t1.shop_promotion_amt)              as shop_promotion_amt -- 门店发起活动促销额
    ,sum(t1.original_price_sale_amt)         as original_price_sale_amt -- 原价销售额
    ,sum(t1.member_coupon_shop_amt)          as member_coupon_shop_amt -- 会员券(门店)
    ,sum(t1.sale_weight)                     as sale_weight      -- 销售重量
    ,sum(t1.init_stock_qty)                  as init_stock_qty   -- 期初库存数量
    ,sum(t1.end_stock_qty)                   as end_stock_qty    -- 期末库存数量
    ,sum(t1.init_stock_amt)                  as init_stock_amt   -- 期初库存金额
    ,sum(t1.end_stock_amt)                   as end_stock_amt    -- 期末库存金额
    ,sum(t1.offline_original_amt)            as offline_original_amt -- 线下原价销售额
    ,sum(t1.avg_purchase_price)              as avg_purchase_price -- 平均进货价
    ,sum(t1.sale_piece_qty)                  as sale_piece_qty      -- 销售件数
    ,sum(t1.bf19_sale_piece_qty)             as bf19_sale_piece_qty -- 19点前销售件数
    ,sum(t1.scm_fin_article_income)          as scm_fin_article_income -- 总销售收入-财务(供应链到店毛利率分母)
    ,sum(t1.scm_fin_article_profit)          as scm_fin_article_profit -- 供应链毛利-财务(供应链到店毛利额)
    ,sum(t1.article_profit_amt)              as article_profit_amt -- 门店商品毛利额
    ,sum(t1.full_link_profit)                as full_link_profit -- 全链路到店毛利额
    ,sum(t1.total_sale_amt)                  as total_sale_amt   -- 销售总金额(全链路到店毛利率的分母)
    ,sum(t1.scm_promotion_amt_total)         as scm_promotion_amt_total -- 出库让利总额(供应链折让率分子)
    ,sum(t1.out_stock_pay_amt)               as out_stock_pay_amt -- 门店商品维度出库金额
    ,sum(t1.pre_sale_amt)                    as pre_sale_amt     -- 理论销售额((实际销售数量+理论损耗数量)*销售原价)
    ,sum(t1.pre_inbound_amount)              as pre_inbound_amount -- 理论进货额(bom处理后进货量*出库原价)

    ,sum(t1.out_stock_amt_cb) as out_stock_amt_cb        -- 出库到店成本含税(出库到店成本)
    ,sum(t1.expect_outstock_amt) as expect_outstock_amt  -- 预期出库金额(出库原价金额)
    ,max(t1.last_sysdate) as last_sysdate
    ,sum(t1.compose_in_qty) as  compose_in_qty    --加工转换入数量
	,sum(t1.compose_in_amt ) as compose_in_amt    --加工转换入金额
	,sum(t1.compose_out_qty ) as compose_out_qty   --加工转换出数量
	,sum(t1.compose_out_amt) as  compose_out_amt   --加工转换出金额
    ,t1.inc_day
    from
    (select 
    business_date                     -- 营业日期
    ,store_id                         -- 门店编码
    ,day_clear                        -- 非日清标识(1:不日清,0：日清)
    ,article_id                       -- 商品编码
    ,receive_qty                      -- 进货数量
    ,receive_amt                      -- 进货金额
    ,sale_amt                         -- 实际销售额
    ,sale_qty                         -- 销售数量
    ,bf19_sale_qty                    -- 19点前销售数量
    ,bf19_sale_amt                    -- 19点前销售金额
    ,af19_sale_qty                    -- 19点后销售数量
    ,af19_sale_amt                    -- 19点后销售金额
    ,profit_amt                       -- 毛利额
    ,lost_amt                         -- 损耗额
    ,know_lost_qty                    -- 已知损耗数量
    ,know_lost_amt                    -- 已知损耗额
    ,unknow_lost_qty                  -- 未知损耗数量
    ,unknow_lost_amt                  -- 未知损耗额
    ,discount_amt                     -- 折扣额
    ,promotion_discount_amt           -- 促销折扣额
    ,hour_discount_amt                -- 时段折扣额
    ,allowance_amt                    -- 补贴款
    ,allowance_amt_profit             -- 补贴后毛利额
    ,shop_promotion_amt               -- 门店发起活动促销额
    ,original_price_sale_amt          -- 原价销售额
    ,member_coupon_shop_amt           -- 会员券(门店)
    ,sale_weight                      -- 销售重量
    ,init_stock_qty                   -- 期初库存数量
    ,end_stock_qty                    -- 期末库存数量
    ,init_stock_amt                   -- 期初库存金额
    ,end_stock_amt                    -- 期末库存金额
    ,offline_original_amt             -- 线下原价销售额
    ,avg_purchase_price               -- 平均进货价
    ,sale_piece_qty                   -- 销售件数
    ,bf19_sale_piece_qty              -- 19点前销售件数
    
    ,0 as scm_fin_article_income -- 总销售收入-财务(供应链到店毛利率分母)
    ,0 as scm_fin_article_profit -- 供应链毛利-财务(供应链到店毛利额)
    ,0 as article_profit_amt     -- 门店商品毛利额
    ,0 as full_link_profit       -- 全链路到店毛利额
    ,0 as total_sale_amt         -- 销售总金额(全链路到店毛利率的分母)
    ,0 as scm_promotion_amt_total           -- 出库让利总额(供应链折让率分子)
    ,0 as out_stock_pay_amt                 -- 门店商品维度出库金额
    ,0 as pre_sale_amt                     -- 理论销售额((实际销售数量+理论损耗数量)*销售原价)
    ,0 as pre_inbound_amount         -- 理论进货额(bom处理后进货量*出库原价)
    ,0 as out_stock_amt_cb  -- 出库到店成本含税(出库到店成本)
    ,0 as expect_outstock_amt  -- 预期出库金额(出库原价金额)
    ,last_sysdate   -- 最后交易时间 
	,compose_in_qty     --加工转换入数量
	,compose_in_amt     --加工转换入金额
	,compose_out_qty    --加工转换出数量
	,compose_out_amt    --加工转换出金额
    ,inc_day
    from dal.dal_transaction_non_daily_store_article_sale_info_di 
    where inc_day between '{start_day}' and '{end_day}'
    
    
    
    union all  -- 拼接全链路指标 
    
    select 
     business_date          --  日期
    ,store_id               -- 门店编码
    ,day_clear              -- 非日清标识(非日清:1,日清:0)
    ,article_id             -- 商品编码
    
    ,0 as receive_qty                      -- 进货数量
    ,0 as receive_amt                      -- 进货金额
    ,0 as sale_amt                         -- 实际销售额
    ,0 as sale_qty                         -- 销售数量
    ,0 as bf19_sale_qty                    -- 19点前销售数量
    ,0 as bf19_sale_amt                    -- 19点前销售金额
    ,0 as af19_sale_qty                    -- 19点后销售数量
    ,0 as af19_sale_amt                    -- 19点后销售金额
    ,0 as profit_amt                       -- 毛利额
    ,0 as lost_amt                         -- 损耗额
    ,0 as know_lost_qty                    -- 已知损耗数量
    ,0 as know_lost_amt                    -- 已知损耗额
    ,0 as unknow_lost_qty                  -- 未知损耗数量
    ,0 as unknow_lost_amt                  -- 未知损耗额
    ,0 as discount_amt                     -- 折扣额
    ,0 as promotion_discount_amt           -- 促销折扣额
    ,0 as hour_discount_amt                -- 时段折扣额
    ,0 as allowance_amt                    -- 补贴款
    ,0 as allowance_amt_profit             -- 补贴后毛利额
    ,0 as shop_promotion_amt               -- 门店发起活动促销额
    ,0 as original_price_sale_amt          -- 原价销售额
    ,0 as member_coupon_shop_amt           -- 会员券(门店)
    ,0 as sale_weight                      -- 销售重量
    ,0 as init_stock_qty                   -- 期初库存数量
    ,0 as end_stock_qty                    -- 期末库存数量
    ,0 as init_stock_amt                   -- 期初库存金额
    ,0 as end_stock_amt                    -- 期末库存金额
    ,0 as offline_original_amt             -- 线下原价销售额
    ,0 as avg_purchase_price               -- 平均进货价
    ,0 as sale_piece_qty                   -- 销售件数
    ,0 as bf19_sale_piece_qty              -- 19点前销售件数
    
    ,scm_fin_article_income -- 总销售收入-财务(供应链到店毛利率分母)
    ,scm_fin_article_profit -- 供应链毛利-财务(供应链到店毛利额)
    ,article_profit_amt     -- 门店商品毛利额
    ,full_link_profit       -- 全链路到店毛利额
    ,total_sale_amt         -- 销售总金额(全链路到店毛利率的分母)
    ,scm_promotion_amt_total           -- 出库让利总额(供应链折让率分子)
    ,out_stock_pay_amt                 -- 门店商品维度出库金额
    ,pre_sale_amt                     -- 理论销售额((实际销售数量+理论损耗数量)*销售原价)
    ,pre_inbound_amount         -- 理论进货额(bom处理后进货量*出库原价)
    ,out_stock_amt_cb  -- 出库到店成本含税(出库到店成本)
    ,expect_outstock_amt  -- 预期出库金额(出库原价金额)
    ,null as last_sysdate   -- 最后交易时间 
	,0 as compose_in_qty     --加工转换入数量
	,0 as compose_in_amt     --加工转换入金额
	,0 as compose_out_qty    --加工转换出数量
	,0 as compose_out_amt    --加工转换出金额
    ,inc_day    -- 取营业日期为日分区
    from tmp_dal.dal_transaction_chdj_store_sale_article_sale_info_di_01
    where inc_day between '{start_day}' and '{end_day}'
    
    )t1 
    group by 
     t1.business_date                     -- 营业日期
    ,t1.store_id                         -- 门店编码
    ,t1.day_clear                        -- 非日清标识(1:不日清,0：日清)
    ,t1.article_id                       -- 商品编码
    ,t1.inc_day
    
    )m1 
    left join (  -- 关联客数表
    select 
    business_date                    -- 营业日期
    ,store_id                        -- 门店编码
    ,sap_store_id                    -- 新门店编码
    ,categroy_id as category_id                     -- 大/中/小类ID/采购部门/采购小组/促销编码
    ,cust_num                        -- 来客数/订单数
    ,bf19_cust_num                   -- 19点前来客数
    ,af19_cust_num                   -- 19点后来客数
    ,member_cust_num                 -- 会员客数
    ,af19_member_cust_num            -- 19点后会员客数
    ,bf19_member_cust_num            -- 19点前会员客数
    ,member_num                      -- 去重人数
    ,bf19_member_num                 -- 19点前来客数去重人数
    ,af19_member_num                 -- 19点后来客数去重人数
    ,offline_cust_num                -- 线下订单数
    ,bf19_offline_cust_num           -- 19点前线下订单数
    ,af19_offline_cust_num           -- 线下19点后订单数
    ,sale_article_num                -- 动销商品数
    ,member_avg_article_num          -- 会员单均商品数
    ,nomember_avg_article_num        -- 非会员单均商品数
    ,avg_article_num                 -- 单均商品数
    ,bf19_usual_cust_num             -- 19点前有效客流
    ,bf12_cust_num                   -- 12点前来客数
    ,business_flag                   -- 是否营业(1：营业中 算店日均、0：未营业 算非店日均)
    ,sale_days                       -- 营业店日数，非店日均为0
    ,sale_days_old                   -- 营业店日数，非店日均不为0 旧逻辑
    from dal.dal_transaction_cbstore_cust_num_info_di
    where inc_day between '{start_day}' and '{end_day}'
    and categroy_type_id='01' and type_id='04'
    
    )m2 
    on m1.business_date=m2.business_date and m1.store_id=m2.store_id and m1.article_id=m2.category_id
    left join (
    select 
    business_date                    -- 营业日期
    ,store_id                        -- 门店编码
    ,day_clear                       -- 非日清标识(1:不日清,0：日清)
    ,article_id                      -- 商品编码
    ,sale_qty                        -- 销售数量
    ,sum(sale_qty) over(partition by store_id, article_id  order by business_date rows between 6 PRECEDING and current row) as sum_7d_sale_qty 
    ,avg(sale_qty) over(partition by store_id, article_id  order by business_date rows between 6 PRECEDING and current row) as avg_7d_sale_qty
    from hive.dal.dal_transaction_non_daily_store_article_sale_info_di 
    where inc_day between date(date_sub('{start_day}',7)) and '{end_day}'

    )m3 
    on m1.business_date=m3.business_date and m1.store_id=m3.store_id and m1.day_clear=m3.day_clear and m1.article_id=m3.article_id
        """

    hive1_sql = re.sub(r'(?s)(?:\/\*.*?\*\/|--[^\n]*)', '', hive1_sql)
    # 去重空行
    hive1_sql = re.sub(r'(\t|\s)*(\n|\r\n)', '\n', hive1_sql)
    hive1_sql = re.sub(r'(\`)', '', hive1_sql)


    return hive1_sql.format(start_day=start_day,end_day=end_day,yesterday=yesterday)


# 执行器
class Execute:

    def __init__(self, sql, STARROCKS_HOST, STARROCKS_PORT, STARROCKS_USER, STARROCKS_PASSWD):
        self.sql = sql
        self.STARROCKS_HOST = STARROCKS_HOST
        self.STARROCKS_PORT = STARROCKS_PORT
        self.STARROCKS_USER = STARROCKS_USER
        self.STARROCKS_PASSWD = STARROCKS_PASSWD

    # 方法一: starrocks执行查询
    def getExecuteSql(self):
        cmd = """
        echo "{sql}" | /usr/bin/mysql -h{STARROCKS_HOST} -P{STARROCKS_PORT} -u{STARROCKS_USER} -p{STARROCKS_PASSWD}
        """.format(sql=self.sql, STARROCKS_HOST=self.STARROCKS_HOST, STARROCKS_PORT=self.STARROCKS_PORT,
                   STARROCKS_USER=self.STARROCKS_USER, STARROCKS_PASSWD=self.STARROCKS_PASSWD)
        print("执行的命令如下:")
        print(cmd)
        sbpss = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, err = sbpss.communicate()
        returncode = sbpss.returncode
        print('>>>>>>> cmd=[{}] out=[{}] err=[{}] returncode=[{}]'.format(cmd, str(out), err, returncode))
        if returncode != 0:
            print(err)
            raise Exception("代码出错了")
        # 返回执行的结果信息
        return out;


if __name__ == '__main__':

    # 开始日期
    start_day=str(sys.argv[1])

    # 结束日期
    end_day=str(sys.argv[2])

    yesterday = str(datetime.now().date() + timedelta(days=-1))
    # 前天
    bef1_yesterday = str(datetime.now().date() + timedelta(days=-3))

    # 获取账号密码:
    account = GetPassWord()
    STARROCKS_HOST = account['STARROCKS_HOST']
    STARROCKS_PORT = int(account['STARROCKS_PORT'])
    STARROCKS_USER = account['STARROCKS_USER']
    STARROCKS_PASSWD =account['STARROCKS_PASSWD']

    # 分割日期
    day_list=timepand(interval=5,rq1=start_day,rq2=end_day)

    num=len(day_list)
    print('日期分段数量为'+str(num))

    # 插入正式分区
    for i in range(num):
        start_date_str=day_list[i][0]
        end_date_str=day_list[i][1]
        hive1_sql = sql_format(start_day=start_date_str, end_day=end_date_str, yesterday=yesterday)
        # 创建临时表2
        print("开始创建临时表2")
        executeHive1 = Execute(hive1_sql, STARROCKS_HOST, STARROCKS_PORT, STARROCKS_USER, STARROCKS_PASSWD)
        executeHive1.getExecuteSql()


