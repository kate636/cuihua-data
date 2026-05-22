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


