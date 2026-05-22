# coding=UTF-8
# Doris数据监控任务
import subprocess
import pandas as pd
import codecs
from concurrent.futures import ThreadPoolExecutor # 导入线程池
import MySQLdb
import random
import warnings
import requests
import sys
import json
import requests
import time
import traceback
import datetime
import threading
import re
import subprocess as sp
from dateutil import parser
from retrying import retry
import re
import calendar
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

# 071验A转spu本身有销售
def sql_format(start_day,end_day,yesterday):
    hive1_sql = """
 
    set catalog hive;
    -- 门店商品进货明细
    insert overwrite hive.tmp_dal.dal_store_article_multi_level_bom_splitting_di_01 partition(inc_day)
    -- 进货的明细 
    SELECT
    header_shop_id as store_id 
    ,header_receive_no as order_no
    ,purchase_order_type  --DIRECT_DELIVERY_ORDER:直送单,PURCHASE_ORDER:寄库单,RETURN_PURCHASE_ORDER:退物流单,RETURN_DIRECT_ORDER:退厂商单,SUPPLEMENT_DIRECT_DELIVERY_ORDER:直送单(补) 
    ,'1' as order_type     -- 1:进货 2:调拨 3:退货
    ,new_dc_id 
    ,sku_code
    ,sum(pur_qty) pur_qty              --订购数量                
    ,sum(case when coalesce(pur_price,0)=0 then pur_qty*receive_price else pur_qty*pur_price end) as order_amt                          
    ,sum(receive_qty) receive_qty         --验收/退货数量  
    ,sum(case when gift_flag='1' then receive_qty else 0 end) as gift_qty   --赠品数量
    ,sum(receive_total_amount) receive_amt --验收/退货总金额            
    ,sum(pick_qty) pick_qty            --物流出库数量              
    ,sum(shipped_qty) shipped_qty         --物流实际出货数 
    ,sales_mode
    ,'1' as same_day_flag    -- 同天
    ,header_receive_no as original_receive_no   -- 原始验收单号
    ,inc_day
    from ddl.ddl_store_receive_info_zt 
    where inc_day between '{start_day}' and '{end_day}'
    group by 
    inc_day
    ,header_shop_id 
    ,header_receive_no 
    ,purchase_order_type  --DIRECT_DELIVERY_ORDER:直送单,PURCHASE_ORDER:寄库单,RETURN_PURCHASE_ORDER:退物流单,RETURN_DIRECT_ORDER:退厂商单,SUPPLEMENT_DIRECT_DELIVERY_ORDER:直送单(补) 
    ,new_dc_id 
    ,sales_mode
    ,sku_code
    
    union all  -- 调拨 
    
    select
     a1.from_store_id as store_id
    ,a1.head_acceptance_order_no as order_no
    ,a1.purchase_order_type
    ,'2' as order_type
    ,a1.new_dc_id
    ,a1.sku_code
    ,0 as pur_qty              --订购数量                
    ,0 as order_amt  
    ,-sum(a1.actual_acceptance_count) as receive_qty
    ,-sum(a1.actual_acceptance_count_gift) as gift_qty
    ,-sum(a1.total_amount) as receive_amt
    ,0 as pick_qty            --物流出库数量              
    ,0 as shipped_qty         --物流实际出货数 
    ,a1.sales_mode
    ,'1' as same_day_flag    -- 同天
    ,a1.head_acceptance_order_no as original_receive_no
    ,a1.inc_day
    from
    (
    select
    inc_day
    ,b1.from_store_id
    ,b1.sku_code
    ,b1.sales_mode
    ,b1.head_acceptance_order_no
    ,b1.actual_acceptance_count
    ,b1.actual_acceptance_count_gift
    ,b1.combo_flag
    ,b2.new_dc_id
    ,b2.purchase_order_type
    -- ,(coalesce(b1.actual_acceptance_count,0)-coalesce(actual_acceptance_count_gift,0))*coalesce(b2.receive_price,0) as total_amount
    ,case when b1.allot_type in ('1','3','4') then b1.total_amount else (coalesce(b1.actual_acceptance_count,0)-coalesce(b1.actual_acceptance_count_gift,0))*coalesce(b2.receive_price,0) end as total_amount
    from
    (select 
    inc_day 
    ,from_store_id 
    ,sku_code
    ,head_acceptance_order_no 
    ,sum(coalesce(allocation_count,0)) as actual_acceptance_count     -- 调拨出去的数量
    ,sum(case when gift_flag='1' then coalesce(allocation_count,0) else 0 end) as actual_acceptance_count_gift  -- 调拨的赠品数量
    ,sum(purchase_price*allocation_count) as total_amount
    ,allot_type
    ,sales_mode
    ,combo_flag
    from ddl.ddl_store_allocation_info_zt 
    where inc_day between '{start_day}' and '{end_day}'
    and head_status='complete'
    group by 
    inc_day 
    ,from_store_id 
    ,sku_code
    ,sales_mode
    ,combo_flag
    ,allot_type
    ,head_acceptance_order_no
    )b1
    left join (
    select 
    header_receive_no
    ,header_shop_id
    ,sku_code
    ,new_dc_id
    ,max(receive_price) as receive_price  
    ,sales_mode 
    , combo_flag
    ,purchase_order_type
    from ddl.ddl_store_receive_info_zt 
    where inc_day between date(date_sub('{start_day}',30)) and '{end_day}'
    group by 
    header_receive_no
    ,sku_code
    ,sales_mode 
    ,combo_flag
    ,purchase_order_type
    ,new_dc_id
    ,header_shop_id
    )b2
    on b1.head_acceptance_order_no=b2.header_receive_no and b1.sku_code=b2.sku_code  
    and coalesce(b1.sales_mode,'-')=coalesce(b2.sales_mode,'-') 
    and coalesce(b1.combo_flag,'-')=coalesce(b2.combo_flag,'-')
    and b1.from_store_id=b2.header_shop_id
    )a1 
    group by 
     a1.inc_day
    ,a1.from_store_id
    ,a1.head_acceptance_order_no
    ,a1.purchase_order_type
    ,a1.new_dc_id
    ,a1.sku_code
    ,a1.sales_mode
    
    union all   -- 退货 
    
    select 
     a1.header_shop_id as store_id 
    ,a1.header_return_no as order_no
    ,a2.purchase_order_type
    ,'3' as order_type
    ,a1.new_dc_id       --新仓id
    ,a1.sku_code
    ,0 as pur_qty              --订购数量                
    ,0 as order_amt   
    ,sum(a1.return_qty)  as receive_qty         --验收/退货数量  
    ,0 as gift_qty   --赠品数量
    ,sum(a1.return_amount) as receive_amt --验收/退货总金额            
    ,0 as pick_qty            --物流出库数量              
    ,0 as shipped_qty         --物流实际出货数 
    ,a1.sales_mode
    ,case when a1.inc_day=a2.inc_day then '1' else '0' end as same_day_flag  -- 同天标识 
    ,a1.associated_no as original_receive_no
    ,a1.inc_day
    from 
    (select * from ddl.ddl_store_refund_info_zt
    where inc_day between '{start_day}' and '{end_day}'
    and order_status='VERIFIED' and deleted<>1
    )a1 
    left join (
    select receive_no ,sales_mode,inc_day,purchase_order_type,sku_code
    from ddl.ddl_store_receive_info_zt 
    where inc_day between date(date_sub('{start_day}',30)) and '{end_day}'
    group by receive_no ,sales_mode,inc_day,purchase_order_type,sku_code
     )a2
     on a1.associated_no=a2.receive_no and a1.sales_mode=a2.sales_mode and a1.sku_code=a2.sku_code
    group by 
     a1.inc_day
    ,a1.header_shop_id 
    ,a1.header_return_no 
    ,a2.purchase_order_type
    ,a1.new_dc_id       --新仓id
    ,a1.sku_code
    ,a1.sales_mode
    ,case when a1.inc_day=a2.inc_day then '1' else '0' end   
    ,a1.associated_no
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


# 设定工作内容 设定重试次数  等待最小时间 最大时间  retry 参数不能外部传参
@retry(stop_max_attempt_number=2,wait_random_min=1, wait_random_max=1000*10)
def work(start_day,end_day):

    sql=sql_format(start_day,end_day,yesterday)

    print("正式开始执行指令")
    start_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())  # 2023-02-21 16:07:14
    print("子线程%s开始时间为%s 参数:%s,%s" % (threading.current_thread(), start_time, start_day, end_day))

    execute=Execute(sql,STARROCKS_HOST,STARROCKS_PORT,STARROCKS_USER,STARROCKS_PASSWD)
    execute.getExecuteSql()

    # 睡眠时间
    sleep_seconds=random.randint(1,3)
    time.sleep(sleep_seconds)

if __name__ == '__main__':

    yesterday = str(datetime.now().date() + timedelta(days=-1))
    # 获取账号密码:
    account = GetPassWord()
    STARROCKS_HOST = account['STARROCKS_HOST']
    STARROCKS_PORT = int(account['STARROCKS_PORT'])
    STARROCKS_USER = account['STARROCKS_USER']
    STARROCKS_PASSWD =account['STARROCKS_PASSWD']

    # 开始日期
    start_day=str(sys.argv[1])

    # 结束日期
    end_day=str(sys.argv[2])

    # 设置线程池数量
    max_workers=int(sys.argv[3])

    # 间隔天数
    interval = int(sys.argv[4])

    day_list=timepand(interval,start_day,end_day)
    # 总共的日期分段数量
    num=len(day_list)
    print('日期分段数量为'+str(num))
    # 设置线程池数量
    pool=ThreadPoolExecutor(max_workers=max_workers)

    future_list=[]
    err_list=[]

    start_time=time.time()
    print('程序开始时间戳为'+str(start_time))
    start_time2=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())  # 2023-02-21 16:07:14
    print('程序开始时间为：'+str(start_time2))

    # 创建多线程
    for i in range(num):
        # print('该步参数为',day_list[i][0],day_list[1])
        future=pool.submit(work, day_list[i][0],day_list[i][1])
        future_obj= {'future':future,'start_day':day_list[i][0],'end_day':day_list[i][1]}
        future_list.append(future_obj)

    for j in range(len(future_list)):
        try:
            result=future_list[j]['future'].result()
        except Exception as e:
            start_day= future_list[j]['start_day']
            end_day= future_list[j]['end_day']
            print('该线程有异常，异常日期区间为:%s 到 %s 请重试'% (start_day,end_day))
            err_list.append([start_day,end_day])
            print(err_list)
    # print(err_list)
    # 等待所有线程完成后关闭
    pool.shutdown(wait=True)
    end_time=time.time()
    print('数据结束时间戳为:'+str(end_time))
    end_time2=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())  # 2023-02-21 16:07:14
    print('结束时间为:'+str(end_time2))
    times=end_time-start_time
    print('总共耗时'+str(times))
    if len(err_list)<=0:
        print('恭喜，所有线程运行成功')
    else:
        print('线程有异常，异常日期如下:')
        print(err_list)
        raise  Exception
