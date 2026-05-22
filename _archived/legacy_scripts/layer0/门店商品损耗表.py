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
    -- 原损耗表2.0版本有数据 就取多级bom损耗表,其余取旧的
    -- 门店商品损耗表
    insert overwrite hive.dal.dal_transaction_store_article_lost_di partition(inc_day)
    select 
     t1.store_id
    ,t1.article_id 
    ,t2.article_name
    ,t2.category_level1_id
    ,t2.category_level1_description
    ,t1.unknow_lost_qty  --  未知损耗数量
    ,t1.unknow_lost_amt  --  未知损耗金额
    ,t1.know_lost_qty      --  已知损耗数量
    ,t1.know_lost_amt      --  已知损耗金额
    ,t1.inc_day
    from 
    (select 
     t.store_id
    ,t.article_id 
    ,sum(t.unknow_lost_qty) as unknow_lost_qty  --  未知损耗数量
    ,sum(t.unknow_lost_amt) as unknow_lost_amt  --  未知损耗金额
    ,sum(t.know_lost_qty) as know_lost_qty      --  已知损耗数量
    ,sum(t.know_lost_amt) as know_lost_amt      --  已知损耗金额
    ,t.inc_day
    from(
    select 
     m1.inc_day  -- 业务日期
    ,m1.store_id       -- 门店ID
    ,m1.article_id     -- 商品编码
    ,m1.unknow_lost_qty  -- '未知损耗数量',
    ,m1.unknow_lost_amt  -- '未知损耗金额',
    ,m1.know_lost_qty              --  已知损耗数量
    ,m1.know_lost_amt              --  已知损耗金额
    from 
    (select 
     business_date as inc_day  -- 业务日期
    ,store_id       -- 门店ID
    ,article_id     -- 商品编码
    ,unknow_lost_qty  -- '未知损耗数量',
    ,unknow_lost_amt  -- '未知损耗金额',
    ,updated_time
    ,0 as know_lost_qty              --  已知损耗数量
    ,0 as know_lost_amt              --  已知损耗金额
    from ods_rt_dws.dws_transaction_store_article_unknowlost_rts_di
    where inc_day between '{start_day}' and '{end_day}'
    and inc_day>='2025-07-22' and abs(unknow_lost_amt)+abs(unknow_lost_qty)>0
    )m1 
    inner join (
    select 
     business_date  -- 业务日期
    ,store_id      -- 门店id
    ,max(updated_time) as updated_time
    from ods_rt_dws.dws_transaction_store_article_unknowlost_rts_di
    where inc_day between '{start_day}' and '{end_day}'
    and inc_day>='2025-07-22'
    group by 
     business_date  -- 业务日期
    ,store_id      -- 门店id
    
    )m2 
    on m1.inc_day=m2.business_date and m1.store_id=m2.store_id and m1.updated_time=m2.updated_time

    
    union all  -- 已知损耗
    select 
     sell_at as inc_day       -- 损耗日期
    ,shop_id as store_id      -- 门店编号
    ,sku_code as article_id   -- 商品编号
    ,0 as unknow_lost_qty     --  未知损耗数量
    ,0 as unknow_lost_amt     --  未知损耗金额
    ,sum(waste_num) as know_lost_qty      -- 损耗数量
    ,sum(waste_money) as know_lost_amt    -- 损耗金额
    from ods_sc_db.t_purchase_wastage
    where inc_day='{yesterday}'
    and is_deleted='0'
    and sell_at between '{start_day}' and '{end_day}'
    and sell_at>='2025-07-22'
    group by 
     sell_at  
    ,shop_id  
    ,sku_code  


    union all   -- 历史数据
    select 
    inc_day
    ,shop_id as store_id
    ,sku_code as article_id 
    ,sum(unknown_wastage_qty) as unknow_lost_qty        --  未知损耗数量
    ,sum(unknown_wastage_amount) as unknow_lost_amt  --  未知损耗金额
    ,sum(know_wastage_qty) as know_lost_qty 
    ,sum(know_wastage_amount) as know_lost_amt
    from ods_sc_db.t_sc_settlement_detail_logs
    where inc_day between '{start_day}' and '{end_day}'
    -- and substr(category_id,1,2) not in ('70','71','98','72','73','74','75','76','77')
    and coalesce(version,'1.0') in ('1.0')
    and inc_day<'2025-07-22'
    group by 
    inc_day
    ,shop_id 
    ,sku_code 

    )t 
    group by 
    t.store_id
    ,t.article_id
    ,t.inc_day
    )t1 
    inner join (
     select * from dim.dim_goods_information_have_pt 
     where inc_day='{yesterday}' and category_level1_id not in ('70','71','98','72','73','74','75','76','77')
    )t2 on t1.article_id=t2.article_id 
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

