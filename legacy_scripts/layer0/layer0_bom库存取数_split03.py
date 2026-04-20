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
    -- 门店商品库存数据
    insert overwrite hive.tmp_dal.dal_store_article_multi_level_bom_splitting_di_03 partition(inc_day)
    select 
     t1.inc_day as business_date
    ,t1.store_id
    ,t3.category_level1_id
    ,t3.category_level1_description
    ,coalesce(t1.article_id,'-') as article_id
    ,t3.article_name
    ,sum(coalesce(t1.init_stock_qty,0)) as init_stock_qty      -- '期初库存数量' 
    ,sum(coalesce(t1.end_stock_qty,0)) as end_stock_qty        -- '期库末存数量' 
    ,sum(coalesce(t1.init_stock_amt,0)) as init_stock_amt      -- '期初库存金额' 
    ,sum(coalesce(t1.end_stock_amt,0)) as end_stock_amt        -- '期初库存金额
    ,t1.inc_day
    from (select * from dsl.dsl_transaction_store_article_inventory_info_di
    where inc_day between '{start_day}' and '{end_day}'
    )t1 
    inner join (select * from dim.dim_goods_information_have_pt where inc_day='{yesterday}')t3 
    on t1.article_id=t3.article_id
    group by 
     t1.inc_day  
    ,t1.store_id
    ,t3.category_level1_id
    ,t3.category_level1_description
    ,coalesce(t1.article_id,'-') 
    ,t3.article_name
    ,t1.inc_day
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
