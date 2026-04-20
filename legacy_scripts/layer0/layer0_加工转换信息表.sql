set mapred.job.name=dsl_transaction_sotre_article_compose_info_di;--设置job名
set mapred.job.queue.name=root.etl;--设置跑数队列
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions=100000; 
SET hive.exec.max.dynamic.partitions.pernode=100000;
set mapreduce.map.memory.mb=6144;
set mapreduce.reduce.memory.mb=6144;
set mapreduce.map.java.opts=-Xmx5120m;
set mapreduce.reduce.java.opts=-Xmx4048m;
set hive.auto.convert.join=false;

insert overwrite table dsl.dsl_transaction_sotre_article_compose_info_di partition(inc_day)


select  
business_date  --营业日期 
,store_id    --门店编码
,store_name   --门店名称
,article_id  --商品编码
,article_name --商品名称
,sum(compose_in_qty ) as compose_in_qty  --加工转换入数量 
,sum(compose_in_amt ) as compose_in_amt --加工转换入金额
,sum(compose_out_qty) as compose_out_qty   --加工转换出数量 
,sum(compose_out_amt) as compose_out_amt   --加工转换出金额
, from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as update_time   --最后更新时间
,business_date as inc_day 
from( 
select 
 business_date  --营业日期 
 ,store_id    --门店编码
,store_name   --门店名称
,article_id  --商品编码
,article_name --商品名称
,0  as compose_in_qty  --加工转换入数量 
,0  as compose_in_amt --加工转换入金额
,compose_in_qty as compose_out_qty  --加工转换出数量 
,compose_in_amt as compose_out_amt  --加工转换出金额
from  ddl.ddl_compose_in_info_di
  where  inc_day  between  '$[time(yyyy-MM-dd,-7d)]'  and '$[time(yyyy-MM-dd,-1d)]'  
union all 
select 
 business_date  --营业日期 
 ,store_id    --门店编码
,store_name   --门店名称
,article_id  --商品编码
,article_name --商品名称
,compose_out_qty as compose_in_qty  --加工转换入数量 
,compose_out_amt as compose_in_amt --加工转换入金额
,0   as compose_out_qty  --加工转换出数量 
,0   as compose_out_amt  --加工转换出金额
from  ddl.ddl_compose_out_info_di
  where  inc_day  between  '$[time(yyyy-MM-dd,-7d)]'  and '$[time(yyyy-MM-dd,-1d)]'  
)zz
group by  business_date  --营业日期 
,store_id    --门店编码
,store_name   --门店名称
,article_id  --商品编码
,article_name --商品名称