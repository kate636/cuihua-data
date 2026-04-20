#!/bin/sh


source /opt/script/password/hive.sh

start_date=$(cat << EOF
$start_date 
EOF);

end_date=$(cat << EOF
$end_date 
EOF);

intervalday=$(cat << EOF
$intervalday 
EOF);


echo "start_date: " $start_date
echo "end_date: " $end_date
echo "intervalday: " $intervalday

# 维度参数 昨天 -1d 
yesterday=$(date -d "1 day ago" +"%Y-%m-%d")  

# 起始日期 转格式 
startdate=`date -d "${start_date} +0 day" +%Y%m%d`
enddate=`date -d "${end_date} +0 day" +%Y%m%d`
originstartdate=`date -d "${start_date} +0 day" +%Y%m%d`


echo "循环开始日期:startdate:"${startdate}
echo "循环结束日期:enddate:"${enddate}
echo "维度表日期:yesterday:"${yesterday}
echo "时间跨度:intervalday:"$intervalday


while [ ${startdate} -le  ${enddate} ] 
do
 
# 开始日期 (循环的开始日期)  字符串 sql第一个参数
start_date=`date -d "${startdate}" +%Y-%m-%d`    

# 加间隔日期 作为第二个参数 
rq2date=`date -d "${startdate} +${intervalday} day" +%Y%m%d`
# 循环的结束日期  转格式
 end_date=`date -d "${rq2date}" +%Y-%m-%d` # sq2 参数用


# 日期+1 天  
rq3=`date -d "${end_date}" +%Y%m%d`

echo "参数开始日期:" ${start_date}
echo "参数结束日期:" ${end_date}


if [ ${rq3} -ge ${enddate} ] 

then 
    echo "end_date:" ${end_date}
    echo "最后日期跨度超了限制的范围"
    end_date=`date -d "${enddate} +0 day" +%Y-%m-%d`
    rq3=`date -d "${end_date}" +%Y%m%d`
    echo "最后循环的开始日期为 start_date:" ${start_date}
    echo "最后循环结束日期调整为 end_date:" ${end_date}
    echo "最后循环结束日期调整为rq3:" ${rq3}
else 
    end_date=${end_date}  # 日期不改变
    echo "最后循环的开始日期为 end_date:" ${start_date}
    echo "最后日期满足要求，不发生改变，结束日期为:" ${end_date}
fi

sqltxt=$(cat << EOF
  
-- 切换空间
set catalog hive;

-- ####################################################################
-- # 任务功能说明：供应链业务环节-出入订存退
-- # 作者：卫嘉俊
-- # 修改记录
-- # 版本      修改人       修改日期        修改说明
-- # v1        卫嘉俊
-- # v2        lnm         2024-07-23      处理D093出库成本逻辑
-- # v3        lnm         2024-10-29      订购模型指标补充(未跑历史数据)
-- # v4        lnm         2024-11-01      新增停业标签
-- # v5        lnm         2024-11-13      新增退货指标
-- # v6        lnm         2024-11-18      新增出库到店份数-满足率低于下限的金额
-- # v7        lnm         2024-11-22      停业标签剔除电商仓
-- # v8        lnm         2024-11-25      sap退货单调整过账日期=到店日期
-- # v9        lnm         2024-11-25      少货、满足订单、零出库处理wms BOM关系
-- # v10       lnm         2024-11-27      925行lower_qty,order_amt改为order_qty_payean
-- # v11       lnm         2024-12-10      出库到店成本相关指标逻辑调整为取SAP单价，时间范围2024-07-01开始
-- # v12       lnm         2024-12-20      新增门店编码store_id
-- # v13       lnm         2024-12-20      出库到店成本相关指标global已处理
-- # v14       lnm         2024-12-23      门店退货获取不到配送方式默认寄库
-- # v15       lnm         2024-12-24      修改停业标签历史逻辑
-- # v16       lnm         2025-01-07      新增赠品让利承担方指标
-- # v17       lnm         2025-01-14      新增赠品数量让利承担方指标
-- # v18       lnm         2025-02-20      ek_bsart in ('Z018','Z019','Z020','Z021','Z027','Z030')  and mdoc_bwart in ('671','673','672','674')替换为条件abs(return_stock_qty)>0
-- # v19       lnm         2025-02-20      参数修改，加循环
-- # v20       lnm         2025-04-01      新增公司承担让利字段
-- # v21       lnm         2025-04-03      新增公司承担促销费用
-- # v22       lnm         2025-04-10      公司承担促销费用直取，直送供应商按日期取
-- # v23       lnm         2025-08-04      新增退货订单次数
-- # v24       lnm         2025-08-21      退货订单次数直送仓编码逻辑处理
-- # v25       lnm         2025-10-16      D093成本逻辑判断由大于0改为空值
-- #################################################################### 
    

drop table if exists tmp_dal.dal_dc_matnr_store_base_wide23_finally_sr force;   
create table tmp_dal.dal_dc_matnr_store_base_wide23_finally_sr 
as
select
    a.new_dc_id as new_dc_id,
    a.mdoc_werks,
    a.new_store_id as new_store_id,
    a.delivery_mode as delivery_mode,
    a.matnr as matnr,
    a.sales_mode as sales_mode,
    sum(out_stock_qty) as out_stock_qty,
    sum(out_stock_qty_give) as out_stock_qty_give,
    sum(a.out_stock_original_amt) as out_stock_original_amt,
    sum(a.out_stock_pay_amt) as out_stock_pay_amt,
    sum(a.out_stock_pay_amt_notax) as out_stock_pay_amt_notax,
    sum(a.out_stock_amt_cb_notax) as out_stock_amt_cb_notax,
    sum(a.out_stock_give_amt_cb_notax) as out_stock_give_amt_cb_notax,
    sum(a.return_stock_qty) as return_stock_qty,
    sum(a.return_stock_original_amt) as return_stock_original_amt,
    sum(a.return_stock_pay_amt) as return_stock_pay_amt,
    sum(a.return_stock_pay_amt_notax) as return_stock_pay_amt_notax,
    sum(a.return_stock_amt_cb_notax) as return_stock_amt_cb_notax,
    sum(a.out_stock_zzckj_amt) as out_stock_zzckj_amt,
    sum(a.out_stock_amt_zzckj_nottax) as out_stock_amt_zzckj_nottax,
    sum(a.out_stock_zzddj_amt) as out_stock_zzddj_amt,
    sum(a.out_stock_amt_zzddj_nottax) as out_stock_amt_zzddj_nottax,
    sum(a.original_outstock_qty) as original_outstock_qty,
    sum(a.original_outstock_amt) as original_outstock_amt,
    sum(a.promotion_outstock_price) as promotion_outstock_price,
    sum(a.promotion_outstock_qty) as promotion_outstock_qty,
    sum(a.promotion_outstock_amt) as promotion_outstock_amt,
    sum(a.out_stock_amt_cb) as out_stock_amt_cb,
    sum(a.return_stock_amt_cb) as return_stock_amt_cb,
    a.inc_day as inc_day,
    a.inc_day as out_stock_date
from
(
select
    a.mdoc_werks,
    a.new_dc_id,
    a.mdoc_wempf as new_store_id,
    case when b.zpslx is null then a.ek_zpslx when b.zpslx is not null then b.zpslx end as delivery_mode,
    a.mdoc_matnr as matnr,
    a.ek_zzxsfs as sales_mode,
    a.out_stock_qty as out_stock_qty,
    a.out_stock_qty_give as out_stock_qty_give,
    a.out_stock_original_amt as out_stock_original_amt,
    a.out_stock_pay_amt as out_stock_pay_amt,
    a.out_stock_pay_amt_notax as out_stock_pay_amt_notax,
    a.out_stock_amt_cb_notax as out_stock_amt_cb_notax,
    a.out_stock_give_amt_cb_notax as out_stock_give_amt_cb_notax,
    a.return_stock_qty as return_stock_qty,
    a.return_stock_original_amt as return_stock_original_amt,
    a.return_stock_pay_amt as return_stock_pay_amt,
    a.return_stock_pay_amt_notax as return_stock_pay_amt_notax,
    a.return_stock_amt_cb_notax as return_stock_amt_cb_notax,
    a.out_stock_zzckj_amt as out_stock_zzckj_amt,
    a.out_stock_amt_zzckj_nottax as out_stock_amt_zzckj_nottax,
    a.out_stock_zzddj_amt as out_stock_zzddj_amt,
    a.out_stock_amt_zzddj_nottax as out_stock_amt_zzddj_nottax,
    a.original_outstock_qty as original_outstock_qty,
    a.original_outstock_amt as original_outstock_amt,
    a.promotion_outstock_price as promotion_outstock_price,
    a.promotion_outstock_qty as promotion_outstock_qty,
    a.promotion_outstock_amt as promotion_outstock_amt,
    a.out_stock_amt_cb as out_stock_amt_cb,
    a.return_stock_amt_cb as return_stock_amt_cb,
    a.out_stock_date as inc_day
from
(
    select
        t.mdoc_ebeln,
        case when t.mdoc_werks = 'D093' then t.mdoc_lgort_sid else t.mdoc_werks end as new_dc_id,
        t.mdoc_wempf,
        t.mdoc_werks,
        case when t.ek_zpslx = '' or t.ek_zpslx is null then 'A' else t.ek_zpslx end as ek_zpslx,
        t.mdoc_matnr,
        t.ek_zzxsfs,
        t.out_stock_qty,
        t.out_stock_qty_give,
        t.out_stock_original_amt,
        t.out_stock_pay_amt,
        t.out_stock_pay_amt_notax,
        t.out_stock_amt_cb_notax,
        t.out_stock_give_amt_cb_notax,
        t.return_stock_qty,
        t.return_stock_original_amt,
        t.return_stock_pay_amt,
        t.return_stock_pay_amt_notax,
        t.return_stock_amt_cb_notax,
        t.out_stock_zzckj_amt,
        t.out_stock_amt_zzckj_nottax,
        t.out_stock_zzddj_amt,
        t.out_stock_amt_zzddj_nottax,
        t.original_outstock_qty,
        t.original_outstock_amt,
        t.promotion_outstock_price,
        t.promotion_outstock_qty,
        t.promotion_outstock_amt,
        t.out_stock_amt_cb  ,
        t.return_stock_amt_cb ,
        t.inc_day,
        case when case when t.ek_zpslx = '' or t.ek_zpslx is null then 'A' else t.ek_zpslx end = 'C' then t.inc_day
        when abs(return_stock_qty)>0 then t.inc_day
         else date(date_add(t.inc_day,1)) end out_stock_date
    from dsl.dsl_scm_info_sap_global_di  t 
     where t.inc_day between date(adddate('$start_date',-1)) and '$end_date' and coalesce(t.ek_zpslx,'aaa') <> 'E' and  t.MDOC_STOCK_QTY<>0
) a
left join
(
select
    max(case when b.zpslx = '' then 'A' else b.zpslx end) as zpslx,
    a.ebeln as ebeln
from 
(
    select 
        inc_day,  --过账日期
        zydh,  --原单号
        werks,  --工厂
        ebeln
    from 
        ods_sap.zsdt014
    where inc_day between date(adddate('$start_date',-17)) and '$end_date'
) a
inner join
(
    select
        ko.inc_day as inc_day,  --过账日期
        ko.zpslx as zpslx,  --配送类型
        ko.submi as submi  --汇总号
    from
    (
        select 
            inc_day,
            zpslx,
            submi
        from ods_sap.ekko 
        where inc_day  between date(adddate('$start_date',-43)) and '$end_date'
    ) ko
)
b
on 
a.zydh = b.submi
where a.ebeln <> ''
group by a.ebeln
) b
on
a.mdoc_ebeln = b.ebeln
) a
group by 
    new_dc_id,mdoc_werks,new_store_id,delivery_mode,matnr,sales_mode,inc_day;









drop table if exists tmp_dal.dal_dc_matnr_store_sum_wide23_finally_sr force;   
create table tmp_dal.dal_dc_matnr_store_sum_wide23_finally_sr  
as
select
    a.out_stock_date as out_stock_date,
    a.new_dc_id as new_dc_id,
    a.new_store_id as new_store_id,
    a.is_zs as is_zs,
    a.matnr as matnr,
    a.sales_mode as sales_mode,
    sum(a.out_stock_qty) as out_stock_qty,
    sum(a.out_stock_qty_give) as out_stock_qty_give,
    sum(a.out_stock_original_amt) as out_stock_original_amt,
    sum(a.out_stock_pay_amt) as out_stock_pay_amt,
    sum(a.out_stock_pay_amt_notax) as out_stock_pay_amt_notax,
    sum(case when a.mdoc_werks ='D093' and b.avg_cost_price_notax is not null then (a.out_stock_qty * b.avg_cost_price_notax) else a.out_stock_amt_cb_notax end) as out_stock_amt_cb_notax,
    sum(case when a.mdoc_werks ='D093' and b.avg_cost_price_notax is not null then (a.out_stock_qty_give * b.avg_cost_price_notax) else a.out_stock_give_amt_cb_notax end ) as out_stock_give_amt_cb_notax,
    sum(a.return_stock_qty) as return_stock_qty,
    sum(a.return_stock_original_amt) as return_stock_original_amt,
    sum(a.return_stock_pay_amt) as return_stock_pay_amt,
    sum(a.return_stock_pay_amt_notax) as return_stock_pay_amt_notax,
    sum(case when a.mdoc_werks ='D093' and b.avg_return_cost_price_notax is not null then (a.return_stock_qty * b.avg_return_cost_price_notax) else a.return_stock_amt_cb_notax end ) as return_stock_amt_cb_notax,
    sum(a.out_stock_zzckj_amt) as out_stock_zzckj_amt,
    sum(a.out_stock_amt_zzckj_nottax) as out_stock_amt_zzckj_nottax,
    sum(a.out_stock_zzddj_amt) as out_stock_zzddj_amt,
    sum(a.out_stock_amt_zzddj_nottax) as out_stock_amt_zzddj_nottax,
    sum(original_outstock_qty) as original_outstock_qty,
    sum(original_outstock_amt) as original_outstock_amt,
    sum(promotion_outstock_price) as promotion_outstock_price,
    sum(promotion_outstock_qty) as promotion_outstock_qty,
    sum(promotion_outstock_amt) as promotion_outstock_amt,
    sum(case when a.mdoc_werks ='D093' and b.avg_cost_price is not null then (a.out_stock_qty * b.avg_cost_price) else a.out_stock_amt_cb end ) as out_stock_amt_cb,
    sum(case when a.mdoc_werks ='D093' and b.avg_return_cost_price_notax is not null then (a.return_stock_qty * b.avg_return_cost_price) else a.return_stock_amt_cb end ) as return_stock_amt_cb,
    a.out_stock_date as inc_day
from
(
select
    new_dc_id,
    mdoc_werks,
    new_store_id,
    case when delivery_mode in ('B','C','D') then '1' else '0' end as is_zs,
    matnr,
    sales_mode,
    out_stock_qty as out_stock_qty,
    out_stock_qty_give as out_stock_qty_give,
    out_stock_original_amt as out_stock_original_amt,
    out_stock_pay_amt as out_stock_pay_amt,
    out_stock_pay_amt_notax as out_stock_pay_amt_notax,
    out_stock_amt_cb_notax as out_stock_amt_cb_notax,
    out_stock_give_amt_cb_notax as out_stock_give_amt_cb_notax,
    return_stock_qty as return_stock_qty,
    return_stock_original_amt as return_stock_original_amt,
    return_stock_pay_amt as return_stock_pay_amt,
    return_stock_pay_amt_notax as return_stock_pay_amt_notax,
    return_stock_amt_cb_notax as return_stock_amt_cb_notax,
    out_stock_zzckj_amt as out_stock_zzckj_amt,
    out_stock_amt_zzckj_nottax as out_stock_amt_zzckj_nottax,
    out_stock_zzddj_amt as out_stock_zzddj_amt,
    out_stock_amt_zzddj_nottax as out_stock_amt_zzddj_nottax,
    original_outstock_qty as original_outstock_qty,
    original_outstock_amt as original_outstock_amt,
    promotion_outstock_price as promotion_outstock_price,
    promotion_outstock_qty as promotion_outstock_qty,
    promotion_outstock_amt as promotion_outstock_amt,
    out_stock_amt_cb as out_stock_amt_cb,
    return_stock_amt_cb as return_stock_amt_cb,
     out_stock_date
from
    tmp_dal.dal_dc_matnr_store_base_wide23_finally_sr 
where inc_day  between '$start_date' and '$end_date'
 ) a left join
 ( 
select 
t.out_stock_date,
t.mdoc_werks,
t.mdoc_matnr matnr,
t.dcmove_out_amt_cb_notax,
t.dcmove_out_amt_cb,
t1.out_stock_qty,
t.dcmove_return_amt_cb_notax,
t.dcmove_return_amt_cb,
t1.return_stock_qty,
if(t1.out_stock_qty>0,t.dcmove_out_amt_cb_notax/t1.out_stock_qty,0) avg_cost_price_notax,
if(t1.out_stock_qty>0,t.dcmove_out_amt_cb/t1.out_stock_qty,0) avg_cost_price,
if(abs(t1.return_stock_qty)>0,abs(t.dcmove_return_amt_cb_notax/t1.return_stock_qty),0) avg_return_cost_price_notax,
if(abs(t1.return_stock_qty)>0,abs(t.dcmove_return_amt_cb/t1.return_stock_qty),0) avg_return_cost_price
from (
select  case when case when ek_zpslx = '' or ek_zpslx is null then 'A' else ek_zpslx end = 'C' then inc_day
        when abs(return_stock_qty)>0 then inc_day
         else date(date_add(inc_day,1)) end as out_stock_date,
mdoc_matnr ,
mdoc_werks,
sum(dcmove_out_amt_cb_notax) dcmove_out_amt_cb_notax,
sum(dcmove_out_amt_cb_notax*(1+coalesce(case when ek_pstyp='7' then sc_zsl else zsl end ,0))) dcmove_out_amt_cb,
sum(dcmove_return_amt_cb_notax) dcmove_return_amt_cb_notax,
sum(dcmove_return_amt_cb_notax*(1+coalesce(case when ek_pstyp='7' then sc_zsl else zsl end ,0))) dcmove_return_amt_cb
from dsl.dsl_scm_info_sap_global_di
    where  inc_day  between date(adddate('$start_date',-1)) and '$end_date'
	and mdoc_wempf = 'D093'
    group by case when case when ek_zpslx = '' or ek_zpslx is null then 'A' else ek_zpslx end = 'C' then inc_day
        when abs(return_stock_qty)>0 then inc_day
         else date(date_add(inc_day,1)) end,mdoc_matnr,mdoc_werks
) t	
	inner join (
	select case when case when ek_zpslx = '' or ek_zpslx is null then 'A' else ek_zpslx end = 'C' then inc_day
        when abs(return_stock_qty)>0 then inc_day
         else date(date_add(inc_day,1)) end as out_stock_date,
mdoc_matnr,
mdoc_lgort_sid mdoc_werks,
sum(out_stock_qty) out_stock_qty ,
sum(return_stock_qty) return_stock_qty 
from dsl.dsl_scm_info_sap_global_di
    where  inc_day  between date(adddate('$start_date',-1)) and '$end_date'
	and mdoc_werks = 'D093'
    group by case when case when ek_zpslx = '' or ek_zpslx is null then 'A' else ek_zpslx end = 'C' then inc_day
        when abs(return_stock_qty)>0 then inc_day
         else date(date_add(inc_day,1)) end,mdoc_matnr,mdoc_lgort_sid		
     ) t1 on t.out_stock_date=t1.out_stock_date and t.mdoc_matnr=t1.mdoc_matnr and t.mdoc_werks=t1.mdoc_werks
) b on a.out_stock_date=b.out_stock_date and a.matnr=b.matnr and a.new_dc_id=b.mdoc_werks
 where a.out_stock_date  between '$start_date' and '$end_date'
group by a.new_dc_id,a.new_store_id,a.is_zs,a.matnr,a.sales_mode,a.out_stock_date;









drop table if exists tmp_dal.dal_dc_matnr_store_mode_purchase_wide634_sr force;    
create table tmp_dal.dal_dc_matnr_store_mode_purchase_wide634_sr
as
select
    ean11 --string comment '订购ean',
    ,ean12 --string comment '结算ean',
    ,dc_id --string COMMENT '仓库编号',
    ,sp_store_id --string COMMENT '店铺id',
    ,matnr --string COMMENT '物料号',  
    ,meins1 --string COMMENT '订购基本单位',
    ,meins2 --string COMMENT '结算基本单位',
    ,coalesce(select_unit,'') as select_unit  --wms分拣单位
    ,coalesce(weightflag,'') as weightflag
    ,zpslx
    ,ek_zzxsfs
    ,sum(order_qty_orderean) as order_qty_orderean --double COMMENT '订购数量(订购单位)',
    ,sum(order_qty_payean) as order_qty_payean --double COMMENT '订购数量(结算单位)',
    ,sum(case when zsfzp='X' then order_qty_payean else 0 end ) as order_qty_gift_payean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_payean end ) as order_qty_value_payean --double COMMENT '有价订购量(结算单位)'
    ,sum(order_amt) as order_amt --double COMMENT '订购金额',
    ,sum(store_recive_amt) as store_recive_amt --double COMMENT '门店收货金额',
    ,sum(store_recive_qty_payean) as store_recive_qty_payean --double COMMENT '门店收货数量(订购单位)',
    ,sum(store_recive_qty_orderean) as store_recive_qty_orderean --double COMMENT '门店收货数量(结算单位)',
    ,sum(case when zsfzp='X' then store_recive_qty_payean else 0 end ) as out_stock_qty_gift --double COMMENT '赠品出库量'
    ,sum(case when zsfzp='X' then 0 else store_recive_qty_payean end ) as out_stock_qty_value --double COMMENT '赠品出库量'
    ,sum(expect_weight) as expect_weight --double COMMENT '理论收货重量',
    ,sum(expect_order_weight) as expect_order_weight --double COMMENT '理论订购重量',
    ,case when sum(order_qty_orderean)>0 then 1 else 0 end as order_num --int comment '订单次数'
    ,case 
        when meins2="KG" then sum(store_recive_qty_payean)/sum(order_qty_payean)
        else case when sum(store_recive_qty_payean)=sum(order_qty_payean) then 1 else 0 end
     end as order_percent
    --,sum(case when order_qty_orderean>0 and store_recive_qty_payean=0 then 1 else 0 end ) as is_zero_num
    ,sum(out_stock_qty_kg2fen) as out_stock_qty_kg2fen
           -- 2021年2月18日 sr begin
    ,sum(case when zsfzp='X' then order_qty_orderean else 0 end ) as order_qty_gift_orderean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_orderean end ) as order_qty_value_orderean --double COMMENT '有价订购量(结算单位)'
    -- 2021年2月18日 sr end
    ,sum(purchase_qty) as  purchase_qty --wms订购数量
    ,sum(purchase_weight) as purchase_weight  --wms订购重量
    ,sum(select_qty) as select_qty  --wms分拣数量
    ,sum(select_weight) as select_weight  --wms分拣重量
    ,case when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_weight) -- A进B出非称重，如蓝莓，用分拣数量/订购重量 
          when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') = meins2 then sum(select_qty)/sum(purchase_qty)
          when coalesce(weightflag,'')='1'  and coalesce(select_unit,'') = meins2 then case when sum(select_weight)>0 then sum(select_weight)/sum(purchase_weight) else sum(store_recive_qty_orderean)/sum(order_qty_orderean) end
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_qty)  -- A进B出，订购按份数，结算按重量的商品，计算满足率时候只要满足数量即可，如鱼，订购条，按KG结算
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 and sum(select_qty) = 0 then sum(select_weight)/sum(purchase_weight) -- 容错写法，避免数据异常导致满足率有误
    else 0 end as satisfy_rate     -- satisfy_rate
    ,sum(select_af_qty) as select_af_qty
    ,sum(select_af_weight) as select_af_weight
    ,inc_day
from
    dsl.dsl_scm_store_purchase_info_di
where
  inc_day between '$start_date' and '$end_date'
and 
    is_bom_false='0' 
and 
    zpslx <> 'E'
and 
    (case when inc_day>='2021-02-27' and inc_day<'2021-12-26' then  (new_dc_id in ('D001','D012','D064') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%')
    when inc_day>='2021-12-26' then  (new_dc_id in ('D001','D012','D064','D056','D027') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%' or dc_name like '%中山%水产%')
    ELSE  (new_dc_id in ('D001') or dc_name like '%广州%水产%') 
    END )
and 
    procerss_ean11 not in ('20500580','20500566','20510985')
group by
    ean11,
    ean12,
    matnr,
    dc_id,
    sp_store_id,
    meins1,
    meins2,
    coalesce(select_unit,''),  --wms分拣单位
    coalesce(weightflag,''),
  zpslx,
  ek_zzxsfs,
    inc_Day

union all
--自出库日期2022年2月24日-4月28日广州和中山水产仓原条恢复，取消bom
--自出库日期2022年4月29日中山水产仓水库鲩鱼从原条变回散件
select
    ean11 --string comment '订购ean',
    ,ean12 --string comment '结算ean',
    ,dc_id --string COMMENT '仓库编号',
    ,sp_store_id --string COMMENT '店铺id',
    ,matnr --string COMMENT '物料号',
    ,meins1 --string COMMENT '订购基本单位',
    ,meins2 --string COMMENT '结算基本单位',
    ,coalesce(select_unit,'') as select_unit  --wms分拣单位
    ,coalesce(weightflag,'') as weightflag
    ,zpslx
    ,ek_zzxsfs
    ,sum(order_qty_orderean) as order_qty_orderean --double COMMENT '订购数量(订购单位)',
    ,sum(order_qty_payean) as order_qty_payean --double COMMENT '订购数量(结算单位)',
    ,sum(case when zsfzp='X' then order_qty_payean else 0 end ) as order_qty_gift_payean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_payean end ) as order_qty_value_payean --double COMMENT '有价订购量(结算单位)'
    ,sum(order_amt) as order_amt --double COMMENT '订购金额',
    ,sum(store_recive_amt) as store_recive_amt --double COMMENT '门店收货金额',
    ,sum(store_recive_qty_payean) as store_recive_qty_payean --double COMMENT '门店收货数量(订购单位)',
    ,sum(store_recive_qty_orderean) as store_recive_qty_orderean --double COMMENT '门店收货数量(结算单位)',
    ,sum(case when zsfzp='X' then store_recive_qty_payean else 0 end ) as out_stock_qty_gift --double COMMENT '赠品出库量'
    ,sum(case when zsfzp='X' then 0 else store_recive_qty_payean end ) as out_stock_qty_value --double COMMENT '赠品出库量'
    ,sum(expect_weight) as expect_weight --double COMMENT '理论收货重量',
    ,sum(expect_order_weight) as expect_order_weight --double COMMENT '理论订购重量',
    ,case when sum(order_qty_orderean)>0 then 1 else 0 end as order_num --int comment '订单次数'
    ,case 
        when meins2="KG" then sum(store_recive_qty_payean)/sum(order_qty_payean)
        else case when sum(store_recive_qty_payean)=sum(order_qty_payean) then 1 else 0 end
    end as order_percent
    --,sum(case when order_qty_orderean>0 and store_recive_qty_payean=0 then 1 else 0 end ) as is_zero_num
    ,sum(out_stock_qty_kg2fen) as out_stock_qty_kg2fen
           -- 2021年2月18日 sr begin
    ,sum(case when zsfzp='X' then order_qty_orderean else 0 end ) as order_qty_gift_orderean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_orderean end ) as order_qty_value_orderean --double COMMENT '有价订购量(结算单位)'
    -- 2021年2月18日 sr end
    ,sum(purchase_qty) as  purchase_qty --wms订购数量
    ,sum(purchase_weight) as purchase_weight  --wms订购重量
    ,sum(select_qty) as select_qty  --wms分拣数量
    ,sum(select_weight) as select_weight  --wms分拣重量
    ,case when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_weight) -- A进B出非称重，如蓝莓，用分拣数量/订购重量 
    when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') = meins2 then sum(select_qty)/sum(purchase_qty)
          when coalesce(weightflag,'')='1'  and coalesce(select_unit,'') = meins2 then case when sum(select_weight)>0 then sum(select_weight)/sum(purchase_weight) else sum(store_recive_qty_orderean)/sum(order_qty_orderean) end
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_qty)  -- A进B出，订购按份数，结算按重量的商品，计算满足率时候只要满足数量即可，如鱼，订购条，按KG结算
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 and sum(select_qty) = 0 then sum(select_weight)/sum(purchase_weight) -- 容错写法，避免数据异常导致满足率有误
    else 0 end as satisfy_rate     -- satisfy_rate
    ,sum(select_af_qty) as select_af_qty
    ,sum(select_af_weight) as select_af_weight
    ,inc_day
from
    dsl.dsl_scm_store_purchase_info_di
where
inc_day >= '2022-02-24'
and 
    inc_day  between '$start_date' and '$end_date'
and 
    is_bom_false='0' 
and 
    zpslx <> 'E' 
and 
    (case when inc_day>='2021-02-27' and inc_day<'2021-12-26' then  (new_dc_id in ('D001','D012','D064') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%')
    when inc_day>='2021-12-26' then  (new_dc_id in ('D001','D012','D064','D056','D027') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%' or dc_name like '%中山%水产%')
    ELSE  (new_dc_id in ('D001') or dc_name like '%广州%水产%') 
    END )
and
(
(case when inc_day >= '2022-02-24' and inc_day <= '2022-04-28' then
(matnr in ('000000000000101919' ,'000000000000101981')
and new_dc_id  in ('W10102','W10117','D001','D027','W10128','W10129','D056') ) end )
or
(case when inc_day >= '2022-04-29' and inc_day <= '2022-05-25'  then
((matnr = '000000000000101981'
and new_dc_id  in ('W10102','W10117','D001','D027','W10128','W10129','D056') ) 
or 
(matnr = '000000000000101919' 
and new_dc_id in ('W10102','W10117','D001'))) end 
)
or
(
case when inc_day >= '2022-05-26'  then
(matnr in ('000000000000101981','000000000000101919')
and new_dc_id  in ('W10102','W10117','D001') ) 
end 
)
)
group by
    ean11,
    ean12,
    matnr,
    dc_id,
    sp_store_id,
    meins1,
    meins2,
    coalesce(select_unit,''),  --wms分拣单位
    coalesce(weightflag,''),
  zpslx,
  ek_zzxsfs,
    inc_Day
    
union all
--深圳鱼仓bom    
select
     a.ean11 as ean11--string comment '订购ean',
    ,a.ean12 as ean12 --string comment '结算ean',
    ,a.dc_id as dc_id --string COMMENT '仓库编号',
    ,a.sp_store_id as sp_store_id --string COMMENT '店铺id',
    ,a.matnr as matnr --string COMMENT '物料号',
    ,a.meins1 as meins1--string COMMENT '订购基本单位',
    ,a.meins2 as meins2--string COMMENT '结算基本单位',
    ,a.select_unit as select_unit  --wms分拣单位
    ,a.weightflag as weightflag
  ,a.zpslx as zpslx
  ,a.ek_zzxsfs as ek_zzxsfs
    ,a.order_qty_orderean as order_qty_orderean --double COMMENT '订购数量(订购单位)',
    ,a.order_qty_payean as order_qty_payean --double COMMENT '订购数量(结算单位)',
    ,a.order_qty_gift_payean as order_qty_gift_payean --double COMMENT '赠品订购量(结算单位)'
    ,a.order_qty_value_payean as order_qty_value_payean --double COMMENT '有价订购量(结算单位)'
    ,a.order_amt as order_amt --double COMMENT '订购金额',
    ,a.store_recive_amt as store_recive_amt --double COMMENT '门店收货金额',
    ,a. store_recive_qty_payean as store_recive_qty_payean --double COMMENT '门店收货数量(订购单位)',
    ,a.store_recive_qty_orderean as store_recive_qty_orderean --double COMMENT '门店收货数量(结算单位)',
    ,a.out_stock_qty_gift as out_stock_qty_gift --double COMMENT '赠品出库量'
    ,a.out_stock_qty_value as out_stock_qty_value --double COMMENT '赠品出库量'
    ,a.expect_weight as expect_weight --double COMMENT '理论收货重量',
    ,a.expect_order_weight as expect_order_weight --double COMMENT '理论订购重量',
    ,a.order_num as order_num --int comment '订单次数'
    ,a.order_percent as order_percent
    --,sum(case when order_qty_orderean>0 and store_recive_qty_payean=0 then 1 else 0 end ) as is_zero_num
    ,a.out_stock_qty_kg2fen as out_stock_qty_kg2fen
           -- 2021年2月18日 sr begin
    ,a.order_qty_gift_orderean as order_qty_gift_orderean --double COMMENT '赠品订购量(结算单位)'
    ,a.order_qty_value_orderean as order_qty_value_orderean --double COMMENT '有价订购量(结算单位)'
    -- 2021年2月18日 sr end
    ,a.purchase_qty as  purchase_qty --wms订购数量
    ,a.purchase_weight as purchase_weight  --wms订购重量
    ,a.select_qty as select_qty  --wms分拣数量
    ,a.select_weight as select_weight  --wms分拣重量
    ,a.satisfy_rate as satisfy_rate
  ,a.select_af_qty as select_af_qty
  ,a.select_af_weight as select_af_weight
    ,a.inc_day as inc_day
from
(
select
    ean11 --string comment '订购ean',
    ,ean12 --string comment '结算ean',
    ,dc_id --string COMMENT '仓库编号',
    ,sp_store_id --string COMMENT '店铺id',
    ,matnr --string COMMENT '物料号',
    ,meins1 --string COMMENT '订购基本单位',
    ,meins2 --string COMMENT '结算基本单位',
    ,coalesce(select_unit,'') as select_unit  --wms分拣单位
    ,coalesce(weightflag,'') as weightflag
  ,zpslx
  ,ek_zzxsfs
    ,sum(order_qty_orderean) as order_qty_orderean --double COMMENT '订购数量(订购单位)',
    ,sum(order_qty_payean) as order_qty_payean --double COMMENT '订购数量(结算单位)',
    ,sum(case when zsfzp='X' then order_qty_payean else 0 end ) as order_qty_gift_payean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_payean end ) as order_qty_value_payean --double COMMENT '有价订购量(结算单位)'
    ,sum(order_amt) as order_amt --double COMMENT '订购金额',
    ,sum(store_recive_amt) as store_recive_amt --double COMMENT '门店收货金额',
    ,sum(store_recive_qty_payean) as store_recive_qty_payean --double COMMENT '门店收货数量(订购单位)',
    ,sum(store_recive_qty_orderean) as store_recive_qty_orderean --double COMMENT '门店收货数量(结算单位)',
    ,sum(case when zsfzp='X' then store_recive_qty_payean else 0 end ) as out_stock_qty_gift --double COMMENT '赠品出库量'
    ,sum(case when zsfzp='X' then 0 else store_recive_qty_payean end ) as out_stock_qty_value --double COMMENT '赠品出库量'
    ,sum(expect_weight) as expect_weight --double COMMENT '理论收货重量',
    ,sum(expect_order_weight) as expect_order_weight --double COMMENT '理论订购重量',
    ,case when sum(order_qty_orderean)>0 then 1 else 0 end as order_num --int comment '订单次数'
    ,case 
        when meins2="KG" then sum(store_recive_qty_payean)/sum(order_qty_payean)
        else case when sum(store_recive_qty_payean)=sum(order_qty_payean) then 1 else 0 end
    end as order_percent
    --,sum(case when order_qty_orderean>0 and store_recive_qty_payean=0 then 1 else 0 end ) as is_zero_num
    ,sum(out_stock_qty_kg2fen) as out_stock_qty_kg2fen
           -- 2021年2月18日 sr begin
    ,sum(case when zsfzp='X' then order_qty_orderean else 0 end ) as order_qty_gift_orderean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_orderean end ) as order_qty_value_orderean --double COMMENT '有价订购量(结算单位)'
    -- 2021年2月18日 sr end
    ,sum(purchase_qty) as  purchase_qty --wms订购数量
    ,sum(purchase_weight) as purchase_weight  --wms订购重量
    ,sum(select_qty) as select_qty  --wms分拣数量
    ,sum(select_weight) as select_weight  --wms分拣重量
    ,case when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_weight) -- A进B出非称重，如蓝莓，用分拣数量/订购重量 
    when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') = meins2 then sum(select_qty)/sum(purchase_qty)
          when coalesce(weightflag,'')='1'  and coalesce(select_unit,'') = meins2 then case when sum(select_weight)>0 then sum(select_weight)/sum(purchase_weight) else sum(store_recive_qty_orderean)/sum(order_qty_orderean) end
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_qty)  -- A进B出，订购按份数，结算按重量的商品，计算满足率时候只要满足数量即可，如鱼，订购条，按KG结算
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 and sum(select_qty) = 0 then sum(select_weight)/sum(purchase_weight) -- 容错写法，避免数据异常导致满足率有误
    else 0 end as satisfy_rate     -- satisfy_rate
  ,sum(select_af_qty) as select_af_qty
  ,sum(select_af_weight) as select_af_weight
    ,inc_day
from
    dsl.dsl_scm_store_purchase_info_di
where
    inc_day between '$start_date' and '$end_date'
and 
    is_bom_false='1' 
and 
    zpslx <> 'E' 
and 
   (case when inc_day>='2021-02-27' and inc_day<'2021-12-26' then  (new_dc_id in ('D001','D012','D064') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%')
    when inc_day>='2021-12-26' then  (new_dc_id in ('D001','D012','D064','D056','D027') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%' or dc_name like '%中山%水产%')
    ELSE  (new_dc_id in ('D001') or dc_name like '%广州%水产%') 
    END )
and 
    procerss_ean11 in ('20500580','20500566','20510985')
group by
    ean11,
    ean12,
    matnr,
    dc_id,
    sp_store_id,
    meins1,
    meins2,
    coalesce(select_unit,''),  --wms分拣单位
    coalesce(weightflag,''),
  zpslx,
  ek_zzxsfs,
    inc_Day
) a
left join
(
select
    ean11 --string comment '订购ean',
    ,ean12 --string comment '结算ean',
    ,dc_id --string COMMENT '仓库编号',
    ,sp_store_id --string COMMENT '店铺id',
    ,matnr --string COMMENT '物料号',
    ,meins1 --string COMMENT '订购基本单位',
    ,meins2 --string COMMENT '结算基本单位',
    ,coalesce(select_unit,'') as select_unit  --wms分拣单位
    ,coalesce(weightflag,'') as weightflag
  ,zpslx
  ,ek_zzxsfs
    ,sum(order_qty_orderean) as order_qty_orderean --double COMMENT '订购数量(订购单位)',
    ,sum(order_qty_payean) as order_qty_payean --double COMMENT '订购数量(结算单位)',
    ,sum(case when zsfzp='X' then order_qty_payean else 0 end ) as order_qty_gift_payean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_payean end ) as order_qty_value_payean --double COMMENT '有价订购量(结算单位)'
    ,sum(order_amt) as order_amt --double COMMENT '订购金额',
    ,sum(store_recive_amt) as store_recive_amt --double COMMENT '门店收货金额',
    ,sum(store_recive_qty_payean) as store_recive_qty_payean --double COMMENT '门店收货数量(订购单位)',
    ,sum(store_recive_qty_orderean) as store_recive_qty_orderean --double COMMENT '门店收货数量(结算单位)',
    ,sum(case when zsfzp='X' then store_recive_qty_payean else 0 end ) as out_stock_qty_gift --double COMMENT '赠品出库量'
    ,sum(case when zsfzp='X' then 0 else store_recive_qty_payean end ) as out_stock_qty_value --double COMMENT '赠品出库量'
    ,sum(expect_weight) as expect_weight --double COMMENT '理论收货重量',
    ,sum(expect_order_weight) as expect_order_weight --double COMMENT '理论订购重量',
    ,case when sum(order_qty_orderean)>0 then 1 else 0 end as order_num --int comment '订单次数'
    ,case 
        when meins2="KG" then sum(store_recive_qty_payean)/sum(order_qty_payean)
        else case when sum(store_recive_qty_payean)=sum(order_qty_payean) then 1 else 0 end
    end as order_percent
    --,sum(case when order_qty_orderean>0 and store_recive_qty_payean=0 then 1 else 0 end ) as is_zero_num
    ,sum(out_stock_qty_kg2fen) as out_stock_qty_kg2fen
           -- 2021年2月18日 sr begin
    ,sum(case when zsfzp='X' then order_qty_orderean else 0 end ) as order_qty_gift_orderean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_orderean end ) as order_qty_value_orderean --double COMMENT '有价订购量(结算单位)'
    -- 2021年2月18日 sr end
    ,inc_day
from
    dsl.dsl_scm_store_purchase_info_di
where
inc_day >= '2022-02-24'
and
    inc_day between '$start_date' and '$end_date'
and 
    is_bom_false='1' 
and 
    zpslx <> 'E' 
and 
   (case when inc_day>='2021-02-27' and inc_day<'2021-12-26' then  (new_dc_id in ('D001','D012','D064') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%')
    when inc_day>='2021-12-26' then  (new_dc_id in ('D001','D012','D064','D056','D027') or dc_name like '%广州%水产%' or dc_name like '%深圳%水产%' or dc_name like '%中山%水产%')
    ELSE  (new_dc_id in ('D001') or dc_name like '%广州%水产%') 
    END )
and 
    procerss_ean11 in ('20500580','20500566','20510985')
and
(
(case when inc_day >= '2022-02-24' and inc_day <= '2022-04-28' then
(matnr  in ('000000000000101982','000000000000101983','000000000000101985','000000000000101920','000000000000101921','000000000000101923') 
and 
new_dc_id  in ('W10102','W10117','D001','D027','W10128','W10129','D056') ) end 
)
or
(case when inc_day >= '2022-04-29' and inc_day <= '2022-05-25' then
(
(matnr  in ('000000000000101982','000000000000101983','000000000000101985') 
and 
new_dc_id  in ('W10102','W10117','D001','D027','W10128','W10129','D056') )
or
(
matnr  in ('000000000000101920','000000000000101921','000000000000101923')
and
new_dc_id in ('W10102','W10117','D001')
)) end
)
or
(
case when inc_day >= '2022-05-26' then
(matnr  in ('000000000000101982','000000000000101983','000000000000101985','000000000000101920','000000000000101921','000000000000101923') 
and 
new_dc_id  in ('W10102','W10117','D001') )
end
)
)
group by
    ean11,
    ean12,
    matnr,
    dc_id,
    sp_store_id,
    meins1,
    meins2,
    coalesce(select_unit,''),  --wms分拣单位
    coalesce(weightflag,''),
  zpslx,
  ek_zzxsfs,
    inc_Day
) b
on
a.ean11 = b.ean11
and a.ean12 = b.ean12
and a.matnr = b.matnr
and a.dc_id = b.dc_id
and a.sp_store_id = b.sp_store_id
and a.meins1 = b.meins1
and a.meins2 = b.meins2
and a.select_unit = b.select_unit
and a.weightflag = b.weightflag
and a.inc_Day = b.inc_Day
and a.zpslx = b.zpslx
and a.ek_zzxsfs = b.ek_zzxsfs
where b.inc_Day is null

union all
--其余
select
    ean11 --string comment '订购ean',
    ,ean12 --string comment '结算ean',
    ,dc_id --string COMMENT '仓库编号',
    ,sp_store_id --string COMMENT '店铺id',
    ,matnr --string COMMENT '物料号',
    ,meins1 --string COMMENT '订购基本单位',
    ,meins2 --string COMMENT '结算基本单位',
    ,coalesce(select_unit,'') as select_unit  --wms分拣单位
    ,coalesce(weightflag,'') as weightflag
  ,min(zpslx) as zpslx
  ,ek_zzxsfs
    ,sum(order_qty_orderean) as order_qty_orderean --double COMMENT '订购数量(订购单位)',
    ,sum(order_qty_payean) as order_qty_payean --double COMMENT '订购数量(结算单位)',
    ,sum(case when zsfzp='X' then order_qty_payean else 0 end ) as order_qty_gift_payean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_payean end ) as order_qty_value_payean --double COMMENT '有价订购量(结算单位)'
    ,sum(order_amt) as order_amt --double COMMENT '订购金额',
    ,sum(store_recive_amt) as store_recive_amt --double COMMENT '门店收货金额',
    ,sum(store_recive_qty_payean) as store_recive_qty_payean --double COMMENT '门店收货数量(订购单位)',
    ,sum(store_recive_qty_orderean) as store_recive_qty_orderean --double COMMENT '门店收货数量(结算单位)',
    ,sum(case when zsfzp='X' then store_recive_qty_payean else 0 end ) as out_stock_qty_gift --double COMMENT '赠品出库量'
    ,sum(case when zsfzp='X' then 0 else store_recive_qty_payean end ) as out_stock_qty_value --double COMMENT '赠品出库量'
    ,sum(expect_weight) as expect_weight --double COMMENT '理论收货重量',
    ,sum(expect_order_weight) as expect_order_weight --double COMMENT '理论订购重量',
    ,case when sum(order_qty_orderean)>0 then 1 else 0 end as order_num --int comment '订单次数'
    ,case 
        when meins2="KG" then sum(store_recive_qty_payean)/sum(order_qty_payean)
        else case when sum(store_recive_qty_payean)=sum(order_qty_payean) then 1 else 0 end
    end as order_percent
    --,sum(case when order_qty_orderean>0 and store_recive_qty_payean=0 then 1 else 0 end ) as is_zero_num
    ,sum(out_stock_qty_kg2fen) as out_stock_qty_kg2fen
           -- 2021年2月18日 sr begin
    ,sum(case when zsfzp='X' then order_qty_orderean else 0 end ) as order_qty_gift_orderean --double COMMENT '赠品订购量(结算单位)'
    ,sum(case when zsfzp='X' then 0 else order_qty_orderean end ) as order_qty_value_orderean --double COMMENT '有价订购量(结算单位)'
    -- 2021年2月18日 sr end
    ,sum(purchase_qty) as  purchase_qty --wms订购数量
    ,sum(purchase_weight) as purchase_weight  --wms订购重量
    ,sum(select_qty) as select_qty  --wms分拣数量
    ,sum(select_weight) as select_weight  --wms分拣重量
    ,case when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_weight) -- A进B出非称重，如蓝莓，用分拣数量/订购重量 
    when coalesce(weightflag,'')<>'1' and coalesce(select_unit,'') = meins2 then sum(select_qty)/sum(purchase_qty)
          when coalesce(weightflag,'')='1'  and coalesce(select_unit,'') = meins2 then case when sum(select_weight)>0 then sum(select_weight)/sum(purchase_weight) else sum(store_recive_qty_orderean)/sum(order_qty_orderean) end
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 then sum(select_qty)/sum(purchase_qty)  -- A进B出，订购按份数，结算按重量的商品，计算满足率时候只要满足数量即可，如鱼，订购条，按KG结算
          when coalesce(weightflag,'')='1' and coalesce(select_unit,'') <> meins2 and sum(select_qty) = 0 then sum(select_weight)/sum(purchase_weight) -- 容错写法，避免数据异常导致满足率有误
    else 0 end as satisfy_rate     -- satisfy_rate
  ,sum(select_af_qty) as select_af_qty
  ,sum(select_af_weight) as select_af_weight
    ,inc_day
from
    dsl.dsl_scm_store_purchase_info_di
where
    inc_day between '$start_date' and '$end_date'
and 
    zpslx <> 'E'
and 
       (case when inc_day>='2021-02-27' and inc_day<'2021-12-26' then  (new_dc_id not in ('D001','D012','D064') and dc_name not like '%广州%水产%' and dc_name not like '%深圳%水产%')
    when inc_day>='2021-12-26' then  (new_dc_id not in ('D001','D012','D064','D056','D027') and dc_name not like '%广州%水产%' and dc_name not like '%深圳%水产%' and dc_name not like '%中山%水产%')
    ELSE  (new_dc_id not in ('D001') and dc_name not like '%广州%水产%') 
    END )
group by
    ean11,
    ean12,
    matnr,
    dc_id,
    sp_store_id,
    meins1,
    meins2,
    coalesce(select_unit,''),  --wms分拣单位
    coalesce(weightflag,''),
  ek_zzxsfs,
    inc_Day    ;










drop table if exists tmp_dal.dal_dc_matnr_store_order_wide25_finally_sr force;  
create table tmp_dal.dal_dc_matnr_store_order_wide25_finally_sr
as
select
    a.out_stock_date as out_stock_date,
    a.new_dc_id as new_dc_id,
    a.is_zs as is_zs,
    a.matnr as matnr,
    a.new_store_id as new_store_id,
    a.ek_zzxsfs as sales_mode,
    sum(a.order_qty_orderean) as order_qty_orderean,
    sum(a.order_qty_payean) as order_qty_payean,
    sum(a.order_qty_gift_orderean) as order_qty_gift_orderean,
    sum(a.order_qty_gift_payean) as order_qty_gift_payean,
    sum(a.order_qty_orderean-a.order_qty_gift_orderean) as order_qty_value_orderean,
    sum(a.order_qty_payean-a.order_qty_gift_payean) as order_qty_value_payean,
    sum(a.order_amt) as order_amt,
    sum(a.store_recive_qty_orderean) as store_recive_qty_orderean,
    sum(a.store_recive_qty_payean) as store_recive_qty_payean,
    sum(a.store_recive_amt) as store_recive_amt,
    sum(case when a.order_qty_payean>0 then 1 else 0 end) as order_num,
    count(distinct case when a.order_qty_payean>0 then a.new_store_id end ) as sp_order_num,  --订购门店次数
    count(distinct case when a.order_qty_payean > 0 and a.store_recive_qty_payean <  a.order_qty_payean then a.new_store_id end) as miss_stock_sp_num,  --少货门店次数
    sum(case when a.order_qty_payean > 0 and a.store_recive_qty_payean <  a.order_qty_payean then a.order_qty_payean - store_recive_qty_payean else 0 end) as miss_stock_qty,  --少货数量
    sum(case when a.order_qty_payean > 0 and a.store_recive_qty_payean <  a.order_qty_payean then a.order_amt - a.store_recive_amt else 0 end) as miss_stock_amt,  --少货金额
    count(distinct case when a.order_qty_payean > 0 and a.store_recive_qty_payean = 0 then a.new_store_id end) as is_zero_num,  --零出库订单数
    count(distinct case when a.order_qty_payean > 0 and a.store_recive_qty_payean = 0 then a.new_store_id end) as zero_sp_num,  --零出库门店次数
    sum(case when a.order_qty_payean > 0 and a.store_recive_qty_payean = 0 then a.order_qty_payean else 0 end) as zero_sp_order_qty,  --零出库订单订购数量(结算单位)
    sum(case when a.order_qty_payean > 0 and a.store_recive_qty_payean = 0 then a.order_amt else 0 end) as zero_sp_order_amt,  --零出库订单理论出库金额
    sum(a.is_over_num) as is_over_num,
    sum(a.over_qty) as over_qty,
    sum(a.over_amt) as over_amt,
    sum(a.is_lower_num) as is_lower_num,
    sum(a.lower_qty) as lower_qty,
    sum(a.lower_amt) as lower_amt,
    sum(a.order_qty_payean-a.over_qty-a.lower_qty) as satisfied_qty,  --满足率满足的订单数量
    sum(a.order_amt-a.over_amt-a.lower_amt) as satisfied_amt,  --满足率满足的订单金额
    sum(a.order_qty_payean_tokg) as order_qty_payean_tokg,
    sum((case when d2.sales_unit_id = 'KG' then ceil(a.order_qty_payean/(if(if(d2.min_pack_weight is null,1,d2.min_pack_weight) != 0,if(d2.min_pack_weight is null,1,d2.min_pack_weight),1))) 
when d2.sales_unit_id = 'ZGJ' then ceil(a.order_qty_payean*(if(if(d2.unit_weight is null,1,d2.unit_weight) != 0,if(d2.unit_weight is null,1,d2.unit_weight),1))/(if(if(d2.min_pack_weight is null,1,d2.min_pack_weight) != 0,if(d2.min_pack_weight is null,1,d2.min_pack_weight),1)))
else ceil(a.order_qty_payean) end)) as order_qty_payean_kgfen,
sum(is_satisfied_num) as is_satisfied_num,
    a.inc_day as inc_day
from
(
select
    a.inc_day as out_stock_date,
    c.new_dc_id as new_dc_id,
    d.new_sp_store_id as new_store_id,
    case when a.zpslx in ('B','C','D') then '1' else '0' end as is_zs,
    a.matnr as matnr,
    a.ek_zzxsfs as ek_zzxsfs,
    sum(coalesce(b.menge1,a.order_qty_orderean) ) as order_qty_orderean,
    sum(coalesce(b.menge2,a.order_qty_payean)) as order_qty_payean,
    sum(coalesce(b.order_qty_gift_orderean,a.order_qty_gift_orderean)) as order_qty_gift_orderean,
    sum(coalesce(b.order_qty_gift_payean,a.order_qty_gift_payean)) as order_qty_gift_payean,
    sum(coalesce(b.menge2_amt,a.order_amt)) as order_amt,
    sum(coalesce(b.menge1_recive,a.store_recive_qty_orderean)) as store_recive_qty_orderean,
    sum(coalesce(b.menge2_recive,a.store_recive_qty_payean)) as store_recive_qty_payean,
    sum(coalesce(b.menge2_recive_amt,a.store_recive_amt)) as store_recive_amt,
    sum(select_qty) as select_qty,
    sum(case when a.select_qty is null or a.select_qty = '' then
    case when a.meins1 = a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1.15 then 1 else 0 end)
    when a.meins1 = a.meins2 and goods.weight_flag='0' then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1 then 1 else 0 end)
    when a.meins1 != a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_orderean=0 then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1.15 then 1 else 0 end)
    else( case when store_recive_qty_orderean/order_qty_orderean>1.15 then 1 else 0 end) end)
    else( case when store_recive_qty_payean/order_qty_payean>1 then 1 else 0 end) end
    else (case when  a.weightflag = '1' and a.satisfy_rate > 1.15 then 1 when  a.weightflag = '0' and a.satisfy_rate > 1 then 1 else 0 end) end) as is_over_num,
    sum(case when a.meins1=a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1.15 then (a.store_recive_qty_payean/a.order_qty_payean-1.15) * a.order_qty_payean else 0 end)
    when a.meins1=a.meins2 and goods.weight_flag='0' then (case when a.store_recive_qty_payean/order_qty_payean>1 then store_recive_qty_payean-order_qty_payean else 0 end)
    when meins1!=meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_orderean=0 then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1.15 then (a.store_recive_qty_payean/a.order_qty_payean-1.15) * a.order_qty_payean else 0 end)
    else(case when a.store_recive_qty_orderean/a.order_qty_orderean>1.15 then (a.store_recive_qty_orderean/a.order_qty_orderean-1.15) * a.order_qty_orderean else 0 end) end)
    else(case when a.store_recive_qty_payean/a.order_qty_payean>1 then a.store_recive_qty_payean-a.order_qty_payean else 0 end) end) as over_qty,
    sum(case when a.meins1=a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1.15 then (a.store_recive_qty_payean/a.order_qty_payean-1.15) * order_amt
    else 0 end)
    when a.meins1=a.meins2 and goods.weight_flag='0' then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1 then a.store_recive_amt- a.order_amt else 0 end)
    when a.meins1!=a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_orderean=0 then (
    case when a.store_recive_qty_payean/a.order_qty_payean>1.15 then (a.store_recive_qty_payean/a.order_qty_payean-1.15) * a.order_amt else 0 end)
    else(case when a.store_recive_qty_orderean/a.order_qty_orderean>1.15 then (a.store_recive_qty_orderean/a.order_qty_orderean-1.15) * a.order_amt else 0 end) end)
    else(case when a.store_recive_qty_payean/a.order_qty_payean>1 then a.store_recive_amt-a.order_amt else 0 end) end) as over_amt,
    sum(case when a.select_qty is null or a.select_qty = '' then
    case when a.meins1 = a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_payean/a.order_qty_payean<0.85 then 1 else 0 end)
    when a.meins1 = a.meins2 and goods.weight_flag='0' then (
    case when a.store_recive_qty_payean/a.order_qty_payean<1 then 1 else 0 end)
    when a.meins1 != a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_orderean=0 then (
    case when a.store_recive_qty_payean/a.order_qty_payean<0.85 then 1 else 0 end)
    else(case when a.store_recive_qty_orderean/a.order_qty_orderean<0.85 then 1 else 0 end) end)
    else(case when a.store_recive_qty_payean/a.order_qty_payean<1 then 1 else 0 end) end
    else (case when  a.weightflag = '1' and a.satisfy_rate < 0.85 then 1
    when  a.weightflag = '0' and a.satisfy_rate < 1 then 1 else 0 end) end) as is_lower_num,
    sum(case when a.meins1 = a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_payean/a.order_qty_payean<0.85 then (1-a.store_recive_qty_payean/a.order_qty_payean) * order_qty_payean
    else 0 end)
    when a.meins1 = a.meins2 and goods.weight_flag='0' then (
    case when a.store_recive_qty_payean/a.order_qty_payean<1 then a.order_qty_payean-a.store_recive_qty_payean else 0 end)
    when a.meins1 != a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_orderean=0 then (
    case when a.store_recive_qty_payean/a.order_qty_payean<0.85 then (0.85-a.store_recive_qty_payean/a.order_qty_payean) * a.order_qty_payean else 0 end)
    else(case when a.store_recive_qty_orderean/a.order_qty_orderean<0.85 then (0.85-a.store_recive_qty_orderean/a.order_qty_orderean) * a.order_qty_payean
    else 0 end) end)
    else(case when a.store_recive_qty_payean/a.order_qty_payean<1 then a.order_qty_payean-a.store_recive_qty_payean else 0 end) end) as lower_qty,
    sum(case when a.meins1 = a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_payean/a.order_qty_payean<0.85 then (1-a.store_recive_qty_payean/a.order_qty_payean) * a.order_amt else 0 end)
    when a.meins1=a.meins2 and goods.weight_flag='0' then (
    case when a.store_recive_qty_payean/a.order_qty_payean<1 then a.order_amt-a.store_recive_amt else 0 end)
    when a.meins1 != a.meins2 and goods.weight_flag='1' then (
    case when a.store_recive_qty_orderean=0 then (
    case when a.store_recive_qty_payean/a.order_qty_payean<0.85 then (0.85-a.store_recive_qty_payean/a.order_qty_payean) * a.order_amt else 0 end)
    else(case when a.store_recive_qty_orderean/a.order_qty_orderean<0.85 then (0.85-a.store_recive_qty_payean/a.order_qty_payean) * a.order_amt else 0 end) end)
    else(case when a.store_recive_qty_payean/a.order_qty_payean<1 then a.order_amt-store_recive_amt else 0 end) end) as lower_amt,
    sum(case when e.order_unit_id='KG' THEN coalesce(b.menge2,a.order_qty_payean) else (coalesce(b.menge2,a.order_qty_payean)*coalesce(if(e.unit_weight<=0,1,e.unit_weight),1)) end ) as order_qty_payean_tokg,
    sum(case when a.select_qty is null or a.select_qty = '' then
case when meins1=meins2 and goods.weight_flag='1' then (
case when store_recive_qty_payean/order_qty_payean >=0.85 and store_recive_qty_payean/order_qty_payean <=1.15 then 1
else 0 end)
when meins1=meins2 and goods.weight_flag='0' then (
case when store_recive_qty_payean=order_qty_payean then 1
else 0 end)
when meins1!=meins2 and goods.weight_flag='1' then (
case when store_recive_qty_orderean=0 then (
case when store_recive_qty_payean/order_qty_payean >=0.85 and store_recive_qty_payean/order_qty_payean <=1.15 then 1
else 0 end)
else( case when store_recive_qty_orderean/order_qty_orderean >=0.85 and store_recive_qty_orderean/order_qty_orderean <=1.15 then 1
else 0 end)
end)
else(case when store_recive_qty_payean=order_qty_payean then 1
else 0 end) end
else (case when a.weightflag = '1' and a.satisfy_rate >= 0.85 and a.satisfy_rate <= 1.15  then 1 
when  a.weightflag = '0' and a.satisfy_rate = 1 then 1 else 0 end) end) as is_satisfied_num,
    a.inc_day as inc_day
from
(
    select * from tmp_dal.dal_dc_matnr_store_mode_purchase_wide634_sr
) a
left join
(
    select
        ean11,
        ean12,
        matnr,
        dc_id,
        sp_store_id,
        zpslx,
        ek_zzxsfs,
        sum(menge1) as menge1,
        sum(menge2) as menge2,
        sum(menge1_recive) as menge1_recive,
        sum(menge2_recive) as menge2_recive,
        sum(case when zsfzp='X' then menge2 else 0 end ) as order_qty_gift_payean,
        sum(case when zsfzp='X' then 0 else menge2 end ) as order_qty_value_payean,
        sum(case when zsfzp='X' then menge1 else 0 end ) as order_qty_gift_orderean,
        sum(case when zsfzp='X' then 0 else menge1 end ) as order_qty_value_orderean,
        sum(case when zsfzp='X' then menge2_recive else 0 end ) as out_stock_qty_gift, --double COMMENT '赠品出库量'
        sum(case when zsfzp='X' then 0 else menge2_recive end ) as out_stock_qty_value, --double COMMENT '赠品出库量'
        sum(case when zsfzp='X' then 0 else coalesce(zzddj,zzckj,0)*menge2 end ) as menge2_amt,
        sum(case when zsfzp='X' then 0 else coalesce(zzddj,zzckj,0)*menge2_recive end ) as menge2_recive_amt,
        inc_day
    from 
        dsl.dsl_scm_store_purchase_info_di
    where is_bom_false='1'  and inc_day between '$start_date' and '$end_date'
    group by 
        ean11,
        ean12,
        matnr,
        dc_id,
        sp_store_id,
        meins1,
        meins2,
        inc_day,
        zpslx,
        ek_zzxsfs
) b
on 
a.ean11=b.ean11
and 
a.ean12=b.ean12
and 
a.sp_store_id=b.sp_store_id
and 
a.inc_day=b.inc_day
and 
a.dc_id=b.dc_id
and 
a.zpslx = b.zpslx
and
a.ek_zzxsfs = b.ek_zzxsfs
left join
(select * from dim.dim_dc_profile where inc_day = '$yestoday' ) c
on a.dc_id = c.dc_id
left join
(select * from dim.dim_store_profile where inc_day = '$yestoday' ) d
on a.sp_store_id = d.sp_store_id
left join 
(
    select
        article_id,
        article_name,
        category_level1_id,
        category_level1_description,
        category_level2_id,
        category_level2_description,
        category_level3_id,
        category_level3_description,
        weight_flag,
        unit_weight
    from dim.dim_goods_information_have_pt
    where inc_day='$yestoday'    
) goods
on a.ean12=goods.article_id
left join 
(select
*
from dim.dim_goods_information_have_pt
where inc_day='$yestoday'   
) e
on a.ean11=e.article_id
group by c.new_dc_id,d.new_sp_store_id,case when a.zpslx in ('B','C','D') then '1' else '0' end,a.matnr,a.inc_day,a.ek_zzxsfs
) a
left join dim.dim_matnr_info_sap_da b 
on b.inc_day = '$yestoday'
and a.matnr = b.matnr
left join 
(select * from dim.dim_goods_information_have_pt  where inc_day = '$yestoday' and article_id <>'') d2
on b.article_id=d2.article_id
group by a.out_stock_date,a.new_dc_id,a.is_zs,a.matnr,a.inc_day,a.ek_zzxsfs,a.new_store_id;










drop table if exists tmp_dal.dal_dc_matnr_store_bear_wide26_finally_sr force;   
create table tmp_dal.dal_dc_matnr_store_bear_wide26_finally_sr
as
select 
t.out_stock_date                         as out_stock_date      -- 出库日期
,t.new_dc_id                           as new_dc_id           -- 仓库编码
,t.new_store_id                        as new_store_id        -- 门店编码
,t.is_zs                                                      -- 是否直送
,t.matnr                              as matnr               -- 物料编码
,t.sales_mode                         as sales_mode          -- 销售方式
,sum(t.scm_promotion_amt_total)       as scm_promotion_amt_total    -- 出库让利总额
,sum(t.scm_bear_nogift_amt)                  as scm_bear_amt              -- 供应链承担金额
,sum(t.business_bear_nogift_amt)             as business_bear_amt         -- 运营承担金额
,sum(t.market_bear_nogift_amt	)               as market_bear_amt           -- 市场承担金额
,sum(t.vendor_bear_nogift_amt)               as vendor_bear_amt           -- 供应商承担金额
,sum(t.scm_promotion_gift_amt)        as scm_promotion_amt_gift    -- 赠品让利金额
,sum(t.vender_bear_gift_amt)          as vender_bear_gift_amt      -- 供应商承担赠品金额
,sum(t.scm_bear_gift_amt)             as scm_bear_gift_amt         -- 供应链承担赠品金额
,sum(t.vender_bear_gift_qty)          as vender_bear_gift_qty      -- 供应商承担赠品数量
,sum(t.scm_bear_gift_qty)             as scm_bear_gift_qty         -- 供应链承担赠品数量
,sum(t.qdm_bear_negative_amt_total)    as qdm_bear_negative_amt_total              -- 公司承担负让利总额
,sum(t.qdm_bear_positive_amt_total)    as qdm_bear_positive_amt_total              -- 公司承担让利总额
,sum(t.qdm_bear_gift_qty)             as qdm_bear_gift_qty                  -- 公司承担赠品数量
,sum(t.qdm_bear_gift_amt)             as qdm_bear_gift_amt                  -- 公司承担赠品金额
,sum(t.qdm_bear_nogift_negative_amt)   as qdm_bear_nogift_negative_amt           -- 公司承担非赠品负让利金额
,sum(t.qdm_bear_nogift_positive_amt)   as qdm_bear_nogift_positive_amt           -- 公司承担非赠品让利金额
,sum(t.qdm_bear_promotion_fee) as qdm_bear_promotion_fee
,t.inc_day
from dal.dal_scm_dc_store_matnr_promotion_detail_di t 
where  t.inc_day between '$start_date' and '$end_date'
group by 
t.out_stock_date
,t.new_dc_id
,t.new_store_id
,t.is_zs
,t.matnr
,t.sales_mode
,t.inc_day
;






drop table if exists tmp_dal.dal_scm_dc_matnr_store_mode_daily_wide_sap_total50_finally_sr force;    
create table tmp_dal.dal_scm_dc_matnr_store_mode_daily_wide_sap_total50_finally_sr
as
select 
out_stock_date
,new_dc_id
,new_store_id
,is_zs
,matnr
,sales_mode
,sum(out_stock_qty)                    as out_stock_qty
,sum(out_stock_qty_give)               as out_stock_qty_give
,sum(out_stock_original_amt)           as out_stock_original_amt
,sum(out_stock_pay_amt)                as out_stock_pay_amt
,sum(out_stock_pay_amt_notax)          as out_stock_pay_amt_notax
,sum(out_stock_amt_cb_notax)           as out_stock_amt_cb_notax
,sum(out_stock_give_amt_cb_notax)      as out_stock_give_amt_cb_notax
,sum(return_stock_qty)                 as return_stock_qty
,sum(return_stock_original_amt)        as return_stock_original_amt
,sum(return_stock_pay_amt)             as return_stock_pay_amt
,sum(return_stock_pay_amt_notax)       as return_stock_pay_amt_notax
,sum(return_stock_amt_cb_notax)        as return_stock_amt_cb_notax
,sum(out_stock_zzckj_amt)             as out_stock_zzckj_amt
,sum(out_stock_amt_zzckj_nottax)      as out_stock_amt_zzckj_nottax
,sum(out_stock_zzddj_amt)             as out_stock_zzddj_amt
,sum(out_stock_amt_zzddj_nottax)      as out_stock_amt_zzddj_nottax
,sum(order_qty_orderean)              as order_qty_orderean
,sum(order_qty_payean)                as order_qty_payean
,sum(order_qty_gift_orderean)         as order_qty_gift_orderean
,sum(order_qty_gift_payean)           as order_qty_gift_payean
,sum(order_amt)                       as order_amt
,sum(scm_promotion_amt_total)         as scm_promotion_amt_total
,sum(scm_bear_amt)                    as scm_bear_amt
,sum(business_bear_amt)               as business_bear_amt
,sum(market_bear_amt)                 as market_bear_amt
,sum(vendor_bear_amt)                 as vendor_bear_amt
,sum(scm_promotion_amt_gift)          as scm_promotion_amt_gift
,sum(original_outstock_qty)           as original_outstock_qty
,sum(original_outstock_amt)           as original_outstock_amt
,sum(promotion_outstock_price)        as promotion_outstock_price
,sum(promotion_outstock_qty)          as promotion_outstock_qty
,sum(promotion_outstock_amt)          as promotion_outstock_amt
,sum(out_stock_amt_cb)               as out_stock_amt_cb
,sum(return_stock_amt_cb)            as return_stock_amt_cb
,sum(short_qty)                      as short_qty
,sum(short_amt)                      as short_amt
,sum(store_order_copies)             as store_order_copies
,sum(satisfied_num)                  as satisfied_num
,sum(zero_out_stock_order_qty)       as zero_out_stock_order_qty
,sum(zero_out_stock_order_amt)       as zero_out_stock_order_amt
,sum(satisfied_qty)                  as satisfied_qty
,sum(satisfied_amt)                  as satisfied_amt
,sum(satisfied_over_qty)             as satisfied_over_qty
,sum(satisfied_over_amt)             as satisfied_over_amt
,sum(satisfied_lower_qty)            as satisfied_lower_qty
,sum(satisfied_lower_amt)            as satisfied_lower_amt
,sum(store_return_qty_shop)          as store_return_qty_shop
,sum(store_return_amt_shop)          as store_return_amt_shop
,sum(store_return_num_shop)          as store_return_num_shop
,sum(delay_store_return_num_shop)    as delay_store_return_num_shop
,sum(nothing_store_return_qty_shop)  as nothing_store_return_qty_shop
,sum(nothing_store_return_amt_shop)  as nothing_store_return_amt_shop
,sum(quality_store_return_qty_shop)  as quality_store_return_qty_shop
,sum(quality_store_return_amt_shop)  as quality_store_return_amt_shop
,sum(quantity_store_return_qty_shop) as quantity_store_return_qty_shop
,sum(quantity_store_return_amt_shop) as quantity_store_return_amt_shop
,sum(customer_store_return_qty_shop) as customer_store_return_qty_shop
,sum(customer_store_return_amt_shop) as customer_store_return_amt_shop
,sum(other_store_return_qty_shop)    as other_store_return_qty_shop
,sum(other_store_return_amt_shop)    as other_store_return_amt_shop
,sum(vender_bear_gift_amt)           as vender_bear_gift_amt
,sum(scm_bear_gift_amt)              as scm_bear_gift_amt
,sum(vender_bear_gift_qty)           as vender_bear_gift_qty
,sum(scm_bear_gift_qty)              as scm_bear_gift_qty
,sum(qdm_bear_negative_amt_total)    as qdm_bear_negative_amt_total
,sum(qdm_bear_positive_amt_total)    as qdm_bear_positive_amt_total
,sum(qdm_bear_gift_qty)              as qdm_bear_gift_qty
,sum(qdm_bear_gift_amt)              as qdm_bear_gift_amt
,sum(qdm_bear_nogift_negative_amt)   as qdm_bear_nogift_negative_amt
,sum(qdm_bear_nogift_positive_amt)   as qdm_bear_nogift_positive_amt
,sum(qdm_bear_promotion_fee)         as qdm_bear_promotion_fee
,sum(quatity_store_return_num) as quatity_store_return_num
,sum(quantity_store_return_num) as quantity_store_return_num
,sum(customer_store_return_num) as customer_store_return_num
,sum(other_store_return_num) as other_store_return_num
,sum(nonquantity_store_return_num) as nonquantity_store_return_num
,inc_day
from
(
    select 
        out_stock_date,
        new_dc_id,
        new_store_id,
        is_zs,
        matnr,
        sales_mode,
        out_stock_qty,
        out_stock_qty_give,
        out_stock_original_amt,
        out_stock_pay_amt,
        out_stock_pay_amt_notax,
        out_stock_amt_cb_notax,
        out_stock_give_amt_cb_notax,
        return_stock_qty,
        return_stock_original_amt,
        return_stock_pay_amt,
        return_stock_pay_amt_notax,
        return_stock_amt_cb_notax,
        out_stock_zzckj_amt,
        out_stock_amt_zzckj_nottax,
        out_stock_zzddj_amt,
        out_stock_amt_zzddj_nottax,
        0 as order_qty_orderean,
        0 as order_qty_payean,
        0 as order_qty_gift_orderean,
        0 as order_qty_gift_payean,
        0 as order_amt,
        0 as scm_promotion_amt_total,
        0 as scm_bear_amt,
        0 as business_bear_amt,
        0 as market_bear_amt,
        0 as vendor_bear_amt,
        0 as scm_promotion_amt_gift,
        original_outstock_qty as original_outstock_qty,
        original_outstock_amt as original_outstock_amt,
        promotion_outstock_price as promotion_outstock_price,
        promotion_outstock_qty as promotion_outstock_qty,
        promotion_outstock_amt as promotion_outstock_amt,
        out_stock_amt_cb as out_stock_amt_cb,
        return_stock_amt_cb as return_stock_amt_cb,
        0 as short_qty,
        0 as short_amt,
        0 as store_order_copies,
        0 as satisfied_num,
        0 as zero_out_stock_order_qty,
        0 as zero_out_stock_order_amt,
        0 as satisfied_qty,
        0 as satisfied_amt,
        0 as satisfied_over_qty,
        0 as satisfied_over_amt,
        0 as satisfied_lower_qty,
        0 as satisfied_lower_amt,
        0 store_return_qty_shop,
0 store_return_amt_shop,
0 store_return_num_shop,
0 delay_store_return_num_shop,
0 nothing_store_return_qty_shop,
0 nothing_store_return_amt_shop,
0 quality_store_return_qty_shop,
0 quality_store_return_amt_shop,
0 quantity_store_return_qty_shop,
0 quantity_store_return_amt_shop,
0 customer_store_return_qty_shop,
0 customer_store_return_amt_shop,
0 other_store_return_qty_shop,
0 other_store_return_amt_shop,
0 vender_bear_gift_amt,
0 scm_bear_gift_amt,
0 vender_bear_gift_qty,
0 scm_bear_gift_qty,
0 qdm_bear_negative_amt_total,
0 qdm_bear_positive_amt_total,
0 qdm_bear_gift_qty,
0 qdm_bear_gift_amt,
0 qdm_bear_nogift_negative_amt,
0 qdm_bear_nogift_positive_amt,
0 qdm_bear_promotion_fee,
0 as quatity_store_return_num, 
0 as quantity_store_return_num,
0 as customer_store_return_num,
0 as other_store_return_num,
0 as nonquantity_store_return_num,
        inc_day
    from 
        tmp_dal.dal_dc_matnr_store_sum_wide23_finally_sr

    union all

    select
        out_stock_date,
        new_dc_id,
        new_store_id,
        is_zs,
        matnr,
        sales_mode, 
        0 as out_stock_qty,
        0 as out_stock_qty_give,
        0 as out_stock_original_amt,
        0 as out_stock_pay_amt,
        0 as out_stock_pay_amt_notax,
        0 as out_stock_amt_cb_notax,
        0 as out_stock_give_amt_cb_notax,
        0 as return_stock_qty,
        0 as return_stock_original_amt,
        0 as return_stock_pay_amt,
        0 as return_stock_pay_amt_notax,
        0 as return_stock_amt_cb_notax,
        0 as out_stock_zzckj_amt,
        0 as out_stock_amt_zzckj_nottax,
        0 as out_stock_zzddj_amt,
        0 as out_stock_amt_zzddj_nottax,
        order_qty_orderean,
        order_qty_payean,
        order_qty_gift_orderean,
        order_qty_gift_payean,
        order_amt,
        0 as scm_promotion_amt_total,
        0 as scm_bear_amt,
        0 as business_bear_amt,
        0 as market_bear_amt,
        0 as vendor_bear_amt,
        0 as scm_promotion_amt_gift,
        0 as original_outstock_qty,
        0 as original_outstock_amt,
        0 as promotion_outstock_price,
        0 as promotion_outstock_qty,
        0 as promotion_outstock_amt,
        0 as out_stock_amt_cb,
        0 as return_stock_amt_cb,
        miss_stock_qty as short_qty,
        miss_stock_amt as short_amt,
        order_qty_payean_kgfen as store_order_copies,
        is_satisfied_num as satisfied_num,
        zero_sp_order_qty as zero_out_stock_order_qty,
        zero_sp_order_amt as zero_out_stock_order_amt,
        satisfied_qty as satisfied_qty,
        satisfied_amt as satisfied_amt,
        over_qty as satisfied_over_qty,
        over_amt as satisfied_over_amt,
        lower_qty as satisfied_lower_qty,
        lower_amt as satisfied_lower_amt,
        0 store_return_qty_shop,
0 store_return_amt_shop,
0 store_return_num_shop,
0 delay_store_return_num_shop,
0 nothing_store_return_qty_shop,
0 nothing_store_return_amt_shop,
0 quality_store_return_qty_shop,
0 quality_store_return_amt_shop,
0 quantity_store_return_qty_shop,
0 quantity_store_return_amt_shop,
0 customer_store_return_qty_shop,
0 customer_store_return_amt_shop,
0 other_store_return_qty_shop,
0 other_store_return_amt_shop,
0 vender_bear_gift_amt,
0 scm_bear_gift_amt,
0 vender_bear_gift_qty,
0 scm_bear_gift_qty,
0 qdm_bear_negative_amt_total,
0 qdm_bear_positive_amt_total,
0 qdm_bear_gift_qty,
0 qdm_bear_gift_amt,
0 qdm_bear_nogift_negative_amt,
0 qdm_bear_nogift_positive_amt,
0 qdm_bear_promotion_fee,
0 as quatity_store_return_num, 
0 as quantity_store_return_num,
0 as customer_store_return_num,
0 as other_store_return_num,
0 as nonquantity_store_return_num,
        inc_day
    from
        tmp_dal.dal_dc_matnr_store_order_wide25_finally_sr

    union all

    select
        out_stock_date,
        new_dc_id,
        new_store_id,
        is_zs,
        matnr,
        sales_mode, 
        0 as out_stock_qty,
        0 as out_stock_qty_give,
        0 as out_stock_original_amt,
        0 as out_stock_pay_amt,
        0 as out_stock_pay_amt_notax,
        0 as out_stock_amt_cb_notax,
        0 as out_stock_give_amt_cb_notax,
        0 as return_stock_qty,
        0 as return_stock_original_amt,
        0 as return_stock_pay_amt,
        0 as return_stock_pay_amt_notax,
        0 as return_stock_amt_cb_notax,
        0 as out_stock_zzckj_amt,
        0 as out_stock_amt_zzckj_nottax,
        0 as out_stock_zzddj_amt,
        0 as out_stock_amt_zzddj_nottax,
        0 as order_qty_orderean,
        0 as order_qty_payean,
        0 as order_qty_gift_orderean,
        0 as order_qty_gift_payean,
        0 as order_amt,
        scm_promotion_amt_total,
        scm_bear_amt,
        business_bear_amt,
        market_bear_amt,
        vendor_bear_amt,
        scm_promotion_amt_gift,
        0 as original_outstock_qty,
        0 as original_outstock_amt,
        0 as promotion_outstock_price,
        0 as promotion_outstock_qty,
        0 as promotion_outstock_amt,
        0 as out_stock_amt_cb,
        0 as return_stock_amt_cb,
        0 as short_qty,
        0 as short_amt,
        0 as store_order_copies,
        0 as satisfied_num,
        0 as zero_out_stock_order_qty,
        0 as zero_out_stock_order_amt,
        0 as satisfied_qty,
        0 as satisfied_amt,
        0 as satisfied_over_qty,
        0 as satisfied_over_amt,
        0 as satisfied_lower_qty,
        0 as satisfied_lower_amt,
        0 store_return_qty_shop,
0 store_return_amt_shop,
0 store_return_num_shop,
0 delay_store_return_num_shop,
0 nothing_store_return_qty_shop,
0 nothing_store_return_amt_shop,
0 quality_store_return_qty_shop,
0 quality_store_return_amt_shop,
0 quantity_store_return_qty_shop,
0 quantity_store_return_amt_shop,
0 customer_store_return_qty_shop,
0 customer_store_return_amt_shop,
0 other_store_return_qty_shop,
0 other_store_return_amt_shop,
 vender_bear_gift_amt,
scm_bear_gift_amt,
vender_bear_gift_qty,
scm_bear_gift_qty,
qdm_bear_negative_amt_total,
qdm_bear_positive_amt_total,
qdm_bear_gift_qty,
qdm_bear_gift_amt,
qdm_bear_nogift_negative_amt,
qdm_bear_nogift_positive_amt,
qdm_bear_promotion_fee,
0 as quatity_store_return_num, 
0 as quantity_store_return_num,
0 as customer_store_return_num,
0 as other_store_return_num,
0 as nonquantity_store_return_num,
        inc_day
    from 
        tmp_dal.dal_dc_matnr_store_bear_wide26_finally_sr

      union all 
select
      a.business_date  out_stock_date,
      coalesce(d1.dc_id,d2.dc_id,a.dc_id)  new_dc_id,
       d.new_sp_store_id  store_id ,
       coalesce(a.delivery_type,'1')   is_zs,
        a.matnr,
        a.sales_mode, 
        0 as out_stock_qty,
        0 as out_stock_qty_give,
        0 as out_stock_original_amt,
        0 as out_stock_pay_amt,
        0 as out_stock_pay_amt_notax,
        0 as out_stock_amt_cb_notax,
        0 as out_stock_give_amt_cb_notax,
        0 as return_stock_qty,
        0 as return_stock_original_amt,
        0 as return_stock_pay_amt,
        0 as return_stock_pay_amt_notax,
        0 as return_stock_amt_cb_notax,
        0 as out_stock_zzckj_amt,
        0 as out_stock_amt_zzckj_nottax,
        0 as out_stock_zzddj_amt,
        0 as out_stock_amt_zzddj_nottax,
        0 as order_qty_orderean,
        0 as order_qty_payean,
        0 as order_qty_gift_orderean,
        0 as order_qty_gift_payean,
        0 as order_amt,
        0 as scm_promotion_amt_total,
        0 as scm_bear_amt,
        0 as business_bear_amt,
        0 as market_bear_amt,
        0 as vendor_bear_amt,
        0 as scm_promotion_amt_gift,
        0 as original_outstock_qty,
        0 as original_outstock_amt,
        0 as promotion_outstock_price,
        0 as promotion_outstock_qty,
        0 as promotion_outstock_amt,
        0 as out_stock_amt_cb,
        0 as return_stock_amt_cb,
        0 as short_qty,
        0 as short_amt,
        0 as store_order_copies,
        0 as satisfied_num,
        0 as zero_out_stock_order_qty,
        0 as zero_out_stock_order_amt,
        0 as satisfied_qty,
        0 as satisfied_amt,
        0 as satisfied_over_qty,
        0 as satisfied_over_amt,
        0 as satisfied_lower_qty,
        0 as satisfied_lower_amt,
         a.store_return_qty_business store_return_qty_shop,
a.store_return_amt_business store_return_amt_shop,
0 store_return_num_shop,
if(abs(a.delay_store_return_qty_shop)>0,1,0) delay_store_return_num_shop,
a.store_return_qty_reason_code_ydwh_business nothing_store_return_qty_shop,
a.store_return_amt_reason_code_ydwh_business nothing_store_return_amt_shop,
a.store_return_qty_reason_type_quality_business quality_store_return_qty_shop,
a.store_return_amt_reason_type_quality_business quality_store_return_amt_shop,
a.store_return_qty_reason_type_quantity_business quantity_store_return_qty_shop,
a.store_return_amt_reason_type_quantity_business quantity_store_return_amt_shop,
a.store_return_qty_reason_type_customer_business customer_store_return_qty_shop,
a.store_return_amt_reason_type_customer_business customer_store_return_amt_shop,
a.store_return_qty_reason_type_other_business other_store_return_qty_shop,
a.store_return_amt_reason_type_other_business other_store_return_amt_shop ,
0 vender_bear_gift_amt,
0 scm_bear_gift_amt,
0 vender_bear_gift_qty,
0 scm_bear_gift_qty,
0 qdm_bear_negative_amt_total,
0 qdm_bear_positive_amt_total,
0 qdm_bear_gift_qty,
0 qdm_bear_gift_amt,
0 qdm_bear_nogift_negative_amt,
0 qdm_bear_nogift_positive_amt,
0 qdm_bear_promotion_fee,
0 as quatity_store_return_num, 
0 as quantity_store_return_num,
0 as customer_store_return_num,
0 as other_store_return_num,
0 as nonquantity_store_return_num,
        a.inc_day
    from 
      (select * from  dsl.dsl_dc_store_article_mode_refund_di where  inc_day between '$start_date' and '$end_date' ) a
     left join (select * from dim.dim_store_profile where inc_day between '$start_date' and '$end_date'  ) d on a.store_id=d.sp_store_id  and a.inc_day=d.inc_day
        left join (select * from dim.dim_dc_store_article_sales_mode_relation_da where  inc_day between '$start_date' and '$end_date') d1 
        on a.inc_day=d1.inc_day and a.store_id=d1.store_id and a.base_article_id=d1.article_id and a.sales_mode=d1.sales_mode_id and a.delivery_type='1'
         left join (select * from dim.dim_dc_store_article_relation_da where  inc_day between '$start_date' and '$end_date' )  d2
         on a.inc_day=d2.inc_day and a.store_id=d2.store_id and a.base_article_id=d2.article_id  and a.delivery_type='1'

      union all 
select
      a.receive_at  out_stock_date,
      coalesce(d1.dc_id,d2.dc_id,a.new_dc_id)  new_dc_id,
       a.new_sp_store_id  store_id ,
       a.delivery_type   is_zs,
        g.matnr,
        a.sales_mode, 
        0 as out_stock_qty,
        0 as out_stock_qty_give,
        0 as out_stock_original_amt,
        0 as out_stock_pay_amt,
        0 as out_stock_pay_amt_notax,
        0 as out_stock_amt_cb_notax,
        0 as out_stock_give_amt_cb_notax,
        0 as return_stock_qty,
        0 as return_stock_original_amt,
        0 as return_stock_pay_amt,
        0 as return_stock_pay_amt_notax,
        0 as return_stock_amt_cb_notax,
        0 as out_stock_zzckj_amt,
        0 as out_stock_amt_zzckj_nottax,
        0 as out_stock_zzddj_amt,
        0 as out_stock_amt_zzddj_nottax,
        0 as order_qty_orderean,
        0 as order_qty_payean,
        0 as order_qty_gift_orderean,
        0 as order_qty_gift_payean,
        0 as order_amt,
        0 as scm_promotion_amt_total,
        0 as scm_bear_amt,
        0 as business_bear_amt,
        0 as market_bear_amt,
        0 as vendor_bear_amt,
        0 as scm_promotion_amt_gift,
        0 as original_outstock_qty,
        0 as original_outstock_amt,
        0 as promotion_outstock_price,
        0 as promotion_outstock_qty,
        0 as promotion_outstock_amt,
        0 as out_stock_amt_cb,
        0 as return_stock_amt_cb,
        0 as short_qty,
        0 as short_amt,
        0 as store_order_copies,
        0 as satisfied_num,
        0 as zero_out_stock_order_qty,
        0 as zero_out_stock_order_amt,
        0 as satisfied_qty,
        0 as satisfied_amt,
        0 as satisfied_over_qty,
        0 as satisfied_over_amt,
        0 as satisfied_lower_qty,
        0 as satisfied_lower_amt,
        0 store_return_qty_shop,
0 store_return_amt_shop,
 return_num store_return_num_shop,
0 delay_store_return_num_shop,
0 nothing_store_return_qty_shop,
0 nothing_store_return_amt_shop,
0 quality_store_return_qty_shop,
0 quality_store_return_amt_shop,
0 quantity_store_return_qty_shop,
0 quantity_store_return_amt_shop,
0 customer_store_return_qty_shop,
0 customer_store_return_amt_shop,
0 other_store_return_qty_shop,
0 other_store_return_amt_shop,
0 vender_bear_gift_amt,
0 scm_bear_gift_amt,
0 vender_bear_gift_qty,
0 scm_bear_gift_qty,
0 qdm_bear_negative_amt_total,
0 qdm_bear_positive_amt_total,
0 qdm_bear_gift_qty,
0 qdm_bear_gift_amt,
0 qdm_bear_nogift_negative_amt,
0 qdm_bear_nogift_positive_amt,
0 qdm_bear_promotion_fee,
return_quality_num	quatity_store_return_num, 
return_quantity_num	quantity_store_return_num,
return_customer_num	customer_store_return_num,
return_other_num	other_store_return_num,
return_num_notinqty	nonquantity_store_return_num,
        a.inc_day
    from 
    dal.dal_scm_dc_store_sales_mode_article_receivedate_return_info_di a 
    left join (select article_id,matnr from dim.dim_goods_information_have_pt where inc_day = '$yestoday') g on a.article_id=g.article_id
    left join (select * from dim.dim_store_profile where inc_day between '$start_date' and '$end_date'  ) d on a.sp_store_id=d.sp_store_id  and a.inc_day=d.inc_day
        left join (select * from dim.dim_dc_store_article_sales_mode_relation_da where  inc_day between '$start_date' and '$end_date') d1 
        on a.inc_day=d1.inc_day and a.sp_store_id=d1.store_id and a.article_id=d1.article_id and a.sales_mode=d1.sales_mode_id and a.delivery_type='1'
         left join (select * from dim.dim_dc_store_article_relation_da where  inc_day between '$start_date' and '$end_date' )  d2
         on a.inc_day=d2.inc_day and a.sp_store_id=d2.store_id and a.article_id=d2.article_id  and a.delivery_type='1'

    
    where a.inc_day between '$start_date' and '$end_date' 

) a 
group by 
    out_stock_date,
    new_dc_id,
    new_store_id,
    is_zs,
    matnr,
    sales_mode,
    inc_day;








drop table if exists tmp_dal.dal_supply_chain_article_dc_store_sale_dcmove_outstock_forward_direction332_sr force;
create table tmp_dal.dal_supply_chain_article_dc_store_sale_dcmove_outstock_forward_direction332_sr
as
select
    a.out_stock_date as out_stock_date,
    a.new_dc_id as new_dc_id,
    a.article_id as article_id,
    '0' as is_zs,
    a.store_id as store_id,
    a.sale_type as sale_type,
	b.avg_in_price_notax,
	b.avg_in_price,
   (a.out_stock_qty * b.avg_in_price_notax) as out_stock_amt_cb_notax_af,
    a.out_stock_amt_cb_notax as out_stock_amt_cb_notax_bf,
   (a.out_stock_qty * b.avg_in_price) as out_stock_amt_cb_af,
	a.out_stock_amt_cb as out_stock_amt_cb_bf,
    a.out_stock_give_amt_cb_notax out_stock_give_amt_cb_notax_bf,
    (a.out_stock_qty_give* b.avg_in_price_notax) out_stock_give_amt_cb_notax_af
from
(
    select
        date(date_add(inc_day,1)) as out_stock_date,
        mdoc_werks as new_dc_id,
        base_unit_ean as article_id,
        mdoc_wempf as store_id,
        ek_zzxsfs as sale_type,
        sum(out_stock_qty) as out_stock_qty,
        sum(out_stock_qty_give) out_stock_qty_give,
        sum(out_stock_give_amt_cb_notax) out_stock_give_amt_cb_notax,
        sum(out_stock_amt_cb_notax) as out_stock_amt_cb_notax,
		sum(out_stock_amt_cb) as out_stock_amt_cb
    from dsl.dsl_scm_info_sap_global_di
    where  inc_day between date(adddate('$start_date',-1)) and date(adddate('$end_date',-1))
    and coalesce(ek_zpslx,'aaa') not in ('B','C','E') 
    and ek_bsart in ('Z014','Z015','Z016','Z017','Z025','Z026')  and mdoc_bwart in ('641','643','642','644') and mdoc_werks <> mdoc_wempf and out_stock_qty > 0
    and coalesce(mdoc_werks,'')<>'D093'
    group by inc_day,mdoc_werks,base_unit_ean,mdoc_wempf,ek_zzxsfs
) a
inner join
(
select
date(date_add(inc_day,1)) as out_stock_date,
mdoc_werks new_dc_id,
base_unit_ean article_id,
-- max(if(ek_pstyp='7',sc_zsl,zsl)) sc_zsl,
sum(dcmove_in_pay_amt_notax+in_stock_pay_amt_notax)/sum(dcmove_in_qty+in_stock_qty) as  avg_in_price_notax,
sum(dcmove_in_pay_amt+in_stock_pay_amt)/sum(dcmove_in_qty+in_stock_qty) as  avg_in_price
from dsl.dsl_scm_info_sap_global_di a
where  inc_day between date(adddate('$start_date',-1)) and date(adddate('$end_date',-1))
-- and mdoc_stock_qty <> 0 
and coalesce(ek_zpslx,'aaa') not in ('B','C','E') 
-- and ek_bsart in ('Z022','Z023','Z031','Z032') and mdoc_umwrk <> mdoc_werks  and mdoc_bwart in ('101','122','102') 
--  and ek_pstyp='7' 
and (dcmove_in_qty>0 or in_stock_qty>0)
group by mdoc_werks,base_unit_ean,date(date_add(inc_day,1))
having sum(dcmove_in_qty) >0
) b
on a.out_stock_date = b.out_stock_date and a.new_dc_id = b.new_dc_id and a.article_id = b.article_id;










drop table if exists tmp_dal.dal_supply_chain_article_dc_store_sale_dcmove_outstock_reverse_direction332_sr force;
create table tmp_dal.dal_supply_chain_article_dc_store_sale_dcmove_outstock_reverse_direction332_sr
as
select
    a.inc_day as out_stock_date,
    a.new_dc_id as new_dc_id,
    a.article_id as article_id,
    '0' as is_zs,
    a.store_id as store_id,
    a.sale_type as sale_type,
    (a.return_stock_qty * b.dcmove_out_price)/(1+COALESCE(b.sc_zsl,0)) as  return_stock_amt_cb_notax_af,
	(a.return_stock_qty * b.dcmove_out_price) return_stock_amt_cb_af,
    a.return_stock_amt_cb_notax as return_stock_amt_cb_notax_bf,
	a.return_stock_amt_cb as return_stock_amt_cb_bf
from
(
    select
        inc_day as inc_day,
        mdoc_werks as new_dc_id,
        base_unit_ean as article_id,
        mdoc_wempf as store_id,
        ek_zzxsfs as sale_type,
        sum(return_stock_qty) as return_stock_qty,
        sum(return_stock_amt_cb_notax) as return_stock_amt_cb_notax,
		sum(return_stock_amt_cb) as return_stock_amt_cb
    from dsl.dsl_scm_info_sap_global_di
    where  
        inc_day between '$start_date' and '$end_date'
        and coalesce(ek_zpslx,'aaa') not in ('B','C','E')  
        and abs(return_stock_qty)>0 
        and coalesce(mdoc_werks,'')<>'D093'
    group by inc_day,mdoc_werks,base_unit_ean,mdoc_wempf,ek_zzxsfs
) a
inner join
(
    select
        inc_day,
        new_dc_id,
        article_id,
        max(sc_zsl) as sc_zsl,
        max(dcmove_out_price) as dcmove_out_price
    from
    (
        select
          t1.inc_day,
          t1.mdoc_wempf as new_dc_id,
          t1.mdoc_werks,
          t1.base_unit_ean as article_id,
          t1.sc_zsl,
          t1.dcmove_out_price as dcmove_out_price
        from
        (
            select
                inc_day,
                mdoc_werks,
                mdoc_wempf,
                base_unit_ean,
                max(if(ek_pstyp='7',sc_zsl,zsl)) sc_zsl,
                count(distinct round(dcmove_out_pay_amt / dcmove_out_qty, 2)) as num,
                group_concat(DISTINCT cast(round(dcmove_out_pay_amt / dcmove_out_qty, 2) as string) SEPARATOR ',') as dcmove_out_price
            from
                dsl.dsl_scm_info_sap_global_di a
            where
                inc_day between date(adddate('$start_date',-1)) and date(adddate('$end_date',-1))
                and mdoc_stock_qty <> 0
                and coalesce(ek_zpslx, 'aaa') not in ('B', 'C', 'E')
                and ek_bsart in ('Z022', 'Z023', 'Z031', 'Z032')
                and mdoc_umwrk <> mdoc_werks
                and mdoc_bwart in ('641', '643', '642', '644')
               -- and ek_pstyp = '7'
                and dcmove_out_qty > 0
            group by
                mdoc_werks,
                mdoc_wempf,
                base_unit_ean,
                inc_day 
            having num = 1
        ) t1
        JOIN
        (
            select
                mdoc_wempf,
                base_unit_ean,
                inc_day,
                COUNT(DISTINCT mdoc_werks) AS werks_count
            from dsl.dsl_scm_info_sap_global_di a
            where
                inc_day between date(adddate('$start_date',-1)) and date(adddate('$end_date',-1))
                and mdoc_stock_qty <> 0
                and coalesce(ek_zpslx, 'aaa') not in ('B', 'C', 'E')
                and ek_bsart in ('Z022', 'Z023', 'Z031', 'Z032')
                and mdoc_umwrk <> mdoc_werks
                and mdoc_bwart in ('641', '643', '642', '644')
              --  and ek_pstyp = '7'
                and dcmove_out_qty > 0
            group by 
                mdoc_wempf,
                base_unit_ean,
                inc_day
            having werks_count = 1
	    ) t2
	    ON t1.mdoc_wempf = t2.mdoc_wempf 
	    AND t1.base_unit_ean = t2.base_unit_ean 
	    AND t1.inc_day = t2.inc_day
    ) tt
    group by inc_day,new_dc_id,article_id
) b
on a.inc_day = date(date_add(b.inc_day,1)) and a.new_dc_id = b.new_dc_id and a.article_id = b.article_id;








-- drop table if exists tmp_dal.dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di_tmp force;
-- create table tmp_dal.dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di_tmp as
insert overwrite dal.dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di
select
    a.out_stock_date as out_stock_date,
    a.virtual_dc_id as virtual_dc_id,
    a.actual_dc_vendor_id as actual_dc_vendor_id,
    a.delivery_type as delivery_type,
    a.sale_type as sale_type,
    a.new_store_id as new_store_id,
    a.matnr as matnr,
    a.meins as meins,
    a.article_id as article_id,
    a.article_unit as article_unit,
    a.sales_unit_id as sales_unit_id,
    a.order_qty_order_unit,
    a.order_qty,
    a.order_amt,
    a.gift_order_qty_order_unit,
    a.gift_order_qty,
    a.outstock_qty,
    a.gift_outstock_qty,
    a.outstock_amt,
    a.outstock_amt_notax,
    case when  a.out_stock_date>='2024-07-01' then a.outstock_cost_notax 
     else (coalesce(a.outstock_cost_notax,0)+coalesce(b.out_stock_amt_cb_notax_af,0)-coalesce(b.out_stock_amt_cb_notax_bf,0)) end as outstock_cost_notax,
    a.store_return_scm_qty,
    a.store_return_scm_amt,
    a.store_return_scm_amt_notax,
    case when  a.out_stock_date>='2024-07-01' then a.store_return_scm_cost_notax 
     else (coalesce(a.store_return_scm_cost_notax,0)+coalesce(c.return_stock_amt_cb_notax_af,0)-coalesce(c.return_stock_amt_cb_notax_bf,0)) end as store_return_scm_cost_notax,
    a.total_benefit_amt,
    a.total_gift_benefit_amt,
    a.scm_bear_nogift_benefit_amt,
    a.business_bear_nogift_benefit_amt,
    a.market_bear_nogift_benefit_amt,
    a.vendor_bear_nogift_benefit_amt,
    a.original_outstock_qty as original_outstock_qty,
    a.original_outstock_amt as original_outstock_amt,
    a.promotion_outstock_price as promotion_outstock_price,
    a.promotion_outstock_qty as promotion_outstock_qty,
    a.promotion_outstock_amt as promotion_outstock_amt,
     case when  a.out_stock_date>='2024-07-01' then a.out_stock_amt_cb 
     else  (coalesce(a.out_stock_amt_cb,0)+coalesce(b.out_stock_amt_cb_af,0)-coalesce(b.out_stock_amt_cb_bf,0)) end as out_stock_amt_cb,
     case when  a.out_stock_date>='2024-07-01' then a.return_stock_amt_cb 
     else  (coalesce(a.return_stock_amt_cb,0)+coalesce(c.return_stock_amt_cb_af,0)-coalesce(c.return_stock_amt_cb_bf,0)) end as  return_stock_amt_cb,
    case when g.main_article_id is null then  a.short_qty else 0 end as short_qty,
    case when g.main_article_id is null then a.short_amt else 0 end as short_amt,
    a.store_order_copies as store_order_copies,
    case when g.main_article_id is null then a.satisfied_num else 0 end as satisfied_num,
    case when a.order_qty>0.03 and a.outstock_qty=0 and a.business_stop_tag  ='0'  and  g.main_article_id is null and a.orderean_outstock_qty=0 then  a.order_qty else 0 end as zero_out_stock_order_qty,
     case when a.order_qty>0.03 and a.outstock_qty=0 and a.business_stop_tag  ='0'  and  g.main_article_id is null and a.orderean_outstock_qty=0 then  a.order_amt else 0 end as zero_out_stock_order_amt,
    case when g.main_article_id is null then  a.satisfied_qty else 0 end as satisfied_qty,
    case when g.main_article_id is null then  a.satisfied_amt else 0 end as satisfied_amt,
    a.business_stop_tag as business_stop_tag,
    a.store_return_qty_shop	as store_return_qty_shop,
    a.store_return_amt_shop	as store_return_amt_shop,
    a.store_return_num_shop	as store_return_num_shop,
    a.delay_store_return_num_shop	as delay_store_return_num_shop,
    a.nothing_store_return_qty_shop	as nothing_store_return_qty_shop,
    a.nothing_store_return_amt_shop	as nothing_store_return_amt_shop,
    a.quality_store_return_qty_shop	as quality_store_return_qty_shop,
    a.quality_store_return_amt_shop	as quality_store_return_amt_shop,
    a.quantity_store_return_qty_shop	as quantity_store_return_qty_shop,
    a.quantity_store_return_amt_shop	as quantity_store_return_amt_shop,
    a.customer_store_return_qty_shop	as customer_store_return_qty_shop,
    a.customer_store_return_amt_shop	as customer_store_return_amt_shop,
    a.other_store_return_qty_shop	as other_store_return_qty_shop,
    a.other_store_return_amt_shop	as other_store_return_amt_shop,
    a.out_stock_copies,
    case when  a.out_stock_date>='2024-07-01' then a.out_stock_give_amt_cb_notax 
     else (coalesce(a.out_stock_give_amt_cb_notax,0)+coalesce(b.out_stock_give_amt_cb_notax_af,0)-coalesce(b.out_stock_give_amt_cb_notax_bf,0)) end as gift_out_stock_cost_notax,
    a.store_order_weight,
    a.out_stock_weight,
    case when g.main_article_id is null then  a.satisfied_over_qty else 0 end as satisfied_over_qty,
    case when g.main_article_id is null then  a.satisfied_over_amt else 0 end as satisfied_over_amt,
    case when g.main_article_id is null then  a.satisfied_lower_qty else 0 end as satisfied_lower_qty,
    case when g.main_article_id is null then  a.satisfied_lower_amt else 0 end as satisfied_lower_amt,
    a.store_id,
    a.vender_bear_gift_amt vender_bear_gift_amt,
    a.scm_bear_gift_amt scm_bear_gift_amt,
    a.vender_bear_gift_qty vender_bear_gift_qty,
    a.scm_bear_gift_qty  scm_bear_gift_qty,
    a.qdm_bear_negative_amt_total,
    a.qdm_bear_positive_amt_total,
    a.qdm_bear_gift_qty,
    a.qdm_bear_gift_amt,
    a.qdm_bear_nogift_negative_amt,
    a.qdm_bear_nogift_positive_amt,
    a.qdm_bear_promotion_fee,
    a.quatity_store_return_num, 
    a.quantity_store_return_num,
    a.customer_store_return_num,
    a.other_store_return_num,
    a.nonquantity_store_return_num,
    a.inc_day
from
(
    select
        a.out_stock_date as out_stock_date,
        a.virtual_dc_id as virtual_dc_id,
        case when a.delivery_type = '0'  then a.virtual_dc_id when  a.delivery_type = '1' then coalesce(v.lifnr,e.lifnr) end as actual_dc_vendor_id,
        a.delivery_type as delivery_type,
        a.sale_type as sale_type,
        a.new_store_id as new_store_id,
        a.matnr as matnr,
        b.meins as meins,
        b.article_id as article_id,
        b.article_unit as article_unit,
        c.category_level1_id as category_level1_id,
        c.sales_unit_id as sales_unit_id,
        a.order_qty_order_unit as order_qty_order_unit,
        a.order_qty as order_qty,
        a.order_amt as order_amt,
        a.gift_order_qty_order_unit as gift_order_qty_order_unit,
        a.gift_order_qty as gift_order_qty,
        a.outstock_qty as outstock_qty,
        a.gift_outstock_qty as gift_outstock_qty,
        a.outstock_amt as outstock_amt,
        a.outstock_amt_notax as outstock_amt_notax,
        a.outstock_cost_notax as outstock_cost_notax,
        a.store_return_scm_qty as store_return_scm_qty,
        a.store_return_scm_amt as store_return_scm_amt,
        a.store_return_scm_amt_notax as store_return_scm_amt_notax,
        a.store_return_scm_cost_notax as store_return_scm_cost_notax,
        a.total_benefit_amt as total_benefit_amt,
        a.total_gift_benefit_amt as total_gift_benefit_amt,
        a.scm_bear_nogift_benefit_amt as scm_bear_nogift_benefit_amt,
        a.business_bear_nogift_benefit_amt as business_bear_nogift_benefit_amt,
        a.market_bear_nogift_benefit_amt as market_bear_nogift_benefit_amt,
        a.vendor_bear_nogift_benefit_amt as vendor_bear_nogift_benefit_amt,
        a.original_outstock_qty as original_outstock_qty,
        a.original_outstock_amt as original_outstock_amt,
        a.promotion_outstock_price as promotion_outstock_price,
        a.promotion_outstock_qty as promotion_outstock_qty,
        a.promotion_outstock_amt as promotion_outstock_amt,
        a.out_stock_amt_cb as out_stock_amt_cb,
        a.return_stock_amt_cb as return_stock_amt_cb,
        a.short_qty as short_qty,
        a.short_amt as short_amt,
        a.store_order_copies as store_order_copies,
        a.satisfied_num as satisfied_num,
        a.zero_out_stock_order_qty as zero_out_stock_order_qty,
        a.zero_out_stock_order_amt as zero_out_stock_order_amt,
        a.satisfied_qty as satisfied_qty,
        a.satisfied_amt as satisfied_amt,
        a.store_return_qty_shop	as store_return_qty_shop,
        a.store_return_amt_shop	as store_return_amt_shop,
        a.store_return_num_shop	as store_return_num_shop,
        a.delay_store_return_num_shop	as delay_store_return_num_shop,
        a.nothing_store_return_qty_shop	as nothing_store_return_qty_shop,
        a.nothing_store_return_amt_shop	as nothing_store_return_amt_shop,
        a.quality_store_return_qty_shop	as quality_store_return_qty_shop,
        a.quality_store_return_amt_shop	as quality_store_return_amt_shop,
        a.quantity_store_return_qty_shop	as quantity_store_return_qty_shop,
        a.quantity_store_return_amt_shop	as quantity_store_return_amt_shop,
        a.customer_store_return_qty_shop	as customer_store_return_qty_shop,
        a.customer_store_return_amt_shop	as customer_store_return_amt_shop,
        a.other_store_return_qty_shop	as other_store_return_qty_shop,
        a.other_store_return_amt_shop	as other_store_return_amt_shop,
         (case when c.sales_unit_id = 'KG' then ceil((a.outstock_qty)/(if(if(c.min_pack_weight is null,1,c.min_pack_weight) != 0,if(c.min_pack_weight is null,1,c.min_pack_weight),1))) 
when c.sales_unit_id = 'ZGJ' then ceil((a.outstock_qty)*(if(if(c.unit_weight is null,1,c.unit_weight) != 0,if(c.unit_weight is null,1,c.unit_weight),1))/(if(if(c.min_pack_weight is null,1,c.min_pack_weight) != 0,if(c.min_pack_weight is null,1,c.min_pack_weight),1)))
else ceil(a.outstock_qty) end) out_stock_copies,
        case when c.sales_unit_id = 'KG' or coalesce(c.unit_weight,0)=0 then a.order_qty   else a.order_qty*c.unit_weight end store_order_weight,
        case when c.sales_unit_id = 'KG' or coalesce(c.unit_weight,0)=0 then a.outstock_qty   else a.outstock_qty*c.unit_weight end out_stock_weight,
        a.out_stock_give_amt_cb_notax,
        a.satisfied_over_qty,
        a.satisfied_over_amt,
        a.satisfied_lower_qty,
        a.satisfied_lower_amt,
        sum(a.outstock_qty) over(partition by a.out_stock_date,
	       a.virtual_dc_id,
		   a.new_store_id,
		   a.sale_type,
           a.delivery_type,
		   coalesce(g.main_article_id,b.article_id) ) orderean_outstock_qty,
           -- 按订购条码统计出库
         case when
     sum(case when c.category_level1_id not in ('73','74','75','76','77') and coalesce(a.sale_type,'00') in ('00','10','20','99')  then a.order_qty else 0 end) 
     over(partition by a.out_stock_date,a.virtual_dc_id,d.original_store_id) >=0 
     and 
        sum(case when c.category_level1_id not in ('73','74','75','76','77') and coalesce(a.sale_type,'00') in ('00','10','20','99')  then a.outstock_qty else 0 end) 
     over(partition by a.out_stock_date,a.virtual_dc_id,d.original_store_id) =0
     and dc.sap_dc_category_id<>'50'
     then '1' else '0' end business_stop_tag,   
      -- 按仓-门店-日，销售方式 in （门店，菜吧，全渠道）的大分类<70的商品总订购量>=0，总出库到店量=0，则门店停业，标识1
      d.original_store_id store_id,
          a.vender_bear_gift_amt vender_bear_gift_amt,
         a.scm_bear_gift_amt scm_bear_gift_amt,
          a.vender_bear_gift_qty vender_bear_gift_qty,
         a.scm_bear_gift_qty scm_bear_gift_qty,
             a.qdm_bear_negative_amt_total,
    a.qdm_bear_positive_amt_total,
    a.qdm_bear_gift_qty,
    a.qdm_bear_gift_amt,
    a.qdm_bear_nogift_negative_amt,
    a.qdm_bear_nogift_positive_amt,
    a.qdm_bear_promotion_fee,
    a.quatity_store_return_num, 
    a.quantity_store_return_num,
    a.customer_store_return_num,
    a.other_store_return_num,
    a.nonquantity_store_return_num,
        a.inc_day as inc_day
    from
    (
        select
            out_stock_date,
            new_dc_id as virtual_dc_id,
            is_zs as delivery_type,
            sales_mode as sale_type,
            new_store_id as new_store_id,
            matnr as matnr,
            order_qty_orderean as order_qty_order_unit,
            order_qty_payean as order_qty,
            order_amt as order_amt,
            order_qty_gift_orderean as gift_order_qty_order_unit,
            order_qty_gift_payean as gift_order_qty,
            out_stock_qty as outstock_qty,
            out_stock_qty_give as gift_outstock_qty,
            out_stock_pay_amt as outstock_amt,
            out_stock_pay_amt_notax as outstock_amt_notax,
            out_stock_amt_cb_notax as outstock_cost_notax,
            return_stock_qty as store_return_scm_qty,
            return_stock_pay_amt as store_return_scm_amt,
            return_stock_pay_amt_notax as store_return_scm_amt_notax,
            return_stock_amt_cb_notax as store_return_scm_cost_notax,
            scm_promotion_amt_total as total_benefit_amt,
            scm_promotion_amt_gift as total_gift_benefit_amt,
            scm_bear_amt as scm_bear_nogift_benefit_amt,
            business_bear_amt as business_bear_nogift_benefit_amt,
            market_bear_amt as market_bear_nogift_benefit_amt,
            vendor_bear_amt as vendor_bear_nogift_benefit_amt,
            original_outstock_qty as original_outstock_qty,
            original_outstock_amt as original_outstock_amt,
            promotion_outstock_price as promotion_outstock_price,
            promotion_outstock_qty as promotion_outstock_qty,
            promotion_outstock_amt as promotion_outstock_amt,
            out_stock_amt_cb as out_stock_amt_cb,
            return_stock_amt_cb as return_stock_amt_cb,
            short_qty as short_qty,
            short_amt as short_amt,
            store_order_copies as store_order_copies,
            satisfied_num as satisfied_num,
            zero_out_stock_order_qty as zero_out_stock_order_qty,
            zero_out_stock_order_amt as zero_out_stock_order_amt,
            satisfied_qty as satisfied_qty,
            satisfied_amt as satisfied_amt,
            store_return_qty_shop	as store_return_qty_shop,
            store_return_amt_shop	as store_return_amt_shop,
            store_return_num_shop	as store_return_num_shop,
            delay_store_return_num_shop	as delay_store_return_num_shop,
            nothing_store_return_qty_shop	as nothing_store_return_qty_shop,
            nothing_store_return_amt_shop	as nothing_store_return_amt_shop,
            quality_store_return_qty_shop	as quality_store_return_qty_shop,
            quality_store_return_amt_shop	as quality_store_return_amt_shop,
            quantity_store_return_qty_shop	as quantity_store_return_qty_shop,
            quantity_store_return_amt_shop	as quantity_store_return_amt_shop,
            customer_store_return_qty_shop	as customer_store_return_qty_shop,
            customer_store_return_amt_shop	as customer_store_return_amt_shop,
            other_store_return_qty_shop	as other_store_return_qty_shop,
            other_store_return_amt_shop	as other_store_return_amt_shop,
            out_stock_give_amt_cb_notax,
            satisfied_over_qty,
            satisfied_over_amt,
            satisfied_lower_qty,
            satisfied_lower_amt,
            vender_bear_gift_amt,
            scm_bear_gift_amt,
            vender_bear_gift_qty,
            scm_bear_gift_qty,
            qdm_bear_negative_amt_total,
            qdm_bear_positive_amt_total,
            qdm_bear_gift_qty,
            qdm_bear_gift_amt,
            qdm_bear_nogift_negative_amt,
            qdm_bear_nogift_positive_amt,
            qdm_bear_promotion_fee,
            quatity_store_return_num, 
            quantity_store_return_num,
            customer_store_return_num,
            other_store_return_num,
            nonquantity_store_return_num,
            inc_day
        from
            tmp_dal.dal_scm_dc_matnr_store_mode_daily_wide_sap_total50_finally_sr
    ) a
    left join 
    (
        select * from dim.dim_matnr_info_sap_da where inc_day = '$yestoday'
    ) b
    on a.matnr = b.matnr
    left join
    (
        select * from dim.dim_goods_information_have_pt where inc_day = '$yestoday'
    ) c
    on b.article_id = c.article_id
    left join
    (
        select inc_day,zmd,ean11,zzxsfs,lifnr from ods_sap.zmmt008_new
        where inc_day between '$start_date' and '$end_date'  and (zsfzs = 'X' or zsfdzd = 'X' or zsfwms = 'X')
         group by inc_day,zmd,ean11,zzxsfs,lifnr
    ) v
    on a.inc_day=v.inc_day and a.new_store_id = v.zmd and b.article_id = v.ean11 and a.sale_type = v.zzxsfs 
    left join
    (
        select inc_day,zmd,matnr,zzxsfs,lifnr from ods_sap.zmmt008_new 
        where inc_day between '$start_date' and '$end_date'  and (zsfzs = 'X' or zsfdzd = 'X' or zsfwms = 'X')
        group by inc_day,zmd,matnr,zzxsfs,lifnr
    ) e
    on a.inc_day=e.inc_day and a.new_store_id = e.zmd and a.matnr = e.matnr and a.sale_type = e.zzxsfs
     left join (
     select main_article_id,sub_article_id from dim.dim_wms_article_bom_info_da where inc_day ='$yestoday' and calcu_flag = 0 -- 计算类型是主条码
    ) g on b.article_id = g.sub_article_id
     left join (select * from dim.dim_dc_profile where inc_day='$yestoday' ) dc on a.virtual_dc_id=dc.new_dc_id
     left join (select * from dim.dim_store_profile where inc_day='$yestoday' ) d on a.new_store_id=d.new_sp_store_id
) a
left join
(
    select * from tmp_dal.dal_supply_chain_article_dc_store_sale_dcmove_outstock_forward_direction332_sr
) b
on a.out_stock_date = b.out_stock_date and a.virtual_dc_id = b.new_dc_id and a.article_id = b.article_id and a.delivery_type = b.is_zs 
and a.sale_type = b.sale_type and a.new_store_id = b.store_id
left join
(
    select * from tmp_dal.dal_supply_chain_article_dc_store_sale_dcmove_outstock_reverse_direction332_sr
) c
on a.out_stock_date = c.out_stock_date and a.virtual_dc_id = c.new_dc_id and a.article_id = c.article_id and a.delivery_type = c.is_zs
and a.sale_type = c.sale_type and a.new_store_id = c.store_id
left join (
        select case when calcu_flag <> 0 then main_article_id else sub_article_id end main_article_id  from dim.dim_wms_article_bom_info_da
          where inc_day = '$yestoday' and calcu_flag in (0,1) and memo <> '猪肉'
         group by case when calcu_flag <> 0 then main_article_id else sub_article_id end
) g on a.article_id=g.main_article_id
where 
a.inc_day between '$start_date' and '$end_date' 
and coalesce(a.new_store_id,'')<>'' 
;






-- 插入多分区时按天插入数据(源表、目标表、分区字段、开始时间、结束时间)

-- dal_dc_matnr_store_base_wide23_finally_sr
-- dal_dc_matnr_store_sum_wide23_finally_sr

-- dal_dc_matnr_store_mode_purchase_wide634_sr
-- dal_dc_matnr_store_order_wide25_finally_sr

-- dal_dc_matnr_store_bear_wide26_finally_sr

-- （三合一）
-- dal_scm_dc_matnr_store_mode_daily_wide_sap_total50_finally_sr
-- dal_supply_chain_article_dc_store_sale_dcmove_outstock_forward_direction332_sr
-- dal_supply_chain_article_dc_store_sale_dcmove_outstock_reverse_direction332_sr

-- dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di_tmp

EOF);

echo "------开始执行----------"
echo "参数原始开始日期(格式化)originstartdate:" ${originstartdate}
echo "参数开始日期(格式化)startdate:" ${startdate}
     execute "$sqltxt"



startdate=`date -d "${startdate} +${intervalday} day" +%Y%m%d`

cstatus=$? 
echo "------执行状态码如下----------"
echo $cstatus

if [ $cstatus != 0 ];
then 
echo "执行失败"
exit 1
fi 
echo "------执行结束----------"


done 

