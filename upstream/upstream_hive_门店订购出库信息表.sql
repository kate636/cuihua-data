----####################################################################
----# 任务功能说明： 门店订购、出库、验收详细  偏订购
----# 作者：但堂凯
----# 修改记录
----# 版本     修改人				修改日期				修改说明
----# v1          wk                                       初始版本
----# v2          dan               2023-02-03         关联bic_azsd_c0011新增门店维度group by 
----# v3          lmq               2024-11-20         修改边猪类取值的编码 
----# v4          lmq               2025-06-20         边猪的ean11优先取拆分前的，验收数量取menge 
----####################################################################
set mapred.job.name=dsl_scm_store_purchase_info_di;--设置job名
set mapred.job.queue.name=root.etl;--设置跑数队列
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.java.opts=-Xmx5120m;     --  设置Map任务JVM的堆空间大小，默认-Xmx1024m
set mapreduce.reduce.java.opts=-Xmx5120m;  -- 设置reduce任务JVM的堆空间大小，默认-Xmx1024m
set mapreduce.map.memory.mb=6144;       -- 每个Map Task需要的内存量
set mapreduce.reduce.memory.mb=6144;    -- 每个Reduce Task需要的内存量 
set hive.auto.convert.join=false;
set hive.exec.parallel=true;
set hive.execution.engine=mr;




--猪肉
drop table if exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_200;
create table if not exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_200
as
SELECT
    d1.mandt as mandt--string comment '集团',
	,d1.zebeln as zebeln --string comment '调拨计划单号',
	,d1.sp_store_id as sp_store_id --string comment '店铺id',
	,d1.sp_store_name as sp_store_name --string comment '店铺名称',
	,d1.sp_type as sp_type --string comment '店铺类型 20加盟/10直营',
	,d1.sp_level as sp_level --string comment '店铺等级 1:实体门店 2:菜吧 3:B端客户 4:虚拟门店 5:测试店',
	,d1.dist_id as dist_id --string comment '区县id',
	,d1.dist_description as dist_description --string comment '区县名称',
	,d1.city_id as city_id --string comment '城市id',
	,d1.city_description as city_description --string comment '城市名称',
	,d1.pro_id as pro_id --string comment '省份id',
	,d1.pro_description as pro_description --string comment '省份名称',
	,d1.area_id as area_id --string comment '运营区域id',
	,d1.area_description as area_description --string comment '运营区域名称',
	,d1.sp_store_status as sp_store_status --int COMMENT '店铺状态',
	,d1.group_manager_code as group_manager_code --string comment '督导id',
	,d1.group_manager as group_manager --string comment '督导姓名',
	,d1.new_store_id as new_store_id --string comment '新门店id',
	,d1.dc_id as dc_id --string comment '仓库编号',
	,d1.dc_name as dc_name --string comment '仓库名称',
	,d1.dc_type as dc_type --string comment '物流中心类型',
	,d1.dc_status as dc_status --string comment '物流中心状态',
	,d1.dc_level as dc_level --string comment '仓库类别',
	,d1.dc_level_name as dc_level_name --string comment '仓库类别名称 虚拟 80  直采 70 综合 60 电商 50  菜吧 40 水产 30 猪肉 20 蔬果 10 物料 1000',
	,d1.new_dc_id as new_dc_id --string comment '新仓库id',
	,d1.article_id as article_id --string comment '商品编码',
	,d1.article_name as article_name --string comment '商品编码',
	,d1.category_level1_id as category_level1_id --string comment '大分类编码',
	,d1.category_level1_description as category_level1_description --string comment '大分类描述',
	,d1.category_level2_id as category_level2_id --string comment '中分类编码',
	,d1.category_level2_description as category_level2_description --string comment '中分类描述',
	,d1.category_level3_id as category_level3_id --string comment '小分类编码',
	,d1.category_level3_description as category_level3_description --string comment '小分类描述',
	,d1.is_sort as is_sort --string comment '是否分拣',
	,d1.zpslx as zpslx --string comment '配送类型',
	,d1.zebelp as zebelp --string comment '采购凭证的项目编号',
	,d1.bsart as bsart --string comment '订单类型（采购）',
	,d1.bedat as bedat --string comment '采购订单日期',
	,d1.eindt as eindt --string comment '项目交货日期',
	,d1.zddrq as zddrq --string comment '到店日期',
	,d1.ean11 as ean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge1 as menge1 --double COMMENT '数量',
	,d1.meins1 as meins1 --string comment '基本计量单位',
	,d1.ean12 as ean12 --string comment '国际文件号(EAN/UPC)',
	,d1.menge2 as menge2 --double COMMENT '数量',
	,d1.meins2 as meins2 --string comment '基本计量单位',
	,d1.waers as waers --string comment '货币码',
	,d1.zsfth as zsfth --string comment '退货项目',
	,d1.zsfzp as zsfzp --string comment '免费项目',
	,d1.lifnr as lifnr --string comment '供应商或债权人的帐号',
	,d1.zsfbk as zsfbk --string comment '是否爆款',
	,d1.zcllx as zcllx --string comment '处理类型',
	,d1.ebeln as ebeln --string comment '采购凭证编号',
	,d1.vbeln as vbeln --string comment '交货',
	,d1.zedel as zedel --string comment '删除标记',
	,d1.zebelpz as zebelpz --string comment '采购凭证的项目编号',
	,d1.ersda as ersda --string comment '创建日期',
	,d1.cputm as cputm --string comment '创建时间',
	,d1.ernam as ernam --string comment '对象创建人姓名',
	,d1.laeda as laeda --string comment '上次更改的日期',
	,d1.aetim as aetim --string comment '上次修改时间',
	,d1.aenam as aenam --string comment '对象更改人的姓名',
	,d1.zzwp as zzwp --string comment '早晚配',
	,d1.zck as zck --string comment 'Char 20',
	,d1.zean11 as zean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge3 as menge3 --double COMMENT '数量',
	,d1.meins3 as meins3 --string comment '基本计量单位',
	,d1.zaft_rj as zaft_rj --string comment '日结后商品传输标记',
  ,d1.zjsfc as zjsfc --string COMMENT '接收方、发送方',
  ,cast(null as string ) as posnr --string COMMENT '交货项目',
  ,d1.zid as zid --string COMMENT '消息ID',
  ,d1.zitem as zitem --string COMMENT '接口消息行',
  ,d1.zzstatus as zzstatus --string COMMENT '订购计划接口子状态',
  ,d1.zifid as zifid --string COMMENT '接口ID',
  ,d1.zsender as zsender --string COMMENT '发送方',
  ,d1.zreceiver as zreceiver --string COMMENT '接受方',
  ,d1.zsrusr as zsrusr --string COMMENT '接受/发送人',
  ,d1.zsrdat as zsrdat --string COMMENT '发送/接收日期',
  ,d1.zsrtim as zsrtim --string COMMENT '发送/接收时间',
  ,d1.zdusr as zdusr --string COMMENT '接口处理人',
  ,d1.zddat as zddat --string COMMENT '接口处理日期',
  ,d1.zdtim as zdtim --string COMMENT '接口处理时间',
  ,d1.zzmsg as zzmsg --string COMMENT '消息文本',
  ,d1.zstatus as zstatus --string COMMENT '接口状态',
  ,d1.zxqhz as zxqhz --string COMMENT '需求汇总号',
  ,d1.zjhdh as zjhdh --string COMMENT '计划单号',
  ,d1.zzitem as zzitem --string COMMENT '计划单号行项目',
  ,d1.datum as datum --string COMMENT '订单创建日期',
  ,d1.zddcjsj as zddcjsj --string COMMENT '订单创建时间',
  ,d1.zdgrq as zdgrq --string COMMENT '订购日期',
  ,d1.zddlx as zddlx --string COMMENT '订单类型',
  ,d1.zdgll as zdgll --string COMMENT '订购来源',
  ,d1.zsgsl as zsgsl --double COMMENT '订购数量',
  ,d1.zzjbs as zzjbs --string COMMENT '照旧标识',
  ,d1.zwerks as zwerks --string COMMENT '照旧门店',
  ,d1.zzjrq as zzjrq --string COMMENT '照旧日期',
  ,d1.zbz as zbz --string COMMENT '备注',
  ,d1.zdel as zdel --string COMMENT '删除标记',
  ,d1.zeandw as zeandw --string COMMENT 'EAN单位',
  ,d1.meins as meins --string COMMENT '基本计量单位',
  ,d1.zdgjs as zdgjs --double COMMENT '订购基数',
  ,d1.zzxsl as zzxsl --double COMMENT '订购数量',
  ,d1.zdgbs as zdgbs --double COMMENT '倍数',
  ,d1.matnr as matnr --string comment '物料号'
  ,d1.is_order as is_order --string comment '是否订购商品'
  ,d1.zoanum as zoanum --string comment 'oa号'
  ,d1.zzddj as zzddj --string comment '到店价'
  ,d1.zzckj as zzckj --string comment '出库价'
  ,sum(d1.menge1) over(partition by d1.ymatnr,d1.zebeln) as order_qty_orderean --double comment '订购ean数量'
  ,sum(d1.menge2) over(partition by d1.ymatnr,d1.zebeln) as order_qty_payean --double comment '订购结算ean数量'
  ,sum(case when d1.zsfzp='X' then 0 else d1.zzddj*d1.menge2 end ) over(partition by d1.ymatnr,d1.zebeln) as order_amt  --double comment '订购金额'
  ,sum(case when d1.zsfzp='X' then 0 else d1.zzddj*d1.menge3 end ) over(partition by d1.ymatnr,d1.zebeln) as out_stock_amt --double COMMENT '出库金额',
  ,sum(d1.menge3) over(partition by d1.ymatnr,d1.zebeln) as out_stock_qty   --double COMMENT '出库数量',
  ,sum(case when d1.zsfzp='X' then 0 else d1.zzddj*coalesce(d1.menge2_recive,d1.menge3,0) end ) over(partition by d1.ymatnr,d1.zebeln) as store_recive_amt --double COMMENT '门店收货金额',
  ,sum(coalesce(d1.menge2_recive,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln) as store_recive_qty_payean --double COMMENT '门店收货数量',
  ,sum(coalesce(d1.menge2_recive,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln) as store_recive_qty_orderean --double COMMENT '门店收货数量(订购ean)',
  ,d1.ymatnr as  process_matnr --string comment '物料编号',
    ,d1.yean11 as procerss_ean11 --string comment '国际文件号(EAN/UPC)',
	,(sum(coalesce(d1.menge2_recive,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln))/(sum(d1.menge2) over(partition by d1.ymatnr,d1.zebeln)) as order_percent	
   ,d1.procerss_qty as procerss_qty --double COMMENT '数量',
   ,sum(coalesce(d1.menge2_recive,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln) as expect_weight--double comment '理论收货重量'
   ,sum(d1.menge2) over(partition by d1.ymatnr,d1.zebeln) as expect_order_weight --double comment '理论订购重量'
   ,case when
	   ((sum(coalesce(d1.menge2_recive,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln))/(sum(d1.menge2) over(partition by d1.ymatnr,d1.zebeln))) between  0.85 and 1.15 then '1' else '0' end as is_satisfied_below85
   ,coalesce(sum(d1.menge3) over(partition by d1.ymatnr,d1.zebeln),0)/coalesce(d1.unit_weight,1) as out_stock_qty_kg2fen
   ,d1.menge1_recive as menge1_recive --double comment 'bom收货量 订购'
   ,d1.menge2_recive as menge2_recive --double comment 'bom收货量 结算'
   	,d1.select_unit as select_unit  --wms分拣单位
	,d1.purchase_qty as purchase_qty  --wms订购数量
	,d1.purchase_weight as purchase_weight  --wms订购重量
	,d1.select_qty as select_qty  --wms分拣数量
	,d1.select_weight as select_weight  --wms分拣重量
	,d1.weightflag as weightflag   --是否称重商品(1-称重 0-非称重)
	,d1.ek_zzxsfs as ek_zzxsfs
	,d1.ek_zzcgms as 	ek_zzcgms
	,d1.select_af_qty as select_af_qty
	,d1.select_af_weight as select_af_weight
   ,d1.inc_Day as inc_day
from
(
select
	 d1.mandt as mandt--string comment '集团',
	,d1.zebeln as zebeln --string comment '调拨计划单号',
	,d1.sp_store_id as sp_store_id --string comment '店铺id',
	,d1.sp_store_name as sp_store_name --string comment '店铺名称',
	,d1.sp_type as sp_type --string comment '店铺类型 20加盟/10直营',
	,d1.sp_level as sp_level --string comment '店铺等级 1:实体门店 2:菜吧 3:B端客户 4:虚拟门店 5:测试店',
	,d1.dist_id as dist_id --string comment '区县id',
	,d1.dist_description as dist_description --string comment '区县名称',
	,d1.city_id as city_id --string comment '城市id',
	,d1.city_description as city_description --string comment '城市名称',
	,d1.pro_id as pro_id --string comment '省份id',
	,d1.pro_description as pro_description --string comment '省份名称',
	,d1.area_id as area_id --string comment '运营区域id',
	,d1.area_description as area_description --string comment '运营区域名称',
	,d1.sp_store_status as sp_store_status --int COMMENT '店铺状态',
	,d1.group_manager_code as group_manager_code --string comment '督导id',
	,d1.group_manager as group_manager --string comment '督导姓名',
	,d1.new_store_id as new_store_id --string comment '新门店id',
	,d1.dc_id as dc_id --string comment '仓库编号',
	,d1.dc_name as dc_name --string comment '仓库名称',
	,d1.dc_type as dc_type --string comment '物流中心类型',
	,d1.dc_status as dc_status --string comment '物流中心状态',
	,d1.dc_level as dc_level --string comment '仓库类别',
	,d1.dc_level_name as dc_level_name --string comment '仓库类别名称 虚拟 80  直采 70 综合 60 电商 50  菜吧 40 水产 30 猪肉 20 蔬果 10 物料 1000',
	,d1.new_dc_id as new_dc_id --string comment '新仓库id',
	,d1.article_id as article_id --string comment '商品编码',
	,d1.article_name as article_name --string comment '商品编码',
	,d1.category_level1_id as category_level1_id --string comment '大分类编码',
	,d1.category_level1_description as category_level1_description --string comment '大分类描述',
	,d1.category_level2_id as category_level2_id --string comment '中分类编码',
	,d1.category_level2_description as category_level2_description --string comment '中分类描述',
	,d1.category_level3_id as category_level3_id --string comment '小分类编码',
	,d1.category_level3_description as category_level3_description --string comment '小分类描述',
	,d1.is_sort as is_sort --string comment '是否分拣',
	,d1.zpslx as zpslx --string comment '配送类型',
	,d1.zebelp as zebelp --string comment '采购凭证的项目编号',
	,d1.bsart as bsart --string comment '订单类型（采购）',
	,d1.bedat as bedat --string comment '采购订单日期',
	,d1.eindt as eindt --string comment '项目交货日期',
	,d1.zddrq as zddrq --string comment '到店日期',
	,d1.ean11 as ean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge1 as menge1 --double COMMENT '数量',
	,d1.meins1 as meins1 --string comment '基本计量单位',
	,d1.ean12 as ean12 --string comment '国际文件号(EAN/UPC)',
	,d1.menge2 as menge2 --double COMMENT '数量',
	,d1.meins2 as meins2 --string comment '基本计量单位',
	,d1.waers as waers --string comment '货币码',
	,d1.zsfth as zsfth --string comment '退货项目',
	,d1.zsfzp as zsfzp --string comment '免费项目',
	,d1.lifnr as lifnr --string comment '供应商或债权人的帐号',
	,d1.zsfbk as zsfbk --string comment '是否爆款',
	,d1.zcllx as zcllx --string comment '处理类型',
	,d1.ebeln as ebeln --string comment '采购凭证编号',
	,d1.vbeln as vbeln --string comment '交货',
	,d1.zedel as zedel --string comment '删除标记',
	,d1.zebelpz as zebelpz --string comment '采购凭证的项目编号',
	,d1.ersda as ersda --string comment '创建日期',
	,d1.cputm as cputm --string comment '创建时间',
	,d1.ernam as ernam --string comment '对象创建人姓名',
	,d1.laeda as laeda --string comment '上次更改的日期',
	,d1.aetim as aetim --string comment '上次修改时间',
	,d1.aenam as aenam --string comment '对象更改人的姓名',
	,d1.zzwp as zzwp --string comment '早晚配',
	,d1.zck as zck --string comment 'Char 20',
	,d1.zean11 as zean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge3 as menge3 --double COMMENT '数量',
	,d1.meins3 as meins3 --string comment '基本计量单位',
	,d1.zaft_rj as zaft_rj --string comment '日结后商品传输标记',
  ,d1.sp_store_id as zjsfc --string COMMENT '接收方、发送方',
  ,d4.zid as zid --string COMMENT '消息ID',
  ,d4.zitem as zitem --string COMMENT '接口消息行',
  ,d4.zzstatus as zzstatus --string COMMENT '订购计划接口子状态',
  ,d4.zifid as zifid --string COMMENT '接口ID',
  ,d4.zsender as zsender --string COMMENT '发送方',
  ,d4.zreceiver as zreceiver --string COMMENT '接受方',
  ,d4.zsrusr as zsrusr --string COMMENT '接受/发送人',
  ,d4.zsrdat as zsrdat --string COMMENT '发送/接收日期',
  ,d4.zsrtim as zsrtim --string COMMENT '发送/接收时间',
  ,d4.zdusr as zdusr --string COMMENT '接口处理人',
  ,d4.zddat as zddat --string COMMENT '接口处理日期',
  ,d4.zdtim as zdtim --string COMMENT '接口处理时间',
  ,d4.zzmsg as zzmsg --string COMMENT '消息文本',
  ,d4.zstatus as zstatus --string COMMENT '接口状态',
  ,d4.zxqhz as zxqhz --string COMMENT '需求汇总号',
  ,d4.zjhdh as zjhdh --string COMMENT '计划单号',
  ,d4.zzitem as zzitem --string COMMENT '计划单号行项目',
  ,d4.datum as datum --string COMMENT '订单创建日期',
  ,d4.zddcjsj as zddcjsj --string COMMENT '订单创建时间',
  ,d4.zdgrq as zdgrq --string COMMENT '订购日期',
  ,d4.zddlx as zddlx --string COMMENT '订单类型',
  ,d4.zdgll as zdgll --string COMMENT '订购来源',
  ,d4.zsgsl as zsgsl --double COMMENT '订购数量',
  ,d4.zzjbs as zzjbs --string COMMENT '照旧标识',
  ,d4.zwerks as zwerks --string COMMENT '照旧门店',
  ,d4.zzjrq as zzjrq --string COMMENT '照旧日期',
  ,d4.zbz as zbz --string COMMENT '备注',
  ,d4.zdel as zdel --string COMMENT '删除标记',
  ,d4.zeandw as zeandw --string COMMENT 'EAN单位',
  ,d4.meins as meins --string COMMENT '基本计量单位',
  ,d4.zdgjs as zdgjs --double COMMENT '订购基数',
  ,d4.zzxsl as zzxsl --double COMMENT '订购数量',
  ,d4.zdgbs as zdgbs --double COMMENT '倍数',
  ,d1.matnr as matnr --string comment '物料号'
  ,d1.is_order as is_order --string comment '是否订购商品'
  ,d2.zoanum as zoanum --string comment 'oa号'
  ,coalesce(b.zckj,c.zckj,d2.zzddj,d5.zckj,d7.zckj,d8.zckj,d9.zckj,0) as zzddj --string comment '到店价'
  ,coalesce(d2.zzckj,d5.zckj,0) as zzckj --string comment '出库价'
    -- ,d2.ZZEANME as menge1_recive --double comment 'bom收货量 订购'
   ,d2.menge as menge1_recive --double comment 'bom收货量 订购'  alter by lmq  取值字段改为menge
   ,d6.menge2 as menge2_recive --double comment 'bom收货量 结算'
   	,d1.select_unit as select_unit  --wms分拣单位
	,d1.purchase_qty as purchase_qty  --wms订购数量
	,d1.purchase_weight as purchase_weight  --wms订购重量
	,d1.select_qty as select_qty  --wms分拣数量
	,d1.select_weight as select_weight  --wms分拣重量
	,d1.weightflag as weightflag   --是否称重商品(1-称重 0-非称重)
	,d1.ymenge1 as procerss_qty --double COMMENT '数量',
	,d1.unit_weight as unit_weight
    ,d1.ymatnr as ymatnr
    ,d1.yean11 as yean11
	,d1.ek_zzxsfs as ek_zzxsfs
	,d1.ek_zzcgms as 	ek_zzcgms
	,d1.select_af_qty as select_af_qty
	,d1.select_af_weight as select_af_weight
   ,d1.inc_Day as inc_day
from
    (select 
      *,coalesce(yean11,ean11) as ean11_1   -- 优先取拆分前的 alter by lmq 20250620
    from  
        ddl.ddl_store_order_processing_info
    where 
        inc_day 
    between 
        '$[time(yyyy-MM-dd,-10d)]' 
    and 
        '$[time(yyyy-MM-dd,+1d)]' --and ZPSLX not in ('B','C','D','E')
    and 
        coalesce(ymatnr,'')<>"" 
    and 
        -- category_level2_id in ('1304','1305')
        category_level3_id in ('130205','130206')  /*add by lmq 20241120 取边猪类型，其他边猪不能算，所以只能取小分类为黑边猪、白边猪*/
      ) d1
    left join
    (select 
      zzddj
      ,zzckj
      ,EBELN
      ,EBELP
      ,zoanum
      ,ZZEANME
      ,menge
      ,matnr
      ,case when coalesce(zmd,'')='' then werks ELSE zmd end as store_id
    from 
       ods_sap.ekpo
    where 
       inc_day 
    between 
       '$[time(yyyy-MM-dd,-30d)]' and '$[time(yyyy-MM-dd,+10d)]') d2
    on 
        d1.ebeln=d2.ebeln 
    and 
        d1.matnr=d2.matnr
    and 
        d1.new_store_id=d2.store_id
    and 
        cast(d1.zebelp as int )=cast(d2.EBELP as int )
	left join
	(select max(zckj) as zckj,inc_day,werks,ean11,zmd from ods_sap.bic_azsd_c0011 where inc_day >= '$[time(yyyy-MM-dd,-12d)]' and inc_day <= '$[time(yyyy-MM-dd,-1d)]' group by inc_day,werks,ean11,zmd) b
	on
	d1.inc_day = date_add(b.inc_day,2) and d1.new_dc_id = b.werks and d1.article_id = b.ean11 and d1.new_store_id = b.zmd
	left join
	(select max(zckj) as zckj,inc_day,werks,ean11,zmd from ods_sap.bic_azsd_c0011 where inc_day >= '$[time(yyyy-MM-dd,-13d)]' and inc_day <= '$[time(yyyy-MM-dd,-2d)]' group by inc_day,werks,ean11,zmd) c
	on
	d1.inc_day = date_add(c.inc_day,3) and d1.new_dc_id = c.werks and d1.article_id = c.ean11 and d1.new_store_id = c.zmd	
    left join 
   (select 
      * 
    from 
       ddl.ddl_store_purchase_info_sap 
    where 
       inc_day 
    between 
       '$[time(yyyy-MM-dd,-20d)]' and '$[time(yyyy-MM-dd,+10d)]') d4
    on 
       d1.ean11=d4.article_id 
    and 
       d1.new_store_id=d4.new_sp_store_id 
    and 
       d1.ZEBELN=d4.ZXQHZ
    left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-10d)]' and '$[time(yyyy-MM-dd,+1d)]' group by inc_day,werks,matnr
    ) d5
    on
        d1.inc_day=d5.inc_day
    and
        d1.new_dc_id=d5.werks
    and
        d1.matnr=d5.matnr
    left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-11d)]' and '$[time(yyyy-MM-dd)]' group by inc_day,werks,matnr
    ) d7
    on
        d1.inc_day=date_add(d7.inc_day,1)
    and
        d1.new_dc_id=d7.werks
    and
        d1.matnr=d7.matnr
       left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-12d)]' and '$[time(yyyy-MM-dd,-1d)]' group by inc_day,werks,matnr
    ) d8
    on
        d1.inc_day=date_add(d8.inc_day,2)
    and
        d1.new_dc_id=d8.werks
    and
        d1.matnr=d8.matnr
           left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-13d)]' and '$[time(yyyy-MM-dd,-2d)]' group by inc_day,werks,matnr
    ) d9
    on
        d1.inc_day=date_add(d9.inc_day,3)
    and
        d1.new_dc_id=d9.werks
    and
        d1.matnr=d9.matnr
    left join
    (
    select 
        zdjh ----string COMMENT '单据号',
        ,zhxm --string COMMENT '行项目',
        ,werks --string COMMENT '工厂',
        ,zjsfc --string COMMENT '接收方、发送方',
        ,ean11 --string COMMENT 'EAN 码',
        ,sum(menge2) as menge2
        ,meins2 --string COMMENT '收货单位',
        ,ean12 --string COMMENT 'EAN 码',
    from 
        ddl.ddl_store_recive_info 
    where 
        inc_day between '$[time(yyyy-MM-dd,-30d)]' and '$[time(yyyy-MM-dd,+10d)]'
    group by
        ZDJH,ZHXM,WERKS,ZJSFC,MATNR,EAN11,EAN12,meins2,meins1
    ) d6 
    on 
       d1.ean11_1=d6.ean11    -- 优先取拆分前的 alter by lmq 20250620
    and 
       d1.ean12=d6.ean12 
    and 
       d1.new_store_id=d6.ZJSFC 
    and 
       d1.zebeln=d6.ZDJH 
    and 
       CAST(d1.ZEBELP as int)=CAST(d6.ZHXM as int)
) d1;





--鱼头鱼身鱼尾
drop table if exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_201;
create table if not exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_201
as
SELECT
    d1.mandt as mandt--string comment '集团',
	,d1.zebeln as zebeln --string comment '调拨计划单号',
	,d1.sp_store_id as sp_store_id --string comment '店铺id',
	,d1.sp_store_name as sp_store_name --string comment '店铺名称',
	,d1.sp_type as sp_type --string comment '店铺类型 20加盟/10直营',
	,d1.sp_level as sp_level --string comment '店铺等级 1:实体门店 2:菜吧 3:B端客户 4:虚拟门店 5:测试店',
	,d1.dist_id as dist_id --string comment '区县id',
	,d1.dist_description as dist_description --string comment '区县名称',
	,d1.city_id as city_id --string comment '城市id',
	,d1.city_description as city_description --string comment '城市名称',
	,d1.pro_id as pro_id --string comment '省份id',
	,d1.pro_description as pro_description --string comment '省份名称',
	,d1.area_id as area_id --string comment '运营区域id',
	,d1.area_description as area_description --string comment '运营区域名称',
	,d1.sp_store_status as sp_store_status --int COMMENT '店铺状态',
	,d1.group_manager_code as group_manager_code --string comment '督导id',
	,d1.group_manager as group_manager --string comment '督导姓名',
	,d1.new_store_id as new_store_id --string comment '新门店id',
	,d1.dc_id as dc_id --string comment '仓库编号',
	,d1.dc_name as dc_name --string comment '仓库名称',
	,d1.dc_type as dc_type --string comment '物流中心类型',
	,d1.dc_status as dc_status --string comment '物流中心状态',
	,d1.dc_level as dc_level --string comment '仓库类别',
	,d1.dc_level_name as dc_level_name --string comment '仓库类别名称 虚拟 80  直采 70 综合 60 电商 50  菜吧 40 水产 30 猪肉 20 蔬果 10 物料 1000',
	,d1.new_dc_id as new_dc_id --string comment '新仓库id',
	,d1.article_id as article_id --string comment '商品编码',
	,d1.article_name as article_name --string comment '商品编码',
	,d1.category_level1_id as category_level1_id --string comment '大分类编码',
	,d1.category_level1_description as category_level1_description --string comment '大分类描述',
	,d1.category_level2_id as category_level2_id --string comment '中分类编码',
	,d1.category_level2_description as category_level2_description --string comment '中分类描述',
	,d1.category_level3_id as category_level3_id --string comment '小分类编码',
	,d1.category_level3_description as category_level3_description --string comment '小分类描述',
	,d1.is_sort as is_sort --string comment '是否分拣',
	,d1.zpslx as zpslx --string comment '配送类型',
	,d1.zebelp as zebelp --string comment '采购凭证的项目编号',
	,d1.bsart as bsart --string comment '订单类型（采购）',
	,d1.bedat as bedat --string comment '采购订单日期',
	,d1.eindt as eindt --string comment '项目交货日期',
	,d1.zddrq as zddrq --string comment '到店日期',
	,d1.ean11 as ean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge1 as menge1 --double COMMENT '数量',
	,d1.meins1 as meins1 --string comment '基本计量单位',
	,d1.ean12 as ean12 --string comment '国际文件号(EAN/UPC)',
	,d1.menge2 as menge2 --double COMMENT '数量',
	,d1.meins2 as meins2 --string comment '基本计量单位',
	,d1.waers as waers --string comment '货币码',
	,d1.zsfth as zsfth --string comment '退货项目',
	,d1.zsfzp as zsfzp --string comment '免费项目',
	,d1.lifnr as lifnr --string comment '供应商或债权人的帐号',
	,d1.zsfbk as zsfbk --string comment '是否爆款',
	,d1.zcllx as zcllx --string comment '处理类型',
	,d1.ebeln as ebeln --string comment '采购凭证编号',
	,d1.vbeln as vbeln --string comment '交货',
	,d1.zedel as zedel --string comment '删除标记',
	,d1.zebelpz as zebelpz --string comment '采购凭证的项目编号',
	,d1.ersda as ersda --string comment '创建日期',
	,d1.cputm as cputm --string comment '创建时间',
	,d1.ernam as ernam --string comment '对象创建人姓名',
	,d1.laeda as laeda --string comment '上次更改的日期',
	,d1.aetim as aetim --string comment '上次修改时间',
	,d1.aenam as aenam --string comment '对象更改人的姓名',
	,d1.zzwp as zzwp --string comment '早晚配',
	,d1.zck as zck --string comment 'Char 20',
	,d1.zean11 as zean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge3 as menge3 --double COMMENT '数量',
	,d1.meins3 as meins3 --string comment '基本计量单位',
	,d1.zaft_rj as zaft_rj --string comment '日结后商品传输标记',
  ,d1.sp_store_id as zjsfc --string COMMENT '接收方、发送方',
  ,cast(null as string ) as posnr --string COMMENT '交货项目',
  ,d4.zid as zid --string COMMENT '消息ID',
  ,d4.zitem as zitem --string COMMENT '接口消息行',
  ,d4.zzstatus as zzstatus --string COMMENT '订购计划接口子状态',
  ,d4.zifid as zifid --string COMMENT '接口ID',
  ,d4.zsender as zsender --string COMMENT '发送方',
  ,d4.zreceiver as zreceiver --string COMMENT '接受方',
  ,d4.zsrusr as zsrusr --string COMMENT '接受/发送人',
  ,d4.zsrdat as zsrdat --string COMMENT '发送/接收日期',
  ,d4.zsrtim as zsrtim --string COMMENT '发送/接收时间',
  ,d4.zdusr as zdusr --string COMMENT '接口处理人',
  ,d4.zddat as zddat --string COMMENT '接口处理日期',
  ,d4.zdtim as zdtim --string COMMENT '接口处理时间',
  ,d4.zzmsg as zzmsg --string COMMENT '消息文本',
  ,d4.zstatus as zstatus --string COMMENT '接口状态',
  ,d4.zxqhz as zxqhz --string COMMENT '需求汇总号',
  ,d4.zjhdh as zjhdh --string COMMENT '计划单号',
  ,d4.zzitem as zzitem --string COMMENT '计划单号行项目',
  ,d4.datum as datum --string COMMENT '订单创建日期',
  ,d4.zddcjsj as zddcjsj --string COMMENT '订单创建时间',
  ,d4.zdgrq as zdgrq --string COMMENT '订购日期',
  ,d4.zddlx as zddlx --string COMMENT '订单类型',
  ,d4.zdgll as zdgll --string COMMENT '订购来源',
  ,d4.zsgsl as zsgsl --double COMMENT '订购数量',
  ,d4.zzjbs as zzjbs --string COMMENT '照旧标识',
  ,d4.zwerks as zwerks --string COMMENT '照旧门店',
  ,d4.zzjrq as zzjrq --string COMMENT '照旧日期',
  ,d4.zbz as zbz --string COMMENT '备注',
  ,d4.zdel as zdel --string COMMENT '删除标记',
  ,d4.zeandw as zeandw --string COMMENT 'EAN单位',
  ,d4.meins as meins --string COMMENT '基本计量单位',
  ,d4.zdgjs as zdgjs --double COMMENT '订购基数',
  ,d4.zzxsl as zzxsl --double COMMENT '订购数量',
  ,d4.zdgbs as zdgbs --double COMMENT '倍数',
  ,d1.matnr as matnr --string comment '物料号'
  ,d1.is_order as is_order --string comment '是否订购商品'
  ,d2.zoanum as zoanum --string comment 'oa号'
  ,coalesce(d2.zzddj,d5.zckj,0) as zzddj --string comment '到店价'
  ,coalesce(d2.zzckj,d5.zckj,0) as zzckj --string comment '出库价'
  ,sum(d1.menge1) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as order_qty_orderean --double comment '订购ean数量'
  ,sum(d1.menge2) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as order_qty_payean --double comment '订购结算ean数量'
  ,sum(case when d1.zsfzp='X' then 0 else coalesce(b.zckj,c.zckj,d2.zzddj,d5.zckj,d7.zckj,d8.zckj,d9.zckj,0)*d1.menge2 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as order_amt --double comment '订购金额'
  ,sum(case when d1.zsfzp='X' then 0 else COALESCE(b.zckj,c.zckj,d2.zzddj,d5.zckj,d7.zckj,d8.zckj,d9.zckj,0)*d1.menge3 end ) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as out_stock_amt --double COMMENT '出库金额',
  ,sum(d1.menge3) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as out_stock_qty --double COMMENT '出库数量',
  ,sum(case when d1.zsfzp='X' then 0 else COALESCE(b.zckj,c.zckj,d2.zzddj,d5.zckj,d7.zckj,d8.zckj,d9.zckj,0)*coalesce(d6.menge2,d1.menge3,0) end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as store_recive_amt --double COMMENT '门店收货金额',
  ,sum(coalesce(d6.menge2,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as store_recive_qty_payean --double COMMENT '门店收货数量',
  ,sum(coalesce(d2.ZZEANME,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id) as store_recive_qty_orderean --double COMMENT '门店收货数量(订购ean)',
  ,d1.ymatnr as  process_matnr --string comment '物料编号',
    ,d1.yean11 as procerss_ean11 --string comment '国际文件号(EAN/UPC)',
    ,case 
        when d1.ymatnr in (
            '000000000000113147',
            '000000000000101980',
            '000000000000101918',
            '000000000000101986',
            '000000000000101919',
            '000000000000101924',
            '000000000000101981')
        then (sum(coalesce(d2.ZZEANME,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))/(sum(case when d1.matnr=d1.ymatnr then 0 else d1.menge1 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))
        when d1.MEINS1<>d1.MEINS2 and d1.MEINS2='KG' then ((sum(coalesce(d2.ZZEANME,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))/(sum(case when d1.matnr=d1.ymatnr then 0 else d1.menge1 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))) 
        else ((sum(coalesce(d6.menge2,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))/(sum(case when d1.matnr=d1.ymatnr then 0 else d1.menge2 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))) 
    end as order_percent 
   ,d1.ymenge1 as procerss_qty --double COMMENT '数量',
   ,case 
   		when d1.meins2<>'KG' then d1.unit_weight*(sum(coalesce(d6.menge2,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))
		else (sum(coalesce(d6.menge2,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id)) 
	end as expect_weight--double comment '理论收货重量'
   ,case 
   		when d1.meins1<>'KG' then d1.unit_weight*(sum(case when d1.matnr=d1.ymatnr then 0 else d1.menge2 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id)) 
		else (sum(case when d1.matnr=d1.ymatnr then 0 else d1.menge2 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id)) 
	end as expect_order_weight --double comment '理论订购重量'
    ,case when d1.MEINS1<>d1.meins2 and d1.MEINS2='KG' then (
      case when (sum(coalesce(d2.ZZEANME,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))=(sum(case when d1.matnr=d1.ymatnr then 0 else d1.menge1 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id)) then '1' else '0' end ) 
    else (case when 
      ((sum(coalesce(d6.menge2,d1.menge3,0)) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))/(sum(case when d1.matnr=d1.ymatnr then 0 else d1.menge2 end) over(partition by d1.ymatnr,d1.zebeln,d1.article_id))) between  0.85 and 1.15 then '1' else '0' end) 
    end as is_satisfied_below85 
   ,CASE 
    	WHEN d1.meins2 ='KG' THEN coalesce(d1.menge3,0)/coalesce(d1.unit_weight,1)  
        ELSE d1.menge3  
	end as out_stock_qty_kg2fen
   ,d2.ZZEANME as menge1_recive --double comment 'bom收货量 订购'
   ,d6.menge2 as menge2_recive --double comment 'bom收货量 结算'
    ,d1.select_unit as select_unit  --wms分拣单位
	,d1.purchase_qty as purchase_qty  --wms订购数量
	,d1.purchase_weight as purchase_weight  --wms订购重量
	,d1.select_qty as select_qty  --wms分拣数量
	,d1.select_weight as select_weight  --wms分拣重量
	,d1.weightflag as weightflag   --是否称重商品(1-称重 0-非称重)
	,d1.ek_zzxsfs as ek_zzxsfs
	,d1.ek_zzcgms as ek_zzcgms
	,d1.select_af_qty as select_af_qty
	,d1.select_af_weight as select_af_weight
   ,d1.inc_Day as inc_day
from
    (select 
      * 
    from  
        ddl.ddl_store_order_processing_info
    where 
        inc_day 
    between 
        '$[time(yyyy-MM-dd,-10d)]' 
    and 
        '$[time(yyyy-MM-dd,+1d)]' --and ZPSLX not in ('B','C','D','E')
     and 
        coalesce(ymatnr,'')<>''
    and 
        -- category_level2_id not in ('1304','1305')
        category_level3_id not in ('130205','130206')  /*add by lmq 20241120 取边猪类型，其他边猪不能算，所以只能取小分类为黑边猪、白边猪*/
      ) d1
    left join
    (select 
      zzddj
      ,zzckj
      ,EBELN
      ,EBELP
      ,zoanum
      ,ZZEANME
      ,matnr
      ,case when coalesce(zmd,'')='' then werks ELSE zmd end as store_id
    from 
       ods_sap.ekpo
    where 
       inc_day 
    between 
       '$[time(yyyy-MM-dd,-30d)]' and '$[time(yyyy-MM-dd,+10d)]') d2
    on 
        d1.ebeln=d2.ebeln 
    and 
        d1.matnr=d2.matnr
    and 
        d1.new_store_id=d2.store_id
    and 
        cast(d1.zebelp as int )=cast(d2.EBELP as int )
	left join
	(select max(zckj) as zckj,inc_day,werks,ean11,zmd from ods_sap.bic_azsd_c0011 where inc_day >= '$[time(yyyy-MM-dd,-12d)]' and inc_day <= '$[time(yyyy-MM-dd,-1d)]' group by inc_day,werks,ean11,zmd) b
	on
	d1.inc_day = date_add(b.inc_day,2) and d1.new_dc_id = b.werks and d1.article_id = b.ean11 and d1.new_store_id = b.zmd
	left join
	(select max(zckj) as zckj,inc_day,werks,ean11,zmd from ods_sap.bic_azsd_c0011 where inc_day >= '$[time(yyyy-MM-dd,-13d)]' and inc_day <= '$[time(yyyy-MM-dd,-2d)]' group by inc_day,werks,ean11,zmd) c
	on
	d1.inc_day = date_add(c.inc_day,3) and d1.new_dc_id = c.werks and d1.article_id = c.ean11 and d1.new_store_id = c.zmd	
    left join 
   (select 
      * 
    from 
       ddl.ddl_store_purchase_info_sap 
    where 
       inc_day 
    between 
       '$[time(yyyy-MM-dd,-20d)]' and '$[time(yyyy-MM-dd,+10d)]') d4
    on 
       d1.ean11=d4.article_id 
    and 
       d1.new_store_id=d4.new_sp_store_id 
    and 
       d1.ZEBELN=d4.ZXQHZ
        left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-10d)]' and '$[time(yyyy-MM-dd,+1d)]' group by inc_day,werks,matnr
    ) d5
    on
        d1.inc_day=d5.inc_day
    and
        d1.new_dc_id=d5.werks
    and
        d1.matnr=d5.matnr
    left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-11d)]' and '$[time(yyyy-MM-dd)]' group by inc_day,werks,matnr
    ) d7
    on
        d1.inc_day=date_add(d7.inc_day,1)
    and
        d1.new_dc_id=d7.werks
    and
        d1.matnr=d7.matnr
       left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-12d)]' and '$[time(yyyy-MM-dd,-1d)]' group by inc_day,werks,matnr
    ) d8
    on
        d1.inc_day=date_add(d8.inc_day,2)
    and
        d1.new_dc_id=d8.werks
    and
        d1.matnr=d8.matnr
           left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-13d)]' and '$[time(yyyy-MM-dd,-2d)]' group by inc_day,werks,matnr
    ) d9
    on
        d1.inc_day=date_add(d9.inc_day,3)
    and
        d1.new_dc_id=d9.werks
    and
        d1.matnr=d9.matnr
     left join
    (
    select 
        zdjh ----string COMMENT '单据号',
        ,zhxm --string COMMENT '行项目',
        ,werks --string COMMENT '工厂',
        ,zjsfc --string COMMENT '接收方、发送方',
        ,ean11 --string COMMENT 'EAN 码',
        ,sum(menge2) as menge2 --double COMMENT '收货数量',
        ,meins2 --string COMMENT '收货单位',
        ,ean12 --string COMMENT 'EAN 码',
    from 
        ddl.ddl_store_recive_info 
    where 
        inc_day between '$[time(yyyy-MM-dd,-30d)]' and '$[time(yyyy-MM-dd,+10d)]'
    group by
        ZDJH,ZHXM,WERKS,ZJSFC,MATNR,EAN11,EAN12,meins2,meins1
    ) d6 
    on 
       d1.ean11=d6.ean11 
    and 
       d1.ean12=d6.ean12 
    and 
       d1.new_store_id=d6.ZJSFC 
    and 
       d1.zebeln=d6.ZDJH 
    and 
       CAST(d1.ZEBELP as int)=CAST(d6.ZHXM as int);


drop table if exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_202;
create table if not exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_202
as
SELECT
  d1.mandt as mandt--string comment '集团',
	,d1.zebeln as zebeln --string comment '调拨计划单号',
	,d1.sp_store_id as sp_store_id --string comment '店铺id',
	,d1.sp_store_name as sp_store_name --string comment '店铺名称',
	,d1.sp_type as sp_type --string comment '店铺类型 20加盟/10直营',
	,d1.sp_level as sp_level --string comment '店铺等级 1:实体门店 2:菜吧 3:B端客户 4:虚拟门店 5:测试店',
	,d1.dist_id as dist_id --string comment '区县id',
	,d1.dist_description as dist_description --string comment '区县名称',
	,d1.city_id as city_id --string comment '城市id',
	,d1.city_description as city_description --string comment '城市名称',
	,d1.pro_id as pro_id --string comment '省份id',
	,d1.pro_description as pro_description --string comment '省份名称',
	,d1.area_id as area_id --string comment '运营区域id',
	,d1.area_description as area_description --string comment '运营区域名称',
	,d1.sp_store_status as sp_store_status --int COMMENT '店铺状态',
	,d1.group_manager_code as group_manager_code --string comment '督导id',
	,d1.group_manager as group_manager --string comment '督导姓名',
	,d1.new_store_id as new_store_id --string comment '新门店id',
	,d1.dc_id as dc_id --string comment '仓库编号',
	,d1.dc_name as dc_name --string comment '仓库名称',
	,d1.dc_type as dc_type --string comment '物流中心类型',
	,d1.dc_status as dc_status --string comment '物流中心状态',
	,d1.dc_level as dc_level --string comment '仓库类别',
	,d1.dc_level_name as dc_level_name --string comment '仓库类别名称 虚拟 80  直采 70 综合 60 电商 50  菜吧 40 水产 30 猪肉 20 蔬果 10 物料 1000',
	,d1.new_dc_id as new_dc_id --string comment '新仓库id',
	,d1.article_id as article_id --string comment '商品编码',
	,d1.article_name as article_name --string comment '商品编码',
	,d1.category_level1_id as category_level1_id --string comment '大分类编码',
	,d1.category_level1_description as category_level1_description --string comment '大分类描述',
	,d1.category_level2_id as category_level2_id --string comment '中分类编码',
	,d1.category_level2_description as category_level2_description --string comment '中分类描述',
	,d1.category_level3_id as category_level3_id --string comment '小分类编码',
	,d1.category_level3_description as category_level3_description --string comment '小分类描述',
	,d1.is_sort as is_sort --string comment '是否分拣',
	,d1.zpslx as zpslx --string comment '配送类型',
	,d1.zebelp as zebelp --string comment '采购凭证的项目编号',
	,d1.bsart as bsart --string comment '订单类型（采购）',
	,d1.bedat as bedat --string comment '采购订单日期',
	,d1.eindt as eindt --string comment '项目交货日期',
	,d1.zddrq as zddrq --string comment '到店日期',
	,d1.ean11 as ean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge1 as menge1 --double COMMENT '数量',
	,d1.meins1 as meins1 --string comment '基本计量单位',
	,d1.ean12 as ean12 --string comment '国际文件号(EAN/UPC)',
	,d1.menge2 as menge2 --double COMMENT '数量',
	,d1.meins2 as meins2 --string comment '基本计量单位',
	,d1.waers as waers --string comment '货币码',
	,d1.zsfth as zsfth --string comment '退货项目',
	,d1.zsfzp as zsfzp --string comment '免费项目',
	,d1.lifnr as lifnr --string comment '供应商或债权人的帐号',
	,d1.zsfbk as zsfbk --string comment '是否爆款',
	,d1.zcllx as zcllx --string comment '处理类型',
	,d1.ebeln as ebeln --string comment '采购凭证编号',
	,d1.vbeln as vbeln --string comment '交货',
	,d1.zedel as zedel --string comment '删除标记',
	,d1.zebelpz as zebelpz --string comment '采购凭证的项目编号',
	,d1.ersda as ersda --string comment '创建日期',
	,d1.cputm as cputm --string comment '创建时间',
	,d1.ernam as ernam --string comment '对象创建人姓名',
	,d1.laeda as laeda --string comment '上次更改的日期',
	,d1.aetim as aetim --string comment '上次修改时间',
	,d1.aenam as aenam --string comment '对象更改人的姓名',
	,d1.zzwp as zzwp --string comment '早晚配',
	,d1.zck as zck --string comment 'Char 20',
	,d1.zean11 as zean11 --string comment '国际文件号(EAN/UPC)',
	,d1.menge3 as menge3 --double COMMENT '数量',
	,d1.meins3 as meins3 --string comment '基本计量单位',
	,d1.zaft_rj as zaft_rj --string comment '日结后商品传输标记',
  ,d1.sp_store_id as zjsfc --string COMMENT '接收方、发送方',
  ,cast(null as string ) as posnr --string COMMENT '交货项目',
  ,d4.zid as zid --string COMMENT '消息ID',
  ,d4.zitem as zitem --string COMMENT '接口消息行',
  ,d4.zzstatus as zzstatus --string COMMENT '订购计划接口子状态',
  ,d4.zifid as zifid --string COMMENT '接口ID',
  ,d4.zsender as zsender --string COMMENT '发送方',
  ,d4.zreceiver as zreceiver --string COMMENT '接受方',
  ,d4.zsrusr as zsrusr --string COMMENT '接受/发送人',
  ,d4.zsrdat as zsrdat --string COMMENT '发送/接收日期',
  ,d4.zsrtim as zsrtim --string COMMENT '发送/接收时间',
  ,d4.zdusr as zdusr --string COMMENT '接口处理人',
  ,d4.zddat as zddat --string COMMENT '接口处理日期',
  ,d4.zdtim as zdtim --string COMMENT '接口处理时间',
  ,d4.zzmsg as zzmsg --string COMMENT '消息文本',
  ,d4.zstatus as zstatus --string COMMENT '接口状态',
  ,d4.zxqhz as zxqhz --string COMMENT '需求汇总号',
  ,d4.zjhdh as zjhdh --string COMMENT '计划单号',
  ,d4.zzitem as zzitem --string COMMENT '计划单号行项目',
  ,d4.datum as datum --string COMMENT '订单创建日期',
  ,d4.zddcjsj as zddcjsj --string COMMENT '订单创建时间',
  ,d4.zdgrq as zdgrq --string COMMENT '订购日期',
  ,d4.zddlx as zddlx --string COMMENT '订单类型',
  ,d4.zdgll as zdgll --string COMMENT '订购来源',
  ,d4.zsgsl as zsgsl --double COMMENT '订购数量',
  ,d4.zzjbs as zzjbs --string COMMENT '照旧标识',
  ,d4.zwerks as zwerks --string COMMENT '照旧门店',
  ,d4.zzjrq as zzjrq --string COMMENT '照旧日期',
  ,d4.zbz as zbz --string COMMENT '备注',
  ,d4.zdel as zdel --string COMMENT '删除标记',
  ,d4.zeandw as zeandw --string COMMENT 'EAN单位',
  ,d4.meins as meins --string COMMENT '基本计量单位',
  ,d4.zdgjs as zdgjs --double COMMENT '订购基数',
  ,d4.zzxsl as zzxsl --double COMMENT '订购数量',
  ,d4.zdgbs as zdgbs --double COMMENT '倍数',
  ,d1.matnr as matnr --string comment '物料号'
  ,d1.is_order as is_order --string comment '是否订购商品'
  ,d3.zoanum as zoanum --string comment 'oa号'
  ,coalesce(d3.zzddj,d5.zckj,0) as zzddj --string comment '到店价'
  ,coalesce(d3.zzckj,d5.zckj,0) as zzckj --string comment '出库价'
  ,d1.menge1 as order_qty_orderean --double comment '订购ean数量'
  ,d1.menge2 as order_qty_payean --double comment '订购结算ean数量'
  ,case when d1.zsfzp='X' then 0 else coalesce(b.zckj,c.zckj,d3.zzddj,d5.zckj,d7.zckj,d8.zckj,d9.zckj,0)*d1.menge2 end as order_amt --double comment '订购金额'
  ,case when d1.zsfzp='X' then 0 else COALESCE(b.zckj,c.zckj,d3.zzckj,d5.zckj,d7.zckj,d8.zckj,d9.zckj,0)*d1.MENGE3 end as out_stock_amt --double COMMENT '出库金额',
  ,d1.menge3 as out_stock_qty --double COMMENT '出库数量',
  ,case when d1.zsfzp='X' then 0 else COALESCE(b.zckj,c.zckj,d3.zzddj,d5.zckj,d7.zckj,d8.zckj,d9.zckj,0)*coalesce(d2.menge2,d1.MENGE3,0) end as store_recive_amt --double COMMENT '门店收货金额',
  ,coalesce(d2.menge2,d1.MENGE3,0) as store_recive_qty_payean --double COMMENT '门店收货数量',
  ,case when d1.meins1=d1.meins2 then coalesce(d2.menge2,d1.MENGE3,0) else coalesce(d3.ZZEANME,0) end as store_recive_qty_orderean --double COMMENT '门店收货数量(订购ean)',
  ,d1.ymatnr as  process_matnr --string comment '物料编号',
    ,d1.yean11 as procerss_ean11 --string comment '国际文件号(EAN/UPC)',
    ,case 
        when d1.MEINS1<>d1.MEINS2 and d1.MEINS2='KG' then coalesce(d3.ZZEANME,0)/d1.menge1 
        else coalesce(d2.menge2,d1.MENGE3,0)/d1.menge2 
    end as order_percent
   ,d1.ymenge1 as procerss_qty --double COMMENT '数量',
   ,case when d1.meins2<>'KG' then d1.unit_weight*coalesce(d2.menge2,d1.MENGE3,0) else coalesce(d2.menge2,d1.MENGE3,0) end as expect_weight--double comment '理论收货重量'
   ,case when d1.meins1<>'KG' then d1.unit_weight*d1.menge1 else d1.menge1 end as expect_order_weight --double comment '理论订购重量'
   ,case when d1.MEINS1<>d1.meins2 and d1.MEINS2='KG' then (
      case when coalesce(d3.ZZEANME,0)=d1.menge1  then '1' else '0' end ) 
    else (case when 
      (coalesce(d2.menge2,d1.MENGE3,0)/d1.menge2) between  0.85 and 1.15 then '1' else '0' end) 
    end as is_satisfied_below85 
   ,CASE 
        WHEN d1.meins2 ='KG' THEN coalesce(d1.menge3,0)/coalesce(d1.unit_weight,1)  
        ELSE d1.menge3  
	end as out_stock_qty_kg2fen
   ,0 as menge1_recive --double comment 'bom收货量 订购'
   ,0 as menge2_recive --double comment 'bom收货量 结算'
    ,d1.select_unit as select_unit  --wms分拣单位
	,d1.purchase_qty as purchase_qty  --wms订购数量
	,d1.purchase_weight as purchase_weight  --wms订购重量
	,d1.select_qty as select_qty  --wms分拣数量
	,d1.select_weight as select_weight  --wms分拣重量
	,d1.weightflag as weightflag   --是否称重商品(1-称重 0-非称重)
	,d1.ek_zzxsfs as ek_zzxsfs
	,d1.ek_zzcgms as ek_zzcgms
	,d1.select_af_qty as select_af_qty
	,d1.select_af_weight as select_af_weight
   ,d1.inc_Day as inc_day
from
    (select 
      * 
    from  
        ddl.ddl_store_order_processing_info
    where 
        inc_day 
    between 
        '$[time(yyyy-MM-dd,-10d)]' 
    and 
        '$[time(yyyy-MM-dd,+1d)]' --and ZPSLX not in ('B','C','D','E')
    and 
        coalesce(ymatnr,'')="" 
    ) d1
		left join
	(select max(zckj) as zckj,inc_day,werks,ean11,zmd from ods_sap.bic_azsd_c0011 where inc_day >= '$[time(yyyy-MM-dd,-12d)]' and inc_day <= '$[time(yyyy-MM-dd,-1d)]' group by inc_day,werks,ean11,zmd) b
	on
	d1.inc_day = date_add(b.inc_day,2) and d1.new_dc_id = b.werks and d1.article_id = b.ean11  and d1.new_store_id = b.zmd
	left join
	(select max(zckj) as zckj,inc_day,werks,ean11,zmd from ods_sap.bic_azsd_c0011 where inc_day >= '$[time(yyyy-MM-dd,-13d)]' and inc_day <= '$[time(yyyy-MM-dd,-2d)]' group by inc_day,werks,ean11,zmd) c
	on
	d1.inc_day = date_add(c.inc_day,3) and d1.new_dc_id = c.werks and d1.article_id = c.ean11  and d1.new_store_id = c.zmd	
    left join
    (
    select 
        zdjh ----string COMMENT '单据号',
        ,zhxm --string COMMENT '行项目',
        ,werks --string COMMENT '工厂',
        ,zjsfc --string COMMENT '接收方、发送方',
        ,ean11 --string COMMENT 'EAN 码',
        --,menge1 as dg_recieve_qty  --double COMMENT '数量',
        --,meins1 as dg_recieve_unit --string COMMENT '基本计量单位',
        ,sum(menge2) as menge2 --double COMMENT '收货数量',
        ,meins2 --string COMMENT '收货单位',
        ,ean12 --string COMMENT 'EAN 码',
    from 
        ddl.ddl_store_recive_info 
    where 
        inc_day between '$[time(yyyy-MM-dd,-15d)]' and '$[time(yyyy-MM-dd,+1d)]'
    group by
        ZDJH,ZHXM,WERKS,ZJSFC,MATNR,EAN11,EAN12,meins2,meins1
    )d2
    on 
       d1.ean11=d2.ean11 
    and 
       d1.ean12=d2.ean12 
    and 
       d1.new_store_id=d2.ZJSFC 
    and 
       d1.zebeln=d2.ZDJH 
    and 
       CAST(d1.ZEBELP as int)=CAST(d2.ZHXM as int)
    left join 
   (select 
      zzddj
      ,zzckj
      ,EBELN
      ,EBELP
      ,zoanum
      ,ZZEANME
      ,matnr
      ,case when coalesce(zmd,'')='' then werks ELSE zmd end as store_id
    from 
       ods_sap.ekpo
    where 
       inc_day 
    between 
       '$[time(yyyy-MM-dd,-30d)]' and '$[time(yyyy-MM-dd,+10d)]') d3
    on 
        d1.ebeln=d3.ebeln 
    and 
        d1.matnr=d3.matnr
    and 
        d1.new_store_id=d3.store_id
    and 
        cast(d1.zebelp as int )=cast(d3.EBELP as int )
    left join
    (select 
      * 
    from 
       ddl.ddl_store_purchase_info_sap 
    where 
       inc_day 
    between 
       '$[time(yyyy-MM-dd,-20d)]' and '$[time(yyyy-MM-dd,+10d)]') d4
    on 
       d1.ean11=d4.article_id 
    and 
       d1.new_store_id=d4.new_sp_store_id 
    and 
       d1.ZEBELN=d4.ZXQHZ
    left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-10d)]' and '$[time(yyyy-MM-dd,+1d)]' group by inc_day,werks,matnr
    ) d5
    on
        d1.inc_day=d5.inc_day
    and
        d1.new_dc_id=d5.werks
    and
        d1.matnr=d5.matnr
    left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-11d)]' and '$[time(yyyy-MM-dd)]' group by inc_day,werks,matnr
    ) d7
    on
        d1.inc_day=date_add(d7.inc_day,1)
    and
        d1.new_dc_id=d7.werks
    and
        d1.matnr=d7.matnr
       left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-12d)]' and '$[time(yyyy-MM-dd,-1d)]' group by inc_day,werks,matnr
    ) d8
    on
        d1.inc_day=date_add(d8.inc_day,2)
    and
        d1.new_dc_id=d8.werks
    and
        d1.matnr=d8.matnr
           left join 
    (
        select max(zckj) as zckj,inc_day,werks,matnr from ods_sap.zmmt022 where inc_day between '$[time(yyyy-MM-dd,-13d)]' and '$[time(yyyy-MM-dd,-2d)]' group by inc_day,werks,matnr
    ) d9
    on
        d1.inc_day=date_add(d9.inc_day,3)
    and
        d1.new_dc_id=d9.werks
    and
        d1.matnr=d9.matnr;



drop table if exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_03;
create table if not exists tmp_dsl.tmp_dsl_scm_store_purchase_info_di_03
as
select
    mandt --string comment '集团',
	,zebeln --string comment '调拨计划单号',
	,sp_store_id --string comment '店铺id',
	,sp_store_name --string comment '店铺名称',
	,sp_type --string comment '店铺类型 20加盟/10直营',
	,sp_level --string comment '店铺等级 1:实体门店 2:菜吧 3:B端客户 4:虚拟门店 5:测试店',
	,dist_id --string comment '区县id',
	,dist_description --string comment '区县名称',
	,city_id --string comment '城市id',
	,city_description --string comment '城市名称',
	,pro_id --string comment '省份id',
	,pro_description --string comment '省份名称',
	,area_id --string comment '运营区域id',
	,area_description --string comment '运营区域名称',
	,sp_store_status --int COMMENT '店铺状态',
	,group_manager_code --string comment '督导id',
	,group_manager --string comment '督导姓名',
	,new_store_id --string comment '新门店id',
	,dc_id --string comment '仓库编号',
	,dc_name --string comment '仓库名称',
	,dc_type --string comment '物流中心类型',
	,dc_status --string comment '物流中心状态',
	,dc_level --string comment '仓库类别',
	,dc_level_name --string comment '仓库类别名称 虚拟 80  直采 70 综合 60 电商 50  菜吧 40 水产 30 猪肉 20 蔬果 10 物料 1000',
	,new_dc_id --string comment '新仓库id',
	,article_id --string comment '商品编码',
	,article_name --string comment '商品编码',
	,category_level1_id --string comment '大分类编码',
	,category_level1_description --string comment '大分类描述',
	,category_level2_id --string comment '中分类编码',
	,category_level2_description --string comment '中分类描述',
	,category_level3_id --string comment '小分类编码',
	,category_level3_description --string comment '小分类描述',
	,is_sort --string comment '是否分拣',
	,zpslx --string comment '配送类型',
	,zebelp --string comment '采购凭证的项目编号',
	,bsart --string comment '订单类型（采购）',
	,bedat --string comment '采购订单日期',
	,eindt --string comment '项目交货日期',
	,zddrq --string comment '到店日期',
	,ean11 --string comment '国际文件号(EAN/UPC)',
	,menge1 --double COMMENT '数量',
	,meins1 --string comment '基本计量单位',
	,ean12 --string comment '国际文件号(EAN/UPC)',
	,menge2 --double COMMENT '数量',
	,meins2 --string comment '基本计量单位',
	,waers --string comment '货币码',
	,zsfth --string comment '退货项目',
	,zsfzp --string comment '免费项目',
	,lifnr --string comment '供应商或债权人的帐号',
	,zsfbk --string comment '是否爆款',
	,zcllx --string comment '处理类型',
	,ebeln --string comment '采购凭证编号',
	,vbeln --string comment '交货',
	,zedel --string comment '删除标记',
	,zebelpz --string comment '采购凭证的项目编号',
	,ersda --string comment '创建日期',
	,cputm --string comment '创建时间',
	,ernam --string comment '对象创建人姓名',
	,laeda --string comment '上次更改的日期',
	,aetim --string comment '上次修改时间',
	,aenam --string comment '对象更改人的姓名',
	,zzwp --string comment '早晚配',
	,zck --string comment 'Char 20',
	,zean11 --string comment '国际文件号(EAN/UPC)',
	,menge3 --double COMMENT '数量',
	,meins3 --string comment '基本计量单位',
	,zaft_rj --string comment '日结后商品传输标记',
    ,zjsfc --string COMMENT '接收方、发送方',
    ,posnr --string COMMENT '交货项目',
    ,zid --string COMMENT '消息ID',
    ,zitem --string COMMENT '接口消息行',
    ,zzstatus --string COMMENT '订购计划接口子状态',
    ,zifid --string COMMENT '接口ID',
    ,zsender --string COMMENT '发送方',
    ,zreceiver --string COMMENT '接受方',
    ,zsrusr --string COMMENT '接受/发送人',
    ,zsrdat --string COMMENT '发送/接收日期',
    ,zsrtim --string COMMENT '发送/接收时间',
    ,zdusr --string COMMENT '接口处理人',
    ,zddat --string COMMENT '接口处理日期',
    ,zdtim --string COMMENT '接口处理时间',
    ,zzmsg --string COMMENT '消息文本',
    ,zstatus --string COMMENT '接口状态',
    ,zxqhz --string COMMENT '需求汇总号',
    ,zjhdh --string COMMENT '计划单号',
    ,zzitem --string COMMENT '计划单号行项目',
    ,datum --string COMMENT '订单创建日期',
    ,zddcjsj --string COMMENT '订单创建时间',
    ,zdgrq --string COMMENT '订购日期',
    ,zddlx --string COMMENT '订单类型',
    ,zdgll --string COMMENT '订购来源',
    ,zsgsl --double COMMENT '订购数量',
    ,zzjbs --string COMMENT '照旧标识',
    ,zwerks --string COMMENT '照旧门店',
    ,zzjrq --string COMMENT '照旧日期',
    ,zbz --string COMMENT '备注',
    ,zdel --string COMMENT '删除标记',
    ,zeandw --string COMMENT 'EAN单位',
    ,meins --string COMMENT '基本计量单位',
    ,zdgjs --double COMMENT '订购基数',
    ,zzxsl --double COMMENT '订购数量',
    ,zdgbs --double COMMENT '倍数',
    ,matnr --string comment '物料号'
    ,is_order --string comment '是否订购商品'
    ,zoanum --string comment 'oa号'
    ,zzddj --string comment '到店价'
    ,zzckj --string comment '出库价'
    ,order_qty_orderean --double comment '订购ean数量'
    ,order_qty_payean --double comment '订购结算ean数量'
    ,order_amt --double comment '订购金额'
    ,out_stock_amt --double COMMENT '出库金额',
    ,out_stock_qty --double COMMENT '出库数量',
    ,store_recive_amt --double COMMENT '门店收货金额',
    ,store_recive_qty_payean --double COMMENT '门店收货数量',
    ,store_recive_qty_orderean --double COMMENT '门店收货数量(订购ean)',
    ,process_matnr --string comment '物料编号',
    ,procerss_ean11 --string comment '国际文件号(EAN/UPC)',
    ,order_percent   
    ,procerss_qty --double COMMENT '数量',
    ,expect_weight--double comment '理论收货重量'
    ,expect_order_weight --double comment '理论订购重量'
    ,is_satisfied_below85
	,out_stock_qty_kg2fen
    ,menge1_recive
    ,menge2_recive
	,t.select_unit as select_unit  --wms分拣单位
	,t.purchase_qty as purchase_qty  --wms订购数量
	,t.purchase_weight as purchase_weight  --wms订购重量
	,t.select_qty as select_qty  --wms分拣数量
	,t.select_weight as select_weight  --wms分拣重量
	,t.weightflag as weightflag   --是否称重商品(1-称重 0-非称重)
	,t.ek_zzxsfs as ek_zzxsfs
	,t.ek_zzcgms as 	ek_zzcgms
	,t.select_af_qty as select_af_qty
	,t.select_af_weight as select_af_weight
    ,inc_day
from(
select * from tmp_dsl.tmp_dsl_scm_store_purchase_info_di_200

union all

select * from tmp_dsl.tmp_dsl_scm_store_purchase_info_di_201

union all

select * from tmp_dsl.tmp_dsl_scm_store_purchase_info_di_202

) t;

set hive.execution.engine=mr;


insert overwrite table dsl.dsl_scm_store_purchase_info_di partition(inc_day)
select
    mandt --string comment '集团',
	,zebeln --string comment '调拨计划单号',
	,sp_store_id --string comment '店铺id',
	,sp_store_name --string comment '店铺名称',
	,sp_type --string comment '店铺类型 20加盟/10直营',
	,sp_level --string comment '店铺等级 1:实体门店 2:菜吧 3:B端客户 4:虚拟门店 5:测试店',
	,dist_id --string comment '区县id',
	,dist_description --string comment '区县名称',
	,city_id --string comment '城市id',
	,city_description --string comment '城市名称',
	,pro_id --string comment '省份id',
	,pro_description --string comment '省份名称',
	,area_id --string comment '运营区域id',
	,area_description --string comment '运营区域名称',
	,sp_store_status --int COMMENT '店铺状态',
	,group_manager_code --string comment '督导id',
	,group_manager --string comment '督导姓名',
	,new_store_id --string comment '新门店id',
	,dc_id --string comment '仓库编号',
	,dc_name --string comment '仓库名称',
	,dc_type --string comment '物流中心类型',
	,dc_status --string comment '物流中心状态',
	,dc_level --string comment '仓库类别',
	,dc_level_name --string comment '仓库类别名称 虚拟 80  直采 70 综合 60 电商 50  菜吧 40 水产 30 猪肉 20 蔬果 10 物料 1000',
	,new_dc_id --string comment '新仓库id',
	,article_id --string comment '商品编码',
	,article_name --string comment '商品编码',
	,category_level1_id --string comment '大分类编码',
	,category_level1_description --string comment '大分类描述',
	,category_level2_id --string comment '中分类编码',
	,category_level2_description --string comment '中分类描述',
	,category_level3_id --string comment '小分类编码',
	,category_level3_description --string comment '小分类描述',
	,is_sort --string comment '是否分拣',
	,zpslx --string comment '配送类型',
	,zebelp --string comment '采购凭证的项目编号',
	,bsart --string comment '订单类型（采购）',
	,bedat --string comment '采购订单日期',
	,eindt --string comment '项目交货日期',
	,zddrq --string comment '到店日期',
	,ean11 --string comment '国际文件号(EAN/UPC)',
	,menge1 --double COMMENT '数量',
	,meins1 --string comment '基本计量单位',
	,ean12 --string comment '国际文件号(EAN/UPC)',
	,menge2 --double COMMENT '数量',
	,meins2 --string comment '基本计量单位',
	,waers --string comment '货币码',
	,zsfth --string comment '退货项目',
	,zsfzp --string comment '免费项目',
	,lifnr --string comment '供应商或债权人的帐号',
	,zsfbk --string comment '是否爆款',
	,zcllx --string comment '处理类型',
	,ebeln --string comment '采购凭证编号',
	,vbeln --string comment '交货',
	,zedel --string comment '删除标记',
	,zebelpz --string comment '采购凭证的项目编号',
	,ersda --string comment '创建日期',
	,cputm --string comment '创建时间',
	,ernam --string comment '对象创建人姓名',
	,laeda --string comment '上次更改的日期',
	,aetim --string comment '上次修改时间',
	,aenam --string comment '对象更改人的姓名',
	,zzwp --string comment '早晚配',
	,zck --string comment 'Char 20',
	,zean11 --string comment '国际文件号(EAN/UPC)',
	,menge3 --double COMMENT '数量',
	,meins3 --string comment '基本计量单位',
	,zaft_rj --string comment '日结后商品传输标记',
    ,zjsfc --string COMMENT '接收方、发送方',
    ,posnr --string COMMENT '交货项目',
    ,zid --string COMMENT '消息ID',
    ,zitem --string COMMENT '接口消息行',
    ,zzstatus --string COMMENT '订购计划接口子状态',
    ,zifid --string COMMENT '接口ID',
    ,zsender --string COMMENT '发送方',
    ,zreceiver --string COMMENT '接受方',
    ,zsrusr --string COMMENT '接受/发送人',
    ,zsrdat --string COMMENT '发送/接收日期',
    ,zsrtim --string COMMENT '发送/接收时间',
    ,zdusr --string COMMENT '接口处理人',
    ,zddat --string COMMENT '接口处理日期',
    ,zdtim --string COMMENT '接口处理时间',
    ,zzmsg --string COMMENT '消息文本',
    ,zstatus --string COMMENT '接口状态',
    ,zxqhz --string COMMENT '需求汇总号',
    ,zjhdh --string COMMENT '计划单号',
    ,zzitem --string COMMENT '计划单号行项目',
    ,datum --string COMMENT '订单创建日期',
    ,zddcjsj --string COMMENT '订单创建时间',
    ,zdgrq --string COMMENT '订购日期',
    ,zddlx --string COMMENT '订单类型',
    ,zdgll --string COMMENT '订购来源',
    ,zsgsl --double COMMENT '订购数量',
    ,zzjbs --string COMMENT '照旧标识',
    ,zwerks --string COMMENT '照旧门店',
    ,zzjrq --string COMMENT '照旧日期',
    ,zbz --string COMMENT '备注',
    ,zdel --string COMMENT '删除标记',
    ,zeandw --string COMMENT 'EAN单位',
    ,meins --string COMMENT '基本计量单位',
    ,zdgjs --double COMMENT '订购基数',
    ,zzxsl --double COMMENT '订购数量',
    ,zdgbs --double COMMENT '倍数',
    ,matnr --string comment '物料号'
    ,is_order --string comment '是否订购商品'
    ,zoanum --string comment 'oa号'
    ,zzddj --string comment '到店价'
    ,zzckj --string comment '出库价'
    ,order_qty_orderean --double comment '订购ean数量'
    ,order_qty_payean --double comment '订购结算ean数量'
    ,order_amt --double comment '订购金额'
    ,out_stock_amt --double COMMENT '出库金额',
    ,out_stock_qty --double COMMENT '出库数量',
    ,store_recive_amt --double COMMENT '门店收货金额',
    ,store_recive_qty_payean --double COMMENT '门店收货数量',
    ,store_recive_qty_orderean --double COMMENT '门店收货数量(订购ean)',
    ,process_matnr --string comment '物料编号',
    ,procerss_ean11 --string comment '国际文件号(EAN/UPC)',
    ,procerss_qty --double COMMENT '数量',
    ,order_percent 
    ,expect_weight--double comment '理论收货重量' 
    ,expect_order_weight --double comment '理论订购重量'
    ,case when process_matnr<>'' and process_matnr<>matnr then '1' else '0' end as is_bom_false --实际没订购的bom 1 没订购 0 订购
    ,is_satisfied_below85 
	  ,out_stock_qty_kg2fen
    ,menge1_recive --double comment 'bom收货量 订购'
    ,menge2_recive --double comment 'bom收货量 结算'
	,select_unit  --wms分拣单位
	,purchase_qty  --wms订购数量
	,purchase_weight  --wms订购重量
	,select_qty  --wms分拣数量
	,select_weight  --wms分拣重量
	,weightflag   --是否称重商品(1-称重 0-非称重)
	,ek_zzxsfs
	,ek_zzcgms
	,select_af_qty as select_af_qty
	,select_af_weight as select_af_weight
    ,inc_day
    from tmp_dsl.tmp_dsl_scm_store_purchase_info_di_03;