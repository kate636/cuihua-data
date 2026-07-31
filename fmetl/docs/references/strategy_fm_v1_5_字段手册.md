# strategy_fm v1.5 字段与表结构手册

> 版本：v1.5 / 2026-07-21
>
> 依据：`翠花数据诊断/huajia_yonghong_etl/versions/v1_5/sync_strategy_fm.sh`、当前仓库
> `fmetl/mirror/registry.py`、相邻项目保存的 Hive 建表元数据、v1.5 的
> `step1_flag_sku_di.sql` 与 `step2_cust.sql`，以及 2026-07-22 通过 QDM API 对 28 张
> StarRocks 镜像表逐表执行的实时查询。
>
> 数据边界：字段名、实时列数、最新分区和行数已通过 StarRocks 只读 API 核对；QDM API
> 不返回 `DESC/SHOW FULL COLUMNS` 的类型和 COMMENT，因此字段类型/注释仍以 Hive DDL 或
> 公司 IDE 的 schema 查询为准。StarRocks 镜像表是 `SELECT *` 同步，正式变更前仍需执行
> 文末 schema 校验 SQL。
>
> 完整实时列名集合见：[2026-07-22 实时列名快照](strategy_fm_v1_5_实时列名快照_2026-07-22.md)。

## 1. 先看结论

### 1.1 v1.5 同步脚本实际涉及 28 张镜像表

脚本中的日志序号仍写着 `1/21`、`22/31` 等历史编号，不能用来判断表总数。按实际
`DELETE/INSERT` 目标表统计，当前是 28 张：

- 10 张业务事实：销售、进货、SCM、SCM 调整、损耗、加工、让利、促销、成本价、价格。
- 9 张维度/关系：日清、门店画像、可订可售、商品主数据、BOM 收货销售、订验关系、父子比例辅助、BOM 边、库存明细。
- 6 张 v1.1/v1.5 辅助事实：全链路商品、门店日销售、SKU 销售件数、翠花商品经营、线下订单、线上订单。
- 2 张公共维度/用户辅助：日历、支付用户映射。
- 1 张采购下单临时表：`strategy_fm_purchase_order_tmp`。

### 1.2 两张“可订”相关表不能混用

| 表 | 同步来源 | v1.5 用途 |
|---|---|---|
| `strategy_fm_purchase_order_tmp` | 同步脚本第 14 步从 `hive.ods_sc_db.t_purchase_order_item_tmp` 写入 | 采购下单快照，包含批量、建议量、采购标记等字段，不是商品可售状态表 |
| `strategy_fm_dim_order_saleable` | 同步脚本第 26 步从 `hive.dim.dim_store_article_order_sale_info_di` 写入 | 门店×商品的可订可售维度；`step1_flag_sku_di.sql` 直接 JOIN 并读取 `saleable` |

当前同步脚本会分别维护两张表，没有表名缺口。判断 SKU 是否可售时，以
`strategy_fm_dim_order_saleable.saleable` 为准；是否可订读取同表的 `is_order`。采购临时表
只能说明下单参数和采购状态，不能替代可订可售维度。

### 1.3 v1.5 新增的用户链路

```text
strategy_fm_trade_user
        │  当日订单 → thirdparty_user_identity
        ▼
strategy_fm_user_first_order（v1.5 ETL 运行时生成，非本同步脚本目标）
        │
        ├── strategy_fm_order_offline_di.thirdparty_user_identity
        └── strategy_fm_order_online_di.thirdparty_user_identity
```

`thirdparty_user_identity` 是跨渠道识别用户的业务身份，不等同于手机号，也不等同于订单号。

### 1.4 2026-07-22 实时核验快照

以下是通过 QDM API 对镜像表执行聚合查询得到的结果。行数是整张 StarRocks 镜像表行数，
不是只筛选 `A3XV` 后的行数；`max_inc_day` 是表内最大增量日。

| 表 | 实时列数 | 行数 | max_inc_day |
|---|---:|---:|---|
| `strategy_fm_sales_di` | 119 | 558,480 | 2026-07-21 |
| `strategy_fm_purchase_di` | 16 | 496,564 | 2026-07-21 |
| `strategy_fm_scm_di` | 54 | 96,415 | 2026-07-21 |
| `strategy_fm_scm_adjust_di` | 未能从空结果返回 | 0 | 2026-07-21 |
| `strategy_fm_loss_di` | 10 | 281,331 | 2026-07-21 |
| `strategy_fm_compose_di` | 11 | 9,051 | 2026-07-21 |
| `strategy_fm_allowance_di` | 76 | 324,786 | 2026-07-21 |
| `strategy_fm_promo_di` | 90 | 572,754 | 2026-07-21 |
| `strategy_fm_inventory_pool_di` | 20 | 478,657 | 2026-07-21 |
| `strategy_fm_price_da` | 54 | 1,598,680 | 2026-07-21 |
| `strategy_fm_dim_day_clear` | 11 | 15,107,655 | 2026-07-21 |
| `strategy_fm_dim_store_profile` | 109 | 1 | 2026-07-21 |
| `strategy_fm_purchase_order_tmp` | 52 | 1,817 | 2026-07-21 |
| `strategy_fm_dim_goods` | 106 | 96,319 | 2026-07-21 |
| `strategy_fm_receive_sale_di` | 20 | 106,498 | 2026-07-21 |
| `strategy_fm_order_receive_di` | 17 | 17,919 | 2025-11-17 |
| `strategy_fm_dim_article_convert` | 9 | 120,022 | 2026-07-21 |
| `strategy_dim_store_article_bom_relation` | 17 | 773,930 | 2026-07-21 |
| `strategy_fm_store_article_inventory_detail_di` | 21 | 480,913 | 2026-07-21 |
| `strategy_fm_full_link_article_di` | 202 | 519,119 | 2026-07-21 |
| `strategy_fm_store_daily_di` | 75 | 310 | 2026-07-21 |
| `strategy_fm_article_sale_di` | 92 | 515,558 | 2026-07-21 |
| `strategy_fm_chdj_article_di` | 75 | 517,773 | 2026-07-21 |
| `strategy_fm_dim_order_saleable` | 26 | 1,012,139 | 2026-07-21 |
| `strategy_fm_order_offline_di` | 111 | 522,926 | 2026-07-21 |
| `strategy_fm_order_online_di` | 117 | 35,570 | 2026-07-21 |
| `strategy_fm_trade_user` | 4 | 211,766 | 2026-07-21 |
| `strategy_fm_dim_calendar` | 70 | 8,712 | 2013-01-01~2036-11-07 |

实时列名已经逐表取代表性记录核对。空表 `strategy_fm_scm_adjust_di` 无法通过数据行返回列名，
其 10 列结构仍以已保存 DDL 为准。

## 2. 共用字段语义

| 字段 | 含义 | 使用规则 |
|---|---|---|
| `business_date` | 业务发生/营业日期 | 销售、库存、SCM 等业务分析主日期；不要用入库日期替代业务日期 |
| `inc_day` | 数据增量/分区日期 | 同步脚本按此字段删除和写入；维度快照也可能只保留最新分区 |
| `store_id` | 门店编码 | 主要事实表使用；当前范围是 `A3XV` |
| `shop_id` | 门店编码的另一套命名 | 促销、成本价、价格、库存明细、采购临时表使用；进入 ETL 通常映射为 `store_id` |
| `article_id` | 商品/SKU 编码 | 通常是销售分析主键；BOM 表中还可能指 parent |
| `abi_article_id` | 销售订单中的 ABI 商品编码 | 映射为下游 `article_id` |
| `sku_code` | 商品编码的另一套命名 | 促销、成本价、价格、订单可售、库存明细使用；通常映射为 `article_id` |
| `article_id` / `sale_article_id` | 验收商品 / 销售商品 | `article_id != sale_article_id` 表示存在 BOM/拆分关系 |
| `order_article_id` / `receive_article_id` | 订购商品 / 验收商品 | 订验桥表和让利表使用，不应直接当作销售 SKU |
| `day_clear` | 日清标识 | `0` 日清，`1` 非日清，`2` 通常表示合计层；源事实表不应随意把空值当 `1` |
| `qty` | 销售/订购单位数量 | 可能是件、盒、份，不一定是 kg |
| `qty_spec` | 规格归一化数量 | 销售成本和重量分析常用；通常由 `qty × spec_num` 得到 |
| `*_qty` | 数量 | 先确认单位，再做跨表相加；BOM parent 单位和 sub 单位不能混用 |
| `*_amt` | 金额 | 默认人民币金额；含税/不含税必须看字段后缀或字段名 |
| `*_price` | 单价/价格 | 需要配套确认计量单位；不可直接跨表比较 |
| `p_*` / `f_*` | 商品本金 / 运费 | `p` 是 principal，`f` 是 freight；销售额口径通常用商品金额，不要把运费重复计入 |
| `original_*` | 原价/原价出库 | 与促销、赠品出库分开 |
| `promotion_*` | 促销出库或促销金额 | 需区分销售促销、供应链让利、优惠券和成本中心 |
| `return_*` | 退货/退仓 | 很多源表已带正负号；不要全表统一翻转符号 |
| `init_stock_*` / `end_stock_*` | 期初/期末库存 | 期末库存是余额，不是当日流量 |
| `created_by` | 记录创建人 | 库存明细中用于区分系统快照和人工盘点证据 |

## 3. 同步目标、源表、粒度和 ETL 用途

| # | StarRocks 镜像表 | Hive 源表 | 主要粒度 | 同步方式 | 当前用途 |
|---:|---|---|---|---|---|
| 1 | `strategy_fm_sales_di` | `hive.dsl.dsl_transaction_non_daily_store_order_details_di` | 订单行×商品×日清 | 按门店/分区替换 | 销售数量、销售额、客数基础 |
| 2 | `strategy_fm_purchase_di` | `hive.dsl.dsl_transaction_non_daily_store_article_purchase_di` | 门店×日×验收品×销售品×日清 | 按门店/分区替换 | 期初期末库存、预拆进货 |
| 3 | `strategy_fm_scm_di` | `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di` | 门店×日×商品 | 按门店/分区替换 | 出库、退仓、采购价、SCM让利 |
| 4 | `strategy_fm_scm_adjust_di` | `hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di` | 门店×日×仓×物料×商品 | 按门店/分区替换 | 差异调整审计，当前常为空 |
| 5 | `strategy_fm_loss_di` | `hive.dal.dal_transaction_store_article_lost_di` | 门店×日×商品 | 按门店/分区替换 | 已知/未知损耗观测 |
| 6 | `strategy_fm_compose_di` | `hive.dsl.dsl_transaction_sotre_article_compose_info_di` | 门店×日×商品 | 按门店/分区替换 | 加工进出数量金额观测 |
| 7 | `strategy_fm_allowance_di` | `hive.dal.dal_activity_article_order_sale_info_di` | 门店×日×订购商品/活动 | 按门店/分区替换 | 补贴及让利输出 |
| 8 | `strategy_fm_promo_di` | `hive.dsl.dsl_promotion_order_item_article_sale_info_di` | 订单×门店×SKU×促销 | 按门店/分区替换 | 促销费用拆解 |
| 9 | `strategy_fm_inventory_pool_di` | `hive.ods_sc_db.t_shop_inventory_sku_pool` | 门店×SKU×库存日期 | 门店全量快照替换 | 成本价观测 |
| 10 | `strategy_fm_price_da` | `hive.dim.dim_store_article_price_info_da` | 价格观测行（实时镜像可重复） | 按门店/分区替换 | 售价、出库价审计观测 |
| 11 | `strategy_fm_dim_day_clear` | `hive.dim.dim_day_clear_article_list_di` | 门店×业务日×商品 | 按门店/分区替换 | 日清标签输入 |
| 12 | `strategy_fm_dim_store_profile` | `hive.dim.dim_store_profile` | 门店×快照日 | 门店全量替换 | 门店、区域、城市属性 |
| 13 | `strategy_fm_purchase_order_tmp` | `hive.ods_sc_db.t_purchase_order_item_tmp` | 门店×SKU快照 | 门店全量替换 | 采购下单参数和结果快照 |
| 14 | `strategy_fm_dim_goods` | `hive.dim.dim_goods_information_have_pt` | 商品×快照日 | 按日期清理后写入 | 品类、物料码、SPU分析维度、单位和商品主数据 |
| 15 | `strategy_fm_receive_sale_di` | `hive.dal.dal_receive_sale_di` | 门店×日×parent×sub | 按门店/分区替换 | BOM验收销售桥、进货主源 |
| 16 | `strategy_fm_order_receive_di` | `hive.dal.dal_store_order_receive_di` | 门店×日×订购品×验收品 | 按门店/分区替换 | 订验桥，当前可能无数据 |
| 17 | `strategy_fm_dim_article_convert` | `hive.dim.dim_store_article_convert_info_da` | 门店×parent×sub | 按门店/分区替换 | 父子方向与单位比例辅助证据 |
| 18 | `strategy_dim_store_article_bom_relation` | `hive.dim.dim_store_article_bom_relation` | 门店×日×parent×sub | 按门店/分区替换 | BOM关系边 |
| 19 | `strategy_fm_store_article_inventory_detail_di` | `hive.ddl.ddl_transaction_store_article_inventory_detail_di` | 门店×库存日×SKU | 按门店/分区替换 | 系统库存与人工盘点快照 |
| 20 | `strategy_fm_full_link_article_di` | `hive.dal_full_link.dal_manage_full_link_store_dc_article_info_di` | 门店×日×商品 | 按门店/分区替换 | v1.5 商品维度底表主事实 |
| 21 | `strategy_fm_store_daily_di` | `hive.dal.dal_transaction_sale_store_daily_di` | 门店×日 | 按门店/分区替换 | 有效营业日筛选 |
| 22 | `strategy_fm_article_sale_di` | `hive.dal.dal_transaction_store_article_sale_info_di` | 门店×日×商品 | 按门店/分区替换 | 销售件数、19点前件数 |
| 23 | `strategy_fm_chdj_article_di` | `hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di` | 门店×日×商品×日清 | 按门店/分区替换 | 翠花口径利润、库存、日清 |
| 24 | `strategy_fm_dim_order_saleable` | `hive.dim.dim_store_article_order_sale_info_di` | 门店×日×商品 | 同步脚本第 26 步按门店/分区替换 | v1.5 `saleable`/`is_order` 权威输入 |
| 25 | `strategy_fm_dim_calendar` | `hive.dim.dim_calendar` | 日期 | 仅首次全量导入 | 周、月、年维度 |
| 26 | `strategy_fm_order_offline_di` | `hive.dsl.dsl_transaction_sotre_order_offline_details_di` + `hive.ods_pay_db.t_trade` | 线下订单行 | 按门店/分区替换 | 客数、渠道、新老客 |
| 27 | `strategy_fm_order_online_di` | `hive.dsl.dsl_transaction_sotre_order_online_details_di` + `hive.ods_pay_db.t_trade` | 线上订单行 | 按门店/分区替换 | 客数、渠道、新老客 |
| 28 | `strategy_fm_trade_user` | `hive.ods_pay_db.t_trade` | 订单×支付用户 | 按分区替换 | `thirdparty_user_identity` 映射 |

## 4. 核心事实表字段手册

### 4.1 销售事实 `strategy_fm_sales_di`（源字段 119 列）

源：`hive.dsl.dsl_transaction_non_daily_store_order_details_di`。粒度是订单商品行，不能在
读取层直接按 SKU 假定一行。下游销售聚合使用 `SUM(qty_spec)`、`SUM(qty)`、`SUM(sales_amt)`。

| 字段组 | 字段 | 含义 |
|---|---|---|
| 日期/门店 | `business_date`, `store_id`, `sp_store_name`, `area_description`, `area_id`, `sp_type`, `sp_level`, `inc_day` | 业务日期、门店、区域、门店类型/等级、增量分区 |
| 订单主键 | `order_id`, `parent_order_id`, `children_order_ids`, `root_order_id`, `serial_id`, `delivery_id`, `outer_order_id`, `outer_order_type`, `order_status`, `order_type`, `order_sub_type`, `channel_id` | 订单、拆单、配送、外部订单和渠道属性 |
| 订单备注/状态 | `tenant_id`, `message`, `internal_comment`, `reason`, `comment_id`, `comment_time`, `sibling_order_ids`, `split_supported`, `afs_order_id`, `payment_type`, `bundle_promo_code`, `sync_seq`, `sync_flag` | 租户、备注、评论、售后、支付、促销关联、同步状态 |
| 商品 | `abi_article_id`, `goods_barcode`, `sale_unit`, `spec_num`, `spec_type` | 商品编码、条码、销售单位、规格和规格类型 |
| 会员 | `customer_id`, `customer_name`, `customer_phone`, `first_buy_flag` | 会员/用户身份和首购标记 |
| 时间 | `inc_time`, `pay_at`, `refund_at`, `erp_order_at`, `split_at`, `complete_at`, `order_at`, `allrefund_time`, `je_date`, `je_order_id`, `rje_date`, `rje_order_id` | 日结、支付、退款、拆单、完成、SAP退货时间/单号 |
| 商品价格 | `display_price`, `list_price`, `sale_price` | 展示价、原价、售价 |
| 数量 | `qty`, `qty_spec`, `actual_weight`, `return_sale_qty`, `gift_qty`, `af19_sales_qty` | 销售件数、规格量、实重、退货、赠品、19点后销量 |
| 商品金额 | `sales_amt`, `actual_amount`, `gmv`, `gmv1`, `p_lp_sub_amt`, `p_sp_sub_amt`, `p_mp_sub_amt`, `return_sale_amt` | 日结实收、实付、含/不含运费应付、原价/售价/中台金额、退货额 |
| 支付拆分 | `p_paid_sub_amt`, `f_paid_sub_amt`, `p_pay_sub_amt`, `f_pay_sub_amt`, `f_sub_amt`, `p_promo_sub_amt`, `f_promo_sub_amt`, `p_pointpay_sub_amt`, `f_pointpay_sub_amt`, `p_balancepay_sub_amt`, `f_balancepay_sub_amt`, `p_cashpay_sub_amt`, `f_cashpay_sub_amt`, `p_change_sub_amt` | 商品本金和运费的实付、应付、促销、积分、余额、现金及抹零 |
| 折扣/促销 | `discount_amt`, `vip_discount_amt`, `hour_discount_amt`, `member_hour_sales_amt`, `shop_promo_sub_amt`, `promotion_amt`, `promotion_cost`, `promotion_amt_shop`, `promotion_amt_platform`, `i_promotion_amt`, `order_promotion_amt`, `ordercoupon_promotion_amt`, `store_paylevel_discount`, `company_paylevel_discount` | 折扣、会员/时段/单品/订单/券促销及承担方金额 |
| 分时段/渠道 | `af19_sales_amt`, `online_flag`, `business_source`, `currency`, `jielong_flag`, `is_hour_promotion` | 19点后金额、线上线下、业务来源、币种、接龙、时段促销 |
| 配送/其他 | `logistics_status`, `courier_company`, `courier_name`, `courier_name_reverse`, `courier_phone`, `postage_shop`, `postage_platform`, `gift_gmv`, `promotion_amt_platform_gs`, `promotion_amt_platform_gys`, `activity_code`, `is_promotion_article` | 物流、邮费、赠品GMV、平台/公司/供应商承担和活动编号 |
| 日清 | `day_clear` | 源销售行的日清标签；不要与 v1.5 `chdj_article` 的日清标签混淆 |

**下游锚点**：`abi_article_id → article_id`，`qty_spec → sale_qty`，`sales_amt → sale_amt`。
退货字段保留源符号，不能在 ETL 再统一取负。

### 4.2 进货验收 `strategy_fm_purchase_di`（16 列）

| 字段 | 含义 |
|---|---|
| `business_date`, `inc_day`, `store_id`, `day_clear` | 业务日期、分区、门店、日清 |
| `article_id`, `article_name` | 验收条码(parent)及名称 |
| `sale_article_id`, `sale_article_name` | 销售条码(sub)及名称 |
| `sale_article_qty`, `sale_article_purchase_amt` | 拆分后销售品数量和进货额 |
| `init_stock_qty`, `init_stock_amt` | 期初库存数量和金额 |
| `end_stock_qty`, `end_stock_amt` | 上游期末库存数量和金额 |
| `inventory_cost` | 库存成本单价，观测字段 |
| `avg_inbound_price` | parent 进货平均价，EUC 兜底参考 |

`article_id = sale_article_id` 通常是标品；不相等表示预拆 BOM 行。`sale_article_qty` 是
销售品单位，不能拿去代替 parent 单位的 BOM 数量。

### 4.3 SCM 出入库 `strategy_fm_scm_di`（54 列）

| 字段组 | 字段 | 含义 |
|---|---|---|
| 主键/维度 | `business_date`, `store_id`, `article_id`, `new_dc_id`, `inc_day` | 业务日、门店、商品、配送中心、分区 |
| 出库数量/金额 | `original_outstock_qty`, `original_outstock_amt`, `promotion_outstock_qty`, `promotion_outstock_amt`, `promotion_outstock_price`, `gift_outstock_qty`, `gift_outstock_amt`, `total_outstock_qty`, `out_stock_pay_amt`, `out_stock_pay_amt_notax`, `out_stock_amt_cb`, `out_stock_amt_cb_notax`, `out_stock_zzckj_amt` | 常规/促销/赠品出库及含税、不含税应付/成本金额 |
| 退仓 | `return_stock_qty`, `return_stock_pay_amt`, `return_stock_pay_amt_notax`, `return_stock_amt_cb`, `return_stock_amt_cb_notax`, `return_stock_original_amt`, `store_return_qty_shop`, `store_return_amt_shop` | 退仓数量、应付、成本和中台退货 |
| 订货 | `store_order_qty`, `order_qty_payean`, `order_amt` | 订货单位数量、结算单位数量、订货额 |
| 调整 | `adjustment_amt`, `adjustment_amt_notax` | 含税/不含税差异调整 |
| SCM让利 | `scm_promotion_amt`, `scm_promotion_amt_gift`, `scm_promotion_qty_gift`, `scm_promotion_cost`, `scm_return_promotion_cost`, `scm_promotion_amt_total` | 非赠品、赠品、退仓和总供应链让利 |
| 承担方 | `scm_bear_amt`, `vendor_bear_amt`, `business_bear_amt`, `business_market_bear_amt`, `market_bear_amt`, `vender_bear_gift_amt`, `scm_bear_gift_amt`, `vender_bear_gift_qty`, `scm_bear_gift_qty` | 供应链、供应商、运营、市场承担金额/数量 |
| QDM承担 | `qdm_bear_negative_amt_total`, `qdm_bear_positive_amt_total`, `qdm_bear_gift_qty`, `qdm_bear_gift_amt`, `qdm_bear_nogift_negative_amt`, `qdm_bear_nogift_positive_amt`, `qdm_bear_promotion_fee` | 公司承担的正负让利、赠品和促销费用 |
| 少货 | `miss_stock_qty`, `miss_stock_amt` | 到货不足数量和金额 |

当前 ETL 主要把 SCM 作为采购价/供应链指标观测，不能把 `out_stock_amt_cb` 再重复记为门店
`receive`。

### 4.4 损耗 `strategy_fm_loss_di`（10 列）

`store_id`, `article_id`, `article_name`, `category_level1_id`, `category_level1_description`,
`know_lost_qty`, `know_lost_amt`, `unknow_lost_qty`, `unknow_lost_amt`, `inc_day`。

`know_lost_*` 是上游登记的已知损耗；`unknow_lost_*` 是上游口径的未知损耗，仅作审计参考。
当前 FM 以自己的库存方程反推未知损耗，不直接覆盖计算层结果。

### 4.5 加工 `strategy_fm_compose_di`（11 列）

`business_date`, `store_id`, `store_name`, `article_id`, `article_name`, `compose_in_qty`,
`compose_in_amt`, `compose_out_qty`, `compose_out_amt`, `update_time`, `inc_day`。

`compose_in` 表示加工流入原料，`compose_out` 表示加工流出/产出；金额是源表观测，不能默认
等同于 v1.5 配方计算的金额。

### 4.6 活动让利 `strategy_fm_allowance_di`（76 列）

| 字段组 | 字段 |
|---|---|
| 维度/活动 | `business_date`, `store_id`, `order_article_id`, `activity_name`, `activity_type`, `activity_id`, `hot_flag` |
| 订货/验收 | `out_price`, `sale_price`, `order_price`, `order_qty`, `settle_order_qty`, `order_amt`, `split_order_qty`, `split_order_amt`, `receive_article_id`, `receive_amt`, `receive_qty`, `purchase_price` |
| 销售商品 | `sale_article_id`, `sale_article_receive_qty`, `sale_article_receive_amt`, `sale_article_receive_price`, `sum_sub_article_qty`, `split_qty`, `qty`, `qty_spec`, `order_weight`, `spilt_receive_weight`, `sale_article_receive_unit_qty`, `split_receive_unit_qty_spec`, `split_receive_unit_order_qty` |
| 销售/损耗 | `sale_amt`, `split_sale_amt`, `lost_qty`, `lost_amt`, `profit_amt`, `allowance_profit_amt`, `split_discount_amt`, `split_member_discount_amt`, `split_hour_discount_amt`, `split_return_sale_qty`, `split_return_sale_amt`, `split_member_hour_sale_amt` |
| 分时段销售 | `split_af19_sale_amt`, `split_af19_sale_qty`, `split_bf9_sale_qty`, `split_bf10_sale_qty`, `split_bf12_sale_qty`, `split_bf16_sale_qty`, `split_bf19_sale_qty`, `split_bf9_sale_amt`, `split_bf10_sale_amt`, `split_bf12_sale_amt`, `split_bf16_sale_amt`, `split_bf19_sale_amt` |
| 价格/补贴 | `split_p_lp_sub_amt`, `split_p_sp_sub_amt`, `split_promotion_discount_amt`, `split_allowance_amt`, `list_price` |
| 重量/客数 | `split_sale_weight`, `split_bf9_sale_weight`, `split_cust_num`, `split_bf19_cust_num` |
| 库存 | `init_stock_qty`, `end_stock_qty`, `init_stock_amt`, `end_stock_amt`, `init_receiveb_qty`, `init_receiveb_amt`, `end_receiveb_qty`, `end_receiveb_amt` |
| 时间/分区 | `last_pay_at`, `inc_day` |

### 4.7 促销 `strategy_fm_promo_di`（90 列）

关键字段包括：

`order_id`, `order_type`, `order_status`, `customer_id`, `delivery_id`, `customer_name`,
`customer_phone`, `shop_id`, `shop_name`, `parent_order_id`, `product_count`, `channel_id`,
`order_sub_type`, `outer_order_id`, `outer_order_type`, `refund_type`, `je_date`, `rje_date`,
`refund_at`, `pay_at`, `order_at`, `cancel_at`, `business_source`, `row_num`, `goods_id`,
`sku_code`, `goods_name`, `category_id`, `spu_code`, `bundle_promo_code`, `order_item_id`,
`p_promo_amt`, `p_promo_total_amt`, `f_promo_amt`, `f_promo_total_amt`, `promotion_category`,
`promotion_code`, `promotion_code2`, `promo_type`, `promo_sub_type`, `coupon_code`, `from_outer`,
`outer_code`, `parent_order_item_id`, `parent_order_item_promotion_id`, `parent_bom_order_item_id`,
`parent_bom_order_item_promo_id`, `cost_center`, `coupon_mode`, `sales_charge_type`,
`cost_tax_rate`, `allocate_rate`, `promotion_cost`, `promo_action_type`, `purchase_limit_qty`,
`promotion_name`, `activity_type`, `activity_level`, `code2`, `rank`, `promo_type1`,
`promo_sub_type1`, `promo_action`, `eligibility_condition`, `promo_condition_type`,
`promo_condition_context`, `promo_action_context`, `name`, `description`, `tag`, `cost_center_info`,
`available_category`, `allowance`, `only_member`, `title`, `discount`, `created_by`,
`promotion_type`, `category_info`, `source`, `online_flag`, `normal_inc_day`, `inc_time`,
`jielong_flag`, `activity_code`, `promo_ext_prop`, `cost_company`, `cost_subject`,
`is_hour_promotion`, `inc_day`。

`shop_id → store_id`，`sku_code → article_id`。`p_promo_amt` 是商品促销金额，`f_promo_amt`
是运费促销金额；促销规则字段不能与销售事实的折扣金额直接相加。

## 5. BOM、库存和维度表字段手册

### 5.1 BOM 收货销售 `strategy_fm_receive_sale_di`（20 列）

`business_date`, `store_id`, `article_id`, `article_name`, `category_level1_id`,
`category_level1_description`, `inbound_amount`, `inbound_qty`, `purchase_price`,
`sale_article_id`, `sale_article_name`, `sale_article_qty`, `spilit_sale_article_amt`,
`sum_sub_article_qty`, `sale_article_price`, `sum_sale_article_qty`, `rate`, `sum_article_qty`,
`sale_recev_rate`, `inc_day`。

这是 v1.5 计算中 parent×sub 的事实桥。`spilit_sale_article_amt` 的拼写是源表固有字段，不能
自行改成 `split_*` 后再用 `SELECT *` 写入镜像。

### 5.2 BOM 关系边 `strategy_dim_store_article_bom_relation`（17 列）

`store_id`, `category_level3_id`, `category_level3_description`, `category_level2_id`,
`category_level2_description`, `category_level1_id`, `category_level1_description`,
`parent_article_id`, `parent_article_unit`, `sub_article_id`, `sub_article_unit`,
`dressing_rate`, `cost_rate`, `bom_type`, `split_mode`, `sp_level`, `inc_day`。

`dressing_rate` 是出成率，`cost_rate` 是成本分摊比例；当 `cost_rate` 为空或无效时，不能
直接当 0，而应执行设计中约定的反推/兜底规则。`bom_type`、`split_mode`、`sp_level` 是关系
类型，不是数量比例。

### 5.3 父子比例辅助 `strategy_fm_dim_article_convert`（9 列）

`store_id`, `parent_article_id`, `parent_article_name`, `sub_article_id`, `sub_article_name`,
`parent_rate`, `sub_rate`, `ctype`, `inc_day`。

`parent_rate`、`sub_rate` 是父子单位换算系数；`ctype` 通常为 `1=BOM`、`2=业务所称
SPU 转换`、`3=混合`。这里的“SPU 转换”不能直接解释为按 `spu_id` 转库存：实际商品
主数据应先核对 `matnr`，再用本表的精确 parent/sub 行补充方向和比例。

该表不是 BOM、物料码关系之外的第三种业务关系。BOM 表定义一父多子拆分边，商品主数据
的同 `matnr` 定义同物料成员；本表只在存在精确父子行时提供换算证据。没有行不代表没有
BOM 或物料码关系。

### 5.4 订验关系 `strategy_fm_order_receive_di`（17 列）

`business_date`, `store_id`, `order_article_id`, `order_article_name`, `order_category_level1_id`,
`order_category_level1_description`, `re_article_id`, `re_article_name`, `re_category_level1_id`,
`re_category_level1_description`, `order_qty`, `order_amt`, `rate`, `type`, `re_order_qty`,
`re_order_amt`, `inc_day`。

`order_*` 是订货侧，`re_*` 是验收侧；`rate` 是订货到验收的关联比例，不能当作 BOM 出成率。

### 5.5 库存明细 `strategy_fm_store_article_inventory_detail_di`（21 列）

`id`, `shop_id`, `inventory_date`, `sku_code`, `sku_name`, `sub_category_id`, `sub_category_name`,
`spec`, `sales_unit`, `main_img`, `sale_stock_qty`, `stock_cost`, `actual_stock_qty`,
`profit_loss_qty`, `gift_flag`, `created_at`, `created_by`, `updated_at`, `updated_by`,
`last_updated_at`, `inc_day`。

`sale_stock_qty` 是系统库存，`actual_stock_qty` 是实盘数量，`profit_loss_qty = actual - sale`。
`created_by` 是盘点证据强度字段，不是商品主数据的创建人。

### 5.6 商品主数据 `strategy_fm_dim_goods`

源表是商品全量快照，当前镜像保留最新 `inc_day`。关键字段：

| 字段 | 含义 |
|---|---|
| `article_id`, `article_name`, `barcode`, `old_article_id` | 当前编码、名称、条码、旧编码 |
| `category_level1_id/description`, `category_level2_id/description`, `category_level3_id/description` | 三层原始分类 |
| `spu_id`, `spu_name` | 分析用 SPU 编码和名称；可用于候选核对，不能单独作为库存转换键 |
| `sale_unit`, `sales_unit_id`, `order_unit`, `order_unit_id` | 销售/订购单位 |
| `unit_weight`, `weight_flag`, `norms` | 单位重量、称重标记、规格 |
| `matnr`, `article_matnr`, `matnr_unit`, `matnr_unit_id` | SAP物料号和基本单位；v1.5 业务口头所称“SPU关系”应以同 `matnr` 识别 |
| `zglfz`, `zglfm`, `atob_value` | 单位换算分子/分母及 A进B出标识 |
| `brand`, `brand_id`, `product_address` | 品牌及产地 |
| `commodity_attribute`, `package_attribute`, `article_type`, `type` | 商品、包装和商品类型 |
| `status_code`, `status_name`, `life_cycle` | 状态和生命周期 |
| `blackwhite_pig_id`, `blackwhite_pig_name` | 黑面/白面猪标签 |
| `inc_day` | 商品主数据快照日期 |

下游必须把它当最新快照表使用，不要在无历史分区时假设它能还原历史商品属性。

### 5.7 日清 `strategy_fm_dim_day_clear`

2026-07-30 对实时镜像执行零行字段投影确认：该表没有 `day_clear` 列。实际字段为
`business_date`, `store_id`, `article_id`, `article_name`、三级分类字段和 `inc_day`。
它是日清商品清单：命中行表示 `day_clear=0`；未命中不能直接证明非日清，v0.14 先回退到
当日销售事实的 `day_clear` 标签，再默认 `day_clear=1`。不得从 v1.5 商品经营结果表回填。

### 5.8 门店画像 `strategy_fm_dim_store_profile`

源表共 109 个业务列，另含分区字段。v1.5/当前 ETL 的稳定依赖字段是：

`sp_store_id`（门店主键）、`sp_store_name`（门店名称）、`manage_area_name`（管理区域）、
`sap_area_name`（运营区域）、`city_description`（城市）、`inc_day`（快照日）。

其余字段包括门店状态、类型、加盟商、片区、目标销售、坐标、开闭店日期和管理方式；除非
业务明确需要，不建议将整张 109 列表当成销售事实 JOIN。

### 5.9 成本价池 `strategy_fm_inventory_pool_di`（实时 20 列）

完整字段：`main_img`, `updated_by`, `gift_flag`, `last_updated_at`, `inventory_date`, `cost_price`,
`sub_category_id`, `spec`, `sub_category_name`, `sku_name`, `created_at`, `created_by`, `inc_day`,
`sales_unit`, `weight_flag`, `seven_days_avg_sale`, `id`, `shop_id`, `sku_code`, `updated_at`。

`cost_price` 是外部成本价观测，不是当前加权平均 EUC 的唯一来源；`inventory_date` 是库存日期，
`inc_day` 是快照/增量日期，二者不能混用。

### 5.10 价格 `strategy_fm_price_da`

关键字段：`id`, `shop_id`, `sku_code`, `tenant_id`, `shop_name`, `dc_code`, `dc_name`,
`current_price`, `yesterday_price`, `original_price`, `unadjust_sale_price`, `anchor_sale_price`,
`dc_price`, `dc_original_price`, `dc_original_price_sap`, `original_dc_price`,
`yesterday_dc_price`, `outstock_addprice_amt`, `outstock_addprice_rate`, `outstock_profit_rate`,
`outstock_lock_price`, `deal_status`, `calc_status`, `confirm_status`, `sale_status`, `lock_status`,
`push_status`, `effective`, `is_new`, `strategy_no`, `promotion_no`, `calc_strategy`,
`calc_effect_at`, `sales_mode`, `inc_day`。

价格表是 SKU×门店×日期快照。`current_price` 是售价，`dc_price` 是出库价，`original_price`
是销售原价，不能把三者当成同一成本字段。

2026-07-30 对 A3XV 2026-07-17 镜像实查：11,345 行只有 5,673 个不同
`id`/`sku_code`，存在完全重复的价格行。因此不得直接当作“门店×SKU×分区日唯一”
快照 JOIN 或汇总。v0.14 当前仅保留为审计证据；未有确定性有效记录规则前，
报表售价和原价金额使用销售事实已聚合字段，不从本表追加过账。

### 5.11 SCM 差异调整 `strategy_fm_scm_adjust_di`

完整字段：`business_date`, `store_id`, `dc_id`, `matnr`, `article_id`, `tax`, `adjustment_amt`,
`adjustment_amt_notax`, `new_sp_store_id`, `inc_day`。

`adjustment_amt` 是含税差异调整，`adjustment_amt_notax` 是不含税差异调整；`matnr` 是 SAP
物料号，`new_sp_store_id` 是新门店编码。该表当前可能为空，空表不代表字段契约不存在。

### 5.12 采购下单临时表 `strategy_fm_purchase_order_tmp`（实时 52 列）

源表 `hive.ods_sc_db.t_purchase_order_item_tmp` 的已知字段为：

实时 52 列为：`purchase_flag`, `last_updated_at`, `max_batch_qty`, `basic_qty`, `core_flag`,
`purchase_price`, `sku_created_at`, `spec_update_date`, `created_at`, `sell_unit`, `article_spec`,
`order_qty`, `seven_days_avg_sale`, `id`, `shop_id`, `updated_at`, `updated_by`, `unit_convert_deno`,
`sub_category_code`, `order_at`, `seven_days_order_qty`, `is_special_price`, `inc_day`, `sku_code`,
`main_img`, `allow_single_order`, `old_spec`, `min_batch_qty`, `shop_name`, `order_weight`,
`category_name`, `mid_category_code`, `sub_category_name`, `tips_flag`, `sku_name`, `only_morning`,
`unit_convert_mole`, `mid_category_name`, `order_amount`, `seven_days_order_count`, `is_deleted`,
`spu_code`, `conver_rate`, `propose_desc`, `sap_order_qty`, `sku_tag`, `category_code`, `created_by`,
`receive_sku_code`, `tenant_id`, `propose_qty`, `pur_unit`。

关键语义：`purchase_flag` 是采购标记，`allow_single_order` 是是否允许单订，`basic_qty`、
`min_batch_qty`、`max_batch_qty` 是订货数量参数，`propose_qty` 是建议订货量，`order_qty`、
`order_amount`、`order_weight` 是实际订货结果。`sku_code → article_id`，`shop_id → store_id`。
此表和 `strategy_fm_dim_order_saleable` 不能因为都含“可订”语义就视为同一结构。前者描述
采购下单过程，后者描述商品是否可订、是否可售；判断销售 SKU 必须查后者。

## 6. v1.5 新增镜像表字段手册

### 6.1 全链路商品 `strategy_fm_full_link_article_di`（实时 202 列）

源表是门店×仓×商品全链路宽表，包含以下字段组：

| 字段组 | 主要字段 | 含义 |
|---|---|---|
| 日期/组织 | `business_date`, `operate_id`, `operate_name`, `area_id`, `area_name`, `dc_id`, `dc_name`, `store_id`, `store_name`, `inc_day` | 日期、运营区域、大区、仓、门店 |
| 商品/分类 | `article_id`, `article_name`, `first_category_id`, `first_category_name`, `second_category_id`, `second_category_name`, `third_category_id`, `third_category_name`, `matnr` | 商品、三层分类和物料号 |
| 价格策略 | `strategy_no`, `strategy_name`, `category_id`, `category_name`, `price_level`, `price_zone`, `current_price`, `dc_price`, `original_price`, `dc_original_price` | 定价、出库价和原价 |
| 销售 | `sale_originalprice_qty`, `sale_originalprice_amt`, `bf19_sale_qty`, `bf19_sale_amt`, `af19_sale_qty`, `af19_sale_amt`, `total_sale_qty`, `total_sale_amt`, `sales_weight`, `sale_piece_qty`, `bf19_sale_piece_qty` | 原价、分时段、总销售数量金额 |
| 客数/渠道 | `sale_originalprice_custs`, `bf19_sale_custs`, `bf19_promotion_custs`, `bf19_sale_custs`, `total_cust_counts`, `offline_cust_num`, `online_cust_num` | 客数和线上线下客数 |
| 库存/进货 | `store_order_qty`, `inbound_qty`, `inbound_amount`, `purchase_weight`, `init_stock_qty`, `init_stock_amt`, `end_stock_qty`, `end_stock_amt`, `avg_purchase_price` | 订货、进货、库存和平均进货价 |
| 毛利/损耗 | `article_profit_amt`, `full_link_article_profit`, `scm_fin_article_profit`, `pre_profit_amt`, `store_lost_qty`, `store_lost_amt`, `store_know_lost_qty`, `store_know_lost_amt`, `store_unknow_lost_qty`, `store_unknow_lost_amt` | 门店/全链路/供应链利润和损耗 |
| SCM/促销 | `out_stock_amt_cb`, `out_stock_pay_amt`, `out_stock_pay_amt_notax`, `return_stock_pay_amt`, `return_stock_amt_cb`, `scm_promotion_amt_total`, `scm_promotion_amt`, `scm_bear_amt`, `vendor_bear_amt`, `business_bear_amt`, `market_bear_amt`, `allowance_amt` | 出库、退仓、让利和补贴 |
| 其他指标 | `expect_outstock_amt`, `pre_sale_amt`, `pre_inbound_amount`, `pre_lost_qty`, `pre_lost_amt`, `adjustment_amt`, `purchase_weight`, `sales_weight`, `business_flag`, `update_time`, `last_sysdate` | 理论指标、调整、营业标记和更新时间 |

此表是 v1.5 `flag_sku` 的主事实，字段多于当前脚本之外的底表。读取时应以实际 Hive DDL
为最终列顺序，不能手工猜测 `SELECT *` 的列序。

### 6.2 门店日销售 `strategy_fm_store_daily_di`（实时 75 列）

核心字段：`store_id`, `business_date`, `inc_day`, `bf19_sale_amt`, `sale_amt`, `af19_sale_amt`,
`profit_amt`, `lost_amt`, `know_lost_amt`, `unknow_lost_amt`, `online_sale_amt`,
`offline_sale_amt`, `online_cust_num`, `offline_cust_num`, `cust_num`, `business_flag`。

v1.5 使用 `bf19_sale_amt >= 500` 选择有效营业日；该表的 `business_flag` 也描述营业状态，
两种口径不能不加说明地混用。

### 6.3 SKU 销售件数 `strategy_fm_article_sale_di`（实时 92 列）

核心字段：`business_date`, `store_id`, `article_id`, `bf19_sale_piece_qty`, `sale_piece_qty`,
`sale_qty`, `sale_amt`, `online_sale_qty`, `offline_sale_qty`, `inc_day`。

它补充的是销售件数口径，不能用 `qty_spec` 替代。`bf19_sale_piece_qty` 是 19 点前件数，
`sale_piece_qty` 是全天件数。

### 6.4 翠花商品经营 `strategy_fm_chdj_article_di`（实时 75 列）

这是 v1.5 的商品经营宽表，核心字段：

`business_date`, `store_id`, `article_id`, `day_clear`, `category_level1_id`,
`category_level1_description`, `category_level2_id`, `category_level2_description`,
`category_level3_id`, `category_level3_description`, `full_link_profit`, `full_link_article_profit`,
`scm_fin_article_profit`, `profit_amt`, `pre_profit_amt`, `total_sale_qty`, `bf19_sale_qty`,
`total_sale_amt`, `bf19_sale_amt`, `inbound_amount`, `purchase_weight`, `expect_outstock_amt`,
`out_stock_amt_cb`, `pre_sale_amt`, `pre_inbound_amount`, `scm_promotion_amt_total`,
`lp_sale_amt`, `discount_amt`, `hour_discount_amt`, `store_lost_qty`, `store_lost_amt`,
`return_amt`, `out_stock_pay_amt`, `out_stock_pay_amt_notax`, `return_stock_pay_amt_notax`,
`total_cust_counts`, `bf19_sale_custs`, `online_cust_num`, `init_stock_qty`, `init_stock_amt`,
`end_stock_qty`, `end_stock_amt`, `avg_7d_sale_qty`, `last_sysdate`, `sale_piece_qty`, `inc_day`。

下游通过它取得 `day_clear`、毛利、库存和损耗，因此它不是单纯的商品维度表。

### 6.5 可订可售 `strategy_fm_dim_order_saleable`（实时 26 列）

源表：`hive.dim.dim_store_article_order_sale_info_di`。v1.5 同步脚本第 26 步按
`store_id + inc_day` 写入；`step1_flag_sku_di.sql` 按
`article_id + inc_day + store_id` LEFT JOIN，并读取 `saleable`。

实时 26 列为：`is_return`, `vendor_id`, `saleable`, `is_must_order`, `shelflife`, `tare`,
`store_name`, `label_norms`, `order_base`, `min_order_base`, `is_hq_order`, `article_name`,
`producer_phone`, `producer_name`, `article_id`, `store_id`, `vendor_name`, `max_order_base`,
`en_location`, `inc_day`, `tenant_id`, `location`, `producer_address`, `is_order`,
`effective_date`, `status`。

`saleable=1` 表示可售，`is_order=1` 表示可订，`status` 是记录状态。三者不能压成一个
`is_saleable`，也不能用 `strategy_fm_purchase_order_tmp.purchase_flag` 替代。

### 6.6 日历 `strategy_fm_dim_calendar`（实时 70 列）

核心字段：`day_wid`, `day_name`, `day_date_chn`, `day_date`, `day_name_of_week`, `day_of_week`,
`day_of_month`, `day_of_year`, `week_wid`, `week_name`, `week_no`, `week_start_date_wid`,
`week_start_date`, `week_end_date_wid`, `week_end_date`, `month_wid`, `month_name`, `month_no`,
`month_days`, `month_start_date_wid`, `month_start_date`, `month_end_date_wid`, `month_end_date`,
`quarter_wid`, `quarter_name`, `quarter_no`, `quarter_start_date_wid`, `quarter_start_date`,
`quarter_end_date_wid`, `quarter_end_date`, `year_wid`, `year_name`, `year_start_date_wid`,
`year_start_date`, `year_end_date_wid`, `year_end_date`。

日期连接优先使用 `business_date = day_name` 或按当前 SQL 的显式规则连接；不要把 `day_wid`
当作日期字符串。

### 6.7 线下/线上订单 `strategy_fm_order_offline_di`（实时 111 列） /
`strategy_fm_order_online_di`（实时 117 列）

两张表沿用订单明细源表的大部分字段，v1.5 额外追加：

`thirdparty_user_identity`。

共同关键字段：`business_date`, `store_id`, `inc_day`, `order_id`, `serial_id`, `root_order_id`,
`afs_order_id`, `je_order_id`, `rje_order_id`, `pay_at`, `abi_article_id`, `order_status`,
`jielong_flag`, `sales_amt`, `qty`, `thirdparty_user_identity`。

线下表的 `online_flag` 通常为线下，线上表还包含 `logistics_status`, `courier_company`,
`courier_name`, `courier_name_reverse`, `courier_phone`, `promotion_amt_platform_gs`,
`promotion_amt_platform_gys`, `activity_code` 等线上配送/平台字段。两表都保留订单行粒度，
不要直接按 `thirdparty_user_identity` 计数而不先去重订单。

### 6.8 支付用户映射 `strategy_fm_trade_user`

脚本显式写入 4 列：

| 字段 | 含义 |
|---|---|
| `order_id` | 支付平台订单号 |
| `thirdparty_user_identity` | 第三方用户身份标识 |
| `trade_time` | 支付/交易时间 |
| `inc_day` | 增量分区日 |

目标表如有其他列，脚本当前不写入。该表只负责订单到用户身份的映射，不是用户首单累计表。

## 7. 字段使用与计算边界

### 7.1 销售链路

```text
sales_di.qty_spec  → 销售规格量
sales_di.sales_amt → 销售额
sales_di.abi_article_id → article_id
article_sale_di.sale_piece_qty → 销售件数
order_offline/online + trade_user → 客数、渠道、新老客
```

### 7.2 BOM 链路

```text
receive_sale_di              → parent×sub 的进货事实
bom_relation                 → dressing_rate / cost_rate / BOM类型
dim_article_convert          → parent_rate / sub_rate 单位换算
purchase_di                  → 期初期末及预拆进货参考
```

`bom_alloc_qty`（parent单位）和 `bom_alloc_qty_sub`（sub单位）必须分开；它们不能在库存方程和
EUC 计算中互换。

### 7.3 成本价边界

`inventory_pool_di.cost_price`、`purchase_di.inventory_cost`、`purchase_di.avg_inbound_price`、
`scm_di.out_stock_amt_cb / total_outstock_qty`、`price_da.dc_price` 都是不同业务口径：

- 成本价池：SKU成本价快照/观测。
- 进货平均价：进货或 parent 口径平均价。
- SCM成本单价：仓出库成本口径。
- 价格表出库价：价格策略口径。
- FM 加权 EUC：计算层按期初、self receive、加工、BOM 流量得到的有效单位成本。

## 8. 推荐的在线 schema 校验 SQL

以下 SQL 需要用户在公司 IDE/有 Hive 权限的环境执行；Codex 当前不能直接访问 Hive 源表。

```sql
-- 1. StarRocks 镜像表：确认列名、顺序、类型
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_sales_di;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_full_link_article_di;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_order_offline_di;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_order_online_di;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_trade_user;

-- 2. 重点检查脚本目标与下游读取目标
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_purchase_order_tmp;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_dim_order_saleable;

-- 3. 逐表确认分区和最新数据日
SELECT 'strategy_fm_sales_di' AS table_name, MAX(inc_day) AS max_inc_day,
       COUNT(*) AS row_count
FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
WHERE store_id = 'A3XV'
UNION ALL
SELECT 'strategy_fm_full_link_article_di', MAX(inc_day), COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_full_link_article_di
WHERE store_id = 'A3XV';

-- 4. Hive 原表字段注释（有 Hive 权限时执行）
DESCRIBE hive.dsl.dsl_transaction_sotre_order_offline_details_di;
DESCRIBE hive.dsl.dsl_transaction_sotre_order_online_details_di;
DESCRIBE hive.dal_full_link.dal_manage_full_link_store_dc_article_info_di;
```

## 9. 维护规则

1. `sync_strategy_fm.sh` 新增、删除或替换 `INSERT ... SELECT *` 时，先更新第 3 节表清单，再更新对应字段章节。
2. 任何 `SELECT *` 镜像表都必须同时核对 Hive 源表列顺序和 StarRocks 目标列顺序；追加字段要记录版本和日期。
3. 新增字段必须注明：源字段、业务含义、单位、符号约定、下游使用者、是否参与计算。
4. Hive 表的中文 COMMENT 是字段语义的第一证据；若 COMMENT 为空，必须标记为“代码/样本推导”，不能写成已确认业务定义。
5. 查询结果或对比结果必须注明来自本地数据、StarRocks 镜像还是服务器生产 DuckDB；三者不能混写。
