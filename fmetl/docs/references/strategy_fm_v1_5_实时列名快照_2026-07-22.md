# strategy_fm v1.5 实时列名快照

> 查询日期：2026-07-22
>
> 查询方式：QDM API 只读 `SELECT *`。每张非空表取最新 `inc_day` 下的代表性主键记录，
> 记录 API 实际返回的列名集合；同时对全表执行 `COUNT(*)` 和 `MAX(inc_day)`。
>
> 限制：QDM API 对 `DESC` / `SHOW FULL COLUMNS` 会自动追加分页语句并报语法错误，因此本快照
> 只确认列名、列数、数据规模和分区日，不确认 SQL 类型、COMMENT、NULL 约束。类型和注释需在
> 公司 IDE 执行 `SHOW FULL COLUMNS` 或读取 Hive DDL。

## 1. 实时规模

| 表 | 列数 | 行数 | 最新分区 |
|---|---:|---:|---|
| `strategy_fm_sales_di` | 119 | 558,480 | 2026-07-21 |
| `strategy_fm_purchase_di` | 16 | 496,564 | 2026-07-21 |
| `strategy_fm_scm_di` | 54 | 96,415 | 2026-07-21 |
| `strategy_fm_scm_adjust_di` | 空表 | 0 | 2026-07-21 |
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
| `strategy_fm_dim_calendar` | 70 | 8,712 | 2013-01-01 ~ 2036-11-07 |

## 2. 实时列名集合

列顺序是 API 返回顺序，不作为 `INSERT ... SELECT *` 的 DDL 顺序依据。

### 2.1 销售、进货、SCM

#### `strategy_fm_sales_di`（119）

`reason`, `bundle_promo_code`, `order_type`, `erp_order_at`, `order_id`, `refund_at`,
`store_paylevel_discount`, `sp_store_name`, `sale_unit`, `internal_comment`, `delivery_id`,
`courier_name_reverse`, `promotion_amt_shop`, `je_date`, `company_paylevel_discount`,
`jielong_flag`, `order_at`, `customer_name`, `is_promotion_article`, `comment_id`, `rje_order_id`,
`je_order_id`, `p_balancepay_sub_amt`, `af19_sales_qty`, `spec_type`, `af19_sales_amt`,
`p_promo_sub_amt`, `promotion_amt_platform`, `split_at`, `p_cashpay_sub_amt`, `p_pay_sub_amt`,
`vip_discount_amt`, `online_flag`, `payment_type`, `outer_order_id`, `business_date`,
`actual_weight`, `gmv`, `courier_name`, `customer_phone`, `serial_id`, `member_hour_sales_amt`,
`customer_id`, `p_lp_sub_amt`, `p_sp_sub_amt`, `currency`, `channel_id`, `f_pay_sub_amt`,
`order_promotion_amt`, `sale_price`, `actual_amount`, `store_id`, `rje_date`, `p_change_sub_amt`,
`root_order_id`, `sales_amt`, `courier_company`, `area_id`, `inc_time`, `f_cashpay_sub_amt`,
`tenant_id`, `courier_phone`, `children_order_ids`, `list_price`, `shop_promo_sub_amt`,
`business_source`, `spec_num`, `f_promo_sub_amt`, `promotion_cost`, `split_supported`,
`activity_code`, `f_sub_amt`, `return_sale_amt`, `postage_shop`, `is_hour_promotion`,
`first_buy_flag`, `p_paid_sub_amt`, `day_clear`, `sibling_order_ids`, `gift_gmv`, `pay_at`,
`promotion_amt_platform_gys`, `area_description`, `i_promotion_amt`, `comment_time`, `inc_day`,
`qty`, `sp_type`, `return_sale_qty`, `ordercoupon_promotion_amt`, `postage_platform`,
`afs_order_id`, `order_sub_type`, `discount_amt`, `gift_qty`, `order_status`, `sp_level`,
`refund_type`, `f_paid_sub_amt`, `gmv1`, `allrefund_time`, `display_price`, `parent_order_id`,
`promotion_amt`, `p_mp_sub_amt`, `f_balancepay_sub_amt`, `sync_flag`, `sync_seq`,
`p_pointpay_sub_amt`, `message`, `abi_article_id`, `f_pointpay_sub_amt`, `complete_at`,
`qty_spec`, `promotion_amt_platform_gs`, `outer_order_type`, `hour_discount_amt`,
`logistics_status`, `goods_barcode`。

#### `strategy_fm_purchase_di`（16）

`article_name`, `init_stock_amt`, `inventory_cost`, `sale_article_id`, `avg_inbound_price`,
`article_id`, `end_stock_amt`, `store_id`, `sale_article_purchase_amt`, `business_date`,
`init_stock_qty`, `inc_day`, `sale_article_name`, `end_stock_qty`, `day_clear`, `sale_article_qty`。

#### `strategy_fm_scm_di`（54）

`scm_bear_amt`, `return_stock_amt_cb`, `scm_promotion_amt`, `scm_promotion_amt_gift`,
`order_amt`, `adjustment_amt_notax`, `out_stock_pay_amt`, `business_market_bear_amt`,
`store_return_qty_shop`, `return_stock_amt_cb_notax`, `gift_outstock_qty`,
`return_stock_pay_amt_notax`, `scm_promotion_amt_total`, `scm_bear_gift_amt`,
`qdm_bear_nogift_positive_amt`, `total_outstock_qty`, `adjustment_amt`, `store_order_qty`,
`vendor_bear_amt`, `vender_bear_gift_qty`, `inc_day`, `original_outstock_qty`, `order_qty_payean`,
`out_stock_pay_amt_notax`, `vender_bear_gift_amt`, `qdm_bear_gift_amt`, `out_stock_zzckj_amt`,
`out_stock_amt_cb`, `return_stock_pay_amt`, `promotion_outstock_amt`,
`qdm_bear_nogift_negative_amt`, `qdm_bear_promotion_fee`, `original_outstock_amt`,
`promotion_outstock_price`, `new_dc_id`, `out_stock_amt_cb_notax`, `qdm_bear_gift_qty`,
`business_date`, `promotion_outstock_qty`, `miss_stock_qty`, `scm_bear_gift_qty`,
`store_return_amt_shop`, `miss_stock_amt`, `article_id`, `return_stock_original_amt`,
`return_stock_qty`, `store_id`, `business_bear_amt`, `scm_promotion_qty_gift`, `market_bear_amt`,
`scm_promotion_cost`, `qdm_bear_positive_amt_total`, `scm_return_promotion_cost`,
`qdm_bear_negative_amt_total`。

#### `strategy_fm_scm_adjust_di`（实时 0 行）

实时无数据行，API 无法返回列名。已保存结构为：
`business_date`, `store_id`, `dc_id`, `matnr`, `article_id`, `tax`, `adjustment_amt`,
`adjustment_amt_notax`, `new_sp_store_id`, `inc_day`。

### 2.2 损耗、加工、让利、促销

#### `strategy_fm_loss_di`（10）

`unknow_lost_amt`, `know_lost_amt`, `article_name`, `inc_day`, `article_id`, `store_id`,
`unknow_lost_qty`, `know_lost_qty`, `category_level1_id`, `category_level1_description`。

#### `strategy_fm_compose_di`（11）

`business_date`, `article_name`, `compose_in_amt`, `inc_day`, `article_id`, `compose_out_qty`,
`store_name`, `update_time`, `store_id`, `compose_in_qty`, `compose_out_amt`。

#### `strategy_fm_allowance_di`（76）

`split_member_hour_sale_amt`, `split_receive_unit_qty_spec`, `sale_article_receive_price`,
`receive_article_id`, `split_bf9_sale_amt`, `split_af19_sale_qty`, `order_amt`, `purchase_price`,
`split_return_sale_qty`, `split_bf10_sale_amt`, `split_bf12_sale_amt`, `allowance_profit_amt`,
`split_discount_amt`, `lost_amt`, `activity_id`, `sale_amt`, `split_hour_discount_amt`,
`split_p_lp_sub_amt`, `split_af19_sale_amt`, `split_qty`, `split_bf19_cust_num`, `order_qty`,
`split_qty_spec`, `receive_amt`, `split_return_sale_amt`, `order_article_id`, `last_pay_at`,
`init_stock_amt`, `split_member_discount_amt`, `activity_name`, `settle_order_qty`, `end_stock_amt`,
`receive_qty`, `sum_sub_article_qty`, `split_bf16_sale_amt`, `init_receiveb_amt`, `hot_flag`,
`init_receiveb_qty`, `inc_day`, `qty`, `split_sale_amt`, `split_bf10_sale_qty`,
`sale_article_receive_unit_qty`, `split_bf12_sale_qty`, `split_bf16_sale_qty`, `end_receiveb_qty`,
`sale_article_id`, `split_order_amt`, `order_weight`, `split_allowance_amt`,
`split_receive_unit_order_qty`, `end_receiveb_amt`, `business_date`, `init_stock_qty`,
`order_price`, `end_stock_qty`, `split_cust_num`, `split_bf9_sale_weight`, `split_order_qty`,
`sale_article_receive_amt`, `split_p_sp_sub_amt`, `sale_price`, `lost_qty`,
`split_promotion_discount_amt`, `store_id`, `split_bf19_sale_amt`, `out_price`, `profit_amt`,
`qty_spec`, `split_bf9_sale_qty`, `sale_article_receive_qty`, `spilt_receive_weight`,
`activity_type`, `split_sale_weight`, `split_bf19_sale_qty`, `list_price`。

#### `strategy_fm_promo_di`（90）

`business_source`, `order_type`, `bundle_promo_code`, `purchase_limit_qty`, `order_id`, `refund_at`,
`promotion_cost`, `discount`, `source`, `product_count`, `promo_sub_type`, `activity_code`,
`delivery_id`, `promo_action`, `row_num`, `je_date`, `rank`, `promo_condition_type`, `shop_id`,
`tag`, `promo_ext_prop`, `is_hour_promotion`, `goods_name`, `activity_level`, `from_outer`,
`cost_company`, `outer_code`, `pay_at`, `promo_condition_context`, `cost_center`, `promo_type`,
`promotion_code`, `promo_action_context`, `allowance`, `jielong_flag`, `order_at`, `customer_name`,
`category_info`, `sales_charge_type`, `normal_inc_day`, `inc_day`, `parent_order_item_promotion_id`,
`name`, `coupon_mode`, `p_promo_amt`, `sku_code`, `order_sub_type`, `code2`, `cost_center_info`,
`goods_id`, `eligibility_condition`, `available_category`, `order_status`, `shop_name`,
`description`, `title`, `online_flag`, `outer_order_id`, `refund_type`, `promotion_category`,
`promo_sub_type1`, `customer_phone`, `promotion_name`, `parent_order_id`, `allocate_rate`,
`customer_id`, `spu_code`, `channel_id`, `only_member`, `p_promo_total_amt`, `promotion_type`,
`cancel_at`, `order_item_id`, `promotion_code2`, `parent_order_item_id`, `rje_date`,
`parent_bom_order_item_promo_id`, `promo_action_type`, `cost_subject`, `promo_type1`,
`parent_bom_order_item_id`, `f_promo_total_amt`, `created_by`, `inc_time`, `outer_order_type`,
`f_promo_amt`, `cost_tax_rate`, `activity_type`, `coupon_code`, `category_id`。

### 2.3 成本价、价格和维度

#### `strategy_fm_inventory_pool_di`（20）

`main_img`, `updated_by`, `gift_flag`, `last_updated_at`, `inventory_date`, `cost_price`,
`sub_category_id`, `spec`, `sub_category_name`, `sku_name`, `created_at`, `created_by`, `inc_day`,
`sales_unit`, `weight_flag`, `seven_days_avg_sale`, `id`, `shop_id`, `sku_code`, `updated_at`。

#### `strategy_fm_price_da`（54）

`last_updated_at`, `source`, `created_at`, `monitor_type`, `monitor_desc`, `dc_name`,
`exception_reason`, `confirm_status`, `yesterday_dc_price`, `unadjust_sale_price`, `id`, `shop_id`,
`calc_effect_at`, `updated_at`, `strategy_no`, `updated_by`, `confirm_by`, `calc_status`,
`deal_status`, `outstock_lock_price`, `anchor_sale_price`, `inc_day`, `sales_unit`, `out_updated_at`,
`dc_code`, `sku_code`, `exception_type`, `original_price`, `shop_name`, `shop_id_sku_code_calc_at`,
`outstock_profit_rate`, `sales_mode`, `category_name`, `sku_name`, `effective`,
`outstock_addprice_rate`, `promotion_no`, `lock_status`, `yesterday_price`, `outstock_addprice_amt`,
`calc_strategy`, `original_dc_price`, `push_status`, `current_price`, `category_code`, `sale_status`,
`is_new`, `dc_original_price`, `dc_price`, `shop_sku`, `dc_original_price_sap`, `calc_by`,
`created_by`, `tenant_id`。

#### `strategy_fm_dim_day_clear`（11）

`category_level3_id`, `business_date`, `category_level2_description`, `article_name`, `inc_day`,
`article_id`, `store_id`, `category_level3_description`, `category_level1_id`,
`category_level1_description`, `category_level2_id`。

#### `strategy_fm_dim_store_profile`（109）

`new_sp_level_name`, `pt`, `area2_name`, `sp_store_name`, `pro_description`,
`sap_store_status_name`, `city_id`, `eblc_longitude`, `transfer_date`, `sp_phone3`,
`area_description_alias`, `stop_reason_apply`, `sap_store_status_id`, `sp_phone1`, `sp_phone2`,
`manage_area_name`, `sap_area_id`, `sp_origin_start_date`, `open_days`, `store_type_name`,
`transfer_store_id`, `closed_reason_name`, `area_id_purchase`, `new_sp_level`, `zone_phone`,
`store_service_name`, `zone_supper_id`, `target_sales_amt`, `bf19_cust_num`, `sp_master_area`,
`sp_rsv_status`, `store_service_range`, `franchisee_id`, `target_bf19_cust_num`, `zone_supper_manager`,
`new_sp_store_name`, `stop_end_date`, `bunk_id`, `sp_master_area`, `city_description`,
`contract_franchisee_phone`, `store_flag_name`, `area2_id_sap`, `stop_start_date`,
`mall_supervisor_name`, `zone_supper_phone`, `sp_store_status`, `area_id`, `op_area_id`,
`zone_manager`, `expand_staff_id`, `expand_staff_name`, `eblc_latitude`, `operate_id_purchase`,
`total_area`, `zzljsrq`, `sp_company_id`, `opera_manager`, `operate_name_purchase`, `store_guide_user`,
`restart_date`, `original_store_id`, `zman_id`, `sp_rsv_status`, `store_service_range`, `franchisee_id`,
`zone_id`, `sp_currency`, `stop_reason_id`, `sp_scale`, `area_description`, `group_manager_tel`,
`new_sp_store_id`, `sap_area_name`, `business_area`, `inc_day`, `sp_type`, `sp_table_flag`,
`price_strategy_start_date`, `area2_id`, `dist_id`, `sp_address`, `sp_sign_date`, `region_name`,
`measuring_area`, `sp_level`, `dist_description`, `sap_area2_name`, `area_type`, `sap_area2_id`,
`group_manager_code`, `zzlksrq`, `manage_area_id`, `group_manager`, `targer_allowance_profit`,
`stop_reason`, `sp_final_end_date`, `sp_purchasing_center_id`, `old_id`, `target_cust_num`,
`sp_store_effective_date`, `sp_purchasing_area`, `region_id`, `pro_id`, `area_id_sap`, `opera_id`,
`mall_supervisor_phone`。

> 注：上述实时返回集合中出现了重复列名（例如 `sp_master_area`、`sp_rsv_status`、
> `store_service_range`、`franchisee_id`），说明 API 返回的 JSON 列映射可能存在同名覆盖或
> 源表/镜像列重复；正式使用前必须以 `SHOW FULL COLUMNS` 的序号结果确认。

#### `strategy_fm_purchase_order_tmp`（52）

`purchase_flag`, `last_updated_at`, `max_batch_qty`, `basic_qty`, `core_flag`, `purchase_price`,
`sku_created_at`, `spec_update_date`, `created_at`, `sell_unit`, `article_spec`, `order_qty`,
`seven_days_avg_sale`, `id`, `shop_id`, `updated_at`, `updated_by`, `unit_convert_deno`,
`sub_category_code`, `order_at`, `seven_days_order_qty`, `is_special_price`, `inc_day`, `sku_code`,
`main_img`, `allow_single_order`, `old_spec`, `min_batch_qty`, `shop_name`, `order_weight`,
`category_name`, `mid_category_code`, `sub_category_name`, `tips_flag`, `sku_name`, `only_morning`,
`unit_convert_mole`, `mid_category_name`, `order_amount`, `seven_days_order_count`, `is_deleted`,
`spu_code`, `conver_rate`, `propose_desc`, `sap_order_qty`, `sku_tag`, `category_code`, `created_by`,
`receive_sku_code`, `tenant_id`, `propose_qty`, `pur_unit`。

#### `strategy_fm_dim_goods`（106）

`zglfm`, `pt`, `matnr`, `shelf_time`, `type`, `sale_unit`, `offlineshop_flag`,
`abi_outer_packing_spec`, `zglfz`, `sales_unit_id`, `category_material_label_id`, `import_flag`,
`weight_flag`, `vegetablebar_flag`, `abi_volume_ratio`, `category_level3_description`, `brand`,
`superior_purchase_department_id`, `sp_info`, `matnr_unit`, `atob_value`, `matnr_name`, `in_date`,
`old_category_level3_description`, `sale_areas`, `brand_id`, `category_level2_id`, `status_code`,
`xx_sl`, `spu_name`, `if_settle_unit`, `type_name`, `purchase_department_id`, `freeze_id`,
`old_category_level1_id`, `abi_new_category_id`, `status_name`, `purchase_department`, `order_unit`,
`article_name`, `abi_create_reason`, `atob_name`, `mtart`, `abi_srmcategory`, `norms_upper_limit`,
`commodity_attribute_name`, `norms`, `jx_sl`, `spu_id`, `abi_quality_days`, `life_cycle`,
`category_level1_id`, `relation_matnr`, `max_order_times`, `matnr_in_date`, `temperature_layer_name`,
`old_category_level2_id`, `superior_purchase_department_name`, `commodity_attribute`, `article_type`,
`product_address`, `logistics_process_tag`, `barcode`, `temperature_layer_id`, `create_date1`,
`logistics_scrap_tag`, `package_attribute_name`, `min_pack_weight`, `order_frequency`, `use_types`,
`inc_day`, `old_article_id`, `article_matnr`, `sp_info_name`, `min_order_times`,
`category_material_label_name`, `abi_outer_packing_lwh`, `category_level1_description`, `online_tag`,
`unit_weight`, `private`, `abi_purchase_group_name`, `matnr_unit_id`, `old_category_level3_id`,
`blackwhite_pig_id`, `category_level2_description`, `load_time`, `article_belong_name`,
`old_category_level2_description`, `min_order_unit`, `article_series_name`, `article_belong_id`,
`norms_lower_limit`, `out_date`, `abi_purchasecategory_id`, `article_id`, `abi_purchase_group`,
`order_unit_id`, `blackwhite_pig_name`, `category_level3_id`, `package_attribute`,
`old_category_level1_description`, `resent_use_date`, `old_article_name`, `onlineshop_flag`。

### 2.4 BOM、库存、全链路

#### `strategy_fm_receive_sale_di`（20）

`article_name`, `sale_article_id`, `article_id`, `purchase_price`, `store_id`, `inbound_qty`,
`sum_sub_article_qty`, `sale_recev_rate`, `business_date`, `sum_sale_article_qty`, `rate`,
`inc_day`, `sum_article_qty`, `sale_article_name`, `sale_article_price`,
`spilit_sale_article_amt`, `inbound_amount`, `category_level1_id`, `category_level1_description`,
`sale_article_qty`。

#### `strategy_fm_order_receive_di`（17）

`order_article_id`, `re_category_level1_id`, `order_category_level1_description`, `re_article_id`,
`order_article_name`, `re_category_level1_description`, `order_amt`, `store_id`, `type`,
`re_order_amt`, `business_date`, `order_category_level1_id`, `re_article_name`, `rate`, `inc_day`,
`order_qty`, `re_order_qty`。

#### `strategy_fm_dim_article_convert`（9）

`sub_article_name`, `parent_rate`, `parent_article_name`, `ctype`, `inc_day`, `parent_article_id`,
`sub_article_id`, `store_id`, `sub_rate`。

#### `strategy_dim_store_article_bom_relation`（17）

`cost_rate`, `parent_article_id`, `sub_article_id`, `split_mode`, `store_id`, `sp_level`,
`sub_article_unit`, `bom_type`, `category_level3_id`, `category_level2_description`, `inc_day`,
`parent_article_unit`, `dressing_rate`, `category_level3_description`, `category_level1_id`,
`category_level1_description`, `category_level2_id`。

#### `strategy_fm_store_article_inventory_detail_di`（21）

`actual_stock_qty`, `main_img`, `updated_by`, `gift_flag`, `last_updated_at`, `inventory_date`,
`sub_category_id`, `profit_loss_qty`, `spec`, `stock_cost`, `sub_category_name`, `sku_name`,
`created_at`, `sale_stock_qty`, `created_by`, `inc_day`, `sales_unit`, `id`, `shop_id`, `sku_code`,
`updated_at`。

#### `strategy_fm_full_link_article_di`（202）

`scm_promotion_amt`, `online_sale_return_qty`, `sale_return_qty`, `price_level`,
`bf19_promotion_sale_qty`, `sale_originalprice_amt`, `first_category_name`, `area_name`,
`member_coupon_shop_amt`, `offline_sale_return_amt`, `scm_promotion_amt_total`,
`vender_bear_gift_qty`, `bf19_avg_price`, `total_sale_amt`, `store_lost_qty`, `online_lp_sale_amt`,
`order_qty_payean`, `bf19_sale_piece_qty`, `store_order_amt`, `af19_sale_amt`,
`qdm_bear_nogift_negative_amt`, `qdm_bear_promotion_fee`, `sales_weight`, `third_category_id`,
`qdm_bear_gift_qty`, `business_date`, `offline_sale_return_cust`, `online_sale_qty`,
`platform_bear_cost`, `init_stock_qty`, `scm_bear_gift_qty`, `end_stock_qty`,
`promotion_discount_amt`, `miss_stock_amt`, `article_name`, `actual_amount`, `cost_price`,
`article_profit_amt`, `store_id`, `online_sale_return_cust`, `pre_lost_qty`, `gift_outstock_amt`,
`lp_sale_amt`, `sale_originalprice_custs`, `sale_profit_amt`, `sale_piece_qty`, `online_bear_cost`,
`store_promotion_cost`, `store_know_lost_qty`, `scm_promotion_amt_gift`, `bf19_promotion_avg_price`,
`adjustment_amt_notax`, `offline_discount_amt`, `avg_purchase_price`, `business_market_bear_amt`,
`bf19_sale_custs`, `bf19_actual_amount`, `promo_amt`, `offline_cust_num`, `init_stock_amt`,
`operate_id`, `supplier_bear_cost`, `bf19_member_custs`, `qdm_bear_nogift_positive_amt`,
`store_bear_cost`, `other_bear_cost`, `business_flag`, `vendor_bear_amt`, `offline_lp_sale_amt`,
`no_ordercoupon_company_promotion_amt`, `original_outstock_qty`, `bf19_sale_qty`, `sale_return_cust`,
`return_amt`, `qdm_bear_gift_amt`, `is_outstock_promo_article_flag`, `pur_market_price`,
`member_coupon_company_amt`, `fulllink_article_expect_profit`, `discount_amt`,
`promotion_outstock_amt`, `store_unknow_lost_amt`, `ordercoupon_company_promotion_amt`,
`af19_actual_amount`, `offline_sale_amt`, `shop_promotion_amt`, `bf19_discount_amt`, `strategy_name`,
`member_custs`, `return_num`, `pro_id`, `second_category_name`, `hour_discount_amt`,
`af19_lp_sale_amt`, `dc_article_expect_profit`, `sale_originalprice_qty`, `scm_bear_amt`,
`return_stock_amt_cb`, `store_lost_amt`, `matnr`, `pro_description`, `bear_cost123000`, `city_id`,
`inbound_qty`, `sale_cost_amt`, `online_cust_num`, `scm_fin_article_profit`, `online_sale_return_amt`,
`bf19_promotion_amt`, `return_stock_amt_cb_notax`, `gift_outstock_qty`, `total_sale_qty`,
`allowance_amt`, `sale_return_amt`, `bf19_promotion_custs`, `scm_bear_gift_amt`, `end_stock_amt`,
`total_outstock_qty`, `store_order_qty`, `settle_unit`, `service_bear_cost`, `out_stock_pay_amt_notax`,
`vender_bear_gift_amt`, `second_category_id`, `bf19_promotion_sale_amt`, `return_stock_pay_amt`,
`avg_instock_price`, `original_outstock_amt`, `promotion_outstock_price`, `out_stock_amt_cb_notax`,
`avg_sale_price`, `af19_sale_qty`, `miss_stock_qty`, `offline_sale_return_qty`, `city_description`,
`scm_fin_article_cost`, `operate_name`, `pre_lost_amt`, `pre_sale_amt`, `online_sale_amt`,
`dc_out_profit`, `update_time`, `sale_originalprice_profit`, `market_bear_cost`, `dc_price`,
`scm_promotion_qty_gift`, `area_id`, `market_bear_amt`, `qdm_bear_positive_amt_total`,
`scm_return_promotion_cost`, `qdm_bear_negative_amt_total`, `pre_inbound_amount`,
`scm_fin_article_income`, `out_stock_pay_amt`, `pre_profit_amt`, `af19_sale_price`, `dc_name`,
`store_know_lost_amt`, `brand_center_bear_cost`, `af19_sale_custs`, `strategy_no`,
`return_stock_pay_amt_notax`, `bf19_sale_amt`, `bf19_promotion_sale_profit`, `adjustment_amt`,
`ordercoupon_shop_promotion_amt`, `purchase_weight`, `member_promo_amt`, `inc_day`,
`total_cust_counts`, `expect_outstock_amt`, `inbound_amount`, `member_discount_amt`,
`finally_total_sale_amt`, `offline_sale_qty`, `out_stock_amt_cb`, `original_price`,
`first_category_id`, `category_name`, `full_link_article_profit`, `promotion_outstock_qty`, `dc_id`,
`purchase_group_id`, `store_name`, `store_unknow_lost_qty`, `price_zone`, `online_discount_amt`,
`bf19_lp_sale_amt`, `af19_sale_profit`, `sale_lp_originalprice_amt`, `article_id`,
`return_stock_qty`, `current_price`, `third_category_name`, `online_avg_sale_price`,
`business_bear_amt`, `dc_original_price`, `bf19_promotion_discount_amt`, `offline_avg_sale_price`,
`scm_promotion_cost`, `category_id`。

### 2.5 门店、SKU、翠花和可售

#### `strategy_fm_store_daily_di`（75）

`member_promotion_amt`, `nomember_avg_article_num`, `store_paylevel_discount`, `cust_num`,
`order_amt`, `member_num`, `theory_lost_amt`, `bf19_shop_promotion_amt`, `sale_cost_amt`,
`avg_article_num`, `pre_profit_amt`, `lost_amt`, `online_cust_num`, `sale_amt`, `return_sale_amt`,
`order_qty`, `recevie_amt`, `member_coupon_shop_amt`, `bf19_sale_amt`, `allowance_amt`,
`offline_cust_num`, `init_stock_amt`, `round_amt`, `end_stock_amt`, `company_paylevel_discount`,
`business_flag`, `return_cust_num`, `member_cust_num`, `know_lost_amt`,
`ordercoupon_shop_promotion_amt`, `inc_day`, `no_ordercoupon_company_promotion_amt`,
`recevie_weight`, `bf19_sale_qty`, `bf19_sale_piece_qty`, `member_discount_amt`, `last_sysdate`,
`return_store_amt`, `member_coupon_company_amt`, `unknow_lost_amt`, `discount_amt`,
`member_avg_article_num`, `offline_original_amt`, `af19_sale_amt`, `bf19_offline_sale_amt`,
`order_weight`, `nomember_discount_amt`, `ordercoupon_company_promotion_amt`, `sale_article_num`,
`original_price_sale_amt`, `business_date`, `bf19_usual_cust_num`, `offline_sale_amt`,
`shop_promotion_amt`, `allowance_amt_profit`, `bf12_cust_num`, `promotion_amt`, `bf19_cust_num`,
`bf19_offline_cust_num`, `company_cost_amt`, `promotion_discount_amt`, `online_sale_amt`,
`bf12_sale_qty`, `member_sale_amt`, `sale_weight`, `update_time`, `store_id`,
`bf19_member_sale_amt`, `profit_amt`, `bf12_sale_amt`, `sale_profit_amt`, `sale_piece_qty`,
`hour_discount_amt`, `bf19_sale_weight`, `af19_cust_num`。

#### `strategy_fm_article_sale_di`（92）

`expect_sales_amt`, `member_promotion_amt`, `store_paylevel_discount`, `matnr`, `cust_num`,
`order_amt`, `tj_time`, `member_num`, `bf19_shop_promotion_amt`, `short_qty`, `sale_cost_amt`,
`pre_profit_amt`, `lost_amt`, `online_cust_num`, `avg_purchase_price`, `sale_amt`,
`return_sale_amt`, `order_qty`, `recevie_amt`, `member_coupon_shop_amt`, `bf19_sale_amt`,
`allowance_amt`, `offline_cust_num`, `init_stock_amt`, `round_amt`, `recevie_qty`, `end_stock_amt`,
`company_paylevel_discount`, `know_lost_qty`, `unknow_lost_qty`, `return_store_qty`,
`business_flag`, `return_cust_num`, `member_cust_num`, `know_lost_amt`,
`ordercoupon_shop_promotion_amt`, `inc_day`, `return_sale_qty`, `no_ordercoupon_company_promotion_amt`,
`short_amt`, `recevie_weight`, `bf19_sale_qty`, `bf19_sale_piece_qty`, `member_discount_amt`,
`last_sysdate`, `return_store_amt`, `member_coupon_company_amt`, `offline_sale_qty`,
`unknow_lost_amt`, `discount_amt`, `offline_original_amt`, `af19_sale_amt`,
`bf19_offline_sale_amt`, `order_weight`, `nomember_discount_amt`,
`ordercoupon_company_promotion_amt`, `original_price_sale_amt`, `business_date`, `af19_sale_qty`,
`offline_sale_amt`, `shop_promotion_amt`, `online_sale_qty`, `init_stock_qty`,
`allowance_amt_profit`, `bf12_cust_num`, `promotion_amt`, `bf19_cust_num`, `strategy_id`,
`company_cost_amt`, `bf19_offline_cust_num`, `end_stock_qty`, `promotion_discount_amt`,
`actual_amt`, `strategy_name`, `online_sale_amt`, `bf12_sale_qty`, `bf19_offline_qty`, `article_id`,
`member_sale_amt`, `cost_price`, `sale_weight`, `store_id`, `bf19_member_sale_amt`, `profit_amt`,
`bf12_sale_amt`, `sale_profit_amt`, `sale_piece_qty`, `hour_discount_amt`, `bf19_sale_weight`,
`sale_qty`, `af19_cust_num`, `category_id`。

#### `strategy_fm_chdj_article_di`（75）

`af20_sale_qty`, `scm_fin_article_income`, `bf16_member_num`, `af20_sale_piece_qty`, `cust_num`,
`member_num`, `out_stock_pay_amt`, `sale_cost_amt`, `pre_profit_amt`, `lost_amt`,
`avg_purchase_price`, `sale_amt`, `af20_sale_amt`, `scm_fin_article_profit`, `receive_amt`,
`compose_out_amt`, `day_clear`, `member_coupon_shop_amt`, `bf19_sale_amt`, `allowance_amt`,
`init_stock_amt`, `scm_promotion_amt_total`, `end_stock_amt`, `receive_qty`, `know_lost_qty`,
`unknow_lost_qty`, `avg7d_sale_qty`, `know_lost_amt`, `is_promotion_article`, `total_sale_amt`,
`inc_day`, `sum7d_sale_qty`, `bf19_sale_qty`, `expect_outstock_amt`, `bf19_sale_piece_qty`,
`last_sysdate`, `pur_market_price`, `unknow_lost_amt`, `discount_amt`, `offline_original_amt`,
`out_stock_amt_cb`, `compose_in_amt`, `af19_sale_amt`, `full_link_profit`, `original_price_sale_amt`,
`business_date`, `af19_sale_qty`, `shop_promotion_amt`, `init_stock_qty`, `bf18_member_num`,
`allowance_amt_profit`, `sell_out_time`, `bf19_cust_num`, `end_stock_qty`, `promotion_discount_amt`,
`pre_sale_amt`, `avg_actual_pay_price`, `max_actual_pay_price`, `sale_price`, `lost_qty`,
`article_id`, `compose_out_qty`, `sale_weight`, `article_profit_amt`, `store_id`,
`sale_allowance_amt_profit`, `compose_in_qty`, `profit_amt`, `min_actual_pay_price`, `sale_profit_amt`,
`sale_piece_qty`, `hour_discount_amt`, `sale_qty`, `pre_inbound_amount`, `list_price`。

#### `strategy_fm_dim_order_saleable`（26）

`is_return`, `vendor_id`, `saleable`, `is_must_order`, `shelflife`, `tare`, `store_name`,
`label_norms`, `order_base`, `min_order_base`, `is_hq_order`, `article_name`, `producer_phone`,
`producer_name`, `article_id`, `store_id`, `vendor_name`, `max_order_base`, `en_location`, `inc_day`,
`tenant_id`, `location`, `producer_address`, `is_order`, `effective_date`, `status`。

### 2.6 订单、用户和日历

#### `strategy_fm_order_offline_di`（111）

实时列名集合与销售订单表相同的字段包括订单、商品、价格、支付、退款、促销、物流等字段，
并实际返回：

`reason`, `bundle_promo_code`, `order_type`, `erp_order_at`, `order_id`, `refund_at`,
`store_paylevel_discount`, `sp_store_name`, `sale_unit`, `internal_comment`, `delivery_id`,
`promotion_amt_shop`, `je_date`, `company_paylevel_discount`, `jielong_flag`, `order_at`,
`customer_name`, `is_promotion_article`, `comment_id`, `rje_order_id`, `je_order_id`,
`p_balancepay_sub_amt`, `af19_sales_qty`, `spec_type`, `af19_sales_amt`, `p_promo_sub_amt`,
`promotion_amt_platform`, `split_at`, `p_cashpay_sub_amt`, `p_pay_sub_amt`, `vip_discount_amt`,
`online_flag`, `payment_type`, `outer_order_id`, `business_date`, `actual_weight`, `gmv`,
`customer_phone`, `serial_id`, `member_hour_sales_amt`, `customer_id`, `p_lp_sub_amt`,
`p_sp_sub_amt`, `currency`, `channel_id`, `f_pay_sub_amt`, `order_promotion_amt`, `sale_price`,
`actual_amount`, `store_id`, `rje_date`, `p_change_sub_amt`, `root_order_id`, `sales_amt`,
`area_id`, `inc_time`, `f_cashpay_sub_amt`, `tenant_id`, `children_order_ids`, `list_price`,
`shop_promo_sub_amt`, `business_source`, `spec_num`, `f_promo_sub_amt`, `promotion_cost`,
`thirdparty_user_identity`, `split_supported`, `f_sub_amt`, `return_sale_amt`, `postage_shop`,
`is_hour_promotion`, `first_buy_flag`, `p_paid_sub_amt`, `sibling_order_ids`, `gift_gmv`, `pay_at`,
`area_description`, `i_promotion_amt`, `comment_time`, `inc_day`, `qty`, `sp_type`,
`return_sale_qty`, `ordercoupon_promotion_amt`, `postage_platform`, `afs_order_id`, `order_sub_type`,
`discount_amt`, `gift_qty`, `order_status`, `sp_level`, `refund_type`, `f_paid_sub_amt`, `gmv1`,
`allrefund_time`, `display_price`, `parent_order_id`, `promotion_amt`, `p_mp_sub_amt`,
`f_balancepay_sub_amt`, `sync_flag`, `sync_seq`, `p_pointpay_sub_amt`, `message`, `abi_article_id`,
`f_pointpay_sub_amt`, `complete_at`, `qty_spec`, `outer_order_type`, `hour_discount_amt`,
`goods_barcode`。

#### `strategy_fm_order_online_di`（117）

线上表在订单公共字段和 `thirdparty_user_identity` 外，实际还包含 `courier_name_reverse`,
`courier_company`, `courier_name`, `courier_phone`, `promotion_amt_platform_gys`,
`activity_code`, `logistics_status`, `promotion_amt_platform_gs` 等线上配送/平台字段。
完整实时列名为：

`reason`, `bundle_promo_code`, `order_type`, `erp_order_at`, `order_id`, `refund_at`,
`sp_store_name`, `sale_unit`, `internal_comment`, `delivery_id`, `courier_name_reverse`,
`promotion_amt_shop`, `je_date`, `jielong_flag`, `order_at`, `customer_name`,
`is_promotion_article`, `comment_id`, `rje_order_id`, `je_order_id`, `p_balancepay_sub_amt`,
`af19_sales_qty`, `spec_type`, `af19_sales_amt`, `p_promo_sub_amt`, `promotion_amt_platform`,
`split_at`, `p_cashpay_sub_amt`, `p_pay_sub_amt`, `vip_discount_amt`, `online_flag`,
`payment_type`, `outer_order_id`, `business_date`, `actual_weight`, `gmv`, `courier_name`,
`customer_phone`, `serial_id`, `member_hour_sales_amt`, `customer_id`, `p_lp_sub_amt`, `p_sp_sub_amt`,
`currency`, `channel_id`, `f_pay_sub_amt`, `order_promotion_amt`, `sale_price`, `actual_amount`,
`store_id`, `rje_date`, `p_change_sub_amt`, `root_order_id`, `sales_amt`, `courier_company`,
`area_id`, `inc_time`, `f_cashpay_sub_amt`, `tenant_id`, `courier_phone`, `children_order_ids`,
`list_price`, `shop_promo_sub_amt`, `business_source`, `spec_num`, `f_promo_sub_amt`,
`promotion_cost`, `thirdparty_user_identity`, `split_supported`, `activity_code`, `f_sub_amt`,
`return_sale_amt`, `postage_shop`, `is_hour_promotion`, `first_buy_flag`, `p_paid_sub_amt`,
`sibling_order_ids`, `gift_gmv`, `pay_at`, `promotion_amt_platform_gys`, `area_description`,
`i_promotion_amt`, `comment_time`, `inc_day`, `qty`, `sp_type`, `return_sale_qty`,
`ordercoupon_promotion_amt`, `postage_platform`, `afs_order_id`, `order_sub_type`, `discount_amt`,
`gift_qty`, `order_status`, `sp_level`, `refund_type`, `f_paid_sub_amt`, `gmv1`, `allrefund_time`,
`display_price`, `parent_order_id`, `promotion_amt`, `p_mp_sub_amt`, `f_balancepay_sub_amt`,
`sync_flag`, `sync_seq`, `p_pointpay_sub_amt`, `message`, `abi_article_id`, `f_pointpay_sub_amt`,
`complete_at`, `qty_spec`, `promotion_amt_platform_gs`, `outer_order_type`, `hour_discount_amt`,
`logistics_status`, `goods_barcode`。

#### `strategy_fm_trade_user`（4）

`trade_time`, `order_id`, `inc_day`, `thirdparty_user_identity`。

#### `strategy_fm_dim_calendar`（70）

`day_name`, `day_of_year`, `year_end_date`, `month_days`, `week_ago_date_wid`,
`quarter_start_date_wid`, `language`, `week_no_name`, `day_ago_date_wid`, `year_start_date`,
`year_end_date_wid`, `day_date_chn`, `is_last_day_of_month`, `quarter_name`, `week_ago_date`,
`w_update_date`, `week54_name`, `week_no`, `month_start_date`, `day_ago_date`, `week54_end_date_wid`,
`week_wid`, `quarter_start_date`, `week54_end_date`, `week_end_date_wid`, `day_wid`,
`year_ago_date_wid`, `month_name`, `week54_wid`, `quarter_ago_date_wid`, `is_last_day_of_year`,
`analysis_week_name`, `day_of_month`, `month_end_date`, `month_end_date_wid`, `day_name_of_week`,
`quarter_end_date`, `holiday_name`, `month_ago_date_wid`, `month_start_date_wid`, `is_rest_day`,
`quarter_wid`, `is_weekend`, `month_wid`, `day_of_week`, `day_date`, `month_no`,
`is_actual_overwork`, `week54_start_date_wid`, `actual_holiday_name`, `year_name`, `week_end_date`,
`year_ago_date`, `year_wid`, `week54_no`, `week_start_date_wid`, `is_actual_holiday`,
`is_last_day_of_week`, `week54_start_date`, `year_start_date_wid`, `analysis_week_wid`,
`w_insert_date`, `quarter_end_date_wid`, `actual_week_no`, `week_start_date`, `quarter_no`,
`quarter_ago_date`, `month_ago_date`, `week_name`, `day54_of_week`。

## 3. 发现和后续核对

1. `strategy_fm_scm_adjust_di` 实时为空，不能仅凭实时数据确认其列名；已用已保存 DDL 补录 10 列。
2. `strategy_fm_order_receive_di` 的最大 `inc_day` 是 2025-11-17，说明该桥表没有跟随其他表更新到 2026-07-21。
3. `strategy_fm_purchase_order_tmp` 实时 52 列，且下游读取的 `strategy_fm_dim_order_saleable` 是另一张实时 26 列的表；同步目标问题仍然存在。
4. `strategy_fm_dim_store_profile` API 返回的列名集合出现重复字段名，必须通过 `SHOW FULL COLUMNS` 的序号确认目标表是否真的存在重复列或 API 做了列名覆盖。
5. 以上结果来自 StarRocks QDM API 实时镜像，不是本地 DuckDB，也不是服务器 `/opt/fm/data/fm.duckdb` 的计算结果。

