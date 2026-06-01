-- ============================================================================
-- strategy_fm_* 表数据写入脚本（修正版）
-- 日期: 2026-04-23
-- 目标: 将 Hive 源库数据写入 StarRocks 商分数据库
-- 字段映射: 基于 fm_etl_v3 extractor 实际使用的字段名
-- ============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. 销售 strategy_fm_sales_di
-- 源表: hive.dsl.dsl_transaction_non_daily_store_order_details_di
-- 字段: inc_day, store_id, abi_article_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_sales_di
SELECT *
FROM hive.dsl.dsl_transaction_non_daily_store_order_details_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. 进货验收 strategy_fm_purchase_di
-- 源表: hive.dsl.dsl_transaction_non_daily_store_article_purchase_di
-- 字段: inc_day, store_id, sale_article_id, business_date, day_clear
-- 注意: 该表有两个日期字段：inc_day（分区键）和 business_date（业务日期）
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_purchase_di
SELECT *
FROM hive.dsl.dsl_transaction_non_daily_store_article_purchase_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. SAP 出入库 strategy_fm_scm_di
-- 源表: hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di
-- 字段: inc_day, store_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_scm_di
SELECT *
FROM hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. SCM 差异调整 strategy_fm_scm_adjust_di
-- 源表: hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di
-- 字段: inc_day, store_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_scm_adjust_di
SELECT *
FROM hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. 损耗 strategy_fm_loss_di
-- 源表: hive.dal.dal_transaction_store_article_lost_di
-- 字段: inc_day, store_id, article_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_loss_di
SELECT *
FROM hive.dal.dal_transaction_store_article_lost_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. 加工转换 strategy_fm_compose_di
-- 源表: hive.dsl.dsl_transaction_sotre_article_compose_info_di
-- 字段: inc_day, store_id, article_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_compose_di
SELECT *
FROM hive.dsl.dsl_transaction_sotre_article_compose_info_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. 活动让利 strategy_fm_allowance_di
-- 源表: hive.dal.dal_activity_article_order_sale_info_di
-- 字段: inc_day, store_id, sale_article_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_allowance_di
SELECT *
FROM hive.dal.dal_activity_article_order_sale_info_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. 促销 strategy_fm_promo_di
-- 源表: hive.dsl.dsl_promotion_order_item_article_sale_info_di
-- 字段: inc_day, shop_id (不是 store_id), sku_code (不是 article_id)
-- ⭐ 注意: 源表用 shop_id，需确认是否与 store_id 同义
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_promo_di
SELECT *
FROM hive.dsl.dsl_promotion_order_item_article_sale_info_di
WHERE inc_day = '2026-04-23'
  AND shop_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 9. 库存成本价池 strategy_fm_inventory_pool_di
-- 源表: hive.ods_sc_db.t_shop_inventory_sku_pool
-- 字段: inc_day, shop_id, sku_code, inventory_date
-- ⭐ 注意: 源表用 shop_id 和 sku_code，inventory_date 是业务日期
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_inventory_pool_di
SELECT *
FROM hive.ods_sc_db.t_shop_inventory_sku_pool
WHERE inc_day = '2026-04-23'
  AND shop_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 10. 门店商品价格 strategy_fm_price_da
-- 源表: hive.dim.dim_store_article_price_info_da
-- 字段: inc_day, store_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_price_da
SELECT *
FROM hive.dim.dim_store_article_price_info_da
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 11. 翠花门店列表 strategy_fm_dim_store_list
-- 源表: hive.dim.dim_chdj_store_list_di
-- 字段: inc_day, store_id
-- ⚠ 此表可能不存在，若报错请跳过
-- ─────────────────────────────────────────────────────────────────────────────
-- INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_store_list
-- SELECT *
-- FROM hive.dim.dim_chdj_store_list_di
-- WHERE inc_day = '2026-04-23'
--   AND store_id IN (
--       SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
--       WHERE store_no = 'food mart'
--   );

-- ─────────────────────────────────────────────────────────────────────────────
-- 12. 日清商品清单 strategy_fm_dim_day_clear
-- 源表: hive.dim.dim_day_clear_article_list_di
-- 字段: inc_day, store_id, article_id, business_date
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_day_clear
SELECT *
FROM hive.dim.dim_day_clear_article_list_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 13. 门店画像 strategy_fm_dim_store_profile
-- 源表: hive.dim.dim_store_profile
-- 字段: inc_day, sp_store_id (映射到 store_id)
-- ⭐ 注意: 源表用 sp_store_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_store_profile
SELECT *
FROM hive.dim.dim_store_profile
WHERE inc_day = '2026-04-23'
  AND sp_store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 14. 可售商品 strategy_fm_dim_saleable
-- 源表: hive.ods_sc_db.t_purchase_order_item_tmp
-- 字段: inc_day, shop_id, sku_code
-- ⭐ 注意: 源表用 shop_id 和 sku_code
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_saleable
SELECT *
FROM hive.ods_sc_db.t_purchase_order_item_tmp
WHERE inc_day = '2026-04-23'
  AND shop_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 15. 商品主数据 strategy_fm_dim_goods
-- 源表: hive.dim.dim_goods_information_have_pt
-- 字段: inc_day, article_id（不过滤门店，全量）
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_goods
SELECT *
FROM hive.dim.dim_goods_information_have_pt
WHERE inc_day = '2026-04-23';

-- ─────────────────────────────────────────────────────────────────────────────
-- 16. 日历 strategy_fm_dim_calendar
-- 源表: hive.dim.dim_calendar
-- 字段: day_date (映射到 business_date)
-- ⭐ 注意: 源表用 day_date，不是 date_key
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_calendar
SELECT *
FROM hive.dim.dim_calendar
WHERE day_date = '2026-04-23';

-- ─────────────────────────────────────────────────────────────────────────────
-- 17. BOM 收货销售关系 strategy_fm_receive_sale_di（v4 新增）
-- 源表: hive.dal.dal_receive_sale_di
-- 字段: inc_day, store_id, article_id (parent), sale_article_id (sub)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_receive_sale_di
SELECT *
FROM hive.dal.dal_receive_sale_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 18. 订验关系 strategy_fm_order_receive_di（v4 新增）
-- 源表: hive.dal.dal_store_order_receive_di
-- 字段: inc_day, store_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_order_receive_di
SELECT *
FROM hive.dal.dal_store_order_receive_di
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 19. 单位转换 strategy_fm_dim_article_convert（v4 新增）
-- 源表: hive.dim.dim_store_article_convert_info_da
-- 字段: inc_day, store_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_fm_dim_article_convert
SELECT *
FROM hive.dim.dim_store_article_convert_info_da
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- 20. BOM 关系边 strategy_dim_store_article_bom_relation（v3.1 新增）
-- 源表: hive.dim.dim_store_article_bom_relation
-- 字段: inc_day, store_id, parent_article_id, sub_article_id
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO default_catalog.ads_business_analysis.strategy_dim_store_article_bom_relation
SELECT *
FROM hive.dim.dim_store_article_bom_relation
WHERE inc_day = '2026-04-23'
  AND store_id IN (
      SELECT store_id FROM default_catalog.ads_business_analysis.chdj_store_info
      WHERE store_no = 'food mart'
  );


-- ============================================================================
-- 验证写入结果
-- ============================================================================
SELECT 'strategy_fm_sales_di' AS table_name, COUNT(*) AS row_count
FROM default_catalog.ads_business_analysis.strategy_fm_sales_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_purchase_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_purchase_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_scm_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_scm_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_loss_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_loss_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_compose_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_compose_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_allowance_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_allowance_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_promo_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_promo_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_inventory_pool_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_inventory_pool_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_price_da', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_price_da
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_fm_receive_sale_di', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_fm_receive_sale_di
WHERE inc_day = '2026-04-23'
UNION ALL
SELECT 'strategy_dim_store_article_bom_relation', COUNT(*)
FROM default_catalog.ads_business_analysis.strategy_dim_store_article_bom_relation
WHERE inc_day = '2026-04-23';


-- ============================================================================
-- 字段映射说明（关键差异）
-- ============================================================================
--
-- | 源表 | 源表字段 | 目标表字段 |
-- |------|---------|-----------|
-- | hive.dsl.dsl_promotion_order_item_article_sale_info_di | shop_id | store_id |
-- | hive.dsl.dsl_promotion_order_item_article_sale_info_di | sku_code | article_id |
-- | hive.ods_sc_db.t_shop_inventory_sku_pool | shop_id | store_id |
-- | hive.ods_sc_db.t_shop_inventory_sku_pool | sku_code | article_id |
-- | hive.ods_sc_db.t_shop_inventory_sku_pool | inventory_date | business_date |
-- | hive.ods_sc_db.t_purchase_order_item_tmp | shop_id | store_id |
-- | hive.ods_sc_db.t_purchase_order_item_tmp | sku_code | article_id |
-- | hive.dim.dim_store_profile | sp_store_id | store_id |
-- | hive.dim.dim_calendar | day_date | business_date |
--
-- 如果 INSERT 时报字段不匹配错误，请检查源表是否包含上述字段。
-- 部分表可能用 `SELECT *` 导致字段顺序不一致，建议显式列出字段名。