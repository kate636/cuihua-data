-- ============================================================================
-- 4 张 hive 表同步到商分库（FM 门店 + 2026-04-20 单日）
--
-- 源表 → 目标表
-- ─────────────────────────────────────────────────────────────────────────────
-- 1. hive.dal.dal_receive_sale_di                     → strategy_fm_receive_sale_di         (验销关系)
-- 2. hive.dal.dal_store_order_receive_di              → strategy_fm_order_receive_di        (订验关系)
-- 3. hive.dim.dim_store_article_convert_info_da       → strategy_fm_dim_article_convert     (单位转换)
-- 4. hive.dal.dal_activity_article_order_sale_info_di → strategy_fm_activity_order_sale_di  (宽表)
--
-- 门店过滤：INNER JOIN default_catalog.ads_business_analysis.chdj_store_info 取 FM 白名单
--          当前只有 A3XV 一家店，后续新门店自动纳入
-- 日期：2026-04-20
--
-- 写法：用 CTAS（CREATE TABLE AS SELECT）让引擎根据源表自动推导字段类型，
--       避免手写 DDL。如果库里已有同名表则 DROP 后重建。
-- ============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. 验销关系 dal_receive_sale_di
--    业务：验收条码 -> 销售条码 的理论进货量 / 进货金额
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS default_catalog.ads_business_analysis.strategy_fm_receive_sale_di;

CREATE TABLE default_catalog.ads_business_analysis.strategy_fm_receive_sale_di AS
SELECT t.*
FROM hive.dal.dal_receive_sale_di t
INNER JOIN default_catalog.ads_business_analysis.chdj_store_info fm
        ON t.store_id = fm.store_id
WHERE t.inc_day = '2026-04-20'
  AND fm.store_no = 'food mart';


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. 订验关系 dal_store_order_receive_di
--    业务：订购条码 -> 验收条码 的数量占比
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS default_catalog.ads_business_analysis.strategy_fm_order_receive_di;

CREATE TABLE default_catalog.ads_business_analysis.strategy_fm_order_receive_di AS
SELECT t.*
FROM hive.dal.dal_store_order_receive_di t
INNER JOIN default_catalog.ads_business_analysis.chdj_store_info fm
        ON t.store_id = fm.store_id
WHERE t.inc_day = '2026-04-20'
  AND fm.store_no = 'food mart';


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. 单位转换 dim_store_article_convert_info_da
--    业务：门店 × 商品 的单位互转比率（_da 全量快照）
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS default_catalog.ads_business_analysis.strategy_fm_dim_article_convert;

CREATE TABLE default_catalog.ads_business_analysis.strategy_fm_dim_article_convert AS
SELECT t.*
FROM hive.dim.dim_store_article_convert_info_da t
INNER JOIN default_catalog.ads_business_analysis.chdj_store_info fm
        ON t.store_id = fm.store_id
WHERE t.inc_day = '2026-04-20'
  AND fm.store_no = 'food mart';


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. 最终宽表 dal_activity_article_order_sale_info_di
--    业务：活动 × 商品 × 订单 × 销售 全链路宽表
-- ─────────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS default_catalog.ads_business_analysis.strategy_fm_activity_order_sale_di;

CREATE TABLE default_catalog.ads_business_analysis.strategy_fm_activity_order_sale_di AS
SELECT t.*
FROM hive.dal.dal_activity_article_order_sale_info_di t
INNER JOIN default_catalog.ads_business_analysis.chdj_store_info fm
        ON t.store_id = fm.store_id
WHERE t.inc_day = '2026-04-20'
  AND fm.store_no = 'food mart';


-- ─────────────────────────────────────────────────────────────────────────────
-- 验证
-- ─────────────────────────────────────────────────────────────────────────────

-- 行数概览
SELECT 'strategy_fm_receive_sale_di'         AS tbl, COUNT(*) AS c FROM default_catalog.ads_business_analysis.strategy_fm_receive_sale_di
UNION ALL SELECT 'strategy_fm_order_receive_di',        COUNT(*) FROM default_catalog.ads_business_analysis.strategy_fm_order_receive_di
UNION ALL SELECT 'strategy_fm_dim_article_convert',     COUNT(*) FROM default_catalog.ads_business_analysis.strategy_fm_dim_article_convert
UNION ALL SELECT 'strategy_fm_activity_order_sale_di',  COUNT(*) FROM default_catalog.ads_business_analysis.strategy_fm_activity_order_sale_di;

-- 各表字段列表（方便告诉我字段名，我据此写 extractor）
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_receive_sale_di;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_order_receive_di;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_dim_article_convert;
SHOW FULL COLUMNS FROM default_catalog.ads_business_analysis.strategy_fm_activity_order_sale_di;
