-- ============================================================================
-- BOM 关系维度表 同步到商分库
-- 源表: hive.dim.dim_store_article_bom_relation
-- 目标: default_catalog.ads_business_analysis.strategy_fm_dim_bom_relation
-- 粒度: store_id × parent_article_id × sub_article_id × inc_day
-- 业务: 定义 parent (原料/整猪) -> sub (可售分割部位) 的出肉率 & 成本比例
-- 典型场景: 猪肉 (整头猪 -> 排骨/五花肉/碎油...)
-- ============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 【方案一】增量分区建表（推荐）
-- 每日 T+1 跑一次，按 inc_day 分区，保留历史快照
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS default_catalog.ads_business_analysis.strategy_fm_dim_bom_relation (
    store_id                      VARCHAR       COMMENT '门店编码',
    category_level1_id            VARCHAR       COMMENT 'bom头商品大类id',
    category_level1_description   VARCHAR       COMMENT 'bom头商品大类名称',
    category_level2_id            VARCHAR       COMMENT 'bom头商品中类id',
    category_level2_description   VARCHAR       COMMENT 'bom头商品中类名称',
    category_level3_id            VARCHAR       COMMENT 'bom头商品小类id',
    category_level3_description   VARCHAR       COMMENT 'bom头商品小类名称',
    parent_article_id             VARCHAR       COMMENT 'bom头商品(原料)',
    parent_article_unit           VARCHAR       COMMENT 'bom头商品单位',
    sub_article_id                VARCHAR       COMMENT 'bom子商品(销售)',
    sub_article_unit              VARCHAR       COMMENT 'bom子商品单位',
    dressing_rate                 DOUBLE        COMMENT '标准出肉率(%)，同一parent下所有sub合计≈100',
    cost_rate                     DOUBLE        COMMENT '成本比例(%)',
    bom_type                      INT           COMMENT '1门店bom 2仓bom 3非门店非仓bom',
    split_mode                    VARCHAR       COMMENT 'bom类型 10一拆多 20一拆一',
    sp_level                      INT           COMMENT 'BI店铺等级 1实体 2菜吧 3B端 4虚拟 5测试 6虚拟仓',
    inc_day                       VARCHAR       COMMENT '增量日期(分区字段)'
)
PARTITIONED BY (inc_day);


-- ─── 每日增量写入 ────────────────────────────────────────────────────────────
-- 参数: ${biz_date}，建议 T+1 执行（如 04-21 凌晨写入 inc_day=2026-04-21 分区）
INSERT OVERWRITE TABLE default_catalog.ads_business_analysis.strategy_fm_dim_bom_relation
PARTITION (inc_day = '${biz_date}')
SELECT
    store_id,
    category_level1_id,
    category_level1_description,
    category_level2_id,
    category_level2_description,
    category_level3_id,
    category_level3_description,
    parent_article_id,
    parent_article_unit,
    sub_article_id,
    sub_article_unit,
    CAST(dressing_rate AS DOUBLE)        AS dressing_rate,
    CAST(cost_rate     AS DOUBLE)        AS cost_rate,
    CAST(bom_type      AS INT)           AS bom_type,
    split_mode,
    CAST(sp_level      AS INT)           AS sp_level
FROM hive.dim.dim_store_article_bom_relation
WHERE inc_day = '${biz_date}'
  AND store_id            IS NOT NULL
  AND parent_article_id   IS NOT NULL
  AND sub_article_id      IS NOT NULL
  AND bom_type IN (1, 2, 3)
  AND sp_level = 1              -- 仅保留实体门店
;


-- ─────────────────────────────────────────────────────────────────────────────
-- 【方案二】首次回刷历史分区（按需执行一次）
-- 把 hive 源表近 N 天数据一次性灌到商分库
-- ─────────────────────────────────────────────────────────────────────────────

INSERT OVERWRITE TABLE default_catalog.ads_business_analysis.strategy_fm_dim_bom_relation
PARTITION (inc_day)
SELECT
    store_id,
    category_level1_id,
    category_level1_description,
    category_level2_id,
    category_level2_description,
    category_level3_id,
    category_level3_description,
    parent_article_id,
    parent_article_unit,
    sub_article_id,
    sub_article_unit,
    CAST(dressing_rate AS DOUBLE)        AS dressing_rate,
    CAST(cost_rate     AS DOUBLE)        AS cost_rate,
    CAST(bom_type      AS INT)           AS bom_type,
    split_mode,
    CAST(sp_level      AS INT)           AS sp_level,
    inc_day
FROM hive.dim.dim_store_article_bom_relation
WHERE inc_day BETWEEN '2026-04-01' AND '2026-04-21'   -- 回刷区间，按需调整
  AND store_id            IS NOT NULL
  AND parent_article_id   IS NOT NULL
  AND sub_article_id      IS NOT NULL
  AND bom_type IN (1, 2, 3)
  AND sp_level = 1
;


-- ─────────────────────────────────────────────────────────────────────────────
-- 数据质量校验（写入后跑一下）
-- ─────────────────────────────────────────────────────────────────────────────

-- 校验 1：同一 parent 下所有 sub 的 dressing_rate 合计应 ≈ 100（允许 ±1 误差）
SELECT
    inc_day, store_id, parent_article_id,
    COUNT(DISTINCT sub_article_id)          AS sub_cnt,
    ROUND(SUM(dressing_rate), 3)            AS dressing_sum
FROM default_catalog.ads_business_analysis.strategy_fm_dim_bom_relation
WHERE inc_day = '${biz_date}'
GROUP BY inc_day, store_id, parent_article_id
HAVING ROUND(SUM(dressing_rate), 3) NOT BETWEEN 99 AND 101
ORDER BY ABS(100 - SUM(dressing_rate)) DESC
LIMIT 50;

-- 校验 2：分门店 / 分大类 行数概览
SELECT
    inc_day,
    store_id,
    category_level1_description,
    COUNT(*)                                AS bom_rows,
    COUNT(DISTINCT parent_article_id)       AS parent_cnt,
    COUNT(DISTINCT sub_article_id)          AS sub_cnt
FROM default_catalog.ads_business_analysis.strategy_fm_dim_bom_relation
WHERE inc_day = '${biz_date}'
GROUP BY inc_day, store_id, category_level1_description
ORDER BY store_id, bom_rows DESC;
