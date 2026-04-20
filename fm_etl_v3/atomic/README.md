# atomic — 原子域提取层

通过 `ApiConnector` 调用 QDM BI API，按日期分批提取原始数据，落到 DuckDB 对应的 `atomic_*` 表。每张表是一个独立的业务域，粒度均为 **门店 × 日期 × article_id**。

## WAF 限制

所有发往 API 的 SQL 必须遵守：

- **禁止 `CASE WHEN`**，改用 `IF(condition, true_val, false_val)`
- 多分支判断用嵌套 `IF()`：`IF(c1, v1, IF(c2, v2, v3))`
- `IN (...)` 列表不超过 300 个值

`sales_extractor.py`、`scm_extractor.py`、`promo_extractor.py` 中已完成转换。

## 基类 `_base.py` — BaseExtractor

所有提取器继承此类，子类只需声明 `TARGET_TABLE` 和实现 `_fetch_sql(start, end, yesterday)`。

`extract()` 方法的行为：

1. 用 `split_date_range` 将区间切成 7 天一段。
2. 每段执行一次 `_fetch_sql` → `ApiConnector.query()` → `DuckDBStore.load_df(..., mode="replace_partition")`。
3. 支持重跑：相同日期分区的旧数据先删后插，幂等。

## 提取器一览

| 类名 | TARGET_TABLE | 源表 | 含 CASE WHEN（已转换）|
|------|-------------|------|----------------------|
| `SalesExtractor` | `atomic_sales` | `hive.dsl.dsl_transaction_non_daily_store_order_details_di` | ✅ 已转 IF() |
| `InventoryExtractor` | `atomic_inventory` | `hive.dsl.dsl_transaction_non_daily_store_article_purchase_di` | 无 |
| `ScmExtractor` | `atomic_scm` | `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di` | ✅ 已转 IF() |
| `LossExtractor` | `atomic_loss` | `hive.dal.dal_transaction_store_article_lost_di` | 无 |
| `ComposeExtractor` | `atomic_compose` | `hive.dsl.dsl_transaction_sotre_article_compose_info_di` | 无 |
| `AllowanceExtractor` | `atomic_allowance` | `hive.dal.dal_activity_article_order_sale_info_di` | 无 |
| `PromoExtractor` | `atomic_promo` | `hive.dsl.dsl_promotion_order_item_article_sale_info_di` | ✅ 已转 IF() |
| `CostPriceExtractor` | `atomic_cost_price` | `hive.ods_sc_db.t_shop_inventory_sku_pool` | 无 |
| `PriceExtractor` | `atomic_price` | `hive.dim.dim_store_article_price_info_da` | 无 |

## DimsExtractor 特殊说明

`DimsExtractor` 不继承 `BaseExtractor`，提供 `extract_all(yesterday, start, end)` 一次性加载所有维度快照：

| DuckDB 表 | 源表 | 用途 |
|-----------|------|------|
| `dim_store_list` | `hive.dim.dim_chdj_store_list_di` | 翠花门店白名单 |
| `dim_day_clear` | `hive.dim.dim_day_clear_article_list_di` | 商品日清/非日清标签 |
| `dim_goods` | `hive.dim.dim_goods_information_have_pt` | 商品主数据 |
| `dim_store_profile` | `hive.dim.dim_store_profile` | 门店属性（区域/城市） |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` | 翠花门店编号、标签 |
| `dim_calendar` | `hive.dim.dim_calendar` | 日历维度（周/月/年） |
| `dim_saleable` | `hive.ods_sc_db.t_purchase_order_item_tmp` | 门店可售商品列表 |

## 过滤规则

- **物料类排除**：`category_level1_id NOT IN ('70'~'77')`，线下渠道才排除，线上不排除。
- **翠花门店过滤**：合并阶段（`calculated/merge.py`）通过 INNER JOIN `dim_store_list` 实现。
- **日清标签**：原子层 `day_clear` 字段直接来自源表事务流水，维度表 `dim_day_clear` 作为后备。
