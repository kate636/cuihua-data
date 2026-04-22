# strategy_fm_* → atomic_* 字段映射手册

本文件**不是** `SHOW FULL COLUMNS` 自动生成的那份（见
[strategy_fm_tables.md](strategy_fm_tables.md)），而是 Phase B 的人工核对结论：

> 每张 `strategy_fm_*` 的字段 → 现有 `atomic_*` / 下游 `t_calc_*`/`t_fm_*` 所依赖字段 的映射，
> 以及字段更名 / 缺失 / 可以直接沿用 的具体备注。

---

## 0. Phase A 探测总结（2026-04-20 为探测日）

1. **15 张表的行数 100% 匹配用户给定基线**（误差 0.0%）。见
   [strategy_fm_tables.md](strategy_fm_tables.md) 的概览表。
2. **所有 `strategy_fm_*_di` 目前只保留 1 天（`2026-04-20`）** — 不是历史全量。
   - `MAX(inc_day) = MIN(inc_day) = 2026-04-20`，`COUNT(DISTINCT inc_day) = 1`
   - 结论：本轮本地试跑只能跑这一天，以后 QDM 侧保留多天历史后再扩展。
3. **翠花目前只有 1 家门店**（`store_id = A3XV`，名称"广州滨江宏岸店"）。
   - `strategy_fm_dim_store_profile` 只有 1 行是**正常**的，不是之前怀疑的"SCD 增量只写最新"。
4. **`strategy_fm_scm_adjust_di` 整表 0 行**（`MAX(inc_day)` 返回 `None`）。
   - 结论：该表存在但上游尚未开始写入数据；extractor 写好后不会报错（空 DataFrame 会被 `load_df` 静默跳过）。
5. **字段 comment 全部为空**：`SHOW FULL COLUMNS FROM ...` 返回的 `comment` 列
   所有 15 张表、所有字段都是空串。
   - 结论：**QDM 元数据层没有维护中文注释**，中英对照只能靠"字段名语义 + 抽样值 + 旧 atomic extractor 字段名"三方交叉推导。
   - 这份 mapping 就是推导后的结果。

---

## 1. `strategy_fm_sales_di` → `atomic_sales`

- **QDM 表粒度**：**订单行级**（order_id × order_item_id × sku）。1,533 行 / 1 店 / 399 个 SKU ≈ 3.8 行/SKU。需要在 extractor 里继续 `GROUP BY store_id, inc_day, abi_article_id, day_clear`。
- **替换要点**：
  - 主表：`hive.dsl.dsl_transaction_non_daily_store_order_details_di` → `strategy_fm_sales_di`。
  - 商品维度辅表：`hive.dim.dim_goods_information_have_pt`（用于 unit_weight / sale_unit / category_level1_id）→ `strategy_fm_dim_goods`（字段名完全对齐，无需改列名）。
  - 条件 `WHERE inc_day = '{yesterday}'` 在新 `dim_goods` 上要去掉（新表只有最新一天，加了反而会没数据）。可以考虑直接不 JOIN 而用翠花 SKU 的 `NOT IN` 白名单，但为保持老口径完全一致，仍然 JOIN。
- **字段对齐**（全部命中，无缺失）：

| `atomic_sales` 字段 | 语义 | `strategy_fm_sales_di` 源字段 | 备注 |
|---|---|---|---|
| `store_id` | 门店 ID | `store_id` | 直接用 |
| `business_date` | 业务日 | `inc_day`（老 extractor 也是用 `inc_day AS business_date`） | 或用 `business_date` 字段，二者一致 |
| `article_id` | 商品 ID | `abi_article_id` | 老 extractor 也是 `abi_article_id AS article_id` |
| `sale_qty` | 销量（规格单位） | `SUM(qty_spec)` | |
| `sale_piece_qty` | 销量（销售单位） | `SUM(qty)` | |
| `return_sale_qty/amt` | 退货数量/金额 | `SUM(return_sale_qty)`, `SUM(return_sale_amt)` | |
| `gift_qty` | 赠品数量 | `SUM(gift_qty)` | |
| `online_sale_qty` / `offline_sale_qty` | 线上/线下销量 | `IF(online_flag='Y', qty*spec_num, 0)` / `IF(online_flag='N', qty_spec, 0)` | |
| `bf19/af19/bf12_*` | 19 点前/后、12 点前销量金额 | `af19_sales_qty/amt`, `SUBSTR(inc_time,12,2) < '19'/'12'` | 老 SQL 原样复用 |
| `sales_weight` | 销售重量 | 需 JOIN `strategy_fm_dim_goods.unit_weight / sale_unit` | 逻辑不变 |
| `sale_amt` | 销售额 | `SUM(sales_amt)` | |
| `original_price_sale_amt` | 原价金额 | `SUM(p_lp_sub_amt)` | |
| `vip_discount_amt`, `hour_discount_amt`, `discount_amt` | 各类折扣 | 同名字段 | |
| `actual_amount` | 实际金额（扣 f_sub/f_promo 后） | `sales_amt - f_sub_amt - f_promo_sub_amt` 口径保持 | 新表字段齐全 |
| `member_sale_amt`, `bf19_member_sale_amt` | 会员销售 | `IF(customer_phone IS NOT NULL …)` | |
| `offline_original_amt` | 线下原价金额 | `IF(online_flag='N', p_lp_sub_amt, 0)` | |
| `store_paylevel_discount`, `company_paylevel_discount` | 门店/公司支付级折扣 | 同名字段 | |
| `af19_sale_amt`, `bf19_sale_amt`, `bf19_offline_sale_amt`, `bf12_sale_amt`, `bf19_sale_piece_qty` | 各类分时段金额/件数 | 以 `af19_sales_amt/qty`, `SUBSTR(inc_time,12,2)` 计算 | |
| `last_sysdate` | 最后收银时间 | `MAX(IF(online_flag='N', pay_at, NULL))` | |
| `day_clear` | 日清标签 | 同名字段 | |

- **缺失字段**：无。所有老 SQL 引用的字段在新表都有同名或有口径对应项。
- **关键行为差异**：新表是**订单行级**（有 `order_id`, `customer_id`, `customer_phone`, `inc_time` 等），继续走 GROUP BY 聚合即可。

---

## 2. `strategy_fm_purchase_di` → `atomic_inventory`

- **粒度**：1,821 行 / 1 店 / 1,748 个 `article_id` ≈ 1.04 行/SKU，**基本等同于 (store_id × article_id × business_date × day_clear) 粒度**。可直接用，不强求 GROUP BY（但为了幂等，extractor 里仍 `SUM(...)`）。
- **替换要点**：
  - 主表：`hive.dsl.dsl_transaction_non_daily_store_article_purchase_di` → `strategy_fm_purchase_di`。
  - 辅表 `dim_goods` 用于 `category_level1_id NOT IN ('70'..'77')` 过滤 → `strategy_fm_dim_goods`。
- **字段对齐**：

| `atomic_inventory` 字段 | 源 | 备注 |
|---|---|---|
| `store_id` | `store_id` | |
| `business_date` | `business_date` | 新表已有，不用 `inc_day` |
| `article_id` | `sale_article_id`（老 extractor 也是这么映的） | |
| `receive_qty` | `sale_article_qty` | 老 extractor 别名 |
| `init_stock_qty` | `init_stock_qty` | |
| `end_stock_qty` | `end_stock_qty` | |
| `day_clear` | `day_clear` | |

- **新增可选字段**：`init_stock_amt`, `end_stock_amt`, `inventory_cost`, `avg_inbound_price`, `sale_article_purchase_amt`。
  - 本轮**不用**（下游 `t_calc_*` 没引用），不写入 `atomic_inventory` 以保持兼容。

---

## 3. `strategy_fm_scm_di` → `atomic_scm`

- **粒度**：300 行 / 1 店 / 300 个 `article_id` = **已经聚合到 (store, date, article) 粒度**。老 extractor 的外层 GROUP BY 变成"形式上的" `SUM(COALESCE(x,0))`，实际上每组只有 1 行。
- **替换要点**：
  - 主表：`hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di` → `strategy_fm_scm_di`。
  - **差异调整**：老 extractor `LEFT JOIN hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di` 算 `adjustment_amt`；**新表 `strategy_fm_scm_di.adjustment_amt` 已经预先合并好**。
    - 结论：**不再 JOIN `strategy_fm_scm_adjust_di`**，直接用 `strategy_fm_scm_di.adjustment_amt`（昨日 300 行样本验证过该列存在）。
    - `strategy_fm_scm_adjust_di` 本轮作为独立原子表单独抽取一份到 DuckDB（按计划要求），但不参与 `AtomicMerger`。
- **价格字段口径**：老 extractor 通过 `SUM(outstock_amt) / SUM(qty)` 自己算单价（`outstock_unit_price` 等）。但**新表 `strategy_fm_scm_di` 里没有 `outstock_amt` 明细字段，却有 `out_stock_pay_amt` 等**。重命名对照：

| `atomic_scm` 字段（老 extractor 产出） | 新表 `strategy_fm_scm_di` 字段 | 口径 |
|---|---|---|
| `original_outstock_qty/amt` | 同名 | 直接用 |
| `promotion_outstock_qty/amt` | 同名 | 直接用 |
| `gift_outstock_qty` | 同名 | 直接用 |
| `return_stock_qty` | `return_stock_qty` | 直接用 |
| `store_return_qty_shop` | 同名 | 直接用 |
| `store_order_qty`, `order_qty_payean` | 同名 | 直接用 |
| `outstock_unit_price` | `IF(total_outstock_qty=0, 0, out_stock_pay_amt / total_outstock_qty)` | 有税口径 |
| `outstock_unit_price_notax` | `IF(total_outstock_qty=0, 0, out_stock_pay_amt_notax / total_outstock_qty)` | 无税口径 |
| `outstock_cost_price` | `IF(total_outstock_qty=0, 0, out_stock_amt_cb / total_outstock_qty)` | 成本价有税 |
| `outstock_cost_price_notax` | `IF(total_outstock_qty=0, 0, out_stock_amt_cb_notax / total_outstock_qty)` | 成本价无税 |
| `return_unit_price`, `return_unit_price_notax` | `return_stock_pay_amt / return_stock_qty`, `return_stock_pay_amt_notax / return_stock_qty` | |
| `return_cost_price`, `return_cost_price_notax` | `return_stock_amt_cb / return_stock_qty`, `return_stock_amt_cb_notax / return_stock_qty` | |
| `order_unit_price` | `IF(store_order_qty=0, 0, order_amt / store_order_qty)` | |
| `scm_promotion_amt_total` | 同名（已预合计） | |
| `scm_promotion_amt_gift` | 同名 | |
| `scm_bear_amt`, `vendor_bear_amt`, `business_bear_amt`, `market_bear_amt` | 同名 | |
| `vender_bear_gift_amt`, `scm_bear_gift_amt` | 同名 | |
| `adjustment_amt` | 同名（新表已经合并好，**不再 JOIN adjust 表**） | |

- **缺失字段**：无实质缺失，但注意 `total_outstock_qty` 是新表里总出库数量，比老 extractor 里三项加起来更靠谱。
- **新增可选字段**（可留将来用）：`qdm_bear_*`, `miss_stock_qty/amt`, `scm_promotion_cost`。

---

## 4. `strategy_fm_scm_adjust_di` → `atomic_scm_adjust`（**新增**）

- **粒度**：当前 0 行。schema 是 (business_date, store_id, dc_id, matnr, article_id, tax, adjustment_amt, adjustment_amt_notax, new_sp_store_id, inc_day)。
- **处理策略**：按计划要求新增 `ScmAdjustExtractor` → 写 DuckDB `atomic_scm_adjust`，**但不并入 `AtomicMerger`**（`strategy_fm_scm_di.adjustment_amt` 已经是合计口径）。Step 2 注册、Step 3 不读，留作后续调查/审计用。

---

## 5. `strategy_fm_loss_di` → `atomic_loss`

- **粒度**：219 行 / 1 店 / 219 个 `article_id` = **已经聚合好**。
- **字段对齐**：

| `atomic_loss` 字段 | 源 | 备注 |
|---|---|---|
| `store_id` | `store_id` | |
| `business_date` | — | **新表没有 `business_date` 字段**，但有 `inc_day`；老 extractor 用 `inc_day AS business_date`，新版一致。 |
| `article_id` | `article_id` | |
| `know_lost_qty` | `know_lost_qty` | |

- **新增可选字段**（新表多出来的）：`unknow_lost_qty`, `unknow_lost_amt`, `know_lost_amt`, `article_name`, `category_level1_id/description`。
  - 按计划约定："`t_calc_*`/`t_fm_*` 不动"，所以不把 `unknow_lost_qty` 写入 `atomic_loss`（下游 `InventoryCalculator` 是用库存方程反推的，写入会导致口径被覆盖）。
- **老 extractor 的 `category_level1_id NOT IN ('70'..'77','98')` 过滤**：新表已经预过滤好（翠花侧 219 行里不会含物料类），可以**去掉**这个 JOIN。

---

## 6. `strategy_fm_compose_di` → `atomic_compose`

- **粒度**：22 行 / 1 店 / 22 个 `article_id` = 已经聚合好。
- **字段对齐**（**无需任何改动**）：

| `atomic_compose` 字段 | 源 | 备注 |
|---|---|---|
| `store_id` / `business_date` / `article_id` | 同名 | |
| `compose_in_qty` / `compose_out_qty` | 同名 | |

- **新增字段**：`compose_in_amt`, `compose_out_amt`（本轮不采，保持兼容）。

---

## 7. `strategy_fm_allowance_di` → `atomic_allowance`

- **粒度**：943 行 / 1 店 / 925 个 `sale_article_id` ≈ 1.02 行/SKU，需要 GROUP BY 聚合。
- **字段对齐**：

| `atomic_allowance` 字段 | 源 | 备注 |
|---|---|---|
| `store_id` / `business_date` | 同名 | |
| `article_id` | `sale_article_id`（老 extractor 一致） | |
| `allowance_amt` | `SUM(split_allowance_amt)` | |

- **翠花门店过滤**：老 extractor 在这里 INNER JOIN 了 `hive.dim.dim_chdj_store_list_di`。新表理论上只含翠花门店（1 家），但为保险起见 extractor 里仍 INNER JOIN `dim_store_list`（DuckDB 内的表，见 §13）。实际执行时不限制结果。

---

## 8. `strategy_fm_promo_di` → `atomic_promo`

- **粒度**：1,545 行 / 1 店 / 399 个 `sku_code`，行级/促销规则级，需要 GROUP BY + IF() 逻辑。
- **字段对齐**（列名有差异）：

| `atomic_promo` 字段 | 源 | 备注 |
|---|---|---|
| `store_id` | `shop_id`（新表列名） | 需 `AS` 重命名 |
| `business_date` | — | 新表只有 `inc_day`；老 extractor 也是用 `inc_day AS business_date` |
| `article_id` | `sku_code` | 需 `AS` 重命名 |
| `member_coupon_shop_amt`, `member_promo_amt`, `member_coupon_company_amt`, `shop_promo_amt`, `no_ordercoupon_company_promotion_amt`, `ordercoupon_shop_promotion_amt`, `ordercoupon_company_promotion_amt` | 老 extractor 基于 `cost_center`, `promo_type`, `promotion_category`, `promo_sub_type`, `promo_ext_prop`, `order_type`, `online_flag`, `p_promo_amt/f_promo_amt`, `store_paylevel_discount` 分别 IF() | 新表这些字段**全部存在**，老 SQL 中的 CASE WHEN (IF()) 逻辑可 1:1 复用 |

- **门店促销 JOIN**：老 SQL `LEFT JOIN hive.dim.dim_store_promotion_info_da t2` 判断 `promotion_code` 来源。新表 `strategy_fm_promo_di` 已预先关联好，含 `promotion_name`, `activity_type`, `activity_level`, `cost_center`, `cost_center_info`, `source` 等。
  - **简化方案**：去掉这个 JOIN。`shop_promo_amt` 改为基于 `cost_center = 'shop' AND promotion_category = 'rule'` 判定。
  - **保守方案**：保留 JOIN `strategy_fm_dim_goods` 做物料类过滤，但删 `promotion_code` 的 JOIN（新表已合并）。
- **本轮采用简化方案**，注释里标 `TODO: 确认 shop_promo_amt 口径是否与老 SQL 1:1`。
- **翠花门店 INNER JOIN `dim_store_list`**：保留，保证只写入翠花的行（当前只会有 1 店）。
- **物料类排除**（`category_level1_id NOT IN ('70'..'77')`）：可以通过 JOIN `strategy_fm_dim_goods`，或相信新表已预过滤。**保守保留 JOIN**。

---

## 9. `strategy_fm_inventory_pool_di` → `atomic_cost_price`

- **粒度**：244,523 行 / 1 店 / 1,769 个 `sku_code` ≈ 138 行/SKU。经排查，每个 (shop_id, sku_code) 有多行是因为**同一 SKU 有多个不同 `inventory_date`**（历史回滚快照全保留）。`id` 字段是物理主键。
- **字段对齐**（列名差异）：

| `atomic_cost_price` 字段 | 源 | 备注 |
|---|---|---|
| `store_id` | `shop_id` | |
| `business_date` | `inventory_date` | 老 extractor 也是 `inventory_date AS business_date` |
| `article_id` | `sku_code` | |
| `cost_price` | `MAX(cost_price)` | 老 extractor 同逻辑 |

- **过滤**：老 extractor `WHERE inc_day = '{yesterday}' AND inventory_date BETWEEN start AND end`；新表 `inc_day` 只有一天（`2026-04-20`），所以 `WHERE inc_day = '{yesterday}'` 可以去掉；`inventory_date BETWEEN start AND end` 保留。
- **聚合**：`GROUP BY shop_id, inventory_date, sku_code` + `MAX(cost_price)` 维持不变。

---

## 10. `strategy_fm_price_da` → `atomic_price`

- **粒度**：5,009 行 / 1 店 / 5,009 个 `sku_code` = 一行一价，**已聚合**。
- **字段对齐**（列名差异）：

| `atomic_price` 字段 | 源 | 备注 |
|---|---|---|
| `store_id` | `shop_id` | |
| `business_date` | — | 新表没有 business_date，只有 `inc_day`。老 extractor 用 `business_date`；**改用 `inc_day AS business_date`** |
| `article_id` | `sku_code` | |
| `current_price`, `yesterday_price`, `dc_original_price`, `original_price` | 同名 | 直接用 |

- **老 `business_date BETWEEN start AND end` 过滤**：改为 `inc_day BETWEEN start AND end`。
- **影响**：下游 merge.py 按 `business_date` 关联；这里用 `inc_day` 当 `business_date`，当前只有 `2026-04-20` 一天，下游跑 `2026-04-20` 能关联上；未来若要跑历史日，需要上游 `strategy_fm_price_da` 开始保留多天分区（否则这张表只能给当天用）。**代码注释里标清这点**。

---

## 11~15. 维度表（`dims_extractor.py`）

### 11. `strategy_fm_dim_day_clear` → DuckDB `dim_day_clear`

- 92,076 行，1 店，92,075 个 article — 基本上是"这个门店的 SKU 清单"（按 category 过滤后的全量），而**不是**"仅日清商品清单"。
- 老 extractor 写的是 `SELECT DISTINCT business_date, store_id, article_id, 1 AS day_clear FROM dim_day_clear_article_list_di`，语义是"这条记录出现 → 是日清"。
- **风险**：如果新表 92K 行代表"全部 SKU 都标 day_clear=1"，下游 `t_atomic_wide.day_clear` 会全部变 1 → `t_fm_sku_dim` 口径失真。
- **决策**（本轮）：
  - 还是从 `strategy_fm_dim_day_clear` 拉取，写入 DuckDB `dim_day_clear` 表，`day_clear = 1`；
  - **但代码注释里标 WARN**：`strategy_fm_dim_day_clear 可能包含全量 SKU，需要与原 dim_day_clear_article_list_di 口径对齐`，Phase D 跑完后对比 `t_atomic_wide` 里 `day_clear='1'` 比例是否与历史相符；
  - 如果比例异常，回 `settings.day_clear_categories_l1/l2` 走分类过滤方案兜底。
- 字段对齐：`business_date / store_id / article_id` 都直接映射。

### 12. `strategy_fm_dim_store_profile` → DuckDB `dim_store_profile`

- 1 行，1 店，字段齐全。
- 字段对齐：

| `dim_store_profile` 字段 | 源 |
|---|---|
| `store_id` | `sp_store_id` |
| `store_name` | `sp_store_name` |
| `manage_area_name` | `manage_area_name` |
| `sap_area_name` | `sap_area_name` |
| `city_description` | `city_description` |

### 13. `dim_store_list`（**保留用 `default_catalog.ads_business_analysis.chdj_store_info`**）

- 按计划约定：`strategy_fm_dim_store_list` 上游未建，本轮不改来源。
- 老 extractor：`SELECT DISTINCT store_id FROM hive.dim.dim_chdj_store_list_di WHERE inc_day BETWEEN start AND end`；
- 新方案：`SELECT DISTINCT store_id FROM default_catalog.ads_business_analysis.chdj_store_info`（无日期过滤，全量翠花门店）。
- 下游 `merge.py` 里 `INNER JOIN chdj_stores ON COALESCE(s.store_id, p.store_id) = cs.store_id` 字段对齐，无需改下游。

### 14. `dim_chdj_store_info`（**保留来源**）

- 老 extractor 直接查 `default_catalog.ads_business_analysis.chdj_store_info`，**本次不动**。

### 15. `strategy_fm_dim_goods` → DuckDB `dim_goods`

- 92,539 行、106 列，全量商品主数据。
- 字段对齐（老 extractor 用了 14 列，新表**全部同名命中**）：`article_id / article_name / category_level1~3_id/description / spu_id / spu_name / blackwhite_pig_id/name / unit_weight / sale_unit`。
- **过滤**：老 extractor 有 `category_level1_id NOT IN ('70'..'77')`，新表不过滤。为保持口径，extractor 里保留这个 `WHERE`。

### 16. `strategy_fm_dim_saleable` → DuckDB `dim_saleable`

- 1,255 行，1 店，1,255 SKU。
- 字段对齐：

| `dim_saleable` 字段 | 源 |
|---|---|
| `store_id` | `shop_id` |
| `article_id` | `sku_code` |

- 老 extractor 查 `hive.ods_sc_db.t_purchase_order_item_tmp`，语义是"可订商品"；新表是"可售商品"。两者是否完全对齐，Phase D 对账时关注；但下游 `t_fm_*` 只用它做 `DISTINCT (store_id, article_id)` 判定，粒度一致，**短期不会影响下游计算**。

### 17. `strategy_fm_dim_calendar` → DuckDB `dim_calendar`

- 全量历史 (2013-01-01 ~ 2036-11-07)，8,712 行（≈ 24 年）。
- 字段对齐（列名差异）：

| `dim_calendar` 字段 | 源 |
|---|---|
| `business_date` | `day_date`（老 extractor `date_key AS business_date`） |
| `week_no`, `week_start_date`, `week_end_date`, `month_wid`, `year_wid` | 同名 |

- 老 extractor `WHERE date_key BETWEEN start AND end`；新表改为 `WHERE day_date BETWEEN start AND end`。

---

## 中英对照字典（按表）

说明：QDM 元数据层 `comment` 为空，以下中文含义为**人工推导**（基于字段名 + 样本 + 旧 atomic extractor 字段语义）。

### `strategy_fm_sales_di`（节选关键列，完整 119 列见 probe 报告）

| 英文 | 中文含义 | 类型 | 说明 |
|---|---|---|---|
| `business_date` / `inc_day` | 业务日 | varchar | 当前两者一致 |
| `store_id` | 门店 ID | varchar | A3XV = 广州滨江宏岸 |
| `sp_store_name` | 门店名称 | varchar | "广州滨江宏岸店" |
| `area_description` / `area_id` | 大区/区 | varchar | |
| `sp_type` / `sp_level` | 门店类型 / 层级 | | |
| `order_id` / `parent_order_id` / `children_order_ids` | 订单号/父/子 | | 订单行级标识 |
| `abi_article_id` | 商品 ID（ABI 规范） | varchar | 映射到 `atomic_sales.article_id` |
| `sale_unit` / `spec_num` | 销售单位 / 规格数 | | `qty * spec_num` = 按规格算的销量 |
| `qty` / `qty_spec` | 销售件数 / 销售规格量 | decimal | |
| `sales_amt` | 销售额 | decimal | 含让利 |
| `p_lp_sub_amt` | 原价金额（不扣折） | decimal | = sale_amt 的 "原价" |
| `p_paid_sub_amt` / `f_paid_sub_amt` / `p_pay_sub_amt` / `f_pay_sub_amt` | 已付/应付 p/f 子金额 | decimal | p=principal, f=freight? |
| `p_promo_sub_amt` / `f_promo_sub_amt` | 促销 p/f 子金额 | decimal | |
| `vip_discount_amt` | 会员折扣金额 | decimal | |
| `hour_discount_amt` | 时段折扣金额 | decimal | 晚市 19 点后折扣 |
| `actual_amount` | 实收金额 | decimal | |
| `return_sale_qty/amt` | 退货数量/金额 | decimal | |
| `af19_sales_amt/qty` | 19 点后销量金额 | decimal | |
| `gift_qty` / `gift_gmv` | 赠品数量/GMV | decimal | |
| `online_flag` | 线上标识 Y/N | varchar | |
| `customer_phone` | 客户手机号 | varchar | 有值代表会员 |
| `inc_time` / `pay_at` / `order_at` | 入库时间/支付/下单时刻 | varchar | 用 SUBSTR 切出 trans_hour |
| `store_paylevel_discount` / `company_paylevel_discount` | 门店/公司支付等级折扣 | decimal | |
| `day_clear` | 日清标签 | varchar | '1' = 当天清货 |

### `strategy_fm_purchase_di`

| 英文 | 中文 |
|---|---|
| `sale_article_id / _name` | 销售 SKU ID/名称 |
| `sale_article_qty` | 销售 SKU 进货数量 |
| `sale_article_purchase_amt` | 销售 SKU 进货金额 |
| `init_stock_qty / amt` | 期初库存数量/金额 |
| `end_stock_qty / amt` | 期末库存数量/金额 |
| `inventory_cost` | 库存成本 |
| `avg_inbound_price` | 平均入库单价 |
| `day_clear` | 日清标签 |

### `strategy_fm_scm_di`

| 英文 | 中文 |
|---|---|
| `original_outstock_qty/amt` | 常规出库数量/金额 |
| `promotion_outstock_qty/amt/price` | 促销出库数量/金额/单价 |
| `gift_outstock_qty` | 赠品出库数量 |
| `total_outstock_qty` | 出库总数量（= 常规+促销+赠品） |
| `out_stock_pay_amt / _notax` | 出库应付金额（含税/不含税） |
| `out_stock_amt_cb / _notax` | 出库成本金额（含税/不含税，CB=cost book） |
| `return_stock_qty / amt` | 退仓数量/金额 |
| `return_stock_pay_amt / _notax` | 退仓应付金额（含税/不含税） |
| `return_stock_amt_cb / _notax` | 退仓成本金额（含税/不含税） |
| `store_order_qty` / `order_qty_payean` | 门店订货数量（按订货单位/按 payean） |
| `order_amt` | 订货金额 |
| `adjustment_amt / _notax` | 差异调整金额（含税/不含税） |
| `scm_promotion_amt_total / _gift` | SAP 让利总金额/赠品部分 |
| `scm_bear_amt` / `vendor_bear_amt` / `business_bear_amt` / `market_bear_amt` | SCM/厂商/商务/市场承担金额 |
| `vender_bear_gift_amt` / `scm_bear_gift_amt` | 厂商/SCM 承担赠品金额 |
| `qdm_bear_*` | QDM 承担费用（正/负/赠品） |

### `strategy_fm_loss_di`

| 英文 | 中文 |
|---|---|
| `know_lost_qty / amt` | 已知损耗数量/金额 |
| `unknow_lost_qty / amt` | 未知损耗数量/金额（由上游库存方程反推，现在下游有自己的反推） |
| `category_level1_id / description` | 一级品类 ID / 名称 |

### `strategy_fm_compose_di`

| 英文 | 中文 |
|---|---|
| `compose_in_qty / amt` | 组合入数量/金额 |
| `compose_out_qty / amt` | 组合出数量/金额 |

### `strategy_fm_allowance_di`（节选）

| 英文 | 中文 |
|---|---|
| `split_allowance_amt` | 拆分后的让利金额（主指标） |
| `activity_type / id / name` | 活动类型/ID/名称 |
| `sale_article_id / name` | 销售 SKU ID/名称 |
| `order_qty / amt` | 订货数量/金额 |
| `split_*` | 拆分到 SKU 级的明细（数量/金额/bf19 分时段等） |
| `profit_amt` / `allowance_profit_amt` | 毛利/让利后毛利 |

### `strategy_fm_promo_di`（节选）

| 英文 | 中文 |
|---|---|
| `shop_id` | 门店 ID |
| `sku_code` | 商品编码 |
| `p_promo_amt` / `f_promo_amt` | 本金促销额 / 运费促销额 |
| `promotion_category` | 促销类别（rule=规则促销） |
| `promo_type` / `promo_sub_type` | 促销类型/子类型（OrderCoupon / n.fold.point 等） |
| `promo_ext_prop` | 促销扩展属性字符串（SUBSTR 取位判活动来源） |
| `cost_center` | 费用承担方（shop / vendor / customer / ...） |
| `promotion_code` / `promotion_code2` | 促销码 |
| `online_flag` / `order_type` | 线上标识 / 订单类型 |
| `store_paylevel_discount` | 门店支付等级折扣 |

### `strategy_fm_inventory_pool_di`

| 英文 | 中文 |
|---|---|
| `shop_id / sku_code` | 门店 ID / SKU |
| `inventory_date` | 库存日期（实际业务日） |
| `cost_price` | 成本价 |
| `sku_name / sub_category_id/name / spec / sales_unit / main_img / gift_flag` | SKU 属性 |

### `strategy_fm_price_da`

| 英文 | 中文 |
|---|---|
| `current_price` / `yesterday_price` | 当前售价 / 昨日售价 |
| `dc_original_price` / `dc_price` / `original_dc_price` / `dc_original_price_sap` | 配送中心各类原/现价 |
| `original_price` | 商品原价 |
| `anchor_sale_price` / `unadjust_sale_price` | 锚定/未调整售价 |
| `strategy_no` / `promotion_no` | 策略编号 / 促销编号 |

### 维度表

- `strategy_fm_dim_day_clear`: `business_date / store_id / article_id + article_name + category_level1/2/3_id+description`
- `strategy_fm_dim_store_profile`: 门店画像 109 列（管理区、SAP 区、地理坐标、目标销售额等）
- `strategy_fm_dim_saleable`: 47 列可售 SKU 属性（shop_id / sku_code / category / sell_unit / basic_qty / propose_qty / ...）
- `strategy_fm_dim_goods`: 商品主数据 106 列（全量 SKU + spu + pig + purchase_department + ...）
- `strategy_fm_dim_calendar`: 70 列日历（day_date / day_wid / week_no / month_wid / year_wid / is_*）

---

## 行数合理性总结（给 Phase A 一个单独的小结）

| 表 | 预期 | 实测 | 判断 |
|---|---:|---:|---|
| sales_di | 1,533 | 1,533 | 合理（订单行级，1 店 × 399 SKU × 3.8 平均行） |
| purchase_di | 1,821 | 1,821 | 合理（1 店 × 1,748 SKU，略多因日清+非日清分行） |
| scm_di | 300 | 300 | 合理（1 店 × 300 个有 SAP 出入库的 SKU） |
| scm_adjust_di | 0 | 0 | 合理（昨日无调整，上游也无历史） |
| loss_di | 219 | 219 | 合理 |
| compose_di | 22 | 22 | 合理 |
| allowance_di | 943 | 943 | 合理（1 店 × 925 SKU，少量重复） |
| promo_di | 1,545 | 1,545 | 合理（多促销规则下同 SKU 可多行） |
| inventory_pool_di | 244,523 | 244,523 | 合理但偏多：每 SKU ~138 行历史快照，不是当日粒度 |
| price_da | 5,009 | 5,009 | 合理 |
| dim_day_clear | 92,076 | 92,076 | **偏高需要注意**：几乎等于 dim_goods 总量，不再是"仅日清商品" |
| dim_store_profile | 1 | 1 | 合理（翠花仅 1 店） |
| dim_saleable | 1,255 | 1,255 | 合理 |
| dim_goods | 92,539 | 92,539 | 合理 |
| dim_calendar | 8,712 (全量) | 全量 | 合理 |

唯一需要后续关注的是 **dim_day_clear 的口径**（见 §11）。
