# Food Mart 底表说明

本目录包含 Food Mart 经营分析数据链路的核心底表，共 4 张，按依赖顺序从上游到下游排列如下：

```
fm_商品维度底表  ──┐
                   ├──→  fm_分类汇总  ──→  fm_结果
fm_客数底表     ──┘
```

---

## 1. fm_商品维度底表

**目标表：** `default_catalog.ads_business_analysis.strategy_fm_flag_sku_di`

**粒度：** 门店 × 营业日期 × article_id（SKU）× day_clear

**用途：** 整个数据链路的最上游基础表，汇聚了销售、进销存、毛利、损耗、售罄、库存等所有 SKU 级别指标，供下游分类汇总使用。

### 数据来源

| 别名 | 源表 | 说明 |
|------|------|------|
| t1 | `hive.dal_full_link.dal_manage_full_link_store_dc_article_info_di` | 全链路门店商品经营主表 |
| b | `hive.dal.dal_transaction_sale_store_daily_di` | 门店日销售汇总（过滤 bf19_sale_amt ≥ 500，用于 RIGHT JOIN 驱动门店范围） |
| c | `hive.dal.dal_transaction_store_article_sale_info_di` | 商品销售件数 |
| e / f | `hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di` | 翠花门店商品经营（含 day_clear、库存、毛利等翠花专属字段） |
| g | `hive.ods_sc_db.t_purchase_order_item_tmp` | 可订可售 SKU |
| h | `default_catalog.ads_business_analysis.chdj_store_info` | 翠花门店信息（store_flag、store_no） |
| i | 同 e，按 article_id 聚合 day_clear | 用于非翠花门店的 day_clear 回填 |
| j | `hive.dim.dim_store_article_order_sale_info_di` | 商品可订可售标识（saleable） |
| t2 | `hive.dim.dim_store_profile` | 门店资料（区域、城市、开店日期等） |
| t3 | `hive.dim.dim_goods_information_have_pt` | 商品资料（分类、spu、黑白猪、规格、温层等） |
| t5 | `hive.dim.dim_calendar` | 日历维度（周、月、年） |

### 筛选条件

- 城市：广州
- 门店：`store_no = 'food mart'`
- 大类：猪肉类、预制菜、水果类、水产类、蔬菜类、肉禽蛋类、冷藏及加工类、标品类

### 输出字段

| 分组 | 字段 | 说明 |
|------|------|------|
| 时间维度 | `business_date`, `week_no`, `week_start_date`, `week_end_date`, `month_wid`, `year_wid` | 营业日期及周/月/年信息 |
| 门店维度 | `manage_area_name`, `sap_area_name`, `city_description`, `store_id`, `store_name`, `store_flag`, `store_no` | 门店基础信息及翠花标识 |
| 分类维度 | `category_level1_id/description`, `category_level2_id/description`, `category_level3_id/description` | 大/中/小分类（翠花门店存在自定义大类重映射逻辑） |
| 商品维度 | `spu_id`, `spu_name`, `blackwhite_pig_name`, `article_id`, `article_name` | SKU 及 SPU 信息 |
| 清场标识 | `day_clear` | 翠花店取翠花经营表字段；其他门店按品类规则判断（0=非日清，1=日清） |
| 毛利 | `full_link_article_profit`, `scm_fin_article_profit`, `article_profit_amt`, `pre_profit_amt` | 全链路毛利额、供应链毛利额、门店毛利额、预期毛利额 |
| 销售数量/重量 | `total_sale_qty`, `bf19_sale_qty`, `sales_weight`, `bf19_sales_weight` | 全天/19点前销售数量及重量 |
| 进销存金额 | `inbound_amount`, `purchase_weight`, `total_sale_amt`, `bf19_sale_amt`, `out_stock_amt_cb`, `pre_sale_amt`, `pre_inbound_amount`, `expect_outstock_amt` | 进货、销售、出库相关金额 |
| 促销折扣 | `scm_promotion_amt_total`, `lp_sale_amt`, `discount_amt`, `hour_discount_amt`, `discount_amt_cate` | 出库让利、原价、折扣额、时段折扣、促销折扣 |
| 损耗/退货 | `store_lost_amt`, `store_lost_qty`, `store_know_lost_amt`, `store_unknow_lost_amt`, `return_amt` | 损耗金额/数量（已知/未知）及退货额 |
| 出入库 | `out_stock_pay_amt`, `out_stock_pay_amt_notax`, `return_stock_pay_amt_notax` | 出库金额（含税/不含税）、退仓额 |
| 库存 | `init_stock_qty`, `init_stock_amt`, `end_stock_qty`, `end_stock_amt`, `inbound_qty`, `avg_7d_sale_qty` | 期初/期末库存量及金额、进货量、7日均销量 |
| 售罄 | `is_soldout_16`, `is_soldout_20`, `is_soldout_16_salesku`, `is_soldout_20_salesku` | 16点/20点售罄标识及对应参考 SKU 数 |
| 客数 | `cust_num`, `bf19_cust_num`, `online_cust_num` | 全天/19点前/线上客数 |
| 其他 | `is_stock_sku`, `sale_piece_qty`, `bf19_sale_piece_qty`, `lost_denominator` | 上架 SKU 标识、销售件数、损耗率分母 |

---

## 2. fm_客数底表

**目标表：** `default_catalog.ads_business_analysis.strategy_fm_cust`

**粒度：** 门店 × 营业日期 × day_clear × level_description × level_id

**用途：** 专门计算各维度层级的**客数**指标，供 `fm_分类汇总` 关联使用。

### 数据来源

| 源表 | 说明 |
|------|------|
| `hive.dsl.dsl_transaction_sotre_order_offline_details_di` | 线下订单明细 |
| `hive.dsl.dsl_transaction_sotre_order_online_details_di` | 线上订单明细（order_id 追加 `*` 作区分） |
| `hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di` | 翠花门店商品经营（获取 day_clear） |
| `default_catalog.ads_business_analysis.chdj_store_info` | 翠花门店信息 |
| `hive.dim.dim_goods_information_have_pt` | 商品资料（分类信息） |
| `hive.dim.dim_store_profile` | 门店资料（城市筛选） |

### 筛选条件

- 城市：广州，`store_no = 'food mart'`，订单状态：`os.completed`
- 大类：同商品维度底表

### 层级展开

每个维度层级均输出两行：一行为该 day_clear 值（0/1），一行为合计（day_clear = `'2'`）。

| level_description | level_id | 说明 |
|---|---|---|
| `门店` | `''` | 门店整体客数 |
| `大分类` | category_level1_id | 按大分类统计 |
| `中分类` | category_level2_id | 按中分类统计 |
| `小分类` | category_level3_id | 按小分类统计 |
| `spu` | spu_id | 按 SPU 统计 |
| `黑白猪` | blackwhite_pig_id | 仅限猪肉类（category_level1_id=13） |

### 输出字段

| 字段 | 说明 |
|------|------|
| `business_date` | 营业日期 |
| `store_id`, `store_name` | 门店信息 |
| `day_clear` | 清场标识（0/1/2合计） |
| `level_description` | 维度层级名称 |
| `level_id` | 维度层级 ID |
| `bf19_cust_num_cate` | 19点前（翠花店20点前）客数 |
| `cust_num_cate` | 全天客数 |
| `sale_article_num_cate` | 动销 SKU 数 |
| `online_order_num_cate` | 线上订单数 |
| `jielong_cust_num_cate` | 接龙客数 |
| `jielong_sale_amt` | 接龙销售额 |

---

## 3. fm_分类汇总

**目标表：** `default_catalog.ads_business_analysis.strategy_fm_levels_sum`

**粒度：** 门店 × 营业日期 × day_clear × level_description（门店/大分类/中分类/小分类/spu/黑白猪/sku）× 对应维度 ID

**用途：** 将 `strategy_fm_flag_sku_di`（商品维度底表）按各层级聚合，并关联 `strategy_fm_cust`（客数底表）补充客数字段，形成各分析维度的汇总层。

### 数据来源

| 源表 | 说明 |
|------|------|
| `strategy_fm_flag_sku_di` | 商品维度底表（主数据源，GROUP BY 各层级） |
| `strategy_fm_cust` | 客数底表（LEFT JOIN，补充客数三字段） |

### 层级展开规则

每个维度层级均输出两行：一行保留原始 day_clear（0/1），一行强制 day_clear = `'2'`（合计）。sku 维度仅输出 day_clear 原始行（不做合计聚合）。

| level_description | 分类字段填充情况 | 客数 JOIN 条件 |
|---|---|---|
| `门店` | 全部分类字段置空 | `level_description = '门店'`，按 store_id + business_date + day_clear |
| `大分类` | level1 有值，其余置空 | `level_description = '大分类'`，额外 JOIN `category_level1_description = level_id` |
| `中分类` | level1+level2 有值 | `level_description = '中分类'`，额外 JOIN `category_level2_id = level_id` |
| `小分类` | level1+level2+level3 有值 | `level_description = '小分类'`，额外 JOIN `category_level3_id = level_id` |
| `spu` | level1+level2+level3+spu 有值 | `level_description = 'spu'`，额外 JOIN `spu_id = level_id` |
| `黑白猪` | level1 有值，spu_name 使用 blackwhite_pig_name，仅限猪肉类 | `level_description = '黑白猪'`，按黑猪/白猪名称 JOIN |
| `sku` | 全部字段原样保留 | 直接取 `strategy_fm_flag_sku_di` 中的 cust_num / bf19_cust_num |

### 输出字段

完整继承 `strategy_fm_flag_sku_di` 的所有指标字段，并追加：

| 字段 | 来源 | 说明 |
|------|------|------|
| `cust_num_cate` | strategy_fm_cust | 全天客数 |
| `bf19_cust_num_cate` | strategy_fm_cust | 19点前客数 |
| `sale_article_num_cate` | strategy_fm_cust | 动销 SKU 数 |
| `level_description` | 固定值 | 维度层级名称 |

---

## 4. fm_结果

**目标表：** `default_catalog.ads_business_analysis.strategy_fm_levels_result`

**粒度：** 门店 × 营业日期 × day_clear × level_description × 分类维度

**用途：** 最终结果层，直接对接 BI 看板。从 `strategy_fm_levels_sum` 读取，计算所有比率型 KPI，输出中文列名。

### 数据来源

`strategy_fm_levels_sum`（分类汇总表）

### 筛选条件

过滤 sku 维度中无任何经营记录的 SKU（全部为 0 的行）。

### 输出字段（中文名）

| 分组 | 字段 |
|------|------|
| 维度标识 | 标签、门店号、日期、门店名称、商品编码、分类名称、大分类、中分类、小分类、分类等级、day_clear、非日清标识 |
| 规模指标 | 营业店日数、营业店数、动销sku数、上架sku数、sku动销率 |
| 毛利 | 全链路毛利额、供应链毛利额、门店毛利额、全链路毛利率、供应链毛利率、门店毛利率、供应链预期毛利率、门店预期毛利率、门店定价毛利率 |
| 销售 | 销售重量、19点前销售重量、销售数量、19点前销售数量、全天销售额、19点前销售额、品效、销售额占比_组内、销售额排名_中分类、销售额排名_大分类 |
| 进货 | 进货额、进货重量、进货价、采购价 |
| 客数/客单 | 全天来客数、全天客单价、19点前客数、19点前客单价、19点前件单价、19点前单件数 |
| 折扣促销 | 折扣率、促销折扣率、时段折扣率、供应链折让率 |
| 损耗退货 | 损耗额、损耗率、损耗率_销售额、损耗率_数量、损耗数量、门店已知损耗额、门店未知损耗额、退货率 |
| 库存周转 | 期初库存额、期末库存额、期初库存量、期末库存量、7天日均销售量、周转率 |
| 售罄 | 售罄率16、售罄率20 |
| 价格 | 平均售价、平均销售原价 |
| 其他分子分母字段 | 理论销售额、原价销售额、出库成本、供应链毛利率_分母、供应链预期毛利率_分子/分母、门店定价毛利率_分子、供应链折让率_分母、损耗率_分母、退货率_分子/分母、19点前销售件数、销售件数 |

---

## 依赖关系与更新顺序

```
1. strategy_fm_flag_sku_di     (fm_商品维度底表)   ← 最上游，每日增量写入
2. strategy_fm_cust            (fm_客数底表)        ← 与上游并行，每日增量写入
3. strategy_fm_levels_sum      (fm_分类汇总)        ← 依赖 1 和 2，每日增量写入
4. strategy_fm_levels_result   (fm_结果)            ← 依赖 3，每日增量写入，供 BI 消费
```

> 更新时需按 1 → 2 → 3 → 4 顺序执行，1 和 2 可并行。
