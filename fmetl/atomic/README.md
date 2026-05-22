# atomic/ — 原子域提取器

原子层是 ETL 管道的数据基础层。每张原子表对应一个独立的业务观测域，数据粒度统一为 `store_id × business_date × article_id × day_clear`。

数据从 QDM BI API 拉取，由 `BaseExtractor` 提供分批写入（默认每 7 天一批）和自动重试（3次，间隔5秒）。

## 字段命名约定

| 前缀/后缀 | 含义 |
|----------|------|
| `_qty` | 数量 |
| `_amt` | 金额 |
| `_price` | 单价 |
| `bf19_` | 19点前 (before 19:00) |
| `af19_` | 19点后 (after 19:00) |
| `bf12_` | 12点前 (before 12:00) |
| `_src` | 源表原始值（与 Python 计算值区分） |
| `original_` | 原价（折扣前） |

---

## 一、维度表 (7张)

### dim_goods — 商品维度表

| 字段 | 类型 | 说明 |
|------|------|------|
| `article_id` | VARCHAR | 商品编码 (SKU) |
| `article_name` | VARCHAR | 商品名称 |
| `category_level1_id` | VARCHAR | 大类ID |
| `category_level1_description` | VARCHAR | 大类名称 (如"鲜肉类"、"蔬菜类") |
| `category_level2_id` | VARCHAR | 中类ID |
| `category_level2_description` | VARCHAR | 中类名称 |
| `category_level3_id` | VARCHAR | 小类ID |
| `category_level3_description` | VARCHAR | 小类名称 |
| `spu_id` | VARCHAR | SPU编码 |
| `spu_name` | VARCHAR | SPU名称 |
| `blackwhite_pig_id` | VARCHAR | 黑白猪编码 |
| `blackwhite_pig_name` | VARCHAR | 黑白猪名称 (白猪/黑猪) |
| `unit_weight` | DOUBLE | 单位重量 (kg/件) |
| `sale_unit` | VARCHAR | 销售单位 (如"千克"、"件") |

**源表**: `strategy_fm_dim_goods`
**过滤**: `category_level1_id NOT IN ('70','71','72','73','74','75','76','77')` (排除物料类)
**提取条件**: `inc_day = '{yesterday}'`

### dim_store_list — 门店列表

| 字段 | 类型 | 说明 |
|------|------|------|
| `store_id` | VARCHAR | 门店编码 (如 A3XV, ALVK) |

**源表**: `ads_business_analysis.chdj_store_info`
**用途**: 所有原子表 INNER JOIN 此表，确保只处理翠花门店数据。

### dim_day_clear — 日清标签表

| 字段 | 类型 | 说明 |
|------|------|------|
| `store_id` | VARCHAR | 门店编码 |
| `business_date` | VARCHAR | 业务日期 |
| `article_id` | VARCHAR | 商品编码 |
| `day_clear` | BIGINT | 日清标识: 0=日清, 1=非日清 |

**源表**: `strategy_fm_dim_day_clear`
**提取条件**: `business_date BETWEEN '{start}' AND '{end}'`
**提取模式**: `replace_partition` (按日期分区替换)

### dim_store_profile — 门店档案表

| 字段 | 类型 | 说明 |
|------|------|------|
| `store_id` | VARCHAR | 门店编码 |
| `store_name` | VARCHAR | 门店名称 |
| `manage_area_name` | VARCHAR | 管理区域 |
| `sap_area_name` | VARCHAR | SAP区域 |
| `city_description` | VARCHAR | 城市 |

**源表**: `strategy_fm_dim_store_profile`
**字段映射**: `sp_store_id` → `store_id`, `sp_store_name` → `store_name`

### dim_calendar — 日历维度表

| 字段 | 类型 | 说明 |
|------|------|------|
| `business_date` | VARCHAR | 营业日期 |
| `week_no` | VARCHAR | 周次 |
| `week_start_date` | VARCHAR | 本周开始日期 |
| `week_end_date` | VARCHAR | 本周结束日期 |
| `month_wid` | VARCHAR | 月份 |
| `year_wid` | VARCHAR | 年份 |

**源表**: `strategy_fm_dim_calendar`
**字段映射**: `day_date` → `business_date`
**提取条件**: `day_date BETWEEN '{start}' AND '{end}'`

### dim_saleable — 可售商品表

| 字段 | 类型 | 说明 |
|------|------|------|
| `store_id` | VARCHAR | 门店编码 |
| `article_id` | VARCHAR | 商品编码 (表示该商品在该店可售) |

**源表**: `strategy_fm_dim_saleable`
**字段映射**: `shop_id` → `store_id`, `sku_code` → `article_id`

### dim_chdj_store_info — 翠花门店信息表

| 字段 | 类型 | 说明 |
|------|------|------|
| `store_id` | VARCHAR | 门店编码 |
| `store_flag` | VARCHAR | 门店标签 (如"翠花店") |
| `store_no` | VARCHAR | 门店号 (如"food mart") |
| `store_name` | VARCHAR | 门店名称 (如"广州滨江宏岸店") |

**源表**: `ads_business_analysis.chdj_store_info`

---

## 二、活跃原子域 (12张)

### 域① atomic_sales — 销售域 (33字段)

**源表**: `strategy_fm_sales_di`
**粒度**: store_id × business_date × article_id × day_clear

#### 销售数量

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `sale_qty` | `SUM(qty_spec)` | 销售规格数 (基本销售单位) |
| `sale_piece_qty` | `SUM(qty)` | 销售件数 |
| `sale_piece_qty` → bf19 | `SUM(qty) - SUM(COALESCE(af19_sales_qty, 0))` | 19点前销售件数 |
| `online_sale_qty` | `SUM(IF(online_flag='Y', qty*spec_num, 0))` | 线上销售规格数 |
| `offline_sale_qty` | `SUM(IF(online_flag='N', qty_spec, 0))` | 线下销售规格数 |
| `bf19_sale_qty` | `SUM(qty_spec - COALESCE(af19_sales_qty*spec_num, 0))` | 19点前销售规格数 |
| `af19_sale_qty` | `SUM(COALESCE(af19_sales_qty*spec_num, 0))` | 19点后销售规格数 |
| `bf12_sale_qty` | `SUM(IF(trans_hour < '12', qty*spec_num, 0))` | 12点前销售规格数 |
| `return_sale_qty` | `SUM(COALESCE(return_sale_qty, 0))` | 退货数量 |
| `gift_qty` | `SUM(COALESCE(gift_qty, 0))` | 赠品数量 |
| `sales_weight` | `SUM(IF(按重量或无量, qty_spec, qty_spec*unit_weight))` | 销售重量 (kg) |

#### 销售金额

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `sale_amt` | `SUM(sales_amt)` | 销售金额 (实收) |
| `original_price_sale_amt` | `SUM(p_lp_sub_amt)` | 原价销售金额 (折扣前) |
| `actual_amount` | `SUM(actual_amount - f_sub_amt - f_promo_sub_amt)` | 实际收款 |
| `return_sale_amt` | `SUM(COALESCE(return_sale_amt, 0))` | 退货金额 |
| `bf19_sale_amt` | `SUM(sales_amt - COALESCE(af19_sales_amt, 0))` | 19点前销售额 |
| `af19_sale_amt` | `SUM(COALESCE(af19_sales_amt, 0))` | 19点后销售额 |
| `bf19_offline_sale_amt` | `SUM(IF(online_flag='N', sales_amt - af19_sales_amt, 0))` | 19点前线下销售额 |
| `bf12_sale_amt` | `SUM(IF(trans_hour < '12', sales_amt, 0))` | 12点前销售额 |
| `member_sale_amt` | `SUM(IF(会员手机号不为空, sales_amt, 0))` | 会员销售额 |
| `bf19_member_sale_amt` | `SUM(IF(会员, sales_amt - af19_sales_amt, 0))` | 19点前会员销售额 |
| `offline_original_amt` | `SUM(IF(online_flag='N', p_lp_sub_amt, 0))` | 线下原价销售额 |

#### 折扣

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `discount_amt` | `SUM(discount_amt)` | 折扣总额 |
| `vip_discount_amt` | `SUM(vip_discount_amt)` | 会员折扣额 |
| `hour_discount_amt` | `SUM(hour_discount_amt)` | 时段折扣额 |
| `member_discount_amt` | `SUM(vip_discount_amt)` | 会员优惠额 (v10: 同 vip_discount_amt) |
| `store_paylevel_discount` | `SUM(COALESCE(store_paylevel_discount, 0))` | 门店支付级折扣 |
| `company_paylevel_discount` | `SUM(COALESCE(company_paylevel_discount, 0))` | 公司支付级折扣 |

#### 其他

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `last_sysdate` | `MAX(IF(online_flag='N', pay_at, NULL))` | 最后线下支付时间 |
| `day_clear` | 源表字段 | 日清标识 |

**过滤条件**:
- `inc_day BETWEEN '{start}' AND '{end}'`
- 排除品类 91 (当 online_flag='N' 时还需排除 70-77)
- 过滤掉 `category_level1_id` 为 NULL 的行 (通过 LEFT JOIN dim_goods 后的 WHERE 条件)

---

### 域② atomic_inventory — 库存域 (7字段, v10简化)

**源表**: `strategy_fm_purchase_di`
**v10变更**: 只提取 init_stock + avg_inbound_price，不再取 receive/end_stock

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `store_id` | 源表字段 | 门店编码 |
| `business_date` | 源表字段 | 业务日期 |
| `article_id` | `sale_article_id` (别名) | 商品编码 |
| `avg_inbound_price` | 加权平均: `SUM(qty × price) / SUM(qty)` | 平均进货价 |
| `init_stock_qty` | `SUM(init_stock_qty)` | 期初库存数量 |
| `init_stock_amt` | `SUM(init_stock_amt)` | 期初库存金额 |
| `day_clear` | 源表字段 (按此分组) | 日清标识 |

**过滤条件**:
- `inc_day BETWEEN '{start}' AND '{end}'`
- LEFT JOIN `dim_goods` → `category_level1_id NOT IN ('70'..'77')`

**avg_inbound_price 计算**:
```sql
CASE WHEN SUM(sale_article_qty) > 0
     THEN SUM(sale_article_qty * avg_inbound_price) / SUM(sale_article_qty)
     ELSE AVG(avg_inbound_price)
END
```

---

### 域③ atomic_scm — 供应链域 (28字段)

**源表**: `strategy_fm_scm_di`

#### 出库/入库数量

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `original_outstock_qty` | `SUM(COALESCE(源, 0))` | 原始出库数量 |
| `promotion_outstock_qty` | `SUM(COALESCE(源, 0))` | 促销出库数量 |
| `gift_outstock_qty` | `SUM(COALESCE(源, 0))` | 赠品出库数量 |
| `return_stock_qty` | `SUM(COALESCE(源, 0))` | 退货入库数量 |
| `store_return_qty_shop` | `SUM(COALESCE(源, 0))` | 门店退货量 |
| `store_order_qty` | `SUM(COALESCE(源, 0))` | 门店订货量 |
| `order_qty_payean` | `SUM(COALESCE(源, 0))` | 订货量(金额口径) |

#### 出库/入库单价 (派生计算)

| 字段 | 计算方式 |
|------|---------|
| `outstock_unit_price` | `IF(total_outstock_qty=0, 0, out_stock_pay_amt / total_outstock_qty)` |
| `outstock_unit_price_notax` | `IF(total_outstock_qty=0, 0, out_stock_pay_amt_notax / total_outstock_qty)` |
| `outstock_cost_price` | `IF(total_outstock_qty=0, 0, out_stock_amt_cb / total_outstock_qty)` |
| `outstock_cost_price_notax` | `IF(total_outstock_qty=0, 0, out_stock_amt_cb_notax / total_outstock_qty)` |
| `return_unit_price` | `IF(return_stock_qty=0, 0, return_stock_pay_amt / return_stock_qty)` |
| `return_unit_price_notax` | `IF(return_stock_qty=0, 0, return_stock_pay_amt_notax / return_stock_qty)` |
| `return_cost_price` | `IF(return_stock_qty=0, 0, return_stock_amt_cb / return_stock_qty)` |
| `return_cost_price_notax` | `IF(return_stock_qty=0, 0, return_stock_amt_cb_notax / return_stock_qty)` |
| `order_unit_price` | `IF(store_order_qty=0, 0, order_amt / store_order_qty)` |

#### 促销分摊/补贴

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `scm_promotion_amt_total` | `SUM(COALESCE(源, 0))` | 供应链促销折让总额 |
| `scm_promotion_amt_gift` | `SUM(COALESCE(源, 0))` | 供应链赠品折让 |
| `scm_bear_amt` | `SUM(COALESCE(源, 0))` | 供应链承担额 |
| `vendor_bear_amt` | `SUM(COALESCE(源, 0))` | 供应商承担额 |
| `business_bear_amt` | `SUM(COALESCE(源, 0))` | 业务承担额 |
| `market_bear_amt` | `SUM(COALESCE(源, 0))` | 市场承担额 |
| `vender_bear_gift_amt` | `SUM(COALESCE(源, 0))` | 供应商赠品承担额 |
| `scm_bear_gift_amt` | `SUM(COALESCE(源, 0))` | 供应链赠品承担额 |
| `adjustment_amt` | `SUM(COALESCE(源, 0))` | 差异调整额 |

---

### 域③附 atomic_scm_adjust — 供应链差异调整 (9字段)

**源表**: `strategy_fm_scm_adjust_di`

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `store_id` | 源表字段 | 门店编码 |
| `business_date` | 源表字段 | 业务日期 |
| `article_id` | 源表字段 | 商品编码 |
| `dc_id` | 源表字段 | DC编码 (配送中心) |
| `matnr` | 源表字段 | 物料号 |
| `tax` | 源表字段 | 税率 |
| `new_sp_store_id` | 源表字段 | 新门店编码 |
| `adjustment_amt` | `SUM(COALESCE(源, 0))` | 差异调整额 (含税) |
| `adjustment_amt_notax` | `SUM(COALESCE(源, 0))` | 差异调整额 (不含税) |

---

### 域④ atomic_loss — 损耗域 (7字段)

**源表**: `strategy_fm_loss_di`

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `know_lost_qty` | `SUM(know_lost_qty)` | 已知损耗数量 |
| `know_lost_amt_src` | `SUM(know_lost_amt)` | 已知损耗金额 (源表值) |
| `unknow_lost_qty_src` | `SUM(unknow_lost_qty)` | 未知损耗数量 (源表值) |
| `unknow_lost_amt_src` | `SUM(unknow_lost_amt)` | 未知损耗金额 (源表值) |

> **v10注意**: 标 `_src` 后缀的字段为源表原始值。在计算层中，未知损耗会由库存方程重新推导 (stock.py)，不再直接使用源表值。

---

### 域⑤ atomic_compose — 加工转换域 (5字段)

**源表**: `strategy_fm_compose_di`

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `compose_in_qty` | `SUM(COALESCE(源, 0))` | 加工流入数量 (如整猪拆分为部位肉: 部位肉获得流入) |
| `compose_out_qty` | `SUM(COALESCE(源, 0))` | 加工流出数量 (如整猪被拆分: 整猪发生流出) |

> **加工语义**: compose_in 表示该商品通过加工获得的数量，compose_out 表示该商品被加工消耗的数量。对于同一商品，in 和 out 通常不会同时出现。

---

### 域⑥ atomic_allowance — 补贴域 (4字段)

**源表**: `strategy_fm_allowance_di`

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `allowance_amt` | `SUM(COALESCE(split_allowance_amt, 0))` | 补贴金额 |

**字段映射**: `sale_article_id` → `article_id`

---

### 域⑦ atomic_promo — 促销优惠域 (10字段)

**源表**: `strategy_fm_promo_di`
**字段映射**: `shop_id` → `store_id`, `sku_code` → `article_id`

| 字段 | 说明 | 计算逻辑 |
|------|------|---------|
| `member_coupon_shop_amt` | 门店承担会员券优惠额 | `cost_center='shop' AND promotion_category='rule' AND promo_type='OrderCoupon' AND order_type='normal' AND online_flag='N'` |
| `member_promo_amt` | 会员促销优惠额 | 非shop/vendor/customer 承担 + n.fold.point 类型 |
| `member_coupon_company_amt` | 公司承担会员券优惠额 | 同上但 SUBSTR(promo_ext_prop,1,2)<>'01' AND SUBSTR(promo_ext_prop,3,2)<>'01' |
| `shop_promo_amt` | 门店促销优惠额 | `cost_center='shop' AND promotion_category='rule'` |
| `no_ordercoupon_company_promotion_amt` | 公司非订单券促销额 | `online_flag='N' AND promo_type IN('O','I','Exchange') AND cost_center NOT IN('shop','customer')` |
| `ordercoupon_shop_promotion_amt` | 门店订单券促销额 | `online_flag='N' AND promo_type='OrderCoupon' AND cost_center='shop'` |
| `ordercoupon_company_promotion_amt` | 公司订单券促销额 | `online_flag='N' AND promo_type='OrderCoupon' AND cost_center NOT IN('customer','shop')` |

**过滤**: INNER JOIN `dim_goods` 排除品类 70-77。

---

### 域⑧ atomic_cost_price — 成本价域 (4字段)

**源表**: `strategy_fm_inventory_pool_di`
**字段映射**: `shop_id` → `store_id`, `sku_code` → `article_id`, `inventory_date` → `business_date`

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `cost_price` | `MAX(cost_price)` | 成本价 (取当日最大值) |

> **v10注意**: cost_price 在 v10 中不再参与 euc 计算 (A6 修复)。euc 统一用 avg_inbound_price 定价 compose。

---

### 域⑨ atomic_price — 价格域 (7字段)

**源表**: `strategy_fm_price_da`

| 字段 | SQL来源 | 说明 |
|------|---------|------|
| `current_price` | `COALESCE(current_price, 0)` | 当前售价 |
| `yesterday_price` | `COALESCE(yesterday_price, 0)` | 昨日售价 |
| `dc_original_price` | `COALESCE(dc_original_price, 0)` | DC配送原价 |
| `original_price` | `COALESCE(original_price, 0)` | 原价 (标签价) |

---

### 域⑪ atomic_receive_sale — 进货销售域 (19字段, BOM核心)

**源表**: `strategy_fm_receive_sale_di`
**v10关键**: 此表是 BOM 分摊和自购数据的唯一数据源。

| 字段 | 类型 | 说明 |
|------|------|------|
| `store_id` | VARCHAR | 门店编码 |
| `business_date` | VARCHAR | 业务日期 |
| `article_id` | VARCHAR | **进货品** (父品, BOM parent) |
| `article_name` | VARCHAR | 进货品名称 |
| `sale_article_id` | VARCHAR | **销售品** (子品, BOM sub) |
| `sale_article_name` | VARCHAR | 销售品名称 |
| `inbound_qty` | DOUBLE | 进货数量 |
| `inbound_amount` | DOUBLE | 进货金额 |
| `purchase_price` | DOUBLE | 采购单价 |
| `sum_article_qty` | DOUBLE | 父品总进货量 |
| `sum_sub_article_qty` | DOUBLE | 子品总进货量 |
| `sum_sale_article_qty` | DOUBLE | 销售品总销售量 |
| `sale_article_qty` | DOUBLE | 该组合下销售品数量 |
| `sale_article_price` | DOUBLE | 销售品售价 |
| `split_sale_article_amt` | DOUBLE | 分摊后销售品金额 |
| `rate` | DOUBLE | 分配比率 |
| `sale_recev_rate` | DOUBLE | 销售回报率 |
| `category_level1_id` | VARCHAR | 大类ID |
| `category_level1_description` | VARCHAR | 大类名称 |

**两种使用方式**:

1. **自购**: `WHERE article_id = sale_article_id` (A = B)
   → 提取为 `self_receive_qty/amt` (自己进货自己卖，无BOM关系)

2. **BOM拆分**: `WHERE article_id != sale_article_id` (A → B)
   → 父品 A 进货，子品 B 销售。需要分摊父品成本到子品

---

### atomic_bom_relation — BOM关系表 (11字段, v10观测用)

**源表**: `strategy_dim_store_article_bom_relation`
**v10状态**: 仅提取观测，不参与计算。BOM分摊核心数据来自 `atomic_receive_sale`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `parent_article_id` | VARCHAR | 父品编码 |
| `sub_article_id` | VARCHAR | 子品编码 |
| `parent_unit` | VARCHAR | 父品单位 |
| `sub_unit` | VARCHAR | 子品单位 |
| `dressing_rate` | DOUBLE | 修整率 (出品率) |
| `cost_rate` | DOUBLE | 成本率 |
| `bom_type` | INTEGER | BOM类型 (1/2/3) |
| `split_mode` | VARCHAR | 拆分模式 |
| `sp_level` | INTEGER | SP层级 |

**过滤**: `bom_type IN (1, 2, 3)`

---

## 三、骨架表 (2张)

### atomic_order_receive (空骨架)

预留表，字段结构参考 `strategy_fm_order_receive_di`。当前 create empty table 但不填充数据。

### atomic_article_convert (空骨架)

预留表，字段结构参考 `strategy_fm_dim_article_convert`。当前 create empty table 但不填充数据。
