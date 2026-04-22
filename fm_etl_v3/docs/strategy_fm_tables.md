# strategy_fm_* 底表体检报告

- 探测日期: **2026-04-20**
- 探测脚本: [fm_etl_v3/scripts/probe_strategy_fm.py](../scripts/probe_strategy_fm.py)
- 生成方式: `SHOW FULL COLUMNS FROM <t>` + `SELECT COUNT/*... WHERE 日期列 = '2026-04-20'`

## 概览

| # | 表 | 业务 | 日期列 | 预期行数 | 实测行数 | distinct store | distinct article | 结论 |
|---|---|---|---|---|---|---|---|---|
| 1 | `strategy_fm_sales_di` | 销售明细（订单行级？） | `inc_day` | 1,533 | 1,533 | 1 | 399 | ✅ 预期 1,533，实测 1,533，误差 0.0% |
| 2 | `strategy_fm_purchase_di` | 进货验收（订单行级？） | `inc_day` | 1,821 | 1,821 | 1 | 1,748 | ✅ 预期 1,821，实测 1,821，误差 0.0% |
| 3 | `strategy_fm_scm_di` | SAP 出入库 | `inc_day` | 300 | 300 | 1 | 300 | ✅ 预期 300，实测 300，误差 0.0% |
| 4 | `strategy_fm_scm_adjust_di` | SCM 差异调整（昨日可能为 0） | `inc_day` | 0 | 0 | 0 | 0 | ✅ 实测 0，与预期一致 |
| 5 | `strategy_fm_loss_di` | 损耗 | `inc_day` | 219 | 219 | 1 | 219 | ✅ 预期 219，实测 219，误差 0.0% |
| 6 | `strategy_fm_compose_di` | 加工转换 | `inc_day` | 22 | 22 | 1 | 22 | ✅ 预期 22，实测 22，误差 0.0% |
| 7 | `strategy_fm_allowance_di` | 活动让利 | `inc_day` | 943 | 943 | 1 | 925 | ✅ 预期 943，实测 943，误差 0.0% |
| 8 | `strategy_fm_promo_di` | 促销（订单项） | `inc_day` | 1,545 | 1,545 | 1 | 399 | ✅ 预期 1,545，实测 1,545，误差 0.0% |
| 9 | `strategy_fm_inventory_pool_di` | 库存成本价池 | `inc_day` | 244,523 | 244,523 | 1 | 1,769 | ✅ 预期 244,523，实测 244,523，误差 0.0% |
| 10 | `strategy_fm_price_da` | 门店商品价格 | `inc_day` | 5,009 | 5,009 | 1 | 5,009 | ✅ 预期 5,009，实测 5,009，误差 0.0% |
| 11 | `strategy_fm_dim_day_clear` | 日清商品清单 | `inc_day` | 92,076 | 92,076 | 1 | 92,075 | ✅ 预期 92,076，实测 92,076，误差 0.0% |
| 12 | `strategy_fm_dim_store_profile` | 门店画像 | `inc_day` | 1 | 1 | — | — | ✅ 预期 1，实测 1，误差 0.0% |
| 13 | `strategy_fm_dim_saleable` | 可售商品 | `inc_day` | 1,255 | 1,255 | 1 | 1,255 | ✅ 预期 1,255，实测 1,255，误差 0.0% |
| 14 | `strategy_fm_dim_goods` | 商品主数据 | `inc_day` | 92,539 | 92,539 | — | 92,539 | ✅ 预期 92,539，实测 92,539，误差 0.0% |
| 15 | `strategy_fm_dim_calendar` | 日历维度（含全量） | `day_date` | — | 1 | — | — | 实测 1 行（无基线参考） |


---

## 1. `strategy_fm_sales_di` — 销售明细（订单行级？）

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 1,533
- 行数实测（2026-04-20）: 1,533
- 当日 distinct `store_id`: 1
- 当日 distinct `abi_article_id`: 399
- 评估: **✅ 预期 1,533，实测 1,533，误差 0.0%**

### 字段结构（119 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| business_date | varchar(1048576) | YES | YES |  |  |
| store_id | varchar(1048576) | YES | NO |  |  |
| sp_store_name | varchar(1048576) | YES | NO |  |  |
| area_description | varchar(1048576) | YES | NO |  |  |
| area_id | varchar(1048576) | YES | NO |  |  |
| sp_type | varchar(1048576) | YES | NO |  |  |
| sp_level | varchar(1048576) | YES | NO |  |  |
| order_id | varchar(1048576) | YES | NO |  |  |
| order_status | varchar(1048576) | YES | NO |  |  |
| parent_order_id | varchar(1048576) | YES | NO |  |  |
| children_order_ids | varchar(1048576) | YES | NO |  |  |
| serial_id | varchar(1048576) | YES | NO |  |  |
| delivery_id | varchar(1048576) | YES | NO |  |  |
| tenant_id | varchar(1048576) | YES | NO |  |  |
| message | varchar(1048576) | YES | NO |  |  |
| internal_comment | varchar(1048576) | YES | NO |  |  |
| reason | varchar(1048576) | YES | NO |  |  |
| first_buy_flag | varchar(1048576) | YES | NO |  |  |
| comment_id | varchar(1048576) | YES | NO |  |  |
| comment_time | varchar(1048576) | YES | NO |  |  |
| sibling_order_ids | varchar(1048576) | YES | NO |  |  |
| split_supported | varchar(1048576) | YES | NO |  |  |
| root_order_id | varchar(1048576) | YES | NO |  |  |
| afs_order_id | varchar(1048576) | YES | NO |  |  |
| outer_order_id | varchar(1048576) | YES | NO |  |  |
| outer_order_type | varchar(1048576) | YES | NO |  |  |
| payment_type | varchar(1048576) | YES | NO |  |  |
| bundle_promo_code | varchar(1048576) | YES | NO |  |  |
| sync_seq | varchar(1048576) | YES | NO |  |  |
| je_date | varchar(1048576) | YES | NO |  |  |
| je_order_id | varchar(1048576) | YES | NO |  |  |
| rje_date | varchar(1048576) | YES | NO |  |  |
| rje_order_id | varchar(1048576) | YES | NO |  |  |
| is_hour_promotion | varchar(1048576) | YES | NO |  |  |
| abi_article_id | varchar(1048576) | YES | NO |  |  |
| is_promotion_article | varchar(1048576) | YES | NO |  |  |
| online_flag | varchar(1048576) | YES | NO |  |  |
| goods_barcode | varchar(1048576) | YES | NO |  |  |
| spec_num | decimal(38,9) | YES | NO |  |  |
| spec_type | varchar(1048576) | YES | NO |  |  |
| customer_id | varchar(1048576) | YES | NO |  |  |
| customer_name | varchar(1048576) | YES | NO |  |  |
| customer_phone | varchar(1048576) | YES | NO |  |  |
| sale_unit | varchar(1048576) | YES | NO |  |  |
| display_price | decimal(38,9) | YES | NO |  |  |
| list_price | decimal(38,9) | YES | NO |  |  |
| sale_price | decimal(38,9) | YES | NO |  |  |
| order_type | varchar(1048576) | YES | NO |  |  |
| order_sub_type | varchar(1048576) | YES | NO |  |  |
| channel_id | varchar(1048576) | YES | NO |  |  |
| inc_time | varchar(1048576) | YES | NO |  |  |
| pay_at | varchar(1048576) | YES | NO |  |  |
| refund_at | varchar(1048576) | YES | NO |  |  |
| refund_type | varchar(1048576) | YES | NO |  |  |
| sync_flag | varchar(1048576) | YES | NO |  |  |
| currency | varchar(1048576) | YES | NO |  |  |
| erp_order_at | varchar(1048576) | YES | NO |  |  |
| split_at | varchar(1048576) | YES | NO |  |  |
| complete_at | varchar(1048576) | YES | NO |  |  |
| order_at | varchar(1048576) | YES | NO |  |  |
| allrefund_time | varchar(1048576) | YES | NO |  |  |
| qty | decimal(38,9) | YES | NO |  |  |
| qty_spec | decimal(38,9) | YES | NO |  |  |
| p_paid_sub_amt | decimal(38,9) | YES | NO |  |  |
| f_paid_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_pay_sub_amt | decimal(38,9) | YES | NO |  |  |
| f_pay_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_lp_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_sp_sub_amt | decimal(38,9) | YES | NO |  |  |
| f_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_promo_sub_amt | decimal(38,9) | YES | NO |  |  |
| f_promo_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_pointpay_sub_amt | decimal(38,9) | YES | NO |  |  |
| f_pointpay_sub_amt | decimal(38,9) | YES | NO |  |  |
| f_balancepay_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_balancepay_sub_amt | decimal(38,9) | YES | NO |  |  |
| f_cashpay_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_cashpay_sub_amt | decimal(38,9) | YES | NO |  |  |
| p_change_sub_amt | decimal(38,9) | YES | NO |  |  |
| sales_amt | decimal(38,9) | YES | NO |  |  |
| discount_amt | decimal(38,9) | YES | NO |  |  |
| vip_discount_amt | decimal(38,9) | YES | NO |  |  |
| hour_discount_amt | decimal(38,9) | YES | NO |  |  |
| return_sale_qty | decimal(38,9) | YES | NO |  |  |
| return_sale_amt | decimal(38,9) | YES | NO |  |  |
| member_hour_sales_amt | decimal(38,9) | YES | NO |  |  |
| af19_sales_amt | decimal(38,9) | YES | NO |  |  |
| af19_sales_qty | decimal(38,9) | YES | NO |  |  |
| shop_promo_sub_amt | decimal(38,9) | YES | NO |  |  |
| promotion_amt | decimal(38,9) | YES | NO |  |  |
| gift_qty | decimal(38,9) | YES | NO |  |  |
| i_promotion_amt | decimal(38,9) | YES | NO |  |  |
| order_promotion_amt | decimal(38,9) | YES | NO |  |  |
| ordercoupon_promotion_amt | decimal(38,9) | YES | NO |  |  |
| actual_weight | decimal(38,9) | YES | NO |  |  |
| promotion_cost | decimal(38,9) | YES | NO |  |  |
| promotion_amt_shop | decimal(38,9) | YES | NO |  |  |
| promotion_amt_platform | decimal(38,9) | YES | NO |  |  |
| actual_amount | decimal(38,9) | YES | NO |  |  |
| gmv | decimal(38,9) | YES | NO |  |  |
| gmv1 | decimal(38,9) | YES | NO |  |  |
| jielong_flag | varchar(1048576) | YES | NO |  |  |
| gift_gmv | decimal(38,9) | YES | NO |  |  |
| postage_shop | decimal(38,9) | YES | NO |  |  |
| postage_platform | decimal(38,9) | YES | NO |  |  |
| logistics_status | varchar(1048576) | YES | NO |  |  |
| courier_company | varchar(1048576) | YES | NO |  |  |
| courier_name | varchar(1048576) | YES | NO |  |  |
| courier_name_reverse | varchar(1048576) | YES | NO |  |  |
| courier_phone | varchar(1048576) | YES | NO |  |  |
| promotion_amt_platform_gs | decimal(38,9) | YES | NO |  |  |
| promotion_amt_platform_gys | decimal(38,9) | YES | NO |  |  |
| business_source | varchar(1048576) | YES | NO |  |  |
| activity_code | varchar(1048576) | YES | NO |  |  |
| p_mp_sub_amt | decimal(38,9) | YES | NO |  |  |
| store_paylevel_discount | decimal(38,9) | YES | NO |  |  |
| company_paylevel_discount | decimal(38,9) | YES | NO |  |  |
| day_clear | varchar(1048576) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| reason   | bundlePromoCode   | orderType   | erpOrderAt   |          orderId | refundAt   |   storePaylevelDiscount | spStoreName   | saleUnit   | internalComment   |          deliveryId | courierNameReverse   |   promotionAmtShop |   jeDate |   companyPaylevelDiscount |
|:---------|:------------------|:------------|:-------------|-----------------:|:-----------|------------------------:|:--------------|:-----------|:------------------|--------------------:|:---------------------|-------------------:|---------:|--------------------------:|
| None     | None              | normal      | None         | 8158050120074324 | None       |                       0 | 广州滨江宏岸店       | 袋          | None              | 8158050120074324001 | None                 |                  0 | 26042010 |                         0 |
| None     | None              | normal      | None         | 8153010660023335 | None       |                       0 | 广州滨江宏岸店       | 瓶          | None              | 8153010660023335001 | None                 |                  0 | 26042019 |                         0 |
| None     | None              | normal      | None         | 8152340290049404 | None       |                       0 | 广州滨江宏岸店       | 份          | None              | 8152340290049404001 | None                 |                  0 | 26042012 |                         0 |


---

## 2. `strategy_fm_purchase_di` — 进货验收（订单行级？）

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 1,821
- 行数实测（2026-04-20）: 1,821
- 当日 distinct `store_id`: 1
- 当日 distinct `article_id`: 1,748
- 评估: **✅ 预期 1,821，实测 1,821，误差 0.0%**

### 字段结构（16 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| business_date | varchar(1048576) | YES | YES |  |  |
| store_id | varchar(1048576) | YES | NO |  |  |
| day_clear | varchar(1048576) | YES | NO |  |  |
| article_id | varchar(1048576) | YES | NO |  |  |
| article_name | varchar(1048576) | YES | NO |  |  |
| sale_article_id | varchar(1048576) | YES | NO |  |  |
| sale_article_name | varchar(1048576) | YES | NO |  |  |
| sale_article_qty | decimal(38,9) | YES | NO |  |  |
| sale_article_purchase_amt | decimal(38,9) | YES | NO |  |  |
| init_stock_qty | decimal(38,9) | YES | NO |  |  |
| end_stock_qty | decimal(38,9) | YES | NO |  |  |
| init_stock_amt | decimal(38,9) | YES | NO |  |  |
| end_stock_amt | decimal(38,9) | YES | NO |  |  |
| inventory_cost | decimal(38,9) | YES | NO |  |  |
| avg_inbound_price | decimal(38,9) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| articleName     |   initStockAmt |   inventoryCost |   saleArticleId |   avgInboundPrice |   articleId |   endStockAmt | storeId   |   saleArticlePurchaseAmt | businessDate   |   initStockQty | incDay     | saleArticleName   |   endStockQty |   dayClear |
|:----------------|---------------:|----------------:|----------------:|------------------:|------------:|--------------:|:----------|-------------------------:|:---------------|---------------:|:-----------|:------------------|--------------:|-----------:|
| 洽洽山核桃瓜子68g(C)   |          33.93 |               0 |        21304200 |              3.77 |    21304200 |         33.93 | A3XV      |                     0    | 2026-04-20     |              9 | 2026-04-20 | 洽洽山核桃瓜子68g(C)     |             9 |          1 |
| 豉油鸡900g/只(Z)    |           0    |             nan |        20004965 |             41.34 |    20004965 |        nan    | A3XV      |                    41.34 | 2026-04-20     |              0 | 2026-04-20 | 豉油鸡900g/只(Z)      |           nan |          1 |
| 闽式椒香小酥肉0.8kg(C) |         145.1  |               0 |        21281457 |             14.51 |    21281457 |        145.1  | A3XV      |                     0    | 2026-04-20     |             10 | 2026-04-20 | 闽式椒香小酥肉0.8kg(C)   |            10 |          1 |


---

## 3. `strategy_fm_scm_di` — SAP 出入库

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 300
- 行数实测（2026-04-20）: 300
- 当日 distinct `store_id`: 1
- 当日 distinct `article_id`: 300
- 评估: **✅ 预期 300，实测 300，误差 0.0%**

### 字段结构（54 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| business_date | varchar(1048576) | YES | YES |  |  |
| new_dc_id | varchar(1048576) | YES | NO |  |  |
| store_id | varchar(1048576) | YES | NO |  |  |
| article_id | varchar(1048576) | YES | NO |  |  |
| out_stock_amt_cb_notax | decimal(38,9) | YES | NO |  |  |
| out_stock_pay_amt | decimal(38,9) | YES | NO |  |  |
| return_stock_pay_amt | decimal(38,9) | YES | NO |  |  |
| return_stock_amt_cb_notax | decimal(38,9) | YES | NO |  |  |
| original_outstock_qty | decimal(38,9) | YES | NO |  |  |
| original_outstock_amt | decimal(38,9) | YES | NO |  |  |
| promotion_outstock_price | decimal(38,9) | YES | NO |  |  |
| promotion_outstock_qty | decimal(38,9) | YES | NO |  |  |
| promotion_outstock_amt | decimal(38,9) | YES | NO |  |  |
| gift_outstock_qty | decimal(38,9) | YES | NO |  |  |
| total_outstock_qty | decimal(38,9) | YES | NO |  |  |
| scm_promotion_cost | decimal(38,9) | YES | NO |  |  |
| scm_return_promotion_cost | decimal(38,9) | YES | NO |  |  |
| return_stock_qty | decimal(38,9) | YES | NO |  |  |
| out_stock_zzckj_amt | decimal(38,9) | YES | NO |  |  |
| return_stock_original_amt | decimal(38,9) | YES | NO |  |  |
| store_order_qty | decimal(38,9) | YES | NO |  |  |
| order_amt | decimal(38,9) | YES | NO |  |  |
| order_qty_payean | decimal(38,9) | YES | NO |  |  |
| adjustment_amt | decimal(38,9) | YES | NO |  |  |
| adjustment_amt_notax | decimal(38,9) | YES | NO |  |  |
| scm_promotion_qty_gift | decimal(38,9) | YES | NO |  |  |
| scm_promotion_amt_gift | decimal(38,9) | YES | NO |  |  |
| scm_promotion_amt | decimal(38,9) | YES | NO |  |  |
| scm_bear_amt | decimal(38,9) | YES | NO |  |  |
| vendor_bear_amt | decimal(38,9) | YES | NO |  |  |
| business_market_bear_amt | decimal(38,9) | YES | NO |  |  |
| business_bear_amt | decimal(38,9) | YES | NO |  |  |
| market_bear_amt | decimal(38,9) | YES | NO |  |  |
| scm_promotion_amt_total | decimal(38,9) | YES | NO |  |  |
| miss_stock_qty | decimal(38,9) | YES | NO |  |  |
| miss_stock_amt | decimal(38,9) | YES | NO |  |  |
| out_stock_pay_amt_notax | decimal(38,9) | YES | NO |  |  |
| return_stock_pay_amt_notax | decimal(38,9) | YES | NO |  |  |
| out_stock_amt_cb | decimal(38,9) | YES | NO |  |  |
| return_stock_amt_cb | decimal(38,9) | YES | NO |  |  |
| vender_bear_gift_amt | decimal(38,9) | YES | NO |  |  |
| scm_bear_gift_amt | decimal(38,9) | YES | NO |  |  |
| store_return_amt_shop | decimal(38,9) | YES | NO |  |  |
| store_return_qty_shop | decimal(38,9) | YES | NO |  |  |
| qdm_bear_negative_amt_total | decimal(38,9) | YES | NO |  |  |
| qdm_bear_positive_amt_total | decimal(38,9) | YES | NO |  |  |
| qdm_bear_gift_qty | decimal(38,9) | YES | NO |  |  |
| qdm_bear_gift_amt | decimal(38,9) | YES | NO |  |  |
| qdm_bear_nogift_negative_amt | decimal(38,9) | YES | NO |  |  |
| qdm_bear_nogift_positive_amt | decimal(38,9) | YES | NO |  |  |
| qdm_bear_promotion_fee | decimal(38,9) | YES | NO |  |  |
| vender_bear_gift_qty | decimal(38,9) | YES | NO |  |  |
| scm_bear_gift_qty | decimal(38,9) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

|   scmBearAmt |   returnStockAmtCb |   scmPromotionAmt |   scmPromotionAmtGift |   orderAmt |   adjustmentAmtNotax |   outStockPayAmt |   businessMarketBearAmt |   storeReturnQtyShop |   returnStockAmtCbNotax |   giftOutstockQty |   returnStockPayAmtNotax |   scmPromotionAmtTotal |   scmBearGiftAmt |   qdmBearNogiftPositiveAmt |
|-------------:|-------------------:|------------------:|----------------------:|-----------:|---------------------:|-----------------:|------------------------:|---------------------:|------------------------:|------------------:|-------------------------:|-----------------------:|-----------------:|---------------------------:|
|            0 |                  0 |                 0 |                     0 |      39.64 |                    0 |            39.64 |                       0 |                    0 |                       0 |                 0 |                        0 |                      0 |                0 |                          0 |
|            0 |                  0 |                 0 |                     0 |      39.66 |                    0 |            39.66 |                       0 |                    0 |                       0 |                 0 |                        0 |                      0 |                0 |                          0 |
|            0 |                  0 |                 0 |                     0 |     157.92 |                    0 |             0    |                       0 |                    0 |                       0 |                 0 |                        0 |                      0 |                0 |                          0 |


---

## 4. `strategy_fm_scm_adjust_di` — SCM 差异调整（昨日可能为 0）

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 0（昨日无数据）
- 行数实测（2026-04-20）: 0
- 当日 distinct `store_id`: 0
- 当日 distinct `article_id`: 0
- 评估: **✅ 实测 0，与预期一致**

### 字段结构（10 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| business_date | date | YES | YES |  |  |
| store_id | varchar(1048576) | YES | YES |  |  |
| dc_id | varchar(1048576) | YES | NO |  |  |
| matnr | varchar(1048576) | YES | NO |  |  |
| article_id | varchar(1048576) | YES | NO |  |  |
| tax | decimal(38,9) | YES | NO |  |  |
| adjustment_amt | decimal(38,9) | YES | NO |  |  |
| adjustment_amt_notax | decimal(38,9) | YES | NO |  |  |
| new_sp_store_id | varchar(1048576) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

_（无样本数据）_


---

## 5. `strategy_fm_loss_di` — 损耗

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 219
- 行数实测（2026-04-20）: 219
- 当日 distinct `store_id`: 1
- 当日 distinct `article_id`: 219
- 评估: **✅ 预期 219，实测 219，误差 0.0%**

### 字段结构（10 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| store_id | varchar(1048576) | YES | YES |  |  |
| article_id | varchar(1048576) | YES | NO |  |  |
| article_name | varchar(1048576) | YES | NO |  |  |
| category_level1_id | varchar(1048576) | YES | NO |  |  |
| category_level1_description | varchar(1048576) | YES | NO |  |  |
| unknow_lost_qty | decimal(38,9) | YES | NO |  |  |
| unknow_lost_amt | decimal(38,9) | YES | NO |  |  |
| know_lost_qty | decimal(38,9) | YES | NO |  |  |
| know_lost_amt | decimal(38,9) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

|   unknowLostAmt |   knowLostAmt | articleName    | incDay     |   articleId | storeId   |   unknowLostQty |   knowLostQty |   categoryLevel1Id | categoryLevel1Description   |
|----------------:|--------------:|:---------------|:-----------|------------:|:----------|----------------:|--------------:|-------------------:|:----------------------------|
|            0    |       60      | 苦瓜400g(c)      | 2026-04-20 |    21292606 | A3XV      |          0      |        16     |                 10 | 蔬菜类                         |
|           -0.08 |        0      | 爱拼才会赢面包拼盘B款(C) | 2026-04-20 |    21320705 | A3XV      |         -4      |         1     |                 26 | 冷藏及加工类                      |
|            1.97 |       16.7426 | 优鲜黑猪上肉         | 2026-04-20 |    20110895 | A3XV      |          0.0919 |         0.782 |                 13 | 猪肉类                         |


---

## 6. `strategy_fm_compose_di` — 加工转换

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 22
- 行数实测（2026-04-20）: 22
- 当日 distinct `store_id`: 1
- 当日 distinct `article_id`: 22
- 评估: **✅ 预期 22，实测 22，误差 0.0%**

### 字段结构（11 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| business_date | varchar(1048576) | YES | YES |  |  |
| store_id | varchar(1048576) | YES | NO |  |  |
| store_name | varchar(1048576) | YES | NO |  |  |
| article_id | varchar(1048576) | YES | NO |  |  |
| article_name | varchar(1048576) | YES | NO |  |  |
| compose_in_qty | decimal(38,9) | YES | NO |  |  |
| compose_in_amt | decimal(38,9) | YES | NO |  |  |
| compose_out_qty | decimal(38,9) | YES | NO |  |  |
| compose_out_amt | decimal(38,9) | YES | NO |  |  |
| update_time | datetime | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| businessDate   | articleName   |   composeInAmt | incDay     |   articleId |   composeOutQty | storeName   | updateTime                                                                                                                                                      | storeId   |   composeInQty |   composeOutAmt |
|:---------------|:--------------|---------------:|:-----------|------------:|----------------:|:------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------|---------------:|----------------:|
| 2026-04-20     | 精品葡式蛋挞皮30个(C) |           0    | 2026-04-20 |    21282416 |            1.2  | 广州滨江宏岸店     | {'dayOfWeek': 'TUESDAY', 'month': 'APRIL', 'hour': 1, 'dayOfYear': 111, 'dayOfMonth': 21, 'year': 2026, 'monthValue': 4, 'nano': 0, 'minute': 34, 'second': 30} | A3XV      |              0 |           16.42 |
| 2026-04-20     | 起酥老婆饼(C)      |           0    | 2026-04-20 |    21296765 |            0.13 | 广州滨江宏岸店     | {'dayOfWeek': 'TUESDAY', 'month': 'APRIL', 'hour': 1, 'dayOfYear': 111, 'dayOfMonth': 21, 'year': 2026, 'monthValue': 4, 'nano': 0, 'minute': 34, 'second': 30} | A3XV      |              0 |            0.69 |
| 2026-04-20     | 蔓越莓贝果(C)      |          12.63 | 2026-04-20 |    21306259 |            0    | 广州滨江宏岸店     | {'dayOfWeek': 'TUESDAY', 'month': 'APRIL', 'hour': 1, 'dayOfYear': 111, 'dayOfMonth': 21, 'year': 2026, 'monthValue': 4, 'nano': 0, 'minute': 34, 'second': 30} | A3XV      |              5 |            0    |


---

## 7. `strategy_fm_allowance_di` — 活动让利

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 943
- 行数实测（2026-04-20）: 943
- 当日 distinct `store_id`: 1
- 当日 distinct `sale_article_id`: 925
- 评估: **✅ 预期 943，实测 943，误差 0.0%**

### 字段结构（76 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| business_date | varchar(1048576) | YES | YES |  |  |
| store_id | varchar(1048576) | YES | NO |  |  |
| order_article_id | varchar(1048576) | YES | NO |  |  |
| activity_name | varchar(1048576) | YES | NO |  |  |
| out_price | varchar(1048576) | YES | NO |  |  |
| sale_price | varchar(1048576) | YES | NO |  |  |
| hot_flag | varchar(1048576) | YES | NO |  |  |
| order_price | decimal(38,9) | YES | NO |  |  |
| order_qty | decimal(38,9) | YES | NO |  |  |
| settle_order_qty | decimal(38,9) | YES | NO |  |  |
| order_amt | decimal(38,9) | YES | NO |  |  |
| split_order_qty | decimal(38,9) | YES | NO |  |  |
| split_order_amt | decimal(38,9) | YES | NO |  |  |
| receive_article_id | varchar(1048576) | YES | NO |  |  |
| receive_amt | decimal(38,9) | YES | NO |  |  |
| receive_qty | decimal(38,9) | YES | NO |  |  |
| purchase_price | decimal(38,9) | YES | NO |  |  |
| sale_article_id | varchar(1048576) | YES | NO |  |  |
| sale_article_receive_qty | decimal(38,9) | YES | NO |  |  |
| sale_article_receive_amt | decimal(38,9) | YES | NO |  |  |
| sale_article_receive_price | decimal(38,9) | YES | NO |  |  |
| sum_sub_article_qty | decimal(38,9) | YES | NO |  |  |
| split_qty | decimal(38,9) | YES | NO |  |  |
| qty | decimal(38,9) | YES | NO |  |  |
| qty_spec | decimal(38,9) | YES | NO |  |  |
| split_qty_spec | decimal(38,9) | YES | NO |  |  |
| sale_amt | decimal(38,9) | YES | NO |  |  |
| split_sale_amt | decimal(38,9) | YES | NO |  |  |
| lost_qty | decimal(38,9) | YES | NO |  |  |
| lost_amt | decimal(38,9) | YES | NO |  |  |
| profit_amt | decimal(38,9) | YES | NO |  |  |
| allowance_profit_amt | decimal(38,9) | YES | NO |  |  |
| split_discount_amt | decimal(38,9) | YES | NO |  |  |
| split_member_discount_amt | decimal(38,9) | YES | NO |  |  |
| split_hour_discount_amt | decimal(38,9) | YES | NO |  |  |
| split_return_sale_qty | decimal(38,9) | YES | NO |  |  |
| split_return_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_member_hour_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_af19_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_af19_sale_qty | decimal(38,9) | YES | NO |  |  |
| split_bf9_sale_qty | decimal(38,9) | YES | NO |  |  |
| split_bf10_sale_qty | decimal(38,9) | YES | NO |  |  |
| split_bf12_sale_qty | decimal(38,9) | YES | NO |  |  |
| split_bf16_sale_qty | decimal(38,9) | YES | NO |  |  |
| split_bf19_sale_qty | decimal(38,9) | YES | NO |  |  |
| split_bf9_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_bf10_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_bf12_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_bf16_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_bf19_sale_amt | decimal(38,9) | YES | NO |  |  |
| split_p_lp_sub_amt | decimal(38,9) | YES | NO |  |  |
| split_p_sp_sub_amt | decimal(38,9) | YES | NO |  |  |
| split_promotion_discount_amt | decimal(38,9) | YES | NO |  |  |
| split_allowance_amt | decimal(38,9) | YES | NO |  |  |
| activity_type | varchar(1048576) | YES | NO |  |  |
| activity_id | varchar(1048576) | YES | NO |  |  |
| order_weight | decimal(38,9) | YES | NO |  |  |
| spilt_receive_weight | decimal(38,9) | YES | NO |  |  |
| split_bf9_sale_weight | decimal(38,9) | YES | NO |  |  |
| split_sale_weight | decimal(38,9) | YES | NO |  |  |
| split_cust_num | decimal(38,9) | YES | NO |  |  |
| split_bf19_cust_num | decimal(38,9) | YES | NO |  |  |
| split_receive_unit_qty_spec | decimal(38,9) | YES | NO |  |  |
| split_receive_unit_order_qty | decimal(38,9) | YES | NO |  |  |
| sale_article_receive_unit_qty | decimal(38,9) | YES | NO |  |  |
| last_pay_at | varchar(1048576) | YES | NO |  |  |
| list_price | decimal(38,9) | YES | NO |  |  |
| init_stock_qty | decimal(38,9) | YES | NO |  |  |
| end_stock_qty | decimal(38,9) | YES | NO |  |  |
| init_stock_amt | decimal(38,9) | YES | NO |  |  |
| end_stock_amt | decimal(38,9) | YES | NO |  |  |
| init_receiveb_qty | decimal(38,9) | YES | NO |  |  |
| init_receiveb_amt | decimal(38,9) | YES | NO |  |  |
| end_receiveb_qty | decimal(38,9) | YES | NO |  |  |
| end_receiveb_amt | decimal(38,9) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

|   splitMemberHourSaleAmt |   splitReceiveUnitQtySpec |   saleArticleReceivePrice |   receiveArticleId |   splitBf9SaleAmt |   splitAf19SaleQty |   orderAmt | purchasePrice   |   splitReturnSaleQty |   splitBf10SaleAmt |   splitBf12SaleAmt |   allowanceProfitAmt |   splitDiscountAmt |   lostAmt | activityId   |
|-------------------------:|--------------------------:|--------------------------:|-------------------:|------------------:|-------------------:|-----------:|:----------------|---------------------:|-------------------:|-------------------:|---------------------:|-------------------:|----------:|:-------------|
|                        0 |                         0 |                      7.14 |           20610234 |                 0 |                  0 |          0 | None            |                    0 |                  0 |                  0 |                 0    |                0   |         0 | None         |
|                        0 |                         1 |                      2.66 |           21269615 |                 0 |                  0 |          0 | None            |                    0 |                  0 |                  0 |                 0.44 |                0.8 |         0 | None         |
|                        0 |                         3 |                      2.58 |           21326097 |                 0 |                  2 |          0 | None            |                    0 |                  0 |                  0 |                 2.76 |                0   |         0 | None         |


---

## 8. `strategy_fm_promo_di` — 促销（订单项）

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 1,545
- 行数实测（2026-04-20）: 1,545
- 当日 distinct `shop_id`: 1
- 当日 distinct `sku_code`: 399
- 评估: **✅ 预期 1,545，实测 1,545，误差 0.0%**

### 字段结构（90 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| order_id | varchar(1048576) | YES | YES |  |  |
| order_type | varchar(1048576) | YES | NO |  |  |
| order_status | varchar(1048576) | YES | NO |  |  |
| customer_id | varchar(1048576) | YES | NO |  |  |
| delivery_id | varchar(1048576) | YES | NO |  |  |
| customer_name | varchar(1048576) | YES | NO |  |  |
| customer_phone | varchar(1048576) | YES | NO |  |  |
| shop_id | varchar(1048576) | YES | NO |  |  |
| shop_name | varchar(1048576) | YES | NO |  |  |
| parent_order_id | varchar(1048576) | YES | NO |  |  |
| product_count | decimal(38,9) | YES | NO |  |  |
| channel_id | varchar(1048576) | YES | NO |  |  |
| order_sub_type | varchar(1048576) | YES | NO |  |  |
| outer_order_id | varchar(1048576) | YES | NO |  |  |
| outer_order_type | varchar(1048576) | YES | NO |  |  |
| refund_type | varchar(1048576) | YES | NO |  |  |
| je_date | varchar(1048576) | YES | NO |  |  |
| rje_date | varchar(1048576) | YES | NO |  |  |
| refund_at | varchar(1048576) | YES | NO |  |  |
| pay_at | varchar(1048576) | YES | NO |  |  |
| order_at | varchar(1048576) | YES | NO |  |  |
| cancel_at | varchar(1048576) | YES | NO |  |  |
| business_source | int | YES | NO |  |  |
| row_num | varchar(1048576) | YES | NO |  |  |
| goods_id | varchar(1048576) | YES | NO |  |  |
| sku_code | varchar(1048576) | YES | NO |  |  |
| goods_name | varchar(1048576) | YES | NO |  |  |
| category_id | varchar(1048576) | YES | NO |  |  |
| spu_code | varchar(1048576) | YES | NO |  |  |
| bundle_promo_code | varchar(1048576) | YES | NO |  |  |
| order_item_id | varchar(1048576) | YES | NO |  |  |
| p_promo_amt | decimal(38,9) | YES | NO |  |  |
| p_promo_total_amt | decimal(38,9) | YES | NO |  |  |
| f_promo_amt | decimal(38,9) | YES | NO |  |  |
| f_promo_total_amt | decimal(38,9) | YES | NO |  |  |
| promotion_category | varchar(1048576) | YES | NO |  |  |
| promotion_code | varchar(1048576) | YES | NO |  |  |
| promotion_code2 | varchar(1048576) | YES | NO |  |  |
| promo_type | varchar(1048576) | YES | NO |  |  |
| promo_sub_type | varchar(1048576) | YES | NO |  |  |
| coupon_code | varchar(1048576) | YES | NO |  |  |
| from_outer | varchar(1048576) | YES | NO |  |  |
| outer_code | varchar(1048576) | YES | NO |  |  |
| parent_order_item_id | varchar(1048576) | YES | NO |  |  |
| parent_order_item_promotion_id | varchar(1048576) | YES | NO |  |  |
| parent_bom_order_item_id | varchar(1048576) | YES | NO |  |  |
| parent_bom_order_item_promo_id | varchar(1048576) | YES | NO |  |  |
| cost_center | varchar(1048576) | YES | NO |  |  |
| coupon_mode | varchar(1048576) | YES | NO |  |  |
| sales_charge_type | varchar(1048576) | YES | NO |  |  |
| cost_tax_rate | decimal(38,9) | YES | NO |  |  |
| allocate_rate | decimal(38,9) | YES | NO |  |  |
| promotion_cost | decimal(38,9) | YES | NO |  |  |
| promo_action_type | varchar(1048576) | YES | NO |  |  |
| purchase_limit_qty | decimal(38,9) | YES | NO |  |  |
| promotion_name | varchar(1048576) | YES | NO |  |  |
| activity_type | varchar(1048576) | YES | NO |  |  |
| activity_level | varchar(1048576) | YES | NO |  |  |
| code2 | varchar(1048576) | YES | NO |  |  |
| rank | varchar(1048576) | YES | NO |  |  |
| promo_type1 | varchar(1048576) | YES | NO |  |  |
| promo_sub_type1 | varchar(1048576) | YES | NO |  |  |
| promo_action | varchar(1048576) | YES | NO |  |  |
| eligibility_condition | varchar(1048576) | YES | NO |  |  |
| promo_condition_type | varchar(1048576) | YES | NO |  |  |
| promo_condition_context | varchar(1048576) | YES | NO |  |  |
| promo_action_context | varchar(1048576) | YES | NO |  |  |
| name | varchar(1048576) | YES | NO |  |  |
| description | varchar(1048576) | YES | NO |  |  |
| tag | varchar(1048576) | YES | NO |  |  |
| cost_center_info | varchar(1048576) | YES | NO |  |  |
| available_category | varchar(1048576) | YES | NO |  |  |
| allowance | varchar(1048576) | YES | NO |  |  |
| only_member | varchar(1048576) | YES | NO |  |  |
| title | varchar(1048576) | YES | NO |  |  |
| discount | decimal(38,9) | YES | NO |  |  |
| created_by | varchar(1048576) | YES | NO |  |  |
| promotion_type | varchar(1048576) | YES | NO |  |  |
| category_info | varchar(1048576) | YES | NO |  |  |
| source | varchar(1048576) | YES | NO |  |  |
| online_flag | varchar(1048576) | YES | NO |  |  |
| normal_inc_day | varchar(1048576) | YES | NO |  |  |
| inc_time | varchar(1048576) | YES | NO |  |  |
| jielong_flag | varchar(1048576) | YES | NO |  |  |
| activity_code | varchar(1048576) | YES | NO |  |  |
| promo_ext_prop | varchar(1048576) | YES | NO |  |  |
| cost_company | varchar(1048576) | YES | NO |  |  |
| cost_subject | varchar(1048576) | YES | NO |  |  |
| is_hour_promotion | varchar(1048576) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

|   businessSource | orderType   | bundlePromoCode   | purchaseLimitQty   |          orderId | refundAt   |   promotionCost | discount   | source   |   productCount | promoSubType   | activityCode   |          deliveryId | promoAction   |   rowNum |
|-----------------:|:------------|:------------------|:-------------------|-----------------:|:-----------|----------------:|:-----------|:---------|---------------:|:---------------|:---------------|--------------------:|:--------------|---------:|
|                0 | normal      | None              | None               | 8150080680022337 | None       |          nan    | None       |          |              1 | None           | None           | 8150080680022337001 |               |        0 |
|                0 | normal      | None              | None               | 8150200050092378 | None       |            0.87 | None       |          |             10 | freight.free   |                | 8150200050092378001 |               |        8 |
|                0 | normal      | None              | None               | 8150200050092378 | None       |            0.16 | None       |          |             10 | freight.free   |                | 8150200050092378001 |               |        6 |


---

## 9. `strategy_fm_inventory_pool_di` — 库存成本价池

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 244,523
- 行数实测（2026-04-20）: 244,523
- 当日 distinct `shop_id`: 1
- 当日 distinct `sku_code`: 1,769
- 评估: **✅ 预期 244,523，实测 244,523，误差 0.0%**

### 字段结构（18 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| id | bigint | YES | YES |  |  |
| shop_id | varchar(1048576) | YES | YES |  |  |
| inventory_date | varchar(1048576) | YES | NO |  |  |
| sku_code | varchar(1048576) | YES | NO |  |  |
| sku_name | varchar(1048576) | YES | NO |  |  |
| sub_category_id | varchar(1048576) | YES | NO |  |  |
| sub_category_name | varchar(1048576) | YES | NO |  |  |
| spec | varchar(1048576) | YES | NO |  |  |
| sales_unit | varchar(1048576) | YES | NO |  |  |
| main_img | varchar(1048576) | YES | NO |  |  |
| gift_flag | int | YES | NO |  |  |
| cost_price | decimal(32,4) | YES | NO |  |  |
| created_at | varchar(1048576) | YES | NO |  |  |
| created_by | varchar(1048576) | YES | NO |  |  |
| updated_at | varchar(1048576) | YES | NO |  |  |
| updated_by | varchar(1048576) | YES | NO |  |  |
| last_updated_at | varchar(1048576) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| mainImg                                                                  | updatedBy   |   giftFlag | lastUpdatedAt       | inventoryDate   |   costPrice |   subCategoryId | spec   | subCategoryName   | skuName            | createdAt           | createdBy   | incDay     | salesUnit   |       id |
|:-------------------------------------------------------------------------|:------------|-----------:|:--------------------|:----------------|------------:|----------------:|:-------|:------------------|:-------------------|:--------------------|:------------|:-----------|:------------|---------:|
| None                                                                     | system      |          0 | 2025-09-13 20:00:04 | 2025-09-13      |       12.79 |          273701 | 760ml  | 调味酱类              | 味事达味极鲜760ml(电商)    | 2025-09-13 20:00:04 | system      | 2026-04-20 | 瓶           | 19254290 |
| https://cnhqvztoss02.qdama.cn//oaeanimg/98a99fe5eb734b7fa7337aa7b0f554d9 | system      |          0 | 2025-09-13 20:00:04 | 2025-09-13      |        7.81 |          273701 | 500ml  | 调味酱类              | 海天0添加草菇老抽500ml(电商) | 2025-09-13 20:00:04 | system      | 2026-04-20 | 瓶           | 19254294 |
| None                                                                     | system      |          0 | 2025-09-13 20:00:04 | 2025-09-13      |       15.98 |          240101 | 600g   | 鸡蛋类               | 圣迪乐村营养谷物蛋12枚(电商)   | 2025-09-13 20:00:04 | system      | 2026-04-20 | 盒           | 19254298 |


---

## 10. `strategy_fm_price_da` — 门店商品价格

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 5,009
- 行数实测（2026-04-20）: 5,009
- 当日 distinct `shop_id`: 1
- 当日 distinct `sku_code`: 5,009
- 评估: **✅ 预期 5,009，实测 5,009，误差 0.0%**

### 字段结构（54 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| id | bigint | YES | YES |  |  |
| shop_id | varchar(1048576) | YES | YES |  |  |
| tenant_id | varchar(1048576) | YES | NO |  |  |
| shop_name | varchar(1048576) | YES | NO |  |  |
| dc_code | varchar(1048576) | YES | NO |  |  |
| dc_name | varchar(1048576) | YES | NO |  |  |
| category_code | varchar(1048576) | YES | NO |  |  |
| category_name | varchar(1048576) | YES | NO |  |  |
| sku_code | varchar(1048576) | YES | NO |  |  |
| sku_name | varchar(1048576) | YES | NO |  |  |
| current_price | decimal(38,9) | YES | NO |  |  |
| yesterday_price | decimal(38,9) | YES | NO |  |  |
| strategy_no | varchar(1048576) | YES | NO |  |  |
| promotion_no | varchar(1048576) | YES | NO |  |  |
| calc_strategy | varchar(1048576) | YES | NO |  |  |
| dc_original_price | decimal(38,9) | YES | NO |  |  |
| dc_price | decimal(38,9) | YES | NO |  |  |
| anchor_sale_price | decimal(38,9) | YES | NO |  |  |
| deal_status | varchar(1048576) | YES | NO |  |  |
| exception_type | varchar(1048576) | YES | NO |  |  |
| exception_reason | varchar(1048576) | YES | NO |  |  |
| monitor_type | varchar(1048576) | YES | NO |  |  |
| monitor_desc | varchar(1048576) | YES | NO |  |  |
| calc_status | varchar(1048576) | YES | NO |  |  |
| confirm_status | varchar(1048576) | YES | NO |  |  |
| sale_status | varchar(1048576) | YES | NO |  |  |
| lock_status | int | YES | NO |  |  |
| push_status | varchar(1048576) | YES | NO |  |  |
| confirm_by | varchar(1048576) | YES | NO |  |  |
| calc_by | varchar(1048576) | YES | NO |  |  |
| calc_effect_at | varchar(1048576) | YES | NO |  |  |
| source | varchar(1048576) | YES | NO |  |  |
| shop_sku | varchar(1048576) | YES | NO |  |  |
| sales_unit | varchar(1048576) | YES | NO |  |  |
| yesterday_dc_price | decimal(38,9) | YES | NO |  |  |
| created_at | varchar(1048576) | YES | NO |  |  |
| created_by | varchar(1048576) | YES | NO |  |  |
| updated_at | varchar(1048576) | YES | NO |  |  |
| updated_by | varchar(1048576) | YES | NO |  |  |
| last_updated_at | varchar(1048576) | YES | NO |  |  |
| shop_id_sku_code_calc_at | varchar(1048576) | YES | NO |  |  |
| out_updated_at | bigint | YES | NO |  |  |
| original_price | decimal(38,9) | YES | NO |  |  |
| effective | int | YES | NO |  |  |
| unadjust_sale_price | decimal(38,9) | YES | NO |  |  |
| is_new | int | YES | NO |  |  |
| original_dc_price | decimal(38,9) | YES | NO |  |  |
| dc_original_price_sap | decimal(38,9) | YES | NO |  |  |
| sales_mode | varchar(1048576) | YES | NO |  |  |
| outstock_addprice_amt | decimal(38,9) | YES | NO |  |  |
| outstock_addprice_rate | decimal(38,9) | YES | NO |  |  |
| outstock_profit_rate | decimal(38,9) | YES | NO |  |  |
| outstock_lock_price | decimal(38,9) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| lastUpdatedAt       | source   | createdAt           | monitorType   | monitorDesc   | dcName    | exceptionReason   | confirmStatus   |   yesterdayDcPrice | unadjustSalePrice   |          id | shopId   | calcEffectAt   | updatedAt           | strategyNo   |
|:--------------------|:---------|:--------------------|:--------------|:--------------|:----------|:------------------|:----------------|-------------------:|:--------------------|------------:|:---------|:---------------|:--------------------|:-------------|
| 2026-04-20 01:00:21 |          | 2026-04-19 12:23:08 | success       | None          | 广州江高虚拟生鲜仓 | 成功                |                 |              23.32 | None                | 28537726918 | A3XV     | 2026-04-20     | 2026-04-19 12:23:08 | 门店商品价格策略配置   |
| 2026-04-20 01:00:21 |          | 2026-04-19 12:23:08 | success       | None          | 广州江高虚拟生鲜仓 | 成功                |                 |              59.36 | None                | 28537726920 | A3XV     | 2026-04-20     | 2026-04-19 12:23:08 | 门店商品价格策略配置   |
| 2026-04-20 01:00:21 |          | 2026-04-19 12:23:08 | success       | None          | 广州江高虚拟生鲜仓 | 成功                |                 |              25.44 | None                | 28537726932 | A3XV     | 2026-04-20     | 2026-04-19 12:23:08 | 门店商品价格策略配置   |


---

## 11. `strategy_fm_dim_day_clear` — 日清商品清单

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 92,076
- 行数实测（2026-04-20）: 92,076
- 当日 distinct `store_id`: 1
- 当日 distinct `article_id`: 92,075
- 评估: **✅ 预期 92,076，实测 92,076，误差 0.0%**

### 字段结构（11 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| business_date | varchar(1048576) | YES | YES |  |  |
| store_id | varchar(1048576) | YES | NO |  |  |
| article_id | varchar(1048576) | YES | NO |  |  |
| article_name | varchar(1048576) | YES | NO |  |  |
| category_level3_id | varchar(1048576) | YES | NO |  |  |
| category_level3_description | varchar(1048576) | YES | NO |  |  |
| category_level2_id | varchar(1048576) | YES | NO |  |  |
| category_level2_description | varchar(1048576) | YES | NO |  |  |
| category_level1_id | varchar(1048576) | YES | NO |  |  |
| category_level1_description | varchar(1048576) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

|   categoryLevel3Id | businessDate   | categoryLevel2Description   | articleName       | incDay     |   articleId | storeId   | categoryLevel3Description   |   categoryLevel1Id | categoryLevel1Description   |   categoryLevel2Id |
|-------------------:|:---------------|:----------------------------|:------------------|:-----------|------------:|:----------|:----------------------------|-------------------:|:----------------------------|-------------------:|
|             273901 | 2026-04-20     | 日杂用品类                       | 爱厨油壶调味罐五件套5件套(电商) | 2026-04-20 |    20908263 | A3XV      | 餐具类                         |                 27 | 标品类                         |               2739 |
|             241702 | 2026-04-20     | 羊肉类                         | 国产羔羊带骨羊后腿2kg(电商)  | 2026-04-20 |    20078003 | A3XV      | 羊分体类                        |                 24 | 肉禽蛋类                        |               2417 |
|             260305 | 2026-04-20     | 烘焙类                         | 桃李豆小方起酥面包140G(电商) | 2026-04-20 |    20827526 | A3XV      | 面包类                         |                 26 | 冷藏及加工类                      |               2603 |


---

## 12. `strategy_fm_dim_store_profile` — 门店画像

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 1
- 行数实测（2026-04-20）: 1
- 评估: **✅ 预期 1，实测 1，误差 0.0%**

### 字段结构（109 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| sp_store_id | varchar(1048576) | YES | YES |  |  |
| sp_store_name | varchar(1048576) | YES | NO |  |  |
| sp_type | varchar(1048576) | YES | NO |  |  |
| sp_level | varchar(1048576) | YES | NO |  |  |
| dist_id | varchar(1048576) | YES | NO |  |  |
| dist_description | varchar(1048576) | YES | NO |  |  |
| city_id | varchar(1048576) | YES | NO |  |  |
| city_description | varchar(1048576) | YES | NO |  |  |
| pro_id | varchar(1048576) | YES | NO |  |  |
| pro_description | varchar(1048576) | YES | NO |  |  |
| area_id | varchar(1048576) | YES | NO |  |  |
| area_description | varchar(1048576) | YES | NO |  |  |
| sp_purchasing_center_id | varchar(1048576) | YES | NO |  |  |
| sp_company_id | varchar(1048576) | YES | NO |  |  |
| sp_purchasing_area | varchar(1048576) | YES | NO |  |  |
| sp_store_status | int | YES | NO |  |  |
| sp_store_effective_date | varchar(1048576) | YES | NO |  |  |
| open_days | varchar(1048576) | YES | NO |  |  |
| sp_phone1 | varchar(1048576) | YES | NO |  |  |
| sp_phone2 | varchar(1048576) | YES | NO |  |  |
| sp_phone3 | varchar(1048576) | YES | NO |  |  |
| sp_address | varchar(1048576) | YES | NO |  |  |
| eblc_longitude | varchar(1048576) | YES | NO |  |  |
| eblc_latitude | varchar(1048576) | YES | NO |  |  |
| sp_origin_start_date | varchar(1048576) | YES | NO |  |  |
| sp_final_end_date | varchar(1048576) | YES | NO |  |  |
| group_manager_code | varchar(1048576) | YES | NO |  |  |
| group_manager | varchar(1048576) | YES | NO |  |  |
| sp_scale | varchar(1048576) | YES | NO |  |  |
| sp_rsv_status | varchar(1048576) | YES | NO |  |  |
| sp_sign_date | varchar(1048576) | YES | NO |  |  |
| sp_table_flag | varchar(1048576) | YES | NO |  |  |
| sp_closed_reason | varchar(1048576) | YES | NO |  |  |
| sp_currency | varchar(1048576) | YES | NO |  |  |
| sp_master_area | varchar(1048576) | YES | NO |  |  |
| op_area_id | varchar(1048576) | YES | NO |  |  |
| total_area | decimal(38,9) | YES | NO |  |  |
| area_description_alias | varchar(1048576) | YES | NO |  |  |
| new_sp_store_id | varchar(1048576) | YES | NO |  |  |
| new_sp_store_name | varchar(1048576) | YES | NO |  |  |
| mandt | varchar(1048576) | YES | NO |  |  |
| zone_manager | varchar(1048576) | YES | NO |  |  |
| zone_id | varchar(1048576) | YES | NO |  |  |
| opera_manager | varchar(1048576) | YES | NO |  |  |
| opera_id | varchar(1048576) | YES | NO |  |  |
| new_sp_level | varchar(1048576) | YES | NO |  |  |
| group_manager_tel | varchar(1048576) | YES | NO |  |  |
| transfer_date | varchar(1048576) | YES | NO |  |  |
| transfer_store_id | varchar(1048576) | YES | NO |  |  |
| stop_start_date | varchar(1048576) | YES | NO |  |  |
| stop_end_date | varchar(1048576) | YES | NO |  |  |
| stop_reason_id | varchar(1048576) | YES | NO |  |  |
| stop_reason | varchar(1048576) | YES | NO |  |  |
| restart_date | varchar(1048576) | YES | NO |  |  |
| business_area | varchar(1048576) | YES | NO |  |  |
| area_id_sap | varchar(1048576) | YES | NO |  |  |
| area_id_purchase | varchar(1048576) | YES | NO |  |  |
| operate_id_purchase | varchar(1048576) | YES | NO |  |  |
| area2_id | varchar(1048576) | YES | NO |  |  |
| area2_name | varchar(1048576) | YES | NO |  |  |
| area2_id_sap | varchar(1048576) | YES | NO |  |  |
| operate_name_purchase | varchar(1048576) | YES | NO |  |  |
| zone_supper_manager | varchar(1048576) | YES | NO |  |  |
| zone_supper_id | varchar(1048576) | YES | NO |  |  |
| zone_supper_phone | varchar(1048576) | YES | NO |  |  |
| zone_phone | varchar(1048576) | YES | NO |  |  |
| franchisee_id | varchar(1048576) | YES | NO |  |  |
| zzlksrq | varchar(1048576) | YES | NO |  |  |
| zzljsrq | varchar(1048576) | YES | NO |  |  |
| area_type | varchar(1048576) | YES | NO |  |  |
| measuring_area | decimal(38,9) | YES | NO |  |  |
| franchisee_name | varchar(1048576) | YES | NO |  |  |
| stop_reason_apply | varchar(1048576) | YES | NO |  |  |
| old_id | varchar(1048576) | YES | NO |  |  |
| contract_franchisee_id | varchar(1048576) | YES | NO |  |  |
| contract_franchisee_name | varchar(1048576) | YES | NO |  |  |
| contract_franchisee_phone | varchar(1048576) | YES | NO |  |  |
| expand_staff_id | varchar(1048576) | YES | NO |  |  |
| expand_staff_name | varchar(1048576) | YES | NO |  |  |
| new_sp_level_name | varchar(1048576) | YES | NO |  |  |
| target_sales_amt | decimal(38,9) | YES | NO |  |  |
| target_bf19_cust_num | int | YES | NO |  |  |
| target_cust_num | int | YES | NO |  |  |
| targer_allowance_profit | decimal(38,9) | YES | NO |  |  |
| price_strategy_start_date | date | YES | NO |  |  |
| sap_store_status_id | varchar(1048576) | YES | NO |  |  |
| sap_store_status_name | varchar(1048576) | YES | NO |  |  |
| store_type_name | varchar(1048576) | YES | NO |  |  |
| store_flag_name | varchar(1048576) | YES | NO |  |  |
| closed_reason_name | varchar(1048576) | YES | NO |  |  |
| manage_area_id | varchar(1048576) | YES | NO |  |  |
| manage_area_name | varchar(1048576) | YES | NO |  |  |
| region_id | varchar(1048576) | YES | NO |  |  |
| region_name | varchar(1048576) | YES | NO |  |  |
| store_guide_user | varchar(1048576) | YES | NO |  |  |
| sap_area_id | varchar(1048576) | YES | NO |  |  |
| sap_area_name | varchar(1048576) | YES | NO |  |  |
| sap_area2_id | varchar(1048576) | YES | NO |  |  |
| sap_area2_name | varchar(1048576) | YES | NO |  |  |
| bunk_id | varchar(1048576) | YES | NO |  |  |
| store_service_range | varchar(1048576) | YES | NO |  |  |
| store_service_name | varchar(1048576) | YES | NO |  |  |
| mall_supervisor_phone | varchar(1048576) | YES | NO |  |  |
| mall_supervisor_name | varchar(1048576) | YES | NO |  |  |
| zman_id | varchar(1048576) | YES | NO |  |  |
| zman_name | varchar(1048576) | YES | NO |  |  |
| original_store_id | varchar(1048576) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |
| pt | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| newSpLevelName   |   pt | area2Name   | spStoreName   | proDescription   | sapStoreStatusName   |   cityId |   eblcLongitude | transferDate   | spPhone3   | areaDescriptionAlias   | stopReasonApply   |   sapStoreStatusId |    spPhone1 | spPhone2   |
|:-----------------|-----:|:------------|:--------------|:-----------------|:---------------------|---------:|----------------:|:---------------|:-----------|:-----------------------|:------------------|-------------------:|------------:|:-----------|
| 新业务门店            |   01 | 孵化项目        | 广州滨江宏岸店       | 广东               | 营业中                  |        1 |           113.3 | None           | None       | 孵化项目                   | None              |                 20 | 13268252003 |            |


---

## 13. `strategy_fm_dim_saleable` — 可售商品

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 1,255
- 行数实测（2026-04-20）: 1,255
- 当日 distinct `shop_id`: 1
- 当日 distinct `sku_code`: 1,255
- 评估: **✅ 预期 1,255，实测 1,255，误差 0.0%**

### 字段结构（47 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| id | bigint | YES | YES |  |  |
| tenant_id | varchar(1048576) | YES | YES |  |  |
| shop_id | varchar(1048576) | YES | NO |  |  |
| shop_name | varchar(1048576) | YES | NO |  |  |
| order_at | varchar(1048576) | YES | NO |  |  |
| category_code | varchar(1048576) | YES | NO |  |  |
| category_name | varchar(1048576) | YES | NO |  |  |
| mid_category_code | varchar(1048576) | YES | NO |  |  |
| mid_category_name | varchar(1048576) | YES | NO |  |  |
| sub_category_code | varchar(1048576) | YES | NO |  |  |
| sub_category_name | varchar(1048576) | YES | NO |  |  |
| sku_code | varchar(1048576) | YES | NO |  |  |
| sku_name | varchar(1048576) | YES | NO |  |  |
| sell_unit | varchar(1048576) | YES | NO |  |  |
| pur_unit | varchar(1048576) | YES | NO |  |  |
| article_spec | varchar(1048576) | YES | NO |  |  |
| old_spec | varchar(1048576) | YES | NO |  |  |
| spec_update_date | varchar(1048576) | YES | NO |  |  |
| purchase_flag | int | YES | NO |  |  |
| basic_qty | decimal(19,4) | YES | NO |  |  |
| min_batch_qty | decimal(19,4) | YES | NO |  |  |
| max_batch_qty | decimal(19,4) | YES | NO |  |  |
| propose_qty | decimal(19,4) | YES | NO |  |  |
| purchase_price | decimal(19,4) | YES | NO |  |  |
| is_special_price | int | YES | NO |  |  |
| order_qty | decimal(19,4) | YES | NO |  |  |
| order_amount | decimal(19,4) | YES | NO |  |  |
| order_weight | decimal(19,4) | YES | NO |  |  |
| conver_rate | decimal(19,4) | YES | NO |  |  |
| only_morning | int | YES | NO |  |  |
| sku_created_at | varchar(1048576) | YES | NO |  |  |
| core_flag | int | YES | NO |  |  |
| tips_flag | int | YES | NO |  |  |
| seven_days_order_count | int | YES | NO |  |  |
| seven_days_order_qty | decimal(19,4) | YES | NO |  |  |
| created_at | varchar(1048576) | YES | NO |  |  |
| created_by | varchar(1048576) | YES | NO |  |  |
| updated_at | varchar(1048576) | YES | NO |  |  |
| updated_by | varchar(1048576) | YES | NO |  |  |
| last_updated_at | varchar(1048576) | YES | NO |  |  |
| is_deleted | int | YES | NO |  |  |
| propose_desc | varchar(1048576) | YES | NO |  |  |
| main_img | varchar(1048576) | YES | NO |  |  |
| allow_single_order | int | YES | NO |  |  |
| sku_tag | int | YES | NO |  |  |
| receive_sku_code | varchar(1048576) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| mainImg                                    |   purchaseFlag |   allowSingleOrder | oldSpec   | lastUpdatedAt       |   maxBatchQty |   minBatchQty | shopName   |   basicQty |   orderWeight |   coreFlag |   purchasePrice | categoryName   |   midCategoryCode | subCategoryName   |
|:-------------------------------------------|---------------:|-------------------:|:----------|:--------------------|--------------:|--------------:|:-----------|-----------:|--------------:|-----------:|----------------:|:---------------|------------------:|:------------------|
| FqOKjUaTdpDMpN728NrxVvGSQPnq               |              0 |                  1 | 280G      | 2026-04-20 06:32:45 |         10000 |             1 | 广州滨江宏岸店    |          1 |             0 |          0 |           11.7  | 水产类            |              1123 | 秋刀鱼类              |
| /oaeanimg/e9333892d95944da872b3a07fe26797e |              0 |                  1 |           | 2026-04-20 06:32:45 |          1000 |             1 | 广州滨江宏岸店    |          1 |             0 |          0 |           10.28 | 水产类            |              1122 | 其他淡水鱼类            |
| /oaeanimg/bab6b69533344c7689ce21560312292d |              0 |                  1 |           | 2026-04-20 06:32:45 |          1000 |             1 | 广州滨江宏岸店    |          1 |             0 |          0 |           29.15 | 水产类            |              1123 | 其他海水鱼类            |


---

## 14. `strategy_fm_dim_goods` — 商品主数据

- 日期过滤列: `inc_day`
- 行数预期（用户给定）: 92,539
- 行数实测（2026-04-20）: 92,539
- 当日 distinct `article_id`: 92,539
- 评估: **✅ 预期 92,539，实测 92,539，误差 0.0%**

### 字段结构（106 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| article_id | varchar(1048576) | YES | YES |  |  |
| article_name | varchar(1048576) | YES | NO |  |  |
| category_level3_id | varchar(1048576) | YES | NO |  |  |
| category_level3_description | varchar(1048576) | YES | NO |  |  |
| category_level2_id | varchar(1048576) | YES | NO |  |  |
| category_level2_description | varchar(1048576) | YES | NO |  |  |
| category_level1_id | varchar(1048576) | YES | NO |  |  |
| category_level1_description | varchar(1048576) | YES | NO |  |  |
| sales_unit_id | varchar(1048576) | YES | NO |  |  |
| sale_unit | varchar(1048576) | YES | NO |  |  |
| order_unit_id | varchar(1048576) | YES | NO |  |  |
| order_unit | varchar(1048576) | YES | NO |  |  |
| brand | varchar(1048576) | YES | NO |  |  |
| product_address | varchar(1048576) | YES | NO |  |  |
| norms | varchar(1048576) | YES | NO |  |  |
| logistics_process_tag | varchar(1048576) | YES | NO |  |  |
| logistics_scrap_tag | varchar(1048576) | YES | NO |  |  |
| online_tag | varchar(1048576) | YES | NO |  |  |
| private | varchar(1048576) | YES | NO |  |  |
| barcode | varchar(1048576) | YES | NO |  |  |
| min_order_unit | varchar(1048576) | YES | NO |  |  |
| min_order_times | varchar(1048576) | YES | NO |  |  |
| max_order_times | varchar(1048576) | YES | NO |  |  |
| xx_sl | varchar(1048576) | YES | NO |  |  |
| jx_sl | varchar(1048576) | YES | NO |  |  |
| order_frequency | varchar(1048576) | YES | NO |  |  |
| status_code | varchar(1048576) | YES | NO |  |  |
| commodity_attribute | varchar(1048576) | YES | NO |  |  |
| load_time | varchar(1048576) | YES | NO |  |  |
| life_cycle | varchar(1048576) | YES | NO |  |  |
| unit_weight | decimal(38,9) | YES | NO |  |  |
| onlineshop_flag | varchar(1048576) | YES | NO |  |  |
| offlineshop_flag | varchar(1048576) | YES | NO |  |  |
| vegetablebar_flag | varchar(1048576) | YES | NO |  |  |
| package_attribute | varchar(1048576) | YES | NO |  |  |
| type | varchar(1048576) | YES | NO |  |  |
| article_type | varchar(1048576) | YES | NO |  |  |
| weight_flag | varchar(1048576) | YES | NO |  |  |
| commodity_attribute_name | varchar(1048576) | YES | NO |  |  |
| package_attribute_name | varchar(1048576) | YES | NO |  |  |
| status_name | varchar(1048576) | YES | NO |  |  |
| type_name | varchar(1048576) | YES | NO |  |  |
| abi_volume_ratio | decimal(38,9) | YES | NO |  |  |
| abi_purchasecategory_id | varchar(1048576) | YES | NO |  |  |
| abi_srmcategory | bigint | YES | NO |  |  |
| abi_quality_days | bigint | YES | NO |  |  |
| abi_outer_packing_spec | varchar(1048576) | YES | NO |  |  |
| abi_create_reason | varchar(1048576) | YES | NO |  |  |
| abi_outer_packing_lwh | varchar(1048576) | YES | NO |  |  |
| abi_purchase_group | varchar(1048576) | YES | NO |  |  |
| abi_new_category_id | varchar(1048576) | YES | NO |  |  |
| old_article_id | varchar(1048576) | YES | NO |  |  |
| old_article_name | varchar(1048576) | YES | NO |  |  |
| old_category_level3_id | varchar(1048576) | YES | NO |  |  |
| old_category_level3_description | varchar(1048576) | YES | NO |  |  |
| old_category_level2_id | varchar(1048576) | YES | NO |  |  |
| old_category_level2_description | varchar(1048576) | YES | NO |  |  |
| old_category_level1_id | varchar(1048576) | YES | NO |  |  |
| old_category_level1_description | varchar(1048576) | YES | NO |  |  |
| article_matnr | varchar(1048576) | YES | NO |  |  |
| matnr_unit_id | varchar(1048576) | YES | NO |  |  |
| matnr_unit | varchar(1048576) | YES | NO |  |  |
| zglfz | decimal(38,9) | YES | NO |  |  |
| zglfm | decimal(38,9) | YES | NO |  |  |
| sp_info | varchar(1048576) | YES | NO |  |  |
| mtart | varchar(1048576) | YES | NO |  |  |
| in_date | varchar(1048576) | YES | NO |  |  |
| create_date1 | varchar(1048576) | YES | NO |  |  |
| out_date | varchar(1048576) | YES | NO |  |  |
| matnr | varchar(1048576) | YES | NO |  |  |
| abi_purchase_group_name | varchar(1048576) | YES | NO |  |  |
| sp_info_name | varchar(1048576) | YES | NO |  |  |
| brand_id | varchar(1048576) | YES | NO |  |  |
| sale_areas | varchar(1048576) | YES | NO |  |  |
| use_types | varchar(1048576) | YES | NO |  |  |
| resent_use_date | varchar(1048576) | YES | NO |  |  |
| matnr_in_date | varchar(1048576) | YES | NO |  |  |
| purchase_department | varchar(1048576) | YES | NO |  |  |
| purchase_department_id | varchar(1048576) | YES | NO |  |  |
| min_pack_weight | decimal(38,9) | YES | NO |  |  |
| shelf_time | varchar(1048576) | YES | NO |  |  |
| freeze_id | varchar(1048576) | YES | NO |  |  |
| atob_value | varchar(1048576) | YES | NO |  |  |
| atob_name | varchar(1048576) | YES | NO |  |  |
| relation_matnr | varchar(1048576) | YES | NO |  |  |
| matnr_name | varchar(1048576) | YES | NO |  |  |
| article_belong_id | varchar(1048576) | YES | NO |  |  |
| article_belong_name | varchar(1048576) | YES | NO |  |  |
| if_settle_unit | int | YES | NO |  |  |
| superior_purchase_department_id | varchar(1048576) | YES | NO |  |  |
| superior_purchase_department_name | varchar(1048576) | YES | NO |  |  |
| category_material_label_id | varchar(1048576) | YES | NO |  |  |
| category_material_label_name | varchar(1048576) | YES | NO |  |  |
| spu_id | varchar(1048576) | YES | NO |  |  |
| spu_name | varchar(1048576) | YES | NO |  |  |
| article_series_id | varchar(1048576) | YES | NO |  |  |
| article_series_name | varchar(1048576) | YES | NO |  |  |
| temperature_layer_id | varchar(1048576) | YES | NO |  |  |
| temperature_layer_name | varchar(1048576) | YES | NO |  |  |
| import_flag | varchar(1048576) | YES | NO |  |  |
| blackwhite_pig_id | varchar(1048576) | YES | NO |  |  |
| blackwhite_pig_name | varchar(1048576) | YES | NO |  |  |
| norms_lower_limit | decimal(38,9) | YES | NO |  |  |
| norms_upper_limit | decimal(38,9) | YES | NO |  |  |
| inc_day | varchar(1048576) | YES | NO |  |  |
| pt | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

|   zglfm |   pt |              matnr |   shelfTime | type   | saleUnit   |   offlineshopFlag | abiOuterPackingSpec   |   zglfz | salesUnitId   | categoryMaterialLabelId   | importFlag   |   weightFlag |   vegetablebarFlag |   abiVolumeRatio |
|--------:|-----:|-------------------:|------------:|:-------|:-----------|------------------:|:----------------------|--------:|:--------------|:--------------------------|:-------------|-------------:|-------------------:|-----------------:|
|    1000 |   01 | 000000000000101855 |          01 |        | 千克         |                 0 | None                  |    1000 | KG            | S                         |              |            1 |                  0 |                0 |
|    1000 |   01 | 000000000000101641 |          01 |        | 千克         |                 0 | None                  |    1000 | KG            | S                         |              |            1 |                  0 |                0 |
|    1000 |   01 | 000000000000101766 |          01 |        | 千克         |                 0 | None                  |    1000 | KG            | S                         |              |            1 |                  0 |                0 |


---

## 15. `strategy_fm_dim_calendar` — 日历维度（含全量）

- 日期过滤列: `day_date`
- 行数预期（用户给定）: 全量
- 行数实测（2026-04-20）: 1
- 评估: **实测 1 行（无基线参考）**

### 字段结构（70 列）

| field | type | null | key | default | comment |
|---|---|---|---|---|---|
| day_wid | varchar(1048576) | YES | YES |  |  |
| day_name | varchar(1048576) | YES | NO |  |  |
| day_date_chn | varchar(1048576) | YES | NO |  |  |
| day_date | varchar(1048576) | YES | NO |  |  |
| day_name_of_week | varchar(1048576) | YES | NO |  |  |
| day_of_week | varchar(1048576) | YES | NO |  |  |
| day_of_month | varchar(1048576) | YES | NO |  |  |
| day_of_year | varchar(1048576) | YES | NO |  |  |
| week_wid | varchar(1048576) | YES | NO |  |  |
| week_name | varchar(1048576) | YES | NO |  |  |
| week_no | varchar(1048576) | YES | NO |  |  |
| week_start_date_wid | varchar(1048576) | YES | NO |  |  |
| week_start_date | varchar(1048576) | YES | NO |  |  |
| week_end_date_wid | varchar(1048576) | YES | NO |  |  |
| week_end_date | varchar(1048576) | YES | NO |  |  |
| month_wid | varchar(1048576) | YES | NO |  |  |
| month_name | varchar(1048576) | YES | NO |  |  |
| month_no | varchar(1048576) | YES | NO |  |  |
| month_days | varchar(1048576) | YES | NO |  |  |
| month_start_date_wid | varchar(1048576) | YES | NO |  |  |
| month_start_date | varchar(1048576) | YES | NO |  |  |
| month_end_date_wid | varchar(1048576) | YES | NO |  |  |
| month_end_date | varchar(1048576) | YES | NO |  |  |
| quarter_wid | varchar(1048576) | YES | NO |  |  |
| quarter_name | varchar(1048576) | YES | NO |  |  |
| quarter_no | varchar(1048576) | YES | NO |  |  |
| quarter_start_date_wid | varchar(1048576) | YES | NO |  |  |
| quarter_start_date | varchar(1048576) | YES | NO |  |  |
| quarter_end_date_wid | varchar(1048576) | YES | NO |  |  |
| quarter_end_date | varchar(1048576) | YES | NO |  |  |
| year_wid | varchar(1048576) | YES | NO |  |  |
| year_name | varchar(1048576) | YES | NO |  |  |
| year_start_date_wid | varchar(1048576) | YES | NO |  |  |
| year_start_date | varchar(1048576) | YES | NO |  |  |
| year_end_date_wid | varchar(1048576) | YES | NO |  |  |
| year_end_date | varchar(1048576) | YES | NO |  |  |
| is_last_day_of_week | varchar(1048576) | YES | NO |  |  |
| is_last_day_of_month | varchar(1048576) | YES | NO |  |  |
| is_last_day_of_year | varchar(1048576) | YES | NO |  |  |
| is_weekend | varchar(1048576) | YES | NO |  |  |
| holiday_name | varchar(1048576) | YES | NO |  |  |
| day_ago_date_wid | varchar(1048576) | YES | NO |  |  |
| day_ago_date | varchar(1048576) | YES | NO |  |  |
| week_ago_date_wid | varchar(1048576) | YES | NO |  |  |
| week_ago_date | varchar(1048576) | YES | NO |  |  |
| month_ago_date_wid | varchar(1048576) | YES | NO |  |  |
| month_ago_date | varchar(1048576) | YES | NO |  |  |
| quarter_ago_date_wid | varchar(1048576) | YES | NO |  |  |
| quarter_ago_date | varchar(1048576) | YES | NO |  |  |
| year_ago_date_wid | varchar(1048576) | YES | NO |  |  |
| year_ago_date | varchar(1048576) | YES | NO |  |  |
| language | varchar(1048576) | YES | NO |  |  |
| w_insert_date | varchar(1048576) | YES | NO |  |  |
| w_update_date | varchar(1048576) | YES | NO |  |  |
| is_actual_holiday | varchar(1048576) | YES | NO |  |  |
| actual_holiday_name | varchar(1048576) | YES | NO |  |  |
| is_actual_overwork | varchar(1048576) | YES | NO |  |  |
| is_rest_day | varchar(1048576) | YES | NO |  |  |
| actual_week_no | varchar(1048576) | YES | NO |  |  |
| day54_of_week | varchar(1048576) | YES | NO |  |  |
| week54_wid | varchar(1048576) | YES | NO |  |  |
| week54_name | varchar(1048576) | YES | NO |  |  |
| week54_no | varchar(1048576) | YES | NO |  |  |
| week54_start_date_wid | varchar(1048576) | YES | NO |  |  |
| week54_start_date | varchar(1048576) | YES | NO |  |  |
| week54_end_date_wid | varchar(1048576) | YES | NO |  |  |
| week54_end_date | varchar(1048576) | YES | NO |  |  |
| week_no_name | varchar(1048576) | YES | NO |  |  |
| analysis_week_wid | varchar(1048576) | YES | NO |  |  |
| analysis_week_name | varchar(1048576) | YES | NO |  |  |

### 样本（前 3 行 × 前 15 列）

| dayName    |   dayOfYear | yearEndDate   |   monthDays |   weekAgoDateWid |   quarterStartDateWid | language   | weekNoName   |   dayAgoDateWid | yearStartDate   |   yearEndDateWid | dayDateChn       | isLastDayOfMonth   | quarterName   | weekAgoDate   |
|:-----------|------------:|:--------------|------------:|-----------------:|----------------------:|:-----------|:-------------|----------------:|:----------------|-----------------:|:-----------------|:-------------------|:--------------|:--------------|
| 2013-01-04 |           4 | 2013-12-31    |          31 |         20121228 |              20130101 | ZHS        | 2012年01周     |        20130103 | 2013-01-01      |         20131231 | 農歷 2012年 11月 23日 | N                  | 2013年1季度      | 2012-12-28    |
| 2013-01-08 |           8 | 2013-12-31    |          31 |         20130101 |              20130101 | ZHS        | 2013年01周     |        20130107 | 2013-01-01      |         20131231 | 農歷 2012年 11月 27日 | N                  | 2013年1季度      | 2013-01-01    |
| 2013-01-12 |          12 | 2013-12-31    |          31 |         20130105 |              20130101 | ZHS        | 2013年01周     |        20130111 | 2013-01-01      |         20131231 | 農歷 2012年 12月 01日 | N                  | 2013年1季度      | 2013-01-05    |
