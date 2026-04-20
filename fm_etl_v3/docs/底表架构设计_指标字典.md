# strategy_fm_flag_sku_di 底层架构梳理（v3）

> **版本说明**：v3 基于 v2 修订，根据实际代码校验，补充完整的指标字典、原子/计算层分类、数据血缘图。

## Context

`strategy_fm_flag_sku_di` 依赖多层 ETL 管道产出的宽表。需要严格区分"独立可观测的原子数据"和"可由公式推导的计算指标"。

**核心原则**：
- **Layer -2（原子层）**：只放**不可再分解的独立观测量**（数量、单价、标识、BOM关系、POS交易金额、SAP让利金额）
- **Layer -1（计算层）**：放**所有可由公式推导的指标**（金额、库存余额、差异、毛利）
- **目的**：底层改一个值，上层自动算对；Python本地处理；AI分析时SKU毛利损耗不出错

---

## 一、原子层 vs 计算层分类总览

### 1.1 字段类型标记

| 标记 | 含义 | 说明 |
|------|------|------|
| **①** | 原子（独立观测量） | 直接从源系统获取，不可由其他字段推导 |
| **②** | 计算（公式推导） | 由原子字段通过公式计算得出 |
| **①/②** | 条件原子 | 有盘点数据时为原子①，无盘点数据时为计算② |

### 1.2 分类汇总表

| 分类 | 原子① | 计算② | 条件①/② |
|------|-------|-------|---------|
| 数量类 | sale_qty, receive_qty, know_lost_qty, compose_in/out_qty, outstock_qty, return_qty, init_stock_qty, physical_count_qty | unknow_lost_qty, end_stock_qty（无盘点时）, lost_qty, total_outstock_qty | end_stock_qty |
| 单价类 | cost_price, current_price, original_price, outstock_unit_price（4个）, return_unit_price（4个）, order_unit_price | avg_purchase_price, avg_price | - |
| 交易金额类（POS直录） | sale_amt, original_price_sale_amt, vip_discount_amt, hour_discount_amt, actual_amount, return_sale_amt | - | - |
| SAP让利金额类（SAP直录） | scm_promotion_amt_total, scm_promotion_amt_gift, scm_bear_amt, vendor_bear_amt, business_bear_amt, market_bear_amt, vender_bear_gift_amt, scm_bear_gift_amt, adjustment_amt | scm_promotion_amt | - |
| 补贴/促销金额类（系统拆分） | allowance_amt, member_coupon_shop_amt, member_promo_amt, member_coupon_company_amt, shop_promo_amt | - | - |
| 推导金额类 | - | receive_amt, init_stock_amt, end_stock_amt, know_lost_amt, unknow_lost_amt, compose_in_amt, compose_out_amt, out_stock_pay_amt（4个）, return_stock_pay_amt（4个）, order_amt, pre_sale_amt, pre_inbound_amount, expect_outstock_amt | - |
| 毛利类 | - | profit_amt, sale_cost_amt, pre_profit_amt, allowance_amt_profit, scm_fin_article_income, scm_fin_article_cost, scm_fin_article_profit, full_link_article_profit, article_profit_amt | - |

---

## 二、Layer -2: 真正的原子层

### 域① 销售域（POS交易数据）

**源表**: `dsl.dsl_transaction_sotre_order_online_details_di` + `dsl.dsl_transaction_sotre_order_offline_details_di`

> **为什么销售域的金额字段保留为原子**：这些是 POS 交易系统直接记录的实际货币收付，不可由数量×单价推导（涉及促销、折扣、四舍五入、会员价等复杂逻辑）。

| 原子字段 | 类型 | 源字段 | 说明 |
|---------|------|--------|------|
| **数量类** ||||
| sale_qty | ① 数量 | qty_spec | 销售数量（规格×份数） |
| sale_piece_qty | ① 数量 | qty | 销售件数（份数） |
| return_sale_qty | ① 数量 | return_sale_qty | 退货数量 |
| gift_qty | ① 数量 | gift_qty | 赠品数量 |
| online_sale_qty | ① 数量 | qty_spec WHERE online_flag='Y' | 线上销售数量 |
| offline_sale_qty | ① 数量 | qty_spec WHERE online_flag='N' | 线下销售数量 |
| bf19_sale_qty | ① 数量 | qty_spec - af19_sales_qty × spec_num | 19点前销售数量 |
| af19_sale_qty | ① 数量 | af19_sales_qty × spec_num | 19点后销售数量 |
| bf12_sale_qty | ① 数量 | qty × spec_num WHERE trans_hour < '12' | 12点前销售数量 |
| sales_weight | ① 数量 | actual_weight 或 qty_spec × unit_weight | 销售重量 |
| **交易金额类（POS直录，不可推导）** ||||
| sale_amt | ① 金额 | sales_amt | 实际交易金额 |
| original_price_sale_amt | ① 金额 | p_lp_sub_amt | 原价销售额（listPrice × qty） |
| vip_discount_amt | ① 金额 | vip_discount_amt | 会员折扣额 |
| hour_discount_amt | ① 金额 | hour_discount_amt | 时段折扣额 |
| actual_amount | ① 金额 | actual_amount | 实付金额（不含运费） |
| return_sale_amt | ① 金额 | return_sale_amt | 退货金额 |
| member_discount_amt | ① 金额 | member_discount_amt | 会员折扣额 |
| promotion_discount_amt | ① 金额 | p_promo_amt + f_promo_amt | 促销折扣额 |
| discount_amt | ① 金额 | discount_amt | 总折扣额 |
| **会员销售金额** ||||
| member_sale_amt | ① 金额 | sales_amt WHERE customer_phone IS NOT NULL | 会员销售额 |
| bf19_member_sale_amt | ① 金额 | 会员19点前销售额 | 19点前会员销售额 |

---

### 域② 进货库存域（只有数量）

**源表**: `dsl.dsl_transaction_non_daily_store_article_purchase_di`（BOM拆分后的进货库存表）

> **重要说明**：BOM 拆分管道由 3 个子脚本构建，写入临时表后最终汇总到 `dsl_transaction_non_daily_store_article_purchase_di`。

| 原子字段 | 类型 | 源字段 | 说明 |
|---------|------|--------|------|
| receive_qty | ① 数量 | sale_article_qty | 进货数量（BOM拆分后） |
| init_stock_qty | ① 数量 | init_stock_qty | 期初库存数量（= 昨日 end_stock_qty，日切依赖） |
| end_stock_qty | ①/② 数量 | end_stock_qty 或 physical_count_qty | 期末库存数量（有盘点时为原子①，无盘点时由库存方程推算为②） |

> ~~receive_amt~~ → 移入计算层 = receive_qty × avg_purchase_price
> ~~init_stock_amt~~ → 移入计算层 = init_stock_qty × avg_price
> ~~end_stock_amt~~ → 移入计算层 = end_stock_qty × avg_price

---

### 域③ 供应链域（数量 + 单价 + SAP让利）

**源表**: `dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di`（SAP交付宽表）

> **为什么 SAP 让利金额保留为原子**：SAP 系统按配置规则计算后直接记录，涉及多方分摊逻辑（供应链、供应商、运营、市场），不适合本地简化计算。

| 原子字段 | 类型 | 源字段 | 说明 |
|---------|------|--------|------|
| **出库数量类** ||||
| original_outstock_qty | ① 数量 | original_outstock_qty | 原价出库数量 |
| promotion_outstock_qty | ① 数量 | promotion_outstock_qty | 促销出库数量 |
| gift_outstock_qty | ① 数量 | gift_outstock_qty | 赠品出库数量 |
| **退仓数量类** ||||
| return_stock_qty | ① 数量 | store_return_scm_qty | 供应链退仓数量 |
| store_return_qty_shop | ① 数量 | store_return_qty_shop | 中台退货数量 |
| **订购数量** ||||
| store_order_qty | ① 数量 | order_qty_order_unit | 门店订购数量（订购单位） |
| order_qty_payean | ① 数量 | order_qty_payean | 门店订购数量（结算单位） |
| **出库单价类（由金额/数量推导，但作为独立观测量使用）** ||||
| outstock_unit_price | ① 单价 | outstock_amt / outstock_qty | 含税出库单价 |
| outstock_unit_price_notax | ① 单价 | outstock_amt_notax / outstock_qty | 不含税出库单价 |
| outstock_cost_price | ① 单价 | outstock_cost / outstock_qty | 含税出库成本单价 |
| outstock_cost_price_notax | ① 单价 | outstock_cost_notax / outstock_qty | 不含税出库成本单价 |
| **退仓单价类** ||||
| return_unit_price | ① 单价 | store_return_scm_amt / store_return_scm_qty | 含税退仓单价 |
| return_unit_price_notax | ① 单价 | store_return_scm_amt_notax / store_return_scm_qty | 不含税退仓单价 |
| return_cost_price | ① 单价 | return_stock_amt_cb / store_return_scm_qty | 含税退仓成本单价 |
| return_cost_price_notax | ① 单价 | store_return_scm_cost_notax / store_return_scm_qty | 不含税退仓成本单价 |
| **订购单价** ||||
| order_unit_price | ① 单价 | order_amt / order_qty_order_unit | 订购单价 |
| **SAP让利金额类（SAP直接记录，不可本地简化）** ||||
| scm_promotion_amt_total | ① 金额 | total_benefit_amt | 出库让利总额 |
| scm_promotion_amt_gift | ① 金额 | total_gift_benefit_amt | 赠品出库让利 |
| scm_bear_amt | ① 金额 | scm_bear_nogift_benefit_amt | 供应链承担非赠品让利 |
| vendor_bear_amt | ① 金额 | vendor_bear_nogift_benefit_amt | 供应商承担非赠品让利 |
| business_bear_amt | ① 金额 | business_bear_nogift_benefit_amt | 运营承担非赠品让利 |
| market_bear_amt | ① 金额 | market_bear_nogift_benefit_amt | 市场承担非赠品让利 |
| vender_bear_gift_amt | ① 金额 | vender_bear_gift_amt | 供应商承担赠品金额 |
| scm_bear_gift_amt | ① 金额 | scm_bear_gift_amt | 供应链承担赠品金额 |
| adjustment_amt | ① 金额 | dal_debit_store_dc_difference_adjustment_di | 差异调整金额 |

---

### 域④ 损耗域（只有数量）

**源表**: `dal.dal_transaction_store_article_lost_di`（由 `门店商品损耗表.py` 生成）

> **数据血缘说明**：`门店商品损耗表.py` 通过 UNION ALL 合并 3 个源，日期分割点为 **2025-07-22**：
> - **>= 2025-07-22 新来源**：
>   - 未知损耗：`ods_rt_dws.dws_transaction_store_article_unknowlost_rts_di`（按 max(updated_time) 去重）
>   - 已知损耗：`ods_sc_db.t_purchase_wastage`（门店报损）
> - **< 2025-07-22 历史来源**：`ods_sc_db.t_sc_settlement_detail_logs`（结算日志，version='1.0'）

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| know_lost_qty | ① 数量 | 已知损耗数量（门店报损） |

> ~~know_lost_amt~~ → 移入计算层 = know_lost_qty × cost_price
> ~~unknow_lost_qty~~ → 移入计算层 = 库存方程推算
> ~~unknow_lost_amt~~ → 移入计算层 = unknow_lost_qty × cost_price

---

### 域⑤ 加工转换域（只有数量）

**源表**: `ddl.ddl_compose_in_info_di` + `ddl.ddl_compose_out_info_di`
**中间表**: `dsl.dsl_transaction_sotre_article_compose_info_di`（由 `layer0_加工转换信息表.sql` 生成）

> **v3 修订**：`compose_in_amt` / `compose_out_amt` 移入计算层，改为 `qty × cost_price` 本地计算。

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| compose_in_qty | ① 数量 | 加工转入数量（成品被生产出来） |
| compose_out_qty | ① 数量 | 加工转出数量（原材料被消耗） |

> ~~compose_in_amt~~ → 移入计算层 = compose_in_qty × cost_price
> ~~compose_out_amt~~ → 移入计算层 = compose_out_qty × cost_price

---

### 域⑥ 补贴域

**源表**: `dal.dal_activity_article_order_sale_info_di`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| allowance_amt | ① 金额 | 补贴金额（系统拆分后直接记录） |

---

### 域⑦ 促销优惠域

**源表**: `dsl.dsl_promotion_order_item_article_sale_info_di`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| member_coupon_shop_amt | ① 金额 | 会员券-门店承担 |
| member_promo_amt | ① 金额 | 会员活动促销费 |
| member_coupon_company_amt | ① 金额 | 会员券-公司承担 |
| shop_promo_amt | ① 金额 | 门店发起促销额 |

---

### 域⑧ 成本价域（单价）

**源表**: `stg_sc_db.t_shop_inventory_sku_pool` → `ods_sc_db.t_shop_inventory_sku_pool`（由 `layer0_库存成本价同步.sql` 同步）

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| cost_price | ① 单价 | 进货成本价 |

---

### 域⑨ 价格域（单价）

**源表**: `dim.dim_store_article_price_info_da`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| current_price | ① 单价 | 今日销售价格 |
| yesterday_price | ① 单价 | 昨日销售价格 |
| dc_original_price | ① 单价 | 出库原价 |
| original_price | ① 单价 | 销售原价 |

---

### 域⑩ 盘点域（打破库存-损耗循环）

**源表**: **待确认**（候选：`ods_sc_db` 盘点表 or 实时库存系统）

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| physical_count_qty | ① 数量 | 物理盘点/系统实盘数量 |

> **盘点优先策略**：
> - **有盘点数据** → `end_stock_qty = physical_count_qty`（原子①），`unknow_lost_qty` 由库存方程反推（计算②）
> - **无盘点数据** → `end_stock_qty` 退回计算层②（库存方程推算），`unknow_lost_qty` 同为计算②
> - `init_stock_qty` = 前一日 `end_stock_qty`（日切依赖），始终归类原子①

---

## 三、Layer -1: 计算层

### 3.1 金额类计算（数量 × 单价）

| 计算字段 | 公式 | 原子依赖 | 说明 |
|---------|------|---------|------|
| receive_amt | receive_qty × avg_purchase_price | 域② + 域⑧/计算 | 进货金额 |
| init_stock_amt | init_stock_qty × avg_price | 域② + 计算(avg_price) | 期初库存金额 |
| end_stock_amt | end_stock_qty × avg_price | 计算(end_stock_qty) + 计算(avg_price) | 期末库存金额 |
| know_lost_amt | know_lost_qty × cost_price | 域④ + 域⑧ | 已知损耗金额 |
| unknow_lost_amt | unknow_lost_qty × cost_price | 计算(unknow_lost_qty) + 域⑧ | 未知损耗金额 |
| compose_in_amt | compose_in_qty × cost_price | 域⑤ + 域⑧ | 加工转入金额 |
| compose_out_amt | compose_out_qty × cost_price | 域⑤ + 域⑧ | 加工转出金额 |

---

### 3.2 供应链金额计算

| 计算字段 | 公式 | 原子依赖 | 说明 |
|---------|------|---------|------|
| out_stock_pay_amt | outstock_qty × outstock_unit_price | 域③ | 出库金额（含税） |
| out_stock_pay_amt_notax | outstock_qty × outstock_unit_price_notax | 域③ | 出库金额（不含税） |
| out_stock_amt_cb | outstock_qty × outstock_cost_price | 域③ | 出库成本（含税） |
| out_stock_amt_cb_notax | outstock_qty × outstock_cost_price_notax | 域③ | 出库成本（不含税） |
| return_stock_pay_amt | return_stock_qty × return_unit_price | 域③ | 退仓金额（含税） |
| return_stock_pay_amt_notax | return_stock_qty × return_unit_price_notax | 域③ | 退仓金额（不含税） |
| return_stock_amt_cb | return_stock_qty × return_cost_price | 域③ | 退仓成本（含税） |
| return_stock_amt_cb_notax | return_stock_qty × return_cost_price_notax | 域③ | 退仓成本（不含税） |
| scm_promotion_amt | scm_promotion_amt_total - scm_promotion_amt_gift | 域③ | 非赠品出库让利 |
| total_outstock_qty | original_outstock_qty + promotion_outstock_qty + gift_outstock_qty | 域③ | 总出库数量 |
| original_outstock_amt | original_outstock_qty × dc_original_price | 域③ + 域⑨ | 原价出库金额 |
| promotion_outstock_amt | promotion_outstock_qty × promotion_price | 域③ + 域⑨ | 促销出库金额 |
| order_amt | store_order_qty × order_unit_price | 域③ | 订购金额 |
| store_return_amt_shop | store_return_qty_shop × return_unit_price | 域③ | 中台退仓金额 |

---

### 3.3 库存方程计算

**前提**: init_stock_qty 是前一天的 end_stock_qty（日切依赖）

```
库存方程（标准形式）:
end_stock_qty = init_stock_qty
              + receive_qty
              + compose_in_qty
              - compose_out_qty
              - sale_qty
              - know_lost_qty
              - unknow_lost_qty

未知损耗反推（有盘点数据时）:
unknow_lost_qty = init_stock_qty
                + receive_qty
                + compose_in_qty
                - compose_out_qty
                - sale_qty
                - know_lost_qty
                - physical_count_qty   ← 盘点域(域⑩)
```

**计算顺序**:
1. **有盘点数据时**：
   - `end_stock_qty = physical_count_qty`（原子①）
   - `unknow_lost_qty = 库存方程反推`（计算②）
2. **无盘点数据时**：
   - `end_stock_qty = init_stock_qty + receive_qty + compose_in_qty - compose_out_qty - sale_qty - know_lost_qty`（计算②，假设 unknow_lost_qty = 0）
   - `unknow_lost_qty = 0`
3. **下一天的期初** = 本日期末（日切依赖）

---

### 3.4 损耗类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| lost_amt | know_lost_amt + unknow_lost_amt | 本层计算 |
| lost_qty | know_lost_qty + unknow_lost_qty | 域④ + 本层计算 |

---

### 3.5 平均价格计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| avg_purchase_price（非日清，有cost_price） | cost_price | 域⑧ |
| avg_purchase_price（日清/无cost_price） | (init_stock_amt + receive_amt + compose_in_amt - compose_out_amt) / (init_stock_qty + receive_qty + compose_in_qty - compose_out_qty) | 域②⑤⑧ + 本层计算 |
| avg_price（库存均价） | 同 avg_purchase_price 逻辑 | 域②⑤⑧ + 本层计算 |

> **说明**：`avg_purchase_price` 和 `avg_price` 在底表 SQL 中不直接输出，已在 Layer 1/2 预算后体现在 `inbound_amount`、`init_stock_amt`、`end_stock_amt` 等字段中。

---

### 3.6 毛利类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| profit_amt（运营毛利额） | sale_amt - (receive_amt + compose_in_amt - compose_out_amt) + (end_stock_amt - init_stock_amt) | 域① + 本层计算 |
| sale_cost_amt（日清） | receive_amt + compose_in_amt - compose_out_amt - lost_amt | 本层计算 |
| sale_cost_amt（非日清） | sale_qty × avg_purchase_price | 域①②⑧ + 本层计算 |
| pre_profit_amt（预期毛利额） | original_price_sale_amt - sale_cost_amt | 域① + 本层计算 |
| allowance_amt_profit（补贴后毛利额） | sale_amt - receive_amt + allowance_amt + (end_stock_amt - init_stock_amt) | 域①⑥ + 本层计算 |

---

### 3.7 供应链毛利类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| scm_fin_article_income | out_stock_pay_amt_notax - abs(return_stock_pay_amt_notax) | 本层计算 |
| scm_fin_article_cost | out_stock_amt_cb_notax - abs(return_stock_amt_cb_notax) | 本层计算 |
| scm_fin_article_profit | scm_fin_article_income - scm_fin_article_cost | 本层计算 |
| full_link_article_profit | article_profit_amt + scm_fin_article_income - scm_fin_article_cost | 本层计算 |

---

### 3.8 定价/预期类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| expect_outstock_amt | out_stock_pay_amt + scm_promotion_amt_total | 本层计算 |
| pre_sale_amt | lost_qty × original_price + original_price_sale_amt | 本层计算 + 域①⑨ |
| pre_inbound_amount | inbound_qty × dc_original_price（或出库均价） | 本层计算 + 域②⑨ |
| discount_amt_cate | discount_amt - hour_discount_amt | 域① |

---

## 四、计算依赖链（完整顺序）

```
Step 1: 原子数据输入
  数量: sale_qty, receive_qty, know_lost_qty, compose_in/out_qty, 
        outstock_qty, return_qty, init_stock_qty, physical_count_qty, ...
  单价: cost_price, current_price, original_price, outstock_unit_price, ...
  交易金额: sale_amt, original_price_sale_amt, discount_amt, ...
  让利金额: scm_promotion_amt_total, scm_bear_amt, ...

Step 2: 库存方程 → 推算期末库存和未知损耗
  有盘点数据:
    end_stock_qty = physical_count_qty
    unknow_lost_qty = init + receive + compose_in - compose_out - sale - known_loss - physical_count
  无盘点数据:
    end_stock_qty = init + receive + compose_in - compose_out - sale - known_loss
    unknow_lost_qty = 0
  lost_qty = know_lost_qty + unknow_lost_qty

Step 3: 金额计算（数量 × 单价）
  receive_amt = receive_qty × avg_purchase_price
  know_lost_amt = know_lost_qty × cost_price
  unknow_lost_amt = unknow_lost_qty × cost_price
  compose_in_amt = compose_in_qty × cost_price
  compose_out_amt = compose_out_qty × cost_price
  out_stock_pay_amt = outstock_qty × outstock_unit_price
  return_stock_pay_amt = return_qty × return_unit_price
  ... (所有供应链金额)
  init_stock_amt = init_stock_qty × avg_price
  end_stock_amt = end_stock_qty × avg_price

Step 4: 均价计算
  avg_purchase_price = cost_price (非日清) 或 加权平均
  avg_price = 同上逻辑

Step 5: 毛利计算
  profit_amt = sale_amt - (receive_amt + compose_in_amt - compose_out_amt) + (end_stock_amt - init_stock_amt)
  sale_cost_amt = CASE day_clear ...
  pre_profit_amt = original_price_sale_amt - sale_cost_amt

Step 6: 供应链毛利
  scm_fin_article_income/cost/profit
  full_link_article_profit

Step 7: 预期类
  expect_outstock_amt, pre_sale_amt, pre_inbound_amount
```

---

## 五、完整数据血缘图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FM 底表数据血缘图                                    │
└─────────────────────────────────────────────────────────────────────────────┘

【上游调度系统（非翠花管道专属）】
                                                                                
  ┌──────────────────────────────────────┐                                     
  │ upstream_sh_供应链SAP出入库全局表.sh   │                                     
  │ → dsl.dsl_scm_info_sap_global_di      │                                     
  │ （含SAP移动数据：出入库/损耗/调拨/委外加工）│                                    
  └───────────────┬──────────────────────┘                                     
                  │                                                             
  ┌───────────────▼──────────────────────┐                                     
  │ upstream_sh_全渠道供应链宽表.sh        │                                     
  │ → 全渠道供应链汇总宽表（大型Shell）     │                                     
  └───────────────┬──────────────────────┘                                     
                  │                                                             
  ┌───────────────▼──────────────────────┐                                     
  │ upstream_hive_门店订购出库信息表.sql   │                                     
  │ → dsl.dsl_scm_store_purchase_info_di  │                                     
  │ （含边猪/鱼头鱼身BOM拆分出库逻辑）      │                                    
  └──────────────────────────────────────┘                                     

═══════════════════════════════════════════════════════════════════════════════

【Layer 0: 原子数据落地（翠花管道）】

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 1. 库存成本价同步                                                            │
  │    layer0_库存成本价同步.sql                                                 │
  │    stg_sc_db.t_shop_inventory_sku_pool → ods_sc_db.t_shop_inventory_sku_pool │
  │    输出: cost_price (域⑧)                                                   │
  └─────────────────────────────────────────────────────────────────────────────┘
                                              │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 2. 加工转换信息表                                                            │
  │    layer0_加工转换信息表.sql                                                 │
  │    ddl.ddl_compose_in_info_di + ddl.ddl_compose_out_info_di                 │
  │    → dsl.dsl_transaction_sotre_article_compose_info_di                       │
  │    输出: compose_in_qty, compose_out_qty (域⑤)                              │
  └─────────────────────────────────────────────────────────────────────────────┘
                                              │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 3. BOM拆分管道（3个脚本并行，写入临时表）                                      │
  │    layer0_bom进货验收取数_split01.py → tmp_dal.*_di_01（进货+调拨+退货）     │
  │    layer0_bom销售取数_split02.py → tmp_dal.*_di_02（POS销售）                │
  │    layer0_bom库存取数_split03.py → tmp_dal.*_di_03（库存快照）               │
  │    → 最终汇总到 dsl.dsl_transaction_non_daily_store_article_purchase_di       │
  │    输出: receive_qty, init_stock_qty, end_stock_qty (域②)                   │
  └─────────────────────────────────────────────────────────────────────────────┘
                                              │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 4. 门店商品损耗表                                                            │
  │    门店商品损耗表.py                                                         │
  │    → dal.dal_transaction_store_article_lost_di                               │
  │    输出: know_lost_qty, unknow_lost_qty (域④)                               │
  │    日期分割: >= 2025-07-22 用新源，< 2025-07-22 用历史                        │
  └─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

【Layer 1: 全渠道门店仓商品宽表】

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ layer1_全渠道门店仓商品销售信息表.sql                                         │
  │ → dal_full_link.dal_manage_full_link_store_dc_article_info_di               │
  │                                                                              │
  │ UNION ALL:                                                                   │
  │   dal_full_link.dal_manage_full_link_dc_store_article_sale_promo_info_di    │
  │   （销售/促销数据）                                                           │
  │ + dal_full_link.dal_manage_full_link_dc_store_article_scm_di                │
  │   （供应链数据）                                                              │
  │                                                                              │
  │ LEFT JOIN:                                                                   │
  │   dim.dim_store_article_price_info_da (价格域)                               │
  │   dim.dim_store_category_price_strategy_di (定价策略)                        │
  │   dal.dal_article_daily_expect_sales_sap_di (预期销售)                       │
  │   dim.dim_goods_information_have_pt (商品主数据)                             │
  │                                                                              │
  │ 输出: 绝大多数运营指标（销售量/额、进货量/额、出库量/额、损耗、客数、库存数量）   │
  └─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

【Layer 2: 非日清汇总宽表】

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ layer2_非日清门店商品汇总宽表.py + .sql                                       │
  │ → dal.dal_transaction_non_daily_store_article_sale_info_di                  │
  │                                                                              │
  │ UNION ALL 5个数据源:                                                          │
  │   1. dsl.dsl_transaction_non_daily_store_order_details_di (POS交易)          │
  │   2. dsl.dsl_transaction_non_daily_store_article_purchase_di (进货库存)      │
  │   3. dal.dal_activity_article_order_sale_info_di (补贴)                      │
  │   4. dal.dal_transaction_store_article_lost_di (损耗)                        │
  │   5. dsl.dsl_transaction_sotre_article_compose_info_di (加工转换)            │
  │                                                                              │
  │ LEFT JOIN:                                                                   │
  │   dim.dim_goods_information_have_pt (商品主数据、过滤物料)                    │
  │   dsl.dsl_promotion_order_item_article_sale_info_di (促销优惠)               │
  │   ods_sc_db.t_shop_inventory_sku_pool (成本价)                               │
  │   dim.dim_chdj_store_list_di (翠花门店列表)                                   │
  │   dim.dim_day_clear_article_list_di (日清标签)                               │
  │                                                                              │
  │ 输出: profit_amt, lost_amt, avg_purchase_price 等计算指标                    │
  └─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

【Layer 3: 翠花门店全链路指标】

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ layer3_翠花门店商品全链路指标.sql                                             │
  │ → tmp_dal.dal_transaction_chdj_store_sale_article_sale_info_di_01            │
  │                                                                              │
  │ 过滤: INNER JOIN dim.dim_chdj_store_list_di (只保留翠花门店)                  │
  │ LEFT JOIN: dim.dim_day_clear_article_list_di (日清标签)                      │
  │                                                                              │
  │ 输出: 供应链毛利、全链路毛利、预期出库金额等（从 Layer 1 聚合）                 │
  └─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

【Layer 4: 翠花最终宽表】

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ layer4_翠花门店商品销售条码宽表.sql                                           │
  │ → dal.dal_transaction_chdj_store_sale_article_sale_info_di                   │
  │                                                                              │
  │ UNION ALL:                                                                   │
  │   dal.dal_transaction_non_daily_store_article_sale_info_di (运营指标)        │
  │ + tmp_dal.dal_transaction_chdj_store_sale_article_sale_info_di_01 (全链路)   │
  │                                                                              │
  │ LEFT JOIN:                                                                   │
  │   dal.dal_transaction_cbstore_cust_num_info_di (客数)                        │
  │   7天滑动窗口计算 avg_7d_sale_qty                                            │
  │                                                                              │
  │ 输出: 翠花门店完整宽表，供 FM 底表消费                                          │
  └─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════

【FM 底表（4张表，按顺序执行）】

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 1. fm_商品维度底表.sql                                                        │
  │    → ads_business_analysis.strategy_fm_flag_sku_di                          │
  │    粒度: 门店 × 日期 × article_id × day_clear                                 │
  │    源表: t1(Layer1宽表) + f(Layer4宽表) + c(销售件数) + b(门店日销售驱动)       │
  │          + e/i(day_clear) + g(可订可售) + h(翠花门店) + j(售罄标识)            │
  │          + t2(门店维度) + t3(商品维度) + t5(日历维度)                          │
  └─────────────────────────────────────────────────────────────────────────────┘
                                              │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 2. fm_客数底表.sql                                                            │
  │    → ads_business_analysis.strategy_fm_cust                                 │
  │    粒度: 门店 × 日期 × day_clear × level_description × level_id              │
  │    源表: POS订单明细（线上+线下）+ day_clear + 商品分类                         │
  └─────────────────────────────────────────────────────────────────────────────┘
                                              │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 3. fm_分类汇总.sql                                                            │
  │    → ads_business_analysis.strategy_fm_levels_sum                           │
  │    粒度: 多层级聚合（门店/大分类/中分类/小分类/SPU/黑白猪/SKU）                  │
  │    源表: strategy_fm_flag_sku_di + strategy_fm_cust                          │
  └─────────────────────────────────────────────────────────────────────────────┘
                                              │
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 4. fm_结果.sql                                                                │
  │    → ads_business_analysis.strategy_fm_levels_result                        │
  │    粒度: 同上                                                                 │
  │    计算: 所有比率型KPI，输出中文列名                                            │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 六、项目文件结构一览

```
翠花数据/
│
├── 【上游 Hive/Shell 脚本（非翠花管道专属，调度系统执行）】
│   ├── upstream_sh_供应链SAP出入库全局表.sh       → dsl.dsl_scm_info_sap_global_di
│   │                                               含SAP移动数据(出入库/损耗/调拨/委外加工)
│   ├── upstream_sh_全渠道供应链宽表.sh             → 全渠道供应链汇总宽表（大型Shell）
│   └── upstream_hive_门店订购出库信息表.sql        → dsl.dsl_scm_store_purchase_info_di
│                                                   含边猪/鱼头鱼身BOM拆分出库逻辑
│
├── 【Layer 0: 原子数据落地（翠花管道）】
│   ├── layer0_库存成本价同步.sql                   → ods_sc_db.t_shop_inventory_sku_pool
│   │                                               (cost_price, 从 stg_sc_db 同步)
│   ├── layer0_加工转换信息表.sql                   → dsl.dsl_transaction_sotre_article_compose_info_di
│   │                                               (compose_in/out_qty，DDL表聚合)
│   ├── layer0_bom进货验收取数_split01.py           → tmp_dal.*_di_01（进货+调拨+退货）
│   ├── layer0_bom销售取数_split02.py               → tmp_dal.*_di_02（POS销售）
│   ├── layer0_bom库存取数_split03.py               → tmp_dal.*_di_03（库存快照）
│   └── 门店商品损耗表.py                           → dal.dal_transaction_store_article_lost_di
│                                                   (know/unknow_lost_qty, 日期分割: >=2025-07-22新源)
│
├── 【Layer 1: 全渠道门店仓商品宽表】
│   └── layer1_全渠道门店仓商品销售信息表.sql        → dal_full_link.dal_manage_full_link_store_dc_article_info_di
│
├── 【Layer 2: 非日清汇总宽表】
│   ├── layer2_非日清门店商品汇总宽表.py            Python 执行器（日期切分）
│   └── layer2_非日清门店商品汇总宽表.sql           → dal.dal_transaction_non_daily_store_article_sale_info_di
│
├── 【Layer 3: 翠花门店全链路指标】
│   └── layer3_翠花门店商品全链路指标.sql           → tmp_dal.dal_transaction_chdj_store_sale_article_sale_info_di_01
│
├── 【Layer 4: 翠花最终宽表（供 FM 底表消费）】
│   └── layer4_翠花门店商品销售条码宽表.sql         → dal.dal_transaction_chdj_store_sale_article_sale_info_di
│
├── 【FM 底表（本目录，4张表）】
│   底表/
│   ├── fm_商品维度底表.sql                         → ads_business_analysis.strategy_fm_flag_sku_di
│   ├── fm_客数底表.sql                             → ads_business_analysis.strategy_fm_cust
│   ├── fm_分类汇总.sql                             → ads_business_analysis.strategy_fm_levels_sum
│   ├── fm_结果.sql                                 → ads_business_analysis.strategy_fm_levels_result
│   ├── fm_底表架构设计_指标字典_v3.md              ← 本文件
│   └── README.md                                   FM 底表快速说明
│
└── 【监控/数据质量（历史版本）】
    ├── Doris数据监控任务.py                         split01 原始版（进货，与 split01.py 功能相同）
    └── Doris数据监控任务2.py                        split02 原始版（销售，与 split02.py 功能相同）
```

### 执行顺序

```
上游调度 → upstream_sh_* / upstream_hive_*（每日）
    ↓
Layer 0 并行执行:
  layer0_库存成本价同步.sql
  layer0_加工转换信息表.sql
  layer0_bom_split01~03.py（→ 最终汇总表）
  门店商品损耗表.py
    ↓
Layer 1 → layer1_全渠道门店仓商品销售信息表.sql
    ↓
Layer 2 → layer2_非日清门店商品汇总宽表.py
    ↓
Layer 3 → layer3_翠花门店商品全链路指标.sql
    ↓
Layer 4 → layer4_翠花门店商品销售条码宽表.sql
    ↓
FM 底表（按 README 顺序执行 1→2→3→4）
```

---

## 七、FM 底表源表映射详情

### fm_商品维度底表.sql 源表映射

| 别名 | 源表 | 负责的主要指标 |
|------|------|--------------|
| `t1` | `hive.dal_full_link.dal_manage_full_link_store_dc_article_info_di` | 绝大多数运营指标：销售量/额、进货量/额、出库量/额、损耗、客数、库存数量（Layer 1 全渠道宽表） |
| `f` | `hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di` | 毛利类（profit_amt / scm_fin_article_profit / full_link_profit）、预期毛利、期末/期初库存金额、avg_7d_sale_qty（Layer 4 翠花宽表） |
| `c` | `hive.dal.dal_transaction_store_article_sale_info_di` | 销售件数：bf19_sale_piece_qty / sale_piece_qty |
| `b` | `hive.dal.dal_transaction_sale_store_daily_di` | RIGHT JOIN 驱动表，过滤有效营业日（bf19_sale_amt ≥ 500） |
| `e` / `i` | `hive.dal.dal_transaction_chdj_store_sale_article_sale_info_di` | day_clear 标识（e 为门店+商品维度，i 为商品维度聚合） |
| `j` | `hive.dim.dim_store_article_order_sale_info_di` | saleable 标识，用于 is_stock_sku 判断和售罄率计算 |
| `t3` | `hive.dim.dim_goods_information_have_pt` | unit_weight（销售重量换算用），商品分类维度 |
| `t2` | `hive.dim.dim_store_profile` | 门店维度（管理区域、大区、城市等） |
| `t5` | `hive.dim.dim_calendar` | 日历维度（周次、月、年） |
| `h` | `default_catalog.ads_business_analysis.chdj_store_info` | 翠花门店标识（store_flag / store_no），自建 ADS 表 |
| `g` | `hive.ods_sc_db.t_purchase_order_item_tmp` | 可订可售 SKU 标识 |

### 关键依赖说明

- **`t1` vs `f` 的分工**：绝大多数加总类指标来自 `t1`（Layer 1）；毛利和库存金额类来自 `f`（Layer 4），因为毛利计算需要加工转换、损耗等多源汇总，Layer 4 已做了完整整合。
- **`e` 和 `i` 的区别**：`e` 是门店+商品级别的 day_clear（用于翠花店），`i` 是商品级别的最大 day_clear 聚合（用于非翠花店的品类规则 fallback）。
- **`b` 的作用**：以门店日销售表做 RIGHT JOIN，确保只保留有效营业日（bf19_sale_amt ≥ 500 过滤异常日期）。

---

## 八、待确认事项

| 事项 | 状态 | 说明 |
|------|------|------|
| 盘点数据源表 | **待确认** | 需确认 `physical_count_qty` 的源表（`ods_sc_db` 盘点表 or 实时库存系统） |
| BOM拆分合并逻辑 | **待开发** | split01~03 的临时表需要重新写合并逻辑 |
| promotion_price | **待确认** | 促销出库单价来源（可能来自 `dim.dim_store_promotion_info_da`） |

---

## 九、版本变更记录

| 版本 | 日期 | 修改内容 |
|------|------|---------|
| v1 | - | 初始版本 |
| v2 | - | 新增：域⑤加工转换金额直传说明、BOM拆分管道详情、fm_商品维度底表源表映射、项目文件结构 |
| v3 | 2026-04-13 | 根据 v2 校验实际代码，补充：完整指标字典、原子/计算层分类表、完整数据血缘图、待确认事项 |
