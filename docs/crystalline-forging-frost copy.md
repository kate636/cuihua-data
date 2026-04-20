# strategy_fm_flag_sku_di 底层架构梳理（v2）

## Context

`strategy_fm_flag_sku_di` 依赖多层 ETL 管道产出的宽表。需要严格区分"独立可观测的原子数据"和"可由公式推导的计算指标"。

**核心原则**：
- Layer -2 只放**不可再分解的独立观测量**（数量、单价、标识、BOM关系）
- Layer -1 放**所有可由公式推导的指标**（金额、库存余额、差异、毛利）
- 目的：底层改一个值，上层自动算对；Python本地处理；AI分析时SKU毛利损耗不出错

---

## 关键发现：原"原子层"中实际是计算的字段

### 1. 期末库存数量/金额 → 计算
- **end_stock_qty** = init_stock_qty + receive_qty + compose_in_qty - compose_out_qty - sale_qty - know_lost_qty - unknow_lost_qty
- **end_stock_amt** = end_stock_qty × avg_price（金额=数量×单价）

### 2. 未知损耗数量/金额 → 计算
- **unknow_lost_qty** = init_stock_qty + receive_qty + compose_in_qty - compose_out_qty - sale_qty - know_lost_qty - end_stock_qty（盘点库存）
- **unknow_lost_amt** = unknow_lost_qty × cost_price

> ⚠️ **期末库存与未知损耗存在循环依赖**，需要一个独立观测量打破循环：
> - 方案A：**物理盘点数量（physical_count_qty）** 作为原子 → 期末库存=盘点数，未知损耗=推算
> - 方案B：**系统期末库存** 作为原子 → 未知损耗=推算
> - 推荐**方案A**，盘点数据最接近物理真实

### 3. 出库额（及不含税、成本） → 计算
- **out_stock_pay_amt** = outstock_qty × outstock_unit_price
- **out_stock_pay_amt_notax** = outstock_qty × outstock_unit_price_notax
- **out_stock_amt_cb** = outstock_qty × outstock_cost_price
- **out_stock_amt_cb_notax** = outstock_qty × outstock_cost_price_notax

### 4. 退仓金额 → 计算
- **return_stock_pay_amt** = return_stock_qty × return_unit_price
- **return_stock_pay_amt_notax** = return_stock_qty × return_unit_price_notax
- **return_stock_amt_cb** = return_stock_qty × return_cost_price
- **return_stock_amt_cb_notax** = return_stock_qty × return_cost_price_notax

### 5. 进货金额 → 计算（BOM拆分后）
- **receive_amt** = receive_qty × avg_purchase_price（或 cost_price）

### 6. 期初库存金额 → 计算
- **init_stock_amt** = init_stock_qty × avg_price

### 7. 已知损耗金额 → 计算
- **know_lost_amt** = know_lost_qty × cost_price

### 8. 加工转换金额 → 计算
- **compose_in_amt** = compose_in_qty × unit_cost
- **compose_out_amt** = compose_out_qty × unit_cost

### 9. 原价/促销出库金额 → 计算
- **original_outstock_amt** = original_outstock_qty × dc_original_price
- **promotion_outstock_amt** = promotion_outstock_qty × promotion_price

### 10. 总出库数量 → 计算
- **total_outstock_qty** = original_outstock_qty + promotion_outstock_qty + gift_outstock_qty

### 11. 订购金额 → 计算
- **order_amt** = store_order_qty × order_unit_price

### 12. 非赠品出库让利 → 计算
- **scm_promotion_amt** = scm_promotion_amt_total - scm_promotion_amt_gift

### 13. 退仓金额（中台） → 计算
- **store_return_amt_shop** = store_return_qty_shop × return_unit_price

### 14. 进货重量 → 计算
- **purchase_weight** = receive_qty × unit_weight

---

## 重新设计的架构

```
┌───────────────────────────────────────────────────────────────────┐
│  strategy_fm_flag_sku_di (ADS 应用层)                              │
│  维度: 时间/门店/商品分类 + 所有指标                                 │
└────────────────────────────┬──────────────────────────────────────┘
                             │
            ┌────────────────┼────────────────┐
            │                                 │
            ▼                                 ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│  Layer -1: 计算层         │    │  维度表 (DIM)                 │
│  所有公式推导的指标         │    │  BOM关系、day_clear标签、      │
│  金额/余额/差异/毛利/损耗  │    │  门店列表、商品主数据、价格信息  │
└────────────┬─────────────┘    └──────────────────────────────┘
             │ 依赖
             ▼
┌───────────────────────────────────────────────────────────────────┐
│  Layer -2: 真正的原子层                                            │
│  只有：数量 × 单价 × 标识 × BOM关系                                 │
│  修改任意一个值 → 计算层自动重算                                      │
│                                                                   │
│  ① 销售域(数量+交易金额)  ② 进货库存域(数量)  ③ 供应链域(数量+单价)  │
│  ④ 损耗域(数量)  ⑤ 加工转换域(数量)  ⑥ 补贴域(金额)                  │
│  ⑦ 促销优惠域(金额)  ⑧ 成本价域(单价)  ⑨ 价格域(单价)                │
│  ⑩ 盘点域(数量) ← 打破库存-损耗循环                                 │
└───────────────────────────────────────────────────────────────────┘
```

---

## Layer -2: 真正的原子层

### 域① 销售域（POS交易数据）

**源表**: `dsl.dsl_transaction_sotre_order_online_details_di` + `dsl.dsl_transaction_sotre_order_offline_details_di`

| 原子字段 | 类型 | 源字段 | 说明 |
|---------|------|--------|------|
| sale_qty | 数量 | qty_spec | 销售数量 |
| sale_piece_qty | 数量 | qty | 销售件数 |
| return_sale_qty | 数量 | return_sale_qty | 退货数量 |
| gift_qty | 数量 | gift_qty | 赠品数量 |
| online_sale_qty | 数量 | qty_spec WHERE online_flag='Y' | 线上销售数量 |
| offline_sale_qty | 数量 | qty_spec WHERE online_flag='N' | 线下销售数量 |
| bf19_sale_qty | 数量 | qty_spec WHERE is_hour_promotion='0' | 19点前销售数量 |
| af19_sale_qty | 数量 | af19_sales_qty × spec_num | 19点后销售数量 |
| sales_weight | 数量 | actual_weight | 销售重量 |
| sale_amt | 交易金额 | sales_amt | **实际交易金额**（POS直接记录） |
| original_price_sale_amt | 交易金额 | p_lp_sub_amt | **原价销售额**（POS直接记录） |
| vip_discount_amt | 交易金额 | vip_discount_amt | **会员折扣额**（POS直接记录） |
| hour_discount_amt | 交易金额 | hour_discount_amt | **时段折扣额**（POS直接记录） |
| actual_amount | 交易金额 | actual_amount | **实付金额**（支付系统直接记录） |
| return_sale_amt | 交易金额 | return_sale_amt | **退货金额**（POS直接记录） |

> 销售域的金额字段保留为原子：这些是POS交易系统直接记录的实际货币收付，不可由数量×单价推导（涉及促销、折扣、四舍五入等）。

---

### 域② 进货库存域（只有数量）

**源表**: `dal.dal_store_article_multi_level_bom_splitting_di`（BOM拆分表）

| 原子字段 | 类型 | 源字段 | 说明 |
|---------|------|--------|------|
| receive_qty | 数量 | receiveb_qty - init_receiveb_qty | 进货数量（BOM拆分后） |
| init_stock_qty | 数量 | init_receiveb_qty | 期初库存数量 |

> ~~receive_amt~~ → 移入计算层 = receive_qty × avg_purchase_price
> ~~init_stock_amt~~ → 移入计算层 = init_stock_qty × avg_price
> ~~end_stock_qty~~ → 移入计算层 = 库存方程推算
> ~~end_stock_amt~~ → 移入计算层 = end_stock_qty × avg_price

---

### 域③ 供应链域（数量 + 单价）

**源表**: `dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di`

| 原子字段 | 类型 | 源字段 | 说明 |
|---------|------|--------|------|
| **出库数量类** | | | |
| original_outstock_qty | 数量 | original_outstock_qty | 原价出库数量 |
| promotion_outstock_qty | 数量 | promotion_outstock_qty | 促销出库数量 |
| gift_outstock_qty | 数量 | gift_outstock_qty | 赠品出库数量 |
| **退仓数量类** | | | |
| return_stock_qty | 数量 | store_return_scm_qty | 退仓数量 |
| store_return_qty_shop | 数量 | store_return_qty_shop | 中台退货数量 |
| **订购数量** | | | |
| store_order_qty | 数量 | order_qty_order_unit | 门店订购数量 |
| **出库单价类** | | | |
| outstock_unit_price | 单价 | outstock_amt / outstock_qty | 含税出库单价 |
| outstock_unit_price_notax | 单价 | outstock_amt_notax / outstock_qty | 不含税出库单价 |
| outstock_cost_price | 单价 | outstock_cost / outstock_qty | 含税出库成本单价 |
| outstock_cost_price_notax | 单价 | outstock_cost_notax / outstock_qty | 不含税出库成本单价 |
| **退仓单价类** | | | |
| return_unit_price | 单价 | store_return_scm_amt / store_return_scm_qty | 含税退仓单价 |
| return_unit_price_notax | 单价 | store_return_scm_amt_notax / store_return_scm_qty | 不含税退仓单价 |
| return_cost_price | 单价 | return_stock_amt_cb / store_return_scm_qty | 含税退仓成本单价 |
| return_cost_price_notax | 单价 | store_return_scm_cost_notax / store_return_scm_qty | 不含税退仓成本单价 |
| **订购单价** | | | |
| order_unit_price | 单价 | order_amt / order_qty_order_unit | 订购单价 |
| **让利（SAP直接记录）** | | | |
| scm_promotion_amt_total | 金额 | total_benefit_amt | 出库让利总额（SAP记录） |
| scm_promotion_amt_gift | 金额 | total_gift_benefit_amt | 赠品出库让利（SAP记录） |
| scm_bear_amt | 金额 | scm_bear_nogift_benefit_amt | 供应链承担让利（SAP记录） |
| vendor_bear_amt | 金额 | vendor_bear_nogift_benefit_amt | 供应商承担让利（SAP记录） |
| business_bear_amt | 金额 | business_bear_nogift_benefit_amt | 运营承担让利（SAP记录） |
| market_bear_amt | 金额 | market_bear_nogift_benefit_amt | 市场承担让利（SAP记录） |
| vender_bear_gift_amt | 金额 | vender_bear_gift_amt | 供应商承担赠品（SAP记录） |
| scm_bear_gift_amt | 金额 | scm_bear_gift_amt | 供应链承担赠品（SAP记录） |
| adjustment_amt | 金额 | dal_debit_store_dc_difference_adjustment_di | 差异调整金额 |

> 供应链让利金额保留为原子：SAP系统按配置规则计算后直接记录，涉及多方分摊逻辑，不适合本地简化计算。

---

### 域④ 损耗域（只有数量）

**源表**: `ods_sc_db.t_purchase_wastage`（已知损耗，>= 2025-07-22）+ 历史表

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| know_lost_qty | 数量 | 已知损耗数量（门店报损） |

> ~~know_lost_amt~~ → 移入计算层 = know_lost_qty × cost_price
> ~~unknow_lost_qty~~ → 移入计算层 = 库存方程推算
> ~~unknow_lost_amt~~ → 移入计算层 = unknow_lost_qty × cost_price

---

### 域⑤ 加工转换域（只有数量）

**源表**: `ddl.ddl_compose_in_info_di` + `ddl.ddl_compose_out_info_di`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| compose_in_qty | 数量 | 加工转入数量（成品被生产出来） |
| compose_out_qty | 数量 | 加工转出数量（原材料被消耗） |

> ~~compose_in_amt~~ → 移入计算层 = compose_in_qty × unit_cost
> ~~compose_out_amt~~ → 移入计算层 = compose_out_qty × unit_cost

---

### 域⑥ 补贴域

**源表**: `dal.dal_activity_article_order_sale_info_di`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| allowance_amt | 金额 | 补贴金额（系统拆分后直接记录） |

---

### 域⑦ 促销优惠域

**源表**: `dsl.dsl_promotion_order_item_article_sale_info_di`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| member_coupon_shop_amt | 金额 | 会员券-门店承担 |
| member_promo_amt | 金额 | 会员活动促销费 |
| member_coupon_company_amt | 金额 | 会员券-公司承担 |
| shop_promo_amt | 金额 | 门店发起促销额 |

> 这些是促销系统按规则计算后直接记录的分摊金额。

---

### 域⑧ 成本价域（单价）

**源表**: `stg_sc_db.t_shop_inventory_sku_pool` → `ods_sc_db.t_shop_inventory_sku_pool`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| cost_price | 单价 | 进货成本价 |

---

### 域⑨ 价格域（单价）

**源表**: `dim.dim_store_article_price_info_da`

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| current_price | 单价 | 今日销售价格 |
| yesterday_price | 单价 | 昨日销售价格 |
| dc_original_price | 单价 | 出库原价 |
| original_price | 单价 | 销售原价 |

---

### 域⑩ 盘点域（打破库存-损耗循环）

**源表**: 需确认（可能来自 `ods_sc_db` 或实时库存系统）

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| physical_count_qty | 数量 | 物理盘点/系统实盘数量 |

> 这是计算 期末库存 和 未知损耗 的关键独立观测量。
> 如果没有独立盘点数据，则退而求其次使用 `dal_store_article_multi_level_bom_splitting_di` 中的 `end_receiveb_qty` 作为输入，此时 end_stock_qty 退回原子层，只有 unknown_loss 进入计算层。

---

## Layer -1: 计算层

### 一、金额类计算（数量 × 单价）

| 计算字段 | 公式 | 原子依赖 |
|---------|------|---------|
| receive_amt (进货金额) | receive_qty × avg_purchase_price | 域② + 域⑧/计算 |
| init_stock_amt (期初库存金额) | init_stock_qty × avg_price | 域② + 计算(avg_price) |
| end_stock_amt (期末库存金额) | end_stock_qty × avg_price | 计算(end_stock_qty) + 计算(avg_price) |
| know_lost_amt (已知损耗金额) | know_lost_qty × cost_price | 域④ + 域⑧ |
| unknow_lost_amt (未知损耗金额) | unknow_lost_qty × cost_price | 计算(unknow_lost_qty) + 域⑧ |
| compose_in_amt (加工转入金额) | compose_in_qty × cost_price | 域⑤ + 域⑧ |
| compose_out_amt (加工转出金额) | compose_out_qty × cost_price | 域⑤ + 域⑧ |

### 二、供应链金额计算

| 计算字段 | 公式 | 原子依赖 |
|---------|------|---------|
| out_stock_pay_amt (出库额) | outstock_qty × outstock_unit_price | 域③ |
| out_stock_pay_amt_notax | outstock_qty × outstock_unit_price_notax | 域③ |
| out_stock_amt_cb (出库成本) | outstock_qty × outstock_cost_price | 域③ |
| out_stock_amt_cb_notax | outstock_qty × outstock_cost_price_notax | 域③ |
| return_stock_pay_amt (退仓额) | return_stock_qty × return_unit_price | 域③ |
| return_stock_pay_amt_notax | return_stock_qty × return_unit_price_notax | 域③ |
| return_stock_amt_cb (退仓成本) | return_stock_qty × return_cost_price | 域③ |
| return_stock_amt_cb_notax | return_stock_qty × return_cost_price_notax | 域③ |
| scm_promotion_amt (非赠品让利) | scm_promotion_amt_total - scm_promotion_amt_gift | 域③ |
| total_outstock_qty (总出库数量) | original + promotion + gift outstock qty | 域③ |
| original_outstock_amt | original_outstock_qty × dc_original_price | 域③ + 域⑨ |
| promotion_outstock_amt | promotion_outstock_qty × promotion_price | 域③ + 域⑨ |
| order_amt (订购金额) | store_order_qty × order_unit_price | 域③ |
| store_return_amt_shop | store_return_qty_shop × return_unit_price | 域③ |

### 三、库存方程计算

**前提**: init_stock_qty 是前一天的 end_stock_qty（日切依赖）

```
end_stock_qty = init_stock_qty
              + receive_qty
              + compose_in_qty
              - compose_out_qty
              - sale_qty
              - know_lost_qty
              - unknow_lost_qty

unknow_lost_qty = init_stock_qty
                + receive_qty
                + compose_in_qty
                - compose_out_qty
                - sale_qty
                - know_lost_qty
                - physical_count_qty   ← 盘点域(域⑩)
```

**计算顺序**:
1. 先用盘点数据算未知损耗: `unknow_lost_qty = 上式`
2. 再算期末库存: `end_stock_qty = physical_count_qty`（即盘点值就是期末库存）
3. 下一天的期初 = 本日期末

### 四、损耗类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| lost_amt (损耗额) | know_lost_amt + unknow_lost_amt | 本层计算 |
| lost_qty (损耗数量) | know_lost_qty + unknow_lost_qty | 域④ + 本层计算 |

### 五、平均价格计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| avg_purchase_price (非日清，有cost_price) | cost_price | 域⑧ |
| avg_purchase_price (其他) | (init_stock_amt + receive_amt + compose_in_amt - compose_out_amt) / (init_stock_qty + receive_qty + compose_in_qty - compose_out_qty) | 域②⑤⑧ + 本层计算 |
| avg_price (库存均价) | 类似 avg_purchase_price 逻辑 | 域②⑤⑧ + 本层计算 |

### 六、毛利类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| profit_amt (运营毛利额) | sale_amt - (receive_amt + compose_in_amt - compose_out_amt) + (end_stock_amt - init_stock_amt) | 域① + 本层计算 |
| sale_cost_amt (日清) | receive_amt + compose_in_amt - compose_out_amt - lost_amt | 本层计算 |
| sale_cost_amt (非日清) | sale_qty × avg_purchase_price | 域①②⑧ + 本层计算 |
| pre_profit_amt (预期毛利额) | original_price_sale_amt - sale_cost_amt | 域① + 本层计算 |
| allowance_amt_profit | sale_amt - receive_amt + allowance_amt + (end_stock_amt - init_stock_amt) | 域①⑥ + 本层计算 |

### 七、供应链毛利类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| scm_fin_article_income | out_stock_pay_amt_notax - abs(return_stock_pay_amt_notax) | 本层计算 |
| scm_fin_article_cost | out_stock_amt_cb_notax - abs(return_stock_amt_cb_notax) | 本层计算 |
| scm_fin_article_profit | scm_fin_article_income - scm_fin_article_cost | 本层计算 |
| full_link_article_profit | article_profit_amt + scm_fin_article_income - scm_fin_article_cost | 本层计算 |

### 八、定价/预期类计算

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| expect_outstock_amt | out_stock_pay_amt + scm_promotion_amt_total | 本层计算 |
| pre_sale_amt | lost_qty × original_price + original_price_sale_amt | 本层计算 + 域①⑨ |
| discount_amt_cate | discount_amt - hour_discount_amt | 域① |

---

## 计算依赖链（完整顺序）

```
Step 1: 原子数据输入
  数量: sale_qty, receive_qty, know_lost_qty, compose_in/out_qty, 
        outstock_qty, return_qty, init_stock_qty, physical_count_qty, ...
  单价: cost_price, current_price, original_price, outstock_unit_price, ...
  交易金额: sale_amt, original_price_sale_amt, discount_amt, ...
  让利金额: scm_promotion_amt_total, scm_bear_amt, ...

Step 2: 库存方程 → 推算未知损耗
  unknow_lost_qty = init + receive + compose_in - compose_out - sale - known_loss - physical_count
  end_stock_qty = physical_count_qty
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

## 改动汇总：从v1到v2移入计算层的字段

| 原域 | 移出字段 | 理由 | 新增的原子字段 |
|------|---------|------|--------------|
| 域② | receive_amt | = qty × price | — (已有 receive_qty + cost_price) |
| 域② | init_stock_amt | = qty × price | — (已有 init_stock_qty) |
| 域② | end_stock_qty | = 库存方程 | + physical_count_qty (域⑩) |
| 域② | end_stock_amt | = qty × price | — |
| 域③ | out_stock_pay_amt (4个) | = qty × unit_price | + outstock_unit_price (4个) |
| 域③ | return_stock_pay_amt (4个) | = qty × unit_price | + return_unit_price (4个) |
| 域③ | original/promotion_outstock_amt | = qty × price | — (已有 qty + dc_original_price) |
| 域③ | total_outstock_qty | = sum of 3 qty | — |
| 域③ | scm_promotion_amt | = total - gift | — |
| 域③ | order_amt | = qty × price | + order_unit_price |
| 域③ | store_return_amt_shop | = qty × price | — (已有 qty + return_unit_price) |
| 域④ | know_lost_amt | = qty × cost_price | — |
| 域④ | unknow_lost_qty | = 库存方程 | — |
| 域④ | unknow_lost_amt | = qty × cost_price | — |
| 域⑤ | compose_in_amt | = qty × cost_price | — |
| 域⑤ | compose_out_amt | = qty × cost_price | — |
| 域② | purchase_weight | = qty × unit_weight | + unit_weight (如需) |

---

## 源表总图

```
POS交易系统:
  dsl.dsl_transaction_sotre_order_online_details_di     ──→ 域① 销售域
  dsl.dsl_transaction_sotre_order_offline_details_di    ──→ 域① 销售域

BOM拆分系统:
  dal.dal_store_article_multi_level_bom_splitting_di    ──→ 域② 进货库存域(数量)

SAP交付系统:
  dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di ──→ 域③ 供应链域(数量+单价+让利)
  dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di ──→ 域③ 差异调整

损耗系统:
  ods_sc_db.t_purchase_wastage                         ──→ 域④ 已知损耗(数量)

加工系统:
  ddl.ddl_compose_in_info_di                           ──→ 域⑤ 加工转换域(数量)
  ddl.ddl_compose_out_info_di                          ──→ 域⑤ 加工转换域(数量)

补贴系统:
  dal.dal_activity_article_order_sale_info_di           ──→ 域⑥ 补贴域

促销系统:
  dsl.dsl_promotion_order_item_article_sale_info_di     ──→ 域⑦ 促销优惠域

库存成本系统:
  stg_sc_db.t_shop_inventory_sku_pool                  ──→ 域⑧ 成本价域

定价系统:
  dim.dim_store_article_price_info_da                   ──→ 域⑨ 价格域

盘点系统:
  待确认 (ods_sc_db / 实时库存系统)                      ──→ 域⑩ 盘点域

维度表:
  dim.dim_day_clear_article_list_di                     ──→ 日清标签
  dim.dim_goods_information_have_pt                     ──→ 商品主数据
  dim.dim_chdj_store_list_di                            ──→ 翠花门店列表
  dim.dim_store_promotion_info_da                       ──→ 门店促销信息
```
