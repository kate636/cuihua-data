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

**最终源表**: `dal.dal_store_article_multi_level_bom_splitting_di`（BOM拆分宽表）

**BOM拆分管道（3个子脚本构建 → tmp 中间表 → 汇总写入最终表）**：

| 子脚本 | 写入中间表 | 数据内容 |
|--------|-----------|---------|
| `layer0_bom进货验收取数_split01.py` | `tmp_dal.dal_store_article_multi_level_bom_splitting_di_01` | 进货（`ddl_store_receive_info_zt`）+ 调拨（`ddl_store_allocation_info_zt`）+ 退货（`ddl_store_refund_info_zt`） |
| `layer0_bom销售取数_split02.py` | `tmp_dal.dal_store_article_multi_level_bom_splitting_di_02` | POS销售（`dsl_transaction_sotre_order_online/offline_details_di`） |
| `layer0_bom库存取数_split03.py` | `tmp_dal.dal_store_article_multi_level_bom_splitting_di_03` | 库存快照（`dsl_transaction_store_article_inventory_info_di`） |

> 注：`Doris数据监控任务.py` / `Doris数据监控任务2.py` 是上述 split01/02 的原始版本，功能相同，属历史备份。

| 原子字段 | 类型 | 源字段 | 说明 |
|---------|------|--------|------|
| receive_qty | 数量 | receiveb_qty - init_receiveb_qty | 进货数量（BOM拆分后） |
| init_stock_qty | 数量 | init_receiveb_qty | 期初库存数量（= 昨日 end_stock_qty，日切依赖） |

> ~~receive_amt~~ → 移入计算层 = receive_qty × avg_purchase_price
> ~~init_stock_amt~~ → 移入计算层 = init_stock_qty × avg_price
> ~~end_stock_qty~~ → 移入计算层 = 库存方程推算（或盘点直接取值）
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

### 域⑤ 加工转换域（数量 + 金额直传）

**源表**: `ddl.ddl_compose_in_info_di` + `ddl.ddl_compose_out_info_di`
**中间表**: `dsl.dsl_transaction_sotre_article_compose_info_di`（由 `layer0_加工转换信息表.sql` 生成）

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| compose_in_qty | 数量 | 加工转入数量（成品被生产出来） |
| compose_out_qty | 数量 | 加工转出数量（原材料被消耗） |
| compose_in_amt | 金额 | 加工转入金额（**DDL表直传，非本地计算**） |
| compose_out_amt | 金额 | 加工转出金额（**DDL表直传，非本地计算**） |

> ⚠️ **重要修正（v2 → 实际情况）**：
> `layer0_加工转换信息表.sql` 实际从 DDL 表直接传递了金额字段：
> - `ddl_compose_in_info_di.compose_in_amt` → 映射为 compose_out_amt（原料消耗金额）
> - `ddl_compose_out_info_di.compose_out_amt` → 映射为 compose_in_amt（成品产出金额）
>
> 这些金额由上游仓储系统预计算，不是 `qty × unit_cost` 本地推导。
> **Layer -2 设计影响**：若想纳入计算层自主管控（避免依赖上游已算金额），需在 Layer -2 只取 qty，金额在 Layer -1 用 `qty × cost_price` 本地重算。但要注意：DDL 表的成本单价可能与 `t_shop_inventory_sku_pool.cost_price` 存在差异（移动加权平均 vs. 固定成本价）。

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

**源表**: 待开发阶段确认（候选：`ods_sc_db` 盘点表 or 实时库存系统）

| 原子字段 | 类型 | 说明 |
|---------|------|------|
| physical_count_qty | 数量 | 物理盘点/系统实盘数量 |

> **盘点优先策略（已确认）**：
> - 有盘点数据 → `end_stock_qty = physical_count_qty`（原子(1)），`unknow_lost_qty` 由库存方程反推
> - 无盘点数据 → `end_stock_qty` 退回计算层(2)（库存方程推算），`unknow_lost_qty` 同为计算层
>
> **`init_stock_qty`** = 前一日 `end_stock_qty`（日切依赖），始终归类原子(1)。

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

> **三个价格字段的性质**：
> - `cost_price` → **原子(1)**，来自域⑧ `ods_sc_db.t_shop_inventory_sku_pool`，库存系统按 SKU 独立记录的成本价，不由本 pipeline 其他字段推导。
> - `avg_purchase_price` → **计算(2)**：非日清时直接引用 `cost_price`（引用≠独立观测），其他情况为加权平均公式。
> - `avg_price` → **计算(2)**：与 `avg_purchase_price` 逻辑相同，专用于库存金额估值（`init/end_stock_amt = qty × avg_price`）。
>
> 两者在底表 SQL 中不直接输出，已在 Layer 1/2 预算后体现在 `inbound_amount`、`init_stock_amt`、`end_stock_amt` 等字段中。

| 计算字段 | 公式 | 依赖 |
|---------|------|------|
| avg_purchase_price (非日清，有cost_price) | cost_price | 域⑧ |
| avg_purchase_price (其他) | (init_stock_amt + receive_amt + compose_in_amt - compose_out_amt) / (init_stock_qty + receive_qty + compose_in_qty - compose_out_qty) | 域②⑤⑧ + 本层计算 |
| avg_price (库存均价) | 同 avg_purchase_price 逻辑，用于期初/期末库存金额估值 | 域②⑤⑧ + 本层计算 |

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

BOM拆分管道（3步 → 最终宽表）:
  ddl.ddl_store_receive_info_zt          ┐
  ddl.ddl_store_allocation_info_zt       ├─→ split01.py → tmp_di_01 ┐
  ddl.ddl_store_refund_info_zt           ┘                           │
  dsl_transaction_sotre_order_*_details_di ──→ split02.py → tmp_di_02 ├→ dal.dal_store_article_multi_level_bom_splitting_di → 域② 进货库存域
  dsl_transaction_store_article_inventory_info_di → split03.py → tmp_di_03 ┘

SAP交付系统（上游Hive脚本：upstream_sh_供应链SAP出入库全局表.sh）:
  ddl.ddl_scm_info_sap                                  ──→ dsl.dsl_scm_info_sap_global_di
  [含: known_lost_qty / unknown_lost_qty 的SAP移动数据]
  dal_scm_dc_matnr_store_mode_delivery_daily_wide_sap_di ──→ 域③ 供应链域(数量+单价+让利)
  dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di ──→ 域③ 差异调整

门店订购出库（上游Hive脚本：upstream_hive_门店订购出库信息表.sql）:
  ddl.ddl_store_order_processing_info   ──→ dsl.dsl_scm_store_purchase_info_di
  [边猪/鱼头鱼身BOM处理，含出库数量/金额]

损耗系统（翠花管道 >= 2025-07-22）:
  ods_sc_db.t_purchase_wastage                         ──→ 域④ 已知损耗(数量)

> ⚠️ SAP 系统也记录了 known_lost_qty/unknown_lost_qty（来自 dsl_scm_info_sap_global_di，
>    移动类型 Z03/Z04/Z05/Z06），与翠花管道 t_purchase_wastage 路径不同，是两套口径。

加工系统（layer0_加工转换信息表.sql → dsl_transaction_sotre_article_compose_info_di）:
  ddl.ddl_compose_in_info_di                           ──→ 域⑤ 加工转换域(数量 + 金额直传)
  ddl.ddl_compose_out_info_di                          ──→ 域⑤ 加工转换域(数量 + 金额直传)

补贴系统:
  dal.dal_activity_article_order_sale_info_di           ──→ 域⑥ 补贴域

促销系统:
  dsl.dsl_promotion_order_item_article_sale_info_di     ──→ 域⑦ 促销优惠域

库存成本系统（layer0_库存成本价同步.sql）:
  stg_sc_db.t_shop_inventory_sku_pool                  ──→ ods_sc_db.t_shop_inventory_sku_pool → 域⑧ 成本价域

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

---

## fm_商品维度底表.sql 源表映射

`fm_商品维度底表.sql` 是 FM 平台的核心查询入口，汇聚了上游 ETL 多张宽表。下表列出 SQL 中每个别名对应的实际源表及其负责的指标范围。

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

## 项目文件结构一览

```
翠花数据/
│
├── 【上游 Hive/Shell 脚本（非翠花管道专属，调度系统执行）】
│   ├── upstream_sh_供应链SAP出入库全局表.sh       → dsl.dsl_scm_info_sap_global_di
│   │                                               含SAP移动数据(出入库/损耗/调拨/委外加工)
│   ├── upstream_sh_全渠道供应链宽表.sh             → 全渠道供应链汇总宽表（大型Shell）
│   └── upstream_hive_门店订购出库信息表.sql        → dsl.dsl_scm_store_purchase_info_di
│                                                   含边猪/鱼头鱼身 BOM 拆分出库逻辑
│
├── 【Layer 0: 原子数据落地（翠花管道 + BOM拆分）】
│   ├── layer0_库存成本价同步.sql                   → ods_sc_db.t_shop_inventory_sku_pool
│   │                                               (cost_price, 从 stg_sc_db 同步)
│   ├── layer0_加工转换信息表.sql                   → dsl.dsl_transaction_sotre_article_compose_info_di
│   │                                               (compose_in/out_qty + amt，DDL表直传)
│   ├── layer0_bom进货验收取数_split01.py           → tmp_dal.*_di_01（进货+调拨+退货）
│   ├── layer0_bom销售取数_split02.py               → tmp_dal.*_di_02（POS 销售）
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
│   ├── fm_底表架构设计_指标字典.md                 ← 本文件
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
  layer0_bom_split01~03.py（→ dal_store_article_multi_level_bom_splitting_di）
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
