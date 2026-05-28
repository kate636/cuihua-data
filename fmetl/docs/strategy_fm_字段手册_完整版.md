# strategy_fm_* 字段手册（完整版）

> **生成方式**：通过 QDM BI API 查询 StarRocks 商分库 `DESC` + 样本数据，结合 Hive 源表注释，逐字段重新理解。
> **版本**：v5.0（2026-04-24）
> **重点**：损耗、BOM、毛利计算相关字段

---

## 一、核心业务概念

### 1.1 商品编码的三种视角

零售生鲜业务存在三种商品编码视角，这是理解 BOM 分摊的基础：

| 视角 | 字段名 | 业务场景 | 示例 |
|---|---|---|---|
| **验收条码** | `article_id` | 进货验收时扫码 | 整头猪(20500283)、白条肉、大包装原料 |
| **销售条码** | `sale_article_id` | POS 销售时扫码 | 排骨、五花肉、梅头肉、散称零售品 |
| **订购条码** | `order_article_id` | 订货系统下单 | 可能与验收/销售一致，也可能不同 |

**BOM 关系本质**：一个验收条码（parent）拆分成多个销售条码（sub）。

### 1.2 day_clear 字段语义（损耗计算关键）

| 值 | 含义 | 业务场景 | 库存处理 |
|---|---|---|---|
| `'0'` | **日清** | 生鲜类，当日必须清空 | 隔夜报废，计入损耗 |
| `'1'` | **非日清** | 标品类，可跨日留存 | 正常库存滚动 |
| `'2'` | **合计** | ETL UNION 生成 | `= 0 + 1 汇总` |

⚠ **重要**：
- 查询"全天合计"必须用 `WHERE day_clear = '2'`
- 按 `day_clear` 分组求 `SUM` 会天然翻倍（设计如此，非 bug）
- 日清商品损耗计算：`期末库存负值 = 当日未售完报废`

### 1.3 门店毛利计算公式 (v10)

**v10 核心公式（含BOM，库存方程）**：
```
profit = sale - receive - bom_in + bom_out - compose_in + compose_out + end_stock - init_stock
```
注: 损耗已通过库存方程反映在 end_stock 中（end减少→利润减少），不再额外扣减。

**销售成本（统一公式）**：
```
sale_cost_amt = sale_qty × euc  (日清/非日清统一)
```

**euc（有效单位成本，加权平均含期初库存）**：
```
euc = (init_stock_amt + self_receive_amt + compose_net_amt + bom_alloc_amt)
    / (init_stock_qty + self_receive_qty + compose_net_qty + bom_alloc_qty_sub)
```
注: bom_alloc_qty 使用子品单位 `bom_alloc_qty_sub`，不是父品单位 `bom_alloc_qty`。

---

## 二、表 1：销售事实 `strategy_fm_sales_di`

**源表**：`hive.dsl.dsl_transaction_non_daily_store_order_details_di`
**粒度**：订单行级（order_id × abi_article_id × day_clear）
**字段数**：119

### 2.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `business_date` | varchar | 2026-04-23 | **营业日期**，业务发生当天 |
| `inc_day` | varchar | 2026-04-23 | **增量日**，数据入库日期，与 business_date 同义 |
| `store_id` | varchar | A3XV | **门店编码**，翠花门店唯一标识 |
| `sp_store_name` | varchar | 广州滨江宏岸店 | 门店名称 |
| `area_id` / `area_description` | varchar | FH01 / 孵化项目 | 大区编码/名称 |
| `sp_type` | varchar | 10 | **门店类型**：10=直营，20=加盟 |
| `sp_level` | varchar | 170 | **店铺等级**：170=新业务门店（实体店=1，菜吧=2，B端=3） |
| `day_clear` | varchar | 1 | **日清标识**：0=日清，1=非日清 |

### 2.2 订单维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `order_id` | varchar | 8153010970050325 | **订单号**，主键 |
| `parent_order_id` | varchar | NULL | 父订单号（拆单场景） |
| `children_order_ids` | varchar | NULL | 子订单号列表（拆单场景） |
| `order_status` | varchar | os.completed | **订单状态**：completed=已完成 |
| `order_type` | varchar | normal | 订单类型：normal=普通订单 |
| `order_sub_type` | varchar | pos | **订单子类型**：pos=收银机，delivery=配送，selfpick=自提，scancode=扫码购 |
| `outer_order_id` | varchar | 8153010970050325 | 外部订单号（与 order_id 同值） |
| `outer_order_type` | varchar | 0 | 外部订单类型 |
| `channel_id` | varchar | 16 | **渠道号**，区分线上线下渠道 |

### 2.3 商品维度字段（毛利计算关键）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `abi_article_id` | varchar | 20005443 | **商品编码（ABI 规范）**，销售条码，毛利计算的 SKU 锚点 |
| `goods_barcode` | varchar | 7000544001825 | 商品条码（物理条码） |
| `sale_unit` | varchar | kg | **销售单位**：kg/盒/件，单位换算关键 |
| `spec_num` | decimal | 0.182 | **规格数**：实际规格量（如 0.182kg） |
| `spec_type` | varchar | 2 | **规格类型**：1=g-kg，2=kg-g，3=盒(1)，4=份(1) |
| `sale_price` | decimal | 68.76 | **商品售价**（元/kg 或元/件） |
| `list_price` | decimal | 85.96 | **商品标价**（原价，不打折时的价格） |
| `display_price` | decimal | NULL | 展示价（拼团划线价等） |

### 2.4 销售数量与金额字段（毛利计算核心）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `qty` | decimal | 1.0 | **销售件数**（销售单位：件/盒） |
| `qty_spec` | decimal | 0.182 | **销售规格量**（kg 口径）= qty × spec_num |
| `actual_weight` | decimal | 0.182 | **实际重量**（散称时称重值，与 qty_spec 同义） |
| `sales_amt` | decimal | 12.51 | **日结金额**（实收金额）= qty_spec × sale_price - 折扣 |
| `p_lp_sub_amt` | decimal | 15.64 | **原价金额** = list_price × qty（不打折时的金额） |
| `p_sp_sub_amt` | decimal | 12.51 | **售价金额** = sale_price × qty |
| `actual_amount` | decimal | 12.51 | **实付金额**（扣运费/促销后） |
| `gmv` | decimal | 15.64 | **应付金额（含运费）** |
| `gmv1` | decimal | 15.64 | **应付金额（不含运费）** |

**理解**：
- `sales_amt` 是毛利计算的起点（销售额）
- `qty_spec` 是成本计算的口径（销售 kg 数）
- 毛利 = `sales_amt` - `qty_spec` × `单位成本`

### 2.5 折扣与促销字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `discount_amt` | decimal | 3.13 | **折扣额**（总折扣 = 原价 - 实收） |
| `vip_discount_amt` | decimal | 0.0 | **会员折扣额**（会员专享折扣） |
| `hour_discount_amt` | decimal | 0.0 | **时段折扣额**（晚市 19 点后折扣） |
| `i_promotion_amt` | decimal | 3.13 | **单品促销金额** |
| `order_promotion_amt` | decimal | 0.0 | **订单级别促销金额** |
| `ordercoupon_promotion_amt` | decimal | 0.0 | **订单优惠券促销金额** |
| `promotion_amt` | decimal | 0.0 | 促销金额（门店与平台优惠） |
| `promotion_amt_shop` | decimal | 3.13 | **门店承担促销费用** |
| `promotion_amt_platform` | decimal | 0.0 | **平台承担促销费用** |
| `promotion_cost` | decimal | 3.13 | **促销费用**（门店承担部分） |
| `shop_promo_sub_amt` | decimal | NULL | 门店促销分摊金额 |
| `store_paylevel_discount` | decimal | 0.0 | **门店支付级优惠金额** |
| `company_paylevel_discount` | decimal | 0.0 | **公司支付级优惠金额** |
| `gift_qty` | decimal | 0.0 | **赠品数量** |
| `gift_gmv` | decimal | 0.0 | **赠品 GMV** |

### 2.6 退货字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `return_sale_qty` | decimal | 0.0 | **退货数量** |
| `return_sale_amt` | decimal | 0.0 | **退货额** |
| `refund_type` | varchar | NULL | 退款类型：整单退/部分退/整单拒收/部分拒收 |
| `refund_at` | varchar | NULL | 退款时间 |
| `je_date` | varchar | 26042321 | **过账日期**（退货 SAP 过账） |
| `je_order_id` | varchar | NULL | 退货订单号 |
| `rje_date` | varchar | NULL | 退货过账日期（反向） |
| `rje_order_id` | varchar | NULL | 退货订单号（反向） |

### 2.7 分时段销售字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `af19_sales_amt` | decimal | 12.51 | **19 点后销售额** |
| `af19_sales_qty` | decimal | 1.0 | **19 点后销售数量** |
| `member_hour_sales_amt` | decimal | 0.0 | **会员时段销售额** |
| `is_hour_promotion` | varchar | 1 | **是否时段促销**：1=是，0=否 |

### 2.8 会员字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `customer_id` | varchar | 681750782131654236 | **会员 ID** |
| `customer_name` | varchar | 空 | 用户昵称 |
| `customer_phone` | varchar | NULL | **会员手机号**（有值=会员，NULL=非会员） |
| `first_buy_flag` | varchar | 0 | **首单标记**：0=否，1=是 |

### 2.9 渠道字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `online_flag` | varchar | N | **线上标识**：Y=线上，N=线下 |
| `business_source` | varchar | 0 | 订单业务来源：0=默认，1=团长订单 |
| `payment_type` | varchar | pay.weixin.micropay | **支付方式**：微信/支付宝/余额 |
| `currency` | varchar | CNY | 币种 |
| `activity_code` | varchar | NULL | **活动编号** |

### 2.10 时间字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `inc_time` | varchar | 2026-04-23-21 | **日结时间**（格式：YYYY-MM-DD-HH） |
| `pay_at` | varchar | 2026-04-23 21:33:35 | **支付时间** |
| `order_at` | varchar | 2026-04-23 21:33:33 | **下单时间** |
| `complete_at` | varchar | 2026-04-23 21:33:35 | **完成时间** |
| `erp_order_at` | varchar | NULL | 财务用下单日期 |

### 2.11 物流字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `delivery_id` | varchar | 8153010970050325001 | **配送表 ID** |
| `logistics_status` | varchar | NULL | **物流状态**：cancelled/shipment_receiving/shipment_in_progress/shipment_shipped |
| `courier_company` | varchar | NULL | **派单平台**：ele=蜂鸟，meituan=美团，dada=达达 |
| `courier_name` | varchar | NULL | 骑手姓名 |
| `courier_phone` | varchar | NULL | 骑手电话 |

### 2.12 支付金额明细字段（p/f 前缀）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `p_paid_sub_amt` | decimal | 12.51 | **商品实付金额**（principal 本金） |
| `f_paid_sub_amt` | decimal | 0.0 | **运费实付金额**（freight 运费） |
| `p_pay_sub_amt` | decimal | 12.51 | **商品应付金额** |
| `f_pay_sub_amt` | decimal | 0.0 | **运费应付金额** |
| `p_promo_sub_amt` | decimal | 0.0 | **商品促销分摊金额** |
| `f_promo_sub_amt` | decimal | 0.0 | **运费促销分摊金额** |
| `p_pointpay_sub_amt` | decimal | 0.0 | **商品积分支付金额** |
| `f_pointpay_sub_amt` | decimal | 0.0 | **运费积分支付金额** |
| `p_balancepay_sub_amt` | decimal | 0.0 | **商品余额支付金额** |
| `f_balancepay_sub_amt` | decimal | 0.0 | **运费余额支付金额** |
| `p_cashpay_sub_amt` | decimal | 0.0 | **商品现金抵扣金额** |
| `f_cashpay_sub_amt` | decimal | 0.0 | **运费现金抵扣金额** |
| `p_change_sub_amt` | decimal | 0.0 | **抹分抹角金额**（零钱处理） |

**理解**：
- `p` = principal（本金/商品）
- `f` = freight（运费）
- `sales_amt = p_paid_sub_amt`（线下无运费时）

### 2.13 其他字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `tenant_id` | varchar | 0210000003 | **商户 ID** |
| `serial_id` | varchar | 空 | 订单序列号 |
| `message` | varchar | NULL | 订单备注 |
| `internal_comment` | varchar | NULL | 给管家端的备注 |
| `sync_seq` | varchar | 23010970050325 | **过账日期** |
| `sync_flag` | varchar | 2 | **同步标识**：0=正常，1=正向待同步，2=正向同步成功，3=逆向待同步，4=逆向同步成功 |
| `bundle_promo_code` | varchar | NULL | 促销号（换购/赠品活动） |
| `is_promotion_article` | varchar | 1 | **是否促销商品**：1=是，0=否 |
| `jielong_flag` | varchar | - | **接龙标识**：community=线上接龙，shop=门店接龙，-=非接龙 |
| `postage_shop` | decimal | 0.0 | 门店承担邮费 |
| `postage_platform` | decimal | 0.0 | 平台承担邮费 |
| `promotion_amt_platform_gs` | decimal | 0.0 | 平台承担（公司部分） |
| `promotion_amt_platform_gys` | decimal | 0.0 | 平台承担（供应商部分） |
| `p_mp_sub_amt` | decimal | 15.64 | 商品中台总价 |

---

## 三、表 2：进货验收 `strategy_fm_purchase_di`（BOM 预拆表）

**源表**：`hive.dsl.dsl_transaction_non_daily_store_article_purchase_di`
**粒度**：(store_id, business_date, article_id, sale_article_id, day_clear)
**字段数**：16
**关键**：这张表已经预拆到 sale_article_id（sub）级，是 BOM 分摊的基础数据。

### 3.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `business_date` | varchar | 2026-04-23 | **营业日期** |
| `inc_day` | varchar | 2026-04-23 | 增量日（分区键） |
| `store_id` | varchar | A3XV | 门店编码 |
| `day_clear` | varchar | 1 | **日清标识**：0=日清，1=非日清 |

### 3.2 商品编码字段（BOM 核心）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `article_id` | varchar | 21292408 | **验收条码**（进货扫码编码，根 parent） |
| `article_name` | varchar | 红头香葱100g(c) | 验收条码名称 |
| `sale_article_id` | varchar | 21292408 | **销售条码**（BOM 子商品，销售 SKU） |
| `sale_article_name` | varchar | 红头香葱100g(c) | 销售条码名称 |

**理解**：
- 当 `article_id = sale_article_id`：标品，无需 BOM 拆分
- 当 `article_id ≠ sale_article_id`：BOM 拆分，验收大件拆成销售散件

### 3.3 进货数量与金额字段（BOM 分摊核心）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `sale_article_qty` | decimal | 20.0 | **销售条码进货量**（sub 分摊到的 kg/件数） |
| `sale_article_purchase_amt` | decimal | 20.8 | **销售条码进货额**（sub 分摊到的金额） |

**理解**：
- 这两个字段是 BOM 分摊的结果
- 对于 BOM 商品：`sale_article_qty` = parent 进货量 × 出肉率
- 对于标品：`sale_article_qty` = parent 进货量（1:1）

### 3.4 库存字段（损耗计算关键）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `init_stock_qty` | decimal | -4.0 | **期初库存数量**（昨日结转） |
| `init_stock_amt` | decimal | -4.16 | **期初库存金额** = init_stock_qty × 成本价 |
| `end_stock_qty` | decimal | -5.0 | **期末库存数量**（当日结余） |
| `end_stock_amt` | decimal | -5.2 | **期末库存金额** = end_stock_qty × 成本价 |
| `inventory_cost` | decimal | 1.04 | **库存成本单价** |

**理解**：
- 期初/期末库存用于库存方程验证
- 日清商品期末负库存 = 当日损耗（未售完报废）
- 库存方程：`期末 - 期初 = 进货 - 销售 - 损耗`

### 3.5 成本价字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `avg_inbound_price` | decimal | 1.04 | **进货平均价**（parent 均价） |

**理解**：
- 这是 parent（验收条码）的进货均价
- 用于 BOM 分摊计算：sub 成本 = parent 均价 × 出肉率 × 成本比例
- 对于标品：直接作为单位成本

---

## 四、表 5：损耗 `strategy_fm_loss_di`

**源表**：`hive.dal.dal_transaction_store_article_lost_di`
**粒度**：(store_id, article_id, inc_day)
**字段数**：10
**关键**：记录商品的已知损耗和未知损耗。

### 4.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `store_id` | varchar | A3XV | 门店编码 |
| `article_id` | varchar | 21304262 | **商品编码**（销售条码） |
| `article_name` | varchar | 洽洽焦糖瓜子68g(C) | 商品名称 |
| `category_level1_id` | varchar | 27 | **大类编码**（标品类=27） |
| `category_level1_description` | varchar | 标品类 | 大类名称 |
| `inc_day` | varchar | 2026-04-23 | 增量日 |

### 4.2 损耗数量与金额字段（毛利扣减项）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `know_lost_qty` | decimal | 0.0 | **已知损耗数量**（门店登记报损） |
| `know_lost_amt` | decimal | 0.0 | **已知损耗金额** = know_lost_qty × 成本价 |
| `unknow_lost_qty` | decimal | 0.0 | **未知损耗数量**（库存方程反推） |
| `unknow_lost_amt` | decimal | 0.0 | **未知损耗金额** |

**理解**：
- **已知损耗**：门店主动登记的报损（过期、损坏、被盗等）
- **未知损耗**：由上游库存方程反推：`未知损耗 = 期初 + 进货 - 销售 - 期末 - 已知损耗`
- 毛利计算扣减：`损耗额 = know_lost_amt + unknow_lost_amt`
- ⚠ 本 ETL 有自己的库存方程反推，不直接用上游 unknow_lost

---

## 五、表 6：加工转换 `strategy_fm_compose_di`

**源表**：`hive.dsl.dsl_transaction_sotre_article_compose_info_di`
**粒度**：(store_id, business_date, article_id)
**字段数**：11
**关键**：记录门店加工转换（原料 → 成品）的数量和金额。

### 5.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `business_date` | varchar | 2026-04-23 | 营业日期 |
| `store_id` | varchar | A3XV | 门店编码 |
| `store_name` | varchar | 广州滨江宏岸店 | 门店名称 |
| `article_id` | varchar | 21282423 | **商品编码**（加工原料或成品） |
| `article_name` | varchar | 葡式蛋挞6个(C) | 商品名称 |
| `inc_day` | varchar | 2026-04-23 | 增量日 |

### 5.2 加工转换数量与金额字段（库存方程关键）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `compose_in_qty` | decimal | 5.0 | **加工转换入数量**（原料入库） |
| `compose_in_amt` | decimal | 32.37 | **加工转换入金额** = compose_in_qty × 原料成本价 |
| `compose_out_qty` | decimal | 0.0 | **加工转换出数量**（成品产出） |
| `compose_out_amt` | decimal | 0.0 | **加工转换出金额** = compose_out_qty × 成本价 |
| `update_time` | datetime | 2026-04-23 | 最后更新时间 |

**理解**：
- **compose_in**：门店收到原料（如收到 5kg 面团）
- **compose_out**：门店产出成品（如产出 0kg 蛋挞，当天未产出）
- 库存方程：`compose_in - compose_out` 加到库存变动
- ⚠ compose 记录在 parent（原料）身上，需配合 BOM 边拆到 sub

---

## 六、表 17：BOM 分摊事实 `strategy_fm_receive_sale_di`（v4 首选主源）

**源表**：`hive.dal.dal_receive_sale_di`
**粒度**：(store_id, business_date, article_id[parent], sale_article_id[sub])
**字段数**：20
**关键**：QDM 侧官方 BOM 分摊事实表，每行是一天里某个 parent 给某个 sub 分到了多少 kg / 多少元。

### 6.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `business_date` | varchar | 2026-04-23 | 营业日期 |
| `store_id` | varchar | A3XV | 门店编码 |
| `inc_day` | varchar | 2026-04-23 | 增量日 |

### 6.2 Parent（验收条码）字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `article_id` | varchar | 20000219 | **根 parent**（验收条码，如西洋菜） |
| `article_name` | varchar | 西洋菜 | parent 名称 |
| `category_level1_id` | varchar | 10 | parent 大分类（蔬菜类=10） |
| `category_level1_description` | varchar | 蔬菜类 | parent 大分类名 |

### 6.3 Parent 进货字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `inbound_qty` | decimal | 1.0 | **parent 进货数量**（kg 或件） |
| `inbound_amount` | decimal | 8.31 | **parent 进货金额**（元） |
| `purchase_price` | decimal | 8.31 | **parent 进货均价** = inbound_amount / inbound_qty |

### 6.4 Sub（销售条码）字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `sale_article_id` | varchar | 20000219 | **sub 销售 SKU**（与 parent 同值表示标品） |
| `sale_article_name` | varchar | 西洋菜 | sub 名称 |
| `sale_article_qty` | decimal | 1.0 | **sub 分摊 qty**（从 parent 拆出的 kg 或件数） |
| `spilit_sale_article_amt` | decimal | 8.31 | **sub 分摊 amt**（从 parent 拆出的金额）⚠ 源表拼写瑕疵 |
| `sale_article_price` | decimal | 8.31 | **sub 分摊单价** = spilit_sale_article_amt / sale_article_qty |

### 6.5 归一化比例字段（多 parent 合并关键）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `rate` | decimal | 1.0 | **parent → sub 的 qty 占比** |
| `sale_recev_rate` | decimal | 1.0 | **sub 占 parent 的占比**（反向比例） |
| `sum_sub_article_qty` | decimal | 1.0 | **parent 下所有 sub 的 qty 汇总**（归一化验证） |
| `sum_article_qty` | decimal | 1.0 | **同一验收条码下销售条码理论销量汇总** |
| `sum_sale_article_qty` | decimal | 1.0 | **同一 sub 从所有 parent 拆出的合计 qty** |

**理解**：
- `rate`：该 parent 给这个 sub 的比例（如 100%=全部给这个 sub）
- `sum_sub_article_qty`：验证出肉率归一化（≈ 100%）
- `sum_sale_article_qty`：多 parent 合并时，按此加权

### 6.6 与 purchase_di 的差异

| 维度 | `purchase_di`（表 2） | `receive_sale_di`（表 17） |
|---|---|---|
| BOM 拆分行占比 | ~4.5%（大部分是标品） | **100%**（全是 BOM 事实） |
| parent × sub 双维度 | 仅 `sale_article_id` 单维度 | **article_id + sale_article_id 双维度** |
| 分摊金额字段 | `sale_article_purchase_amt` | **`spilit_sale_article_amt` + `inbound_amount`** |
| 归一化比例字段 | 无 | **rate / sale_recev_rate / sum_* ** |

**结论**：v4 用 receive_sale_di 作为 BOM 分摊主源，更完整。

---

## 七、表 20：BOM 关系边 `strategy_dim_store_article_bom_relation`

**粒度**：(store_id, inc_day, parent_article_id, sub_article_id)
**字段数**：17
**关键**：定义 parent → sub 的出肉率和成本比例。

### 7.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `store_id` | varchar | A3XV | 门店编码 |
| `inc_day` | varchar | 2026-04-23 | 增量日 |

### 7.2 分类字段（parent 大分类）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `category_level1_id` | varchar | 13 | **parent 大分类**（猪肉类=13） |
| `category_level1_description` | varchar | 猪肉类 | parent 大分类名 |
| `category_level2_id` | varchar | 1302 | parent 中分类（边猪类） |
| `category_level2_description` | varchar | 边猪类 | parent 中分类名 |
| `category_level3_id` | varchar | 130206 | parent 小分类（黑边猪类） |
| `category_level3_description` | varchar | 黑边猪类 | parent 小分类名 |

### 7.3 BOM 边字段（核心）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `parent_article_id` | varchar | 20500283 | **BOM 头商品**（parent，如整头猪） |
| `parent_article_unit` | varchar | KG | **parent 单位**：KG/件/头 |
| `sub_article_id` | varchar | 20110840 | **BOM 子商品**（sub，如排骨） |
| `sub_article_unit` | varchar | KG | **sub 单位**：KG/件 |

### 7.4 出肉率与成本比例（BOM 分摊核心参数）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `dressing_rate` | decimal | 11.099 | **标准出肉率（%）**：parent 拆出 sub 的比例 |
| `cost_rate` | decimal | 0.0 | **成本比例（%）**：sub 承担 parent 成本的比例 ⚠ 可能 NULL |

**理解**：
- **出肉率**：1 头猪拆出 11.099% 的排骨（重量占比）
- 同一 parent 下所有 sub 的 `dressing_rate` 合计 ≈ 100%
- **成本比例**：财务分摊比例，合计 = 100%
- 当 `cost_rate = 0 或 NULL`：需反推兜底（用 sub 原价 × dressing_rate 加权）

### 7.5 BOM 类型字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `bom_type` | int | 3 | **门店 BOM 类型**：1=门店 BOM，2=仓 BOM，3=非门店非仓 |
| `split_mode` | varchar | 10 | **拆分模式**：10=一拆多，20=一拆一 |
| `sp_level` | int | 170 | **BI 店铺等级**：170=新业务门店 |

---

## 八、表 3：SAP 出入库 `strategy_fm_scm_di`

**粒度**：(store_id, business_date, article_id)
**字段数**：54
**关键**：记录 SAP 供应链出入库事实（出库成本、退仓、让利）。

### 8.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `business_date` | varchar | 2026-04-23 | 业务日期 |
| `store_id` | varchar | A3XV | 门店编码 |
| `article_id` | varchar | 21255403 | 商品编码 |
| `new_dc_id` | varchar | D094 | **仓 ID**（配送中心） |
| `inc_day` | varchar | 2026-04-23 | 增量日 |

### 8.2 出库数量字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `original_outstock_qty` | decimal | 1.0 | **常规出库数量**（原价出库） |
| `original_outstock_amt` | decimal | 19.39 | **常规出库金额** |
| `promotion_outstock_qty` | decimal | 0.0 | **促销出库数量** |
| `promotion_outstock_amt` | decimal | 0.0 | **促销出库金额** |
| `promotion_outstock_price` | decimal | 0.0 | 促销出库单价 |
| `gift_outstock_qty` | decimal | 0.0 | **赠品出库数量** |
| `total_outstock_qty` | decimal | 1.0 | **总出库数量** = 常规 + 促销 + 赠品 |

### 8.3 出库金额字段（成本关键）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `out_stock_pay_amt` | decimal | 19.39 | **出库应付金额（含税）** |
| `out_stock_pay_amt_notax` | decimal | 17.7889 | **出库应付金额（不含税）** |
| `out_stock_amt_cb` | decimal | 17.2874 | **出库成本金额（含税，CB=cost book）** |
| `out_stock_amt_cb_notax` | decimal | 15.86 | **出库成本金额（不含税）** |
| `out_stock_zzckj_amt` | decimal | 0.0 | 出库原价金额（加价额） |

**理解**：
- `out_stock_amt_cb` 是 SAP 出库成本（门店收货成本）
- 出库成本单价 = `out_stock_amt_cb / total_outstock_qty`

### 8.4 退仓字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `return_stock_qty` | decimal | 0.0 | **门店退仓数量** |
| `return_stock_pay_amt` | decimal | 0.0 | **退仓应付金额（含税）** |
| `return_stock_pay_amt_notax` | decimal | 0.0 | **退仓应付金额（不含税）** |
| `return_stock_amt_cb` | decimal | 0.0 | **退仓成本金额（含税）** |
| `return_stock_amt_cb_notax` | decimal | 0.0 | **退仓成本金额（不含税）** |
| `return_stock_original_amt` | decimal | 0.0 | 退仓原价金额 |
| `store_return_qty_shop` | decimal | 0.0 | 门店退货数量（中台） |
| `store_return_amt_shop` | decimal | 0.0 | 门店退货金额（中台） |

### 8.5 订货字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `store_order_qty` | decimal | 1.0 | **门店订购数量**（订货单位） |
| `order_qty_payean` | decimal | 1.0 | **订购数量（结算单位）** |
| `order_amt` | decimal | 19.39 | **订购金额** |

### 8.6 调整字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `adjustment_amt` | decimal | 0.0 | **差异调整金额（含税）** |
| `adjustment_amt_notax` | decimal | 0.0 | **差异调整金额（不含税）** |

### 8.7 让利承担字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `scm_promotion_amt_total` | decimal | 0.0 | **出库让利总额** |
| `scm_promotion_amt` | decimal | 0.0 | **非赠品出库让利金额** |
| `scm_promotion_amt_gift` | decimal | 0.0 | **赠品出库让利金额** |
| `scm_promotion_qty_gift` | decimal | 0.0 | 赠品出库让利数量 |
| `scm_promotion_cost` | decimal | 0.0 | 供应链出库让利金额 |
| `scm_return_promotion_cost` | decimal | 0.0 | 供应链让利费用金额（退仓） |
| `scm_bear_amt` | decimal | 0.0 | **供应链承担非赠品让利** |
| `vendor_bear_amt` | decimal | 0.0 | **供应商承担非赠品让利** |
| `business_bear_amt` | decimal | 0.0 | **运营承担非赠品让利** |
| `business_market_bear_amt` | decimal | 0.0 | 运营市场承担让利 |
| `market_bear_amt` | decimal | 0.0 | **市场承担非赠品让利** |
| `vender_bear_gift_amt` | decimal | 0.0 | 供应商承担赠品金额 |
| `scm_bear_gift_amt` | decimal | 0.0 | 供应链承担赠品金额 |
| `vender_bear_gift_qty` | decimal | 0.0 | 供应商承担赠品数量 |
| `scm_bear_gift_qty` | decimal | 0.0 | 供应链承担赠品数量 |

### 8.8 QDM 承担字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `qdm_bear_negative_amt_total` | decimal | 0.0 | 公司承担负让利总额 |
| `qdm_bear_positive_amt_total` | decimal | 0.0 | 公司承担让利总额 |
| `qdm_bear_gift_qty` | decimal | 0.0 | 公司承担赠品数量 |
| `qdm_bear_gift_amt` | decimal | 0.0 | 公司承担赠品金额 |
| `qdm_bear_nogift_negative_amt` | decimal | 0.0 | 公司承担非赠品负让利 |
| `qdm_bear_nogift_positive_amt` | decimal | 0.0 | 公司承担非赠品让利 |
| `qdm_bear_promotion_fee` | decimal | 0.0 | 公司承担促销费用 |

### 8.9 少货字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `miss_stock_qty` | decimal | 0.0 | **少货数量**（到货不足） |
| `miss_stock_amt` | decimal | 0.0 | **少货金额** |

---

## 九、表 9：成本价池 `strategy_fm_inventory_pool_di`

**源表**：`hive.ods_sc_db.t_shop_inventory_sku_pool`
**粒度**：(shop_id, sku_code, inventory_date)
**字段数**：18
**关键**：存储 SKU 的成本价快照。

### 9.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `id` | bigint | 19254290 | 物理主键 |
| `shop_id` | varchar | A3XV | **门店编号** ⚠ 映射到 store_id |
| `sku_code` | varchar | 20639808 | **商品编号** ⚠ 映射到 article_id |
| `inventory_date` | varchar | 2025-09-13 | **盘点日期** ⚠ 映射到 business_date |
| `inc_day` | varchar | 2026-04-23 | 增量日 |

### 9.2 商品属性字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `sku_name` | varchar | 味事达味极鲜760ml(电商) | 商品名称 |
| `sub_category_id` | varchar | 273701 | 小分类编码 |
| `sub_category_name` | varchar | 调味酱类 | 小分类名称 |
| `spec` | varchar | 760ml | 规格 |
| `sales_unit` | varchar | 瓶 | 销售单位 |
| `main_img` | varchar | NULL | 商品主图 |
| `gift_flag` | int | 0 | **赠品标识**：0=非赠品，1=赠品 |

### 9.3 成本价字段（一级成本来源）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `cost_price` | decimal(32,4) | 12.79 | **成本价**（元/单位）⭐ 一级成本来源 |

### 9.4 时间字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `created_at` | varchar | 2025-09-13 20:00:04 | 创建时间 |
| `created_by` | varchar | system | 创建人 |
| `updated_at` | varchar | 2025-09-13 20:00:04 | 更新时间 |
| `updated_by` | varchar | system | 更新人 |
| `last_updated_at` | varchar | 2025-09-13 20:00:04 | 最后更新时间 |

---

## 十、表 10：价格 `strategy_fm_price_da`

**源表**：`hive.dim.dim_store_article_price_info_da`
**粒度**：(shop_id, sku_code, inc_day)
**字段数**：54
**关键**：存储 SKU 的售价和出库价。

### 10.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `id` | bigint | 28578251009 | 物理主键 |
| `shop_id` | varchar | A3XV | 门店编号 |
| `sku_code` | varchar | 20001674 | 商品编号 |
| `tenant_id` | varchar | 0210000001 | 租户编码 |
| `shop_name` | varchar | 广州滨江宏岸店 | 门店名称 |
| `dc_code` | varchar | D094 | **仓库编号** |
| `dc_name` | varchar | 广州江高虚拟生鲜仓 | 仓库名称 |
| `inc_day` | varchar | 2026-04-23 | 增量日 |

### 10.2 售价字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `current_price` | decimal | 1.96 | **今日售价** |
| `yesterday_price` | decimal | 2.36 | **昨日售价** |
| `original_price` | decimal | 1.8 | **销售原价** ⭐ cost_rate 反推权重 |
| `unadjust_sale_price` | decimal | NULL | 调整前价格 |
| `anchor_sale_price` | decimal | NULL | 锚定销售价 |

### 10.3 出库价字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `dc_price` | decimal | 1.2 | **出库价格**（仓出库价） |
| `dc_original_price` | decimal | 1.2 | **出库原价格** |
| `dc_original_price_sap` | decimal | 1.2 | **出库原价（来自 SAP）** |
| `original_dc_price` | decimal | 1.8 | 销售原价（根据出库价计算） |
| `yesterday_dc_price` | decimal | 1.49 | **昨日出库价格** |

### 10.4 加价/毛利率字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `outstock_addprice_amt` | decimal | 0.36 | **出库加价额** = current_price - dc_price |
| `outstock_addprice_rate` | decimal | 0.0 | **出库加价率** |
| `outstock_profit_rate` | decimal | 0.0 | **出库毛利率** |
| `outstock_lock_price` | decimal | 0.0 | 出库固定价 |

### 10.5 状态字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `deal_status` | varchar | UNREQUIRED | 处理状态：UNDEAL/DEALT/UNREQUIRED |
| `calc_status` | varchar | RIGHT | 计算状态：RIGHT/WRONG |
| `confirm_status` | varchar | 空 | 确认状态：UNCONFIRM/CONFIRMED |
| `sale_status` | varchar | SALABLE | **可售状态**：SALABLE/UNSALE |
| `lock_status` | int | 1 | **锁定状态**：0=非锁定，1=锁定 |
| `push_status` | varchar | JOB_PUSHED | 推送状态：pushed/unpush |
| `effective` | int | NULL | 是否生效 |
| `is_new` | int | 0 | 新/旧价格策略数据 |

### 10.6 策略字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `strategy_no` | varchar | 门店商品价格策略配置 | 策略编号 |
| `promotion_no` | varchar | 空 | 促销编号 |
| `calc_strategy` | varchar | JSON | **参与价格计算策略详情** |
| `calc_effect_at` | varchar | 2026-04-23 | 价格计算生效时间 |
| `sales_mode` | varchar | 10 | 销售方式 |

---

## 十一、表 19：单位换算 `strategy_fm_dim_article_convert`

**粒度**：(store_id, parent_article_id, sub_article_id)
**字段数**：9
**关键**：定义 parent → sub 的单位换算系数。

### 11.1 BOM 边字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `store_id` | varchar | A3XV | 门店编码 |
| `parent_article_id` | varchar | 20032777 | **父商品编码**（香菜约50g） |
| `parent_article_name` | varchar | 香菜约50g | 父商品名称 |
| `sub_article_id` | varchar | 21292446 | **子商品编码**（香菜100g(c)） |
| `sub_article_name` | varchar | 香菜100g(c) | 子商品名称 |

### 11.2 换算系数字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `parent_rate` | decimal | 0.5 | **父单位量 × rate = 子单位量** |
| `sub_rate` | decimal | 2.0 | **子单位量 × rate = 父单位量** |

**理解**：
- `parent_rate = 0.5`：1 个父商品 = 0.5 个子商品（50g → 100g，反向）
- `sub_rate = 2.0`：1 个子商品 = 2 个父商品（100g → 50g × 2）
- ⚠ 实际出肉率用表 20 的 `dressing_rate`

### 11.3 类型字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `ctype` | int | 1 | **场景类型**：1=BOM，2=spu，3=spu-BOM 混合 |

---

## 十二、BOM 毛利计算逻辑汇总

### 12.1 BOM 分摊三级数据源

| 优先级 | 数据源 | 表 | 适用场景 |
|---|---|---|---|
| **1** | RECEIVE_SALE | 表 17 | 上游已拆好 BOM 事实，直接取 sale_article_qty / spilit_sale_article_amt |
| **2** | COMPOSE | 表 6 + 表 20 | receive_sale 缺失但有加工转换，用 compose_out_qty × dressing_rate |
| **3** | BOM_THEORETICAL | 表 20 + 表 2 | 完全理论值，parent.receive_qty × dressing_rate |

### 12.2 cost_rate 反推公式

当表 20 的 `cost_rate = 0 或 NULL` 时：

```sql
w_i = sub.original_price × dressing_rate
cost_rate_inferred = w_i / SUM(w_i) OVER parent
alloc_amt = parent.inbound_amount × cost_rate_inferred
```

### 12.3 四级成本来源

```sql
effective_unit_cost = COALESCE(
    NULLIF(cost_price, 0),        -- ① DIRECT: 表 9 成本价池
    NULLIF(bom_unit_cost, 0),     -- ② BOM_ALLOC: 表 17 分摊成本
    NULLIF(avg_inbound_price, 0), -- ③ PURCHASE_AVG: 表 2 进货均价
    0                             -- ④ MISSING: 告警
)
```

### 12.4 门店毛利公式

**销售方程**：
```
门店毛利 = SUM(sales_amt) - SUM(qty_spec × effective_unit_cost) - SUM(know_lost_amt) - SUM(unknow_lost_amt)
```

**库存方程验证**：
```
门店毛利 = 销售额 - (进货额 + compose_in_amt - compose_out_amt) + (期末库存 - 期初库存) - 损耗额
```

### 12.5 损耗计算

- **已知损耗**：表 5 `know_lost_amt`
- **未知损耗**：库存方程反推
  ```
  unknow_lost = init_stock + receive + compose_in - compose_out - sale_qty - end_stock - know_lost
  ```
- 日清商品：期末负库存 = 当日损耗

---

## 十三、表 21：门店商品库存明细 `strategy_fm_store_article_inventory_detail_di`（v4 新增）

**源表**：`hive.ddl.ddl_transaction_store_article_inventory_detail_di`
**StarRocks 路径**：`default_catalog.ads_business_analysis.strategy_fm_store_article_inventory_detail_di`
**粒度**：(shop_id, sku_code, inventory_date)
**字段数**：21
**关键**：记录每日每个门店每 SKU 的系统库存和实盘库存快照，是区分"算的库存"和"盘的库存"的核心表。

### 13.1 维度字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `id` | bigint | 3134046 | 物理主键 |
| `shop_id` | varchar | A3XV | **门店编号** ⚠ 映射到 store_id |
| `inventory_date` | varchar | 2026-04-23 | **盘点日期** ⚠ 映射到 business_date |
| `sku_code` | varchar | 21292699 | **商品编号** ⚠ 映射到 article_id |
| `inc_day` | varchar | 2026-04-23 | 增量日（分区键） |

### 13.2 库存数量与金额字段（核心）

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `sale_stock_qty` | decimal(32,4) | 67.00 | **系统库存数量** — 库存方程计算的结果 |
| `stock_cost` | decimal(32,4) | 112.56 | **库存总成本** — 系统库存 × 成本单价 |
| `actual_stock_qty` | decimal(32,4) | 67.00 | **实盘库存数量** — 人工盘点实际数 |
| `profit_loss_qty` | decimal(32,4) | 0.00 | **盈亏数量** = actual - sale |

### 13.3 商品属性字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `sku_name` | varchar | 宁夏菜心300g(c) | 商品名称 |
| `sub_category_id` | varchar | 101015 | 小分类编码 |
| `sub_category_name` | varchar | 菜心类 | 小分类名称 |
| `spec` | varchar | 300g | 规格 |
| `sales_unit` | varchar | 袋 | 销售单位 |
| `main_img` | varchar | /popimg/... | 商品主图 |
| `gift_flag` | int | 0 | 赠品标识：0=非赠品，1=赠品 |

### 13.4 时间字段

| 字段 | 类型 | 样本值 | 我的理解 |
|---|---|---|---|
| `created_at` | varchar | 2026-04-23 23:05:04 | 创建时间 |
| `created_by` | varchar | 系统 | 创建人 |
| `updated_at` | varchar | 2026-04-23 23:05:04 | 更新时间 |
| `updated_by` | varchar | 系统 | 更新人 |
| `last_updated_at` | varchar | 2026-04-23 23:05:05 | 最后更新时间 |

**理解**：
- **`sale_stock_qty`**：由上游库存方程计算得出，等价于 `purchase_di.end_stock_qty`
- **`actual_stock_qty`**：门店实际盘点数量，仅在盘点日有差异
- **`profit_loss_qty`**：当 ≠ 0 时，说明当天有盘点且系统库存与实际不符
- **v10 用法**: `actual_stock_qty` + `created_by` 判定盘点: `created_by != '系统'` → is_counted（信任实盘值覆盖 end_stock）；`created_by = '系统'` → 继续用库存方程自算
- 该表可用来验证 `t_calc_stock.end_stock_qty` 是否与上游系统库存一致

### 13.5 与 purchase_di 的关系

| 维度 | `purchase_di`（表 2） | `store_article_inventory_detail_di`（表 21） |
|---|---|---|
| 数据性质 | 进销存流水（流入流出） | **库存快照**（时点余额） |
| end_stock_qty | 有（每日结余） | **sale_stock_qty = 同一概念** |
| 实盘库存 | 无 | **有（actual_stock_qty）** |
| 盘点盈亏 | 无 | **有（profit_loss_qty）** |
| 用途 | ETL 取 init_stock / avg_inbound_price / purchase_receive | **v10: 人工盘点判定 (is_counted)** |

---

## 附录A：Hive 源表完整字段（数据源）

> 以下按 Hive 源表组织，列出每张源表的全部字段。字段名即 Hive/StarRocks 实际列名。
> 映射关系：`Hive源表 →(SELECT *)→ strategy_fm_* →(ETL)→ DuckDB atomic_*`

### A.1 源表总览

| 序号 | Hive 源表 | → strategy_fm | → DuckDB | 字段数 |
|---|---:|---|---|---:|
| 1 | `hive.dsl.dsl_transaction_non_daily_store_order_details_di` | `strategy_fm_sales_di` | `atomic_sales` | 119 |
| 2 | `hive.dsl.dsl_transaction_non_daily_store_article_purchase_di` | `strategy_fm_purchase_di` | `atomic_inventory` | 16 |
| 3 | `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di` | `strategy_fm_scm_di` | `atomic_scm` | 54 |
| 4 | `hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di` | `strategy_fm_scm_adjust_di` | `atomic_scm_adjust` | 10 |
| 5 | `hive.dal.dal_transaction_store_article_lost_di` | `strategy_fm_loss_di` | `atomic_loss` | 10 |
| 6 | `hive.dsl.dsl_transaction_sotre_article_compose_info_di` | `strategy_fm_compose_di` | `atomic_compose` | 11 |
| 7 | `hive.dal.dal_activity_article_order_sale_info_di` | `strategy_fm_allowance_di` | `atomic_allowance` | 76 |
| 8 | `hive.dsl.dsl_promotion_order_item_article_sale_info_di` | `strategy_fm_promo_di` | `atomic_promo` | 90 |
| 9 | `hive.ods_sc_db.t_shop_inventory_sku_pool` | `strategy_fm_inventory_pool_di` | `atomic_cost_price` | 18 |
| 10 | `hive.dim.dim_store_article_price_info_da` | `strategy_fm_price_da` | `atomic_price` | 54 |
| 11 | `hive.dim.dim_chdj_store_list_di` | `strategy_fm_dim_store_list` | `dim_store_list` | *(未启用)* |
| 12 | `hive.dim.dim_day_clear_article_list_di` | `strategy_fm_dim_day_clear` | `dim_day_clear` | 11 |
| 13 | `hive.dim.dim_store_profile` | `strategy_fm_dim_store_profile` | `dim_store_profile` | 109 |
| 14 | `hive.ods_sc_db.t_purchase_order_item_tmp` | `strategy_fm_dim_saleable` | `dim_saleable` | 47 |
| 15 | `hive.dim.dim_goods_information_have_pt` | `strategy_fm_dim_goods` | `dim_goods` | 106 |
| 16 | `hive.dim.dim_calendar` | `strategy_fm_dim_calendar` | `dim_calendar` | 70 |
| 17 | `hive.dal.dal_receive_sale_di` | `strategy_fm_receive_sale_di` | `atomic_receive_sale` | 20 |
| 18 | `hive.dal.dal_store_order_receive_di` | `strategy_fm_order_receive_di` | `atomic_order_receive` | 17 |
| 19 | `hive.dim.dim_store_article_convert_info_da` | `strategy_fm_dim_article_convert` | `atomic_article_convert` | 9 |
| 20 | `hive.dim.dim_store_article_bom_relation` | `strategy_dim_store_article_bom_relation` | `atomic_bom_relation` | 17 |
| 21 | `hive.ddl.ddl_transaction_store_article_inventory_detail_di` | `strategy_fm_store_article_inventory_detail_di` | `atomic_inventory_detail` (v10: 人工盘点判定) | 21 |

### A.2 字段映射注意事项

部分 Hive 源表字段名与 strategy_fm 不同（已由商分 SQL 层面处理），常见差异：

| Hive 源表 | 源字段 → 目标字段 |
|---|---|
| `t_shop_inventory_sku_pool` | `shop_id` → `store_id`, `sku_code` → `article_id`, `inventory_date` → `business_date` |
| `t_purchase_order_item_tmp` | `shop_id` → `store_id`, `sku_code` → `article_id` |
| `dsl_promotion_order_item_article_sale_info_di` | `shop_id` → `store_id`, `sku_code` → `article_id` |
| `dim_store_profile` | `sp_store_id` → `store_id` |
| `dim_calendar` | `day_date` → `business_date` |

---

### ① 销售 `hive.dsl.dsl_transaction_non_daily_store_order_details_di`

119 字段。分区键 `inc_day`，门店过滤 `store_id`。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `business_date` | varchar |
| 2 | `store_id` | varchar |
| 3 | `sp_store_name` | varchar |
| 4 | `area_description` | varchar |
| 5 | `area_id` | varchar |
| 6 | `sp_type` | varchar |
| 7 | `sp_level` | varchar |
| 8 | `order_id` | varchar |
| 9 | `order_status` | varchar |
| 10 | `parent_order_id` | varchar |
| 11 | `children_order_ids` | varchar |
| 12 | `serial_id` | varchar |
| 13 | `delivery_id` | varchar |
| 14 | `tenant_id` | varchar |
| 15 | `message` | varchar |
| 16 | `internal_comment` | varchar |
| 17 | `reason` | varchar |
| 18 | `first_buy_flag` | varchar |
| 19 | `comment_id` | varchar |
| 20 | `comment_time` | varchar |
| 21 | `sibling_order_ids` | varchar |
| 22 | `split_supported` | varchar |
| 23 | `root_order_id` | varchar |
| 24 | `afs_order_id` | varchar |
| 25 | `outer_order_id` | varchar |
| 26 | `outer_order_type` | varchar |
| 27 | `payment_type` | varchar |
| 28 | `bundle_promo_code` | varchar |
| 29 | `sync_seq` | varchar |
| 30 | `je_date` | varchar |
| 31 | `je_order_id` | varchar |
| 32 | `rje_date` | varchar |
| 33 | `rje_order_id` | varchar |
| 34 | `is_hour_promotion` | varchar |
| 35 | `abi_article_id` | varchar |
| 36 | `is_promotion_article` | varchar |
| 37 | `online_flag` | varchar |
| 38 | `goods_barcode` | varchar |
| 39 | `spec_num` | decimal |
| 40 | `spec_type` | varchar |
| 41 | `customer_id` | varchar |
| 42 | `customer_name` | varchar |
| 43 | `customer_phone` | varchar |
| 44 | `sale_unit` | varchar |
| 45 | `display_price` | decimal |
| 46 | `list_price` | decimal |
| 47 | `sale_price` | decimal |
| 48 | `order_type` | varchar |
| 49 | `order_sub_type` | varchar |
| 50 | `channel_id` | varchar |
| 51 | `inc_time` | varchar |
| 52 | `pay_at` | varchar |
| 53 | `refund_at` | varchar |
| 54 | `refund_type` | varchar |
| 55 | `sync_flag` | varchar |
| 56 | `currency` | varchar |
| 57 | `erp_order_at` | varchar |
| 58 | `split_at` | varchar |
| 59 | `complete_at` | varchar |
| 60 | `order_at` | varchar |
| 61 | `allrefund_time` | varchar |
| 62 | `qty` | decimal |
| 63 | `qty_spec` | decimal |
| 64 | `p_paid_sub_amt` | decimal |
| 65 | `f_paid_sub_amt` | decimal |
| 66 | `p_pay_sub_amt` | decimal |
| 67 | `f_pay_sub_amt` | decimal |
| 68 | `p_lp_sub_amt` | decimal |
| 69 | `p_sp_sub_amt` | decimal |
| 70 | `f_sub_amt` | decimal |
| 71 | `p_promo_sub_amt` | decimal |
| 72 | `f_promo_sub_amt` | decimal |
| 73 | `p_pointpay_sub_amt` | decimal |
| 74 | `f_pointpay_sub_amt` | decimal |
| 75 | `f_balancepay_sub_amt` | decimal |
| 76 | `p_balancepay_sub_amt` | decimal |
| 77 | `f_cashpay_sub_amt` | decimal |
| 78 | `p_cashpay_sub_amt` | decimal |
| 79 | `p_change_sub_amt` | decimal |
| 80 | `sales_amt` | decimal |
| 81 | `discount_amt` | decimal |
| 82 | `vip_discount_amt` | decimal |
| 83 | `hour_discount_amt` | decimal |
| 84 | `return_sale_qty` | decimal |
| 85 | `return_sale_amt` | decimal |
| 86 | `member_hour_sales_amt` | decimal |
| 87 | `af19_sales_amt` | decimal |
| 88 | `af19_sales_qty` | decimal |
| 89 | `shop_promo_sub_amt` | decimal |
| 90 | `promotion_amt` | decimal |
| 91 | `gift_qty` | decimal |
| 92 | `i_promotion_amt` | decimal |
| 93 | `order_promotion_amt` | decimal |
| 94 | `ordercoupon_promotion_amt` | decimal |
| 95 | `actual_weight` | decimal |
| 96 | `promotion_cost` | decimal |
| 97 | `promotion_amt_shop` | decimal |
| 98 | `promotion_amt_platform` | decimal |
| 99 | `actual_amount` | decimal |
| 100 | `gmv` | decimal |
| 101 | `gmv1` | decimal |
| 102 | `jielong_flag` | varchar |
| 103 | `gift_gmv` | decimal |
| 104 | `postage_shop` | decimal |
| 105 | `postage_platform` | decimal |
| 106 | `logistics_status` | varchar |
| 107 | `courier_company` | varchar |
| 108 | `courier_name` | varchar |
| 109 | `courier_name_reverse` | varchar |
| 110 | `courier_phone` | varchar |
| 111 | `promotion_amt_platform_gs` | decimal |
| 112 | `promotion_amt_platform_gys` | decimal |
| 113 | `business_source` | varchar |
| 114 | `activity_code` | varchar |
| 115 | `p_mp_sub_amt` | decimal |
| 116 | `store_paylevel_discount` | decimal |
| 117 | `company_paylevel_discount` | decimal |
| 118 | `day_clear` | varchar |
| 119 | `inc_day` | varchar |

### ② 进货验收 `hive.dsl.dsl_transaction_non_daily_store_article_purchase_di`

16 字段。⚠ 有两个日期列：`inc_day`（分区键）和 `business_date`（业务日期）。

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `business_date` | varchar | 业务日期 |
| 2 | `store_id` | varchar | 门店 |
| 3 | `day_clear` | varchar | 日清标记 |
| 4 | `article_id` | varchar | 进货商品（parent） |
| 5 | `article_name` | varchar | 进货商品名称 |
| 6 | `sale_article_id` | varchar | 销售商品（sub） |
| 7 | `sale_article_name` | varchar | 销售商品名称 |
| 8 | `sale_article_qty` | decimal | 拆分后销售量 |
| 9 | `sale_article_purchase_amt` | decimal | 拆分后进货额 |
| 10 | `init_stock_qty` | decimal | 期初库存量 |
| 11 | `end_stock_qty` | decimal | 期末库存量 |
| 12 | `init_stock_amt` | decimal | 期初库存额 |
| 13 | `end_stock_amt` | decimal | 期末库存额 |
| 14 | `inventory_cost` | decimal | 库存成本 |
| 15 | `avg_inbound_price` | decimal | 平均进货价 |
| 16 | `inc_day` | varchar | 分区键 |

### ③ SAP 出入库 `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di`

54 字段。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `business_date` | varchar |
| 2 | `new_dc_id` | varchar |
| 3 | `store_id` | varchar |
| 4 | `article_id` | varchar |
| 5 | `out_stock_amt_cb_notax` | decimal |
| 6 | `out_stock_pay_amt` | decimal |
| 7 | `return_stock_pay_amt` | decimal |
| 8 | `return_stock_amt_cb_notax` | decimal |
| 9 | `original_outstock_qty` | decimal |
| 10 | `original_outstock_amt` | decimal |
| 11 | `promotion_outstock_price` | decimal |
| 12 | `promotion_outstock_qty` | decimal |
| 13 | `promotion_outstock_amt` | decimal |
| 14 | `gift_outstock_qty` | decimal |
| 15 | `total_outstock_qty` | decimal |
| 16 | `scm_promotion_cost` | decimal |
| 17 | `scm_return_promotion_cost` | decimal |
| 18 | `return_stock_qty` | decimal |
| 19 | `out_stock_zzckj_amt` | decimal |
| 20 | `return_stock_original_amt` | decimal |
| 21 | `store_order_qty` | decimal |
| 22 | `order_amt` | decimal |
| 23 | `order_qty_payean` | decimal |
| 24 | `adjustment_amt` | decimal |
| 25 | `adjustment_amt_notax` | decimal |
| 26 | `scm_promotion_qty_gift` | decimal |
| 27 | `scm_promotion_amt_gift` | decimal |
| 28 | `scm_promotion_amt` | decimal |
| 29 | `scm_bear_amt` | decimal |
| 30 | `vendor_bear_amt` | decimal |
| 31 | `business_market_bear_amt` | decimal |
| 32 | `business_bear_amt` | decimal |
| 33 | `market_bear_amt` | decimal |
| 34 | `scm_promotion_amt_total` | decimal |
| 35 | `miss_stock_qty` | decimal |
| 36 | `miss_stock_amt` | decimal |
| 37 | `out_stock_pay_amt_notax` | decimal |
| 38 | `return_stock_pay_amt_notax` | decimal |
| 39 | `out_stock_amt_cb` | decimal |
| 40 | `return_stock_amt_cb` | decimal |
| 41 | `vender_bear_gift_amt` | decimal |
| 42 | `scm_bear_gift_amt` | decimal |
| 43 | `store_return_amt_shop` | decimal |
| 44 | `store_return_qty_shop` | decimal |
| 45 | `qdm_bear_negative_amt_total` | decimal |
| 46 | `qdm_bear_positive_amt_total` | decimal |
| 47 | `qdm_bear_gift_qty` | decimal |
| 48 | `qdm_bear_gift_amt` | decimal |
| 49 | `qdm_bear_nogift_negative_amt` | decimal |
| 50 | `qdm_bear_nogift_positive_amt` | decimal |
| 51 | `qdm_bear_promotion_fee` | decimal |
| 52 | `vender_bear_gift_qty` | decimal |
| 53 | `scm_bear_gift_qty` | decimal |
| 54 | `inc_day` | varchar |

### ④ SCM 差异调整 `hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di`

10 字段。目前为骨架表（空数据）。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `business_date` | date |
| 2 | `store_id` | varchar |
| 3 | `dc_id` | varchar |
| 4 | `matnr` | varchar |
| 5 | `article_id` | varchar |
| 6 | `tax` | decimal |
| 7 | `adjustment_amt` | decimal |
| 8 | `adjustment_amt_notax` | decimal |
| 9 | `new_sp_store_id` | varchar |
| 10 | `inc_day` | varchar |

### ⑤ 损耗 `hive.dal.dal_transaction_store_article_lost_di`

10 字段。

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `store_id` | varchar | 门店 |
| 2 | `article_id` | varchar | 商品 |
| 3 | `article_name` | varchar | 商品名称 |
| 4 | `category_level1_id` | varchar | 一级分类 |
| 5 | `category_level1_description` | varchar | 一级分类名 |
| 6 | `unknow_lost_qty` | decimal | 未知损耗量 |
| 7 | `unknow_lost_amt` | decimal | 未知损耗额 |
| 8 | `know_lost_qty` | decimal | 已知损耗量 |
| 9 | `know_lost_amt` | decimal | 已知损耗额 |
| 10 | `inc_day` | varchar | 分区键 |

### ⑥ 加工转换 `hive.dsl.dsl_transaction_sotre_article_compose_info_di`

11 字段。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `business_date` | varchar |
| 2 | `store_id` | varchar |
| 3 | `store_name` | varchar |
| 4 | `article_id` | varchar |
| 5 | `article_name` | varchar |
| 6 | `compose_in_qty` | decimal |
| 7 | `compose_in_amt` | decimal |
| 8 | `compose_out_qty` | decimal |
| 9 | `compose_out_amt` | decimal |
| 10 | `update_time` | datetime |
| 11 | `inc_day` | varchar |

### ⑦ 活动让利 `hive.dal.dal_activity_article_order_sale_info_di`

76 字段。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `business_date` | varchar |
| 2 | `store_id` | varchar |
| 3 | `order_article_id` | varchar |
| 4 | `activity_name` | varchar |
| 5 | `out_price` | varchar |
| 6 | `sale_price` | varchar |
| 7 | `hot_flag` | varchar |
| 8 | `order_price` | decimal |
| 9 | `order_qty` | decimal |
| 10 | `settle_order_qty` | decimal |
| 11 | `order_amt` | decimal |
| 12 | `split_order_qty` | decimal |
| 13 | `split_order_amt` | decimal |
| 14 | `receive_article_id` | varchar |
| 15 | `receive_amt` | decimal |
| 16 | `receive_qty` | decimal |
| 17 | `purchase_price` | decimal |
| 18 | `sale_article_id` | varchar |
| 19 | `sale_article_receive_qty` | decimal |
| 20 | `sale_article_receive_amt` | decimal |
| 21 | `sale_article_receive_price` | decimal |
| 22 | `sum_sub_article_qty` | decimal |
| 23 | `split_qty` | decimal |
| 24 | `qty` | decimal |
| 25 | `qty_spec` | decimal |
| 26 | `split_qty_spec` | decimal |
| 27 | `sale_amt` | decimal |
| 28 | `split_sale_amt` | decimal |
| 29 | `lost_qty` | decimal |
| 30 | `lost_amt` | decimal |
| 31 | `profit_amt` | decimal |
| 32 | `allowance_profit_amt` | decimal |
| 33 | `split_discount_amt` | decimal |
| 34 | `split_member_discount_amt` | decimal |
| 35 | `split_hour_discount_amt` | decimal |
| 36 | `split_return_sale_qty` | decimal |
| 37 | `split_return_sale_amt` | decimal |
| 38 | `split_member_hour_sale_amt` | decimal |
| 39 | `split_af19_sale_amt` | decimal |
| 40 | `split_af19_sale_qty` | decimal |
| 41 | `split_bf9_sale_qty` | decimal |
| 42 | `split_bf10_sale_qty` | decimal |
| 43 | `split_bf12_sale_qty` | decimal |
| 44 | `split_bf16_sale_qty` | decimal |
| 45 | `split_bf19_sale_qty` | decimal |
| 46 | `split_bf9_sale_amt` | decimal |
| 47 | `split_bf10_sale_amt` | decimal |
| 48 | `split_bf12_sale_amt` | decimal |
| 49 | `split_bf16_sale_amt` | decimal |
| 50 | `split_bf19_sale_amt` | decimal |
| 51 | `split_p_lp_sub_amt` | decimal |
| 52 | `split_p_sp_sub_amt` | decimal |
| 53 | `split_promotion_discount_amt` | decimal |
| 54 | `split_allowance_amt` | decimal |
| 55 | `activity_type` | varchar |
| 56 | `activity_id` | varchar |
| 57 | `order_weight` | decimal |
| 58 | `spilt_receive_weight` | decimal |
| 59 | `split_bf9_sale_weight` | decimal |
| 60 | `split_sale_weight` | decimal |
| 61 | `split_cust_num` | decimal |
| 62 | `split_bf19_cust_num` | decimal |
| 63 | `split_receive_unit_qty_spec` | decimal |
| 64 | `split_receive_unit_order_qty` | decimal |
| 65 | `sale_article_receive_unit_qty` | decimal |
| 66 | `last_pay_at` | varchar |
| 67 | `list_price` | decimal |
| 68 | `init_stock_qty` | decimal |
| 69 | `end_stock_qty` | decimal |
| 70 | `init_stock_amt` | decimal |
| 71 | `end_stock_amt` | decimal |
| 72 | `init_receiveb_qty` | decimal |
| 73 | `init_receiveb_amt` | decimal |
| 74 | `end_receiveb_qty` | decimal |
| 75 | `end_receiveb_amt` | decimal |
| 76 | `inc_day` | varchar |

### ⑧ 促销 `hive.dsl.dsl_promotion_order_item_article_sale_info_di`

90 字段。⚠ 源表用 `shop_id`(非 store_id) 和 `sku_code`(非 article_id)。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `order_id` | varchar |
| 2 | `order_type` | varchar |
| 3 | `order_status` | varchar |
| 4 | `customer_id` | varchar |
| 5 | `delivery_id` | varchar |
| 6 | `customer_name` | varchar |
| 7 | `customer_phone` | varchar |
| 8 | `shop_id` | varchar |
| 9 | `shop_name` | varchar |
| 10 | `parent_order_id` | varchar |
| 11 | `product_count` | decimal |
| 12 | `channel_id` | varchar |
| 13 | `order_sub_type` | varchar |
| 14 | `outer_order_id` | varchar |
| 15 | `outer_order_type` | varchar |
| 16 | `refund_type` | varchar |
| 17 | `je_date` | varchar |
| 18 | `rje_date` | varchar |
| 19 | `refund_at` | varchar |
| 20 | `pay_at` | varchar |
| 21 | `order_at` | varchar |
| 22 | `cancel_at` | varchar |
| 23 | `business_source` | int |
| 24 | `row_num` | varchar |
| 25 | `goods_id` | varchar |
| 26 | `sku_code` | varchar |
| 27 | `goods_name` | varchar |
| 28 | `category_id` | varchar |
| 29 | `spu_code` | varchar |
| 30 | `bundle_promo_code` | varchar |
| 31 | `order_item_id` | varchar |
| 32 | `p_promo_amt` | decimal |
| 33 | `p_promo_total_amt` | decimal |
| 34 | `f_promo_amt` | decimal |
| 35 | `f_promo_total_amt` | decimal |
| 36 | `promotion_category` | varchar |
| 37 | `promotion_code` | varchar |
| 38 | `promotion_code2` | varchar |
| 39 | `promo_type` | varchar |
| 40 | `promo_sub_type` | varchar |
| 41 | `coupon_code` | varchar |
| 42 | `from_outer` | varchar |
| 43 | `outer_code` | varchar |
| 44 | `parent_order_item_id` | varchar |
| 45 | `parent_order_item_promotion_id` | varchar |
| 46 | `parent_bom_order_item_id` | varchar |
| 47 | `parent_bom_order_item_promo_id` | varchar |
| 48 | `cost_center` | varchar |
| 49 | `coupon_mode` | varchar |
| 50 | `sales_charge_type` | varchar |
| 51 | `cost_tax_rate` | decimal |
| 52 | `allocate_rate` | decimal |
| 53 | `promotion_cost` | decimal |
| 54 | `promo_action_type` | varchar |
| 55 | `purchase_limit_qty` | decimal |
| 56 | `promotion_name` | varchar |
| 57 | `activity_type` | varchar |
| 58 | `activity_level` | varchar |
| 59 | `code2` | varchar |
| 60 | `rank` | varchar |
| 61 | `promo_type1` | varchar |
| 62 | `promo_sub_type1` | varchar |
| 63 | `promo_action` | varchar |
| 64 | `eligibility_condition` | varchar |
| 65 | `promo_condition_type` | varchar |
| 66 | `promo_condition_context` | varchar |
| 67 | `promo_action_context` | varchar |
| 68 | `name` | varchar |
| 69 | `description` | varchar |
| 70 | `tag` | varchar |
| 71 | `cost_center_info` | varchar |
| 72 | `available_category` | varchar |
| 73 | `allowance` | varchar |
| 74 | `only_member` | varchar |
| 75 | `title` | varchar |
| 76 | `discount` | decimal |
| 77 | `created_by` | varchar |
| 78 | `promotion_type` | varchar |
| 79 | `category_info` | varchar |
| 80 | `source` | varchar |
| 81 | `online_flag` | varchar |
| 82 | `normal_inc_day` | varchar |
| 83 | `inc_time` | varchar |
| 84 | `jielong_flag` | varchar |
| 85 | `activity_code` | varchar |
| 86 | `promo_ext_prop` | varchar |
| 87 | `cost_company` | varchar |
| 88 | `cost_subject` | varchar |
| 89 | `is_hour_promotion` | varchar |
| 90 | `inc_day` | varchar |

### ⑨ 成本价池 `hive.ods_sc_db.t_shop_inventory_sku_pool`

18 字段。⚠ 源表用 `shop_id`、`sku_code`、`inventory_date`。

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `id` | bigint | 物理主键 |
| 2 | `shop_id` | varchar | 门店编号 |
| 3 | `inventory_date` | varchar | 盘点日期 |
| 4 | `sku_code` | varchar | 商品编号 |
| 5 | `sku_name` | varchar | 商品名称 |
| 6 | `sub_category_id` | varchar | 小分类编码 |
| 7 | `sub_category_name` | varchar | 小分类名称 |
| 8 | `spec` | varchar | 规格 |
| 9 | `sales_unit` | varchar | 销售单位 |
| 10 | `main_img` | varchar | 商品主图 |
| 11 | `gift_flag` | int | 赠品标识 |
| 12 | `cost_price` | decimal(32,4) | ⭐ 成本价 |
| 13 | `created_at` | varchar | 创建时间 |
| 14 | `created_by` | varchar | 创建人 |
| 15 | `updated_at` | varchar | 更新时间 |
| 16 | `updated_by` | varchar | 更新人 |
| 17 | `last_updated_at` | varchar | 最后更新时间 |
| 18 | `inc_day` | varchar | 分区键 |

### ⑩ 价格 `hive.dim.dim_store_article_price_info_da`

54 字段。

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `id` | bigint | 物理主键 |
| 2 | `shop_id` | varchar | 门店 |
| 3 | `tenant_id` | varchar | 租户 |
| 4 | `shop_name` | varchar | 门店名称 |
| 5 | `dc_code` | varchar | 仓库编号 |
| 6 | `dc_name` | varchar | 仓库名称 |
| 7 | `category_code` | varchar | 分类编码 |
| 8 | `category_name` | varchar | 分类名称 |
| 9 | `sku_code` | varchar | 商品编号 |
| 10 | `sku_name` | varchar | 商品名称 |
| 11 | `current_price` | decimal | 今日售价 |
| 12 | `yesterday_price` | decimal | 昨日售价 |
| 13 | `strategy_no` | varchar | 策略编号 |
| 14 | `promotion_no` | varchar | 促销编号 |
| 15 | `calc_strategy` | varchar | 计算策略 |
| 16 | `dc_original_price` | decimal | 出库原价 |
| 17 | `dc_price` | decimal | 出库价格 |
| 18 | `anchor_sale_price` | decimal | 锚定售价 |
| 19 | `deal_status` | varchar | 处理状态 |
| 20 | `exception_type` | varchar | 异常类型 |
| 21 | `exception_reason` | varchar | 异常原因 |
| 22 | `monitor_type` | varchar | 监控类型 |
| 23 | `monitor_desc` | varchar | 监控描述 |
| 24 | `calc_status` | varchar | 计算状态 |
| 25 | `confirm_status` | varchar | 确认状态 |
| 26 | `sale_status` | varchar | 销售状态 |
| 27 | `lock_status` | int | 锁定状态 |
| 28 | `push_status` | varchar | 推送状态 |
| 29 | `confirm_by` | varchar | 确认人 |
| 30 | `calc_by` | varchar | 计算人 |
| 31 | `calc_effect_at` | varchar | 生效时间 |
| 32 | `source` | varchar | 来源 |
| 33 | `shop_sku` | varchar | 门店SKU |
| 34 | `sales_unit` | varchar | 销售单位 |
| 35 | `yesterday_dc_price` | decimal | 昨日出库价 |
| 36 | `created_at` | varchar | 创建时间 |
| 37 | `created_by` | varchar | 创建人 |
| 38 | `updated_at` | varchar | 更新时间 |
| 39 | `updated_by` | varchar | 更新人 |
| 40 | `last_updated_at` | varchar | 最后更新 |
| 41 | `shop_id_sku_code_calc_at` | varchar | 联合键 |
| 42 | `out_updated_at` | bigint | 外部更新时间 |
| 43 | `original_price` | decimal | ⭐ 销售原价 |
| 44 | `effective` | int | 是否生效 |
| 45 | `unadjust_sale_price` | decimal | 调整前价格 |
| 46 | `is_new` | int | 是否新品 |
| 47 | `original_dc_price` | decimal | 原始出库价 |
| 48 | `dc_original_price_sap` | decimal | SAP出库原价 |
| 49 | `sales_mode` | varchar | 销售模式 |
| 50 | `outstock_addprice_amt` | decimal | 出库加价额 |
| 51 | `outstock_addprice_rate` | decimal | 出库加价率 |
| 52 | `outstock_profit_rate` | decimal | 出库毛利率 |
| 53 | `outstock_lock_price` | decimal | 出库锁定价 |
| 54 | `inc_day` | varchar | 分区键 |

### ⑫ 日清商品清单 `hive.dim.dim_day_clear_article_list_di`

11 字段。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `business_date` | varchar |
| 2 | `store_id` | varchar |
| 3 | `article_id` | varchar |
| 4 | `article_name` | varchar |
| 5 | `category_level3_id` | varchar |
| 6 | `category_level3_description` | varchar |
| 7 | `category_level2_id` | varchar |
| 8 | `category_level2_description` | varchar |
| 9 | `category_level1_id` | varchar |
| 10 | `category_level1_description` | varchar |
| 11 | `inc_day` | varchar |

### ⑬ 门店画像 `hive.dim.dim_store_profile`

109 字段。⚠ 源表用 `sp_store_id`(非 store_id)。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `sp_store_id` | varchar |
| 2 | `sp_store_name` | varchar |
| 3 | `sp_type` | varchar |
| 4 | `sp_level` | varchar |
| 5 | `dist_id` | varchar |
| 6 | `dist_description` | varchar |
| 7 | `city_id` | varchar |
| 8 | `city_description` | varchar |
| 9 | `pro_id` | varchar |
| 10 | `pro_description` | varchar |
| 11 | `area_id` | varchar |
| 12 | `area_description` | varchar |
| 13 | `sp_purchasing_center_id` | varchar |
| 14 | `sp_company_id` | varchar |
| 15 | `sp_purchasing_area` | varchar |
| 16 | `sp_store_status` | int |
| 17 | `sp_store_effective_date` | varchar |
| 18 | `open_days` | varchar |
| 19 | `sp_phone1` | varchar |
| 20 | `sp_phone2` | varchar |
| 21 | `sp_phone3` | varchar |
| 22 | `sp_address` | varchar |
| 23 | `eblc_longitude` | varchar |
| 24 | `eblc_latitude` | varchar |
| 25 | `sp_origin_start_date` | varchar |
| 26 | `sp_final_end_date` | varchar |
| 27 | `group_manager_code` | varchar |
| 28 | `group_manager` | varchar |
| 29 | `sp_scale` | varchar |
| 30 | `sp_rsv_status` | varchar |
| 31 | `sp_sign_date` | varchar |
| 32 | `sp_table_flag` | varchar |
| 33 | `sp_closed_reason` | varchar |
| 34 | `sp_currency` | varchar |
| 35 | `sp_master_area` | varchar |
| 36 | `op_area_id` | varchar |
| 37 | `total_area` | decimal |
| 38 | `area_description_alias` | varchar |
| 39 | `new_sp_store_id` | varchar |
| 40 | `new_sp_store_name` | varchar |
| 41 | `mandt` | varchar |
| 42 | `zone_manager` | varchar |
| 43 | `zone_id` | varchar |
| 44 | `opera_manager` | varchar |
| 45 | `opera_id` | varchar |
| 46 | `new_sp_level` | varchar |
| 47 | `group_manager_tel` | varchar |
| 48 | `transfer_date` | varchar |
| 49 | `transfer_store_id` | varchar |
| 50 | `stop_start_date` | varchar |
| 51 | `stop_end_date` | varchar |
| 52 | `stop_reason_id` | varchar |
| 53 | `stop_reason` | varchar |
| 54 | `restart_date` | varchar |
| 55 | `business_area` | varchar |
| 56 | `area_id_sap` | varchar |
| 57 | `area_id_purchase` | varchar |
| 58 | `operate_id_purchase` | varchar |
| 59 | `area2_id` | varchar |
| 60 | `area2_name` | varchar |
| 61 | `area2_id_sap` | varchar |
| 62 | `operate_name_purchase` | varchar |
| 63 | `zone_supper_manager` | varchar |
| 64 | `zone_supper_id` | varchar |
| 65 | `zone_supper_phone` | varchar |
| 66 | `zone_phone` | varchar |
| 67 | `franchisee_id` | varchar |
| 68 | `zzlksrq` | varchar |
| 69 | `zzljsrq` | varchar |
| 70 | `area_type` | varchar |
| 71 | `measuring_area` | decimal |
| 72 | `franchisee_name` | varchar |
| 73 | `stop_reason_apply` | varchar |
| 74 | `old_id` | varchar |
| 75 | `contract_franchisee_id` | varchar |
| 76 | `contract_franchisee_name` | varchar |
| 77 | `contract_franchisee_phone` | varchar |
| 78 | `expand_staff_id` | varchar |
| 79 | `expand_staff_name` | varchar |
| 80 | `new_sp_level_name` | varchar |
| 81 | `target_sales_amt` | decimal |
| 82 | `target_bf19_cust_num` | int |
| 83 | `target_cust_num` | int |
| 84 | `targer_allowance_profit` | decimal |
| 85 | `price_strategy_start_date` | date |
| 86 | `sap_store_status_id` | varchar |
| 87 | `sap_store_status_name` | varchar |
| 88 | `store_type_name` | varchar |
| 89 | `store_flag_name` | varchar |
| 90 | `closed_reason_name` | varchar |
| 91 | `manage_area_id` | varchar |
| 92 | `manage_area_name` | varchar |
| 93 | `region_id` | varchar |
| 94 | `region_name` | varchar |
| 95 | `store_guide_user` | varchar |
| 96 | `sap_area_id` | varchar |
| 97 | `sap_area_name` | varchar |
| 98 | `sap_area2_id` | varchar |
| 99 | `sap_area2_name` | varchar |
| 100 | `bunk_id` | varchar |
| 101 | `store_service_range` | varchar |
| 102 | `store_service_name` | varchar |
| 103 | `mall_supervisor_phone` | varchar |
| 104 | `mall_supervisor_name` | varchar |
| 105 | `zman_id` | varchar |
| 106 | `zman_name` | varchar |
| 107 | `original_store_id` | varchar |
| 108 | `inc_day` | varchar |
| 109 | `pt` | varchar |

### ⑭ 可售商品 `hive.ods_sc_db.t_purchase_order_item_tmp`

47 字段。⚠ 源表用 `shop_id`、`sku_code`。

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `id` | bigint | 主键 |
| 2 | `tenant_id` | varchar | 租户 |
| 3 | `shop_id` | varchar | 门店编号 |
| 4 | `shop_name` | varchar | 门店名称 |
| 5 | `order_at` | varchar | 下单时间 |
| 6 | `category_code` | varchar | 分类编码 |
| 7 | `category_name` | varchar | 分类名称 |
| 8 | `mid_category_code` | varchar | 中分类编码 |
| 9 | `mid_category_name` | varchar | 中分类名称 |
| 10 | `sub_category_code` | varchar | 小分类编码 |
| 11 | `sub_category_name` | varchar | 小分类名称 |
| 12 | `sku_code` | varchar | 商品编号 |
| 13 | `sku_name` | varchar | 商品名称 |
| 14 | `sell_unit` | varchar | 销售单位 |
| 15 | `pur_unit` | varchar | 采购单位 |
| 16 | `article_spec` | varchar | 规格 |
| 17 | `old_spec` | varchar | 旧规格 |
| 18 | `spec_update_date` | varchar | 规格更新日 |
| 19 | `purchase_flag` | int | 采购标识 |
| 20 | `basic_qty` | decimal | 基础数量 |
| 21 | `min_batch_qty` | decimal | 最小批量 |
| 22 | `max_batch_qty` | decimal | 最大批量 |
| 23 | `propose_qty` | decimal | 建议数量 |
| 24 | `purchase_price` | decimal | 采购价 |
| 25 | `is_special_price` | int | 是否特价 |
| 26 | `order_qty` | decimal | 订货量 |
| 27 | `order_amount` | decimal | 订货额 |
| 28 | `order_weight` | decimal | 订货重量 |
| 29 | `conver_rate` | decimal | 转换率 |
| 30 | `only_morning` | int | 仅早上 |
| 31 | `sku_created_at` | varchar | SKU创建时间 |
| 32 | `core_flag` | int | 核心标识 |
| 33 | `tips_flag` | int | 提示标识 |
| 34 | `seven_days_order_count` | int | 7天订货次数 |
| 35 | `seven_days_order_qty` | decimal | 7天订货量 |
| 36 | `created_at` | varchar | 创建时间 |
| 37 | `created_by` | varchar | 创建人 |
| 38 | `updated_at` | varchar | 更新时间 |
| 39 | `updated_by` | varchar | 更新人 |
| 40 | `last_updated_at` | varchar | 最后更新 |
| 41 | `is_deleted` | int | 删除标记 |
| 42 | `propose_desc` | varchar | 建议描述 |
| 43 | `main_img` | varchar | 商品图 |
| 44 | `allow_single_order` | int | 允许单订 |
| 45 | `sku_tag` | int | SKU标签 |
| 46 | `receive_sku_code` | varchar | 收货SKU |
| 47 | `inc_day` | varchar | 分区键 |

### ⑮ 商品主数据 `hive.dim.dim_goods_information_have_pt`

106 字段。不过滤门店，全量同步。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `article_id` | varchar |
| 2 | `article_name` | varchar |
| 3 | `category_level3_id` | varchar |
| 4 | `category_level3_description` | varchar |
| 5 | `category_level2_id` | varchar |
| 6 | `category_level2_description` | varchar |
| 7 | `category_level1_id` | varchar |
| 8 | `category_level1_description` | varchar |
| 9 | `sales_unit_id` | varchar |
| 10 | `sale_unit` | varchar |
| 11 | `order_unit_id` | varchar |
| 12 | `order_unit` | varchar |
| 13 | `brand` | varchar |
| 14 | `product_address` | varchar |
| 15 | `norms` | varchar |
| 16 | `logistics_process_tag` | varchar |
| 17 | `logistics_scrap_tag` | varchar |
| 18 | `online_tag` | varchar |
| 19 | `private` | varchar |
| 20 | `barcode` | varchar |
| 21 | `min_order_unit` | varchar |
| 22 | `min_order_times` | varchar |
| 23 | `max_order_times` | varchar |
| 24 | `xx_sl` | varchar |
| 25 | `jx_sl` | varchar |
| 26 | `order_frequency` | varchar |
| 27 | `status_code` | varchar |
| 28 | `commodity_attribute` | varchar |
| 29 | `load_time` | varchar |
| 30 | `life_cycle` | varchar |
| 31 | `unit_weight` | decimal |
| 32 | `onlineshop_flag` | varchar |
| 33 | `offlineshop_flag` | varchar |
| 34 | `vegetablebar_flag` | varchar |
| 35 | `package_attribute` | varchar |
| 36 | `type` | varchar |
| 37 | `article_type` | varchar |
| 38 | `weight_flag` | varchar |
| 39 | `commodity_attribute_name` | varchar |
| 40 | `package_attribute_name` | varchar |
| 41 | `status_name` | varchar |
| 42 | `type_name` | varchar |
| 43 | `abi_volume_ratio` | decimal |
| 44 | `abi_purchasecategory_id` | varchar |
| 45 | `abi_srmcategory` | bigint |
| 46 | `abi_quality_days` | bigint |
| 47 | `abi_outer_packing_spec` | varchar |
| 48 | `abi_create_reason` | varchar |
| 49 | `abi_outer_packing_lwh` | varchar |
| 50 | `abi_purchase_group` | varchar |
| 51 | `abi_new_category_id` | varchar |
| 52 | `old_article_id` | varchar |
| 53 | `old_article_name` | varchar |
| 54 | `old_category_level3_id` | varchar |
| 55 | `old_category_level3_description` | varchar |
| 56 | `old_category_level2_id` | varchar |
| 57 | `old_category_level2_description` | varchar |
| 58 | `old_category_level1_id` | varchar |
| 59 | `old_category_level1_description` | varchar |
| 60 | `article_matnr` | varchar |
| 61 | `matnr_unit_id` | varchar |
| 62 | `matnr_unit` | varchar |
| 63 | `zglfz` | decimal |
| 64 | `zglfm` | decimal |
| 65 | `sp_info` | varchar |
| 66 | `mtart` | varchar |
| 67 | `in_date` | varchar |
| 68 | `create_date1` | varchar |
| 69 | `out_date` | varchar |
| 70 | `matnr` | varchar |
| 71 | `abi_purchase_group_name` | varchar |
| 72 | `sp_info_name` | varchar |
| 73 | `brand_id` | varchar |
| 74 | `sale_areas` | varchar |
| 75 | `use_types` | varchar |
| 76 | `resent_use_date` | varchar |
| 77 | `matnr_in_date` | varchar |
| 78 | `purchase_department` | varchar |
| 79 | `purchase_department_id` | varchar |
| 80 | `min_pack_weight` | decimal |
| 81 | `shelf_time` | varchar |
| 82 | `freeze_id` | varchar |
| 83 | `atob_value` | varchar |
| 84 | `atob_name` | varchar |
| 85 | `relation_matnr` | varchar |
| 86 | `matnr_name` | varchar |
| 87 | `article_belong_id` | varchar |
| 88 | `article_belong_name` | varchar |
| 89 | `if_settle_unit` | int |
| 90 | `superior_purchase_department_id` | varchar |
| 91 | `superior_purchase_department_name` | varchar |
| 92 | `category_material_label_id` | varchar |
| 93 | `category_material_label_name` | varchar |
| 94 | `spu_id` | varchar |
| 95 | `spu_name` | varchar |
| 96 | `article_series_id` | varchar |
| 97 | `article_series_name` | varchar |
| 98 | `temperature_layer_id` | varchar |
| 99 | `temperature_layer_name` | varchar |
| 100 | `import_flag` | varchar |
| 101 | `blackwhite_pig_id` | varchar |
| 102 | `blackwhite_pig_name` | varchar |
| 103 | `norms_lower_limit` | decimal |
| 104 | `norms_upper_limit` | decimal |
| 105 | `inc_day` | varchar |
| 106 | `pt` | varchar |

### ⑯ 日历 `hive.dim.dim_calendar`

70 字段。⚠ 用 `day_date`(非 business_date) 过滤。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `day_wid` | varchar |
| 2 | `day_name` | varchar |
| 3 | `day_date_chn` | varchar |
| 4 | `day_date` | varchar |
| 5 | `day_name_of_week` | varchar |
| 6 | `day_of_week` | varchar |
| 7 | `day_of_month` | varchar |
| 8 | `day_of_year` | varchar |
| 9 | `week_wid` | varchar |
| 10 | `week_name` | varchar |
| 11 | `week_no` | varchar |
| 12 | `week_start_date_wid` | varchar |
| 13 | `week_start_date` | varchar |
| 14 | `week_end_date_wid` | varchar |
| 15 | `week_end_date` | varchar |
| 16 | `month_wid` | varchar |
| 17 | `month_name` | varchar |
| 18 | `month_no` | varchar |
| 19 | `month_days` | varchar |
| 20 | `month_start_date_wid` | varchar |
| 21 | `month_start_date` | varchar |
| 22 | `month_end_date_wid` | varchar |
| 23 | `month_end_date` | varchar |
| 24 | `quarter_wid` | varchar |
| 25 | `quarter_name` | varchar |
| 26 | `quarter_no` | varchar |
| 27 | `quarter_start_date_wid` | varchar |
| 28 | `quarter_start_date` | varchar |
| 29 | `quarter_end_date_wid` | varchar |
| 30 | `quarter_end_date` | varchar |
| 31 | `year_wid` | varchar |
| 32 | `year_name` | varchar |
| 33 | `year_start_date_wid` | varchar |
| 34 | `year_start_date` | varchar |
| 35 | `year_end_date_wid` | varchar |
| 36 | `year_end_date` | varchar |
| 37 | `is_last_day_of_week` | varchar |
| 38 | `is_last_day_of_month` | varchar |
| 39 | `is_last_day_of_year` | varchar |
| 40 | `is_weekend` | varchar |
| 41 | `holiday_name` | varchar |
| 42 | `day_ago_date_wid` | varchar |
| 43 | `day_ago_date` | varchar |
| 44 | `week_ago_date_wid` | varchar |
| 45 | `week_ago_date` | varchar |
| 46 | `month_ago_date_wid` | varchar |
| 47 | `month_ago_date` | varchar |
| 48 | `quarter_ago_date_wid` | varchar |
| 49 | `quarter_ago_date` | varchar |
| 50 | `year_ago_date_wid` | varchar |
| 51 | `year_ago_date` | varchar |
| 52 | `language` | varchar |
| 53 | `w_insert_date` | varchar |
| 54 | `w_update_date` | varchar |
| 55 | `is_actual_holiday` | varchar |
| 56 | `actual_holiday_name` | varchar |
| 57 | `is_actual_overwork` | varchar |
| 58 | `is_rest_day` | varchar |
| 59 | `actual_week_no` | varchar |
| 60 | `day54_of_week` | varchar |
| 61 | `week54_wid` | varchar |
| 62 | `week54_name` | varchar |
| 63 | `week54_no` | varchar |
| 64 | `week54_start_date_wid` | varchar |
| 65 | `week54_start_date` | varchar |
| 66 | `week54_end_date_wid` | varchar |
| 67 | `week54_end_date` | varchar |
| 68 | `week_no_name` | varchar |
| 69 | `analysis_week_wid` | varchar |
| 70 | `analysis_week_name` | varchar |

### ⑰ BOM 收货销售 `hive.dal.dal_receive_sale_di`

20 字段。⭐ BOM 分摊核心源表。

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `business_date` | varchar | 业务日期 |
| 2 | `store_id` | varchar | 门店 |
| 3 | `article_id` | varchar | 进货商品(parent) |
| 4 | `article_name` | varchar | 进货商品名称 |
| 5 | `category_level1_id` | varchar | 一级分类 |
| 6 | `category_level1_description` | varchar | 一级分类名 |
| 7 | `inbound_amount` | decimal | 进货金额 |
| 8 | `inbound_qty` | decimal | 进货数量 |
| 9 | `purchase_price` | decimal | 采购价 |
| 10 | `sale_article_id` | varchar | 销售商品(sub) |
| 11 | `sale_article_name` | varchar | 销售商品名称 |
| 12 | `sale_article_qty` | decimal | 拆分销售量 |
| 13 | `spilit_sale_article_amt` | decimal | 拆分销售额 |
| 14 | `sum_sub_article_qty` | decimal | 子商品总进货量 |
| 15 | `sale_article_price` | decimal | 销售商品售价 |
| 16 | `sum_sale_article_qty` | decimal | 总销售量 |
| 17 | `rate` | decimal | 比率 |
| 18 | `sum_article_qty` | decimal | 总进货量 |
| 19 | `sale_recev_rate` | decimal | 销收比 |
| 20 | `inc_day` | varchar | 分区键 |

### ⑱ 订验关系 `hive.dal.dal_store_order_receive_di`

17 字段。目前为骨架表（空数据）。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `business_date` | varchar |
| 2 | `store_id` | varchar |
| 3 | `order_article_id` | varchar |
| 4 | `order_article_name` | varchar |
| 5 | `order_category_level1_id` | varchar |
| 6 | `order_category_level1_description` | varchar |
| 7 | `re_article_id` | varchar |
| 8 | `re_article_name` | varchar |
| 9 | `re_category_level1_id` | varchar |
| 10 | `re_category_level1_description` | varchar |
| 11 | `order_qty` | decimal |
| 12 | `order_amt` | decimal |
| 13 | `rate` | decimal |
| 14 | `type` | varchar |
| 15 | `re_order_qty` | decimal |
| 16 | `re_order_amt` | decimal |
| 17 | `inc_day` | varchar |

### ⑲ 单位转换 `hive.dim.dim_store_article_convert_info_da`

9 字段。目前为骨架表（空数据）。

| # | 字段 | 类型 |
|---|---:|---|
| 1 | `store_id` | varchar |
| 2 | `parent_article_id` | varchar |
| 3 | `parent_article_name` | varchar |
| 4 | `sub_article_id` | varchar |
| 5 | `sub_article_name` | varchar |
| 6 | `parent_rate` | decimal |
| 7 | `sub_rate` | decimal |
| 8 | `ctype` | int |
| 9 | `inc_day` | varchar |

### ㉑ 库存明细 `hive.ddl.ddl_transaction_store_article_inventory_detail_di`

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `id` | bigint | 物理主键 |
| 2 | `shop_id` | varchar | 门店编号 |
| 3 | `inventory_date` | varchar | 盘点日期（业务日期） |
| 4 | `sku_code` | varchar | 商品编号 |
| 5 | `sku_name` | varchar | 商品名称 |
| 6 | `sub_category_id` | varchar | 小分类编码 |
| 7 | `sub_category_name` | varchar | 小分类名称 |
| 8 | `spec` | varchar | 规格 |
| 9 | `sales_unit` | varchar | 销售单位 |
| 10 | `main_img` | varchar | 商品主图 |
| 11 | `sale_stock_qty` | decimal(32,4) | **系统库存数量** |
| 12 | `stock_cost` | decimal(32,4) | **库存总成本** |
| 13 | `actual_stock_qty` | decimal(32,4) | **实盘库存数量** |
| 14 | `profit_loss_qty` | decimal(32,4) | **盈亏数量** = actual - sale |
| 15 | `gift_flag` | int | 赠品标识 |
| 16 | `created_at` | varchar | 创建时间 |
| 17 | `created_by` | varchar | 创建人 |
| 18 | `updated_at` | varchar | 更新时间 |
| 19 | `updated_by` | varchar | 更新人 |
| 20 | `last_updated_at` | varchar | 最后更新时间 |
| 21 | `inc_day` | varchar | 日分区 |

### ⑳ BOM 关系边 `hive.dim.dim_store_article_bom_relation`

17 字段。⭐ BOM 结构定义表（降级为观测）。

| # | 字段 | 类型 | 说明 |
|---|---:|---|---|
| 1 | `store_id` | varchar | 门店 |
| 2 | `category_level3_id` | varchar | 三级分类 |
| 3 | `category_level3_description` | varchar | 三级分类名 |
| 4 | `category_level2_id` | varchar | 二级分类 |
| 5 | `category_level2_description` | varchar | 二级分类名 |
| 6 | `category_level1_id` | varchar | 一级分类 |
| 7 | `category_level1_description` | varchar | 一级分类名 |
| 8 | `parent_article_id` | varchar | 父商品 |
| 9 | `parent_article_unit` | varchar | 父商品单位 |
| 10 | `sub_article_id` | varchar | 子商品 |
| 11 | `sub_article_unit` | varchar | 子商品单位 |
| 12 | `dressing_rate` | decimal | 出成率 |
| 13 | `cost_rate` | decimal | 成本率 |
| 14 | `bom_type` | int | BOM类型 |
| 15 | `split_mode` | varchar | 拆分模式 |
| 16 | `sp_level` | int | 层级 |
| 17 | `inc_day` | varchar | 分区键 |

---

## 附录B：完整表序号速查（三列对照）

> 一行看清：Hive源表 → strategy_fm → DuckDB

| 序号 | Hive 源表 | strategy_fm 表 | DuckDB 表 | 域 | 字段数 |
|---:|---|---|---|---:|---:|
| 1 | `hive.dsl.dsl_transaction_non_daily_store_order_details_di` | `strategy_fm_sales_di` | `atomic_sales` | ①销售 | 119 |
| 2 | `hive.dsl.dsl_transaction_non_daily_store_article_purchase_di` | `strategy_fm_purchase_di` | `atomic_inventory` | ②库存 | 16 |
| 3 | `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di` | `strategy_fm_scm_di` | `atomic_scm` | ③供应链 | 54 |
| 4 | `hive.dal_bi_rpt.dal_debit_store_dc_difference_adjustment_di` | `strategy_fm_scm_adjust_di` | `atomic_scm_adjust` | ③附 调整 | 10 |
| 5 | `hive.dal.dal_transaction_store_article_lost_di` | `strategy_fm_loss_di` | `atomic_loss` | ④损耗 | 10 |
| 6 | `hive.dsl.dsl_transaction_sotre_article_compose_info_di` | `strategy_fm_compose_di` | `atomic_compose` | ⑤加工 | 11 |
| 7 | `hive.dal.dal_activity_article_order_sale_info_di` | `strategy_fm_allowance_di` | `atomic_allowance` | ⑥补贴 | 76 |
| 8 | `hive.dsl.dsl_promotion_order_item_article_sale_info_di` | `strategy_fm_promo_di` | `atomic_promo` | ⑦促销 | 90 |
| 9 | `hive.ods_sc_db.t_shop_inventory_sku_pool` | `strategy_fm_inventory_pool_di` | `atomic_cost_price` | ⑧成本价 | 18 |
| 10 | `hive.dim.dim_store_article_price_info_da` | `strategy_fm_price_da` | `atomic_price` | ⑨价格 | 54 |
| 11 | `hive.dim.dim_chdj_store_list_di` | `strategy_fm_dim_store_list` | `dim_store_list` | 门店白名单 | — |
| 12 | `hive.dim.dim_day_clear_article_list_di` | `strategy_fm_dim_day_clear` | `dim_day_clear` | 日清标签 | 11 |
| 13 | `hive.dim.dim_store_profile` | `strategy_fm_dim_store_profile` | `dim_store_profile` | 门店画像 | 109 |
| 14 | `hive.ods_sc_db.t_purchase_order_item_tmp` | `strategy_fm_dim_saleable` | `dim_saleable` | 可售商品 | 47 |
| 15 | `hive.dim.dim_goods_information_have_pt` | `strategy_fm_dim_goods` | `dim_goods` | 商品主数据 | 106 |
| 16 | `hive.dim.dim_calendar` | `strategy_fm_dim_calendar` | `dim_calendar` | 日历 | 70 |
| 17 | `hive.dal.dal_receive_sale_di` | `strategy_fm_receive_sale_di` | `atomic_receive_sale` | ⭐BOM事实 | 20 |
| 18 | `hive.dal.dal_store_order_receive_di` | `strategy_fm_order_receive_di` | `atomic_order_receive` | 订验(空) | 17 |
| 19 | `hive.dim.dim_store_article_convert_info_da` | `strategy_fm_dim_article_convert` | `atomic_article_convert` | 单位换算 | 9 |
| 20 | `hive.dim.dim_store_article_bom_relation` | `strategy_dim_store_article_bom_relation` | `atomic_bom_relation` | ⭐BOM关系 | 17 |
| 21 | `hive.ddl.ddl_transaction_store_article_inventory_detail_di` | `strategy_fm_store_article_inventory_detail_di` | `atomic_inventory_detail` | 库存明细 | 21 |

*生成时间：2026-04-24*
*版本：v5.0 完整版*
*查询来源：QDM BI API + Hive 源表注释*