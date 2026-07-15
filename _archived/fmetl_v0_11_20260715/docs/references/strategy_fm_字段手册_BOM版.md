# strategy_fm_* 字段手册（BOM毛利计算专用版）

> 本手册重点梳理与 **BOM 分摊 → 销售成本 → 门店毛利** 计算链路相关的字段逻辑。
> 版本：v4.0（2026-04-24）

---

## 一、核心业务概念

### 1.1 商品编码的三种视角

| 视角 | 字段名 | 含义 | 示例场景 |
|---|---|---|---|
| **验收条码** | `article_id` | 进货时扫码登记的商品编码 | 整头猪、白条肉、大包装原料 |
| **销售条码** | `sale_article_id` | POS 销售时扫码的商品编码 | 排骨、五花肉、梅头肉、散称零售 |
| **订购条码** | `order_article_id` | 订货系统里下单的商品编码 | 可能与验收或销售一致，也可能不同 |

**BOM 关系本质**：一个验收条码（parent）拆分成多个销售条码（sub）。

### 1.2 day_clear 字段语义

| 值 | 含义 | 业务场景 |
|---|---|---|
| `'0'` | 日清 | 生鲜类，当日必须清空，隔夜报废 |
| `'1'` | 非日清 | 标品类，可跨日留存 |
| `'2'` | 合计 | **ETL UNION 生成**，`= 0 + 1 汇总** |

⚠ **重要**：查询"全天合计"必须用 `WHERE day_clear = '2'`。按 `day_clear` 分组求 `SUM` 会天然翻倍（设计如此，非 bug）。

### 1.3 门店毛利额双口径

| 口径 | 公式 | 适用场景 |
|---|---|---|
| **销售方程** | `sale_amt - sale_qty × effective_unit_cost - losses` | 推荐口径，直接扣减销售成本 |
| **库存方程** | `sale_amt - (receive + compose_in - compose_out) + (end - init) - losses` | 验证口径，用于核对库存变动 |

---

## 二、BOM 分摊核心表（P0）

### 2.1 表序号对照表

| 序号 | StarRocks 目标表 | Hive 源表 | 用途 |
|---|---|---|---|
| 2 | `strategy_fm_purchase_di` | `dsl_transaction_non_daily_store_article_purchase_di` | 进货验收事实 |
| 6 | `strategy_fm_compose_di` | `dsl_transaction_sotre_article_compose_info_di` | 加工转换事实 |
| 17 | `strategy_fm_receive_sale_di` | `dal_receive_sale_di` | **BOM 分摊事实主源** |
| 19 | `strategy_fm_dim_article_convert` | `dim_store_article_convert_info_da` | 单位换算系数 |
| 20 | `strategy_dim_store_article_bom_relation` | `dim_store_article_bom_relation` | **BOM 关系边定义** |

---

## 三、表 2：进货验收 `strategy_fm_purchase_di`

### 3.1 字段详解

| 字段 | 类型 | 含义 | 计算用途 |
|---|---|---|---|
| `business_date` | varchar | 营业日期 | 分区键 + 业务日锚点 |
| `store_id` | varchar | 门店编码 | 门店维度 |
| `day_clear` | varchar | 日清标识 `'0'`/`'1'` | 区分生鲜/标品 |
| `article_id` | varchar | **验收条码**（进货扫码编码） | BOM parent 锚点 |
| `article_name` | varchar | 验收条码名称 | 人工核对 |
| `sale_article_id` | varchar | **销售条码**（BOM 子商品） | ⭐ 关键：已拆到 sub 级 |
| `sale_article_name` | varchar | 销售条码名称 | 人工核对 |
| `sale_article_qty` | double | 销售条码转换量（kg/件） | ⭐ sub 理论进货 qty |
| `sale_article_purchase_amt` | double | 销售条码理论进货金额 | ⭐ sub 理论进货 amt |
| `init_stock_qty` | double | 期初库存数量 | 库存方程 |
| `end_stock_qty` | double | 期末库存数量 | 库存方程 |
| `init_stock_amt` | double | 期初库存金额 | 库存方程验证 |
| `end_stock_amt` | double | 期末库存金额 | 库存方程验证 |
| `inventory_cost` | double | 库存成本 | 备用 |
| `avg_inbound_price` | double | 进货平均价 | ⭐ parent 均价 fallback |
| `inc_day` | varchar | 增量日（分区键） | 与 business_date 同义 |

### 3.2 关键逻辑

```
进货验收表已经预拆到 sale_article_id（sub）级：
- article_id        = 验收时扫的大件（整猪/白条）
- sale_article_id   = 实际销售的散件（排骨/五花）
- sale_article_qty  = 按出肉率换算的 sub 进货量
- sale_article_purchase_amt = sub 分摊进货金额
```

⚠ **注意**：该表约 4.5% 行是 BOM 拆分，其余是标品（`article_id = sale_article_id`）。

---

## 四、表 6：加工转换 `strategy_fm_compose_di`

### 4.1 字段详解

| 字段 | 类型 | 含义 | 计算用途 |
|---|---|---|---|
| `business_date` | varchar | 营业日期 | 分区键 |
| `store_id` | varchar | 门店编码 | 门店维度 |
| `article_id` | varchar | 商品编码 | ⭐ 记录在 **parent** 身上 |
| `article_name` | varchar | 商品名称 | 人工核对 |
| `compose_in_qty` | double | 加工转换入数量 | ⭐ parent 收到的原料量 |
| `compose_in_amt` | double | 加工转换入金额 | parent 原料成本 |
| `compose_out_qty` | double | 加工转换出数量 | ⭐ parent 产出的成品量 |
| `compose_out_amt` | double | 加工转换出金额 | parent 成品金额 |
| `update_time` | datetime | 最后更新时间 | 审计 |
| `inc_day` | varchar | 增量日（分区键） | 与 business_date 同义 |

### 4.2 关键逻辑

```
加工转换记录在 parent 身上：
- compose_in_qty  = 门店收到的原料量（如收到 50kg 猪肉）
- compose_out_qty = 门店产出的成品量（如产出 45kg 散件）

⚠ compose 表不记录 sub → 需配合 BOM 边拆分到具体 sub_article_id
```

---

## 五、表 17：BOM 分摊事实 `strategy_fm_receive_sale_di`（v4 首选）

### 5.1 字段详解（核心）

| 字段 | 类型 | 含义 | 计算用途 |
|---|---|---|---|
| `business_date` | varchar | 营业日期 | 分区键 |
| `store_id` | varchar | 门店编码 | 门店维度 |
| `article_id` | varchar | **根 parent**（验收条码） | ⭐ BOM 起点 |
| `article_name` | varchar | 根 parent 名称 | 人工核对 |
| `category_level1_id` | varchar | parent 大分类 | 过滤物料类 |
| `category_level1_description` | varchar | parent 大分类名 | 人工核对 |
| `inbound_amount` | double | parent 进货额（实际） | ⭐ parent 入库 amt |
| `inbound_qty` | double | parent 进货数（实际） | ⭐ parent 入库 qty |
| `purchase_price` | double | parent 进货平均价 | ⭐ parent 均价 |
| `sale_article_id` | varchar | **sub 销售 SKU** | ⭐ BOM 终点 |
| `sale_article_name` | varchar | sub 销售 SKU 名称 | 人工核对 |
| `sale_article_qty` | double | sub 分摊 qty（kg/件） | ⭐ **核心指标** |
| `spilit_sale_article_amt` | double | sub 分摊 amt（元） | ⭐ **核心指标**（源表拼写瑕疵） |
| `sale_article_price` | double | sub 分摊单价 | 派生 = amt / qty |
| `sum_sub_article_qty` | double | parent 下所有 sub 的 qty 汇总 | 归一化验证 |
| `sum_article_qty` | double | 同一验收条码下销售条码理论销量汇总 | 备用 |
| `sum_sale_article_qty` | double | 同一 sale_article_id 从所有 parent 拆出的合计 | ⭐ 多 parent 合并 |
| `rate` | double | 验收条码相对于销售条码的占比 | ⭐ 分摊比例 |
| `sale_recev_rate` | double | 销售条码占验收条码的占比 | ⭐ 反向比例 |
| `inc_day` | varchar | 增量日（分区键） | 与 business_date 同义 |

### 5.2 与 purchase_di 的差异对比

| 维度 | `purchase_di` | `receive_sale_di`（v4 首选） |
|---|---|---|
| BOM 拆分行占比 | ~4.5% | **100%** |
| parent × sub 双维度 | 仅 `sale_article_id` | `article_id`（父）+ `sale_article_id`（子） |
| 分摊金额字段 | `sale_article_purchase_amt` | `spilit_sale_article_amt` + `inbound_amount` |
| 归一化比例字段 | 无 | `rate` / `sale_recev_rate` |
| 多 parent 合并 | 无法处理 | `sum_sale_article_qty` 直接可用 |

### 5.3 BOM 分摊计算链

```
1. parent 进货事实：inbound_qty × purchase_price = inbound_amount
2. 分摊到 sub：sale_article_qty × sale_article_price = spilit_sale_article_amt
3. 归一化验证：SUM(sale_article_qty) OVER parent ≈ inbound_qty × 出肉率
4. 多 parent 合并：SUM(spilit_sale_article_amt) OVER sub → sub 总进货成本
```

---

## 六、表 19：单位换算 `strategy_fm_dim_article_convert`

### 6.1 字段详解

| 字段 | 类型 | 含义 | 计算用途 |
|---|---|---|---|
| `store_id` | varchar | 门店编码（旧 id） | 门店维度 |
| `parent_article_id` | varchar | 父商品编码 | BOM 边起点 |
| `parent_article_name` | varchar | 父商品名称 | 人工核对 |
| `sub_article_id` | varchar | 子商品编码 | BOM 边终点 |
| `sub_article_name` | varchar | 子商品名称 | 人工核对 |
| `parent_rate` | double | 父单位量 × rate = 子单位量 | ⭐ 单位换算 |
| `sub_rate` | double | 子单位量 × rate = 父单位量 | 反向换算 |
| `ctype` | int | 场景类型 | 1=BOM, 2=spu, 3=spu-BOM混合 |
| `inc_day` | varchar | 增量日（分区键） | 日分区 |

### 6.2 单位换算示例

```
parent = 整猪（单位：头）
sub = 排骨（单位：kg）
parent_rate = 80  → 1头猪 = 80kg 排骨

⚠ 实际出肉率需用表 20 的 dressing_rate
```

---

## 七、表 20：BOM 关系边 `strategy_dim_store_article_bom_relation`

### 7.1 字段详解（BOM 定义核心）

| 字段 | 类型 | 含义 | 计算用途 |
|---|---|---|---|
| `store_id` | varchar | 门店编码 | 门店维度 |
| `category_level3_id` | varchar | BOM 头商品小类编码 | 分类过滤 |
| `category_level3_description` | varchar | BOM 头商品小类名称 | 人工核对 |
| `category_level2_id` | varchar | BOM 头商品中类编码 | 分类过滤 |
| `category_level2_description` | varchar | BOM 头商品中类名称 | 人工核对 |
| `category_level1_id` | varchar | BOM 头商品大类编码 | 分类过滤 |
| `category_level1_description` | varchar | BOM 头商品大类名称 | 人工核对 |
| `parent_article_id` | varchar | **BOM 头商品**（parent） | ⭐ BOM 边起点 |
| `parent_article_unit` | varchar | BOM 头商品单位 | kg/件/头 |
| `sub_article_id` | varchar | **BOM 子商品**（sub） | ⭐ BOM 边终点 |
| `sub_article_unit` | varchar | BOM 子商品单位 | kg/件 |
| `dressing_rate` | double | **标准出肉率**（%） | ⭐ **核心参数** |
| `cost_rate` | double | **成本比例**（%） | ⭐ **核心参数**（可能 NULL） |
| `bom_type` | int | 门店 BOM 类型 | 1=门店, 2=仓, 3=非门店非仓 |
| `split_mode` | varchar | BOM 类型 | 10=一拆多, 20=一拆一 |
| `sp_level` | int | BI 店铺等级 | 1=实体店, 2=菜吧, 3=B端... |
| `inc_day` | varchar | 增量日（分区键） | 日分区 |

### 7.2 出肉率与成本比例

```
dressing_rate = 出肉率（标准定义）
- 整猪拆成排骨、五花、梅头...
- 同一 parent 下所有 sub 的 dressing_rate 合计 ≈ 100%

cost_rate = 成本比例（财务分摊）
- 每个 sub 承担 parent 成本的百分比
- 合计应 = 100%
- ⚠ 源表可能 NULL → 需反推兜底
```

### 7.3 cost_rate 反推公式

当 `cost_rate` 为 NULL 或 0 时，用 sub 原价 × dressing_rate 做权重反推：

```sql
w_i = sub.original_price × dressing_rate
cost_rate_inferred = w_i / SUM(w_i) OVER parent
alloc_amt = parent.inbound_amount × cost_rate_inferred
```

---

## 八、BOM 分摊计算流程（v4 完整版）

### 8.1 三级数据源 fallback

| 优先级 | 数据源 | 字段来源 | 适用场景 |
|---|---|---|---|
| **1** | `RECEIVE_SALE` | `atomic_receive_sale` | 上游已拆好，直接取 `sale_article_qty` / `spilit_sale_article_amt` |
| **2** | `COMPOSE` | `atomic_compose` + `atomic_bom_relation` | receive_sale 缺失但有加工转换，用 `compose_out_qty × dressing_rate` |
| **3** | `BOM_THEORETICAL` | `atomic_bom_relation` + `parent.receive_qty` | 完全理论值，`parent.receive_qty × dressing_rate` |

### 8.2 多 parent 合并

当同一 sub 从多个 parent 拆出时：

```sql
-- 按 parent_inbound_qty × dressing_rate 加权
effective_unit_cost = 
    SUM(parent_inbound_qty × dressing_rate × parent_unit_price) 
    / SUM(parent_inbound_qty × dressing_rate)
```

等价于：按分摊进货量占比加权 parent 均价。

### 8.3 四级成本 fallback

```sql
effective_unit_cost = COALESCE(
    NULLIF(cost_price, 0),        -- ① DIRECT: 标品成本价
    NULLIF(bom_unit_cost, 0),     -- ② BOM_ALLOC: BOM 分摊后 sub 成本
    NULLIF(avg_inbound_price, 0), -- ③ PURCHASE_AVG: 进货均价兜底
    0                             -- ④ MISSING: 告警
)
```

### 8.4 门店毛利计算

```sql
-- 销售方程（推荐）
store_profit_sales = sale_amt - sale_qty × effective_unit_cost - losses

-- 库存方程（验证）
store_profit_stock = sale_amt - (receive + compose_in - compose_out) 
                    + (end_stock - init_stock) - losses
```

---

## 九、辅助表字段

### 9.1 表 1：销售 `strategy_fm_sales_di`

| 关键字段 | 含义 | 计算用途 |
|---|---|---|
| `abi_article_id` | 商品编码（ABI 规范） | 映射到 `sale_article_id` |
| `qty` | 销售件数（销售单位） | 客数统计 |
| `qty_spec` | 销售规格量（kg） | ⭐ 成本计算口径 |
| `sales_amt` | 销售额 | ⭐ 毛利起点 |
| `p_lp_sub_amt` | 原价金额 | 折扣计算 |
| `vip_discount_amt` | 会员折扣 | 分类折扣 |
| `hour_discount_amt` | 时段折扣 | 晚市折扣 |
| `return_sale_qty/amt` | 退货数量/金额 | 销售净额 |
| `day_clear` | 日清标识 | 生鲜/标品区分 |
| `online_flag` | 线上标识 | 渠道分析 |
| `customer_phone` | 会员手机号 | 会员识别 |

### 9.2 表 3：SAP 出入库 `strategy_fm_scm_di`

| 关键字段 | 含义 | 计算用途 |
|---|---|---|
| `original_outstock_qty/amt` | 常规出库 | SAP 出库量 |
| `promotion_outstock_qty/amt` | 促销出库 | SAP 促销出库 |
| `gift_outstock_qty` | 赠品出库 | SAP 赠品 |
| `total_outstock_qty` | 总出库数量 | ⭐ 出库总量 |
| `out_stock_pay_amt` | 出库应付金额（含税） | 出库金额 |
| `out_stock_amt_cb` | 出库成本金额 | ⭐ SAP 成本 |
| `return_stock_qty` | 门店退仓量 | 退货 |
| `adjustment_amt` | 差异调整金额 | ⭐ SAP 调整 |
| `scm_promotion_amt_total` | 出库让利总额 | 供应链让利 |

### 9.3 表 5：损耗 `strategy_fm_loss_di`

| 关键字段 | 含义 | 计算用途 |
|---|---|---|
| `know_lost_qty/amt` | 已知损耗 | ⭐ 毛利扣减项 |
| `unknow_lost_qty/amt` | 未知损耗 | ⚠ 上游反推，下游有自己的反推 |

### 9.4 表 9：成本价池 `strategy_fm_inventory_pool_di`

**源表**：`hive.ods_sc_db.t_shop_inventory_sku_pool`

| 关键字段 | 含义 | 计算用途 |
|---|---|---|
| `shop_id` | 门店编号 | 映射到 `store_id` |
| `sku_code` | 商品编号 | 映射到 `article_id` |
| `inventory_date` | 盘点日期 | 映射到 `business_date` |
| `cost_price` | 成本价 | ⭐ **一级成本来源** |

### 9.5 表 10：价格 `strategy_fm_price_da`

**源表**：`hive.dim.dim_store_article_price_info_da`

| 关键字段 | 含义 | 计算用途 |
|---|---|---|
| `current_price` | 今日售价 | 售价锚点 |
| `yesterday_price` | 昨日售价 | 价格变动 |
| `original_price` | 销售原价 | ⭐ cost_rate 反推权重 |
| `dc_price` | 出库价格 | SAP 出库价 |

---

## 十、字段命名约定总结

### 10.1 ID 字段对照

| 语义 | purchase_di | receive_sale_di | bom_relation | sales_di | 其他表 |
|---|---|---|---|---|---|
| 验收条码（parent） | `article_id` | `article_id` | `parent_article_id` | — | `article_id` |
| 销售条码（sub） | `sale_article_id` | `sale_article_id` | `sub_article_id` | `abi_article_id` | `sale_article_id` |
| 订购条码 | — | — | — | — | `order_article_id` |

### 10.2 Qty 字段对照

| 语义 | purchase_di | receive_sale_di | compose_di | bom_relation |
|---|---|---|---|---|
| parent 进货量 | — | `inbound_qty` | `compose_in_qty` | — |
| sub 分摊量 | `sale_article_qty` | `sale_article_qty` | — | — |
| parent 产出量 | — | — | `compose_out_qty` | — |
| 出肉率换算 | — | — | — | `dressing_rate` |

### 10.3 Amount 字段对照

| 语义 | purchase_di | receive_sale_di | compose_di |
|---|---|---|---|
| parent 进货额 | `avg_inbound_price × ?` | `inbound_amount` | `compose_in_amt` |
| sub 分摊额 | `sale_article_purchase_amt` | `spilit_sale_article_amt` | — |
| parent 产出额 | — | — | `compose_out_amt` |

---

## 十一、ETL 使用建议

### 11.1 BOM 分摊表选择

| 场景 | 推荐表 | 原因 |
|---|---|---|
| 正常计算 | `receive_sale_di`（表 17） | 100% BOM 拆分，字段完整 |
| receive_sale 缺失 | `compose_di`（表 6）+ `bom_relation`（表 20） | compose 有 parent 产出量 |
| 完全兜底 | `bom_relation`（表 20）+ `purchase_di`（表 2） | 理论值 = receive_qty × dressing_rate |

### 11.2 成本来源优先级

```
1. cost_price（表 9）        → 标品有直接成本价
2. bom_unit_cost（计算层）   → BOM sub 分摊成本
3. avg_inbound_price（表 2） → 自采兜底
4. 0 + MISSING tag          → 告警
```

### 11.3 查询口径注意事项

1. **day_clear 分组翻倍**：`GROUP BY day_clear` 后 `SUM` 会得到 2× 全天总量
2. **shop_id vs store_id**：表 8、9、14 用 `shop_id`，其他表用 `store_id`
3. **sku_code vs article_id**：表 8、9、14 用 `sku_code`，其他表用 `article_id`
4. **inventory_date vs business_date**：表 9 用 `inventory_date` 当业务日
5. **spilit 拼写**：表 17 `spilit_sale_article_amt` 是源表拼写瑕疵，勿修正

---

## 附录：完整表序号速查

| 序号 | StarRocks 目标表 | 核心用途 |
|---|---|---|
| 1 | `strategy_fm_sales_di` | 销售事实 |
| 2 | `strategy_fm_purchase_di` | 进货验收事实（含 BOM 预拆） |
| 3 | `strategy_fm_scm_di` | SAP 出入库事实 |
| 4 | `strategy_fm_scm_adjust_di` | SCM 差异调整（当前空） |
| 5 | `strategy_fm_loss_di` | 损耗事实 |
| 6 | `strategy_fm_compose_di` | 加工转换事实 |
| 7 | `strategy_fm_allowance_di` | 活动让利事实 |
| 8 | `strategy_fm_promo_di` | 促销明细（用 shop_id/sku_code） |
| 9 | `strategy_fm_inventory_pool_di` | 成本价池（用 shop_id/sku_code） |
| 10 | `strategy_fm_price_da` | 价格日表 |
| 12 | `strategy_fm_dim_day_clear` | 日清商品清单 |
| 13 | `strategy_fm_dim_store_profile` | 门店画像（用 sp_store_id） |
| 14 | `strategy_fm_dim_saleable` | 可售商品（用 shop_id/sku_code） |
| 15 | `strategy_fm_dim_goods` | 商品主数据 |
| 16 | `strategy_fm_dim_calendar` | 日历（用 day_date） |
| 17 | `strategy_fm_receive_sale_di` | ⭐ BOM 分摊事实主源 |
| 18 | `strategy_fm_order_receive_di` | 订验关系（当前空） |
| 19 | `strategy_fm_dim_article_convert` | 单位换算 |
| 20 | `strategy_dim_store_article_bom_relation` | ⭐ BOM 关系边定义 |

---

## 十一、Hive 源表索引

> 完整字段清单见「完整版」附录A。此处仅列出映射关系。

| 序号 | Hive 源表 | → strategy_fm | → DuckDB | 字段数 |
|---|---:|---|---:|---:|
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

### 字段映射注意事项

| Hive 源表 | 源字段 → 目标字段 |
|---|---|
| `t_shop_inventory_sku_pool` | `shop_id` → `store_id`, `sku_code` → `article_id`, `inventory_date` → `business_date` |
| `t_purchase_order_item_tmp` | `shop_id` → `store_id`, `sku_code` → `article_id` |
| `dsl_promotion_...` | `shop_id` → `store_id`, `sku_code` → `article_id` |
| `dim_store_profile` | `sp_store_id` → `store_id` |
| `dim_calendar` | `day_date` → `business_date` |

---

*生成时间：2026-04-24*
*版本：v4.0 BOM 毛利计算专用版*
