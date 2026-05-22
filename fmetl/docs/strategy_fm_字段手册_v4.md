# FM ETL v10 字段手册

> **版本**: v4 (v10.0) / 2026-05-08
> **范围**: ETL 产出的 FM 底表 + 计算层中间表
> **源表参考**: v3 完整版字段手册 (fm_etl_v3/docs/)

---

## 一、核心业务概念

### 1.1 商品编码三种视角

| 视角 | 出现位置 | 业务含义 | 示例 |
|---|---|---|---|
| **验收条码 (订购码)** | `atomic_receive_sale.article_id` (parent) | 进货验收时扫码，整箱/整头猪 | 优鲜大白猪A级(20500351)、海大虾12kg/箱(21259821) |
| **销售条码 (销售码)** | `atomic_receive_sale.sale_article_id` (sub) | POS 扫码销售，散称/小包装 | 排骨(20003470)、梅头肉(20015855) |
| **自购码** | `article_id = sale_article_id` | 直接采购直接卖，不进 BOM | 普通标品 |

**BOM 关系**: 一个验收条码(订购码) 拆分成多个销售条码(销售码)。父品进货 → bom_out 流出 → 子品 bom_in 流入。

### 1.2 day_clear 字段

| 值 | 含义 | 库存规则 | 典型商品 |
|---|---|---|---|
| `0` | 日清 | end_stock=0, 残差→unknow_lost | 生鲜 |
| `1` | 非日清 | 正常库存方程, end可非零 | 标品 |
| `2` | 合计 | ETL自动UNION生成, = 0+1 | 查询用 |

⚠ 查询"全天合计"必须 `WHERE day_clear='2'`。GROUP BY day_clear 再 SUM 会翻倍。

### 1.3 门店毛利公式 (v10)

**核心公式** (四流合一):
```
profit = sale_amt                             -- 销售额
       - receive_amt                          -- 自购进货
       - bom_in_amt  + bom_out_amt            -- BOM净额 (子品-bom_in, 父品+bom_out)
       - compose_in_amt + compose_out_amt     -- 加工净额
       + end_stock_amt - init_stock_amt       -- 库存变动
```

**销售成本**:
```
日清(day_clear='0'):  sale_cost = receive + bom_in - bom_out + compose_in - compose_out
非日清(day_clear='1'): sale_cost = sale_qty × effective_unit_cost
```

**损耗处理**: 损耗已通过库存方程反映在 end_stock 中 (损耗↑ → end↓ → 毛利↓)，不额外减去 lost_amt (A20)。

**库存方程** (决定 end_stock 和 unknow_lost):
```
eq = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost
```

| 条件 | end_stock | unknow_lost |
|---|---|---|
| day_clear='0' (日清) | 0 | eq |
| eq < 0 | 0 | -eq |
| know_lost_qty > 0 (盘点) | eq | 0 |
| else (非日清无盘点) | eq | 0 |

期末库存 end_stock 直接进入上面的毛利公式。损耗量 (know + unknow) 不显式扣减——它通过压低 end_stock 自动拉低毛利，显式再减一次会重复 (A20)。

---

## 二、FM 底表: `t_fm_sku_dim` — SKU维度完整宽表

**粒度**: store_id × business_date × article_id × day_clear
**来源**: t_calc_stock + dim_goods + dim_calendar + dim_store_list + dim_chdj_store_info
**字段数**: 80

### 2.1 维度字段

| 字段 | 类型 | 示例 | 说明 |
|---|---|---|---|
| `store_id` | VARCHAR | A3XV | 门店编码 |
| `store_name` | VARCHAR | 广州滨江宏岸店 | 门店名称 |
| `store_flag` | VARCHAR | 翠花店 | 门店标签 |
| `store_no` | VARCHAR | food mart | 门店号 |
| `business_date` | VARCHAR | 2026-04-23 | 营业日期 |
| `article_id` | VARCHAR | 20003470 | 商品编码(销售条码) |
| `article_name` | VARCHAR | 优鲜排骨 | 商品名称 |
| `day_clear` | VARCHAR | 1 | 0=日清, 1=非日清 |
| `last_sysdate` | VARCHAR | 2026-04-23 21:00 | 最后更新 |

### 2.2 分类维度 (与 cust.py 重映射逻辑一致)

| 字段 | 类型 | 示例 | 说明 |
|---|---|---|---|
| `category_level1_id` | VARCHAR | 猪肉类 | **大分类**(已重映射) |
| `category_level1_description` | VARCHAR | 猪肉类 | 大分类名 |
| `category_level2_id` | VARCHAR | 边猪类 | 中分类编码 |
| `category_level2_description` | VARCHAR | 边猪类 | 中分类名 |
| `category_level3_id` | VARCHAR | 黑边猪类 | 小分类编码 |
| `category_level3_description` | VARCHAR | 黑边猪类 | 小分类名 |
| `spu_id` | VARCHAR | — | SPU编码 |
| `spu_name` | VARCHAR | — | SPU名称 |
| `blackwhite_pig_name` | VARCHAR | 大白猪 | 黑白猪名称 |

**分类重映射规则** (cust.py → sku_dim.py):
| 原始 dim_goods | 重映射后 |
|---|---|
| L2='烘焙类' | 烘焙类 |
| L3以'熟食'结尾 | 熟食类 |
| L1='肉禽蛋类' 且 L2≠'蛋类' | 肉禽类 |
| L2 IN ('冷藏奶制品类','饮料类') | 乳制品及水饮类 |
| L1 IN ('冷藏及加工类','预制菜') | 冷藏加工及预制菜类 |
| 其他 | 保持 L1 原名 |

### 2.3 区域维度

| 字段 | 类型 | 说明 |
|---|---|---|
| `manage_area_name` | VARCHAR | 管理区 |
| `sap_area_name` | VARCHAR | SAP区域 |
| `city_description` | VARCHAR | 城市 |

### 2.4 时间维度

| 字段 | 类型 | 说明 |
|---|---|---|
| `week_no` | VARCHAR | 周次 |
| `week_start_date` | VARCHAR | 周开始日期 |
| `week_end_date` | VARCHAR | 周结束日期 |
| `month_wid` | VARCHAR | 月 |
| `year_wid` | VARCHAR | 年 |

### 2.5 销售数量

| 字段 | 类型 | 说明 |
|---|---|---|
| `total_sale_qty` | DOUBLE | **销售总数量**(kg/件) |
| `sale_piece_qty` | DOUBLE | **销售件数** |
| `bf19_sale_qty` | DOUBLE | 19点前销售数量 |
| `bf19_sale_piece_qty` | DOUBLE | 19点前销售件数 |
| `bf12_sale_qty` | DOUBLE | 12点前销售数量 |
| `online_sale_qty` | DOUBLE | 线上销售数量 |
| `offline_sale_qty` | DOUBLE | 线下销售数量 |
| `return_qty` | DOUBLE | 退货数量 |

### 2.6 销售金额

| 字段 | 类型 | 说明 |
|---|---|---|
| `total_sale_amt` | DOUBLE | **销售额**(实收, 毛利公式起点) |
| `bf19_sale_amt` | DOUBLE | 19点前销售额 |
| `lp_sale_amt` | DOUBLE | **原价销售额**(list_price × qty) |
| `member_sale_amt` | DOUBLE | 会员销售额 |
| `bf19_member_sale_amt` | DOUBLE | 19点前会员销售额 |
| `return_amt` | DOUBLE | 退货额 |

### 2.7 折扣

| 字段 | 类型 | 说明 |
|---|---|---|
| `discount_amt` | DOUBLE | **折扣总额** |
| `hour_discount_amt` | DOUBLE | 时段折扣额(19点后) |
| `discount_amt_cate` | DOUBLE | **促销折扣额**(品类) |
| `member_discount_amt` | DOUBLE | 会员折扣额 |

### 2.8 客数

| 字段 | 类型 | 说明 |
|---|---|---|
| `online_cust_num` | DOUBLE | 线上客数 |

### 2.9 进货 (四流: receive)

| 字段 | 类型 | 说明 |
|---|---|---|
| `inbound_qty` | DOUBLE | **进货数量** |
| `inbound_amount` | DOUBLE | **进货金额**(源值, 非计算) |
| `purchase_weight` | DOUBLE | 采购重量(结算单位) |

### 2.10 库存

| 字段 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `init_stock_qty` | DOUBLE | stock.py | **期初库存数量** (首日=atomic_inventory源值, 次日=昨日end) |
| `init_stock_amt` | DOUBLE | stock.py | **期初库存金额** |
| `end_stock_qty` | DOUBLE | stock.py | **期末库存数量** (库存方程分支结果) |
| `end_stock_amt` | DOUBLE | stock.py | **期末库存金额** = end_qty × euc |

### 2.11 损耗

| 字段 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `store_lost_qty` | DOUBLE | stock.py | **总损耗数量** = know + unknow |
| `store_lost_amt` | DOUBLE | stock.py | **总损耗金额** = know + unknow |
| `store_know_lost_amt` | DOUBLE | stock.py | **已知损耗额** (门店报损) |
| `store_unknow_lost_amt` | DOUBLE | stock.py | **未知损耗额** (方程反推) |
| `lost_denominator` | DOUBLE | stock.py | 损耗率分母 |

⚠ 损耗已体现在库存方程中(损耗→end_stock↓→毛利↓)，毛利公式不额外减损耗额。

### 2.12 成本

| 字段 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `effective_unit_cost` | DOUBLE | sku_cost.py | **有效单位成本(euc)** |
| `cost_source` | VARCHAR | sku_cost.py | 成本来源: V10_WEIGHTED_AVG / MISSING |
| `sale_cost_amt` | DOUBLE | profit.py | **销售成本** (日清=全进货成本, 非日清=sale_qty×euc) |

### 2.13 毛利

| 字段 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `article_profit_amt` | DOUBLE | profit.py | ⭐ **门店毛利额** |
| `full_link_article_profit` | DOUBLE | profit.py | **全链路毛利** = 门店毛利 + SCM毛利 |
| `scm_fin_article_profit` | DOUBLE | profit.py | **供应链金融毛利** |
| `pre_profit_amt` | DOUBLE | profit.py | **预期毛利额**(原价口径) |
| `pre_sale_amt` | DOUBLE | profit.py | 预期销售额 |
| `pre_inbound_amount` | DOUBLE | profit.py | 预期进货额 |

### 2.14 供应链 SCM

| 字段 | 类型 | 说明 |
|---|---|---|
| `out_stock_pay_amt` | DOUBLE | 出库应付(含税) |
| `out_stock_pay_amt_notax` | DOUBLE | 出库应付(不含税) |
| `out_stock_amt_cb` | DOUBLE | 出库成本(含税) |
| `return_stock_pay_amt_notax` | DOUBLE | 退仓应付(不含税) |
| `scm_promotion_amt_total` | DOUBLE | 出库让利总额 |
| `expect_outstock_amt` | DOUBLE | 预期出库额 |

### 2.15 售罄/上架/其他

| 字段 | 类型 | 说明 |
|---|---|---|
| `is_soldout_16` | DOUBLE | 16点售罄标记 |
| `is_soldout_20` | DOUBLE | 20点售罄标记 |
| `is_stock_sku` | BIGINT | 是否上架SKU |
| `saleable` | BIGINT | 是否可售 |
| `sales_weight` | DOUBLE | 销售重量(kg) |
| `bf19_sales_weight` | DOUBLE | 19点前销售重量 |
| `avg_7d_sale_qty` | DOUBLE | 近7天日均销量 |

---

## 三、FM 底表: `t_fm_levels_result` — 平台对接表

**粒度**: 7级分类 × day_clear × business_date
**来源**: t_fm_levels_sum (聚合 t_fm_sku_dim)
**用途**: 输出到 QDM BI，供运营/管理层使用

### 3.1 维度 + 分类

| 字段 | 类型 | 说明 |
|---|---|---|
| `标签` | VARCHAR | 门店标签(store_flag) |
| `门店号` | VARCHAR | store_no |
| `门店名称` | VARCHAR | store_name (NULL时显示'广州') |
| `日期` | VARCHAR | business_date |
| `商品编码` | VARCHAR | level_id |
| `分类名称` | VARCHAR | 根据level_description取对应分类描述 |
| `大分类` | VARCHAR | category_level1_description |
| `中分类` | VARCHAR | category_level2_description |
| `小分类` | VARCHAR | category_level3_description |
| `分类等级` | VARCHAR | 门店/大类/中类/小类/SPU/黑白猪/SKU |
| `日清` | VARCHAR | 0/1/2 (原始值) |
| `非日清标识` | VARCHAR | 日清/非日清/合计 (中文) |

### 3.2 毛利指标

| 字段 | 表达式 | 说明 |
|---|---|---|
| `门店毛利额` | AVG(article_profit_amt) | **门店毛利额** |
| `全链路毛利额` | AVG(full_link_article_profit) | 全链路毛利 |
| `供应链毛利额` | AVG(scm_fin_article_profit) | SCM毛利 |
| `门店毛利率` | Σprofit / Σsale | 门店毛利率 |
| `全链路毛利率` | Σfull_link_profit / Σsale | 全链路毛利率 |
| `供应链毛利率` | Σscm_profit / Σ(out-return)_notax | SCM毛利率 |
| `门店预期毛利率` | Σpre_profit / Σlp_sale | 预期毛利率(原价口径) |
| `门店定价毛利率` | (Σpre_sale-Σpre_inbound+Δstock) / Σpre_sale | 定价毛利率 |

### 3.3 销售指标

| 字段 | 说明 |
|---|---|
| `全天销售额` | total_sale_amt |
| `19点前销售额` | bf19_sale_amt |
| `销售重量` | sales_weight |
| `19点前销售重量` | bf19_sales_weight |
| `销售数量` | total_sale_qty |
| `19点前销售数量` | bf19_sale_qty |
| `动销sku数` | sale_article_num_cate |

### 3.4 客数指标

| 字段 | 说明 |
|---|---|
| `全天来客数` | cust_num_cate |
| `全天客单价` | sale_amt / cust_num |
| `19点前客数` | bf19_cust_num_cate |
| `19点前客单价` | bf19_sale_amt / bf19_cust_num |
| `19点前件单价` | bf19_sale_amt / bf19_sale_piece_qty |
| `19点前单件数` | bf19_sale_piece_qty / bf19_cust_num |

### 3.5 损耗指标

| 字段 | 说明 |
|---|---|
| `损耗额` | store_lost_amt |
| `损耗率` | lost_amt / lost_denominator |
| `已知损耗率` | know_lost_amt / lost_denominator |
| `未知损耗率` | unknow_lost_amt / lost_denominator |

### 3.6 进货/成本/折扣

| 字段 | 说明 |
|---|---|
| `进货额` | inbound_amount |
| `采购价` | out_stock_amt_cb / purchase_weight |
| `平均售价` | sale_amt / sales_weight |
| `供应链折让率` | scm_promotion / (scm_promotion + out_stock_notax) |
| `折扣率` | discount_amt / lp_sale_amt |
| `促销折扣率` | discount_amt_cate / lp_sale_amt |
| `时段折扣率` | hour_discount_amt / lp_sale_amt |

### 3.7 售罄/上架

| 字段 | 说明 |
|---|---|
| `售罄率16` | is_soldout_16 |
| `售罄率20` | is_soldout_20 |
| `上架sku数` | is_stock_sku |
| `近7天日均销量` | avg_7d_sale_qty |
| `营业店日数` | COUNT(store_id) |
| `营业店数` | COUNT(DISTINCT store_id) |

---

## 四、计算层: `t_calc_stock` — 库存与金额中枢

**粒度**: store_id × business_date × article_id × day_clear
**来源**: stock.py (Python 四流合一)
**字段数**: 33

### 4.1 四流入

| 字段 | 来源 | 说明 |
|---|---|---|
| `receive_qty` | merge.py (atomic_receive_sale自购行) | **自购进货量** |
| `receive_amt` | merge.py (源值) | **自购进货额** |
| `bom_in_qty` | bom_alloc.py (按sub聚合) | **BOM拆分流入量**(子品视角) |
| `bom_in_amt` | bom_alloc.py | **BOM拆分流入额** |
| `compose_in_qty` | atomic_compose | **加工流入量** |
| `compose_in_amt` | qty × avg_inbound_price | **加工流入额** |

### 4.2 三流出

| 字段 | 来源 | 说明 |
|---|---|---|
| `bom_out_qty` | bom_alloc.py (按parent聚合) | **BOM拆分流出量**(父品视角) |
| `bom_out_amt` | bom_alloc.py | **BOM拆分流出额** |
| `compose_out_qty` | atomic_compose | **加工流出量** |
| `compose_out_amt` | qty × avg_inbound_price | **加工流出额** |

### 4.3 销售 + 损耗 + 库存

| 字段 | 说明 |
|---|---|
| `sale_qty` / `sale_amt` | 销售数量/金额 |
| `know_lost_qty` / `know_lost_amt` | 已知损耗(qty × euc) |
| `unknow_lost_qty` / `unknow_lost_amt` | 未知损耗(qty × euc) |
| `lost_qty` / `lost_amt` | **总损耗** = know + unknow |
| `init_stock_qty` / `init_stock_amt` | 期初库存 (首日=源表, 次日=昨日期末) |
| `end_stock_qty` / `end_stock_amt` | 期末库存 (分支逻辑结果) |
| `effective_unit_cost` | 有效单位成本 (来自 sku_cost) |
| `cost_source` | 成本来源标记 |

### 4.4 SCM 字段

| 字段 | 说明 |
|---|---|
| `out_stock_pay_amt` / `out_stock_pay_amt_notax` | 出库应付(含税/不含税) |
| `out_stock_amt_cb` | 出库成本 |
| `return_stock_pay_amt_notax` | 退仓应付(不含税) |
| `scm_promotion_amt_total` | 让利总额 |
| `expect_outstock_amt` | 预期出库 |
| `purchase_weight` | 采购重量 |

---

## 五、计算层: `t_calc_sku_cost` — 有效单位成本

**粒度**: store_id × business_date × article_id
**字段数**: 17

| 字段 | 说明 |
|---|---|
| `store_id` / `business_date` / `article_id` | 维度 |
| `self_inbound_qty` / `self_inbound_amt` | 自购进货量/额 |
| `compose_net_qty` / `compose_net_amt` | 加工净额 (in - out) × avg_inbound_price |
| `bom_alloc_qty` / `bom_alloc_amt` | BOM分摊流入量/额 (仅子品) |
| `init_stock_qty` / `init_stock_amt` | 期初库存 (首日=源表, 次日=昨日期末) |
| `avg_inbound_price` | 进货均价 |
| `total_cost_amt` | 成本金额合计 |
| `cost_qty` | 成本数量合计 |
| `effective_unit_cost` | ⭐ **有效单位成本** = cost_amt / cost_qty (仅当cost_qty>0) |
| `cost_source` | V10_WEIGHTED_AVG |
| `is_first_day` | 是否首日 |

**euc 公式**:
```
cost_amt = init_stock_amt + self_receive_amt + compose_net_amt + bom_alloc_amt
cost_qty = init_stock_qty + self_receive_qty + compose_net_qty + bom_alloc_qty
euc = cost_amt / cost_qty  (仅当 cost_qty > 0)
```

---

## 六、计算层: `t_calc_bom_alloc` — BOM分摊事实表

**粒度**: parent_article_id × sub_article_id × business_date
**字段数**: 28

| 字段 | 说明 |
|---|---|
| `parent_article_id` / `sub_article_id` | BOM边 |
| `parent_inbound_qty` / `parent_inbound_amount` | 父品进货量/额 |
| `parent_unit_price` | 父品进货单价 |
| `sale_qty` / `know_lost_qty` | 子品销售/已知损耗 |
| `consume_qty` / `consume_weight` | 消耗量 = sale + know_lost, 消耗权重 = consume × list_price |
| `self_inbound_qty` / `self_inbound_amt` | 子品自购量/额 (Type A判据) |
| `is_type_a` | 1=自购型(拆分需求=消耗-自购), 0=依赖型(拆分需求=消耗) |
| `split_need_weight` / `split_need_qty` | 拆分需求权重/量 (A14: max(0, consume-self)防负) |
| `group_total_weight` | Σ总权重 (共享组=组内所有sub权重和) |
| `alloc_ratio` | 分配占比 = split_need_weight / group_total_weight |
| `bom_alloc_amt` / `bom_alloc_qty` | ⭐ **分摊金额/数量** |
| `dressing_rate` | = alloc_ratio |
| `sub_qty_actual` / `sub_qty_source` | 子品实际消耗量 |
| `sub_alloc_amt` / `sub_unit_cost` | 子品分摊额/单价 |

**v10 关键修复**:
- A13: 共享组 parent_inbound_qty 取组总 qty
- A14: split_need_qty = max(0, consume - self_inbound)
- A18: 共享子品按进货额比例拆分为两行, 各父品承担自身份额
- A19: bom_alloc_qty 单位归一化 = split_need_qty × (parent_qty / sum_sub_qty)

---

## 七、BOM 分摊算法详解

### 7.1 数据流

```
atomic_receive_sale (parent × sub)
    │
    ├─ article_id = sale_article_id  → self_receive (自购)
    └─ article_id ≠ sale_article_id → BOM relations
            │
            ↓
    bom_alloc.py (Python)
            │
            ├─ 消耗权重 = (sale_qty + know_lost_qty) × list_price
            ├─ 自购权重 = self_inbound_qty × list_price
            ├─ 拆分需求 = 消耗权重 - 自购权重 (Type A) / 消耗权重 (Type B/C)
            ├─ 共享组识别: parent_B.subs ⊆ parent_A.subs → 合并
            ├─ Σ总权重 = 组内所有sub拆分需求权重和
            ├─ 分配占比 = 拆分需求权重 / Σ总权重
            └─ bom_alloc_amt = 分配占比 × 组总进货额
```

### 7.2 共享组处理

当 parent_B 的 subs 是 parent_A subs 的子集时，合并为共享组:
- 共享 sub: bom_out 按 parent_amt/total_amt 比例分拆给两个父品
- parent-only sub: bom_out 全归该父品

### 7.3 单位归一化

`bom_alloc_qty` 从子品单位换算到父品单位:
```
conv_ratio = parent_qty / parent_sum_sub_qty
bom_alloc_qty (parent units) = split_need_qty (sub units) × conv_ratio
```
例: 海大虾 1箱=12kg, 子品消耗10kg → bom_out = 10 × (1/12) = 0.833箱

---

## 八、源表映射速查

| QDM 源表 | DuckDB 表 | 域 | v4角色 |
|---|---|---|---|
| `strategy_fm_sales_di` | `atomic_sales` | ①销售 | sale_qty/amt, 时段, 会员 |
| `strategy_fm_purchase_di` | `atomic_inventory` | ②库存 | init_stock, avg_inbound_price (首日) |
| `strategy_fm_receive_sale_di` | `atomic_receive_sale` | ⭐BOM核心 | self_receive + BOM relations |
| `strategy_fm_scm_di` | `atomic_scm` | ③SCM | 出库/退仓/让利 |
| `strategy_fm_loss_di` | `atomic_loss` | ④损耗 | know_lost_qty/amt |
| `strategy_fm_compose_di` | `atomic_compose` | ⑤加工 | compose_in/out qty |
| `strategy_fm_allowance_di` | `atomic_allowance` | ⑥补贴 | allowance_amt |
| `strategy_fm_promo_di` | `atomic_promo` | ⑦促销 | 优惠券/促销承担 |
| `strategy_fm_inventory_pool_di` | `atomic_cost_price` | ⑧成本价 | cost_price (观测) |
| `strategy_fm_price_da` | `atomic_price` | ⑨价格 | current/original_price |
| `strategy_dim_store_article_bom_relation` | `atomic_bom_relation` | BOM关系 | dressing_rate (观测用) |

| 维度源表 | DuckDB 表 | 用途 |
|---|---|---|
| `strategy_fm_dim_goods` | `dim_goods` | 商品分类(3级)+SPU+黑白猪 |
| `ads_business_analysis.chdj_store_info` | `dim_store_list` | 门店过滤 |
| `strategy_fm_dim_day_clear` | `dim_day_clear` | 日清标记 |
| `strategy_fm_dim_calendar` | `dim_calendar` | 周/月/年维度 |
| `strategy_fm_dim_saleable` | `dim_saleable` | 可售标记 |

---

## 九、v10 修复完全对照表

| # | 异常 | 修复位置 | 修复方案 |
|---|---|---|---|
| A1 | receive 从 purchase_di 取 | merge.py | 切为 receive_sale_di 自购行 |
| A2 | 负库存不转 unknow_lost | stock.py | eq<0 → end=0, unknow=-eq |
| A3 | 库存方程缺 BOM | stock.py | +bom_in -bom_out |
| A4 | receive_amt 用 euc 算 | merge.py | 直接取源表值 |
| A5 | init 每天重算 | sku_cost.py | 取昨天 end_stock |
| A6 | compose 两套定价 | sku_cost.py | 统一 avg_inbound_price |
| A7 | 盘点用 amt 判 | stock.py | 改用 know_lost_qty > 0 |
| A8 | 首日 init_amt 算错 | sku_cost.py | 直接用源表值 |
| A9 | prev_end JOIN 限 day_clear | sku_cost.py | Python merge 不限制 |
| A10 | 毛利缺 BOM | profit.py | 新公式含 bom_in/out |
| A11 | profit_amt 冗余 | profit.py | 只保留一个 |
| A12 | 日清 sale_cost 缺 BOM | profit.py | 新公式含 BOM |
| A13 | 共享组 qty 取错 | bom_alloc.py | 组总 qty |
| A14 | split_need_qty 负值 | bom_alloc.py | max(0, consume-self) |
| A15 | inbound 不一致 | sku_dim.py | 统一从 stock 取 |
| A16 | cust 未过滤品类 | cust.py | Python JOIN dim_goods |
| A17 | 父品进货条码无进货额 | merge.py | 双路 self_receive + 父品补入 |
| A18 | 共享组 bom_out 全归一个父品 | bom_alloc.py | 按进货额比例分拆 |
| A19 | bom_out_qty 单位不匹配 | bom_alloc.py | qty × (parent_qty/sum_sub_qty) |
| A20 | 毛利重复扣损耗 | profit.py | 去掉显式 -lost_amt |

---

*生成时间: 2026-05-08*
*版本: v4 (v10.0)*
*数据来源: fmetl ETL 管线输出*
