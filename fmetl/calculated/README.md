# calculated/ — 计算层

> **公式权威信源**：核心公式（profit / EUC / BOM 分摊 / 库存方程）的 canonical 定义以
> [`CLAUDE.md`](../../CLAUDE.md) 的「Core Business Logic」+ [`docs/architecture/ETL_v0.11_完整处理逻辑.md`](../docs/architecture/ETL_v0.11_完整处理逻辑.md) 为准。
> 本 README 提供模块级实现细节（字段、分支、来源），与信源保持同步；如发现不一致，**以代码 + 信源为准**，并回头修正本文件。

计算层是 ETL 管道的核心逻辑层。所有复杂计算（分支、加权、聚合）在 Python (pandas + NumPy) 中完成，SQL (DuckDB) 仅用于数据提取 (`SELECT ... FROM ...`)。

## 模块依赖关系

```
t_atomic_wide  ← merge.py (全SQL)
     │
     ├──→ bom_alloc.py ──→ t_calc_bom_alloc
     │         │
     │         └──→ sku_cost.py ──→ t_calc_sku_cost
     │                   │
     │                   └──→ stock.py ──→ t_calc_stock (中枢)
     │                             │
     │                             └──→ profit.py ──→ t_calc_profit
     │
     └──→ (同时被 profit.py 读取, 获取原价/补贴字段)
```

---

## 一、merge.py — t_atomic_wide (82字段)

**功能**: 将 12 张活跃原子表合并为一张宽表，是计算层的唯一数据入口。

### 合并方式

```
基表: atomic_sales FULL OUTER JOIN atomic_inventory
      ON store_id, business_date, article_id

   + INNER JOIN dim_store_list (确保翠花门店)
   + LEFT JOIN dim_day_clear (补 day_clear 标签)

   + LEFT JOIN _tmp_self_receive (自购数据: receive_sale WHERE article_id=sale_article_id)
   + LEFT JOIN atomic_scm
   + LEFT JOIN atomic_loss
   + LEFT JOIN atomic_compose
   + LEFT JOIN atomic_allowance
   + LEFT JOIN atomic_promo
   + LEFT JOIN atomic_cost_price
   + LEFT JOIN atomic_price
```

### 关键新增列 (v0.10)

| 字段 | 来源 | 说明 |
|------|------|------|
| `self_receive_qty` | `_tmp_self_receive` | 自购进货量: SUM(inbound_qty) FROM receive_sale WHERE article_id=sale_article_id |
| `self_receive_amt` | `_tmp_self_receive` | 自购进货额: SUM(inbound_amount) 同条件 |
| `init_stock_qty_src` | `atomic_inventory` | 期初库存量 (源表值, 带 `_src` 后缀表示可被计算层替换) |
| `init_stock_amt_src` | `atomic_inventory` | 期初库存额 (源表值) |
| `avg_inbound_price` | `atomic_inventory` | 平均进货价 |

### 完整字段列表

#### 销售域 (来自 atomic_sales, 32字段)
`sale_qty`, `sale_piece_qty`, `return_sale_qty`, `gift_qty`, `online_sale_qty`, `offline_sale_qty`, `bf19_sale_qty`, `af19_sale_qty`, `bf12_sale_qty`, `sales_weight`, `sale_amt`, `original_price_sale_amt`, `vip_discount_amt`, `hour_discount_amt`, `actual_amount`, `return_sale_amt`, `member_discount_amt`, `discount_amt`, `member_sale_amt`, `bf19_member_sale_amt`, `offline_original_amt`, `store_paylevel_discount`, `company_paylevel_discount`, `af19_sale_amt`, `bf19_sale_amt`, `bf19_offline_sale_amt`, `bf12_sale_amt`, `bf19_sale_piece_qty`, `last_sysdate`

#### 库存域 (v0.10简化, 3字段)
`init_stock_qty_src`, `init_stock_amt_src`, `avg_inbound_price`

#### 自购域 (v0.10新增, 2字段)
`self_receive_qty`, `self_receive_amt`

#### 供应链域 (来自 atomic_scm, 28字段)
`original_outstock_qty`, `promotion_outstock_qty`, `gift_outstock_qty`, `return_stock_qty`, `store_return_qty_shop`, `store_order_qty`, `order_qty_payean`, `outstock_unit_price`, `outstock_unit_price_notax`, `outstock_cost_price`, `outstock_cost_price_notax`, `return_unit_price`, `return_unit_price_notax`, `return_cost_price`, `return_cost_price_notax`, `order_unit_price`, `scm_promotion_amt_total`, `scm_promotion_amt_gift`, `scm_bear_amt`, `vendor_bear_amt`, `business_bear_amt`, `market_bear_amt`, `vender_bear_gift_amt`, `scm_bear_gift_amt`, `adjustment_amt`

#### 损耗域 (来自 atomic_loss, 4字段)
`know_lost_qty`, `unknow_lost_qty_src`, `know_lost_amt_src`, `unknow_lost_amt_src`

#### 加工域 (来自 atomic_compose, 2字段)
`compose_in_qty`, `compose_out_qty`

#### 补贴域 (来自 atomic_allowance, 1字段)
`allowance_amt`

#### 促销域 (来自 atomic_promo, 7字段)
`member_coupon_shop_amt`, `member_promo_amt`, `member_coupon_company_amt`, `shop_promo_amt`, `no_ordercoupon_company_promotion_amt`, `ordercoupon_shop_promotion_amt`, `ordercoupon_company_promotion_amt`

#### 价格域 (来自 atomic_price + atomic_cost_price, 5字段)
`cost_price`, `current_price`, `yesterday_price`, `dc_original_price`, `original_price`

---

## 二、bom_alloc.py — t_calc_bom_alloc (27字段)

**功能**: 将 BOM 父品的进货成本按权重分摊到各个子品。

**粒度**: store_id × business_date × parent_article_id × sub_article_id

### 算法: Σ总权重法

#### Step 1 — 数据加载

从以下表读取数据到 Python:

| 数据 | 来源 | 用途 |
|------|------|------|
| BOM关系 | `atomic_receive_sale WHERE article_id != sale_article_id` | 识别 parent→sub 关系 |
| 销售/价格 | `atomic_sales` + `atomic_price` + `atomic_loss` | 计算消耗权重 |
| 自购 | `atomic_receive_sale WHERE article_id = sale_article_id` | 计算自购权重 |

#### Step 2 — 消耗与自购权重 (per sub)

```
consume_qty     = sub.sale_qty + sub.know_lost_qty
consume_weight  = consume_qty × sub.list_price (original_price)

self_inbound_qty    = self_receive_qty (article_id = sale_article_id)
self_inbound_weight = self_inbound_qty × list_price
```

#### Step 3 — 拆分需求权重

```
IF self_inbound_qty > 0 (Type A — 子品自己有独立进货):
    split_need_weight = max(0, consume_weight - self_inbound_weight)  ← A14修复
    split_need_qty    = max(0, consume_qty - self_inbound_qty)        ← A14修复

ELSE (Type B/C — 子品完全依赖父品拆分):
    split_need_weight = consume_weight
    split_need_qty    = consume_qty
```

#### Step 4 — 共享组识别 (A13修复)

如果 parent_A 的子品集合 ⊆ parent_B 的子品集合（或反之），两者合并为一个共享组。

```
共享组 total_qty = Σ(parent.qty)  ← A13: 所有 parent qty 之和，非仅第一个
共享组 total_amt = Σ(parent.amt)
```

#### Step 5 — 分配占比与金额

```
Σ总权重 = Σ(共享组内所有 subs 的 split_need_weight)

对于每个 sub:
  alloc_ratio   = split_need_weight / Σ总权重        (Σ总权重 > 0)
  bom_alloc_amt = alloc_ratio × parent_inbound_amount  (父品总进货额)

  ── 单位归一化 (v0.10 fix, 两套 qty 不可混用) ──
  bom_alloc_qty     = alloc_ratio × parent_qty   ← 父品单位, 用于 stock.py 的 bom_out
  bom_alloc_qty_sub = alloc_ratio × sum_sub      ← 子品单位, 用于 sku_cost.py 的 bom_in
                       (sum_sub = parent_sum_sub_qty, 无则回退 parent_qty)
```

> ⚠️ **关键**: `bom_alloc_qty`(父品单位) 与 `bom_alloc_qty_sub`(子品单位) 是两套量，
> 混用会导致 euc 暴涨（海大虾 200.84→54.47 案例）。sku_cost.py 计算子品 euc 的 bom_in
> 必须用 `bom_alloc_qty_sub`；stock.py 计算父品 bom_out 用 `bom_alloc_qty`。

### 全部输出字段 (28列)

| 字段 | 公式/来源 | 说明 |
|------|----------|------|
| `store_id` | 数据源 | 门店编码 |
| `business_date` | 数据源 | 业务日期 |
| `parent_article_id` | BOM关系 | 父品编码 |
| `sub_article_id` | BOM关系 | 子品编码 |
| `parent_inbound_qty` | 父品总进货量 (共享组=组总qty) | 父品进货量 |
| `parent_inbound_amount` | 父品总进货额 (共享组=组总amt) | 父品进货额 |
| `parent_unit_price` | `parent_inbound_amount / parent_inbound_qty` | 父品进货单价 |
| `sale_qty` | atomic_sales | 子品销售量 |
| `know_lost_qty` | atomic_loss | 子品已知损耗量 |
| `consume_qty` | `sale_qty + know_lost_qty` | 子品消耗量 |
| `consume_weight` | `consume_qty × list_price` | 子品消耗权重 |
| `self_inbound_qty` | receive_sale (自购) | 子品自购进货量 |
| `self_inbound_amt` | receive_sale (自购) | 子品自购进货额 |
| `self_inbound_weight` | `self_inbound_qty × list_price` | 子品自购权重 |
| `is_type_a` | `self_inbound_qty > 0` | 是否Type A (有自购) |
| `split_need_weight` | Type A: `max(0, consume_weight - self_inbound_weight)`; else: `consume_weight` | 拆分需求权重 |
| `split_need_qty` | Type A: `max(0, consume_qty - self_inbound_qty)`; else: `consume_qty` | 拆分需求数量 |
| `group_total_weight` | Σ(共享组内 sub.split_need_weight) | 组总权重 |
| `alloc_ratio` | `split_need_weight / group_total_weight` | 分配占比 |
| `bom_alloc_amt` | `alloc_ratio × 父品进货额` | BOM分摊金额 |
| `bom_alloc_qty` | `alloc_ratio × parent_qty` | BOM分摊数量(**父品单位**, 给 stock.py bom_out) |
| `bom_alloc_qty_sub` | `alloc_ratio × sum_sub` | BOM分摊数量(**子品单位**, 给 sku_cost.py bom_in) |
| `dressing_rate` | `alloc_ratio` | 修整率 (=分配占比) |
| `cost_rate_effective` | `0.0` | 有效成本率 (预留) |
| `cost_rate_source` | `'V10_WEIGHTED_ALLOC'` | 成本率来源标记 |
| `sub_qty_actual` | `consume_qty` | 子品实际消耗量 |
| `sub_qty_source` | `'SALES+LOSS'` | 子品消耗量来源 |
| `sub_alloc_amt` | `bom_alloc_amt` | 子品分摊额 (=bom_alloc_amt副本) |
| `sub_unit_cost` | `bom_alloc_amt / bom_alloc_qty_sub` (qty_sub>0) | 子品分摊单位成本 |

---

## 三、sku_cost.py — t_calc_sku_cost (17字段)

**功能**: 计算每个 SKU 的有效单位成本 (effective_unit_cost, euc)。

**粒度**: store_id × business_date × article_id

### 加权平均算法

#### 数据加载

| 来源 | 字段 | 用途 |
|------|------|------|
| `t_atomic_wide` | self_receive_qty, self_receive_amt, compose_in_qty, compose_out_qty, init_stock_qty_src, init_stock_amt_src, avg_inbound_price | 基础数据 |
| `t_calc_bom_alloc` | bom_alloc_qty_sub (子品单位, 别名为 bom_alloc_qty), bom_alloc_amt (按 sub_article_id 聚合) | BOM分摊量/额 |
| `t_calc_stock` (昨天) | end_stock_qty, end_stock_amt (日期+1天shift) | 昨日→今日期初 |

#### 期初库存解析

```
IF 昨天 t_calc_stock 存在且非空:
    将 prev 的 business_date + 1天, 匹配 today 的 business_date
    init_stock_qty = COALESCE(prev_end_stock_qty, init_stock_qty_src)
    init_stock_amt = COALESCE(prev_end_stock_amt, init_stock_amt_src)
    is_first_day   = 1 IF prev 没有匹配行 ELSE 0

ELSE (首日, 无历史):
    init_stock_qty = init_stock_qty_src     ← A8修复: 直接用源表值
    init_stock_amt = init_stock_amt_src
    is_first_day   = 1 (全部标记为首日)
```

#### 加工净额 (v0.10 compose correction)

加工成品成本由**加工关系**配方推算，不再使用 QDM 源表金额：

```
base_euc = (init + self_receive + bom_alloc) / (init_qty + self_receive_qty + bom_alloc_qty_sub)
            ← 注意: 分母用子品单位 bom_alloc_qty_sub (SQL 中按 sub 聚合后别名为 bom_alloc_qty)

成品: compose_in_amt = compose_in_qty × Σ(raw_qty / yield_qty × raw_base_euc)
原料: compose_out_amt = compose_out_qty × base_euc (价值守恒)

compose_net_qty = compose_in_qty - compose_out_qty
compose_net_amt = compose_in_amt - compose_out_amt
```

#### 加权成本 (含兜底链)

```
total_cost_amt = init_stock_amt + self_receive_amt + compose_net_amt + bom_alloc_amt
cost_qty       = init_stock_qty + self_receive_qty + compose_net_qty + bom_alloc_qty
                 ← bom_alloc_qty 此处 = SUM(bom_alloc_qty_sub), 子品单位 (sku_cost.py L82 别名)
effective_unit_cost = total_cost_amt / cost_qty    (仅当 cost_qty > 0)
cost_source         = 'V10_WEIGHTED_AVG'
```

**euc 兜底链** (cost_qty=0 时，依次尝试):
1. ffill 沿 (store_id, article_id) 从前一营业日继承 → `V10_INHERITED_EUC`
2. avg_inbound_price → `V10_AVG_INBOUND_FALLBACK`
3. 加工关系推算 → `V10_PROCESSING_RELATION`
4. 同 matnr 兄弟 SKU 按重量比互推 → `V10_MATNR_CONVERT`

> `cost_price` 和 `current_price×0.40` **均不参与** euc 计算（早期设计已移除）。
> 实际 cost_source 标记只有上述 5 种（含主算法 `V10_WEIGHTED_AVG`），无 `V10_COST_PRICE_FALLBACK`。

### 输出字段 (19列)

| 字段 | 公式/来源 | 说明 |
|------|----------|------|
| `total_cost_amt` | `init + self_receive + compose_net + bom_alloc` | 总成本额 (分子) |
| `cost_qty` | `init + self_receive + compose_net + bom_alloc` | 总成本数量 (分母) |
| `effective_unit_cost` | `total_cost_amt / cost_qty` | 有效单位成本 |
| `cost_source` | `'V10_WEIGHTED_AVG'` | 成本来源标记 |
| `self_inbound_qty` | `self_receive_qty` | 自购进货量 |
| `self_inbound_amt` | `self_receive_amt` | 自购进货额 |
| `compose_net_qty` | `compose_in_qty - compose_out_qty` | 加工净增量 |
| `compose_net_amt` | `compose_in_amt - compose_out_amt` | 加工净增额 (配方推算) |
| `compose_in_amt` | 成品=配方推算, 否则源表值 | 加工流入金额 |
| `compose_out_amt` | `qty × base_euc` | 加工流出金额 (价值守恒) |
| `bom_alloc_amt` | SUM from t_calc_bom_alloc | BOM分摊金额 |
| `bom_alloc_qty` | `SUM(bom_alloc_qty_sub)` from t_calc_bom_alloc | BOM分摊数量(子品单位) |
| `init_stock_qty` | 昨日end / 首日源表 | 期初库存量 |
| `init_stock_amt` | 昨日end / 首日源表 | 期初库存额 |
| `avg_inbound_price` | t_atomic_wide | 平均进货价 |
| `is_first_day` | 0=有历史库存, 1=首日 | 首日标记 |

---

## 四、stock.py — t_calc_stock (35字段, 中枢表)

**功能**: 库存与金额的中枢计算。四流分离 + 跨日滚动 + 分支逻辑 + SCM 金额。

**粒度**: store_id × business_date × article_id × day_clear

### 数据加载与合并

```
wide_df (t_atomic_wide)          ← 基础数据
  + euc_df (t_calc_sku_cost)     ← 有效单位成本
  + bom_in_df (t_calc_bom_alloc, GROUP BY sub)    ← BOM流入
  + bom_out_df (t_calc_bom_alloc, GROUP BY parent) ← BOM流出
  + prev_df (t_calc_stock, 日期+1day)              ← 昨日库存
  + 缺失BOM父品补全                                ← v0.10新增
```

### BOM父品补全 (v0.10新增)

如果某个 BOM 父品只在 `t_calc_bom_alloc` 中有 bom_out 但不在 `t_atomic_wide` 中，自动创建一行（所有非BOM字段为0），确保 bom_out 不会丢失。这保证了 BOM 对称性 (Σbom_in = Σbom_out)。

### 四流库存方程

```
eq_end_qty = init_stock_qty
           + receive_qty        (自购流入)
           + bom_in_qty         (BOM拆分流入)
           - bom_out_qty        (BOM拆分流出)
           + compose_in_qty     (加工流入)
           - compose_out_qty    (加工流出)
           - sale_qty           (销售流出)
           - know_lost_qty      (已知损耗流出)
```

### 分支逻辑 (v0.10 6分支)

| 优先级 | 条件 | end_stock_qty | unknow_lost_qty | 语义 |
|--------|------|---------------|-----------------|------|
| 1 | `is_counted` (人工盘点 或 系统实盘>0) | `actual_stock_qty` | `eq - actual` (允许负=盘盈) | 信任实盘值 |
| 2 | `day_clear = '0'` | `max(0, init - consumed)` | `新供给 - sale - kl` (允许负) | 软日清：只清新供给，init可部分消耗 |
| 3 | `eq < 0` | 0 | `-eq` | 负库存保护 |
| 4 | `know_lost_qty > 0` | `eq` | 0 | 已知损耗日信任方程 |
| 5 | `actual_stock_qty > eq` | `actual_stock_qty` | `eq - actual` (负=盘盈) | 系统快照盘盈检测 |
| 6 | 其他 | `eq` | 0 | 正常 |

> **v0.10 修复**: is_counted 从仅人工盘点(`created_by != '系统'`)扩展到包含系统快照(`actual>0`)。分支5新增盘盈检测：系统记录的实盘超过方程计算值时自动识别盘盈。

### 金额派生

所有库存和损耗金额统一用 `effective_unit_cost` 定价：

```
end_stock_amt      = end_stock_qty × effective_unit_cost
unknow_lost_amt    = unknow_lost_qty × effective_unit_cost
know_lost_amt      = know_lost_qty × effective_unit_cost

lost_qty = know_lost_qty + unknow_lost_qty
lost_amt = know_lost_amt + unknow_lost_amt
```

流金额：

```
receive_qty/amt   = self_receive_qty/amt (passthrough, 源表值) ← A4修复
compose_in_amt    = 来自 sku_cost 加工关系配方推算（成品=Σ(raw_qty/yield_qty×raw_euc)）
compose_out_amt   = 来自 sku_cost 价值守恒计算（compose_out_qty × base_euc）
sale_qty/amt      = passthrough from wide
```

> **A4修复**: receive_amt 直接使用 receive_sale_di 源值，不再用 euc 重算。

### SCM金融字段

```
out_stock_pay_amt       = outstock_unit_price × original_outstock_qty
out_stock_pay_amt_notax = outstock_unit_price_notax × original_outstock_qty
out_stock_amt_cb        = outstock_cost_price × original_outstock_qty
return_stock_pay_amt_notax = return_unit_price_notax × return_stock_qty
expect_outstock_amt     = order_unit_price × store_order_qty
purchase_weight         = order_qty_payean × outstock_unit_price
scm_promotion_amt_total = passthrough from wide (源表值)
```

### 浮点精度保护

```python
df['end_stock_qty']   = np.round(end_qty, 6)
df['unknow_lost_qty'] = np.round(unknow_qty, 6)
```

四舍五入到 6 位小数，消除 1e-15 量级的浮点误差。

### 输出字段 (35列)

#### 基础标识
`store_id`, `business_date`, `article_id`, `day_clear`

#### 四流入
`receive_qty`, `receive_amt`, `bom_in_qty`, `bom_in_amt`, `compose_in_qty`, `compose_in_amt`

#### 三流出
`bom_out_qty`, `bom_out_amt`, `compose_out_qty`, `compose_out_amt`

#### 销售
`sale_qty`, `sale_amt`

#### 损耗
`know_lost_qty`, `know_lost_amt`, `unknow_lost_qty`, `unknow_lost_amt`, `lost_qty`, `lost_amt`

#### 库存
`init_stock_qty`, `init_stock_amt`, `end_stock_qty`, `end_stock_amt`

#### 成本
`effective_unit_cost`, `cost_source`

#### SCM
`out_stock_pay_amt`, `out_stock_pay_amt_notax`, `out_stock_amt_cb`, `return_stock_pay_amt_notax`, `scm_promotion_amt_total`, `expect_outstock_amt`, `purchase_weight`

---

## 五、profit.py — t_calc_profit (16字段)

**功能**: 门店毛利、SCM金融毛利、全链路毛利。

**粒度**: store_id × business_date × article_id × day_clear

### 数据来源

| 来源 | 字段 | 用途 |
|------|------|------|
| `t_calc_stock` | 全量库存+SCM字段 | 主力计算 |
| `t_atomic_wide` | original_price_sale_amt, allowance_amt, original_price, dc_original_price, outstock_cost_price_notax, return_cost_price_notax, original_outstock_qty, return_stock_qty | 价格字段补充 |

### 核心毛利公式 (v0.10)

```
profit_amt = sale_amt
           - receive_amt
           - bom_in_amt + bom_out_amt            ← A10修复: 包含BOM
           - compose_in_amt + compose_out_amt
           + end_stock_amt - init_stock_amt
```

> 损耗已通过库存方程反映在 end_stock 中（end减少→成本增加→利润减少），不再额外减去 lost_amt。

#### v0.11 FIX-019: 负库存钉零分支的透支成本扣减

非日清品 (`day_clear='1'`) 当库存方程 `eq<0`（超卖/超损）时，stock.py 走负库存保护：
`end` 钉零、透支量 `-eq` 转入 `unknow_lost`。但上面的核心公式只用 `end-init`，`end` 又被
钉高到 0 → 透支成本既不进 end 也不进利润 → **利润虚高**。FIX-019 在主公式后对精确命中
钉零分支的行扣回透支成本：

```python
clamp_mask = (day_clear=='1') & (eq_end_qty<-0.001) & (end_stock_qty<0.001) & (unknow_lost_qty>0.001)
profit_amt[clamp_mask] -= unknow_lost_amt[clamp_mask]
```

四条件精确锁定 stock.py 的 `elif eq<0` 分支，**不碰日清 `dc='0'`**（其 unknow 是软日清正常
残差/盘盈，扣了会双重惩罚），`unknow_qty>0` 守卫排除 is_counted 实盘=0 的盘盈角落。
效果：6/18–22 总毛利 FM−QDM 从 +2,129(+18.9%) 降至 +707(+6.3%)。详见
[FIX-019](../docs/fixes/FIX-019-negative-stock-clamp-cost.md)。

### 销售成本 (v0.10 统一公式)

```
日清/非日清统一: sale_cost_amt = sale_qty × effective_unit_cost
```

日清差异仅在 stock.py 端体现（end强制=0 → 残差转unknow_lost），不影响 sale_cost 公式。

### 预期毛利额 (原价口径)

```
pre_profit_amt = original_price_sale_amt - sale_qty × effective_unit_cost
```

### 补贴后毛利

```
allowance_amt_profit = sale_amt - receive_amt + allowance_amt + end_stock_amt - init_stock_amt
```

### SCM 金融毛利

```
out_stock_amt_cb_notax  = outstock_cost_price_notax × original_outstock_qty
return_stock_amt_cb_notax = return_cost_price_notax × return_stock_qty

scm_fin_article_income  = |out_stock_pay_amt_notax| - |return_stock_pay_amt_notax|
scm_fin_article_cost    = |out_stock_amt_cb_notax| - |return_stock_amt_cb_notax|
scm_fin_article_profit  = scm_fin_article_income - scm_fin_article_cost
```

### 全链路毛利

```
full_link_article_profit = profit_amt + scm_fin_article_income - scm_fin_article_cost
```

### 预期销售额与理论进货额

```
pre_sale_amt       = lost_qty × original_price + original_price_sale_amt
pre_inbound_amount = receive_qty × dc_original_price
```

### 输出字段 (16列)

| 字段 | 公式 | 说明 |
|------|------|------|
| `profit_amt` | 见核心公式 | 门店毛利额 (v0.10唯一毛利口径) ← A11修复 |
| `sale_cost_amt` | 见销售成本公式 | 销售成本 |
| `pre_profit_amt` | `original_price_sale_amt - expected_cost` | 预期毛利额 (原价口径) |
| `allowance_amt_profit` | `sale - receive + allowance + end - init` | 补贴后毛利 |
| `scm_fin_article_income` | `|out_pay_notax| - |return_pay_notax|` | SCM应收 |
| `scm_fin_article_cost` | `|out_cb_notax| - |return_cb_notax|` | SCM成本 |
| `scm_fin_article_profit` | `income - cost` | SCM金融毛利 |
| `full_link_article_profit` | `profit + scm_income - scm_cost` | 全链路毛利 |
| `pre_sale_amt` | `lost_qty × original_price + original_price_sale_amt` | 预期销售额 |
| `pre_inbound_amount` | `receive_qty × dc_original_price` | 理论进货额 |
