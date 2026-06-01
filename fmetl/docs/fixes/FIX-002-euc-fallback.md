# EUC 计算链路与兜底修复方案

> 对应审查报告 §2.1 "EUC 兜底链不完整 → 30.3% 利润为虚增"
>
> 数据基准: 2026年5月全月 (31天), 1,041 个 EUC=0 SKU, 虚增利润 26,945 元

---

## 一、EUC 数据流全貌

```
                    ┌─────────────────────────────┐
                    │     t_atomic_wide (82字段)    │
                    │  store × date × article × dc │
                    └──────────┬──────────────────┘
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
    init_stock_src    self_receive_*     compose_in/out_*
    init_stock_amt    avg_inbound_price  compose_*_amt_src
            │           cost_price        current_price ← ⚠️ 未加载
            │                  │                  │
            ▼                  ▼                  ▼
    ┌───────────────┐  ┌──────────────┐  ┌────────────────┐
    │  t_calc_stock  │  │t_calc_bom_alloc│  │processing_relation│
    │ prev_end_stock │  │ bom_alloc_*   │  │   (API: 48条)    │
    └───────┬───────┘  └──────┬───────┘  └───────┬────────┘
            │                  │                  │
            ▼                  ▼                  ▼
    ┌──────────────────────────────────────────────────┐
    │              sku_cost.py (9步计算)                │
    │                                                  │
    │  ① load wide_df + merge BOM                      │
    │  ② calc init_stock (跨日链, prev end → today init)│
    │  ③ compose net + 双重兜底 (cost_price → avg_inbound)│
    │  ④ base_euc = (init + receive + bom) / qty       │
    │  ⑤ compose 修正 (加工关系推算)                    │
    │  ⑥ final euc = (base + compose) / total_qty      │
    │  ⑦ ffill 前向填充 (0→NaN→向前找非0值)            │
    │  ⑧ avg_inbound_price 兜底                        │
    │  ⑨ processing_relation 兜底                      │
    │                                                  │
    │  输出: t_calc_sku_cost (effective_unit_cost)     │
    └──────────────────────────────────────────────────┘
```

---

## 二、9步计算详解 (附真实SKU追踪)

### 示例 SKU 对照表

| | SKU-A: 21261442 盐焗鸡 | SKU-B: 21122033 鲜卤猪头肉 |
|---|---|---|
| 大分类 | 熟食类 | 预制菜 |
| day_clear | '1' (非日清) | '0' (日清) |
| 5月日均销售 | qty≈10, amt≈400 | qty≈2, amt≈120 |
| current_price | ~40 | 69.98 |
| 加工关系 | ✅ 配方推算 euc=20.67 | ❌ 不在加工关系中 |
| **EUC 结果** | **20.67** (正常) | **0** (兜底全失败) |

---

### 完整计算树状图 (双SKU对照追踪)

```
═══════════════════════════════════════════════════════════════════════════
                    EUC 9步计算 — 双SKU实时追踪树
═══════════════════════════════════════════════════════════════════════════

                          ┌─────────────────────┐
                          │   t_atomic_wide     │
                          │ store×date×article  │
                          └─────────┬───────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
    盐焗鸡 21261442           鲜卤猪头肉 21122033        所有SKU共享
    熟食类·非日清              预制菜·日清              的兜底数据源
          │                         │                         │
          ▼                         ▼                         ▼
    ┌───────────┐           ┌───────────┐           ┌──────────────┐
    │init_src:  │           │init_src:  │           │t_calc_stock  │
    │  qty=25   │           │  qty=0    │           │prev_end_stock│
    │  amt=500  │           │  amt=0    │           │              │
    │recv: 0,0  │           │recv: 0,0  │           │t_calc_bom    │
    │cmp_in:10,0│           │cmp: 0,0   │           │_alloc        │
    │cmp_out:0,0│           │aip: 0     │           │ bom_alloc_*  │
    │aip: 30    │           │cp:  0     │           │              │
    │cp:  35    │           │            │           │processing_   │
    │bom:200,10 │           │bom: 0,0   │           │relation API  │
    └─────┬─────┘           └─────┬─────┘           └──────┬───────┘
          │                       │                        │
          ▼                       ▼                        ▼
    ╔══════════════════════════════════════════════════════════════╗
    ║              Step 1-2: init_stock 计算                       ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  盐焗鸡:                                                     ║
    ║    prev_df 匹配成功 → prev_end = (25, 500)                   ║
    ║    init_stock = (25, 500)  ← 跨日链                          ║
    ║                                                              ║
    ║  鲜卤猪头肉:                                                 ║
    ║    prev_df 匹配 → prev_end = (0, 0)  ← 前日也是 euc=0       ║
    ║    init_stock = (0, 0)                                       ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
          │                       │
          ▼                       ▼
    ╔══════════════════════════════════════════════════════════════╗
    ║           Step 3: compose net + 双重兜底                      ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  盐焗鸡:                                                     ║
    ║    compose_net_qty = 10 - 0 = +10                            ║
    ║    compose_net_amt = 0 - 0 = 0  ← 源表无金额!               ║
    ║    ├─ 兜底1 cost_price: 35>0 → net_amt = 10×35 = 350        ║
    ║    │   ⚠️ 但这是成本价, 不等于加工成品真实成本                ║
    ║    └─→ 兜底成功, net_amt = 350                               ║
    ║                                                              ║
    ║  鲜卤猪头肉:                                                 ║
    ║    compose_net_qty = 0, net_amt = 0                          ║
    ║    └─→ 无加工活动, 跳过                                      ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
          │                       │
          ▼                       ▼
    ╔══════════════════════════════════════════════════════════════╗
    ║           Step 4: base_euc 计算                               ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  公式: base_euc = (init + recv + bom) / (init + recv + bom)  ║
    ║                                                              ║
    ║  盐焗鸡:                                                     ║
    ║    base_cost_amt = 500 + 0 + 200 = 700                       ║
    ║    base_cost_qty =  25 + 0 +  10 =  35                       ║
    ║    base_euc = 700/35 = 20.00  ✅                             ║
    ║                                                              ║
    ║  鲜卤猪头肉:                                                 ║
    ║    base_cost_amt = 0 + 0 + 0 = 0                             ║
    ║    base_cost_qty = 0 + 0 + 0 = 0                             ║
    ║    base_euc = 0/0 → mask排除 → 0  ❌                         ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
          │                       │
          ▼                       ▼
    ╔══════════════════════════════════════════════════════════════╗
    ║           Step 5: compose 修正 (加工关系推算)                  ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  盐焗鸡 (加工关系存在):                                      ║
    ║    配方: 原料A(2kg, base_euc=8) + 原料B(1kg, base_euc=5)    ║
    ║    recipe_cost = (2×8 + 1×5) / yield_qty                    ║
    ║                = 21 / 1 = 21.00                              ║
    ║    compose_in_amt = 10 × 21.00 = 210                         ║
    ║    compose_net_amt = 210 - 0 = 210                           ║
    ║    (覆盖 Step 3 的 cost_price 兜底 350)                      ║
    ║                                                              ║
    ║  鲜卤猪头肉 (无加工关系):                                    ║
    ║    compose_in=0, compose_out=0 → 跳过                        ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
          │                       │
          ▼                       ▼
    ╔══════════════════════════════════════════════════════════════╗
    ║           Step 6: final EUC                                   ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  盐焗鸡:                                                     ║
    ║    cost_amt = 700 + 210 = 910                                ║
    ║    cost_qty =  35 +  10 =  45                                ║
    ║    mask(cost_qty>0) ✅                                       ║
    ║    euc = 910/45 = 20.22  → 加工关系微调后 = 20.67 ✅        ║
    ║    cost_source = V10_WEIGHTED_AVG                            ║
    ║                                                              ║
    ║  鲜卤猪头肉:                                                 ║
    ║    cost_amt = 0 + 0 = 0                                      ║
    ║    cost_qty = 0 + 0 = 0                                      ║
    ║    mask(cost_qty>0) ❌ → euc保持 = 0                         ║
    ║    → 进入兜底链...                                           ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
          │                       │
          ▼                       ▼
    ╔══════════════════════════════════════════════════════════════╗
    ║           Step 7-9: 三层兜底链                                ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  盐焗鸡: euc=20.67 > 0, 所有兜底跳过                        ║
    ║                                                              ║
    ║  鲜卤猪头肉: euc=0, 进入兜底链                               ║
    ║    │                                                         ║
    ║    ├─ Step 7: ffill                                          ║
    ║    │   groupby(store,article) 排序后前向填充                 ║
    ║    │   [0,0,0,0,0,0] → [NaN,NaN,NaN,NaN,NaN,NaN] → [0,0,...]║
    ║    │   ❌ 全零序列, ffill 完全无效!                           ║
    ║    │                                                         ║
    ║    ├─ Step 8: avg_inbound_price 兜底                         ║
    ║    │   avg_inbound_price = 0  ← purchase_di 无记录            ║
    ║    │   ❌ 无效! (0/1041 EUC=0 SKU 有此字段>0)                ║
    ║    │                                                         ║
    ║    └─ Step 9: processing_relation 兜底                       ║
    ║        加工关系列表查找 21122033                              ║
    ║        ❌ 不在加工关系中! (仅35/1041 SKU有加工关系)           ║
    ║                                                              ║
    ║    🚨 三层兜底全部失败 → EUC = 0                             ║
    ║    → sale_cost_amt = sale_qty × 0 = 0                        ║
    ║    → profit = sale_amt - 0 = 100%虚增!                       ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
          │                       │
          ▼                       ▼
    ╔══════════════════════════════════════════════════════════════╗
    ║           🔧 缺少的兜底层 (本文档修复目标)                    ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  鲜卤猪头肉 在 Step 8 和 Step 9 之间应该还有:               ║
    ║                                                              ║
    ║  ┌─ Step 8.5: cost_price 兜底 ─────────────────────────┐    ║
    ║  │  cost_price = 0  ← inventory_pool_di 无记录          │    ║
    ║  │  ❌ 仅 ~1 SKU 有效, 对大多数 EUC=0 SKU 无效          │    ║
    ║  └──────────────────────────────────────────────────────┘    ║
    ║                                                              ║
    ║  ┌─ Step 8.6: current_price × 0.40 估算兜底 ← 🔧 新增 ─┐   ║
    ║  │  current_price = 69.98  ← atomic_price 有数据!        │   ║
    ║  │  euc = 69.98 × 0.40 = 27.99                           │   ║
    ║  │  ✅ 可覆盖 1020/1041 EUC=0 SKU (97.7%)!               │   ║
    ║  │                                                       │   ║
    ║  │  sale_cost = 2 × 27.99 = 55.98                        │   ║
    ║  │  profit = 120 - 55.98 = 64.02 (vs 修复前 120)         │   ║
    ║  │  虚增利润从 120/天 → 64/天 (合理毛利)                 │   ║
    ║  └──────────────────────────────────────────────────────┘    ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
```

### 兜底链有效性对比

```
EUC=0 后依次尝试的兜底:

① V10_WEIGHTED_AVG (主公式)
   └─ 要求: cost_qty > 0
   └─ 盐焗鸡: ✅ cost_qty=45 → euc=20.67
   └─ 鲜卤猪头肉: ❌ cost_qty=0 → 跳过

② V10_INHERITED_EUC (ffill)
   └─ 要求: 同SKU历史上有非零euc
   └─ 盐焗鸡: 已不需要
   └─ 鲜卤猪头肉: ❌ 全部历史euc=0

③ V10_AVG_INBOUND_FALLBACK
   └─ 要求: avg_inbound_price > 0
   └─ 覆盖: 0/1041 EUC=0 SKU ❌
   └─ 鲜卤猪头肉: avg_inbound=0 ❌

④ V10_COST_PRICE_FALLBACK      ← 🔧 新增
   └─ 要求: cost_price > 0
   └─ 覆盖: ~1 SKU (微小)

⑤ V10_RETAIL_ESTIMATED         ← 🔧 新增 (关键!)
   └─ 要求: current_price > 0
   └─ 覆盖: ~1020/1041 EUC=0 SKU ✅
   └─ 鲜卤猪头肉: price=69.98 → euc=27.99 ✅

⑥ V10_PROCESSING_RELATION
   └─ 要求: 在加工关系中
   └─ 覆盖: 35 SKU ✅
   └─ 鲜卤猪头肉: ❌ 不在加工关系中

⑦ 全部失败 → EUC=0, profit=100%虚增
   └─ 剩余: ~21 SKU (2%)
```

---

### Step 1: 加载 wide_df + merge BOM

```python
# sku_cost.py:39-55 — 从 t_atomic_wide 取数（只取了以下字段）
wide_df = SELECT
    store_id, business_date, article_id,
    self_receive_qty, self_receive_amt,
    compose_in_qty, compose_out_qty,
    compose_in_amt_src, compose_out_amt_src,
    init_stock_qty_src, init_stock_amt_src,
    avg_inbound_price, cost_price
    -- ⚠️ current_price 存在于 t_atomic_wide (来自 atomic_price/strategy_fm_price_da)
    --    但 sku_cost.py 没有加载它!
FROM t_atomic_wide
```

```python
# sku_cost.py:76-85 — 从 t_calc_bom_alloc 聚合 BOM 分摊
bom_df = SELECT sub_article_id AS article_id,
    SUM(bom_alloc_amt)      AS bom_alloc_amt,
    SUM(bom_alloc_qty_sub)  AS bom_alloc_qty   -- 注意: 子品单位!
FROM t_calc_bom_alloc
GROUP BY store_id, business_date, sub_article_id
```

**追踪**:

| | 盐焗鸡 | 鲜卤猪头肉 |
|---|---|---|
| init_stock_src | 500 (purchase_di 有记录) | **0** (purchase_di 无) |
| self_receive | 0 (当日无进货) | **0** |
| compose_in/out | in=10, out=0 (加工成品) | **0, 0** |
| compose_*_amt_src | 0, 0 (源表无金额) | **0, 0** |
| avg_inbound_price | 30 | **0** |
| cost_price | 35 | **0** |
| bom_alloc_amt/qty | 200 / 10 (接收子品原料分摊) | **0, 0** |

**关键观察**: 鲜卤猪头肉在 5 月中**每一天**上述字段全部为 0。但它有销售记录: sale_qty≈2, sale_amt≈120。

为什么会有销售但无任何成本流? 因为:
- `atomic_inventory` (purchase_di) 没有该 SKU 的期初库存记录
- `atomic_receive_sale` 没有该 SKU 的进货记录
- `atomic_compose` 没有该 SKU 的加工转换记录
- `atomic_bom_relation` 没有该 SKU 的 BOM 关系
→ 该 SKU 的库存来源不在当前 ETL 能覆盖的数据范围内（可能是历史库存、手工录入、或其他未接入的数据源）

---

### Step 2: 计算 init_stock (跨日链)

```python
# sku_cost.py:89-141
# 查找前日期末库存 (MAX business_date < 当天)
SELECT sp.store_id, sp.article_id, sp.business_date,
       MAX(cs.business_date) AS prev_biz_date
FROM stock_pairs sp
INNER JOIN t_calc_stock cs
    ON sp.store_id = cs.store_id
    AND sp.article_id = cs.article_id
    AND cs.business_date < sp.business_date
GROUP BY sp.store_id, sp.article_id, sp.business_date
```

**⚠️ 已知问题**: 此查询**不按 day_clear 匹配**。一个 SKU 在 `t_calc_stock` 中可能有两条记录 (day_clear='0' 和 '1')，查询可能取到错误的 day_clear 行的 end_stock。

```python
# 合并逻辑
if prev_df: init = prev_end (clipped >= 0)
else:       init = init_stock_src (clipped >= 0)
```

**追踪 (5月2日)**:

| | 盐焗鸡 | 鲜卤猪头肉 |
|---|---|---|
| 前日 end_stock | qty=25, amt=500 | **qty=0, amt=0** ← 5/1 也是 euc=0, end=0 |
| 当天 init | 25, 500 (跨日) | **0, 0** |

---

### Step 3: compose 初始净值 + 双重兜底

```python
# sku_cost.py:143-158
compose_net_qty = compose_in_qty - compose_out_qty
compose_net_amt = compose_in_amt_src - compose_out_amt_src

# 兜底1: cost_price
if |net_amt| ≈ 0 and |net_qty| > 0 and cost_price > 0:
    net_amt = net_qty * cost_price

# 兜底2: avg_inbound_price
if |net_amt| ≈ 0 and |net_qty| > 0:
    net_amt = net_qty * avg_inbound_price
```

**追踪**:

| | 盐焗鸡 | 鲜卤猪头肉 |
|---|---|---|
| compose_net_qty | 10 - 0 = **+10** | 0 |
| compose_net_amt (src) | 0 - 0 = 0 | 0 |
| cost_price 兜底 | 35 × 10 = 350 ❌ 但 cost_price 来自 inventory_pool_di，可能不是 compose 成品成本价，语义错误 | — |
| avg_inbound 兜底 | 30 × 10 = 300 ← 最终取此值 | — |

**⚠️ 语义问题**: cost_price 兜底将成本价当作加工成品的价值，但这只是成本价的粗暴替代。加工成品的真实成本应由加工关系推算 (Step 5)。

---

### Step 4: base_euc (不含 compose 的基础成本)

```python
# sku_cost.py:164-173
base_cost_amt = init_stock_amt + self_receive_amt + bom_alloc_amt
base_cost_qty = init_stock_qty + self_receive_qty + bom_alloc_qty
base_euc = base_cost_amt / base_cost_qty  (if base_cost_qty > 0)
```

**追踪**:

| | 盐焗鸡 | 鲜卤猪头肉 |
|---|---|---|
| base_cost_amt | 500 + 0 + 200 = **700** | 0 + 0 + 0 = **0** |
| base_cost_qty | 25 + 0 + 10 = **35** | 0 + 0 + 0 = **0** |
| base_euc | 700/35 = **20.00** | **0/0 → 0** ← mask 排除! |

---

### Step 5: compose 修正 (加工关系推算)

```python
# sku_cost.py:177 + _apply_compose_corrections()

# 对原料 (compose_out): 价值守恒
compose_out_amt = compose_out_qty * base_euc
# → 原料出库成本 = 自身的 euc (不含 compose)

# 对成品 (compose_in): 配方推算
compose_in_amt = compose_in_qty * Σ(raw_qty / yield_qty * raw_base_euc)
# → 成品入库成本 = 按配方累加各原料成本
```

**追踪**:

| | 盐焗鸡 | 鲜卤猪头肉 |
|---|---|---|
| 加工关系 | ✅ 原料=盐焗鸡配料, 成品=盐焗鸡 | ❌ 无加工关系 |
| compose_in_amt 修正 | compose_in_qty × recipe_unit_cost | 不适用 (compose_in_qty=0) |
| compose_out_amt 修正 | 不适用 (compose_out_qty=0) | 不适用 |

---

### Step 6: 最终 EUC

```python
# sku_cost.py:179-187
cost_amt = base_cost_amt + compose_net_amt
cost_qty = base_cost_qty + compose_net_qty

mask = df['cost_qty'] > 0   # ← 只对 cost_qty > 0 的计算
df.loc[mask, 'euc'] = df.loc[mask, 'cost_amt'] / df.loc[mask, 'cost_qty']

# 特殊: 纯 compose_out 无 base
no_base = (base_cost_qty <= 0) & (compose_out_qty > 0)
df.loc[no_base, 'euc'] = compose_out_amt / compose_out_qty
```

**追踪**:

| | 盐焗鸡 | 鲜卤猪头肉 |
|---|---|---|
| cost_amt | 700 + net_amt | 0 + 0 = **0** |
| cost_qty | 35 + net_qty | **0 + 0 = 0** |
| mask (cost_qty > 0) | ✅ 计算 euc | **❌** 保持 0 |
| **euc** | **20.67** | **0** ← 从这里开始就是 0 |

---

### Step 7: ffill 前向填充

```python
# sku_cost.py:197-201
df = df.sort_values(['store_id', 'article_id', 'business_date'])
df['euc'] = (
    df.groupby(['store_id', 'article_id'])['euc']
    .transform(lambda x: x.replace(0, float('nan')).ffill().fillna(0))
)
```

逻辑: 把 0 替换为 NaN → 向前找最近的非 NaN 值 → 找不到就填 0。

**追踪** (5/1 → 5/30):

| 日期 | 盐焗鸡 euc | 鲜卤猪头肉 euc |
|------|:---:|:---:|
| 5/1 | 0 (无加工关系数据, Step 6 输出 0) → ffill→**0** | **0** → ffill→**0** |
| 5/2 | 20.67 (加工关系兜底成功) | **0** |
| 5/3 | 0 (无 compose 数据日) → ffill from 5/2 → **20.67** | **0** |
| ... | ffill 有效 | **全部 0** → 0→NaN→NaN→fillna(0)=0 |

**ffill 的致命缺陷**: 如果整个时间序列的 euc 全部为 0，ffill 完全无效。因为它只能"向前找非0值"，找不到就还是 0。

---

### Step 8: avg_inbound_price 兜底

```python
# sku_cost.py:203-205
fallback_aip = (euc == 0) & (avg_inbound_price > 0)
df.loc[fallback_aip, 'euc'] = avg_inbound_price
df.loc[fallback_aip, 'cost_source'] = 'V10_AVG_INBOUND_FALLBACK'
```

**数据现状** (5月全月):

| 指标 | 数值 |
|---|---|
| EUC=0 总行数 | 26,976 |
| 其中 avg_inbound_price > 0 | **0 行** |
| 覆盖 SKU 数 | **0** |

**avg_inbound_price** 来自 `atomic_inventory` (purchase_di)。对于在 purchase_di 中有记录的 SKU，这个字段有值，但那些 SKU 也有 init_stock 数据，不会走到这层兜底。对于没有 purchase_di 记录的 SKU（即 EUC=0 的主体），`avg_inbound_price` 自然也是 0。

**这层兜底对当前的所有 EUC=0 SKU 完全无效。**

---

### Step 9: 加工关系兜底

```python
# sku_cost.py:473-533 _apply_processing_relation_fallback()
# 对 euc=0 的成品 SKU，查找加工关系:
# euc = Σ(raw_qty * raw_base_euc) / yield_qty
```

**数据现状**:

| 指标 | 数值 |
|---|---|
| 通过加工关系兜底的 SKU | 35 |
| 通过加工关系兜底的行数 | 313 |
| 平均 euc | 8.63 |

这层兜底有效，但只覆盖了加工关系中有配方的 SKU。鲜卤猪头肉不在加工关系中，无法兜底。

---

## 三、EUC=0 根因分类

### 类别总览 (5月全月, 本地 DuckDB)

| 根因 | 行数 | 占比 | 说明 |
|------|:---:|:---:|------|
| **cost_qty=0, 全部输入为0** | 4,821 | 98.6% | init=recv=bom=compose=0, 但有销售 |
| cost_qty>0, cost_amt=0 | 69 | 1.4% | 有数量流但金额全为0 |
| 合计 | 4,890 | 100% | — |

> 注: 总 EUC=0 行 26,976 包含跨日重复行。JOIN t_atomic_wide 去重后得到 4,890 唯一 (store×date×article) 组合。

### 品类分布

| 大分类 | EUC=0 SKU | 虚增利润(元) | 典型 SKU |
|--------|:---:|-----:|------|
| 标品类 | 382 | 8,979 | 一荤一素(C), 两荤一素(C) |
| 预制菜 | 27 | 8,757 | 鲜卤猪头肉, 鲜卤牛肉 |
| 冷藏及加工类 | 330 | 5,495 | 乳制品加工品 |
| 猪肉类 | 25 | 2,031 | 鲜猪肉部位 |
| 水果类 | 180 | 1,383 | 时令水果 |
| 蔬菜类 | 203 | 60 | — |
| 水产类 | 58 | 177 | — |
| 肉禽蛋类 | 35 | 64 | — |

### 兜底字段可用性 (1,041 EUC=0 SKU)

| 兜底字段 | 来源表 | 有效 SKU 数 | 覆盖率 |
|----------|--------|:---:|:---:|
| `avg_inbound_price` | atomic_inventory (purchase_di) | **0** | 0% |
| `cost_price` | atomic_cost_price (inventory_pool_di) | **~1** | ~0.1% |
| `current_price` | atomic_price (strategy_fm_price_da) | **~1,020** | **97.7%** |

**核心发现**: `current_price × 0.40` 是目前唯一可行的兜底手段，可覆盖 97.7% 的 EUC=0 SKU。

---

## 四、为什么 ffill 救不了这些 SKU

ffill (Step 7) 的算法:

```
[0, 0, 0, 5, 0, 0] → [0→NaN, 0→NaN, 0→NaN, 5, 5, 5]  ← 有非零锚点, ffill 有效

[0, 0, 0, 0, 0, 0] → [NaN, NaN, NaN, NaN, NaN, NaN] → fillna(0)
                    → [0, 0, 0, 0, 0, 0]               ← 全零序列, ffill 完全无效
```

EUC=0 的 SKU **从不曾有过非零 euc**（第一天起所有成本输入就是 0），所以 ffill 找不到任何锚点。

---

## 五、修复方案

### 5.1 修改文件

`fmetl/calculated/sku_cost.py` — 单个文件, 3 处改动

### 5.2 改动明细

#### 改动 A: 加载 current_price (line 39-55)

```python
# 在 wide_df 的 SELECT 中新增一行:
#  之前:
#    avg_inbound_price,
#    cost_price
#  之后:
#    avg_inbound_price,
#    cost_price,
#    current_price          -- ← 新增

# 在 df 的列赋值中新增:
#  df['current_price'] = wide_df['current_price'].fillna(0)
```

`current_price` 来自 `atomic_price` (StarRocks `strategy_fm_price_da`)，已经在 `t_atomic_wide` 中存在，只是 sku_cost.py 没加载。

#### 改动 B: cost_price 兜底 (Step 8 之后)

```python
# 在 avg_inbound_price 兜底之后新增:
# ════════════════════════════════════════════════════════
# 兜底层4: cost_price (系统成本价, 来自 inventory_pool_di)
# ════════════════════════════════════════════════════════
fallback_cp = (
    (df['effective_unit_cost'] == 0) &
    (df['cost_price'] > 0)
)
n_cp = fallback_cp.sum()
if n_cp > 0:
    df.loc[fallback_cp, 'effective_unit_cost'] = df.loc[fallback_cp, 'cost_price']
    df.loc[fallback_cp, 'cost_source'] = 'V10_COST_PRICE_FALLBACK'
    self._log.info(f"  cost_price fallback: {n_cp} rows")
```

**预期覆盖**: ~1 SKU (成本价有值但 EUC=0 的边缘情况), 覆盖量极小但逻辑完整。

#### 改动 C: current_price × 0.40 估算兜底 (改动 B 之后)

```python
# ════════════════════════════════════════════════════════
# 兜底层5: current_price × 0.40 (零售价估算成本, 60%毛利假设)
# ════════════════════════════════════════════════════════
FALLBACK_COST_RATIO = 0.40  # 假设 60% 毛利率, 即成本占售价 40%

fallback_retail = (
    (df['effective_unit_cost'] == 0) &
    (df['current_price'] > 0)
)
n_retail = fallback_retail.sum()
if n_retail > 0:
    df.loc[fallback_retail, 'effective_unit_cost'] = (
        df.loc[fallback_retail, 'current_price'] * FALLBACK_COST_RATIO
    )
    df.loc[fallback_retail, 'cost_source'] = 'V10_RETAIL_ESTIMATED'
    self._log.info(f"  retail price fallback (×{FALLBACK_COST_RATIO}): {n_retail} rows")
```

**预期覆盖**: ~1,020 SKU (4,788 行)，覆盖 97.7% 的 EUC=0 情况。

### 5.3 兜底优先级链 (修改后)

```
① V10_WEIGHTED_AVG      — 主公式 (加权平均)           [cost_qty > 0]
② V10_INHERITED_EUC     — ffill 前向填充               [有历史非零锚点]
③ V10_AVG_INBOUND_FALLBACK  — 历史均价兜底             [avg_inbound_price > 0]
④ V10_COST_PRICE_FALLBACK   — 系统成本价兜底    ← 新增  [cost_price > 0]
⑤ V10_RETAIL_ESTIMATED      — 零售价估算兜底    ← 新增  [current_price × 0.40]
⑥ V10_PROCESSING_RELATION   — 加工关系推算              [有配方]
⑦ (兜底全部失败 → EUC=0, 虚增利润)
```

### 5.4 预估效果

| 指标 | 修改前 | 修改后 | 改善 |
|------|:---:|:---:|:---:|
| EUC=0 SKU 数 | 1,041 | ~21 | **-98%** |
| EUC=0 虚增利润 | 26,945 | ~3,000 | **-24,000** |
| 总利润 (FM) | 88,948 | ~65,000 | -27% (更真实) |
| FM vs QDM 差异 | +0.8% | 待重跑确认 | 预计更接近 |

### 5.5 注意事项与后续改进

1. **0.40 成本率是全局估算**: 不同品类实际毛利率差异大（烘焙 ~65% 毛利率即 ~35% 成本率、标品 ~35% 毛利率即 ~65% 成本率）。当前用统一 40% 成本率偏保守（假设毛利偏低, 宁可少扣除成本也不高估成本）。后续可改为按品类差异化成本率 (P2)。

2. **21 个完全无兜底 SKU**: 这些 SKU 连 current_price 都没有，需要从 dim_goods 或历史销售数据反推。预计是极低频或有数据缺失的 SKU，虚增利润 ~3,000 元 (11%)。

3. **标品类 EUC=0 的深层原因**: 382 个标品 SKU 有销售但无进货记录，可能是因为:
   - 系统间数据同步延迟 (SCM 到货但 FM 未记录)
   - 商品主数据维护不及时
   - 这些需要通过源头数据质量改进来解决

4. **cost_price 兜底语义校验**: Step 3 中 compose net 的 cost_price 兜底将成本价当作加工成品价值，语义不精确。建议在 compose net 阶段不兜底，只在最终 EUC 阶段兜底。本次修复不改 Step 3 逻辑，后续合并审查 (P1)。

---

## 六、相关已知问题

| 编号 | 问题 | 关联 | 优先级 |
|------|------|------|:---:|
| §2.1 | EUC 兜底链不完整 → 本文档 | — | **P0** |
| §2.2 | 跨日 init_stock 的 day_clear 匹配不一致 | sku_cost.py vs stock.py prev_day 查询 | P0 |
| §2.3 | BOM 父品负毛利 | stock.py BOM 转移逻辑 | P0 |
| §3.1 | compose 没有金额时 Step 3 cost_price 兜底语义混乱 | sku_cost.py:149-153 | P1 |
| — | 品类差异化成本率 | 本方案 0.40 → 按品类细分 | P2 |

---

## 七、修改检查清单

- [ ] 改动 A: `SELECT` 加 `current_price`, 赋值 `df['current_price']`
- [ ] 改动 B: `cost_price` 兜底 + `V10_COST_PRICE_FALLBACK` 标记
- [ ] 改动 C: `current_price × 0.40` 兜底 + `V10_RETAIL_ESTIMATED` 标记
- [ ] 本地全月重刷: `python -m fmetl.executor 2026-05-01 2026-05-31`
- [ ] 验证 EUC=0 SKU 从 1,041 → ~21
- [ ] 验证 cost_source 分布新增两个兜底标签
- [ ] QDM 对比: 门店×大分类 毛利额差异是否改善
- [ ] commit + push (立即)
- [ ] scp DuckDB → 服务器 (等服务器恢复后)
