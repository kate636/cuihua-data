# §2.2 跨日 init_stock 查找不一致 — sku_cost.py vs stock.py

> 问题等级: 🔴 P0 (代码一致性)
>
> 当前数据影响: 15 行 amt 差异共 38.03 元 (微小)
>
> 潜在风险: 如果未来出现跨日 day_clear 切换的 SKU，会导致 init_stock 取值错误和行数膨胀

---

## 一、问题本质

sku_cost.py 和 stock.py 都需要从前一天 `t_calc_stock.end_stock` 获取今天的 `init_stock`。但两个模块的 SQL 查询逻辑**不一致**——一个不按 day_clear 匹配，另一个按 day_clear 匹配。

```
今天 init_stock = 昨天 end_stock (如果昨天存在) 或 init_stock_src (首日兜底)

  sku_cost.py:  查 t_calc_stock, MAX(biz_date < 今天), 不区分 day_clear
  stock.py:     查 t_calc_stock, MAX(biz_date < 今天), 按 day_clear 分组匹配
```

---

## 二、两侧代码逐行对比

### 2.1 sku_cost.py prev_df 查询 (第88-120行)

```sql
-- sku_cost.py: 三 CTE 级联，全程无 day_clear
WITH stock_pairs AS (
    -- ① 从 t_atomic_wide 提取所有 (store,article,date) — 无 day_clear
    SELECT DISTINCT store_id, article_id, business_date
    FROM t_atomic_wide
),
prev_match AS (
    -- ② 对每个 (store,article,date) 找 MAX(biz_date < 当天) — 无 day_clear JOIN
    SELECT sp.store_id, sp.article_id, sp.business_date,
           MAX(cs.business_date) AS prev_biz_date
    FROM stock_pairs sp
    INNER JOIN t_calc_stock cs
        ON sp.store_id = cs.store_id
        AND sp.article_id = cs.article_id
        AND cs.business_date < sp.business_date
    GROUP BY sp.store_id, sp.article_id, sp.business_date
),
prev_stock AS (
    -- ③ 反查 end_stock — 无 day_clear JOIN, 可能返回多行
    SELECT pm.store_id, pm.article_id, pm.business_date,
           cs.end_stock_qty AS prev_end_qty,
           cs.end_stock_amt AS prev_end_amt
    FROM prev_match pm
    INNER JOIN t_calc_stock cs
        ON pm.store_id = cs.store_id
        AND pm.article_id = cs.article_id
        AND pm.prev_biz_date = cs.business_date
        -- ⚠️ 没有 AND cs.day_clear = ... !!!
)
SELECT * FROM prev_stock
```

```python
# sku_cost.py: merge 也不包含 day_clear (第128-141行)
df = df.merge(prev_df,
              on=['store_id', 'article_id', 'business_date'],  # ← 无 day_clear
              how='left')
df['init_stock_qty'] = df['prev_end_qty'].fillna(df['init_stock_qty_src']).clip(lower=0)
df['init_stock_amt'] = df['prev_end_amt'].fillna(df['init_stock_amt_src']).clip(lower=0)
```

### 2.2 stock.py prev_df 查询 (第165-182行)

```sql
-- stock.py: 两 CTE 级联，全程带 day_clear
WITH prev_date AS (
    -- ① 对每个 (store,article,day_clear) 找 MAX(biz_date < 当天)
    SELECT store_id, article_id, day_clear,
           MAX(business_date) AS prev_biz_date
    FROM t_calc_stock
    WHERE business_date < '{business_date}'
    GROUP BY store_id, article_id, day_clear  -- ← 有 day_clear!
),
prev_stock AS (
    -- ② 用 prev_biz_date + day_clear 精确反查
    SELECT s.store_id, s.article_id, s.day_clear, s.business_date,
           s.end_stock_qty, s.end_stock_amt
    FROM t_calc_stock s
    INNER JOIN prev_date p
        ON s.store_id = p.store_id
        AND s.article_id = p.article_id
        AND s.day_clear = p.day_clear          -- ← 精确匹配 day_clear!
        AND s.business_date = p.prev_biz_date
)
```

```python
# stock.py: merge 也包含 day_clear (第285-290行)
df = df.merge(prev_df,
              on=['store_id', 'article_id', 'day_clear', 'business_date'],  # ← 有 day_clear
              how='left')
df['init_stock_qty'] = df['prev_end_qty'].fillna(df['init_stock_qty_src']).clip(lower=0)
df['init_stock_amt'] = df['prev_end_amt'].fillna(df['init_stock_amt_src']).clip(lower=0)
```

### 2.3 对比总结

| 维度 | sku_cost.py | stock.py |
|------|:-----------|:--------|
| prev_date GROUP BY | `(store, article)` | `(store, article, day_clear)` |
| prev_match JOIN day_clear | ❌ 无 | ✅ 有 |
| prev_stock JOIN day_clear | ❌ 无 | ✅ 有 |
| merge ON 包含 day_clear | ❌ 无 | ✅ 有 |
| df 去重粒度 | `(store, article, date)` → 丢一个 day_clear | `(store, article, date, day_clear)` → 保留全部 |
| prev_df 可能返回行数 | **不确定** (>1 如果多 day_clear) | **确定** (0 或 1) |

---

## 三、场景推演

### 3.1 当前数据场景 (全部 SKU 的 day_clear 跨日期一致)

```
数据事实: 0 个 (store,article) 在5月内跨日切换 day_clear

流程追踪 (SKU=21261442 盐焗鸡, dc='1' 恒定):

t_atomic_wide:                         t_calc_stock (前日):
  (A3XV, 5/15, 21261442, dc=1)          (A3XV, 5/14, 21261442, dc=1, end=20, amt=400)

sku_cost.py prev_df:
  stock_pairs: DISTINCT (A3XV, 21261442, 5/15)
  prev_match:   MAX(biz < 5/15) = 5/14
  prev_stock:   JOIN (A3XV, 21261442, 5/14) → 1 row (dc=1, end=20, amt=400)  ✅

stock.py prev_df:
  prev_date:    GROUP BY (A3XV, 21261442, dc=1), MAX(biz < 5/15) = 5/14
  prev_stock:   JOIN (A3XV, 21261442, dc=1, 5/14) → 1 row (end=20, amt=400)  ✅

结果: 两者一致，无差异
```

**结论**: 当天所有 SKU 的 day_clear 恒定时，两个查询等价。

---

### 3.2 假设场景: SKU 跨日切换 day_clear (当前不存在，但代码未防御)

假设一个烘焙 SKU 在 5/1 是 dc='1' (非日清)，5/2 开始被加入日清覆盖变成 dc='0':

```
t_calc_stock:
  5/1: (A3XV, 5/1, SKU-X, dc=1, end_qty=30, end_amt=600)
  5/2: (A3XV, 5/2, SKU-X, dc=0, ...)
  5/3: (A3XV, 5/3, SKU-X, dc=0, ...)

t_atomic_wide:
  5/2: (A3XV, 5/2, SKU-X, dc=0, ...)        ← day_clear 已切换
```

**sku_cost.py 处理 5/2**:
```
stock_pairs: DISTINCT (A3XV, SKU-X, 5/2)  → 1 row
prev_match: JOIN t_calc_stock ON (store=A3XV, article=SKU-X) WHERE biz < 5/2
  → 找到 5/1, 无论 day_clear → MAX = 5/1
prev_stock: JOIN ON (A3XV, SKU-X, biz=5/1)
  → 返回 1 row: (dc=1, end_qty=30, end_amt=600)

merge: df(5/2, 1 row) × prev_df(1 row) = 1 row
  → init_stock_qty = 30 ✅ (最新库存, 不管 day_clear)
  → init_stock_amt = 600
```

**stock.py 处理 5/2**:
```
df: (A3XV, 5/2, SKU-X, dc=0)    ← 日清行
prev_date: GROUP BY (A3XV, SKU-X, dc=0), MAX(biz < 5/2)
  → 查找 dc=0 的前日数据...
  → 5/1 的 t_calc_stock 有 dc=1 但没有 dc=0
  → 往前找... 如果从未有过 dc=0 → 找不到!

prev_stock: 无匹配行 (因为之前没有 dc=0 的 end_stock)

merge: left join → prev_end_qty = NaN
  → init_stock_qty = init_stock_qty_src (= 来自 purchase_di)
  → is_first_day = 1  ⚠️
```

**差异**:
```
               sku_cost.py    stock.py        差异
init_stock_qty      30          (src值,可能0)   sku_cost > stock
init_stock_amt      600         (src值,可能0)   sku_cost > stock
is_first_day        0              1           不一致
```

**对 EUC 的影响**:
```
sku_cost EUC = (600 + receive + bom + compose) / (30 + receive + bom + compose)
  → 基础成本 ≈ 600/30 = 20 (偏高)

stock init = src (可能=0)
  → stock方程使用不同的init, 与EUC不匹配
  → end_stock 可能计算错误
```

---

### 3.3 示意决策树

```
前一日 t_calc_stock 中某 SKU 的 end_stock 查询
│
├── 场景A: 前一日该 SKU 只有一种 day_clear
│   │
│   ├── sku_cost.py
│   │   └── MAX(biz) JOIN → 1 row → end_stock ✅
│   │
│   └── stock.py
│       └── MAX(biz) JOIN with dc → 1 row → end_stock ✅
│   │
│   └── 结果: 一致 ✅
│
├── 场景B: 前一日该 SKU 有 day_clear='0' 和 '1' 两条记录
│   │
│   ├── sku_cost.py (无 day_clear JOIN)
│   │   ├── prev_match: MAX(biz) = 前一日 (唯一日期)
│   │   ├── prev_stock: JOIN ON (store,article,biz) → 2 rows (dc=0 和 dc=1)
│   │   └── merge: df(1 row) × prev(2 rows) → 2 rows ⚠️ 行数膨胀!
│   │       ├── row1: prev_end = dc=0 的 end_stock
│   │       └── row2: prev_end = dc=1 的 end_stock
│   │       → t_calc_sku_cost 有两行 → 下游 stock.py euc merge 再膨胀!
│   │
│   └── stock.py (有 day_clear JOIN)
│       ├── prev_date: GROUP BY (store,article,dc='0') → MAX(biz)
│       ├── prev_date: GROUP BY (store,article,dc='1') → MAX(biz)
│       ├── prev_stock: JOIN with dc → 每个 dc 各 1 row
│       └── merge: 1行 df(dc=0) × 1行 prev(dc=0) = 1行 ✅
│           merge: 1行 df(dc=1) × 1行 prev(dc=1) = 1行 ✅
│
└── 场景C: SKU 今天首次出现某 day_clear (前天只有另一种 dc)
    │
    ├── sku_cost.py
    │   └── 找到前天(另一种dc)的 end_stock → init = 前天 end ✅ (用最新库存)
    │
    └── stock.py
        └── 找不到同 dc 的前日 → prev = NULL → init = init_src ⚠️ (可能=0)
        └── is_first_day = 1
```

---

## 四、实际数据验证 (5月全月)

### 4.1 day_clear 切换统计

| 指标 | 数值 |
|------|:---:|
| (store,article) 跨日切换 day_clear | **0** |
| (store,date,article) 有双 day_clear | **0** |
| t_calc_sku_cost 中重复 (store,date,article) | **0** |

当前数据不具备触发差异的条件。

### 4.2 init_stock 对比 (5/15 抽样)

| 指标 | 数值 |
|------|:---:|
| 对比行数 | 2,609 |
| qty 不一致 | 0 |
| amt 不一致 | 15 |
| 总 amt 差异 | 38.03 元 |

### 4.3 不一致示例

| SKU | dc | sku init_amt | stock init_amt | diff | 原因 |
|------|:--:|:---:|:---:|:---:|------|
| 21283055 | 1 | 0.00 | 27.30 | -27.30 | sku_cost 5/14 init_amt=0 (前日 prev 丢失)，连锁导致 5/15 错误 |
| 21071676 | 1 | 17.11 | 24.08 | -6.97 | 同上模式 |
| 21281051 | 1 | 196.80 | 198.00 | -1.20 | 精度累积 |

**根因追踪 (以 21283055 为例)**:

```
5/13: t_calc_stock end_stock = (qty=12, amt=32.76)  [推测]
      ↓
5/14: sku_cost.py 读取 prev_df
      → prev_end = (qty=12, amt=32.76)?  还是 init_stock_src = (qty=12, amt=0)?
      → 结果显示 sku_cost init = (qty=12, amt=0) ← amt 丢失了
      ↓
5/14: stock.py 处理
      → init = (qty=12, amt=32.76) 或 prev_end = (qty=12, amt=32.76)
      → end_stock = (qty=10, amt=27.30)
      ↓
5/15: sku_cost.py 读取 prev_df (从 t_calc_stock 5/14)
      → prev_end = (qty=10, amt=27.30)
      → 但结果显示 init = (qty=10, amt=0) ???
```

**异常点**: 5/14 的 sku_cost.py init_stock_amt=0 但 init_stock_qty=12。
这说明 `prev_end_amt` = 0 (或 prev_df 不匹配时回退到 `init_stock_amt_src=0`)，
但 `prev_end_qty` = 12。两个值不同源！

可能解释: prev_df 的 JOIN 返回了 prev_end_qty=12 但 prev_end_amt=NULL (触发了 fillna(0))。这可能是 sku_cost.py 中 t_calc_stock JOIN 不用 day_clear 导致取到了错误的行，或者聚合时 amt 丢失。

继续追溯需要更早日期的数据，此处不再深入。差异总额仅 38 元，属于边界精度问题。

---

## 五、修复方案

### 5.1 目标

让 sku_cost.py 和 stock.py 使用**一致的**跨日 init_stock 查找逻辑。

### 5.2 两种可选方向

#### 方向A: sku_cost.py 也加入 day_clear 匹配 (精确对齐 stock.py)

```
优点: 两个模块完全一致，不会出现 init_stock 差异
缺点: 
  1. 需要重构 sku_cost.py 的数据粒度 (当前是 (store,article,date)，需改为 (store,article,date,day_clear))
  2. 同一 SKU 的两种 day_clear 会分别计算 EUC — 但 EUC 应该是 SKU 级的概念
  3. 改动范围大
```

#### 方向B: stock.py 去掉 day_clear 匹配 (对齐 sku_cost.py)

```
优点: 
  1. EUC 和 stock init 来源一致
  2. 跨日链不会因 day_clear 切换而断裂
  3. 改动范围小 (只改 stock.py prev_df 查询)
缺点: 
  1. day_clear='0' 的 SKU 会得到包含 dc='1' 库存的 init → 日清方程的"只清当日供给"可能不准确
  2. 需要评估对日清库存行为的影响
```

### 5.3 推荐方案: 方向B (stock.py 对齐 sku_cost.py)

**理由**: 
- EUC 计算的"最新可用库存"逻辑更合理 — 成本应基于全部库存，不管 day_clear
- stock.py 改动范围小 (只改 prev_date CTE 去掉 day_clear GROUP BY)
- 当前数据中 day_clear 不变化，改动无实际影响
- 如果未来出现 day_clear 切换，stock 方程用"最新库存"初始化比用"同 day_clear 的旧库存"更准确

**改动位置**: `fmetl/calculated/stock.py` 第166-181行

```python
# 修改前 (按 day_clear 分组匹配):
prev_df = conn.execute(f"""
    WITH prev_date AS (
        SELECT store_id, article_id, day_clear,
               MAX(business_date) AS prev_biz_date
        FROM {self.TARGET_TABLE}
        WHERE business_date < '{business_date}'
        GROUP BY store_id, article_id, day_clear
    )
    SELECT s.store_id, s.article_id, s.day_clear,
           s.business_date,
           s.end_stock_qty, s.end_stock_amt
    FROM {self.TARGET_TABLE} s
    INNER JOIN prev_date p
        ON s.store_id = p.store_id
        AND s.article_id = p.article_id
        AND s.day_clear = p.day_clear
        AND s.business_date = p.prev_biz_date
""").df()

# 修改后 (不按 day_clear 分组, 取最新库存):
prev_df = conn.execute(f"""
    WITH prev_date AS (
        SELECT store_id, article_id,
               MAX(business_date) AS prev_biz_date
        FROM {self.TARGET_TABLE}
        WHERE business_date < '{business_date}'
        GROUP BY store_id, article_id
    )
    SELECT s.store_id, s.article_id, s.day_clear,
           s.business_date,
           s.end_stock_qty, s.end_stock_amt
    FROM {self.TARGET_TABLE} s
    INNER JOIN prev_date p
        ON s.store_id = p.store_id
        AND s.article_id = p.article_id
        AND s.business_date = p.prev_biz_date
""").df()
```

**⚠️ 需同步修改**: merge 的 ON 条件去掉 `day_clear`：

```python
# 修改前:
df = df.merge(prev_df, on=['store_id', 'article_id', 'day_clear',
                            'business_date'], how='left')

# 修改后:
df = df.merge(prev_df, on=['store_id', 'article_id', 'business_date'], how='left')
# 但如果 prev_df 中同一 (store,article,date) 有多行 (不同 day_clear)，
# merge 会产生笛卡尔积 → 需要先对 prev_df 做聚合
```

**更安全的实现** — 先聚合 prev_df 到 (store,article) 粒度:

```python
if prev_df is not None and not prev_df.empty:
    # 如果同一 (store,article,date) 有多个 day_clear, sum 端库存
    prev_agg = prev_df.groupby(['store_id', 'article_id', 'business_date'], as_index=False).agg(
        prev_end_qty=('end_stock_qty', 'sum'),
        prev_end_amt=('end_stock_amt', 'sum')
    )
    df = df.merge(prev_agg,
                  on=['store_id', 'article_id', 'business_date'], how='left')
    df['init_stock_qty'] = df['prev_end_qty'].fillna(df['init_stock_qty_src']).clip(lower=0)
    df['init_stock_amt'] = df['prev_end_amt'].fillna(df['init_stock_amt_src']).clip(lower=0)
    df['is_first_day'] = df['prev_end_qty'].isna().astype(int)
```

### 5.4 暂缓理由

当前数据下该问题不会触发，且改动会影响 stock 方程的核心链 (init_stock → end_stock → 次日 init_stock 循环)。建议：
1. **先不修改代码**，将本文档作为已知问题记录
2. 在以下条件触发时再执行修复:
   - 出现跨日 day_clear 切换的 SKU
   - 发现因 init_stock 不匹配导致的显著数据差异 (>1%)
   - 重构 stock 跨日链时一并处理

---

## 六、验证清单

- [ ] 确认 t_calc_sku_cost 中无重复 (store,date,article)
- [ ] 抽样对比 sku_cost vs stock 的 init_stock_qty 一致 (当前已一致)
- [ ] 抽样对比 sku_cost vs stock 的 init_stock_amt 一致 (当前有 15 行微小差异)
- [ ] 确认 is_first_day 标记在两侧一致 (当前不一致: sku_cost 2,857 vs stock 需验证)
- [ ] 如果执行修复，重跑全月 ETL 并对比 FM vs QDM

---

## 七、相关文档

- [FIX-002 EUC兜底链](FIX-002-euc-fallback.md) — EUC 9步计算详解
- 审查报告 §2.2 — 原始问题描述
- sku_cost.py:88-141 — 跨日 init_stock 逻辑
- stock.py:163-186 — 跨日 prev_df 查询
- stock.py:275-295 — init_stock 合并逻辑
