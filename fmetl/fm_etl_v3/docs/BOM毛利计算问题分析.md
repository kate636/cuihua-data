# BOM 毛利计算问题分析（v4 ETL 链路诊断）

> **生成时间**：2026-04-24
> **目的**：诊断现有 ETL 链路中 BOM 分摊逻辑的问题，为新方案设计提供依据

---

## 一、核心问题汇总

### 问题 1：进货验收表（表 2）的 parent 信息丢失

**现状**：
- `purchase_di` 包含 `article_id`（验收条码/parent）和 `sale_article_id`（销售条码/sub）
- 但 `inventory_extractor.py` 只取了 `sale_article_id AS article_id`
- **parent 信息完全丢失**

**样本数据**：
```
article_id        = 21292408（验收条码）
sale_article_id   = 21292408（销售条码）
sale_article_qty  = 20.0
sale_article_purchase_amt = 20.8
```

**问题**：
- 对于 BOM 商品（`article_id ≠ sale_article_id`），我们只存了 sub 级数据
- 无法追溯 parent 的进货均价（`avg_inbound_price` 是 parent 的，不是 sub 的）
- 无法验证 BOM 分摊的正确性

**影响**：
- `t_calc_bom_alloc` 无法利用 `purchase_di` 的预分摊数据
- 必须依赖 `receive_sale_di`（表 17）或自造 BOM 逻辑

---

### 问题 2：receive_sale_di（表 17）的标品混入问题

**现状**：
- `receive_sale_di` 样本数据显示 `article_id = sale_article_id`
- 西洋菜的样本：parent 和 sub 相同，说明这是标品
- **标品和 BOM 商品混在同一表**

**样本数据**：
```
article_id        = 20000219（西洋菜）
sale_article_id   = 20000219（西洋菜）
sale_article_qty  = 1.0
spilit_sale_article_amt = 8.31
rate              = 1.0
```

**问题**：
- 无法区分 BOM 商品（需要分摊）和标品（直接使用）
- 现有逻辑假设 `receive_sale_di` 只有 BOM 事实，但实际包含标品

**影响**：
- 对标品也做 BOM 分摊计算，逻辑冗余
- 可能影响成本计算的准确性

---

### 问题 3：BOM 关系边表（表 20）的 cost_rate 缺失

**现状**：
- `bom_relation` 样本数据显示 `cost_rate = 0.0`
- 需要用 sub 原价 × dressing_rate 反推

**样本数据**：
```
parent_article_id   = 20500283（整猪）
sub_article_id      = 20110840（排骨）
dressing_rate       = 11.099%
cost_rate           = 0.0 ⚠️
```

**问题**：
- 源表 `cost_rate` 可能经常为 NULL 或 0
- 反推逻辑依赖 `atomic_price.original_price`，但价格表可能缺失

**影响**：
- BOM 分摊金额可能不准确
- 多 sub 的分摊比例可能不一致

---

### 问题 4：compose_di（表 6）的 parent 定位问题

**现状**：
- `compose_di` 的 `compose_out_qty` 记录在 parent 身上
- 样本数据：`article_id = 21282423`（葡式蛋挞），`compose_in_qty = 5.0`，`compose_out_qty = 0.0`

**样本数据**：
```
article_id        = 21282423
compose_in_qty    = 5.0（原料入库）
compose_out_qty   = 0.0（成品产出）
```

**问题**：
- compose 记录的是加工转换的"原料入库"和"成品产出"
- 但没有 parent → sub 的映射关系
- 需配合 `bom_relation` 才能拆到 sub

**影响**：
- 当 `receive_sale_di` 缺失时，compose fallback 需要额外逻辑
- compose 和 BOM 的关系不明确

---

### 问题 5：现有三级 fallback 的复杂性

**现状 `bom_alloc.py` 逻辑**：
```
1. RECEIVE_SALE: atomic_receive_sale（主源）
2. COMPOSE: atomic_compose + atomic_bom_relation（次源）
3. BOM_THEORETICAL: atomic_bom_relation + parent.receive_qty（兜底）
```

**问题**：
- 三级 fallback 逻辑复杂，SQL 语句超过 270 行
- FULL OUTER JOIN 导致性能问题
- 各种 COALESCE 和 CASE WHEN 嵌套，难以理解和维护

**影响**：
- 代码可读性差
- 难以排查问题
- 性能可能有瓶颈

---

## 二、数据源对比分析

### 表 2 vs 表 17：哪个是"官方 BOM 分摊"？

| 维度 | `purchase_di`（表 2） | `receive_sale_di`（表 17） |
|---|---|---|
| 粒度 | (store, date, article_id[parent], sale_article_id[sub], day_clear) | (store, date, article_id[parent], sale_article_id[sub]) |
| parent 维度 | **有** `article_id` | **有** `article_id` |
| sub 维度 | **有** `sale_article_id` | **有** `sale_article_id` |
| 分摊数量 | `sale_article_qty` | `sale_article_qty` |
| 分摊金额 | `sale_article_purchase_amt` | `spilit_sale_article_amt` |
| parent 均价 | **有** `avg_inbound_price` | `purchase_price` |
| 归一化比例 | **无** | **有** `rate` / `sum_*` |
| 标品占比 | ~95%（大部分 article_id = sale_article_id） | 100%（包含标品） |
| day_clear 维度 | **有** | **无** |

**结论**：
- 表 2 有 parent 信息 + day_clear 维度，更适合作为进货验收主源
- 表 17 有归一化比例字段，更适合作为 BOM 分摊验证
- **两者应该配合使用，而不是只用表 17**

---

## 三、BOM 分摊的正确链路（我的理解）

### 3.1 商品分类

根据数据特征，商品可分为三类：

| 类型 | 特征 | 处理方式 |
|---|---|---|
| **标品** | `article_id = sale_article_id`（验收=销售） | 直接用 `cost_price` 或 `avg_inbound_price` |
| **BOM 预拆** | `article_id ≠ sale_article_id`，表 2/表 17 有分摊数据 | 用官方分摊数据 |
| **BOM 理论** | 表 2/表 17 缺失，表 20 有 BOM 边定义 | 用出肉率 × parent 均价 |

### 3.2 正确的 BOM 分摊链路

```
Step 1: 区分商品类型
  - 标品：article_id = sale_article_id（从表 2 识别）
  - BOM 商品：article_id ≠ sale_article_id

Step 2: BOM 商品成本来源优先级
  ① 表 17 receive_sale_di：spilit_sale_article_amt / sale_article_qty
  ② 表 2 purchase_di：sale_article_purchase_amt / sale_article_qty
  ③ 表 20 bom_relation：parent.avg_inbound_price × dressing_rate × cost_rate

Step 3: 标品成本来源优先级
  ① 表 9 cost_price_pool：cost_price
  ② 表 2 purchase_di：avg_inbound_price
  ③ 表 3 scm_di：out_stock_amt_cb / total_outstock_qty
```

---

## 四、关键问题待确认

### Q1：表 2 的 parent 进货信息如何处理？

当前 `inventory_extractor.py` 只取了 `sale_article_id`，丢失了 parent。
- **方案 A**：新增 `atomic_inventory_parent` 表，单独存储 parent 级进货信息
- **方案 B**：在 `atomic_inventory` 中同时存储 parent 和 sub（增加 parent_article_id 列）

### Q2：表 17 的标品混入如何处理？

- **方案 A**：在 extractor 中过滤 `WHERE article_id != sale_article_id`
- **方案 B**：在下游计算中判断是否为标品

### Q3：cost_rate 反推逻辑是否必要？

- 如果表 20 的 `cost_rate` 经常为 NULL，反推逻辑必须有
- 反推权重：`sub.original_price × dressing_rate` 是否合理？

### Q4：compose 和 BOM 的关系是什么？

- compose 是"加工转换"，BOM 是"出肉率定义"
- compose 的 compose_out_qty 是否需要按 BOM 边拆到 sub？
- 还是 compose 只用于库存方程，不参与成本分摊？

---

## 五、建议的改造方向

### 5.1 简化 BOM 分摊逻辑

**当前**：三级 fallback + FULL OUTER JOIN + 270 行 SQL

**建议**：
```
1. 先从表 2 识别标品 vs BOM 商品
2. 标品直接走四级成本 fallback（cost_price → avg_inbound_price → scm → MISSING）
3. BOM 商品：
   - 有表 17 数据：直接用 spilit_sale_article_amt
   - 无表 17：用表 2 的 sale_article_purchase_amt
   - 都无：用表 20 理论值
```

### 5.2 新增 atomic_inventory_parent 表

存储 parent 级进货信息：
```sql
CREATE TABLE atomic_inventory_parent AS
SELECT
    store_id,
    business_date,
    article_id AS parent_article_id,
    SUM(avg_inbound_price * sale_article_qty) / SUM(sale_article_qty) AS parent_avg_inbound_price,
    SUM(sale_article_purchase_amt) AS parent_inbound_amount,
    day_clear
FROM strategy_fm_purchase_di
WHERE article_id != sale_article_id  -- 只取 BOM 商品
GROUP BY store_id, business_date, article_id, day_clear
```

### 5.3 在 t_atomic_wide 中保留 parent 信息

当前的 merge.py 合并时，可以增加 `parent_article_id` 列，用于 BOM 分摊追溯。

---

## 六、下一步讨论

请确认以上问题分析，然后我们一起设计新的 BOM 逻辑：

1. **数据源选择**：用哪些表作为 BOM 分摊的主源？
2. **商品分类逻辑**：如何区分标品 vs BOM 商品？
3. **成本计算链路**：四级 fallback 如何简化？
4. **库存方程调整**：compose 和 BOM 如何配合？

---

*生成时间：2026-04-24*