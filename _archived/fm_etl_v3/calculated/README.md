# calculated — DuckDB 内部计算层

Pipeline 的 **Step 3~7**。原子域提取完成后，所有计算都在 DuckDB 里做，**不再回查任何外部系统**。五个模块严格顺序执行，后一步依赖前一步的产出表。

---

## 依赖关系图

```
(atomic_sales, atomic_inventory, atomic_scm, atomic_loss,
 atomic_compose, atomic_allowance, atomic_promo,
 atomic_cost_price, atomic_price, dim_store_list, dim_day_clear)
         │
         ▼
  AtomicMerger ─────▶ t_atomic_wide
         │
         ├────────────▶ InventoryCalculator ─▶ t_calc_inventory
         │                                            │
         ├────────────▶ AvgPriceCalculator ───────────┤──▶ t_calc_avg_price
         │                                            │          │
         └────────────▶ AmountsCalculator ────────────┴──────────┴──▶ t_calc_amounts
                                                                          │
                       ProfitCalculator ◀─────────────────────────────────┘
                              │
                              ▼
                       t_calc_profit
```

粒度全部是 `store_id × business_date × article_id × day_clear`。

---

## Step 3 · `merge.py` — `AtomicMerger` → `t_atomic_wide`

每次运行先 `DROP TABLE IF EXISTS t_atomic_wide` 再整体重建（表数据量小，重建比分区删更安全）。

**合并逻辑**：
1. `atomic_sales FULL OUTER JOIN atomic_inventory` 取得基底（保留**有进货但无销售**或**有销售但无进货**的商品）
2. 依次 `LEFT JOIN` 其余 7 个原子表（scm / loss / compose / allowance / promo / cost_price / price）
3. `INNER JOIN dim_store_list` **只保留翠花门店**
4. `day_clear` 字段用三级优先：销售原子 > 进货原子 > `dim_day_clear` 维度表（`COALESCE` 兜底）

**为什么以 sales 为基底 FULL OUTER JOIN inventory**：单独只有进货或只有销售的记录都是业务真实事件（比如新到货但当天没卖、或者退货但当天没进货），丢任何一边都会漏数据。

产出列数 ≈ 80（见 SQL 内的字段逐一 `COALESCE(..., 0)`），所有数量/金额字段都用 0 填充 NULL，方便下游无脑做算术。

---

## Step 4 · `inventory.py` — `InventoryCalculator` → `t_calc_inventory`

修正库存方程，解决源数据中**期末库存字段常缺失或异常**的问题。

```
end_stock_qty = init_stock_qty
              + receive_qty
              - sale_qty
              - compose_out_qty
              + compose_in_qty
              - know_lost_qty
```

**边界规则**：

| 情况 | 处理 |
|------|------|
| 日清商品（`day_clear='0'`）算出来为负 | 置 0 |
| 非日清算出来为负 | 保留原负值（便于发现数据异常） |
| 源表 `end_stock_qty_raw` 和计算值差异大 | 用计算值（源表期末库存极不可靠） |

**产出字段**：在 `t_atomic_wide` 基础上新增 `end_stock_qty`（修正后的期末）。

---

## Step 5 · `avg_price.py` — `AvgPriceCalculator` → `t_calc_avg_price`

计算**加权平均进货价** `avg_purchase_price`。

```
avg_purchase_price =
    receive_amt / receive_qty     (receive_qty > 0 时)
    cost_price                     (无进货时，退回成本价底表)
```

**用途**：
- 非日清商品的销售成本计算（`sale_qty × avg_purchase_price`）
- 库存金额反推（`init_stock_qty × avg_purchase_price`、`end_stock_qty × avg_purchase_price`）

`receive_amt` 这时还没算出来，本步需要**先用 `original_price_sale_amt / sale_qty` 或源表的 `outstock_unit_price_notax` 做暂估**（见源码具体实现）。

---

## Step 6 · `amounts.py` — `AmountsCalculator` → `t_calc_amounts`

基于修正后的库存数量，把所有**数量**字段换算成**金额**字段。

| 金额字段 | 计算公式 |
|---------|---------|
| `receive_amt` | `receive_qty × avg_purchase_price` |
| `init_stock_amt` | `init_stock_qty × avg_purchase_price` |
| `end_stock_amt` | `end_stock_qty × avg_purchase_price` |
| `compose_in_amt` | `compose_in_qty × avg_purchase_price` |
| `compose_out_amt` | `compose_out_qty × avg_purchase_price` |
| `lost_amt` | `know_lost_amt + unknow_lost_amt`（直接用源表金额，不反算） |
| `out_stock_pay_amt_notax` | 供应链含税出库额**去税**后 |
| `purchase_weight` | 进货重量（千克品种直接用 qty，其他 = qty × 单重） |

---

## Step 7 · `profit.py` — `ProfitCalculator` → `t_calc_profit`

**9 个核心毛利与成本指标**，每个都有明确的业务口径：

| 指标 | 公式 | 业务含义 |
|------|------|---------|
| `profit_amt`（运营毛利额） | `sale_amt − (receive_amt + compose_in_amt − compose_out_amt) + (end_stock_amt − init_stock_amt)` | 按"销售 − 进货净 + 库存变化"口径的毛利 |
| `sale_cost_amt`（销售成本） | 日清：`receive_amt + compose_in_amt − compose_out_amt − lost_amt`<br>非日清：`sale_qty × avg_purchase_price` | 日清按"当天进货视为当天卖完"计成本，非日清按单价×销量 |
| `pre_profit_amt`（预期毛利额） | `original_price_sale_amt − sale_cost_amt` | 如果没打折卖能赚多少 |
| `allowance_amt_profit`（补贴后毛利额） | `sale_amt − receive_amt + allowance_amt + (end_stock_amt − init_stock_amt)` | 含上补贴口径 |
| `scm_fin_article_income` | `out_stock_pay_amt_notax − \|return_stock_pay_amt_notax\|` | 供应链财务收入（供应链侧） |
| `scm_fin_article_cost` | `out_stock_amt_cb_notax − \|return_stock_amt_cb_notax\|` | 供应链财务成本（供应链侧） |
| `scm_fin_article_profit` | 供应链收入 − 供应链成本 | 供应链财务毛利 |
| `full_link_article_profit`（全链路毛利额） | `profit_amt + scm_fin_income − scm_fin_cost` | **门店毛利 + 供应链毛利 = 全链路视角** |
| `pre_sale_amt`（预期销售额） | `lost_qty × original_price + original_price_sale_amt` | 算上损耗的商品如果都能按原价卖能收多少 |
| `pre_inbound_amount`（理论进货额） | `receive_qty × dc_original_price` | 按 DC 原价计算的理论进货成本 |

**口径关键点**（常踩坑）：
- **日清 vs 非日清** 销售成本公式不同，`IF(day_clear='0', ..., ...)` 分支
- **`scm_fin_*` 全链路口径**用的是 `atomic_scm` 的**去税价**，不要和门店侧的含税价混用
- **补贴额 `allowance_amt`** 已经是**已扣减**的补贴（源表口径），不需要再减

---

## 调试技巧

**单独重跑一步**（不跑整个 pipeline）：

```python
from fm_etl_v3.connectors import DuckDBStore
from fm_etl_v3.calculated import ProfitCalculator

duck = DuckDBStore()
ProfitCalculator(duck).run()
duck.close()
```

前提是 `t_atomic_wide` / `t_calc_*` 等依赖表已经存在（前面步骤已跑过）。

**看中间结果**：

```sql
SELECT store_id, business_date, article_id, day_clear,
       sale_qty, receive_qty, end_stock_qty, avg_purchase_price,
       sale_cost_amt, profit_amt, full_link_article_profit
FROM t_calc_profit
WHERE business_date = '2026-04-19'
  AND article_id = 'xxx'
ORDER BY store_id;
```

**毛利对不上**：按 `sale_amt - sale_cost_amt` 手算一遍，再对照 `profit_amt` 公式。最大概率是混用了日清/非日清分支，或者 `avg_purchase_price` 取到了 0。
