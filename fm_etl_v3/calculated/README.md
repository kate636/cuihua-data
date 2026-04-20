# calculated — DuckDB 计算层

原子域提取完成后，所有计算都在本地 DuckDB 内完成，不再回查 StarRocks。五个模块严格顺序执行，后一步依赖前一步的产出表。

## 执行顺序与表依赖

```
atomic_sales
atomic_inventory        ─┐
atomic_scm               │
atomic_loss              │  → AtomicMerger → t_atomic_wide
atomic_compose           │
atomic_allowance         │
atomic_promo             │
atomic_cost_price        │
atomic_price            ─┘
dim_store_list
dim_day_clear

t_atomic_wide  → InventoryCalculator → t_calc_inventory
t_atomic_wide
t_calc_inventory        ─┐  → AvgPriceCalculator → t_calc_avg_price

t_atomic_wide
t_calc_inventory        ─┐  → AmountsCalculator  → t_calc_amounts
t_calc_avg_price        ─┘

t_atomic_wide
t_calc_inventory        ─┐
t_calc_amounts           │  → ProfitCalculator   → t_calc_profit
t_calc_avg_price        ─┘
```

## 各模块说明

### `merge.py` — AtomicMerger → `t_atomic_wide`

粒度：`store_id × business_date × article_id × day_clear`

以 `atomic_sales` 为基底，`FULL OUTER JOIN atomic_inventory`（保留有进货但无销售的商品），再依次 LEFT JOIN 其余 7 个原子表，最后 INNER JOIN `dim_store_list` 过滤出翠花门店。

日清标签优先级：事务流水 `day_clear` > 进货流水 `day_clear` > `dim_day_clear`维度表。

### `inventory.py` — InventoryCalculator → `t_calc_inventory`

修正库存方程，解决源数据中期末库存缺失/异常的情况：

```
end_stock_qty = init_stock_qty + receive_qty - sale_qty - compose_out_qty + compose_in_qty - know_lost_qty
```

日清商品（`day_clear='0'`）期末库存不得为负，如计算结果为负则置 0。

### `avg_price.py` — AvgPriceCalculator → `t_calc_avg_price`

计算加权平均进货价 `avg_purchase_price`，用于非日清商品的销售成本：

```
avg_purchase_price = receive_amt / receive_qty  （receive_qty > 0 时）
                   = cost_price                  （无进货时，退回到成本价底表）
```

### `amounts.py` — AmountsCalculator → `t_calc_amounts`

基于修正后的库存数量，将数量字段换算为金额字段：

| 金额字段 | 计算逻辑 |
|---------|---------|
| `receive_amt` | `receive_qty × avg_purchase_price` |
| `init_stock_amt` | `init_stock_qty × avg_purchase_price` |
| `end_stock_amt` | `end_stock_qty × avg_purchase_price` |
| `compose_in_amt` | `compose_in_qty × avg_purchase_price` |
| `compose_out_amt` | `compose_out_qty × avg_purchase_price` |
| `lost_amt` | `know_lost_amt + unknow_lost_amt`（损耗额，来自源表直接金额字段） |
| `out_stock_pay_amt_notax` | 供应链含税出库额去税后 |
| `purchase_weight` | 进货重量（千克品种直接用 qty，其他乘单重） |

### `profit.py` — ProfitCalculator → `t_calc_profit`

| 指标 | 公式 |
|------|------|
| `profit_amt`（运营毛利额） | `sale_amt − (receive_amt + compose_in_amt − compose_out_amt) + (end_stock_amt − init_stock_amt)` |
| `sale_cost_amt`（销售成本） | 日清：`receive_amt + compose_in_amt − compose_out_amt − lost_amt`；非日清：`sale_qty × avg_purchase_price` |
| `pre_profit_amt`（预期毛利额） | `original_price_sale_amt − sale_cost_amt` |
| `allowance_amt_profit`（补贴后毛利额） | `sale_amt − receive_amt + allowance_amt + (end_stock_amt − init_stock_amt)` |
| `scm_fin_article_income` | `out_stock_pay_amt_notax − \|return_stock_pay_amt_notax\|` |
| `scm_fin_article_cost` | `out_stock_amt_cb_notax − \|return_stock_amt_cb_notax\|` |
| `scm_fin_article_profit` | 供应链财务收入 − 供应链财务成本 |
| `full_link_article_profit`（全链路毛利额） | `profit_amt + scm_fin_income − scm_fin_cost` |
| `pre_sale_amt`（预期销售额） | `lost_qty × original_price + original_price_sale_amt` |
| `pre_inbound_amount`（理论进货额） | `receive_qty × dc_original_price` |
