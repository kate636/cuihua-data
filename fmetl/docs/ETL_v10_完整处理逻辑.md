# FM ETL v10 完整处理逻辑

> 粒度：`(store_id, business_date, article_id, day_clear)` 贯穿全链路。
>
> 数据流：`StarRocks strategy_fm_* (QDM API)` → `DuckDB atomic_*` → `DuckDB t_calc_*` → `DuckDB t_fm_*`

---

## 一、价格体系：全部 price 字段及用途

ETL 中有 **10+ 个 price 字段**，分属四个不同来源，各自用途不同。

### 1.1 售价侧（`atomic_price`，源表 `strategy_fm_price_da`）

| 字段 | 含义 | 在 ETL 中的用途 |
|------|------|----------------|
| `original_price` | **销售原价**（list price，标签价） | **BOM 消耗权重**：`weight = (sale_qty + know_lost_qty) × original_price`；**预期销售额**：`pre_sale_amt = lost_qty × original_price + original_price_sale_amt` |
| `current_price` | 今日实际售价 | 观测值，写入 `t_atomic_wide`，下游不参与计算 |
| `yesterday_price` | 昨日售价 | 观测值，写入 `t_atomic_wide`，下游不参与计算 |
| `dc_original_price` | **出库原价**（配送中心出库价） | **理论进货额**：`pre_inbound_amount = receive_qty × dc_original_price` |

### 1.2 成本侧（`atomic_inventory`，源表 `strategy_fm_purchase_di`）

| 字段 | 含义 | 在 ETL 中的用途 |
|------|------|----------------|
| `avg_inbound_price` | **进货均价**（parent 验收均价） | compose_net_amt **回退**（源表无 amt 时 = `qty × avg_inbound_price`）；euc **回退**（cost_qty=0 时 = `avg_inbound_price`） |

### 1.3 成本侧（`atomic_cost_price`，源表 `strategy_fm_inventory_pool_di`）

| 字段 | 含义 | 在 ETL 中的用途 |
|------|------|----------------|
| `cost_price` | **成本价快照**（observable only） | **v10 废弃，不参与任何计算**。仅写入 `t_atomic_wide`，供人工查看 |

### 1.4 SCM 侧（`atomic_scm`，源表 `strategy_fm_scm_di`）—— 7 个单价

| 字段 | 含义 | 用途 |
|------|------|------|
| `outstock_unit_price` | 出库单价（含税） | `out_stock_pay_amt = outstock_unit_price × original_outstock_qty`；`purchase_weight = order_qty_payean × outstock_unit_price` |
| `outstock_unit_price_notax` | 出库单价（不含税） | `out_stock_pay_amt_notax = price × original_outstock_qty`；参与 `scm_fin_article_income` |
| `outstock_cost_price` | 出库成本价（含税） | `out_stock_amt_cb = price × original_outstock_qty` |
| `outstock_cost_price_notax` | 出库成本价（不含税） | 参与 `scm_fin_article_cost` |
| `return_unit_price` | 退货单价 | `return_stock_pay_amt = price × return_stock_qty` |
| `return_unit_price_notax` | 退货单价（不含税） | 参与 `scm_fin_article_income` |
| `return_cost_price` | 退货成本价 | 参与 `scm_fin_article_cost` |
| `return_cost_price_notax` | 退货成本价（不含税） | 同 |
| `order_unit_price` | 订货单价 | `expect_outstock_amt = price × store_order_qty` |

### 1.5 各 price 的 ETL 计算链路图

```
original_price ──→ bom_alloc.py: consume_weight = (sale + know_lost) × price
               ──→ profit.py:    pre_sale_amt = lost_qty × price + original_price_sale_amt
dc_original_price ──→ profit.py: pre_inbound_amount = receive_qty × price
avg_inbound_price ──→ sku_cost.py: compose_net_amt fallback; euc fallback
                 ──→ stock.py:    compose_net_amt fallback
SCM 7 prices      ──→ stock.py:    out_stock_*/return_*/expect_* amt
                 ──→ profit.py:   scm_fin_article_income/cost/profit
cost_price        ──→ (不参与计算，仅观测)
current_price     ──→ (不参与计算，仅观测)
yesterday_price   ──→ (不参与计算，仅观测)
```

---

## 二、7 天 chunk 机制

`BaseExtractor.extract()` 调用 `split_date_range(start, end, 7)` 将日期范围切分为每 7 天一段，逐段发 API 请求。

**为什么做**：QDM BI API 有查询超时和结果集大小限制。单次查太多天会导致超时或返回数据截断。7 天是经验值，在 API 稳定性和请求次数之间取得平衡。

**对单日 ETL**（如 `2026-04-23 2026-04-23`）：日期范围只有 1 天，不会拆分，7 天 chunk 无实际影响。

---

## 三、完整 13 步骤

### Step 1: 维度表 (`DimsExtractor`)

7 张维表从 StarRocks 全量拉到 DuckDB。`dim_goods` 排除品类 70-77（物料类）。

### Step 2: 14 个原子域取数器

| # | 取数器 | 目标 DuckDB 表 | 取什么 |
|---|--------|---------------|--------|
| 2a | SalesExtractor | `atomic_sales` | 20+ 销售指标 |
| 2b | InventoryExtractor | `atomic_inventory` | `init_stock_qty/amt` + `avg_inbound_price` |
| 2c | InventoryDetailExtractor | `atomic_inventory_detail` | `actual_stock_qty` + `created_by`（判定人工/系统盘点） |
| 2d | ScmExtractor | `atomic_scm` | 出库/退货/订货的 qty + 7 个单价 |
| 2e | ScmAdjustExtractor | `atomic_scm_adjust` | SCM 差异调整 |
| 2f | LossExtractor | `atomic_loss` | `know_lost_qty` + `*_amt_src` |
| 2g | ComposeExtractor | `atomic_compose` | `compose_in/out_qty/amt` |
| 2h | AllowanceExtractor | `atomic_allowance` | `allowance_amt` |
| 2i | PromoExtractor | `atomic_promo` | 7 个促销金额 |
| 2j | CostPriceExtractor | `atomic_cost_price` | `cost_price`（观测值） |
| 2k | PriceExtractor | `atomic_price` | 4 个售价 |
| 2l | BomRelationExtractor | `atomic_bom_relation` | `dressing_rate`, `cost_rate` |
| 2m | ReceiveSaleExtractor | `atomic_receive_sale` | BOM 核心表 |

### Step 3: 原子宽表合并 (`AtomicMerger` → `t_atomic_wide`)

**3a. 自购数据提取**（`_tmp_self_receive`）：从 `atomic_receive_sale` 提取两路：
- **自购**（`article_id = sale_article_id`）：`SUM(inbound_qty/amt)` per article
- **BOM 父品**（`article_id ≠ sale_article_id`）：`MAX(inbound_qty/amt)`（去重）

**3b. 宽表 JOIN**：`atomic_sales FULL OUTER JOIN atomic_inventory` → LEFT JOIN `dim_day_clear` → INNER JOIN `dim_store_list` → LEFT JOIN 10 张原子表

**3c. BOM 父品补行**：父品不在宽表中时 INSERT 补全

**3d. 日清覆盖**（三组商品强制 `day_clear='0'`）：

| 规则 | SQL 条件 |
|------|---------|
| 猪肉类 | `category_level1_description = '猪肉类'` |
| 熟食类 | `category_level3_description LIKE '%熟食'` |
| 鲜牛肉 | `category_level1_description = '肉禽蛋类' AND article_name LIKE '鲜黄牛%'` |

### Step 4: BOM 分摊 (`BomAllocCalculator` → `t_calc_bom_alloc`)

**消耗权重**：`weight = (sale_qty + know_lost_qty) × original_price`

**自购减扣**：子品自己有进货的（Type A），BOM 只分摊自购不够的部分：
```
split_need_weight = consume_weight - self_inbound_weight  (≥0)
```

**共享组**：父品 A 的子品 ⊆ 父品 B 的子品时，合并为一组。所有子品按权重比例分 `group_total_amt`，共享子品按进货额比例分拆到两个父品。

**单位归一化**：
- `bom_alloc_qty`（父品单位）→ stock.py 的 `bom_out`
- `bom_alloc_qty_sub`（子品单位）→ sku_cost.py 的 `bom_in`

### Step 5: SKU 有效单位成本 (`SkuCostCalculator` → `t_calc_sku_cost`)

**euc 公式（加权平均含期初库存）**：

```
cost_amt = init_stock_amt + self_receive_amt + compose_net_amt + bom_alloc_amt
cost_qty = init_stock_qty + self_receive_qty + compose_net_qty + bom_alloc_qty

其中:
  compose_net_amt = compose_in_amt - compose_out_amt   ← compose 已 net，进出都计
  compose_net_qty = compose_in_qty - compose_out_qty
  bom_alloc_amt   = 子品收到的 BOM 分摊金额            ← bom 只计流入（子品收到多少）
  bom_alloc_qty   = bom_alloc_qty_sub（子品单位）

euc = cost_amt / cost_qty     （cost_qty > 0 时）
```

**为什么 compose 是 net 而 bom 只计流入**：
- compose 的 in 和 out 都发生在同一个 SKU 身上（原料收进来 → 成品转出去），所以 net
- BOM 的 in 在子品（收到分摊），out 在父品（付出原料）。euc 是按子品算的，子品只收到 bom_in，bom_out 是父品的事

**父品 euc**：父品自身也参与 euc 计算（有自己的 init + receive + bom_alloc），但父品通常没有 bom_in（它是给别人分的，不是收别人的）。父品的 euc 用于计算它的 `end_stock_amt`。

**BOM 父品剩余库存处理**（不在 sku_cost.py，在 stock.py Step 6.12）：
父品 `sale_qty=0, bom_out>0, end>0` → 按 BOM 分摊比例将父品 end_stock 全部转移给子品，父品 end 归零。

**euc 参与的后续计算**：

```
stock.py:
  end_stock_amt    = end_qty    × euc    ← 影响 t_calc_stock.end_stock_amt
  unknow_lost_amt  = unknow_qty × euc    ← 影响 t_calc_stock.unknow_lost_amt
  know_lost_amt    = know_qty   × euc    ← 影响 t_calc_stock.know_lost_amt

profit.py:
  profit_amt       公式中的 + end_stock_amt - init_stock_amt （euc 间接影响）
  sale_cost_amt    = sale_qty × euc      ← 输出到 t_calc_profit，不参与中间计算
  pre_profit_amt   = original_price_sale_amt - sale_qty × euc
  allowance_amt_profit 公式中的 + end_stock_amt - init_stock_amt （euc 间接影响）

明天的 sku_cost.py:
  init_stock_amt   = 昨天的 end_stock_amt （跨日滚动）
```

### Step 6: 库存与金额 (`StockCalculator` → `t_calc_stock`)

**四流合一库存方程**：

```
eq = init_stock + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost
```

**分支逻辑**（优先级从高到低）：

```
1. is_counted (人工盘点, created_by != '系统'):
   → end = actual_stock_qty
   → unknow = max(0, eq - actual)
   判定: atomic_inventory_detail.created_by != '系统' → 信任实盘值

2. day_clear = '0' (软日清):
   → 新供给 = receive + bom_in - bom_out + compose_in - compose_out
   → consumed_from_init = max(0, (sale + k_lost) - 新供给)
   → end = max(0, init - consumed_from_init)
   → unknow = max(0, 新供给 - sale - k_lost)
   解释：只清当日新供给的未售部分。存量 init 不日清（已在前几天算过失耗）

3. eq < 0 (负库存保护):
   → end = 0
   → unknow = -eq

4. know_lost_qty > 0 (有已知损耗):
   → end = eq
   → unknow = 0
   解释：知道丢了什么、丢了多少，方程已扣减 know_lost，算出来的 eq 就是应有的库存。

5. 其他（正常）:
   → end = eq
   → unknow = 0
```

**金额统一用 euc**：所有 amt = qty × euc（包括 know_lost_amt、unknow_lost_amt、end_stock_amt）

**跨日 init_stock**：今天 init = 昨天 end_stock（+1 天 shift），首日 SKU 回退到源表 `init_stock_qty_src` 并 clamp ≥0

**BOM 父品库存转移**（Step 6.12）：父品 `sale=0, bom_out>0, end>0` → 按 BOM 比例转移给子品 → 父品 end=0 → profit=0

### Step 7: 门店毛利 (`ProfitCalculator` → `t_calc_profit`)

**毛利公式**：

```
profit = sale - receive - bom_in + bom_out - compose_in + compose_out + (end - init)
```

损耗不在公式中单独扣减（已通过库存方程的 `end_stock` 体现）。

**`sale_cost_amt` 的定位**：`sale_qty × euc`（日清/非日清统一公式），作为独立字段写入 `t_calc_profit` 和 `t_fm_sku_dim`，用于后续的毛利率 KPI 计算（`t_fm_levels_result`），**不参与 profit.py 内部中间计算**。日清差异在 stock.py 端体现（end=0 → 残差转 unknow_lost），不影响 sale_cost 公式。

### Step 8-13: FM 底表

| Step | 产出 | 说明 |
|------|------|------|
| 8 | `t_fm_sku_dim` | SKU 级完整宽表，60+ 字段，含分类重映射、is_soldout、7 日滚动均量 |
| 9 | `t_fm_cust` | 客数聚合，按 6 个分类层级统计 distinct order_id |
| 10 | `t_fm_levels_sum` | 7 级分类 × 3 种 day_clear 汇总 |
| 11 | `t_fm_levels_result` | 中文列名 + 20+ KPI 比率 |
| 12 | `t_fm_bom_breakdown` | BOM 溯源：parent × sub 明细 |
| 13 | `t_fm_stock_roll` | 库存八要素滚动 + balance_qty 校验 |

---

## 四、分类重映射

| 条件 | 新分类名 |
|------|---------|
| `L2 IN ('蛋类','烘焙类')` | 蛋类 / 烘焙类（独立） |
| `L2 IN ('冷藏奶制品类','饮料类')` | **乳制品及水饮类** |
| `L1 = '肉禽蛋类' AND L2 ≠ '蛋类'` | **肉禽类** |
| `L3 LIKE '%熟食'` | **熟食类** |
| `L1 IN ('冷藏及加工类','预制菜')` | **冷藏加工及预制菜类** |
| 其他 | 保持原始 L1 分类 |

---

## 五、待确认 / 已知局限性

1. **`is_counted` 逻辑**：依赖 `atomic_inventory_detail.created_by != '系统'` 判定人工盘点。`created_by = '系统'` 的记为系统快照（继续用方程自算），其余为人工盘点（信任实盘值覆盖 end_stock）。若源表无人工盘点记录，此分支不会触发。

2. **`end_stock_qty/amt` 未使用源表值**：`strategy_fm_purchase_di` 有上游算好的 `end_stock_qty/amt`，v10 选择自算。差异可能源于盘点日源表用实盘覆盖而我们用方程推算。

3. **`know_lost_*_src` 提取但未使用**：`atomic_loss` 有源表的 know/unknow_lost_amt，下游 stock.py 统一用 `euc × qty` 重算金额，未参考源表值。
