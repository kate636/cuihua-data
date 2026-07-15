# FM ETL v0.11 完整处理逻辑

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
| `current_price` | 今日实际售价 | **不参与 euc 计算**（早期 `current_price×0.40` 兜底已移除）；仅写入 `t_atomic_wide` 作观测 |
| `yesterday_price` | 昨日售价 | 观测值，写入 `t_atomic_wide`，下游不参与计算 |
| `dc_original_price` | **出库原价**（配送中心出库价） | **理论进货额**：`pre_inbound_amount = receive_qty × dc_original_price` |

### 1.2 成本侧（`atomic_inventory`，源表 `strategy_fm_purchase_di`）

| 字段 | 含义 | 在 ETL 中的用途 |
|------|------|----------------|
| `avg_inbound_price` | **进货均价**（parent 验收均价） | euc 兜底链第 2 层：cost_qty=0 且前向填充(ffill)无值时 `euc = avg_inbound_price`，标记 `V10_AVG_INBOUND_FALLBACK` |

### 1.3 成本侧（`atomic_cost_price`，源表 `strategy_fm_inventory_pool_di`）

| 字段 | 含义 | 在 ETL 中的用途 |
|------|------|----------------|
| `cost_price` | **标准成本价**（系统维护的标准成本） | **不参与计算**：`atomic_cost_price` 仍提取、sku_cost.py 仍 SELECT，但 euc 与 compose 均不使用（无 `V10_COST_PRICE_FALLBACK` 标记）。纯观测值 |

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
cost_price       ──→ (v0.10 不再参与 euc 计算，仅作观测值)
avg_inbound_price ──→ sku_cost.py: euc 第二层兜底 (V10_AVG_INBOUND_FALLBACK)
current_price    ──→ (v0.10 不再参与 euc 计算，仅作观测值)
SCM 7 prices      ──→ stock.py:    out_stock_*/return_*/expect_* amt
                 ──→ profit.py:   scm_fin_article_income/cost/profit
yesterday_price   ──→ (不参与计算，仅观测)
```

---

## 二、日期分片 chunk 机制

`split_date_range(start, end, interval)` 将日期范围切分为每 `interval` 天一段，逐段发 API 请求。

- **BaseExtractor 默认**: `interval=7` 天（`_base.py` extract 方法签名）
- **Executor 实际使用**: `chunk=30` 天（`executor.py` L104），因 7 天 chunk 在某些源表上会导致 API 分页截断，单次 30 天获取完整数据

**为什么做 chunk**：QDM BI API 有查询超时和结果集大小限制。单次查太多天会导致超时或返回数据截断。

---

## 三、完整 14 步骤

### Step 1: 维度表 (`DimsExtractor`)

7 张维表从 StarRocks 全量拉到 DuckDB。`dim_goods` 排除品类 70-77（物料类）。

**dim_goods 日期获取逻辑**：以 `{yesterday}` 为基准日期，向前回溯最多 8 天 + 向后扫描最多 3 天，取第一个有数据的 `inc_day` 快照。若 14 天内均无数据，保留现有 `dim_goods` 不覆盖（避免历史数据丢失）。表中无 `inc_day` 列。

**日清覆盖白名单** (`dim_day_clear_override`)：从 `dim_goods` 派生分类规则 + 从 FM 日清标签管理 API 拉取手动录入，供 merge.py 日清覆盖使用，隔离对 dim_goods 的直接依赖。包含：
- `猪肉类`：`category_level1_description = '猪肉类'`
- `熟食类`：`category_level1_description = '熟食类'` / `L3 LIKE '%熟食'` / `L2 IN ('即烹类','即热类')` / 预制菜+千克
- `手动录入`：从 `/api/dayclear/list?manual_only=1` 拉取（烘焙类等业务手动添加）

### Step 2: 13+2 个原子域取数器

并行提取 13 个域（`ThreadPoolExecutor`, 6 workers）+ 2 个骨架表在末尾串行创建。每个 worker 创建独立 `ApiConnector` + `DuckDBStore`。DuckDB 文件级锁串行化写入，API 调用并行。Step 2 从 ~90s 降至 ~15s。

> **v0.10 fix**: `SalesExtractor` / `InventoryExtractor` 中 LEFT JOIN `strategy_fm_dim_goods` 使用 `inc_day = '{end}'`（dim_goods 已有 end 日快照时直接命中，不需要回溯到 yesterday）。
>
> **v0.10 fix**: `PromoExtractor` 使用 **INNER JOIN**（非 LEFT JOIN），且同样使用 `inc_day = '{end}'`。不在 dim_goods 中的 promo 记录会被丢弃。
>
> **v0.10 fix**: `InventoryExtractor` 先 DROP 旧表（schema 变更），再走标准分区写入。写入前调用 `_ensure_table_exists()` 确保 API 返回 0 行时下游 merge 不崩溃。
>
> **品类过滤说明**：所有原子表排除品类 70-77（物料类）。`SalesExtractor` 对线下销售（`online_flag='N'`）做品类过滤，线上销售不受限制，且额外排除 `category_level1_id='91'`。

| # | 取数器 | 目标 DuckDB 表 | 取什么 |
|---|--------|---------------|--------|
| 2a | SalesExtractor | `atomic_sales` | 20+ 销售指标 |
| 2b | InventoryExtractor | `atomic_inventory` | `init_stock_qty/amt` + `avg_inbound_price` + `purchase_receive_qty/amt` |
| 2c | InventoryDetailExtractor | `atomic_inventory_detail` | `actual_stock_qty` + `created_by`（判定人工/系统盘点） |
| 2d | ScmExtractor | `atomic_scm` | 出库/退货/订货的 qty + 7 个单价 |
| 2e | ScmAdjustExtractor | `atomic_scm_adjust` | SCM 差异调整 |
| 2f | LossExtractor | `atomic_loss` | `know_lost_qty` + `*_amt_src` |
| 2g | ComposeExtractor | `atomic_compose` | `compose_in/out_qty/amt` |
| 2h | AllowanceExtractor | `atomic_allowance` | `allowance_amt` |
| 2i | PromoExtractor | `atomic_promo` | 7 个促销金额 |
| 2j | CostPriceExtractor | `atomic_cost_price` | `cost_price`（标准成本，EUC 兜底用） |
| 2k | PriceExtractor | `atomic_price` | 4 个售价 |
| 2l | BomRelationExtractor | `atomic_bom_relation` | `dressing_rate`, `cost_rate`, `parent_unit`, `sub_unit` |
| 2m | ReceiveSaleExtractor | `atomic_receive_sale` | BOM 核心表：自购+BOM 父品进货行的 `inbound_qty/amt`、`sum_sub_article_qty` |
| 2n | OrderReceiveExtractor | `atomic_order_receive` | 空骨架表 |
| 2o | ArticleConvertExtractor | `atomic_article_convert` | 空骨架表 |

> 注意：`OrderReceiveExtractor` 和 `ArticleConvertExtractor` 不参与并行提取，在 Step 2 末尾单独执行 `ensure_empty_skeleton()`。

### Step 3: 原子宽表合并 (`AtomicMerger` → `t_atomic_wide`)

**3a. 自购数据提取**（`_tmp_self_receive`）：从 `atomic_receive_sale` 提取两路：
- **自购**（`article_id = sale_article_id`）：`SUM(inbound_qty/amt)` per article
- **BOM 父品**（`article_id ≠ sale_article_id`）：`MAX(inbound_qty/amt)`（去重，同一父品多行对应不同子品）

同时构建 `_tmp_bom_subs` 辅助表（BOM 子品集合），标记哪些 SKU 是 BOM 子品，防止 self_receive 被 `purchase_di` 回退覆盖。

**3b. 宽表 JOIN**：`atomic_sales` 和 `atomic_inventory` 分别在子查询中 `GROUP BY (store_id, business_date, article_id)` 聚合后再 FULL OUTER JOIN，消除 day_clear 维度差异。结果外层再套 `ROW_NUMBER() OVER (PARTITION BY store_id, business_date, article_id, day_clear)` 全局去重，保证 t_atomic_wide 零重复行。

day_clear 优先级：sales.day_clear > inventory.day_clear > dim_day_clear(1→'1' else '0') > 默认 '0'。

**3c. self_receive 回退逻辑**：
- 优先用 `atomic_receive_sale` 的 `self_receive_qty/amt`
- 若 receive_sale 无数据，但 SKU 是 BOM 子品 → 回退量 = 0（不用 purchase_di 回退）
- 若 receive_sale 无数据，且 SKU 不是 BOM 子品 → 回退到 `atomic_inventory.purchase_receive_qty/amt`

**3d. BOM 父品补行**：父品不在宽表中时 INSERT 补全行（父品有 self_receive 但无销售，需出现在宽表以便 downstream 处理其进货和 BOM 流出）。父品行 day_clear 固定为 '1'，所有销售/库存/损耗等字段填 0。

**3e. 日清覆盖**（三组商品强制 `day_clear='0'`，通过 `dim_day_clear_override` 白名单实现，与 `merge.py` 的三段 UPDATE 一致）：

| override_type | 派生规则（dims_extractor.py 构建） |
|---------------|--------------|
| `猪肉类` | `category_level1_description = '猪肉类'` |
| `熟食类` | `L1='熟食类'` OR `L3 LIKE '%熟食'` OR `L2 IN ('即烹类','即热类')` OR (`L1='预制菜'` AND `sale_unit='千克'`) |
| `烘焙类`（及其他手动录入） | 从 FM 日清标签管理 API `?manual_only=1` 拉取，按 API 返回的 `override_type` 追加 |

> **v0.10 fix**：鲜牛肉日清覆盖已移除。牛肉有持续期初库存（`purchase_di` 的 `init_stock ≠ 0`），日清会导致巨额虚假亏损。
>
> 注意：本 Step 3e 列表与 Step 1 的"日清覆盖白名单"是同一份 `dim_day_clear_override`，构建在 Step 1（dims_extractor），应用在 Step 3（merge.py）。

### Step 4: BOM 分摊 (`BomAllocCalculator` → `t_calc_bom_alloc`)

**消耗权重**：`weight = (sale_qty + know_lost_qty) × original_price`

**自购减扣**：子品自己有进货的（Type A），BOM 只分摊自购不够的部分：
```
split_need_weight = consume_weight - self_inbound_weight  (≥0)
```

**共享组**：父品 A 的子品 ⊆ 父品 B 的子品时，合并为一组。所有子品按权重比例分 `group_total_amt`，共享子品按进货额比例分拆到两个父品。

**数量分配（v0.10 fix — 按总产量分配）**：
- `bom_alloc_qty`（父品单位）：`alloc_ratio × parent_qty` — 母品入库总量按权重比例分配，用于 stock.py 的 `bom_out`
- `bom_alloc_qty_sub`（子品单位）：`alloc_ratio × sum_sub` — 子品总产量（`parent_sum_sub_qty` 或 `parent_qty`）按权重比例分配，用于 sku_cost.py 的 `bom_in`

> **v0.10 fix**：原公式用 `split_need_qty_val`（日销量口径）作为分母，导致母品全部采购成本只分摊到当日销售的子品上（而非全部产量），EUC 被高估 6x+。改为 `alloc_ratio × 总产量`，成本均匀分摊到全部产出子品上。

**子品单位成本**：`sub_unit_cost = bom_alloc_amt / bom_alloc_qty_sub`（当 `bom_alloc_qty_sub > 0` 时）

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

**compose 金额计算**（v0.10 加工关系推算，不使用源表金额）：
- 成品 `compose_in_amt = compose_in_qty × Σ(raw_qty / yield_qty × raw_base_euc)` — 配方推算
- 原料 `compose_out_amt = compose_out_qty × base_euc` — 价值守恒
- `compose_in_qty` 从业务行为反推：`max(0, sale + loss - init - recv)`，有盘点时 `max(0, actual + sale + loss - init - recv)`
- `compose_out_qty` 从配方反推：`Σ(成品 compose_in × raw_qty / yield_qty)`

**euc 兜底链**（`cost_qty = 0` 时，从前到后依次尝试）：
1. 前向填充（ffill）：沿 `(store_id, article_id)` 从上一营业日继承 euc，标记 `V10_INHERITED_EUC`
2. `avg_inbound_price`：历史采购均价，标记 `V10_AVG_INBOUND_FALLBACK`
3. 加工关系推算：`Σ(原料用量 × 原料euc) / 产出数量`，标记 `V10_PROCESSING_RELATION`
4. 以上均失败 → euc 保持 0，WARNING 日志输出受影响的 SKU 列表

注意：仅向前填充（ffill），不做反向填充（bfill）。`cost_price` 和 `current_price×0.40` 已从兜底链移除（v0.10 不再使用）。

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

明天的 sku_cost.py:
  init_stock_amt   = 前一营业日的 end_stock_amt（MAX(business_date) < 当天，非 timedelta -1）
```

> **v0.10 fix**：sku_cost.py 和 stock.py 的跨日查找均改为 `MAX(business_date) < 当天`，自动跳过非连续日期（周末/节假日/数据缺失）。

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
   若盘点日无人工盘点记录，此分支不触发

2. day_clear = '0' (软日清):
   → 新供给 = receive + bom_in - bom_out + compose_in - compose_out
   → consumed_from_init = max(0, (sale + k_lost) - 新供给)
   → end = max(0, init - consumed_from_init)
   → unknow = 新供给 - sale - k_lost
   解释：只清当日新供给的未售部分。存量 init 不日清（已在前几天算过失耗）
   注意：unknow 可为负值（新供给小于销售时，消耗期初库存）

3. eq < 0 (负库存保护):
   → end = 0
   → unknow = eq  (FIX-020 口径B: 负值=盘盈, 使 end+unknow=eq 精确平衡;
                   超卖含义=进货/期初被低估而非丢货)
   → neg_clamp_cost_amt = -eq × euc  (透支成本, 仅此分支非0, 供 profit.py 扣回)

4. know_lost_qty > 0 (有已知损耗):
   → end = eq
   → unknow = 0
   解释：知道丢了什么、丢了多少，方程已扣减 know_lost，算出来的 eq 就是应有的库存。

5. actual_stock_qty > eq + 0.001 (系统快照盘盈检测):
   → end = actual_stock_qty
   → unknow = eq - actual  (负值 = 盘盈)
   解释：系统记录的实盘数超过方程计算值，用实盘覆盖 end，差额记为负 unknow。

6. 其他（正常）:
   → end = eq
   → unknow = 0
```

> 共 **6 个分支**（优先级从高到低）。stock.py 的实际判断顺序见
> `[calculated/README.md](../../calculated/README.md)` 分支表。

**金额统一用 euc**：所有 amt = qty × euc（包括 know_lost_amt、unknow_lost_amt、end_stock_amt）

**跨日 init_stock**：今天 init = 前一营业日的 end_stock（`MAX(business_date) < 当天`，非 timedelta -1 天）。非连续日期（周末、节假日、数据缺失）自动跳过。首日 SKU 回退到源表 `init_stock_qty_src` 并 clamp ≥0。

**BOM 父品库存转移**（Step 6.12）：父品 `sale=0, bom_out>0, end>0` → 按 BOM 比例转移给子品：
  - 父品 `end=0`，`stock_transfer_out = 原end_stock`（记录于 `stock_transfer_out_qty/amt` 字段）
  - 子品 `end += transfer`，记录 `stock_transfer_in_qty/amt`；同步增加 `bom_in_amt/qty`（抵消 end 增加对子品 profit 的影响）
  - v0.10 fix：不再重复增加父品 `bom_out_amt/qty`。BOM alloc 已将 100% 母品成本分摊给子品，stock_transfer 清零 end_stock 后无需额外调整 bom_out

**跨日 init_stock 查找注意**：前日 end_stock 查找按 `(store_id, article_id, day_clear)` 分组。若同一 SKU 的 day_clear 在不同日期间变化（例如从 '1' 变为 '0'），跨日链条会断裂，该 SKU 回退到源表 `init_stock_qty_src`。

### Step 7: 门店毛利 (`ProfitCalculator` → `t_calc_profit`)

**核心毛利公式**：

```
profit = sale - receive - bom_in + bom_out - compose_in + compose_out + (end - init)
```

损耗不在公式中单独扣减（已通过库存方程的 `end_stock` 体现）。

> **v0.11 FIX-019（负库存钉零透支成本扣减）**：非日清品 (`day_clear='1'`) 当库存方程
> `eq<0` 时，stock.py 把 `end` 钉零、透支量 `-eq` 转入 `unknow_lost`。但毛利公式只用
> `end-init`、`end` 又被钉高到 0 → 透支成本既不进 end 也不进利润 → 利润虚高。profit.py
> 对精确命中钉零分支的行 (`dc='1' & eq<0 & end≈0 & unknow_qty>0`) 扣回 `unknow_lost_amt`，
> 不碰日清 `dc='0'`（其 unknow 是软日清正常残差/盘盈）。6/18–22 总毛利差 +18.9%→+6.3%。
> 详见 [FIX-019](../fixes/FIX-019-negative-stock-clamp-cost.md)。

**辅助毛利指标**：

| 字段 | 公式 | 用途 |
|------|------|------|
| `sale_cost_amt` | `sale_qty × euc` | 销售成本，日清/非日清统一公式，写入 `t_fm_sku_dim` 用于毛利率 KPI |
| `pre_profit_amt` | `original_price_sale_amt - sale_qty × euc` | 原价口径毛利额 |
| `allowance_amt_profit` | `sale - receive + allowance + (end - init)` | 补贴后毛利额 |
| `scm_fin_article_income` | `\|out_stock_pay_amt_notax\| - \|return_stock_pay_amt_notax\|` | SCM 金融收入 |
| `scm_fin_article_cost` | `\|outstock_cost_price_notax × outstock_qty\| - \|return_cost_price_notax × return_qty\|` | SCM 金融成本 |
| `scm_fin_article_profit` | `scm_fin_article_income - scm_fin_article_cost` | SCM 金融毛利 |
| `full_link_article_profit` | `profit + scm_fin_article_profit` | 全链路毛利 |

**`sale_cost_amt` 的定位**：作为独立字段写入 `t_calc_profit` 和 `t_fm_sku_dim`，用于后续的毛利率 KPI 计算（`t_fm_levels_result`），不参与 profit.py 内部中间计算。日清差异在 stock.py 端体现（end=0 → 残差转 unknow_lost），不影响 sale_cost 公式。

### Step 8-13: FM 底表

| Step | 产出 | 说明 |
|------|------|------|
| 8 | `t_fm_sku_dim` | SKU 级完整宽表，60+ 字段，含分类重映射、is_soldout、7 日滚动均量 |
| 9 | `t_fm_cust` | 客数聚合，按 6 个分类层级统计 distinct order_id（通过 dim_goods JOIN 过滤品类 70-77） |
| 10 | `t_fm_levels_sum` | 7 级分类 × 3 种 day_clear 汇总（'0'日清 / '1'非日清 / '2'合计） |
| 11 | `t_fm_levels_result` | 中文列名 + 20+ KPI 比率（毛利率、损耗率、促销费率等） |
| 12 | `t_fm_bom_breakdown` | BOM 溯源：parent × sub 明细，含 `sub_unit_cost`、`sub_qty_actual` |
| 13 | `t_fm_stock_roll` | 库存八要素滚动（init/receive/bom_in/bom_out/compose_in/compose_out/sale/lost → end）+ balance_qty 校验 |
| 14 | `_sync_processing_candidates` | 加工候选 SKU 提取 → SQLite → SCP 到云端 `/opt/fm/proc-rel/proc_candidates.db`，供加工关系管理使用 |

---

## 四、分类重映射

`sku_dim.py` `build()` 方法中 pandas 向量化实现，与 v3 SQL CASE WHEN 逻辑一致。产出 `category_level1_id_remap` 和 `category_level1_description_remap` 两列。

| 条件 | 新 ID | 新描述 |
|------|-------|--------|
| L2 IN ('蛋类', '烘焙类') | '' | L2 本身（蛋类 / 烘焙类） |
| L2 IN ('冷藏奶制品类', '饮料类') | '' | 乳制品及水饮类 |
| L1 = '肉禽蛋类' AND L2 ≠ '蛋类' | '' | 肉禽类 |
| L3.endswith('熟食') | '' | 熟食类 |
| L1 IN ('冷藏及加工类', '预制菜') | '' | 冷藏加工及预制菜类 |
| 其他 | 原始 L1 ID | 原始 L1 描述 |

> 注意：`_remap_category()` 函数（L24-59）是 v3 遗留的单行应用函数，当前 `build()` 使用 numpy.where 链式向量化实现（L188-207），该函数未被调用。重映射后两类字段（原始 + 重映射）同时存在于 FM 底表中。

---

## 五、数据源表

### 5.1 原子域表（15 张：13 活跃 + 2 空骨架）

| DuckDB 表 | QDM 商分源表 | 域 | 说明 |
|-----------|-------------|---|------|
| `atomic_sales` | `strategy_fm_sales_di` | ①销售 | 20+ 销售指标，含 `abi_article_id→article_id` |
| `atomic_inventory` | `strategy_fm_purchase_di` | ②库存 | `init_stock` + `avg_inbound_price` + `purchase_receive` |
| `atomic_inventory_detail` | `strategy_fm_store_article_inventory_detail_di` | ②附 盘点 | `actual_stock_qty` + `created_by`，`shop_id→store_id`, `sku_code→article_id` |
| `atomic_scm` | `strategy_fm_scm_di` | ③供应链 | 出库/退货/订货 + 7 个单价 |
| `atomic_scm_adjust` | `strategy_fm_scm_adjust_di` | ③附 | SCM 差异调整 |
| `atomic_loss` | `strategy_fm_loss_di` | ④损耗 | `know_lost_qty` + `*_amt_src` |
| `atomic_compose` | `strategy_fm_compose_di` | ⑤加工 | `compose_in/out_qty/amt` |
| `atomic_allowance` | `strategy_fm_allowance_di` | ⑥补贴 | `allowance_amt` |
| `atomic_promo` | `strategy_fm_promo_di` | ⑦促销 | 7 个促销金额，`shop_id→store_id`, `sku_code→article_id` |
| `atomic_cost_price` | `strategy_fm_inventory_pool_di` | ⑧成本价 | `cost_price`，`shop_id→store_id`, `sku_code→article_id`, `inventory_date→business_date` |
| `atomic_price` | `strategy_fm_price_da` | ⑨价格 | 4 个售价 |
| `atomic_bom_relation` | `strategy_dim_store_article_bom_relation` | BOM关系 | `dressing_rate`, `cost_rate`, `parent_unit`, `sub_unit`, `bom_type` |
| `atomic_receive_sale` | `strategy_fm_receive_sale_di` | BOM核心 | 自购+BOM父品进货的 `inbound_qty/amt`、`sum_sub_article_qty` |
| `atomic_order_receive` | (空骨架) | — | 预留，当前不参与计算 |
| `atomic_article_convert` | (空骨架) | — | 预留，当前不参与计算 |

### 5.2 维度表（7 张）

| DuckDB 表 | QDM 源表 | 写入模式 |
|-----------|---------|---------|
| `dim_goods` | `strategy_fm_dim_goods` | 全量替换（end 日回溯 7 天取最新快照） |
| `dim_day_clear` | `strategy_fm_dim_day_clear` | 分区替换（7 天 chunk） |
| `dim_day_clear_override` | (派生) | 从 dim_goods 派生，DROP + CREATE |
| `dim_store_list` | `ads_business_analysis.chdj_store_info` | 全量替换 |
| `dim_store_profile` | `strategy_fm_dim_store_profile` | 全量替换 |
| `dim_calendar` | `strategy_fm_dim_calendar` | 全量替换 |
| `dim_saleable` | `strategy_fm_dim_saleable` | 全量替换 |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` | 全量替换 |

### 5.3 计算层表（5 张）

| DuckDB 表 | 算法 | 粒度 |
|-----------|------|------|
| `t_atomic_wide` | FULL OUTER JOIN + 父品补全 + 日清覆盖 + ROW_NUMBER 去重 | store×date×article×day_clear |
| `t_calc_bom_alloc` | Σ总权重 + 共享组识别 + 产量比例分配 | store×date×parent×sub |
| `t_calc_sku_cost` | 加权平均含期初库存（主算法 + 4 层兜底） | store×date×article |
| `t_calc_stock` | 四流合一 + 跨日滚动（中枢模块） | store×date×article×day_clear |
| `t_calc_profit` | 含 BOM + SCM 金融 + 全链路 + 多口径毛利 | store×date×article×day_clear |

### 5.4 FM 底表（6 张）

| DuckDB 表 | 粒度 | 说明 |
|-----------|------|------|
| `t_fm_sku_dim` | store×date×article×day_clear | SKU 级完整宽表，含分类重映射、7日均量 |
| `t_fm_cust` | store×date×day_clear×level | 客数聚合（6 层级） |
| `t_fm_levels_sum` | store×date×day_clear×level | 7 级分类汇总（数量/金额/比率） |
| `t_fm_levels_result` | store×date×day_clear×level | 平台对接表（中文列名+KPI） |
| `t_fm_bom_breakdown` | store×date×parent×sub | BOM 分摊溯源 |
| `t_fm_stock_roll` | store×date×article×day_clear | 库存八要素滚动 + balance_qty 校验 |

---

## 六、已知局限性 / 设计决策

1. **`is_counted` 逻辑**：依赖 `atomic_inventory_detail.created_by != '系统'` 判定人工盘点。`created_by = '系统'` 的记为系统快照（继续用方程自算），其余为人工盘点（信任实盘值覆盖 end_stock）。若源表无人工盘点记录，此分支不会触发。

2. **`end_stock_qty/amt` 未使用源表值**：`strategy_fm_purchase_di` 有上游算好的 `end_stock_qty/amt`，v0.10 选择自算。差异可能源于盘点日源表用实盘覆盖而我们用方程推算。

3. **`know_lost_*_src` 提取但未使用**：`atomic_loss` 有源表的 know/unknow_lost_amt，下游 stock.py 统一用 `euc × qty` 重算金额，未参考源表值。

4. **euc 与 QDM 成本方法差异**：fmetl 使用加权平均含期初库存的 euc，QDM 使用 `cost_price` 或 `avg_purchase_price`。fmetl 通过 `V10_INHERITED_EUC` 前向填充和 `V10_PROCESSING_RELATION` 配方推算减少差异。

5. **EUC 兜底链（主算法 + 4 层，实际 cost_source 标记）**：
   `V10_WEIGHTED_AVG`(主) → `V10_INHERITED_EUC`(ffill 继承前日) → `V10_AVG_INBOUND_FALLBACK`(进货均价) → `V10_PROCESSING_RELATION`(配方推算 `Σ(原料用量×原料euc)/产出数量`) → `V10_MATNR_CONVERT`(同 matnr 兄弟 SKU 按重量比互推)。
   `cost_price` 和 `current_price×0.40` 兜底均已移除，**无** `V10_COST_PRICE_FALLBACK`。

6. **日清品 unknow_lost 可为负值**：日清分支中 `unknow = 新供给 - sale - know_lost`，当新供给小于销售时产生负值（表示消耗期初库存）。软日清设计有意允许此行为。

7. **双口径毛利已移除**：v9 提供 `store_profit_sales` vs `store_profit_stock` 双口径对比用于检测库存-销售毛利差异，v0.10 简化为单一 `profit_amt`。如需诊断可考虑恢复。

8. **dim_goods 关联时机**：dim_goods 在 FM 底表层（sku_dim.py）统一 JOIN，所有历史日期使用最新 dim_goods 快照。计算层（stock/profit/merge）不直接引用 dim_goods 分类。日清覆盖通过 `dim_day_clear_override` 辅助表隔离。

9. **跨日依赖闭环**：自 REVIEW-009 修复后，计算层不再一次性跑完整 Step 5 再跑完整 Step 6，而是先全量计算 BOM alloc，再按营业日期串行执行 `SkuCostCalculator(date=d) -> StockCalculator(date=d)`。这样当日 `sku_cost` 读取的是本轮刚写出的前一日 `t_calc_stock.end_stock`，跨日链无需通过"运行两次"收敛。

10. **标品类数据覆盖**：部分标品 SKU 在 QDM 有销售/利润但 fmetl 无对应数据，可能源自数据提取窗口或源表覆盖差异。`chunk=30` 已改善数据完整性，但仍有少量 SKU 存在差异。
