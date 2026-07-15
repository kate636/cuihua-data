# DESIGN-001: 当日成本-库存状态机改造方案

> 日期: 2026-06-25  
> 状态: 设计稿, 待实现  
> 目标: 按"当日父品/原料单价先定价 BOM/加工转出, 最终库存单价=end_stock_amt/end_stock_qty"重构计算层  
> 相关模块: `bom_alloc.py`, `sku_cost.py`, `stock.py`, `profit.py`, `bom_breakdown.py`, `sku_dim.py`

---

## 1. 背景与问题

当前 v0.11 已修复 Step 5/6 跨日状态错位: `sku_cost` 和 `stock` 对同一 SKU 同一天使用同一套期初库存。

但现有链路仍有一个更深的会计语义问题:

```text
t_calc_bom_alloc 先用 parent_inbound_amount 直接固定 bom_alloc_amt
sku_cost 再把 bom_alloc_amt 当作子品成本池输入
stock 再把同一个 bom_alloc_amt 同时当作父品 bom_out 和子品 bom_in
```

这隐含了一个假设:

```text
BOM 父品转出金额 = 当日父品进货金额按比例分摊
```

但业务原则应是:

```text
BOM 父品转出金额 = 父品转出数量 × 父品当日可用成本单价
加工原料转出金额 = 原料转出数量 × 原料当日可用成本单价
期末库存成本单价 = 今日期末库存金额 / 今日期末库存数量
```

因此, `t_calc_bom_alloc` 不应过早成为金额事实表。它应主要描述:

- 父子关系
- 父品单位转出数量
- 子品单位流入数量
- 分配比例
- 共享组比例
- 单位换算依据

真正的金额应在同一个"当日成本-库存状态机"里, 使用当日父品/原料单价计算。

---

## 2. 设计原则

### 2.1 每个数字只算一次

`bom_out_amt`, `bom_in_amt`, `compose_out_amt`, `compose_in_amt`, `end_stock_amt`, `effective_unit_cost` 必须在同一个按日状态机中产出, 下游只消费结果, 不重复推导。

### 2.2 区分两种单价

必须显式区分:

| 字段 | 含义 | 用途 |
|---|---|---|
| `issue_unit_cost` | 当日转出成本单价 | 计算 BOM/加工/销售/损耗转出金额 |
| `effective_unit_cost` | 期末库存成本单价 | `end_stock_amt / end_stock_qty`, 写给下游展示和次日期初 |

`effective_unit_cost` 不再作为当天所有转出的唯一输入。当天转出应使用当日可用成本池算出的 `issue_unit_cost`。

### 2.3 金额跟随数量流动

数量先确定, 金额后定价:

```text
BOM 数量: t_calc_bom_alloc / t_calc_bom_plan 决定
加工数量: 加工关系 + 销售/损耗/库存反推决定
销售数量: atomic_sales 决定
损耗数量: atomic_loss / 库存方程决定
金额: 在状态机中按 issue_unit_cost 统一计算
```

### 2.4 单位转换不可混用

BOM 必须保留两套数量:

| 字段 | 单位 | 用途 |
|---|---|---|
| `bom_alloc_qty` | 父品单位 | 父品 `bom_out_qty`, 乘父品 `issue_unit_cost` |
| `bom_alloc_qty_sub` | 子品单位 | 子品 `bom_in_qty`, 进入子品库存方程 |

禁止用 `bom_alloc_qty_sub` 算父品金额, 也禁止用 `bom_alloc_qty` 算子品库存数量。

### 2.5 尽量不改公共输出表名

为了控制影响面, 保持以下表名不变:

- `t_calc_bom_alloc`
- `t_calc_sku_cost`
- `t_calc_stock`
- `t_calc_profit`
- `t_fm_sku_dim`
- `t_fm_bom_breakdown`

但可新增字段, 并重新定义部分字段的来源。

---

## 3. 改造后的计算顺序

现有 Step 4-7 改为:

```text
Step 4: BomAllocCalculator
    只产出 BOM 关系/数量/比例计划, 不再作为金额权威

Step 5/6: DailyCostStockCalculator 按日期串行
    for business_date in dates:
        1. 读取前一天 t_calc_stock.end_stock_qty/amt 作为今天 init
        2. 读取今日 receive / sale / loss / BOM计划 / 加工关系 / 盘点
        3. 构建当日 SKU 状态表
        4. 计算初始可用成本池
        5. 按 BOM 图计算父品 bom_out_amt 和子品 bom_in_amt
        6. 按加工关系计算原料 compose_out_amt 和成品 compose_in_amt
        7. 计算库存方程和分支
        8. 计算 end_stock_amt 和 effective_unit_cost
        9. 同时写出 t_calc_sku_cost 和 t_calc_stock

Step 7: ProfitCalculator
    只读取 t_calc_stock 已经算好的金额流
```

建议新增模块:

```text
fmetl/calculated/daily_cost_stock.py
```

`SkuCostCalculator` 和 `StockCalculator` 可以保留为兼容包装, 但新主流程应调用 `DailyCostStockCalculator`。

---

## 4. 状态机核心公式

### 4.1 初始成本池

每个 `store_id × business_date × article_id × day_clear` 先建立基础状态:

```text
init_qty = 昨日 t_calc_stock.end_stock_qty
init_amt = 昨日 t_calc_stock.end_stock_amt

receive_qty = self_receive_qty
receive_amt = self_receive_amt
```

首日或无前日记录时:

```text
init_qty = init_stock_qty_src
init_amt = init_stock_amt_src
```

初始可用池:

```text
pool_qty_0 = init_qty + receive_qty
pool_amt_0 = init_amt + receive_amt

issue_unit_cost_0 =
  pool_amt_0 / pool_qty_0, if pool_qty_0 > 0
  fallback_cost, otherwise
```

### 4.2 BOM 定价

`t_calc_bom_alloc` 只提供:

```text
parent_article_id
sub_article_id
bom_alloc_qty       -- 父品单位
bom_alloc_qty_sub   -- 子品单位
alloc_ratio
group_id / group_total_weight / parent_share_ratio
```

父品转出:

```text
parent_issue_unit_cost = 父品当前可用成本池单价

parent_bom_out_qty = SUM(bom_alloc_qty)
parent_bom_out_amt = parent_bom_out_qty × parent_issue_unit_cost
```

子品流入:

```text
sub_bom_in_qty = bom_alloc_qty_sub
sub_bom_in_amt = parent_bom_out_amt × 子品分配比例
```

分配比例建议:

```text
单父品:
  sub_share = row.alloc_ratio / SUM(parent rows alloc_ratio)

共享组:
  先按 parent_share_ratio 拆父品转出金额
  再按该 parent 下 row.alloc_ratio 拆到子品
```

如果 `SUM(parent rows alloc_ratio)` 因异常为 0:

```text
优先按 bom_alloc_qty_sub 比例分摊
仍为 0 时金额分摊为 0 并记录 validation warning
```

### 4.3 BOM 计算顺序

若存在多层 BOM:

```text
A -> B
B -> C
```

B 作为父品转出时, 应先接收 A 的 `bom_in` 后再计算自己的 `issue_unit_cost`。

因此需要对当日 BOM 图做拓扑排序:

```text
边: parent -> sub
按 parent 在前, sub 在后处理入库
当某个 sub 同时也是 parent, 它的出库单价应包含上游 bom_in
```

如果检测到环:

```text
1. 记录 error/warning 到日志
2. 环内 SKU 使用 pool_qty_0/pool_amt_0 的 issue_unit_cost
3. 不让 ETL 崩溃, 但在 etl-check 中标红
```

当前数据大概率是一层 BOM, 但实现时应保留拓扑能力。

### 4.4 加工定价

加工关系与 BOM 不同:

```text
BOM: 1 个父品 -> 多个子品
加工: 多个原料 -> 1 个成品
```

数量仍按当前逻辑推导:

```text
成品 compose_in_qty =
  若有盘点:
    max(0, actual_stock_qty + sale_qty + know_lost_qty - init_qty - receive_qty)
  否则:
    max(0, sale_qty + know_lost_qty - init_qty - receive_qty)

原料 compose_out_qty =
  SUM(成品 compose_in_qty × raw_qty / yield_qty)
```

金额改为在状态机中按原料今日单价算:

```text
raw_issue_unit_cost = 原料当前可用成本池单价
raw_compose_out_amt = raw_compose_out_qty × raw_issue_unit_cost

finished_compose_in_amt =
  SUM(raw_compose_out_amt 按该成品消耗的原料份额归集)
```

加工应在 BOM 之后处理, 因为原料可能先接收 BOM 流入。

如果存在加工链:

```text
原料 A -> 半成品 B -> 成品 C
```

同样需要拓扑排序:

```text
先处理 B 的 compose_in, 再允许 B 作为 C 的 raw compose_out
```

如果加工关系出现环, 处理策略同 BOM 环。

### 4.5 库存数量方程

数量方程保持现有语义:

```text
eq_qty =
  init_qty
+ receive_qty
+ bom_in_qty - bom_out_qty
+ compose_in_qty - compose_out_qty
- sale_qty
- know_lost_qty
```

分支仍沿用当前 `stock.py`:

```text
1. is_counted:
   end_qty = actual_stock_qty
   unknow_lost_qty = eq_qty - actual_stock_qty

2. day_clear='0':
   new_supply = receive_qty + bom_in_qty - bom_out_qty + compose_in_qty - compose_out_qty
   end_qty = max(0, init_qty - max(0, sale_qty + know_lost_qty - new_supply))
   unknow_lost_qty = new_supply - sale_qty - know_lost_qty

3. eq_qty < 0:
   end_qty = 0
   unknow_lost_qty = eq_qty
   neg_clamp_qty = -eq_qty

4. know_lost_qty > 0:
   end_qty = eq_qty
   unknow_lost_qty = 0

5. actual_stock_qty > eq_qty + 0.001:
   end_qty = actual_stock_qty
   unknow_lost_qty = eq_qty - actual_stock_qty

6. normal:
   end_qty = eq_qty
   unknow_lost_qty = 0
```

### 4.6 库存金额方程

状态机不再简单使用:

```text
end_stock_amt = end_stock_qty × euc
```

而是先计算所有转出金额:

```text
sale_cost_amt       = sale_qty × issue_unit_cost_after_inflows
know_lost_amt       = know_lost_qty × issue_unit_cost_after_inflows
unknow_lost_amt     = unknow_lost_qty × issue_unit_cost_after_inflows
neg_clamp_cost_amt  = neg_clamp_qty × issue_unit_cost_after_inflows
```

金额方程:

```text
eq_amt =
  init_amt
+ receive_amt
+ bom_in_amt - bom_out_amt
+ compose_in_amt - compose_out_amt
- sale_cost_amt
- know_lost_amt
```

分支金额:

```text
is_counted:
  end_amt = actual_stock_qty × issue_unit_cost_after_inflows
  unknow_lost_amt = eq_amt - end_amt

day_clear='0':
  end_amt = end_qty × issue_unit_cost_after_inflows
  unknow_lost_amt = eq_amt - end_amt

eq_qty < 0:
  end_amt = 0
  unknow_lost_amt = unknow_lost_qty × issue_unit_cost_after_inflows
  neg_clamp_cost_amt = neg_clamp_qty × issue_unit_cost_after_inflows

normal:
  end_amt = eq_amt
```

最终库存单价:

```text
effective_unit_cost =
  end_amt / end_qty, if end_qty > 0
  issue_unit_cost_after_inflows, if end_qty = 0 and issue_unit_cost_after_inflows > 0
  fallback_cost, otherwise
```

注意:

- `effective_unit_cost` 是期末库存单价, 供次日期初继承。
- `issue_unit_cost` 是当日转出单价, 供 BOM/加工/销售/损耗金额计算。
- 当 `end_qty=0` 时, `effective_unit_cost` 只能作为下游展示/兜底参考, 不代表真实库存余额。

---

## 5. 表结构调整

### 5.1 `t_calc_bom_alloc`

保留现有字段, 但重新定义金额字段:

| 字段 | 调整 |
|---|---|
| `bom_alloc_qty` | 保留, 父品单位, 权威 |
| `bom_alloc_qty_sub` | 保留, 子品单位, 权威 |
| `alloc_ratio` | 保留, 成本/数量分配权重 |
| `bom_alloc_amt` | 不再由 `bom_alloc.py` 固定; 后续由状态机回填或视为 legacy |
| `sub_alloc_amt` | 同上 |
| `sub_unit_cost` | 同上 |
| `parent_unit_price` | 改为观测字段, 不参与金额计算 |

建议新增:

| 字段 | 用途 |
|---|---|
| `bom_group_id` | 共享组标识 |
| `parent_share_ratio` | 共享组中父品金额/数量份额 |
| `qty_split_ratio` | 父品数量在共享组中的拆分比例 |
| `amount_source` | `DAILY_STATE` / `LEGACY_PARENT_INBOUND` |

实现方式:

- Phase 1 可保留 `bom_alloc_amt` 的 legacy 值, 但 `stock.py` 不再读取它作为金额权威。
- Phase 2 由状态机更新/另写 BOM 金额明细, 供 `bom_breakdown.py` 展示。

### 5.2 `t_calc_sku_cost`

保留表名, 但从"最终成本计算模块输出"改为"当日成本状态摘要":

新增字段:

| 字段 | 含义 |
|---|---|
| `issue_unit_cost` | 当日转出单价 |
| `ending_unit_cost` | `end_stock_amt / end_stock_qty` |
| `pool_qty_before_out` | 转出前可用池数量 |
| `pool_amt_before_out` | 转出前可用池金额 |
| `bom_in_amt_daily` | 状态机计算的 BOM 流入金额 |
| `bom_out_amt_daily` | 状态机计算的 BOM 流出金额 |
| `compose_in_amt_daily` | 状态机计算的加工流入金额 |
| `compose_out_amt_daily` | 状态机计算的加工流出金额 |

兼容字段:

```text
effective_unit_cost = ending_unit_cost
total_cost_amt      = pool_amt_before_out
cost_qty            = pool_qty_before_out
```

### 5.3 `t_calc_stock`

新增字段:

| 字段 | 含义 |
|---|---|
| `issue_unit_cost` | 当日转出单价 |
| `ending_unit_cost` | 期末库存单价 |
| `sale_cost_amt` | 销售成本金额, 从 profit.py 前移到 stock.py |
| `amount_balance_residual` | 金额方程残差 |
| `qty_balance_residual` | 数量方程残差 |

兼容字段:

```text
effective_unit_cost = ending_unit_cost
```

现有字段 `bom_in_amt`, `bom_out_amt`, `compose_in_amt`, `compose_out_amt` 改由状态机计算。

### 5.4 `t_calc_profit`

`ProfitCalculator` 不再计算 `sale_cost_amt = sale_qty × effective_unit_cost`。

改为:

```text
sale_cost_amt = t_calc_stock.sale_cost_amt
```

利润主公式仍可保持:

```text
profit =
  sale_amt
- receive_amt
- bom_in_amt + bom_out_amt
- compose_in_amt + compose_out_amt
+ end_stock_amt - init_stock_amt
- neg_clamp_cost_amt
```

前提是 `t_calc_stock` 的金额流已由状态机统一产出。

### 5.5 `t_fm_bom_breakdown`

展示字段改为读取状态机后的金额:

```text
sub_alloc_amt = daily_bom_in_amt allocated to sub
sub_unit_cost = sub_alloc_amt / bom_alloc_qty_sub
parent_unit_price = parent issue_unit_cost
```

可继续使用 `t_calc_bom_alloc` 作为主表, 但金额字段必须来自状态机回填或新增 `t_calc_bom_daily_amount`。

建议新增中间表:

```text
t_calc_bom_daily_amount
  store_id
  business_date
  parent_article_id
  sub_article_id
  parent_issue_unit_cost
  parent_bom_out_qty
  parent_bom_out_amt
  sub_bom_in_qty
  sub_bom_in_amt
  alloc_ratio
  parent_share_ratio
```

`t_fm_bom_breakdown` 后续以此表为金额权威。

---

## 6. 文件级改造方案

### 6.1 `bom_alloc.py`

职责调整:

```text
从"金额分摊器"改为"BOM数量与比例计划生成器"
```

改动:

1. 保留共享组识别、消费权重、分配比例计算。
2. 保留 `bom_alloc_qty` / `bom_alloc_qty_sub`。
3. 保留 legacy `bom_alloc_amt` 字段但标记为非权威。
4. 增加 `bom_group_id`, `parent_share_ratio`, `qty_split_ratio`, `amount_source`。
5. 不再让下游以 `bom_alloc_amt` 作为 BOM 金额来源。

### 6.2 新增 `daily_cost_stock.py`

新增类:

```python
class DailyCostStockCalculator:
    def run(self, start: str, end: str) -> None:
        ...
```

内部建议拆函数:

```text
_load_day_inputs(date)
_load_prev_stock(date)
_build_day_state(inputs, prev_stock)
_derive_compose_qty(state)
_apply_bom_amounts(state, bom_plan)
_apply_compose_amounts(state, processing_relations)
_apply_stock_quantity_branches(state)
_apply_stock_amount_branches(state)
_write_sku_cost(state)
_write_stock(state)
_write_bom_daily_amount(state)
```

### 6.3 `sku_cost.py`

两种选择:

| 方案 | 做法 | 推荐 |
|---|---|---|
| A | 保留 `SkuCostCalculator`, 内部委托 `DailyCostStockCalculator` 的成本摘要输出 | 过渡期推荐 |
| B | 删除主流程调用, 仅保留加工关系 helper | 长期推荐 |

Phase 1 推荐 A, 降低改动面。

### 6.4 `stock.py`

两种选择:

| 方案 | 做法 | 推荐 |
|---|---|---|
| A | 保留 `StockCalculator`, 内部委托 `DailyCostStockCalculator` 的库存输出 | 过渡期推荐 |
| B | 将 `StockCalculator` 改造成新状态机 | 可行但文件会过大 |

Phase 1 推荐新增文件, 让 `stock.py` 保留兼容。

### 6.5 `executor.py`

当前:

```text
for d in dates:
    SkuCostCalculator.run(d)
    StockCalculator.run(d)
```

改为:

```text
DailyCostStockCalculator(duck).run(start, end)
```

保留日志:

```text
Step 5/6: 当日成本-库存状态机 -> t_calc_sku_cost + t_calc_stock
```

### 6.6 `profit.py`

改动:

1. 从 `t_calc_stock` 读取 `sale_cost_amt`。
2. `pre_profit_amt` 用 `sale_cost_amt` 或 `sale_qty × issue_unit_cost`, 不再用 `effective_unit_cost`。
3. 保持 `profit_amt` 主公式不变。

### 6.7 `sku_dim.py`

改动:

1. 继续读取 `effective_unit_cost`, 但其语义变为期末库存单价。
2. 新增读取 `issue_unit_cost` 和 `sale_cost_amt`。
3. 报表如展示销售成本, 应优先用 `sale_cost_amt`。

### 6.8 `bom_breakdown.py`

改动:

1. 优先读取 `t_calc_bom_daily_amount`。
2. 若不存在, 回退 `t_calc_bom_alloc` legacy 金额, 并标记 `amount_source='LEGACY'`。

---

## 7. 关键边界与处理策略

### 7.1 父品没有可用成本池

若父品 `pool_qty_0 <= 0` 且需要 BOM 转出:

兜底顺序:

```text
1. 前一日 ending_unit_cost
2. 当日 avg_inbound_price
3. 同 matnr 换算
4. 仍为 0: 金额为 0, cost_source='ZERO_COST_BOM_PARENT'
```

该情况必须进入 etl-check。

### 7.2 子品自采 + BOM 同日并存

子品成本池应同时包含:

```text
init + self_receive + bom_in
```

如果子品当天还有销售/损耗, 销售成本使用包含 BOM 流入后的 `issue_unit_cost_after_inflows`。

### 7.3 父品剩余库存转移

现有 `stock_transfer` 逻辑保留, 但金额来源改为状态机金额:

```text
transfer_out_amt = 父品剩余 end_amt
transfer_in_amt = transfer_out_amt × 子品分配比例
```

仍然:

```text
父品 end = 0
子品 end += transfer_in
子品 bom_in += transfer_in
不额外增加父品 bom_out
```

因为父品的 BOM 转出金额已在当日状态机中按父品单价完整计算, 再补父品 `bom_out` 会重复。

### 7.4 日清品

日清品数量分支不变。金额侧要注意:

```text
end_amt = end_qty × issue_unit_cost_after_inflows
unknow_lost_amt = eq_amt - end_amt
```

允许 `unknow_lost_amt` 为负。

### 7.5 负库存钉零

继续使用 FIX-020 口径:

```text
end_qty = 0
unknow_lost_qty = eq_qty
neg_clamp_qty = -eq_qty
```

金额:

```text
neg_clamp_cost_amt = neg_clamp_qty × issue_unit_cost_after_inflows
```

`profit.py` 继续扣减 `neg_clamp_cost_amt`。

### 7.6 期末库存为 0

如果:

```text
end_qty = 0
end_amt = 0
```

则:

```text
effective_unit_cost = issue_unit_cost_after_inflows
```

仅作为次日无供给时的成本参考, 不代表当前库存余额。

---

## 8. 验收标准

### 8.1 内部一致性

1. `t_calc_sku_cost` 与 `t_calc_stock` 期初一致:

```sql
SELECT COUNT(*)
FROM t_calc_sku_cost c
JOIN t_calc_stock s USING(store_id,business_date,article_id)
WHERE ABS(c.init_stock_qty-s.init_stock_qty)>0.001
   OR ABS(c.init_stock_amt-s.init_stock_amt)>0.01;
```

期望: 0。

2. BOM 金额守恒:

```sql
SELECT business_date,
       ROUND(SUM(bom_in_amt)-SUM(bom_out_amt)-SUM(stock_transfer_in_amt), 2) AS residual
FROM t_calc_stock
GROUP BY business_date;
```

期望: residual 接近 0。

3. 加工金额守恒:

```sql
SELECT business_date,
       ROUND(SUM(compose_in_amt)-SUM(compose_out_amt), 2) AS compose_net
FROM t_calc_stock
GROUP BY business_date;
```

期望: 若加工关系只表示内部转化, 全局接近 0; 若存在损耗/产出率差, 差异需可解释。

4. 数量方程残差:

```sql
SELECT COUNT(*)
FROM t_calc_stock
WHERE ABS(qty_balance_residual) > 0.001;
```

期望: 0 或全部有明确分支解释。

5. 金额方程残差:

```sql
SELECT COUNT(*), SUM(ABS(amount_balance_residual))
FROM t_calc_stock
WHERE ABS(amount_balance_residual) > 0.01;
```

期望: 大额残差为 0。

### 8.2 关键 SKU 验收

必须跟踪:

| SKU | 原因 |
|---|---|
| `21110009` 海大虾 | 水产差异关键 SKU, BOM 子品 |
| `21275531` | BOM 父品库存转移例子 |
| `21285257` | BOM 子品接收转移例子 |
| 烘焙加工成品 TOP SKU | 加工关系验证 |
| euc=0 且有销售 SKU | 兜底链验证 |

### 8.3 QDM 对比

对比范围先用本地已有完整窗口:

```text
2026-06-18 ~ 2026-06-22
```

核心指标:

| 指标 | 期望 |
|---|---|
| 销售额 | 0 差异 |
| 门店毛利总额 | 不应变差, 目标继续接近 ±5% |
| 水产类 | 应解释差异变化, 不要求强行归零 |
| 熟食/烘焙 | 加工关系改动后不应恶化 |
| BOM 父品 | 独立毛利接近 0 或差异可解释 |

---

## 9. 实施阶段建议

### Phase 1: 影子表验证

新增 `DailyCostStockCalculator`, 先写影子表:

```text
t_calc_sku_cost_v2
t_calc_stock_v2
t_calc_bom_daily_amount_v2
```

不影响现有生产表。跑 6/18-6/22, 对比:

```text
t_calc_stock_v2 vs t_calc_stock
t_fm_levels_result_v2 vs QDM
```

### Phase 2: 切主链路

当影子表验证通过:

```text
executor.py Step 5/6 切到 DailyCostStockCalculator
正式写 t_calc_sku_cost / t_calc_stock / t_calc_bom_daily_amount
```

### Phase 3: 下游清理

更新:

```text
profit.py
sku_dim.py
bom_breakdown.py
stock_roll.py
etl-check skill
architecture docs
```

### Phase 4: 云端部署

部署前必须:

1. 本地全量验证通过。
2. 代码提交并推送。
3. 云端 `git pull --ff-only` 后确认代码含 `DailyCostStockCalculator`。
4. 云端重跑目标日期。
5. 查询云端 `sku_cost/stock` 期初错位为 0。

---

## 10. 风险与回滚

### 10.1 主要风险

| 风险 | 影响 | 控制 |
|---|---|---|
| BOM 多层/环未处理 | 金额顺序错误 | 拓扑排序 + 环检测 |
| 加工关系不完整 | 熟食/烘焙成本异常 | 保留 fallback + 输出缺口清单 |
| `effective_unit_cost` 语义变化 | 下游报表误解 | 新增 `issue_unit_cost`, 文档同步 |
| QDM 差异短期变大 | 影响验收 | 先影子表对比, 不直接切主表 |
| `bom_breakdown` 金额来源变化 | 溯源口径变化 | 新增 `amount_source` |

### 10.2 回滚策略

Phase 1 使用影子表, 无需回滚。

Phase 2 切主链路后, 回滚方式:

```text
1. executor.py 恢复 Step 5/6 调用 SkuCostCalculator + StockCalculator
2. profit.py 恢复 sale_cost_amt = sale_qty × effective_unit_cost
3. bom_breakdown.py 恢复读取 t_calc_bom_alloc legacy 金额
4. 重新跑目标日期 ETL
```

---

## 11. 最终设计判断

这次不是简单修一个水产 SKU, 而是要把成本会计方向理顺:

```text
关系/数量计划先行
当日父品/原料单价定价转出
金额随库存流转
期末库存单价由 end_amt/end_qty 得出
次日继承期末库存金额和数量
```

实现后, BOM 和加工都会从"金额预分摊"变成"数量驱动的当日成本流", 更符合库存会计逻辑, 也更容易解释分类 × 日期差异矩阵。
