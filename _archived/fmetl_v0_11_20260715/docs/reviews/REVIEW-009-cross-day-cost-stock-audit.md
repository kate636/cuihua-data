# REVIEW-009: 跨日成本-库存闭环审计与水产差异根因

> **日期**: 2026-06-25  
> **范围**: fmetl v0.11 全链路代码审查 + 阿里云生产 DuckDB 只读实证  
> **重点**: BOM 处理、物料号处理、库存转换处理、加工逻辑处理，以及分类 × 日期差异矩阵的最大可修问题  
> **生产数据源**: 阿里云 `/opt/fm/data/fm.duckdb`，数据范围 2026-06-18 ~ 2026-06-24  
> **QDM 基准**: `default_catalog.ads_business_analysis.strategy_fm_levels_result`  

---

## 1. 执行摘要

目前能解决的最大问题不是"水产类本身"，而是 **`sku_cost` 与 `stock` 的跨日依赖顺序导致同一 SKU 同一天的期初库存不一致**。水产类，尤其 `21110009 海大虾`，是这个问题最明显的暴露点。

**判断把握: 90%。**

这个问题不同于"涉及 BOM 的 SKU 计算差异是正常的"。BOM 父子品利润在 SKU 级与 QDM 不一致可以接受；但 `t_calc_sku_cost` 和 `t_calc_stock` 对同一行使用不同期初库存，是工程链路不闭合，不是业务口径差。

---

## 2. 审查方法

本轮先审查、不改代码。读取并使用:

- CC memory: `qdm_comparison_methodology`、`feedback_server_data`、`karpathy_software_engineering_rules`、`reference-master-data-authority`
- 项目 skills: `.cursor/skills/qdm-compare/SKILL.md`、`.cursor/skills/etl-check/SKILL.md`、`.cursor/skills/master-data/SKILL.md`
- 代码文件: `executor.py`、`atomic/*`、`calculated/*`、`fm_tables/*`
- 生产 DuckDB: SSH 到阿里云后 `duckdb.connect('/opt/fm/data/fm.duckdb', read_only=True)`
- QDM API: 只读查询 `strategy_fm_levels_result`

对比原则:

1. 销售额、进货额应同源一致。
2. 验收看分类 × 日期矩阵，SKU 级 BOM 差异允许存在。
3. 先按 SKU 级 QDM 口径下钻，再做分类重映射或 1:1 品类聚合。
4. 区分可修工程问题、业务口径差、数据源缺口。

---

## 3. P0 根因: Step 5/6 跨日链不闭合

### 3.1 代码事实

`executor.py` 当前计算层执行顺序:

```python
BomAllocCalculator(duck).run()
SkuCostCalculator(duck).run()
StockCalculator(duck).run()
ProfitCalculator(duck).run()
```

即 Step 5 `sku_cost` 先于 Step 6 `stock`。

但 `sku_cost.py` 会读取 `t_calc_stock` 的前日 `end_stock` 来计算当日 EUC:

```sql
FROM t_atomic_wide sp
INNER JOIN t_calc_stock cs
  ON sp.store_id = cs.store_id
 AND sp.article_id = cs.article_id
 AND cs.business_date < sp.business_date
```

这意味着当次运行中，`sku_cost` 使用的是 **上一次 ETL 运行残留的 `t_calc_stock`**，而不是本轮刚算出的前日库存。

架构文档已有类似说明: `sku_cost (Step 5) 读取 t_calc_stock (Step 6 产出)`，因此"修复 BOM 等影响库存的模块后需运行两次才能完全更新跨日链"。本轮生产实证显示，仅靠"运行两次"不是稳健机制。

### 3.2 生产实证: 海大虾 6/22 期初不一致

`21110009 海大虾` 在 2026-06-22 的同一行，成本层和库存层的期初不一致:

| 日期 | SKU | `sku_cost` 期初数量/金额 | `stock` 期初数量/金额 | EUC |
|---|---|---:|---:|---:|
| 2026-06-22 | 21110009 海大虾 | 8.542918 / 923.22 | 15.824375 / 747.67 | 108.0689 |

同一 SKU、同一天，`sku_cost` 用一套期初，`stock` 用另一套期初。这个差异直接把 EUC 从约 47 跳到 108，并持续影响后续日期。

### 3.3 全局影响

生产库中 `t_calc_sku_cost.init_stock_*` 与 `t_calc_stock.init_stock_*` 不一致的行按原始大类分布:

| 原始大类 | 行数 | 期初数量绝对差 | 期初金额绝对差 | 涉及销售额 | 涉及毛利 |
|---|---:|---:|---:|---:|---:|
| 预制菜 | 141 | 578.437 | 20,236.09 | 483.13 | -373.57 |
| 冷藏及加工类 | 204 | 896.403 | 7,075.80 | 3,074.40 | 22.90 |
| 蔬菜类 | 163 | 834.483 | 5,008.70 | 5,140.60 | 868.02 |
| 肉禽蛋类 | 25 | 30.240 | 2,271.64 | 5,052.57 | -270.90 |
| 水产类 | 35 | 42.978 | 1,919.89 | 1,727.49 | 813.94 |
| 水果类 | 75 | 97.006 | 1,749.14 | 7,257.13 | 219.81 |
| 标品类 | 85 | 28.000 | 268.29 | 278.18 | -289.38 |

水产不是唯一受害品类，但它在矩阵上最刺眼，因为 `海大虾` 单 SKU 的波动大、BOM 参与明确、销售额集中。

---

## 4. 水产类是否第一优先

**结论: 是第一优先验证靶子，但不是第一优先根因。**

QDM SKU 级水产汇总:

| 日期 | QDM 销售额 | QDM 毛利 |
|---|---:|---:|
| 2026-06-18 | 1,266.18 | 29.13 |
| 2026-06-19 | 828.18 | 182.53 |
| 2026-06-20 | 848.57 | -512.42 |
| 2026-06-21 | 758.41 | -520.62 |
| 2026-06-22 | 1,035.59 | 314.50 |
| 2026-06-23 | 735.08 | 58.62 |
| 2026-06-24 | 716.21 | 140.01 |

FM `t_fm_levels_result` 水产:

| 日期 | FM 销售额 | FM 毛利 | FM-QDM |
|---|---:|---:|---:|
| 2026-06-18 | 1,266.18 | 139.23 | +110.10 |
| 2026-06-19 | 828.18 | 148.19 | -34.34 |
| 2026-06-20 | 848.57 | 52.61 | +565.03 |
| 2026-06-21 | 758.41 | -174.63 | +345.99 |
| 2026-06-22 | 1,035.59 | 695.40 | +380.90 |
| 2026-06-23 | 735.08 | -191.69 | -250.31 |
| 2026-06-24 | 716.21 | -506.31 | -646.32 |

单 SKU `21110009 海大虾` 是主驱动:

| 日期 | QDM 毛利 | FM 毛利 | FM-QDM |
|---|---:|---:|---:|
| 2026-06-20 | -364.07 | +27.21 | +391.28 |
| 2026-06-21 | -293.78 | +62.61 | +356.39 |
| 2026-06-22 | +253.26 | +608.15 | +354.89 |
| 2026-06-24 | +180.08 | -497.08 | -677.16 |

这说明水产类是最好的 P0 验收对象。修复跨日成本-库存闭环后，应先看 `海大虾` 的 EUC 是否不再异常跳变，再看水产矩阵是否收窄。

---

## 5. BOM 处理审查

### 5.1 当前结论

BOM 主逻辑目前不是最大问题。生产健康检查:

| 指标 | 值 |
|---|---:|
| `SUM(bom_in_amt) - SUM(bom_out_amt)` | 456.34 |
| `SUM(stock_transfer_in_amt)` | 456.34 |
| 真残差 | 0.00 |

这符合当前设计不变式:

```text
bom_in - bom_out == stock_transfer_in
residual = bom_in - bom_out - stock_transfer_in ≈ 0
```

因此，BOM 金额守恒成立。

### 5.2 海大虾 BOM 链

`21110009 海大虾` 的 BOM 来源:

| 日期 | 父品 | 子品 | 父品入库金额 | 分摊金额 | 子品单位成本 |
|---|---|---|---:|---:|---:|
| 2026-06-18 | 21259821 海大虾12kg/箱 | 21110009 海大虾 | 566.85 | 566.85 | 47.2375 |
| 2026-06-20 | 21259821 海大虾12kg/箱 | 21110009 海大虾 | 566.85 | 566.85 | 47.2375 |
| 2026-06-21 | 21259821 海大虾12kg/箱 | 21110009 海大虾 | 566.85 | 566.85 | 47.2375 |

BOM 分摊本身给出的子品成本稳定在 47.2375。异常发生在后续跨日 EUC 继承，而不是 BOM 单日分摊金额。

### 5.3 中风险: 3+ 父品共享组

`bom_alloc.py` 的共享组识别是两两子集配对，对 3+ 父品链不够干净。已有 REVIEW-008 也标注为当前无害但逻辑不够稳健。

本轮没有证据表明它是水产最大差异来源。建议在 P0 后补单测:

```sql
SELECT store_id, business_date, parent_article_id,
       SUM(bom_alloc_amt) AS alloc_amt,
       MAX(parent_inbound_amount) AS parent_amt
FROM t_calc_bom_alloc
GROUP BY 1,2,3
HAVING ABS(SUM(bom_alloc_amt) - MAX(parent_inbound_amount)) > 0.01;
```

---

## 6. 物料号处理审查

### 6.1 当前结论

`matnr` 不是当前最大问题。生产数据中 `V10_MATNR_CONVERT` 主要出现在冷藏乳品、烘焙、冷藏加工及预制菜类，未看到它驱动水产差异。

### 6.2 结构缺口

`atomic_article_convert` 仍是空骨架，A 进 B 出、包装/称重换算、同物料多 SKU 的业务转换没有真正进入主链路。

`MatnrResultBuilder` 只是 FM 报表层按 `matnr` 合并 SKU，不会修正 SKU 级成本流。它能提供物料号口径展示，但不能解决库存和 EUC 的跨 SKU 传递问题。

优先级: P1/P2。应在 P0 跨日闭环修复后单独设计。

---

## 7. 库存转换处理审查

### 7.1 父品库存转移

父品库存转移机制当前符合设计:

1. 父品 `sale=0, bom_out>0, end>0` 时触发。
2. 父品 `end_stock` 清零。
3. 按 BOM 分摊比例转给子品。
4. 子品同步增加 `stock_transfer_in` 和 `bom_in`，抵消对子品 profit 的影响。

生产 BOM 真残差为 0，说明该机制金额守恒。

### 7.2 库存方程残差

生产健康检查:

| day_clear | 残差 |
|---|---:|
| 0 | 45.20 |
| 1 | -873.29 |

这与 FIX-020 文档中"非日清 dc='1' 应接近 0"存在冲突。可能原因:

1. 生产 DuckDB 尚未按 FIX-020 完整重刷。
2. 服务器代码与本地代码/文档版本不同步。
3. 部分历史日期仍沿用旧口径残留。

这不是本轮最大根因，但修复 P0 后必须一起验收。

---

## 8. 加工逻辑审查

### 8.1 当前结论

加工逻辑不是水产差异的主因。

`sku_cost.py` 的加工逻辑走:

```text
成品 compose_in_qty = max(0, sale + loss - init - recv)
原料 compose_out_qty = Σ(成品 compose_in × raw_qty / yield_qty)
成品 compose_in_amt = compose_in_qty × Σ(raw_qty / yield_qty × raw_base_euc)
原料 compose_out_amt = compose_out_qty × base_euc
```

这个方向符合"加工关系推算 + 价值守恒"的设计。

### 8.2 风险

加工关系覆盖不足仍会影响熟食类、预制菜、冷藏加工类。生产零成本有销售行显示，熟食类、猪肉类、冷藏乳品类等仍有 `effective_unit_cost=0` 但有销售的行。

这属于 P1: EUC 兜底链和加工关系补全。它不应抢在 P0 跨日闭环前面。

---

## 9. 分类映射与客数口径

`sku_dim.py` 声称使用 master-data v2.3，但仍有 TODO:

- 冷冻类应使用 SKU 清单 `config/frozen_skus.json`，当前临时使用中分类 `冰品类`。

`cust.py` 注释和逻辑仍是 master-data v2.1，缺少:

- `预制菜 + sale_unit='千克' → 熟食类`
- `category_level3_description LIKE '%熟食' → 熟食类`

这会污染客数、动销 SKU、品效等分类指标，也可能影响非毛利类矩阵。但不是当前水产毛利差异主因。

优先级: P2，可与 master-data 对齐专项一起做。

---

## 10. 建议修复方案

### 10.1 P0: 按日期串行闭环 Step 5/6

目标: 让当日 `sku_cost` 使用本轮刚计算出的前日 `stock.end_stock`，而不是旧表残留。

建议方向:

1. `BomAllocCalculator` 仍可先全量跑。
2. 将 `SkuCostCalculator` 和 `StockCalculator` 改造成支持单日/日期参数。
3. 计算层按日期串行:

```text
for d in dates:
    SkuCostCalculator.run(date=d, prev_stock=本轮已算出的前日 stock)
    StockCalculator.run(date=d)
ProfitCalculator.run(start,end)
```

或更彻底:

```text
把 EUC 与 stock 合并进同一个按日状态机:
  读前日 end_stock
  算当日 euc
  算当日 stock/end_stock
  写出 sku_cost + stock
```

第二种结构更干净，但改动更大。第一种更贴近现有代码，风险较低。

### 10.2 P0 验收标准

修复后必须验证:

1. `t_calc_sku_cost.init_stock_qty/amt` 与 `t_calc_stock.init_stock_qty/amt` 在同一 `(store,date,article)` 上一致。
2. `21110009 海大虾` 6/20~6/24 EUC 不再从 47 跳到 108，除非有真实进货/盘点证据。
3. 水产类分类 × 日期差异矩阵收窄，尤其 2026-06-24 的 -646 差异应显著下降。
4. BOM 真残差仍为 0:

```sql
SUM(bom_in_amt) - SUM(bom_out_amt) - SUM(stock_transfer_in_amt) ≈ 0
```

5. 非日清库存方程残差接近 0，或明确解释残差来源。
6. 总销售额、进货额与 QDM 仍一致。

---

## 11. 优先级排序

| 优先级 | 问题 | 判断 | 原因 |
|---|---|---|---|
| P0 | `sku_cost` / `stock` 跨日状态不一致 | 必须先修 | 已有生产实证，直接驱动水产和生鲜矩阵 |
| P0 验证靶子 | 水产类 / `21110009 海大虾` | 先盯它 | 单 SKU 驱动大额日差，BOM 链清晰 |
| P1 | 零成本有销售兜底 | P0 后修 | 熟食/猪肉/乳品仍有销售=毛利的行 |
| P1/P2 | A 进 B 出 / 物料号转换入主链 | 单独设计 | 当前 matnr 只是报表聚合，不修 SKU 成本流 |
| P2 | master-data v2.3 对齐 cust/冷冻清单 | 需要做 | 影响客数、动销、品效和分类稳定性 |
| P2 | BOM 3+ 父品共享组单测 | 加固 | 当前守恒，但算法结构不够稳健 |

---

## 12. 当前回答不了的问题

1. 生产库 dc='1' 库存方程残差 -873.29 的精确来源，需要在修复 P0 后重新跑健康检查确认。
2. `zglfz/zglfm` 的物理语义方向仍需公司 IDE 或源表字段说明验证。
3. 全量分类 × 日期矩阵如果严格按 master-data v2.3，需要从 QDM SKU 级重新 JOIN/重映射，不应直接混用 QDM 大分类层。

---

## 13. 最终判断

水产类确实应该作为当前优先下钻对象，但修复目标不应写成"修水产"。更准确的 P0 是:

> 修复跨日成本-库存闭环，使 `sku_cost` 与 `stock` 对同一 SKU 同一天使用同一套期初库存状态。

这个问题修掉后，再评估水产、熟食、牛羊等剩余差异。若水产仍有大差，再进入 BOM 分摊细节或 QDM 结构性口径差判断。

---

## 14. 执行记录 (2026-06-25)

### 14.1 已实施改动

1. `executor.py` 将计算层改为按日期串行闭环:

```text
Step 4: BOM alloc
for d in dates:
    Step 5: SkuCostCalculator.run(start=d, end=d)
    Step 6: StockCalculator.run(start=d, end=d)
Step 7: ProfitCalculator
```

2. `SkuCostCalculator.run()` 支持日期过滤，并在单日运行时从已写出的 `t_calc_stock` 读取前一营业日 `end_stock_qty/amt` 作为当日期初。
3. `StockCalculator.run()` 支持日期过滤，按单日分区覆盖写入，保留其他日期的已算结果。
4. 修复 `sku_cost.py` 前日库存 SQL 未使用 f-string 的问题，并收窄异常捕获，避免 SQL 错误被误当成"首日无前日库存"。
5. 仅本地刷新 `t_fm_sku_dim -> t_fm_levels_sum -> t_fm_levels_result`，跳过 `CustBuilder` 和 Step 14 云端候选同步，避免本地验证写远端状态。

### 14.2 本地验证结果

验证范围: 本地 DuckDB `data/fm.duckdb`, `2026-06-18` ~ `2026-06-22`。

| 验证项 | 结果 |
|---|---:|
| `py_compile` | 通过 |
| `python3 -m fmetl.executor 2026-06-18 2026-06-22 --calc-only` | 通过, 38.2s |
| `t_calc_sku_cost` vs `t_calc_stock` 期初错位行数 | 0 |
| 期初错位数量绝对值合计 | 0 |
| 期初错位金额绝对值合计 | 0 |
| `t_calc_stock.end_stock_qty < 0` | 0 |
| BOM 真残差 `bom_in - bom_out - transfer_in` | 0.00 |
| 本地 FM 三表刷新 | `t_fm_sku_dim/t_fm_levels_sum/t_fm_levels_result` 通过 |

`21110009 海大虾` 关键链路已对齐:

| 日期 | cost_init_qty | stock_init_qty | cost_init_amt | stock_init_amt | euc |
|---|---:|---:|---:|---:|---:|
| 2026-06-18 | 0.000000 | 0.000000 | 0.00 | 0.00 | 47.237500 |
| 2026-06-19 | 0.806000 | 0.806000 | 38.07 | 38.07 | 47.237500 |
| 2026-06-20 | 0.000000 | 0.000000 | 0.00 | 0.00 | 47.247852 |
| 2026-06-21 | 8.281457 | 8.281457 | 391.28 | 391.28 | 47.247852 |
| 2026-06-22 | 15.824375 | 15.824375 | 747.67 | 747.67 | 47.247852 |

这修掉了审查时发现的 2026-06-22 成本层 `8.542918 / 923.22` 与库存层 `15.824375 / 747.67` 的状态错位。

### 14.3 QDM 对比结果

QDM 只读 API 对比范围: `2026-06-18` ~ `2026-06-22`。对比口径:

- QDM: `strategy_fm_levels_result`, `level_description='中分类'`, `day_clear='2'`, 按 master-data 中分类规则重映射到报告大类。
- FM: 本地 `t_fm_levels_result`, `分类等级='大类'`, `day_clear='2'`。

| 指标 | QDM | FM | 差异 | 差异率 |
|---|---:|---:|---:|---:|
| 全天销售额 | 同源 | 同源 | 0.00 | 0.0% |
| 门店毛利额 | 11,246.10 | 11,952.91 | +706.81 | +6.28% |

剩余差异 TOP 大类合计:

| 大类 | FM毛利 | QDM毛利 | 差异 | 判断 |
|---|---:|---:|---:|---|
| 水产类 | -15.65 | -506.88 | +491.23 | 仍是第一下钻对象 |
| 牛羊类 | 27.06 | -355.51 | +382.57 | 生鲜/BOM/库存口径差候选 |
| 熟食类 | 1,078.84 | 1,409.65 | -330.81 | 加工关系与日清口径候选 |
| 冷藏加工及预制菜类 | 619.03 | 844.25 | -225.22 | 加工关系/分类口径候选 |
| 水果类 | 1,322.32 | 1,153.25 | +169.07 | 日清/库存口径候选 |

水产类逐日矩阵:

| 日期 | FM毛利 | QDM毛利 | 差异 |
|---|---:|---:|---:|
| 2026-06-18 | 85.46 | 29.13 | +56.33 |
| 2026-06-19 | -41.86 | 182.53 | -224.39 |
| 2026-06-20 | 26.41 | -512.42 | +538.83 |
| 2026-06-21 | -156.58 | -520.62 | +364.04 |
| 2026-06-22 | 70.91 | 314.50 | -243.59 |

结论: P0 状态错位已修复，销售额保持 0 差异，总毛利仍在 +6.28%。水产类仍是剩余矩阵最大差异，但已不再是 `sku_cost`/`stock` 同日状态错位问题；下一轮应下钻 QDM 成本口径、BOM 子品日间利润分配、以及水产相关 SKU 的盘点/损耗事件。
