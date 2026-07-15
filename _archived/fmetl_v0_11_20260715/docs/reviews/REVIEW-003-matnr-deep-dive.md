# matnr 深层审查：称重→包装对 + 交叉验证分析

> **审查日期**: 2026-06-23 | **数据范围**: 2026-06-18 ~ 2026-06-22, 广州滨江宏岸店
> **动机**: 修正 REVIEW-002 中关于"称重→包装双方都有独立进货数据"的错误结论
> **关联**: [REVIEW-002](REVIEW-002-matnr-euc-integration.md), [V10_MATNR_CONVERT](~fmetl/calculated/sku_cost.py)

## 1. 前次结论修正

**REVIEW-002 中的错误陈述**:

> 大量称重→包装对（蔬菜/水果）的双方都有独立进货数据，走 V10_WEIGHTED_AVG 主路径

**实际数据**:

```
称重→包装 matnr 对分布 (dim_goods 全量):
  无基准(全无euc):        3,111 对  ← 双方都没有有效 EUC
  可fallback(目标不在管线):  197 对  ← 称重有euc，包装不在管线中
  双方都有euc(可交叉验证):     0 对  ← 称重→包装没有一对是双方都有独立euc的!
```

**根因**: 蔬菜/水果的称重 SKU 和包装 SKU 大部分在当前5天窗口内**没有任何进货数据**。
称重品即使有销售，receive=0 → cost_qty=0 → euc=0 → 自身都无法获得 EUC，更谈不上给包装品提供基准。

**结论**: 称重→包装对的 EUC 问题比预想的更严重——不是"双方都有数据"，而是 **"双方都没有数据"**。

## 2. matnr 对全景 (4,716 对)

按 EUC 覆盖状态分三组：

### 组 A: 可 fallback — 基准有 euc，目标 euc=0 或不在管线

| 子类型 | matnr 对 | 基准数 | 目标数 | 当前状态 |
|--------|:---:|:---:|:---:|------|
| 多规格(管线内目标) | 0 | — | — | V10_MATNR_CONVERT 已全部覆盖 (10 SKU) |
| 多规格(目标不在管线) | 27 | 29 | 30 | 目标无活动 → 不需要 EUC |
| 称重→包装(管线内目标) | 0 | — | — | 不存在此情况 |
| 称重→包装(目标不在管线) | **197** | 198 | 439 | 目标无活动 → 不需要 EUC |

**关键结论**: 197 对称重→包装中，439 个包装 SKU 不在管线的原因是**它们在这 5 天没有任何销售/进货/库存活动**。没有 sales → 没有出现在 t_fm_sku_dim → 不需要 EUC。当它们未来出现销售时，V10_MATNR_CONVERT 会自动捕获。

### 组 B: 双方都有 euc — 交叉验证候选 (11 对)

这是 matnr 最有价值的应用场景。11 对中有实际意义的 5 对（其余为 BOM 自引用）：

| matnr | SKU A | EUC A | SKU B | EUC B | wt/zgl 期望比 | 实际比 | 偏差 | BOM |
|-------|-------|-------|-------|-------|:---:|:---:|:---:|:---:|
| 110561 | 蒙牛每日鲜语250ml瓶 | 3.40 | 蒙牛每日鲜语250ml×4排 | 7.95 | 4.0 | **2.34** | **-41.5%** | ✅ |
| 102458 | A级-进口山竹(称重) | 31.80 | A级-进口山竹5-7kg件 | 167.54 | 6.0 | **5.27** | **-12.2%** | ❌ |
| 124752 | 蒙牛鲜牛奶12包箱 | 16.30 | 蒙牛牧场鲜牛奶6包袋 | 32.60 | 0.5 | **0.5*** | **见注释** | ✅ |
| 103353 | 咸香鸡半只450g | 20.95 | 咸香鸡900g/只 | 41.90 | 2.0 | 2.0 | 0% | ✅ |
| 102728 | 花卷1个 | 0.825 | 老面葱油花卷300g | 3.30 | 4.0 | 4.0 | 0% | ❌ |

*\*124752: euc比=0.5 看似正确，但这是因为 EUC 来自 BOM 分配(非独立计算)。如果按 cost_price 交叉验证，6包袋的实际成本被高估 4 倍。且 zglfz/zglfm 写反（已知 bug）。*

**三个异常:**

1. **蒙牛每日鲜语3.6** (matnr 110561): 4排装 EUC 仅 7.95，按单瓶 3.40 × 4 = 13.60 应为 13.60。偏差 -41.5%。这是 BOM 相关——4排装可能作为 BOM 子品获得了不正确的成本分配。

2. **A级-进口山竹** (matnr 102458): 5-7kg 件装 EUC=167.54，期望(31.80×6.0)=190.8，偏差 -12.2%。可能是 BOM alloc 成本分配偏低，或称重品 EUC 本身偏高。

3. **蒙牛鲜牛奶** (matnr 124752): 已知 zglfz/zglfm 写反导致 unit_weight 计算错误，进而影响 BOM alloc 的 qty_sub 计算。

### 组 C: 无基准 (4,681 对)

双方都没有有效 EUC。这批 SKU 在当前日期范围内没有任何进货数据（或进货量不足以支撑加权平均 EUC）。不是 matnr 能解决的问题——需要从 receive 数据或采购定价入手。

## 3. V10_MATNR_CONVERT 的定位

当前 V10_MATNR_CONVERT 作为 EUC 兜底链的第 5 层，**位置和设计都是正确的**。

但它只覆盖了 matnr 应用场景的一半：

| 场景 | 已覆盖 | 方法 |
|------|:---:|------|
| 目标 euc=0，同 matnr 兄弟有 euc | ✅ | V10_MATNR_CONVERT 兜底 |
| 双方都有 euc，但比率不一致 | ❌ | 需要**交叉验证层** |

**交叉验证的设计位置**应该在 sku_cost.py 计算完成后（Step 5 末尾），作为只读的 WARNING/HINT 输出，不修改 EUC（因为不确定哪一方是对的）：

```
if euc_ratio deviates from weight_ratio by > 20%:
    WARNING: matnr 124752: 20740962 euc=16.30, 21281075 euc=32.60,
             euc_ratio=0.50, weight_ratio=2.00, deviation=75%
             BOM overlap detected, possible zglfz/zglfm reversal
```

## 4. 称重→包装的根本问题

**称重→包装对的 EUC 问题不在 matnr，而在进货数据缺失**。

以"红萝卜 → 红萝卜约500g"为例：
- 称重 SKU: receive_qty=0, init_stock=0, euc=0
- 包装 SKU: receive_qty=0, init_stock=0, euc=0
- 两者都有销售（来自 atomic_sales）
- 但都没有进货数据 → cost_qty=0 → euc=0

**这说明 receive 数据只覆盖了部分 SKU**。可能原因：
1. 验收单只记录称重品的进货（包装品由称重品拆包得到，不单独验收）
2. 验收单按 matnr 层级记录，不按 SKU 粒度区分包装规格
3. 验收数据的时间窗口问题

如果验收单确实按 matnr 粒度记录（不区分 称重/包装），那么：
- 验收金额应按**重量比**分配给同 matnr 的多个 SKU
- 这本质上就是 matnr 转换应该做的事情，但不应该只在 fallback 层做
- 应该在 **receive 拆分阶段**就介入

**这个假设如果成立，当前的 ETL 架构有结构性缺陷**:
- 验收数据只体现为称重 SKU 的 receive_amt
- 包装 SKU 虽然同样是"进货"，但 receive=0
- 包装 SKU 的 EUC 永远靠兜底链推算，而不是从 receive 直接计算

### 验证路径

检查验收单数据源 (`strategy_fm_purchase_di`) 是否按 matnr 粒度记录：

```sql
-- 在同 matnr 多 SKU 组中，验收数据是否只落在称重品上
SELECT 
    dg.matnr,
    dg.article_id, dg.article_name, dg.unit_weight, dg.sale_unit,
    SUM(p.self_receive_qty) as total_recv_qty,
    SUM(p.self_receive_amt) as total_recv_amt
FROM dim_goods dg
JOIN t_atomic_wide p ON dg.article_id = p.article_id
WHERE dg.matnr IN (同matnr多SKU列表)
  AND p.business_date BETWEEN '2026-06-18' AND '2026-06-22'
GROUP BY dg.matnr, dg.article_id, dg.article_name, dg.unit_weight, dg.sale_unit
ORDER BY dg.matnr, dg.unit_weight
```

如果确认验收数据按 matnr 粒度记录，则应考虑在 AtomicMerger (merge.py) 中增加**按 matnr + unit_weight 比拆分 receive 数据**的逻辑。

## 5. P0 验证结果：receive 数据明确落在称重品上

**验证结论**: 在 117 个有 receive 数据的称重→包装 matnr 对中：

| receive 落点 | matnr 对 | 验收金额 | 
|:---|---:|---:|
| **只有称重品有 receive** | **106** | **34,863 元** |
| 只有包装品有 receive | 11 | 362 元 |
| 双方都有 receive | **0** | — |
| 双方都无 receive | 3,191 | — |

**验收数据按 matnr 粒度记录，总是落在称重品 (unit_weight=0) 上。包装品 receive=0。**

这意味着：包装品虽然和称重品是同一个物理货物，但在当前 ETL 中被当作"进了 0 件货"处理。
包装品的 EUC 要么靠 init_stock 继承（跨日），要么靠兜底链（V10_MATNR_CONVERT）。

## 6. 正确的修复方案：receive 拆分

### 6.1 为什么不在 sku_cost 兜底层做

V10_MATNR_CONVERT 作为兜底是正确的——覆盖"偶尔出现的 euc=0"场景。但让 106 个 matnr 对的包装品全部依赖兜底链是**架构问题**：不应该把主链路数据缺失当成兜底场景。

receive 拆分应该在 **merge.py / AtomicMerger** 中完成，作为数据 normalize 步骤：
- 让包装品的 `self_receive_qty/amt` 在 merge 阶段就有正确的值
- 自然流经 V10_WEIGHTED_AVG → 正确的 EUC
- matnr 兜底仅在"拆分后仍有遗漏"时触发

### 6.2 拆分公式

```
对于同 matnr 的所有 SKU:

weighted_qty_i = sale_qty_i × unit_weight_equivalent_i
  其中 unit_weight_equivalent = unit_weight (wt>0)
                            = zglfz/zglfm (wt=0)

split_ratio_i = weighted_qty_i / Σ(weighted_qty_j for j in same matnr)

各 SKU 的 receive = 原始 receive(落在称重品上) × split_ratio_i
```

### 6.3 实现位置

- **merge.py** (AtomicMerger): Step 3 产出 t_atomic_wide 时，识别同 matnr 的称重→包装组，按 weight-equivalent sales ratio 拆分 receive
- 拆分后的 receive 数据写入 t_atomic_wide 新增列 `recv_from_matnr_split` (标记来源)
- 或者在 self_receive_qty/amt 的原始列上直接修改 + 增加 `recv_split_source` 标记列

## 7. 建议优先级（修订版）

| 优先级 | 行动 | 效果 |
|:---:|------|------|
| **P0** | ✅ 验证完成：验收数据按 matnr 记录，落在称重品 | 确认结构性缺陷 |
| **P1** | ~~在 merge.py 中按 matnr weight 比拆分 receive~~ → V10_MATNR_CONVERT 兜底替代 | receive 落在称重品是正确的, 包装品 EUC 通过 matnr 兜底推算 |
| **P2** | ~~matnr 交叉验证层（Step 5 末尾 WARNING）~~ ✅ 已完成 | 发现 BOM + matnr 重叠的 EUC 异常 (FIX-018) |
| **P3** | V10_MATNR_CONVERT 继续兜底（覆盖拆分后仍为零的少量 SKU） | 安全网 |

## 8. 链路总结（修订版）

```
验收单 (purchase_di)
  │
  │  已验证: receive 数据按 matnr 粒度记录，落在称重品上 (106/117对)
  │
  ├─ 【当前】称重品 receive=100kg, 包装品 receive=0
  │   → 包装品 euc 依赖兜底链 → 结构性缺陷
  │
  └─ 【修复后】receive 按 weight-equivalent sales 比拆分
      → 称重品 receive=88.9kg, 包装品 receive=11.1kg (按销量比例)
      → 双方都走 V10_WEIGHTED_AVG → 正确的 EUC
      
V10_MATNR_CONVERT 兜底: 仅覆盖拆分后有 euc=0 的少量边缘 SKU
matnr 交叉验证: 检测双方 euc>0 但比率不一的异常 (如蒙牛3.6偏差41.5%)
```

---

*关联: [REVIEW-002](REVIEW-002-matnr-euc-integration.md) — matnr+EUC 整合方案, [REVIEW-001](REVIEW-001-2026-06-18-to-22.md) — QDM 对比*
