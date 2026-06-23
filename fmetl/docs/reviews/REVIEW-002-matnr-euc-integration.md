# matnr 物料号转换与 EUC 成本体系整合方案

> **审查日期**: 2026-06-23 | **数据范围**: 2026-06-18 ~ 2026-06-22, 广州滨江宏岸店
> **参考**: [matnr-convert skill](~/.cursor/skills/matnr-convert/SKILL.md), [REVIEW-001](REVIEW-001-2026-06-18-to-22.md)

## 1. matnr 体系全景

### 1.1 基本概念

```
SAP 物料号 (matnr) ─── 1:N ─── 商品编码 (article_id / SKU)
     │                              │
     └── 同一物料，不同包装规格        └── 称重 vs 固定包装 / 整件 vs 零售 / 大包 vs 小包
```

**数据来源**: `dim_goods.matnr` 字段（来自 `strategy_fm_dim_goods`，DuckDB 中已存在）。

**可用字段**:
| 字段 | 来源 | 说明 |
|------|------|------|
| `matnr` | dim_goods | 18位 SAP 物料号 |
| `unit_weight` | dim_goods | 单位重量(kg)，称重商品为 0 |
| `sale_unit` | dim_goods | 销售单位（千克/份/盒/袋/个...） |

**注意**: `zglfz/zglfm`（单位换算分子/分母）已在 2026-06-23 通过 DimsExtractor 改造拉取到 DuckDB dim_goods。`matnr_unit`、`order_unit`、`atob_value` 也一并拉取。

### 1.2 活跃 matnr 对规模

当前 5 天数据中，**429 对**同 matnr 多 SKU 存在销售或库存活动。按业务特征分为：

| 类型 | 数量 | 特征 | 示例 |
|------|:---:|------|------|
| **称重→包装** | ~300+ | wt1=0 (千克散装), wt2>0 (固定包装) | 红萝卜(0kg) → 红萝卜约500g(0.5kg) |
| **整件→零售** | ~60+ | 大包装件 → 小包装零售，wt 差异 10-100× | A级-进口山竹5-7kg → 零售散装 |
| **多联包→单杯** | ~30+ | 酸奶/牛奶 3联包/6联包 ↔ 单杯 | 卡士鲜酪乳100g×3 ↔ 单杯100g |
| **BOM重叠** | 3 | matnr 对同时存在 BOM 关系 | 咸香鸡、蒙牛鲜牛奶、每日鲜语 |
| **原料→加工品** | ~20 | 鲜猪肉称重 → 盒装分割肉 | 优鲜排骨(0kg) → 优鲜排骨板(4.75kg) |

### 1.3 BOM vs matnr 的本质区别

| | BOM (BomAllocCalculator) | matnr (物料号转换) |
|---|---|---|
| **关系语义** | 1个父品 **拆分为** 多个不同子品 | 1个物料 **对应** 多个包装规格 |
| **成本逻辑** | 父品成本 **分摊** 给子品 | 同物料不同规格的成本 **按重量等比换算** |
| **典型场景** | 整猪 → 排骨+五花肉+瘦肉+... | 牛奶12包箱 ↔ 牛奶6包袋 |
| **数据来源** | `atomic_bom_relation` / `atomic_receive_sale` | `dim_goods.matnr` |
| **当前 ETL** | Step 4 BomAllocCalculator | Step 11b MatnrResultBuilder（仅报表聚合） |
| **EUC 应用** | ✅ 已用于 EUC | ❌ 未用于 EUC |

**关键判断**: 两者是互补关系，不是替代关系。matnr 转换不应干扰 BOM 分摊，而是作为 BOM 覆盖不到的场景的补充。

## 2. 当前 matnr 在 ETL 中的使用

### 2.1 MatnrResultBuilder（仅报表层）

[matnr_result.py](fmetl/fm_tables/matnr_result.py) 在 FM 底表层将 SKU 级数据按 `matnr` 聚合，产出 `t_fm_levels_result_matnr` 表。这是**纯报表聚合**——将同 matnr 的多个 SKU 合并为一行，SUM 数量/金额。

```sql
-- 核心逻辑
SELECT g.matnr AS level_id, '物料号' AS level_description, ...
FROM t_fm_levels_sum s
JOIN dim_goods g ON s.level_id = g.article_id
WHERE s.level_description = 'SKU'
GROUP BY g.matnr, ...
```

**对 EUC 无影响**——聚合发生在计算链路的最末端（Step 11b），不回溯到计算层。

### 2.2 计算层完全没有 matnr 参与

Step 4-7（BomAlloc → SkuCost → Stock → Profit）完全不使用 matnr。这意味着：

- 同 matnr 的 6-pack 和 12-pack 在 EUC 计算中是**完全独立的两个 SKU**
- 如果 6-pack 没有进货数据（receive=0），EUC = 0 → 进入兜底链
- 即使 12-pack 有明确的成本（euc=16.30），也不会传导给 6-pack

## 3. 典型案例：matnr 导致或可修复的 EUC 异常

### 案例1：蒙牛鲜牛奶 matnr 124752 — BOM+matnr 重叠

| SKU | 包装 | unit_weight | FM euc | 期望 euc | 偏差 |
|-----|------|-------------|--------|----------|:---:|
| 20740962 | 12包/箱 | 2.16 kg | **16.30** | 16.30 | 基准 |
| 21281075 | 6包/袋 | 1.08 kg | **24.7~32.6** | **8.15** | 3-4× |

**根因链**:
1. BOM alloc 将 20740962 (12包) → 21281075 (6包) 建立父子关系
2. 父品 receive_amt = 16.30, bom_out_amt = 16.30 → 子品 bom_in_amt = 16.30
3. 但 bom_in_qty = 1.0 (父品单位=箱), bom_in_qty_sub = 1.0 → 应该是 0.5 箱!
4. `zglfz/zglfm` 写反导致 unit_weight 计算错误（matnr skill 已识别此 bug）
5. 结果：6-pack 的 euc = 16.30/0.5 = 32.60（应为 16.30 × 0.5 = 8.15）

**修复路径**:
- 短期: 在 sku_cost.py 中增加 matnr unit_weight 校验，检测并修正异常 EUC
- 长期: 修复 BOM 中的 parent_rate/sub_rate 或引入 zglfz/zglfm 字段

### 案例2：咸香鸡 matnr 103353 — BOM+matnr 重叠

| SKU | 包装 | unit_weight | FM euc | FM profit |
|-----|------|-------------|--------|-----------|
| 20004989 | 900g/只 | 0.90 kg | 0~41.9 | **+722.8** (应为0) |
| 20004996 | 半只450g | 0.45 kg | 20.95 | **-346.2** |

**matnr 视角**: unit_weight 比 = 0.90/0.45 = 2.0。如果引入 matnr 校验：
- 父品 euc = 41.9 时，子品 euc 应为 20.95 ✅（当前已正确）
- 但父品本身不应该有独立 EUC（它是 BOM 父品，只做分配不做销售）
- 父品 profit = 722.8 说明**BOM 父品利润清零逻辑有缺陷**（详见 REVIEW-001）

**结论**: matnr 可以验证 BOM 分配的正确性，但根因在 BOM 父品利润处理。

### 案例3：称重→包装（蔬菜/水果）— 无 BOM，EUC 独立计算

| matnr | SKU1 (称重) | SKU2 (包装) | wt比 | SKU1 euc | SKU2 euc | 期望比 |
|-------|------------|------------|------|----------|----------|:---:|
| 101823 | 20000110 西葫芦 | 20045807 西葫芦650g | 0 : 0.65 | 6.58 | ? | — |
| 101772 | 20001346 土豆 | 20032623 土豆约500g | 0 : 0.50 | — | — | — |
| 101591 | 20000035 红萝卜 | 20032227 红萝卜约500g | 0 : 0.50 | — | — | — |

**称重商品 unit_weight=0**，无法直接用 wt 比。需要 zglfz/zglfm：
```
ratio = (目标SKU.zglfz / 目标SKU.zglfm) / (基准SKU.zglfz / 基准SKU.zglfm)
```

**现状**: 这 300+ 对在当前 ETL 中完全独立计算 EUC。差异来自：
- 称重品：通过加权平均得到 per-kg euc
- 包装品：通过自身进货计算 per-份 euc
- 两者之间的单位换算关系未被利用

## 4. 整合方案

### 4.1 总体设计

在 `SkuCostCalculator` 中新增 **V10_MATNR_CONVERT** 兜底层，位于现有兜底链的最末端：

```
EUC 计算优先级:
  1. V10_WEIGHTED_AVG        ← 主路径 (cost_amt / cost_qty > 0)
  2. V10_INHERITED_EUC        ← 向前继承昨日 euc
  3. V10_AVG_INBOUND_FALLBACK ← avg_inbound_price
  4. V10_PROCESSING_RELATION  ← 加工配方推算
  5. V10_MATNR_CONVERT   ← 🆕 同 matnr 兄弟 SKU euc × unit_ratio
```

### 4.2 触发条件

仅当以下条件全部满足时触发 matnr 转换：
1. 目标 SKU 的 `effective_unit_cost == 0`（前面所有兜底都失败了）
2. 目标 SKU 的 `matnr` 非空
3. 同 matnr 下存在另一个 SKU（同店同日）其 `effective_unit_cost > 0`
4. 两个 SKU 之间**不存在 BOM 关系**（避免与 BOM 分摊冲突）

### 4.3 转换公式

**方法1: unit_weight 比（优先）**
```python
if 基准SKU.unit_weight > 0 and 目标SKU.unit_weight > 0:
    ratio = 目标SKU.unit_weight / 基准SKU.unit_weight
    目标EUC = 基准SKU.euc × ratio
```

**方法2: sale_unit 数量推断（unit_weight=0 时）**
```python
# 从 article_name 中提取数量信息
# 例如: "红萝卜约500g" → 0.5kg, "蒙牛鲜牛奶180ml*6" → 6包
# 配合 dim_goods 的 sale_unit 和 order_unit 做交叉验证
```

**方法3: zglfz/zglfm 比（需先拉取字段）**
```python
ratio = (目标.zglfz / 目标.zglfm) / (基准.zglfz / 基准.zglfm)
```

### 4.4 实现路径

#### Phase 1: 拉取 zglfz/zglfm（DimsExtractor 改造）✅ 已完成 2026-06-23

`strategy_fm_dim_goods` 源表本身包含 `zglfz, zglfm, matnr_unit, order_unit, atob_value`，
只是 DimsExtractor 的 SELECT 未包含。无需 JOIN Hive，直接在 SELECT 中增加 5 个字段即可：

```sql
-- 改造后 (dims_extractor.py _extract_goods)
SELECT
    article_id, article_name,
    category_level1_id, category_level1_description,
    category_level2_id, category_level2_description,
    category_level3_id, category_level3_description,
    spu_id, spu_name,
    blackwhite_pig_name, blackwhite_pig_id,
    unit_weight, sale_unit,
    matnr,
    zglfz, zglfm, matnr_unit, order_unit, atob_value
FROM strategy_fm_dim_goods
WHERE inc_day = '{ds}'
  AND category_level1_id NOT IN ('70','71','72','73','74','75','76','77')
```

**风险评估**: 无风险。字段均在 `strategy_fm_dim_goods` 中，无需额外权限或 JOIN。

#### Phase 2: matnr 转换模块（SkuCostCalculator 新增方法）✅ 已完成 2026-06-23

实现在 `sku_cost.py` `_apply_matnr_conversion()` 方法（~170 行）。

**核心逻辑**:
1. 从 `dim_goods` 加载 matnr / unit_weight / zglfz / zglfm
2. 从 `t_calc_bom_alloc` 加载 BOM 配对（排除已覆盖的 SKU 对）
3. 按 (date, store, matnr) 索引 euc>0 的候选基准 SKU
4. 对每个 euc=0 的目标 SKU：
   - 同店同日找基准 → 跨店回退
   - 排除 BOM 对 + 品质差异检查（avg_inbound_price 差异 >30%）
   - 选最优基准：cost_qty>0 优先 → cost_qty 最大
   - ratio = unit_weight比（优先） 或 zglfz/zglfm比（回退）
   - 目标EUC = 基准EUC × ratio

**验证结果**: 50 行 (10 SKU × 5 天) 转换，3 对 BOM 排除，0 品质排除。数学验证全部正确。
详见 `_apply_matnr_conversion` 方法内注释。

#### Phase 3: 与现有 BOM 的协同

关键规则：**matnr 转换不覆盖 BOM 已处理的 SKU 对**。

判断方式：
```sql
SELECT parent_article_id, sub_article_id FROM t_calc_bom_alloc
WHERE (parent_article_id, sub_article_id) IN (matnr_pair)
   OR (sub_article_id, parent_article_id) IN (matnr_pair)
```

如果同 matnr 对已经存在 BOM 关系，matnr 转换**仅做交叉验证**（warn 不一致），不修改 EUC。

### 4.5 预期覆盖范围

| 场景 | matnr 对数 | 当前 EUC=0 | 可修复 | 备注 |
|------|:---:|:---:|:---:|------|
| 称重→包装（蔬菜） | ~120 | ~20-30 | ~20-30 | 需 zgl 比或名称推断 |
| 称重→包装（水果） | ~60 | ~10-15 | ~10-15 | 同上 |
| 多联包→单杯（乳品） | ~30 | ~5-10 | ~5-10 | unit_weight 可用 |
| 整件→零售（水果件装） | ~60 | ~10 | ~10 | wt 差异大但比例精确 |
| BOM 重叠 | 3 | 0 | 0 | 仅做验证 |
| 原料→加工品（猪肉） | ~20 | ~5 | 0 | BOM 覆盖 |

**预计总可修复 SKU**: ~50-70 个（当前 euc=0 或异常的 SKU）。

## 5. 不适用 matnr 转换的场景

### 5.1 品质差异

同 matnr 但品质不同的 SKU（如普通鲜牛奶 vs 现代牧场鲜牛奶）虽然共享 matnr，但价格差异来自品质而非包装。matnr 转换会错误地将高端品成本赋给普通品。

**判断方法**: 如果两个 SKU 的 `avg_inbound_price` 差异 >30%，标记为"品质差异"，不触发转换。

### 5.2 BOM 已覆盖

BOM 分摊已处理父子品成本传递。matnr 转换在此场景下仅做**验证**，不做修改。

### 5.3 无同店同日基准

如果同 matnr 的兄弟 SKU 在同店同日也没有有效 EUC，无法转换。

## 6. 建议优先级

| 优先级 | 行动 | 效果 |
|:---:|------|------|
| **P1** | ~~DimsExtractor 增加 zglfz/zglfm 字段拉取~~ ✅ 已完成 | 解锁 matnr 转换的称重商品覆盖 |
| **P1** | ~~SkuCostCalculator 增加 V10_MATNR_CONVERT 兜底层~~ ✅ 已完成 | 修复 10 个 EUC=0 的 SKU（50 行） |
| **P2** | matnr 交叉验证: 对已有 EUC 的同 matnr SKU 做 ratio 一致性检查 | 发现异常 EUC（如蒙牛 6-pack 高估4×） |
| **P2** | BOM + matnr 重叠场景的联合校验 | 检测 BOM 分配错误 |
| **P3** | MatnrResultBuilder 中使用 matnr-euc 修正后的数据 | 改善报表层的数据质量 |

## 7. 与现有架构的兼容性

matnr 转换作为 `SkuCostCalculator` 的新增兜底方法，**不改变现有计算链路**：

- Step 4 (BomAllocCalculator): 不变
- Step 5 (SkuCostCalculator): 新增 `_apply_matnr_conversion()` 方法，在 `_apply_processing_relation_fallback()` 之后调用
- Step 6-7 (StockCalculator, ProfitCalculator): 不变，自动使用修正后的 euc
- Step 8-13 (FM 底表): 不变

新增字段: `cost_source = 'V10_MATNR_CONVERT'`（对齐现有 cost_source 枚举模式）。

---

*关联: [REVIEW-001](REVIEW-001-2026-06-18-to-22.md) — 同期 QDM 对比审查, [matnr-convert skill](~/.cursor/skills/matnr-convert/SKILL.md)*
