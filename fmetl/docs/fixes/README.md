# 数据修复文档索引

> 审查报告来源: `fmetl/docs/全面审查报告_v0.10_2026-06-01.md`
>
> 所有修复基于 2026-05 全月数据审查，追踪 4 个核心问题。

---

## 修复状态总览

| FIX | 问题 | 状态 | ETL验证 | 修改文件 | Commit |
|-----|------|:---:|:---:|------|--------|
| FIX-001 | compose 纯加工关系计算 | ✅ 已实现 | ✅ | sku_cost.py, stock.py | `277f296` `c2cb613` |
| FIX-002 | EUC 兜底链完善 | ⏳ 待实现 | — | sku_cost.py | — |
| FIX-003 | 跨日 init_stock 不一致 | 📋 低优先级 | — | stock.py | — |
| FIX-004 | BOM 父品转移负毛利 | ⏳ 待实现 | — | stock.py | — |
| FIX-005 | 金额平衡公式修正 | 📋 低优先级 | — | stock_roll.py | — |
| FIX-006 | 蛋类 -34.9% 偏差 | 🟡 波及分析 | — | — (FIX-004 自动修复) | — |
| FIX-007 | 5/29 FM 巨损 | 🟡 波及分析 | — | — (FIX-004 自动修复) | — |
| FIX-008 | 标品库存核销 | 🟡 被取代 | — | — (FIX-009 修复) | — |
| FIX-009 | is_counted 系统快照 | ✅ 已实现 | ❌ | stock.py | `6ad7e91` |
| FIX-010 | 盘盈机制分析 | 📋 结构性差异 | N/A | 无需改代码 | — |
| FIX-011 | 品类差异化成本率 | ⏳ 待实现 | — | sku_cost.py | — |
| FIX-012 | L2=即食类→熟食类映射 | 📋 已记录 | — | sku_dim.py, executor.py 等多文件 | — |
| — | sku_dim.py L3 LIKE '%熟食' | ✅ 已实现 | ❌ | sku_dim.py | `3b47390` |
| — | dims_extractor 全量手动日清 | ✅ 已实现 | ❌ | dims_extractor.py | `ed8c7ac` |

**已实现 3 个（FIX-001 ETL 已验证），待实现 3 个 (FIX-002 + FIX-004 + FIX-011)，其余为波及分析、结构性差异或低优先级。**

### 状态图例

| 标记 | 含义 |
|:---:|------|
| ✅ 已实现 | 代码已修改并提交 |
| ⏳ 待实现 | 根因确认，修复方案明确，等待编码 |
| 🟡 波及分析 | 非独立 bug，上游修复后自动解决 |
| 📋 低优先级 | 当前数据不触发或影响极小 |

### ETL 验证状态

| 标记 | 含义 |
|:---:|------|
| ❌ 未跑 | 代码已提交但未执行 ETL 重跑验证 |
| 🔄 验证中 | ETL 正在运行 |
| ✅ 已验证 | ETL 重跑完成，数据对比通过 |

---

## 修复依赖关系图

```
审查报告 (4 个问题)
│
├── §2.1 EUC 兜底链不完整 → 30.3% 利润虚增
│   │
│   ├── FIX-001 加工金额纯加工关系计算 ✅ 已实现
│   │   └── compose 数量和金额完全不依赖源表，从业务行为推导
│   │       修改: sku_cost.py, stock.py
│   │       依赖: 无
│   │       被依赖: FIX-002
│   │
│   └── FIX-002 EUC 兜底链完善 ⏳ 待实现
│       └── 加载 current_price, 增加 cost_price + current_price×0.40 兜底
│           修改: sku_cost.py
│           依赖: FIX-001 (compose 金额必须先正确)
│           被依赖: FIX-003, FIX-004, FIX-011
│
│   └── FIX-011 品类差异化成本率 ⏳ 待实现 (依赖 FIX-002)
│       └── 将 uniform 0.40 替换为品类差异化成本率 (0.66-0.85)
│           修改: sku_cost.py
│           依赖: FIX-002
│           被依赖: 无
│
├── §2.2 跨日 init_stock 查找不一致 🔴
│   │
│   └── FIX-003 sku_cost 与 stock 的 init_stock 对齐 🟡 低优先级
│       └── stock.py 去掉 day_clear 匹配，与 sku_cost 一致
│           修改: stock.py
│           依赖: FIX-001, FIX-002
│           被依赖: 无
│           注意: 当前数据不触发此 bug (无 day_clear 切换的 SKU)
│
├── §2.3 BOM 父品库存转移负毛利 🔴 -1,272.80 元
│   │
│   └── FIX-004 BOM 父品 transfer 增加 bom_out ⏳ 待实现
│       └── stock_transfer 时父品同步增加 bom_out_amt/qty
│           修改: stock.py
│           依赖: FIX-001, FIX-002
│           被依赖: FIX-005
│
└── §2.6 库存方程金额平衡大面积失败 (23.9%)
    │
    └── FIX-005 金额平衡公式修正 + balance_amt 列 🟡 低优先级
        └── 审查报告用了 sale_amt(售价) 而非 sale_cost_amt(成本)
            正确公式下 98.0% 行平衡, 剩余 2% 是结构性差异
            修改: stock_roll.py (增加 balance_amt 列), 审查报告 (更新公式)
            依赖: FIX-002, FIX-004
            被依赖: 无
```

---

## 文件结构

```
fmetl/docs/fixes/
├── README.md                          (本文件 — 索引)
├── FIX-001-compose-pure-pr.md         (加工金额纯加工关系计算 ✅)
├── FIX-002-euc-fallback.md            (EUC计算链路与兜底修复方案 ⏳)
├── FIX-003-init-stock-consistency.md  (跨日init_stock查找不一致 🟡)
├── FIX-004-bom-transfer.md            (BOM父品库存转移负毛利 ⏳)
├── FIX-005-amount-balance.md          (库存方程金额平衡分析 🟡)
├── FIX-006-egg-category-deviation.md  (蛋类 -34.9% → FIX-004波及 🟡)
├── FIX-007-may29-loss.md              (5/29巨损 → FIX-004波及 🟡)
├── FIX-008-inventory-writeoff.md      (标品核销分析 → 根因FIX-009 🟡)
├── FIX-009-is-counted-snapshot.md     (is_counted系统快照 ✅)
├── FIX-010-inventory-gain.md          (盘盈机制分析 📋)
├── FIX-011-category-cost-ratio.md     (品类差异化成本率 ⏳)
└── FIX-012-cooked-instant-remap.md    (L2=即食类→熟食类映射 ⏳)
```
```

---

## 修复清单

### FIX-001: 加工金额纯加工关系计算 ✅

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-001-compose-pure-pr.md](FIX-001-compose-pure-pr.md) |
| **审查报告** | §2.1 EUC 兜底链 — 第1点 |
| **状态** | ✅ 已实现 (`277f296`, `c2cb613`) |
| **修改文件** | `fmetl/calculated/sku_cost.py`, `fmetl/calculated/stock.py` |
| **优先级** | 🔴 P0 |

**做了什么**:
- compose 数量不再从源表 `strategy_fm_compose_di` 读取，改为从业务行为推导:
  - 成品 `compose_in_qty = max(0, sale + loss - init - recv)`
  - 原料 `compose_out_qty = Σ(成品 compose_in × raw_qty / yield_qty)` (配方反推)
- compose 金额 100% 由加工关系推算（compose_in: 配方成本, compose_out: 价值守恒）
- 删除所有源表 `compose_in/out_amt_src` 引用和 `cost_price`/`avg_inbound` 兜底
- stock.py 从 `t_calc_sku_cost` 读取推导数量替代 `t_atomic_wide`

**影响**: 49 个加工关系成品生效，28 个有 compose_in 活动（2026-06-03 实测，含葡式蛋挞6个新配方）

**副作用**: 对 FIX-002 至关重要 — EUC 兜底链依赖 compose 金额正确

---

### FIX-002: EUC 兜底链完善 ⏳

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-002-euc-fallback.md](FIX-002-euc-fallback.md) |
| **审查报告** | §2.1 EUC 兜底链 — 第2点 |
| **状态** | ⏳ 待实现 |
| **修改文件** | `fmetl/calculated/sku_cost.py` |
| **优先级** | 🔴 P0 |
| **依赖** | FIX-001 |

**要做什么**:
- SELECT 加载 `current_price`（当前在 `t_atomic_wide` 中有但 sku_cost 未加载）
- Step 8.5: `cost_price` 兜底（`euc = cost_price`，覆盖 0 个 SKU）
- Step 8.6: `current_price × 0.40` 兜底（覆盖 97.7% 的 EUC=0 SKU）
- 预计修复 1,041 个 EUC=0 SKU 的 26,945 元虚增利润

**阻塞**: 需要 FIX-001 先完成（EUC 计算依赖 compose 金额正确）

---

### FIX-003: 跨日 init_stock 查找不一致 🟡

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-003-init-stock-consistency.md](FIX-003-init-stock-consistency.md) |
| **审查报告** | §2.2 |
| **状态** | 🟡 低优先级 (当前数据不触发) |
| **修改文件** | `fmetl/calculated/stock.py` |
| **优先级** | 🟡 P2 |
| **依赖** | FIX-001, FIX-002 |

**问题**: sku_cost.py 的 prev_df 查询不匹配 day_clear，stock.py 匹配。当 SKU 跨日切换 day_clear 时 init 会找错。

**当前影响**: 0 个 SKU 切换 day_clear，15 行受影响 (38 元)，可忽略。

**修复**: stock.py 去掉 day_clear 匹配，与 sku_cost.py 对齐。

---

### FIX-004: BOM 父品库存转移负毛利 🔴

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-004-bom-transfer.md](FIX-004-bom-transfer.md) |
| **审查报告** | §2.3 |
| **状态** | ⏳ 待实现 |
| **修改文件** | `fmetl/calculated/stock.py` |
| **优先级** | 🔴 P0 |
| **依赖** | FIX-001, FIX-002 |

**问题**: stock_transfer 清零父品 end_stock 但不增加 bom_out → init_stock 变成净亏损。

**修复**: 父品 transfer 时同步增加 `bom_out_amt` += transfer_amt, `bom_out_qty` += transfer_qty。

**影响**: 11 个父品，23 行，虚增亏损 -1,272.80 元 → 修复后 +148.06 元。

---

### FIX-005: 库存方程金额平衡公式修正 🟡

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-005-amount-balance.md](FIX-005-amount-balance.md) |
| **审查报告** | §2.6 |
| **状态** | 🟡 低优先级 |
| **修改文件** | `fmetl/fm_tables/stock_roll.py`, 审查报告 |
| **优先级** | 🟡 P2 |
| **依赖** | FIX-002, FIX-004 |

**问题**: 审查报告用 `sale_amt`(售价) 而非 `sale_cost_amt`(成本) 计算金额平衡，得出 23.9% 偏差率。

**正确公式**: `balance = init + receive + bom_in - bom_out + compose_in - compose_out + transfer_out - transfer_in - sale_cost - know_lost - unknow_lost - end`

**正确结果**: 98.0% 行平衡 (<0.01元)，剩余 2% 是结构性差异（采购价≠euc、跨日euc变化、BOM异源定价），不应"修复"。

**建议**: 更新审查报告公式，在 stock_roll.py 增加 `balance_amt` 监控列。

---

### FIX-006: 蛋类 -34.9% 偏差 🟡

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-006-egg-category-deviation.md](FIX-006-egg-category-deviation.md) |
| **审查报告** | §3.8 |
| **状态** | 🟡 由 FIX-004 修复后解决 |
| **依赖** | FIX-004 |

**结论**: 蛋类 8 个 SKU 中，2 个 BOM 父子 SKU 拖累 -1,030 元。去除后 FM 3,276 vs QDM 3,448 = -5.0%，在目标范围内。非独立 bug。

---

### FIX-007: 5/29 FM 巨损 🟡

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-007-may29-loss.md](FIX-007-may29-loss.md) |
| **审查报告** | §7.1 #7 |
| **状态** | 🟡 主要由 FIX-004 修复 |
| **依赖** | FIX-004 |

**结论**: 5/29 利润 532（正常 2,500-4,000），主要由 BOM 父品 transfer(-662) 和大批量收货(-12,548)叠加。FIX-004 修复后恢复到 ~1,194。

---

### FIX-008: 非易腐品库存核销 🟡

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-008-inventory-writeoff.md](FIX-008-inventory-writeoff.md) |
| **审查报告** | §3.5 |
| **状态** | 🟡 根因已被 FIX-009 修复 |

**结论**: 初步分析认为是盘点差异正常暴露，后续发现根因是 is_counted 条件过宽。

---

### FIX-009: is_counted 系统快照导致虚假核销 ✅

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-009-is-counted-snapshot.md](FIX-009-is-counted-snapshot.md) |
| **审查报告** | §3.5 |
| **状态** | ✅ 已实现 (`6ad7e91`) |
| **修改文件** | `fmetl/calculated/stock.py` |
| **优先级** | 🔴 P0 |

**修复**: is_counted 不再对系统快照（created_by='系统'）触发，仅人工盘点（created_by!='系统'）触发。盘盈检测（分支5）保留。消除每日 ~1,400 SKU、~1,000-5,000 元的虚假库存核销。

---

### FIX-010: 盘盈机制分析与结构性差异 📋

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-010-inventory-gain.md](FIX-010-inventory-gain.md) |
| **审查报告** | 独立分析（非审查报告问题） |
| **状态** | 📋 结构性差异（非 bug） |
| **修改文件** | 无需改代码 |
| **优先级** | 🟡 P2 |

**分析**: FM 与 QDM 对"损耗"有根本性语义差异。QDM 的 `lost_amt` 是运营记录（可正可负），FM 的 `unknow_lost` 是库存方程残差。FM 已有 5 个盘盈捕获机制（is_counted -$19K, day_clear=0 -$64K, eq<0 -$5.5K, know_lost>0, act>eq 检测 -$0.3K），正常分支中 99.99% 的 unknow=0 是设计意图。推荐方案：第一阶段接受结构性差异，第二阶段在报表层增加盘盈估计 KPI。

---

### FIX-011: 品类差异化成本率 ⏳

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-011-category-cost-ratio.md](FIX-011-category-cost-ratio.md) |
| **审查报告** | 独立分析（FIX-002 的 0.40 假设优化） |
| **状态** | ⏳ 待实现 |
| **修改文件** | `fmetl/calculated/sku_cost.py` |
| **优先级** | 🔴 P0 |
| **依赖** | FIX-002 |

**做什么**:
- 将 FIX-002 硬编码的 `current_price * 0.40` 兜底替换为品类差异化成本率
- 基于 A3XV 5月实测数据反推各品类加权 EUC/price 比率

**品类成本率**:

| 品类 | 成本率 |
|------|:---:|
| 预制菜 (含熟食) | 0.66 |
| 蔬菜类 | 0.72 |
| 冷藏及加工类 (含烘焙) | 0.75 |
| 水产类 | 0.75 |
| 猪肉类 | 0.75 |
| 肉禽蛋类 | 0.77 |
| 标品类 | 0.80 |
| 水果类 | 0.85 |
| 默认兜底 | 0.78 |

**效果**: 对 EUC=0 的 130 个可挽救 SKU，利润虚增从 15,836 降至 7,403 (-53%)。

---

### FIX-012: L2=即食类 → 熟食类映射规则补充 ⏳

| 属性 | 值 |
|------|-----|
| **文档** | [FIX-012-cooked-instant-remap.md](FIX-012-cooked-instant-remap.md) |
| **来源** | 日清品清单产品审查 (2026-06-03) |
| **状态** | ⏳ 待实现 |
| **修改文件** | `sku_dim.py`, `executor.py`, `category_mapping.py`, `cloud_api.py`（日清+加工关系） |
| **优先级** | 🟡 P1 |

**问题**: 当前熟食类映射规则只覆盖 L2 IN ('即烹类','即热类')，不包含 L2='即食类'。瑞士鸡翅、香煎三文鱼骨等 6 个 SKU（L1=预制菜, L2=即食类, L3=其他即食类）不命中任何规则，归入冷藏加工及预制菜类。

**修复**: 新增第 5 条规则 `L2 = '即食类' → 熟食类`，5 个文件需同步修改。

---

## 实现顺序

### ✅ 已实现

```
FIX-001 ✅  2026-06-01  compose纯加工关系计算 (sku_cost.py + stock.py)
FIX-009 ✅  2026-06-01  is_counted系统快照移除 (stock.py)
```

### ⏳ 待实现（按顺序）

```
1. FIX-002 ⏳  EUC兜底链完善 (sku_cost.py)
   └── 阻塞: 无 (FIX-001 已完成)
   └── 影响: 修复 97.7% 的 EUC=0 SKU, 消除 ~12,000 虚增利润

2. FIX-011 ⏳  品类差异化成本率 (sku_cost.py)
   └── 阻塞: FIX-002 (需要 V10_RETAIL_ESTIMATED 兜底层先存在)
   └── 影响: 将 FIX-002 的 uniform 0.40 替换为品类差异化 0.66-0.85
            利润虚增从 15,836 → 7,403 (-53%)

3. FIX-004 ⏳  BOM父品转移负毛利 (stock.py)
   └── 阻塞: FIX-002 (需要 EUC 先正确)
   └── 影响: 修复 11 个父品 -1,273 → +148,
            连带修复蛋类(FIX-006) 和 5/29(FIX-007)
```

### 🟡 波及分析（待上游修复后验证）

```
FIX-006 🟡  蛋类 -34.9%  → 验证 FIX-004 修复后蛋类进入 ±5%
FIX-007 🟡  5/29 巨损    → 验证 FIX-004 修复后 5/29 利润恢复正常
FIX-008 🟡  标品核销     → 已被 FIX-009 修复, 验证无遗留
```

### 📋 低优先级（上线后处理）

```
FIX-003 📋  init_stock 对齐   (0 SKU 触发, 15 行 38 元)
FIX-005 📋  金额平衡公式     (审查报告公式 bug, 不影响 ETL 正确性)
```

---

## 相关文档

| 文档 | 路径 |
|------|------|
| 项目总览 | [CLAUDE.md](../../../CLAUDE.md) |
| ETL 完整处理逻辑 | [ETL_v0.10_完整处理逻辑.md](../ETL_v0.10_完整处理逻辑.md) |
| 差异问题与待办 | [差异问题与待办事项_v0.10.md](../差异问题与待办事项_v0.10.md) |
| 全面审查报告 | [全面审查报告_v0.10_2026-06-01.md](../全面审查报告_v0.10_2026-06-01.md) |
| 源表字段手册 | [strategy_fm_字段手册_完整版.md](../strategy_fm_字段手册_完整版.md) |
