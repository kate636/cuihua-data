# 数据修复文档索引

> 审查报告来源: `fmetl/docs/全面审查报告_v10_2026-06-01.md`
>
> 所有修复基于 2026-05 全月数据审查，追踪 4 个核心问题。

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
│           被依赖: FIX-003, FIX-004
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
├── README.md                       (本文件 — 索引)
├── FIX-001-compose-pure-pr.md      (加工金额纯加工关系计算)
├── FIX-002-euc-fallback.md         (EUC计算链路与兜底修复方案)
├── FIX-003-init-stock-consistency.md (跨日init_stock查找不一致)
├── FIX-004-bom-transfer.md         (BOM父品库存转移负毛利)
└── FIX-005-amount-balance.md       (库存方程金额平衡分析)
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

**影响**: 45 个加工关系成品全部生效，25 个源表漏报的成品现在能正确计算加工量

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

## 实现顺序

```
1. FIX-001 (✅ 已完成)     ← 无依赖，影响面最小
      │
2. FIX-002 (⏳ 下一步)     ← 依赖 FIX-001，影响面最大
      │
      ├── 3a. FIX-004 (⏳)  ← 依赖 FIX-001/002
      │
      └── 3b. FIX-003 (🟡)  ← 低优先级，依赖 FIX-001/002
      
4. FIX-005 (🟡)             ← 低优先级，依赖 FIX-002/004
```

---

## 相关文档

| 文档 | 路径 |
|------|------|
| 项目总览 | [CLAUDE.md](../../../CLAUDE.md) |
| ETL 完整处理逻辑 | [ETL_v10_完整处理逻辑.md](../ETL_v10_完整处理逻辑.md) |
| 差异问题与待办 | [差异问题与待办事项_v10.md](../差异问题与待办事项_v10.md) |
| 全面审查报告 | [全面审查报告_v10_2026-06-01.md](../全面审查报告_v10_2026-06-01.md) |
| 源表字段手册 | [strategy_fm_字段手册_完整版.md](../strategy_fm_字段手册_完整版.md) |
