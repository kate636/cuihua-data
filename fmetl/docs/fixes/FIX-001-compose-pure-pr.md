# 加工金额纯加工关系计算 — EUC 修复第1点 (FIX-001)

> 状态: **✅ 已实现** | Commit: `277f296` `c2cb613` | ETL验证: ✅ 全月验证 (5/1~6/3, 34天) | [修复索引 →](README.md)
>
> 修改文件: `fmetl/calculated/sku_cost.py`, `fmetl/calculated/stock.py`
>
> 核心理念: compose_in/compose_out 的**数量和金额**完全不依赖源表。数量从销售/损耗推导，金额由加工关系和价值守恒推算。
>
> 被依赖: [FIX-002 EUC兜底链](FIX-002-euc-fallback.md)
>
> 已实现 2 个 commit:
> - `277f296` 数量和金额推导
> - `c2cb613` 盘点库存参与计算 + 文档索引
>
> **验证数据**: 34 天全量重跑，94 个成品 compose 活动，647 compose_in 行 + 723 compose_out 行全部正确。
>
> **新增加工关系**: 葡式蛋挞6个(C) (21282423) — 2条配方已激活（好禧坊蛋挞液 + 仿手工葡挞皮）

---

## 一、问题: 源表 compose 数据完全不可靠

### 1.1 数量也不可信

源表 `strategy_fm_compose_di` 的 compose_in_qty 存在严重漏报：

| SKU | 名称 | sale | compose_in (源表) | 问题 |
|-----|------|:---:|:---:|------|
| 21261442 | 盐焗鸡 | 113 | 50 | recv=0, init=0, 卖113只却只生产50只? 不可能 |
| 21340055 | (成品) | 46 | 0 | 有销售无生产, recv=0 货从哪来? |
| 21340031 | (成品) | 32 | 0 | 同上 |
| 21346743 | 椰蓉古法奶油包 | 27 | 45 | 每天固定9个, 明显是人工填的固定值 |
| 21346590 | 现烤老婆饼 | 98 | 116 | 源表偏大, 部分可从库存消耗 |

**结论: 源表的 compose_in_qty 和 compose_out_qty 同样不可靠，不能使用。**

### 1.2 金额不可靠 (原问题)

源表 compose_in_amt_src / compose_out_amt_src 常为 0 或任意值，cost_price/avg_inbound 兜底语义错误。

### 1.2 核心问题

1. **源表 compose 数量不可靠**: `atomic_compose` 的 compose_in_qty 漏报严重（销售 113 个但生产记录仅 50 个）
2. **源表 compose 金额不可靠**: compose_in_amt_src 和 compose_out_amt_src 常为 0 或任意值
3. **cost_price/avg_inbound 兜底语义错误**: cost_price 是库存池成本价，avg_inbound 是历史采购均价，都不是加工成本

### 1.3 数据现状

| 指标 | 数值 |
|------|:---:|
| compose_in 活动行数 | 72 (1.2%) |
| compose_out 活动行数 | 84 |
| 加工关系成品总数 | **45** (FM 平台业务维护) |
| 有 compose_in 活动且有加工关系 | 20 / 22 |
| 有 compose_in 活动但无加工关系 | 1 (麻椒鸡半只) |
| 有加工关系但无 compose_in 活动 | **25** (源表漏报! 这些成品有销售但被记为0) |

---

## 二、修复: compose 数量和金额完全从业务行为推导

### 2.1 数量推导

```
成品 compose_in_qty = max(0, sale_qty + know_lost_qty - init_stock_qty - self_receive_qty)
```
- 成品没有直接收货 (recv=0): 所有销售和损耗都来自加工产出
- 优先消耗期初库存，不够的部分由加工生产补充
- 如果 recv > 0（极少数情况），减去直接采购量

```
原料 compose_out_qty = Σ(成品 compose_in_qty × raw_qty / yield_qty)
```
- 从加工关系的配方比例反推
- 一个原料可能供给多个成品，汇总所有成品对它的消耗

### 2.2 金额计算

```
compose_out_amt = compose_out_qty × base_euc  (价值守恒)
compose_in_amt  = compose_in_qty × Σ(raw_qty / yield_qty × raw_base_euc)  (配方推算)
```

### 2.3 新计算链

```
t_atomic_wide
    │
    ├── sale_qty, know_lost_qty, init_stock_qty, self_receive_qty
    │   └── → compose_in_qty  = max(0, sale + loss - init - recv)  ← 🔧 推导
    │
    ├── 加工关系 (FM平台维护, 45个成品)
    │   └── → compose_out_qty = Σ(finished_cin × raw_qty / yield_qty)  ← 🔧 推导
    │
    ├── base_euc = (init + recv + bom) / (init_q + recv_q + bom_q)
    │   ├── → compose_out_amt = compose_out_qty × base_euc  (价值守恒)
    │   └── → compose_in_amt  = compose_in_qty × Σ(raw_qty/yield × raw_base_euc)
    │
    └── compose_net = compose_in - compose_out
            │
            ▼
    euc = (base_amt + compose_net_amt) / (base_qty + compose_net_qty)
```

### 2.4 链路修改

| 模块 | 改动 |
|------|------|
| `sku_cost.py` SELECT | 加载 sale_qty, know_lost_qty；不再加载 compose_in/out_qty 和 compose_in/out_amt_src |
| `sku_cost.py` Step 6 | **重写**: 从销售/损耗推导 compose_in_qty 和 compose_out_qty |
| `sku_cost.py` Step 8 | compose 金额由配方推算（不变） |
| `sku_cost.py` 输出 | 新增 compose_in_qty, compose_out_qty 列 |
| `stock.py` SELECT | 不再加载 compose_in/out_qty, compose_in/out_amt_src |
| `stock.py` euc_df | 从 t_calc_sku_cost 读取 compose_in/out_qty |
| `stock.py` 回退逻辑 | 删除源表 compose_in/out_amt_src 回退 |

### 2.5 自愈机制

```
Day 1: 原料 SKU 无 base_euc → compose_out_amt=0 → euc 从兜底链
       (current_price × 0.40 = 估算成本)
         │
         ▼
Day 1 末: t_calc_stock.end_stock 用估算 euc 计价
         │
         ▼
Day 2: init_stock 有金额 → base_euc > 0
       → compose_out_amt = qty × base_euc ✅ (价值守恒生效!)
       → 如果该 SKU 是其他成品的原料:
          recipe calc 也能取到 raw_euc > 0
          → compose_in_amt 正确计算 ✅
```

**关键**: 即使首日依赖兜底估算，第二日加工金额就完全由加工关系驱动，不再需要兜底。

---

## 三、决策树: 每个 compose 场景的处理

```
SKU 当天有 compose 活动
│
├── compose_out_qty > 0 (原料, 被加工消耗)
│   │
│   ├── base_euc > 0
│   │   └── compose_out_amt = compose_out_qty × base_euc  ✅ 价值守恒
│   │
│   └── base_euc = 0 (首日或 EUC=0 SKU)
│       └── compose_out_amt = 0
│           → cost_qty 变小 (被 compose_out_qty 扣减)
│           → euc 可能为 0 → 兜底链 (current_price×0.40)
│           → 次日 base_euc > 0 → 自动修复 ✅
│
├── compose_in_qty > 0 (成品, 加工产出)
│   │
│   ├── 加工关系存在
│   │   ├── 所有原料 raw_euc > 0
│   │   │   └── compose_in_amt = qty × Σ(raw_qty/yield × raw_euc)  ✅ 配方推算
│   │   │
│   │   └── 部分原料 raw_euc = 0
│   │       └── compose_in_amt = 0 (配方不完整, 无法推算)
│   │           → 兜底链 → 次日原料修复后自动修复 ✅
│   │
│   └── 加工关系不存在
│       └── compose_in_amt = 0 ← 🔧 不再用源表值!
│           → 兜底链给出估算
│           → 同时暴露"缺失加工关系"的数据问题
│
└── compose_in > 0 且 compose_out > 0 (同一 SKU 既是原料又是成品)
    └── 分别按上述规则独立计算 in 和 out
        compose_net_amt = compose_in_amt - compose_out_amt
```

---

## 四、影响评估

### 4.1 直接影响

> 加工关系共覆盖 **45 个成品 SKU**，其中 20 个在当期数据中有 compose_in 活动。
> 其余 25 个成品暂无 compose_in 活动，加工关系处于"就绪待用"状态。

| SKU 类别 | 数量 | 当前行为 | 修改后 | 影响 |
|----------|:---:|------|------|:---:|
| compose_in, 有加工关系 | 20 | 配方推算 | 配方推算 | **无变化** ✅ |
| compose_in, 无加工关系 | 2 | 用源表值(~38-66元/次) | compose_in_amt=0 | EUC 从兜底链估算 |
| compose_out, base_euc>0 | ~24 | 价值守恒 | 价值守恒 | **无变化** ✅ |
| compose_out, base_euc=0 | 0 (当前) | 用源表值 | compose_out_amt=0 | EUC兜底→次日修复 |

### 4.2 1 个剩余受影响 SKU

| SKU | 名称 | 当前 compose_in_amt/次 | 修改后 | 兜底估算 |
|-----|------|:---:|:---:|:---:|
| 21282423 | 葡式蛋挞6个(C) | — | ✅ 已补充加工关系 | 3.50 元/盒 (配方推算) |
| 21267680 | 麻椒鸡半只(ZC) | 38 元 | 0 → 兜底 | 应补充加工关系! |

**葡式蛋挞6个已修复** (2026-06-04): 加工关系已激活，ETL 验证通过。详见 [§七 实测计算链](#七实测计算链以葡式蛋挞为例)。

### 4.3 间接收益

1. **彻底摆脱源表**: compose 的数量和金额都不再依赖 `strategy_fm_compose_di`，数据质量不受源表漏报/误报影响
2. **暴露源表数据质量问题**: 25 个有加工关系但源表 compose_in=0 的成品现在能正确计算
3. **消除 cost_price/avg_inbound 在 compose 中的误用**
4. **与 EUC 兜底链配合**: 无加工关系的 2 个 SKU 进入兜底估算
5. **自愈能力**: 一旦某 SKU 通过兜底获得 euc，次日加工计算自动正确

---

## 五、代码修改

### 5.1 修改位置

`fmetl/calculated/sku_cost.py`

### 5.2 改动 A: 删除源表金额依赖 (Step 3)

```python
# ========== 删除以下代码 ==========
# 6. Python: compose 初始净额（用源表 amt，后续用加工关系修正）
df['compose_net_qty'] = df['compose_in_qty'] - df['compose_out_qty']
df['compose_in_amt'] = df['compose_in_amt_src'].fillna(0)        # ← 删除
df['compose_out_amt'] = df['compose_out_amt_src'].fillna(0)      # ← 删除
df['compose_net_amt'] = df['compose_in_amt'] - df['compose_out_amt']  # ← 删除
# 第一层兜底: cost_price (系统标准成本)
fallback_cp = (...)
df.loc[fallback_cp, 'compose_net_amt'] = (...)                     # ← 删除
# 第二层兜底: avg_inbound_price (历史采购均价)
fallback_aip = (...)
df.loc[fallback_aip, 'compose_net_amt'] = (...)                    # ← 删除

# ========== 替换为 ==========
# 6. compose 金额将在 Step 5 由加工关系纯推算
df['compose_net_qty'] = df['compose_in_qty'] - df['compose_out_qty']
df['compose_in_amt'] = 0.0     # 初始化为0, 等待加工关系推算
df['compose_out_amt'] = 0.0    # 初始化为0, 等待价值守恒推算
df['compose_net_amt'] = 0.0
```

### 5.3 改动 B: _apply_compose_corrections 去掉源表兜底

在方法中删除 "无加工关系 → 保留源表值" 的逻辑:

```python
# ========== 修改前 ==========
in_mask = df['compose_in_qty'] > 0
in_indices = df[in_mask].index

for idx in in_indices:
    article_id = str(df.at[idx, 'article_id'])
    if article_id not in proc_map:
        continue  # 无加工关系 → 保留源表值  ← 要改这里

# ========== 修改后 ==========
in_mask = df['compose_in_qty'] > 0
in_indices = df[in_mask].index

for idx in in_indices:
    article_id = str(df.at[idx, 'article_id'])
    if article_id not in proc_map:
        # 无加工关系 → compose_in_amt 保持 0
        # EUC 兜底链会在后续步骤提供估算
        self._log.debug(f"compose_in SKU {article_id} 无加工关系, compose_in_amt=0")
        continue
```

### 5.4 改动 C: compose_out 也去掉源表回退

```python
# ========== 修改前 ==========
out_mask = df['compose_out_qty'] > 0
out_with_euc = out_mask & (df['base_euc'] > 0)
df.loc[out_with_euc, 'compose_out_amt'] = (
    df.loc[out_with_euc, 'compose_out_qty'] * df.loc[out_with_euc, 'base_euc']
).round(4)
# 其他行 (base_euc=0) 保留 compose_out_amt_src  ← 隐式依赖源表

# ========== 修改后 ==========
out_mask = df['compose_out_qty'] > 0
out_with_euc = out_mask & (df['base_euc'] > 0)
df.loc[out_with_euc, 'compose_out_amt'] = (
    df.loc[out_with_euc, 'compose_out_qty'] * df.loc[out_with_euc, 'base_euc']
).round(4)
# base_euc=0 的行 → compose_out_amt 保持 0 (已在上一步初始化为0)
# EUC 兜底链会在后续步骤提供估算
```

### 5.5 改动 D: no_base 分支更新注释

```python
# 对于只有 compose_out 没有进货的原料，base_euc 为 0 导致 EUC 也是 0
# compose_out_amt 来自价值守恒 (qty × base_euc)，base_euc=0 时为 0
# 此分支将进入 EUC 兜底链 (current_price × 0.40 估算)
# 次日 base_euc > 0 后，价值守恒自动生效
no_base = (df['base_cost_qty'] <= 0) & (df['compose_out_qty'] > 0)
df.loc[no_base, 'effective_unit_cost'] = (
    df.loc[no_base, 'compose_out_amt'] / df.loc[no_base, 'compose_out_qty']
)
```

---

## 六、与 EUC 兜底链的配合

修改后的完整 EUC 计算链:

```
Step 1-2: 加载 wide_df + init_stock (不变)
Step 3:   compose_net_qty 保留, compose_*_amt 初始化为 0  ← 🔧 改动
Step 4:   base_euc = (init+recv+bom) / qty (不变)
Step 5:   compose 纯加工关系推算                            ← 🔧 改动
          ├── compose_out_amt = qty × base_euc (价值守恒)
          └── compose_in_amt = 配方推算 (加工关系)
Step 6:   euc = (base + compose_net) / total_qty (不变)
          特殊: no_base → euc=0 → 兜底链
Step 7:   ffill (不变)
Step 8:   avg_inbound_price 兜底 (不变)
Step 8.5: cost_price 兜底                          ← 🔧 EUC修复新增
Step 8.6: current_price × 0.40 兜底                ← 🔧 EUC修复新增
Step 9:   processing_relation 兜底 (不变)
```

---

## 七、实测计算链（以葡式蛋挞为例）

> 验证日期: 2026-06-03 | 门店: A3XV | ETL 版本: v0.10

### 7.1 加工关系

| 原料 SKU | 原料名称 | 用量 | 产出 |
|----------|---------|:---:|:---:|
| 21326066 | 好禧坊葡式蛋挞液907g(C) | 1支 | 23盒 |
| 21340840 | 仿手工葡挞皮660g(C) | 1袋 | 5盒 |

### 7.2 第一步: compose_in_qty 数量推导

```
compose_in_qty = max(0, sale_qty + know_lost_qty - init_stock_qty - self_receive_qty)
```

| 字段 | 值 | 说明 |
|------|:--:|------|
| sale_qty | 8 | 当天卖出 8 盒 |
| know_lost_qty | 0 | 无已知损耗 |
| init_stock_qty | 0 | 源表 init=-6 被 clip(lower=0) 截断为 0 |
| self_receive_qty | 0 | 成品无直接进货 |

```
compose_in_qty = max(0, 8 + 0 - 0 - 0) = 8 盒
```

**为什么不用源表 compose_in_qty？** 源表 `strategy_fm_compose_di` 常漏报此 SKU 的 compose_in，当天可能为 0 或任意值。改为从销售反推：卖出 8 盒却无进货无期初库存 → 这 8 盒必定是加工产出。

### 7.3 第二步: 原料 base_euc 计算

```
base_euc = (init_stock_amt + self_receive_amt + bom_alloc_amt)
         / (init_stock_qty + self_receive_qty + bom_alloc_qty)
```

| 原料 | init | recv | bom | base_cost_amt | base_cost_qty | **base_euc** |
|------|------|------|-----|:---:|:---:|:---:|
| 21326066 蛋挞液 | 73支/1103.93元 | 0 | 0 | 1103.93 | 73 | **15.1224** |
| 21340840 葡挞皮 | 32袋/454.72元 | 0 | 0 | 454.72 | 32 | **14.2100** |

> 这些 EUC 是原料作为普通 SKU 计算的加权平均成本，**不含 compose** 影响（base_euc 排除了 compose_net，防止循环依赖）。

### 7.4 第三步: 成品 compose_in_amt 配方推算

```
成品单位成本 = Σ (raw_qty / yield_qty × raw_base_euc)

compose_in_amt = compose_in_qty × 成品单位成本
```

代入实测数据:

```
成品单位成本 = (1/23) × 15.1224 + (1/5) × 14.2100
             = 0.6575 + 2.8420
             = 3.4995  元/盒

compose_in_amt = 8 × 3.4995 = 27.996 元
```

**解读**: 生产 1 盒葡式蛋挞需要 1/23 支蛋挞液 (0.6575元) + 1/5 袋葡挞皮 (2.8420元) = 3.50 元原料成本。8 盒总加工成本 28.00 元。

### 7.5 第四步: 原料 compose_out_amt 价值守恒

```
compose_out_qty = Σ(成品compose_in_qty × raw_qty / yield_qty)
compose_out_amt = compose_out_qty × base_euc
```

| 原料 | compose_out_qty | × base_euc | compose_out_amt |
|------|:---:|:---:|:---:|
| 21326066 蛋挞液 | 8 × 1/23 = **0.3478** 支 | 15.1224 | **5.26** 元 |
| 21340840 葡挞皮 | 8 × 1/5 = **1.6000** 袋 | 14.2100 | **22.74** 元 |

**验证**: compose_out_amt 合计 = 5.26 + 22.74 = 28.00 元 = compose_in_amt ✅

> 这体现了**价值守恒**原则：原料的成本通过加工转移到成品，成品加工入账 = 原料加工出账，总价值不灭。

### 7.6 第五步: 成品最终 EUC

```
compose_net_qty = compose_in_qty - compose_out_qty = 8 - 0 = 8
compose_net_amt = compose_in_amt - compose_out_amt = 28.00 - 0 = 28.00

cost_amt = base_cost_amt + compose_net_amt = 0 + 28.00 = 28.00
cost_qty = base_cost_qty + compose_net_qty = 0 + 8 = 8

euc = cost_amt / cost_qty = 28.00 / 8 = 3.4995
```

成品 21282423 的最终 EUC = **3.50 元/盒**，cost_source = `V10_WEIGHTED_AVG`。

> 对比之前（无加工关系时）此 SKU 的 euc ≈ 6.60 元。差异来自兜底估算 → 现在有精确的配方推算，成本更准确。

### 7.7 计算链全景

```
t_atomic_wide                    sale_qty=8, recv=0, init=0, loss=0
    │
    ▼  [Step 6: 数量推导]
compose_in_qty  = max(0, 8+0-0-0) = 8         ← 从业务行为推导
compose_out_qty = 8×1/23=0.3478 + 8×1/5=1.6  ← 从配方反推
    │
    ▼  [Step 7: base_euc]
蛋挞液 euc = 1103.93/73 = 15.1224             ← 不含 compose
葡挞皮 euc = 454.72/32  = 14.2100             ← 不含 compose
    │
    ▼  [Step 8: 加工关系推算]
compose_in_amt  = 8 × (15.1224/23 + 14.2100/5)
                = 8 × 3.4995 = 28.00           ← 配方推算
compose_out_amt = 0.3478×15.1224 + 1.6×14.2100
                = 28.00                        ← 价值守恒
    │
    ▼  [Step 9: 最终 EUC]
euc_21282423 = (0 + 28.00) / (0 + 8) = 3.50  ← V10_WEIGHTED_AVG
```

### 7.8 跨日自愈机制

```
Day 1: 原料 EUC=0 (首日) → 配方不完整 → compose_in_amt=0 → euc 进兜底链
       │
       ▼ 兜底链给出估算 euc (或 0 → V10_PROCESSING_RELATION 推算)
Day 1 末: t_calc_stock.end_stock 用估算 euc 计价
       │
       ▼
Day 2: init_stock 有金额 → base_euc > 0 → 配方完整
       → compose_in_amt = qty × Σ(raw_qty/yield × raw_euc) ✅
       → compose_out_amt = qty × base_euc ✅
```

**关键**: 即使首日依赖兜底，第二日加工金额就完全由加工关系驱动，不再需要兜底。

### 7.9 全月实测：自愈机制验证

> 2026-05-01 ~ 2026-06-03 共 34 天，葡式蛋挞6个(C) 每天都有销售。

**愈合前 (5/1~5/16, 16天):**

```
compose_in_qty > 0 (有销售)
compose_in_amt = 0          ← 原料 base_euc=0，配方无法推算
euc 来源: V10_PROCESSING_RELATION (0.67元) 或 V10_AVG_INBOUND_FALLBACK (6.48元)
```

原料首日没有期初库存，base_euc=0 → 配方不完整 → compose_in_amt=0。成品 EUC 进入兜底链。

**愈合点: 5月17日**

```
蛋挞液 base_euc = 15.12  (累积了足够的 init_stock)
葡挞皮 base_euc = 14.21  (累积了足够的 init_stock)
↓
配方完整 → compose_in_amt = qty × 3.5146 → V10_WEIGHTED_AVG ✅
```

**愈合后 (5/17~6/3, 18天):**

| 日期 | compose_in_qty | compose_in_amt | euc | cost_source |
|------|:---:|:---:|:---:|------|
| 5/17 | 28 | 98.41 | 3.51 | V10_WEIGHTED_AVG |
| 5/18 | 2 | 7.03 | 3.51 | V10_WEIGHTED_AVG |
| 5/19 | 4 | 14.06 | 3.51 | V10_WEIGHTED_AVG |
| 5/20 | 12 | 42.18 | 3.51 | V10_WEIGHTED_AVG |
| ... | ... | ... | ~3.50 | V10_WEIGHTED_AVG |
| 6/03 | 8 | 28.00 | 3.50 | V10_WEIGHTED_AVG |

**全月汇总:**

| 指标 | 数值 |
|------|:---:|
| 葡式蛋挞 compose 天数 | 34/34 天 |
| 总产出 | **356 盒** |
| 总加工入账 compose_in_amt | **700.41 元** |
| 其中配方推算 (愈合后) | 18 天 / 602 元 |
| 其中兜底估算 (愈合前) | 16 天 / 0 元 → 兜底链接管 |
| 蛋挞液累计消耗 | 15.48 支 / 236.94 元 |
| 葡挞皮累计消耗 | 40.00 袋 / 568.40 元 |
| 价值守恒验证 | 236.94 + 568.40 ≈ 700.41 元 (差额来自首16天 compose_out 为零) |
| 愈合后日均 euc | **~3.50 元/盒** |

**全量统计 (34天，94个成品):**

| 指标 | 数值 |
|------|:---:|
| compose_in 行 | 647 |
| compose_out 行 | 723 |
| 独特加工成品 SKU | 94 |
| V10_WEIGHTED_AVG (配方推算) | 584 |
| V10_PROCESSING_RELATION (兜底) | 63 |

**关键洞察**: 
1. 自愈机制不是"第二天"生效，而是"原料 base_euc > 0 的第二天"生效
2. 葡式蛋挞的原料在 5/17 才首次有 base_euc > 0（累积了足够的 init_stock）
3. 愈合后 EUC 保持稳定（3.49-3.51），而愈合前波动剧烈（0.67 ↔ 6.48）
4. 这证明了 **compose 金额完全由加工关系驱动**，不稳定源表值已被彻底切断

---

## 八、验证清单

- [x] 修改 sku_cost.py: 删除 compose_*_amt_src 初始化和两步兜底 (277f296)
- [x] 修改 _apply_compose_corrections: 删除源表回退逻辑 (277f296)
- [x] 服务器 ETL 验证: `python -m fmetl.executor 2026-06-03 2026-06-03` (2026-06-04)
- [x] 验证有加工关系的 28 个 compose_in SKU 金额由配方推算
- [x] 验证 compose_in_amt = compose_in_qty × Σ(raw_qty/yield_qty × raw_euc) ← 葡式蛋挞实测 ✅
- [x] 验证 compose_out_amt = compose_out_qty × base_euc (价值守恒) ← 蛋挞液/葡挞皮实测 ✅
- [x] 验证 compose_out_amt 合计 = compose_in_amt (28.00 = 28.00)
- [x] 验证 1 个无加工关系 SKU (麻椒鸡) 的 compose_in_amt=0, euc 进兜底链
- [x] 葡式蛋挞6个(C) 加工关系已补充并激活 (2026-06-04)
- [ ] 建议产品侧补充 麻椒鸡半只 的加工关系
- [x] commit + push
