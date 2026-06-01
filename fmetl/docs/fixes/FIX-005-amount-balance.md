# §2.6 库存方程金额平衡分析

> 审查报告声称: 23.9% 的行 (18,367/76,933) 金额偏差超过 0.01 元
>
> **重新验证结论: 该数据存在公式错误。正确公式下 98.0% 的行平衡 (< 0.01 元)。**
>
> 剩余 2% 的不平衡由结构性因素驱动 (欧佩/进价差异、跨日 EUC 变化、BOM 金额异源定价)。

---

## 一、数量方程 vs 金额方程

### 1.1 数量方程 (由代码强制保证，100% 平衡)

```python
# stock.py:304-310 — 每个 qty 字段都被等式约束
eq_end_qty = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost

# 然后 eq_end_qty 被分配到 end 或 unknow:
#   eq=0  → end=0,  unknow=0
#   eq<0  → end=0,  unknow=-eq
#   eq>0  → end=eq, unknow=0
```

**数量方程永远不会失败** — `end_qty` 和 `unknow_qty` 就是从 `eq` 计算出来的，它们是 `eq` 的因变量。

### 1.2 金额方程 (不强制保证，依赖多源定价)

```
balance_amt = init_stock_amt + receive_amt + bom_in_amt - bom_out_amt
            + compose_in_amt - compose_out_amt
            - sale_cost_amt
            - know_lost_amt - unknow_lost_amt - end_stock_amt
            + stock_transfer_out_amt - stock_transfer_in_amt
```

理想情况 `balance_amt = 0`。但**不同流使用不同的计价基础**:

| 流 | 计价基础 | 与 euc 一致? |
|---|---|---|
| `sale_cost_amt` | `sale_qty × current_euc` | ✅ 精确一致 |
| `end_stock_amt` | `end_qty × current_euc` | ✅ 精确一致 |
| `know_lost_amt` | `know_qty × current_euc` | ✅ 精确一致 |
| `unknow_lost_amt` | `unknow_qty × current_euc` | ✅ 精确一致 |
| `stock_transfer_*` | `transfer_qty × current_euc` | ✅ 精确一致 |
| `init_stock_amt` | `init_qty × prev_day_euc` | ⚠️ 跨日 EUC 变化时会偏离 |
| `receive_amt` | 采购发票价格 (源表) | ❌ 独立于 euc |
| `bom_in/out_amt` | BOM 分配金额 (父品采购价×比例) | ❌ 独立于子品 euc |
| `compose_in/out_amt` | 加工配方成本或源表金额 | ❌ 独立于 euc |

---

## 二、审查报告公式的 Bug

### 2.1 审查报告使用的公式

审查报告 §2.6 的库存方程写道:
```
init + receive + bom_in - bom_out + compose_in - compose_out
- sale - know_lost - end - unknow = 0
```

这个公式中的 `- sale` 使用的是 **`sale_amt`（销售额/售价）**，而不是 `sale_cost_amt`（销售成本）。

### 2.2 为什么这是错误的

```
┌─────────────────────────────────────────────────────────────────┐
│                   售价 vs 成本价 的区别                         │
│                                                                 │
│   sale_amt (售价)          = 销售数量 × 零售价                  │
│   sale_cost_amt (销售成本) = 销售数量 × euc (加权平均成本)      │
│                                                                 │
│   例如: 一瓶矿泉水                                              │
│     euc = 0.80 (进货成本)                                       │
│     sale_amt = 2.00 (卖价)                                      │
│     sale_cost_amt = 1 × 0.80 = 0.80 (成本)                     │
│                                                                 │
│   库存方程是成本流转方程，应该用 sale_cost (0.80)               │
│   如果用了 sale_amt (2.00)，方程会"不平衡" 1.20 元              │
│   但这 1.20 元是毛利, 不是库存错误!                             │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 正确公式的平衡率

| 公式 | 偏差<0.01元的比例 |
|---|---|
| 审查报告公式 (用 `sale_amt`) | 76.1% (= 100% - 23.9%) |
| **正确公式** (用 `sale_cost_amt` + `stock_transfer`) | **98.0%** |

---

## 三、正确公式的偏差分布

```
偏差范围        行数      占比
─────────────────────────────────
≈0 (<0.01元)   75,357    98.0%   ← 完美平衡
0.01~1元          731     1.0%   ← 微小偏差 (四舍五入/跨日euc差异)
1~10元            390     0.5%   ← receive采购价≠euc估值
10~100元          336     0.4%   ← BOM/compose金额差异
>100元            119     0.2%   ← 大额BOM/compose活动
─────────────────────────────────
总计           76,933   100.0%
```

**平均绝对偏差: 0.66 元/行**

---

## 四、偏差根因逐项分析

### 4.1 EUC=0 SKU (26,976 行, 偏差=0)

EUC=0 的 SKU 所有 `qty × euc` 项都为 0，且 `receive_amt`、`bom_amt` 也为 0。方程两侧都为 0 → 完美平衡。

### 4.2 EUC>0 SKU (49,957 行)

```
┌──────────────────────────────────────────────────────────────────┐
│              金额方程不平衡的三大来源 (树状图)                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  金额方程:                                                       │
│  balance = init + receive + bom_in - bom_out                     │
│          + compose_in - compose_out + transfer_out - transfer_in │
│          - sale_cost - know_lost - unknow_lost - end             │
│                                                                  │
│  ├── 来源1: receive_amt ≠ receive_qty × euc                     │
│  │   ├── 原因: receive_amt 来自采购发票 (源表)                  │
│  │   │         euc 是加权平均成本 (含期初库存+历史采购)          │
│  │   ├── 示例: 今天进价19元, 但euc=17.25元 (含低价库存)         │
│  │   │         receive_qty=12, receive_amt=228                   │
│  │   │         receive_qty×euc = 12×17.25 = 207                  │
│  │   │         偏差 = 228 - 207 = 21 元                          │
│  │   └── 影响: 平均偏差 1.02 元 (大偏差行)                      │
│  │                                                                  │
│  ├── 来源2: init_stock_amt 使用 prev_day euc                     │
│  │   ├── 原因: init_stock_amt = 昨日期末 × 昨天euc              │
│  │   │         但今天 euc 可能已变化                              │
│  │   ├── 示例: 昨天 end=42件×13.729=576.62                      │
│  │   │         今天 euc=17.247                                    │
│  │   │         init = 576.62 (昨天euc计价)                       │
│  │   │         init_qty × 今天euc = 42×17.247 = 724.39           │
│  │   │         偏差 = 724.39 - 576.62 = 147.77 元                │
│  │   └── 影响: 平均偏差 4.46 元 (大偏差行), 是最大偏差源        │
│  │                                                                  │
│  └── 来源3: BOM / Compose 金额异源定价                            │
│      ├── 原因: bom_in_amt = 父品采购价 × 分配比例               │
│      │         ≠ 子品 euc × bom_in_qty                            │
│      │         compose 同理，配方成本 ≠ euc                       │
│      ├── 示例: 父品进价20元分给子品，子品euc=15元                │
│      │         bom_in_amt=200 (父品计价)                          │
│      │         bom_in_qty×euc = 10×15=150 (子品euc计价)           │
│      │         偏差 = 200-150 = 50元                              │
│      └── 影响: 大偏差行中 BOM/Compose 平均流量 484.59 元         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 4.3 按 day_clear 分类

| day_clear | 行数 | 平均偏差 | >10元偏差行数 |
|:---|:---:|:---:|:---:|
| '0' (日清) | 3,952 | 6.24 | 71 |
| '1' (非日清) | 72,981 | 0.36 | 384 |

日清品偏差更大 (6.24 vs 0.36) 因为:
- 日清品 `end_stock` 被强制归零，残差转入 `unknow_lost`
- `unknow_lost` 按 euc 计价，但 `receive` 按采购价
- 日清品多为生鲜，进价波动大 → receive vs euc 差距大

---

## 五、119 行大额偏差 (>100元) 深度剖析

```
特征:
  平均偏差:        337.18 元
  receive vs euc:    0.31 元   ← 几乎无贡献
  init vs euc:       8.46 元   ← 轻微贡献
  BOM+Compose 流量: 484.59 元  ← 主要驱动!
```

**结论**: 大额偏差几乎全部由 BOM 父品/加工品造成。

追踪示例 — BOM 父品 21037825 (杂粮鲜鸡蛋30枚):

```
5/29 金额方程:
  init_stock_amt    = +576.62   (42件 × 昨天euc=13.729)
  receive_amt       = +228.00   (12件 × 采购价19元)
  bom_out_amt       = -228.00   (12件 × 父品进价19元, 分给子品)
  sale_cost_amt     = 0
  end_stock_amt     = 0        (转移后清空)
  stock_transfer_out = +724.39  (42件 × 今天euc=17.247)
  stock_transfer_in  = 0        (父品不收转移)

  balance = 576.62 + 228 - 228 + 724.39 - 0 - 0 - 0 - 0
          = 1301.01  ← 不可能是0! sale_cost=0, 没有扣减项

  但这是公式误用 — transfer_out 是内部转移, 不是外部流入
  正确理解: end_stock被转移到子品, 父品方程应扣减它
```

实际上，119 行大偏差全部来自有 `stock_transfer_out` 的 BOM 父品行。这些行的特征是:
- `sale = 0` (父品当天无销售)
- `stock_transfer_out >> 0` (大量库存转移给子品)
- `bom_out` 和 `receive` 近似相等 (当日进货全部分配)

金额不平衡是因为 **stock_transfer 在父品上标记了流出，但子品上的流入已被 bom_in 和 end_stock 抵消**。父品方程中 transfer_out 没有对应的扣减项。

**这不是库存错误，而是公式对 stock_transfer 的会计处理不完整。** 修复方案见 [FIX-004 BOM父品负毛利](FIX-004-bom-transfer.md)——在父品上增加 bom_out 以匹配 transfer_out。

---

## 六、结论与建议

### 6.1 审查报告的 23.9% 需要修正

| 项 | 审查报告当前值 | 建议修正值 |
|---|---|---|
| 公式 | `init+...+bom-...-sale-...-end-unknow` | `+ transfer_out - transfer_in - sale_cost` |
| 偏差率 | 23.9% | **2.0%** |
| 问题等级 | 🔴 P0 | **🟡 P2 (结构性偏差, 非Bug)** |

### 6.2 剩余 2% 偏差的本质

2% 的行 (1,576行, 主要是 BOM/加工/日清品) 存在金额偏差 > 0.01 元。这个偏差是**结构性**的，原因:

1. **采购价 ≠ euc**: receive_amt 按发票价, euc 按加权平均。这是会计核算的正常现象。
2. **跨日 euc 变化**: init 用昨天 euc, 今天方程用今天 euc。euc 变化是正常的市场波动。
3. **BOM/加工 异源定价**: BOM 金额基于父品成本, compose 金额基于配方成本, 都独立于 SKU 自身的 euc。

**这三者都不应被"修复"——它们是 ETL 正确反映业务现实的体现。**

### 6.3 可修复的部分

唯一可修复的大额偏差来自 **§2.3 BOM 父品负毛利**——当父品 bom_out 不包含 stock_transfer 时，金额方程会产生永久不平衡。修复 §2.3 后，大额偏差 (>100元) 应从 119 行显著减少。

### 6.4 建议

1. **更新审查报告 §2.6**: 将偏差率从 23.9% 修正为 2.0%，说明正确公式和剩余偏差的结构性原因
2. **在 stock_roll.py 中增加 `balance_amt` 列**: 便于后续监控 (当前只有 `balance_qty`)
3. **设置合理的偏差阈值**: 对非 BOM/日清品，偏差 > 1元标记为异常；对 BOM/日清品，偏差 > 100元标记为需关注

---

## 七、验证清单

- [ ] 更新审查报告 §2.6 的公式和偏差率
- [ ] 在 stock_roll.py 增加 `balance_amt` 计算列
- [ ] 在 stock_roll.py 增加 `balance_amt` 的 `ABS()>1` 标记列
- [ ] 修复 §2.3 BOM 父品 transfer 后重新验证大额偏差是否减少
- [ ] commit + push

---

## 八、相关文档

- [FIX-004 BOM父品转移](FIX-004-bom-transfer.md) — Stock Transfer 导致负毛利 (同一根因)
- 审查报告 §2.6 — 原始问题描述 (需修正公式)
- stock.py:304-310 — 库存方程 (qty)
- stock.py:365-371 — 金额计算 (amt = qty × euc)
- stock_roll.py:77-83 — balance_qty 计算
