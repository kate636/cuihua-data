# FIX-020：负库存钉零口径改为盘盈（Option B）+ 利润扣减解耦

> **日期**: 2026-06-24 | **影响模块**: stock.py, profit.py | **状态**: ✅ 已实现（ETL 已验证）
> **来源**: [REVIEW-008](../reviews/REVIEW-008-full-chain-logic-audit.md) 的"待用户决策"项，用户选定口径 B
> **前置**: 扩展并解耦 [FIX-019](FIX-019-negative-stock-clamp-cost.md)

## 问题

当库存方程 `eq = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost < 0`（超卖/超损）时，stock.py 把 `end` 钉到 0（库存不可为负）。原 FIX-019（口径 A）把透支量 `-eq` 记为**正的 unknow_lost**（额外未知损耗）。这带来两个问题：

1. **库存方程不平衡**：`end + unknow = 0 + (-eq) = -eq`，但平衡要求 `end + unknow = eq`（eq<0）。每行偏离 `2×eq`。6/18–22 全表数量残差因此达 **−812.52**，其中钉零行精确贡献 `Σ(2×eq) = −897.29`（REVIEW-008 实证）。
2. **未知损耗率虚高**：把超卖记成"额外丢货"在物理上讲不通——货是被**卖掉**的，不是丢的。这让"未知损耗率"这个防损 KPI 虚高近一倍（钉零占未知损耗 73%）。

## 根因 / 语义辨析

`eq<0` 意味着账面 `sale + know_lost` 超过 `init + 供给`。"卖出多于账面拥有"在物理上只能解释为**进货/期初被低估**（你不可能卖出不存在的货），而非额外丢货。因此正确的库存调整应是**盘盈**（负 unknow），不是损耗（正 unknow）。

代码中 `is_counted` / `day_clear='0'` / 系统盘盈检测三个分支早已用"负 unknow = 盘盈"的约定，FIX-020 只是把 `eq<0` 分支也纳入同一约定，保持一致。

## 方案

### stock.py（口径 B）

`eq<0` 分支：

```python
elif eq < 0:
    end_qty[idx] = 0
    unknow_qty[idx] = eq          # 负值 = 盘盈(库存被低估), 使 end+unknow=eq 精确平衡
    neg_clamp_qty[idx] = -eq      # 透支量(正), 仅此分支非0, 供 profit.py 扣回成本
```

新增列 `neg_clamp_cost_amt = neg_clamp_qty × euc`（仅 `eq<0` 钉零分支非 0；`is_counted`/盘盈/日清角落均为 0）。幂等 `ALTER ADD COLUMN` 迁移已有表。

### profit.py（解耦扣减）

利润公式用 `end=0`（钉高）会虚高 `(-eq)×euc`，必须扣回。原 FIX-019 借 `unknow_lost_amt` 做句柄，但口径 B 下 unknow 变负，句柄失效。改为直接扣 stock.py 精确算出的 `neg_clamp_cost_amt`：

```python
df['neg_clamp_cost_amt'] = df['neg_clamp_cost_amt'].fillna(0.0)
df['profit_amt'] -= df['neg_clamp_cost_amt']
```

`neg_clamp_cost_amt` 只在真钉零分支非 0，自动排除了 `is_counted` 实盘=0 且 eq<0 的角落（69 行），不再需要 `unknow_lost_qty>0` 守卫。

## 影响（6/18–22 实测）

| 指标 | 口径 A (FIX-019) | 口径 B (FIX-020) | 说明 |
|------|:---:|:---:|------|
| Σprofit | 11952.91 | **11952.91** | **完全不变**（扣减 1422.57 ≈ 原 1422.54） |
| QDM 矩阵差 | +6.3% | **+6.3%** | 利润不变 → 矩阵不回退，无需重跑对比 |
| 库存方程残差(dc=1) | −812.52 | **+52.0** | 钉零 −897 失真消除，剩浮点 |
| 总损耗率 | 14.36% | **11.90%** | 钉零不再计入损耗 |
| 未知损耗率 | 1.69% | **−0.77%** | 转为净盘盈（账面供给被低估的直接体现） |
| 负库存行 | 0 | 0 | — |
| BOM 真残差 | 0.0000 | 0.0000 | — |

**关键**：利润与 QDM 对比矩阵**完全不受影响**（FIX-019 的利润修正被 FIX-020 完整保留，只是换了扣减句柄）。FIX-020 改变的只有**库存数量口径**和**损耗率 KPI 呈现**。

## 副作用提示

未知损耗率转为 **−0.77%**（净盘盈）。这是口径 B 的预期后果：把所有超卖解释为"账面供给被低估"。如果运营更希望看到"未知损耗 ≥ 0"的呈现，可考虑口径 C（钉零分支 unknow=0、只钉零不记盈亏），届时利润扣减仍走 `neg_clamp_cost_amt` 不变。本次按用户选定的口径 B 实现。

## 验证清单

- [x] 库存方程 dc=1 残差从 −812.52 → +52（钉零失真消除）
- [x] Σprofit 不变（11952.91），透支扣减 1422.57
- [x] 负库存 0；BOM 真残差 0.0000
- [x] 损耗率 14.36%→11.90%，未知损耗率转净盘盈
- [x] 新列 `neg_clamp_cost_amt` 幂等迁移，INSERT 列序对齐
