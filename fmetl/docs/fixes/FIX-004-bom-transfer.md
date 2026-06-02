# §2.3 BOM 父品库存转移后产生负毛利 (FIX-004)

> 状态: **⏳ 待实现** | 阻塞: 依赖 FIX-001 ✅ + FIX-002 ⏳ | [修复索引 →](README.md)
>
> 修改文件: `fmetl/calculated/stock.py`
>
> 问题等级: 🔴 P0
>
> 影响: 11 个父品 SKU, 23 行, 虚增亏损 -1,272.80 元
>
> 修复后: 父品利润 -1,273 → +148, 蛋类 -34.9% → ~0%, 5/29 利润 +662
>
> 根因: stock_transfer 清零父品 end_stock 但不调整 bom_out, 导致 init_stock 成为净亏损

---

## 一、问题现象

BOM 父品（整猪→排骨+五花肉等）在 `stock_transfer_out` 后，毛利变为负数。这些父品理论上不应产生毛利（所有成本通过 BOM 分摊给子品），但转移操作破坏了利润方程的平衡。

---

## 二、毛利公式复习

```python
# profit.py:94-100
profit = sale_amt
       - receive_amt
       - bom_in_amt + bom_out_amt
       - compose_in_amt + compose_out_amt
       + end_stock_amt - init_stock_amt
```

**关键**: `stock_transfer_out_amt` 和 `stock_transfer_in_amt` **不在公式中**。

符号规则:
- `- bom_in`: 子品接收 BOM 拆分流入 → 视为成本 (**减法**)
- `+ bom_out`: 父品 BOM 拆分流出 → 视为收入 (**加法**)
- `+ end - init`: 库存变动 → 库存增加 = 利润增加

---

## 三、stock_transfer 触发条件与操作

```python
# stock.py:397-451
# 触发: end_stock > 0 AND bom_out > 0 AND sale = 0
# 操作:
#   父品: end_stock = 0, stock_transfer_out = end_stock
#   子品: end_stock += transfer, bom_in += transfer
```

这是为了清理 BOM 父品的残留库存——父品不应该有期末库存（所有物料都拆给了子品）。

---

## 四、真实案例逐日追踪

### 4.1 案例: 21037825 杂粮鲜鸡蛋30枚 (肉禽蛋类)

该 SKU 既是正常销售品，也是一部分销售的 BOM 父品。

```
5/24~5/27: 正常进货+销售, 无 BOM 分配
5/28:     首次出现 BOM 分配 (分配5件, 但金额为0!)
5/29:     BOM 分配 + stock_transfer → 巨额负利润
5/31:     重置后继续有负利润
```

#### 详细数据:

| 日期 | recv_q | recv_a | bom_out_q | bom_out_a | init_q | init_a | end_q | end_a | sale_q | sale_a | profit |
|------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|-----:|
| 5/24 | 24 | 402.72 | 0 | 0 | 0 | 0 | 19 | 318.82 | 5 | 99.47 | +15.57 |
| 5/25 | 48 | 805.44 | 0 | 0 | 19 | 318.82 | 59 | 990.02 | 8 | 159.14 | +24.90 |
| 5/26 | 0 | 0 | 0 | 0 | 59 | 990.02 | 45 | 755.10 | 14 | 278.57 | +43.65 |
| 5/27 | 0 | 0 | 0 | 0 | 45 | 755.10 | 38 | 637.64 | 7 | 139.24 | +21.78 |
| **5/28** | 10 | **0** | 5 | **0** | 38 | 637.64 | 42 | 576.62 | 1 | 9.90 | **-51.12** |
| **5/29** | 12 | 228 | 12 | 228 | 42 | 576.62 | **0** | **0** | 0 | 0 | **-576.62** ← 关键日 |
| 5/31 | 24 | 456 | 0 | 0 | 0 | 0 | 22 | 386.15 | 2 | 39.80 | -30.05 |

---

### 4.2 5/29 逐步骤追踪 (关键日)

```
═══════════════════════════════════════════════════════════════════
                     BOM 父品转移负利润计算树
═══════════════════════════════════════════════════════════════════

输入:
  init_stock:  qty=42, amt=576.62  ← 前日 (5/28) end_stock
  receive:     qty=12, amt=228.00  ← 当日进货 (12件×19元)
  bom_out:     qty=12, amt=228.00  ← BOM 分配到子品 (12件×19元)
  sale:        qty=0,  amt=0       ← 当日无销售

┌─ 库存方程 ─────────────────────────────────────────────
│ eq = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost
│    = 42   + 12      + 0      - 12     + 0         - 0          - 0    - 0
│    = 42
│
│ 分支: 正常 (eq≥0, 无盘点, 非日清, 无已知损耗)
│   → end_stock = eq = 42 qty
│   → end_stock_amt = 42 × euc(17.247) = 724.39
│
├─ Stock Transfer 触发 ──────────────────────────────────
│ 条件: end_q=42>0 ✓ | bom_out=12>0 ✓ | sale=0 ✓ → 触发!
│
│ 父品操作:
│   stock_transfer_out_qty = 42 (原end)
│   stock_transfer_out_amt = 724.39 (原end_amt)
│   end_stock_qty = 0  ← 清零!
│   end_stock_amt = 0  ← 清零!
│
│ 子品 (20669362) 操作 (ratio=1.0, 仅一个子品):
│   end_stock_qty += 42
│   end_stock_amt += 724.39
│   bom_in_qty    += 42
│   bom_in_amt    += 724.39
│   stock_transfer_in_qty += 42
│   stock_transfer_in_amt += 724.39
│
├─ 利润计算 (BEFORE transfer) ──────────────────────────
│ profit = sale - receive - bom_in + bom_out + end - init
│        = 0    - 228     - 0      + 228    + 724.39 - 576.62
│        = 147.77  ← 估值收益 (euc 从 13.73→17.25)
│
├─ 利润计算 (AFTER transfer) ───────────────────────────
│ profit = 0 - 228 - 0 + 228 + 0 - 576.62
│        = -576.62  ← ⚠️ 亏损 = init_stock_amt!
│
│ 差异分析:
│   end_stock_amt: 724.39 → 0   (减少 724.39)
│   profit 变化:   147.77 → -576.62 (减少 724.39)
│
│   减少额 724.39 = transfer_out_amt = original end_stock_amt
│
├─ 子品利润变化 ────────────────────────────────────────
│ bom_in_amt += 724.39     → profit -= 724.39
│ end_stock_amt += 724.39  → profit += 724.39
│ 净影响: -724.39 + 724.39 = 0  ← 子品利润不变!
│
└─ 结论 ────────────────────────────────────────────────
   transfer 将 724.39 的 end_stock 从父品移到子品
   父品: end -724.39, 无补偿 → profit -724.39
   子品: bom_in +724.39, end +724.39 → 互相抵消
   全系统: profit 净减少 724.39 ✗
```

---

### 4.3 为什么 end_stock 会 > 0？

理论上，如果 BOM 分配完美覆盖所有入库，`eq = init + receive - bom_out = 0`。

但在 21037825 的案例中:

```
5/24-5/27: SKU 以正常销售品运作, 积累 end_stock = 42 qty, 637.64 amt
5/28:     BOM 开始分配, 但 bom_out_amt=0 (源表 inbound_amount=0), 只扣了 qty
          → init 仍是 637.64, 但 bom_out 只扣了 0 amt
          → end = 576.62 (42 qty, euc 变化导致 amt 不同)
5/29:     BOM 分配了当日进货 (12件=228) 但没分配存量 (42件=576.62)
          → end = 576.62 → transfer → -576.62 亏损
```

**根本原因**: BOM 分配金额 (`bom_out_amt`) 覆盖的是 `receive_amt` (当日进货金额)，但不覆盖 `init_stock_amt` (历史累积库存金额)。当历史库存未被 BOM 分配覆盖时，残留的 end_stock 被 transfer 清零，产生亏损。

---

## 五、影响范围统计

| 指标 | 数值 |
|------|:---:|
| 受影响父品数 | 11 |
| 受影响行数 | 23 |
| 总虚增亏损 | **-1,272.80 元** |
| 总 transfer_out (损失额) | 1,420.86 元 |
| 总 init_stock (≈ 亏损额) | 1,272.80 元 |
| 修复后预估利润 | +148.06 元 (估值收益) |

### init 累积程度分布

```
          init=0:  313 行, 利润 -912.38  ← 这些即使没有累积也有亏损
   init>receive:    6 行, 利润 -288.44   ← 历史库存主导
  init>>receive:    3 行, 利润 -663.85   ← 严重累积 (21037825等)
       init>0:     14 行, 利润 -320.51   ← 有累积但不大
```

注意: 313 行 `init=0` 也有负利润，这是因为即使没有历史累积，receive + bom_out 不完全匹配 (金额 ≠ qty×euc 导致)。

---

## 六、修复方案

### 6.1 核心思路

stock_transfer 本质上是 "额外的 BOM 流出"——父品把残留库存分给子品。应该在转移时同步增加父品的 `bom_out_amt` 和 `bom_out_qty`。

### 6.2 修改位置

`fmetl/calculated/stock.py` 第 432-433 行，将注释 `不重复增加` 改为 `增加 bom_out 以保持利润平衡`:

```python
# 修改前 (第417-426行附近):
df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
       (df['store_id'] == pr['store_id']) &
       (df['business_date'] == pr['business_date']),
       'end_stock_qty'] = 0.0
df.loc[parent_mask & ..., 'end_stock_amt'] = 0.0

# v0.10 fix: 不重复增加父品 bom_out_amt/qty
# BOM alloc 已将 100% 母品成本分摊给子品, stock_transfer 清零
# end_stock 后 bom_out 已等于 receive_amt, 再加 transfer 会重复记账

# 修改后:
# v0.10 fix: end_stock 清零，同时增加 bom_out 以保持利润方程平衡
# stock_transfer 的本质是父品将残留库存分给子品 = 额外的 BOM 流出
df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
       (df['store_id'] == pr['store_id']) &
       (df['business_date'] == pr['business_date']),
       'bom_out_qty'] += transfer_qty       # ← 新增
df.loc[parent_mask & (df['article_id'] == pr['article_id']) &
       (df['store_id'] == pr['store_id']) &
       (df['business_date'] == pr['business_date']),
       'bom_out_amt'] += transfer_amt       # ← 新增
```

### 6.3 为什么不会双计

子品的 stock_transfer_in 已经在子品利润中通过 `(+end_stock) + (-bom_in)` 互相抵消。父品增加 bom_out 只影响父品利润，不会传到子品:

```
父品利润 (修改后):
  profit = sale - receive + (bom_out原 + transfer_amt) + 0(end) - init
         = 0 - receive + bom_out原 + transfer_amt - init
         = (transfer_amt 抵消了 end_stock 清零的损失)
         ≈ 0 (估值变化可能有微小残差)

子品利润 (不变):
  子品的 bom_in 和 end_stock 已在 transfer 代码中同步增加
  profit 中 -bom_in + end_stock 两项互相抵消
  父品 bom_out 增加不影响子品 (bom_out 是父品的流出，子品的流入是 bom_in)
```

### 6.4 验证计算

以 21037825 5/29 为例:

```
修改前:
  bom_out = 228.00
  end_stock = 0 (after transfer)
  profit = 0 - 228 + 228 + 0 - 576.62 = -576.62 ✗

修改后:
  bom_out = 228.00 + 724.39 = 952.39
  end_stock = 0 (after transfer)
  profit = 0 - 228 + 952.39 + 0 - 576.62 = 147.77

  147.77 = (724.39 - 576.62) = end_stock_before - init_stock
         = 估值收益 (euc 从 13.729 → 17.247)
         ≈ 0 (真实业务中 euc 不会这么剧烈变化)
```

### 6.5 副作用: 子品 bom_in 需要同步

当前 transfer 代码已经增加了子品的 `bom_in_amt/qty`。如果父品 `bom_out` 也增加，子品 `bom_in` 不需要额外变化——它已经能抵消子品 `end_stock` 的增加。

但是，从**数据一致性**角度看，理想情况下父品 `bom_out` 的增加应该和子品 `bom_in` 的增加来源一致。当前子品 `bom_in` 的增加在 transfer 代码中直接操作，而不是通过 bom_alloc 表。这在逻辑上是可以接受的，因为 transfer 本就不是 BOM 标准分配。

### 6.6 预估修复效果

| 指标 | 修改前 | 修改后 |
|------|:---:|:---:|
| 受影响父品总利润 | -1,272.80 | +148.06 |
| FM 总利润变化 | — | +1,420.86 |
| 全系统一致性 | 父品虚亏，子品不变 | 父品≈0，系统平衡 |

---

## 七、替代方案: 在 profit.py 中增加 transfer 项

如果不希望在 stock.py 中修改 bom_out，也可以在 profit.py 的毛利公式中添加 transfer 项:

```python
# profit.py:94-100 修改后
df['profit_amt'] = (
    df['sale_amt']
    - df['receive_amt']
    - df['bom_in_amt'] + df['bom_out_amt']
    - df['compose_in_amt'] + df['compose_out_amt']
    + df['end_stock_amt'] - df['init_stock_amt']
    + df['stock_transfer_out_amt'] - df['stock_transfer_in_amt']  # ← 新增
)
```

**权衡**:

| 方案 | 优点 | 缺点 |
|------|------|------|
| **A: stock.py 增加 bom_out** | 改动小，语义清晰 (transfer=BOM流出) | bom_out 字段含义变宽 |
| **B: profit.py 增加 transfer** | 公式完整，每个项目独立 | 需要确保 stock_transfer_in/out 在所有路径都正确填充 |

**推荐方案A** (stock.py 增加 bom_out)，理由:
- 改动范围最小 (2行)
- transfer 确实可视为 "延迟的 BOM 流出"
- 不需要改动 profit.py 的通用公式
- 已在 v0.10 架构中预留了这个扩展 (原注释提到"不重复增加"，改为"增加"即可)

---

## 八、验证清单

- [ ] 修改 stock.py: 父品 transfer 时增加 bom_out_amt/qty
- [ ] 本地全月重刷: `python -m fmetl.executor 2026-05-01 2026-05-31`
- [ ] 验证 21037825 5/29 利润从 -576.62 → ≈0
- [ ] 验证所有 transfer 父品 (11个) 利润 ≈0
- [ ] 验证子品利润未因父品 bom_out 变化而改变
- [ ] 验证 stock equation balance 未恶化
- [ ] QDM 对比确认总利润变化方向正确
- [ ] commit + push

---

## 九、相关文档

- [FIX-003 init_stock一致性](FIX-003-init-stock-consistency.md) — 同文件 init_stock 查找差异
- stock.py:387-451 — stock_transfer 完整逻辑
- profit.py:94-100 — 毛利公式
- 审查报告 §2.3 — 原始问题描述
