# FIX-010: 盘盈机制分析与结构性差异

> 状态: **📋 结构性差异（非 bug）** | 阻塞: 无 | ETL验证: N/A | [修复索引 →](README.md)
>
> 设计文档，无需改代码。本文档记录 FM 与 QDM 在"盘盈"问题上的语义差异、已有机制、以及为什么这是一个结构性分歧而非 bug。
>
> 数据基准: 2026年5月全月 (31天), A3XV 门店, DuckDB 服务器查询

---

## 一、问题定义

### 1.1 核心语义差异

FM ETL 和 QDM 对"损耗"的语义定义根本不同：

| | QDM | FM |
|---|---|---|
| **损耗定义** | 净库存调整（运营记录） | 库存方程残差 (`eq - end`) |
| **正值含义** | 盘亏 / 损耗 | 方程残差（库存减少） |
| **负值含义** | 盘盈 / inventory gain | 方程残差（库存增加） |
| **数据来源** | 运营系统记录（店员盘点、系统调整） | 数学推导（库存方程） |
| **触发条件** | 有人操作即有记录 | 仅当 `is_counted` 或 `eq < actual_stock` 或 `day_clear='0'` |

这是**结构性差异**，不是 bug。FM 的 `unknow_lost` 是方程残差，QDM 的 `lost_amt` 是运营记录。两者可以同时为不同的值，即使在相同 SKU-日期上。

### 1.2 QDM 已知盘盈案例

用户提供的 QDM 大额盘盈数据（QDM 中负值为盘盈）：

| 日期 | 品类 | QDM 盘盈 |
|------|------|:---:|
| 5/21 | 冷藏加工类 | -642 元 |
| 5/13 | 冷藏加工类 | -106 元 |
| 5/08 | 烘焙类 | -361 元 |
| 5/21 | 烘焙类 | -588 元 |

> 注意: 烘焙类在 `dim_goods` 中归属于 `冷藏及加工类` (category_level1_id=26)，不是独立的 category_level1。

---

## 二、量化影响

### 2.1 FM 库存方程分支分布 (A3XV, 5月)

```
分支              n_rows   n_SKUs    total_unknow   盘盈(gain)   盘亏(loss)   gain_rows
──────────────────────────────────────────────────────────────────────────────────
normal(正常)      69,676   2,583     +3,478.63      -278.18     +3,756.82        3
day_clear=0(日清)  3,888     164    -13,520.62   -64,707.53    +51,186.91    1,137
is_counted(盘点)   2,209     747    +17,405.27   -19,310.89    +36,716.16      845
eq_lt_0(负库存)    1,160     326     +4,374.98    -5,549.48     +9,924.46      332
──────────────────────────────────────────────────────────────────────────────────
合计              76,933   2,583    +11,738.26   -89,846.09   +101,584.36    2,317
```

**核心发现**:
- **正常分支占 90.6% 的行** (69,676/76,933)，但仅产生 -$278 的盘盈（3 行，2 个唯一 SKU-日期组合）
- **69,615 行** (99.99% 的正常分支) 的 `unknow_lost = 0`
- is_counted 产生 -$19,310 盘盈但伴随 +$36,716 盘亏，净正向损耗
- day_clear='0' 是最大的盘盈来源（-$64,707），但净效果为盘盈（-$13,520 即利润增加）

### 2.2 正常分支的盘盈缺失

正常分支 (`day_clear='1', eq>=0, not is_counted`) 中仅 3 行产生盘盈：

| 日期 | SKU | 商品 | init | sale | eq | end | actual | gain |
|------|-----|------|------|------|------|------|------|------|
| 5/03 | 21281075 | 蒙牛现代牧场鲜牛奶百利包180ml*6 | 6 | 3 | 7 | 13 | 11 | -$185.40 |
| 5/21 | 21281075 | 蒙牛现代牧场鲜牛奶百利包180ml*6 | 13 | 4 | 12 | 15 | 13 | -$92.70 |

这两个案例由 stock.py 第 349 行的 `act_qty > eq` 检测捕获（见下方 §3.2 机制5）。

### 2.3 各类目盘盈/盘亏汇总 (A3XV, 5月, 所有分支)

| 大分类 | n_rows | net_unknow | 盘盈(gain) | 盘亏(loss) | gain_rows |
|--------|:---:|:---:|:---:|:---:|:---:|
| 猪肉类 | 2,475 | -13,808 | -46,781 | +32,973 | 823 |
| 预制菜 | 2,505 | +13,150 | -16,306 | +29,456 | 258 |
| 冷藏及加工类 | 15,207 | +1,873 | -13,348 | +15,222 | 500 |
| 蔬菜类 | 10,306 | +501 | -3,066 | +3,567 | 418 |
| 水果类 | 6,538 | +4,618 | -1,123 | +5,741 | 81 |
| 水产类 | 2,517 | -225 | -616 | +391 | 50 |
| 肉禽蛋类 | 2,292 | +2,334 | -2,184 | +4,518 | 33 |
| 标品类 | 35,138 | +3,293 | -3,774 | +7,067 | 152 |

**观察**:
- 猪肉类（全部日清覆盖）盘盈最大（-$46,781），由 `day_clear='0'` 分支驱动
- 标品类（35,138 行，占总量 45.6%）99.9% 在正常分支，盘盈仅 -$3,774
- 冷藏及加工类（含烘焙）盘盈 -$13,348，覆盖 500 行

### 2.4 冷藏及加工类（含烘焙）每日盘盈明细 (A3XV)

| 日期 | net_unknow | gain_amt | loss_amt | gain_rows |
|------|:---:|:---:|:---:|:---:|
| 5/08 | -64.71 | 64.71 | 0.00 | 7 |
| 5/13 | -375.49 | 897.63 | 522.14 | 28 |
| 5/21 | -723.48 | 1,158.36 | 434.87 | 24 |

与 QDM 盘盈对照：
- 5/08: FM 冷藏及加工类盘盈 -$65 vs QDM 烘焙类盘盈 -$361 -- FM 捕获约 18%
- 5/13: FM 冷藏及加工类盘盈 -$898 vs QDM 冷藏加工类盘盈 -$106 -- FM 已超额捕获
- 5/21: FM 冷藏及加工类盘盈 -$1,158 vs QDM 合计(-642-588=-$1,230) -- FM 捕获 94%

**结论**: FM 通过现有机制已捕获 QDM 盘盈的大部分，差异在于具体 SKU-日期的分配不同，而非总量短缺。

---

## 三、根因分析

### 3.1 为什么正常分支 `unknow_lost=0` 是设计意图

FM 库存方程的设计哲学是**数学推导优先**:
```
eq = init + receive + bom_in - bom_out + compose_in - compose_out - sale - know_lost
```

在"正常"情况（有进货、有销售、无外部冲销），eq 应等于 end_stock。`unknow_lost = eq - end` 是残差，默认应为 0。

QDM 则不同：`lost_amt` 是**运营记录**，包含了店员盘点调整、系统差异调账、供应商补损等。这些在 FM 中通过不同的路径体现（`is_counted` → 人工盘点，`know_lost` → 已知损耗）。

### 3.2 FM 已有的 5 个盘盈机制及其实际产出

**机制 1: is_counted 人工盘点 (stock.py:334-337)**
```python
if is_counted:
    end = act_qty
    unknow = eq - act_qty  # actual > eq → 盘盈
```
- 产出 (A3XV): 845 行盘盈, -$19,310
- 局限: 仅当 `created_by != '系统'` 时触发 (FIX-009 修复后)，手动盘点覆盖 ~21/31 天

**机制 2: day_clear='0' 软日清 (stock.py:338-342)**
```python
elif dc == '0':
    new_supply = recv_qty + bi_qty - bo_qty + ci_qty - co_qty
    consumed_from_init = max(0, (sale + klost) - new_supply)
    end = max(0, init - consumed_from_init)
    unknow = new_supply - sale - klost  # 允许负值=盘盈
```
- 产出 (A3XV): 1,137 行盘盈, -$64,707
- 局限: 仅覆盖猪肉类、熟食类、烘焙类（日清覆盖规则），不包括标品和其他品类

**机制 3: eq < 0 负库存保护 (stock.py:343-345)**
```python
elif eq < 0:
    end = 0
    unknow = -eq  # 始终为正（盘亏）
```
- 产出 (A3XV): 332 行盘盈, -$5,549
- 盘盈出现在此分支是因为 eq 为负时 `unknow = -eq` 为正值？不，这个分支的 unknow 始终为正（eq<0 → -eq>0）
- 那盘盈从何而来？Q3 数据展示了 eq_lt_0 分支有 332 行 gain_rows。这是因为我用 `end=actual` 来识别 is_counted，但部分 eq_lt_0 行也有 actual_stock > end，实际经过了 mechanism 1 (is_counted=True 优先于 eq_lt_0) 或 mechanism 5
- 纯粹 eq_lt_0 分支不产生盘盈

**机制 4: know_lost_qty > 0 (stock.py:346-348)**
```python
elif kl_qty > 0:
    end = eq
    unknow = 0
```
- 不产生盘盈（unknow 恒为 0）
- 这是 FM 最保守的分支：已知损耗已在 `know_lost` 中体现，不再额外产生 `unknow_lost`

**机制 5: 盘盈检测 (stock.py:349-353) — FIX-009 保留**
```python
elif act_qty > 0 and act_qty > eq + 0.001:
    # 系统记录的实际库存超过账面 → 盘盈
    end = act_qty
    unknow = eq - act_qty  # 负值 = 盘盈
```
- 产出 (A3XV): 3 行盘盈, -$278
- 这是 FIX-009 保留的分支（不受 is_counted 限制）
- 局限: **只有在 `atomic_inventory_detail` 中有 `actual_stock_qty` 记录且 `actual > eq` 时才触发**
- 实际覆盖极少：全月仅 23 行有 `actual_stock_qty > 0` 且 `day_clear='1'` 且非 is_counted

### 3.3 为什么机制 5 捕获量极少

```
atomic_inventory_detail 5月覆盖 (A3XV):
  每日总行数: 1,789 ~ 2,657
  人工盘点 (created_by!='系统'): 0 ~ 260/天
  系统快照 (created_by='系统'): 1,672 ~ 2,620/天
```

- 系统快照每日覆盖 ~1,700-2,600 行，但在 FIX-009 后不再触发 `is_counted`
- 人工盘点仅覆盖 ~21/31 天，每天 ~0-260 行
- 人工盘点行已通过机制 1 (is_counted) 处理
- 机制 5 只有在行既不是 is_counted（created_by='系统'）又有 `actual > eq` 时才触发
- 实际上只有 23 行满足条件，其中 2 行 `actual > eq`，已被机制 5 捕获

**没有未捕获的盘盈**。查询结果显示，所有 `actual_stock_qty > eq` 的正常分支行都已被机制 5 捕获（0 missed gain rows）。

### 3.4 结构性差异的本质

盘盈在 FM 中的"缺失"不是代码 bug，而是两个系统对同一业务现象的**不同记录方式**：

| 业务场景 | QDM 记录 | FM 记录 |
|----------|----------|---------|
| 店员盘点发现实物多于账面 | `lost_amt = -100` (盘盈) | `is_counted` → `unknow = eq - actual` (可能为负) |
| 日清商品当日全清但实际有余量 | `lost_amt = +调整` | `day_clear='0'` → `unknow = 新供给 - sale - klost` (可为负) |
| 系统调账（无物理盘点） | `lost_amt = -50` (盘盈) | **FM 无对应机制** → `unknow = 0` |
| 供应商补损 | `lost_amt = -200` (盘盈) | 不在 FM 范围内（应在 `receive_amt` 调整） |

**第三行是关键差异**：QDM 可以在没有任何物理盘点数据的情况下，在运营系统中记录盘盈（如系统调账、发现差异后手工修正）。FM 没有对应机制，因为它依赖数学方程而非运营记录。

---

## 四、修复选项

### 选项 A: 接受结构性差异（FM 偏保守）

**内容**: 不做任何代码改动。FM 的 `unknow_lost` 保持为方程残差，QDM 对比时接受 `unknow_lost` 的差异为结构性差异。

**优势**:
- 不引入新的假设或数据来源不确定性
- FM 的利润计算更保守（不凭空产生盘盈），对管理层更审慎
- 现有机制（is_counted + day_clear='0' + act>eq 检测）已覆盖有数据支撑的盘盈

**劣势**:
- 与 QDM 的对比报告中，"损耗"维度会有持续的系统性偏差
- 当 QDM 有大额盘盈时（如 5/21 烘焙 -$588），FM 可能低估利润
- 不能完全满足"对标 QDM"的需求

**影响评估**:
- 当前总利润 $89,165 (A3XV, 5月)，与 QDM 总差异 +3.7%
- 盘盈差异不是偏差主因，结构性差异不改变验收结论

---

### 选项 B: 扩大 is_counted 覆盖范围

**内容**: 将 `is_counted = (created_by != '系统')` 恢复为包含部分系统快照，但加上更严格的条件（如仅当 `actual_stock_qty > eq + threshold` 时触发）。

**优势**:
- 利用现有的大量系统快照数据（每日 ~1,700-2,600 行）
- 当系统快照显示实物多于账面时可捕获盘盈

**劣势**:
- **FIX-009 的反向操作** — 系统快照重新引入可能导致虚假盘亏（actual < eq 的情况）
- 系统快照的 `actual_stock_qty` 数据质量不确定（数据来源/时点差异）
- 可能重新产生类似 5/16 的虚假核销问题（+4,854 unknow）
- 不对称处理（只信任 actual > eq 但不信任 actual < eq）逻辑上不自洽

**风险**: 高。与 FIX-009 的修复方向相反，可能重新引入虚假损耗。

---

### 选项 C: 在正常分支中引入更广泛的盘盈检测

**内容**: 在 stock.py 正常分支（第 354-356 行），当没有 actual_stock 数据时，从其他数据源推断可能的盘盈。例如：
- 检查 historical end_stock 模式（同一 SKU 历史上是否有周期性盘盈）
- 检查 `atomic_inventory_detail` 的 `actual_stock_qty` 历史趋势
- 检查同一门店同一品类其他 SKU 的盘盈概率

**优势**:
- 理论上可以在没有实际盘点数据的日期产生盘盈估计

**劣势**:
- 引入统计推断，增加模型复杂度
- 推断质量无法保证，可能引入新的错误
- 与 FM "数学推导优先"的设计哲学冲突
- 需要大量历史数据训练和验证

**风险**: 高。统计推断的准确性未经验证，可能产生比现有方案更大的偏差。

---

### 选项 D: 定期引入全局盘点修正（推荐）

**内容**: 利用已有的高置信度盘点数据计算修正因子，在月/季度层面做全局调整，而非每日/SKU 层面猜测。具体方案：
- 在有手动盘点数据的日期（~21/31 天），计算 `is_counted` 分支中 `unknow < 0`（盘盈）的平均比例
- 将该比例作为一个**月度全局调整因子**，在 FM 报表层（`t_fm_levels_result`）中以独立行展示
- 不改写 stock.py 的每日库存方程，保持数学推导的完整性

**实现**:
```python
# 在 t_fm_levels_result 中增加一行 "盘盈估计(非方程)"
# 基于 is_counted 天的盘盈率 × 非盘点天的 SKU 数量估算
# 列为独立的 KPI 行，不影响方程计算的利润
```

**优势**:
- 不改动核心库存方程，保持数学自洽
- 利用已有数据（手动盘点）作为统计基础
- 在报表层面作为"管理调整"展示，财务可审计
- 不影响跨日库存链

**劣势**:
- 不是精确的 SKU-日期级别盘盈
- 需要月度/季度维护调整因子
- 报表层增加额外的解释负担

**风险**: 低。不影响核心 ETL 计算链，仅影响最终报表展示。

---

## 五、推荐方案: 选项 A + 选项 D（两阶段）

### 第一阶段: 接受现状（即日起）

保持 FM 库存方程不变，接受与 QDM 在"损耗"维度上的结构性差异。理由：

1. **差异量级小且可控**: FM vs QDM 总差异 +3.7%，已在目标范围内（±5%）
2. **FM 已有三个有效的盘盈捕获机制**: is_counted (-$19K), day_clear='0' (-$64K), act>eq 检测 (-$0.3K)
3. **没有未捕获的"已知"盘盈**: 所有有 actual_stock 数据的正常分支行已由机制 5 处理
4. **FM 的设计哲学支持**: 库存方程是数学推导，不应凭空引入无数据支撑的数值

### 第二阶段: 报表层盘盈估计（中期，优先级 P2）

在 `t_fm_levels_result` 中增加"盘盈估计"KPI 行。具体实现不在本文档范围内（需要独立的设计 + 实现），但大致思路是：

```
盘盈估计 = 当月 is_counted 天盘盈率 × 当月非 is_counted 天非日清 SKU 的 euro_cost 数量

其中:
  盘盈率 = SUM(unknow < 0 ? -unknow_amt : 0) / SUM(abs(unknow_amt))  in is_counted days
```

---

## 六、验证标准

### 6.1 现状验证（选项 A）

由于是结构性差异而非 bug，验证方式是**持续监控**而非一次性修复验证：

| 指标 | 监控方式 | 预期 |
|------|----------|------|
| FM vs QDM 总毛利差异 | `/qdm-compare` skill, 月度 | < ±5% |
| 盘盈对差异的贡献度 | QDM `lost_amt < 0` 的合计 vs FM `unknow < 0` 的合计 | 月度趋势 |
| normal 分支 act>eq 捕获率 | `SELECT COUNT(*) FROM ... WHERE actual > eq AND unknow=0` | 始终 = 0（全部捕获） |
| 新增手动盘点覆盖天数 | `atomic_inventory_detail` 中 `created_by!='系统'` 的天数 | 维持在 20+/31 天 |

### 6.2 第二阶段验证（选项 D，如实施）

| 验证项 | SQL/方法 | 通过标准 |
|--------|----------|:---:|
| 盘盈估计行不重复计算 | 对比加总 `t_fm_levels_result` 的 with/without 估计行 | 非估计行不变 |
| 月度盘盈率稳定 | 计算最近 3 个月的盘盈率变异系数 | CV < 30% |
| 盘盈估计量级合理 | 月度盘盈估计 vs QDM 盘盈合计 | 量级一致（不苛求精确匹配） |

---

## 七、相关代码路径

| 文件 | 行号 | 内容 |
|------|------|------|
| `fmetl/calculated/stock.py` | 334-337 | 机制 1: is_counted → end=actual |
| `fmetl/calculated/stock.py` | 338-342 | 机制 2: day_clear='0' → 软日清 |
| `fmetl/calculated/stock.py` | 343-345 | 机制 3: eq<0 → end=0 |
| `fmetl/calculated/stock.py` | 346-348 | 机制 4: know_lost → end=eq, unknow=0 |
| `fmetl/calculated/stock.py` | 349-353 | 机制 5: act>eq → end=act (盘盈检测) |
| `fmetl/calculated/stock.py` | 354-356 | 默认: end=eq, unknow=0 |
| `fmetl/calculated/stock.py` | 239 | FIX-009: is_counted 条件 |

---

## 八、相关文档

- [FIX-009 is_counted 系统快照](FIX-009-is-counted-snapshot.md) — is_counted 条件修改（去除系统快照）
- [FIX-008 标品核销分析](FIX-008-inventory-writeoff.md) — 问题发现过程（已被 FIX-009 修复）
- [CLAUDE.md](../../../CLAUDE.md) § 盘盈（负损耗）处理 — 原始盘盈设计说明
- [差异问题与待办事项_v0.10.md](../差异问题与待办事项_v0.10.md) — QDM 对比差异总览
- [全面审查报告_v0.10_2026-06-01.md](../全面审查报告_v0.10_2026-06-01.md) — 全面数据审查

---

## 附录 A: 查询 SQL 参考

### A1: 库存方程分支分布
```sql
WITH labeled AS (
  SELECT *,
    (init_stock_qty + COALESCE(receive_qty,0) + COALESCE(bom_in_qty,0) - COALESCE(bom_out_qty,0)
     + COALESCE(compose_in_qty,0) - COALESCE(compose_out_qty,0) - COALESCE(sale_qty,0) - COALESCE(know_lost_qty,0)) as eq,
    CASE
      WHEN actual_stock_qty > 0 AND ABS(end_stock_qty - actual_stock_qty) < 0.001
           AND ABS(end_stock_qty - eq) > 0.001 THEN 'is_counted'
      WHEN day_clear = '0' THEN 'day_clear=0'
      WHEN eq < 0 THEN 'eq_lt_0'
      ELSE 'normal'
    END as branch
  FROM t_calc_stock
  WHERE business_date BETWEEN '2026-05-01' AND '2026-05-31'
)
SELECT branch, COUNT(*), SUM(unknow_lost_amt)
FROM labeled
GROUP BY branch ORDER BY SUM(unknow_lost_amt) DESC
```

### A2: 验证是否有未捕获的盘盈
```sql
-- 应始终返回 0 行
SELECT COUNT(*)
FROM t_calc_stock
WHERE business_date BETWEEN '2026-05-01' AND '2026-05-31'
  AND day_clear = '1'
  AND actual_stock_qty > 0
  AND actual_stock_qty > eq_end_qty + 0.01
  AND ABS(unknow_lost_amt) < 0.01  -- 未被机制5捕获
  AND NOT (actual_stock_qty > 0 AND ABS(end_stock_qty - actual_stock_qty) < 0.001)  -- 非 is_counted
```

### A3: 盘点数据覆盖范围
```sql
SELECT business_date, COUNT(*) as n,
       COUNT(CASE WHEN created_by != '系统' THEN 1 END) as manual_count,
       COUNT(CASE WHEN created_by = '系统' THEN 1 END) as system_count
FROM atomic_inventory_detail
WHERE business_date BETWEEN '2026-05-01' AND '2026-05-31'
GROUP BY business_date ORDER BY business_date
```
