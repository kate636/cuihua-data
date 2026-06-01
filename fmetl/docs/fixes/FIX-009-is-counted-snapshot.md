# FIX-009: is_counted 系统快照导致大量虚假库存核销

> 审查报告 §3.5 | [修复索引 →](README.md)
>
> 状态: ✅ 已修复
>
> 修改文件: `fmetl/calculated/stock.py`

---

## 一、问题

审计报告 §3.5 发现标品类（纸巾/瓶装水/大米/酒）在特定日期出现大额库存核销。经分析发现根因是 `is_counted` 判断条件过宽，将系统快照（`created_by='系统'`）也当作盘点处理。

## 二、根因

### 2.1 旧代码（第 236 行）

```python
df['is_counted'] = (df['created_by'] != '系统') | 
                   ((df['actual_stock_qty'] > 0) & (df['created_by'] == '系统'))
```

两个条件都会触发 `is_counted`：
- `created_by != '系统'` → 真正的人工盘点 ✅
- `actual_stock_qty > 0 AND created_by = '系统'` → 系统快照 ⚠️

### 2.2 系统快照数据量

```
created_by     rows    SKUs    days
系统           42,107  1,832   31     ← 每天 ~1,400 SKU
人工(162031等)  2,157    671   21     ← 仅 21/31 天, 每天 ~100 SKU
```

系统快照**每天**覆盖 ~1,400 SKU，但其 `actual_stock_qty` 可能与账面不一致（数据来源/时点差异）。

### 2.3 is_counted 的影响

当 `is_counted = True` 时：
```python
end = actual_stock_qty      # 强制覆盖为系统值
unknow = eq - actual_stock   # 差额 = 损耗(正) 或 盘盈(负)
```

系统快照显示的实际库存常低于账面（可能是数据口径差异），差额被记为 `unknow_lost` → 虚增亏损。

### 2.4 5/16 案例

```
5/16 unknow_lost 来源:
  系统快照 (created_by='系统'):  +4,854  ← 1,395 SKU 被错误触发!
  人工盘点 (created_by!='系统'):   -347  ← 93 SKU, 正常行为
  无盘点数据:                       -66  ← 999 SKU, 正常行为

典型案例:
  洁柔纸巾: init=238, actual=82 → is_counted → unknow=153 units = -1,224 元
  农夫山泉: init=196, actual=143 → is_counted → unknow=45 units = -453 元
```

**5/16 的 4,854 虚假核销占当天 unknow_lost 的 99%。**

## 三、修复

### 3.1 代码改动

```python
# 旧
df['is_counted'] = (df['created_by'] != '系统') | ((df['actual_stock_qty'] > 0) & (df['created_by'] == '系统'))

# 新: 只有人工盘点才触发 is_counted
df['is_counted'] = (df['created_by'] != '系统')
```

### 3.2 为什么保留分支 5（盘盈）

stock.py 第 349 行有独立的盘盈检测：
```python
elif act_qty > 0 and act_qty > eq + 0.001:
    # 系统记录的实际库存超过账面 → 盘盈
    end = act_qty
    unknow = eq - act_qty  # 负值
```

这个分支**不依赖 is_counted**，对所有来源（包括系统快照）生效。所以：

| 场景 | 旧行为 | 新行为 |
|------|--------|--------|
| 人工盘点 | is_counted → end=actual | is_counted → end=actual ✅ |
| 系统快照, actual < book | is_counted → **虚假盘亏** | 默认分支 → end=eq ✅ |
| 系统快照, actual > book | is_counted → 盘盈 | 分支5 → 盘盈 ✅ |

**盘盈保留，虚假盘亏去除。**

## 四、影响评估

| 指标 | 修复前 | 修复后 |
|------|:---:|:---:|
| 5/16 unknow_lost (系统快照) | +4,854 | 0 |
| 5/16 洁柔纸巾 profit | -1,221 | ~+19 (正常) |
| 5/16 农夫山泉 profit | -457 | ~+76 (正常) |
| 5/16 全月利润 | 1,044 | ~5,898 |
| 每日虚假核销 | ~1,000-5,000 | 消除 |

## 五、相关文档

- [FIX-008 标品核销分析](FIX-008-inventory-writeoff.md) — 问题发现过程（结论已被本修复更新）
- stock.py:236 — is_counted 判断（已修复）
- stock.py:349 — 盘盈检测分支（保留）
- 审查报告 §3.5 — 原始问题描述
