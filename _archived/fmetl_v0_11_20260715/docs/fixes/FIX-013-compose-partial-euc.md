# FIX-013: compose_in_amt 部分原料 euc=0 时不应整体归零

| 属性 | 值 |
|------|-----|
| **日期** | 2026-06-04 |
| **状态** | ✅ 已实现 |
| **修改文件** | `fmetl/calculated/sku_cost.py` |
| **Commit** | `7dab133` |
| **优先级** | 🔴 P0 |
| **ETL验证** | ✅ 全月 (2026-05-01 ~ 2026-06-03) |

---

## 问题描述

葡式蛋挞6个(C) (21282423) 有 2 个原料配方：

| 原料 | 配方 | 原料 base_euc |
|------|------|:---:|
| 好禧坊葡式蛋挞液 (21326066) | 1KG → 3.33盒 | ~15.47 |
| 仿手工葡挞皮 (21340840) | 1袋 → 5盒 | **0** (无库存) |

因为葡挞皮没有期初库存和进货，`base_euc=0`，导致 **整个 compose_in_amt=0**，蛋挞液的加工成本（~¥4.65/盒）也被丢弃。

**影响范围**: 蛋挞 34 天中 27 天 compose_in_amt=0，成品 EUC 暴跌至 ¥0.19~0.58。

---

## 根因

`sku_cost.py:476` 的判断条件：

```python
if finished_unit_cost > 0 and all_raw_found:  # ← all_raw_found 是问题
```

`finished_unit_cost` 本身只累加了 `raw_euc > 0` 的原料（474行的判断），但 `all_raw_found` 要求**所有**原料的 euc 都能查到。一个原料缺 euc → `all_raw_found=False` → 整个 compose_in_amt 归零。

```python
# 474行: 只有 raw_euc > 0 才累加 (正确)
if raw_euc > 0:
    finished_unit_cost += (raw_qty / yield_qty) * raw_euc

# 470-471行: 查不到 raw_euc → all_raw_found=False (问题)
else:
    all_raw_found = False

# 476行: all_raw_found=False → 整个跳过 (问题)
if finished_unit_cost > 0 and all_raw_found:
```

---

## 修复

移除 `all_raw_found` 要求。`finished_unit_cost` 已正确排除了 euc=0 的原料，不需要额外的全局开关：

```python
# 修复前
if finished_unit_cost > 0 and all_raw_found:

# 修复后
if finished_unit_cost > 0:
```

**逻辑保证**：
- 原料 euc=0 → 474行跳过，不累加 → `finished_unit_cost` 只包含有成本的原料
- 原料 euc>0 → 正常按配方比例累加
- `finished_unit_cost > 0` 意味着至少一个原料有有效成本，应该使用

---

## 验证数据

### 修复前

| 指标 | 数值 |
|------|------|
| 总产出 | 340盒 (33天) |
| 总加工成本 | **¥335.46** (仅3天有值) |
| 有成本天数 | 3/33 (9%) |
| EUC 范围 | ¥0.19 ~ 7.49 (剧烈波动) |

### 修复后

| 指标 | 数值 |
|------|------|
| 总产出 | 344盒 (33天) |
| 总加工成本 | **¥1,563.40** (30天有值) |
| 有成本天数 | 30/33 (91%) |
| EUC 范围 | ¥4.20 ~ 7.49 (稳定) |

### EUC 验证

纯蛋挞液成本 = `1/3.33 × 15.47` = **¥4.65**  
加葡挞皮成本 = `4.65 + 1/5 × 14.21` = **¥7.49**

修复后 EUC 分布于这两个值之间，与配方推算一致。

---

## 修改文件

| 文件 | 改动 |
|------|------|
| `fmetl/calculated/sku_cost.py:476` | `if finished_unit_cost > 0 and all_raw_found:` → `if finished_unit_cost > 0:` |
| `fmetl/calculated/sku_cost.py:422-425` | 注释更新：去掉"原料 euc 不全 → 保持 0"的说法 |

---

## 依赖关系

```
FIX-001 (compose 纯加工关系) ──→ FIX-013 (部分原料 euc=0 不归零)
                                        │
                                        └──→ FIX-002 (EUC 兜底链，依赖 compose 金额正确)
```

FIX-013 是 FIX-001 的补充修复。FIX-001 确保了 compose 金额从配方推算，FIX-013 确保了配方推算在有原料缺 euc 时不会整体失败。
