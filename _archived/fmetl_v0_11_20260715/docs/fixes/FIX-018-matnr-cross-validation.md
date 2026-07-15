# FIX-018: matnr EUC 交叉验证层

> **日期**: 2026-06-23 | **影响模块**: sku_cost.py | **关联**: [REVIEW-003](../reviews/REVIEW-003-matnr-deep-dive.md)

## 问题

同 matnr（物料号）的多个 SKU 在 ETL 中独立计算 EUC，没有任何机制检测 EUC 比率是否与重量比率一致。

典型异常：蒙牛鲜牛奶 12包/箱 (euc=16.30) vs 6包/袋 (euc=30.89)。
按 unit_weight 比 (2.16/1.08=0.5)，6包袋的 EUC 应为 8.15，实际 30.89，偏差 279%。

## 实现

在 `SkuCostCalculator.run()` 末尾（所有 fallback 应用后、数据写出前）新增 `_cross_validate_matnr_euc()` 方法。

- **只读**: 不修改任何 EUC 值，仅输出 WARNING
- **触发**: 同 matnr 有 ≥2 个 SKU 的 euc>0，euc_ratio 与 wt_ratio/zgl_ratio 偏差 >20%
- **去重**: 同一 matnr 对跨天汇总为一条 WARNING
- **标记**: BOM 重叠的配对加 `[BOM重叠!]` 标记

### 比率计算

```
euc_ratio = SKU_B.euc / SKU_A.euc
expected_ratio: unit_weight比(优先) 或 zglfz/zglfm比(回退)
deviation = |euc_ratio - expected_ratio| / expected_ratio
```

## 验证

5 天数据 (A3XV, 6/18-22):

| 检测结果 | 说明 |
|------|------|
| matnr 124752 蒙牛鲜牛奶 (279%偏差) | ✅ 检测到 — BOM重叠, zglfz/zglfm写反已知bug |
| matnr 110561 蒙牛每日鲜语3.6 | ✅ 已修复 — FIX-017 附带修正，偏差从41.5%→0% |
| 其他 matnr 对 | ✅ 正常 — 无不一致 |

## 文件变更

`fmetl/calculated/sku_cost.py`:
- `run()`: 新增 `_cross_validate_matnr_euc(df, conn)` 调用
- 新增 `_cross_validate_matnr_euc()` 方法 (~90行)
