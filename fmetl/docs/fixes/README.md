# 修复记录索引（v0.12～v0.16 重建树）

> v0.11 及更早的 FIX-001 ~ FIX-020 见旧树 git 历史（`44bc75e` 之前）。
> 编号全局递增，不重置。

| 编号 | 标题 | 状态 | 影响模块 |
|---|---|---|---|
| [FIX-021](FIX-021-mirror-exact-duplicate-dedup.md) | 镜像提取层完全重复行去重 | ✅已实现 / ✅07-24～30 影子运行通过 | `mirror/extract.py` |
| [FIX-022](FIX-022-explicit-operator-count-gate.md) | 人工盘点账号证据门禁 | ✅已实现 / ✅07-27～08-04 影子运行通过 | `facts/sku_day.py`、库存日账 |
| [FIX-023](FIX-023-explicit-window-and-safe-rates.md) | 连续区间重刷与聚合比率安全除法 | ✅已实现 / ✅07-27～08-04 影子运行通过 | `cli.py`、`validation/run_v014.py`、`outputs/levels_result.py` |
| [FIX-024](FIX-024-v015-evidence-led-release-gates.md) | 证据驱动发布门禁与同口径对比 | ✅代码与测试通过 / ⛔零成本门禁未通过 | 输出层、验证层、v1.5 对比层 |
| [FIX-025](FIX-025-v016-category-lineage-and-comparison.md) | 分类血缘、双视图对比和发布门禁 | ✅代码与定向测试通过 / ⛔历史生效方式待确认 | 主数据、运行清单、v1.5 对比层 |

## 版本迭代说明

- [v0.14 相比 v0.13 的实际迭代](V0.14_FROM_V0.13.md)：按当前代码说明新增能力、核心口径变化、已通过校验和发布阻塞项。

## 依赖关系

FIX-021 与 FIX-022 独立；FIX-023 在 FIX-022 盘点口径上完成连续区间重刷；FIX-024
使用两日固定样本审核层级展示、成本证据和对比口径；FIX-025 在分类来源未闭合时
保留诊断能力并阻止发布。
