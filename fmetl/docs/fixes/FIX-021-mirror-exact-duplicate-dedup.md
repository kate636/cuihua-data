# FIX-021 源数据副本提取层完全重复行去重

> 状态：✅已实现 ｜ Commit：`417182f` ｜ ETL验证：✅07-24～30 本地试算周通过
>
> 编号沿用 v0.11 时代的全局 FIX 序列（v0.11 最后为 FIX-020）；本条是 v0.12 重建树的第一条修复。

## 问题现象

`v014-fetch-mirrors --end 2026-07-30` 在 `compose 2026-07-24` 中断：

```
GrainViolation: strategy_fm_compose_di: duplicate grain
['store_id', 'business_date', 'article_id']
sample=[{'store_id': 'A3XV', 'business_date': '2026-07-24', 'article_id': '20005016'}, ...]
```

07-17~23 本地试算周从未触发该错误。

## 根因

上游 Hive 表 `dsl_transaction_sotre_article_compose_info_di` JOIN 门店维表时，
A3XV 门店名在快照中同时存在「滨江宏岸店」和「广州滨江宏岸店」两个变体，
JOIN 扇出产生双份记录。两行在全部业务字段（数量、金额、日期、SKU）上完全一致，
仅 `store_name` 不同；而 v0.14 源数据副本合同的投影列不含 `store_name`，
投影后成为字节级完全重复行，直接违反 `expected_grain` 唯一性断言。

事实（把握 95%）：已通过 StarRocks 直查确认 2026-07-24 分区 A3XV 的
20005016 / 20005009 各有两行，业务字段全同、仅 store_name 不同。

## 方案

`fmetl/mirror/extract.py::MirrorExtractor.extract_day`：
在投影 reindex 之后、`assert_unique` 之前，对完全重复行 `drop_duplicates`，
并将删除行数记入 `result.attrs["exact_duplicates_dropped"]`。

边界保持严格：同粒度但数值不同的行不是完全重复，去重后仍会触发
`GrainViolation`，不会掩盖真实的粒度冲突。

## 影响

- 适用于全部源数据副本合同（去重发生在通用提取路径），不改任何合同定义。
- 理论风险：若某源表允许「同粒度、数值完全相同的两条真实事件」，去重会低估。
  对 `*_di` 日聚合表该情形不存在；对事件级表（订单粒度）grain 含订单号，不受影响。
- 不影响 07-17~23 已验证结果（该周无重复行）。

## 验证清单

- [x] 新增回归测试：完全重复行去重、数值冲突仍报错（`test_mirror_extractor.py`）
- [x] 全部单测 141 项通过
- [x] 07-24～30 源数据副本拉取通过 compose 及后续源表
- [x] 本地试算周 8 项必须通过的检查通过
