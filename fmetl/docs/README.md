# fmetl 文档从哪里开始读

## 当前 v0.17

按下面顺序读，不需要先了解旧版本：

| 文档 | 回答的问题 | 适合谁读 |
|---|---|---|
| [fmetl README](../README.md) | 整个系统读取什么、怎样计算、怎样运行 | 第一次了解项目的人 |
| [DESIGN-008](designs/DESIGN-008-v0.17-processing-evidence-and-profit-correction.md) | 商品 A 怎样转成 B、加工原料怎样计算、哪些情况不计算 | 审核业务逻辑的人 |
| [FIX-027](fixes/FIX-027-v017-processing-backflush-and-receipt-bridge.md) | v0.17 具体修了什么，修复后数字怎样变化 | 审查本次改动的人 |
| [v0.17 运行审核](reviews/V0.17_RELEASE_REVIEW.md) | 2026-08-06～11 实际跑出了什么，为什么仍不能发布 | 决定是否继续迭代或发布的人 |
| [v0.17 逻辑图](reports/v0.17-etl-current-logic-map-20260811.html) | 用流程图查看从原始数据到毛利的完整路径 | 需要人工逐节点判断的人 |

当前文字统一遵守：先写条件，再写系统动作和计算结果，最后写表名或状态码。
文字必须直接、准确、简洁、按因果顺序表达，不使用情绪词。`反冲`、`过账`、`门禁`、`隔离`、`守恒`、`成本池` 等内部术语不能单独出现，必须说明实际扣减、增加、停止或检查了什么。

## 技术参考

- [strategy_fm v1.5 字段手册](references/strategy_fm_v1_5_字段手册.md)：字段名、类型和来源表。
- [strategy_fm v1.5 实时列名快照（2026-07-22）](references/strategy_fm_v1_5_实时列名快照_2026-07-22.md)：当日接口实际返回的列名。
- [修复记录索引](fixes/README.md)：每次修复解决的问题、状态和前后依赖。

字段手册必须保留真实表名和字段名，因此英文代码较多；它是查字段用的参考表，不是业务流程
说明。

## 历史文档

以下文档记录当时版本的设计和判断，保留原文是为了能复盘“当时为什么这样做”。其中的
“当前”、数字和处理规则只对文档标题中的版本有效，不能用来解释 v0.17：

- v0.13：[DESIGN-003](designs/DESIGN-003-v0.13-clean-rebuild.md)、
  [DESIGN-004](designs/DESIGN-004-v0.13-multilevel-bom-iteration-plan.md)、
  [架构说明](architecture/V0.13_ARCHITECTURE.md)；
- v0.14：[DESIGN-005](designs/DESIGN-005-v0.14-product-group-etl-plan.md)、
  [已实现架构](architecture/V0.14_IMPLEMENTED_ARCHITECTURE.md)、
  [七日验证](reviews/V0.14_SHADOW_VALIDATION.md)、
  [相比 v0.13 的改动](fixes/V0.14_FROM_V0.13.md)；
- v0.15：[DESIGN-006](designs/DESIGN-006-v0.15-evidence-led-iteration-method.md)；
- v0.16：[DESIGN-007](designs/DESIGN-007-v0.16-invariant-led-correction.md)、
  [发布审核](reviews/V0.16_RELEASE_REVIEW.md)、
  [2026-08-12 全文件检查快照](architecture/PROJECT_FULL_FILE_AUDIT_2026-08-12.md)。

其他历史记录也不代表 v0.17 现状：

- 修复记录：[FIX-021 至 FIX-027 索引](fixes/README.md)；FIX-027 是当前版本，其他条目仅说明对应旧版本的改动。
- 运行和对比记录：[v0.14 周报](reports/v0.14-etl-weekly-report.md)、
  [v0.16 与 v1.5 单日分类毛利对比](reports/v0.16-v1.5-daily-store-category-profit-comparison-20260806-20260811.md)、
  [v1.5 与 v0.13 周报](reports/v1.5-v0.13-frontline-weekly-report.md)。
- 建造和专项检查记录：[v0.13 建造记录](reviews/V0.13_INCREMENTAL_BUILD_LOG.md)、
  [v0.14 分类与 SPU 单价检查](reviews/V0.14_CATEGORY_SPU_UNIT_PRICE_CHAIN_REVIEW.md)、
  [v0.16 加工与生熟联动检查](reviews/V0.16_PROCESSING_SSLS_CHAIN_REVIEW.md)。

v0.11 及更早版本统一保存在 `_archived/fmetl_v0_11_20260715/`。服务器生产任务目前仍运行
v0.11；v0.17 只是本地试算，尚未切换生产。`_archived/` 中的文字是归档记录，不是当前操作说明。
