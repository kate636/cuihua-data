# fmetl 文档入口

## 当前 v0.18

按下面顺序读：

| 文档 | 回答的问题 | 适合谁读 |
|---|---|---|
| [fmetl README](../README.md) | 系统读取什么、怎样计算、当前为什么不能发布 | 第一次了解项目的人 |
| [DESIGN-009](designs/DESIGN-009-v0.18-evidence-led-inventory-conversion-cost-profit.md) | 库存、BOM、加工、换码、熟食联动、成本和毛利的完整公式 | 审核业务和计算逻辑的人 |
| [v0.11/v0.17/v0.18 逐项审计](reviews/V0.11_V0.17_V0.18_LOGIC_AUDIT.md) | 三个版本每个主要逻辑节点有什么不同 | 判断改动是否合理的人 |
| [v0.18 本地运行审核](reviews/V0.18_RELEASE_REVIEW.md) | 2026-08-05～11 实际跑出什么、哪些条件仍阻止发布 | 决定是否继续迭代或发布的人 |
| [FIX-028](fixes/FIX-028-v018-evidence-led-ledger-and-release-audit.md) | 本次代码修改、删除内容和验证结果 | 复查实现范围的人 |
| [v0.18 审计工作簿](../../outputs/019ff4d6-fcf2-7a43-879a-b856f3c9a53d/fm_v018_audit_20260805_20260811.xlsx) | 逐日、逐分类、逐 SKU、关系、成本和发布问题明细 | 人工判断计算合理性的人 |
| [修复记录](fixes/README.md) | 每一版解决了什么、还有什么没有解决 | 复查代码变更的人 |

当前 v0.18 只在 A3XV 本地试算。服务器生产任务仍运行 v0.11，没有切换。`v014_*` 是兼容
物理表名，不是引擎版本。

当前文字统一按“数据来源或条件 → 计算动作或公式 → 结果和影响”书写。说明必须直白、精准、
简洁、按因果顺序、无情绪。内部状态码和字段名保留原值；`反冲`、`过账`、`门禁`、`隔离`、
`守恒`、`成本池` 不能单独作为解释，必须写明扣了什么、加了什么、为什么停止以及影响什么。

面向人的说明使用“熟食联动”。源表原值 `reason='生熟联动'` 和技术码 `SSLS` 不改，避免改变
已有接口和审计结果。

## 技术参考

- [strategy_fm v1.5 字段手册](references/strategy_fm_v1_5_字段手册.md)：字段名、类型和来源表。
- [strategy_fm v1.5 实时列名快照](references/strategy_fm_v1_5_实时列名快照_2026-07-22.md)：接口实际返回列名。
- [2026-08-12 全文件检查](architecture/PROJECT_FULL_FILE_AUDIT_2026-08-12.md)：v0.16 历史工作区快照，不代表当前代码。

字段手册保留真实表名和字段名，因此英文代码较多。它用于查字段，不代替当前业务设计。

## 历史版本

以下文档只解释标题中的历史版本：

- v0.13：[DESIGN-003](designs/DESIGN-003-v0.13-clean-rebuild.md)、
  [DESIGN-004](designs/DESIGN-004-v0.13-multilevel-bom-iteration-plan.md)、
  [架构说明](architecture/V0.13_ARCHITECTURE.md)。
- v0.14：[DESIGN-005](designs/DESIGN-005-v0.14-product-group-etl-plan.md)、
  [已实现架构](architecture/V0.14_IMPLEMENTED_ARCHITECTURE.md)、
  [七日验证](reviews/V0.14_SHADOW_VALIDATION.md)。
- v0.15：[DESIGN-006](designs/DESIGN-006-v0.15-evidence-led-iteration-method.md)。
- v0.16：[DESIGN-007](designs/DESIGN-007-v0.16-invariant-led-correction.md)、
  [发布审核](reviews/V0.16_RELEASE_REVIEW.md)。
- v0.17：[DESIGN-008](designs/DESIGN-008-v0.17-processing-evidence-and-profit-correction.md)、
  [FIX-027](fixes/FIX-027-v017-processing-backflush-and-receipt-bridge.md)、
  [发布审核](reviews/V0.17_RELEASE_REVIEW.md)。

v0.11 及更早版本保存在 `_archived/fmetl_v0_11_20260715/`。历史文档中的“当前”、数字和公式
只对该版本有效，不能覆盖 v0.18 的 README、DESIGN-009 和本地运行审核。
