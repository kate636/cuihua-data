# 审查报告索引

| 报告 | 日期 | 范围 | 关键发现 |
|------|------|------|---------|
| [REVIEW-001](REVIEW-001-2026-06-18-to-22.md) | 2026-06-23 | 6/18-22 QDM对比 | 销售0差异；利润+20%；BOM父品利润不为0；5大可修正+4个结构性差异 |
| [REVIEW-002](REVIEW-002-matnr-euc-integration.md) | 2026-06-23 | matnr+EUC整合 | 429对活跃matnr；3对BOM重叠；V10_MATNR_CONVERT兜底方案；Phase 1+2已完成 |
| [REVIEW-003](REVIEW-003-matnr-deep-dive.md) | 2026-06-23 | matnr深层审查 | 修正前次错误；receive落在称重品(106/117对)；需merge.py拆分receive |
| [REVIEW-007](REVIEW-007-clamp-cost-leak.md) | 2026-06-24 | 差异矩阵根因下钻 | FIX-019: 负库存钉零透支成本泄漏; 总差+18.9%→+6.3%; REVIEW-006方案(FIX-004)已否决回滚 |
| [REVIEW-006](REVIEW-006-next-fix-target.md) | 2026-06-23 | 差异矩阵下钻 | 下一个修复目标: BOM父品利润归零(后作FIX-004实现→**回滚**); TOP5 SKU差异-872元 |
| [REVIEW-005](REVIEW-005-2026-06-23-second-review.md) | 2026-06-23 | 修复后二次审查 | 毛利+19.0%(↓1pp); 6/19从-18%→+4.2%; 9/13分类在±5%内 |
| [REVIEW-004](REVIEW-004-receive-source-audit.md) | 2026-06-23 | 验收数据源对比 | receive_sale_di vs dal_manage_*; dws_sh_analysis不可达; 277 SKU有当日验收 |
| [全面审查报告 v0.10](全面审查报告_v0.10_2026-06-01.md) | 2026-06-01 | 5月全月 | v0.10 初始审查 |
| [差异问题与待办](差异问题与待办事项_v0.10.md) | — | v0.10 | 已知差异问题跟踪 |

## 审查 SOP

1. 确定对比日期范围和门店
2. QDM SKU级数据 → JOIN dim_goods → v2.3 品类映射
3. FM t_fm_sku_dim → JOIN dim_goods → v2.3 品类映射
4. 门店×大分类 → 按日 → SKU差异 TOP → 深挖根因
5. 分类：可修正（代码/BOM/加工关系）vs 结构性（设计差异）
6. 出报告 + 更新本索引
