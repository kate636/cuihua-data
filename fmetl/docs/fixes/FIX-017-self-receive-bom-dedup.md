# FIX-006: self_receive BOM 父品收货去重

> **日期**: 2026-06-23 | **影响模块**: merge.py | **对比基准**: `strategy_fm_flag_sku_di`
> **关联**: [REVIEW-004](../reviews/REVIEW-004-receive-source-audit.md)

## 问题

FM ETL 的 `t_fm_sku_dim.inbound_amount` 比 QDM `strategy_fm_flag_sku_di.inbound_amount` 多 302 元（+0.3%）。

### 根因

`merge.py` 的 `_tmp_self_receive` 将 `atomic_receive_sale` 的两条路径做了 UNION ALL + SUM：

- **Path 1**: `article_id = sale_article_id` → 自购收货
- **Path 2**: `article_id ≠ sale_article_id` → BOM 父品收货

当同一个 SKU 在同一天既有自购行又有 BOM 父品行时（如西葫芦 20000110 既是自购又是西葫芦500g的父品），两行被加总 → self_receive 被双倍计算。

QDM 的 `dal_manage_full_link_store_dc_article_info_di` 按 SKU 单行记录，不会出现同一 SKU 两行的情况。

### 影响范围

5 天数据中 6 行受影响（西葫芦 4 天 + 普罗旺斯番茄 2 天），合计 302 元。

## 修复

**策略**: Path 1 (自购) 优先。Path 2 (BOM父品) 仅在 Path 1 为 0 时补充。

纯 BOM 父品（只有 Path 2，如优鲜大白猪A级）保留 Path 2 的收货值，不受影响。

```sql
-- 修复前: SUM(Path1 + Path2) → 双重计数
SUM(self_recv_qty) + SUM(bom_recv_qty)

-- 修复后: Path1 > 0 ? Path1 : Path2
CASE WHEN SUM(self_recv_qty) > 0
     THEN SUM(self_recv_qty)
     ELSE SUM(bom_recv_qty)
END
```

### 验证

修复前后对比 (A3XV, 2026-06-18~22):

| 指标 | 修复前 | 修复后 |
|------|:---:|:---:|
| 总金额差 | +302 元 | +0.06 元 |
| >0.1 元差异行数 | 6 | 0 |

## 文件变更

`fmetl/calculated/merge.py` — `_tmp_self_receive` SQL 重构
