# 验收数据源对比报告

> **审查日期**: 2026-06-23 | **数据日期**: 2026-06-22, 广州滨江宏岸店 (A3XV)
> **目的**: 对比 ETL 当前使用的验收数据源与文档中的验收数据源

## 1. ETL 当前验收数据源

### 源表链路

```
strategy_fm_receive_sale_di  (QDM BOM核心源表, StarRocks)
  ↓ receive_sale_extractor.py
atomic_receive_sale           (DuckDB 原子层, 20列)
  ↓ merge.py: article_id = sale_article_id → SUM(inbound_qty/amt)
t_atomic_wide.self_receive_qty / self_receive_amt
  ↓ sku_cost.py
EUC = (init_stock + self_receive + compose + bom) / qty
```

### 表结构 (20 列)

| 字段 | 说明 |
|------|------|
| `article_id` | 验收 SKU |
| `sale_article_id` | 销售 SKU（≠ article_id 时为 BOM 父品验收、子品销售） |
| `inbound_qty` | 验收数量 |
| `inbound_amount` | 验收金额 |
| `purchase_price` | 采购价 |
| `sale_article_qty` | 销售数量 |
| `sale_article_price` | 销售价 |
| `rate` / `sale_recev_rate` | BOM 拆分比率 |
| `spilit_sale_article_amt` | 拆分后的销售额 |
| `sum_sub_article_qty` / `sum_article_qty` / `sum_sale_article_qty` | 汇总数量 |
| `store_id`, `business_date`, `inc_day` | 维度 |
| `category_level1_id`, `category_level1_description` | 品类 |
| `article_name`, `sale_article_name` | 商品名 |

**数据量**: 361 行 (A3XV, 2026-06-22)。其中 `article_id = sale_article_id`（自购行）占 277 SKU。

## 2. 文档中的验收源表

### 表路径

`dws_sh_analysis.dal_manage_full_link_store_dc_article_info_di`

### 验收相关字段

| 字段 | 业务定义 |
|------|---------|
| `inbound_amount` | 门店实际验收金额（出库到店金额 − 门店调拨金额 − 门店退货金额） |
| `inbound_qty` | 门店实际验收数量 |
| `inbound_weight` | 门店实际验收重量(KG) |
| `pre_inbound_amount` | 理论进货额（按出库原价，不做折让） |
| `avg_purchase_price` | 平均进货价 |

## 3. 关键差异

### 3.1 数据库可达性

| 表 | 数据库 | API 可访问 |
|---|---|---|
| `strategy_fm_receive_sale_di` | `default_catalog.ads_business_analysis` | ✅ |
| `dal_manage_full_link_store_dc_article_info_di` | `dws_sh_analysis` | **❌ 不可达** |

`dws_sh_analysis` 数据库不在当前 API 账号的权限范围内。

### 3.2 字段差异

| 字段 | receive_sale_di | 文档 dal_manage_* |
|------|:---:|:---:|
| `inbound_qty` (验收数量) | ✅ | ✅ |
| `inbound_amount` (验收金额) | ✅ | ✅ |
| `inbound_weight` (验收重量) | **❌** | ✅ |
| `pre_inbound_amount` (理论进货额) | **❌** | ✅ |
| `avg_purchase_price` (平均进货价) | ❌ (仅 `purchase_price`) | ✅ |
| BOM 拆分字段 | ✅ (`article_id`≠`sale_article_id`) | ❌ |

### 3.3 表的语义差异

- **`strategy_fm_receive_sale_di`**: BOM 专用表。核心是 `article_id` → `sale_article_id` 的验收→销售映射关系。同时覆盖自购行和 BOM 父品验收行。
- **`dal_manage_full_link_store_dc_article_info_di`**: 全链路表。门店+仓+商品维度的完整验收数据，不区分 BOM 关系。

**ETL 源表可能只包含 BOM 相关的验收数据子集**——那些与 BOM 拆分链条相关的商品。非 BOM 商品的验收数据可能不在这张表中。

### 3.4 数据量级对比

| 数据源 | SKU 数 | 金额 (A3XV, 一天) |
|------|:---:|:---:|
| `receive_sale_di` self_receive | 277 | 12,360 元 |
| `purchase_di` init_stock | 2,816 | 181,992 元 |
| 重叠 | 277 | — |
| 仅 purchase_di 有 | 2,539 | 169,632 元 |

**全部 277 个有 self_receive 的 SKU 在 purchase_di 中都有 init_stock。**
但 purchase_di 多出 2,539 个 SKU——这些 SKU 只有期初库存、没有当日验收。

## 4. 风险分析

### 4.1 当前链路是否遗漏非 BOM 验收数据？

如果 `strategy_fm_receive_sale_di` 只覆盖 BOM 链路相关的验收，那么**非 BOM 商��的当日验收数据可能被遗漏**。

影响：
- 标品（日杂、水饮、休闲食品等）的验收数据如果在 `receive_sale_di` 中不完整 → self_receive = 0
- 这些 SKU 的 EUC 完全依赖 init_stock（跨日继承）或兜底链
- 如果是首日（is_first_day=1）且 receive 数据缺失 → EUC=0 → 利润计算异常

### 4.2 缺失字段的影响

- **`inbound_weight`**: 无法交叉验证 unit_weight 口径的进货重量
- **`pre_inbound_amount`** (理论进货额): 无法区分"实收金额"和"标准金额"的差异——这对应 QDM 中的验收折让处理

### 4.3 purchase_di 已经提供 `avg_inbound_price`

ETL 在 `t_calc_sku_cost` 中使用了 `avg_inbound_price` 作为 EUC 兜底（V10_AVG_INBOUND_FALLBACK）。这个字段来自 `strategy_fm_purchase_di`，覆盖了 2,816 SKU。

## 5. 建议

| 优先级 | 行动 | 说明 |
|:---:|------|------|
| **P0** | 确认 `strategy_fm_receive_sale_di` 是否包含全部验收数据 | 请 DBA 或数据团队确认：非 BOM 商品的验收数据是否也在这张表中 |
| **P0** | 争取 `dws_sh_analysis` 数据库的只读权限 | 解锁全链路表的直接查询 |
| **P1** | 如 receive_sale_di 不完整：补充 `strategy_fm_purchase_di` 的 inbound 字段 | purchase_di 已有 init_stock，可能可以扩展 |
| **P2** | 对比 receive_sale_di 和 dal_manage_* 同一天的验收金额 | 确认数据量级差异（需 DBA 协助） |

## 6. 附录：ETL 中验收数据的三个作用

```
验收单 (strategy_fm_receive_sale_di)
  │
  ├─ ① 自购验收 (self_receive)
  │   article_id = sale_article_id
  │   → t_atomic_wide.self_receive_qty/amt
  │   → EUC 加权平均的主数据源 (cost_amt/qty)
  │
  ├─ ② BOM 父品验收 (bom_parent_receive)
  │   article_id != sale_article_id
  │   → BomAllocCalculator 的 receive_amt 输入
  │   → BOM 分摊给子品
  │
  └─ ③ 首日期初库存 (via purchase_di)
      strategy_fm_purchase_di → atomic_inventory
      → t_atomic_wide.init_stock_qty_src/amt_src
      → is_first_day=1 时的 init_stock 来源
```

---

*关联: [REVIEW-003](REVIEW-003-matnr-deep-dive.md) — matnr 深层审查, [字段手册](../references/strategy_fm_字段手册_完整版.md)*
