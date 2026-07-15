# FIX-012: L2=即食类 → 熟食类映射规则补充

| 属性 | 值 |
|------|-----|
| **文档** | FIX-012-cooked-instant-remap.md |
| **来源** | 日清品清单产品审查 (2026-06-03) |
| **状态** | ⏳ 待实现 |
| **修改文件** | `fmetl/fm_tables/sku_dim.py`, `fmetl/executor.py`, `category_mapping.py`, `cloud_api.py`（日清+加工关系） |
| **优先级** | 🟡 P1 |

## 问题

当前 master-data v2.3 熟食类映射规则覆盖 4 种情况，但不包括 `L2 = '即食类'` + `L3 = '其他即食类'` 的 SKU：

### 当前规则（4条）
1. `L1 = '熟食类'`
2. `L2 IN ('即烹类', '即热类')`
3. `预制菜` + `sale_unit = '千克'`
4. `L3 LIKE '%熟食'`

### 遗漏场景
`L1 = 预制菜` + `L2 = 即食类` + `L3 = 其他即食类` → 不命中任何规则，归入 **冷藏加工及预制菜类**

## 具体案例

32 个临时录入的 SKU 中，26 个命中 R4（L3 含"熟食"），6 个未命中：

| SKU | 名称 | L1 | L2 | L3 |
|------|------|----|----|-----|
| 21303241 | 瑞士鸡翅(C) | 预制菜 | 即食类 | 其他即食类 |
| 21303289 | 瑞士鸡腿(C) | 预制菜 | 即食类 | 其他即食类 |
| 21303296 | 瑞士鸡翅根(C) | 预制菜 | 即食类 | 其他即食类 |
| 21303302 | 香煎三文鱼骨(C) | 预制菜 | 即食类 | 其他即食类 |
| 21303319 | 香煎白鲳鱼(C) | 预制菜 | 即食类 | 其他即食类 |
| 21303326 | 酥香煎带鱼(C) | 预制菜 | 即食类 | 其他即食类 |

这些都是熟制即食品（烤鸡翅、煎鱼），但从 dim_goods 分类 L3 只看到"其他即食类"，无法与熟食关联。

加上之前加工关系页面中 12 个 `即食类+L3=其他即食类` 的成品（马拉糕、烧麦、虾饺等），共有约 18 个 SKU 受此影响。

## 修复方案

### 新增第 5 条规则
```
L2 = '即食类' → 熟食类
```

加入熟食类中分类映射，即 `cond_instant = (c2 == '即食类')` 也触发熟食类映射。

### 需修改位置

| 文件 | 修改内容 |
|------|---------|
| `fmetl/fm_tables/sku_dim.py` | 新增 `cond_instant = (c2 == '即食类')`，desc 中 `np.where(cond_instant, '熟食类', desc)` |
| `fmetl/executor.py` | `_sync_processing_candidates()` 的 CASE WHEN 新增即食类→熟食类 |
| `category_mapping.py` | `remap_category_level1()` 新增 `if cat2 == '即食类': return '熟食类'` |
| `cloud_api.py`（日清） | `/list` 的熟食类规则 SQL 新增 `g.category_level2_description = '即食类'` |
| `cloud_api.py`（加工关系） | `/list` 的 category_path 查询逻辑，以及 candidates 规则 |
| `dims_extractor.py` | 日清覆盖 dim_day_clear_override 规则（如需） |

### 风险

- **即食类 包含范围可能过宽**：dim_goods 中 L2=即食类的 SKU 不仅仅是熟制即食品，也可能包含一些不需要日清覆盖的预包装食品
- **建议先统计**：`SELECT COUNT(DISTINCT article_id), category_level3_description FROM dim_goods WHERE category_level2_description = '即食类' GROUP BY 2 ORDER BY 1 DESC` 确认即食类下所有 L3 的业务含义

## 影响评估

- **日清覆盖**: 这些即食类 SKU 在门店往往是现制现售的熟食，应当日清
- **经营监控看板**: 类别重映射后，这些 SKU 归入熟食类，影响大类毛利统计
- **加工关系**: 候选 SKU 和已配置关系的分类路径会更准确
