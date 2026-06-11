# FIX-016 手动日清清单重整 (93→72 SKU)

## 问题

手动日清清单（`/opt/fm/dayclear/day_clear.db`）共 93 个 SKU，存在三类问题：

1. **烘焙冷冻原料被错误日清**（~21个）：坚果酸法包、橄榄菜腊肠恰巴塔、丹麦菠萝包等加工原料，本身是冷冻面团/半成品，不应当日清。被日清后 end_stock 强制清零 → 库存无法跨日结转 → EUC 无法继承 → **加工成品的 compose_in 成本在原料无进货日归零**（FIX-002 关联问题）
2. **临时覆盖残留**：鲜肉类 12 个、蔬菜类 3 个、熟食类 32 个、其他 1 个，共 ~48 个历史临时追加，已不再需要
3. **缺少盘点实货 SKU**：部分需要日清控制的实货 SKU 不在清单中

**根因影响链**: `原料被日清 → end_stock=0 → 次日 init_stock=0 → base_euc=0（无进货日）→ compose_in_amt=0 → 成品当天"暴利"`

典型例子：手作坚果酸法包(C) 在 6/3-6/7 五天中，3 天 compose_in=0（原料无进货 → euc=0），导致成品毛利虚高 +50~+67/天。

## 修复方案

服务器直接操作 `day_clear.db` SQLite 数据库，不涉及代码修改。

### 目标清单

| 类型 | 数量 | 说明 |
|------|:---:|------|
| 烘焙成品 | 24 | 面包/蛋糕/点心等，必须日清 |
| 盘点实货 SKU | 48 | 库存盘点中确认需要日清的 SKU |
| **合计** | **72** | |

### 操作步骤

```sql
-- 1. 停用非目标 SKU (is_active=0)
UPDATE day_clear_override SET is_active = 0
WHERE article_id NOT IN ('<72个目标SKU>');

-- 2. 保留/恢复目标 SKU (is_active=1)
UPDATE day_clear_override SET is_active = 1
WHERE article_id IN ('<72个目标SKU>');
```

API `/api/dayclear/list?manual_only=1` 验证返回 72 个，与目标完全匹配。

### 移除非日清的烘焙原料（21个）

坚果酸法包原料、橄榄菜腊肠恰巴塔原料、丹麦菠萝包原料等冷冻面团半成品，原本是冷冻库存商品，恢复为**非日清** → 有正常 init/end 库存 → EUC 可跨日继承。

## 效果

- 烘焙冷冻原料恢复跨日库存结转，有正常的 init_stock / end_stock
- 加工成品 compose_in 成本不再因原料 EUC=0 而间歇归零
- 手动日清清单从 93 精简到 72，消除历史临时覆盖

## 影响范围

- 服务器: `/opt/fm/dayclear/day_clear.db`（SQLite 直接 UPDATE，无需重启 API）
- ETL: `dims_extractor.py` 通过 API `?manual_only=1` 拉取，自动生效
- 无代码修改，无 git commit

## 关联

- [[FIX-002]] — EUC 跨日继承问题，本次修复缓解了原料端症状但未从根本上解决
- [[FIX-013]] — compose 部分原料 euc=0 不归零，互补修复
