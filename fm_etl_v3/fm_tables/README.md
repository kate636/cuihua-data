# fm_tables — FM 底表构建层

Pipeline 的 **Step 8~11**：从 DuckDB 的计算结果表出发，构建面向 FM 平台看板和分析师的 4 张最终底表。

**关键设计**：v3 起所有底表**直接留在 DuckDB 里**，不再写回 StarRocks。看板查询通过 `query_api/` HTTP 服务或 DBeaver SSH 隧道直读 `fm.duckdb`。

---

## 四张底表概览

| DuckDB 表 | 构建器 | 粒度 | 主要用途 |
|---|---|---|---|
| `t_fm_sku_dim` | `SkuDimBuilder` | 门店 × 日期 × article_id × day_clear | SKU 级完整宽表（供 levels_sum 展开用，也能单独查询） |
| `t_fm_cust` | `CustBuilder` | 门店 × 日期 × day_clear × level | 按 6 个层级聚合的**客数** |
| `t_fm_levels_sum` | `LevelsSumBuilder` | 门店 × 日期 × 分类等级 × day_clear | 7 层分类 UNION ALL 展开（数量/金额字段） |
| `t_fm_levels_result` | `LevelsResultBuilder` | 同 levels_sum | **最终对接看板**（中文列名 + 比率型 KPI） |

### 构建依赖顺序

```
t_atomic_wide, t_calc_inventory, t_calc_avg_price,
t_calc_amounts, t_calc_profit, 7 个 dim_*
              │
              ▼
    ┌─── SkuDimBuilder ───┐
    │                     │
    │  t_fm_sku_dim ◀─────┘
    │        │
    │        ▼
    │  CustBuilder (需 ApiConnector 拉订单明细)
    │        │
    │        ▼
    │  t_fm_cust
    │        │
    │        ▼
    │  LevelsSumBuilder ───▶ t_fm_levels_sum
    │                                │
    │                                ▼
    │                      LevelsResultBuilder ───▶ t_fm_levels_result
```

---

## Step 8 · `sku_dim.py` — `SkuDimBuilder`

**DuckDB 表**：`t_fm_sku_dim`  
**粒度**：门店 × 日期 × article_id × day_clear

```python
SkuDimBuilder(duck).build(start="2026-01-01", end="2026-01-31")
```

**做什么**：
- 从 `t_atomic_wide` / `t_calc_inventory` / `t_calc_amounts` / `t_calc_profit` JOIN 所有 `dim_*` 表
- 产出 SKU 级完整宽表
- **重点处理**：
  - 类别重映射（大类/中类/小类的人工重编，与 `fm_商品维度底表.sql` 保持一致）
  - 销售重量计算（千克品种直接用 qty，件品种 = qty × 单重）
  - 售罄标识（`end_stock_qty <= 0 AND sale_qty > 0`）
  - **近 7 天日均销量**（DuckDB 窗口函数 `AVG(...) OVER (PARTITION BY store_id, article_id ORDER BY business_date RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW)`）

**每次运行**：DROP + CREATE 整表重建（表本身会根据传入的 start/end 控制行数）。

---

## Step 9 · `cust.py` — `CustBuilder`

**DuckDB 表**：`t_fm_cust`  
**粒度**：门店 × 日期 × day_clear × level_description × level_id

```python
CustBuilder(duck, api).build(start="2026-01-01", end="2026-01-31", yesterday="2025-12-31")
```

**做什么**：
- **这是 pipeline 里唯一在 Step 8 之后还调 QDM API 的 builder**
- 因为客数必须从订单明细（`order_id` 去重）聚合，而订单粒度数据量大、不落地到 `atomic_*`
- 从两张 Hive 表 `UNION ALL` 订单明细：日清流水表 + 非日清流水表
- 在 DuckDB 中按 **6 个层级** GROUP BY 客数（`COUNT(DISTINCT order_id)`）：
  1. 门店
  2. 大类
  3. 中类
  4. 小类
  5. SPU / 黑白猪
  6. SKU
- SQL 里原本的 `CASE WHEN`（品类重映射）已改为嵌套 `IF()`，符合 WAF 要求

**为什么不放到 `atomic/`**：订单明细粒度 = 订单行，全量落地太大（单日几千万行）；客数是 pre-aggregated 的，粒度已经是聚合后的 level，落地量级和其他 `t_fm_*` 相当。

---

## Step 10 · `levels_sum.py` — `LevelsSumBuilder`

**DuckDB 表**：`t_fm_levels_sum`  
**粒度**：门店 × 日期 × 分类等级 × day_clear

```python
LevelsSumBuilder(duck).build(start=..., end=...)
```

**做什么**：
- 从 `t_fm_sku_dim` 展开为 7 层分类 UNION ALL：
  1. 门店 level（store 汇总到"门店"一行）
  2. 大类 level（`category_level1`）
  3. 中类 level（`category_level2`）
  4. 小类 level（`category_level3`）
  5. SPU level（`spu_id`）
  6. 黑白猪 level（特殊 SPU 分组）
  7. SKU level（原始 article 粒度）
- 每一层按粒度 SUM 所有数量 / 金额字段
- **`day_clear='2'`** 行 = 日清 + 非日清合计（额外加的一层）

**产出行数** ≈ `t_fm_sku_dim` 行数 × 7 × 2（day_clear=0,1 各一份 + day_clear=2 合计）

---

## Step 11 · `levels_result.py` — `LevelsResultBuilder`

**DuckDB 表**：`t_fm_levels_result`  
**粒度**：同 levels_sum

```python
LevelsResultBuilder(duck).build(start=..., end=...)
```

**做什么**：
- 从 `t_fm_levels_sum` 再聚合一层（处理门店/公司维度展平、标签列等）
- 计算所有**比率型 KPI**
- **输出中文列名**，直接对接 FM 平台看板（前端不做列名转换）

**主要输出字段**：

| 中文列名 | 英文 | 公式（安全除法：分母为 0 时返回 NULL） |
|---------|------|---------------------------------------|
| 门店毛利率 | — | `profit_amt / sale_amt` |
| 全链路毛利率 | — | `full_link_article_profit / sale_amt` |
| 损耗率 | — | `lost_amt / receive_amt` |
| 折扣率 | — | `discount_amt / original_price_sale_amt` |
| 客单价 | — | `sale_amt / cust_num` |
| 件单价 | — | `sale_amt / sale_piece_qty` |
| 均价 | — | `sale_amt / sale_qty` |
| 采购价 | — | `receive_amt / receive_qty` |
| 19点前销售占比 | — | `bf19_sale_amt / sale_amt` |

安全除法用 `CASE WHEN COALESCE(den,0)=0 THEN NULL ELSE num/den END`（这里的 `CASE WHEN` 是本地 DuckDB，不经过 API WAF，所以可以用）。

---

## 与旧版（`legacy_scripts/`）的区别

| 旧版 | v3 |
|------|-----|
| 四个 builder 跑完后用 `StarRocksConnector.write_dataframe()` 回写到 StarRocks | **不再回写**，结果留在 DuckDB |
| 平台查询走 StarRocks 连接池 | 平台查询走 `query_api/` HTTP 服务或 DBeaver 直连 DuckDB |
| 补数需要 StarRocks 权限 | 补数只需要 ssh + 云端 venv |
| 历史数据修正需 DELETE + INSERT | 同一日期分区直接重跑（幂等） |

---

## 常见查询示例

通过 Query API 或 DBeaver：

```sql
-- 昨天广州 food mart 全门店大类毛利率
SELECT 门店号, 大分类, 门店毛利率
FROM t_fm_levels_result
WHERE 日期 = '2026-04-19'
  AND 分类等级 = '大类'
  AND day_clear = '2';

-- 某 SKU 过去一周的销售和库存
SELECT business_date, sum(total_sale_qty), sum(end_stock_qty)
FROM t_fm_sku_dim
WHERE article_id = 'SKU001'
  AND business_date BETWEEN '2026-04-13' AND '2026-04-19'
GROUP BY 1
ORDER BY 1;

-- 客数和销售联合分析
SELECT
    s.business_date,
    s.store_id,
    s.day_clear,
    SUM(s.total_sale_amt) AS sale_amt,
    c.cust_num
FROM t_fm_sku_dim s
JOIN t_fm_cust c
  ON s.store_id = c.store_id
 AND s.business_date = c.business_date
 AND s.day_clear = c.day_clear
WHERE c.level_description = '门店'
  AND s.business_date = '2026-04-19'
GROUP BY 1,2,3,c.cust_num;
```
