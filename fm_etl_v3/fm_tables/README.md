# fm_tables — FM 底表构建层

Pipeline 的最后一层。从 DuckDB 的计算结果表读取数据，构建面向 FM 平台的宽表，**结果直接留在 DuckDB/MotherDuck**，不再写回 StarRocks。团队成员通过 MotherDuck Web UI 或 DuckDB 客户端查询。

## 四张底表

### `sku_dim.py` — SkuDimBuilder

**DuckDB 表：** `t_fm_sku_dim`
**粒度：** 门店 × 日期 × article_id × day_clear

```python
SkuDimBuilder(duck).build(start=start, end=end)
```

从 `t_atomic_wide`、`t_calc_inventory`、`t_calc_amounts`、`t_calc_profit` JOIN 所有 dim 表，输出 SKU 级完整宽表。主要处理：类别重映射、销售重量计算、售罄标识、近 7 天日均销量（DuckDB 窗口函数）。

---

### `cust.py` — CustBuilder

**DuckDB 表：** `t_fm_cust`
**粒度：** 门店 × 日期 × day_clear × level_description × level_id

```python
CustBuilder(duck, api).build(start=start, end=end, yesterday=yesterday)
```

唯一仍需 `ApiConnector` 的 builder——订单明细数据从 API 实时拉取（两张 hive 表 UNION ALL），在 DuckDB 中按 6 个层级聚合客数。SQL 中的 `CASE WHEN`（品类重映射）已转为嵌套 `IF()`。

---

### `levels_sum.py` — LevelsSumBuilder

**DuckDB 表：** `t_fm_levels_sum`
**粒度：** 门店 × 日期 × 分类等级（门店/大类/中类/小类/SPU/黑白猪/SKU）× day_clear

```python
LevelsSumBuilder(duck).build(start=start, end=end)
```

从 `t_fm_sku_dim` 展开 7 个分类层级，UNION ALL 合并，`day_clear='2'` 为日清+非日清合计行。

---

### `levels_result.py` — LevelsResultBuilder

**DuckDB 表：** `t_fm_levels_result`
**粒度：** 同 levels_sum（按门店/日期/分类/day_clear GROUP BY）

```python
LevelsResultBuilder(duck).build(start=start, end=end)
```

从 `t_fm_levels_sum` 聚合计算所有**比率型 KPI**，输出**中文列名**，直接对接 FM 平台看板。主要指标：门店毛利率、全链路毛利率、损耗率、折扣率、客单价、采购价、平均售价等。

## 与旧版的区别

旧版四个 builder 在 DuckDB 计算完后会调用 `StarRocksConnector.write_dataframe()` 将数据写回 StarRocks。**v3 改造后去掉了这一步**，DuckDB/MotherDuck 即为最终存储，通过 MotherDuck Web UI 或 Flask API 对外提供查询。
