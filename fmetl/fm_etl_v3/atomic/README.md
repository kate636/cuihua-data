# atomic — 原子域提取层

Pipeline 的 **Step 1 & Step 2**：通过 `ApiConnector` 调 QDM BI API 拉原始数据，分批写入 DuckDB 的 `atomic_*` 和 `dim_*` 表。

**粒度**：所有原子表都是 `门店 × 业务日期 × article_id`（有 day_clear 字段但不入粒度）。

---

## `_base.py` — `BaseExtractor` 基类

所有 9 个业务原子提取器继承此类。子类只要声明两件事：

```python
class XxxExtractor(BaseExtractor):
    TARGET_TABLE = "atomic_xxx"

    def _fetch_sql(self, start: str, end: str, yesterday: str) -> str:
        return f"SELECT ... FROM hive.xxx WHERE business_date BETWEEN '{start}' AND '{end}'"
```

### `extract()` 执行流程

1. 用 `split_date_range(start, end, chunk=7)` 把长区间切成 **7 天一段**（避免 API 超时和 WAF 限流）
2. 对每个分段：
   - 调 `_fetch_sql(seg_start, seg_end, yesterday)` 生成 SQL
   - `ApiConnector.query(sql)` → `pandas.DataFrame`
   - `DuckDBStore.load_df(df, TARGET_TABLE, date_col="business_date", start=seg_start, end=seg_end, mode="replace_partition")`
3. 最后打一条 `total rows = N` 的日志

**幂等性**：`replace_partition` 模式保证同一日期区间重跑会先删后插，不产生重复。

---

## 9 个业务提取器

| 类名 | TARGET_TABLE | 来源 Hive 表 | 说明 | CASE→IF 改造 |
|------|--------------|--------------|------|----------|
| `SalesExtractor` | `atomic_sales` | `hive.dsl.dsl_transaction_non_daily_store_order_details_di` | 销售流水（原价额、补贴额、渠道） | ✅ 已转 |
| `InventoryExtractor` | `atomic_inventory` | `hive.dsl.dsl_transaction_non_daily_store_article_purchase_di` | 进货验收流水 | 无 |
| `ScmExtractor` | `atomic_scm` | `hive.dal_full_link.dal_manage_full_link_dc_store_article_scm_di` | 供应链 SAP 出入库 | ✅ 已转 |
| `LossExtractor` | `atomic_loss` | `hive.dal.dal_transaction_store_article_lost_di` | 已知损耗流水 | 无 |
| `ComposeExtractor` | `atomic_compose` | `hive.dsl.dsl_transaction_sotre_article_compose_info_di` | 加工转换（一 SKU 拆/合为另一 SKU） | 无 |
| `AllowanceExtractor` | `atomic_allowance` | `hive.dal.dal_activity_article_order_sale_info_di` | 订单级补贴 | 无 |
| `PromoExtractor` | `atomic_promo` | `hive.dsl.dsl_promotion_order_item_article_sale_info_di` | 促销活动明细 | ✅ 已转 |
| `CostPriceExtractor` | `atomic_cost_price` | `hive.ods_sc_db.t_shop_inventory_sku_pool` | 成本价底表 | 无 |
| `PriceExtractor` | `atomic_price` | `hive.dim.dim_store_article_price_info_da` | 商品售价 / 原价 / DC 原价 | 无 |

---

## `DimsExtractor` — 维度表（特殊）

不继承 `BaseExtractor`，提供 `extract_all(yesterday, start, end)` **一次性**加载所有维度快照（`replace` 模式，整表 DROP + CREATE）。

| DuckDB 表 | 源表 | 用途 |
|-----------|------|------|
| `dim_store_list` | `hive.dim.dim_chdj_store_list_di` | **翠花门店白名单**（merge 阶段 INNER JOIN 过滤用） |
| `dim_day_clear` | `hive.dim.dim_day_clear_article_list_di` | 商品日清/非日清标签（后备） |
| `dim_goods` | `hive.dim.dim_goods_information_have_pt` | 商品主数据（品类层级、单重、规格） |
| `dim_store_profile` | `hive.dim.dim_store_profile` | 门店属性（区域/城市） |
| `dim_chdj_store_info` | `ads_business_analysis.chdj_store_info` | 翠花门店编号、标签 |
| `dim_calendar` | `hive.dim.dim_calendar` | 日历（周/月/年） |
| `dim_saleable` | `hive.ods_sc_db.t_purchase_order_item_tmp` | 门店可售商品列表 |

为什么是 `replace`：维度数据量小（通常几万行），每天重拉一次成本很低；而且源表可能被后台人工校正，增量拉取会遗漏修正。

---

## 关键过滤规则

### 物料类商品

- **原子层**：`category_level1_id NOT IN ('70','71',...'77')`，**但仅线下渠道排除，线上保留**
- **FM 底表**（`fm_tables/`）：无论线上线下都排除物料类

### 翠花门店过滤

原子层**不过滤**门店（拉全量），`calculated/merge.py` 在产出 `t_atomic_wide` 时做 `INNER JOIN dim_store_list` 才过滤到翠花门店。

这样做的好处：维度表和原子表的**全量数据都在 DuckDB 里**，如果后续要扩展到非翠花门店（比如加福利店），改 merge 的 JOIN 即可，不用重跑原子层。

### 日清标签优先级

`t_atomic_wide.day_clear` 字段的确定规则：

```
事务流水 day_clear > 进货流水 day_clear > dim_day_clear 维度表
```

在 `calculated/merge.py` 里用 `COALESCE(a.day_clear, b.day_clear, d.day_clear)` 实现。

---

## 排查技巧

**某一个原子表行数突然为 0**：
1. 看日志 `extracting atomic_xxx: ... in N segments` 有没有打出 `fetched 0 rows`
2. 单独跑一天直接 `python -c "from fm_etl_v3.atomic import XxxExtractor; ..."`
3. 把 SQL 里的日期范围缩小到具体一天，通过 `scripts/probe_tables.py` 或 bdapp 控制台直接跑

**WAF 429 / 400 错误**：
- 90% 是 `CASE WHEN` 没改干净 → 搜 `grep -r "CASE WHEN" fm_etl_v3/atomic/`
- 10% 是 `IN (...)` 列表太长 → 分批

**源表 schema 变了**：
- `scripts/probe_tables.py` 可以快速探测字段是否还在
- SQL 里用具体列名（不要 `SELECT *`），字段少了会直接报错，不会沉默
