# DESIGN-002: v1_5 业务能力并入完整重构 ETL 的调整方案

> 日期: 2026-07-15
> 状态: 已被 `DESIGN-003-v0.12-clean-rebuild.md` 替代
> 建议目标版本: fmetl v0.12
> 设计阶段: 只定义边界、数据合同、改造顺序与验收标准, 不包含代码实现
> 关联设计: `DESIGN-001-daily-cost-stock-state-machine.md`
> 参考实现: `/Users/zhukate/Desktop/Projects/qdm/翠花数据诊断/huajia_yonghong_etl/versions/v1_5`

> 2026-07-15 用户进一步确认了 A3XV、订单客次、v1_5 新客/品类/特殊损耗口径、不迁移运营 KPI，并要求以 `sync_strategy_fm.sh` 镜像层为所有 ETL 输入边界。因这些决策改变了本文多个前提，不在本文局部修补；实施一律以 DESIGN-003 为准。

---

## 1. 执行结论

### 1.1 当前项目架构是否清晰

结论分两层:

- **概念架构清晰**: `atomic -> calculated -> fm_tables` 三层结构、统一入口、表名前缀和业务模块基本可读。
- **实现边界不够清晰**: 规则分散、状态依赖不闭合、增量失效范围不明确、抽取失败可继续发布、部分“抽取表”并非计算权威、结果层存在断链。

综合判断: **5.5/10, 把握 90%**。

更准确的描述是:

> 当前项目是一套“主流程可追踪、核心业务已重构、但契约和运行安全尚未工程化”的 ETL。它能够继续演进, 但不适合直接叠加 v1_5 SQL。

### 1.2 v1_5 的真实定位

`v1_5` 不是 fmetl 的后续完整版本。它的真实链路是:

```text
公司 Hive/StarRocks 已加工结果
  full_link_article / chdj_article / store_daily
        ↓
SKU 指标再分类 + 损耗/利润补丁
        ↓
订单客数 + 渠道 + 周内新老客
        ↓
6 层汇总 + 中文 KPI
        ↓
写入 _v1_5 表及生产表 test 日期
```

它直接读取公司已算好的:

- 期初/期末库存;
- 门店毛利、供应链毛利、全链路毛利;
- 损耗金额;
- 7 日均量;
- 进货、出库和成本指标。

因此它是**公司 ETL 后的半加工链路**, 不能作为 fmetl 成本、库存、BOM、加工和毛利的上游真值。

### 1.3 本方案的核心决定

本次更新采用“能力迁移, 不迁移派生依赖”的原则:

1. 保留 fmetl 自己重算 BOM、成本、库存、损耗和毛利的目标。
2. 迁移 v1_5 的订单、渠道、周内新老客、完整层级键和结果 KPI 能力。
3. 分类逻辑进入单一主数据模块, 不复制 v1_5 的硬编码 CASE。
4. 炒菜机成本和生熟联动先进入独立调整事实, 未证明守恒前不直接改核心毛利。
5. 先修运行安全和状态边界, 再扩充业务字段。
6. 新逻辑先写影子表并验证, 不直接覆盖现有正式输出。

---

## 2. 审阅范围与事实来源

### 2.1 本次完整审阅范围

当前项目活跃代码:

- `fmetl/executor.py`
- `fmetl/config/`
- `fmetl/connectors/`
- `fmetl/atomic/`
- `fmetl/calculated/`
- `fmetl/fm_tables/`
- `fmetl/docs/architecture/`
- `fmetl/docs/designs/DESIGN-001-*`
- 与当前行为直接相关的 reviews/fixes/reference 文档

v1_5 参考代码:

- `run_corrected.py`
- `run_daily_1.5.sh`
- `batch_backfill.py`
- `sync_strategy_fm.sh`
- `backfill_mirrors.sql`
- `backfill_thirdparty.sql`
- `sql/step0` 至 `step4`
- `sql/validate_channel_sales.sql`

未作为当前主架构依据的范围:

- `_archived/` 历史实现;
- 旧版独立脚本;
- 只用于历史说明且已被活跃代码替代的文档。

### 2.2 代码快照状态

本设计以 **2026-07-15 当前 working tree** 为准。

审阅时工作树已有他人/用户未提交改动, 包括:

- `executor.py` 的 Step 5/6 按日闭环;
- `sku_cost.py` 和 `stock.py` 的日期范围改造;
- `REVIEW-009`;
- 待实现的 `DESIGN-001`。

本设计不修改、不覆盖这些已有改动。

### 2.3 品类权威来源核对

2026-07-15 已只读核对服务器:

```text
/opt/fm/主数据/category-mapping.md
```

服务器当前权威版本仍为 **v2.3**, 不是 v1_5 日志声称的 v2.5。

权威规则明确要求:

- 冷冻类使用 126 个 SKU 清单;
- 冰品类归入冷冻类;
- 预制菜按千克销售、即烹/即热、小类名含“熟食”的 SKU 按规则归入熟食;
- SKU 数、动销 SKU 数、品效排除加工原料 SKU。

但服务器主数据目录和当前仓库均没有 `config/frozen_skus.json`。v1_5 只有 119 个硬编码 SKU。故 126-SKU 完整清单是实施前阻塞项, 不能用 119 个清单冒充权威数据。

### 2.4 数据结论来源声明

本文件以代码审阅和本地结构检查为主, 不声称完成生产数据验证。

若引用本地 `data/fm.duckdb` 的结构或样例, 它只代表本地 2026-06-18 至 2026-06-22 快照, **不是生产权威数据**。实施后的数据验收必须在服务器 `/opt/fm/data/fm.duckdb` 上完成。

---

## 3. 当前 fmetl 的真实架构

### 3.1 实际执行链

```text
python -m fmetl.executor start end [stage]
  │
  ├─ Step 1  DimsExtractor
  │    ├─ dim_goods
  │    ├─ dim_store_list
  │    ├─ dim_day_clear
  │    ├─ dim_day_clear_override
  │    ├─ dim_store_profile
  │    ├─ dim_calendar
  │    ├─ dim_chdj_store_info
  │    └─ dim_saleable
  │
  ├─ Step 2  13 个活跃原子提取器, 6 线程并发
  │    └─ 另建 2 张空骨架表
  │
  ├─ Step 3  AtomicMerger -> t_atomic_wide
  │
  ├─ Step 4  BomAllocCalculator -> t_calc_bom_alloc
  │
  ├─ Step 5/6 按业务日闭环
  │    ├─ SkuCostCalculator -> t_calc_sku_cost
  │    └─ StockCalculator   -> t_calc_stock
  │
  ├─ Step 7  ProfitCalculator -> t_calc_profit
  │
  ├─ Step 8  t_fm_sku_dim
  ├─ Step 9  t_fm_cust
  ├─ Step 10 t_fm_levels_sum
  ├─ Step 11 t_fm_levels_result
  ├─ Step 11b t_fm_levels_result_matnr
  ├─ Step 12 t_fm_bom_breakdown
  ├─ Step 13 t_fm_stock_roll
  └─ Step 14 同步加工候选到外部 SQLite/服务器
```

实际不是严格的“14 个纯计算步骤”:

- Step 11b 是额外输出步骤;
- Step 14 有 SCP/SSH/SQLite 外部写副作用;
- `--fm-only` 仍会访问 QDM API 并执行外部同步;
- `--calc-only` 依赖数据库中已有的 `t_atomic_wide`。

### 3.2 当前数据层与职责

| 层 | 核心对象 | 当前职责 | 清晰度 |
|---|---|---|---|
| 连接/配置 | API, DuckDB, settings | 只读 API + 本地/服务器 DuckDB | 基本清晰 |
| 维度 | `dim_*` | 最新商品/门店/日清/可售快照 | 规则版本不清晰 |
| 原子 | `atomic_*` | 源表字段归一化和按日落地 | 部分表抽而不用 |
| 宽表 | `t_atomic_wide` | 销售/库存主脊柱 + 其他域 LEFT JOIN | 事件覆盖不完整 |
| 计算 | `t_calc_*` | BOM、成本、库存、利润 | 职责相互穿透 |
| 输出 | `t_fm_*` | SKU、客数、层级、平台结果、追溯 | 分类和客数断链 |
| 发布 | Step 14 | 加工候选外部同步 | 与核心 ETL 混合 |

### 3.3 当前不可破坏的核心业务合同

#### BOM 两套数量不可混用

```text
bom_alloc_qty      = 父品单位, 用于父品 bom_out
bom_alloc_qty_sub  = 子品单位, 用于子品 bom_in 和成本池
```

#### 当前库存数量方程

```text
eq = init
   + receive
   + bom_in - bom_out
   + compose_in - compose_out
   - sale
   - know_lost
```

#### 当前毛利公式

```text
profit = sale
       - receive
       - bom_in + bom_out
       - compose_in + compose_out
       + end_stock - init_stock
       - neg_clamp_cost_amt
```

损耗金额不再额外扣一次, 因为损耗已经通过库存方程改变期末库存。

#### 跨日依赖

```text
今天 init_qty/amt = 该 SKU 前一营业日 end_qty/amt
```

任何历史日期的以下内容变化都会使之后状态失效:

- 进货;
- BOM 关系/分配;
- 加工关系;
- 日清规则;
- 盘点;
- 单位换算;
- 成本兜底规则。

当前 executor 只重算用户传入区间, 没有自动向后传播失效范围。

### 3.4 当前架构清晰度评分

| 维度 | 评分 | 事实依据 |
|---|---:|---|
| 目录与命名 | 8/10 | atomic/calculated/fm_tables 可读 |
| 主链追踪 | 7/10 | 入口集中, 但 Step 11b/14 和 stage 语义偏离 |
| 业务规则单一来源 | 4/10 | 分类、日清、加工规则分散多处 |
| 状态与增量正确性 | 3/10 | 无前向失效, 部分模块无日期参数 |
| 数据合同 | 4/10 | 粒度多样, `SELECT *`/列序写入, 无 schema manifest |
| 失败安全 | 3/10 | 抽取失败可继续, 空分区保留旧数据 |
| 可测试性 | 3/10 | 无活跃测试体系, 主要依赖脚本和人工审查 |
| 新能力承接 | 5/10 | 可扩展, 但直接增加字段会放大现有问题 |
| 综合 | **5.5/10** | 概念清晰, 实现边界不牢 |

---

## 4. 当前链路的优先风险

### 4.1 P0: 运行和增量安全

#### P0-1 空结果不会清理旧分区

`BaseExtractor` 和 `DuckDBStore.load_df()` 都会在空 DataFrame 时跳过写入。

后果:

```text
源表某日原来有数据 -> 重刷时源表变为 0 行
结果: DuckDB 仍保留旧分区
```

这破坏幂等性。空结果必须区分:

- 合法空分区: 清旧分区后发布 0 行;
- 异常空分区: 阻断发布并报警。

#### P0-2 并发抽取异常只记录日志

任何原子表线程失败后, executor 仍继续 merge。这样会把部分新分区和部分旧分区混在一起。

目标行为:

```text
任一必需原子域失败 -> 本轮 run FAILED -> 不发布下游分区
```

#### P0-3 API 分页可能漏数或重复

- `pageData` 多页路径没有显式页号;
- 自动 keyset 默认取第一列, 多数查询第一列是非唯一 `store_id`;
- 恰好 20,000 行时可能跳过同键剩余行。

所有大表 extractor 必须声明稳定、唯一、复合排序键, 或改用 API 原生可验证分页。

#### P0-4 ProfitCalculator 重刷范围不闭合

`t_atomic_wide` 只保留本次区间, `t_calc_stock` 保留历史, 但 `ProfitCalculator.run()` 无日期参数并读取全量 stock。

这可能用缺失的历史 wide 字段重新计算历史 profit。

目标:

```text
ProfitCalculator.run(start, end)
只读取和覆盖 [start, end]
```

#### P0-5 写入无 staging 发布

多数步骤直接 `DELETE` 再 `INSERT`。中间失败会留下空分区。

目标模式:

```text
构建 _stg_run_id
-> 验证行数/键/平衡
-> BEGIN
-> DELETE 正式分区
-> INSERT BY NAME
-> COMMIT
```

### 4.2 P1: 计算语义

1. `t_atomic_wide` 只由销售和库存建立主脊柱, 纯损耗、纯加工、纯 SCM 事件可能没有行。
2. `atomic_bom_relation` 已抽取但不参与主算法。
3. `atomic_compose` 已抽取但加工数量/金额由另一套逻辑反推。
4. `atomic_scm_adjust` 存在但主链主要读取 `atomic_scm.adjustment_amt`。
5. `stock.py` 同时处理库存、SCM 金额和父品库存转移, 职责过多。
6. `sku_cost.py` 同时加载远程加工关系、写缓存、推加工数量、算加工金额、做 EUC 兜底和物料号互转。
7. BOM 共享组采用两两子集配对, 3 个以上父品的共享链不稳。
8. BOM 负权重只在输出时钉零, 分母和比例可能已经受负值影响。
9. BOM 零权重时缺少可审计回退, 父品金额可能完全不分配。
10. 当前 BOM 金额过早绑定父品当日进货额, 未充分考虑父品期初库存和上游 BOM 流入。该问题由 DESIGN-001 处理。
11. 库存转移修改期末库存, 但 `t_fm_stock_roll.balance_qty` 没有 transfer 项。
12. `day_clear` 被当作跨日状态键; 标签改变会让前日状态断链。

### 4.3 P1: 主数据和输出

1. 分类映射在 `dims_extractor.py`、`sku_dim.py`、`cust.py`、Step 14 中重复实现。
2. `sku_dim.py` 仍用“冰品类”代替 126-SKU 冷冻清单。
3. `cust.py` 仍标注 v2.1, 缺少 v2.2/v2.3 的全部熟食规则。
4. 加工原料未从 SKU 数、动销 SKU 数和品效分母中排除。
5. `t_fm_cust` 没有 SKU 层, `MatnrResultBuilder` 却查询 SKU 客数, 因而物料号客数链断裂。
6. `day_clear='2'` 客数由各 day_clear 相加, 同一订单跨日清/非日清商品时会重复计客。
7. 当前大类映射后常把“名称”塞入 ID 字段或把 ID 置空, ID/名称语义不稳定。

---

## 5. v1_5 的可迁移能力与不可迁移依赖

### 5.1 能力分类矩阵

| v1_5 能力 | 决策 | fmetl 处理方式 |
|---|---|---|
| 线上/线下客数、金额、数量 | 迁移 | 新订单原子域 + 订单增强事实 |
| 接龙/及时达拆解 | 迁移并重定义为互斥轴 | `order_channel` + `online_subchannel` |
| 周内新客/老客 | 迁移, 字段语义显式化 | 全历史首单维度 + `week_customer_type` |
| SKU 客数层 | 迁移 | 修复当前 matnr 客数断链 |
| 中/小/SPU 完整父路径键 | 迁移 | 内部 join 使用完整维度元组 |
| 渠道/新老客 KPI | 迁移 | 扩展 levels_sum/result |
| 冷冻和熟食品类规则 | 迁移需求, 不复制实现 | 服务器 master-data 单一模块 |
| 炒菜机成本 | 先影子建模 | 独立 adjustment ledger, 业务确认后入主链 |
| 生熟联动 | 重新设计 | store/date/day_clear 粒度守恒转移 |
| 售罄/上架 | 保留并规范 | 运营指标模块, 不混入成本计算 |
| 源表 preflight | 迁移并增强 | run manifest + source contract |
| 按日回刷 | 迁移思想 | 通用 backfill 命令, 禁止固定日期脚本 |
| DELETE + INSERT | 不照搬 | staging + 事务发布 |
| 动态列序推生产 | 不照搬 | 显式 schema contract |
| `{date}-test` 影子发布 | 迁移思想 | 独立 shadow 表/视图, 不污染日期字段 |
| 公司 full_link/chdj 派生指标 | 禁止成为主链上游 | 仅 `ref_qdm_*` 对照 |

### 5.2 v1_5 不可直接复制的公司派生字段

以下字段只能作为对比基准, 不能进入 fmetl atomic 真值:

- `full_link_profit`;
- `scm_fin_article_profit`;
- `profit_amt`;
- `pre_profit_amt`;
- `init_stock_amt`, `end_stock_amt`, `end_stock_qty`;
- `avg_7d_sale_qty`;
- 公司口径 `store_lost_*`;
- 公司计算的 `inbound_amount`, `purchase_weight`, `expect_outstock_amt`, `out_stock_amt_cb`。

若这些字段进入主链, fmetl 就会重新依赖公司 ETL 的库存和利润结果, 与“完整重构”目标矛盾。

### 5.3 v1_5 已发现的实现风险

迁移需求时不能继承以下缺陷:

1. Step 1 `ROW_NUMBER` 没有真正过滤 `rn=1`。
2. 首单只从运行日起增量, 没有全历史 bootstrap。
3. Step 0 失败被吞, 用户会静默落入“其他”。
4. 生熟联动只按日期汇总, 未按 store/day_clear, 多店会重复扣减。
5. 特殊损耗按 SKU/date JOIN, 没有 store_id。
6. 非翠花店 day_clear 派生值和结果标签疑似相反。
7. 119 个冷冻 SKU 清单复制两份, 且与权威 126 不一致。
8. 订单与交易表按 order_id 直接 JOIN, 未先去重, 可能放大订单行。
9. 新客 + 老客不等于总客数, 但没有“其他/未知”完整性指标。
10. 渠道无匹配时 SUM 返回 NULL, 未统一补 0。
11. `sale_piece_qty` 多层 AVG, 失去可加总性。
12. 结果层直接除法, 无统一零分母保护。
13. 多数 INSERT 依赖目标表列顺序。
14. 历史回刷使用运行时最新商品维度, 不是历史可重现快照。
15. 只验证线上 + 线下 = 总销售, 验收覆盖不足。

---

## 6. 目标架构

### 6.1 目标总图

```text
源镜像层 StarRocks
  ├─ 已有 strategy_fm_* 原始/近原始镜像
  ├─ offline / online order detail
  ├─ trade user identity
  └─ special wastage observation
            │
            ▼
Layer -3 规则与运行治理
  ├─ etl_run_manifest
  ├─ source_snapshot_manifest
  ├─ category_rule_snapshot
  ├─ day_clear_rule_snapshot
  └─ processing_relation_snapshot
            │
            ▼
Layer -2 原子事实 atomic_*
  ├─ sale / inventory / scm / loss / receive / bom ...
  ├─ atomic_order_offline
  ├─ atomic_order_online
  ├─ atomic_trade_user
  └─ atomic_special_wastage
            │
            ▼
Layer -1.5 规范化事实
  ├─ fact_sku_day_spine
  ├─ fact_bom_plan
  ├─ fact_processing_plan
  ├─ fact_order_line
  └─ dim_user_first_order
            │
            ├──────────────────────────────┐
            ▼                              ▼
Layer -1 核心计算                    客户/运营计算
  DailyCostStockCalculator           t_calc_order_enriched
  ├─ t_calc_sku_cost                 t_calc_customer_metrics
  ├─ t_calc_stock                    t_calc_operating_metrics
  ├─ t_calc_bom_daily_amount         t_calc_business_adjustment_shadow
  └─ t_calc_profit                          │
            └──────────────────────┬────────┘
                                   ▼
Layer 0 兼容输出
  ├─ t_fm_sku_dim
  ├─ t_fm_cust
  ├─ t_fm_levels_sum
  ├─ t_fm_levels_result
  ├─ t_fm_levels_result_matnr
  ├─ t_fm_bom_breakdown
  └─ t_fm_stock_roll
                                   │
                                   ▼
独立发布阶段
  platform publish / candidate sync / shadow comparison
```

### 6.2 为什么增加“规范化事实”而不是继续加宽 `t_atomic_wide`

当前 `t_atomic_wide` 以销售和库存作为行集合。新域继续 LEFT JOIN 会产生两个问题:

- 没有销售/库存但有损耗、加工、订单或调整的事实无法独立建行;
- 不同粒度强行压到 SKU-day 宽表, 容易重复。

目标改为先建立:

```text
fact_sku_day_spine = UNION DISTINCT(
  所有 SKU 日事件域的 store_id, business_date, article_id
)
```

再把各域聚合到明确合同后关联。

订单行不进入 `t_atomic_wide`; 它保持订单粒度, 进入独立客户事实链。

### 6.3 状态主键调整

推荐将库存状态身份从:

```text
store_id × article_id × day_clear
```

调整为:

```text
store_id × article_id
```

本节是对 `DESIGN-001` 中状态粒度的显式修订: `DESIGN-001` 原文若将 `day_clear` 放在跨日状态键中, 实现时以本设计为准。`day_clear` 仍保留在当日事实和输出表, 但不用于识别“是否同一个跨日库存状态”。

每日事实粒度为:

```text
store_id × business_date × article_id
```

`day_clear` 是该日业务规则属性, 不是 SKU 的永久身份。

原因:

- 日清标签会变;
- 标签切换不应导致前日期末库存找不到;
- 当前代码已经隐含要求同一 SKU 同日只有一个 day_clear。

兼容输出仍保留 `day_clear` 列。实施前必须验证:

```text
COUNT(DISTINCT day_clear) <= 1
for each store/date/article
```

若真实业务允许同一 SKU 同日两种 day_clear, 则需要单独设计库存拆分规则, 不能静默按 MAX 合并。

---

## 7. 主数据单一来源设计

### 7.1 新增共享模块

建议新增:

```text
fmetl/master_data/
├── __init__.py
├── category_mapping.py
├── day_clear_rules.py
└── sku_roles.py

fmetl/config/master_data/
├── frozen_skus.json
└── category_rule_version.json
```

所有调用者只允许调用共享函数, 不允许自行写 CASE/NumPy 条件:

```text
map_report_category(df, rule_snapshot)
resolve_day_clear(df, rule_snapshot)
classify_sku_role(df, processing_snapshot)
build_category_path_key(df)
```

### 7.2 分类输出合同

不要继续重载 `category_level1_id`。新增内部标准字段:

| 字段 | 含义 |
|---|---|
| `source_category_level1_id` | dim_goods 原大类 ID |
| `source_category_level1_description` | dim_goods 原大类名称 |
| `report_category_code` | 稳定报告品类编码 |
| `report_category_name` | 报告品类名称 |
| `category_rule_version` | 本轮规则版本 |
| `category_rule_reason` | 命中规则: frozen_sku/cooked_l3/... |

兼容表可继续把 `report_category_name` 映射到现有“大分类”列, 但内部不再用中文名称充当 ID。

### 7.3 冷冻 SKU 清单

实施前必须拿到权威 126-SKU 清单, 并满足:

```text
JSON count = 126
all article_id unique
每个 SKU 有来源、更新时间和规则版本
v1_5 119 清单只能做差异对照
```

建议记录差异:

```text
authority_126 - v1_5_119
v1_5_119 - authority_126
```

`frozen_skus.json` 只能是服务器权威主数据经审批生成的机器可读快照, 不是第二个权威源。它必须记录源文件版本/摘要、审批人和生成时间, 并由同一个同步流程再生成, 不允许手工独立维护。

在清单未补齐前, 分类模块可以影子运行, 但不得宣称已对齐 master-data。

### 7.4 加工原料 SKU

按权威规则, 加工原料只影响 SKU 数相关分母:

```text
动销 SKU 数 = active_sku - active_processing_raw_sku
上架 SKU 数 = stock_sku - stock_processing_raw_sku
品效分母     = active_sku_days - raw_active_sku_days
```

销售额、销售量、毛利分子不删除。

加工原料身份优先来自本项目加工关系快照中的 `raw_sku`, 不再依赖硬编码排除列表。

---

## 8. 订单、渠道和新老客链路

### 8.1 新增原子表

建议新增 3 个 extractor, 均继承 `BaseExtractor`, 只实现 SQL:

```text
OfflineOrderExtractor -> atomic_order_offline
OnlineOrderExtractor  -> atomic_order_online
TradeUserExtractor    -> atomic_trade_user
```

最小字段合同:

| 字段 | 说明 |
|---|---|
| `business_date` | 业务日期 |
| `inc_day` | 源分区日期 |
| `store_id` | 门店 |
| `order_id` | 原始订单 ID, 不追加 `*` |
| `canonical_order_key` | `source_channel + order_id` 的稳定复合键, 用于跨渠道去重 |
| `article_id` | SKU |
| `pay_at` | 支付时间 |
| `order_status` | completed/split/refund/return |
| `jielong_flag` | 接龙标记 |
| `sales_amt` | 与总销售可对平的净销售口径 |
| `qty` | 与销售数量可对平的数量口径 |
| `thirdparty_user_identity` | 跨渠道用户身份 |
| `source_channel` | offline/online |
| `source_row_key` | 经源表主键审计后确定的唯一键 |

禁止通过给线上 `order_id` 加 `*` 来区分渠道。渠道是独立字段。但线上和线下原始订单号可能相同, 所以所有跨渠道 distinct 必须使用 `canonical_order_key`, 不能只用 `order_id`。

### 8.2 源行唯一性

实施前必须确认 offline/online 源表的真实主键。

不得默认 `order_id + article_id` 唯一, 因为可能存在:

- 拆单;
- 同订单同 SKU 多行;
- 退款/退货事件;
- 多条 trade 记录。

验收 SQL 必须检查候选键重复率。若源无稳定主键, 才使用确定性 `source_row_hash`, 且哈希字段合同必须固定。

### 8.3 用户首单维度

新增:

```text
dim_user_first_order
grain = thirdparty_user_identity
```

字段:

| 字段 | 说明 |
|---|---|
| `thirdparty_user_identity` | 用户身份 |
| `first_order_date` | 全历史最早有效订单日期 |
| `first_order_id` | 可选, 用于追溯 |
| `source_min_date` | bootstrap 覆盖起点 |
| `last_refresh_date` | 最近刷新日期 |
| `is_history_complete` | 是否完成全历史回溯 |

构建分两步:

1. 全历史 bootstrap: 从可用最早日期到当前日取 MIN。
2. 每日增量: 与已有 first_order_date 取更小值, 幂等 upsert。

如果没有完整历史, 新老客指标只能标记为实验口径, 不能正式发布。

### 8.4 新老客定义

v1_5 实际定义是“周内新客”, 不是“当天新客”。保留业务口径, 但内部字段必须准确命名:

```text
first_order_date < week_start_date  -> old
first_order_date >= week_start_date -> new
first_order_date IS NULL            -> unknown
```

内部字段:

```text
week_customer_type = new / old / unknown
```

兼容输出可显示“新客/老客”, 同时必须新增:

- 未识别客数;
- 用户身份覆盖率;
- `new + old + unknown` 完整性检查。

### 8.5 渠道模型

使用两个互不混淆的维度:

```text
order_channel = online / offline

online_subchannel =
  jielong  if online and jielong_flag != '-'
  jsd      if online and jielong_flag = '-'
  null     if offline
```

这样可以建立严格对平:

```text
online + offline = total
jielong + jsd = online
```

如果业务确认存在“线下接龙”, 则 `jielong` 应作为独立交叉标签, 不能继续假设它是线上子渠道。

### 8.6 订单状态口径

建议沿用 v1_5 的意图, 但先验证符号:

```text
客数:
  只统计 os.completed 的 distinct order

金额/数量:
  completed + split + refund.completed + return.completed
  前提: 退款/退货行已按负号表达净额
```

验收必须证明:

```text
SUM(order_line.sales_amt) = atomic_sales.sale_amt
SUM(order_line.qty_in_sales_unit) = atomic_sales.sale_qty 或可解释差异
```

若退款/退货行不是负数, 必须在规范化事实层显式翻转符号。

### 8.7 “客数”与“用户数”必须分开

v1_5 和当前项目的 `cust_num` 本质上是 `COUNT DISTINCT order`，语义更接近“交易客次/订单数”, 不是去重购买用户数。

目标链路必须同时保留两组底项:

```text
order_count = COUNT DISTINCT canonical_order_key
user_count  = COUNT DISTINCT thirdparty_user_identity, 且排除 NULL
```

其中:

- 现有平台字段“客数”为保持兼容, 短期仍映射 `order_count`;
- 内部字段必须命名为 `*_order_count`, 不得继续叫 `*_cust_num`;
- 如产品需要“去重新客人数/老客人数”, 另外输出 `*_user_count`;
- 订单数可按新/老/未识别客群严格对平, 去重用户数不能跨分类简单相加。

### 8.8 订单增强事实

新增:

```text
t_calc_order_enriched
```

保持订单行粒度, 关联:

- 商品维度和报告品类;
- day_clear;
- 门店维度;
- 日历周;
- 用户首单维度;
- 加工原料角色;
- 渠道和子渠道。

`CustBuilder` 不再直接访问 QDM API, 只消费该本地事实。这使 `--fm-only` 真正离线且可重现。

---

## 9. 客数与层级汇总设计

### 9.1 t_fm_cust 新合同

粒度:

```text
store_id × business_date × day_clear × level_description × group_key
```

层级:

```text
门店 / 大类 / 中类 / 小类 / SPU / 黑白猪 / SKU
```

新增底项字段:

```text
order_count_cate
bf19_order_count_cate
sale_article_num_cate

online_order_count_cate
offline_order_count_cate
jielong_order_count_cate
jsd_order_count_cate

online_sale_amt
offline_sale_amt
jielong_sale_amt
jsd_sale_amt

online_qty
offline_qty
jielong_qty
jsd_qty

new_order_count
old_order_count
unknown_order_count
new_user_count
old_user_count
unknown_identity_order_count
new_cust_sale_amt
old_cust_sale_amt
unknown_cust_sale_amt
new_cust_qty
old_cust_qty
unknown_cust_qty
```

### 9.2 day_clear='2' 必须重新聚合

禁止:

```text
total_cust = day_clear_0_cust + day_clear_1_cust
```

正确做法:

```text
在原始订单增强事实上去掉 day_clear 维度后重新 COUNT DISTINCT canonical_order_key
```

同一订单同时买日清和非日清 SKU 时, 合计只算 1 个订单。

### 9.3 完整父路径键

内部聚合和 JOIN 不只使用末级 ID。

建议直接按完整维度列关联:

```text
大类: report_category_code
中类: report_category_code + category_level2_id
小类: report_category_code + category_level2_id + category_level3_id
SPU:  report_category_code + category_level2_id + category_level3_id + spu_id
SKU:  article_id
```

如果需要字符串 `group_key`, 只作为内部技术键, 对外 `level_id` 仍保持稳定业务含义。

### 9.4 可加总字段规则

以下字段在 SKU/订单层先算, 上层只能 SUM:

- 销售额;
- 销售数量;
- 销售件数;
- 渠道金额/数量;
- 新老客金额/数量底项。

比率、均值和 distinct 客数不能从下级简单相加或平均, 必须从正确底项重新计算。

---

## 10. 特殊损耗与利润调整

### 10.1 新增原子事实

建议新增:

```text
SpecialWastageExtractor -> atomic_special_wastage
```

最小合同:

| 字段 | 说明 |
|---|---|
| `store_id` | 必须有; 若源无此字段则阻断多店应用 |
| `business_date` | `DATE(created_at)` |
| `article_id` | source/raw SKU |
| `reason_code` | ccj_cost / raw_cooked_link |
| `waste_qty` | 数量 |
| `waste_amt` | 金额 |
| `source_inc_day` | 最新快照日期 |
| `source_record_id` | 可追溯主键 |

### 10.2 调整账本

新增影子表:

```text
t_calc_business_adjustment_shadow
```

粒度:

```text
adjustment_id × line_no × metric_account
```

字段:

```text
adjustment_id, line_no
store_id, business_date, day_clear
metric_account                    # profit / loss_qty / loss_amt
article_id, report_category_code
counterparty_article_id, counterparty_report_category_code
adjustment_type
signed_qty, signed_amt
affects_loss
affects_profit
is_conservative
rule_version
source_record_id
```

记账规则:

- `signed_qty/signed_amt` 为该目标 SKU/品类的有符号增量;
- 守恒转移必须至少生成 from/to 两行, 同一 `adjustment_id + metric_account` 内 `SUM(signed_amt)=0`, 数量可转换时同时要求定义单位后的 `SUM(signed_qty)=0`;
- 非守恒政策调整必须 `is_conservative=false`, 单独显示政策差异, 不得冒充会计守恒分录;
- 正式 `accounting_profit` 永远不消费非守恒行; 只有经审批的 `business_adjusted_profit` 影子口径才可展示该差异。

### 10.3 炒菜机成本

v1_5 做法是:

```text
损耗 -= ccj_waste
利润 += ccj_waste
```

这会提高总利润, 不是守恒转移。

在完整 ETL 中必须先回答:

1. 这是误记损耗, 应完全撤销?
2. 这是加工成本, 应转给哪个成品?
3. 是门店利润调整还是仅 KPI 展示调整?

在答案确认前:

- 可以生成影子“业务调整后损耗/利润”;
- 不改 `t_calc_stock` 和 `t_calc_profit` 的会计口径字段。

### 10.4 生熟联动

v1_5 意图是:

```text
原料类利润 + ssls_waste
熟食类利润 - ssls_waste
全店利润不变
```

完整 ETL 推荐优先转为加工关系:

```text
raw compose_out -> finished compose_in
```

只有拿不到成品 SKU 映射时, 才允许报告层分类重分配。即使报告层调整也必须满足:

```text
按 store_id × business_date × day_clear 守恒
SUM(signed_amt) by adjustment_id/metric_account = 0
每笔 from/to 至少两行且 counterparty 可追溯
```

禁止继续只按 business_date 汇总后对每个门店/日清分组重复扣减。

---

## 11. 核心成本库存改造与 DESIGN-001 的关系

### 11.1 DESIGN-001 是计算层前置基础

v1_5 新能力大多位于客户和输出侧, 可以与 DESIGN-001 并行开发; 但特殊损耗、加工成本和利润调整不能在旧计算链上继续打补丁。

计算层目标仍按 DESIGN-001:

```text
BomAllocCalculator
  -> 只产 BOM 关系、数量、比例计划

DailyCostStockCalculator 按日
  -> 读取前日状态
  -> 先确定数量流
  -> 用当日可用成本池定价 BOM/加工转出
  -> 计算库存数量/金额
  -> 同时产出 cost + stock + BOM daily amount

ProfitCalculator(start,end)
  -> 只消费已确定金额流
```

### 11.2 两种成本单价

必须区分:

| 字段 | 含义 |
|---|---|
| `issue_unit_cost` | 当日销售/BOM/加工/损耗转出单价 |
| `effective_unit_cost` | 期末库存金额 / 期末库存数量, 传给次日 |

### 11.3 前向失效范围

新增 affected range 计算:

```text
requested_start = 用户要求重刷起点
affected_start  = 最早发生输入/规则变化的日期
affected_end    = 数据最新业务日, 或状态收敛日
```

对成本/库存状态, 默认安全策略是从 `affected_start` 重放到最新业务日。

仅当满足以下收敛条件时才允许提前停止:

```text
所有 SKU 的 end_qty/end_amt 与旧版本完全相同
且之后规则快照没有变化
```

---

## 12. 运行治理与发布安全

### 12.1 run manifest

新增:

```text
etl_run_manifest
```

字段至少包括:

```text
run_id
requested_start, requested_end
affected_start, affected_end
stage
code_commit
category_rule_version
day_clear_rule_version
processing_relation_snapshot_at
source_snapshot_ids
status
started_at, finished_at
error_step, error_message
```

### 12.2 源表 preflight

每个 extractor 声明:

```text
required / optional
expected_grain
partition_column
minimum_count_rule
unique_key
pagination_key
```

preflight 不只检查 `COUNT > 0`, 还检查:

- 分区是否存在;
- 源行数是否异常突降;
- 关键字段 NULL 比例;
- 唯一键重复;
- 维度快照版本;
- 所有必需源是否同一天就绪。

### 12.3 副作用拆分

Step 14 从核心 `run()` 中移出:

```text
python -m fmetl.executor ...             # 只构建 DuckDB
python -m fmetl.publish ...              # 显式发布
python -m fmetl.sync_processing_candidates ...
```

或通过显式 `--publish` 开关授权。

`--fm-only` 必须做到:

- 不访问 QDM API;
- 不写服务器;
- 只消费已落地原子/计算事实。

### 12.4 影子发布

不要把测试状态编码进 `business_date='{date}-test'`。

推荐:

```text
t_fm_levels_result_shadow
或
publish_version = 'v0.12-shadow'
```

日期字段始终保持合法业务日期。

---

## 13. 结果层字段扩展

### 13.1 保持现有字段兼容

现有中文列不删除、不改名。新字段追加到结果表末尾。

### 13.2 新增渠道指标

```text
线上客数, 线下客数, 接龙客数, 及时达客数
线上销售额, 线下销售额, 接龙销售额, 及时达销售额
线上销售件数, 线下销售件数, 接龙销售件数, 及时达销售件数
线上客单价, 线下客单价, 接龙客单价, 及时达客单价
线上件单价, 线下件单价, 接龙件单价, 及时达件单价
线上单件数, 线下单件数, 接龙单件数, 及时达单件数
```

### 13.3 新增客群指标

```text
新客客数, 老客客数, 未识别客数
新客销售额, 老客销售额, 未识别客销售额
新客销售件数, 老客销售件数, 未识别客销售件数
新客客单价, 老客客单价
新客件单价, 老客件单价
新客单件数, 老客单件数
用户身份覆盖率
```

### 13.4 统一安全除法

所有 KPI 使用统一 helper:

```text
safe_div(numerator, denominator)
```

分母为 0 返回 NULL, 不返回 Infinity, 不静默返回 0。

### 13.5 销售额占比和排名

可以迁移 v1_5 的需求, 但必须定义清楚分母范围:

- 门店/大中小类: 同日期、门店、层级、day_clear;
- SPU/SKU: 是否按大类或中类分组由产品口径确认;
- 排名并列时使用 `DENSE_RANK` 还是强制 `ROW_NUMBER` 需确认。

---

## 14. 文件级改造清单

### 14.1 新增文件

| 文件 | 目的 |
|---|---|
| `fmetl/master_data/category_mapping.py` | 唯一品类映射实现 |
| `fmetl/master_data/day_clear_rules.py` | 唯一日清规则实现 |
| `fmetl/master_data/sku_roles.py` | 加工原料/成品角色 |
| `fmetl/config/master_data/frozen_skus.json` | 权威 126-SKU 清单 |
| `fmetl/atomic/order_offline_extractor.py` | 线下订单原子表 |
| `fmetl/atomic/order_online_extractor.py` | 线上订单原子表 |
| `fmetl/atomic/trade_user_extractor.py` | 用户身份原子表 |
| `fmetl/atomic/special_wastage_extractor.py` | 特殊损耗观测 |
| `fmetl/calculated/order_enrichment.py` | 渠道/客群/分类增强事实 |
| `fmetl/calculated/business_adjustment.py` | 特殊调整影子账本 |
| `fmetl/calculated/daily_cost_stock.py` | DESIGN-001 每日状态机 |
| `fmetl/validation/contracts.py` | 表粒度和字段合同 |
| `fmetl/validation/run_checks.py` | 运行验收和阻断规则 |
| `tests/` 对应单元/集成测试 | 自动化验收 |

### 14.2 修改文件

| 文件 | 调整 |
|---|---|
| `executor.py` | preflight、run_id、affected range、按阶段失败、移出发布副作用 |
| `connectors/api_connector.py` | 可证明完整的分页 |
| `connectors/duckdb_store.py` | 空分区语义、staging、事务、BY NAME |
| `atomic/_base.py` | required/optional contract、空分区清理、唯一键检查 |
| `atomic/dims_extractor.py` | 只构建规则快照, 不重复分类 CASE |
| `calculated/merge.py` | 用全事件 spine, day_clear 不再做状态身份 |
| `calculated/bom_alloc.py` | 数量计划化、共享组连通分量、零/负权重处理 |
| `calculated/sku_cost.py` | 逐步退为兼容 wrapper |
| `calculated/stock.py` | 逐步退为兼容 wrapper, transfer 进入方程展示 |
| `calculated/profit.py` | 增加日期参数, 只消费状态机金额流 |
| `fm_tables/sku_dim.py` | 使用共享分类/角色, 增加规则版本 |
| `fm_tables/cust.py` | 改为消费本地 order_enriched, 增加 SKU/渠道/客群 |
| `fm_tables/levels_sum.py` | 完整键、正确 distinct 合计、追加底项 |
| `fm_tables/levels_result.py` | 新 KPI、安全除法、排名 |
| `fm_tables/matnr_result.py` | 使用 SKU 客数, 修复当前 0 客数断链 |
| `fm_tables/stock_roll.py` | 展示 transfer 并修复 balance_qty |
| `fm_tables/bom_breakdown.py` | 金额读取 daily state 结果 |

### 14.3 文档同步

实现时必须同步:

- `CLAUDE.md` / `AGENTS.md` 中实际步骤和边界;
- `fmetl/README.md`;
- `fmetl/atomic/README.md`;
- `fmetl/calculated/README.md`;
- `fmetl/fm_tables/README.md`;
- `fmetl/docs/architecture/ETL_v0.12_完整处理逻辑.md`;
- 字段手册;
- fixes/reviews 索引。

---

## 15. 实施顺序

### Phase 0: 口径冻结与阻塞项关闭

目标: 不写业务代码前先补齐无法从代码推断的业务事实。

必须完成:

1. 获取权威 126 个冷冻 SKU 清单。
2. 确认“新客”是周内新客还是当日新客。
3. 确认“及时达”正式中文名称是否应为“即时达”。
4. 确认 offline/online 订单源表主键和退款符号。
5. 确认特殊损耗源是否只有 A3XV, 是否能提供 store_id。
6. 明确炒菜机成本的去向。
7. 明确生熟联动是否有 raw -> finished SKU 映射。
8. 确认当前项目门店范围: 全部 food mart 还是仅 A3XV/广州。
9. 确认 `bf19_sale_amt >= 500` 是否是正式有效营业日规则。
10. 确认平台字段“客数”继续表示订单客次, 还是改为去重用户数。

交付物:

- 业务口径确认表;
- 源表唯一键/符号审计;
- master-data 快照。

### Phase 1: 运行安全加固

先修:

- API 分页;
- 原子抽取失败阻断;
- 合法空分区清理;
- Profit 日期闭环;
- staging + transaction;
- run manifest;
- Step 14 副作用拆分。

验收: 人为制造一个 extractor 失败, 正式表不得发生部分更新。

### Phase 2: 主数据统一

实现共享 category/day_clear/sku_role 模块, 让以下调用者全部切换:

- dims override;
- sku_dim;
- cust;
- processing candidates;
- QDM compare;
- tests。

验收:

- 所有调用者同一 SKU 输出完全一致;
- frozen 清单唯一数为 126;
- 无重复 CASE/硬编码清单。

### Phase 3: 订单与客户链

实现:

- 3 个订单/用户 extractor;
- 全历史 first-order bootstrap;
- order_enriched;
- 新 t_fm_cust;
- SKU 客数;
- 渠道/客群底项;
- day_clear='2' 重新 distinct。

先写影子表:

```text
t_fm_cust_v012_shadow
t_fm_levels_sum_v012_shadow
```

### Phase 4: DESIGN-001 每日成本库存状态机

按 DESIGN-001 实现并补充:

- 状态 key 不依赖 day_clear;
- 全事件 spine;
- 前向失效重放;
- transfer 进入数量/金额验证;
- 原子发布 cost/stock/BOM amount。

### Phase 5: 特殊损耗影子账本

先产出:

```text
accounting_profit
business_adjusted_profit_shadow
accounting_loss
business_adjusted_loss_shadow
```

只有守恒、门店粒度和业务去向全部验证后才讨论切正式口径。

### Phase 6: 结果层扩展与影子对比

追加渠道/客群 KPI、排名和占比, 保持现有列兼容。

对比:

- fmetl v0.11;
- fmetl v0.12 shadow;
- 公司 QDM 基准;
- v1_5 结果。

### Phase 7: 切换与回刷

1. 先单日影子。
2. 再连续 7 日。
3. 再完整月。
4. 再全历史回刷。
5. 本地验证通过后同步服务器 DuckDB。
6. 最后切平台读取表/视图。

---

## 16. 验收标准

### 16.1 抽取完整性

```text
required source partitions ready = 100%
source unique key duplicates = 0
API extracted count = source count
stale partitions after legal-empty refresh = 0
partial publish after failure = 0
```

### 16.2 主键与层级

```text
t_atomic/t_calc/t_fm 声明键重复 = 0
store/date/article 的 day_clear distinct <= 1
中/小/SPU 跨父级碰撞挂错 = 0
SKU 客数可回挂率 = 100%
```

### 16.3 渠道对平

按 store/date/day_clear/level:

```text
online_sale_amt + offline_sale_amt = total_sale_amt, tolerance 0.01
jielong_sale_amt + jsd_sale_amt = online_sale_amt, tolerance 0.01
online_qty + offline_qty = total_qty, tolerance 0.001
jielong_qty + jsd_qty = online_qty, tolerance 0.001
```

若业务存在无法归类渠道, 必须新增 `unknown_channel_*`, 不允许差额消失。

### 16.4 客群对平

```text
new_order_count + old_order_count + unknown_order_count = total_order_count
new/old/unknown user_count 按去重用户口径另行校验, 不与订单数混算
identity coverage rate 单独输出
first_order_date <= current business_date
历史 bootstrap 前后的老客比例变化可解释
```

客数是 distinct 指标, 不要求各分类客数之和等于门店客数; 但同一层级同一分组必须从订单事实可重算。

### 16.5 库存和成本

```text
end_stock_qty < 0 count = 0
当前 v0.11 兼容口径:
  Σbom_in_amt - Σbom_out_amt - Σstock_transfer_in_amt ≈ 0

目标 v0.12 库存数量残差:
  init + receive + bom_in - bom_out
  + compose_in - compose_out
  + stock_transfer_in - stock_transfer_out
  - sale - know_lost - end - unknow_lost = 0

目标 v0.12 库存金额残差按同样流向符号 ≈ 0
Σcompose_in_amt - Σcompose_out_amt ≈ 0  # 按 store/date 校验
实现必须保证 transfer 只记一次, 不得同时嵌入 bom_in 又记显式 transfer_in
父品残余库存/利润满足设计规则
零成本有销售行数量在阈值内且全部有 cost_source
历史重刷后 D+1 期初 = D 期末
```

### 16.6 特殊调整

生熟联动:

```text
SUM(signed_amt) by adjustment_id/metric_account = 0
每笔 from/to 至少两行且 counterparty 完整
不得改变全店 accounting_profit
```

炒菜机:

- 若认定为撤销误记: 必须有业务审批和源记录追溯;
- 若认定为成本转移: 必须有目标成品和守恒验证;
- 未确认时不得修改正式 accounting_profit。

### 16.7 QDM 核心验收

使用生产服务器 DuckDB 和 StarRocks 基准:

1. 销售额按门店/日期对平。
2. 门店 × 大分类毛利差异默认进入 ±5%。
3. 门店 × 日期总毛利差异给出差异额和差异率。
4. SKU 差异记录但不作为唯一阻断项。
5. 两边应用同一 master-data 规则快照。
6. 明确标注结果来自服务器生产数据。

### 16.8 回归要求

客户/渠道功能上线时, 在未启用特殊利润调整的前提下:

```text
现有 SKU 销售/库存/成本/毛利底项不得变化
```

这条把“结果侧能力扩展”和“核心会计口径变化”隔离开。

---

## 17. 待业务确认项

以下问题无法从代码可靠推断, 实施前必须由业务/数据负责人确认:

| 编号 | 问题 | 不确认的风险 |
|---|---|---|
| Q1 | 权威 126 冷冻 SKU 清单在哪里 | 分类继续不一致 |
| Q2 | v1_5 所谓 master-data v2.5 是否存在正式文件 | 误把实验规则当权威 |
| Q3 | 新客是周内新客还是当天/历史首次新客 | 指标名称误导 |
| Q4 | “及时达”还是“即时达” | 对外字段命名不统一 |
| Q5 | offline/online 源表唯一键是什么 | JOIN/回刷放大 |
| Q6 | refund/return 的 sales_amt/qty 是否已带负号 | 渠道净额错误 |
| Q7 | 特殊损耗是否只属于 A3XV | 多店复制金额 |
| Q8 | 炒菜机成本应撤销还是转给成品 | 总利润口径错误 |
| Q9 | 生熟联动是否有 raw->finished 映射 | 只能做分类补丁 |
| Q10 | 有效营业日是否要求 bf19 销售 >= 500 | 门店日范围不一致 |
| Q11 | fmetl 目标范围是全部 food mart 还是仅广州/A3XV | 数据范围不一致 |
| Q12 | SPU/SKU 排名使用 ROW_NUMBER 还是 DENSE_RANK | 并列排名行为不同 |
| Q13 | “客数”是订单客次还是去重购买用户数 | 客单价和新老客口径混淆 |

---

## 18. 明确不做的事情

1. 不把 `strategy_fm_full_link_article_di` 的公司利润接入 fmetl 核心计算。
2. 不把 `strategy_fm_chdj_article_di` 的公司库存/损耗当成本项目真值。
3. 不复制 v1_5 两份 119-SKU CASE。
4. 不在分类汇总层直接修改 SKU 会计毛利且不给追溯。
5. 不继续用日期加 `-test` 表示发布版本。
6. 不在核心 ETL 默认执行 SCP/SSH/外部 SQLite 写入。
7. 不在首单历史不完整时发布正式新老客指标。
8. 不用本地 DuckDB 结果冒充生产验证。
9. 不在一次提交中同时切成本状态机、特殊利润口径和客户结果层。

---

## 19. 最终建议

推荐路线不是“把 v1_5 更新到 fmetl”, 而是:

```text
先把 fmetl 的运行合同和主数据做稳
    ↓
迁移 v1_5 的订单/渠道/客群能力
    ↓
按 DESIGN-001 收拢成本库存状态机
    ↓
把特殊损耗建成可守恒、可追溯的调整账本
    ↓
最后扩展兼容结果层并影子切换
```

这条路线保留 v1_5 已验证的业务需求, 同时不破坏本项目“完整重构公司 ETL”的目标。

判断把握:

- v1_5 不能直接作为 fmetl 上游: **95%**。
- 客户/渠道能力可以独立迁移且不改变核心利润: **90%**。
- 分类必须先统一为单一来源: **95%**。
- 特殊损耗必须先建守恒账本再入正式口径: **90%**。
- 当前架构应先做运行安全加固再扩字段: **90%**。
