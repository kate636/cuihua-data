# DESIGN-003: fmetl v0.13 全新重构执行设计

> 日期: 2026-07-15
> 状态: 已确认, 进入实施
> 目标版本: v0.13
> 上游边界: `huajia_yonghong_etl/versions/v1_5/sync_strategy_fm.sh`
> 业务范围: 滨江宏岸店 `A3XV`
> 关联设计: `DESIGN-001-daily-cost-stock-state-machine.md`
> 替代: `DESIGN-002-v1_5-capabilities-integration.md`

---

## 1. 已冻结决策

1. 新 ETL 不在旧 `fmetl` 上继续修补。旧目录完整归档, 重新建立 v0.13 目录、数据合同和测试。
2. 所有公司数据只从 StarRocks 镜像层读取, 不直连 Hive。镜像同步权威脚本为 v1_5 `sync_strategy_fm.sh`。
3. “镜像层权威”是字段级边界, 不等于公司派生利润/库存可重新进入核心计算。
4. 范围固定为 `store_id='A3XV'`。出现其他门店直接阻断运行。
5. 有效营业日与 v1_5 一致: `strategy_fm_store_daily_di.bf19_sale_amt >= 500`。
6. 平台“客数”是订单客次: `COUNT DISTINCT canonical_order_key`, 不是去重用户数。
7. 新老客与 v1_5 一致: 首单日早于周一为老客, 否则为本周新客; 无身份映射为“其他”。
8. 特殊损耗的业务展示值按 v1_5 公式实现, 同时保留调整前值和追溯字段。
9. 运营 KPI（售罄、上架 SKU、排名、销售占比、品效）不在本次迁移范围。
10. 报告品类精确采用 v1_5 CASE 顺序和 119 个冷冻 SKU 清单, 但只实现一份共享规则。
11. 猪肉多产出拆分、蔬菜包装换码、熟食/烘焙配方加工和 matnr 成本参考必须分流, 不再统称 BOM。
12. DESIGN-001 的“按日成本库存状态机”继续作为核心, 但关系解析和数量流来源以本文修订为准。

---

## 2. 事实核对结果

### 2.1 镜像层

`sync_strategy_fm.sh` 实际同步 28 张 StarRocks 表, 全部限定 A3XV, 包括:

- 销售、进货、SCM、损耗、加工、促销、让利;
- 日清、商品、门店、日历、可订可售维度;
- receive_sale、订验关系、单位换算、BOM 关系边、盘点明细;
- offline/online 订单明细和 trade user;
- full_link、store_daily、article_sale、chdj_article 公司结果表。

v0.13 不把 Hive 表名写入运行 SQL。Hive 血缘只在 contract 和文档中保留。

特殊损耗沿用 v1_5 所读的 StarRocks 原生业务表
`default_catalog.ads_business_analysis.cuihua_t_purchase_wastage`。它不是上述 28 张
Hive 镜像目标，因此在 registry 中明确标为 `managed_by_sync_script=false` 的辅助观测源，
不能伪装成 `sync_strategy_fm.sh` 的同步目标；除这一项 v1_5 既有原生源外，核心事实与维度
均来自该脚本维护的镜像表。

可订可售的权威源明确为同步脚本第 26 步生成的
`strategy_fm_dim_order_saleable`（Hive 血缘
`dim_store_article_order_sale_info_di`），日粒度业务键为
`store_id + inc_day + article_id`。v1.5 `step1_flag_sku_di.sql` 只消费
`saleable` 做售罄展示，但镜像同时提供 `is_order`。v0.13 分别保存：

```text
is_order  -> is_orderable（可订）
saleable  -> is_saleable（可售）
status    -> saleability_status（源状态）
```

三列不得合并。`strategy_fm_purchase_order_tmp` 是下单商品池和订货参数快照，
不是最终可订可售标记，也不进入库存成本池。

### 2.2 订单符号（2026-07-01 至 2026-07-14, StarRocks 镜像）

线上:

- completed: 19,399.46 元, 全部非负;
- refund.completed: -635.75 元 / -121 件, 负数行 112, 无正数行;
- return.completed: -65.09 元 / -6 件, 全部负数;
- split: +602.03 元 / +67 件。

线下:

- completed: 330,151.33 元;
- return.completed 明细有正负成对行, 订单级合计 -1,252.91 元 / -65 件;
- split: +2,546.78 元 / +230 件。

结论:

> `sales_amt` 和 `qty` 已是有符号事件值。v0.13 原样求和, 禁止根据 `order_status` 再次乘 -1。

运行质量检查仍要监控 refund/return 每日净额符号, 但异常时阻断, 不自动翻号。

### 2.3 订单键（同期 StarRocks 镜像）

- offline: 36,332 行; `store+day+order+SKU` 仅 35,325 个;
- online: 2,790 行; 同样候选键仅 2,765 个;
- 加入 status/pay_at 不增加唯一性;
- `serial_id` 不是行主键;
- 近 14 日 trade_user 的 `inc_day+order_id` 为 13,120/13,120 唯一, 但 contract 不假定永久唯一。

因此:

```text
客次业务键 = channel + store_id + business_date + order_id
原子行技术键 = source_row_hash + duplicate_ordinal
```

`duplicate_ordinal` 保留完全相同的多行性。不能为了制造唯一键而丢弃数量/金额。

### 2.4 加工事实

2026-07-01 至 07-14 的 `strategy_fm_compose_di`:

| 源大类 | 行数 | SKU | compose_in_qty | compose_out_qty | in_amt | out_amt |
|---|---:|---:|---:|---:|---:|---:|
| 冷藏及加工类 | 550 | 51 | 2,088 | 362.29 | 8,017.20 | 8,017.18 |
| 预制菜 | 96 | 8 | 371 | 185.50 | 7,393.60 | 7,393.60 |

这证明源 compose 不是空观测。新 ETL 数量流优先使用它, 配方用于把 raw/finished 连成组和解释缺侧; 销售/库存反推降为告警或影子估算。

### 2.5 猪肉 BOM 与蔬菜包装换码

receive_sale 近 14 日跨 SKU 关系:

- 猪肉类: 8 个活跃 parent、75 个 sub、930 行, 是典型多产出拆分;
- 蔬菜类: 8 个 parent、8 个 sub、34 行, 是典型一对一包装/换码。

蔬菜对能在 `strategy_fm_dim_article_convert` 找到明确换算:

- 西葫芦 kg -> 西葫芦 500g: `parent_rate=2`, `sub_rate=0.5`;
- 黄秋葵 kg -> 250g: `4 / 0.25`;
- 普罗旺斯番茄 kg -> 500g: `2 / 0.5`;
- 红薯 25kg/件 -> kg: `25 / 0.04`。

`strategy_fm_dim_article_convert` 在 2026-07-01 至 07-14 有 77,998 条唯一快照关系, 不是空骨架。

---

## 3. 镜像字段级权威矩阵

| 镜像 | 可进核心的字段 | 禁止作为重构真值的字段 |
|---|---|---|
| sales_di | 销售数量/金额、订单观测 | 公司派生利润 |
| purchase_di | 门店销售 SKU 进货 `sale_article_qty/sale_article_purchase_amt`；源期初作首日基线 | 用 `avg_inbound_price/inventory_cost` 覆盖当日真实进货额；上游未知损耗推导 |
| scm_di | 门店订货、DC 实际出库、应付额、SCM 账面成本、已带符号退仓 | 把 DC 出库/采购成本再次记作门店库存进货；公司全链路利润 |
| loss_di | 登记已知损耗 | 方程未知损耗 |
| compose_di | 实际 compose in/out qty; 源金额作对照 | 无追溯的成本覆盖 |
| bom_relation | parent/sub、rate、split_mode、unit | 不经分流的“全部都是 BOM”假设 |
| article_convert | 包装/单位换算比例 | 无实际转换事件时虚构数量流 |
| receive_sale | 兼容 parent/sub 当日计划 | 长期宣称为原始事实; 它是 DAL 派生表 |
| order offline/online | 有符号金额/数量、订单客次、渠道 | 强制去重的商品行 |
| trade_user | 订单身份和首单日 | 未去重直接 JOIN 订单明细 |
| store_daily | A3XV 有效营业日 | SKU 销售事实 |
| dim_order_saleable | `is_order`、`saleable`、`status` 及订货基数 | 库存数量、采购/进货成本 |
| purchase_order_tmp | 下单商品池、订货参数的参考快照 | 最终可订可售标记、采购/进货事实 |
| chdj_article | day_clear/业务标签的兼容参考 | profit、stock、loss、avg7d |
| full_link/article_sale | 验收和影子对比 | 核心利润/库存上游 |

DuckDB `mirror_*`/`atomic_*` 是这些授权字段的幂等本地副本, 不发明新业务事实。

### 3.1 采购、出库、进货三套账

```text
供应链订货:
  qty = store_order_qty / order_qty_payean
  amt = order_amt

DC 实际出库及供应链财务:
  qty = total_outstock_qty
  结算/收入 = out_stock_pay_amt
  供应链成本 = out_stock_amt_cb

门店销售 SKU 进货（库存池唯一外部入流）:
  qty = purchase_di.sale_article_qty
  amt = purchase_di.sale_article_purchase_amt
```

SCM 退仓字段已经是负数，原样进入 SCM 净额对账，禁止再次翻号。它们默认不直接
改变门店库存成本池；门店实物退仓必须有独立的门店事件证据。

`receive_sale` 的 parent `inbound_qty/inbound_amount` 会在每个 child 行重复，只有在
猪肉/包装采用 parent 重构模式时，才可先按 parent 做一致性检查和去重后形成一次外部
进货。此时同一 parent 的 `purchase_di` child 分配行只作 shadow 对账，不能再次入池。

### 3.2 SKU 日事实的符号与标签

2026-07-08 至 07-14 镜像实查确认：

- `sales_di.qty_spec/sales_amt` 的退货事件已经出现负值；
- `return_sale_qty/return_sale_amt` 也已带负号，但部分业务退货会拆成“负销售行 +
  qty=0 的退货标签行”，不能把两套字段都记作库存退回；
- 正式库存数量按 `qty_spec` 正负拆为非负 `gross_sale_qty` 和 `sale_return_qty`，源
  return 字段仅作业务标签审计；收入保留 `net_sale_amt=SUM(sales_amt)`；
- `loss_di.know_lost_qty` 是正式已知损耗数量，源已知/未知损耗金额和未知数量仅作对照；
- 库存明细的合法非负 `actual_stock_qty` 是期末余额观测，均可进入库存状态机；
  `created_by/updated_by` 只记录操作人证据，不能用 `created_by=系统` 反推“没有人工盘点”；
- 系统负库存快照无业务意义，阻断其覆盖；`inventory_date` 必须等于 `inc_day`，并校验
  `profit_loss_qty = actual_stock_qty - sale_stock_qty`；
- 日清兼容标签来自 `strategy_fm_chdj_article_di.day_clear`。

`strategy_fm_dim_day_clear` 一周每天约 9.1 万 SKU，实查为全商品日快照；它没有
`day_clear` 字段，禁止用“存在记录”推导 `day_clear=0`。

盘点来源存在不可逆的信息损失。2026-07-14 的 SKU `21279829` 是已确认样例：
`created_by=updated_by=系统` 且快照三数量均为 0，但当天外部进货 10、销售 2、已知
损耗 0，业务确认该快照来自实盘。v0.13 因而不猜测操作者，而是将合法快照作为期末
余额事实，由自身状态机计算 `unknown_lost_qty = 10 - 2 - 0 = 8`。源
`loss_di.unknow_lost_qty=8` 仅用于对照，禁止直接过账。该政策可能吸收上游快照的时点
差异，必须在周测中单列“系统余额覆盖产生的未知损耗”并与 v1.5 对照。

---

## 4. v0.13 全新目录

```text
fmetl/
├── __init__.py
├── cli.py
├── config/
│   ├── settings.py
│   └── v1_5_category_rules.json
├── contracts/
│   ├── mirror.py
│   ├── grains.py
│   └── quality.py
├── connectors/
│   ├── qdm_api.py
│   └── duckdb_store.py
├── mirror/
│   ├── extract.py
│   └── registry.py
├── master_data/
│   ├── category.py
│   ├── day_clear.py
│   └── valid_business_day.py
├── relations/
│   ├── snapshots.py
│   ├── resolver.py
│   └── graph.py
├── facts/
│   ├── sku_day.py
│   ├── orders.py
│   ├── bom_plan.py
│   ├── processing_plan.py
│   └── pack_plan.py
├── calculations/
│   ├── daily_cost_stock.py
│   ├── profit.py
│   ├── customers.py
│   └── special_wastage.py
├── outputs/
│   ├── sku.py
│   ├── levels.py
│   ├── customers.py
│   ├── matnr.py
│   ├── bom_breakdown.py
│   └── stock_roll.py
├── validation/
│   ├── preflight.py
│   ├── balances.py
│   └── comparison.py
└── docs/
```

旧 `fmetl` 保存到:

```text
_archived/fmetl_v0_11_20260715/
```

新目录不导入归档代码。需要参考时只通过文档/测试写清业务合同。

---

## 5. 运行和抽取合同

### 5.1 run manifest

每次运行记录:

```text
run_id, version, git_commit
requested_start/end, affected_start/end
store_id=A3XV
mirror_sync_script_version/checksum
category_rule_version/checksum
relation_snapshot_id/checksum
status, failed_step, error
```

### 5.2 幂等分区

1. 先抽到 staging;
2. 检查行数、NULL、符号、粒度和 store;
3. 合法空分区也必须删除旧分区;
4. 所有 required 源通过后才在一个 DuckDB 事务中发布;
5. 任一源失败, 正式表不变。

### 5.3 API 分页

不再用“结果第一列”自动 keyset。每张镜像 contract 必须声明:

```text
partition_column
projection allowlist
stable_order_columns
expected_grain
required/optional
```

无可证明唯一排序键的大表按日和门店继续分片, 不伪造 keyset 完整性。

---

## 6. v1_5 分类单一实现

### 6.1 决策冲突声明

2026-07-15 服务器 `/opt/fm/主数据/category-mapping.md` 仍是 v2.3/126 SKU。用户本轮明确指定 v0.13 与 v1_5 一致, 因此本版采用:

```text
category_rule_version = v1_5-frozen-20260715
frozen SKU unique count = 119
```

它是 v0.13 已批准业务规则, 不冒充服务器 v2.3, 也不宣称是未存在的“v2.5”。

### 6.2 精确顺序

1. 119 SKU -> 冷冻类;
2. 标品类+冰品类 -> 冷冻类;
3. 蛋、烘焙、冷藏乳品、水饮、牛羊、禽类;
4. 标品拆为基础食品、休闲食品、日杂用品;
5. 源熟食类 -> 熟食类;
6. 冷藏及加工/预制菜+即食类 -> 熟食类;
7. 同源大类+即烹/即热/米面制品+销售单位千克 -> 熟食类;
8. SKU `21315626` -> 熟食类;
9. 小类名以“熟食”结尾 -> 熟食类;
10. 其余冷藏及加工/预制菜 -> 冷藏加工及预制菜类;
11. 其他保留源大类。

119 清单只存 JSON, category/order/SKU/result 全部调同一函数。

---

## 7. 订单、渠道和新老客

### 7.1 原子行

```text
atomic_order_event
grain = source_channel + source_row_hash + duplicate_ordinal
```

保留:

```text
business_date, inc_day, store_id
order_id, article_id, order_status
pay_at, jielong_flag
sales_amt, qty
thirdparty_user_identity
source_channel
canonical_order_key
source_row_hash, duplicate_ordinal
```

`canonical_order_key` 不修改原 order_id:

```text
offline|A3XV|business_date|order_id
online|A3XV|business_date|order_id
```

### 7.2 身份映射与首单

sync 脚本当前把 order 与 trade 按 order_id 直接 JOIN。v0.13 抽取时要求:

1. 先把 trade_user 压到 `inc_day+order_id` 一行;
2. 同一订单多身份直接阻断;
3. JOIN 前后订单行数相等;
4. 首次建表扫描可用全历史 `MIN(DATE(trade_time))`;
5. 每日增量与旧值取更小日期。

### 7.3 客次和渠道

```text
客次: order_status='os.completed' 的 COUNT DISTINCT canonical_order_key
金额/数量: completed + split + refund.completed + return.completed 的有符号求和
```

```text
order_channel = online/offline
jielong = jielong_flag != '-'
及时达 = online AND jielong_flag = '-'
```

“及时达”保持 v1_5 现有字段名称, 不在本次改名。

### 7.4 新老客

```text
first_order_date < week_start_date  -> 老客
first_order_date >= week_start_date -> 新客
first_order_date IS NULL            -> 其他
```

新/老客客数仍是完成订单客次, 不是用户人数。“其他”只做完整性审计, 兼容结果只输出新客/老客。

### 7.5 day_clear 合计

`day_clear='2'` 必须在订单事实上去掉 day_clear 后重新 distinct, 不可相加 0/1 的客次。

---

## 8. 关系解析: BOM、加工、包装、物料码

### 8.1 关系快照

```text
dim_bom_edge_snapshot
dim_article_convert_snapshot
dim_processing_recipe_snapshot
dim_matnr_member_snapshot
t_calc_relation_resolution
```

加工关系每次运行开始时从 Foodmart 经营监控平台公开代理
`/api/proc-rel/export` 导出一次, 写入 DuckDB 快照并记录 checksum。同一 run 不再
访问可变外部状态。不得直连平台内部 `:5003` 端口；不得在请求失败时静默读取历史 JSON。

### 8.2 唯一关系类型

每个 from/to 对每日只能是以下一种:

```text
SELF_RECEIVE
DISASSEMBLY_BOM
PACK_CONVERT
RECIPE_COMPOSE
PROCUREMENT_ALIAS
UNRESOLVED
QUARANTINED
```

解析顺序:

1. parent=sub -> SELF_RECEIVE;
2. 已审批 processing recipe -> RECIPE_COMPOSE;
3. article_convert 有有效双向比例, 且属于一对一包装/换单位 -> PACK_CONVERT;
4. BOM edge 为一拆多/父品 fanout>=2, 且经品类形态允许 -> DISASSEMBLY_BOM;
5. order_receive 只证明订货码->验收码 -> PROCUREMENT_ALIAS;
6. 证据不足 -> UNRESOLVED, 不产生正式内部流;
7. 同时命中多种 -> QUARANTINED。

### 8.3 猪肉 DISASSEMBLY_BOM

保留当前合理语义:

- 整猪/白条 parent -> 多个部位 sub;
- 父品单位数量与子品单位数量分开;
- 子品自购和 BOM 流不重复;
- 共享父品使用二部图连通分量, 不再两两子集配对;
- parent 转出金额使用当日 `issue_unit_cost`;
- `consume_all_parent` 父品期末为 0、独立利润为 0;
- 剩余必须生成显式 residual transfer, 不直接改 end_stock。

数量计划优先级:

1. receive_sale `sale_article_qty` 作为 v0.13 兼容桥, 标记 `UPSTREAM_DAL_RECEIVE_SALE`;
2. purchase + BOM relation + dressing rate 重建 shadow;
3. 销售/损耗权重反推只作差异分析。

成本分摊优先级（金额始终守恒）:

1. 当日源 child 拆分金额完整、全部活跃 child 有正金额时，使用源拆分金额比例；
2. 零价赠品或同一 parent 出现有价/零价混合 child 时，全组使用
   `sale_recev_rate`，不能只让零价 child 继承零成本；
3. 前两项不可用时，只有所有活跃部位已可靠换算到同一库存计量单位（例如全部为 kg）
   才可按标准重量分摊父品总成本。此时各部位取得相同的公共单位成本：
   `child_in_amt = normalized_child_qty * parent_total_cost / sum(normalized_child_qty)`；
4. 单位换算覆盖不足，尤其“件”和“公斤”无法可靠互换时，进入 quarantine，不得直接
   假定每件等于每公斤。

当前构建里程碑已实现第 1、2 层；第 3 层尚缺“部位库存数量 -> 公共标准重量”的 100%
覆盖 adapter，代码会按第 4 层 fail closed。该 adapter 完成并通过真实周测前，不得宣称
“同公共单位成本”已进入正式过账。

“先算猪肉分类毛利率，再把同一毛利率回填每个部位”只允许作为报表展示分摊口径。
它通过销售额反推 SKU 成本，会形成循环并污染跨日库存金额，因此禁止写入核心库存成本、
SKU 会计毛利或下一日期初。展示表使用时必须保留 `allocation_basis=REPORT_ONLY_CATEGORY_MARGIN`。

### 8.4 蔬菜/水果 PACK_CONVERT

- article_convert 优先于 matnr/name 推断;
- 必须按 `store_id + business_date + parent + sub` 使用当天镜像快照，禁止借用其他日期比例;
- 有实际 receive_sale/compose 换码事件才产生 pack out/in;
- 无事件时不因关系存在而虚构数量;
- 按公共重量单位守恒;
- 不使用猪肉销售价值权重分摊;
- 无效/互逆不一致比例进 quarantine。

### 8.5 RECIPE_COMPOSE

数量来源:

1. `atomic_compose` 实际 in/out qty;
2. 一侧实际数量 + 快照配方推导缺侧;
3. 只对显式允许的日清加工 SKU, 销售/库存平衡反推作 shadow;
4. 两侧都无观测时不默认生产量=销售量。

金额:

```text
raw_out_amt = raw_out_qty * raw_issue_unit_cost
sum(finished_in_amt) = sum(raw_out_amt)
```

正式配方成本覆盖率必须 100%。部分原料零成本只进 shadow, 不进会计成本。

### 8.6 matnr

matnr 只是 SAP 物料身份, 本身不产生数量流。

- 换算有 article_convert 时用 article_convert;
- 无实际事件只能用于成本对照/有来源的单位换算;
- 禁止跨店 sibling EUC 进正式成本;
- 同 matnr 跨报告品类不能 `MAX(category)` 任取;
- matnr 结果按 `(matnr, report_category_code)` 聚合, 或对品类唯一性做阻断检查。

---

## 9. 每日成本库存状态机

状态身份:

```text
store_id + article_id
```

当日事实:

```text
store_id + business_date + article_id
```

`day_clear` 是当日属性, 不是跨日状态键。这一定义覆盖 DESIGN-001 中任何把 day_clear 放入状态键的旧表达。

每日顺序:

```text
1. D-1 end -> D init
2. self receive / procurement alias
3. DISASSEMBLY_BOM
4. PACK_CONVERT
5. RECIPE_COMPOSE
6. sale / known loss
7. inventory count / day_clear / negative protection
8. end qty and end amount
9. profit consumes finalized amount flows
```

软日清补充约束：当 `new_supply - sale - known_loss < 0` 时，负的
`unknown_lost_qty` 仅作为“消耗期初库存”的展示/追溯值；期初消耗已包含在 `end_qty`，
不能再把这个展示值直接第二次过账到库存方程。计算层另存
`balance_unknown_qty = eq_qty - end_qty`，金额同理保存
`balance_unknown_amt = eq_amt - end_amt`；平衡校验使用过账字段，损耗展示使用原字段。

符号进入状态机前必须拆流：订单事实继续无损保留退款/退货负号；库存状态的
`sale_qty`/`known_lost_qty` 只表示非负出流，退货和盘盈分别进入非负的
`sale_return_qty/amt`、`inventory_gain_qty/amt`。禁止把带符号订单净额直接塞入库存出流，
也禁止静默假设每日净值一定非负。

两种成本:

```text
available_qty       = D-1 end_qty + 当日门店进货 + 正式内部流入 + 实物退货/盘盈
available_amt       = D-1 end_amt + 当日门店进货额 + 正式内部流入额 + 实物退货/盘盈额
issue_unit_cost     = available_amt / available_qty
内部/销售/损耗转出 = 对应数量 * issue_unit_cost
ending_unit_cost    = end_stock_amt / end_stock_qty, 传给次日
```

镜像事实只有日粒度，因此这是“跨日滚动、日内期间加权”，不是按单据时间排序的实时
移动平均。D+1 必须直接复制 D 的 `end_qty/end_amt`，禁止用展示时四舍五入后的
`ending_unit_cost` 反算金额。

任一历史输入/规则/关系快照改变, 从受影响日重放至最新日, 除非能证明所有 SKU 期末数量和金额已收敛一致。

---

## 10. v1_5 特殊损耗正式口径

`cuihua_t_purchase_wastage` 没有 store_id。因 v0.13 仅有 A3XV, 抽取时固定赋值 `store_id='A3XV'`, 不再作为阻断。

保留原值:

```text
accounting_lost_amt
accounting_lost_qty
accounting_known_lost_amt
accounting_profit
```

v1_5 调整值:

```text
adjusted_lost_amt       = accounting_lost_amt - ccj_amt - ssls_amt
adjusted_lost_qty       = accounting_lost_qty - ccj_qty - ssls_qty
adjusted_known_lost_amt = accounting_known_lost_amt - ccj_amt - ssls_amt
adjusted_profit         = accounting_profit + ccj_amt
adjusted_full_profit    = accounting_full_profit + ccj_amt
```

生熟联动报告层:

```text
来源 SKU/来源大类 adjusted_profit += ssls_amt
熟食类 adjusted_profit -= same store/date total_ssls_amt
```

兼容要求:

- 每笔调整保留 source_record_id、SKU、日期、reason、qty、amt;
- `total_ssls_amt` 精确沿用 v1_5 只按 `business_date` 汇总的语义，不擅自增加
  `day_clear` 分组；如果同日多个 day_clear 导致展示层不守恒，记录
  `ssls_transfer_delta` 供审计，不在 v0.13 内静默改口径;
- 原值和调整值并存, 不覆盖追溯。

---

## 11. 输出范围

保留/重建:

- SKU 底项;
- 门店/大中小/SPU/黑白猪/SKU 层级汇总;
- 订单客次;
- 线上/线下/接龙/及时达;
- 本周新客/老客的客次、金额、数量;
- BOM、加工、包装换码追溯;
- 库存滚动;
- `(matnr, report_category_code)` 物料码结果。

不迁移/不新增:

- 售罄率;
- 上架 SKU;
- SKU 动销率和品效新逻辑;
- 销售占比;
- SPU/SKU 排名;
- 其他仅为 v1_5 运营展示而新增的 KPI。

旧平台必须存在的列可以兼容保留, 但不在 v0.13 内发明新上游算法。

---

## 12. 验收合同

### 12.1 范围和抽取

```text
store_id != A3XV rows = 0
required mirror partitions ready = 100%
valid business day = bf19_sale_amt >= 500
source rows = extracted rows, including duplicate multiplicity
legal-empty refresh leaves stale rows = 0
partial publish after failure = 0
```

### 12.2 订单

```text
JOIN trade before_rows = after_rows
one order/day has at most one thirdparty identity
refund/return source sign is preserved
online + offline = total signed sales/qty
jielong + 及时达 = online  # 按 v1_5 渠道口径
new_order_count + old_order_count + other_order_count = completed_order_count
day_clear=2 is re-distinct, not 0+1
```

### 12.3 关系解析

```text
each from/to/day resolves to exactly one relation type
same pair never enters BOM + pack + compose twice
quarantined relations create no formal flow
relation snapshot checksum is in run manifest
```

### 12.4 猪肉 BOM

```text
self receive and parent receive do not duplicate
bom parent qty and sub qty use separate units
sum(child bom_in_amt) - parent bom_out_amt ~= 0.01
parent bom_out_qty <= parent available qty
consume_all parent end_stock=0 and standalone profit~=0
shared parents are one connected component
same-common-unit fallback requires 100% verified unit conversion coverage
report-only category margin allocation never writes back to inventory cost
```

### 12.5 蔬菜/包装

```text
article_convert pair never defaults to DISASSEMBLY_BOM
no observed conversion -> no invented pack flow
common-weight residual ~= 0
invalid reciprocal factor -> quarantine
article_convert must match the same store and business date
```

回归 SKU 至少包括:

- 西葫芦/500g;
- 普罗旺斯番茄/500g;
- 黄秋葵/250g;
- 红薯 25kg/件 -> kg;
- 粉西红柿(中)/500g。

### 12.6 加工

```text
actual compose qty is not overwritten by inferred qty
sum(compose_in_amt)-sum(compose_out_amt) ~= 0.01 by store/date/relation
formal recipe cost coverage = 100%
processing graph has no unhandled cycle
```

### 12.7 库存和利润

```text
D+1 init_qty/amt = D end_qty/amt
qty balance residual ~= 0
amount balance residual ~= 0
negative end stock count = 0
each internal flow is posted once
A3XV x 大分类 profit difference vs QDM within default +/-5%
```

### 12.8 特殊损耗

```text
adjusted_lost formula equals v1_5
ccj adjusted profit = accounting profit + ccj
ssls date-level debit equals v1_5 total_ssls_amt
accounting and adjusted values both trace to source rows
```

---

## 13. 实施顺序

### Phase 1: 归档和新骨架

- 完整归档旧 `fmetl`;
- 新建 v0.13 包;
- 建立 settings/contracts/store/API;
- 不发布业务结果。

### Phase 2: 镜像原子层

- 按 `sync_strategy_fm.sh` 建 registry;
- 完成 A3XV、分区、粒度、符号和 staging 验证;
- 建 valid business day;
- 建 v1_5 category snapshot。

### Phase 3: 关系审计层

- 固化 BOM/article_convert/recipe/matnr 快照;
- 建 relation resolution;
- 猪肉、蔬菜、compose 先产审计表, 不改利润。

### Phase 4: 订单和客次

- 原子订单事件;
- 首单 bootstrap;
- 渠道/新老客;
- 七层客次和 day_clear 合计。

### Phase 5: DailyCostStock

- 猪肉 BOM;
- PACK_CONVERT;
- RECIPE_COMPOSE;
- 每日数量/金额状态;
- profit。

### Phase 6: 特殊损耗和输出

- v1_5 调整值;
- SKU/levels/customer/matnr/trace 输出;
- 不迁移运营 KPI。

### Phase 7: 影子验收与切换

1. 单日;
2. 连续 7 日;
3. 完整月;
4. 全历史;
5. 服务器 `/opt/fm/data/fm.duckdb` 验收;
6. 显式 publish, 不在核心 run 中自动 SCP/SSH。

切换门禁：现有服务器 cron 仍执行 `python -m fmetl.executor`。只有在 v0.13 executor/CLI
完成、服务器 cron 已原子切换且可回滚后，才允许把重构分支合并到 `main`；骨架阶段不得覆盖
生产入口。

---

## 14. 文档和文件清理

### 14.1 保留到归档

- v0.11 代码、README、architecture;
- DESIGN-001/002;
- FIX-001~020;
- REVIEW-001~009 和历史报告;
- 用户当前未提交改动。

这些是审计证据, 不作物理删除。

### 14.2 新目录只保留

- DESIGN-003;
- v0.13 architecture;
- 镜像字段合同;
- 当前 README;
- 当前 validation/review 索引。

### 14.3 可删除

- `.DS_Store`;
- `__pycache__` / `*.pyc`;
- 已完整合并到字段手册的旧 `strategy_fm_字段手册_BOM版.md`;
- 新目录中与 v0.13 无关的历史状态报告。

删除前必须证明内容已存在归档或新手册, 不使用“看起来过时”作为删除依据。

---

## 15. 不做的事情

1. 不直连 Hive。
2. 不把 full_link/chdj 的公司利润、库存接入核心。
3. 不复制两份 v1_5 品类 CASE。
4. 不根据订单状态二次翻转源金额/数量。
5. 不把订单客次更名为用户人数。
6. 不把蔬菜包装换码当成猪肉 BOM。
7. 不在无实际事件时根据 matnr 虚构库存流。
8. 不用部分零成本配方生成正式成品成本。
9. 不迁移售罄/上架/排名/占比等运营 KPI。
10. 不在 ETL 运行中默认执行 SCP/SSH/外部 SQLite 写入。

---

## 16. 最终链路

```text
v1_5 sync_strategy_fm.sh
        ↓
StarRocks mirror allowlist (A3XV)
        ↓
DuckDB atomic + source contracts
        ↓
relation resolution
  ├─ pork disassembly BOM
  ├─ vegetable/fruit pack conversion
  ├─ recipe compose
  └─ matnr audit/fallback
        ↓
DailyCostStock state machine
        ↓
accounting profit + v1_5 adjusted profit/loss
        ↓
order visits + channel + weekly new/old
        ↓
compatible FM tables and explicit publish
```

这是“基于权威镜像层的完整 ETL 重构”, 不是对公司派生结果做第二次包装。
