# AI 阿里云 ETL 只读取数指南

> 目标读者: 被授权帮助业务分析数据的 AI agent。
> 核心规则: 本文档只授权取数查询，不授权任何写入、部署、运维、改配置或改代码动作。

## 1. 权限边界

### 1.1 允许做什么

AI agent 只能执行以下动作:

1. 连接阿里云 ECS。
2. 以只读方式打开 DuckDB 数据库 `/opt/fm/data/fm.duckdb`。
3. 运行 `SELECT` / `WITH` / `DESCRIBE` / `PRAGMA table_info` 等只读查询。
4. 将查询结果汇总成业务口径说明、表格、CSV 或分析结论。

### 1.2 禁止做什么

AI agent 不得执行以下动作:

1. 不得运行 ETL: 禁止执行 `/opt/fm/etl/daily_run.sh`、`python -m fmetl.executor` 或任何重刷命令。
2. 不得写入数据库: 禁止 `INSERT`、`UPDATE`、`DELETE`、`CREATE`、`DROP`、`ALTER`、`COPY TO`、`VACUUM`、`CHECKPOINT`。
3. 不得修改服务器文件: 禁止编辑 `/opt/fm/etl/`、`/opt/fm/data/`、`/opt/fm/reports/`、`/opt/fm/proc-rel/` 下任何文件。
4. 不得改服务: 禁止 `systemctl`、`service`、`crontab`、`nginx`、进程重启、端口操作。
5. 不得拉取或提交代码: 禁止 `git pull`、`git push`、`git reset`、`git checkout`。
6. 不得读取或输出密钥: 禁止查看 `.env`、shell history、私钥、API access key、secret key。
7. 不得连接 Hive 源库或公司 IDE。Hive 表只能让用户在公司 IDE 自行查询。

### 1.3 默认失败策略

如果业务问题需要写权限、重刷 ETL、查 Hive、改加工关系或修复数据，AI agent 必须停止并说明:

```text
当前请求超出只读取数权限。本文档只允许查询 /opt/fm/data/fm.duckdb 的现有数据。
需要人工授权后由项目维护者处理。
```

## 2. 数据库介绍

### 2.1 生产数据库

| 项 | 值 |
|---|---|
| 服务器 | 阿里云 ECS，广州 |
| 主机 | `47.115.213.115` |
| 数据库类型 | DuckDB 单文件数据库 |
| 生产数据库路径 | `/opt/fm/data/fm.duckdb` |
| 访问方式 | SSH 到服务器后，用 Python DuckDB 只读连接 |
| 数据来源 | QDM BI API 拉取 `strategy_fm_*` 源表后，由 fmetl 计算生成 |
| 更新方式 | 服务器每日定时增量；全量重刷由维护者在本地完成后同步 |

生产查询一律以服务器 `/opt/fm/data/fm.duckdb` 为准，不使用本地 `data/fm.duckdb`。

### 2.2 数据层级

DuckDB 内主要有三层表:

| 层级 | 表名模式 | 用途 | 是否建议业务查询 |
|---|---|---|---|
| 原子层 | `atomic_*` | 从 QDM 源表抽取的观测量 | 一般不查，仅排查源数据时查 |
| 计算层 | `t_calc_*` | Python 推导出的库存、成本、毛利等中间结果 | 排查公式时查 |
| 对外底表 | `t_fm_*` | 面向业务和平台的数据结果 | 优先查询 |

业务取数优先使用 `t_fm_*` 表。

### 2.3 主要业务表

| 表 | 粒度 | 适用问题 |
|---|---|---|
| `t_fm_sku_dim` | 门店 × 日期 × SKU × 日清标记 | SKU 销售、毛利、库存、成本、损耗、品类明细 |
| `t_fm_levels_sum` | 门店 × 日期 × 日清标记 × 分类层级 | 分类汇总的数量和金额 |
| `t_fm_levels_result` | 门店 × 日期 × 日清标记 × 分类层级 | 平台对接结果，中文列名和 KPI 比率 |
| `t_fm_cust` | 门店 × 日期 × 日清标记 × 分类层级 | 客数、订单数相关分析 |
| `t_fm_bom_breakdown` | 门店 × 日期 × 父品 × 子品 | BOM 分摊、原料到成品的成本溯源 |
| `t_fm_stock_roll` | 门店 × 日期 × SKU × 日清标记 | 库存八要素滚动、库存方程排查 |
| `t_fm_levels_result_matnr` | 门店 × 日期 × 物料号/分类 | 物料号口径的汇总结果 |

### 2.4 关键字段口径

| 字段 | 说明 |
|---|---|
| `business_date` | 业务日期 |
| `store_id` | 门店 ID |
| `article_id` | SKU ID |
| `day_clear` | 日清标记。`'0'`=日清，`'1'`=非日清，`'2'`=合计 |
| `total_sale_amt` | 销售额 |
| `article_profit_amt` | SKU 毛利额 |
| `sale_cost_amt` | 销售成本 |
| `end_stock_qty` / `end_stock_amt` | 期末库存数量/金额 |
| `know_lost_qty` / `know_lost_amt` | 已知损耗数量/金额 |
| `unknow_lost_qty` / `unknow_lost_amt` | 未知损耗或盘盈口径数量/金额 |
| `effective_unit_cost` / `euc` | 有效单位成本，来自加权平均成本计算 |

注意: 查询全天合计分类结果时，优先使用 `day_clear = '2'`。查询 SKU 明细时，通常使用 `day_clear IN ('0','1')`，避免与合计行重复。

## 3. 连接方式

### 3.1 推荐方式: SSH + Python DuckDB 只读查询

只读连接模板:

```bash
ssh root@47.115.213.115 'python3 - <<'"'"'PY'"'"'
import duckdb

DB = "/opt/fm/data/fm.duckdb"
sql = """
SELECT
  business_date,
  store_id,
  SUM(total_sale_amt) AS sale_amt,
  SUM(article_profit_amt) AS profit_amt
FROM t_fm_sku_dim
WHERE business_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-07'
  AND day_clear IN ('0', '1')
GROUP BY 1, 2
ORDER BY 1, 2
"""

conn = duckdb.connect(DB, read_only=True)
print(conn.execute(sql).df().to_string(index=False))
conn.close()
PY'
```

强制要求:

1. `duckdb.connect(DB, read_only=True)` 必须保留。
2. SQL 只能包含只读语句。
3. 不在服务器上生成临时文件，除非用户明确要求导出结果。

### 3.2 大结果导出

如果用户明确要求导出 CSV，可以在本地执行远程查询并把标准输出保存到本地文件。不要让服务器写文件。

```bash
ssh root@47.115.213.115 'python3 - <<'"'"'PY'"'"'
import duckdb

DB = "/opt/fm/data/fm.duckdb"
sql = """
SELECT *
FROM t_fm_levels_result
WHERE business_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-07'
  AND day_clear = '2'
"""

conn = duckdb.connect(DB, read_only=True)
df = conn.execute(sql).df()
print(df.to_csv(index=False))
conn.close()
PY' > fm_levels_result_20260601_20260607.csv
```

### 3.3 不推荐方式

不要直接复制 `/opt/fm/data/fm.duckdb` 到本地再查，除非维护者明确要求。生产查询以服务器当前文件为准。

## 4. 查询前检查清单

AI agent 在写 SQL 前必须先确认:

1. 业务要看什么指标: 销售、毛利、损耗、库存、客数、BOM、物料号。
2. 日期范围是什么。
3. 粒度是什么: 门店、日期、SKU、分类、物料号。
4. 是否需要日清拆分: `day_clear='0'`、`'1'` 或合计 `'2'`。
5. 是否需要排除物料类或特殊品类。如果不确定，优先使用 `t_fm_*` 对外底表已有口径。
6. 查询结果行数是否可能很大。大结果应先 `COUNT(*)` 或聚合后再取明细。

## 5. 常用查询模板

### 5.1 查看表结构

```sql
DESCRIBE t_fm_sku_dim;
```

或:

```sql
PRAGMA table_info('t_fm_sku_dim');
```

### 5.2 查看可用日期范围

```sql
SELECT
  MIN(business_date) AS min_date,
  MAX(business_date) AS max_date,
  COUNT(DISTINCT business_date) AS days
FROM t_fm_sku_dim;
```

### 5.3 门店日销售和毛利

```sql
SELECT
  business_date,
  store_id,
  SUM(total_sale_amt) AS sale_amt,
  SUM(article_profit_amt) AS profit_amt,
  SUM(article_profit_amt) / NULLIF(SUM(total_sale_amt), 0) AS profit_rate
FROM t_fm_sku_dim
WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND day_clear IN ('0', '1')
GROUP BY 1, 2
ORDER BY 1, 2;
```

### 5.4 分类合计结果

适合经营看板或业务汇总。使用 `day_clear='2'` 避免重复统计。

```sql
SELECT *
FROM t_fm_levels_result
WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND day_clear = '2'
  AND "分类等级" = '大类'
ORDER BY business_date, store_id, "分类名称";
```

### 5.5 SKU 明细下钻

```sql
SELECT
  business_date,
  store_id,
  article_id,
  article_name,
  category_level1_description,
  category_level2_description,
  total_sale_amt,
  sale_qty,
  sale_cost_amt,
  article_profit_amt,
  end_stock_qty,
  end_stock_amt
FROM t_fm_sku_dim
WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND day_clear IN ('0', '1')
  AND store_id = '{store_id}'
ORDER BY business_date, total_sale_amt DESC
LIMIT 200;
```

### 5.6 库存方程排查

```sql
SELECT
  business_date,
  store_id,
  article_id,
  article_name,
  day_clear,
  init_stock_qty,
  receive_qty,
  bom_in_qty,
  bom_out_qty,
  compose_in_qty,
  compose_out_qty,
  sale_qty,
  know_lost_qty,
  unknow_lost_qty,
  end_stock_qty,
  balance_qty
FROM t_fm_stock_roll
WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND store_id = '{store_id}'
  AND article_id = '{article_id}'
ORDER BY business_date, day_clear;
```

### 5.7 BOM 分摊溯源

```sql
SELECT *
FROM t_fm_bom_breakdown
WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND store_id = '{store_id}'
ORDER BY business_date, parent_article_id, sub_article_id
LIMIT 200;
```

### 5.8 物料号口径汇总

```sql
SELECT *
FROM t_fm_levels_result_matnr
WHERE business_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND day_clear = '2'
ORDER BY business_date, store_id
LIMIT 200;
```

## 6. 输出规范

AI agent 返回业务结果时必须包含:

1. 查询数据源: `/opt/fm/data/fm.duckdb`。
2. 查询时间范围。
3. 使用表名。
4. 粒度。
5. 是否使用 `day_clear='2'` 合计或 `day_clear IN ('0','1')` 明细。
6. 指标口径说明。
7. 若存在不确定性，明确写出。

推荐格式:

```text
数据源: 阿里云 /opt/fm/data/fm.duckdb
表: t_fm_sku_dim
日期: 2026-06-01 至 2026-06-07
粒度: 门店 × 日期
day_clear: '0','1' 明细加总
口径: 销售额=sum(total_sale_amt)，毛利额=sum(article_profit_amt)
```

## 7. 安全判定规则

执行任何命令前，AI agent 必须做字符串级检查:

1. 命令中不能包含 `rm`、`mv`、`cp`、`scp`、`rsync`、`chmod`、`chown`、`systemctl`、`service`、`crontab`、`git`。
2. 命令中不能包含 `daily_run.sh` 或 `fmetl.executor`。
3. SQL 中不能包含 `INSERT`、`UPDATE`、`DELETE`、`CREATE`、`DROP`、`ALTER`、`COPY`、`VACUUM`、`CHECKPOINT`、`ATTACH`。
4. Python 连接 DuckDB 时必须包含 `read_only=True`。

如果检查不通过，拒绝执行并说明原因。

## 8. 典型拒绝示例

用户要求: "帮我把服务器 ETL 重跑一下。"

回答:

```text
我不能执行这个操作。当前文档只授权从 /opt/fm/data/fm.duckdb 只读取数，不授权运行 ETL 或修改服务器状态。
```

用户要求: "帮我查一下 .env 里的 QDM key。"

回答:

```text
我不能读取或输出密钥。当前文档只授权查询 DuckDB 业务数据。
```

用户要求: "查 Hive 源表字段。"

回答:

```text
我不能直接访问 Hive。可以给你一段 SQL 模板，由你在公司 IDE 运行后把结果发回来。
```

