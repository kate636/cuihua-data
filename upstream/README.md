# 上游数据脚本 (upstream/)

上游数据同步和调度脚本。

## 文件说明

| 文件 | 类型 | 说明 |
|---|---|---|
| `upstream_hive_门店订购出库信息表.sql` | Hive | 门店订购出库信息同步 |
| `upstream_sh_供应链SAP出入库全局表.sh` | Shell | 供应链SAP出入库数据调度 |
| `upstream_sh_全渠道供应链宽表.sh` | Shell | 全渠道供应链宽表调度 |

## 执行方式

### Hive 脚本

通过调度系统执行，使用 `$[time()]` 宏替换日期参数：

```bash
hive -f upstream_hive_门店订购出库信息表.sql
```

### Shell 脚本

```bash
sh upstream_sh_供应链SAP出入库全局表.sh <start_date> <end_date> <interval_days>
```

## 数据流向

```
上游系统 (SAP/POS/SCM)
        ↓
    ODS/STG 层
        ↓
    本脚本处理
        ↓
    DSL/DAL 层
        ↓
    fm_etl_pipeline
```

## 调度说明

这些脚本通常由外部调度系统（如 DolphinScheduler、Airflow）定时触发：

- 日增量同步：每日凌晨执行
- 全量同步：每周执行一次
- 实时同步：通过 CDC 或消息队列

## 与 ETL Pipeline 的关系

上游脚本负责将原始数据同步到数据仓库的 ODS/STG 层，`fm_etl_pipeline` 从这些表读取数据进行后续处理。

主要源表：
- `ods_rt_dws.*` - 实时数据
- `ods_sc_db.*` - 门店中心数据
- `ods_sap.*` - SAP数据
- `ddl.*` - BOM定义数据
