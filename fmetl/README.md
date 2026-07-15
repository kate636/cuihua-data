# fmetl v0.13

这是基于 v1.5 StarRocks 镜像层重新建设的 A3XV 单店完整 ETL。旧 v0.11 已整体归档到
`_archived/fmetl_v0_11_20260715/`，本目录不导入旧实现。

## 当前状态

已完成本地一周影子链路及其基础设施：

- 28 张权威镜像表清单，其中 18 张已实现字段投影合同和 API 完整性保护；
- A3XV 范围、有效营业日 `bf19_sale_amt >= 500`、v1.5 119 SKU 分类规则；
- 无损订单事件、源符号保留、订单客次、周内新老客；
- 猪肉拆分、包装换码、配方加工、采购别名的关系隔离及 plan schema 骨架；
- 每日成本库存单步状态机、利润消费接口、v1.5 特殊损耗调整；
- 统一 BOM/包装/加工日内 DAG、跨日精确金额滚动、SKU/门店/大中小分类影子输出；
- 2026-07-08 至 07-14 A3XV 本地影子库及 v1.5 参考下钻比较；
- 固定周 run/source/reference manifest、逐源逐分区行数与 SHA-256、三类关系输入快照；
- 单元测试和设计文档。

尚未完成生产发布：生产 executor、订单客次/完整 FM 兼容输出、服务器影子对比、月度/全历史
重放与切换仍属于后续阶段。当前代码只写本地 `data/fm_v013.duckdb`，不会写生产 DuckDB，
也不会自动 SSH/SCP。

> 合并门禁：服务器现有 cron 仍调用 `python -m fmetl.executor`。v0.13 在兼容 executor 完成或
> cron 显式切换前禁止合并到 `main`；当前开发只允许停留在 `codex/fmetl-v0-13-rebuild`。

## 数据边界

- 核心上游只读 `default_catalog.ads_business_analysis.strategy_fm_*` 镜像和
  `strategy_dim_store_article_bom_relation`；preflight 记录批准的同步脚本 SHA-256，
  固定周验证已生成本地 manifest，但尚未接入生产调度和线上发布 manifest。
- 特殊损耗按 v1.5 读取同 catalog 的原生业务表 `cuihua_t_purchase_wastage`，在合同中显式标为
  非同步脚本管理的辅助观测源。
- 范围固定为滨江宏岸店 `A3XV`，其他门店会阻断。
- 本地开发库默认 `data/fm_v013.duckdb`；生产权威库仍是服务器
  `/opt/fm/data/fm.duckdb`，当前阶段不发布。

## 本地检查

```bash
python3 -m fmetl.cli preflight \
  --sync-script /Users/zhukate/Desktop/Projects/qdm/翠花数据诊断/huajia_yonghong_etl/versions/v1_5/sync_strategy_fm.sh
python3 -m unittest discover -s fmetl/tests -v

# 固定验收周：实时镜像 -> 本地 data/fm_v013.duckdb
python3 -m fmetl.validation.run_shadow_week

# v1.5 仅作参考，生成分类和可覆盖日期的 SKU 比较表
python3 -m fmetl.validation.compare_shadow_v15
```

完整决策见 [DESIGN-003](docs/designs/DESIGN-003-v0.13-clean-rebuild.md)。
