# 旧版脚本 (legacy_scripts/)

存放原有 ETL 脚本，作为参考和备份。

## 目录结构

```
legacy_scripts/
├── layer0/     # Layer 0: 上游数据收集
├── layer1/     # Layer 1: 全渠道门店仓商品销售信息
├── layer2/     # Layer 2: 非日清门店商品汇总宽表
├── layer3/     # Layer 3: 翠花门店商品全链路指标
└── layer4/     # Layer 4: 翠花门店商品销售条码宽表
```

## Layer 0 - 上游数据收集

| 文件 | 说明 | 输出表 |
|---|---|---|
| `门店商品损耗表.py` | 损耗数据（已知+未知） | dal_transaction_store_article_lost_di |
| `layer0_加工转换信息表.sql` | BOM加工转换 | dsl_transaction_sotre_article_compose_info_di |
| `layer0_库存成本价同步.sql` | 成本价同步 | t_shop_inventory_sku_pool |
| `layer0_bom进货验收取数_split01.py` | BOM进货验收 | - |
| `layer0_bom销售取数_split02.py` | BOM销售数据 | - |
| `layer0_bom库存取数_split03.py` | BOM库存数据 | - |

## Layer 1 - 全渠道门店仓商品销售信息

| 文件 | 说明 | 输出表 |
|---|---|---|
| `layer1_全渠道门店仓商品销售信息表.sql` | 合并销售促销与供应链数据 | dal_manage_full_link_store_dc_article_info_di |

## Layer 2 - 非日清门店商品汇总宽表

| 文件 | 说明 | 输出表 |
|---|---|---|
| `layer2_非日清门店商品汇总宽表.py` | Python执行器 | dal_transaction_non_daily_store_article_sale_info_di |
| `layer2_非日清门店商品汇总宽表.sql` | SQL逻辑 | - |

## Layer 3 - 翠花门店商品全链路指标

| 文件 | 说明 | 输出表 |
|---|---|---|
| `layer3_翠花门店商品全链路指标.sql` | 翠花门店过滤+日清标签 | tmp_dal.dal_transaction_chdj_store_sale_article_sale_info_di_01 |

## Layer 4 - 翠花门店商品销售条码宽表

| 文件 | 说明 | 输出表 |
|---|---|---|
| `layer4_翠花门店商品销售条码宽表.sql` | 最终宽表输出 | dal.dal_transaction_chdj_store_sale_article_sale_info_di |

## 与新版 Pipeline 对应关系

| 旧版 Layer | 新版模块 |
|---|---|
| Layer 0 | `extraction/atomic_extractors.py` |
| Layer 1 | `layers/product_summary.py` |
| Layer 2 | `models/calculated/metrics.py` + `layers/product_summary.py` |
| Layer 3 | `layers/product_summary.py` (翠花门店过滤) |
| Layer 4 | `layers/result_layer.py` |

## 注意事项

- 这些脚本仅作参考，新开发请使用 `fm_etl_pipeline/`
- 旧版脚本直接执行 SQL，新版使用 Python 模块化设计
- 新版支持 BOM 毛利计算的两种版本
