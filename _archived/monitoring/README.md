# 监控脚本 (monitoring/)

数据质量和任务监控脚本。

## 文件说明

| 文件 | 说明 |
|---|---|
| `Doris数据监控任务.py` | Doris 数据质量监控（版本1） |
| `Doris数据监控任务2.py` | Doris 数据质量监控（版本2） |

## 监控内容

### 数据质量检查

- 数据完整性：检查关键表是否有数据
- 数据一致性：检查数据量是否符合预期
- 数据时效性：检查数据是否按时更新

### 告警机制

- 数据延迟告警
- 数据异常告警
- 任务失败告警

## 执行方式

```bash
# 定时执行（如每5分钟）
python Doris数据监控任务.py

# 或通过调度系统配置
```

## 监控指标示例

| 指标 | 说明 | 告警阈值 |
|---|---|---|
| 数据延迟时间 | 最新数据时间与当前时间差 | > 2小时 |
| 日数据量 | 每日数据行数 | 波动 > 50% |
| 空值率 | 关键字段空值比例 | > 10% |
| 重复率 | 主键重复率 | > 0% |

## 扩展建议

可以集成到 `fm_etl_pipeline` 的结果层：

```python
from fm_etl_pipeline.layers import ResultLayer

layer = ResultLayer(connector, config)
validation = layer.validate_output(df, validation_rules={
    'no_negative': ['sale_qty', 'sale_amt'],
    'primary_keys': ['inc_day', 'store_id', 'article_id']
})

if not validation['passed']:
    # 发送告警
    send_alert(validation['errors'])
```
