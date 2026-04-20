# config — 全局配置管理

## 文件

- **`settings.py`** — 唯一配置文件，定义 `ApiConfig`、`Settings` 两个 dataclass 和工厂函数 `get_settings()`。

## 配置加载

凭证通过 `.env` 文件或环境变量加载（`python-dotenv` 自动读取项目根目录的 `.env`）。

```bash
cp .env.example .env  # 填入真实凭证
```

## ApiConfig — QDM BI API 凭证

| 字段 | 环境变量 | 默认值 |
|------|---------|--------|
| `host` | `QDM_HOST` | `https://bdapp.qdama.cn` |
| `api_id` | `QDM_API_ID` | `i_fjl10g687-790` |
| `access_key` | `QDM_ACCESS_KEY` | **必填** |
| `secret_key` | `QDM_SECRET_KEY` | **必填** |
| `version` | `QDM_VERSION` | `1.0` |

## Settings — 完整配置项

| 属性 | 环境变量 | 说明 |
|------|---------|------|
| `api` | — | `ApiConfig` 实例 |
| `motherduck_token` | `MOTHERDUCK_TOKEN` | 有值则连 MotherDuck 云，空则用本地文件 |
| `motherduck_db` | `MOTHERDUCK_DB` | MotherDuck 数据库名（默认 `fm_etl_v3`） |
| `duckdb_path` | `FM_DUCKDB_PATH` | 本地 DuckDB 路径（token 为空时生效） |

`duckdb_conn_str` 属性根据是否有 token 自动返回正确连接串：

```python
# 有 MOTHERDUCK_TOKEN
"md:fm_etl_v3?motherduck_token=<token>"

# 没有 MOTHERDUCK_TOKEN
"/path/to/fm_etl_v3.duckdb"
```

## 业务过滤常量

| 常量 | 值 | 用途 |
|------|-----|------|
| `material_category_ids` | `70~77` | 过滤物料类商品（不计入销售分析） |
| `day_clear_categories_l1` | 水果类、预制菜、冷藏及加工类 | 非翠花店日清品类（大类） |
| `day_clear_categories_l2` | 蛋类、冷藏奶制品类、烘焙类 | 非翠花店日清品类（中类） |
| `fm_allowed_categories` | 猪肉/预制菜/水果/水产/蔬菜/肉禽蛋/冷藏加工/标品 | FM 底表保留的大类 |
| `fm_city_filter` | `广州` | FM 底表只保留广州门店 |
| `fm_store_no_filter` | `food mart` | FM 底表只保留 food mart 门店 |
| `valid_day_bf19_threshold` | `500.0` | 有效营业日：19 点前销售额阈值（元） |

## 使用方式

```python
from fm_etl_v3.config import get_settings

cfg = get_settings()          # 单例，进程内只初始化一次
cfg.api.access_key            # QDM API access key
cfg.duckdb_conn_str           # DuckDB 连接串（自动判断本地/云）
cfg.material_category_ids     # ('70', '71', ..., '77')
```
