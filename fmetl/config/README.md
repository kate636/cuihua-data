# config — 全局配置

> 一个文件 `settings.py`，负责所有环境变量和业务常量。pipeline 里所有模块都通过 `get_settings()` 单例访问。

## 加载流程

1. 进程启动 → `from dotenv import load_dotenv; load_dotenv()` 自动读取**项目根目录**的 `.env`
2. 首次调用 `get_settings()` → `Settings.from_env()` 从环境变量构造配置对象
3. 后续调用 → 返回同一个单例（进程内只初始化一次）

`.env` 的搜索路径：运行目录 → 向上递归。**约定放在仓库根** `翠花数据/.env`。

## 两个 dataclass

### `ApiConfig` — QDM BI API 凭证

| 字段 | 环境变量 | 默认值 | 必填 |
|------|---------|--------|------|
| `host` | `QDM_HOST` | `https://bdapp.qdama.cn` | ❌ |
| `api_id` | `QDM_API_ID` | `i_fjl10g687-790` | ❌ |
| `access_key` | `QDM_ACCESS_KEY` | — | ✅ |
| `secret_key` | `QDM_SECRET_KEY` | — | ✅ |
| `version` | `QDM_VERSION` | `1.0` | ❌ |

### `Settings` — 顶层配置

| 字段 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| `api` | — | `ApiConfig()` | API 凭证子对象 |
| `duckdb_path` | `FM_DUCKDB_PATH` | `<repo>/data/fm.duckdb` | DuckDB 文件路径（云端设 `/opt/fm/data/fm.duckdb`） |
| `material_category_ids` | 硬编码 | `('70'~'77')` | 物料类商品大类 ID（原子层会**保留**，merge 阶段/fm_tables 过滤） |
| `day_clear_categories_l1` | 硬编码 | 水果类、预制菜、冷藏及加工类 | 非翠花店日清大类（后备规则） |
| `day_clear_categories_l2` | 硬编码 | 蛋类、冷藏奶制品类、烘焙类 | 非翠花店日清中类（后备规则） |
| `fm_allowed_categories` | 硬编码 | 猪肉/预制菜/水果/水产/蔬菜/肉禽蛋/冷藏加工/标品 | FM 底表保留的 8 个大类 |
| `fm_city_filter` | 硬编码 | `广州` | FM 底表只保留广州门店 |
| `fm_store_no_filter` | 硬编码 | `food mart` | FM 底表只保留 food mart 系列门店 |
| `valid_day_bf19_threshold` | 硬编码 | `500.0` | 有效营业日阈值：19 点前销售额（元） |

## 使用方式

```python
from fm_etl_v3.config import get_settings

cfg = get_settings()              # 单例
cfg.api.access_key                # QDM access key
cfg.duckdb_conn_str               # DuckDB 文件路径（str）
cfg.material_category_ids         # ('70', '71', ..., '77')
cfg.fm_city_filter                # '广州'
```

## 为什么业务常量硬编码在 `settings.py` 而不是 `.env`

- 这些值极少变动（比如 FM 只做广州的 food mart）
- 改这些值等于改业务口径，必须走 git commit 留痕
- 放 `.env` 容易不同环境配置漂移，导致本地和线上算出的数不一致

**新增业务常量**直接改 `Settings` dataclass 的字段默认值，通过 git 审核。**不要**加新的环境变量。

## 修改配置示例

**本地跑完整月**：改 `.env` 里 `FM_DUCKDB_PATH=data/fm_full.duckdb`，避免污染默认库。

**加一个新的 FM 大类**：改 `fm_allowed_categories` tuple 值 → commit → 下次 pipeline 自动生效。

**切换 API 环境**（比如测试环境）：改 `.env` 的 `QDM_HOST` + `QDM_API_ID`，不动 `settings.py`。
