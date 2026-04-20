"""
全局配置管理

数据源: bdapp.qdama.cn HTTP API（只读）
计算层: 本地 DuckDB 或 MotherDuck（云）

凭证通过环境变量或 .env 文件加载（python-dotenv）。
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

# ── 路径常量 ─────────────────────────────────────────────────────────────────
_PROJ_ROOT = Path(__file__).parent.parent.parent  # 翠花数据/
_DATA_DIR  = _PROJ_ROOT / "data"


@dataclass
class ApiConfig:
    """QDM BI API 凭证（bdapp.qdama.cn）。"""
    host:       str = "https://bdapp.qdama.cn"
    api_id:     str = "i_fjl10g687-790"
    access_key: str = ""
    secret_key: str = ""
    version:    str = "1.0"


@dataclass
class Settings:
    api: ApiConfig = field(default_factory=ApiConfig)

    # MotherDuck 连接 token（空 = 使用本地 DuckDB 文件）
    motherduck_token: str = ""
    motherduck_db:    str = "fm_etl_v3"

    # 本地 DuckDB 回退路径（仅 motherduck_token 为空时使用）
    duckdb_path: Path = field(default_factory=lambda: _DATA_DIR / "fm_etl_v3.duckdb")

    # ── 业务过滤常量 ──────────────────────────────────────────────────────────
    material_category_ids: tuple = ("70", "71", "72", "73", "74", "75", "76", "77")

    day_clear_categories_l1: tuple = ("水果类", "预制菜", "冷藏及加工类")
    day_clear_categories_l2: tuple = ("蛋类", "冷藏奶制品类", "烘焙类")

    fm_allowed_categories: tuple = (
        "猪肉类", "预制菜", "水果类", "水产类", "蔬菜类", "肉禽蛋类", "冷藏及加工类", "标品类"
    )

    fm_city_filter:     str   = "广州"
    fm_store_no_filter: str   = "food mart"
    valid_day_bf19_threshold: float = 500.0

    @property
    def duckdb_conn_str(self) -> str:
        """返回 DuckDB 连接串：有 token 走 MotherDuck，否则走本地文件。"""
        if self.motherduck_token:
            return f"md:{self.motherduck_db}?motherduck_token={self.motherduck_token}"
        return str(self.duckdb_path)

    @classmethod
    def from_env(cls) -> "Settings":
        api = ApiConfig(
            host       = os.getenv("QDM_HOST",       "https://bdapp.qdama.cn"),
            api_id     = os.getenv("QDM_API_ID",     "i_fjl10g687-790"),
            access_key = os.environ["QDM_ACCESS_KEY"],
            secret_key = os.environ["QDM_SECRET_KEY"],
            version    = os.getenv("QDM_VERSION",    "1.0"),
        )
        md_token = os.getenv("MOTHERDUCK_TOKEN", "")
        md_db    = os.getenv("MOTHERDUCK_DB",    "fm_etl_v3")
        db_path  = Path(os.getenv("FM_DUCKDB_PATH", str(_DATA_DIR / "fm_etl_v3.duckdb")))
        return cls(api=api, motherduck_token=md_token, motherduck_db=md_db, duckdb_path=db_path)

    def ensure_data_dir(self) -> None:
        if not self.motherduck_token:
            self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)


_global: Optional[Settings] = None


def get_settings() -> Settings:
    global _global
    if _global is None:
        _global = Settings.from_env()
        _global.ensure_data_dir()
    return _global
