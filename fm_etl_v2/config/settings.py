"""
配置管理模块

管理数据库连接、路径、原子域配置等
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
import os
import json


@dataclass
class StarRocksConfig:
    """StarRocks 数据库连接配置"""
    host: str = "localhost"
    port: int = 9030
    user: str = "root"
    password: str = ""
    database: str = "default_catalog"
    hive_catalog: str = "hive"

    @property
    def connection_string(self) -> str:
        """生成 MySQL 兼容的连接字符串"""
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class PathConfig:
    """路径配置"""
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent / "data")

    @property
    def atomic_dir(self) -> Path:
        """原子层 Parquet 文件目录"""
        return self.base_dir / "atomic"

    @property
    def calculated_dir(self) -> Path:
        """计算层 Parquet 文件目录"""
        return self.base_dir / "calculated"

    @property
    def output_dir(self) -> Path:
        """输出层 Parquet 文件目录"""
        return self.base_dir / "output"

    def ensure_dirs(self) -> None:
        """确保所有目录存在"""
        self.atomic_dir.mkdir(parents=True, exist_ok=True)
        self.calculated_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class Settings:
    """
    全局配置类

    包含所有 ETL 管道需要的配置项
    """
    # 数据库配置
    starrocks: StarRocksConfig = field(default_factory=StarRocksConfig)

    # 路径配置
    paths: PathConfig = field(default_factory=PathConfig)

    # 原子域配置 (10 大域)
    atomic_domains: List[str] = field(default_factory=lambda: [
        "sale",        # 域① 销售域
        "purchase",    # 域② 进货库存域
        "scm",         # 域③ 供应链域
        "loss",        # 域④ 损耗域
        "compose",     # 域⑤ 加工转换域
        "subsidy",     # 域⑥ 补贴域
        "promo",       # 域⑦ 促销优惠域
        "cost_price",  # 域⑧ 成本价域
        "price",       # 域⑨ 价格域
        "inventory",   # 域⑩ 盘点域
    ])

    # 业务配置
    chdj_store_filter: bool = True  # 是否只取翠花门店
    category_filter: bool = True    # 是否过滤物料类商品

    # 日清判断规则 (非翠花店的品类规则)
    day_clear_categories: List[str] = field(default_factory=lambda: [
        "水果类",
        "预制菜",
        "冷藏及加工类",
        "蛋类",
        "冷藏奶制品类",
        "烘焙类",
    ])

    @classmethod
    def from_env(cls) -> "Settings":
        """从环境变量加载配置"""
        starrocks = StarRocksConfig(
            host=os.getenv("STARROCKS_HOST", "localhost"),
            port=int(os.getenv("STARROCKS_PORT", "9030")),
            user=os.getenv("STARROCKS_USER", "root"),
            password=os.getenv("STARROCKS_PASSWORD", ""),
            database=os.getenv("STARROCKS_DATABASE", "default_catalog"),
        )

        base_dir = os.getenv("FM_ETL_DATA_DIR")
        paths = PathConfig(
            base_dir=Path(base_dir) if base_dir else Path(__file__).parent.parent.parent / "data"
        )

        return cls(starrocks=starrocks, paths=paths)

    @classmethod
    def from_file(cls, config_path: Path) -> "Settings":
        """从 JSON 文件加载配置"""
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        starrocks = StarRocksConfig(**data.get("starrocks", {}))
        paths_data = data.get("paths", {})
        paths = PathConfig(
            base_dir=Path(paths_data.get("base_dir", ""))
        )

        return cls(
            starrocks=starrocks,
            paths=paths,
            atomic_domains=data.get("atomic_domains", cls.__dataclass_fields__["atomic_domains"].default_factory()),
            day_clear_categories=data.get("day_clear_categories", cls.__dataclass_fields__["day_clear_categories"].default_factory()),
        )

    def to_file(self, config_path: Path) -> None:
        """保存配置到 JSON 文件"""
        data = {
            "starrocks": {
                "host": self.starrocks.host,
                "port": self.starrocks.port,
                "user": self.starrocks.user,
                "password": self.starrocks.password,
                "database": self.starrocks.database,
            },
            "paths": {
                "base_dir": str(self.paths.base_dir),
            },
            "atomic_domains": self.atomic_domains,
            "day_clear_categories": self.day_clear_categories,
        }

        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


# 全局配置实例
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """获取全局配置实例"""
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
        _settings.paths.ensure_dirs()
    return _settings


def set_settings(settings: Settings) -> None:
    """设置全局配置实例"""
    global _settings
    _settings = settings
    _settings.paths.ensure_dirs()
