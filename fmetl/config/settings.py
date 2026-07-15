from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the A3XV-only v0.12 pipeline."""

    store_id: str
    duckdb_path: Path
    qdm_host: str
    qdm_api_id: str
    qdm_access_key: str
    qdm_secret_key: str
    qdm_version: str
    page_size: int = 20_000
    valid_day_bf19_threshold: float = 500.0

    @classmethod
    def from_env(cls) -> "Settings":
        store_id = os.getenv("FM_STORE_ID", "A3XV").upper()
        if store_id != "A3XV":
            raise ValueError(f"v0.12 is scoped to A3XV, got {store_id!r}")
        return cls(
            store_id=store_id,
            duckdb_path=Path(
                os.getenv("FM_DUCKDB_PATH", str(PROJECT_ROOT / "data" / "fm_v012.duckdb"))
            ),
            qdm_host=os.getenv("QDM_HOST", "https://bdapp.qdama.cn"),
            qdm_api_id=os.getenv("QDM_API_ID", "i_fjl10g687-790"),
            qdm_access_key=os.environ["QDM_ACCESS_KEY"],
            qdm_secret_key=os.environ["QDM_SECRET_KEY"],
            qdm_version=os.getenv("QDM_VERSION", "1.0"),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings.from_env()
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return settings
