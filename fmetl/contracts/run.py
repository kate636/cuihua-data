from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    version: str
    git_commit: str
    requested_start: str
    requested_end: str
    affected_start: str
    affected_end: str
    store_id: str
    mirror_sync_checksum: str
    category_rule_checksum: str
    relation_snapshot_checksum: str
    status: str = "started"
    failed_step: str | None = None
    error: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
