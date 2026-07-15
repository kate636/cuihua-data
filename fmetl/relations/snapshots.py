from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib

import pandas as pd


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    checksum: str
    created_at: datetime
    _frame: pd.DataFrame

    @property
    def frame(self) -> pd.DataFrame:
        return self._frame.copy(deep=True)


def _canonical_csv(frame: pd.DataFrame, keys: list[str]) -> bytes:
    missing = sorted(set(keys) - set(frame.columns))
    if missing:
        raise KeyError(f"snapshot missing key columns: {missing}")
    canonical = frame.copy()
    canonical.columns = canonical.columns.astype(str)
    canonical = canonical.sort_values(keys, kind="stable", na_position="first")
    canonical = canonical.reindex(sorted(canonical.columns), axis=1)
    return canonical.to_csv(index=False, lineterminator="\n", na_rep="<NULL>").encode("utf-8")


def build_snapshot(frame: pd.DataFrame, keys: list[str], namespace: str) -> Snapshot:
    """Freeze one deterministic relation input for the whole ETL run."""
    if frame[keys].isna().any().any():
        raise ValueError("snapshot keys cannot contain NULL")
    if frame.duplicated(keys).any():
        raise ValueError(f"snapshot keys are not unique: {keys}")
    payload = _canonical_csv(frame, keys)
    checksum = hashlib.sha256(payload).hexdigest()
    return Snapshot(
        snapshot_id=f"{namespace}:{checksum[:16]}",
        checksum=checksum,
        created_at=datetime.now(timezone.utc),
        _frame=frame.copy(deep=True),
    )
