from __future__ import annotations

import hashlib
from pathlib import Path

from fmetl.mirror.registry import AUXILIARY_STARROCKS_TABLES, EXTRACTION_CONTRACTS, SYNC_MIRROR_TABLES


SYNC_SCRIPT_SHA256 = "42a42695eaed454a932216caf12c07e28126dd6fa9f0745f453d0893d36693cf"


def verify_sync_script(path: Path) -> str:
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    if checksum != SYNC_SCRIPT_SHA256:
        raise ValueError(
            f"sync script checksum changed: approved={SYNC_SCRIPT_SHA256}, actual={checksum}"
        )
    return checksum


def validate_mirror_registry() -> None:
    if len(SYNC_MIRROR_TABLES) != 28 or len(set(SYNC_MIRROR_TABLES)) != 28:
        raise ValueError("v1.5 sync mirror contract must contain exactly 28 unique targets")
    allowed = set(SYNC_MIRROR_TABLES) | set(AUXILIARY_STARROCKS_TABLES)
    unknown = sorted(contract.name for contract in EXTRACTION_CONTRACTS.values() if contract.name not in allowed)
    if unknown:
        raise ValueError(f"extraction contracts outside authoritative mirror layer: {unknown}")
    for key, contract in EXTRACTION_CONTRACTS.items():
        if not contract.projection:
            raise ValueError(f"{key}: empty projection allowlist")
        if contract.shards > 1 and not contract.shard_key:
            raise ValueError(f"{key}: shard_key required")
        if contract.managed_by_sync_script and contract.name not in SYNC_MIRROR_TABLES:
            raise ValueError(f"{key}: incorrectly claims sync-script authority")
