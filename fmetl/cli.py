from __future__ import annotations

import argparse
import json
from pathlib import Path

from fmetl import __version__
from fmetl.master_data.category import load_category_mapper
from fmetl.mirror.registry import EXTRACTION_CONTRACTS, SYNC_MIRROR_TABLES
from fmetl.validation.preflight import SYNC_SCRIPT_SHA256, validate_mirror_registry, verify_sync_script


def _preflight(sync_script: Path | None) -> int:
    validate_mirror_registry()
    category = load_category_mapper()
    verified_checksum = verify_sync_script(sync_script) if sync_script else None
    payload = {
        "version": __version__,
        "store_id": "A3XV",
        "sync_mirror_table_count": len(SYNC_MIRROR_TABLES),
        "implemented_extraction_contract_count": len(EXTRACTION_CONTRACTS),
        "sync_script_sha256": SYNC_SCRIPT_SHA256,
        "sync_script_verified": verified_checksum is not None,
        "category_rule_version": category.version,
        "frozen_sku_count": len(category.frozen_skus),
        "status": "foundation_ready_not_publishable",
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="fmetl v0.12 development CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    preflight = subparsers.add_parser(
        "preflight", help="validate local contracts without production reads or writes"
    )
    preflight.add_argument(
        "--sync-script", type=Path,
        help="optional path to the approved v1_5 sync_strategy_fm.sh for a live checksum comparison",
    )
    args = parser.parse_args()
    if args.command == "preflight":
        return _preflight(args.sync_script)
    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
