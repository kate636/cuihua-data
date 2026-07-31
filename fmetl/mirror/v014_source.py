from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Mapping, Protocol, Sequence

import duckdb
import pandas as pd

from fmetl.connectors import QdmApi
from fmetl.mirror.extract import MirrorExtractor
from fmetl.mirror.registry import EXTRACTION_CONTRACTS, HIVE_SOURCE_BY_MIRROR
from fmetl.validation.manifest import stable_frame_checksum


# These are original observations or dimensions mirrored from Hive.  The two
# application-owned sources (product_group and stock_convert_detail) are kept
# here because they are relationship evidence, never because they are allowed
# to replace a Hive fact.
DAILY_SOURCE_KEYS: tuple[str, ...] = (
    "sales",
    "store_receipt",
    "supply_chain",
    "supply_chain_adjust",
    "known_loss",
    "inventory_detail",
    "day_clear",
    "store_daily",
    "order_offline",
    "order_online",
    "compose",
    "receive_sale",
    "bom_relation",
    "article_convert",
    "order_saleability",
    "article_sale",
    "allowance",
    "promotion",
    "price",
    "inventory_pool",
    "product_group",
    "stock_convert_detail",
)

LATEST_SOURCE_KEYS: tuple[str, ...] = ("goods", "store_profile", "purchase_wastage")


class DayExtractor(Protocol):
    def extract_day(self, contract: object, business_day: date | str) -> pd.DataFrame: ...


@dataclass(frozen=True)
class MirrorSourceBundle:
    store_id: str
    requested_days: tuple[str, ...]
    frames: Mapping[str, pd.DataFrame]
    manifest: pd.DataFrame
    completeness: pd.DataFrame


def seven_days_ending(end: str | date) -> tuple[str, ...]:
    end_day = date.fromisoformat(end) if isinstance(end, str) else end
    return tuple((end_day - timedelta(days=6 - offset)).isoformat() for offset in range(7))


def shadow_source_days_ending(end: str | date) -> tuple[str, ...]:
    """Return D-1 warm-up plus the seven publishable business days."""
    end_day = date.fromisoformat(end) if isinstance(end, str) else end
    return tuple((end_day - timedelta(days=7 - offset)).isoformat() for offset in range(8))


def discover_latest_mirror_day(
    api: QdmApi,
    *,
    store_id: str,
    end: str = "auto",
) -> str:
    """Find a common upper partition without mixing dates across sources."""
    maxima: list[date] = []
    ceiling = date.today() - timedelta(days=1) if end == "auto" else date.fromisoformat(end)
    for source_key in DAILY_SOURCE_KEYS:
        contract = EXTRACTION_CONTRACTS[source_key]
        clauses = list(contract.base_predicates)
        if contract.store_column:
            clauses.append(f"{contract.store_column} = '{store_id}'")
        clauses.append(f"{contract.partition_column} <= '{ceiling.isoformat()}'")
        where = " AND ".join(clauses) if clauses else "1 = 1"
        result = api.query(
            f"SELECT MAX({contract.partition_column}) AS source_day "
            f"FROM {contract.full_name} WHERE {where}"
        )
        if result.empty or pd.isna(result.iloc[0].get("source_day")):
            if contract.allow_empty:
                continue
            raise RuntimeError(f"{contract.name}: no source partition at or before {ceiling}")
        maxima.append(date.fromisoformat(str(result.iloc[0]["source_day"])))
    if not maxima:
        raise RuntimeError("no required v0.14 mirror exposes a usable partition")
    return min(maxima).isoformat()


def extract_v014_mirror_sources(
    *,
    store_id: str,
    days: Sequence[str],
    extractor: DayExtractor | None = None,
) -> MirrorSourceBundle:
    """Read the v0.14 source boundary from Hive-backed mirrors.

    Every requested daily partition is fetched independently and checked by
    :class:`MirrorExtractor`.  Latest-snapshot dimensions are fetched once and
    their actual source partition is recorded.  This function never reads a
    v1.5 result, inventory, cost, loss or profit output table.
    """
    requested_days = tuple(map(str, days))
    if not requested_days:
        raise ValueError("v0.14 mirror extraction requires at least one business day")
    if requested_days != tuple(sorted(set(requested_days))):
        raise ValueError("v0.14 mirror extraction days must be unique and sorted")
    active_extractor = extractor or MirrorExtractor(QdmApi(), store_id=store_id)
    frames: dict[str, pd.DataFrame] = {}
    manifest_rows: list[dict[str, object]] = []
    complete_rows: list[dict[str, object]] = []

    for source_key in DAILY_SOURCE_KEYS:
        contract = EXTRACTION_CONTRACTS[source_key]
        parts: list[pd.DataFrame] = []
        for business_day in requested_days:
            frame = active_extractor.extract_day(contract, business_day)
            parts.append(frame)
            complete_rows.append({
                "store_id": store_id,
                "business_date": business_day,
                "source_name": source_key,
                "is_complete": True,
                "row_count": len(frame),
                "source_table": contract.full_name,
            })
        combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
            columns=contract.projection
        )
        frames[source_key] = combined
        manifest_rows.append({
            "source_name": source_key,
            "source_table": contract.full_name,
            "requested_start": requested_days[0],
            "requested_end": requested_days[-1],
            "source_partition": f"{requested_days[0]}..{requested_days[-1]}",
            "row_count": len(combined),
            "checksum": stable_frame_checksum(combined),
            "authority": contract.authority.value,
            "source_system": (
                "HIVE_MIRROR" if contract.name in HIVE_SOURCE_BY_MIRROR
                else "AUXILIARY_RELATION_EVIDENCE"
            ),
            "hive_source_tables": " + ".join(HIVE_SOURCE_BY_MIRROR.get(contract.name, ())),
        })

    for source_key in LATEST_SOURCE_KEYS:
        contract = EXTRACTION_CONTRACTS[source_key]
        frame = active_extractor.extract_day(contract, requested_days[-1])
        frames[source_key] = frame
        source_partition = str(frame.attrs.get("source_snapshot_day", ""))
        manifest_rows.append({
            "source_name": source_key,
            "source_table": contract.full_name,
            "requested_start": requested_days[0],
            "requested_end": requested_days[-1],
            "source_partition": source_partition,
            "row_count": len(frame),
            "checksum": stable_frame_checksum(frame),
            "authority": contract.authority.value,
            "source_system": (
                "HIVE_MIRROR" if contract.name in HIVE_SOURCE_BY_MIRROR
                else "AUXILIARY_RELATION_EVIDENCE"
            ),
            "hive_source_tables": " + ".join(HIVE_SOURCE_BY_MIRROR.get(contract.name, ())),
        })
        for business_day in requested_days:
            complete_rows.append({
                "store_id": store_id,
                "business_date": business_day,
                "source_name": source_key,
                "is_complete": True,
                "row_count": len(frame),
                "source_table": contract.full_name,
            })

    return MirrorSourceBundle(
        store_id=store_id,
        requested_days=requested_days,
        frames=frames,
        manifest=pd.DataFrame(manifest_rows),
        completeness=pd.DataFrame(complete_rows),
    )


def persist_mirror_source_bundle(path: Path | str, bundle: MirrorSourceBundle) -> Path:
    """Atomically replace the local, read-only-to-calculation source cache."""
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(target))
    try:
        conn.execute("BEGIN TRANSACTION")
        for source_name, frame in bundle.frames.items():
            table = f"v014_mirror_{source_name}"
            registered = f"_source_{source_name}"
            conn.register(registered, frame)
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{registered}"')
            conn.unregister(registered)
        for table, frame in (
            ("v014_mirror_source_manifest", bundle.manifest),
            ("v014_mirror_source_completeness", bundle.completeness),
        ):
            registered = f"_{table}"
            conn.register(registered, frame)
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{registered}"')
            conn.unregister(registered)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    return target


def extract_and_persist_v014_mirror_sources(
    *,
    store_id: str,
    days: Sequence[str],
    path: Path | str,
    extractor: DayExtractor | None = None,
) -> MirrorSourceBundle:
    """Extract a resumable local mirror cache one source partition at a time.

    A successful source/day is committed immediately with its completeness
    marker. Re-running the command skips those exact partitions, so one slow
    or timed-out API request cannot discard the already verified cache.
    """
    requested_days = tuple(map(str, days))
    if not requested_days or requested_days != tuple(sorted(set(requested_days))):
        raise ValueError("v0.14 mirror extraction days must be non-empty, unique and sorted")
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    active_extractor = extractor or MirrorExtractor(QdmApi(), store_id=store_id)
    conn = duckdb.connect(str(target))
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS v014_mirror_source_completeness ("
            "store_id VARCHAR, business_date VARCHAR, source_name VARCHAR, "
            "is_complete BOOLEAN, row_count BIGINT, source_table VARCHAR)"
        )
        existing_days = {
            str(row[0]) for row in conn.execute(
                "SELECT DISTINCT business_date FROM v014_mirror_source_completeness "
                "WHERE store_id=?", [store_id]
            ).fetchall()
        }
        unexpected_days = existing_days - set(requested_days)
        if unexpected_days:
            raise ValueError(
                "existing v0.14 mirror cache contains dates outside the requested set; "
                f"existing={sorted(existing_days)}, requested={list(requested_days)}"
            )

        def table_exists(table: str) -> bool:
            return bool(conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
                [table],
            ).fetchone()[0])

        def is_complete(source_name: str, business_day: str) -> bool:
            return bool(conn.execute(
                "SELECT COUNT(*) FROM v014_mirror_source_completeness "
                "WHERE store_id=? AND business_date=? AND source_name=? AND is_complete",
                [store_id, business_day, source_name],
            ).fetchone()[0])

        def write_frame(source_name: str, contract: object, frame: pd.DataFrame) -> None:
            table = f"v014_mirror_{source_name}"
            registered = "_v014_increment"
            conn.register(registered, frame)
            if table_exists(table):
                partition = getattr(contract, "partition_column")
                values = tuple(frame[partition].dropna().astype(str).unique())
                for value in values:
                    conn.execute(
                        f'DELETE FROM "{table}" WHERE CAST("{partition}" AS VARCHAR)=?',
                        [value],
                    )
                conn.execute(
                    f'INSERT INTO "{table}" BY NAME SELECT * FROM "{registered}"'
                )
            else:
                conn.execute(
                    f'CREATE TABLE "{table}" AS SELECT * FROM "{registered}"'
                )
            conn.unregister(registered)

        for source_name in DAILY_SOURCE_KEYS:
            contract = EXTRACTION_CONTRACTS[source_name]
            for business_day in requested_days:
                if is_complete(source_name, business_day) and table_exists(
                    f"v014_mirror_{source_name}"
                ):
                    continue
                print(
                    f"[v014-mirror] {source_name} {business_day}",
                    flush=True,
                )
                try:
                    frame = active_extractor.extract_day(contract, business_day)
                except Exception as exc:
                    raise RuntimeError(
                        f"v0.14 mirror extraction failed: "
                        f"source={source_name}, day={business_day}"
                    ) from exc
                conn.execute("BEGIN TRANSACTION")
                try:
                    write_frame(source_name, contract, frame)
                    conn.execute(
                        "DELETE FROM v014_mirror_source_completeness "
                        "WHERE store_id=? AND business_date=? AND source_name=?",
                        [store_id, business_day, source_name],
                    )
                    conn.execute(
                        "INSERT INTO v014_mirror_source_completeness VALUES (?, ?, ?, TRUE, ?, ?)",
                        [
                            store_id, business_day, source_name, len(frame),
                            contract.full_name,
                        ],
                    )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

        for source_name in LATEST_SOURCE_KEYS:
            contract = EXTRACTION_CONTRACTS[source_name]
            if all(is_complete(source_name, day) for day in requested_days) and table_exists(
                f"v014_mirror_{source_name}"
            ):
                continue
            print(f"[v014-mirror] {source_name} latest", flush=True)
            try:
                frame = active_extractor.extract_day(contract, requested_days[-1])
            except Exception as exc:
                raise RuntimeError(
                    f"v0.14 latest mirror extraction failed: source={source_name}"
                ) from exc
            conn.execute("BEGIN TRANSACTION")
            try:
                table = f"v014_mirror_{source_name}"
                registered = "_v014_latest"
                conn.register(registered, frame)
                conn.execute(f'DROP TABLE IF EXISTS "{table}"')
                conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{registered}"')
                conn.unregister(registered)
                conn.execute(
                    "DELETE FROM v014_mirror_source_completeness "
                    "WHERE store_id=? AND source_name=?",
                    [store_id, source_name],
                )
                for business_day in requested_days:
                    conn.execute(
                        "INSERT INTO v014_mirror_source_completeness VALUES (?, ?, ?, TRUE, ?, ?)",
                        [
                            store_id, business_day, source_name, len(frame),
                            contract.full_name,
                        ],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        manifest_rows: list[dict[str, object]] = []
        for source_name in (*DAILY_SOURCE_KEYS, *LATEST_SOURCE_KEYS):
            contract = EXTRACTION_CONTRACTS[source_name]
            frame = conn.execute(
                f'SELECT * FROM "v014_mirror_{source_name}"'
            ).df()
            manifest_rows.append({
                "source_name": source_name,
                "source_table": contract.full_name,
                "requested_start": requested_days[0],
                "requested_end": requested_days[-1],
                "source_partition": (
                    f"{requested_days[0]}..{requested_days[-1]}"
                    if source_name in DAILY_SOURCE_KEYS
                    else str(frame[contract.partition_column].dropna().astype(str).max())
                ),
                "row_count": len(frame),
                "checksum": stable_frame_checksum(frame),
                "authority": contract.authority.value,
                "source_system": (
                    "HIVE_MIRROR" if contract.name in HIVE_SOURCE_BY_MIRROR
                    else "AUXILIARY_RELATION_EVIDENCE"
                ),
                "hive_source_tables": " + ".join(
                    HIVE_SOURCE_BY_MIRROR.get(contract.name, ())
                ),
            })
        manifest = pd.DataFrame(manifest_rows)
        conn.register("_v014_manifest", manifest)
        conn.execute('DROP TABLE IF EXISTS "v014_mirror_source_manifest"')
        conn.execute(
            'CREATE TABLE "v014_mirror_source_manifest" AS '
            'SELECT * FROM "_v014_manifest"'
        )
        conn.unregister("_v014_manifest")
    finally:
        conn.close()
    return load_mirror_source_bundle(target, store_id=store_id)


def load_mirror_source_bundle(
    path: Path | str,
    *,
    store_id: str,
) -> MirrorSourceBundle:
    """Load the exact local cache written by :func:`persist_mirror_source_bundle`."""
    source = Path(path).resolve()
    if not source.exists():
        raise FileNotFoundError(f"v0.14 mirror source cache not found: {source}")
    conn = duckdb.connect(str(source), read_only=True)
    try:
        existing = {
            str(row[0]) for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        required_tables = {
            *(f"v014_mirror_{name}" for name in (*DAILY_SOURCE_KEYS, *LATEST_SOURCE_KEYS)),
            "v014_mirror_source_manifest", "v014_mirror_source_completeness",
        }
        missing = sorted(required_tables - existing)
        if missing:
            raise KeyError(f"v0.14 mirror source cache missing tables: {missing}")
        frames = {
            name: conn.execute(f'SELECT * FROM "v014_mirror_{name}"').df()
            for name in (*DAILY_SOURCE_KEYS, *LATEST_SOURCE_KEYS)
        }
        manifest = conn.execute("SELECT * FROM v014_mirror_source_manifest").df()
        completeness = conn.execute("SELECT * FROM v014_mirror_source_completeness").df()
    finally:
        conn.close()
    scoped = completeness.loc[completeness["store_id"].astype(str).eq(store_id)]
    if scoped.empty:
        raise ValueError(f"v0.14 mirror source cache has no completeness rows for {store_id}")
    days = tuple(sorted(scoped["business_date"].astype(str).unique()))
    return MirrorSourceBundle(
        store_id=store_id,
        requested_days=days,
        frames=frames,
        manifest=manifest,
        completeness=completeness,
    )
