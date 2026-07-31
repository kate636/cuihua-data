from __future__ import annotations

import uuid
from pathlib import Path

import duckdb
import pandas as pd

from fmetl.contracts.v014 import OUTPUT_CONTRACT


INTERNAL_TABLES = (
    "v014_relation_registry", "v014_relation_resolution", "v014_internal_posting",
    "v014_sku_daily", "v014_quarantine", "v014_run_manifest", "v014_validation_result",
)


def _replace(conn: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame) -> None:
    registered = f"_v014_{uuid.uuid4().hex[:12]}"
    conn.register(registered, frame)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" AS SELECT * FROM "{registered}"')
    finally:
        conn.unregister(registered)


def _replace_levels_result(
    conn: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
) -> None:
    registered = f"_v014_{uuid.uuid4().hex[:12]}"
    conn.register(registered, frame)
    try:
        conn.execute('DROP TABLE IF EXISTS "t_v014_levels_result"')
        definitions = ", ".join(
            f'"{field.name}" {field.duckdb_type}' for field in OUTPUT_CONTRACT
        )
        conn.execute(f'CREATE TABLE "t_v014_levels_result" ({definitions})')
        projections = ", ".join(
            f'CAST("{field.name}" AS {field.duckdb_type}) AS "{field.name}"'
            for field in OUTPUT_CONTRACT
        )
        conn.execute(
            f'INSERT INTO "t_v014_levels_result" SELECT {projections} FROM "{registered}"'
        )
    finally:
        conn.unregister(registered)


def persist_v014_shadow(
    db_path: Path | str,
    *,
    levels_result: pd.DataFrame,
    relation_registry: pd.DataFrame | None,
    relation_resolution: pd.DataFrame | None,
    relation_audit_db: Path | str | None = None,
    internal_posting: pd.DataFrame,
    sku_daily: pd.DataFrame,
    quarantine: pd.DataFrame,
    run_manifest: pd.DataFrame,
    validation_result: pd.DataFrame,
) -> None:
    """Atomically replace only local v0.14 shadow tables."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    expected = [field.name for field in OUTPUT_CONTRACT]
    if list(levels_result.columns) != expected:
        raise ValueError("t_v014_levels_result does not match the frozen 125-field contract")
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("BEGIN")
        _replace_levels_result(conn, levels_result)
        if relation_audit_db is not None:
            audit_path = str(Path(relation_audit_db).resolve()).replace("'", "''")
            conn.execute(f"ATTACH '{audit_path}' AS relation_audit (READ_ONLY)")
            for table in ("v014_relation_registry", "v014_relation_resolution"):
                conn.execute(f'DROP TABLE IF EXISTS "{table}"')
                conn.execute(
                    f'CREATE TABLE "{table}" AS '
                    f'SELECT * FROM relation_audit."{table}"'
                )
            conn.execute("DETACH relation_audit")
        elif relation_registry is None or relation_resolution is None:
            raise ValueError(
                "relation audit requires frames or a local relation_audit_db"
            )
        else:
            _replace(conn, "v014_relation_registry", relation_registry)
            _replace(conn, "v014_relation_resolution", relation_resolution)
        for table, frame in (
            ("v014_internal_posting", internal_posting),
            ("v014_sku_daily", sku_daily),
            ("v014_quarantine", quarantine),
            ("v014_run_manifest", run_manifest),
            ("v014_validation_result", validation_result),
        ):
            _replace(conn, table, frame)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
