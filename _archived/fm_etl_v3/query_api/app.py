"""FastAPI 只读查询服务

启动方式（生产，uvicorn 托管）：
    uvicorn fm_etl_v3.query_api.app:app --host 127.0.0.1 --port 5003

依赖环境变量：
    FM_DUCKDB_PATH  DuckDB 文件路径（如 /opt/fm/data/fm.duckdb）
    FM_TOKENS       鉴权 Token 列表，格式 alice:xxx,bob:yyy
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any

import duckdb
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from .auth import verify_token
from .models import (
    ColumnInfo,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
    TableInfo,
    TablesResponse,
)
from .sql_guard import SQLGuardError, validate_sql


DB_PATH = os.getenv("FM_DUCKDB_PATH", "/opt/fm/data/fm.duckdb")
QUERY_TIMEOUT_SEC = int(os.getenv("FM_QUERY_TIMEOUT_SEC", "60"))

app = FastAPI(
    title="FM Query API",
    version="0.1.0",
    description="DuckDB 只读 HTTP 查询服务（/opt/fm/data/fm.duckdb）",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)


@contextmanager
def _readonly_conn():
    """每请求新建只读连接，避免和 ETL 抢写锁。"""
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        try:
            conn.execute(f"SET statement_timeout = '{QUERY_TIMEOUT_SEC}s'")
        except Exception:
            pass
        yield conn
    finally:
        conn.close()


def _json_safe(v: Any) -> Any:
    """把 DuckDB 返回的非 JSON 友好类型转成字符串。"""
    if v is None:
        return None
    if isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _json_safe(x) for k, x in v.items()}
    return str(v)


@app.get("/api/health")
def health() -> dict[str, str]:
    """健康检查，不需要鉴权。"""
    return {"status": "ok", "db_path": DB_PATH}


@app.post("/api/query", response_model=QueryResponse)
def query(req: QueryRequest, user: str = Depends(verify_token)) -> QueryResponse:
    try:
        safe_sql = validate_sql(req.sql)
    except SQLGuardError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"SQL rejected by guard: {e}",
        )

    start = time.perf_counter()
    with _readonly_conn() as conn:
        try:
            wrapped = f"SELECT * FROM ({safe_sql}) _q LIMIT {req.limit + 1}"
            df = conn.execute(wrapped).df()
        except duckdb.Error as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"DuckDB error: {e}",
            )

    elapsed_ms = int((time.perf_counter() - start) * 1000)
    truncated = len(df) > req.limit
    if truncated:
        df = df.iloc[: req.limit]

    rows = [
        {col: _json_safe(val) for col, val in record.items()}
        for record in df.to_dict(orient="records")
    ]

    return QueryResponse(
        rows=rows,
        row_count=len(rows),
        elapsed_ms=elapsed_ms,
        truncated=truncated,
    )


@app.get("/api/tables", response_model=TablesResponse)
def list_tables(user: str = Depends(verify_token)) -> TablesResponse:
    with _readonly_conn() as conn:
        rows = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
            "ORDER BY table_schema, table_name"
        ).fetchall()

        tables: list[TableInfo] = []
        for schema, name in rows:
            try:
                cnt = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{name}"').fetchone()
                row_count = int(cnt[0]) if cnt else 0
            except Exception:
                row_count = -1
            tables.append(TableInfo(name=name, row_count=row_count, schema_name=schema))

    return TablesResponse(tables=tables, database_path=DB_PATH)


@app.get("/api/schema/{table}", response_model=SchemaResponse)
def describe_table(table: str, user: str = Depends(verify_token)) -> SchemaResponse:
    import re as _re
    if not _re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid table name",
        )

    with _readonly_conn() as conn:
        if not conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = ?",
            [table],
        ).fetchone()[0]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table '{table}' not found",
            )

        cols = conn.execute(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ? "
            "ORDER BY ordinal_position",
            [table],
        ).fetchall()

        row_count = int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])

    return SchemaResponse(
        table=table,
        columns=[
            ColumnInfo(name=c[0], type=c[1], nullable=(str(c[2]).upper() == "YES"))
            for c in cols
        ],
        row_count=row_count,
    )
