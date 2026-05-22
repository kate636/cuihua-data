"""Pydantic 请求/响应模型"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(..., description="要执行的 SELECT SQL 语句", min_length=1, max_length=20000)
    limit: int = Field(10000, description="最大返回行数（安全上限 50000）", ge=1, le=50000)


class QueryResponse(BaseModel):
    rows: list[dict[str, Any]]
    row_count: int
    elapsed_ms: int
    truncated: bool = Field(False, description="结果是否因 limit 被截断")


class TableInfo(BaseModel):
    name: str
    row_count: int
    schema_name: str = "main"


class TablesResponse(BaseModel):
    tables: list[TableInfo]
    database_path: str


class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool


class SchemaResponse(BaseModel):
    table: str
    columns: list[ColumnInfo]
    row_count: int


class ErrorResponse(BaseModel):
    error: str
    detail: str | None = None
