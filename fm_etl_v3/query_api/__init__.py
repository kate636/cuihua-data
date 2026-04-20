"""FM Query API — DuckDB 只读 HTTP 查询服务

提供三个只读端点，供 5 人以内团队远程查 /opt/fm/data/fm.duckdb：

- POST /api/query          执行 SELECT SQL
- GET  /api/tables         列出所有表 + 行数
- GET  /api/schema/{table} 查看字段

安全机制（四重防护）：
  1. Bearer Token 鉴权（FM_TOKENS 环境变量）
  2. SQL 白名单（只允许 SELECT/SHOW/DESCRIBE/EXPLAIN/WITH）
  3. 只读连接（duckdb.connect(path, read_only=True)）
  4. 单查询超时 60 秒
"""

__version__ = "0.1.0"
