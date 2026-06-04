"""
tools/raw_query.py — The execute_sql tool.

This is the most powerful tool in the server — it lets Claude write any
SELECT query against the silver layer. The semantic layer tools exist to
give Claude enough context to use this well.

Safety rules:
  1. Only SELECT statements are allowed (read-only DuckDB connection also
     enforces this at the DB level, but we reject early for a clear error).
  2. Queries are capped at MAX_ROWS rows.
  3. Query length is capped to prevent abuse.
"""

import re
from database import run_query
from config import MAX_ROWS, MAX_SQL_LENGTH


def execute_sql(sql: str, limit: int = 100) -> dict:
    """
    Execute a read-only SQL query against the F1 silver layer.

    Use this for any analysis not covered by the pre-built tools.
    Before writing SQL, call get_schema() and get_query_pattern() to
    understand the available tables, column names, and join keys.

    Tables available (all in the 'silver' schema):
      - silver.fact_race_results  (one row per driver per race)
      - silver.dim_driver         (driver attributes per season)
      - silver.dim_constructor    (team attributes per season)
      - silver.dim_circuit        (circuit characteristics)
      - silver.dim_races          (race event metadata)
      - silver.dim_status         (result status codes)

    Rules:
      - SELECT statements only. INSERT/UPDATE/DELETE will be rejected.
      - Results capped at 500 rows (use limit param to set lower).
      - Always qualify table names with the schema: silver.fact_race_results

    Returns rows, column names, row count, execution time, and any era caveats.
    """
    # Validate length
    if len(sql) > MAX_SQL_LENGTH:
        return {
            "status": "error",
            "error": f"Query too long ({len(sql)} chars). Maximum is {MAX_SQL_LENGTH}.",
            "rows": [],
        }

    # Validate it's a SELECT (strip comments and leading whitespace first)
    clean = _strip_comments(sql).strip().upper()
    if not clean.startswith("SELECT") and not clean.startswith("WITH"):
        return {
            "status": "error",
            "error": "Only SELECT (or WITH ... SELECT) statements are permitted.",
            "rows": [],
        }

    # Check for any mutating keywords — belt and braces on top of read-only conn
    mutating = r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|COPY)\b'
    if re.search(mutating, clean):
        return {
            "status": "error",
            "error": "Query contains a disallowed keyword. Only read operations are permitted.",
            "rows": [],
        }

    capped_limit = min(limit, MAX_ROWS)
    return run_query(sql, limit=capped_limit)


# ── helpers ───────────────────────────────────────────────────────────────────

def _strip_comments(sql: str) -> str:
    """Remove SQL line comments (--) and block comments (/* */)."""
    # Block comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    # Line comments
    sql = re.sub(r'--[^\n]*', '', sql)
    return sql
