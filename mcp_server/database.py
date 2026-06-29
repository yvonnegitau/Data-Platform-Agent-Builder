"""
database.py — single source of truth for query access.

All queries flow through `run_query`, which:
  - Reads the silver layer directly from Postgres (the f1_silver schema)
  - Returns a consistent JSON envelope that every MCP tool can rely on
  - Auto-injects era caveats when results span known F1 boundary years

Tool SQL is written against a `silver.` schema prefix (a holdover from the
DuckDB days); run_query rewrites that to the real Postgres schema
(`f1_silver`) centrally, so the tools need no changes. The connection is
read-only, so nothing in the silver layer can be mutated through this path.
"""

import re
import time
from decimal import Decimal

import psycopg2

from config import PG_DSN, PG_SILVER_SCHEMA, MAX_ROWS, ERA_CAVEATS

# Rewrites a bare `silver.` schema prefix to the real Postgres schema.
# `\bsilver\.` never matches inside `f1_silver.` (the char before "silver"
# there is "_", a word char, so the word boundary does not apply).
_SCHEMA_RE = re.compile(r"\bsilver\.")


def _connect() -> "psycopg2.extensions.connection":
    conn = psycopg2.connect(PG_DSN)
    conn.set_session(readonly=True, autocommit=True)
    return conn


def _coerce(value):
    """Make values JSON-friendly: Decimal -> float (dates handled downstream)."""
    if isinstance(value, Decimal):
        return float(value)
    return value


def run_query(sql: str, params: list | None = None, limit: int = 100) -> dict:
    """
    Execute a SQL query against the F1 silver layer and return a response envelope.

    Returns:
        {
            "status": "success" | "error",
            "rows": [ {col: val, ...}, ... ],
            "columns": ["col1", "col2", ...],
            "row_count": int,
            "execution_ms": int,
            "notes": ["era caveat if applicable", ...]   # may be empty
        }
    """
    start = time.perf_counter()
    conn = None

    try:
        sql = _SCHEMA_RE.sub(f"{PG_SILVER_SCHEMA}.", sql)
        capped_sql = _apply_limit(sql, min(limit, MAX_ROWS))

        conn = _connect()
        cur = conn.cursor()
        if params:
            # Tool SQL uses `?` placeholders (DuckDB style); psycopg uses `%s`.
            cur.execute(capped_sql.replace("?", "%s"), params)
        else:
            # No params: execute without interpolation so literal `%`
            # (e.g. ILIKE '%name%' in query patterns) is left untouched.
            cur.execute(capped_sql)

        columns = [desc[0] for desc in cur.description]
        rows = [{c: _coerce(v) for c, v in zip(columns, row)} for row in cur.fetchall()]
        cur.close()

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        notes = _era_notes(rows, columns)

        return {
            "status": "success",
            "rows": rows,
            "columns": columns,
            "row_count": len(rows),
            "execution_ms": elapsed_ms,
            "notes": notes,
        }

    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return {
            "status": "error",
            "error": str(e),
            "rows": [],
            "columns": [],
            "row_count": 0,
            "execution_ms": elapsed_ms,
            "notes": [],
        }
    finally:
        if conn is not None:
            conn.close()


def _apply_limit(sql: str, limit: int) -> str:
    """Add LIMIT clause if the query doesn't already have one."""
    normalized = sql.strip().rstrip(";").upper()
    if "LIMIT" not in normalized:
        return f"{sql.strip().rstrip(';')} LIMIT {limit}"
    return sql


def _era_notes(rows: list[dict], columns: list[str]) -> list[str]:
    """
    Scan result rows for 'season' values that cross known F1 era boundaries
    and return relevant caveats. Empty list if no caveats apply.
    """
    if "season" not in columns or not rows:
        return []

    seasons = {row["season"] for row in rows if row.get("season") is not None}
    if not seasons:
        return []

    min_season = min(seasons)
    notes = []
    for caveat in ERA_CAVEATS:
        if min_season < caveat["before_year"]:
            notes.append(caveat["note"])

    return notes
