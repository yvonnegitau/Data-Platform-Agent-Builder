"""
database.py — single source of truth for DuckDB access.

All queries flow through the `run_query` function, which:
  - Opens the database read-only (so nothing in the silver layer can be mutated)
  - Returns a consistent JSON envelope that every MCP tool can rely on
  - Auto-injects era caveats when results span known F1 boundary years
"""

import time
import duckdb
from config import DB_PATH, MAX_ROWS, ERA_CAVEATS


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

    try:
        # Read-only mode: DuckDB will refuse any INSERT/UPDATE/DELETE
        conn = duckdb.connect(str(DB_PATH), read_only=True)

        # Apply a safety limit if the query doesn't already have one
        capped_sql = _apply_limit(sql, min(limit, MAX_ROWS))

        result = conn.execute(capped_sql, params or [])
        columns = [desc[0] for desc in result.description]
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
        conn.close()

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
