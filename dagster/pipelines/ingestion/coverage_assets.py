"""
coverage_assets.py — Dagster asset that refreshes warehouse coverage metrics.

Runs daily (scheduled in repo.py) and writes a snapshot row to
warehouse.data_coverage_history in DuckDB. This builds the historical
trend data that the MCP get_coverage_chart_data tool can surface.

The asset:
  1. Opens DuckDB in read-write mode (safe — MCP server is read-only)
  2. Creates the warehouse schema and history table if they don't exist
  3. Appends a new snapshot row with current coverage metrics
  4. Returns a summary for Dagster's asset materialisation record
"""

import os
from datetime import date
from pathlib import Path

import duckdb
import dagster as dg

DB_PATH = Path(os.getenv("DUCKDB_PATH", "/data/medallion/f1_data.duckdb"))


@dg.asset(
    group_name="warehouse_coverage",
    description=(
        "Refreshes the data_coverage_history table in DuckDB with a daily snapshot "
        "of how complete each season's data is. Powers the 'coverage over time' chart "
        "in the Open WebUI warehouse assistant."
    ),
)
def coverage_refresh(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Compute and store current warehouse coverage metrics."""

    conn = duckdb.connect(str(DB_PATH))  # read-write for this asset only

    # Ensure the warehouse schema and history table exist
    conn.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS warehouse.data_coverage_history (
            snapshot_date         DATE NOT NULL,
            season                INTEGER NOT NULL,
            rounds_loaded         INTEGER NOT NULL,
            rounds_in_schedule    INTEGER NOT NULL,
            pct_complete          DECIMAL(5,1) NOT NULL,
            total_fact_rows       INTEGER NOT NULL,
            PRIMARY KEY (snapshot_date, season)
        )
    """)

    today = date.today().isoformat()

    # Delete today's existing rows (idempotent re-run)
    conn.execute(
        "DELETE FROM warehouse.data_coverage_history WHERE snapshot_date = ?",
        [today],
    )

    # Compute current coverage and insert
    conn.execute(f"""
        INSERT INTO warehouse.data_coverage_history
        SELECT
            DATE '{today}'               AS snapshot_date,
            s.season,
            COALESCE(l.rounds_loaded, 0) AS rounds_loaded,
            s.rounds_in_schedule,
            ROUND(
                COALESCE(l.rounds_loaded, 0)::FLOAT / s.rounds_in_schedule * 100, 1
            )                            AS pct_complete,
            COALESCE(f.fact_rows, 0)     AS total_fact_rows
        FROM (
            SELECT season, COUNT(*) AS rounds_in_schedule
            FROM silver.dim_races GROUP BY season
        ) s
        LEFT JOIN (
            SELECT season, COUNT(DISTINCT round) AS rounds_loaded
            FROM silver.fact_race_results GROUP BY season
        ) l ON s.season = l.season
        LEFT JOIN (
            SELECT season, COUNT(*) AS fact_rows
            FROM silver.fact_race_results GROUP BY season
        ) f ON s.season = f.season
    """)

    # Fetch summary for materialisation metadata
    summary = conn.execute("""
        SELECT
            COUNT(*)                                          AS seasons_snapshotted,
            COUNT(*) FILTER (WHERE pct_complete = 100)       AS complete_seasons,
            COUNT(*) FILTER (WHERE pct_complete BETWEEN 1 AND 99) AS partial_seasons,
            ROUND(AVG(pct_complete), 1)                      AS avg_pct_complete
        FROM warehouse.data_coverage_history
        WHERE snapshot_date = CURRENT_DATE
    """).fetchone()

    conn.close()

    seasons_snapshotted, complete, partial, avg_pct = summary

    context.log.info(
        f"Coverage snapshot written: {seasons_snapshotted} seasons, "
        f"{complete} complete, {partial} partial, avg {avg_pct}% complete"
    )

    return dg.MaterializeResult(
        metadata={
            "snapshot_date": today,
            "seasons_snapshotted": int(seasons_snapshotted or 0),
            "complete_seasons": int(complete or 0),
            "partial_seasons": int(partial or 0),
            "avg_pct_complete": float(avg_pct or 0),
        }
    )
