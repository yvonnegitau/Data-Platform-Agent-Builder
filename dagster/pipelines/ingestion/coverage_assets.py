"""
coverage_assets.py — Dagster asset that refreshes warehouse coverage metrics.

Runs daily (scheduled in repo.py) and appends a snapshot row to
f1_silver.data_coverage_history in Postgres. This builds the historical
trend data behind coverage reporting. (Current, point-in-time coverage is
also available as the f1_silver.season_completeness view built by dbt.)

The asset:
  1. Connects to Postgres (read-write for this asset only)
  2. Creates the history table if it doesn't exist
  3. Replaces today's snapshot rows with current coverage metrics
  4. Returns a summary for Dagster's asset materialisation record
"""

import os
from datetime import date

import psycopg2
import dagster as dg

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "dagster"),
    user=os.getenv("POSTGRES_USER", "dagster"),
    password=os.getenv("POSTGRES_PASSWORD", "dagsterpass"),
)


@dg.asset(
    group_name="warehouse_coverage",
    description=(
        "Refreshes the f1_silver.data_coverage_history table in Postgres with a daily "
        "snapshot of how complete each season's data is. Builds the 'coverage over time' "
        "trend; current coverage is also available via the season_completeness view."
    ),
)
def coverage_refresh(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Compute and store current warehouse coverage metrics in Postgres."""

    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS f1_silver")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS f1_silver.data_coverage_history (
            snapshot_date         DATE NOT NULL,
            season                INTEGER NOT NULL,
            rounds_loaded         INTEGER NOT NULL,
            rounds_in_schedule    INTEGER NOT NULL,
            pct_complete          NUMERIC(5,1) NOT NULL,
            total_fact_rows       INTEGER NOT NULL,
            PRIMARY KEY (snapshot_date, season)
        )
    """)

    today = date.today().isoformat()

    # Idempotent re-run: clear today's rows first
    cur.execute(
        "DELETE FROM f1_silver.data_coverage_history WHERE snapshot_date = %s",
        [today],
    )

    cur.execute("""
        INSERT INTO f1_silver.data_coverage_history
        SELECT
            %s::date                     AS snapshot_date,
            s.season,
            COALESCE(l.rounds_loaded, 0) AS rounds_loaded,
            s.rounds_in_schedule,
            ROUND(
                COALESCE(l.rounds_loaded, 0)::numeric / s.rounds_in_schedule * 100, 1
            )                            AS pct_complete,
            COALESCE(f.fact_rows, 0)     AS total_fact_rows
        FROM (
            SELECT season, COUNT(*) AS rounds_in_schedule
            FROM f1_silver.dim_races GROUP BY season
        ) s
        LEFT JOIN (
            SELECT season, COUNT(DISTINCT round) AS rounds_loaded
            FROM f1_silver.fact_race_results GROUP BY season
        ) l ON s.season = l.season
        LEFT JOIN (
            SELECT season, COUNT(*) AS fact_rows
            FROM f1_silver.fact_race_results GROUP BY season
        ) f ON s.season = f.season
    """, [today])

    cur.execute("""
        SELECT
            COUNT(*)                                              AS seasons_snapshotted,
            COUNT(*) FILTER (WHERE pct_complete = 100)            AS complete_seasons,
            COUNT(*) FILTER (WHERE pct_complete BETWEEN 1 AND 99) AS partial_seasons,
            ROUND(AVG(pct_complete), 1)                           AS avg_pct_complete
        FROM f1_silver.data_coverage_history
        WHERE snapshot_date = CURRENT_DATE
    """)
    seasons_snapshotted, complete, partial, avg_pct = cur.fetchone()

    cur.close()
    conn.close()

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
