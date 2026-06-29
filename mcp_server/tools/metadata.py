"""
tools/metadata.py — Data warehouse metadata and coverage tools.

These tools answer questions about the warehouse itself:
  - What data exists? Which seasons? How fresh?
  - Are there gaps in the data?
  - What does completeness look like season by season?

Also exposes get_coverage_chart_data() which returns structured JSON
purpose-built for chart generation by the LLM (Chart.js).
"""

import yaml
from pathlib import Path
from database import run_query

_PERSONAS_DIR = Path(__file__).parent.parent / "personas"


def get_data_freshness() -> dict:
    """
    Returns a quick snapshot of data freshness across the warehouse:
    latest season loaded, latest race, total races in the database,
    and the earliest season available.

    Call this when someone asks 'how current is the data?' or
    'when was the data last updated?'
    """
    sql = """
        SELECT
            MAX(f.season)          AS latest_season,
            MIN(f.season)          AS earliest_season,
            COUNT(DISTINCT f.season) AS total_seasons,
            COUNT(*)               AS total_race_entries,
            COUNT(DISTINCT f.round || '-' || f.season::VARCHAR) AS total_races,
            MAX(r.race_date)       AS latest_race_date,
            MAX(r.race_name)       AS latest_race_name
        FROM silver.fact_race_results f
        JOIN silver.dim_races r ON f.race_key = r.dim_race_key
    """
    result = run_query(sql, limit=1)
    result["tool"] = "get_data_freshness"
    return result


def get_data_coverage() -> dict:
    """
    Returns a row-count and season-range summary for each table
    in the silver layer. Use this to understand what data exists
    across all tables, not just race results.

    Shows: table name, total rows, first and last season, number of
    distinct seasons loaded, and the latest data point.
    """
    sql = """
        SELECT 'fact_race_results' AS table_name,
            COUNT(*)                    AS total_rows,
            MIN(f.season)               AS first_season,
            MAX(f.season)               AS last_season,
            COUNT(DISTINCT f.season)    AS seasons_loaded,
            MAX(r.race_date)            AS latest_date
        FROM silver.fact_race_results f
        JOIN silver.dim_races r ON f.race_key = r.dim_race_key

        UNION ALL

        SELECT 'dim_driver',
            COUNT(*), MIN(season), MAX(season), COUNT(DISTINCT season), NULL
        FROM silver.dim_driver

        UNION ALL

        SELECT 'dim_constructor',
            COUNT(*), MIN(season), MAX(season), COUNT(DISTINCT season), NULL
        FROM silver.dim_constructor

        UNION ALL

        SELECT 'dim_races',
            COUNT(*), MIN(season), MAX(season), COUNT(DISTINCT season), MAX(race_date)
        FROM silver.dim_races

        UNION ALL

        SELECT 'dim_circuit',
            COUNT(*), NULL, NULL, NULL, NULL
        FROM silver.dim_circuit

        ORDER BY table_name
    """
    result = run_query(sql, limit=20)
    result["tool"] = "get_data_coverage"
    return result


def get_season_completeness(season: int | None = None) -> dict:
    """
    Shows how complete each season's data is — rounds loaded versus
    the total rounds in the race schedule for that season.

    Returns: season, rounds_loaded, rounds_in_schedule, pct_complete,
    and a status flag (Complete / Partial / Minimal).

    Pass a season year to check one specific season.
    Leave empty to see all seasons.

    Use this to answer: 'Do we have all the races for 2023?'
    or 'Which seasons have gaps in the data?'
    """
    season_filter = "AND f.season = ?" if season else ""
    params = [season] if season else []

    sql = f"""
        WITH schedule AS (
            SELECT season, COUNT(*) AS rounds_in_schedule
            FROM silver.dim_races
            GROUP BY season
        ),
        loaded AS (
            SELECT season, COUNT(DISTINCT round) AS rounds_loaded
            FROM silver.fact_race_results
            {season_filter.replace('f.season', 'season')}
            GROUP BY season
        )
        SELECT
            s.season,
            COALESCE(l.rounds_loaded, 0)        AS rounds_loaded,
            s.rounds_in_schedule,
            ROUND(
                COALESCE(l.rounds_loaded, 0)::numeric
                / s.rounds_in_schedule * 100, 1
            )                                   AS pct_complete,
            CASE
                WHEN COALESCE(l.rounds_loaded, 0) = s.rounds_in_schedule THEN 'Complete'
                WHEN COALESCE(l.rounds_loaded, 0) >= s.rounds_in_schedule * 0.5 THEN 'Partial'
                WHEN COALESCE(l.rounds_loaded, 0) > 0 THEN 'Minimal'
                ELSE 'Missing'
            END                                 AS status
        FROM schedule s
        LEFT JOIN loaded l ON s.season = l.season
        {'WHERE s.season = ?' if season else ''}
        ORDER BY s.season DESC
    """
    # Rebuild cleanly to avoid double filter
    if season:
        sql = """
            WITH schedule AS (
                SELECT season, COUNT(*) AS rounds_in_schedule
                FROM silver.dim_races
                GROUP BY season
            ),
            loaded AS (
                SELECT season, COUNT(DISTINCT round) AS rounds_loaded
                FROM silver.fact_race_results
                WHERE season = ?
                GROUP BY season
            )
            SELECT
                s.season,
                COALESCE(l.rounds_loaded, 0)        AS rounds_loaded,
                s.rounds_in_schedule,
                ROUND(
                    COALESCE(l.rounds_loaded, 0)::numeric
                    / s.rounds_in_schedule * 100, 1
                )                                   AS pct_complete,
                CASE
                    WHEN COALESCE(l.rounds_loaded, 0) = s.rounds_in_schedule THEN 'Complete'
                    WHEN COALESCE(l.rounds_loaded, 0) >= s.rounds_in_schedule * 0.5 THEN 'Partial'
                    WHEN COALESCE(l.rounds_loaded, 0) > 0 THEN 'Minimal'
                    ELSE 'Missing'
                END                                 AS status
            FROM schedule s
            LEFT JOIN loaded l ON s.season = l.season
            WHERE s.season = ?
            ORDER BY s.season DESC
        """
        params = [season, season]
    else:
        sql = """
            WITH schedule AS (
                SELECT season, COUNT(*) AS rounds_in_schedule
                FROM silver.dim_races
                GROUP BY season
            ),
            loaded AS (
                SELECT season, COUNT(DISTINCT round) AS rounds_loaded
                FROM silver.fact_race_results
                GROUP BY season
            )
            SELECT
                s.season,
                COALESCE(l.rounds_loaded, 0)        AS rounds_loaded,
                s.rounds_in_schedule,
                ROUND(
                    COALESCE(l.rounds_loaded, 0)::numeric
                    / s.rounds_in_schedule * 100, 1
                )                                   AS pct_complete,
                CASE
                    WHEN COALESCE(l.rounds_loaded, 0) = s.rounds_in_schedule THEN 'Complete'
                    WHEN COALESCE(l.rounds_loaded, 0) >= s.rounds_in_schedule * 0.5 THEN 'Partial'
                    WHEN COALESCE(l.rounds_loaded, 0) > 0 THEN 'Minimal'
                    ELSE 'Missing'
                END                                 AS status
            FROM schedule s
            LEFT JOIN loaded l ON s.season = l.season
            ORDER BY s.season DESC
        """
        params = []

    result = run_query(sql, params=params, limit=100)
    result["tool"] = "get_season_completeness"
    result["season_filter"] = season
    return result


def get_coverage_chart_data() -> dict:
    """
    Returns data structured specifically for chart generation.
    Call this when asked to 'show a dashboard', 'chart the coverage',
    or 'visualise the data'.

    Returns three sections:
      - by_season: rounds loaded vs. schedule per season (for bar chart)
      - by_table: row counts and season ranges per table (for health summary)
      - summary: headline numbers (total seasons, complete vs. partial)

    After calling this, generate an HTML chart artifact using Chart.js.
    """
    # Season completeness for chart
    seasons_result = run_query("""
        WITH schedule AS (
            SELECT season, COUNT(*) AS rounds_in_schedule
            FROM silver.dim_races GROUP BY season
        ),
        loaded AS (
            SELECT season, COUNT(DISTINCT round) AS rounds_loaded
            FROM silver.fact_race_results GROUP BY season
        )
        SELECT
            s.season,
            COALESCE(l.rounds_loaded, 0)  AS rounds_loaded,
            s.rounds_in_schedule,
            ROUND(COALESCE(l.rounds_loaded, 0)::numeric / s.rounds_in_schedule * 100, 1) AS pct_complete
        FROM schedule s
        LEFT JOIN loaded l ON s.season = l.season
        ORDER BY s.season
    """, limit=200)

    # Table health
    tables_result = run_query("""
        SELECT 'fact_race_results' AS table_name, COUNT(*) AS row_count,
               MIN(season) AS first_season, MAX(season) AS last_season
        FROM silver.fact_race_results
        UNION ALL
        SELECT 'dim_driver', COUNT(*), MIN(season), MAX(season) FROM silver.dim_driver
        UNION ALL
        SELECT 'dim_constructor', COUNT(*), MIN(season), MAX(season) FROM silver.dim_constructor
        UNION ALL
        SELECT 'dim_races', COUNT(*), MIN(season), MAX(season) FROM silver.dim_races
        UNION ALL
        SELECT 'dim_circuit', COUNT(*), NULL, NULL FROM silver.dim_circuit
        ORDER BY table_name
    """, limit=10)

    # Summary numbers
    summary_result = run_query("""
        WITH season_status AS (
            WITH schedule AS (SELECT season, COUNT(*) AS total FROM silver.dim_races GROUP BY season),
                 loaded   AS (SELECT season, COUNT(DISTINCT round) AS loaded FROM silver.fact_race_results GROUP BY season)
            SELECT s.season,
                   CASE WHEN COALESCE(l.loaded,0) = s.total THEN 'complete' ELSE 'partial' END AS status
            FROM schedule s LEFT JOIN loaded l ON s.season = l.season
        )
        SELECT
            COUNT(*)                                    AS total_seasons,
            COUNT(*) FILTER (WHERE status = 'complete') AS complete_seasons,
            COUNT(*) FILTER (WHERE status = 'partial')  AS partial_seasons,
            MIN(season)                                 AS first_season,
            MAX(season)                                 AS last_season
        FROM season_status
    """, limit=1)

    latest_result = run_query("""
        SELECT MAX(r.race_date) AS latest_race_date, MAX(r.race_name) AS latest_race
        FROM silver.fact_race_results f
        JOIN silver.dim_races r ON f.race_key = r.dim_race_key
    """, limit=1)

    summary = summary_result["rows"][0] if summary_result["rows"] else {}
    if latest_result["rows"]:
        summary.update(latest_result["rows"][0])

    return {
        "status": "success",
        "tool": "get_coverage_chart_data",
        "by_season": seasons_result["rows"],
        "by_table": tables_result["rows"],
        "summary": summary,
        "chart_hint": (
            "Use by_season for a horizontal bar chart: x=pct_complete, "
            "y=season, colour green if >=90, amber if 50-89, red if <50. "
            "Use by_table for a summary card grid. "
            "Use Chart.js from https://cdn.jsdelivr.net/npm/chart.js"
        ),
    }


def get_persona(persona: str) -> dict:
    """
    Returns the system prompt for a named persona.
    persona: 'fan' | 'journalist' | 'content_creator' | 'warehouse_assistant'

    Use this to load working instructions for a specific role.
    """
    persona_file = _PERSONAS_DIR / f"{persona}.md"
    if not persona_file.exists():
        available = [p.stem for p in _PERSONAS_DIR.glob("*.md")]
        return {
            "status": "error",
            "error": f"Persona '{persona}' not found.",
            "available_personas": available,
        }
    return {
        "status": "success",
        "persona": persona,
        "system_prompt": persona_file.read_text(),
    }
