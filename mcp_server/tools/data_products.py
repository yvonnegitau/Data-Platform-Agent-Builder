"""
tools/data_products.py — Pre-built data product tools.

These are convenience tools for the most common F1 analysis patterns.
Each one wraps a parameterised SQL query so Claude doesn't need to write
SQL from scratch for frequent requests.

Fuzzy name matching (ILIKE '%name%') is used throughout so callers can
pass "Hamilton" instead of "Lewis Hamilton".
"""

from database import run_query


def get_driver_career(driver_name: str, season: int | None = None) -> dict:
    """
    Full career summary for a driver — season-by-season wins, podiums,
    points, teams, and championship finishing position.

    Use this when someone asks about a driver's history, achievements,
    or how they performed across their career.

    driver_name: fuzzy-matched (e.g. 'Hamilton', 'Max', 'Senna')
    season: if provided, returns only that season's races in detail
    """
    if season:
        sql = """
            SELECT
                r.round,
                r.race_name,
                ci.circuit_name,
                c.constructor_name  AS team,
                f.grid_position,
                f.position,
                f.points,
                f.is_win,
                f.is_podium,
                f.season_points_running,
                f.season_wins_running,
                f.championship_position_running,
                f.points_behind_leader_running,
                f.grid_to_finish_diff
            FROM silver.fact_race_results f
            JOIN silver.dim_driver      d  ON f.driver_key      = d.dim_driver_key
            JOIN silver.dim_races       r  ON f.race_key        = r.dim_race_key
            JOIN silver.dim_circuit     ci ON f.circuit_key     = ci.dim_circuit_key
            JOIN silver.dim_constructor c  ON f.constructor_key = c.dim_constructor_key
            WHERE d.full_name ILIKE ?
              AND f.season = ?
            ORDER BY r.round
        """
        result = run_query(sql, params=[f"%{driver_name}%", season], limit=30)
        result["query_type"] = "single_season"
    else:
        sql = """
            SELECT
                f.season,
                c.constructor_name                          AS team,
                COUNT(*)                                    AS races,
                SUM(f.is_win)                               AS wins,
                SUM(f.is_podium)                            AS podiums,
                SUM(f.is_points_finish)                     AS points_finishes,
                SUM(f.points)                               AS total_points,
                COUNT(*) FILTER (WHERE f.position IS NULL)  AS dnfs,
                MIN(f.championship_position_running)        AS best_championship_pos,
                MAX(f.career_wins_running)                  AS career_wins_by_season_end,
                MAX(f.career_podiums_running)               AS career_podiums_by_season_end
            FROM silver.fact_race_results f
            JOIN silver.dim_driver      d  ON f.driver_key      = d.dim_driver_key
            JOIN silver.dim_constructor c  ON f.constructor_key = c.dim_constructor_key
            WHERE d.full_name ILIKE ?
            GROUP BY f.season, c.constructor_name
            ORDER BY f.season
        """
        result = run_query(sql, params=[f"%{driver_name}%"], limit=100)
        result["query_type"] = "career_overview"

    result["driver_searched"] = driver_name
    return result


def get_season_standings(season: int, after_round: int | None = None, top_n: int = 10) -> dict:
    """
    Championship standings for a season — either final standings or
    standings at a specific point in the season.

    Use this to answer questions like:
    - "Who won the 2023 championship?"
    - "What were the standings after round 5 of 2021?"
    - "How close was the 2021 title battle?"

    season: the year (e.g. 2023)
    after_round: if provided, standings as of that round
    top_n: number of drivers to return (default 10)
    """
    round_filter = "AND r.round = ?" if after_round else ""
    # Without after_round, get each driver's last race of the season
    if after_round:
        sql = f"""
            SELECT
                f.championship_position_running  AS position,
                d.full_name                      AS driver,
                c.constructor_name               AS team,
                f.season_points_running          AS points,
                f.season_wins_running            AS wins,
                f.season_podiums_running         AS podiums,
                f.points_behind_leader_running   AS points_behind_leader,
                r.round                          AS after_round,
                r.race_name                      AS after_race
            FROM silver.fact_race_results f
            JOIN silver.dim_driver      d  ON f.driver_key      = d.dim_driver_key
            JOIN silver.dim_constructor c  ON f.constructor_key = c.dim_constructor_key
            JOIN silver.dim_races       r  ON f.race_key        = r.dim_race_key
            WHERE f.season = ?
              {round_filter}
            ORDER BY f.championship_position_running
            LIMIT ?
        """
        params = [season, after_round, top_n]
    else:
        sql = """
            WITH last_round AS (
                SELECT MAX(r.round) AS max_round
                FROM silver.fact_race_results f
                JOIN silver.dim_races r ON f.race_key = r.dim_race_key
                WHERE f.season = ?
            )
            SELECT
                f.championship_position_running  AS position,
                d.full_name                      AS driver,
                c.constructor_name               AS team,
                f.season_points_running          AS points,
                f.season_wins_running            AS wins,
                f.season_podiums_running         AS podiums,
                f.points_behind_leader_running   AS points_behind_leader
            FROM silver.fact_race_results f
            JOIN silver.dim_driver      d  ON f.driver_key      = d.dim_driver_key
            JOIN silver.dim_constructor c  ON f.constructor_key = c.dim_constructor_key
            JOIN silver.dim_races       r  ON f.race_key        = r.dim_race_key
            CROSS JOIN last_round lr
            WHERE f.season = ?
              AND r.round = lr.max_round
            ORDER BY f.championship_position_running
            LIMIT ?
        """
        params = [season, season, top_n]

    result = run_query(sql, params=params, limit=top_n)
    result["season"] = season
    result["after_round"] = after_round
    return result


def compare_drivers(
    driver_a: str,
    driver_b: str,
    season_from: int | None = None,
    season_to: int | None = None,
) -> dict:
    """
    Head-to-head comparison between two drivers at every race where
    both competed. Shows who finished ahead, points won, and an
    overall head-to-head record.

    Use this for rivalry analysis, teammate comparisons, or
    "who was better" questions.

    driver_a, driver_b: fuzzy-matched names
    season_from / season_to: optional year range filter
    """
    season_clause = ""
    params: list = [f"%{driver_a}%", f"%{driver_b}%"]

    if season_from and season_to:
        season_clause = "AND f.season BETWEEN ? AND ?"
        params += [season_from, season_to]
    elif season_from:
        season_clause = "AND f.season >= ?"
        params.append(season_from)
    elif season_to:
        season_clause = "AND f.season <= ?"
        params.append(season_to)

    sql = f"""
        WITH both_drivers AS (
            SELECT
                f.season, r.round, r.race_name,
                d.full_name,
                f.position,
                f.grid_position,
                f.points,
                f.is_win,
                f.is_podium
            FROM silver.fact_race_results f
            JOIN silver.dim_driver d ON f.driver_key = d.dim_driver_key
            JOIN silver.dim_races r  ON f.race_key   = r.dim_race_key
            WHERE (d.full_name ILIKE ? OR d.full_name ILIKE ?)
              {season_clause}
        )
        SELECT
            a.season,
            a.round,
            a.race_name,
            a.full_name       AS driver_a,
            a.position        AS driver_a_pos,
            a.points          AS driver_a_pts,
            b.full_name       AS driver_b,
            b.position        AS driver_b_pos,
            b.points          AS driver_b_pts,
            CASE
                WHEN a.position IS NULL AND b.position IS NULL THEN 'both_dnf'
                WHEN a.position IS NULL                        THEN b.full_name
                WHEN b.position IS NULL                        THEN a.full_name
                WHEN a.position < b.position                   THEN a.full_name
                ELSE b.full_name
            END AS finished_ahead
        FROM both_drivers a
        JOIN both_drivers b
          ON a.season = b.season AND a.round = b.round
         AND a.full_name ILIKE ?
         AND b.full_name ILIKE ?
        ORDER BY a.season, a.round
    """
    params += [f"%{driver_a}%", f"%{driver_b}%"]

    result = run_query(sql, params=params, limit=500)

    # Compute summary counts from the rows
    if result["status"] == "success" and result["rows"]:
        rows = result["rows"]
        total = len(rows)
        a_name = rows[0]["driver_a"]
        b_name = rows[0]["driver_b"]
        a_wins = sum(1 for r in rows if r["finished_ahead"] == a_name)
        b_wins = sum(1 for r in rows if r["finished_ahead"] == b_name)
        both_dnf = sum(1 for r in rows if r["finished_ahead"] == "both_dnf")
        result["summary"] = {
            "shared_races": total,
            driver_a: {"finished_ahead": a_wins, "name": a_name},
            driver_b: {"finished_ahead": b_wins, "name": b_name},
            "both_dnf": both_dnf,
        }

    return result


def get_greatest_races(
    criteria: str = "biggest_comeback",
    limit: int = 10,
    season_from: int | None = None,
    season_to: int | None = None,
) -> dict:
    """
    Find historically significant races by a given criteria.

    criteria options:
      - 'biggest_comeback'  : winner who gained the most places from grid to finish
      - 'most_places_gained': any driver (not just winner) who gained most places
      - 'dominant_wins'     : wins from pole with largest winning margin (most laps led proxy)
      - 'dnf_carnage'       : races with the highest number of DNFs

    season_from / season_to: optional year range filter
    """
    season_clause = ""
    params: list = []

    if season_from and season_to:
        season_clause = "AND f.season BETWEEN ? AND ?"
        params += [season_from, season_to]
    elif season_from:
        season_clause = "AND f.season >= ?"
        params.append(season_from)
    elif season_to:
        season_clause = "AND f.season <= ?"
        params.append(season_to)

    if criteria == "biggest_comeback":
        sql = f"""
            SELECT
                f.season,
                r.race_name,
                d.full_name           AS driver,
                c.constructor_name    AS team,
                f.grid_position,
                f.position,
                f.grid_to_finish_diff AS places_gained,
                f.points
            FROM silver.fact_race_results f
            JOIN silver.dim_driver      d  ON f.driver_key      = d.dim_driver_key
            JOIN silver.dim_constructor c  ON f.constructor_key = c.dim_constructor_key
            JOIN silver.dim_races       r  ON f.race_key        = r.dim_race_key
            WHERE f.is_win = 1
              AND f.grid_to_finish_diff IS NOT NULL
              AND f.grid_to_finish_diff > 0
              {season_clause}
            ORDER BY f.grid_to_finish_diff DESC
            LIMIT ?
        """

    elif criteria == "most_places_gained":
        sql = f"""
            SELECT
                f.season,
                r.race_name,
                d.full_name           AS driver,
                f.grid_position,
                f.position,
                f.grid_to_finish_diff AS places_gained
            FROM silver.fact_race_results f
            JOIN silver.dim_driver d ON f.driver_key = d.dim_driver_key
            JOIN silver.dim_races r  ON f.race_key   = r.dim_race_key
            WHERE f.grid_to_finish_diff IS NOT NULL
              AND f.grid_to_finish_diff > 0
              {season_clause}
            ORDER BY f.grid_to_finish_diff DESC
            LIMIT ?
        """

    elif criteria == "dominant_wins":
        sql = f"""
            SELECT
                f.season,
                r.race_name,
                d.full_name        AS driver,
                c.constructor_name AS team,
                f.grid_position,
                f.laps
            FROM silver.fact_race_results f
            JOIN silver.dim_driver      d  ON f.driver_key      = d.dim_driver_key
            JOIN silver.dim_constructor c  ON f.constructor_key = c.dim_constructor_key
            JOIN silver.dim_races       r  ON f.race_key        = r.dim_race_key
            WHERE f.is_win = 1
              AND f.grid_position = 1
              {season_clause}
            ORDER BY f.season DESC
            LIMIT ?
        """

    elif criteria == "dnf_carnage":
        sql = f"""
            SELECT
                f.season,
                r.race_name,
                COUNT(*) FILTER (WHERE f.position IS NULL) AS dnf_count,
                COUNT(*)                                   AS starters,
                ROUND(
                    COUNT(*) FILTER (WHERE f.position IS NULL)::FLOAT / COUNT(*) * 100,
                    1
                )                                          AS dnf_pct
            FROM silver.fact_race_results f
            JOIN silver.dim_races r ON f.race_key = r.dim_race_key
            WHERE 1=1 {season_clause}
            GROUP BY f.season, r.race_name
            ORDER BY dnf_count DESC
            LIMIT ?
        """

    else:
        return {
            "status": "error",
            "error": f"Unknown criteria '{criteria}'.",
            "valid_criteria": ["biggest_comeback", "most_places_gained", "dominant_wins", "dnf_carnage"],
        }

    params.append(limit)
    result = run_query(sql, params=params, limit=limit)
    result["criteria"] = criteria
    return result


def get_constructor_history(
    constructor_name: str,
    season_from: int | None = None,
    season_to: int | None = None,
) -> dict:
    """
    Season-by-season performance history for a constructor (team).

    Returns wins, podiums, points, drivers used, and best championship
    position per season.

    Use this for questions about team dominance, decline, or history.

    constructor_name: fuzzy-matched (e.g. 'Red Bull', 'Ferrari', 'McLaren')
    """
    season_clause = ""
    params: list = [f"%{constructor_name}%"]

    if season_from and season_to:
        season_clause = "AND f.season BETWEEN ? AND ?"
        params += [season_from, season_to]
    elif season_from:
        season_clause = "AND f.season >= ?"
        params.append(season_from)
    elif season_to:
        season_clause = "AND f.season <= ?"
        params.append(season_to)

    sql = f"""
        SELECT
            f.season,
            COUNT(DISTINCT d.full_name)                 AS drivers_used,
            COUNT(*)                                    AS race_entries,
            SUM(f.is_win)                               AS wins,
            SUM(f.is_podium)                            AS podiums,
            SUM(f.is_points_finish)                     AS points_finishes,
            SUM(f.points)                               AS total_points,
            COUNT(*) FILTER (WHERE f.position IS NULL)  AS dnfs,
            MIN(f.championship_position_running)        AS best_driver_champ_pos,
            STRING_AGG(DISTINCT d.full_name, ', ')      AS driver_lineup
        FROM silver.fact_race_results f
        JOIN silver.dim_constructor c ON f.constructor_key = c.dim_constructor_key
        JOIN silver.dim_driver      d ON f.driver_key      = d.dim_driver_key
        WHERE c.constructor_name ILIKE ?
          {season_clause}
        GROUP BY f.season
        ORDER BY f.season
    """

    result = run_query(sql, params=params, limit=100)
    result["constructor_searched"] = constructor_name
    return result


def get_circuit_stats(circuit_name: str) -> dict:
    """
    Historical statistics for a circuit — all winners, most successful
    drivers and teams, biggest upsets (winner started furthest back).

    Use this for circuit guides, race previews, or venue history questions.

    circuit_name: fuzzy-matched (e.g. 'Monaco', 'Monza', 'Silverstone')
    """
    sql = """
        SELECT
            f.season,
            r.race_name,
            d.full_name        AS winner,
            c.constructor_name AS team,
            f.grid_position    AS started_from,
            f.grid_to_finish_diff AS places_gained,
            f.laps
        FROM silver.fact_race_results f
        JOIN silver.dim_circuit     ci ON f.circuit_key     = ci.dim_circuit_key
        JOIN silver.dim_driver      d  ON f.driver_key      = d.dim_driver_key
        JOIN silver.dim_constructor c  ON f.constructor_key = c.dim_constructor_key
        JOIN silver.dim_races       r  ON f.race_key        = r.dim_race_key
        WHERE ci.circuit_name ILIKE ?
          AND f.is_win = 1
        ORDER BY f.season DESC
    """
    result = run_query(sql, params=[f"%{circuit_name}%"], limit=100)
    result["circuit_searched"] = circuit_name

    # Surface the biggest upset in metadata
    if result["status"] == "success" and result["rows"]:
        biggest_upset = max(
            (r for r in result["rows"] if r.get("places_gained") is not None),
            key=lambda r: r["places_gained"],
            default=None,
        )
        if biggest_upset:
            result["biggest_upset"] = biggest_upset

    return result
