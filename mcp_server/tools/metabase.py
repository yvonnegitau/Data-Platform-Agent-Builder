"""
tools/metabase.py — publish analytics to Metabase from chat.

The official Metabase MCP creates questions via a multi-step MBQL 5 workflow
that small local models can't reliably orchestrate. These tools take the
simple path instead: a name + a raw SQL SELECT (which our semantic layer
already helps the model write well) + a chart type, and create a native-SQL
question through Metabase's REST API.

Everything lands in a shared collection (default "F1 Analytics") so it's
visible in the normal Metabase browse — never the service account's hidden
personal collection.

SQL runs in Metabase against Postgres directly, so it must reference the
`f1_silver` schema. As a convenience, a bare `silver.` prefix is rewritten to
`f1_silver.` so the model can use either.
"""

import re

import httpx

from config import (
    MB_BASE_URL,
    MB_PUBLIC_URL,
    MB_API_KEY,
    MB_DATABASE_NAME,
    MB_DEFAULT_COLLECTION,
)
from tools.knowledge import search_knowledge

_cache: dict = {}

# Compact schema crib returned on validation failure so the model can self-correct
# without a separate get_schema round-trip. These are the real column names.
SCHEMA_HINT = (
    "f1_silver schema — key columns:\n"
    "  fact_race_results: season(int), round, driver_key, constructor_key, circuit_key,\n"
    "    race_key, position, position_text ('R'=DNF), points, is_win(0/1), is_podium(0/1),\n"
    "    is_points_finish(0/1), grid_position, total_laps, grid_to_finish_diff\n"
    "  dim_driver: dim_driver_key, full_name        (join: fact.driver_key = dim_driver.dim_driver_key)\n"
    "  dim_constructor: dim_constructor_key, constructor_name\n"
    "  dim_circuit: dim_circuit_key, circuit_name\n"
    "  dim_races: dim_race_key, season, round, race_name, race_date\n"
    "Notes: there is no winner/driver_id column — count wins with SUM(is_win); "
    "filter a year with `season = 2023` (not YEAR(race_date)).\n"
    "METRIC FORMULAS (use these exact expressions on fact_race_results):\n"
    "  win_rate    = COUNT(*) FILTER (WHERE is_win = 1)::numeric / COUNT(*)\n"
    "  dnf_rate    = COUNT(*) FILTER (WHERE position_text = 'R')::numeric / COUNT(*)\n"
    "  podium_rate = COUNT(*) FILTER (WHERE is_podium = 1)::numeric / COUNT(*)\n"
    "  avg_finish  = AVG(position) FILTER (WHERE position_text != 'R')\n"
    "  points_per_race = SUM(points)::numeric / COUNT(*)\n"
    "(Postgres: cast ratios with ::numeric, not ::FLOAT, before ROUND.)\n"
    "The fact table is ALWAYS f1_silver.fact_race_results (alias f). Always JOIN the dim.\n"
    "EXAMPLE wins per driver:\n"
    "  SELECT d.full_name AS driver, SUM(f.is_win) AS wins\n"
    "  FROM f1_silver.fact_race_results f\n"
    "  JOIN f1_silver.dim_driver d ON f.driver_key = d.dim_driver_key\n"
    "  WHERE f.season = 2023 GROUP BY d.full_name ORDER BY wins DESC\n"
    "EXAMPLE points per constructor:\n"
    "  SELECT c.constructor_name AS team, SUM(f.points) AS points\n"
    "  FROM f1_silver.fact_race_results f\n"
    "  JOIN f1_silver.dim_constructor c ON f.constructor_key = c.dim_constructor_key\n"
    "  WHERE f.season = 2023 GROUP BY c.constructor_name ORDER BY points DESC\n"
    "EXAMPLE podiums per driver:\n"
    "  SELECT d.full_name AS driver, SUM(f.is_podium) AS podiums\n"
    "  FROM f1_silver.fact_race_results f\n"
    "  JOIN f1_silver.dim_driver d ON f.driver_key = d.dim_driver_key\n"
    "  WHERE f.season = 2023 GROUP BY d.full_name ORDER BY podiums DESC\n"
    "For DATA COMPLETENESS / COVERAGE charts, SELECT from the ready-made view\n"
    "  f1_silver.season_completeness (columns: season, rounds_loaded,\n"
    "  rounds_in_schedule, pct_complete, status). EXAMPLE:\n"
    "  SELECT season, pct_complete FROM f1_silver.season_completeness ORDER BY season\n"
    "For a metric OVER ROUNDS for the TOP N drivers (multi-line: pass series=driver),\n"
    "use championship_position_running for standing per round. EXAMPLE (top 5, 2025):\n"
    "  WITH top5 AS (SELECT driver_key FROM f1_silver.fact_race_results\n"
    "    WHERE season=2025 GROUP BY driver_key ORDER BY SUM(points) DESC LIMIT 5)\n"
    "  SELECT r.round, d.full_name AS driver, f.championship_position_running AS position\n"
    "  FROM f1_silver.fact_race_results f\n"
    "  JOIN f1_silver.dim_driver d ON f.driver_key = d.dim_driver_key\n"
    "  JOIN f1_silver.dim_races r ON f.race_key = r.dim_race_key\n"
    "  WHERE f.season=2025 AND f.driver_key IN (SELECT driver_key FROM top5)\n"
    "  ORDER BY r.round\n"
    "  -> display=line, x_axis=round, y_axis=position, series=driver"
)

# Matches a `silver.` schema prefix but not `f1_silver.` (the char before
# "silver" in "f1_silver" is "_", a word char, so \b won't match there).
_SCHEMA_FIX = re.compile(r"\bsilver\.")


def _client() -> httpx.Client:
    if not MB_API_KEY:
        raise RuntimeError("MB_API_KEY is not set — cannot reach Metabase.")
    return httpx.Client(
        base_url=MB_BASE_URL,
        headers={"x-api-key": MB_API_KEY, "Content-Type": "application/json"},
        timeout=30.0,
    )


def _database_id(client: httpx.Client) -> int:
    if "db" in _cache:
        return _cache["db"]
    body = client.get("/api/database").json()
    rows = body.get("data", body) if isinstance(body, dict) else body
    for db in rows:
        if db.get("name") == MB_DATABASE_NAME:
            _cache["db"] = db["id"]
            return db["id"]
    raise RuntimeError(f"Metabase database {MB_DATABASE_NAME!r} not found.")


def _collection_id(client: httpx.Client, name: str) -> int:
    key = f"coll:{name}"
    if key in _cache:
        return _cache[key]
    for c in client.get("/api/collection").json():
        if c.get("name") == name and not c.get("personal_owner_id"):
            _cache[key] = c["id"]
            return c["id"]
    created = client.post(
        "/api/collection",
        json={"name": name, "description": "Agent-built F1 analytics"},
    ).json()
    _cache[key] = created["id"]
    return created["id"]


def _validate_sql(client: httpx.Client, db_id: int, sql: str) -> str | None:
    """Dry-run the SQL via Metabase. Returns an error string, or None if it runs."""
    resp = client.post(
        "/api/dataset",
        json={"type": "native", "database": db_id, "native": {"query": sql}},
    )
    # Metabase returns 200 or 202 for dataset queries; both carry the result.
    if resp.status_code not in (200, 202):
        return f"{resp.status_code}: {resp.text[:300]}"
    body = resp.json()
    if body.get("status") == "failed" or body.get("error"):
        return body.get("error") or "query failed"
    return None


def create_metabase_question(
    name: str,
    sql: str,
    display: str = "table",
    description: str | None = None,
    collection: str | None = None,
    x_axis: str | None = None,
    y_axis: str | None = None,
    series: str | None = None,
) -> dict:
    """
    Create (save) a native-SQL question in Metabase and return its link.

    name: title shown in Metabase
    sql: a SELECT against the f1_silver schema (e.g. f1_silver.fact_race_results)
    display: table | bar | line | pie | row | scalar | area | combo
    collection: shared collection name (defaults to F1 Analytics)
    x_axis / y_axis: column names to map for bar/line/pie charts (optional)
    """
    sql = _SCHEMA_FIX.sub("f1_silver.", sql.strip())
    try:
        with _client() as client:
            db_id = _database_id(client)

            # Validate the SQL before saving so we never persist a broken card.
            sql_error = _validate_sql(client, db_id, sql)
            if sql_error:
                # Auto-retrieve a relevant worked pattern (the chart name is a
                # good proxy for the question) and hand it to the retry — so we
                # don't depend on the model calling search_knowledge first.
                retrieved = ""
                kb = search_knowledge(f"{name} {description or ''}", k=1)
                if kb.get("status") == "success" and kb.get("query_patterns"):
                    p = kb["query_patterns"][0]
                    retrieved = (
                        f"\n\nMOST RELEVANT TEMPLATE — adapt this exact pattern:\n"
                        f"-- {p['name']}: {p['description']}\n{p['sql']}"
                    )
                return {
                    "status": "error",
                    "error": f"SQL did not run: {sql_error}",
                    "hint": "Fix the SQL and call create_metabase_question again. "
                            "Use only these columns:\n" + SCHEMA_HINT + retrieved,
                }

            coll_id = _collection_id(client, collection or MB_DEFAULT_COLLECTION)

            viz: dict = {}
            if x_axis and y_axis:
                # `series` gives one line/bar per category value (multi-series).
                dims = [x_axis, series] if series else [x_axis]
                viz = {"graph.dimensions": dims, "graph.metrics": [y_axis]}

            payload = {
                "name": name,
                "collection_id": coll_id,
                "display": display,
                "visualization_settings": viz,
                "dataset_query": {
                    "type": "native",
                    "database": db_id,
                    "native": {"query": sql},
                },
            }
            if description:
                payload["description"] = description

            resp = client.post("/api/card", json=payload)
            resp.raise_for_status()
            card = resp.json()
            return {
                "status": "success",
                "question_id": card["id"],
                "name": card["name"],
                "collection": collection or MB_DEFAULT_COLLECTION,
                "url": f"{MB_PUBLIC_URL}/question/{card['id']}",
            }
    except httpx.HTTPStatusError as e:
        return {"status": "error", "error": f"{e.response.status_code}: {e.response.text[:300]}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def create_metabase_dashboard(
    name: str,
    question_ids: list[int],
    description: str | None = None,
    collection: str | None = None,
) -> dict:
    """
    Create a dashboard from existing saved questions and return its link.

    Cards are auto-arranged two per row on the 24-column grid.

    name: dashboard title
    question_ids: ids returned by create_metabase_question
    collection: shared collection name (defaults to F1 Analytics)
    """
    try:
        with _client() as client:
            coll_id = _collection_id(client, collection or MB_DEFAULT_COLLECTION)
            body = {"name": name, "collection_id": coll_id}
            if description:
                body["description"] = description
            dash = client.post("/api/dashboard", json=body).json()
            dash_id = dash["id"]

            dashcards = []
            for i, qid in enumerate(question_ids):
                dashcards.append({
                    "id": -(i + 1),          # negative id = new card
                    "card_id": qid,
                    "row": (i // 2) * 8,
                    "col": (i % 2) * 12,
                    "size_x": 12,
                    "size_y": 8,
                })
            resp = client.put(f"/api/dashboard/{dash_id}", json={"dashcards": dashcards})
            resp.raise_for_status()
            return {
                "status": "success",
                "dashboard_id": dash_id,
                "name": name,
                "cards_added": len(question_ids),
                "collection": collection or MB_DEFAULT_COLLECTION,
                "url": f"{MB_PUBLIC_URL}/dashboard/{dash_id}",
            }
    except httpx.HTTPStatusError as e:
        return {"status": "error", "error": f"{e.response.status_code}: {e.response.text[:300]}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}
