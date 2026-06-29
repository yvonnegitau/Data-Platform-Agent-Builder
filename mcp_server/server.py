"""
server.py — F1 Data MCP Server entry point.

This is the script Claude Desktop connects to. It registers all tools
and listens for Claude to call them over stdin/stdout.

To add a new tool: define a function in tools/, import it here,
and add it to the @server.list_tools() and @server.call_tool() handlers.

To connect to Claude Desktop, add this to claude_desktop_config.json:
{
  "mcpServers": {
    "f1-data": {
      "command": "/path/to/mcp_server/.venv/bin/python3",
      "args": ["/path/to/mcp_server/server.py"]
    }
  }
}
"""

import asyncio
import json
import logging
import sys
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

# ── Tool imports ──────────────────────────────────────────────────────────────
from tools.schema import (
    get_schema,
    get_metric_definition,
    get_query_pattern,
    get_glossary,
)
from tools.raw_query import execute_sql
from tools.data_products import (
    get_driver_career,
    get_season_standings,
    compare_drivers,
    get_greatest_races,
    get_constructor_history,
    get_circuit_stats,
)
from tools.metadata import (
    get_data_freshness,
    get_data_coverage,
    get_season_completeness,
    get_coverage_chart_data,
    get_persona,
)
from tools.metabase import (
    create_metabase_question,
    create_metabase_dashboard,
)
from tools.knowledge import search_knowledge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("f1-mcp")

# ── Server definition ─────────────────────────────────────────────────────────
server = Server("f1-data")

# Registry: name → (function, description, input_schema)
# The input_schema is JSON Schema describing what parameters the tool accepts.
# Claude reads these descriptions to decide when and how to call each tool.
TOOLS: dict[str, tuple] = {

    # ── Semantic layer ─────────────────────────────────────────────────────
    "get_schema": (
        get_schema,
        "Get schema information for the F1 silver layer tables. "
        "Call with no argument to list all tables. Call with a table name "
        "(e.g. 'fact_race_results') for full column definitions and join keys. "
        "Always call this before writing SQL.",
        {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Table name to inspect. Omit to list all tables.",
                }
            },
        },
    ),

    "get_metric_definition": (
        get_metric_definition,
        "Get the SQL formula and business definition for a named metric "
        "(win_rate, dnf_rate, podium_rate, avg_finishing_position, points_per_race). "
        "Call with no argument to list all available metrics.",
        {
            "type": "object",
            "properties": {
                "metric_name": {
                    "type": "string",
                    "description": "Metric name. Omit to list all metrics.",
                }
            },
        },
    ),

    "get_query_pattern": (
        get_query_pattern,
        "Get a worked SQL example to use as a template for ad-hoc analysis. "
        "Patterns include window functions (rolling averages, LAG, RANK, streaks). "
        "Call with no argument to list all patterns. Call with a pattern name "
        "to get the full SQL template with {parameter} placeholders.",
        {
            "type": "object",
            "properties": {
                "pattern_name": {
                    "type": "string",
                    "description": "Pattern name. Omit to list all patterns.",
                }
            },
        },
    ),

    "get_glossary": (
        get_glossary,
        "Look up F1 domain terminology — DNF, DRS, Pole Position, Points System "
        "changes, eras, and more. Call with no argument to list all terms.",
        {
            "type": "object",
            "properties": {
                "term": {
                    "type": "string",
                    "description": "Term to look up. Omit to list all available terms.",
                }
            },
        },
    ),

    # ── Raw SQL ────────────────────────────────────────────────────────────
    "execute_sql": (
        execute_sql,
        "Execute a read-only SQL SELECT query against the F1 silver layer in Postgres. "
        "Use this for any analysis not covered by the pre-built tools. "
        "Before writing SQL, call get_schema() to confirm column names and join keys. "
        "Call get_query_pattern() for SQL templates including window functions. "
        "Tables: silver.fact_race_results, silver.dim_driver, silver.dim_constructor, "
        "silver.dim_circuit, silver.dim_races, silver.dim_status. "
        "Results capped at 500 rows.",
        {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL SELECT query to execute.",
                },
                "limit": {
                    "type": ["integer", "null"],
                    "description": "Max rows to return (default 100, max 500).",
                    "default": 100,
                },
            },
            "required": ["sql"],
        },
    ),

    # ── Data products ──────────────────────────────────────────────────────
    "get_driver_career": (
        get_driver_career,
        "Full career summary for a driver — season-by-season wins, podiums, points, "
        "teams, and championship positions. Pass a season to get individual race "
        "detail for that year. Driver name is fuzzy-matched.",
        {
            "type": "object",
            "properties": {
                "driver_name": {
                    "type": "string",
                    "description": "Driver name (fuzzy-matched, e.g. 'Hamilton', 'Max').",
                },
                "season": {
                    "type": ["integer", "null"],
                    "description": "Optional: return race-by-race detail for this season.",
                },
            },
            "required": ["driver_name"],
        },
    ),

    "get_season_standings": (
        get_season_standings,
        "Championship standings for a season — includes wins, podiums, and points per driver. "
        "Use this for ANY question about a season's results: who won the most races, "
        "who scored the most points, who was champion, title battle analysis, "
        "or standings at a specific round. Returns final standings by default; "
        "pass after_round to see standings mid-season.",
        {
            "type": "object",
            "properties": {
                "season": {
                    "type": ["integer", "null"],
                    "description": "The season year as a 4-digit integer extracted from the user's question (e.g. 2024). Required to run this tool.",
                },
                "after_round": {
                    "type": ["integer", "null"],
                    "description": "Optional: standings after this round number. Omit for final standings.",
                },
                "top_n": {
                    "type": ["integer", "null"],
                    "description": "Number of drivers to return (default 10).",
                    "default": 10,
                },
            },
            "required": [],
        },
    ),

    "compare_drivers": (
        compare_drivers,
        "Head-to-head comparison between two drivers at every race where both competed. "
        "Returns who finished ahead in each race plus an overall win count summary. "
        "Optionally filter to a year range. Driver names are fuzzy-matched.",
        {
            "type": "object",
            "properties": {
                "driver_a": {"type": "string", "description": "First driver name."},
                "driver_b": {"type": "string", "description": "Second driver name."},
                "season_from": {"type": ["integer", "null"], "description": "Optional start year."},
                "season_to":   {"type": ["integer", "null"], "description": "Optional end year."},
            },
            "required": ["driver_a", "driver_b"],
        },
    ),

    "get_greatest_races": (
        get_greatest_races,
        "Find historically significant races by criteria: "
        "'biggest_comeback' (winner gained most places from grid), "
        "'most_places_gained' (any driver), "
        "'dominant_wins' (won from pole), "
        "'dnf_carnage' (most retirements). "
        "Optionally filter by year range.",
        {
            "type": "object",
            "properties": {
                "criteria": {
                    "type": "string",
                    "enum": ["biggest_comeback", "most_places_gained", "dominant_wins", "dnf_carnage"],
                    "description": "The type of race to find.",
                    "default": "biggest_comeback",
                },
                "limit": {
                    "type": ["integer", "null"],
                    "description": "Number of races to return (default 10).",
                    "default": 10,
                },
                "season_from": {"type": ["integer", "null"], "description": "Optional start year."},
                "season_to":   {"type": ["integer", "null"], "description": "Optional end year."},
            },
        },
    ),

    "get_constructor_history": (
        get_constructor_history,
        "Season-by-season performance history for a constructor (team) — "
        "wins, podiums, points, driver lineups, and championship positions. "
        "Constructor name is fuzzy-matched.",
        {
            "type": "object",
            "properties": {
                "constructor_name": {
                    "type": "string",
                    "description": "Team name (fuzzy-matched, e.g. 'Red Bull', 'Ferrari').",
                },
                "season_from": {"type": ["integer", "null"], "description": "Optional start year."},
                "season_to":   {"type": ["integer", "null"], "description": "Optional end year."},
            },
            "required": ["constructor_name"],
        },
    ),

    "get_circuit_stats": (
        get_circuit_stats,
        "Historical race winners and statistics for a circuit. "
        "Returns all winners, the teams they drove for, grid positions, "
        "and flags the biggest upset. Circuit name is fuzzy-matched.",
        {
            "type": "object",
            "properties": {
                "circuit_name": {
                    "type": "string",
                    "description": "Circuit name (fuzzy-matched, e.g. 'Monaco', 'Monza').",
                },
            },
            "required": ["circuit_name"],
        },
    ),

    # ── Warehouse / coverage tools ─────────────────────────────────────────
    "get_data_freshness": (
        get_data_freshness,
        "Returns a snapshot of data freshness: latest season loaded, latest race, "
        "total races in the database, and the earliest season available. "
        "Call this when asked 'how current is the data?' or 'when was it last updated?'",
        {"type": "object", "properties": {}},
    ),

    "get_data_coverage": (
        get_data_coverage,
        "Returns row counts and season ranges for every table in the silver layer. "
        "Shows which tables exist, how many rows they have, and what date range they cover. "
        "Use this for a full warehouse health overview.",
        {"type": "object", "properties": {}},
    ),

    "get_season_completeness": (
        get_season_completeness,
        "Shows how complete each season's data is — rounds loaded versus the total "
        "rounds in the race schedule. Returns pct_complete and a status flag "
        "(Complete / Partial / Minimal / Missing). "
        "Pass a season year to check one season. Leave empty for all seasons. "
        "Use this to answer: 'Do we have all races for 2023?' or 'Which seasons have gaps?'",
        {
            "type": "object",
            "properties": {
                "season": {
                    "type": ["integer", "null"],
                    "description": "Season year to check. Omit for all seasons.",
                }
            },
        },
    ),

    "get_coverage_chart_data": (
        get_coverage_chart_data,
        "Returns data structured for chart generation: by_season (rounds loaded vs. schedule "
        "per year), by_table (row counts per silver table), and a summary of headline numbers. "
        "Call this when asked to 'show a dashboard', 'chart the coverage', or 'visualise the data'. "
        "After calling, generate an HTML Chart.js artifact from the returned data.",
        {"type": "object", "properties": {}},
    ),

    "get_persona": (
        get_persona,
        "Returns the system prompt for a named persona. "
        "Available: 'fan', 'journalist', 'content_creator', 'warehouse_assistant'.",
        {
            "type": "object",
            "properties": {
                "persona": {
                    "type": "string",
                    "description": "Persona name.",
                    "enum": ["fan", "journalist", "content_creator", "warehouse_assistant"],
                }
            },
            "required": ["persona"],
        },
    ),

    # ── Semantic-layer retrieval ─────────────────────────────────────────────
    "search_knowledge": (
        search_knowledge,
        "Retrieve the most relevant query patterns (worked SQL templates), metric "
        "formulas, and glossary terms for a question. ALWAYS call this FIRST when a "
        "request needs custom SQL or a chart, then reuse what it returns instead of "
        "guessing column names or formulas. Pass the user's question as the query.",
        {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The user's question or analysis goal."},
                "k": {"type": ["integer", "null"], "description": "How many of each kind to return (default 3)."},
            },
            "required": ["query"],
        },
    ),

    # ── Publish to Metabase ──────────────────────────────────────────────────
    "create_metabase_question": (
        create_metabase_question,
        "Save a chart/question to Metabase BI so it persists and is visible to "
        "everyone. Use this when the user asks to 'create a chart', 'save this', "
        "'build a dashboard', or 'put this in Metabase'. "
        "Pass a SQL SELECT against the f1_silver schema. The SQL is validated "
        "before saving; if it errors you get the message + schema back to retry. "
        "Schema — fact_race_results(season, round, driver_key, constructor_key, "
        "circuit_key, position, position_text('R'=DNF), points, is_win, is_podium, "
        "grid_position, total_laps); dim_driver(dim_driver_key, full_name); "
        "dim_constructor(dim_constructor_key, constructor_name); "
        "dim_circuit(dim_circuit_key, circuit_name); "
        "dim_races(dim_race_key, season, round, race_name, race_date). "
        "Join fact.driver_key = dim_driver.dim_driver_key. Count wins with SUM(is_win); "
        "filter a year with season=2023 (NOT YEAR(race_date)). There is no winner_id column. "
        "Metric formulas: win_rate/dnf_rate/podium_rate = COUNT(*) FILTER (WHERE is_win=1 / position_text='R' / is_podium=1)::numeric / COUNT(*); "
        "avg_finish = AVG(position) FILTER (WHERE position_text != 'R'). "
        "ALWAYS join the dimension tables you reference. Working example to adapt: "
        "SELECT d.full_name AS driver, SUM(f.is_win) AS wins "
        "FROM f1_silver.fact_race_results f "
        "JOIN f1_silver.dim_driver d ON f.driver_key = d.dim_driver_key "
        "WHERE f.season = 2023 GROUP BY d.full_name ORDER BY wins DESC. "
        "For a chart set display (bar/line/pie/row/area/scalar) and x_axis/y_axis to "
        "column aliases from your SELECT. Saved to the shared 'F1 Analytics' collection; "
        "returns a clickable Metabase URL.",
        {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Title for the question/chart."},
                "sql": {
                    "type": "string",
                    "description": "SQL SELECT against the f1_silver schema. Runs in Postgres via Metabase.",
                },
                "display": {
                    "type": "string",
                    "enum": ["table", "bar", "line", "pie", "row", "area", "combo", "scalar"],
                    "description": "Visualization type. Use 'table' if unsure.",
                    "default": "table",
                },
                "x_axis": {"type": ["string", "null"], "description": "Column for the chart's x-axis / category (bar/line/pie)."},
                "y_axis": {"type": ["string", "null"], "description": "Column for the chart's y-axis / value (bar/line/pie)."},
                "series": {"type": ["string", "null"], "description": "Column to split into multiple lines/bars (e.g. 'driver' for one line per driver over rounds)."},
                "description": {"type": ["string", "null"], "description": "Optional description."},
                "collection": {"type": ["string", "null"], "description": "Collection name. Defaults to 'F1 Analytics'."},
            },
            "required": ["name", "sql"],
        },
    ),

    "create_metabase_dashboard": (
        create_metabase_dashboard,
        "Create a Metabase dashboard from questions you already saved with "
        "create_metabase_question. Pass the question_ids returned by those calls. "
        "Cards are auto-arranged. Returns a clickable dashboard URL. "
        "Saved to the shared 'F1 Analytics' collection automatically.",
        {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Dashboard title."},
                "question_ids": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "IDs of saved questions to place on the dashboard.",
                },
                "description": {"type": ["string", "null"], "description": "Optional description."},
                "collection": {"type": ["string", "null"], "description": "Collection name. Defaults to 'F1 Analytics'."},
            },
            "required": ["name", "question_ids"],
        },
    ),
}


# ── MCP protocol handlers ─────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    """Tell Claude which tools are available and what they do."""
    return [
        types.Tool(
            name=name,
            description=desc,
            inputSchema=schema,
        )
        for name, (_, desc, schema) in TOOLS.items()
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    """
    Claude calls this when it wants to use a tool.
    We look up the function, call it with the provided arguments,
    and return the result as a JSON string.
    """
    if name not in TOOLS:
        raise ValueError(f"Unknown tool: {name}")

    func, _, _ = TOOLS[name]

    try:
        logger.info(f"Tool called: {name} | args: {arguments}")
        result = func(**arguments)
        output = json.dumps(result, indent=2, default=str)
        logger.info(f"Tool {name} returned {result.get('row_count', '?')} rows")
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}")
        output = json.dumps({"status": "error", "error": str(e)})

    return [types.TextContent(type="text", text=output)]


# ── Entry point ───────────────────────────────────────────────────────────────

async def run_stdio():
    """stdio transport — used by Claude Desktop."""
    logger.info("F1 Data MCP Server starting (stdio)...")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def run_http(host: str = "0.0.0.0", port: int = 8000):
    """HTTP transport — used by Open WebUI (streamable-http)."""
    from contextlib import asynccontextmanager
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Mount
    import uvicorn

    logger.info(f"F1 Data MCP Server starting (http) on {host}:{port}...")

    session_manager = StreamableHTTPSessionManager(
        app=server,
        event_store=None,
        json_response=True,
        stateless=True,
    )

    async def handle_mcp(scope, receive, send):
        await session_manager.handle_request(scope, receive, send)

    @asynccontextmanager
    async def lifespan(app):
        async with session_manager.run():
            yield

    starlette_app = Starlette(
        lifespan=lifespan,
        routes=[Mount("/", app=handle_mcp)],
    )

    uvicorn.run(starlette_app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"
    if transport == "http":
        run_http()
    else:
        asyncio.run(run_stdio())
