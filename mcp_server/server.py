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
        "Execute a read-only SQL SELECT query against the F1 silver layer in DuckDB. "
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
                    "type": "integer",
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
                    "type": "integer",
                    "description": "Optional: return race-by-race detail for this season.",
                },
            },
            "required": ["driver_name"],
        },
    ),

    "get_season_standings": (
        get_season_standings,
        "Championship standings for a season. Returns final standings by default. "
        "Pass after_round to see the standings at a specific point in the season. "
        "Useful for title battle analysis.",
        {
            "type": "object",
            "properties": {
                "season": {
                    "type": "integer",
                    "description": "The season year (e.g. 2023).",
                },
                "after_round": {
                    "type": "integer",
                    "description": "Optional: standings after this round number.",
                },
                "top_n": {
                    "type": "integer",
                    "description": "Number of drivers to return (default 10).",
                    "default": 10,
                },
            },
            "required": ["season"],
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
                "season_from": {"type": "integer", "description": "Optional start year."},
                "season_to":   {"type": "integer", "description": "Optional end year."},
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
                    "type": "integer",
                    "description": "Number of races to return (default 10).",
                    "default": 10,
                },
                "season_from": {"type": "integer", "description": "Optional start year."},
                "season_to":   {"type": "integer", "description": "Optional end year."},
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
                "season_from": {"type": "integer", "description": "Optional start year."},
                "season_to":   {"type": "integer", "description": "Optional end year."},
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

async def main():
    logger.info("F1 Data MCP Server starting...")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
