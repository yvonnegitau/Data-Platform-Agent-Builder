"""
f1_agent.py — a minimal, fully-local F1 agent.

Bypasses any chat frontend: talks directly to Ollama's OpenAI-compatible API
with the F1 tools bound natively, executes tool calls against the MCP tool
functions, and loops until the model produces a final answer.

This exists because LibreChat's agent harness was unreliable at binding tools
to a small local model — but the model itself tool-calls perfectly through the
/v1 API, and the tools work. This proves the end-to-end loop with no UI in the way.

Usage:
    python scripts/f1_agent.py "Who won the most races in 2023?"
    python scripts/f1_agent.py "Save a bar chart of wins per driver in 2023 to Metabase"

Env (defaults for local docker-compose):
    OLLAMA_URL (http://localhost:11434), F1_MODEL (f1-qwen),
    MB_BASE_URL (http://localhost:3002), MB_API_KEY (required for publishing)
"""

import json
import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mcp_server"))

from tools.data_products import (  # noqa: E402
    get_season_standings,
    get_driver_career,
    compare_drivers,
    get_circuit_stats,
)
from tools.raw_query import execute_sql  # noqa: E402
from tools.metabase import create_metabase_question  # noqa: E402
from tools.knowledge import search_knowledge  # noqa: E402

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL = os.getenv("F1_MODEL", "f1-qwen")

# Map tool name -> (python callable, OpenAI tool schema)
TOOLS = {
    "get_season_standings": (
        get_season_standings,
        {
            "type": "function",
            "function": {
                "name": "get_season_standings",
                "description": "Championship standings for a season: wins, points, podiums per driver. Use for 'who won the most races / the title in YEAR'.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "season": {"type": "integer", "description": "4-digit year, e.g. 2023"},
                        "top_n": {"type": "integer", "description": "how many drivers (default 10)"},
                    },
                    "required": ["season"],
                },
            },
        },
    ),
    "get_driver_career": (
        get_driver_career,
        {
            "type": "function",
            "function": {
                "name": "get_driver_career",
                "description": "A driver's season-by-season career: wins, podiums, points, teams.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "driver_name": {"type": "string"},
                        "season": {"type": "integer"},
                    },
                    "required": ["driver_name"],
                },
            },
        },
    ),
    "compare_drivers": (
        compare_drivers,
        {
            "type": "function",
            "function": {
                "name": "compare_drivers",
                "description": "Head-to-head between two drivers across races where both competed.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "driver_a": {"type": "string"},
                        "driver_b": {"type": "string"},
                    },
                    "required": ["driver_a", "driver_b"],
                },
            },
        },
    ),
    "get_circuit_stats": (
        get_circuit_stats,
        {
            "type": "function",
            "function": {
                "name": "get_circuit_stats",
                "description": "Historical winners and stats for a circuit.",
                "parameters": {
                    "type": "object",
                    "properties": {"circuit_name": {"type": "string"}},
                    "required": ["circuit_name"],
                },
            },
        },
    ),
    "create_metabase_question": (
        create_metabase_question,
        {
            "type": "function",
            "function": {
                "name": "create_metabase_question",
                "description": "Save a chart to Metabase from a SQL SELECT against f1_silver. Count wins with SUM(is_win); join fact.driver_key = dim_driver.dim_driver_key; filter year with season=YEAR. Returns a URL.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "sql": {"type": "string"},
                        "display": {"type": "string", "enum": ["table", "bar", "line", "pie", "row"]},
                        "x_axis": {"type": "string"},
                        "y_axis": {"type": "string"},
                    },
                    "required": ["name", "sql"],
                },
            },
        },
    ),
}

SYSTEM = (
    "You are an F1 data assistant. Always answer in English. "
    "You MUST use the tools to get data — never invent numbers. "
    "After a tool returns, give a concise answer using its data."
)


def run(question: str) -> None:
    client = httpx.Client(base_url=OLLAMA_URL, timeout=120.0)

    # Retrieve relevant semantic-layer knowledge and inject it as context.
    kb = search_knowledge(question, k=2)
    kb_text = ""
    if kb.get("status") == "success":
        parts = []
        if kb["metrics"]:
            parts.append("Relevant metric formulas:\n" + "\n".join(
                f"  {m['name']} = {m['sql']}" for m in kb["metrics"]))
        if kb["glossary"]:
            parts.append("Relevant terms:\n" + "\n".join(
                f"  {g['term']}: {g['definition']}" for g in kb["glossary"]))
        if kb["query_patterns"]:
            parts.append("Relevant SQL templates (adapt these):\n" + "\n\n".join(
                f"-- {p['name']}: {p['description']}\n{p['sql']}" for p in kb["query_patterns"]))
        kb_text = "\n\n".join(parts)

    system = SYSTEM + (f"\n\nCONTEXT FOR THIS QUESTION:\n{kb_text}" if kb_text else "")
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": question},
    ]
    tool_schemas = [schema for (_, schema) in TOOLS.values()]

    for step in range(6):
        resp = client.post(
            "/v1/chat/completions",
            json={"model": MODEL, "messages": messages, "tools": tool_schemas, "stream": False},
        ).json()
        msg = resp["choices"][0]["message"]
        messages.append(msg)

        calls = msg.get("tool_calls")
        if not calls:
            print("\n=== ANSWER ===")
            print(msg.get("content", "(no content)"))
            return

        for call in calls:
            name = call["function"]["name"]
            args = json.loads(call["function"]["arguments"] or "{}")
            print(f"  → calling {name}({args})")
            fn = TOOLS.get(name, (None, None))[0]
            result = fn(**args) if fn else {"status": "error", "error": f"unknown tool {name}"}
            messages.append({
                "role": "tool",
                "tool_call_id": call["id"],
                "content": json.dumps(result, default=str)[:4000],
            })

    print("(stopped after max steps)")


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "Who won the most races in 2023?"
    print(f"Q: {q}")
    run(q)
