"""
tools/schema.py — Semantic layer tools.

These are the tools Claude calls BEFORE writing any SQL.
They answer: "What tables exist?", "What does this column mean?",
"How do I calculate win rate?", "What is DRS?", "Show me an example query."

All data comes from the static YAML files in semantic/ — no database calls needed.
"""

import yaml
from pathlib import Path

# Load all three semantic files once at import time
_SEMANTIC_DIR = Path(__file__).parent.parent / "semantic"
_CATALOG  = yaml.safe_load((_SEMANTIC_DIR / "catalog.yaml").read_text())
_GLOSSARY = yaml.safe_load((_SEMANTIC_DIR / "glossary.yaml").read_text())
_PATTERNS = yaml.safe_load((_SEMANTIC_DIR / "query_patterns.yaml").read_text())


def get_schema(table_name: str | None = None) -> dict:
    """
    Returns schema information for the F1 silver layer.

    - Call with no argument to get a summary of all available tables.
    - Call with a table name (e.g. 'fact_race_results') to get full column
      definitions, join keys, and grain for that table.

    Always call this before writing SQL so you know the exact column names.
    """
    tables = _CATALOG["tables"]

    if table_name is None:
        # Return a high-level summary: table name, description, grain
        summary = {
            name: {
                "description": meta["description"],
                "grain": meta.get("grain", ""),
                "schema": meta.get("schema", "silver"),
                "column_count": len(meta.get("columns", {})),
            }
            for name, meta in tables.items()
        }
        return {
            "status": "success",
            "tables": summary,
            "tip": "Call get_schema(table_name) for full column definitions of a specific table.",
        }

    # Normalise: allow partial match (e.g. "driver" → "dim_driver")
    match = _find_table(table_name, tables)
    if not match:
        return {
            "status": "error",
            "error": f"Table '{table_name}' not found.",
            "available_tables": list(tables.keys()),
        }

    meta = tables[match]
    return {
        "status": "success",
        "table": match,
        "full_ref": f"silver.{match}",
        "description": meta["description"],
        "grain": meta.get("grain", ""),
        "primary_key": meta.get("primary_key", ""),
        "join_keys": meta.get("join_keys", {}),
        "columns": meta.get("columns", {}),
    }


def get_metric_definition(metric_name: str | None = None) -> dict:
    """
    Returns the SQL formula and business definition for a named metric.

    Available metrics: win_rate, dnf_rate, podium_rate,
    avg_finishing_position, points_per_race.

    Call with no argument to list all available metrics.
    Use the returned SQL formula directly in your queries.
    """
    metrics = _CATALOG["metrics"]

    if metric_name is None:
        return {
            "status": "success",
            "available_metrics": {
                name: m["definition"] for name, m in metrics.items()
            },
        }

    match = _find_key(metric_name, metrics)
    if not match:
        return {
            "status": "error",
            "error": f"Metric '{metric_name}' not found.",
            "available_metrics": list(metrics.keys()),
        }

    m = metrics[match]
    return {
        "status": "success",
        "metric": match,
        "definition": m["definition"],
        "sql": m["sql"],
        "note": m.get("note", ""),
    }


def get_query_pattern(pattern_name: str | None = None) -> dict:
    """
    Returns a worked SQL example you can adapt for ad-hoc analysis.

    Call with no argument to list all available patterns with descriptions.
    Call with a pattern name to get the full SQL template.

    Substitute {parameter_name} placeholders with real values before
    passing the SQL to execute_sql().

    Patterns with window functions are tagged with the functions they use.
    """
    patterns = {p["name"]: p for p in _PATTERNS["patterns"]}

    if pattern_name is None:
        return {
            "status": "success",
            "available_patterns": {
                name: {
                    "description": p["description"],
                    "parameters": p.get("parameters", []),
                    "window_functions": p.get("window_functions", []),
                }
                for name, p in patterns.items()
            },
        }

    match = _find_key(pattern_name, patterns)
    if not match:
        return {
            "status": "error",
            "error": f"Pattern '{pattern_name}' not found.",
            "available_patterns": list(patterns.keys()),
        }

    p = patterns[match]
    return {
        "status": "success",
        "pattern": match,
        "description": p["description"],
        "parameters": p.get("parameters", []),
        "window_functions": p.get("window_functions", []),
        "sql_template": p["sql"],
        "tip": "Replace {parameter} placeholders with real values, then call execute_sql().",
    }


def get_glossary(term: str | None = None) -> dict:
    """
    Look up F1 domain terminology.

    Call with no argument to list all defined terms.
    Call with a term (e.g. 'DNF', 'DRS', 'points system') to get its definition.

    Useful for understanding what data values mean before writing queries
    (e.g. what does status_category = 'Lapped' mean?).
    """
    terms = _GLOSSARY["terms"]

    if term is None:
        return {
            "status": "success",
            "terms": list(terms.keys()),
            "tip": "Call get_glossary(term) for a full definition.",
        }

    match = _find_key(term, terms)
    if not match:
        # Try partial match on definition text
        lower = term.lower()
        close = [k for k in terms if lower in k.lower()]
        return {
            "status": "not_found",
            "searched_for": term,
            "possible_matches": close,
            "tip": "Try one of the possible_matches above.",
        }

    t = terms[match]
    result = {
        "status": "success",
        "term": match,
        "definition": t["definition"] if isinstance(t, dict) else t,
    }
    if isinstance(t, dict) and "full" in t:
        result["full_name"] = t["full"]
    return result


# ── helpers ──────────────────────────────────────────────────────────────────

def _find_table(name: str, tables: dict) -> str | None:
    """Exact match first, then partial match."""
    if name in tables:
        return name
    lower = name.lower()
    for key in tables:
        if lower in key.lower():
            return key
    return None


def _find_key(name: str, d: dict) -> str | None:
    """Case-insensitive exact match, then partial match."""
    if name in d:
        return name
    lower = name.lower()
    for key in d:
        if key.lower() == lower:
            return key
    for key in d:
        if lower in key.lower():
            return key
    return None
