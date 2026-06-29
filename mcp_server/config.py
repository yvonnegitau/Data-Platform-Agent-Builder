import os

# ── Postgres serving layer ────────────────────────────────────────────────
# The silver star schema is served from Postgres (single source of truth,
# shared with Metabase). The MCP server reads it directly via psycopg.
PG_HOST = os.getenv("PG_HOST", os.getenv("POSTGRES_HOST", "localhost"))
PG_PORT = os.getenv("PG_PORT", os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("PG_DB", os.getenv("POSTGRES_DB", "dagster"))
PG_USER = os.getenv("PG_USER", os.getenv("POSTGRES_USER", "dagster"))
PG_PASSWORD = os.getenv("PG_PASSWORD", os.getenv("POSTGRES_PASSWORD", "dagsterpass"))
PG_SILVER_SCHEMA = os.getenv("PG_SILVER_SCHEMA", "f1_silver")
PG_DSN = (
    f"dbname={PG_DB} host={PG_HOST} user={PG_USER} "
    f"password={PG_PASSWORD} port={PG_PORT}"
)

# ── Metabase publishing ───────────────────────────────────────────────────
# Used by the "publish to Metabase" tools to create native-SQL questions and
# dashboards via Metabase's REST API. MB_BASE_URL is how this server reaches
# Metabase (metabase:3000 in docker; localhost:3002 from the host). MB_PUBLIC_URL
# is the browser-facing URL embedded in returned links.
MB_BASE_URL = os.getenv("MB_BASE_URL", "http://localhost:3002")
MB_PUBLIC_URL = os.getenv("MB_PUBLIC_URL", os.getenv("MB_BASE_URL", "http://localhost:3002"))
MB_API_KEY = os.getenv("MB_API_KEY", os.getenv("METABASE_API_KEY", ""))
MB_DATABASE_NAME = os.getenv("MB_DATABASE_NAME", "F1 Silver")
MB_DEFAULT_COLLECTION = os.getenv("MB_DEFAULT_COLLECTION", "F1 Analytics")

# Safety caps on raw SQL queries
MAX_ROWS = 500
MAX_SQL_LENGTH = 4000

# The schema where all silver models live
SILVER_SCHEMA = "silver"

# F1 era boundary years — used to auto-inject caveats into query results
ERA_CAVEATS = [
    {
        "before_year": 2010,
        "note": "Points system changed in 2010 (top 10 score, 25 pts for a win). "
                "Pre-2010 raw points totals are not directly comparable to modern seasons.",
    },
    {
        "before_year": 1991,
        "note": "Before 1991, only the top 6 finishers scored points. "
                "Win counts and podiums are more reliable cross-era comparisons than points.",
    },
]
