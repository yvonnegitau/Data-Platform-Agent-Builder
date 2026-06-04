import os
from pathlib import Path

# Path to the DuckDB file produced by the dbt silver layer
DB_PATH = Path(os.getenv("F1_DB_PATH", "/Users/aura/data/f1/medallion/f1_data.duckdb"))

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
