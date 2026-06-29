# AI Serving Layer — Architecture & Findings

How the F1 data warehouse is served to a self-hosted AI assistant that answers
questions and builds Metabase dashboards from natural language, running entirely
on local hardware.

## Final architecture

```
Open WebUI (chat, :3000)
  └─ Ollama model (f1-qwen) ── native function calling
       └─ OpenAPI tool server  ──>  mcpo-openapi (:8001)
                                      └─ F1 MCP server (server.py)
                                           ├─ reads  Postgres f1_silver  (direct, psycopg)
                                           └─ writes Metabase questions/dashboards (REST API)
  Metabase (BI, :3002) ── reads ──> Postgres f1_silver
```

### Components

| Service | Port | Role |
|---|---|---|
| `postgres` | 5432 | bronze + **`f1_silver`** serving schema (single source of truth) |
| `mcpo_openapi` | 8001 | MCP server exposed as OpenAPI so Open WebUI can call tools natively |
| `metabase` | 3002 | dashboards/charts over `f1_silver`; admin: wamseve@gmail.com |
| `open_webui` | 3000 | Ollama-native chat UI (the agent harness) |

Dagster (3001) + pgAdmin are stopped by default to free RAM; start when needed.

### Data flow into the serving layer
dlt loads raw data into Postgres `f1_bronze`. dbt (the `dbt-postgres` adapter)
builds staging and the silver star schema **directly in Postgres** — `f1_bronze_staging`
and `f1_silver` — plus the `season_completeness` view. The MCP server reads `f1_silver`
directly via psycopg; its tool SQL uses a `silver.` prefix that `database.py` rewrites
to `f1_silver` centrally. **DuckDB has been removed entirely**: no file, no sync script,
no postgres-extension indirection. The only DuckDB-specific SQL that needed rewriting
was `strftime` → `to_char` in `dim_date`. Dagster's dbt assets target the `prod`
(Postgres) profile; run a `dbt build` to verify the migration end to end.

## Why Postgres, not DuckDB, as the serving store
DuckDB as the OLAP layer hit three walls: single-file write lock (Metabase's
persistent connection vs. Dagster writes vs. MCP reads), no first-class Metabase
driver, and no concurrency (embedded, not a server). At F1 data size (tens of
thousands of rows) columnar OLAP buys nothing, so Postgres — already in the stack —
is the pragmatic choice. ClickHouse would be the move only at 100M+ rows.

## Harness finding: Open WebUI > LibreChat (for local models)
LibreChat's agent harness would **not reliably bind tools** to a small local
Ollama model — it narrated ("I'm calling X…") or emitted text-format tool calls
instead of structured calls, even with correct config (right model, few tools, 16k
context, `requiresOAuth:false`, renamed endpoint). The model itself tool-calls
perfectly through Ollama's `/v1` OpenAI-compatible API (verified streaming + not).
Open WebUI, being **Ollama-native with "Native" function calling**, works where
LibreChat didn't. LibreChat has since been removed from the stack.

### The model
`f1-qwen` = a custom Ollama model: `qwen2.5:7b` + `num_ctx 16384` + `temperature 0.1`.
Created because Ollama's default context (2048) truncated the tool schemas →
garbled / wrong-language output. On a 16 GB MacBook Air, a 14B swaps and hangs
(Docker VM reserves ~7.6 GB; only ~5-6 GB left for Ollama), so 7B is the ceiling
here. A 14B — or 24B with longer context — would materially improve analytical
correctness; it needs a machine with more RAM.

### Metabase MCP: why we don't use the official one
Metabase ships a built-in MCP (`/api/metabase-mcp`, header auth via API key works,
no OAuth needed). But its create flow uses a multi-step **MBQL 5** workflow whose
strict JSON schemas overwhelm small models. We instead added our own
`create_metabase_question` / `create_metabase_dashboard` to the F1 MCP — they take
`name + raw SQL + chart type`, validate the SQL, and save via Metabase's REST API
into a shared collection. Simple enough for a 7B; the official MCP is removed.

## The core finding: grounding a weak model for text-to-SQL

A 7B can't author correct warehouse SQL from a bare prompt — it hallucinates
columns (`driver_name`, `winner_id`), wrong tables (`fact`), wrong filters
(`YEAR(race_date)`). What makes it reliable is a stack of **grounding**, not a
bigger prompt:

1. **Schema + metric formulas in the tool description** — the tricky domain logic
   (DNF = `position_text='R'`, `win_rate = COUNT(*) FILTER (WHERE is_win=1)::numeric / COUNT(*)`)
   stated once, reused everywhere.
2. **SQL validation before save** — `create_metabase_question` dry-runs the SQL via
   Metabase's dataset API and rejects broken SQL with the real error **plus the
   schema**, so the model self-corrects instead of saving a dead chart.
3. **A retry loop** — the agent calls again with the corrected SQL. A 7B typically
   converges in 2–3 tries.
4. **Aggregate server-side, hand the model a sentence** — e.g. `compare_drivers`
   returns a `headline` ("Across 135 races, Verstappen finished ahead 89 to 44")
   and a `by_season` tally instead of 135 raw rows that overflow context.
5. **Expose computed metrics as views** — coverage isn't a table, so
   `f1_silver.season_completeness` is a view the model can `SELECT` and chart
   directly. The right pattern for any "computed by a tool" metric.

### Semantic-layer retrieval (the interesting part)

Hand-writing a worked example per question is "teaching to the test" — it doesn't
scale and masks the model's real limits. The scalable, honest version is
**retrieval over the semantic layer**: given the question, fetch the most relevant
query patterns, metric formulas, and glossary terms, and feed *those* as context.
The model then **composes** them for queries it has never seen.

- `tools/knowledge.py::search_knowledge(query, k)` — dependency-free token-overlap
  scoring over `query_patterns.yaml`, `catalog.yaml` (metrics), and `glossary.yaml`.
  Small corpus, so keyword scoring is enough; swap in embeddings if it grows.
- It is delivered to the model **three ways**, because small models won't reliably
  call a "retrieve first" tool on their own:
  1. **Registered MCP tool** `search_knowledge` — the system prompt tells the model
     to call it first (works, but a 7B often skips it).
  2. **Pushed in the script** — `scripts/f1_agent.py` calls `search_knowledge`
     before the model turn and injects the results into the system prompt. Most
     reliable; this is why the script never needs nudging.
  3. **Auto-retrieved on validation error** — when `create_metabase_question` SQL
     fails, it runs `search_knowledge` on the chart name and appends the
     best-matching pattern to the error hint. This is the key trick for Open WebUI:
     retrieval rides along on the tool the model *already* calls, so we don't depend
     on it choosing to call `search_knowledge`.

**Proven result:** a never-seeded "rolling 3-race average finishing position"
(window functions) and "DNF rate per circuit over 5 seasons" both succeeded —
because retrieval surfaced the `rolling_form` pattern and the `dnf_rate` metric,
and the model adapted them. That's genuine generalization, not a hand-fed answer.

### Where it breaks (verified by testing)
A round of test questions, each checked against the database, mapped the boundary:

| Question | Result |
|---|---|
| Points gap, Verstappen vs Norris, 2024 | ✅ correct (55 = 399 − 344) |
| Hamilton's longest win streak | ✅ count right (5, 2020); race-name labels slipped |
| Best average finishing position, 2023 | ❌ wrong: answered "Hamilton"; real answer Verstappen 1.27 (Hamilton 3rd, 5.57) |
| Circuit with highest DNF rate | ✅ correct (Reims-Gueux 88.9%, 16/18); no small-sample caveat |

The pattern: **metric-on-a-dimension** and **single-entity** questions work, but
**superlatives that require ranking across entities** ("which driver had the best…")
break — the model grabs a single-entity pattern, invents a subject, and asserts a
winner it never actually computed. Validation catches SQL that does not *run*, not
SQL that runs and answers the *wrong question*. Retrieval surfaced the right
knowledge every time; the ceiling is the 7B choosing the wrong piece of it. A larger
model is the fix.

## Hardware sizing for client deployments

The binding constraint is running a model large enough for reliable analytical
SQL **alongside** the Docker data stack. A 7B is POC-grade (hallucinates nuanced
SQL); a 14B is the practical floor for a *good* experience; 32B is the best local
reasoning we'd realistically deploy.

Approximate memory budget (q4 quantization, ~16k context):

| Component | RAM |
|---|---|
| Docker stack (Postgres, Metabase, Open WebUI, MCP) | ~3–4 GB |
| OS / overhead | ~3–4 GB |
| Model — 7B | ~6 GB |
| Model — **14B** | ~11–12 GB |
| Model — **32B** | ~22–24 GB |

Recommended tiers:

| Tier | Spec | Model | Use |
|---|---|---|---|
| POC only | 16 GB unified | 7B | demos; unreliable analytics |
| **Minimum viable** | **32 GB unified** (or 16 GB RAM + 16–24 GB GPU) | **14B** | solid single-user |
| Recommended | 64 GB unified (or 24 GB GPU) | 32B | best reasoning / light multi-user |

Caveats to price in:
- **Apple Silicon vs GPU box.** Mac = unified memory (RAM doubles as VRAM). On a
  Linux server the model lives in **GPU VRAM** (e.g. a 24 GB GPU runs a 32B q4),
  with system RAM separate for Docker. A GPU box is usually better value + faster
  for a dedicated deployment.
- **Speed ≠ fitting.** Inference responsiveness tracks memory bandwidth / GPU, not
  just capacity. M-series is good; CPU-only is sluggish at 14B+.
- **Concurrency.** One model instance handles one request at a time well. Multiple
  simultaneous users → more VRAM/RAM or a batching inference server (e.g. vLLM),
  pushing toward the 64 GB / dedicated-GPU tier.

**Deployment shape:** the stack decouples the model from the data services, so a
serious client deployment typically runs **one GPU inference server** (sized for
the model) with the lightweight data stack pointed at it — cleaner and more
scalable than a single fat workstation.

## Pending / next steps
- **Inline chart rendering** in the chat (Metabase inline charts only render in
  Claude/Cursor/ChatGPT clients; Open WebUI shows a Metabase URL). A rendering/
  embed service (Metabase public/embedded links surfaced as mcp-ui resources) would
  show charts inline.
- **Per-user Open WebUI login → per-user Metabase identity** for RBAC, so created
  charts belong to the requesting user instead of the shared API-key service account.
- **Verify the dbt-postgres build** end to end (`dbt build`) after the DuckDB
  removal, then confirm Dagster's dbt assets run against the `prod` (Postgres) target.
- **Bigger model** (14B/24B, more RAM) to lift the analytical-correctness ceiling.

## Operational notes
- Metabase service API key lives in `infrastructure/.env` (`METABASE_API_KEY`,
  gitignored); used by the MCP publish tools and was Administrators-scoped for local
  dev — scope down before any real deployment.
- Charts land in the shared **"F1 Analytics"** Metabase collection (never the
  service account's hidden personal collection — pass an explicit `collection_id`).
- `scripts/f1_agent.py` is the reference local agent loop (and the only place
  retrieval is auto-pushed) — handy for testing without the UI.
