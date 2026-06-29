"""
tools/knowledge.py — retrieval over the semantic layer.

Given a natural-language question, returns the most relevant query patterns,
metrics, and glossary terms so the model gets the right building blocks as
context instead of guessing. Dependency-free keyword/token-overlap scoring —
the semantic files are small, so this is fast and good enough; swap in
embeddings later if the corpus grows.

This is the scalable, non-overfitting version of grounding: reusable knowledge
is retrieved per question rather than every example being hand-fed.
"""

import re
from pathlib import Path

import yaml

_SEMANTIC_DIR = Path(__file__).resolve().parent.parent / "semantic"
_CATALOG = yaml.safe_load((_SEMANTIC_DIR / "catalog.yaml").read_text())
_GLOSSARY = yaml.safe_load((_SEMANTIC_DIR / "glossary.yaml").read_text())
_PATTERNS = yaml.safe_load((_SEMANTIC_DIR / "query_patterns.yaml").read_text())

_STOP = {
    "the", "a", "an", "of", "for", "in", "on", "to", "and", "or", "by", "with",
    "show", "me", "give", "chart", "save", "metabase", "plot", "graph", "get",
    "which", "what", "who", "how", "many", "most", "each", "all", "over", "per",
    "is", "are", "was", "were", "do", "we", "have", "that", "this", "it",
}


def _tokens(text: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", text.lower()) if t and t not in _STOP and len(t) > 1}


def _score(query_tokens: set[str], text: str) -> int:
    return len(query_tokens & _tokens(text))


def search_knowledge(query: str, k: int = 3) -> dict:
    """
    Retrieve the most relevant semantic-layer knowledge for a question.

    Returns query_patterns (worked SQL templates), metrics (formulas), and
    glossary terms ranked by relevance. Call this BEFORE writing SQL so you
    reuse vetted logic instead of guessing.

    query: the user's question or analysis goal
    k: how many of each kind to return (default 3)
    """
    q = _tokens(query)
    if not q:
        return {"status": "error", "error": "empty query"}

    # Query patterns
    pats = []
    for p in _PATTERNS.get("patterns", []):
        text = f"{p.get('name','')} {p.get('description','')} {' '.join(p.get('parameters',[]))}"
        s = _score(q, text)
        if s:
            pats.append((s, {"name": p.get("name"), "description": p.get("description"), "sql": p.get("sql")}))
    pats.sort(key=lambda x: x[0], reverse=True)

    # Metrics
    mets = []
    for name, m in (_CATALOG.get("metrics") or {}).items():
        text = f"{name} {m.get('definition','')}"
        s = _score(q, text)
        if s:
            mets.append((s, {"name": name, "definition": m.get("definition"), "sql": m.get("sql")}))
    mets.sort(key=lambda x: x[0], reverse=True)

    # Glossary
    gloss = []
    for term, g in (_GLOSSARY.get("terms") or {}).items():
        defn = g.get("definition", "") if isinstance(g, dict) else str(g)
        full = g.get("full", "") if isinstance(g, dict) else ""
        s = _score(q, f"{term} {full} {defn}")
        if s:
            gloss.append((s, {"term": term, "definition": defn}))
    gloss.sort(key=lambda x: x[0], reverse=True)

    return {
        "status": "success",
        "query": query,
        "query_patterns": [p for _, p in pats[:k]],
        "metrics": [m for _, m in mets[:k]],
        "glossary": [g for _, g in gloss[:k]],
    }
