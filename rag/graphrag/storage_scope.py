"""Keep GraphRAG records separate from knowledge-compilation records.

Both features use knowledge_graph_kwd; compilation additionally sets compile_kwd.
Apply these guards to GraphRAG reads and mutations, including historical rows.
"""

from copy import deepcopy


def graph_condition(condition: dict) -> dict:
    """Add an exclusion without losing an existing missing-field condition."""
    condition = deepcopy(condition)
    excluded = condition.setdefault("must_not", {})
    fields = excluded.get("exists", [])
    fields = list(fields) if isinstance(fields, list) else [fields]
    if "compile_kwd" not in fields:
        fields.append("compile_kwd")
    excluded["exists"] = fields[0] if len(fields) == 1 else fields
    return condition


def graph_query(body: dict) -> dict:
    """Scope a native GraphRAG bool query (scroll or search_after)."""
    body = deepcopy(body)
    query = body["query"]["bool"]
    excluded = query.setdefault("must_not", [])
    if isinstance(excluded, dict):
        excluded = [excluded]
        query["must_not"] = excluded
    guard = {"exists": {"field": "compile_kwd"}}
    if guard not in excluded:
        excluded.append(guard)
    return body
