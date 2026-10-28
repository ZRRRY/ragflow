"""Regression tests for incremental graph persistence and candidate recall.

Run real graph functions against a store whose writes are invisible until an
explicit refresh. Service/LLM boundaries are replaced, not graph algorithms.
"""

import importlib.util
import json
import logging
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import networkx as nx
import numpy as np
import pytest

for _name in ("api.db.services.document_service", "api.db.services.knowledgebase_service"):
    if _name not in sys.modules:
        sys.modules[_name] = MagicMock()

# Keep the isolated GraphRAG tests independent of model-provider SDKs.
try:
    import rag.llm.siliconflow_timeout_patch  # noqa: F401
except Exception:
    for _name in ("rag.llm", "rag.llm.embedding_model", "rag.llm.siliconflow_timeout_patch"):
        if _name not in sys.modules:
            sys.modules[_name] = MagicMock()

from common import settings
from rag.graphrag import document_delete_extras, utils, utils_extras, utils_pagination
from rag.graphrag.config import GraphRAGConfig
import rag.graphrag.entity_resolution  # noqa: F401  # Install the custom resolver before importing its extras.
from rag.graphrag.entity_resolution_extras import build_excluded_types
from rag.graphrag.general import index_extras
from rag.graphrag.utils import GraphChange


async def run_inline(fn, *args, **kwargs):
    return fn(*args, **kwargs)


class Store:
    def __init__(self):
        self.rows = {}
        self.visible = {}
        self.deletions = []
        self.fail_bulk = False
        self.fail_query = False
        self.fail_refresh = False
        self.fail_refresh_at = None
        self.refreshes = 0

    def refresh_idx(self, _index):
        self.refreshes += 1
        if self.fail_refresh or self.refreshes == self.fail_refresh_at:
            return False
        self.visible = deepcopy(self.rows)
        return True

    @staticmethod
    def matches(row, filters):
        for key, value in filters.items():
            if key == "must_not":
                if row.get(value["exists"]):
                    return False
                continue
            actual = row.get(key, [])
            actual = actual if isinstance(actual, list) else [actual]
            wanted = value if isinstance(value, list) else [value]
            if not set(actual) & set(wanted):
                return False
        return True

    def search(self, fields, highlight, filters, expressions, order, offset, limit, index, kb_ids):
        if self.fail_query:
            raise RuntimeError("query unavailable")
        rows = [row for row in self.visible.values() if row["kb_id"] in kb_ids and self.matches(row, filters)]
        return {"rows": rows[offset:offset + limit], "total": len(rows)}

    def get_doc_ids(self, result):
        return [row["id"] for row in result["rows"]]

    def get_fields(self, result, fields):
        return {row["id"]: {key: deepcopy(row[key]) for key in fields if key in row} for row in result["rows"]}

    def get_total(self, result):
        return result["total"]

    def insert(self, rows, index, kb_id):
        for row in rows:
            self.rows[row["id"]] = deepcopy(row)
            if self.fail_bulk:
                return ["injected partial bulk failure"]
        return []

    def delete(self, condition, index, kb_id, refresh=True):
        ids = [cid for cid, row in self.visible.items() if row["kb_id"] == kb_id and self.matches(row, condition)]
        for cid in ids:
            self.rows.pop(cid, None)
        self.deletions.append(deepcopy(condition))
        if refresh:
            self.refresh_idx(index)
        return len(ids)

    def update(self, condition, values, index, kb_id):
        for cid, row in self.visible.items():
            if row["kb_id"] != kb_id or not self.matches(row, condition):
                continue
            if "remove" in values:
                for key, value in values["remove"].items():
                    self.rows[cid][key] = [item for item in row.get(key, []) if item != value]
            else:
                self.rows[cid].update(values)
        self.refresh_idx(index)
        return True

    def search_with_scroll(self, index, query, fields, **kwargs):
        filters = {}
        for clause in query["query"]["bool"]["filter"]:
            filters.update(clause.get("term", clause.get("terms", {})))
        rows = [row for row in self.visible.values() if self.matches(row, filters)]
        return {"rows": rows, "total": len(rows)}


def entity(name, source="old", cid=None, kind="person"):
    meta = {"entity_type": kind, "description": source + " description", "source_id": [source], "pagerank": 0.001, "rank": 1}
    return {"id": cid or "old-" + name, "kb_id": "kb", "entity_kwd": name, "entity_type_kwd": kind,
            "knowledge_graph_kwd": "entity", "source_id": [source], "content_with_weight": json.dumps(meta)}


def relation(source="old"):
    meta = {"description": source + " edge", "source_id": [source], "weight": 2, "keywords": ["k"]}
    return {"id": "old-edge", "kb_id": "kb", "from_entity_kwd": "A", "to_entity_kwd": "B",
            "knowledge_graph_kwd": "relation", "source_id": [source], "content_with_weight": json.dumps(meta)}


def subgraph(source="new"):
    graph = nx.Graph(source_id=[source])
    # Deliberately reverse insertion order: Graph.edges() yields (B, A).
    for name in ("B", "A"):
        graph.add_node(name, **json.loads(entity(name, source)["content_with_weight"]))
    graph.add_edge("B", "A", **json.loads(relation(source)["content_with_weight"]))
    return graph


@pytest.fixture
def environment(monkeypatch):
    store = Store()
    for row in (entity("A"), entity("B"), relation()):
        store.rows[row["id"]] = row
    store.refresh_idx("idx")
    monkeypatch.setattr(settings, "docStoreConn", store)
    for module in (utils, utils_extras, utils_pagination, index_extras):
        monkeypatch.setattr(module, "thread_pool_exec", run_inline)
    monkeypatch.setattr(utils, "get_embed_cache", lambda *args: None)
    monkeypatch.setattr(utils, "set_embed_cache", lambda *args: None)
    monkeypatch.setattr(utils_extras.rag_tokenizer, "tokenize", lambda text: text)
    monkeypatch.setattr(utils_extras.rag_tokenizer, "fine_grained_tokenize", lambda text: text)
    monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", True)
    monkeypatch.setattr(GraphRAGConfig, "GRAPHRAG_MERGE_SKIP_UNCHANGED", True)
    monkeypatch.setattr(GraphRAGConfig, "GRAPHRAG_REL_QUERY_EXACT", True)
    monkeypatch.setattr(GraphRAGConfig, "SET_GRAPH_DELTA_REFRESH_AFTER_INSERT", False)
    monkeypatch.setattr(utils_extras, "_SET_GRAPH_STREAM_WINDOW", 2)
    model = SimpleNamespace(llm_name="test", encode=lambda texts: (np.ones((len(texts), 2)), 0))
    return store, model


async def merge(model, source="new"):
    return await index_extras.merge_subgraph_incremental("tenant", "kb", source, subgraph(source), model, lambda **kw: None)


def assert_merged(store, sources):
    assert set(store.rows) == {"old-A", "old-B", "old-edge"}
    for row in store.rows.values():
        meta = json.loads(row["content_with_weight"])
        assert set(meta["source_id"]) == set(sources)
        assert "old" in meta["description"]
    assert json.loads(store.rows["old-edge"]["content_with_weight"])["weight"] == 2 * len(sources)


async def test_embedding_failure_keeps_history_and_retry_reuses_ids(environment):
    store, model = environment
    original = deepcopy(store.rows)
    encode = model.encode
    model.encode = MagicMock(side_effect=RuntimeError("embedding unavailable"))
    with pytest.raises(RuntimeError, match="embedding unavailable"):
        await merge(model)
    assert store.rows == original
    assert not store.deletions
    model.encode = encode
    await merge(model)
    assert_merged(store, ["old", "new"])


async def test_partial_bulk_failure_can_be_retried_without_losing_history(environment):
    store, model = environment
    store.fail_bulk = True
    with pytest.raises(Exception, match="partial bulk failure"):
        await merge(model)
    assert set(store.rows) == {"old-A", "old-B", "old-edge"}
    assert not store.deletions
    store.fail_bulk = False
    await merge(model)
    await merge(model)
    assert_merged(store, ["old", "new"])


async def test_partial_first_build_retry_does_not_duplicate_successful_writes(environment):
    store, model = environment
    store.rows.clear()
    store.refresh_idx("idx")
    store.fail_bulk = True
    with pytest.raises(Exception, match="partial bulk failure"):
        await merge(model)
    store.fail_bulk = False
    await merge(model)
    ids = set(store.rows)
    await merge(model)
    assert len(ids) == 3
    assert set(store.rows) == ids
    assert all(json.loads(row["content_with_weight"])["source_id"] == ["new"] for row in store.rows.values())


async def test_consecutive_merges_see_previous_writes_with_refresh_flag_off(environment):
    store, model = environment
    await merge(model, "doc1")
    await merge(model, "doc2")
    assert_merged(store, ["old", "doc1", "doc2"])
    assert store.visible.keys() == store.rows.keys()


async def test_refresh_failure_after_upsert_retries_without_duplicates(environment):
    store, model = environment
    store.fail_refresh_at = store.refreshes + 3
    with pytest.raises(RuntimeError, match="refresh failed"):
        await merge(model)
    assert store.visible.keys() == {"old-A", "old-B", "old-edge"}
    assert not store.deletions

    store.fail_refresh_at = None
    await merge(model)
    assert_merged(store, ["old", "new"])
    assert store.visible.keys() == store.rows.keys()


async def test_new_records_have_stable_namespaced_ids(environment):
    store, model = environment
    store.rows.clear()
    store.refresh_idx("idx")
    await merge(model)
    ids = set(store.rows)
    await merge(model)
    assert set(store.rows) == ids
    assert len(ids) == 3
    make_id = utils_extras._graph_chunk_id
    assert make_id("kb", "relation", "A", "B") == make_id("kb", "relation", "B", "A")
    assert make_id("kb", "entity", "A") != make_id("other", "entity", "A")


@pytest.mark.parametrize("exact", [True, False])
@pytest.mark.parametrize("stored_endpoints", [("A", "B"), ("B", "A")])
async def test_reverse_edge_query_finds_stored_edge(environment, monkeypatch, exact, stored_endpoints):
    monkeypatch.setattr(GraphRAGConfig, "GRAPHRAG_REL_QUERY_EXACT", exact)
    store, _ = environment
    store.rows["old-edge"]["from_entity_kwd"], store.rows["old-edge"]["to_entity_kwd"] = stored_endpoints
    store.refresh_idx("idx")
    rows = await utils_extras.query_existing_relations("tenant", "kb", [("B", "A"), ("A", "B")])
    assert list(rows) == [("A", "B")]
    assert rows[("A", "B")]["id"] == "old-edge"


async def test_legacy_reversed_edge_is_reused_and_removed_by_id(environment):
    store, model = environment
    store.rows["old-edge"]["from_entity_kwd"] = "B"
    store.rows["old-edge"]["to_entity_kwd"] = "A"
    store.refresh_idx("idx")
    graph = subgraph()
    change = GraphChange(added_updated_edges={("A", "B")})
    await utils_extras.set_graph_delta("tenant", "kb", model, graph, change, None)
    assert store.rows["old-edge"]["from_entity_kwd"] == "A"
    assert len(store.rows) == 3

    change = GraphChange(removed_edges={("B", "A")})
    await utils_extras.set_graph_delta("tenant", "kb", model, graph, change, None)
    assert "old-edge" not in store.rows


@pytest.mark.parametrize("failure", ["query", "metadata", "duplicate_entity", "duplicate_relation", "refresh"])
async def test_invalid_reads_abort_before_writing(environment, failure):
    store, model = environment
    if failure == "query":
        store.fail_query = True
    elif failure == "metadata":
        store.rows["old-A"]["content_with_weight"] = "[]"
    elif failure.startswith("duplicate"):
        row = deepcopy(store.rows["old-A" if failure == "duplicate_entity" else "old-edge"])
        row["id"] = "duplicate"
        store.rows[row["id"]] = row
    store.refresh_idx("idx")
    if failure == "refresh":
        store.fail_refresh = True
    before = deepcopy(store.rows)
    with pytest.raises((RuntimeError, ValueError)):
        await merge(model)
    assert store.rows == before
    assert not store.deletions


async def test_removals_happen_only_after_all_replacements_succeed(environment):
    store, model = environment
    graph = subgraph()
    graph.remove_node("B")
    change = GraphChange(added_updated_nodes={"A"}, removed_nodes={"B"}, removed_edges={("B", "A")})
    encode = model.encode
    model.encode = MagicMock(side_effect=RuntimeError("embedding unavailable"))
    with pytest.raises(RuntimeError):
        await utils_extras.set_graph_delta("tenant", "kb", model, graph, change, None)
    assert not store.deletions
    model.encode = encode
    await utils_extras.set_graph_delta("tenant", "kb", model, graph, change, None)
    assert set(store.rows) == {"old-A"}


@pytest.mark.parametrize("chunk_num", [0, 10])
def test_document_delete_without_graph_blob_keeps_shared_records(environment, monkeypatch, chunk_num):
    store, _ = environment
    monkeypatch.setattr(GraphRAGConfig, "DELETE_SUBGRAPH_ON_DOC_DELETE", True)
    store.rows["old-A"]["source_id"].append("shared")
    for kind in ("subgraph", "merge_state", "community_report"):
        store.rows[kind] = {"id": kind, "kb_id": "kb", "knowledge_graph_kwd": kind, "source_id": ["old"],
                            "content_with_weight": json.dumps({"source_id": ["old"]})}
    foreign = entity("foreign", cid="foreign")
    foreign["kb_id"] = "another-kb"
    store.rows["foreign"] = foreign
    unrelated_empty = entity("unrelated", cid="unrelated-empty")
    unrelated_empty["source_id"] = []
    store.rows["unrelated-empty"] = unrelated_empty
    store.refresh_idx("idx")
    document_delete_extras.cleanup_knowledge_graph_references(SimpleNamespace(id="old", kb_id="kb", chunk_num=chunk_num), "idx")
    assert set(store.rows) == {"old-A", "foreign", "unrelated-empty"}
    assert store.rows["old-A"]["source_id"] == ["shared"]
    assert json.loads(store.rows["old-A"]["content_with_weight"])["source_id"] == ["shared"]


async def test_deleted_source_is_not_resurrected_by_a_later_merge(environment, monkeypatch):
    store, model = environment
    monkeypatch.setattr(GraphRAGConfig, "DELETE_SUBGRAPH_ON_DOC_DELETE", True)
    for row in store.rows.values():
        row["source_id"].append("shared")
    store.refresh_idx("idx")
    document_delete_extras.cleanup_knowledge_graph_references(SimpleNamespace(id="old", kb_id="kb", chunk_num=0), "idx")

    await merge(model, "shared")
    for row in store.rows.values():
        assert set(row["source_id"]) == {"shared"}
        assert set(json.loads(row["content_with_weight"])["source_id"]) == {"shared"}


def test_document_delete_metadata_failure_aborts_cleanup(environment, monkeypatch):
    store, _ = environment
    monkeypatch.setattr(GraphRAGConfig, "DELETE_SUBGRAPH_ON_DOC_DELETE", True)
    store.rows["old-A"]["content_with_weight"] = "not-json"
    store.refresh_idx("idx")
    before = deepcopy(store.rows)
    with pytest.raises(json.JSONDecodeError):
        document_delete_extras.cleanup_knowledge_graph_references(SimpleNamespace(id="old", kb_id="kb", chunk_num=0), "idx")
    assert store.rows == before
    assert not store.deletions


@pytest.mark.parametrize("names", [{"Alice", "Alicia"}, {"Alice"}])
async def test_resolution_includes_new_pairs_and_does_not_exclude_extraction_types(environment, monkeypatch, names):
    store, model = environment
    store.rows = {row["id"]: row for row in (entity("Alice"), entity("Alicia"), entity("Book", kind="Book"))}
    store.refresh_idx("idx")
    captured = []

    class Resolver:
        def __init__(self, llm, excluded_types):
            assert "person" not in excluded_types
            assert "Book" in excluded_types

        async def __call__(self, graph, nodes, **kwargs):
            captured.append(kwargs["candidate_resolution"])
            return SimpleNamespace(graph=graph, change=GraphChange())

    monkeypatch.setattr(index_extras, "EntityResolution", Resolver)
    monkeypatch.setattr(index_extras, "is_similarity_str", lambda a, b: True)
    monkeypatch.setattr(index_extras, "load_checkpoints", AsyncMock(return_value={}))
    monkeypatch.setattr(index_extras, "cleanup_checkpoints", AsyncMock())
    await index_extras.resolve_entities_incremental(None, names | {"Book"}, "tenant", "kb", None, None, model,
                                                     lambda **kw: None, entity_types=["person", "Book"])
    assert captured == [{"person": [("Alice", "Alicia")]}]


def test_structural_aliases_are_excluded_without_configuration():
    excluded = build_excluded_types()
    assert {"书籍", "章节", "book", "Book", "Chapter", "section"} <= excluded
    assert not {"organization", "person", "geo", "event", "category"} & excluded


@pytest.mark.parametrize("fields,projection", [(["entity_kwd"], ["entity_kwd"]), (["_id"], False), (["_id", "entity_kwd"], ["entity_kwd"])])
def test_scroll_projects_source_without_mutating_query(monkeypatch, fields, projection):
    # Load the real extras module without instantiating the service singleton.
    path = Path(__file__).resolve().parents[4] / "common/doc_store/es_conn_extras.py"
    spec = importlib.util.spec_from_file_location("review_es_extras", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setitem(sys.modules, "rag.utils.es_conn", SimpleNamespace(ATTEMPT_TIME=1))
    raw = MagicMock()
    raw.search.return_value = {"_scroll_id": "scroll", "hits": {"hits": [{"_id": "record", "_source": {"entity_kwd": "A"}}]}}
    raw.scroll.return_value = {"hits": {"hits": []}}
    query = {"query": {"match_all": {}}}
    before = deepcopy(query)
    result = module._es_search_with_scroll(SimpleNamespace(es=raw, logger=logging.getLogger(__name__)), "idx", query, fields)
    assert query == before
    assert raw.search.call_args.kwargs["body"]["_source"] == projection
    assert result["hits"]["hits"][0]["_id"] == "record"
    raw.clear_scroll.assert_called_once()


async def test_search_after_failure_is_not_reported_as_missing(environment, monkeypatch):
    store, _ = environment
    store.es = SimpleNamespace(search=MagicMock(side_effect=RuntimeError("ES unavailable")))
    with pytest.raises(RuntimeError, match="ES unavailable"):
        await utils_extras.query_existing_entities("tenant", "kb", ["A"])


async def test_search_after_limit_is_fail_closed(environment):
    store, _ = environment
    store.es = SimpleNamespace(search=lambda **kw: {"hits": {"hits": [{"_id": "one", "_source": {}, "sort": ["A", 0]}]}})
    with pytest.raises(RuntimeError, match="max_pages"):
        await utils_pagination.collect_all({}, "idx", "kb", ["entity_kwd"], "entity_kwd", page_size=1, max_pages=1)
