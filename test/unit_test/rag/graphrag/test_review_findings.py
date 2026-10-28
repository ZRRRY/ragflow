"""Behavioral regressions for the ZRRRY review findings and upstream upgrades.

Assertions describe the required behavior, so unfixed defects fail normally.
Run this module alone or with the GraphRAG suite. No live services are needed:
graph algorithms, orchestration, dispatchers and persistence functions are real;
the store, Redis locks/checkpoints and model responses are deterministic doubles.
"""

import asyncio
import json
import sys
from copy import deepcopy
from itertools import pairwise
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import networkx as nx
import numpy as np
import pytest

# Match the existing service isolation, including standalone collection.
for _name in (
    "editdistance",
    "spacy",
    "api.db.joint_services",
    "api.db.joint_services.tenant_model_service",
    "api.db.services.document_service",
    "api.db.services.knowledgebase_service",
    "api.db.services.llm_service",
):
    if _name not in sys.modules:
        sys.modules[_name] = MagicMock()

try:
    import rag.llm.siliconflow_timeout_patch  # noqa: F401
except ImportError:
    for _name in ("rag.llm", "rag.llm.embedding_model", "rag.llm.siliconflow_timeout_patch"):
        if _name not in sys.modules:
            sys.modules[_name] = MagicMock()

from common import settings
from rag.graphrag import entity_resolution, utils, utils_extras, utils_pagination
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.entity_resolution_extras import IncrementalEntityResolution
from rag.graphrag.general import index, index_extras, index_patch
from rag.graphrag.general.community_reports_extractor import CommunityReportsExtractor
from rag.graphrag.general.extractor import Extractor
from rag.svr import task_executor_extras


class RefreshStore:
    """ES-style search visibility: acknowledged writes need a refresh.

    delete_by_query sees the search snapshot and refreshes after deleting,
    matching the default refresh=True used by the monolithic writer.
    """

    def __init__(self):
        self.rows = {}
        self.visible = {}
        self.truncate_kind = None

    @staticmethod
    def matches(row, filters):
        for key, wanted in filters.items():
            actual = row.get(key, [])
            actual = actual if isinstance(actual, list) else [actual]
            wanted = wanted if isinstance(wanted, list) else [wanted]
            if not set(actual).intersection(wanted):
                return False
        return True

    def refresh_idx(self, index_name):
        self.visible = deepcopy(self.rows)
        return True

    def insert(self, rows, index_name, kb_id):
        for row in rows:
            self.rows[row["id"]] = deepcopy(row)
        return []

    def delete(self, condition, index_name, kb_id, refresh=True):
        ids = [cid for cid, row in self.visible.items() if row["kb_id"] == kb_id and self.matches(row, condition)]
        for cid in ids:
            self.rows.pop(cid, None)
        if refresh:
            self.refresh_idx(index_name)
        return len(ids)

    def search(self, fields, highlight, filters, expressions, order, offset, limit, index_name, kb_ids):
        rows = [row for row in self.visible.values() if row["kb_id"] in kb_ids and self.matches(row, filters)]
        return {"rows": rows[offset : offset + limit], "total": len(rows)}

    def get_fields(self, result, fields):
        return {row["id"]: {key: deepcopy(row[key]) for key in fields if key in row} for row in result["rows"]}

    def get_doc_ids(self, result):
        return [row["id"] for row in result["rows"]]

    def get_total(self, result):
        return result["total"]

    def search_with_scroll(self, index_name, query, fields, **kwargs):
        filters = {}
        for clause in query["query"]["bool"]["filter"]:
            filters.update(clause.get("term", clause.get("terms", {})))
        rows = [row for row in self.visible.values() if self.matches(row, filters)]
        truncated = filters["knowledge_graph_kwd"] == [self.truncate_kind]
        return {"rows": rows[:1] if truncated else rows, "_truncated": truncated}

    async def retrieve(self, condition, index_name, kb_ids):
        fields = condition["fields"]
        filters = {key: value for key, value in condition.items() if key not in ("fields", "size")}
        result = self.search(fields, [], filters, [], None, 0, condition["size"], index_name, kb_ids)
        return SimpleNamespace(total=result["total"], ids=self.get_doc_ids(result), field=self.get_fields(result, fields))


def make_graph(doc_id, *names):
    graph = nx.Graph(source_id=[doc_id])
    for name in names:
        graph.add_node(name, entity_type="person", description=f"About {name}", source_id=[doc_id], pagerank=0.5)
    for left, right in pairwise(names):
        graph.add_edge(left, right, description="related", source_id=[doc_id], weight=1, keywords=["related"])
    return graph


def seed_graph(store, graph):
    for name, metadata in graph.nodes(data=True):
        store.rows[name] = {
            "id": name,
            "kb_id": "kb",
            "knowledge_graph_kwd": "entity",
            "entity_kwd": name,
            "entity_type_kwd": metadata["entity_type"],
            "source_id": metadata["source_id"],
            "content_with_weight": json.dumps(metadata),
        }
    for left, right, metadata in graph.edges(data=True):
        cid = f"{left}-{right}"
        store.rows[cid] = {
            "id": cid,
            "kb_id": "kb",
            "knowledge_graph_kwd": "relation",
            "from_entity_kwd": left,
            "to_entity_kwd": right,
            "source_id": metadata["source_id"],
            "content_with_weight": json.dumps(metadata),
        }
    store.refresh_idx("idx")


def seed_checkpoint(store, doc_id, graph, merged=False):
    store.rows[f"sg-{doc_id}"] = {
        "id": f"sg-{doc_id}",
        "kb_id": "kb",
        "knowledge_graph_kwd": "subgraph",
        "source_id": [doc_id],
        "removed_kwd": "N",
        "content_with_weight": json.dumps(nx.node_link_data(graph, edges="edges")),
    }
    if merged:
        store.rows[f"state-{doc_id}"] = {
            "id": f"state-{doc_id}",
            "kb_id": "kb",
            "knowledge_graph_kwd": "merge_state",
            "source_id": [doc_id],
            "content_with_weight": json.dumps({"state": "merged", "updated_at": 1}),
        }
    store.refresh_idx("idx")


@pytest.fixture
def runtime(monkeypatch):
    store = RefreshStore()
    monkeypatch.setattr(settings, "docStoreConn", store)
    monkeypatch.setattr(settings, "retriever", SimpleNamespace(search=store.retrieve))

    async def inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    for module in (utils, utils_extras, utils_pagination, index, index_extras):
        monkeypatch.setattr(module, "thread_pool_exec", inline)
    monkeypatch.setattr(utils, "get_uuid", lambda: uuid4().hex)
    monkeypatch.setattr(utils_extras, "get_uuid", lambda: uuid4().hex)
    monkeypatch.setattr(utils, "get_embed_cache", lambda *args: None)
    monkeypatch.setattr(utils, "set_embed_cache", lambda *args: None)
    monkeypatch.setattr(utils.rag_tokenizer, "tokenize", lambda text: text)
    monkeypatch.setattr(utils.rag_tokenizer, "fine_grained_tokenize", lambda text: text)
    for flag, value in {
        "USE_INCREMENTAL_GRAPH": True,
        "USE_INCREMENTAL_MERGE": True,
        "USE_INCREMENTAL_RESOLUTION": True,
        "USE_ASYNC_COMMUNITY": False,
        "USE_ASYNC_KG_PHASES": False,
        "GRAPHRAG_ASYNC_PROGRESS": False,
        "KEEP_SUBGRAPH": True,
        "KEEP_MERGE": True,
        "KEEP_RESOLUTION": True,
        "RECALC_GLOBAL_PAGERANK_AFTER_MERGE": False,
        "GRAPHRAG_MERGE_STATE_MARK_MERGING": False,
        "SET_GRAPH_DELTA_REFRESH_AFTER_INSERT": False,
    }.items():
        monkeypatch.setattr(GraphRAGConfig, flag, value)

    phases = set()
    for module in (index_extras, task_executor_extras):
        monkeypatch.setattr(module, "has_phase_marker", lambda kb, phase: phase in phases)
        monkeypatch.setattr(module, "set_phase_marker", lambda kb, phase: phases.add(phase))
        monkeypatch.setattr(module, "clear_phase_markers", lambda *args: phases.clear())
        monkeypatch.setattr(
            module,
            "RedisDistributedLock",
            lambda *args, **kwargs: SimpleNamespace(
                acquire=lambda: True,
                spin_acquire=AsyncMock(),
                release=lambda: None,
            ),
        )
    monkeypatch.setattr(index_extras, "_record_lock_metric", AsyncMock())
    for module in (index, index_extras):
        monkeypatch.setattr(module, "load_checkpoints", AsyncMock(return_value={}))
        monkeypatch.setattr(module, "save_checkpoint", AsyncMock(return_value=True))
        monkeypatch.setattr(module, "cleanup_checkpoints", AsyncMock())
    # config normally selects this class at import time. Tests switch flags
    # after import, so install the same real class explicitly.
    monkeypatch.setattr(index_extras, "EntityResolution", IncrementalEntityResolution)
    monkeypatch.setattr(index_extras, "resolve_entities", index_patch._wrap_resolve_entities)
    monkeypatch.setattr(index_extras, "merge_subgraph", index_patch._wrap_merge_subgraph)
    monkeypatch.setattr(index_extras, "extract_community", index_patch._wrap_extract_community)
    monkeypatch.setattr(entity_resolution, "has_canceled", lambda task: False)
    from rag.graphrag import entity_resolution_extras

    monkeypatch.setattr(entity_resolution_extras, "is_english", lambda text: False)

    # Deterministic LLM answers; candidate recall and node merging stay real.
    llm_calls = []

    async def answer(self, *args, **kwargs):
        llm_calls.append(args)
        return "<|>1<|>&&yes&&"

    async def summarize(self, name, description, task_id=""):
        return description

    monkeypatch.setattr(IncrementalEntityResolution, "_async_chat", answer)
    monkeypatch.setattr(Extractor, "_handle_entity_relation_summary", summarize)
    community_inputs = []

    async def community_response(self, graph, **kwargs):
        community_inputs.append(graph.copy())
        return SimpleNamespace(structured_output=[], output=[])

    monkeypatch.setattr(CommunityReportsExtractor, "__call__", community_response)
    model = SimpleNamespace(llm_name="kb-embedding", encode=lambda texts: (np.full((len(texts), 2), 1.0), 0))
    return SimpleNamespace(store=store, model=model, phases=phases, llm_calls=llm_calls, community_inputs=community_inputs)


async def run_kb(runtime, doc_ids, with_community=False, **graphrag_config):
    return await index_extras.run_graphrag_for_kb(
        {"id": "task", "tenant_id": "tenant", "kb_id": "kb"},
        doc_ids,
        "English",
        {"graphrag": {"retry_attempts": 1, **graphrag_config}},
        None,
        runtime.model,
        lambda **kwargs: None,
        with_resolution=True,
        with_community=with_community,
    )


@pytest.mark.parametrize("force_refresh", [False, True], ids=["default", "refresh-control"])
async def test_review_1_monolithic_merge_preserves_previous_document(runtime, monkeypatch, force_refresh):
    monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", False)
    monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", False)
    monkeypatch.setattr(GraphRAGConfig, "SET_GRAPH_DELTA_REFRESH_AFTER_INSERT", force_refresh)
    merge = index_patch._ORIGINALS["merge_subgraph"]
    await merge("tenant", "kb", "doc1", make_graph("doc1", "Alice", "Bob"), runtime.model, lambda **kwargs: None)
    result = await merge("tenant", "kb", "doc2", make_graph("doc2", "Carol", "Dave"), runtime.model, lambda **kwargs: None)
    assert set(result.nodes) == {"Alice", "Bob", "Carol", "Dave"}
    assert set(result.graph["source_id"]) == {"doc1", "doc2"}


@pytest.mark.parametrize("community,async_community", [(False, False), (True, False), (True, True)], ids=["no-community", "synchronous-community-control", "indexed-community"])
async def test_review_2_resume_runs_pending_resolution(runtime, monkeypatch, community, async_community):
    monkeypatch.setattr(GraphRAGConfig, "USE_ASYNC_COMMUNITY", async_community)
    graph = make_graph("doc1", "Alice", "Bob")
    seed_graph(runtime.store, graph)
    seed_checkpoint(runtime.store, "doc1", graph, merged=True)
    result = await run_kb(runtime, ["doc1"], with_community=community)
    assert result["ok_docs"] == ["doc1"]
    assert index_extras.PHASE_RESOLUTION in runtime.phases


def prepare_consumer(runtime, monkeypatch, *, community=False, default_model=None):
    monkeypatch.setattr(GraphRAGConfig, "USE_ASYNC_KG_PHASES", True)
    # The names pass the real character-similarity filter; the model approves
    # their merge. The surviving entity is re-embedded by the real delta writer.
    seed_graph(runtime.store, make_graph("doc1", "中国科学院", "中国科学院院"))
    stop = asyncio.Event()
    worker = SimpleNamespace(stop_event=stop, CONSUMER_NAME="review")
    message = SimpleNamespace(
        get_message=lambda: {
            "tenant_id": "tenant",
            "kb_id": "kb",
            "task_id": "task",
            "with_resolution": True,
            "with_community": community,
            "kb_task_llm_id": "chat",
        },
        ack=MagicMock(side_effect=stop.set),
    )
    monkeypatch.setattr(task_executor_extras, "_get_task_executor_module", lambda: worker)
    monkeypatch.setattr(task_executor_extras, "REDIS_CONN", SimpleNamespace(queue_consumer=lambda *args: message))
    monkeypatch.setattr(task_executor_extras, "kg_limiter", asyncio.Semaphore(1))
    detail = MagicMock(return_value={"embd_id": "kb-embedding", "parser_config": {}})
    monkeypatch.setattr(task_executor_extras, "KnowledgebaseService", SimpleNamespace(get_detail=detail))
    default = default_model or SimpleNamespace(llm_name="tenant-embedding", encode=lambda texts: (np.full((len(texts), 2), 9.0), 0))
    get_default = MagicMock(return_value=default)
    get_provider = MagicMock(side_effect=lambda tenant, kind, model_id: runtime.model if model_id == "kb-embedding" else None)
    monkeypatch.setattr(task_executor_extras, "get_tenant_default_model_by_type", get_default)
    monkeypatch.setattr(task_executor_extras, "get_model_config_from_provider_instance", get_provider)
    bundle = MagicMock(side_effect=lambda tenant, config, **kwargs: config)
    monkeypatch.setattr(task_executor_extras, "LLMBundle", bundle)
    return SimpleNamespace(message=message, detail=detail, get_default=get_default, get_provider=get_provider, bundle=bundle)


@pytest.mark.parametrize("use_kb_default", [False, True], ids=["different-models", "same-model-control"])
async def test_review_3_postprocess_preserves_kb_embedding_space(runtime, monkeypatch, use_kb_default):
    consumer = prepare_consumer(
        runtime,
        monkeypatch,
        default_model=runtime.model if use_kb_default else None,
    )
    await asyncio.wait_for(task_executor_extras.kg_postprocess_consumer(), timeout=5)
    consumer.message.ack.assert_called_once()
    assert runtime.llm_calls, "the real resolver must reach the LLM boundary"
    assert index_extras.PHASE_RESOLUTION in runtime.phases, "consumer must finish, not silently acknowledge an error"
    entities = [row for row in runtime.store.rows.values() if row["knowledge_graph_kwd"] == "entity"]
    assert len(entities) == 1, "the real resolver must persist the merge"
    assert list(entities[0]["q_2_vec"]) == [1.0, 1.0], "postprocess wrote vectors from the tenant default instead of the KB model"


@pytest.mark.parametrize("async_community", [False, True], ids=["synchronous", "indexed-community-control"])
async def test_review_4_community_uses_resolved_graph(runtime, monkeypatch, async_community):
    monkeypatch.setattr(GraphRAGConfig, "USE_ASYNC_COMMUNITY", async_community)
    seed_graph(runtime.store, make_graph("doc1", "中国科学院"))
    seed_checkpoint(runtime.store, "doc2", make_graph("doc2", "中国科学院院"))
    await run_kb(runtime, ["doc2"], with_community=True)
    persisted = await utils_extras.get_graph_from_index("tenant", "kb")
    assert runtime.llm_calls
    assert len(persisted) == 1, "the actual incremental resolver must remove one alias"
    assert len(runtime.community_inputs) == 1
    assert set(runtime.community_inputs[0]) == set(persisted), "community extraction received an entity already removed by resolution"


@pytest.mark.parametrize("truncated", ["entity", "relation", None], ids=["entities", "relations", "complete-control"])
async def test_review_5_full_graph_loader_rejects_truncation(runtime, truncated):
    seed_graph(runtime.store, make_graph("doc1", "Alice", "Bob", "Carol"))
    runtime.store.truncate_kind = truncated
    if truncated:
        with pytest.raises(RuntimeError, match="(?i)truncat|incomplete|cap"):
            await utils_extras.get_graph_from_index("tenant", "kb")
    else:
        graph = await utils_extras.get_graph_from_index("tenant", "kb")
        assert set(graph) == {"Alice", "Bob", "Carol"}
        assert len(graph.edges) == 2


@pytest.mark.parametrize("raises", [False, True], ids=["refresh-false", "refresh-exception"])
async def test_monolithic_refresh_failure_aborts_write(runtime, monkeypatch, raises):
    monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", False)
    refresh = runtime.store.refresh_idx
    calls = 0

    def fail_after_insert(index_name):
        nonlocal calls
        calls += 1
        if calls == 2:  # delete_by_query refreshes before the bulk insertion.
            if raises:
                raise RuntimeError("refresh unavailable")
            return False
        return refresh(index_name)

    monkeypatch.setattr(runtime.store, "refresh_idx", fail_after_insert)
    graph = make_graph("doc1", "Alice", "Bob")
    change = utils.GraphChange(added_updated_nodes=set(graph), added_updated_edges=set(graph.edges))
    with pytest.raises(RuntimeError, match="refresh"):
        await utils_extras.set_graph("tenant", "kb", runtime.model, graph, change, None)
    assert calls == 2
    assert runtime.store.rows, "failure must be the post-insert refresh"
    assert not runtime.store.visible


async def test_forced_refresh_keeps_backend_without_refresh_compatible(monkeypatch):
    monkeypatch.setattr(settings, "docStoreConn", SimpleNamespace())
    await utils_extras.refresh_graphrag_index("tenant")


@pytest.mark.parametrize("embedding_id", [None, "", "kb-embedding"])
async def test_postprocess_embedding_selection_and_dimension(runtime, monkeypatch, embedding_id):
    default = SimpleNamespace(llm_name="tenant-embedding", encode=lambda texts: (np.full((len(texts), 3), 9.0), 0))
    consumer = prepare_consumer(runtime, monkeypatch, default_model=default)
    consumer.detail.return_value["embd_id"] = embedding_id
    await asyncio.wait_for(task_executor_extras.kg_postprocess_consumer(), timeout=5)
    consumer.message.ack.assert_called_once()
    assert index_extras.PHASE_RESOLUTION in runtime.phases
    entities = [row for row in runtime.store.rows.values() if row["knowledge_graph_kwd"] == "entity"]
    assert len(entities) == 1
    vector_fields = {key for key in entities[0] if key.startswith("q_") and key.endswith("_vec")}
    if embedding_id:
        consumer.get_default.assert_not_called()
        assert vector_fields == {"q_2_vec"}
    else:
        consumer.get_default.assert_called_once()
        assert vector_fields == {"q_3_vec"}


@pytest.mark.parametrize("failure", ["missing-kb", "kb-error", "missing-model", "model-error", "bundle-error"])
async def test_postprocess_model_failure_never_uses_default(runtime, monkeypatch, failure):
    consumer = prepare_consumer(runtime, monkeypatch)
    if failure == "missing-kb":
        consumer.detail.return_value = None
    elif failure == "kb-error":
        consumer.detail.side_effect = RuntimeError("KB unavailable")
    elif failure == "missing-model":
        consumer.get_provider.side_effect = None
        consumer.get_provider.return_value = None
    elif failure == "model-error":
        consumer.get_provider.side_effect = RuntimeError("Model unavailable")
    else:
        consumer.bundle.side_effect = RuntimeError("Cannot bind model")
    before = deepcopy(runtime.store.rows)
    await asyncio.wait_for(task_executor_extras.kg_postprocess_consumer(), timeout=5)
    consumer.message.ack.assert_called_once()
    consumer.get_default.assert_not_called()
    assert runtime.store.rows == before
    assert not runtime.phases
    assert not runtime.llm_calls


@pytest.mark.parametrize("async_community", [False, True])
async def test_postprocess_community_reloads_after_resolution_under_lock(runtime, monkeypatch, async_community):
    consumer = prepare_consumer(runtime, monkeypatch, community=True)
    monkeypatch.setattr(GraphRAGConfig, "USE_ASYNC_COMMUNITY", async_community)
    # A multi-document snapshot used to be mistaken for a fresh complete graph.
    for row in runtime.store.rows.values():
        meta = json.loads(row["content_with_weight"])
        meta["source_id"] = ["doc1", "doc2"]
        row.update(source_id=meta["source_id"], content_with_weight=json.dumps(meta))
    runtime.store.refresh_idx("idx")
    locked = False

    async def acquire(**kwargs):
        nonlocal locked
        locked = True

    def release():
        nonlocal locked
        locked = False

    monkeypatch.setattr(task_executor_extras, "RedisDistributedLock", lambda *args, **kwargs: SimpleNamespace(spin_acquire=acquire, release=release))
    scroll = runtime.store.search_with_scroll

    def locked_scroll(*args, **kwargs):
        assert locked, "graph reads must happen after acquiring the KB lock"
        return scroll(*args, **kwargs)

    monkeypatch.setattr(runtime.store, "search_with_scroll", locked_scroll)
    await asyncio.wait_for(task_executor_extras.kg_postprocess_consumer(), timeout=5)
    consumer.message.ack.assert_called_once()
    assert not locked
    assert index_extras.PHASE_COMMUNITY in runtime.phases
    persisted = {row["entity_kwd"] for row in runtime.store.rows.values() if row["knowledge_graph_kwd"] == "entity"}
    assert len(persisted) == 1
    assert len(runtime.community_inputs) == 1
    assert set(runtime.community_inputs[0]) == persisted


@pytest.mark.parametrize("truncated", ["entity", "relation"])
async def test_truncation_propagates_without_legacy_fallback(runtime, monkeypatch, truncated):
    seed_graph(runtime.store, make_graph("doc1", "Alice", "Bob", "Carol"))
    runtime.store.truncate_kind = truncated
    legacy = AsyncMock(return_value=make_graph("old", "Outdated"))
    monkeypatch.setattr(utils, "get_graph_from_json", legacy)
    with pytest.raises(utils_extras.GraphTopologyTruncatedError, match=f"{truncated}.*kb=kb"):
        await utils_extras.get_graph("tenant", "kb")
    legacy.assert_not_called()


@pytest.mark.parametrize("async_community", [False, True])
@pytest.mark.parametrize("truncated", ["entity", "relation"])
async def test_truncated_community_does_not_mark_complete(runtime, monkeypatch, async_community, truncated):
    monkeypatch.setattr(GraphRAGConfig, "USE_ASYNC_COMMUNITY", async_community)
    graph = make_graph("doc1", "Alice", "Bob", "Carol")
    seed_graph(runtime.store, graph)
    seed_checkpoint(runtime.store, "doc1", graph, merged=True)
    runtime.phases.add(index_extras.PHASE_RESOLUTION)
    runtime.store.truncate_kind = truncated
    before = deepcopy(runtime.store.rows)
    with pytest.raises(utils_extras.GraphTopologyTruncatedError):
        await run_kb(runtime, ["doc1"], with_community=True)
    assert index_extras.PHASE_COMMUNITY not in runtime.phases
    assert not runtime.community_inputs
    assert runtime.store.rows == before


@pytest.mark.parametrize("truncated", ["entity", "relation"])
async def test_postprocess_truncation_does_not_mark_complete(runtime, monkeypatch, truncated):
    consumer = prepare_consumer(runtime, monkeypatch, community=True)
    runtime.store.truncate_kind = truncated
    before = deepcopy(runtime.store.rows)
    await asyncio.wait_for(task_executor_extras.kg_postprocess_consumer(), timeout=5)
    consumer.message.ack.assert_called_once()
    assert not runtime.phases
    assert runtime.store.rows == before


@pytest.mark.parametrize("async_community", [False, True])
async def test_community_retry_reads_new_snapshot(runtime, monkeypatch, async_community):
    monkeypatch.setattr(GraphRAGConfig, "USE_ASYNC_COMMUNITY", async_community)
    graph = make_graph("doc1", "Alice", "Bob")
    seed_graph(runtime.store, graph)
    seed_checkpoint(runtime.store, "doc1", graph, merged=True)
    runtime.phases.add(index_extras.PHASE_RESOLUTION)
    inputs = []

    async def response(self, graph, **kwargs):
        inputs.append(set(graph))
        if len(inputs) == 1:
            seed_graph(runtime.store, make_graph("doc2", "Carol"))
            raise RuntimeError("transient community failure")
        return SimpleNamespace(structured_output=[], output=[])

    monkeypatch.setattr(CommunityReportsExtractor, "__call__", response)
    await run_kb(runtime, ["doc1"], with_community=True, community_retry_attempts=2, retry_backoff_seconds=0)
    assert inputs == [{"Alice", "Bob"}, {"Alice", "Bob", "Carol"}]
    assert index_extras.PHASE_COMMUNITY in runtime.phases


@pytest.mark.parametrize("phase", ["resolution", "community", "indexed-community"])
async def test_missing_persisted_graph_does_not_mark_complete(runtime, monkeypatch, phase):
    graph = make_graph("doc1", "Alice", "Bob")
    seed_checkpoint(runtime.store, "doc1", graph, merged=True)
    if phase == "resolution":
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_RESOLUTION", False)
    else:
        runtime.phases.add(index_extras.PHASE_RESOLUTION)
    monkeypatch.setattr(GraphRAGConfig, "USE_ASYNC_COMMUNITY", phase == "indexed-community")
    before = set(runtime.phases)
    with pytest.raises(RuntimeError, match="No persisted graph"):
        await run_kb(runtime, ["doc1"], with_community=phase != "resolution")
    assert runtime.phases == before


@pytest.mark.parametrize("incremental", [False, True])
async def test_indexed_community_preserves_monolithic_backend_fallback(runtime, monkeypatch, incremental):
    monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", incremental)
    monkeypatch.delattr(RefreshStore, "search_with_scroll")
    graph = make_graph("doc1", "Alice", "Bob")
    runtime.store.rows["full-graph"] = {
        "id": "full-graph",
        "kb_id": "kb",
        "knowledge_graph_kwd": "graph",
        "removed_kwd": "N",
        "source_id": ["doc1"],
        "content_with_weight": json.dumps(nx.node_link_data(graph, edges="edges")),
    }
    runtime.store.refresh_idx("idx")
    await index_extras.extract_community_async(None, "tenant", "kb", None, None, runtime.model, lambda **kwargs: None)
    assert len(runtime.community_inputs) == 1
    assert set(runtime.community_inputs[0]) == {"Alice", "Bob"}


@pytest.mark.parametrize("entity_type", ["Book", "Chapter", "书籍", "章节"])
async def test_v0264_full_writer_skips_structural_embedding(runtime, monkeypatch, entity_type):
    graph = make_graph("doc1", "STRUCTURE")
    graph.nodes["STRUCTURE"]["entity_type"] = entity_type
    monkeypatch.setattr(utils.REDIS_CONN, "mget", lambda keys: [None] * len(keys))
    encode = MagicMock(side_effect=runtime.model.encode)
    runtime.model.encode = encode
    change = utils.GraphChange(added_updated_nodes={"STRUCTURE"})

    await utils._set_graph_impl("tenant", "kb", runtime.model, graph, change, lambda **kwargs: None)

    encode.assert_not_called()
    entities = [row for row in runtime.store.rows.values() if row["knowledge_graph_kwd"] == "entity"]
    assert len(entities) == 1
    assert not any(key.endswith("_vec") for key in entities[0])


async def test_v0264_full_writer_batches_only_semantic_nodes_and_edges(runtime, monkeypatch):
    graph = make_graph("doc1", "BOOK", "CHAPTER", "ALICE", "BOB")
    graph.nodes["BOOK"]["entity_type"] = "Book"
    graph.nodes["CHAPTER"]["entity_type"] = "Chapter"
    cache = {}
    monkeypatch.setattr(utils.REDIS_CONN, "mget", lambda keys: [None] * len(keys))
    monkeypatch.setattr(utils, "get_embed_cache", lambda model, key: cache.get(key))
    monkeypatch.setattr(utils, "set_embed_cache", lambda model, key, value: cache.__setitem__(key, value))
    encode = MagicMock(side_effect=runtime.model.encode)
    runtime.model.encode = encode
    change = utils.GraphChange(added_updated_nodes=set(graph.nodes), added_updated_edges=set(graph.edges))

    await utils._set_graph_impl("tenant", "kb", runtime.model, graph, change, lambda **kwargs: None)

    assert encode.call_count == 2
    assert set(encode.call_args_list[0].args[0]) == {"ALICE", "BOB"}
    assert encode.call_args_list[1].args[0] == ["ALICE->BOB: related"]
    for row in runtime.store.rows.values():
        if row["knowledge_graph_kwd"] not in {"entity", "relation"}:
            continue
        structural = row.get("entity_kwd") in {"BOOK", "CHAPTER"} or row.get("from_entity_kwd") in {"BOOK", "CHAPTER"}
        assert any(key.endswith("_vec") for key in row) is (not structural)


@pytest.mark.parametrize(
    ("description", "keep"),
    [
        ("There is no clear relationship between these entities.", False),
        ("Charlie works for Acme.", False),
        ("Alice works with Bob.", True),
        ("related", True),
    ],
)
async def test_v0264_custom_extractor_applies_relationship_quality(runtime, monkeypatch, description, keep):
    monkeypatch.setattr(GraphRAGConfig, "USE_CHAPTER_GRAPH", False)
    monkeypatch.setattr(index_extras, "does_graph_contains", AsyncMock(return_value=False))
    monkeypatch.setattr(index_extras, "write_merge_state", AsyncMock())
    relation = {"src_id": "ALICE", "tgt_id": "BOB", "description": description, "weight": 1, "keywords": ["related"]}
    assert index._relationship_looks_valid(relation) is keep

    class ExtractorDouble:
        def __init__(self, *args, **kwargs):
            pass

        async def __call__(self, *args, **kwargs):
            return [{"entity_name": name, "entity_type": "person", "description": name} for name in ("ALICE", "BOB")], [relation]

    graph = await index_extras.generate_subgraph(
        ExtractorDouble, "tenant", "kb", "doc1", ["text"], "English", ["person"], None, runtime.model, lambda **kwargs: None
    )

    assert graph.has_edge("ALICE", "BOB") is keep
    assert set(graph.nodes) == {"ALICE", "BOB"}


async def test_v0264_generated_structure_links_bypass_llm_filter(runtime, monkeypatch):
    monkeypatch.setattr(GraphRAGConfig, "USE_CHAPTER_GRAPH", True)
    monkeypatch.setattr(index_extras, "does_graph_contains", AsyncMock(return_value=False))
    monkeypatch.setattr(index_extras, "write_merge_state", AsyncMock())
    # The text deliberately fails the LLM quality check; generated links must
    # still survive because their provenance, rather than endpoint type, matters.
    def link(left, right):
        return {"src_id": left, "tgt_id": right, "description": "no clear relationship", "weight": 1, "keywords": ["contains"]}

    monkeypatch.setattr(index_extras, "_extract_book_and_chapters", lambda *args: (
        "BOOK",
        [{"entity_name": name, "entity_type": kind, "description": name} for name, kind in [("BOOK", "书籍"), ("CHAPTER", "章节")]],
        [link("BOOK", "CHAPTER")],
        [["CHAPTER"]],
    ))
    monkeypatch.setattr(index_extras, "_link_entities_to_chapters", lambda *args: [link("ALICE", "CHAPTER")])

    class ExtractorDouble:
        def __init__(self, *args, **kwargs):
            pass

        async def __call__(self, *args, **kwargs):
            return [{"entity_name": name, "entity_type": "person", "description": name} for name in ("ALICE", "BOB")], [link("ALICE", "BOB")]

    graph = await index_extras.generate_subgraph(
        ExtractorDouble, "tenant", "kb", "doc1", ["text"], "English", ["person"], None, runtime.model, lambda **kwargs: None
    )

    assert graph.has_edge("BOOK", "CHAPTER")
    assert graph.has_edge("ALICE", "CHAPTER")
    assert not graph.has_edge("ALICE", "BOB")
