#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""Unit tests for the Phase 2.5 global PageRank recalc path.

Covers ``recalc_global_pagerank`` (rag/graphrag/general/index_extras.py):
  - partial-update write-back (rank_flt + content_with_weight.pagerank)
  - structural (book/chapter) nodes pinned to 0.0
  - hits-cap truncation aborts before any write
  - entities missing from the pagerank map are skipped, never zeroed
  - _final_graph_is_delta guards official phases from per-doc delta graphs
"""

import json
import sys
from unittest.mock import MagicMock

import networkx as nx
import pytest

# Stub heavy third-party / service modules before importing index_extras
# (mirrors test_incremental_bugfixes.py).
for _m in [
    "editdistance",
    "spacy",
    "api.db.joint_services",
    "api.db.joint_services.tenant_model_service",
    "api.db.services.document_service",
    "common.misc_utils",
]:
    if _m not in sys.modules:
        sys.modules[_m] = MagicMock()

try:
    import rag.llm.siliconflow_timeout_patch  # noqa: F401
except Exception:
    for _m in ("rag.llm", "rag.llm.embedding_model", "rag.llm.siliconflow_timeout_patch"):
        if _m not in sys.modules:
            sys.modules[_m] = MagicMock()

from common import settings  # mocked by the shared conftest
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.general import index_extras


async def _run_sync(func, *args, **kwargs):
    """Drop-in for the real thread_pool_exec: run inline in the test loop."""
    return func(*args, **kwargs)


class _FakeDocStore:
    """Minimal doc-store double: scroll scan + partial update + refresh."""

    def __init__(self, hits):
        self._hits = hits
        self.scan_queries = []
        self.update_calls = []
        self.refreshed = False

    def search_with_scroll(self, index_name, query_body, fields, hits_cap=None):
        self.scan_queries.append(query_body)
        return {"hits": {"hits": list(self._hits)}}

    def update_docs(self, index_name, updates):
        self.update_calls.append(list(updates))
        return []

    def refresh_idx(self, index_name):
        self.refreshed = True


def _entity_hit(cid, name, entity_type, pagerank=0.001, description="d"):
    return {
        "_id": cid,
        "_source": {
            "entity_kwd": name,
            "content_with_weight": json.dumps(
                {"entity_type": entity_type, "pagerank": pagerank, "description": description},
                ensure_ascii=False,
            ),
        },
    }


def _graph():
    """Alpha-Beta-Gamma chain plus one structural (book) node linked to Alpha."""
    g = nx.Graph()
    g.add_node("Alpha", entity_type="organization", pagerank=0.001, source_id=["d1"])
    g.add_node("Beta", entity_type="organization", pagerank=0.001, source_id=["d1"])
    g.add_node("Gamma", entity_type="person", pagerank=0.001, source_id=["d1"])
    g.add_node("Book X", entity_type="书籍", pagerank=0.001, source_id=["d1"])
    g.add_edge("Alpha", "Beta", weight=1.0)
    g.add_edge("Beta", "Gamma", weight=1.0)
    g.add_edge("Book X", "Alpha", weight=1.0)
    return g


def _hits():
    return [
        _entity_hit("c1", "Alpha", "organization"),
        _entity_hit("c2", "Beta", "organization"),
        _entity_hit("c3", "Gamma", "person"),
        _entity_hit("c4", "Book X", "书籍"),
    ]


def _setup(monkeypatch, hits, graph):
    """Wire the fake doc store and graph loader into index_extras."""
    msgs = []
    store = _FakeDocStore(hits)
    monkeypatch.setattr(settings, "docStoreConn", store)
    monkeypatch.setattr(index_extras, "thread_pool_exec", _run_sync)

    async def _fake_loader(tenant_id, kb_id):
        return graph

    monkeypatch.setattr(index_extras, "get_graph_from_index", _fake_loader)

    def _callback(msg=None, **kwargs):
        if msg:
            msgs.append(msg)

    return store, msgs, _callback


def _collected_updates(store):
    return {cid: fields for call in store.update_calls for cid, fields in call}


class TestRecalcGlobalPagerank:
    @pytest.mark.asyncio
    async def test_writes_partial_updates_with_global_pagerank(self, monkeypatch):
        store, msgs, callback = _setup(monkeypatch, _hits(), _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        updates = _collected_updates(store)
        assert set(updates) == {"c1", "c2", "c3", "c4"}

        # Structural node is removed from the graph before PageRank runs.
        expected_graph = _graph()
        expected_graph.remove_node("Book X")
        expected = nx.pagerank(expected_graph)

        for cid, name in [("c1", "Alpha"), ("c2", "Beta"), ("c3", "Gamma")]:
            assert updates[cid]["rank_flt"] == pytest.approx(expected[name])
            meta = json.loads(updates[cid]["content_with_weight"])
            assert meta["pagerank"] == pytest.approx(expected[name])
            # untouched meta fields survive the partial update
            assert meta["description"] == "d"
            assert meta["entity_type"] in ("organization", "person")

        # Structural node is pinned to 0.0, not left at the 0.001 placeholder.
        assert updates["c4"]["rank_flt"] == 0.0
        assert json.loads(updates["c4"]["content_with_weight"])["pagerank"] == 0.0

        # The scan must use the narrow _source projection (no vector fields).
        assert store.scan_queries[0]["_source"] == ["entity_kwd", "content_with_weight"]
        assert store.refreshed is True
        assert any("updated 4 entities, skipped 0" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_aborts_when_entity_scan_reaches_hits_cap(self, monkeypatch):
        # Graph has 3 nodes after structural filtering (< cap=4), but the
        # entity scan returns 4 hits == cap → write-back must not happen.
        monkeypatch.setattr(GraphRAGConfig, "SEARCH_WITH_SCROLL_HITS_CAP", 4)
        store, msgs, callback = _setup(monkeypatch, _hits(), _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        assert store.update_calls == []
        assert any("hits cap" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_aborts_when_graph_load_reaches_hits_cap(self, monkeypatch):
        # cap=3 → filtered graph (3 nodes) reaches the cap → abort before scan.
        monkeypatch.setattr(GraphRAGConfig, "SEARCH_WITH_SCROLL_HITS_CAP", 3)
        store, msgs, callback = _setup(monkeypatch, _hits(), _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        assert store.scan_queries == []
        assert store.update_calls == []
        assert any("node cap" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_entity_missing_from_pagerank_is_skipped_not_zeroed(self, monkeypatch):
        hits = _hits() + [_entity_hit("c5", "Ghost", "organization")]
        store, msgs, callback = _setup(monkeypatch, hits, _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        updates = _collected_updates(store)
        assert "c5" not in updates
        assert set(updates) == {"c1", "c2", "c3", "c4"}
        assert any("skipped 1" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_skips_when_backend_lacks_update_docs(self, monkeypatch):
        store, msgs, callback = _setup(monkeypatch, _hits(), _graph())
        monkeypatch.delattr(_FakeDocStore, "update_docs")

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        assert store.scan_queries == []
        assert any("update_docs" in m for m in msgs)


class TestFinalGraphIsDelta:
    def test_incremental_merge_marks_graph_as_delta(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", True)
        assert index_extras._final_graph_is_delta(nx.Graph()) is True
        assert index_extras._final_graph_is_delta(None) is False

    def test_official_mode_never_delta(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", False)
        assert index_extras._final_graph_is_delta(nx.Graph()) is False
