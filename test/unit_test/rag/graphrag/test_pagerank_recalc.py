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
  - topology truncation (entity OR relation scroll over hits_cap) aborts
    before any write
  - entities missing from the pagerank map are skipped, never zeroed
  - _final_graph_is_delta guards official phases from per-doc delta graphs

The entity write-back scan itself streams via ``search_after`` and is no
longer bounded by ``hits_cap``; the topology load enforces the cap (covered
by test_graphrag_topology.py).
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
from rag.graphrag.utils_extras import GraphTopologyTruncatedError


async def _run_sync(func, *args, **kwargs):
    """Drop-in for the real thread_pool_exec: run inline in the test loop."""
    return func(*args, **kwargs)


class _FakeDocStore:
    """Minimal doc-store double: capability attrs + partial update + refresh."""

    def __init__(self):
        self.es = MagicMock()  # capability marker for search_after support
        self.update_calls = []
        self.refreshed = False

    def search_with_scroll(self, index_name, query_body, fields, hits_cap=None):
        # Capability marker only; the recalc path no longer scans with scroll.
        raise AssertionError("search_with_scroll should not be called by recalc")

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
    """Alpha-Beta-Gamma chain; the structural node is filtered at load time."""
    g = nx.Graph()
    g.add_node("Alpha")
    g.add_node("Beta")
    g.add_node("Gamma")
    g.add_edge("Alpha", "Beta")
    g.add_edge("Beta", "Gamma")
    return g


def _hits():
    return [
        _entity_hit("c1", "Alpha", "organization"),
        _entity_hit("c2", "Beta", "organization"),
        _entity_hit("c3", "Gamma", "person"),
        _entity_hit("c4", "Book X", "书籍"),
    ]


def _setup(monkeypatch, hits, graph, topology_error=None):
    """Wire the fake doc store, topology loader and search_after into index_extras."""
    msgs = []
    store = _FakeDocStore()
    monkeypatch.setattr(settings, "docStoreConn", store)
    monkeypatch.setattr(index_extras, "thread_pool_exec", _run_sync)

    async def _fake_topology(tenant_id, kb_id, skip_entity_type=None, hits_cap=None):
        if topology_error is not None:
            raise topology_error
        return graph

    monkeypatch.setattr(index_extras, "get_graph_topology_from_index", _fake_topology)

    sa_calls = []

    def _fake_search_after(filters, index_name, kb_id, fields, sort_field, page_size=1000, max_pages=1000):
        sa_calls.append({"filters": filters, "fields": fields, "sort_field": sort_field})
        page = [dict(h["_source"], id=h["_id"]) for h in hits]

        async def _gen():
            yield page

        return _gen()

    monkeypatch.setattr(index_extras, "search_all_by_search_after", _fake_search_after)

    def _callback(msg=None, **kwargs):
        if msg:
            msgs.append(msg)

    return store, sa_calls, msgs, _callback


def _collected_updates(store):
    return {cid: fields for call in store.update_calls for cid, fields in call}


class TestRecalcGlobalPagerank:
    @pytest.mark.asyncio
    async def test_writes_partial_updates_with_global_pagerank(self, monkeypatch):
        store, sa_calls, msgs, callback = _setup(monkeypatch, _hits(), _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        updates = _collected_updates(store)
        assert set(updates) == {"c1", "c2", "c3", "c4"}

        expected = nx.pagerank(_graph())

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

        # The scan must use the narrow field projection (no vector fields).
        assert sa_calls[0]["fields"] == ["entity_kwd", "content_with_weight"]
        assert store.refreshed is True
        assert any("updated 4 entities, skipped 0" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_aborts_when_topology_is_truncated(self, monkeypatch):
        # Entity or relation scroll cut off by hits_cap → no scan, no writes.
        store, sa_calls, msgs, callback = _setup(
            monkeypatch,
            _hits(),
            None,
            topology_error=GraphTopologyTruncatedError("relation scroll hit the hits_cap=50000"),
        )

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        assert sa_calls == []
        assert store.update_calls == []
        assert any("abort pagerank recalc" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_entity_missing_from_pagerank_is_skipped_not_zeroed(self, monkeypatch):
        hits = _hits() + [_entity_hit("c5", "Ghost", "organization")]
        store, sa_calls, msgs, callback = _setup(monkeypatch, hits, _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        updates = _collected_updates(store)
        assert "c5" not in updates
        assert set(updates) == {"c1", "c2", "c3", "c4"}
        assert any("skipped 1" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_skips_when_backend_lacks_update_docs(self, monkeypatch):
        store, sa_calls, msgs, callback = _setup(monkeypatch, _hits(), _graph())
        monkeypatch.delattr(_FakeDocStore, "update_docs")

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        assert sa_calls == []
        assert any("update_docs" in m for m in msgs)


class TestFinalGraphIsDelta:
    def test_incremental_merge_marks_graph_as_delta(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", True)
        assert index_extras._final_graph_is_delta(nx.Graph()) is True
        assert index_extras._final_graph_is_delta(None) is False

    def test_official_mode_never_delta(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", False)
        assert index_extras._final_graph_is_delta(nx.Graph()) is False
