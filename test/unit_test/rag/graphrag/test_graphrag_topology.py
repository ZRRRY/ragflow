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

"""Unit tests for utils_extras.get_graph_topology_from_index."""

import pytest

from rag.graphrag import utils_extras
from rag.graphrag.utils_extras import (
    GraphTopologyTruncatedError,
    get_graph_topology_from_index,
)


class _FakeConn:
    """Minimal doc-store double with scroll support.

    ``entities`` / ``relations`` are lists of _source dicts. ``truncated``
    selects which scroll kinds report ``_truncated=True``.
    """

    def __init__(self, entities, relations, truncated=()):
        self._entities = entities
        self._relations = relations
        self._truncated = set(truncated)

    def search_with_scroll(self, index_name, query_body, fields, hits_cap=None):
        kwd = query_body["query"]["bool"]["filter"][1]["terms"]["knowledge_graph_kwd"][0]
        rows = self._entities if kwd == "entity" else self._relations
        hits = [{"_id": f"{kwd}-{i}", "_source": row} for i, row in enumerate(rows)]
        return {"hits": {"hits": hits}, "_truncated": kwd in self._truncated}

    def get_fields(self, res, fields):
        out = {}
        for hit in res["hits"]["hits"]:
            out[hit["_id"]] = {f: hit["_source"][f] for f in fields if f in hit["_source"]}
        return out


class _NoScrollConn:
    pass


def _entity(name, ent_type="organization"):
    return {"entity_kwd": name, "entity_type_kwd": ent_type}


def _relation(src, tgt):
    return {"from_entity_kwd": src, "to_entity_kwd": tgt}


@pytest.fixture
def conn(monkeypatch):
    conn = _FakeConn(
        entities=[
            _entity("苹果"),
            _entity("苹果公司"),
            _entity("《三体》", "书籍"),
            _entity("第三章", "章节"),
            _entity(["list-valued"], "organization"),
        ],
        relations=[
            _relation("苹果", "苹果公司"),
            _relation("第三章", "苹果"),  # touches an excluded node
            _relation("《三体》", "第三章"),  # both endpoints excluded
            _relation("苹果", "苹果"),  # self-loop
            _relation("苹果", ""),  # missing endpoint
        ],
    )
    monkeypatch.setattr(utils_extras.settings, "docStoreConn", conn)
    return conn


@pytest.mark.asyncio
async def test_topology_excludes_structural_nodes_and_their_edges(conn):
    graph = await get_graph_topology_from_index(
        "tenant1", "kb1", skip_entity_type=lambda t: t in {"书籍", "章节"}
    )
    assert set(graph.nodes) == {"苹果", "苹果公司", "list-valued"}
    assert set(graph.edges) == {tuple(sorted(("苹果", "苹果公司")))}


@pytest.mark.asyncio
async def test_topology_without_exclusion_keeps_everything(conn):
    graph = await get_graph_topology_from_index("tenant1", "kb1")
    assert "第三章" in graph.nodes
    assert ("第三章", "苹果") in graph.edges or ("苹果", "第三章") in graph.edges


@pytest.mark.asyncio
async def test_entity_truncation_raises(monkeypatch):
    conn = _FakeConn(entities=[_entity("a")], relations=[], truncated={"entity"})
    monkeypatch.setattr(utils_extras.settings, "docStoreConn", conn)
    with pytest.raises(GraphTopologyTruncatedError):
        await get_graph_topology_from_index("tenant1", "kb1")


@pytest.mark.asyncio
async def test_relation_truncation_raises(monkeypatch):
    conn = _FakeConn(
        entities=[_entity("a"), _entity("b")],
        relations=[_relation("a", "b")],
        truncated={"relation"},
    )
    monkeypatch.setattr(utils_extras.settings, "docStoreConn", conn)
    with pytest.raises(GraphTopologyTruncatedError):
        await get_graph_topology_from_index("tenant1", "kb1")


@pytest.mark.asyncio
async def test_backend_without_scroll_returns_none(monkeypatch):
    monkeypatch.setattr(utils_extras.settings, "docStoreConn", _NoScrollConn())
    assert await get_graph_topology_from_index("tenant1", "kb1") is None


@pytest.mark.asyncio
async def test_empty_kb_returns_none(monkeypatch):
    conn = _FakeConn(entities=[], relations=[])
    monkeypatch.setattr(utils_extras.settings, "docStoreConn", conn)
    assert await get_graph_topology_from_index("tenant1", "kb1") is None
