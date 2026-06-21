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
"""Custom tests for graphrag utils extras: book/chapter embedding skip."""

from types import SimpleNamespace

import networkx as nx
import numpy as np
import pytest

import rag.graphrag.utils as graphrag_utils
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.utils import GraphChange


def _has_vector(chunk):
    return any(k.startswith("q_") and k.endswith("_vec") for k in chunk)


@pytest.fixture
def fake_embd(monkeypatch):
    # Skip the real encode/Redis path by returning a cached embedding.
    monkeypatch.setattr(graphrag_utils, "get_embed_cache", lambda *_a, **_k: np.array([0.1, 0.2, 0.3]))
    return graphrag_utils


class TestShouldSkipEmbedding:
    def test_book_and_chapter_types_skip_embedding(self):
        assert GraphRAGConfig.should_skip_embedding("书籍")
        assert GraphRAGConfig.should_skip_embedding("章节")
        assert GraphRAGConfig.should_skip_embedding("Book")
        assert GraphRAGConfig.should_skip_embedding("Chapter")
        assert GraphRAGConfig.should_skip_embedding("section")

    def test_other_types_do_not_skip_embedding(self):
        assert not GraphRAGConfig.should_skip_embedding("person")
        assert not GraphRAGConfig.should_skip_embedding("organization")
        assert not GraphRAGConfig.should_skip_embedding("")
        assert not GraphRAGConfig.should_skip_embedding(None)


class TestGraphNodeToChunkSkipsBookAndChapter:
    @pytest.mark.asyncio
    async def test_skips_embedding_for_book_and_chapter(self, fake_embd):
        for etype in ("书籍", "章节", "Book", "Chapter"):
            chunks = []
            meta = {"entity_type": etype, "description": "desc", "source_id": ["s1"]}
            await fake_embd.graph_node_to_chunk("kb1", SimpleNamespace(llm_name="m"), "X", meta, chunks)
            assert len(chunks) == 1
            assert not _has_vector(chunks[0])

    @pytest.mark.asyncio
    async def test_keeps_embedding_for_other_entities(self, fake_embd):
        chunks = []
        meta = {"entity_type": "person", "description": "desc", "source_id": ["s1"]}
        await fake_embd.graph_node_to_chunk("kb1", SimpleNamespace(llm_name="m"), "Alice", meta, chunks)
        assert _has_vector(chunks[0])


class TestGraphEdgeToChunkSkipsBookAndChapter:
    @pytest.mark.asyncio
    async def test_skips_embedding_for_book_and_chapter_edges(self, fake_embd):
        for from_type, to_type in (("书籍", "章节"), ("章节", "person"), ("person", "章节")):
            chunks = []
            meta = {
                "description": "desc",
                "keywords": ["k"],
                "source_id": ["s1"],
                "weight": 1,
            }
            skip = (
                GraphRAGConfig.should_skip_embedding(from_type)
                or GraphRAGConfig.should_skip_embedding(to_type)
            )
            await fake_embd.graph_edge_to_chunk(
                "kb1", SimpleNamespace(llm_name="m"), "A", "B", meta, chunks, skip_embedding=skip
            )
            assert len(chunks) == 1
            assert not _has_vector(chunks[0])

    @pytest.mark.asyncio
    async def test_keeps_embedding_for_other_edges(self, fake_embd):
        chunks = []
        meta = {
            "description": "desc",
            "keywords": ["k"],
            "source_id": ["s1"],
            "weight": 1,
        }
        await fake_embd.graph_edge_to_chunk(
            "kb1", SimpleNamespace(llm_name="m"), "A", "B", meta, chunks, skip_embedding=False
        )
        assert _has_vector(chunks[0])


class TestBatchEmbedNodesSkipsBookAndChapter:
    @pytest.mark.asyncio
    async def test_batch_embed_nodes_skips_book_and_chapter(self, monkeypatch):
        from rag.graphrag import utils_extras

        called_items = []

        async def fake_batch_embed_items(kb_id, embd_mdl, items, chunks, callback, label):
            called_items.extend(items)

        monkeypatch.setattr(utils_extras, "_batch_embed_items", fake_batch_embed_items)

        graph = nx.Graph()
        graph.add_node(
            "《X》Ch1",
            entity_type="章节",
            description="chapter desc",
            source_id=["d1"],
        )
        graph.add_node(
            "Alice",
            entity_type="person",
            description="person desc",
            source_id=["d1"],
        )
        change = GraphChange(added_updated_nodes={"《X》Ch1", "Alice"})
        chunks = []
        embd = SimpleNamespace(llm_name="m")

        await utils_extras._batch_embed_nodes("kb1", embd, graph, change, chunks)

        assert len(chunks) == 1
        assert chunks[0]["entity_kwd"] == "《X》Ch1"
        assert chunks[0]["entity_type_kwd"] == "章节"
        assert not _has_vector(chunks[0])
        assert len(called_items) == 1
        assert called_items[0][0]["entity_kwd"] == "Alice"


class TestBatchEmbedEdgesSkipsBookAndChapter:
    @pytest.mark.asyncio
    async def test_batch_embed_edges_skips_chapter_related_edges(self, monkeypatch):
        from rag.graphrag import utils_extras

        called_items = []

        async def fake_batch_embed_items(kb_id, embd_mdl, items, chunks, callback, label):
            called_items.extend(items)

        monkeypatch.setattr(utils_extras, "_batch_embed_items", fake_batch_embed_items)

        graph = nx.Graph()
        graph.add_node("Book", entity_type="书籍")
        graph.add_node("《Book》Ch1", entity_type="章节")
        graph.add_node("Alice", entity_type="person")
        graph.add_node("Bob", entity_type="person")
        graph.add_edge("Book", "《Book》Ch1", description="contains", keywords=["c"], source_id=["d1"], weight=1)
        graph.add_edge("《Book》Ch1", "Alice", description="involves", keywords=["i"], source_id=["d1"], weight=1)
        graph.add_edge("Alice", "Bob", description="knows", keywords=["k"], source_id=["d1"], weight=1)

        change = GraphChange(
            added_updated_edges={
                ("Book", "《Book》Ch1"),
                ("《Book》Ch1", "Alice"),
                ("Alice", "Bob"),
            }
        )
        chunks = []
        embd = SimpleNamespace(llm_name="m")
        await utils_extras._batch_embed_edges("kb1", embd, graph, change, chunks)

        assert len(chunks) == 2
        for chunk in chunks:
            assert not _has_vector(chunk)
        assert len(called_items) == 1
        assert called_items[0][0]["from_entity_kwd"] == "Alice"
        assert called_items[0][0]["to_entity_kwd"] == "Bob"
