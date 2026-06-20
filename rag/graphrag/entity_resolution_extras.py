#
#  Copyright 2024 The InfiniFlow Authors. All Rights Reserved.
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
"""Incremental entity resolution strategy.

This subclass is activated at import time when
``GraphRAGConfig.USE_INCREMENTAL_RESOLUTION`` is enabled; the parent class in
``rag/graphrag/entity_resolution.py`` is left unchanged so official code diffs
remain minimal.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
from typing import Any, Awaitable, Callable

import editdistance
import networkx as nx

from rag.nlp import is_english
from rag.graphrag.checkpoints import resolution_checkpoint_key
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.entity_resolution import (
    DEFAULT_ENTITY_INDEX_DELIMITER,
    DEFAULT_RECORD_DELIMITER,
    DEFAULT_RESOLUTION_RESULT_DELIMITER,
    EntityResolution,
    EntityResolutionResult,
)
from rag.graphrag.utils import GraphChange


# Entity types that should skip resolution (book and chapter nodes are
# intentionally kept separate even if they share names across documents).
EXCLUDED_RESOLUTION_TYPES = {"书籍", "章节"}


_EXCLUDED_TYPE_ALIASES = {
    "书籍": {"书籍", "book", "books", "Book", "Books", "BOOK"},
    "章节": {"章节", "chapter", "chapters", "Chapter", "Chapters", "CHAPTER", "section", "sections", "Section", "Sections", "SECTION"},
}


def build_excluded_types(excluded_from_config: list[str] | None = None) -> set[str]:
    """Build the set of entity types that should skip resolution.

    Expands common aliases so English ``entity_types`` like ``Book``/``Chapter``
    are handled the same way as the Chinese defaults.
    """
    result: set[str] = set(EXCLUDED_RESOLUTION_TYPES)
    if not excluded_from_config:
        return result
    for cfg_type in excluded_from_config:
        if not isinstance(cfg_type, str):
            continue
        cfg_type_stripped = cfg_type.strip()
        if not cfg_type_stripped:
            continue
        result.add(cfg_type_stripped)
        for canonical, aliases in _EXCLUDED_TYPE_ALIASES.items():
            all_aliases_lower = {a.lower() for a in aliases}
            if cfg_type_stripped.lower() in all_aliases_lower:
                result.update(aliases)
    return result


def _has_digit_in_2gram_diff(a: str, b: str) -> bool:
    """Return True if any 2-gram in the symmetric difference contains a digit."""
    if not isinstance(a, str) or not isinstance(b, str) or not a or not b:
        return False

    def to_2gram_set(s):
        return {s[i : i + 2] for i in range(len(s) - 1)}

    set_a = to_2gram_set(a)
    set_b = to_2gram_set(b)
    diff = set_a ^ set_b

    return any(isinstance(pair, str) and len(pair) >= 2 and any(c.isdigit() for c in pair) for pair in diff)


def is_similarity_str(a: str, b: str) -> bool:
    """Return True if two entity names are likely referring to the same entity."""
    if not isinstance(a, str) or not isinstance(b, str):
        return False
    if _has_digit_in_2gram_diff(a, b):
        return False

    if is_english(a) and is_english(b):
        if editdistance.eval(a, b) <= min(len(a), len(b)) // 2:
            return True
        return False

    a, b = set(a), set(b)
    max_l = max(len(a), len(b))
    if max_l < 4:
        return len(a & b) > 1

    return len(a & b) * 1.0 / max_l >= 0.8


class IncrementalEntityResolution(EntityResolution):
    """Entity resolution variant used by the incremental GraphRAG pipeline.

    Differences from the parent class:
      - ``excluded_types`` can be supplied to skip resolution for noisy types
        such as books / chapters.
      - ``candidate_resolution`` can be injected by the caller instead of being
        computed from the full graph.
      - Batch / concurrency sizes are read from ``GraphRAGConfig``.
    """

    def __init__(self, llm_invoker, excluded_types: set[str] | None = None):
        super().__init__(llm_invoker)
        self._excluded_types = excluded_types if excluded_types is not None else set()

    async def __call__(
        self,
        graph: nx.Graph,
        subgraph_nodes: set[str],
        prompt_variables: dict[str, Any] | None = None,
        callback: Callable | None = None,
        task_id: str = "",
        candidate_resolution: dict[str, list[tuple[str, str]]] | None = None,
        checkpoints: dict[str, Any] | None = None,
        save_checkpoint: Callable[[str, Any], Awaitable[bool]] | None = None,
    ) -> EntityResolutionResult:
        if prompt_variables is None:
            prompt_variables = {}

        # Wire defaults into the prompt variables
        self.prompt_variables = {
            **prompt_variables,
            self._record_delimiter_key: prompt_variables.get(self._record_delimiter_key) or DEFAULT_RECORD_DELIMITER,
            self._entity_index_delimiter_key: prompt_variables.get(self._entity_index_delimiter_key) or DEFAULT_ENTITY_INDEX_DELIMITER,
            self._resolution_result_delimiter_key: prompt_variables.get(self._resolution_result_delimiter_key) or DEFAULT_RESOLUTION_RESULT_DELIMITER,
        }

        nodes = sorted(graph.nodes())
        entity_types = sorted(etype for etype in set(graph.nodes[node].get("entity_type", "-") for node in nodes) if etype not in self._excluded_types)
        node_clusters = {entity_type: [] for entity_type in entity_types}

        for node in nodes:
            etype = graph.nodes[node].get("entity_type", "-")
            if etype in self._excluded_types:
                continue
            node_clusters[etype].append(node)

        if candidate_resolution is not None:
            candidate_resolution = {
                etype: [(a, b) for a, b in pairs if etype not in self._excluded_types and (a in subgraph_nodes or b in subgraph_nodes) and self.is_similarity(a, b)]
                for etype, pairs in candidate_resolution.items()
            }
        else:
            candidate_resolution = {entity_type: [] for entity_type in entity_types}
            for k, v in node_clusters.items():
                candidate_resolution[k] = [(a, b) for a, b in itertools.combinations(v, 2) if (a in subgraph_nodes or b in subgraph_nodes) and self.is_similarity(a, b)]
        num_candidates = sum([len(candidates) for _, candidates in candidate_resolution.items()])
        callback(msg=f"Identified {num_candidates} candidate pairs")
        remain_candidates_to_resolve = num_candidates

        resolution_result = set()
        resolution_result_lock = asyncio.Lock()
        resolution_batch_size = getattr(GraphRAGConfig, "RESOLUTION_BATCH_SIZE", 100) or 100
        max_concurrent_tasks = getattr(GraphRAGConfig, "RESOLUTION_MAX_CONCURRENT_TASKS", 5) or 5
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        checkpoints = checkpoints or {}

        async def limited_resolve_candidate(candidate_batch, result_set, result_lock):
            nonlocal remain_candidates_to_resolve, callback
            async with semaphore:
                try:
                    checkpoint_key = resolution_checkpoint_key(candidate_batch[0], candidate_batch[1])
                    checkpoint = checkpoints.get(checkpoint_key)
                    if isinstance(checkpoint, list):
                        async with result_lock:
                            for pair in checkpoint:
                                if isinstance(pair, (list, tuple)) and len(pair) == 2:
                                    result_set.add((pair[0], pair[1]))
                        remain_candidates_to_resolve -= len(candidate_batch[1])
                        callback(msg=f"Replayed {len(candidate_batch[1])} resolved pairs from checkpoint, {remain_candidates_to_resolve} remain.")
                        return
                    enable_timeout_assertion = os.environ.get("ENABLE_TIMEOUT_ASSERTION")
                    timeout_sec = 280 if enable_timeout_assertion else 1_000_000_000

                    try:
                        selected_pairs = await asyncio.wait_for(self._resolve_candidate(candidate_batch, result_set, result_lock, task_id), timeout=timeout_sec)
                        if selected_pairs is not None and save_checkpoint:
                            await save_checkpoint(checkpoint_key, [list(pair) for pair in selected_pairs])
                        remain_candidates_to_resolve -= len(candidate_batch[1])
                        callback(msg=f"Resolved {len(candidate_batch[1])} pairs, {remain_candidates_to_resolve} remain.")

                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout resolving {candidate_batch}, skipping...")
                        remain_candidates_to_resolve -= len(candidate_batch[1])
                        callback(msg=f"Failed to resolve {len(candidate_batch[1])} pairs due to timeout, skipped. {remain_candidates_to_resolve} remain.")

                except Exception as exception:
                    logging.error(f"Error resolving candidate batch: {exception}")

        tasks = []
        for key, lst in candidate_resolution.items():
            if not lst:
                continue
            for i in range(0, len(lst), resolution_batch_size):
                batch = (key, lst[i : i + resolution_batch_size])
                tasks.append(limited_resolve_candidate(batch, resolution_result, resolution_result_lock))
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error(f"Error resolving candidate pairs: {e}")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        callback(msg=f"Resolved {num_candidates} candidate pairs, {len(resolution_result)} of them are selected to merge.")

        change = GraphChange()
        connect_graph = nx.Graph()
        connect_graph.add_edges_from(resolution_result)

        merge_lock = asyncio.Lock()

        async def limited_merge_nodes(graph, nodes, change):
            async with merge_lock:
                await self._merge_graph_nodes(graph, nodes, change, task_id)

        tasks = []
        for sub_connect_graph in nx.connected_components(connect_graph):
            merging_nodes = list(sub_connect_graph)
            tasks.append(asyncio.create_task(limited_merge_nodes(graph, merging_nodes, change)))
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error(f"Error merging nodes: {e}")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        # Update pagerank
        pr = nx.pagerank(graph)
        for node_name, pagerank in pr.items():
            graph.nodes[node_name]["pagerank"] = pagerank

        return EntityResolutionResult(
            graph=graph,
            change=change,
        )

    def is_similarity(self, a, b):
        return is_similarity_str(a, b)
