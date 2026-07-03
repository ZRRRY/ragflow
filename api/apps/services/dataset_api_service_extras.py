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
"""Custom extensions for ``dataset_api_service``.

This module isolates GraphRAG incremental / chapter-graph logic that was
previously inlined in ``api/apps/services/dataset_api_service.py``.

Behavior is controlled by ``rag.graphrag.config.GraphRAGConfig`` flags so that
the default (all flags off) is equivalent to the official v0.26.1 path.
"""

from networkx.readwrite import json_graph

from common import settings
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.utils import get_graph_from_json
from rag.graphrag.utils_extras import get_graph_from_index_for_visualization

# Knowledge-graph keyword types that must be wiped when deleting a graph.
# "merge_state" is an incremental-build artefact and is harmless when no such
# rows exist (official path).
GRAPH_DELETE_KEYWORDS = [
    "graph",
    "subgraph",
    "entity",
    "relation",
    "community_report",
    "merge_state",
]


def graph_delete_keywords():
    """Return the list of knowledge-graph keyword types to delete."""
    return GRAPH_DELETE_KEYWORDS


async def _fetch_raw_knowledge_graph(dataset_id: str, tenant_id: str):
    """Fetch the raw (un-truncated) knowledge graph data from the monolithic JSON blob.

    This is the official v0.26.1 default path. The incremental path uses
    ``get_graph_from_index_for_visualization`` directly in ``get_knowledge_graph``.

    Defensive depth: re-check ``KnowledgebaseService.accessible`` here so this
    helper is safe to call from any new entry point (not only ``get_knowledge_graph``),
    preventing accidental authz bypass if the caller forgets the check.
    """
    # 防御性深度权限校验:避免被其他入口绕过 (P2-9 安全回归修复)
    from api.db.services.knowledgebase_service import KnowledgebaseService
    from rag.nlp import search

    if not KnowledgebaseService.accessible(dataset_id, tenant_id):
        return {"graph": {}, "mind_map": {}}

    _, kb = KnowledgebaseService.get_by_id(dataset_id)
    if not kb:
        return {"graph": {}, "mind_map": {}}

    obj = {"graph": {}, "mind_map": {}}

    if not settings.docStoreConn.index_exist(search.index_name(kb.tenant_id), dataset_id):
        return obj

    graph = await get_graph_from_json(kb.tenant_id, dataset_id)
    if graph is not None and len(graph.nodes) > 0:
        obj["graph"] = json_graph.node_link_data(graph, edges="edges")

    return obj


async def get_knowledge_graph(dataset_id: str, tenant_id: str):
    """Get knowledge graph for a dataset (truncated to 256 nodes and 128 edges for visualization).

    :param dataset_id: dataset ID
    :param tenant_id: tenant ID
    :return: (success, result) or (success, error_message)
    """
    from api.db.services.knowledgebase_service import KnowledgebaseService

    if not KnowledgebaseService.accessible(dataset_id, tenant_id):
        return False, "No authorization."

    if GraphRAGConfig.USE_INCREMENTAL_GRAPH:
        _, kb = KnowledgebaseService.get_by_id(dataset_id)
        graph = await get_graph_from_index_for_visualization(
            kb.tenant_id, dataset_id, max_nodes=256, max_edges=128,
            exclude_entity_types={"书籍", "章节"},
        )
        obj = {"graph": {}, "mind_map": {}}
        if graph is not None and len(graph.nodes) > 0:
            obj["graph"] = json_graph.node_link_data(graph, edges="edges")
    else:
        obj = await _fetch_raw_knowledge_graph(dataset_id, tenant_id)

    if "nodes" in obj["graph"]:
        all_nodes = obj["graph"]["nodes"]
        if GraphRAGConfig.USE_CHAPTER_GRAPH:
            protected_types = {"书籍", "章节"}
            protected_nodes = [n for n in all_nodes if n.get("entity_type") in protected_types]
            other_nodes = [n for n in all_nodes if n.get("entity_type") not in protected_types]
            sorted_other_nodes = sorted(
                other_nodes,
                key=lambda x: x.get("pagerank", 0),
                reverse=True,
            )[:max(0, 256 - len(protected_nodes))]
            obj["graph"]["nodes"] = protected_nodes + sorted_other_nodes
        else:
            obj["graph"]["nodes"] = sorted(
                all_nodes,
                key=lambda x: x.get("pagerank", 0),
                reverse=True,
            )[:256]

        if "edges" in obj["graph"]:
            node_id_set = {o["id"] for o in obj["graph"]["nodes"]}
            filtered_edges = [
                o
                for o in obj["graph"]["edges"]
                if o["source"] != o["target"]
                and o["source"] in node_id_set
                and o["target"] in node_id_set
            ]
            obj["graph"]["edges"] = sorted(
                filtered_edges,
                key=lambda x: x.get("weight", 0),
                reverse=True,
            )[:128]

    return True, obj
