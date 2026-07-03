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
"""GraphRAG incremental / optimization implementations.

This module isolates custom logic that was previously inlined in
``rag/graphrag/utils.py``.  The official ``utils.py`` remains aligned with the
upstream release and only contains thin dispatch markers for the functions that
need to deviate when incremental features are enabled.

All behavior is controlled by ``rag.graphrag.config.GraphRAGConfig`` flags so
that the default (all flags off) is equivalent to the official v0.26.1 path.
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict

import networkx as nx

from common import settings
from common.doc_store.doc_store_base import OrderByExpr
from common.misc_utils import get_uuid, thread_pool_exec
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.utils_pagination import (
    collect_all as _collect_all_search_after,
    supports_search_after as _supports_search_after,
)
from rag.nlp import rag_tokenizer, search

# Official helpers reused by the custom paths.
from rag.graphrag import utils as _utils

logger = logging.getLogger(__name__)

# Doc-store insert batching for GraphRAG subgraph/node/edge/community_report
# chunks.  Defaults (64 docs per batch, up to 4 batches in flight) mirror the
# regular ingest pipeline in document_service.py while still keeping the total
# number of simultaneous requests to ES/Infinity bounded.  Override with
# GRAPHRAG_INSERT_BULK_SIZE and GRAPHRAG_INSERT_CONCURRENCY.
_INSERT_BULK_SIZE = max(1, int(os.environ.get("GRAPHRAG_INSERT_BULK_SIZE", 64)))
_INSERT_CONCURRENCY = max(1, int(os.environ.get("GRAPHRAG_INSERT_CONCURRENCY", 4)))

# OpenSearch / Elasticsearch default limit for the number of terms in a terms
# query/filter.  Splitting large to_node lists avoids "max_terms_count" errors
# during bulk edge deletion.
_MAX_TERMS_COUNT = max(1, int(os.environ.get("GRAPHRAG_DELETE_MAX_TERMS_COUNT", 65536)))


async def _post_insert_refresh(tenant_id: str, callback=None, label: str = "set_graph"):
    """Refresh the doc store index after a bulk insert so downstream
    queries see the new data immediately.

    Opt out via ``SET_GRAPH_DELTA_REFRESH_AFTER_INSERT=0`` to rely on the
    default OpenSearch refresh_interval (1s). Used by both the
    monolithic and delta ``set_graph`` paths so they share a single
    refresh policy.
    """
    if not GraphRAGConfig.SET_GRAPH_DELTA_REFRESH_AFTER_INSERT:
        return
    refresh_fn = getattr(settings.docStoreConn, "refresh_idx", None)
    if refresh_fn is None:
        logging.debug(
            "%s: docStoreConn has no refresh_idx; relying on default refresh_interval",
            label,
        )
        return
    try:
        await thread_pool_exec(refresh_fn, search.index_name(tenant_id))
    except Exception:
        logging.exception(
            "%s: post-insert refresh_idx failed (will rely on default refresh_interval)",
            label,
        )


async def _delete_relation_edges_bulk(
    tenant_id: str,
    kb_id: str,
    from_node: str,
    to_nodes: list[str],
    *,
    max_retries: int = 3,
    label: str = "del_edges_bulk",
) -> None:
    """Delete relation edges for a single ``from_node`` in ``to_nodes`` batches.

    Each ``to_entity_kwd`` terms list is capped at ``_MAX_TERMS_COUNT`` to stay
    below the OpenSearch / Elasticsearch ``index.max_terms_count`` default.
    """
    for batch_offset in range(0, len(to_nodes), _MAX_TERMS_COUNT):
        to_batch = to_nodes[batch_offset : batch_offset + _MAX_TERMS_COUNT]
        for attempt in range(max_retries):
            try:
                async with _utils.chat_limiter:
                    await thread_pool_exec(
                        settings.docStoreConn.delete,
                        {
                            "knowledge_graph_kwd": ["relation"],
                            "from_entity_kwd": from_node,
                            "to_entity_kwd": to_batch,
                        },
                        search.index_name(tenant_id),
                        kb_id,
                    )
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logging.warning(
                        "%s(from=%s, n_to=%d, batch_offset=%d) attempt %d failed: %s, retrying in %ds",
                        label, from_node, len(to_batch), batch_offset, attempt + 1, e, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    raise


async def write_merge_state(
    tenant_id: str,
    kb_id: str,
    doc_id: str,
    state: str,
    expected_nodes: int = 0,
    expected_edges: int = 0,
    extra: dict | None = None,
):
    """写入文档的合并状态。同一 doc_id 的旧记录会被删除后再写入新记录。"""
    # 清理旧记录
    try:
        await thread_pool_exec(
            settings.docStoreConn.delete,
            {
                "knowledge_graph_kwd": ["merge_state"],
                "source_id": [doc_id],
                "kb_id": kb_id,
            },
            search.index_name(tenant_id),
            kb_id,
        )
    except Exception:
        logging.exception("Failed to clean old merge_state for doc %s (non-fatal)", doc_id)

    meta = {
        "state": state,
        "expected_nodes": expected_nodes,
        "expected_edges": expected_edges,
        "updated_at": time.time(),
    }
    if extra:
        meta.update(extra)

    chunk = {
        "id": get_uuid(),
        "kb_id": kb_id,
        "source_id": [doc_id],
        "knowledge_graph_kwd": "merge_state",
        "content_with_weight": json.dumps(meta, ensure_ascii=False),
        "available_int": 0,
        "removed_kwd": "N",
    }

    await thread_pool_exec(
        settings.docStoreConn.insert,
        [chunk],
        search.index_name(tenant_id),
        kb_id,
    )


async def query_merge_state(
    tenant_id: str,
    kb_id: str,
    doc_id: str,
) -> dict | None:
    """查询文档的最新合并状态。返回 None 表示无记录。

    由于 ``write_merge_state`` 删除旧记录是非关键操作，异常时可能残留多
    条同 ``doc_id`` 的 ``merge_state`` 文档。这里取 ``updated_at`` 最大
    的一条，确保行为确定（P3-2）。
    """
    condition = {
        "knowledge_graph_kwd": ["merge_state"],
        "source_id": [doc_id],
    }
    try:
        res = await thread_pool_exec(
            settings.docStoreConn.search,
            ["content_with_weight"], [], condition, [], OrderByExpr(),
            0, 10, search.index_name(tenant_id), [kb_id],
        )
        fields = settings.docStoreConn.get_fields(res, ["content_with_weight"])
        latest_meta = None
        latest_ts = -1.0
        for row in fields.values():
            try:
                meta = json.loads(row["content_with_weight"])
            except Exception:
                continue
            ts = float(meta.get("updated_at", 0) or 0)
            if ts > latest_ts:
                latest_ts = ts
                latest_meta = meta
        return latest_meta
    except Exception:
        logging.exception("query_merge_state failed for doc %s", doc_id)
    return None


async def is_doc_merged(tenant_id: str, kb_id: str, doc_id: str) -> bool:
    """Check whether a document has already been merged into the global graph.

    基于 merge_state 精确状态判断。无状态记录视为未合并。
    """
    state_meta = await query_merge_state(tenant_id, kb_id, doc_id)
    return state_meta is not None and state_meta.get("state") == "merged"


async def query_existing_entities(tenant_id, kb_id, node_names):
    """Batch-query existing entity documents from the doc store by name.

    Returns a dict mapping ``entity_name -> doc fields``.
    """
    if not node_names:
        return {}

    BATCH_SIZE = 100
    existing = {}

    for i in range(0, len(node_names), BATCH_SIZE):
        batch = node_names[i:i + BATCH_SIZE]
        conds = {
            "fields": ["entity_kwd", "entity_type_kwd", "content_with_weight", "source_id"],
            "size": len(batch),
            "knowledge_graph_kwd": ["entity"],
            "entity_kwd": batch,
        }
        try:
            es_res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id])
            for id in es_res.ids:
                fields = es_res.field[id]
                ent_name = fields.get("entity_kwd")
                if isinstance(ent_name, list):
                    ent_name = ent_name[0]
                if ent_name:
                    existing[ent_name] = fields
        except Exception as e:
            logging.warning("query_existing_entities batch %d failed: %s", i, e)

    return existing


async def fetch_node_vectors(tenant_id, kb_id, node_names, vector_dim):
    """Batch-read node vectors (``q_{vector_dim}_vec``) from the doc store.

    Returns a dict mapping ``entity_name -> vector``.
    """
    if not node_names:
        return {}

    vector_field = f"q_{vector_dim}_vec"
    BATCH_SIZE = 100
    existing = {}

    for i in range(0, len(node_names), BATCH_SIZE):
        batch = node_names[i:i + BATCH_SIZE]
        conds = {
            "fields": ["entity_kwd", vector_field],
            "size": len(batch),
            "knowledge_graph_kwd": ["entity"],
            "entity_kwd": batch,
        }
        try:
            es_res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id])
            for id in es_res.ids:
                fields = es_res.field[id]
                ent_name = fields.get("entity_kwd")
                if isinstance(ent_name, list):
                    ent_name = ent_name[0]
                vec = fields.get(vector_field)
                if ent_name and vec is not None:
                    existing[ent_name] = vec
        except Exception as e:
            logging.warning("fetch_node_vectors batch %d failed: %s", i, e)

    return existing


async def query_existing_relations(tenant_id, kb_id, edge_pairs):
    """Batch-query existing relation documents from the doc store.

    ``edge_pairs`` is a list of ``(from_node, to_node)`` tuples.
    Returns a dict mapping ``(from, to) -> doc fields``.

    Phase 2.3: 用 search_after 分页代替 ``size: 10000`` 硬卡，绕开 OS / ES
    的 max_result_window 截断。后端不支持 search_after 时回退到旧路径。
    """
    if not edge_pairs:
        return {}

    conn = settings.docStoreConn
    if not _supports_search_after(conn):
        # Fallback: 旧 size 截断路径
        return await _query_existing_relations_legacy(tenant_id, kb_id, edge_pairs)

    # INPUT_BATCH_SIZE: 每次喂入 query 的 edge_pairs 数量(输入维度)。
    # 注意:实际 ES 单次 search_after round-trip 的 filter 范围 =
    # |union(batch from_nodes ∪ to_nodes)|(可能远大于 INPUT_BATCH_SIZE,
    # 例如 50 对 (A,B) 去重后可能 80 个 unique node)。
    # ES round-trip 的 page_size 由下方常量 SEARCH_AFTER_PAGE_SIZE 控制。
    INPUT_BATCH_SIZE = 50
    SEARCH_AFTER_PAGE_SIZE = 1000
    index_name = search.index_name(tenant_id)
    existing: dict = {}

    # 每个 batch 单独发 search_after 翻页查询，filter 与旧实现保持一致
    # (from_entity_kwd ∈ all_nodes AND to_entity_kwd ∈ all_nodes)
    for i in range(0, len(edge_pairs), INPUT_BATCH_SIZE):
        batch = edge_pairs[i:i + INPUT_BATCH_SIZE]
        all_nodes = list(set(u for u, v in batch) | set(v for u, v in batch))

        filters = {
            "knowledge_graph_kwd": ["relation"],
            "from_entity_kwd": all_nodes,
            "to_entity_kwd": all_nodes,
        }
        try:
            hits = await _collect_all_search_after(
                filters=filters,
                index_name=index_name,
                kb_id=kb_id,
                fields=["from_entity_kwd", "to_entity_kwd", "content_with_weight", "source_id"],
                sort_field="from_entity_kwd",
                page_size=SEARCH_AFTER_PAGE_SIZE,
            )
        except Exception as e:
            logging.warning("query_existing_relations batch %d search_after failed: %s", i, e)
            continue

        for fields in hits:
            # search_after 返回的 doc 不一定带 id（取决于 _source 投影），从 fields 中取
            from_node = fields.get("from_entity_kwd")
            to_node = fields.get("to_entity_kwd")
            if isinstance(from_node, list):
                from_node = from_node[0]
            if isinstance(to_node, list):
                to_node = to_node[0]
            if from_node and to_node:
                key = _utils.get_from_to(from_node, to_node)
                existing[key] = fields

    return existing


async def _query_existing_relations_legacy(tenant_id, kb_id, edge_pairs):
    """Phase 2.3: 不支持 search_after 的后端（旧 Infinity 等）走 size=10000 fallback。"""
    BATCH_SIZE = 50
    existing = {}
    for i in range(0, len(edge_pairs), BATCH_SIZE):
        batch = edge_pairs[i:i + BATCH_SIZE]
        all_nodes = list(set(u for u, v in batch) | set(v for u, v in batch))
        conds = {
            "fields": ["from_entity_kwd", "to_entity_kwd", "content_with_weight", "source_id"],
            "size": min(len(batch) * 10, 10000),
            "knowledge_graph_kwd": ["relation"],
            "from_entity_kwd": all_nodes,
            "to_entity_kwd": all_nodes,
        }
        try:
            es_res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id])
            for id in es_res.ids:
                fields = es_res.field[id]
                from_node = fields.get("from_entity_kwd")
                to_node = fields.get("to_entity_kwd")
                if isinstance(from_node, list):
                    from_node = from_node[0]
                if isinstance(to_node, list):
                    to_node = to_node[0]
                if from_node and to_node:
                    key = _utils.get_from_to(from_node, to_node)
                    existing[key] = fields
        except Exception as e:
            logging.warning("query_existing_relations_legacy batch %d failed: %s", i, e)
    return existing


async def query_node_relations(tenant_id, kb_id, node_names):
    """Query all relation documents where at least one endpoint is in ``node_names``.

    Returns a list of doc field dicts (deduplicated).

    Phase 2.3: 用 search_after 翻页拉全量（绕过 10000 上限），后端不支持时
    回退到 size=10000 旧路径。
    """
    if not node_names:
        return []

    conn = settings.docStoreConn
    if not _supports_search_after(conn):
        return await _query_node_relations_legacy(tenant_id, kb_id, node_names)

    # INPUT_BATCH_SIZE: 每次喂入的 node 数量(输入维度)。
    # ES 单次 round-trip 的 page_size 由下方常量 SEARCH_AFTER_PAGE_SIZE 控制。
    INPUT_BATCH_SIZE = 100
    SEARCH_AFTER_PAGE_SIZE = 1000
    index_name = search.index_name(tenant_id)
    all_fields: list[dict] = []
    seen: set = set()

    for i in range(0, len(node_names), INPUT_BATCH_SIZE):
        batch = node_names[i:i + INPUT_BATCH_SIZE]

        # Query by from_entity_kwd
        try:
            from_hits = await _collect_all_search_after(
                filters={
                    "knowledge_graph_kwd": ["relation"],
                    "from_entity_kwd": batch,
                },
                index_name=index_name,
                kb_id=kb_id,
                fields=["from_entity_kwd", "to_entity_kwd", "content_with_weight", "source_id"],
                sort_field="from_entity_kwd",
                page_size=SEARCH_AFTER_PAGE_SIZE,
            )
        except Exception as e:
            logging.warning("query_node_relations from-batch %d search_after failed: %s", i, e)
            from_hits = []

        # Query by to_entity_kwd
        try:
            to_hits = await _collect_all_search_after(
                filters={
                    "knowledge_graph_kwd": ["relation"],
                    "to_entity_kwd": batch,
                },
                index_name=index_name,
                kb_id=kb_id,
                fields=["from_entity_kwd", "to_entity_kwd", "content_with_weight", "source_id"],
                sort_field="to_entity_kwd",
                page_size=SEARCH_AFTER_PAGE_SIZE,
            )
        except Exception as e:
            logging.warning("query_node_relations to-batch %d search_after failed: %s", i, e)
            to_hits = []

        for fields in from_hits + to_hits:
            from_node = fields.get("from_entity_kwd")
            to_node = fields.get("to_entity_kwd")
            if isinstance(from_node, list):
                from_node = from_node[0]
            if isinstance(to_node, list):
                to_node = to_node[0]
            key = _utils.get_from_to(from_node, to_node)
            if key not in seen:
                seen.add(key)
                all_fields.append(fields)

    return all_fields


async def _query_node_relations_legacy(tenant_id, kb_id, node_names):
    """Phase 2.3: 不支持 search_after 的后端走 size=10000 fallback。"""
    BATCH_SIZE = 100
    all_fields = []
    seen = set()

    for i in range(0, len(node_names), BATCH_SIZE):
        batch = node_names[i:i + BATCH_SIZE]
        for endpoint in ("from_entity_kwd", "to_entity_kwd"):
            conds = {
                "fields": ["from_entity_kwd", "to_entity_kwd", "content_with_weight", "source_id"],
                "size": 10000,
                "knowledge_graph_kwd": ["relation"],
                endpoint: batch,
            }
            try:
                es_res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id])
                for id in es_res.ids:
                    fields = es_res.field[id]
                    from_node = fields.get("from_entity_kwd")
                    to_node = fields.get("to_entity_kwd")
                    if isinstance(from_node, list):
                        from_node = from_node[0]
                    if isinstance(to_node, list):
                        to_node = to_node[0]
                    key = _utils.get_from_to(from_node, to_node)
                    if key not in seen:
                        seen.add(key)
                        all_fields.append(fields)
            except Exception as e:
                logging.warning("query_node_relations_legacy %s-batch %d failed: %s", endpoint, i, e)
    return all_fields


async def get_graph_from_index(tenant_id, kb_id, exclude_entity_types=None):
    """Assemble the global graph from discrete ``entity`` and ``relation``
    chunks stored in the doc store (incremental / decoupled storage mode).

    This replaces the monolithic ``knowledge_graph_kwd="graph"`` JSON blob
    with on-the-fly assembly from indexed node/edge documents.

    Args:
        exclude_entity_types: Optional set of entity types to exclude from the
            assembled graph (e.g. book/chapter structural nodes). Relations
            incident to excluded nodes are also dropped.
    """
    graph = nx.Graph()
    graph.graph["source_id"] = []
    seen_sources = set()
    total_entities = 0
    total_relations = 0
    excluded_nodes = set()

    # search_with_scroll is only implemented by OpenSearch.  ES / Infinity
    # callers fall back to None and let the caller pick the monolithic path.
    if not hasattr(settings.docStoreConn, "search_with_scroll"):
        return None

    # ------------------------------------------------------------------
    # 1. Pull all entity chunks via scroll (bypass max_result_window)
    # ------------------------------------------------------------------
    ent_flds = ["entity_kwd", "entity_type_kwd", "content_with_weight", "source_id"]

    ent_query = {
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"kb_id": [kb_id]}},
                    {"terms": {"knowledge_graph_kwd": ["entity"]}},
                ]
            }
        }
    }

    es_res = await thread_pool_exec(
        settings.docStoreConn.search_with_scroll,
        search.index_name(tenant_id),
        ent_query,
        ent_flds,
        hits_cap=GraphRAGConfig.SEARCH_WITH_SCROLL_HITS_CAP,
    )
    es_res = settings.docStoreConn.get_fields(es_res, ent_flds)

    for _cid, d in es_res.items():
        try:
            meta = json.loads(d["content_with_weight"])
            ent_name = d["entity_kwd"]
            if isinstance(ent_name, list):
                ent_name = ent_name[0] if ent_name else None
            if not ent_name:
                continue

            ent_type = meta.get("entity_type")
            if exclude_entity_types and ent_type in exclude_entity_types:
                excluded_nodes.add(ent_name)
                continue

            graph.add_node(ent_name, **meta)
            total_entities += 1
            for sid in meta.get("source_id", []):
                seen_sources.add(sid)
        except Exception:
            logging.exception("Failed to parse entity chunk %s", _cid)
            continue

    if len(graph.nodes) == 0:
        return None

    # ------------------------------------------------------------------
    # 2. Pull all relation chunks via scroll (bypass max_result_window)
    # ------------------------------------------------------------------
    rel_flds = ["from_entity_kwd", "to_entity_kwd", "content_with_weight", "source_id"]

    rel_query = {
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"kb_id": [kb_id]}},
                    {"terms": {"knowledge_graph_kwd": ["relation"]}},
                ]
            }
        }
    }

    es_res = await thread_pool_exec(
        settings.docStoreConn.search_with_scroll,
        search.index_name(tenant_id),
        rel_query,
        rel_flds,
        hits_cap=GraphRAGConfig.SEARCH_WITH_SCROLL_HITS_CAP,
    )
    es_res = settings.docStoreConn.get_fields(es_res, rel_flds)

    for _cid, d in es_res.items():
        try:
            meta = json.loads(d["content_with_weight"])
            from_node = d["from_entity_kwd"]
            to_node = d["to_entity_kwd"]
            if isinstance(from_node, list):
                from_node = from_node[0] if from_node else None
            if isinstance(to_node, list):
                to_node = to_node[0] if to_node else None
            if (
                from_node
                and to_node
                and from_node not in excluded_nodes
                and to_node not in excluded_nodes
                and from_node in graph.nodes
                and to_node in graph.nodes
            ):
                graph.add_edge(from_node, to_node, **meta)
                total_relations += 1
                for sid in meta.get("source_id", []):
                    seen_sources.add(sid)
        except Exception:
            logging.exception("Failed to parse relation chunk %s", _cid)
            continue

    graph.graph["source_id"] = sorted(seen_sources)
    logging.info(
        "get_graph_from_index: kb=%s entities=%d relations=%d sources=%d excluded=%d",
        kb_id, total_entities, total_relations, len(seen_sources), len(excluded_nodes),
    )
    return graph


async def get_graph_from_index_for_visualization(
    tenant_id, kb_id, max_nodes=256, max_edges=128, scan_limit=5000, exclude_entity_types=None
):
    """Assemble a truncated graph for visualization from indexed chunks.

    Unlike ``get_graph_from_index``, this function does **not** load the full
    entity/relation index. It samples up to ``scan_limit`` entities, keeps the
    top ``max_nodes`` by pagerank, and only fetches relations between those
    selected nodes. This keeps the visualization API memory-safe even for
    very large knowledge graphs in incremental mode.

    Args:
        exclude_entity_types: Optional set/list of entity types to exclude from
            visualization (e.g. {"书籍", "章节"}). Relations incident to excluded
            nodes are also dropped.
    """
    graph = nx.Graph()
    graph.graph["source_id"] = []
    seen_sources = set()

    # 1. Sample entities without loading the entire index.
    ent_flds = ["entity_kwd", "entity_type_kwd", "content_with_weight", "source_id"]
    ent_query = {
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"kb_id": [kb_id]}},
                    {"terms": {"knowledge_graph_kwd": ["entity"]}},
                ]
            }
        }
    }

    # ES / Infinity doc store has no search_with_scroll; fall back to None so
    # callers handle "unsupported backend" uniformly.
    if not hasattr(settings.docStoreConn, "search_with_scroll"):
        return None

    es_res = await thread_pool_exec(
        settings.docStoreConn.search_with_scroll,
        search.index_name(tenant_id),
        ent_query,
        ent_flds,
        batch_size=1000,
        max_pages=max(1, scan_limit // 1000),
    )
    es_res = settings.docStoreConn.get_fields(es_res, ent_flds)

    candidate_nodes = []
    excluded_nodes = set()
    for _cid, d in es_res.items():
        try:
            meta = json.loads(d["content_with_weight"])
            ent_name = d["entity_kwd"]
            if isinstance(ent_name, list):
                ent_name = ent_name[0] if ent_name else None
            if not ent_name:
                continue

            ent_type = meta.get("entity_type")
            if exclude_entity_types and ent_type in exclude_entity_types:
                excluded_nodes.add(ent_name)
                continue

            pagerank = float(meta.get("pagerank", 0) or 0)
            candidate_nodes.append((ent_name, pagerank, meta))
            for sid in meta.get("source_id", []):
                seen_sources.add(sid)
        except Exception:
            logging.exception("Failed to parse entity chunk %s", _cid)
            continue

    if excluded_nodes:
        logging.info(
            "get_graph_from_index_for_visualization: kb=%s excluded %d nodes by type %s",
            kb_id, len(excluded_nodes), exclude_entity_types,
        )

    if not candidate_nodes:
        return None

    # Keep the top-ranked nodes for visualization.
    candidate_nodes.sort(key=lambda x: x[1], reverse=True)
    top_nodes = candidate_nodes[:max_nodes]
    node_set = {name for name, _, _ in top_nodes}

    for ent_name, _, meta in top_nodes:
        graph.add_node(ent_name, **meta)

    # 2. Fetch only relations whose endpoints are both in the selected node set.
    rel_flds = ["from_entity_kwd", "to_entity_kwd", "content_with_weight", "source_id"]
    rel_condition = {
        "kb_id": [kb_id],
        "knowledge_graph_kwd": ["relation"],
        "from_entity_kwd": list(node_set),
        "to_entity_kwd": list(node_set),
    }

    rel_res = await thread_pool_exec(
        settings.docStoreConn.search,
        rel_flds,
        [],
        rel_condition,
        [],
        OrderByExpr(),
        0,
        max_edges * 2,
        search.index_name(tenant_id),
        [kb_id],
    )
    rel_fields = settings.docStoreConn.get_fields(rel_res, rel_flds)

    kept_edges = 0
    for _cid, d in rel_fields.items():
        try:
            meta = json.loads(d["content_with_weight"])
            from_node = d["from_entity_kwd"]
            to_node = d["to_entity_kwd"]
            if isinstance(from_node, list):
                from_node = from_node[0] if from_node else None
            if isinstance(to_node, list):
                to_node = to_node[0] if to_node else None
            if (
                from_node
                and to_node
                and from_node in node_set
                and to_node in node_set
            ):
                graph.add_edge(from_node, to_node, **meta)
                kept_edges += 1
                if kept_edges >= max_edges:
                    break
            for sid in meta.get("source_id", []):
                seen_sources.add(sid)
        except Exception:
            logging.exception("Failed to parse relation chunk %s", _cid)
            continue

    graph.graph["source_id"] = sorted(seen_sources)
    logging.info(
        "get_graph_from_index_for_visualization: kb=%s nodes=%d edges=%d sources=%d",
        kb_id, len(graph.nodes), len(graph.edges), len(seen_sources),
    )
    return graph


async def _batch_embed_nodes(kb_id, embd_mdl, graph, change, chunks, callback=None):
    """Batch-embed nodes and append chunks. Replaces the per-node asyncio.gather pattern."""
    if not change.added_updated_nodes:
        return

    items = []
    for node in change.added_updated_nodes:
        node_attrs = graph.nodes[node]
        chunk = {
            "id": get_uuid(),
            "important_kwd": [node],
            "title_tks": rag_tokenizer.tokenize(node),
            "entity_kwd": node,
            "knowledge_graph_kwd": "entity",
            "entity_type_kwd": node_attrs.get("entity_type", ""),
            "content_with_weight": json.dumps(node_attrs, ensure_ascii=False),
            "content_ltks": rag_tokenizer.tokenize(node_attrs.get("description", "")),
            "source_id": node_attrs.get("source_id", []),
            "kb_id": kb_id,
            "available_int": 0,
            "removed_kwd": "N",
        }
        chunk["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(chunk["content_ltks"])
        # Preserve the fields expected by v0.26.0 KGSearch / UI
        chunk["rank_flt"] = float(node_attrs.get("pagerank", 0) or 0)
        chunk["n_hop_with_weight"] = json.dumps(_utils.n_neighbor(graph, node) or [], ensure_ascii=False)
        # 书籍/章节类结构节点不需要语义向量，直接入库；不进入批量 embedding。
        if GraphRAGConfig.should_skip_embedding(node_attrs.get("entity_type")):
            chunks.append(chunk)
            continue
        items.append((chunk, node, node))

    await _batch_embed_items(kb_id, embd_mdl, items, chunks, callback, label="nodes")


async def _batch_embed_edges(kb_id, embd_mdl, graph, change, chunks, callback=None):
    """Batch-embed edges and append chunks. Replaces the per-edge asyncio.gather pattern."""
    if not change.added_updated_edges:
        return

    items = []
    for from_node, to_node in change.added_updated_edges:
        edge_attrs = graph.get_edge_data(from_node, to_node)
        if not edge_attrs:
            continue
        chunk = {
            "id": get_uuid(),
            "from_entity_kwd": from_node,
            "to_entity_kwd": to_node,
            "knowledge_graph_kwd": "relation",
            "content_with_weight": json.dumps(edge_attrs, ensure_ascii=False),
            "content_ltks": rag_tokenizer.tokenize(edge_attrs.get("description", "")),
            "important_kwd": edge_attrs.get("keywords", []),
            "source_id": edge_attrs.get("source_id", []),
            "weight_int": int(edge_attrs.get("weight", 0)),
            "kb_id": kb_id,
            "available_int": 0,
            "removed_kwd": "N",
        }
        chunk["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(chunk["content_ltks"])
        # 与书籍/章节相关的结构关系不需要语义向量，直接入库；不进入批量 embedding。
        from_type = graph.nodes[from_node].get("entity_type")
        to_type = graph.nodes[to_node].get("entity_type")
        if GraphRAGConfig.should_skip_embedding(from_type) or GraphRAGConfig.should_skip_embedding(to_type):
            chunks.append(chunk)
            continue
        cache_key = f"{from_node}->{to_node}"
        embed_text = f"{cache_key}: {edge_attrs.get('description', '')}"
        items.append((chunk, cache_key, embed_text))

    await _batch_embed_items(kb_id, embd_mdl, items, chunks, callback, label="edges")


async def _batch_embed_items(kb_id, embd_mdl, items, chunks, callback, label):
    """Shared batch embedding logic with cache lookup and batch API calls.

    Batches are executed concurrently up to ``chat_limiter`` (default 10) so
    throughput is batch_size × concurrency.
    """
    if not items:
        return

    missed_indices = []
    missed_texts = []
    embeddings = [None] * len(items)

    for idx, (_, cache_key, embed_text) in enumerate(items):
        ebd = _utils.get_embed_cache(embd_mdl.llm_name, cache_key)
        if ebd is not None:
            embeddings[idx] = ebd
        else:
            missed_indices.append(idx)
            missed_texts.append(embed_text)

    if missed_texts:
        batch_size = GraphRAGConfig.EMBED_BATCH_SIZE
        total = len(missed_texts)

        async def _embed_one_batch(i):
            async with _utils.chat_limiter:
                if callback:
                    callback(msg=f"Get embedding of {label}: {i}/{total}")
                batch = missed_texts[i:i + batch_size]
                ebd_arr, _ = await asyncio.wait_for(
                    thread_pool_exec(embd_mdl.encode, batch),
                    timeout=30000000,
                )
                return i, ebd_arr

        tasks = []
        for i in range(0, total, batch_size):
            tasks.append(asyncio.create_task(_embed_one_batch(i)))

        try:
            results = await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error("Error in batch embedding of %s: %s", label, e)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        for i, ebd_arr in results:
            for j, ebd in enumerate(ebd_arr):
                global_idx = missed_indices[i + j]
                embeddings[global_idx] = ebd
                _, cache_key, _ = items[global_idx]
                _utils.set_embed_cache(embd_mdl.llm_name, cache_key, ebd)

    for idx, (chunk, _, _) in enumerate(items):
        ebd = embeddings[idx]
        assert ebd is not None
        chunk["q_%d_vec" % len(ebd)] = ebd
        chunks.append(chunk)


async def _pre_delete_added_updated(
    tenant_id: str,
    kb_id: str,
    change: _utils.GraphChange,
    callback=None,
):
    """Pre-delete old versions of nodes/edges that will be re-inserted.

    ``_batch_embed_nodes`` / ``_batch_embed_edges`` generate new chunks with
    random UUIDs.  Without this step, merging the same document again would
    leave the previous entity/relation documents behind, creating multi-version
    residue.  Deleting by business key before insert keeps exactly one version
    per entity/relation in the doc store.
    """

    if change.added_updated_nodes:
        BATCH_SIZE = 100
        sorted_nodes = sorted(change.added_updated_nodes)
        for i in range(0, len(sorted_nodes), BATCH_SIZE):
            batch = sorted_nodes[i:i + BATCH_SIZE]
            await thread_pool_exec(
                settings.docStoreConn.delete,
                {"knowledge_graph_kwd": ["entity"], "entity_kwd": batch},
                search.index_name(tenant_id),
                kb_id
            )

    if change.added_updated_edges:
        update_buckets: dict[str, list[str]] = defaultdict(list)
        for from_node, to_node in change.added_updated_edges:
            update_buckets[from_node].append(to_node)

        tasks = [
            asyncio.create_task(
                _delete_relation_edges_bulk(
                    tenant_id, kb_id, from_node, to_nodes, label="pre-del_update_edges_bulk"
                )
            )
            for from_node, to_nodes in update_buckets.items()
        ]
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error("Error while pre-deleting update edges: %s", e)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    if callback and (change.added_updated_nodes or change.added_updated_edges):
        callback(msg=f"pre-deleted old versions of {len(change.added_updated_nodes)} nodes and {len(change.added_updated_edges)} edges")


async def set_graph_delta(tenant_id: str, kb_id: str, embd_mdl, graph: nx.Graph, change: _utils.GraphChange, callback):
    """Incremental write path for Phase 1 (storage decoupling).

    Key differences from the monolithic path:

    1. **No subgraph rewrite** – per-document subgraph checkpoints are already
       managed by ``generate_subgraph``; rewriting them here is redundant and
       expensive for large KBs.
    2. **No graph JSON shadow storage** – the monolithic graph JSON blob is
       intentionally NOT rewritten because ``delta_graph`` only contains the
       current document's nodes/edges.  Writing it would overwrite the global
       graph with partial data.  The canonical graph is assembled on demand by
       ``get_graph_from_index`` from the indexed entity/relation documents.
    3. **Entity / relation chunks are produced for the delta** so the
       ``get_graph_from_index`` path can assemble a consistent graph.
    """
    start = asyncio.get_running_loop().time()

    # ------------------------------------------------------------------
    # 1. Embeddings for the delta only
    chunks = []
    await _batch_embed_nodes(kb_id, embd_mdl, graph, change, chunks, callback)
    await _batch_embed_edges(kb_id, embd_mdl, graph, change, chunks, callback)

    now = asyncio.get_running_loop().time()
    if callback:
        callback(msg=f"set_graph converted graph change to {len(chunks)} chunks in {now - start:.2f}s.")
    start = now

    # ------------------------------------------------------------------
    # 2. Delete removed entities/edges (graph JSON shadow storage is left alone)
    # ------------------------------------------------------------------
    if change.removed_nodes:
        BATCH_SIZE = 100
        sorted_nodes = sorted(change.removed_nodes)
        for i in range(0, len(sorted_nodes), BATCH_SIZE):
            batch = sorted_nodes[i:i + BATCH_SIZE]
            await thread_pool_exec(
                settings.docStoreConn.delete,
                {"knowledge_graph_kwd": ["entity"], "entity_kwd": batch},
                search.index_name(tenant_id),
                kb_id
            )

    if change.removed_edges:
        from_buckets: dict[str, list[str]] = defaultdict(list)
        for from_node, to_node in change.removed_edges:
            from_buckets[from_node].append(to_node)

        tasks = [
            asyncio.create_task(
                _delete_relation_edges_bulk(
                    tenant_id, kb_id, from_node, to_nodes, label="del_edges_bulk"
                )
            )
            for from_node, to_nodes in from_buckets.items()
        ]
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error("Error while bulk-deleting edges: %s", e)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    # 3. Pre-delete old versions of entities/edges that will be re-inserted
    await _pre_delete_added_updated(tenant_id, kb_id, change, callback)

    del_now = asyncio.get_running_loop().time()
    if callback:
        callback(msg=f"set_graph removed {len(change.removed_nodes)} nodes and {len(change.removed_edges)} edges from index in {del_now - start:.2f}s.")
    start = del_now

    await _utils.insert_chunks_bounded(chunks, tenant_id, kb_id, callback=callback, label="Insert chunks")
    await _post_insert_refresh(tenant_id, callback=callback, label="set_graph_delta")
    now = asyncio.get_running_loop().time()
    if callback:
        callback(msg=f"set_graph added/updated {len(change.added_updated_nodes)} nodes and {len(change.added_updated_edges)} edges from index in {now - start:.2f}s.")


async def _set_graph_monolithic(tenant_id: str, kb_id: str, embd_mdl, graph: nx.Graph, change: _utils.GraphChange, callback):
    """Custom monolithic path: official implementation plus post-insert refresh."""
    await _utils._set_graph_impl(tenant_id, kb_id, embd_mdl, graph, change, callback)
    await _post_insert_refresh(tenant_id, callback=callback, label="set_graph_monolithic")


async def set_graph(tenant_id: str, kb_id: str, embd_mdl, graph: nx.Graph, change: _utils.GraphChange, callback):
    """Router: dispatches to the monolithic or incremental path based on
    ``GraphRAGConfig.USE_INCREMENTAL_GRAPH``.

    Callers (``merge_subgraph``, ``resolve_entities``, ``extract_community``)
    do not need to change; the switch is transparent.
    """
    if GraphRAGConfig.USE_INCREMENTAL_GRAPH:
        await set_graph_delta(tenant_id, kb_id, embd_mdl, graph, change, callback)
    else:
        await _set_graph_monolithic(tenant_id, kb_id, embd_mdl, graph, change, callback)


async def get_graph(tenant_id, kb_id, exclude_rebuild=None):
    """Dual-path graph loader.

    When ``USE_INCREMENTAL_GRAPH`` is enabled we first try to assemble the
    graph from indexed ``entity`` / ``relation`` documents.  If that yields an
    empty graph or raises, we automatically fall back to the legacy monolithic
    JSON blob so the pipeline never breaks.
    """
    if GraphRAGConfig.USE_INCREMENTAL_GRAPH:
        try:
            graph = await get_graph_from_index(tenant_id, kb_id)
            if graph is not None and len(graph.nodes) > 0:
                return graph
            logging.warning(
                "get_graph_from_index returned empty for kb=%s; falling back to JSON",
                kb_id,
            )
        except Exception as exc:
            logging.error(
                "get_graph_from_index failed for kb=%s: %s; falling back to JSON",
                kb_id, exc,
            )

    return await _utils.get_graph_from_json(tenant_id, kb_id, exclude_rebuild)


async def _does_graph_contains_legacy(tenant_id, kb_id, doc_id):
    """Legacy 2× round-trip fallback used when combined bool.should is unavailable."""
    fields = ["source_id"]
    condition = {
        "knowledge_graph_kwd": ["graph"],
        "removed_kwd": "N",
    }
    res = await thread_pool_exec(
        settings.docStoreConn.search,
        fields, [], condition, [], OrderByExpr(),
        0, 1, search.index_name(tenant_id), [kb_id],
    )
    fields2 = settings.docStoreConn.get_fields(res, fields)
    for chunk_id in fields2.keys():
        graph_doc_ids = set(fields2[chunk_id]["source_id"])
        if doc_id in graph_doc_ids:
            return True

    if GraphRAGConfig.USE_INCREMENTAL_GRAPH:
        condition = {
            "knowledge_graph_kwd": ["subgraph"],
            "source_id": doc_id,
        }
        res = await thread_pool_exec(
            settings.docStoreConn.search,
            fields, [], condition, [], OrderByExpr(),
            0, 1, search.index_name(tenant_id), [kb_id],
        )
        fields2 = settings.docStoreConn.get_fields(res, fields)
        return len(fields2) > 0

    return False


async def does_graph_contains(tenant_id, kb_id, doc_id):
    # Phase 2.4: single ES round-trip via bool.should, avoiding the previous
    # 2× round-trip pattern (one for monolithic graph JSON, one for per-doc subgraph).
    # Two clauses cover both worlds:
    #   1) monolithic path: knowledge_graph_kwd=graph AND source_id contains doc_id
    #   2) incremental path: knowledge_graph_kwd=subgraph AND source_id=doc_id
    # Backends without native should (Infinity/OceanBase) keep the legacy 2× path.
    index_name = search.index_name(tenant_id)

    if GraphRAGConfig.USE_INCREMENTAL_GRAPH and hasattr(settings.docStoreConn, "os"):
        raw = settings.docStoreConn.os
        body = {
            "size": 1,
            "query": {
                "bool": {
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {"term": {"knowledge_graph_kwd": "graph"}},
                                    {"term": {"removed_kwd": "N"}},
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {"term": {"knowledge_graph_kwd": "subgraph"}},
                                    {"term": {"source_id": doc_id}},
                                ]
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                    "filter": [{"term": {"kb_id": kb_id}}],
                }
            },
        }
        try:
            res = await thread_pool_exec(raw.search, index=index_name, body=body)
        except Exception as e:
            logging.warning("does_graph_contains combined query failed: %s; falling back to legacy", e)
            return await _does_graph_contains_legacy(tenant_id, kb_id, doc_id)

        # 必须仍然校验 monolithic chunk 是否真的包含 doc_id (因为 source_id 是 list 字段)
        hits = res.get("hits", {}).get("hits", [])
        for h in hits:
            kw = h.get("_source", {}).get("knowledge_graph_kwd")
            if kw == "subgraph":
                return True
            if kw == "graph":
                src = h.get("_source", {}).get("source_id") or []
                if doc_id in (src if isinstance(src, list) else [src]):
                    return True
        return False

    # 官方默认路径(无增量):只查 monolithic 一路
    return await _does_graph_contains_legacy(tenant_id, kb_id, doc_id)
