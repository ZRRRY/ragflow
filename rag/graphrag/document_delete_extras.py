# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""GraphRAG custom helpers for document deletion.

Isolates Phase 2.5 incremental GraphRAG changes from
``api/db/services/document_service.py`` so that the official file can be
restored to the v0.26.0 baseline.
"""

import json
import logging

from common import settings
from common.doc_store.doc_store_base import OrderByExpr
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.storage_scope import graph_condition, graph_query

logger = logging.getLogger(__name__)


def cleanup_knowledge_graph_references(doc, chunk_index_name):
    """Cleanup knowledge graph references when deleting a document.

    Dispatches between the official v0.26.0 behavior and the custom incremental
    GraphRAG behavior based on ``GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE``.
    Both paths exclude compilation records, which share the graph type field.
    """
    # Preserve the official chunk-count shortcut only for the legacy path;
    # delta records can exist even when the document counter is zero.
    if not GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE and not getattr(doc, "chunk_num", 0):
        return
    if GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE:
        _cleanup_knowledge_graph_references_incremental(doc, chunk_index_name)
    else:
        _cleanup_knowledge_graph_references_official(doc, chunk_index_name)


def _cleanup_knowledge_graph_references_official(doc, chunk_index_name):
    """Legacy cleanup, restricted to GraphRAG records."""
    graph_source = settings.docStoreConn.get_fields(
        settings.docStoreConn.search(
            ["source_id"],
            [],
            graph_condition({"kb_id": doc.kb_id, "knowledge_graph_kwd": ["graph"]}),
            [],
            OrderByExpr(),
            0,
            1,
            chunk_index_name,
            [doc.kb_id],
        ),
        ["source_id"],
    )
    if len(graph_source) > 0 and doc.id in list(graph_source.values())[0]["source_id"]:
        settings.docStoreConn.update(
            graph_condition({"kb_id": doc.kb_id, "knowledge_graph_kwd": ["entity", "relation", "graph", "subgraph", "community_report"], "source_id": doc.id}),
            {"remove": {"source_id": doc.id}},
            chunk_index_name,
            doc.kb_id,
        )
        settings.docStoreConn.update(graph_condition({"kb_id": doc.kb_id, "knowledge_graph_kwd": ["graph"]}), {"removed_kwd": "Y"}, chunk_index_name, doc.kb_id)
        settings.docStoreConn.delete(
            graph_condition({"kb_id": doc.kb_id, "knowledge_graph_kwd": ["entity", "relation", "graph", "subgraph", "community_report"], "must_not": {"exists": "source_id"}}),
            chunk_index_name,
            doc.kb_id,
        )


def _graph_records_with_source(doc, chunk_index_name, kg_types):
    """Read only affected records so their embedded provenance can be updated too."""
    conn = settings.docStoreConn
    filters = graph_condition({"knowledge_graph_kwd": kg_types, "source_id": doc.id})
    fields = ["content_with_weight", "source_id", "knowledge_graph_kwd"]
    if hasattr(conn, "search_with_scroll"):
        result = conn.search_with_scroll(
            chunk_index_name,
            graph_query({"query": {"bool": {"filter": [
                {"terms": {"kb_id": [doc.kb_id]}},
                {"terms": {"knowledge_graph_kwd": kg_types}},
                {"term": {"source_id": doc.id}},
            ]}}}),
            fields,
        )
        if result.get("_truncated"):
            raise RuntimeError(f"Graph cleanup result was truncated for document {doc.id}")
        return conn.get_fields(result, fields)

    result = conn.search(fields, [], filters, [], OrderByExpr(), 0, 10000, chunk_index_name, [doc.kb_id])
    ids = conn.get_doc_ids(result)
    if conn.get_total(result) > len(ids) or len(ids) >= 10000:
        raise RuntimeError(f"Graph cleanup result exceeded the backend limit for document {doc.id}")
    rows = conn.get_fields(result, fields)
    return {cid: rows[cid] for cid in ids}


def _cleanup_knowledge_graph_references_incremental(doc, chunk_index_name):
    """Clean by provenance even when delta storage has no graph JSON record."""
    conn = settings.docStoreConn
    # Keep content_with_weight and the indexed source_id in agreement. Otherwise
    # a later merge would resurrect the deleted document from stale JSON.
    kg_types = ["entity", "relation", "graph", "community_report"]
    affected = _graph_records_with_source(doc, chunk_index_name, kg_types)
    emptied_ids = []
    for cid, fields in affected.items():
        sources = fields.get("source_id", [])
        if not isinstance(sources, list):
            raise ValueError(f"Invalid indexed graph provenance for record {cid}")
        if doc.id in sources and not [source for source in sources if source != doc.id]:
            emptied_ids.append(cid)
        metadata = json.loads(fields["content_with_weight"])
        if not isinstance(metadata, dict):
            raise ValueError(f"Invalid graph metadata for record {cid}")
        sources = metadata.get("source_id", [])
        if not isinstance(sources, list):
            raise ValueError(f"Invalid graph provenance for record {cid}")
        if doc.id not in sources:
            continue
        metadata["source_id"] = [source for source in fields.get("source_id", []) if source != doc.id]
        result = conn.update(
            {"id": cid}, {"content_with_weight": json.dumps(metadata, ensure_ascii=False)},
            chunk_index_name, doc.kb_id,
        )
        if result is False:
            raise RuntimeError(f"Failed to update graph provenance for record {cid}")

    # delete_by_query applies to all matching checkpoints without a scroll cap.
    conn.delete(
        graph_condition({"knowledge_graph_kwd": ["subgraph", "merge_state"], "source_id": doc.id}),
        chunk_index_name, doc.kb_id,
    )
    result = conn.update(
        graph_condition({"knowledge_graph_kwd": kg_types, "source_id": doc.id}),
        {"remove": {"source_id": doc.id}},
        chunk_index_name, doc.kb_id,
    )
    if result is False:
        raise RuntimeError(f"Failed to remove graph provenance for document {doc.id}")
    # A legacy JSON graph may still exist after enabling incremental mode.
    result = conn.update(graph_condition({"knowledge_graph_kwd": ["graph"]}), {"removed_kwd": "Y"}, chunk_index_name, doc.kb_id)
    if result is False:
        raise RuntimeError(f"Failed to mark the legacy graph removed for document {doc.id}")
    for offset in range(0, len(emptied_ids), 1000):
        conn.delete(
            graph_condition({"id": emptied_ids[offset:offset + 1000], "knowledge_graph_kwd": kg_types}),
            chunk_index_name, doc.kb_id,
        )
