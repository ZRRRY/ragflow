# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""GraphRAG custom helpers for document deletion.

Isolates Phase 2.5 incremental GraphRAG changes from
``api/db/services/document_service.py`` so that the official file can be
restored to the v0.26.0 baseline.
"""

import logging

from common import settings
from common.doc_store.doc_store_base import OrderByExpr
from rag.graphrag.config import GraphRAGConfig

logger = logging.getLogger(__name__)


def cleanup_knowledge_graph_references(doc, chunk_index_name):
    """Cleanup knowledge graph references when deleting a document.

    Dispatches between the official v0.26.0 behavior and the custom incremental
    GraphRAG behavior based on ``GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE``.
    When incremental GraphRAG is disabled, the official logic is executed
    verbatim to keep the default behavior unchanged.
    """
    # Documents that were never parsed have no chunks and no graph data;
    # skip the ES round-trips entirely.
    if not getattr(doc, "chunk_num", 0):
        return
    if GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE:
        _cleanup_knowledge_graph_references_incremental(doc, chunk_index_name)
    else:
        _cleanup_knowledge_graph_references_official(doc, chunk_index_name)


def _cleanup_knowledge_graph_references_official(doc, chunk_index_name):
    """Official v0.26.0 cleanup logic (kept verbatim)."""
    graph_source = settings.docStoreConn.get_fields(
        settings.docStoreConn.search(
            ["source_id"],
            [],
            {"kb_id": doc.kb_id, "knowledge_graph_kwd": ["graph"]},
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
            {"kb_id": doc.kb_id, "knowledge_graph_kwd": ["entity", "relation", "graph", "subgraph", "community_report"], "source_id": doc.id},
            {"remove": {"source_id": doc.id}},
            chunk_index_name,
            doc.kb_id,
        )
        settings.docStoreConn.update({"kb_id": doc.kb_id, "knowledge_graph_kwd": ["graph"]}, {"removed_kwd": "Y"}, chunk_index_name, doc.kb_id)
        settings.docStoreConn.delete(
            {"kb_id": doc.kb_id, "knowledge_graph_kwd": ["entity", "relation", "graph", "subgraph", "community_report"], "must_not": {"exists": "source_id"}},
            chunk_index_name,
            doc.kb_id,
        )


def _cleanup_knowledge_graph_references_incremental(doc, chunk_index_name):
    """Phase 2.5 incremental GraphRAG cleanup logic.

    Under incremental GraphRAG (delta graph or incremental merge), eagerly
    delete the document's subgraph so intermediate products do not linger.
    """
    # Safety: source_id is a list field, and term-filter behavior on
    # missing/typed values is backend-dependent. Resolve matching subgraph
    # chunk IDs explicitly so we never accidentally delete every subgraph in
    # the KB (or none of them).
    subgraph_res = settings.docStoreConn.search(
        ["_id"],
        [],
        {"kb_id": doc.kb_id, "knowledge_graph_kwd": ["subgraph"], "source_id": doc.id},
        [],
        OrderByExpr(),
        0,
        10000,
        chunk_index_name,
        [doc.kb_id],
    )
    subgraph_ids = settings.docStoreConn.get_doc_ids(subgraph_res)
    if subgraph_ids:
        settings.docStoreConn.delete(
            {"id": subgraph_ids},
            chunk_index_name,
            doc.kb_id,
        )

    graph_source = settings.docStoreConn.get_fields(
        settings.docStoreConn.search(
            ["source_id"],
            [],
            {"kb_id": doc.kb_id, "knowledge_graph_kwd": ["graph"]},
            [],
            OrderByExpr(),
            0,
            100,
            chunk_index_name,
            [doc.kb_id],
        ),
        ["source_id"],
    )
    doc_in_graph_source = any(
        doc.id in row.get("source_id", [])
        for row in graph_source.values()
    )
    if doc_in_graph_source:
        kg_types = ["entity", "relation", "graph", "community_report"]
        settings.docStoreConn.update(
            {"kb_id": doc.kb_id, "knowledge_graph_kwd": kg_types, "source_id": doc.id},
            {"remove": {"source_id": doc.id}},
            chunk_index_name,
            doc.kb_id,
        )
        settings.docStoreConn.update({"kb_id": doc.kb_id, "knowledge_graph_kwd": ["graph"]}, {"removed_kwd": "Y"}, chunk_index_name, doc.kb_id)
        settings.docStoreConn.delete(
            {"kb_id": doc.kb_id, "knowledge_graph_kwd": kg_types, "must_not": {"exists": "source_id"}},
            chunk_index_name,
            doc.kb_id,
        )
