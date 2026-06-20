#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
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
"""Custom GraphRAG incremental / optimization implementations.

This module is imported lazily by ``rag.graphrag.general.index_patch`` only
when at least one GraphRAG feature flag is enabled. It keeps the official
``rag/graphrag/general/index.py`` file aligned with the upstream release.
"""

import asyncio
import json
import logging
import os
import re
from collections import defaultdict

import networkx as nx

from api.db.services.document_service import DocumentService
from common import settings
from common.connection_utils import timeout
from common.doc_store.doc_store_base import OrderByExpr
from common.exceptions import TaskCanceledException
from common.misc_utils import thread_pool_exec
from rag.graphrag.checkpoints import (
    COMMUNITY_CHECKPOINT,
    cleanup_checkpoints,
    load_checkpoints,
    save_checkpoint,
)
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.entity_resolution import (
    EntityResolution,
    build_excluded_types,
    is_similarity_str,
)
from rag.graphrag.general.community_reports_extractor import CommunityReportsExtractor
from rag.graphrag.general.extractor import Extractor
from rag.graphrag.general.index import (
    _acquire_lock,
    _batch_chunk_token_size_config,
    _bounded_float_config,
    _bounded_int_config,
    _has_cancel_and_exit,
    _lock_acquire_timeout_config,
    _run_with_retry,
    _select_extractor,
    extract_community,
    load_subgraph_from_store,
    merge_subgraph,
    resolve_entities,
)
from rag.graphrag.phase_markers import (
    PHASE_COMMUNITY,
    PHASE_RESOLUTION,
    clear_phase_markers,
    has_phase_marker,
    set_phase_marker,
)
from rag.graphrag.utils import (
    GRAPH_FIELD_SEP,
    GraphChange,
    chunk_id,
    get_from_to,
    insert_chunks_bounded,
    tidy_graph,
)
from rag.graphrag.utils_extras import (
    does_graph_contains,
    fetch_node_vectors,
    get_graph,
    get_graph_from_index,
    is_doc_merged,
    query_existing_entities,
    query_existing_relations,
    query_node_relations,
    set_graph,
    write_merge_state,
)
from rag.nlp import rag_tokenizer, search
from rag.utils.redis_conn import REDIS_CONN, RedisDistributedLock

DEFAULT_GRAPHRAG_BATCH_CHUNK_TOKEN_SIZE = 4096
MIN_GRAPHRAG_BATCH_CHUNK_TOKEN_SIZE = 512
MAX_GRAPHRAG_BATCH_CHUNK_TOKEN_SIZE = 8196
# The defaults below are intentionally identical to the official v0.26.x values;
# environment variables allow production tuning without editing source files.
DEFAULT_GRAPHRAG_RETRY_ATTEMPTS = int(os.environ.get("GRAPHRAG_RETRY_ATTEMPTS", "2"))
DEFAULT_GRAPHRAG_RETRY_BACKOFF_SECONDS = float(
    os.environ.get("GRAPHRAG_RETRY_BACKOFF_SECONDS", "2.0")
)
DEFAULT_GRAPHRAG_RETRY_BACKOFF_MAX_SECONDS = float(
    os.environ.get("GRAPHRAG_RETRY_BACKOFF_MAX_SECONDS", "60.0")
)
DEFAULT_GRAPHRAG_BUILD_SUBGRAPH_TIMEOUT_PER_CHUNK_SECONDS = int(
    os.environ.get("GRAPHRAG_BUILD_SUBGRAPH_TIMEOUT_PER_CHUNK_SECONDS", "300")
)
DEFAULT_GRAPHRAG_BUILD_SUBGRAPH_MIN_TIMEOUT_SECONDS = 600
DEFAULT_GRAPHRAG_MERGE_TIMEOUT_SECONDS = int(
    os.environ.get("GRAPHRAG_MERGE_TIMEOUT_SECONDS", "180")
)
DEFAULT_GRAPHRAG_RESOLUTION_TIMEOUT_SECONDS = 1800
DEFAULT_GRAPHRAG_COMMUNITY_TIMEOUT_SECONDS = 1800
DEFAULT_GRAPHRAG_LOCK_ACQUIRE_TIMEOUT_SECONDS = 600


def _select_extractor_type(graphrag_config: dict):
    return graphrag_config.get("method", "light")


def _min_timeout_config(value: int, default: int, minimum: int = 60) -> int:
    """Return a timeout value with a sensible lower bound.

    A value of 0 or extremely small timeouts would otherwise be treated as
    "no timeout" by ``_run_with_retry`` and could hang indefinitely. This
    helper clamps the timeout to at least ``minimum`` seconds.
    """
    if value is None or value <= 0:
        return default if default >= minimum else minimum
    return max(value, minimum)


# Phase 4.3: record lock wait/held durations in a Redis hash so external
# dashboards / exporters can scrape them.
_LOCK_METRICS_KEY_TMPL = "graphrag:lock_metrics:{}"
_LOCK_METRICS_TTL = 3600


async def _record_lock_metric(
    kb_id: str,
    lock_name: str,
    *,
    waited_seconds: float | None = None,
    held_seconds: float | None = None,
) -> None:
    """Persist a lock event to Redis. Failures are non-fatal."""

    def _write(redis, key: str, field: str, value: str, ttl: int):
        redis.hset(key, field, value)
        redis.expire(key, ttl)

    try:
        import time

        key = _LOCK_METRICS_KEY_TMPL.format(kb_id)
        value = f"{waited_seconds or 0:.3f},{held_seconds or 0:.3f},{time.time():.0f}"
        if REDIS_CONN.REDIS is not None:
            await thread_pool_exec(
                _write, REDIS_CONN.REDIS, key, lock_name, value, _LOCK_METRICS_TTL
            )
    except Exception:
        logging.exception("record_lock_metric write failed (non-fatal)")


async def run_graphrag_for_kb(
    row: dict,
    doc_ids: list[str],
    language: str,
    kb_parser_config: dict,
    chat_model,
    embedding_model,
    callback,
    *,
    with_resolution: bool = True,
    with_community: bool = True,
    max_parallel_docs: int = GraphRAGConfig.GRAPHRAG_MAX_PARALLEL_DOCS,
) -> dict:
    tenant_id, kb_id = row["tenant_id"], row["kb_id"]
    task_id = row["id"]
    start = asyncio.get_running_loop().time()
    fields_for_chunks = ["content_with_weight", "doc_id"]
    graphrag_config = kb_parser_config.get("graphrag", {})
    kb_entity_types = graphrag_config.get("entity_types", []) or []
    batch_chunk_token_size = _batch_chunk_token_size_config(
        graphrag_config, "batch_chunk_token_size", DEFAULT_GRAPHRAG_BATCH_CHUNK_TOKEN_SIZE
    )
    retry_attempts = _bounded_int_config(
        graphrag_config, "retry_attempts", DEFAULT_GRAPHRAG_RETRY_ATTEMPTS, 1, 10
    )
    retry_backoff_seconds = _bounded_float_config(
        graphrag_config, "retry_backoff_seconds", DEFAULT_GRAPHRAG_RETRY_BACKOFF_SECONDS, 0.0, 600.0
    )
    retry_backoff_max_seconds = _bounded_float_config(
        graphrag_config, "retry_backoff_max_seconds", DEFAULT_GRAPHRAG_RETRY_BACKOFF_MAX_SECONDS, 0.0, 3600.0
    )
    build_subgraph_retry_attempts = _bounded_int_config(
        graphrag_config, "build_subgraph_retry_attempts", retry_attempts, 1, 10
    )
    merge_retry_attempts = _bounded_int_config(
        graphrag_config, "merge_retry_attempts", retry_attempts, 1, 10
    )
    resolution_retry_attempts = _bounded_int_config(
        graphrag_config, "resolution_retry_attempts", retry_attempts, 1, 10
    )
    community_retry_attempts = _bounded_int_config(
        graphrag_config, "community_retry_attempts", retry_attempts, 1, 10
    )
    build_subgraph_timeout_per_chunk_seconds = _bounded_int_config(
        graphrag_config,
        "build_subgraph_timeout_per_chunk_seconds",
        DEFAULT_GRAPHRAG_BUILD_SUBGRAPH_TIMEOUT_PER_CHUNK_SECONDS,
        1,
        86400,
    )
    build_subgraph_min_timeout_seconds = _bounded_int_config(
        graphrag_config,
        "build_subgraph_min_timeout_seconds",
        DEFAULT_GRAPHRAG_BUILD_SUBGRAPH_MIN_TIMEOUT_SECONDS,
        1,
        86400,
    )
    merge_timeout_seconds = _min_timeout_config(
        _bounded_int_config(
            graphrag_config, "merge_timeout_seconds", DEFAULT_GRAPHRAG_MERGE_TIMEOUT_SECONDS, 0, 86400
        ),
        DEFAULT_GRAPHRAG_MERGE_TIMEOUT_SECONDS,
    )
    resolution_timeout_seconds = _min_timeout_config(
        _bounded_int_config(
            graphrag_config, "resolution_timeout_seconds", DEFAULT_GRAPHRAG_RESOLUTION_TIMEOUT_SECONDS, 0, 86400
        ),
        DEFAULT_GRAPHRAG_RESOLUTION_TIMEOUT_SECONDS,
    )
    community_timeout_seconds = _min_timeout_config(
        _bounded_int_config(
            graphrag_config, "community_timeout_seconds", DEFAULT_GRAPHRAG_COMMUNITY_TIMEOUT_SECONDS, 0, 86400
        ),
        DEFAULT_GRAPHRAG_COMMUNITY_TIMEOUT_SECONDS,
    )
    lock_acquire_timeout_seconds = _lock_acquire_timeout_config(graphrag_config)

    if not doc_ids:
        logging.info("Fetching all docs for %s", kb_id)
        docs, _ = DocumentService.get_by_kb_id(
            kb_id=kb_id,
            page_number=0,
            items_per_page=0,
            orderby="create_time",
            desc=False,
            keywords="",
            run_status=[],
            types=[],
            suffix=[],
        )
        doc_ids = [doc["id"] for doc in docs]

    doc_ids = list(dict.fromkeys(doc_ids))
    if not doc_ids:
        callback(msg=f"[GraphRAG] dataset:{kb_id} has no processable doc_id.")
        return {"ok_docs": [], "failed_docs": [], "total_docs": 0, "total_chunks": 0, "seconds": 0.0}
    else:
        callback(msg=f"[GraphRAG] dataset:{kb_id} has {len(doc_ids)} documents to process.")

    # Phase 6: conditional cleanup based on environment switches
    if not GraphRAGConfig.KEEP_SUBGRAPH:
        try:
            await thread_pool_exec(
                settings.docStoreConn.delete,
                {"kb_id": kb_id, "knowledge_graph_kwd": ["subgraph"]},
                search.index_name(tenant_id),
                kb_id,
            )
            callback(msg=f"[GraphRAG] Cleared previous subgraph checkpoints for dataset:{kb_id}")
        except Exception as e:
            logging.warning(
                "[GraphRAG] Failed to clear previous subgraph checkpoints for dataset %s: %s",
                kb_id,
                e,
            )

    if not GraphRAGConfig.KEEP_MERGE:
        try:
            await thread_pool_exec(
                settings.docStoreConn.delete,
                {"kb_id": kb_id, "knowledge_graph_kwd": ["graph", "entity", "relation", "merge_state"]},
                search.index_name(tenant_id),
                kb_id,
            )
            callback(msg=f"[GraphRAG] Cleared previous graph/entity/relation/merge_state for dataset:{kb_id}")
        except Exception as e:
            logging.warning(
                "[GraphRAG] Failed to clear previous merge results for dataset %s: %s",
                kb_id,
                e,
            )

    if not GraphRAGConfig.KEEP_RESOLUTION:
        clear_phase_markers(kb_id, (PHASE_RESOLUTION, PHASE_COMMUNITY))
        callback(msg=f"[GraphRAG] Cleared phase markers to force rerun resolution/community for dataset:{kb_id}")

    def load_doc_chunks(doc_id: str) -> list[str]:
        from common.token_utils import num_tokens_from_string

        chunks = []
        current_chunk = ""

        raw_chunks = list(
            settings.retriever.chunk_list(
                doc_id,
                tenant_id,
                [kb_id],
                fields=fields_for_chunks,
                sort_by_position=True,
                retrieve_all=True,
            )
        )

        callback(msg=f"[GraphRAG] chunk_list returned {len(raw_chunks)} raw chunks for doc:{doc_id}")

        contents = [
            content for chunk in raw_chunks if (content := chunk.get("content_with_weight", ""))
        ]
        # For NER-based extraction, no need to batch extract entity and relation
        if _select_extractor_type(graphrag_config) == "ner":
            return contents

        for content in contents:
            if num_tokens_from_string(current_chunk + content) < batch_chunk_token_size:
                current_chunk += content
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = content

        if current_chunk:
            chunks.append(current_chunk)

        callback(
            msg=f"[GraphRAG] chunk_list combine {len(raw_chunks)} raw chunks to {len(chunks)} chunks for LLM extraction for doc:{doc_id}"
        )
        return chunks

    total_chunks = 0

    semaphore = asyncio.Semaphore(max_parallel_docs)

    subgraphs: dict[str, object] = {}
    failed_docs: list[tuple[str, str]] = []  # (doc_id, error)

    async def build_one(doc_id: str):
        nonlocal total_chunks

        _has_cancel_and_exit(task_id, f"Task {task_id} cancelled, stopping execution.", callback)

        kg_extractor = _select_extractor(graphrag_config)

        async with semaphore:
            _has_cancel_and_exit(
                task_id, f"Task {task_id} cancelled before loading checkpoint for doc {doc_id}.", callback
            )
            # Subgraph checkpoints are only used in incremental paths.
            existing_sg = None
            if GraphRAGConfig.USE_INCREMENTAL_GRAPH or GraphRAGConfig.USE_INCREMENTAL_MERGE:
                existing_sg = await load_subgraph_from_store(tenant_id, kb_id, doc_id)
            if existing_sg:
                subgraphs[doc_id] = existing_sg
                callback(msg=f"[GraphRAG] doc:{doc_id} subgraph found in store, skipping LLM extraction.")
                return
            try:
                _has_cancel_and_exit(
                    task_id, f"Task {task_id} cancelled before loading chunks for doc {doc_id}.", callback
                )
                chunks = load_doc_chunks(doc_id)
                total_chunks += len(chunks)
                if not chunks:
                    callback(msg=f"[GraphRAG] doc:{doc_id} has no available chunks, skip generation.")
                    return

                build_subgraph_timeout_seconds = max(
                    build_subgraph_min_timeout_seconds,
                    len(chunks) * build_subgraph_timeout_per_chunk_seconds,
                )
                label = f"build_subgraph doc:{doc_id}"
                msg = f"[GraphRAG] {label}"
                callback(
                    msg=f"{msg} start (chunks={len(chunks)}, timeout={build_subgraph_timeout_seconds}s, attempts={build_subgraph_retry_attempts})"
                )

                try:
                    _, doc_obj = DocumentService.get_by_id(doc_id)
                    fallback_title = os.path.splitext(doc_obj.name or "")[0] if doc_obj else ""
                except Exception:
                    fallback_title = ""

                _has_cancel_and_exit(
                    task_id, f"Task {task_id} cancelled before subgraph generation for doc {doc_id}.", callback
                )
                try:
                    async def build_subgraph_attempt():
                        if GraphRAGConfig.USE_INCREMENTAL_GRAPH or GraphRAGConfig.USE_INCREMENTAL_MERGE:
                            checkpoint_sg = await load_subgraph_from_store(tenant_id, kb_id, doc_id)
                            if checkpoint_sg:
                                callback(
                                    msg=f"[GraphRAG] doc:{doc_id} subgraph found in store during retry, skipping LLM extraction."
                                )
                                return checkpoint_sg
                        return await generate_subgraph(
                            kg_extractor,
                            tenant_id,
                            kb_id,
                            doc_id,
                            chunks,
                            language,
                            kb_parser_config.get("graphrag", {}).get("entity_types", []),
                            chat_model,
                            embedding_model,
                            callback,
                            task_id=task_id,
                            fallback_title=fallback_title,
                        )

                    sg = await _run_with_retry(
                        label,
                        build_subgraph_attempt,
                        attempts=build_subgraph_retry_attempts,
                        timeout_seconds=build_subgraph_timeout_seconds,
                        backoff_seconds=retry_backoff_seconds,
                        backoff_max_seconds=retry_backoff_max_seconds,
                        callback=callback,
                        task_id=task_id,
                    )
                except asyncio.TimeoutError:
                    failed_docs.append((doc_id, f"timeout after {build_subgraph_timeout_seconds}s"))
                    callback(msg=f"{msg} FAILED: timeout after {build_subgraph_timeout_seconds}s")
                    return
                if sg:
                    subgraphs[doc_id] = sg
                    callback(msg=f"{msg} done")
                else:
                    failed_docs.append((doc_id, "subgraph is empty"))
                    callback(msg=f"{msg} empty")
            except TaskCanceledException as canceled:
                callback(msg=f"[GraphRAG] build_subgraph doc:{doc_id} FAILED: {canceled}")
                raise
            except Exception as e:
                failed_docs.append((doc_id, repr(e)))
                callback(msg=f"[GraphRAG] build_subgraph doc:{doc_id} FAILED: {e!r}")

    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before processing documents.", callback)

    tasks = [asyncio.create_task(build_one(doc_id)) for doc_id in doc_ids]
    try:
        await asyncio.gather(*tasks, return_exceptions=False)
    except Exception as e:
        logging.error(f"Error in asyncio.gather: {e}")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    if total_chunks == 0 and not subgraphs:
        callback(msg=f"[GraphRAG] dataset:{kb_id} has no available chunks in all documents, skip.")
        return {
            "ok_docs": [],
            "failed_docs": [(doc_id, "no available chunks") for doc_id in doc_ids],
            "total_docs": len(doc_ids),
            "total_chunks": 0,
            "seconds": 0.0,
        }

    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled after document processing.", callback)

    ok_docs = [d for d in doc_ids if d in subgraphs]
    final_graph = None

    resolution_pending = with_resolution and not has_phase_marker(kb_id, PHASE_RESOLUTION)
    community_pending = with_community and not has_phase_marker(kb_id, PHASE_COMMUNITY)

    if not ok_docs and not resolution_pending and not community_pending:
        callback(msg=f"[GraphRAG] dataset:{kb_id} no subgraphs to merge and no phases pending, end.")
        now = asyncio.get_running_loop().time()
        return {
            "ok_docs": [],
            "failed_docs": failed_docs,
            "total_docs": len(doc_ids),
            "total_chunks": total_chunks,
            "seconds": now - start,
        }

    kb_lock = RedisDistributedLock(
        f"graphrag_task_{kb_id}", lock_value=f"batch_merge:{task_id}", timeout=1200
    )
    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before acquiring merge lock.", callback)
    lock_acquire_t0 = asyncio.get_running_loop().time()
    await _acquire_lock(kb_lock, "merge lock", lock_acquire_timeout_seconds, callback, task_id)
    lock_held_t0 = asyncio.get_running_loop().time()
    try:
        await _record_lock_metric(
            kb_id=kb_id,
            lock_name="batch_merge",
            waited_seconds=lock_held_t0 - lock_acquire_t0,
            held_seconds=None,
        )
    except Exception:
        logging.exception("record_lock_metric failed (non-fatal)")
    callback(msg=f"[GraphRAG] dataset:{kb_id} merge lock acquired")

    try:
        _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before merging subgraphs.", callback)

        union_nodes: set = set()

        for doc_id in ok_docs:
            _has_cancel_and_exit(
                task_id, f"Task {task_id} cancelled before merging subgraph for doc {doc_id}.", callback
            )
            sg = subgraphs[doc_id]
            union_nodes.update(set(sg.nodes()))

            if GraphRAGConfig.USE_INCREMENTAL_MERGE:
                await write_merge_state(
                    tenant_id,
                    kb_id,
                    doc_id,
                    state="merging",
                    expected_nodes=len(sg.nodes),
                    expected_edges=len(sg.edges),
                )

            try:
                async def merge_subgraph_attempt():
                    if GraphRAGConfig.USE_INCREMENTAL_MERGE:
                        # Incremental path: rely on merge_state table to detect duplicates.
                        if await is_doc_merged(tenant_id, kb_id, doc_id):
                            callback(
                                msg=f"[GraphRAG] merge_subgraph doc:{doc_id} already merged, skipping retry."
                            )
                            return None
                    else:
                        # Official full-graph path: load global graph and check source_id.
                        current_graph = await get_graph(tenant_id, kb_id)
                        if current_graph and doc_id in current_graph.graph.get("source_id", []):
                            callback(
                                msg=f"[GraphRAG] merge_subgraph doc:{doc_id} already merged, skipping retry."
                            )
                            return current_graph
                    return await merge_subgraph(
                        tenant_id,
                        kb_id,
                        doc_id,
                        sg,
                        embedding_model,
                        callback,
                    )

                new_graph = await _run_with_retry(
                    f"merge_subgraph doc:{doc_id}",
                    merge_subgraph_attempt,
                    attempts=merge_retry_attempts,
                    timeout_seconds=merge_timeout_seconds,
                    backoff_seconds=retry_backoff_seconds,
                    backoff_max_seconds=retry_backoff_max_seconds,
                    callback=callback,
                    task_id=task_id,
                )
            except TaskCanceledException:
                raise
            except Exception as e:
                if GraphRAGConfig.USE_INCREMENTAL_MERGE:
                    await write_merge_state(
                        tenant_id,
                        kb_id,
                        doc_id,
                        state="failed",
                        expected_nodes=len(sg.nodes),
                        expected_edges=len(sg.edges),
                        extra={"error": repr(e)},
                    )
                failed_docs.append((doc_id, f"merge failed: {e!r}"))
                callback(msg=f"[GraphRAG] merge_subgraph doc:{doc_id} FAILED: {e!r}")
                raise

            if new_graph is not None:
                final_graph = new_graph
                if GraphRAGConfig.USE_INCREMENTAL_MERGE:
                    await write_merge_state(
                        tenant_id,
                        kb_id,
                        doc_id,
                        state="merged",
                        expected_nodes=len(sg.nodes),
                        expected_edges=len(sg.edges),
                    )

        if ok_docs and final_graph is None:
            callback(msg=f"[GraphRAG] dataset:{kb_id} merge finished (no in-memory graph returned).")
        elif ok_docs:
            callback(msg=f"[GraphRAG] dataset:{kb_id} merge finished, graph ready.")
            clear_phase_markers(kb_id)
            resolution_pending = with_resolution
            community_pending = with_community
            callback(msg=f"[GraphRAG] dataset:{kb_id} cleared phase markers after merge.")
    finally:
        try:
            _held = asyncio.get_running_loop().time() - lock_held_t0
            await _record_lock_metric(
                kb_id=kb_id,
                lock_name="batch_merge",
                waited_seconds=None,
                held_seconds=_held,
            )
        except Exception:
            logging.exception("record_lock_metric(held) failed (non-fatal)")
        kb_lock.release()

    if not with_resolution and not with_community:
        now = asyncio.get_running_loop().time()
        callback(msg=f"[GraphRAG] KB merge done in {now - start:.2f}s. ok={len(ok_docs)} / total={len(doc_ids)}")
        return {
            "ok_docs": ok_docs,
            "failed_docs": failed_docs,
            "total_docs": len(doc_ids),
            "total_chunks": total_chunks,
            "seconds": now - start,
        }

    if not resolution_pending and not community_pending:
        now = asyncio.get_running_loop().time()
        callback(msg=f"[GraphRAG] dataset:{kb_id} all requested phases already complete; nothing to do.")
        return {
            "ok_docs": ok_docs,
            "failed_docs": failed_docs,
            "total_docs": len(doc_ids),
            "total_chunks": total_chunks,
            "seconds": now - start,
        }

    if GraphRAGConfig.USE_ASYNC_KG_PHASES:
        queue_payload = {
            "tenant_id": tenant_id,
            "kb_id": kb_id,
            "task_id": row["id"],
            "with_resolution": with_resolution and resolution_pending,
            "with_community": with_community and community_pending,
            "kb_task_llm_id": row.get("llm_id"),
            "task_language": language,
        }
        ok = REDIS_CONN.queue_product(GraphRAGConfig.KG_POSTPROCESS_QUEUE, queue_payload)
        if ok:
            logging.info(
                "[GraphRAG] kb:%s queued resolution/community to %s",
                kb_id,
                GraphRAGConfig.KG_POSTPROCESS_QUEUE,
            )
            now = asyncio.get_running_loop().time()
            return {
                "ok_docs": ok_docs,
                "failed_docs": failed_docs,
                "total_docs": len(doc_ids),
                "total_chunks": total_chunks,
                "seconds": now - start,
                "postprocess_queued": True,
            }
        else:
            logging.warning(
                "[GraphRAG] kb:%s FAILED to queue postprocess; falling back to synchronous execution.",
                kb_id,
            )

    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before resolution/community extraction.", callback)

    _has_cancel_and_exit(
        task_id, f"Task {task_id} cancelled before acquiring post-merge lock.", callback
    )
    await _acquire_lock(kb_lock, "post-merge lock", lock_acquire_timeout_seconds, callback, task_id)
    callback(msg=f"[GraphRAG] dataset:{kb_id} post-merge lock acquired for resolution/community")

    try:
        _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before resolution/community extraction.", callback)

        need_preload_full_graph = (
            final_graph is None
            and (
                (resolution_pending and not GraphRAGConfig.USE_INCREMENTAL_RESOLUTION)
                or (community_pending and not GraphRAGConfig.USE_ASYNC_COMMUNITY)
            )
        )
        if need_preload_full_graph:
            final_graph = await get_graph(tenant_id, kb_id)
            if final_graph is None:
                callback(
                    msg=f"[GraphRAG] dataset:{kb_id} no persisted graph found; cannot run resolution/community."
                )
                now = asyncio.get_running_loop().time()
                return {
                    "ok_docs": ok_docs,
                    "failed_docs": failed_docs,
                    "total_docs": len(doc_ids),
                    "total_chunks": total_chunks,
                    "seconds": now - start,
                }
            callback(msg=f"[GraphRAG] dataset:{kb_id} loaded persisted graph for resume.")

        subgraph_nodes = set()
        for sg in subgraphs.values():
            subgraph_nodes.update(set(sg.nodes()))
        if not subgraph_nodes:
            subgraph_nodes = set(final_graph.nodes())
            max_safe_resume_nodes = GraphRAGConfig.KG_MAX_SAFE_RESUME_NODES
            if len(subgraph_nodes) > max_safe_resume_nodes:
                callback(
                    msg=(
                        f"[GraphRAG] dataset:{kb_id} resume path would load "
                        f"{len(subgraph_nodes)} nodes for resolution, "
                        f"exceeds KG_MAX_SAFE_RESUME_NODES={max_safe_resume_nodes}. "
                        f"Please resubmit the task with all docs merged instead of resuming."
                    )
                )
                now = asyncio.get_running_loop().time()
                return {
                    "ok_docs": ok_docs,
                    "failed_docs": failed_docs,
                    "total_docs": len(doc_ids),
                    "total_chunks": total_chunks,
                    "seconds": now - start,
                }

        if resolution_pending:
            _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before entity resolution.", callback)

            async def run_resolution_attempt():
                graph_for_resolution = final_graph.copy()
                await resolve_entities(
                    graph_for_resolution,
                    subgraph_nodes,
                    tenant_id,
                    kb_id,
                    None,
                    chat_model,
                    embedding_model,
                    callback,
                    task_id=task_id,
                    entity_types=kb_entity_types,
                )
                return graph_for_resolution

            final_graph = await _run_with_retry(
                "entity resolution",
                run_resolution_attempt,
                attempts=resolution_retry_attempts,
                timeout_seconds=resolution_timeout_seconds,
                backoff_seconds=retry_backoff_seconds,
                backoff_max_seconds=retry_backoff_max_seconds,
                callback=callback,
                task_id=task_id,
            )
            set_phase_marker(kb_id, PHASE_RESOLUTION)
        elif with_resolution:
            callback(msg=f"[GraphRAG] dataset:{kb_id} resolution already completed previously, skipping.")

        if community_pending:
            _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before community extraction.", callback)

            async def run_community_attempt():
                await extract_community(
                    final_graph.copy(),
                    tenant_id,
                    kb_id,
                    None,
                    chat_model,
                    embedding_model,
                    callback,
                    task_id=task_id,
                )

            await _run_with_retry(
                "community extraction",
                run_community_attempt,
                attempts=community_retry_attempts,
                timeout_seconds=community_timeout_seconds,
                backoff_seconds=retry_backoff_seconds,
                backoff_max_seconds=retry_backoff_max_seconds,
                callback=callback,
                task_id=task_id,
            )
            set_phase_marker(kb_id, PHASE_COMMUNITY)
        elif with_community:
            callback(msg=f"[GraphRAG] dataset:{kb_id} community detection already completed previously, skipping.")
    finally:
        kb_lock.release()

    now = asyncio.get_running_loop().time()
    callback(
        msg=f"[GraphRAG] GraphRAG for KB {kb_id} done in {now - start:.2f} seconds. "
        f"ok={len(ok_docs)} failed={len(failed_docs)} total_docs={len(doc_ids)} total_chunks={total_chunks}"
    )
    return {
        "ok_docs": ok_docs,
        "failed_docs": failed_docs,  # [(doc_id, error), ...]
        "total_docs": len(doc_ids),
        "total_chunks": total_chunks,
        "seconds": now - start,
    }


def _extract_book_and_chapters(doc_id: str, chunks: list[str], fallback_title: str = ""):
    """Extract book title and chapter hierarchy from Markdown chunks."""
    book_title = None
    for chunk in chunks:
        m = re.search(r"^#\s+(.+)$", chunk, re.MULTILINE)
        if m:
            book_title = m.group(1).strip()
            break

    if not book_title:
        book_title = fallback_title
    if not book_title:
        logging.warning(
            "[ChapterGraph] doc_id=%s: no # title and no fallback_title, skipping chapter extraction",
            doc_id,
        )
        return None, [], [], []

    chapter_entities = []
    chapter_relations = []
    seen_chapters = set()
    chunk_chapters = []

    chapter_entities.append(
        {
            "entity_name": book_title,
            "entity_type": "书籍",
            "description": f"书籍《{book_title}》",
            "source_id": [doc_id],
        }
    )

    has_markdown_headers = False
    for chunk in chunks:
        chapters_in_chunk = []
        for m in re.finditer(r"^##\s+(.+)$", chunk, re.MULTILINE):
            has_markdown_headers = True
            chapter = m.group(1).strip()
            chapter_node_name = f"《{book_title}》{chapter}"
            chapters_in_chunk.append(chapter_node_name)
            if chapter_node_name not in seen_chapters:
                seen_chapters.add(chapter_node_name)
                chapter_entities.append(
                    {
                        "entity_name": chapter_node_name,
                        "entity_type": "章节",
                        "description": f"《{book_title}》的章节：{chapter}",
                        "source_id": [doc_id],
                    }
                )
                chapter_relations.append(
                    {
                        "src_id": book_title,
                        "tgt_id": chapter_node_name,
                        "description": f"《{book_title}》包含章节《{chapter}》",
                        "keywords": ["contains", "章节", "书籍"],
                        "weight": 1,
                        "source_id": [doc_id],
                    }
                )
        chunk_chapters.append(chapters_in_chunk)

    if not has_markdown_headers:
        for i, chunk in enumerate(chunks):
            if i == 0:
                continue
            first_line = ""
            for line in chunk.split("\n"):
                stripped = line.strip()
                if stripped:
                    first_line = stripped
                    break
            if first_line and len(first_line) <= 100:
                chapter_node_name = f"《{book_title}》{first_line}"
                chunk_chapters[i].append(chapter_node_name)
                if chapter_node_name not in seen_chapters:
                    seen_chapters.add(chapter_node_name)
                    chapter_entities.append(
                        {
                            "entity_name": chapter_node_name,
                            "entity_type": "章节",
                            "description": f"《{book_title}》的章节：{first_line}",
                            "source_id": [doc_id],
                        }
                    )
                    chapter_relations.append(
                        {
                            "src_id": book_title,
                            "tgt_id": chapter_node_name,
                            "description": f"《{book_title}》包含章节《{first_line}》",
                            "keywords": ["contains", "章节", "书籍"],
                            "weight": 1,
                            "source_id": [doc_id],
                        }
                    )
        if chunks and len(chunk_chapters) > 1:
            first_chapter = None
            for chapters in chunk_chapters[1:]:
                if chapters:
                    first_chapter = chapters[0]
                    break
            if first_chapter and not chunk_chapters[0]:
                chunk_chapters[0].append(first_chapter)

    logging.info(
        "[ChapterGraph] doc_id=%s: book_title=%s, chapters=%d, has_md_headers=%s",
        doc_id,
        book_title,
        len(seen_chapters),
        has_markdown_headers,
    )
    return book_title, chapter_entities, chapter_relations, chunk_chapters


def _link_entities_to_chapters(
    doc_id: str, chunks: list[str], entities: list[dict], chunk_chapters: list[list[str]]
):
    """Link entities to the chapters that mention them."""
    relations = []
    seen_pairs = set()
    chunk_texts = [chunk.lower() for chunk in chunks]

    for ent in entities:
        if ent.get("entity_type") in ("书籍", "章节"):
            continue
        ent_name = ent["entity_name"]
        ent_name_lower = ent_name.lower()
        matched = False
        for idx, text in enumerate(chunk_texts):
            if ent_name_lower in text:
                matched = True
                for chapter in chunk_chapters[idx]:
                    pair = (chapter, ent_name)
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    relations.append(
                        {
                            "src_id": chapter,
                            "tgt_id": ent_name,
                            "description": f"章节《{chapter}》涉及实体《{ent_name}》",
                            "keywords": ["involves", "章节", "实体"],
                            "weight": 1,
                            "source_id": [doc_id],
                        }
                    )
        if not matched:
            logging.debug(
                "[ChapterGraph] doc_id=%s: entity '%s' not found in any chunk text",
                doc_id,
                ent_name,
            )
    logging.info(
        "[ChapterGraph] doc_id=%s: entity_chapter_relations=%d, entities_processed=%d",
        doc_id,
        len(relations),
        len(entities),
    )
    return relations


async def generate_subgraph(
    extractor: Extractor,
    tenant_id: str,
    kb_id: str,
    doc_id: str,
    chunks: list[str],
    language,
    entity_types,
    llm_bdl,
    embed_bdl,
    callback,
    task_id: str = "",
    fallback_title: str = "",
):
    _has_cancel_and_exit(
        task_id, f"Task {task_id} cancelled during subgraph generation for doc {doc_id}.", callback
    )

    contains = await does_graph_contains(tenant_id, kb_id, doc_id)
    if contains:
        callback(msg=f"Graph already contains {doc_id}")
        return None
    _has_cancel_and_exit(
        task_id, f"Task {task_id} cancelled before extracting entities for doc {doc_id}.", callback
    )
    start = asyncio.get_running_loop().time()
    ext = extractor(
        llm_bdl,
        language=language,
        entity_types=entity_types,
    )
    llm_ents, llm_rels = await ext(doc_id, chunks, callback, task_id=task_id)

    chapter_ents, chapter_rels, chunk_chapters = [], [], []
    if GraphRAGConfig.USE_CHAPTER_GRAPH:
        for i, ck in enumerate(chunks[:2]):
            preview = ck[:300].replace("\n", " | ")
            callback(msg=f"[ChapterGraph DEBUG] chunk {i} preview: {preview}")
        callback(msg=f"[ChapterGraph DEBUG] total chunks={len(chunks)}, fallback_title={fallback_title}")

        _, chapter_ents, chapter_rels, chunk_chapters = _extract_book_and_chapters(
            doc_id, chunks, fallback_title
        )
        callback(msg=f"[ChapterGraph DEBUG] chapter_ents={len(chapter_ents)}, chapter_rels={len(chapter_rels)}")
        if chapter_ents:
            callback(msg=f"[ChapterGraph DEBUG] chapter_entities={[e['entity_name'] for e in chapter_ents]}")
        if chapter_rels:
            callback(
                msg=f"[ChapterGraph DEBUG] chapter_rels_sample={(chapter_rels[0]['src_id'], chapter_rels[0]['tgt_id'])}"
            )

    ents = list(llm_ents)
    rels = list(llm_rels)

    if chapter_ents:
        merged_ents = {}
        for ent in llm_ents:
            merged_ents[ent["entity_name"]] = dict(ent)
        for cent in chapter_ents:
            name = cent["entity_name"]
            if name in merged_ents:
                existing = merged_ents[name]
                existing["source_id"] = sorted(
                    set(existing.get("source_id", []) + cent.get("source_id", []))
                )
                if cent.get("entity_type") in ("书籍", "章节"):
                    existing["entity_type"] = cent["entity_type"]
                    existing["description"] = cent["description"]
            else:
                merged_ents[name] = dict(cent)
        ents = list(merged_ents.values())
        rels.extend(chapter_rels)

        entity_chapter_rels = _link_entities_to_chapters(doc_id, chunks, llm_ents, chunk_chapters)
        rels.extend(entity_chapter_rels)
        callback(msg=f"[ChapterGraph DEBUG] entity_chapter_rels={len(entity_chapter_rels)}")

    subgraph = nx.Graph()

    for ent in ents:
        _has_cancel_and_exit(
            task_id, f"Task {task_id} cancelled during entity processing for doc {doc_id}.", callback
        )

        assert "description" in ent, f"entity {ent} does not have description"
        ent["source_id"] = [doc_id]
        subgraph.add_node(ent["entity_name"], **ent)

    ignored_rels = 0
    ignored_rel_samples = []
    for rel in rels:
        _has_cancel_and_exit(
            task_id, f"Task {task_id} cancelled during relationship processing for doc {doc_id}.", callback
        )

        assert "description" in rel, f"relation {rel} does not have description"
        has_src = subgraph.has_node(rel["src_id"])
        has_tgt = subgraph.has_node(rel["tgt_id"])
        if not has_src or not has_tgt:
            ignored_rels += 1
            if len(ignored_rel_samples) < 5:
                ignored_rel_samples.append(
                    {
                        "src_id": rel["src_id"],
                        "tgt_id": rel["tgt_id"],
                        "has_src": has_src,
                        "has_tgt": has_tgt,
                    }
                )
            continue
        rel["source_id"] = [doc_id]
        subgraph.add_edge(
            rel["src_id"],
            rel["tgt_id"],
            **rel,
        )
    if ignored_rels:
        callback(msg=f"ignored {ignored_rels} relations due to missing entities.")
    _has_cancel_and_exit(
        task_id, f"Task {task_id} cancelled before tidying subgraph for doc {doc_id}.", callback
    )
    tidy_graph(subgraph, callback, check_attribute=False)

    subgraph.graph["source_id"] = [doc_id]
    chunk = {
        "content_with_weight": json.dumps(
            nx.node_link_data(subgraph, edges="edges"), ensure_ascii=False
        ),
        "knowledge_graph_kwd": "subgraph",
        "kb_id": kb_id,
        "source_id": [doc_id],
        "available_int": 0,
        "removed_kwd": "N",
    }
    cid = chunk_id(chunk)
    _has_cancel_and_exit(
        task_id, f"Task {task_id} cancelled before saving subgraph for doc {doc_id}.", callback
    )
    await thread_pool_exec(
        settings.docStoreConn.delete,
        {"knowledge_graph_kwd": "subgraph", "source_id": doc_id},
        search.index_name(tenant_id),
        kb_id,
    )
    await thread_pool_exec(
        settings.docStoreConn.insert,
        [{"id": cid, **chunk}],
        search.index_name(tenant_id),
        kb_id,
    )

    if GraphRAGConfig.USE_INCREMENTAL_MERGE:
        await write_merge_state(
            tenant_id,
            kb_id,
            doc_id,
            state="pending",
            expected_nodes=len(subgraph.nodes),
            expected_edges=len(subgraph.edges),
        )

    now = asyncio.get_running_loop().time()
    callback(msg=f"generated subgraph for doc {doc_id} in {now - start:.2f} seconds.")
    return subgraph


async def merge_subgraph_incremental(
    tenant_id: str,
    kb_id: str,
    doc_id: str,
    subgraph: nx.Graph,
    embedding_model,
    callback,
):
    """Incremental merge: does not load the global graph into memory.

    Only queries existing nodes / edges that appear in the new subgraph,
    merges attributes in memory, skips global PageRank, and writes delta.
    """
    start = asyncio.get_running_loop().time()
    change = GraphChange()

    node_names = list(subgraph.nodes())

    logging.info("[P2] Querying %d entities for existing data...", len(node_names))
    existing_entities = await query_existing_entities(tenant_id, kb_id, node_names)
    logging.info("[P2] Found %d existing entities.", len(existing_entities))

    delta_graph = nx.Graph()
    delta_graph.graph["source_id"] = list(subgraph.graph.get("source_id", []))

    for node_name, attr in subgraph.nodes(data=True):
        if node_name in existing_entities:
            old_fields = existing_entities[node_name]
            try:
                old_meta = json.loads(old_fields["content_with_weight"])
            except Exception:
                old_meta = {}

            merged_attr = dict(old_meta)
            new_desc = attr.get("description", "")
            if new_desc:
                old_desc = merged_attr.get("description", "")
                merged_attr["description"] = (
                    old_desc + GRAPH_FIELD_SEP + new_desc if old_desc else new_desc
                )
            old_sources = set(merged_attr.get("source_id", []))
            new_sources = set(attr.get("source_id", []))
            merged_attr["source_id"] = sorted(old_sources | new_sources)
            if attr.get("entity_type"):
                if not merged_attr.get("entity_type"):
                    merged_attr["entity_type"] = attr["entity_type"]
            for k, v in attr.items():
                if k not in merged_attr:
                    merged_attr[k] = v
            if "pagerank" not in merged_attr:
                merged_attr["pagerank"] = old_meta.get("pagerank", 0.001)

            delta_graph.add_node(node_name, **merged_attr)
            change.added_updated_nodes.add(node_name)
        else:
            new_attr = dict(attr)
            if "pagerank" not in new_attr:
                new_attr["pagerank"] = 0.001
            delta_graph.add_node(node_name, **new_attr)
            change.added_updated_nodes.add(node_name)

    edge_pairs = list(subgraph.edges())
    logging.info("[P2] Querying %d relations for existing data...", len(edge_pairs))
    existing_relations = await query_existing_relations(tenant_id, kb_id, edge_pairs)
    logging.info("[P2] Found %d existing relations.", len(existing_relations))

    for source, target, attr in subgraph.edges(data=True):
        edge_key = get_from_to(source, target)
        if edge_key in existing_relations:
            old_fields = existing_relations[edge_key]
            try:
                old_meta = json.loads(old_fields["content_with_weight"])
            except Exception:
                old_meta = {}

            merged_attr = dict(old_meta)
            merged_attr["weight"] = merged_attr.get("weight", 0) + attr.get("weight", 0)
            new_desc = attr.get("description", "")
            if new_desc:
                old_desc = merged_attr.get("description", "")
                merged_attr["description"] = (
                    old_desc + GRAPH_FIELD_SEP + new_desc if old_desc else new_desc
                )
            old_kw = set(merged_attr.get("keywords", []))
            new_kw = set(attr.get("keywords", []))
            merged_attr["keywords"] = sorted(old_kw | new_kw)
            old_sources = set(merged_attr.get("source_id", []))
            new_sources = set(attr.get("source_id", []))
            merged_attr["source_id"] = sorted(old_sources | new_sources)
            for k, v in attr.items():
                if k not in merged_attr:
                    merged_attr[k] = v

            delta_graph.add_edge(source, target, **merged_attr)
            change.added_updated_edges.add(edge_key)
        else:
            delta_graph.add_edge(source, target, **attr)
            change.added_updated_edges.add(edge_key)

    for node_name in delta_graph.nodes:
        delta_graph.nodes[node_name]["rank"] = int(delta_graph.degree(node_name))

    await set_graph(tenant_id, kb_id, embedding_model, delta_graph, change, callback)

    now = asyncio.get_running_loop().time()
    logging.info(
        "[P2] incremental merge for doc %s done in %.2fs (nodes: %d, edges: %d).",
        doc_id,
        now - start,
        len(change.added_updated_nodes),
        len(change.added_updated_edges),
    )
    return delta_graph


async def resolve_entities_incremental(
    tenant_id: str,
    kb_id: str,
    union_nodes: set[str],
    llm_bdl,
    embed_bdl,
    callback,
    task_id: str = "",
    entity_types: list[str] | None = None,
):
    """Incremental entity resolution via OpenSearch KNN or char-level filtering."""
    start = asyncio.get_running_loop().time()
    excluded_types = build_excluded_types(entity_types)

    if not union_nodes:
        logging.info("[P3] No new nodes, skipping resolution.")
        return

    new_node_fields = await query_existing_entities(tenant_id, kb_id, list(union_nodes))

    new_nodes_by_type = defaultdict(list)
    node_attrs = {}
    for node_name in union_nodes:
        fields = new_node_fields.get(node_name)
        if not fields:
            continue
        try:
            meta = json.loads(fields["content_with_weight"])
        except Exception:
            continue
        ent_type = meta.get("entity_type", "-")
        new_nodes_by_type[ent_type].append(node_name)
        node_attrs[node_name] = meta

    if not new_nodes_by_type:
        logging.info("[P3] No valid new nodes with types, skipping resolution.")
        return

    candidate_pairs: set[tuple[str, str]] = set()
    candidate_neighbors: set[str] = set()

    async def _fetch_existing_names_by_type(tenant_id, kb_id, ent_type):
        """Fetch all existing entity names of a given type via scroll."""
        index_name = search.index_name(tenant_id)
        query_body = {
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"kb_id": [kb_id]}},
                        {"terms": {"knowledge_graph_kwd": ["entity"]}},
                        {"term": {"entity_type_kwd": ent_type}},
                    ]
                }
            }
        }
        try:
            # ES / Infinity doc store does not implement search_with_scroll;
            # in that case fall back to an empty set so resolution can still
            # proceed via char-level filtering.
            if not hasattr(settings.docStoreConn, "search_with_scroll"):
                return set()
            res = await thread_pool_exec(
                settings.docStoreConn.search_with_scroll,
                index_name,
                query_body,
                ["entity_kwd"],
            )
            fields_map = settings.docStoreConn.get_fields(res, ["entity_kwd"])
            names = set()
            for cid, row in fields_map.items():
                name = row.get("entity_kwd")
                if isinstance(name, list):
                    name = name[0]
                if name:
                    names.add(name)
            return names
        except Exception as e:
            logging.warning("[P3] Failed to fetch existing names for type %s: %s", ent_type, e)
            return set()

    if GraphRAGConfig.USE_KNN_FOR_RESOLUTION:
        if not hasattr(settings.docStoreConn, "knn_search_entities"):
            logging.warning(
                "[P3] KNN resolution requested but %s does not support knn_search_entities; "
                "falling back to char-level filtering.",
                type(settings.docStoreConn).__name__,
            )
        else:
            vector_dim = getattr(embed_bdl, "dimension", None)
            if vector_dim is None:
                try:
                    test_emb, _ = await asyncio.get_running_loop().run_in_executor(
                        None, embed_bdl.encode, ["DIM_CHECK"]
                    )
                    vector_dim = len(test_emb[0])
                except Exception as e:
                    logging.warning("[P3] Failed to detect embedding dimension: %s", e)
                    return

            new_node_vectors = await fetch_node_vectors(
                tenant_id, kb_id, list(union_nodes), vector_dim
            )
            if not new_node_vectors:
                logging.info("[P3] No vectors found for new nodes, skipping resolution.")
                return

            vector_field = f"q_{vector_dim}_vec"
            knn_semaphore = asyncio.Semaphore(GraphRAGConfig.ENTITY_RESOLUTION_KNN_CONCURRENCY)

            async def _knn_one(node_name, vector, ent_type):
                async with knn_semaphore:
                    try:
                        res = await thread_pool_exec(
                            settings.docStoreConn.knn_search_entities,
                            [search.index_name(tenant_id)],
                            [kb_id],
                            vector,
                            vector_field,
                            GraphRAGConfig.ENTITY_RESOLUTION_TOP_K,
                            GraphRAGConfig.ENTITY_RESOLUTION_SIM_THRESHOLD,
                            entity_type=ent_type,
                            exclude_name=node_name,
                        )
                        fields_map = settings.docStoreConn.get_fields(res, ["entity_kwd"])
                        neighbors = []
                        for cid, row in fields_map.items():
                            neighbor_name = row.get("entity_kwd")
                            if isinstance(neighbor_name, list):
                                neighbor_name = neighbor_name[0]
                            if not neighbor_name or neighbor_name == node_name:
                                continue
                            if not is_similarity_str(node_name, neighbor_name):
                                continue
                            neighbors.append(neighbor_name)
                        return node_name, neighbors
                    except Exception as e:
                        logging.warning("KNN search failed for node %s: %s", node_name, e)
                        return node_name, []

            for ent_type, new_nodes in new_nodes_by_type.items():
                if not new_nodes or ent_type in excluded_types:
                    continue

                tasks = []
                for node_name in new_nodes:
                    vector = new_node_vectors.get(node_name)
                    if not vector:
                        continue
                    tasks.append(asyncio.create_task(_knn_one(node_name, vector, ent_type)))

                if not tasks:
                    continue

                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, Exception):
                        logging.warning("KNN task exception: %s", res)
                        continue
                    node_name, neighbors = res
                    for neighbor_name in neighbors:
                        a, b = (
                            (node_name, neighbor_name)
                            if node_name < neighbor_name
                            else (neighbor_name, node_name)
                        )
                        candidate_pairs.add((a, b))
                        candidate_neighbors.add(neighbor_name)

    if not candidate_pairs:
        logging.info("[P3] Falling back to char-level filtering for entity resolution.")
        # Phase 2.4 safety: cap the per-type candidate set and batch the new-node
        # side so a 10k existing x 100 new KB does not produce a 1M Cartesian
        # product in memory. Each batch reuses the existing_names set, so the
        # cost is O(batches x |existing|) string compares per type, bounded by
        # RESOLUTION_CHAR_BATCH_SIZE * RESOLUTION_CHAR_MAX_CANDIDATES.
        char_batch = max(1, int(GraphRAGConfig.RESOLUTION_CHAR_BATCH_SIZE))
        max_candidates = max(1, int(GraphRAGConfig.RESOLUTION_CHAR_MAX_CANDIDATES))

        for ent_type, new_nodes in new_nodes_by_type.items():
            if not new_nodes or ent_type in excluded_types:
                continue
            if len(candidate_pairs) >= max_candidates:
                logging.info(
                    "[P3] Char-level candidate set reached cap (%d) before type=%s, skipping remaining",
                    max_candidates,
                    ent_type,
                )
                break

            existing_names = await _fetch_existing_names_by_type(tenant_id, kb_id, ent_type)
            existing_names = existing_names - set(new_nodes)

            for batch_start in range(0, len(new_nodes), char_batch):
                if len(candidate_pairs) >= max_candidates:
                    break
                batch_new = new_nodes[batch_start : batch_start + char_batch]
                for node_name in batch_new:
                    for existing_name in existing_names:
                        if not is_similarity_str(node_name, existing_name):
                            continue
                        a, b = (
                            (node_name, existing_name)
                            if node_name < existing_name
                            else (existing_name, node_name)
                        )
                        candidate_pairs.add((a, b))
                        candidate_neighbors.add(existing_name)
                        if len(candidate_pairs) >= max_candidates:
                            break
                    if len(candidate_pairs) >= max_candidates:
                        break

    if not candidate_pairs:
        logging.info("[P3] No candidates found, skipping resolution.")
        return

    neighbor_attrs = await query_existing_entities(tenant_id, kb_id, list(candidate_neighbors))

    candidate_resolution = defaultdict(list)
    for a, b in candidate_pairs:
        type_a = node_attrs.get(a, {}).get("entity_type")
        if type_a is None and a in neighbor_attrs:
            try:
                meta = json.loads(neighbor_attrs[a]["content_with_weight"])
                type_a = meta.get("entity_type", "-")
            except Exception:
                type_a = "-"
        type_b = node_attrs.get(b, {}).get("entity_type")
        if type_b is None and b in neighbor_attrs:
            try:
                meta = json.loads(neighbor_attrs[b]["content_with_weight"])
                type_b = meta.get("entity_type", "-")
            except Exception:
                type_b = "-"
        if type_a == type_b and type_a not in excluded_types:
            candidate_resolution[type_a].append((a, b))

    if not candidate_resolution:
        logging.info("[P3] No valid candidates after type validation, skipping resolution.")
        return

    local_graph = nx.Graph()
    for node_name in union_nodes:
        if node_name in node_attrs:
            local_graph.add_node(node_name, **node_attrs[node_name])
    for node_name, fields in neighbor_attrs.items():
        try:
            meta = json.loads(fields["content_with_weight"])
        except Exception:
            meta = {}
        local_graph.add_node(node_name, **meta)

    all_local_nodes = list(local_graph.nodes())
    rel_fields = await query_node_relations(tenant_id, kb_id, all_local_nodes)
    for fields in rel_fields:
        from_node = fields.get("from_entity_kwd")
        to_node = fields.get("to_entity_kwd")
        if isinstance(from_node, list):
            from_node = from_node[0]
        if isinstance(to_node, list):
            to_node = to_node[0]
        if from_node and to_node:
            try:
                meta = json.loads(fields["content_with_weight"])
            except Exception:
                meta = {}
            local_graph.add_edge(from_node, to_node, **meta)

    logging.info(
        "[P3] Recalled %d candidates, local graph: %d nodes, %d edges.",
        len(candidate_pairs),
        local_graph.number_of_nodes(),
        local_graph.number_of_edges(),
    )

    er = EntityResolution(
        llm_bdl,
        excluded_types=excluded_types,
    )
    try:
        reso = await er(
            local_graph,
            set(union_nodes),
            callback=callback,
            task_id=task_id,
            candidate_resolution=dict(candidate_resolution),
        )
    except Exception as e:
        logging.warning("P3: EntityResolution failed: %s", e)
        raise

    change = reso.change
    logging.info(
        "[P3] Resolution removed %d nodes and %d edges.",
        len(change.removed_nodes),
        len(change.removed_edges),
    )

    await set_graph(tenant_id, kb_id, embed_bdl, reso.graph, change, callback)
    now = asyncio.get_running_loop().time()
    logging.info("[P3] incremental resolution done in %.2fs.", now - start)


async def _extract_community_core(
    graph: nx.Graph,
    tenant_id: str,
    kb_id: str,
    llm_bdl,
    callback,
    task_id: str = "",
):
    """Shared implementation of community detection + indexing."""
    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled before community extraction.", callback)

    start = asyncio.get_running_loop().time()
    checkpoints = await load_checkpoints(tenant_id, kb_id, COMMUNITY_CHECKPOINT)

    async def save_community_checkpoint(checkpoint_key: str, payload):
        return await save_checkpoint(tenant_id, kb_id, COMMUNITY_CHECKPOINT, checkpoint_key, payload)

    ext = CommunityReportsExtractor(llm_bdl)
    cr = await ext(
        graph,
        callback=callback,
        task_id=task_id,
        checkpoints=checkpoints,
        save_checkpoint=save_community_checkpoint,
    )

    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled during community extraction.", callback)

    community_structure = cr.structured_output
    community_reports = cr.output
    doc_ids = graph.graph.get("source_id", [])

    now = asyncio.get_running_loop().time()
    callback(msg=f"Graph extracted {len(cr.structured_output)} communities in {now - start:.2f}s.")
    start = now
    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled during community indexing.", callback)

    chunks = []
    for stru, rep in zip(community_structure, community_reports):
        obj = {
            "report": rep,
            "evidences": "\n".join([f.get("explanation", "") for f in stru["findings"]]),
        }
        chunk_payload_for_id = {
            "content_with_weight": f"community_report::{stru['title']}",
            "kb_id": kb_id,
        }
        chunk = {
            "id": chunk_id(chunk_payload_for_id),
            "docnm_kwd": stru["title"],
            "title_tks": rag_tokenizer.tokenize(stru["title"]),
            "content_with_weight": json.dumps(obj, ensure_ascii=False),
            "content_ltks": rag_tokenizer.tokenize(obj["report"] + " " + obj["evidences"]),
            "knowledge_graph_kwd": "community_report",
            "weight_flt": stru["weight"],
            "entities_kwd": stru["entities"],
            "important_kwd": stru["entities"],
            "kb_id": kb_id,
            "source_id": list(doc_ids),
            "available_int": 0,
        }
        chunk["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(chunk["content_ltks"])
        chunks.append(chunk)

    new_ids: set[str] = {c["id"] for c in chunks}

    old_ids: list[str] = []
    try:
        existing_res = await thread_pool_exec(
            settings.docStoreConn.search,
            ["id"],
            [],
            {"knowledge_graph_kwd": ["community_report"]},
            [],
            OrderByExpr(),
            0,
            10000,
            search.index_name(tenant_id),
            [kb_id],
        )
        existing_fields = settings.docStoreConn.get_fields(existing_res, ["id"])
        old_ids = list(existing_fields.keys())
    except Exception:
        logging.exception(
            "Failed to enumerate existing community reports for kb %s; falling back to delete-then-insert.",
            kb_id,
        )
        await thread_pool_exec(
            settings.docStoreConn.delete,
            {"knowledge_graph_kwd": "community_report", "kb_id": kb_id},
            search.index_name(tenant_id),
            kb_id,
        )
        old_ids = []

    await insert_chunks_bounded(
        chunks, tenant_id, kb_id, callback=callback, label="Insert community reports"
    )

    stale_ids = [i for i in old_ids if i not in new_ids]
    if stale_ids:
        try:
            await thread_pool_exec(
                settings.docStoreConn.delete,
                {"knowledge_graph_kwd": ["community_report"], "id": stale_ids},
                search.index_name(tenant_id),
                kb_id,
            )
        except Exception:
            logging.exception("Failed to prune %d stale community reports for kb %s", len(stale_ids), kb_id)

    _has_cancel_and_exit(task_id, f"Task {task_id} cancelled after community indexing.", callback)
    await cleanup_checkpoints(tenant_id, kb_id, COMMUNITY_CHECKPOINT)

    now = asyncio.get_running_loop().time()
    callback(msg=f"Graph indexed {len(cr.structured_output)} communities in {now - start:.2f}s.")
    return community_structure, community_reports


async def extract_community_indexed(
    tenant_id: str,
    kb_id: str,
    llm_bdl,
    embed_bdl,
    callback,
    task_id: str = "",
):
    """Load the full graph from the doc-store index and run community detection.

    This is the P4 async path: it guarantees that community detection sees the
    complete global topology regardless of whether the caller passed a delta
    subgraph or a full graph.
    """
    start = asyncio.get_running_loop().time()
    graph = await get_graph_from_index(tenant_id, kb_id)
    if graph is None or len(graph.nodes) == 0:
        logging.info("[P4] No graph found in index, skipping community extraction.")
        return [], []

    logging.info(
        "[P4] Loaded %d nodes, %d edges from index for community detection in %.2fs.",
        len(graph.nodes),
        len(graph.edges),
        asyncio.get_running_loop().time() - start,
    )
    return await _extract_community_core(
        graph, tenant_id, kb_id, llm_bdl, callback, task_id=task_id
    )


@timeout(60 * 30, 1)
async def extract_community_async(
    graph,
    tenant_id: str,
    kb_id: str,
    doc_id: str,
    llm_bdl,
    embed_bdl,
    callback,
    task_id: str = "",
):
    """Async-community dispatcher: loads the full graph from index when needed."""
    source_ids = graph.graph.get("source_id", []) if graph else []
    if len(source_ids) <= 1:
        logging.info("[P4] Incoming graph appears to be a delta; loading full graph from index.")
        return await extract_community_indexed(
            tenant_id, kb_id, llm_bdl, embed_bdl, callback, task_id=task_id
        )
    else:
        logging.info("[P4] Incoming graph appears complete; using it directly for community detection.")

    return await _extract_community_core(
        graph, tenant_id, kb_id, llm_bdl, callback, task_id=task_id
    )
