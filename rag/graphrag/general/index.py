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
import asyncio
import json
import logging
import os
import re

import networkx as nx

from api.db.services.document_service import DocumentService
from api.db.services.task_service import has_canceled
from common.exceptions import TaskCanceledException
from common.connection_utils import timeout
from rag.graphrag.entity_resolution import EntityResolution, EXCLUDED_RESOLUTION_TYPES
from rag.graphrag.general.community_reports_extractor import CommunityReportsExtractor
from rag.graphrag.general.extractor import Extractor
from rag.graphrag.general.graph_extractor import GraphExtractor as GeneralKGExt
from rag.graphrag.light.graph_extractor import GraphExtractor as LightKGExt
from rag.graphrag.ner.graph_extractor import GraphExtractor as NerKGExt
from rag.graphrag.phase_markers import (
    PHASE_COMMUNITY,
    PHASE_RESOLUTION,
    clear_phase_markers,
    has_phase_marker,
    set_phase_marker,
)
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.utils import (
    GRAPH_FIELD_SEP,
    GraphChange,
    chunk_id,
    does_graph_contains,
    get_from_to,
    get_graph,
    graph_merge,
    insert_chunks_bounded,
    query_existing_entities,
    query_existing_relations,
    query_node_relations,
    set_graph,
    tidy_graph,
)
from common.misc_utils import thread_pool_exec
from rag.nlp import rag_tokenizer, search
from rag.utils.redis_conn import RedisDistributedLock, REDIS_CONN
from common import settings
from common.doc_store.doc_store_base import OrderByExpr


def _select_extractor(graphrag_config: dict):
    """Return the extractor class matching ``graphrag_config["method"]``.

    Supported values:
    - ``"general"``  – Microsoft GraphRAG LLM-based extractor (default in
      earlier versions).
    - ``"light"``   – LightRAG-style LLM-based extractor (the default when
      *method* is omitted or unrecognised).
    - ``"ner"``     – NER-based extractor using spaCy (no LLM
      needed for entity / relation extraction itself).
    """
    method = graphrag_config.get("method", "light")
    if method == "general":
        return GeneralKGExt
    if method == "ner":
        return NerKGExt
    return LightKGExt


async def load_subgraph_from_store(tenant_id: str, kb_id: str, doc_id: str):
    """Load a previously saved subgraph from the doc store.

    Filters directly by source_id (== doc_id) and knowledge_graph_kwd in the
    query so the doc store index does the heavy lifting.  Expects at most one
    matching chunk per doc_id (as written by generate_subgraph).
    Returns a networkx Graph on hit, or None on miss.
    """
    fields = ["content_with_weight", "source_id"]
    condition = {
        "knowledge_graph_kwd": ["subgraph"],
        "removed_kwd": "N",
        "source_id": [doc_id],
    }
    try:
        res = await thread_pool_exec(
            settings.docStoreConn.search,
            fields, [], condition, [], OrderByExpr(),
            0, 1, search.index_name(tenant_id), [kb_id]
        )
        field_map = settings.docStoreConn.get_fields(res, fields)
        for cid, row in field_map.items():
            content = row.get("content_with_weight", "")
            if not content:
                continue
            try:
                data = json.loads(content)
                sg = nx.node_link_graph(data, edges="edges")
                sg.graph["source_id"] = [doc_id]
                logging.info(
                    "Checkpoint hit: subgraph for doc %s (tenant=%s kb=%s) found at chunk %s",
                    doc_id, tenant_id, kb_id, cid,
                )
                return sg
            except Exception:
                logging.exception(
                    "Failed to parse subgraph JSON for doc %s chunk %s", doc_id, cid
                )
    except Exception:
        logging.exception("Failed to load subgraph from store for doc %s", doc_id)
        return None
    logging.info(
        "Checkpoint miss: no subgraph for doc %s (tenant=%s kb=%s)",
        doc_id, tenant_id, kb_id,
    )
    return None


async def run_graphrag(
    row: dict,
    language,
    with_resolution: bool,
    with_community: bool,
    chat_model,
    embedding_model,
    callback,
):
    enable_timeout_assertion = os.environ.get("ENABLE_TIMEOUT_ASSERTION")
    start = asyncio.get_running_loop().time()
    tenant_id, kb_id, doc_id = row["tenant_id"], str(row["kb_id"]), row["doc_id"]
    chunks = []
    doc_name = ""
    for d in settings.retriever.chunk_list(doc_id, tenant_id, [kb_id], max_count=10000, fields=["content_with_weight", "doc_id", "docnm_kwd"], sort_by_position=True):
        chunks.append(d["content_with_weight"])
        if not doc_name:
            doc_name = d.get("docnm_kwd", "")
    if doc_name:
        doc_name = os.path.splitext(doc_name)[0]

    timeout_sec = max(120, len(chunks) * 60 * 10) if enable_timeout_assertion else 10000000000

    try:
        subgraph = await asyncio.wait_for(
            generate_subgraph(
                _select_extractor(row["kb_parser_config"].get("graphrag", {})),
                tenant_id,
                kb_id,
                doc_id,
                chunks,
                language,
                row["kb_parser_config"]["graphrag"].get("entity_types", []),
                chat_model,
                embedding_model,
                callback,
                fallback_title=doc_name,
            ),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        logging.error("generate_subgraph timeout")
        raise

    if not subgraph:
        return

    graphrag_task_lock = RedisDistributedLock(f"graphrag_task_{kb_id}", lock_value=doc_id, timeout=1200)
    await graphrag_task_lock.spin_acquire()
    callback(msg=f"run_graphrag {doc_id} graphrag_task_lock acquired")

    try:
        subgraph_nodes = set(subgraph.nodes())
        new_graph = await merge_subgraph(
            tenant_id,
            kb_id,
            doc_id,
            subgraph,
            embedding_model,
            callback,
        )
        assert new_graph is not None

        if not with_resolution and not with_community:
            return

        if with_resolution:
            await graphrag_task_lock.spin_acquire()
            callback(msg=f"run_graphrag {doc_id} graphrag_task_lock acquired")
            await resolve_entities(
                new_graph,
                subgraph_nodes,
                tenant_id,
                kb_id,
                doc_id,
                chat_model,
                embedding_model,
                callback,
                task_id=row["id"],
            )
        if with_community:
            await graphrag_task_lock.spin_acquire()
            callback(msg=f"run_graphrag {doc_id} graphrag_task_lock acquired")
            await extract_community(
                new_graph,
                tenant_id,
                kb_id,
                doc_id,
                chat_model,
                embedding_model,
                callback,
                task_id=row["id"],
            )
    finally:
        graphrag_task_lock.release()
    now = asyncio.get_running_loop().time()
    callback(msg=f"GraphRAG for doc {doc_id} done in {now - start:.2f} seconds.")
    return


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
    max_parallel_docs: int = 4,
) -> dict:
    tenant_id, kb_id = row["tenant_id"], row["kb_id"]
    enable_timeout_assertion = os.environ.get("ENABLE_TIMEOUT_ASSERTION")
    start = asyncio.get_running_loop().time()
    fields_for_chunks = ["content_with_weight", "doc_id"]

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
        callback(msg=f"[GraphRAG] kb:{kb_id} has no processable doc_id.")
        return {"ok_docs": [], "failed_docs": [], "total_docs": 0, "total_chunks": 0, "seconds": 0.0}

    def load_doc_chunks(doc_id: str) -> list[str]:
        from common.token_utils import num_tokens_from_string

        chunks = []
        current_chunk = ""

        # DEBUG: Obtener todos los chunks primero
        raw_chunks = list(settings.retriever.chunk_list(
            doc_id,
            tenant_id,
            [kb_id],
            max_count=10000,  # FIX: Aumentar límite para procesar todos los chunks
            fields=fields_for_chunks,
            sort_by_position=True,
        ))

        callback(msg=f"[DEBUG] chunk_list() returned {len(raw_chunks)} raw chunks for doc {doc_id}")

        for d in raw_chunks:
            content = d["content_with_weight"]
            # FIX: 合并 chunk 时添加换行符分隔，确保 Markdown 标题（如 ##）仍在行首
            separator = "\n" if current_chunk and not current_chunk.endswith("\n") else ""
            proposed = current_chunk + separator + content
            if num_tokens_from_string(proposed) < 4096:
                current_chunk = proposed
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = content

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    all_doc_chunks: dict[str, list[str]] = {}
    total_chunks = 0
    for doc_id in doc_ids:
        chunks = load_doc_chunks(doc_id)
        all_doc_chunks[doc_id] = chunks
        total_chunks += len(chunks)

    if total_chunks == 0:
        callback(msg=f"[GraphRAG] kb:{kb_id} has no available chunks in all documents, skip.")
        return {"ok_docs": [], "failed_docs": doc_ids, "total_docs": len(doc_ids), "total_chunks": 0, "seconds": 0.0}

    semaphore = asyncio.Semaphore(max_parallel_docs)

    subgraphs: dict[str, object] = {}
    failed_docs: list[tuple[str, str]] = []  # (doc_id, error)

    async def build_one(doc_id: str):
        if has_canceled(row["id"]):
            callback(msg=f"Task {row['id']} cancelled, stopping execution.")
            raise TaskCanceledException(f"Task {row['id']} was cancelled")

        chunks = all_doc_chunks.get(doc_id, [])
        if not chunks:
            callback(msg=f"[GraphRAG] doc:{doc_id} has no available chunks, skip generation.")
            return

        kg_extractor = _select_extractor(kb_parser_config.get("graphrag", {}))

        deadline = max(120, len(chunks) * 60 * 10) if enable_timeout_assertion else 10000000000

        async with semaphore:
            # CHECKPOINT: bounded by semaphore so doc-store lookups respect max_parallel_docs
            existing_sg = await load_subgraph_from_store(tenant_id, kb_id, doc_id)
            if existing_sg:
                subgraphs[doc_id] = existing_sg
                callback(msg=f"[GraphRAG] doc:{doc_id} subgraph found in store, skipping LLM extraction.")
                return
            try:
                msg = f"[GraphRAG] build_subgraph doc:{doc_id}"
                callback(msg=f"{msg} start (chunks={len(chunks)}, timeout={deadline}s)")

                try:
                    sg = await asyncio.wait_for(
                        generate_subgraph(
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
                            task_id=row["id"]
                        ),
                        timeout=deadline,
                    )
                except asyncio.TimeoutError:
                    failed_docs.append((doc_id, "timeout"))
                    callback(msg=f"{msg} FAILED: timeout")
                    return
                if sg:
                    subgraphs[doc_id] = sg
                    callback(msg=f"{msg} done")
                else:
                    failed_docs.append((doc_id, "subgraph is empty"))
                    callback(msg=f"{msg} empty")
            except TaskCanceledException as canceled:
                callback(msg=f"[GraphRAG] build_subgraph doc:{doc_id} FAILED: {canceled}")
            except Exception as e:
                failed_docs.append((doc_id, repr(e)))
                callback(msg=f"[GraphRAG] build_subgraph doc:{doc_id} FAILED: {e!r}")
                # P5: record LLM rate-limit signals for adaptive limiter
                from rag.graphrag.limiter import current_limiter
                if current_limiter:
                    err_str = str(e).lower()
                    if any(k in err_str for k in ("rate limit", "429", "tpm limit", "too many requests", "requests per minute")):
                        current_limiter.record_event_sync("llm_rate_limit")

    if has_canceled(row["id"]):
        callback(msg=f"Task {row['id']} cancelled before processing documents.")
        raise TaskCanceledException(f"Task {row['id']} was cancelled")

    tasks = [asyncio.create_task(build_one(doc_id)) for doc_id in doc_ids]
    try:
        await asyncio.gather(*tasks, return_exceptions=False)
    except Exception as e:
        logging.error(f"Error in asyncio.gather: {e}")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    if has_canceled(row["id"]):
        callback(msg=f"Task {row['id']} cancelled after document processing.")
        raise TaskCanceledException(f"Task {row['id']} was cancelled")

    ok_docs = [d for d in doc_ids if d in subgraphs]
    final_graph = None

    # Determine whether the resolution/community phases still need to run on
    # this KB. Markers from a prior task let us skip already-completed phases
    # even when no new docs are merged this round (the resume path).
    resolution_pending = with_resolution and not has_phase_marker(kb_id, PHASE_RESOLUTION)
    community_pending = with_community and not has_phase_marker(kb_id, PHASE_COMMUNITY)

    if not ok_docs and not resolution_pending and not community_pending:
        callback(msg=f"[GraphRAG] kb:{kb_id} no subgraphs to merge and no phases pending, end.")
        now = asyncio.get_running_loop().time()
        return {"ok_docs": [], "failed_docs": failed_docs, "total_docs": len(doc_ids), "total_chunks": total_chunks, "seconds": now - start}

    kb_lock = RedisDistributedLock(f"graphrag_task_{kb_id}", lock_value="batch_merge", timeout=1200)
    await kb_lock.spin_acquire()
    callback(msg=f"[GraphRAG] kb:{kb_id} merge lock acquired")

    if has_canceled(row["id"]):
        callback(msg=f"Task {row['id']} cancelled before merging subgraphs.")
        raise TaskCanceledException(f"Task {row['id']} was cancelled")

    try:
        union_nodes: set = set()

        for doc_id in ok_docs:
            sg = subgraphs[doc_id]
            union_nodes.update(set(sg.nodes()))

            new_graph = await merge_subgraph(
                tenant_id,
                kb_id,
                doc_id,
                sg,
                embedding_model,
                callback,
            )
            if new_graph is not None:
                final_graph = new_graph

        if ok_docs and final_graph is None:
            callback(msg=f"[GraphRAG] kb:{kb_id} merge finished (no in-memory graph returned).")
        elif ok_docs:
            callback(msg=f"[GraphRAG] kb:{kb_id} merge finished, graph ready.")
            # New content was merged into the global graph; any prior
            # resolution/community results are now stale and must be redone
            # on this or a future run. Clear phase markers accordingly.
            clear_phase_markers(kb_id)
            resolution_pending = with_resolution
            community_pending = with_community
            callback(msg=f"[GraphRAG] kb:{kb_id} cleared phase markers after merge.")
    finally:
        kb_lock.release()

    if not with_resolution and not with_community:
        now = asyncio.get_running_loop().time()
        callback(msg=f"[GraphRAG] KB merge done in {now - start:.2f}s. ok={len(ok_docs)} / total={len(doc_ids)}")
        return {"ok_docs": ok_docs, "failed_docs": failed_docs, "total_docs": len(doc_ids), "total_chunks": total_chunks, "seconds": now - start}

    if not resolution_pending and not community_pending:
        now = asyncio.get_running_loop().time()
        callback(msg=f"[GraphRAG] kb:{kb_id} all requested phases already complete; nothing to do.")
        return {"ok_docs": ok_docs, "failed_docs": failed_docs, "total_docs": len(doc_ids), "total_chunks": total_chunks, "seconds": now - start}

    # P5-T3: optionally offload resolution/community to a Redis Stream queue
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
            logging.info("[GraphRAG] kb:%s queued resolution/community to %s", kb_id, GraphRAGConfig.KG_POSTPROCESS_QUEUE)
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
            logging.warning("[GraphRAG] kb:%s FAILED to queue postprocess; falling back to synchronous execution.", kb_id)

    if has_canceled(row["id"]):
        callback(msg=f"Task {row['id']} cancelled before resolution/community extraction.")
        raise TaskCanceledException(f"Task {row['id']} was cancelled")

    await kb_lock.spin_acquire()
    callback(msg=f"[GraphRAG] kb:{kb_id} post-merge lock acquired for resolution/community")

    try:
        # Resume path: no docs were merged this round but pending phases
        # require the previously-persisted graph. Load it from the doc store.
        if final_graph is None:
            final_graph = await get_graph(tenant_id, kb_id)
            if final_graph is None:
                callback(msg=f"[GraphRAG] kb:{kb_id} no persisted graph found; cannot run resolution/community.")
                now = asyncio.get_running_loop().time()
                return {"ok_docs": ok_docs, "failed_docs": failed_docs, "total_docs": len(doc_ids), "total_chunks": total_chunks, "seconds": now - start}
            callback(msg=f"[GraphRAG] kb:{kb_id} loaded persisted graph for resume.")

        subgraph_nodes = set()
        for sg in subgraphs.values():
            subgraph_nodes.update(set(sg.nodes()))
        # On a pure-resume run (no new docs) the union of "newly added" nodes
        # is empty, but resolution still needs *some* anchor set. Fall back to
        # all graph nodes so candidate pairing actually finds something.
        if not subgraph_nodes:
            subgraph_nodes = set(final_graph.nodes())

        if resolution_pending:
            await resolve_entities(
                final_graph,
                subgraph_nodes,
                tenant_id,
                kb_id,
                None,
                chat_model,
                embedding_model,
                callback,
                task_id=row["id"],
            )
            set_phase_marker(kb_id, PHASE_RESOLUTION)
        elif with_resolution:
            callback(msg=f"[GraphRAG] kb:{kb_id} resolution already completed previously, skipping.")

        if community_pending:
            await extract_community(
                final_graph,
                tenant_id,
                kb_id,
                None,
                chat_model,
                embedding_model,
                callback,
                task_id=row["id"],
            )
            set_phase_marker(kb_id, PHASE_COMMUNITY)
        elif with_community:
            callback(msg=f"[GraphRAG] kb:{kb_id} community detection already completed previously, skipping.")
    finally:
        kb_lock.release()

    now = asyncio.get_running_loop().time()
    callback(msg=f"[GraphRAG] GraphRAG for KB {kb_id} done in {now - start:.2f} seconds. ok={len(ok_docs)} failed={len(failed_docs)} total_docs={len(doc_ids)} total_chunks={total_chunks}")
    return {
        "ok_docs": ok_docs,
        "failed_docs": failed_docs,  # [(doc_id, error), ...]
        "total_docs": len(doc_ids),
        "total_chunks": total_chunks,
        "seconds": now - start,
    }


def _extract_book_and_chapters(doc_id: str, chunks: list[str], fallback_title: str = ""):
    """从 Markdown chunks 中提取书名和章节名"""
    book_title = None
    for chunk in chunks:
        m = re.search(r"^#\s+(.+)$", chunk, re.MULTILINE)
        if m:
            book_title = m.group(1).strip()
            break

    if not book_title:
        book_title = fallback_title
    if not book_title:
        logging.warning(f"[ChapterGraph] doc_id={doc_id}: 未找到 # 书名，且未提供 fallback_title，跳过章节提取")
        return None, [], [], []

    chapter_entities = []
    chapter_relations = []
    seen_chapters = set()
    chunk_chapters = []

    chapter_entities.append({
        "entity_name": book_title,
        "entity_type": "书籍",
        "description": f"书籍《{book_title}》",
        "source_id": [doc_id],
    })

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
                chapter_entities.append({
                    "entity_name": chapter_node_name,
                    "entity_type": "章节",
                    "description": f"《{book_title}》的章节：{chapter}",
                    "source_id": [doc_id],
                })
                chapter_relations.append({
                    "src_id": book_title,
                    "tgt_id": chapter_node_name,
                    "description": f"《{book_title}》包含章节《{chapter}》",
                    "keywords": ["contains", "章节", "书籍"],
                    "weight": 1,
                    "source_id": [doc_id],
                })
        chunk_chapters.append(chapters_in_chunk)

    # 如果没有找到任何 ## 标题（说明 chunk 生成时已把 ## 当作 delimiter 切掉了），
    # 则从每个后续 chunk 的第一行非空文本提取章节标题
    if not has_markdown_headers:
        for i, chunk in enumerate(chunks):
            if i == 0:
                continue
            first_line = ""
            for line in chunk.split('\n'):
                stripped = line.strip()
                if stripped:
                    first_line = stripped
                    break
            if first_line and len(first_line) <= 100:
                chapter_node_name = f"《{book_title}》{first_line}"
                chunk_chapters[i].append(chapter_node_name)
                if chapter_node_name not in seen_chapters:
                    seen_chapters.add(chapter_node_name)
                    chapter_entities.append({
                        "entity_name": chapter_node_name,
                        "entity_type": "章节",
                        "description": f"《{book_title}》的章节：{first_line}",
                        "source_id": [doc_id],
                    })
                    chapter_relations.append({
                        "src_id": book_title,
                        "tgt_id": chapter_node_name,
                        "description": f"《{book_title}》包含章节《{first_line}》",
                        "keywords": ["contains", "章节", "书籍"],
                        "weight": 1,
                        "source_id": [doc_id],
                    })
        # FIX: chunks[0] 通常包含书名和第一章内容，fallback 时应把它关联到第一个章节
        if chunks and len(chunk_chapters) > 1:
            first_chapter = None
            for chapters in chunk_chapters[1:]:
                if chapters:
                    first_chapter = chapters[0]
                    break
            if first_chapter and not chunk_chapters[0]:
                chunk_chapters[0].append(first_chapter)

    logging.info(f"[ChapterGraph] doc_id={doc_id}: book_title={book_title}, chapters={len(seen_chapters)}, has_md_headers={has_markdown_headers}")
    return book_title, chapter_entities, chapter_relations, chunk_chapters


def _link_entities_to_chapters(doc_id: str, chunks: list[str], entities: list[dict], chunk_chapters: list[list[str]]):
    """基于文本匹配，将实体与包含它的章节建立关系"""
    relations = []
    seen_pairs = set()
    chunk_texts = [chunk.lower() for chunk in chunks]

    for ent in entities:
        # 跳过书籍和章节自身
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
                    relations.append({
                        "src_id": chapter,
                        "tgt_id": ent_name,
                        "description": f"章节《{chapter}》涉及实体《{ent_name}》",
                        "keywords": ["involves", "章节", "实体"],
                        "weight": 1,
                        "source_id": [doc_id],
                    })
        if not matched:
            logging.debug(f"[ChapterGraph] doc_id={doc_id}: entity '{ent_name}' not found in any chunk text")
    logging.info(f"[ChapterGraph] doc_id={doc_id}: entity_chapter_relations={len(relations)}, entities_processed={len(entities)}")
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
    if task_id and has_canceled(task_id):
        callback(msg=f"Task {task_id} cancelled during subgraph generation for doc {doc_id}.")
        raise TaskCanceledException(f"Task {task_id} was cancelled")

    contains = await does_graph_contains(tenant_id, kb_id, doc_id)
    if contains:
        callback(msg=f"Graph already contains {doc_id}")
        return None
    start = asyncio.get_running_loop().time()
    ext = extractor(
        llm_bdl,
        language=language,
        entity_types=entity_types,
    )
    llm_ents, llm_rels = await ext(doc_id, chunks, callback, task_id=task_id)

    # ---------- 注入章节层级与实体关联 ----------
    # DEBUG: 打印前2个chunk的内容预览，确认标题是否存在
    for i, ck in enumerate(chunks[:2]):
        preview = ck[:300].replace("\n", " | ")
        callback(msg=f"[ChapterGraph DEBUG] chunk {i} preview: {preview}")
    callback(msg=f"[ChapterGraph DEBUG] total chunks={len(chunks)}, fallback_title={fallback_title}")

    _, chapter_ents, chapter_rels, chunk_chapters = _extract_book_and_chapters(doc_id, chunks, fallback_title)
    callback(msg=f"[ChapterGraph DEBUG] chapter_ents={len(chapter_ents)}, chapter_rels={len(chapter_rels)}")
    if chapter_ents:
        callback(msg=f"[ChapterGraph DEBUG] chapter_entities={[e['entity_name'] for e in chapter_ents]}")
    if chapter_rels:
        callback(msg=f"[ChapterGraph DEBUG] chapter_rels_sample={(chapter_rels[0]['src_id'], chapter_rels[0]['tgt_id'])}")

    ents = list(llm_ents)
    rels = list(llm_rels)

    if chapter_ents:
        # 合并 LLM 实体与章节实体，同名时保留 Book/Chapter 类型
        merged_ents = {}
        for ent in llm_ents:
            merged_ents[ent["entity_name"]] = dict(ent)
        for cent in chapter_ents:
            name = cent["entity_name"]
            if name in merged_ents:
                existing = merged_ents[name]
                existing["source_id"] = sorted(set(existing.get("source_id", []) + cent.get("source_id", [])))
                if cent.get("entity_type") in ("书籍", "章节"):
                    existing["entity_type"] = cent["entity_type"]
                    existing["description"] = cent["description"]
            else:
                merged_ents[name] = dict(cent)
        ents = list(merged_ents.values())
        rels.extend(chapter_rels)

        # 建立章节-实体关系（只对 LLM 提取的原始实体做匹配）
        entity_chapter_rels = _link_entities_to_chapters(doc_id, chunks, llm_ents, chunk_chapters)
        rels.extend(entity_chapter_rels)
        callback(msg=f"[ChapterGraph DEBUG] entity_chapter_rels={len(entity_chapter_rels)}")
    # -------------------------------------------

    subgraph = nx.Graph()

    for ent in ents:
        if task_id and has_canceled(task_id):
            callback(msg=f"Task {task_id} cancelled during entity processing for doc {doc_id}.")
            raise TaskCanceledException(f"Task {task_id} was cancelled")

        assert "description" in ent, f"entity {ent} does not have description"
        ent["source_id"] = [doc_id]
        subgraph.add_node(ent["entity_name"], **ent)

    ignored_rels = 0
    ignored_rel_samples = []
    for rel in rels:
        if task_id and has_canceled(task_id):
            callback(msg=f"Task {task_id} cancelled during relationship processing for doc {doc_id}.")
            raise TaskCanceledException(f"Task {task_id} was cancelled")

        assert "description" in rel, f"relation {rel} does not have description"
        has_src = subgraph.has_node(rel["src_id"])
        has_tgt = subgraph.has_node(rel["tgt_id"])
        if not has_src or not has_tgt:
            ignored_rels += 1
            if len(ignored_rel_samples) < 5:
                ignored_rel_samples.append({
                    "src_id": rel["src_id"],
                    "tgt_id": rel["tgt_id"],
                    "has_src": has_src,
                    "has_tgt": has_tgt,
                })
            continue
        rel["source_id"] = [doc_id]
        subgraph.add_edge(
            rel["src_id"],
            rel["tgt_id"],
            **rel,
        )
    if ignored_rels:
        callback(msg=f"ignored {ignored_rels}/{len(rels)} relations due to missing entities.")
        if ignored_rel_samples:
            callback(msg=f"[ChapterGraph DEBUG] ignored relation samples: {ignored_rel_samples}")
    callback(msg=f"[ChapterGraph DEBUG] subgraph nodes={subgraph.number_of_nodes()}, edges={subgraph.number_of_edges()}")
    tidy_graph(subgraph, callback, check_attribute=False)

    subgraph.graph["source_id"] = [doc_id]
    chunk = {
        "content_with_weight": json.dumps(nx.node_link_data(subgraph, edges="edges"), ensure_ascii=False),
        "knowledge_graph_kwd": "subgraph",
        "kb_id": kb_id,
        "source_id": [doc_id],
        "available_int": 0,
        "removed_kwd": "N",
    }
    cid = chunk_id(chunk)
    await thread_pool_exec(settings.docStoreConn.delete,{"knowledge_graph_kwd": "subgraph", "source_id": doc_id},search.index_name(tenant_id),kb_id,)
    await thread_pool_exec(settings.docStoreConn.insert,[{"id": cid, **chunk}],search.index_name(tenant_id),kb_id,)
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

    # 1. Query existing entities in the doc store
    logging.info("[P2] Querying %d entities for existing data...", len(node_names))
    existing_entities = await query_existing_entities(tenant_id, kb_id, node_names)
    logging.info("[P2] Found %d existing entities.", len(existing_entities))

    # 2. Build delta graph with merged nodes
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
            # Merge description
            new_desc = attr.get("description", "")
            if new_desc:
                old_desc = merged_attr.get("description", "")
                merged_attr["description"] = old_desc + GRAPH_FIELD_SEP + new_desc if old_desc else new_desc
            # Merge source_id (deduplicate)
            old_sources = set(merged_attr.get("source_id", []))
            new_sources = set(attr.get("source_id", []))
            merged_attr["source_id"] = sorted(old_sources | new_sources)
            # Entity type: prefer existing, fallback to new
            if attr.get("entity_type"):
                if not merged_attr.get("entity_type"):
                    merged_attr["entity_type"] = attr["entity_type"]
            # Copy any new attributes not in old
            for k, v in attr.items():
                if k not in merged_attr:
                    merged_attr[k] = v
            # Preserve pagerank from old if present
            if "pagerank" not in merged_attr:
                merged_attr["pagerank"] = old_meta.get("pagerank", 0.001)

            delta_graph.add_node(node_name, **merged_attr)
            change.added_updated_nodes.add(node_name)
        else:
            # New node
            new_attr = dict(attr)
            if "pagerank" not in new_attr:
                new_attr["pagerank"] = 0.001
            delta_graph.add_node(node_name, **new_attr)
            change.added_updated_nodes.add(node_name)

    # 3. Query existing relations
    edge_pairs = list(subgraph.edges())
    logging.info("[P2] Querying %d relations for existing data...", len(edge_pairs))
    existing_relations = await query_existing_relations(tenant_id, kb_id, edge_pairs)
    logging.info("[P2] Found %d existing relations.", len(existing_relations))

    # 4. Build delta edges
    for source, target, attr in subgraph.edges(data=True):
        edge_key = get_from_to(source, target)
        if edge_key in existing_relations:
            old_fields = existing_relations[edge_key]
            try:
                old_meta = json.loads(old_fields["content_with_weight"])
            except Exception:
                old_meta = {}

            merged_attr = dict(old_meta)
            # Merge weight
            merged_attr["weight"] = merged_attr.get("weight", 0) + attr.get("weight", 0)
            # Merge description
            new_desc = attr.get("description", "")
            if new_desc:
                old_desc = merged_attr.get("description", "")
                merged_attr["description"] = old_desc + GRAPH_FIELD_SEP + new_desc if old_desc else new_desc
            # Merge keywords (deduplicate)
            old_kw = set(merged_attr.get("keywords", []))
            new_kw = set(attr.get("keywords", []))
            merged_attr["keywords"] = sorted(old_kw | new_kw)
            # Merge source_id
            old_sources = set(merged_attr.get("source_id", []))
            new_sources = set(attr.get("source_id", []))
            merged_attr["source_id"] = sorted(old_sources | new_sources)
            # Copy any new attributes
            for k, v in attr.items():
                if k not in merged_attr:
                    merged_attr[k] = v

            delta_graph.add_edge(source, target, **merged_attr)
            change.added_updated_edges.add(edge_key)
        else:
            delta_graph.add_edge(source, target, **attr)
            change.added_updated_edges.add(edge_key)

    # 5. Update rank (degree-based approximation; global PageRank is deferred)
    for node_name in delta_graph.nodes:
        delta_graph.nodes[node_name]["rank"] = int(delta_graph.degree(node_name))

    # 6. Write delta
    await set_graph(tenant_id, kb_id, embedding_model, delta_graph, change, callback)

    now = asyncio.get_running_loop().time()
    logging.info("[P2] incremental merge for doc %s done in %.2fs (nodes: %d, edges: %d).",
                 doc_id, now - start, len(change.added_updated_nodes), len(change.added_updated_edges))
    return delta_graph


@timeout(60 * 3)
async def merge_subgraph(
    tenant_id: str,
    kb_id: str,
    doc_id: str,
    subgraph: nx.Graph,
    embedding_model,
    callback,
):
    start = asyncio.get_running_loop().time()
    change = GraphChange()

    if GraphRAGConfig.USE_INCREMENTAL_MERGE:
        try:
            return await merge_subgraph_incremental(
                tenant_id, kb_id, doc_id, subgraph, embedding_model, callback
            )
        except Exception as exc:
            logging.error("merge_subgraph_incremental failed, falling back to monolithic merge: %s", exc, exc_info=True)
            logging.warning("[P2] incremental merge failed, falling back to monolithic merge: %s", exc)
            # fall through to monolithic path

    old_graph = await get_graph(tenant_id, kb_id, subgraph.graph["source_id"])
    if old_graph is not None:
        logging.info("Merge with an exiting graph...................")
        tidy_graph(old_graph, callback)
        new_graph = graph_merge(old_graph, subgraph, change)
    else:
        new_graph = subgraph
        change.added_updated_nodes = set(new_graph.nodes())
        change.added_updated_edges = set(new_graph.edges())
    pr = nx.pagerank(new_graph)
    for node_name, pagerank in pr.items():
        new_graph.nodes[node_name]["pagerank"] = pagerank

    # DEBUG: 检查全局图中 Book 节点的邻居
    for n in new_graph.nodes:
        if new_graph.nodes[n].get("entity_type") == "书籍":
            neighbors = [nb for nb in new_graph.neighbors(n) if new_graph.nodes[nb].get("entity_type") == "章节"]
            callback(msg=f"[ChapterGraph DEBUG] After merge, Book '{n}' has {len(neighbors)} Chapter neighbors: {neighbors}")
            break

    await set_graph(tenant_id, kb_id, embedding_model, new_graph, change, callback)
    now = asyncio.get_running_loop().time()
    callback(msg=f"merging subgraph for doc {doc_id} into the global graph done in {now - start:.2f} seconds.")
    return new_graph


async def resolve_entities_incremental(
    tenant_id: str,
    kb_id: str,
    subgraph_nodes: set[str],
    llm_bdl,
    embed_bdl,
    callback,
    task_id: str = "",
):
    """Incremental entity resolution without loading the global graph.

    For each entity type present in ``subgraph_nodes``, queries existing
    entities of the same type from the doc store, builds a local subgraph
    (including all neighbours so edge-redirects are not lost), and runs
    :class:`EntityResolution` on that local view only.
    """
    from collections import defaultdict

    start = asyncio.get_running_loop().time()

    if not subgraph_nodes:
        logging.info("[P3] No subgraph nodes, skipping resolution.")
        return

    # 1. Query attributes of new nodes
    new_node_fields = await query_existing_entities(tenant_id, kb_id, list(subgraph_nodes))

    # 2. Group new nodes by type
    new_nodes_by_type = defaultdict(list)
    node_attrs = {}
    for node_name in subgraph_nodes:
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

    er = EntityResolution(llm_bdl)
    overall_change = GraphChange()
    all_local_graphs = []

    for ent_type, new_nodes in new_nodes_by_type.items():
        if not new_nodes or ent_type in EXCLUDED_RESOLUTION_TYPES:
            continue

        # 3. Query existing nodes of the same type
        conds = {
            "fields": ["entity_kwd", "content_with_weight"],
            "size": 10000,
            "knowledge_graph_kwd": ["entity"],
            "entity_type_kwd": ent_type,
        }
        existing_node_attrs = {}
        try:
            es_res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id])
            for id in es_res.ids:
                fields = es_res.field[id]
                ent_name = fields.get("entity_kwd")
                if isinstance(ent_name, list):
                    ent_name = ent_name[0]
                if ent_name and ent_name not in subgraph_nodes:
                    try:
                        meta = json.loads(fields["content_with_weight"])
                    except Exception:
                        meta = {}
                    existing_node_attrs[ent_name] = meta
        except Exception as e:
            logging.warning("P3: failed to query existing %s entities: %s", ent_type, e)
            continue

        if not existing_node_attrs:
            continue

        # 4. Build local graph with new + existing nodes of this type
        local_graph = nx.Graph()
        for node_name in new_nodes:
            if node_name in node_attrs:
                local_graph.add_node(node_name, **node_attrs[node_name])
        for ent_name, meta in existing_node_attrs.items():
            local_graph.add_node(ent_name, **meta)

        # 5. Pull all relations touching any node in the local graph
        #    (including neighbours of other types) so _merge_graph_nodes
        #    can redirect every edge correctly.
        all_node_names = list(local_graph.nodes())
        rel_fields = await query_node_relations(tenant_id, kb_id, all_node_names)
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
            "[P3] Type '%s': %d new vs %d existing, %d relations, %d nodes in local graph.",
            ent_type, len(new_nodes), len(existing_node_attrs), len(rel_fields), local_graph.number_of_nodes(),
        )

        # 6. Run EntityResolution on the local graph
        try:
            reso = await er(local_graph, set(new_nodes), callback=callback, task_id=task_id)
        except Exception as e:
            logging.warning("P3: EntityResolution failed for type %s: %s", ent_type, e)
            continue

        change = reso.change
        overall_change.removed_nodes.update(change.removed_nodes)
        overall_change.added_updated_nodes.update(change.added_updated_nodes)
        overall_change.removed_edges.update(change.removed_edges)
        overall_change.added_updated_edges.update(change.added_updated_edges)
        all_local_graphs.append(reso.graph)

    if not all_local_graphs:
        logging.info("[P3] No resolution performed (no existing candidates).")
        return

    # 7. Build combined graph from all local graphs so set_graph can read attrs
    combined_graph = nx.Graph()
    for g in all_local_graphs:
        for node_name, attrs in g.nodes(data=True):
            combined_graph.add_node(node_name, **attrs)
        for u, v, attrs in g.edges(data=True):
            combined_graph.add_edge(u, v, **attrs)
        if g.graph.get("source_id"):
            combined_graph.graph.setdefault("source_id", []).extend(g.graph["source_id"])
    combined_graph.graph["source_id"] = list(set(combined_graph.graph.get("source_id", [])))

    logging.info(
        "[P3] Overall resolution removed %d nodes and %d edges.",
        len(overall_change.removed_nodes), len(overall_change.removed_edges),
    )

    await set_graph(tenant_id, kb_id, embed_bdl, combined_graph, overall_change, callback)
    now = asyncio.get_running_loop().time()
    logging.info("[P3] incremental resolution done in %.2fs.", now - start)


@timeout(60 * 30, 1)
async def resolve_entities(
    graph,
    subgraph_nodes: set[str],
    tenant_id: str,
    kb_id: str,
    doc_id: str,
    llm_bdl,
    embed_bdl,
    callback,
    task_id: str = "",
):
    # Check if task has been canceled before resolution
    if task_id and has_canceled(task_id):
        callback(msg=f"Task {task_id} cancelled during entity resolution.")
        raise TaskCanceledException(f"Task {task_id} was cancelled")

    start = asyncio.get_running_loop().time()

    if GraphRAGConfig.USE_INCREMENTAL_RESOLUTION:
        try:
            await resolve_entities_incremental(
                tenant_id, kb_id, subgraph_nodes, llm_bdl, embed_bdl, callback, task_id=task_id
            )
            now = asyncio.get_running_loop().time()
            callback(msg=f"Graph resolution done in {now - start:.2f}s.")
            return
        except Exception as exc:
            logging.error("resolve_entities_incremental failed, falling back to monolithic: %s", exc, exc_info=True)
            logging.warning("[P3] incremental resolution failed, falling back to monolithic: %s", exc)
            # fall through to monolithic path

    er = EntityResolution(
        llm_bdl,
    )
    reso = await er(graph, subgraph_nodes, callback=callback, task_id=task_id)
    graph = reso.graph
    change = reso.change
    callback(msg=f"Graph resolution removed {len(change.removed_nodes)} nodes and {len(change.removed_edges)} edges.")
    callback(msg="Graph resolution updated pagerank.")

    # DEBUG: 检查实体消解后 Book 节点的邻居
    for n in graph.nodes:
        if graph.nodes[n].get("entity_type") == "书籍":
            neighbors = [nb for nb in graph.neighbors(n) if graph.nodes[nb].get("entity_type") == "章节"]
            callback(msg=f"[ChapterGraph DEBUG] After resolution, Book '{n}' has {len(neighbors)} Chapter neighbors: {neighbors}")
            break

    if task_id and has_canceled(task_id):
        callback(msg=f"Task {task_id} cancelled after entity resolution.")
        raise TaskCanceledException(f"Task {task_id} was cancelled")

    await set_graph(tenant_id, kb_id, embed_bdl, graph, change, callback)
    now = asyncio.get_running_loop().time()
    callback(msg=f"Graph resolution done in {now - start:.2f}s.")


async def _extract_community_core(
    graph: nx.Graph,
    tenant_id: str,
    kb_id: str,
    llm_bdl,
    callback,
    task_id: str = "",
):
    """Shared implementation of community detection + indexing.

    Operates on the supplied ``graph`` (which may be the full global graph or
    a delta).  Returns ``(community_structure, community_reports)``.
    """
    start = asyncio.get_running_loop().time()
    ext = CommunityReportsExtractor(llm_bdl)
    cr = await ext(graph, callback=callback, task_id=task_id)

    if task_id and has_canceled(task_id):
        callback(msg=f"Task {task_id} cancelled during community extraction.")
        raise TaskCanceledException(f"Task {task_id} was cancelled")

    community_structure = cr.structured_output
    community_reports = cr.output
    doc_ids = graph.graph.get("source_id", [])

    now = asyncio.get_running_loop().time()
    callback(msg=f"Graph extracted {len(cr.structured_output)} communities in {now - start:.2f}s.")
    start = now

    if task_id and has_canceled(task_id):
        callback(msg=f"Task {task_id} cancelled during community indexing.")
        raise TaskCanceledException(f"Task {task_id} was cancelled")

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
            "docnm_kwd": stru['title'],
            "title_tks": rag_tokenizer.tokenize(stru['title']),
            "content_with_weight": json.dumps(obj, ensure_ascii=False),
            "content_ltks": rag_tokenizer.tokenize(obj["report"] + " " + obj["evidences"]),
            "knowledge_graph_kwd": "community_report",
            "weight_flt": stru['weight'],
            "entities_kwd": stru['entities'],
            "important_kwd": stru['entities'],
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
            ["id"], [], {"knowledge_graph_kwd": ["community_report"]}, [], OrderByExpr(),
            0, 10000, search.index_name(tenant_id), [kb_id],
        )
        existing_fields = settings.docStoreConn.get_fields(existing_res, ["id"])
        old_ids = list(existing_fields.keys())
    except Exception:
        logging.exception("Failed to enumerate existing community reports for kb %s; falling back to delete-then-insert.", kb_id)
        await thread_pool_exec(settings.docStoreConn.delete, {"knowledge_graph_kwd": "community_report", "kb_id": kb_id}, search.index_name(tenant_id), kb_id)
        old_ids = []

    await insert_chunks_bounded(chunks, tenant_id, kb_id, callback=callback, label=f"{label_prefix}Insert community reports")

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

    if task_id and has_canceled(task_id):
        callback(msg=f"Task {task_id} cancelled after community indexing.")
        raise TaskCanceledException(f"Task {task_id} was cancelled")

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
        len(graph.nodes), len(graph.edges), asyncio.get_running_loop().time() - start,
    )
    return await _extract_community_core(
        graph, tenant_id, kb_id, llm_bdl, callback, task_id=task_id
    )


@timeout(60 * 30, 1)
async def extract_community(
    graph,
    tenant_id: str,
    kb_id: str,
    doc_id: str,
    llm_bdl,
    embed_bdl,
    callback,
    task_id: str = "",
):
    if task_id and has_canceled(task_id):
        callback(msg=f"Task {task_id} cancelled before community extraction.")
        raise TaskCanceledException(f"Task {task_id} was cancelled")

    start = asyncio.get_running_loop().time()

    if GraphRAGConfig.USE_ASYNC_COMMUNITY:
        # P4: If the incoming graph is a delta (single-doc source_id), load
        # the full graph from index so Leiden sees the global topology.
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
