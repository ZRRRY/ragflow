# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

from common.misc_utils import thread_pool_exec

"""
Reference:
 - [graphrag](https://github.com/microsoft/graphrag)
 - [LightRag](https://github.com/HKUDS/LightRAG)
"""

import asyncio
import dataclasses
import html
import json
import logging
import os
import re
import time
from collections import defaultdict
from hashlib import md5
from typing import Any, Callable, Set, Tuple

import networkx as nx
import numpy as np
import xxhash
from networkx.readwrite import json_graph

from common.misc_utils import get_uuid
from common.connection_utils import timeout
from common.asyncio_utils import LoopLocalSemaphore
from rag.nlp import rag_tokenizer, search
from rag.utils.redis_conn import REDIS_CONN
from common import settings
from common.doc_store.doc_store_base import OrderByExpr
from rag.graphrag.config import GraphRAGConfig
# Phase 2.3: search_after 分页，避免 size=10000 硬截断
from rag.graphrag.utils_pagination import (
    collect_all as _collect_all_search_after,
    search_all_by_search_after as _search_all_by_search_after,
    supports_search_after as _supports_search_after,
)

GRAPH_FIELD_SEP = "<SEP>"

ErrorHandlerFn = Callable[[BaseException | None, str | None, dict | None], None]

chat_limiter = LoopLocalSemaphore(int(os.environ.get("MAX_CONCURRENT_CHATS", 10)))

# Doc-store insert batching for GraphRAG subgraph/node/edge/community_report
# chunks.  Defaults (64 docs per batch, up to 4 batches in flight) mirror the
# regular ingest pipeline in document_service.py while still keeping the total
# number of simultaneous requests to ES/Infinity bounded.  Override with
# GRAPHRAG_INSERT_BULK_SIZE and GRAPHRAG_INSERT_CONCURRENCY.
_INSERT_BULK_SIZE = max(1, int(os.environ.get("GRAPHRAG_INSERT_BULK_SIZE", 64)))
_INSERT_CONCURRENCY = max(1, int(os.environ.get("GRAPHRAG_INSERT_CONCURRENCY", 2)))


async def insert_chunks_bounded(chunks, tenant_id, kb_id, *, callback=None, label="Insert chunks"):
    """Insert ``chunks`` into the doc store in batches with bounded concurrency and retries.

    Batch size is controlled by ``GRAPHRAG_INSERT_BULK_SIZE`` (default 64) and
    the number of batches in flight by ``GRAPHRAG_INSERT_CONCURRENCY``
    (default 4).  Each batch has the same retry / timeout behaviour as the
    previous hand-rolled loop (3 attempts, exponential backoff).

    Raises the first unrecoverable error; other in-flight batches are then
    cancelled by ``asyncio.gather``.
    """
    if not chunks:
        return
    enable_timeout_assertion = os.environ.get("ENABLE_TIMEOUT_ASSERTION")
    sem = asyncio.Semaphore(_INSERT_CONCURRENCY)
    total = len(chunks)
    progress = {"done": 0, "next_report": 100}
    progress_lock = asyncio.Lock()
    # P5: adaptive limiter event injection
    from rag.graphrag.limiter import current_limiter
    from rag.graphrag.config import GraphRAGConfig

    async def _one(offset: int) -> None:
        batch = chunks[offset : offset + _INSERT_BULK_SIZE]
        timeout_s = 3 if enable_timeout_assertion else 30000000
        max_retries = 3
        async with sem:
            for attempt in range(max_retries):
                try:
                    t0 = asyncio.get_running_loop().time()
                    result = await asyncio.wait_for(
                        thread_pool_exec(
                            settings.docStoreConn.insert,
                            batch,
                            search.index_name(tenant_id),
                            kb_id,
                        ),
                        timeout=timeout_s,
                    )
                    elapsed_ms = (asyncio.get_running_loop().time() - t0) * 1000
                    if result:
                        raise Exception(f"Insert chunk error: {result}, please check log file and Elasticsearch/Infinity status!")
                    # Record slow doc-store ops for adaptive limiter
                    if current_limiter and elapsed_ms > GraphRAGConfig.ES_SLOW_THRESHOLD_MS:
                        current_limiter.record_event_sync("es_slow")
                    break
                except asyncio.TimeoutError:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logging.warning("Insert batch at offset %d/%d attempt %d timed out, retrying in %ds", offset, total, attempt + 1, wait)
                        await asyncio.sleep(wait)
                    else:
                        raise
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logging.warning("Insert batch at offset %d/%d attempt %d failed: %s, retrying in %ds", offset, total, attempt + 1, e, wait)
                        # P5: record potential CAS / version conflicts for adaptive limiter
                        if current_limiter:
                            err_str = str(e).lower()
                            if any(k in err_str for k in ("conflict", "version", "concurrent modification")):
                                current_limiter.record_event_sync("cas_conflict")
                        await asyncio.sleep(wait)
                    else:
                        raise
        if callback:
            async with progress_lock:
                progress["done"] += len(batch)
                if progress["done"] >= progress["next_report"] or progress["done"] == total:
                    callback(msg=f"{label}: {progress['done']}/{total}")
                    progress["next_report"] = progress["done"] + 100

    await asyncio.gather(*(asyncio.create_task(_one(o)) for o in range(0, total, _INSERT_BULK_SIZE)))


@dataclasses.dataclass
class GraphChange:
    removed_nodes: Set[str] = dataclasses.field(default_factory=set)
    added_updated_nodes: Set[str] = dataclasses.field(default_factory=set)
    removed_edges: Set[Tuple[str, str]] = dataclasses.field(default_factory=set)
    added_updated_edges: Set[Tuple[str, str]] = dataclasses.field(default_factory=set)


def perform_variable_replacements(input: str, history: list[dict] | None = None, variables: dict | None = None) -> str:
    """Perform variable replacements on the input string and in a chat log."""
    if history is None:
        history = []
    if variables is None:
        variables = {}
    result = input

    def replace_all(input: str) -> str:
        result = input
        for k, v in variables.items():
            result = result.replace(f"{{{k}}}", str(v))
        return result

    result = replace_all(result)
    for i, entry in enumerate(history):
        if entry.get("role") == "system":
            entry["content"] = replace_all(entry.get("content") or "")

    return result


def clean_str(input: Any) -> str:
    """Clean an input string by removing HTML escapes, control characters, and other unwanted characters."""
    # If we get non-string input, just give it back
    if not isinstance(input, str):
        return input

    result = html.unescape(input.strip())
    # https://stackoverflow.com/questions/4324790/removing-control-characters-from-a-string-in-python
    return re.sub(r"[\"\x00-\x1f\x7f-\x9f]", "", result)


def dict_has_keys_with_types(data: dict, expected_fields: list[tuple[str, type]]) -> bool:
    """Return True if the given dictionary has the given keys with the given types."""
    for field, field_type in expected_fields:
        if field not in data:
            return False

        value = data[field]
        if not isinstance(value, field_type):
            return False
    return True


def get_llm_cache(llmnm, txt, history, genconf):
    hasher = xxhash.xxh64()
    hasher.update((str(llmnm)+str(txt)+str(history)+str(genconf)).encode("utf-8"))

    k = hasher.hexdigest()
    bin = REDIS_CONN.get(k)
    if not bin:
        return None
    return bin


def set_llm_cache(llmnm, txt, v, history, genconf):
    hasher = xxhash.xxh64()
    hasher.update((str(llmnm)+str(txt)+str(history)+str(genconf)).encode("utf-8"))
    k = hasher.hexdigest()
    REDIS_CONN.set(k, v.encode("utf-8"), 24 * 3600)


def get_embed_cache(llmnm, txt):
    hasher = xxhash.xxh64()
    hasher.update(str(llmnm).encode("utf-8"))
    hasher.update(str(txt).encode("utf-8"))

    k = hasher.hexdigest()
    bin = REDIS_CONN.get(k)
    if not bin:
        return
    return np.array(json.loads(bin))


def set_embed_cache(llmnm, txt, arr):
    hasher = xxhash.xxh64()
    hasher.update(str(llmnm).encode("utf-8"))
    hasher.update(str(txt).encode("utf-8"))

    k = hasher.hexdigest()
    arr = json.dumps(arr.tolist() if isinstance(arr, np.ndarray) else arr)
    REDIS_CONN.set(k, arr.encode("utf-8"), 24 * 3600)


def get_tags_from_cache(kb_ids):
    hasher = xxhash.xxh64()
    hasher.update(str(kb_ids).encode("utf-8"))

    k = hasher.hexdigest()
    bin = REDIS_CONN.get(k)
    if not bin:
        return
    return bin


def set_tags_to_cache(kb_ids, tags):
    hasher = xxhash.xxh64()
    hasher.update(str(kb_ids).encode("utf-8"))

    k = hasher.hexdigest()
    REDIS_CONN.set(k, json.dumps(tags).encode("utf-8"), 600)


def tidy_graph(graph: nx.Graph, callback, check_attribute: bool = True):
    """
    Ensure all nodes and edges in the graph have some essential attribute.
    """

    def is_valid_item(node_attrs: dict) -> bool:
        valid_node = True
        for attr in ["description", "source_id"]:
            if attr not in node_attrs:
                valid_node = False
                break
        return valid_node

    if check_attribute:
        purged_nodes = []
        for node, node_attrs in graph.nodes(data=True):
            if not is_valid_item(node_attrs):
                purged_nodes.append(node)
        for node in purged_nodes:
            graph.remove_node(node)
        if purged_nodes and callback:
            callback(msg=f"Purged {len(purged_nodes)} nodes from graph due to missing essential attributes.")

    purged_edges = []
    for source, target, attr in graph.edges(data=True):
        if check_attribute:
            if not is_valid_item(attr):
                purged_edges.append((source, target))
        if "keywords" not in attr:
            attr["keywords"] = []
    for source, target in purged_edges:
        graph.remove_edge(source, target)
    if purged_edges and callback:
        callback(msg=f"Purged {len(purged_edges)} edges from graph due to missing essential attributes.")


def get_from_to(node1, node2):
    if node1 < node2:
        return (node1, node2)
    else:
        return (node2, node1)


def graph_merge(g1: nx.Graph, g2: nx.Graph, change: GraphChange):
    """Merge graph g2 into g1 in place."""
    for node_name, attr in g2.nodes(data=True):
        change.added_updated_nodes.add(node_name)
        if not g1.has_node(node_name):
            g1.add_node(node_name, **attr)
            continue
        node = g1.nodes[node_name]
        node["description"] += GRAPH_FIELD_SEP + attr["description"]
        # A node's source_id indicates which chunks it came from.
        node["source_id"] += attr["source_id"]

    for source, target, attr in g2.edges(data=True):
        change.added_updated_edges.add(get_from_to(source, target))
        edge = g1.get_edge_data(source, target)
        if edge is None:
            g1.add_edge(source, target, **attr)
            continue
        edge["weight"] += attr.get("weight", 0)
        edge["description"] += GRAPH_FIELD_SEP + attr["description"]
        edge["keywords"] += attr["keywords"]
        # A edge's source_id indicates which chunks it came from.
        edge["source_id"] += attr["source_id"]

    for node_degree in g1.degree:
        g1.nodes[str(node_degree[0])]["rank"] = int(node_degree[1])
    # A graph's source_id indicates which documents it came from.
    if "source_id" not in g1.graph:
        g1.graph["source_id"] = []
    g1.graph["source_id"] += g2.graph.get("source_id", [])
    return g1


def compute_args_hash(*args):
    return md5(str(args).encode()).hexdigest()


def handle_single_entity_extraction(
    record_attributes: list[str],
    chunk_key: str,
):
    if len(record_attributes) < 4 or record_attributes[0] != '"entity"':
        return None
    # add this record as a node in the G
    entity_name = clean_str(record_attributes[1].upper())
    if not entity_name.strip():
        return None
    entity_type = clean_str(record_attributes[2].upper())
    entity_description = clean_str(record_attributes[3])
    entity_source_id = chunk_key
    return dict(
        entity_name=entity_name.upper(),
        entity_type=entity_type.upper(),
        description=entity_description,
        source_id=entity_source_id,
    )


def handle_single_relationship_extraction(record_attributes: list[str], chunk_key: str):
    if len(record_attributes) < 5 or record_attributes[0] != '"relationship"':
        return None
    # add this record as edge
    source = clean_str(record_attributes[1].upper())
    target = clean_str(record_attributes[2].upper())
    edge_description = clean_str(record_attributes[3])

    edge_keywords = clean_str(record_attributes[4])
    edge_source_id = chunk_key
    weight = float(record_attributes[-1]) if is_float_regex(record_attributes[-1]) else 1.0
    pair = sorted([source.upper(), target.upper()])
    return dict(
        src_id=pair[0],
        tgt_id=pair[1],
        weight=weight,
        description=edge_description,
        keywords=edge_keywords,
        source_id=edge_source_id,
        metadata={"created_at": time.time()},
    )


def pack_user_ass_to_openai_messages(*args: str):
    roles = ["user", "assistant"]
    return [{"role": roles[i % 2], "content": content} for i, content in enumerate(args)]


def split_string_by_multi_markers(content: str, markers: list[str]) -> list[str]:
    """Split a string by multiple markers"""
    if not markers:
        return [content]
    results = re.split("|".join(re.escape(marker) for marker in markers), content)
    return [r.strip() for r in results if r.strip()]


def is_float_regex(value):
    return bool(re.match(r"^[-+]?[0-9]*\.?[0-9]+$", value))


def chunk_id(chunk):
    return xxhash.xxh64((chunk["content_with_weight"] + chunk["kb_id"]).encode("utf-8")).hexdigest()


async def graph_node_to_chunk(kb_id, embd_mdl, ent_name, meta, chunks):
    global chat_limiter
    enable_timeout_assertion = os.environ.get("ENABLE_TIMEOUT_ASSERTION")
    chunk = {
        "id": get_uuid(),
        "important_kwd": [ent_name],
        "title_tks": rag_tokenizer.tokenize(ent_name),
        "entity_kwd": ent_name,
        "knowledge_graph_kwd": "entity",
        "entity_type_kwd": meta["entity_type"],
        "content_with_weight": json.dumps(meta, ensure_ascii=False),
        "content_ltks": rag_tokenizer.tokenize(meta["description"]),
        "source_id": meta["source_id"],
        "kb_id": kb_id,
        "available_int": 0,
        "removed_kwd": "N",
    }
    chunk["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(chunk["content_ltks"])
    ebd = get_embed_cache(embd_mdl.llm_name, ent_name)
    if ebd is None:
        async with chat_limiter:
            timeout = 3 if enable_timeout_assertion else 30000000
            ebd, _ = await asyncio.wait_for(
                thread_pool_exec(embd_mdl.encode, [ent_name]),
                timeout=timeout
            )
        ebd = ebd[0]
        set_embed_cache(embd_mdl.llm_name, ent_name, ebd)
    assert ebd is not None
    chunk["q_%d_vec" % len(ebd)] = ebd
    chunks.append(chunk)


@timeout(3, 3)
async def get_relation(tenant_id, kb_id, from_ent_name, to_ent_name, size=1):
    ents = from_ent_name
    if isinstance(ents, str):
        ents = [from_ent_name]
    if isinstance(to_ent_name, str):
        to_ent_name = [to_ent_name]
    ents.extend(to_ent_name)
    ents = list(set(ents))
    conds = {"fields": ["content_with_weight"], "size": size, "from_entity_kwd": ents, "to_entity_kwd": ents, "knowledge_graph_kwd": ["relation"]}
    res = []
    es_res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id] if isinstance(kb_id, str) else kb_id)
    for id in es_res.ids:
        try:
            if size == 1:
                return json.loads(es_res.field[id]["content_with_weight"])
            res.append(json.loads(es_res.field[id]["content_with_weight"]))
        except Exception:
            continue
    return res


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
    """查询文档的最新合并状态。返回 None 表示无记录。"""
    condition = {
        "knowledge_graph_kwd": ["merge_state"],
        "source_id": [doc_id],
    }
    try:
        res = await thread_pool_exec(
            settings.docStoreConn.search,
            ["content_with_weight"], [], condition, [], OrderByExpr(),
            0, 1, search.index_name(tenant_id), [kb_id],
        )
        fields = settings.docStoreConn.get_fields(res, ["content_with_weight"])
        for row in fields.values():
            try:
                return json.loads(row["content_with_weight"])
            except Exception:
                continue
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

    BATCH_SIZE = 50
    index_name = search.index_name(tenant_id)
    existing: dict = {}

    # 每个 batch 单独发 search_after 翻页查询，filter 与旧实现保持一致
    # (from_entity_kwd ∈ batch_nodes AND to_entity_kwd ∈ batch_nodes)
    for i in range(0, len(edge_pairs), BATCH_SIZE):
        batch = edge_pairs[i:i + BATCH_SIZE]
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
                page_size=1000,
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
                key = get_from_to(from_node, to_node)
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
                    key = get_from_to(from_node, to_node)
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

    BATCH_SIZE = 100
    index_name = search.index_name(tenant_id)
    all_fields: list[dict] = []
    seen: set = set()

    for i in range(0, len(node_names), BATCH_SIZE):
        batch = node_names[i:i + BATCH_SIZE]

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
                page_size=1000,
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
                page_size=1000,
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
            key = get_from_to(from_node, to_node)
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
                    key = get_from_to(from_node, to_node)
                    if key not in seen:
                        seen.add(key)
                        all_fields.append(fields)
            except Exception as e:
                logging.warning("query_node_relations_legacy %s-batch %d failed: %s", endpoint, i, e)
    return all_fields


async def graph_edge_to_chunk(kb_id, embd_mdl, from_ent_name, to_ent_name, meta, chunks):
    enable_timeout_assertion = os.environ.get("ENABLE_TIMEOUT_ASSERTION")
    chunk = {
        "id": get_uuid(),
        "from_entity_kwd": from_ent_name,
        "to_entity_kwd": to_ent_name,
        "knowledge_graph_kwd": "relation",
        "content_with_weight": json.dumps(meta, ensure_ascii=False),
        "content_ltks": rag_tokenizer.tokenize(meta["description"]),
        "important_kwd": meta["keywords"],
        "source_id": meta["source_id"],
        "weight_int": int(meta["weight"]),
        "kb_id": kb_id,
        "available_int": 0,
        "removed_kwd": "N",
    }
    chunk["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(chunk["content_ltks"])
    txt = f"{from_ent_name}->{to_ent_name}"
    ebd = get_embed_cache(embd_mdl.llm_name, txt)
    if ebd is None:
        async with chat_limiter:
            timeout = 3 if enable_timeout_assertion else 300000000
            ebd, _ = await asyncio.wait_for(
                thread_pool_exec(
                    embd_mdl.encode,
                    [txt + f": {meta['description']}"]
                ),
                timeout=timeout
            )
        ebd = ebd[0]
        set_embed_cache(embd_mdl.llm_name, txt, ebd)
    assert ebd is not None
    chunk["q_%d_vec" % len(ebd)] = ebd
    chunks.append(chunk)


async def does_graph_contains(tenant_id, kb_id, doc_id):
    # Legacy path: check monolithic graph chunk
    fields = ["source_id"]
    condition = {
        "knowledge_graph_kwd": ["graph"],
        "removed_kwd": "N",
    }
    res = await thread_pool_exec(
        settings.docStoreConn.search,
        fields, [], condition, [], OrderByExpr(),
        0, 1, search.index_name(tenant_id), [kb_id]
    )
    fields2 = settings.docStoreConn.get_fields(res, fields)
    for chunk_id in fields2.keys():
        graph_doc_ids = set(fields2[chunk_id]["source_id"])
        if doc_id in graph_doc_ids:
            return True

    # Incremental path: check per-document subgraph chunks
    condition = {
        "knowledge_graph_kwd": ["subgraph"],
        "source_id": doc_id,
    }
    res = await thread_pool_exec(
        settings.docStoreConn.search,
        fields, [], condition, [], OrderByExpr(),
        0, 1, search.index_name(tenant_id), [kb_id]
    )
    fields2 = settings.docStoreConn.get_fields(res, fields)
    return len(fields2) > 0


async def get_graph_doc_ids(tenant_id, kb_id) -> list[str]:
    conds = {"fields": ["source_id"], "removed_kwd": "N", "size": 1, "knowledge_graph_kwd": ["graph"]}
    res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id])
    doc_ids = []
    if res.total == 0:
        return doc_ids
    for id in res.ids:
        doc_ids = res.field[id]["source_id"]
    return doc_ids


async def get_graph_from_index(tenant_id, kb_id):
    """Assemble the global graph from discrete ``entity`` and ``relation``
    chunks stored in the doc store (incremental / decoupled storage mode).

    This replaces the monolithic ``knowledge_graph_kwd="graph"`` JSON blob
    with on-the-fly assembly from indexed node/edge documents.
    """
    graph = nx.Graph()
    graph.graph["source_id"] = []
    seen_sources = set()
    total_entities = 0
    total_relations = 0

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
    )
    es_res = settings.docStoreConn.get_fields(es_res, ent_flds)

    for _cid, d in es_res.items():
        try:
            meta = json.loads(d["content_with_weight"])
            ent_name = d["entity_kwd"]
            if isinstance(ent_name, list):
                ent_name = ent_name[0] if ent_name else None
            if ent_name:
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
            if from_node and to_node and from_node in graph.nodes and to_node in graph.nodes:
                graph.add_edge(from_node, to_node, **meta)
                total_relations += 1
            for sid in meta.get("source_id", []):
                seen_sources.add(sid)
        except Exception:
            logging.exception("Failed to parse relation chunk %s", _cid)
            continue

    graph.graph["source_id"] = sorted(seen_sources)
    logging.info(
        "get_graph_from_index: kb=%s entities=%d relations=%d sources=%d",
        kb_id, total_entities, total_relations, len(seen_sources),
    )
    return graph


async def get_graph_from_json(tenant_id, kb_id, exclude_rebuild=None):
    """Legacy path: load the monolithic graph JSON blob."""
    conds = {"fields": ["content_with_weight", "removed_kwd", "source_id"], "size": 1, "knowledge_graph_kwd": ["graph"]}
    res = await settings.retriever.search(conds, search.index_name(tenant_id), [kb_id])
    if not res.total == 0:
        for id in res.ids:
            try:
                if res.field[id]["removed_kwd"] == "N":
                    g = json_graph.node_link_graph(json.loads(res.field[id]["content_with_weight"]), edges="edges")
                    if "source_id" not in g.graph:
                        g.graph["source_id"] = res.field[id]["source_id"]
                else:
                    g = await rebuild_graph(tenant_id, kb_id, exclude_rebuild)
                return g
            except Exception:
                continue
    return None


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

    return await get_graph_from_json(tenant_id, kb_id, exclude_rebuild)


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
        ebd = get_embed_cache(embd_mdl.llm_name, cache_key)
        if ebd is not None:
            embeddings[idx] = ebd
        else:
            missed_indices.append(idx)
            missed_texts.append(embed_text)

    if missed_texts:
        batch_size = 64
        total = len(missed_texts)

        async def _embed_one_batch(i):
            async with chat_limiter:
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
                set_embed_cache(embd_mdl.llm_name, cache_key, ebd)

    for idx, (chunk, _, _) in enumerate(items):
        ebd = embeddings[idx]
        assert ebd is not None
        chunk["q_%d_vec" % len(ebd)] = ebd
        chunks.append(chunk)


async def _set_graph_monolithic(tenant_id: str, kb_id: str, embd_mdl, graph: nx.Graph, change: GraphChange, callback):
    global chat_limiter
    start = asyncio.get_running_loop().time()

    # Build all new chunks first (graph, subgraphs, node/edge embeddings) before
    # deleting anything.  This ensures that if embedding generation or any other
    # step crashes, the old graph and per-doc subgraph checkpoints remain intact
    # so the pipeline can resume without re-running earlier phases.
    chunks = [
        {
            "id": get_uuid(),
            "content_with_weight": json.dumps(nx.node_link_data(graph, edges="edges"), ensure_ascii=False),
            "knowledge_graph_kwd": "graph",
            "kb_id": kb_id,
            "source_id": graph.graph.get("source_id", []),
            "available_int": 0,
            "removed_kwd": "N",
        }
    ]

    # 延迟导入避免循环引用
    from rag.graphrag.general.index import load_subgraph_from_store

    # 查询哪些文档的 subgraph 已存在于存储中（由 generate_subgraph 写入）
    existing_sg_doc_ids = set()
    for source in graph.graph["source_id"]:
        existing_sg = await load_subgraph_from_store(tenant_id, kb_id, source)
        if existing_sg:
            existing_sg_doc_ids.add(source)

    # generate updated subgraphs (only for docs without existing checkpoint)
    for source in graph.graph["source_id"]:
        if source in existing_sg_doc_ids:
            if callback:
                callback(msg=f"[GraphRAG] doc:{source} subgraph checkpoint exists, skipping monolithic rewrite.")
            continue
        subgraph = graph.subgraph([n for n in graph.nodes if source in graph.nodes[n]["source_id"]]).copy()
        subgraph.graph["source_id"] = [source]
        for n in subgraph.nodes:
            subgraph.nodes[n]["source_id"] = [source]
        chunks.append(
            {
                "id": get_uuid(),
                "content_with_weight": json.dumps(nx.node_link_data(subgraph, edges="edges"), ensure_ascii=False),
                "knowledge_graph_kwd": "subgraph",
                "kb_id": kb_id,
                "source_id": [source],
                "available_int": 0,
                "removed_kwd": "N",
            }
        )


    await _batch_embed_nodes(kb_id, embd_mdl, graph, change, chunks, callback)
    await _batch_embed_edges(kb_id, embd_mdl, graph, change, chunks, callback)

    now = asyncio.get_running_loop().time()
    if callback:
        callback(msg=f"set_graph converted graph change to {len(chunks)} chunks in {now - start:.2f}s.")
    start = now

    # All new chunks are ready.  Now delete old data and insert the new data.
    # Deleting only after chunks are built ensures that a crash during embedding
    # generation above does not destroy the old graph/subgraph checkpoints.
    # 保留 subgraph checkpoint（per-doc），清理其余全量产物以便重新生成。
    await thread_pool_exec(
        settings.docStoreConn.delete,
        {"knowledge_graph_kwd": ["graph", "entity", "relation", "community_report"], "kb_id": kb_id},
        search.index_name(tenant_id),
        kb_id
    )

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
        # Phase 2.4: 把"一对一边"删除改成"按 from_node 分组批量 + terms filter"。
        # 旧实现每条边触发一次 delete_by_query（OpenSearch 单 slice 单线程扫描），
        # 几十万条边在大 KB 下耗时几十小时；新实现按 from_node 分桶，
        # 每桶用 terms 一次 delete_by_query，IO 减少 100-1000 倍。
        from collections import defaultdict
        from_buckets: dict[str, list[str]] = defaultdict(list)
        for from_node, to_node in change.removed_edges:
            from_buckets[from_node].append(to_node)

        async def del_edges_bulk(from_node: str, to_nodes: list[str]):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with chat_limiter:
                        await thread_pool_exec(
                            settings.docStoreConn.delete,
                            {
                                "knowledge_graph_kwd": ["relation"],
                                "from_entity_kwd": from_node,
                                "to_entity_kwd": to_nodes,
                            },
                            search.index_name(tenant_id),
                            kb_id,
                        )
                    return
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logging.warning(
                            "del_edges_bulk(from=%s, n_to=%d) attempt %d failed: %s, retrying in %ds",
                            from_node, len(to_nodes), attempt + 1, e, wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise

        tasks = [
            asyncio.create_task(del_edges_bulk(from_node, to_nodes))
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

    await insert_chunks_bounded(chunks, tenant_id, kb_id, callback=callback, label="Insert chunks")
    # Phase 2.1: 全部 batch 写完后主动 refresh 一次。
    if chunks:
        try:
            refresh_fn = getattr(settings.docStoreConn, "refresh_idx", None)
            if refresh_fn is not None:
                await thread_pool_exec(refresh_fn, search.index_name(tenant_id))
        except Exception:
            logging.exception("post-insert refresh_idx failed (will rely on default refresh_interval)")
    now = asyncio.get_running_loop().time()
    if callback:
        callback(msg=f"set_graph added/updated {len(change.added_updated_nodes)} nodes and {len(change.added_updated_edges)} edges from index in {now - start:.2f}s.")


async def _pre_delete_added_updated(
    tenant_id: str,
    kb_id: str,
    change: GraphChange,
    callback=None,
):
    """Pre-delete old versions of nodes/edges that will be re-inserted.

    ``_batch_embed_nodes`` / ``_batch_embed_edges`` generate new chunks with
    random UUIDs.  Without this step, merging the same document again would
    leave the previous entity/relation documents behind, creating multi-version
    residue.  Deleting by business key before insert keeps exactly one version
    per entity/relation in the doc store.
    """
    global chat_limiter

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
        from collections import defaultdict
        update_buckets: dict[str, list[str]] = defaultdict(list)
        for from_node, to_node in change.added_updated_edges:
            update_buckets[from_node].append(to_node)

        async def del_update_edges_bulk(from_node: str, to_nodes: list[str]):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with chat_limiter:
                        await thread_pool_exec(
                            settings.docStoreConn.delete,
                            {
                                "knowledge_graph_kwd": ["relation"],
                                "from_entity_kwd": from_node,
                                "to_entity_kwd": to_nodes,
                            },
                            search.index_name(tenant_id),
                            kb_id,
                        )
                    return
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logging.warning(
                            "pre-del_update_edges_bulk(from=%s, n_to=%d) attempt %d failed: %s, retrying in %ds",
                            from_node, len(to_nodes), attempt + 1, e, wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise

        tasks = [
            asyncio.create_task(del_update_edges_bulk(from_node, to_nodes))
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


async def set_graph_delta(tenant_id: str, kb_id: str, embd_mdl, graph: nx.Graph, change: GraphChange, callback):
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
    global chat_limiter
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
        # Phase 2.4: 同样改批量（与 set_graph_delta 一致）
        from collections import defaultdict
        from_buckets: dict[str, list[str]] = defaultdict(list)
        for from_node, to_node in change.removed_edges:
            from_buckets[from_node].append(to_node)

        async def del_edges_bulk(from_node: str, to_nodes: list[str]):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with chat_limiter:
                        await thread_pool_exec(
                            settings.docStoreConn.delete,
                            {
                                "knowledge_graph_kwd": ["relation"],
                                "from_entity_kwd": from_node,
                                "to_entity_kwd": to_nodes,
                            },
                            search.index_name(tenant_id),
                            kb_id,
                        )
                    return
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logging.warning(
                            "del_edges_bulk(from=%s, n_to=%d) attempt %d failed: %s, retrying in %ds",
                            from_node, len(to_nodes), attempt + 1, e, wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise

        tasks = [
            asyncio.create_task(del_edges_bulk(from_node, to_nodes))
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

    await insert_chunks_bounded(chunks, tenant_id, kb_id, callback=callback, label="Insert chunks")
    # Phase 2.1: 全部 batch 写完后主动 refresh 一次。
    # insert 内部已经改 refresh="false"，避免 per-batch 等 1-2s。
    # 这里 refresh 让后续 query（resolution / community 阶段读图）能立刻看到新数据。
    if chunks:
        try:
            refresh_fn = getattr(settings.docStoreConn, "refresh_idx", None)
            if refresh_fn is not None:
                await thread_pool_exec(refresh_fn, search.index_name(tenant_id))
            else:
                # 兜底：Infinity 等其他 doc store 可能没有 refresh_idx 接口
                # 退而依赖默认 refresh_interval
                logging.debug("docStoreConn has no refresh_idx; relying on default refresh_interval")
        except Exception:
            logging.exception("post-insert refresh_idx failed (will rely on default refresh_interval)")
    now = asyncio.get_running_loop().time()
    if callback:
        callback(msg=f"set_graph added/updated {len(change.added_updated_nodes)} nodes and {len(change.added_updated_edges)} edges from index in {now - start:.2f}s.")


async def set_graph(tenant_id: str, kb_id: str, embd_mdl, graph: nx.Graph, change: GraphChange, callback):
    """Router: dispatches to the monolithic or incremental path based on
    ``GraphRAGConfig.USE_INCREMENTAL_GRAPH``.

    Callers (``merge_subgraph``, ``resolve_entities``, ``extract_community``)
    do not need to change; the switch is transparent.
    """
    if GraphRAGConfig.USE_INCREMENTAL_GRAPH:
        await set_graph_delta(tenant_id, kb_id, embd_mdl, graph, change, callback)
    else:
        await _set_graph_monolithic(tenant_id, kb_id, embd_mdl, graph, change, callback)


def is_continuous_subsequence(subseq, seq):
    def find_all_indexes(tup, value):
        indexes = []
        start = 0
        while True:
            try:
                index = tup.index(value, start)
                indexes.append(index)
                start = index + 1
            except ValueError:
                break
        return indexes

    index_list = find_all_indexes(seq, subseq[0])
    for idx in index_list:
        if idx != len(seq) - 1:
            if seq[idx + 1] == subseq[-1]:
                return True
    return False


def merge_tuples(list1, list2):
    result = []
    for tup in list1:
        last_element = tup[-1]
        if last_element in tup[:-1]:
            result.append(tup)
        else:
            matching_tuples = [t for t in list2 if t[0] == last_element]
            already_match_flag = 0
            for match in matching_tuples:
                matchh = (match[1], match[0])
                if is_continuous_subsequence(match, tup) or is_continuous_subsequence(matchh, tup):
                    continue
                already_match_flag = 1
                merged_tuple = tup + match[1:]
                result.append(merged_tuple)
            if not already_match_flag:
                result.append(tup)
    return result


async def get_entity_type2samples(idxnms, kb_ids: list):
    es_res = await settings.retriever.search({"knowledge_graph_kwd": "ty2ents", "kb_id": kb_ids, "size": 10000, "fields": ["content_with_weight"]},idxnms,kb_ids)

    res = defaultdict(list)
    for id in es_res.ids:
        smp = es_res.field[id].get("content_with_weight")
        if not smp:
            continue
        try:
            smp = json.loads(smp)
        except Exception as e:
            logging.exception("Failed to parse entity type samples: %s", e)

        for ty, ents in smp.items():
            res[ty].extend(ents)
    return res


def flat_uniq_list(arr, key):
    res = []
    for a in arr:
        a = a[key]
        if isinstance(a, list):
            res.extend(a)
        else:
            res.append(a)
    return list(set(res))


async def rebuild_graph(tenant_id, kb_id, exclude_rebuild=None):
    graph = nx.Graph()
    flds = ["knowledge_graph_kwd", "content_with_weight", "source_id"]
    bs = 5000
    for i in range(0, 1024 * 256, bs):
        es_res = await thread_pool_exec(
            settings.docStoreConn.search,
            flds, [], {"kb_id": kb_id, "knowledge_graph_kwd": ["subgraph"]},
            [], OrderByExpr(), i, bs, search.index_name(tenant_id), [kb_id]
        )
        # tot = settings.docStoreConn.get_total(es_res)
        es_res = settings.docStoreConn.get_fields(es_res, flds)

        if len(es_res) == 0:
            break

        for id, d in es_res.items():
            assert d["knowledge_graph_kwd"] == "subgraph"
            if isinstance(exclude_rebuild, list):
                if sum([n in d["source_id"] for n in exclude_rebuild]):
                    continue
            elif exclude_rebuild in d["source_id"]:
                continue

            next_graph = json_graph.node_link_graph(json.loads(d["content_with_weight"]), edges="edges")
            merged_graph = nx.compose(graph, next_graph)
            merged_source = {n: graph.nodes[n]["source_id"] + next_graph.nodes[n]["source_id"] for n in graph.nodes & next_graph.nodes}
            nx.set_node_attributes(merged_graph, merged_source, "source_id")
            if "source_id" in graph.graph:
                merged_graph.graph["source_id"] = graph.graph["source_id"] + next_graph.graph["source_id"]
            else:
                merged_graph.graph["source_id"] = next_graph.graph["source_id"]
            graph = merged_graph

    if len(graph.nodes) == 0:
        return None
    graph.graph["source_id"] = sorted(graph.graph["source_id"])
    return graph
