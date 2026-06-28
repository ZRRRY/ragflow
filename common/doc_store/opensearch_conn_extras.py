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
"""Custom extras for the OpenSearch connection.

This module monkey-patches additional methods onto ``rag.utils.opensearch_conn.OSConnection``
so that custom GraphRAG incremental/optimization logic can use OpenSearch-specific queries
without modifying the official connection class.

Methods installed
=================
* ``OSConnection.knn_search_entities(...) -> dict``
  KNN query filtered to entity chunks.
* ``OSConnection.search_with_scroll(...) -> dict``
  Scroll-based retrieval for large result sets.
* ``OSConnection.insert(documents, index_name, knowledgebase_id) -> list[str]``
  Bulk insert with ``refresh="false"`` and increased timeout for high-volume GraphRAG writes.
* ``OSConnection.count(condition, index_name, knowledgebase_ids) -> int``
  Count documents matching ``condition`` within the given knowledge bases.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import time

from opensearchpy import NotFoundError
from opensearchpy import Q

_logger = logging.getLogger(__name__)

_INSTALL_FLAG = "_ragflow_opensearch_conn_extras_installed"

# Memory guard for scroll-based retrieval. Override via GRAPHRAG_SEARCH_WITH_SCROLL_HITS_CAP.
_DEFAULT_HITS_CAP = max(1, int(os.environ.get("GRAPHRAG_SEARCH_WITH_SCROLL_HITS_CAP", "50000")))


def _should_auto_refresh_after_insert(operations: list[dict], index_name: str) -> bool:
    """Best-effort heuristic for add_chunk-like single-chunk inserts.

    operations is an OpenSearch bulk body with alternating action-metadata and
    source-document dicts. Auto-refresh only when we see exactly one source doc
    in a non-graph index and that doc carries a chunk vector field.
    """
    if not operations:
        return False
    docs = [operations[i] for i in range(1, len(operations), 2) if isinstance(operations[i], dict)]
    if len(docs) != 1:
        return False
    if "graph" in index_name.lower():
        return False
    doc = docs[0]
    return any(k.startswith("q_") and k.endswith("_vec") for k in doc)


def _os_knn_search_entities(
    self,
    index_names: str | list[str],
    knowledgebase_ids: list[str],
    vector: list[float],
    vector_column_name: str,
    k: int,
    min_score: float | None = None,
    entity_type: str | None = None,
    exclude_name: str | None = None,
) -> dict:
    """OpenSearch KNN query for entity chunks.

    Returns the raw OpenSearch response so callers can use
    ``get_fields()`` / ``get_scores()`` on it.
    """
    from rag.utils.opensearch_conn import ATTEMPT_TIME

    if isinstance(index_names, str):
        index_names = index_names.split(",")

    filter_must = [
        {"terms": {"kb_id": knowledgebase_ids}},
        {"terms": {"knowledge_graph_kwd": ["entity"]}},
    ]
    if entity_type:
        filter_must.append({"term": {"entity_type_kwd": entity_type}})

    knn_body = {
        "query": {
            "knn": {
                vector_column_name: {
                    "vector": list(vector),
                    "k": k,
                    "filter": {"bool": {"must": filter_must}},
                }
            }
        }
    }
    if min_score is not None:
        knn_body["query"]["knn"][vector_column_name]["min_score"] = min_score
    if exclude_name:
        knn_body["query"]["knn"][vector_column_name]["filter"]["bool"]["must_not"] = [
            {"term": {"entity_kwd": exclude_name}}
        ]

    for i in range(ATTEMPT_TIME):
        try:
            res = self.os.search(
                index=index_names,
                body=knn_body,
                timeout=600,
                track_total_hits=True,
                _source=True,
            )
            _logger.debug(f"OSConnection.knn_search_entities {str(index_names)} res: " + str(res))
            return res
        except Exception as e:
            _logger.exception(
                f"OSConnection.knn_search_entities {str(index_names)} query: " + str(knn_body)
            )
            if str(e).find("Timeout") > 0:
                continue
            raise e
    _logger.error(f"OSConnection.knn_search_entities timeout for {ATTEMPT_TIME} times!")
    raise Exception("OSConnection.knn_search_entities timeout.")


def _os_search_with_scroll(
    self,
    index_names,
    query_body: dict,
    fields: list[str],
    scroll_timeout="2m",
    batch_size=1000,
    max_pages: int = 1000,
    hits_cap: int | None = None,
):
    """Use OpenSearch scroll API to fetch all results safely.

    This bypasses the index.max_result_window limit and is suitable
    for retrieving large result sets (e.g. GraphRAG entity/relation
    chunks) without causing OpenSearch OOM or connection storms.

    Args:
        max_pages: Hard upper bound on the number of scroll pages.
            Default 1000 (matches ``search_all_by_search_after``) so a
            single query returns at most ``max_pages × batch_size``
            documents (~1M). Prevents the scroll loop from hanging
            indefinitely if OpenSearch starts returning empty pages
            without raising (e.g. transient connection blip).
        hits_cap: Hard upper bound on the number of hits to collect.
            Defaults to ``GRAPHRAG_SEARCH_WITH_SCROLL_HITS_CAP`` (50000).
    """
    cap = hits_cap if hits_cap is not None else _DEFAULT_HITS_CAP

    scroll_id = None
    try:
        res = self.os.search(
            index=index_names,
            body=query_body,
            scroll=scroll_timeout,
            size=batch_size,
            _source=True,
        )
        scroll_id = res.get("_scroll_id")
        hits = res["hits"]["hits"]

        # Memory guard: a large KB can produce millions of hits per scroll
        # call.  Cap at 50k hits (~50MB Python heap) by default to avoid
        # worker OOM in callers that load the whole result set into a dict.
        pages_consumed = 1
        hit_cap_reached = False
        while pages_consumed < max_pages:
            page = self.os.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
            page_hits = page["hits"]["hits"]
            if not page_hits:
                break
            hits.extend(page_hits)
            if len(hits) >= cap:
                hit_cap_reached = True
                break
            scroll_id = page.get("_scroll_id")
            if not scroll_id:
                break
            pages_consumed += 1

        if hit_cap_reached:
            _logger.warning(
                "search_with_scroll hit hits_cap=%d (collected %d hits); "
                "narrow query or add post-filter",
                cap, len(hits),
            )
        elif pages_consumed >= max_pages:
            # Reached max_pages without an empty page — log so operators
            # know to raise the cap or narrow the query.
            _logger.warning(
                "search_with_scroll hit max_pages=%d cap (collected %d hits); "
                "narrow query or raise max_pages",
                max_pages, len(hits),
            )

        # Return format compatible with __getSource / get_fields
        return {"hits": {"hits": hits}}
    except Exception as e:
        _logger.exception(
            f"OSConnection.search_with_scroll {str(index_names)} query: " + json.dumps(query_body)
        )
        raise e
    finally:
        if scroll_id:
            try:
                self.os.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


def _os_insert(self, documents: list[dict], indexName: str, knowledgebaseId: str = None) -> list[str]:
    """Insert documents via the OpenSearch bulk API.

    Contract (P2-15)
    ================
    This method writes with ``refresh="false"`` for throughput. The caller
    **MUST** ensure an explicit refresh (e.g. via ``refresh_idx``) is issued
    once *after* all batches of the same logical operation complete, so that
    downstream queries observe the new data.

    Failure mode if violated
    -----------------------
    If the caller forgets the post-batch refresh, intermediate queries will
    observe stale results for up to ``index.refresh_interval`` seconds
    (default 1s in OpenSearch). For single-batch callers this may be
    acceptable; for multi-batch GraphRAG writes (e.g. ``set_graph_delta``,
    ``set_graph_monolithic``, ``insert_chunks_bounded``) it is **NOT**.

    Acceptable refresh strategies:
      * ``await docStoreConn.os.indices.refresh(index=...)``           # explicit
      * rely on the default ``index.refresh_interval`` (1s)            # best-effort
    """
    from rag.utils.opensearch_conn import ATTEMPT_TIME

    # Refers to https://opensearch.org/docs/latest/api-reference/document-apis/bulk/
    operations = []
    for d in documents:
        assert "_id" not in d
        assert "id" in d
        d_copy = copy.deepcopy(d)
        # Use id as _id for uniqueness, but keep "id" in the document so the
        # doc-meta read path (DocMetadataService filters on / sorts by the
        # "id" field) can find it, mirroring ESConnection.insert().
        meta_id = d_copy.get("id", "")
        operations.append(
            {"index": {"_index": indexName, "_id": meta_id}})
        operations.append(d_copy)

    res = []
    for _ in range(ATTEMPT_TIME):
        try:
            res = []
            # Phase 2.1: refresh="wait_for" -> "false"
            # OpenSearch 默认 refresh_interval=1s，每批 bulk 等 1-2s refresh
            # 在 100w+ chunks 的 KB 上浪费 4-8 小时。改 false 后由调用方在
            # 全部 batch 写完时主动 refresh 一次（见 set_graph_delta/set_graph_monolithic）。
            # 中间查询的 stale 风险靠 OpenSearch 默认 1s 自然 refresh 兜底。
            r = self.os.bulk(index=(indexName), body=operations,
                             refresh="false", timeout=300)
            if not r["errors"]:
                # Best-effort auto-refresh for add_chunk-like single chunk inserts
                # so the UI list_chunks call sees the new document immediately.
                if _should_auto_refresh_after_insert(operations, indexName):
                    try:
                        self.refresh_idx(indexName)
                    except Exception:
                        _logger.exception("refresh_idx failed after insert (will rely on default refresh_interval)")
                return res

            for item in r["items"]:
                for action in ["create", "delete", "index", "update"]:
                    if action in item and "error" in item[action]:
                        res.append(str(item[action]["_id"]) + ":" + str(item[action]["error"]))
            return res
        except Exception as e:
            res.append(str(e))
            _logger.warning("OSConnection.insert got exception: " + str(e))
            res = []
            if re.search(r"(Timeout|time out)", str(e), re.IGNORECASE):
                res.append(str(e))
                time.sleep(3)
                continue
    return res


def _os_count(self, condition: dict, indexName: str, knowledgebaseIds: list[str]) -> int:
    assert "_id" not in condition
    cond = condition.copy()
    cond["kb_id"] = knowledgebaseIds

    from rag.utils.opensearch_conn import ATTEMPT_TIME

    bqry = Q("bool", must=[])
    for k, v in cond.items():
        if k == "available_int":
            if v == 0:
                bqry.filter.append(Q("range", available_int={"lt": 1}))
            else:
                bqry.filter.append(
                    Q("bool", must_not=Q("range", available_int={"lt": 1})))
            continue
        if not v:
            continue
        if isinstance(v, list):
            bqry.filter.append(Q("terms", **{k: v}))
        elif isinstance(v, str) or isinstance(v, int):
            bqry.filter.append(Q("term", **{k: v}))
        else:
            raise Exception(
                f"Condition `{str(k)}={str(v)}` value type is {str(type(v))}, expected to be int, str or list.")

    if not bqry.filter and not bqry.must and not bqry.must_not:
        qry = {"match_all": {}}
    else:
        qry = bqry.to_dict()
    body = {"query": qry}
    _logger.debug(f"OSConnection.count {indexName} query: " + json.dumps(body))

    for i in range(ATTEMPT_TIME):
        try:
            res = self.os.count(index=indexName, body=body)
            return int(res.get("count", 0))
        except NotFoundError:
            return 0
        except Exception as e:
            _logger.exception(f"OSConnection.count {indexName} query: " + json.dumps(body))
            if str(e).find("Timeout") > 0:
                continue
            raise e
    _logger.error(f"OSConnection.count timeout for {ATTEMPT_TIME} times!")
    raise Exception("OSConnection.count timeout.")


def install() -> None:
    """Idempotently install custom extras onto ``OSConnection``."""
    from rag.utils.opensearch_conn import OSConnection

    if getattr(OSConnection, _INSTALL_FLAG, False):
        return

    if not hasattr(OSConnection, "knn_search_entities"):
        OSConnection.knn_search_entities = _os_knn_search_entities
        _logger.info("OSConnection.knn_search_entities custom extra installed")

    if not hasattr(OSConnection, "search_with_scroll"):
        OSConnection.search_with_scroll = _os_search_with_scroll
        _logger.info("OSConnection.search_with_scroll custom extra installed")

    if not hasattr(OSConnection, "count"):
        OSConnection.count = _os_count
        _logger.info("OSConnection.count custom extra installed")

    # Always override insert to use the GraphRAG-optimized bulk settings.
    OSConnection.insert = _os_insert
    _logger.info("OSConnection.insert custom extra installed")

    setattr(OSConnection, _INSTALL_FLAG, True)
