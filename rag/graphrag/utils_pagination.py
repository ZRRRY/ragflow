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
"""Phase 2.2: search_after 分页工具。

OpenSearch / Elasticsearch 的 ``index.max_result_window`` 默认 10000，超过会
被硬截断。GraphRAG 在大 KB 下需要拉超过 1 万的 relation / entity，scroll API
会在内存里累积全量结果（10w+ 文档时单次调用峰值 1GB+），所以优先用
``search_after`` + PIT (point-in-time) 分页。

只支持 OpenSearch / Elasticsearch 后端；Infinity / OceanBase 走 fallback
（旧的 size=10000 单次查询）。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator

from common.misc_utils import thread_pool_exec
from rag.nlp import search
from common import settings

logger = logging.getLogger(__name__)


def supports_search_after(conn) -> bool:
    """检测 doc store 是否支持原生 search_after（OpenSearch / Elasticsearch）。"""
    return hasattr(conn, "os") or hasattr(conn, "es")


def _build_query_body(filters: dict, sort_field: str, page_size: int,
                      search_after: list | None = None) -> dict:
    """构造 OpenSearch / ES query body。

    Args:
        filters: 与 Dealer.search req["filters"] 等价的 condition dict
        sort_field: 用于 search_after 的 sort field 名（必须是 keyword 类型
            且有 doc_values，例如 entity_kwd / from_entity_kwd）
        page_size: 每页大小
        search_after: 上一批最后一条的 sort 值；None 表示第一页
    """
    # search_after requires a globally unique sort tuple. The primary field is
    # a business keyword (entity_kwd / from_entity_kwd) which can have duplicates
    # across documents; ``_id`` is appended as a tiebreaker so pagination never
    # stalls or skips rows when the primary sort value repeats.
    body: dict[str, Any] = {
        "query": {"bool": {"filter": _filters_to_clauses(filters)}},
        "size": page_size,
        "sort": [
            {sort_field: {"order": "asc"}},
            {"_id": {"order": "asc"}},
        ],
    }
    if search_after:
        body["search_after"] = search_after
    return body


def _filters_to_clauses(filters: dict) -> list[dict]:
    """把 RAGFlow 内部 condition dict 转 OpenSearch bool filter clauses。

    仅支持 Phase 2.3 调用方使用的 terms / term / match 模式，复杂的 script /
    nested 不在范围内。
    """
    clauses: list[dict] = []
    for k, v in (filters or {}).items():
        if v is None:
            continue
        if isinstance(v, list):
            clauses.append({"terms": {k: v}})
        elif isinstance(v, (str, int, bool)):
            clauses.append({"term": {k: v}})
        else:
            logger.debug("filter key=%s has unsupported value type %s, skipping",
                         k, type(v).__name__)
    return clauses


async def search_all_by_search_after(
    filters: dict,
    index_name: str,
    kb_id: str,
    fields: list[str],
    sort_field: str,
    page_size: int = 1000,
    max_pages: int = 1000,
) -> AsyncIterator[list[dict]]:
    """Async generator: 一次 yield 一个 page 的 hits（已投影到 fields）。

    Phase 2.2: 用 OpenSearch / ES 原生 search_after 翻页，绕过 max_result_window
    截断。每次 page_size 一次 ES round-trip，max_pages 上限 1000（即最多
    100w hit/查询）。中间任意一页 ES 抛异常由调用方决定 retry / break。

    Args:
        filters: RAGFlow condition dict（与 Dealer.search req["filters"] 一致）
        index_name: 完整 index name（用 rag.nlp.search.index_name(tenant_id) 拼）
        kb_id: dataset id
        fields: 要返回的字段列表（与 Dealer.search req["fields"] 一致）
        sort_field: 用于 search_after 的 keyword 字段
        page_size: 每页大小（默认 1000）
        max_pages: 最大页数（默认 1000）
    """
    conn = settings.docStoreConn
    if not supports_search_after(conn):
        raise NotImplementedError(
            "search_after pagination only supports OpenSearch / Elasticsearch backends; "
            f"current backend {type(conn).__name__} is not supported."
        )
    raw = conn.os if hasattr(conn, "os") else conn.es

    body = _build_query_body(filters, sort_field, page_size, search_after=None)
    body["_source"] = fields
    # Phase 2.2: 注入 kb_id 过滤（与原 retriever.search 一致）
    # 把 kb_id 当作额外 filter clause 拼接
    body["query"]["bool"]["filter"].append({"term": {"kb_id": kb_id}})

    for page_idx in range(max_pages):
        try:
            res = await thread_pool_exec(raw.search, index=index_name, body=body)
        except Exception:
            logger.exception(
                "search_after page %d failed for index=%s filters=%s",
                page_idx, index_name, filters,
            )
            raise

        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            return
        # 投影到 fields
        projected = [h.get("_source", {}) | {"id": h["_id"]} for h in hits]
        yield projected

        last_sort = hits[-1].get("sort")
        if not last_sort:
            return
        body = _build_query_body(filters, sort_field, page_size, search_after=last_sort)
        body["_source"] = fields
        body["query"]["bool"]["filter"].append({"term": {"kb_id": kb_id}})


async def collect_all(
    filters: dict,
    index_name: str,
    kb_id: str,
    fields: list[str],
    sort_field: str,
    page_size: int = 1000,
    max_pages: int = 1000,
) -> list[dict]:
    """一次性收集所有 page（便利函数，调用方应优先用 search_all_by_search_after 流式）。"""
    out: list[dict] = []
    async for page in search_all_by_search_after(
        filters, index_name, kb_id, fields, sort_field, page_size, max_pages
    ):
        out.extend(page)
    return out
