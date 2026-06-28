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
"""Custom extras for the Elasticsearch connection.

This module monkey-patches additional methods onto ``rag.utils.es_conn.ESConnection``
so that custom GraphRAG logic can query document counts without modifying the
official connection class.

Methods installed
=================
* ``ESConnection.count(condition, index_name, knowledgebase_ids) -> int``
  Count documents matching ``condition`` within the given knowledge bases.
* ``ESConnection.search_with_scroll(index_names, query_body, fields, ...) -> dict``
  Scroll-based retrieval for large result sets, mirroring the OpenSearch extra.
"""

from __future__ import annotations

import inspect
import json
import logging

from elasticsearch import NotFoundError
from elasticsearch_dsl import Q
from elastic_transport import ConnectionTimeout

_logger = logging.getLogger(__name__)

_INSTALL_FLAG = "_ragflow_es_conn_extras_installed"


def _es_search_with_scroll(
    self,
    index_names,
    query_body: dict,
    fields: list[str],
    scroll_timeout="2m",
    batch_size=1000,
    max_pages: int = 1000,
):
    """Use Elasticsearch scroll API to fetch large result sets safely.

    Mirrors ``OSConnection.search_with_scroll`` so the GraphRAG incremental
    paths can assemble the global graph on Elasticsearch backends too.
    """
    from rag.utils.es_conn import ATTEMPT_TIME

    scroll_id = None
    try:
        for _ in range(ATTEMPT_TIME):
            try:
                res = self.es.search(
                    index=index_names,
                    body=query_body,
                    scroll=scroll_timeout,
                    size=batch_size,
                )
                break
            except ConnectionTimeout:
                self.logger.exception("ES search_with_scroll initial request timeout")
                self._connect()
                continue
            except Exception:
                raise
        else:
            raise Exception("ESConnection.search_with_scroll timeout for initial request.")

        scroll_id = res.get("_scroll_id")
        hits = res["hits"]["hits"]

        _HITS_CAP = 50000
        pages_consumed = 1
        hit_cap_reached = False
        while pages_consumed < max_pages:
            for _ in range(ATTEMPT_TIME):
                try:
                    page = self.es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
                    break
                except ConnectionTimeout:
                    self.logger.exception("ES search_with_scroll scroll request timeout")
                    self._connect()
                    continue
                except Exception:
                    raise
            else:
                raise Exception("ESConnection.search_with_scroll timeout for scroll request.")

            page_hits = page["hits"]["hits"]
            if not page_hits:
                break
            hits.extend(page_hits)
            if len(hits) >= _HITS_CAP:
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
                _HITS_CAP, len(hits),
            )
        elif pages_consumed >= max_pages:
            _logger.warning(
                "search_with_scroll hit max_pages=%d cap (collected %d hits); "
                "narrow query or raise max_pages",
                max_pages, len(hits),
            )

        return {"hits": {"hits": hits}}
    except Exception as e:
        _logger.exception("ESConnection.search_with_scroll query: %s", json.dumps(query_body))
        raise e
    finally:
        if scroll_id:
            try:
                self.es.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


def _es_count(self, condition: dict, index_name: str, knowledgebase_ids: list[str]) -> int:
    """Count documents matching ``condition`` filtered by ``knowledgebase_ids``."""
    # Import the attempt constant from the same module that defines ESConnection
    # so we stay consistent with its retry semantics.
    from rag.utils.es_conn import ATTEMPT_TIME

    assert "_id" not in condition
    cond = condition.copy()
    cond["kb_id"] = knowledgebase_ids

    bool_query = Q("bool", must=[])
    for k, v in cond.items():
        if k == "available_int":
            if v == 0:
                bool_query.filter.append(Q("range", available_int={"lt": 1}))
            else:
                bool_query.filter.append(
                    Q("bool", must_not=Q("range", available_int={"lt": 1}))
                )
            continue
        if not v:
            continue
        if isinstance(v, list):
            bool_query.filter.append(Q("terms", **{k: v}))
        elif isinstance(v, str) or isinstance(v, int):
            bool_query.filter.append(Q("term", **{k: v}))
        else:
            raise Exception(
                f"Condition `{str(k)}={str(v)}` value type is {str(type(v))}, expected to be int, str or list."
            )

    if not bool_query.filter and not bool_query.must and not bool_query.must_not:
        qry = {"match_all": {}}
    else:
        qry = bool_query.to_dict()
    body = {"query": qry}
    self.logger.debug(f"ESConnection.count {index_name} query: " + json.dumps(body))

    for _ in range(ATTEMPT_TIME):
        try:
            res = self.es.count(index=index_name, body=body)
            return int(res.get("count", 0))
        except NotFoundError:
            return 0
        except ConnectionTimeout:
            self.logger.exception("ES request timeout")
            self._connect()
            continue
        except Exception as e:
            self.logger.exception(f"ESConnection.count {index_name} query: " + json.dumps(body) + str(e))
            raise e
    self.logger.error(f"ESConnection.count timeout for {ATTEMPT_TIME} times!")
    raise Exception("ESConnection.count timeout.")


def _should_auto_refresh_after_insert(documents: list[dict], index_name: str) -> bool:
    """Best-effort heuristic for add_chunk-like single-chunk inserts.

    Auto-refresh only for single-document inserts into a non-graph index where
    the document carries a chunk vector field. This mirrors the explicit
    refresh_idx call previously added to api/apps/restful_apis/chunk_api.py.
    """
    if not documents or len(documents) != 1:
        return False
    if "graph" in index_name.lower():
        return False
    doc = documents[0]
    return any(k.startswith("q_") and k.endswith("_vec") for k in doc)


def _es_insert(self, documents: list[dict], index_name: str, knowledgebase_id: str = None) -> list[str]:
    """Wrap official insert with a best-effort refresh for single chunk inserts."""
    result = self._original_insert(documents, index_name, knowledgebase_id)
    if _should_auto_refresh_after_insert(documents, index_name):
        try:
            self.refresh_idx(index_name)
        except Exception:
            _logger.exception("refresh_idx failed after insert (will rely on default refresh_interval)")
    return result


def _get_real_es_connection_class():
    """Return the real ESConnection class, unwrapping the singleton decorator.

    ``rag.utils.es_conn.ESConnection`` is decorated with ``@singleton``, which
    replaces the class with a factory function.  Monkey-patching the factory
    function does not affect instances, so we must reach the underlying class.
    """
    from rag.utils.es_conn import ESConnection

    if inspect.isclass(ESConnection):
        return ESConnection

    if inspect.isfunction(ESConnection) and ESConnection.__closure__:
        for cell in ESConnection.__closure__:
            val = cell.cell_contents
            if inspect.isclass(val) and val.__name__ == "ESConnection":
                return val

    raise RuntimeError(
        "Could not locate the real ESConnection class inside the singleton wrapper."
    )


def install() -> None:
    """Idempotently install custom extras onto the real ``ESConnection`` class."""
    cls = _get_real_es_connection_class()

    if getattr(cls, _INSTALL_FLAG, False):
        return

    if not hasattr(cls, "search_with_scroll"):
        cls.search_with_scroll = _es_search_with_scroll
        _logger.info("ESConnection.search_with_scroll custom extra installed")

    if not hasattr(cls, "count"):
        cls.count = _es_count
        _logger.info("ESConnection.count custom extra installed")

    if not hasattr(cls, "_original_insert"):
        cls._original_insert = cls.insert
        cls.insert = _es_insert
        _logger.info("ESConnection.insert auto-refresh wrapper installed")

    setattr(cls, _INSTALL_FLAG, True)
