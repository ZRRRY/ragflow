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
"""

from __future__ import annotations

import json
import logging

from elasticsearch import NotFoundError
from elasticsearch_dsl import Q
from elastic_transport import ConnectionTimeout

_logger = logging.getLogger(__name__)

_INSTALL_FLAG = "_ragflow_es_conn_extras_installed"


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


def install() -> None:
    """Idempotently install custom extras onto ``ESConnection``."""
    from rag.utils.es_conn import ESConnection

    if getattr(ESConnection, _INSTALL_FLAG, False):
        return

    if not hasattr(ESConnection, "count"):
        ESConnection.count = _es_count
        _logger.info("ESConnection.count custom extra installed")

    if not hasattr(ESConnection, "_original_insert"):
        ESConnection._original_insert = ESConnection.insert
        ESConnection.insert = _es_insert
        _logger.info("ESConnection.insert auto-refresh wrapper installed")

    setattr(ESConnection, _INSTALL_FLAG, True)
