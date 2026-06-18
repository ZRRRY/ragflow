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
"""Audit hook for docStoreConn.delete.

Background
==========
Prior to this hook, ``settings.docStoreConn.delete(...)`` calls that removed
GraphRAG artefacts (subgraph / graph / entity / relation / community_report
chunks) had no observable log line. As a result, "154 subgraphs vanished
overnight" was impossible to debug from logs alone. This module installs a
thin wrapper around ``delete`` that emits a single WARNING line per call
identifying the kind of payload being removed and the caller location.

Design goals
============
* Zero impact on the hot path: the wrapper must not call the doc store
  itself (no extra search / count query) and must NEVER raise into the
  caller. Any audit failure is logged at DEBUG and swallowed.
* Backwards compatible: ``delete(condition, indexName, knowledgebaseId)``
  is preserved exactly — same signature, same return value, same
  exceptions raised by the underlying connection.
* Idempotent install: calling ``install()`` more than once is a no-op.
* Always-on, no opt-out flag: the wrapper is intentionally not gated by
  an env var; the cost is one logging call per delete and the value of
  auditability is too high to leave it off by default.

Sample log line
===============
::

    AUDIT docStoreConn.delete: kws=['subgraph'] index=ragflow_xxx dataset=yyy
        condition_keys=['kb_id', 'knowledge_graph_kwd', 'source_id']
        caller=rag/graphrag/general/index.py:1110

Where to install
================
At the bottom of ``common/settings.py`` immediately after ``docStoreConn``
is assigned in ``init_settings`` (it covers the API server, the task
executor, the admin service and the data sync service in one place).
"""

from __future__ import annotations

import inspect
import logging
import os
from typing import Any

_logger = logging.getLogger(__name__)

# Knowledge-graph keywords that, if matched by a delete condition, are worth
# auditing. ``raptor_kwd`` is included for symmetry — wiping raptor artefacts
# has historically also surprised operators.
_AUDITED_KEY_FIELDS: tuple[str, ...] = ("knowledge_graph_kwd", "raptor_kwd")

_KG_KEYWORDS: frozenset[str] = frozenset({
    "subgraph",
    "graph",
    "entity",
    "relation",
    "community_report",
    "merge_state",
    # legacy alias seen in some doc-store implementations
    "raptor",
})

_INSTALL_FLAG = "_ragflow_docstore_audit_installed"


def _is_audited(condition: Any) -> tuple[bool, list[str], list[str]]:
    """Return (is_audited, key_field_names, matched_keyword_values)."""
    if not isinstance(condition, dict):
        return False, [], []
    key_fields = [k for k in _AUDITED_KEY_FIELDS if k in condition]
    if not key_fields:
        return False, [], []
    matched: list[str] = []
    for f in key_fields:
        v = condition[f]
        items = v if isinstance(v, (list, tuple, set)) else [v]
        for kw in items:
            if isinstance(kw, str) and kw in _KG_KEYWORDS:
                matched.append(kw)
    return (len(matched) > 0), key_fields, matched


def _caller_location(skip_frames: int = 3) -> str:
    """Return a short ``file:line`` for the immediate caller of delete().

    ``skip_frames`` accounts for: 0=this fn, 1=wrapper, 2=caller of
    wrapper. We add 1 more to land on the *direct* caller's line, not on
    a deep internal frame.
    """
    try:
        frame = inspect.currentframe()
        for _ in range(skip_frames):
            if frame is None:
                break
            frame = frame.f_back
        if frame is None:
            return "<unknown>"
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Trim leading absolute path to keep log lines compact.
        # Use both POSIX and Windows separators so the trim is portable.
        for needle in ("/ragflow/", "\\ragflow\\", "/ragflow\\", "\\ragflow/"):
            idx = filename.rfind(needle)
            if idx >= 0:
                # Skip the leading separator that rfind landed on.
                start = idx + len(needle)
                return f"{filename[start:]}:{lineno}"
        return f"{filename}:{lineno}"
    except Exception:  # never let audit machinery raise
        return "<unresolvable>"


def make_audited_delete(original_delete):
    """Build a wrapper that audits KG deletes but is otherwise identical."""

    def audited_delete(condition, indexName, knowledgebaseId):
        try:
            is_audited, key_fields, matched = _is_audited(condition)
            if is_audited:
                _logger.warning(
                    "AUDIT docStoreConn.delete: kws=%s condition_keys=%s "
                    "index=%s dataset=%s caller=%s",
                    sorted(set(matched)),
                    key_fields,
                    indexName,
                    knowledgebaseId,
                    _caller_location(skip_frames=3),
                )
        except Exception:
            # Audit must never block a real delete.
            _logger.debug("docStoreConn.delete audit hook failed (non-fatal)", exc_info=True)
        return original_delete(condition, indexName, knowledgebaseId)

    # Preserve introspection: mark the wrapper so debugging tools see it
    # as a distinct function, not as the bare original.
    audited_delete.__wrapped__ = original_delete  # type: ignore[attr-defined]
    audited_delete.__name__ = "audited_delete"
    audited_delete.__doc__ = original_delete.__doc__
    return audited_delete


def install(docStoreConn) -> bool:
    """Install the audit wrapper on ``docStoreConn``.

    Returns True if installation was performed, False if already installed
    or if the connection lacks a ``delete`` attribute (e.g. legacy stubs).
    """
    if docStoreConn is None:
        _logger.debug("docStoreConn is None; skipping audit hook install")
        return False
    if getattr(docStoreConn, _INSTALL_FLAG, False):
        return False
    original = getattr(docStoreConn, "delete", None)
    if original is None or not callable(original):
        _logger.debug("docStoreConn has no callable delete; skipping audit hook install")
        return False
    # Avoid double-wrapping: if the current delete is already our wrapper
    # (e.g. import chain re-installed), bail out.
    if getattr(original, "__name__", "") == "audited_delete":
        setattr(docStoreConn, _INSTALL_FLAG, True)
        return False
    docStoreConn.delete = make_audited_delete(original)
    setattr(docStoreConn, _INSTALL_FLAG, True)
    _logger.info("docStoreConn.delete audit hook installed (target=%s)", type(docStoreConn).__name__)
    return True


def uninstall(docStoreConn) -> bool:
    """Restore the original ``delete`` method. Mainly for tests."""
    if not getattr(docStoreConn, _INSTALL_FLAG, False):
        return False
    current = getattr(docStoreConn, "delete", None)
    original = getattr(current, "__wrapped__", None)
    if original is not None:
        docStoreConn.delete = original
    setattr(docStoreConn, _INSTALL_FLAG, False)
    return True
