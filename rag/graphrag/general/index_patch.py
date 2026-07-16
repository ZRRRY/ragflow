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
"""GraphRAG incremental/optimization monkey-patch dispatcher.

This module is imported at the very end of ``rag.graphrag.general.index`` so
that the custom implementations can replace the official functions in-place.
When every feature switch is off the original implementations are preserved.
"""

import logging

from rag.graphrag.config import GraphRAGConfig

logger = logging.getLogger(__name__)

_ORIGINALS: dict[str, callable] = {}


def _store_original(module, name: str):
    _ORIGINALS[name] = getattr(module, name)


def apply_patch(module):
    """Replace selected functions in ``rag.graphrag.general.index``.

    Must be called once after the module is fully imported. The original
    callables are kept so that wrappers can fall back to official behaviour
    when the corresponding feature flags are disabled.
    """
    names = [
        "run_graphrag_for_kb",
        "generate_subgraph",
        "merge_subgraph",
        "resolve_entities",
        "extract_community",
    ]
    for name in names:
        _store_original(module, name)

    module.run_graphrag_for_kb = _wrap_run_graphrag_for_kb
    module.generate_subgraph = _wrap_generate_subgraph
    module.merge_subgraph = _wrap_merge_subgraph
    module.resolve_entities = _wrap_resolve_entities
    module.extract_community = _wrap_extract_community
    logger.debug("GraphRAG index patch applied; flags=%s", _feature_flags())


def _feature_flags():
    return {
        "incremental_graph": GraphRAGConfig.USE_INCREMENTAL_GRAPH,
        "incremental_merge": GraphRAGConfig.USE_INCREMENTAL_MERGE,
        "incremental_resolution": GraphRAGConfig.USE_INCREMENTAL_RESOLUTION,
        "async_community": GraphRAGConfig.USE_ASYNC_COMMUNITY,
        "async_kg_phases": GraphRAGConfig.USE_ASYNC_KG_PHASES,
        "chapter_graph": GraphRAGConfig.USE_CHAPTER_GRAPH,
        "keep_subgraph": GraphRAGConfig.KEEP_SUBGRAPH,
        "keep_merge": GraphRAGConfig.KEEP_MERGE,
        "keep_resolution": GraphRAGConfig.KEEP_RESOLUTION,
    }


def _needs_custom_run_graphrag_for_kb() -> bool:
    """Return True when the orchestration deviates from the official path."""
    if GraphRAGConfig.USE_INCREMENTAL_GRAPH:
        return True
    if GraphRAGConfig.USE_INCREMENTAL_MERGE:
        return True
    if GraphRAGConfig.USE_INCREMENTAL_RESOLUTION:
        return True
    if GraphRAGConfig.USE_ASYNC_COMMUNITY:
        return True
    if GraphRAGConfig.USE_ASYNC_KG_PHASES:
        return True
    if GraphRAGConfig.USE_CHAPTER_GRAPH:
        return True
    if not GraphRAGConfig.KEEP_SUBGRAPH:
        return True
    if not GraphRAGConfig.KEEP_MERGE:
        return True
    if not GraphRAGConfig.KEEP_RESOLUTION:
        return True
    if GraphRAGConfig.GRAPHRAG_MAX_PARALLEL_DOCS != 4:
        return True
    if GraphRAGConfig.KG_MAX_SAFE_RESUME_NODES != 5000:
        return True
    return False


def _wrap_run_graphrag_for_kb(*args, **kwargs):
    if _needs_custom_run_graphrag_for_kb():
        from rag.graphrag.general.index_extras import run_graphrag_for_kb as _custom

        return _custom(*args, **kwargs)
    return _ORIGINALS["run_graphrag_for_kb"](*args, **kwargs)


def _wrap_generate_subgraph(*args, **kwargs):
    if GraphRAGConfig.USE_CHAPTER_GRAPH or GraphRAGConfig.USE_INCREMENTAL_MERGE:
        from rag.graphrag.general.index_extras import generate_subgraph as _custom

        return _custom(*args, **kwargs)
    return _ORIGINALS["generate_subgraph"](*args, **kwargs)


def _wrap_merge_subgraph(*args, **kwargs):
    if GraphRAGConfig.USE_INCREMENTAL_MERGE:
        from rag.graphrag.general.index_extras import (
            merge_subgraph_incremental as _custom,
        )

        return _custom(*args, **kwargs)
    return _ORIGINALS["merge_subgraph"](*args, **kwargs)


def _wrap_resolve_entities(*args, **kwargs):
    if GraphRAGConfig.USE_INCREMENTAL_RESOLUTION:
        from rag.graphrag.general.index_extras import (
            resolve_entities_incremental as _custom,
        )

        return _custom(*args, **kwargs)
    # 官方 resolve_entities 不收 entity_types；自定义调用点
    # （index_extras / task_executor_extras）会带此关键字参数，
    # 回退官方实现前必须剔除，否则 TypeError。
    kwargs.pop("entity_types", None)
    return _ORIGINALS["resolve_entities"](*args, **kwargs)


def _wrap_extract_community(*args, **kwargs):
    # The official entry does not perform an early task-cancellation check.
    # Preserve the custom addition so that long community runs can be aborted
    # promptly without waiting for the extractor's first checkpoint.
    task_id = kwargs.get("task_id", "")
    if args and len(args) > 7:
        task_id = args[7]
    if task_id:
        from api.db.services.task_service import has_canceled
        from common.exceptions import TaskCanceledException

        if has_canceled(task_id):
            callback = kwargs.get("callback")
            if args and len(args) > 6:
                callback = args[6]
            if callback:
                callback(msg=f"Task {task_id} cancelled before community extraction.")
            raise TaskCanceledException(f"Task {task_id} was cancelled")

    if GraphRAGConfig.USE_ASYNC_COMMUNITY:
        from rag.graphrag.general.index_extras import (
            extract_community_async as _custom,
        )

        return _custom(*args, **kwargs)
    return _ORIGINALS["extract_community"](*args, **kwargs)
