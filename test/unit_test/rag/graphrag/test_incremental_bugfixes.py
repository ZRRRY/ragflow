#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
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
"""Regression tests for the 2026-07-16 branch-review bugfixes.

- Bug 1: ``resolve_entities`` wrapper must adapt the official call convention
  for both the incremental implementation and the official fallback
  (previously ``TypeError: got multiple values for argument 'task_id'``).
- Bug 2: ``patch_task_executor`` must locate the module when task_executor
  runs as ``__main__`` (production launches it as a script, so
  ``sys.modules["rag.svr.task_executor"]`` never exists).
- Bug 3: dangerous flag combinations (MERGE/RESOLUTION without GRAPH) must be
  normalized instead of letting ``set_graph`` overwrite the full graph with a
  single-document delta.
"""

import asyncio
import inspect
import logging
import sys
import types
from unittest.mock import MagicMock

import pytest

# task_executor_extras / index_extras pull heavy service modules that the
# shared conftest does not mock; stub them before importing those modules.
for _m in [
    "api.db.joint_services",
    "api.db.joint_services.tenant_model_service",
    "api.db.services.document_service",
    "api.db.services.knowledgebase_service",
    "api.db.services.llm_service",
    "common.misc_utils",
    "rag.svr.task_executor_limiter",
]:
    if _m not in sys.modules:
        sys.modules[_m] = MagicMock()

# config.py 尾部会 import siliconflow_timeout_patch，进而触发 rag.llm 包的
# 重量级动态导入（openai 等 SDK）。完整依赖环境下保留真实模块（不泄漏到
# rag/llm 的单元测试）；精简测试环境下回退为 mock。
try:
    import rag.llm.siliconflow_timeout_patch  # noqa: F401
except Exception:
    for _m in ("rag.llm", "rag.llm.embedding_model", "rag.llm.siliconflow_timeout_patch"):
        if _m not in sys.modules:
            sys.modules[_m] = MagicMock()

import rag.graphrag.general.index_patch as index_patch
from rag.graphrag.config import GraphRAGConfig

_OFFICIAL_CALL_ARGS = ("GRAPH", {"n1"}, "tenant1", "kb1", None, "CHAT", "EMBED", "CB")
_OFFICIAL_CALL_KWARGS = {"task_id": "task1", "entity_types": ["organization"]}


class TestResolveEntitiesCallConvention:
    """Bug 1: wrapper must be compatible with the official call shape."""

    def test_incremental_receives_official_shaped_args(self, monkeypatch):
        captured = {}

        async def fake_incremental(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs

        fake_module = types.ModuleType("rag.graphrag.general.index_extras")
        fake_module.resolve_entities_incremental = fake_incremental
        monkeypatch.setitem(sys.modules, "rag.graphrag.general.index_extras", fake_module)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_RESOLUTION", True)

        asyncio.run(index_patch._wrap_resolve_entities(*_OFFICIAL_CALL_ARGS, **_OFFICIAL_CALL_KWARGS))

        assert captured["args"] == _OFFICIAL_CALL_ARGS
        assert captured["kwargs"] == _OFFICIAL_CALL_KWARGS

    def test_official_fallback_strips_entity_types(self, monkeypatch):
        captured = {}

        async def fake_official(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs

        monkeypatch.setitem(index_patch._ORIGINALS, "resolve_entities", fake_official)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_RESOLUTION", False)

        asyncio.run(index_patch._wrap_resolve_entities(*_OFFICIAL_CALL_ARGS, **_OFFICIAL_CALL_KWARGS))

        assert captured["args"] == _OFFICIAL_CALL_ARGS
        # 官方实现不收 entity_types，wrapper 回退时必须剔除
        assert captured["kwargs"] == {"task_id": "task1"}

    def test_real_incremental_signature_matches_official_convention(self):
        """Exact reproduction of the original TypeError: bind the official call
        shape (8 positional + task_id=/entity_types=) against the real
        incremental implementation."""
        mod = pytest.importorskip("rag.graphrag.general.index_extras")
        sig = inspect.signature(mod.resolve_entities_incremental)

        bound = sig.bind(*_OFFICIAL_CALL_ARGS, **_OFFICIAL_CALL_KWARGS)
        assert bound.arguments["subgraph_nodes"] == {"n1"}
        assert bound.arguments["tenant_id"] == "tenant1"
        assert bound.arguments["entity_types"] == ["organization"]

        positional = [
            p.name
            for p in sig.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        assert positional == [
            "graph",
            "subgraph_nodes",
            "tenant_id",
            "kb_id",
            "doc_id",
            "llm_bdl",
            "embed_bdl",
            "callback",
            "task_id",
            "entity_types",
        ]


class TestTaskExecutorModuleLookup:
    """Bug 2: patch installation must work when task_executor runs as __main__."""

    @staticmethod
    def _fake_executable_module(name="__main__"):
        mod = types.ModuleType(name)
        mod.set_progress = lambda *a, **k: None

        async def _main():
            return None

        mod.main = _main
        return mod

    def test_named_module_preferred_over_main(self, monkeypatch):
        import rag.svr.task_executor_extras as extras

        named = self._fake_executable_module("rag.svr.task_executor")
        fake_main = self._fake_executable_module()
        monkeypatch.setitem(sys.modules, "rag.svr.task_executor", named)
        monkeypatch.setitem(sys.modules, "__main__", fake_main)
        monkeypatch.setattr(extras, "_te_module", None)

        assert extras._get_task_executor_module() is named

    def test_patch_installs_on_main_module(self, monkeypatch):
        """Production script mode: only __main__ exists."""
        import rag.svr.task_executor_extras as extras

        fake_main = self._fake_executable_module()
        monkeypatch.delitem(sys.modules, "rag.svr.task_executor", raising=False)
        monkeypatch.setitem(sys.modules, "__main__", fake_main)
        monkeypatch.setattr(extras, "_te_module", None)

        extras.patch_task_executor()

        assert getattr(fake_main, "_task_executor_patched", False) is True
        # set_progress / main 已被包装（不再是原始对象）
        assert fake_main.set_progress is not None
        assert hasattr(fake_main, "_original_set_progress")
        assert hasattr(fake_main, "_original_main")
        # 包装后函数名保持 set_progress
        assert fake_main.set_progress.__name__ == "set_progress"
        # 模块 import 场景下的 CONSUMER_NAME 兜底
        assert hasattr(fake_main, "CONSUMER_NAME")
        assert extras._te_module is fake_main

    def test_patch_warns_and_noops_when_module_missing(self, monkeypatch, caplog):
        import rag.svr.task_executor_extras as extras

        bare_main = types.ModuleType("__main__")  # 无 set_progress/main
        monkeypatch.delitem(sys.modules, "rag.svr.task_executor", raising=False)
        monkeypatch.setitem(sys.modules, "__main__", bare_main)
        monkeypatch.setattr(extras, "_te_module", None)

        with caplog.at_level(logging.WARNING):
            extras.patch_task_executor()

        assert not hasattr(bare_main, "_task_executor_patched")
        assert any("NOT installed" in r.message for r in caplog.records)

    def test_patch_is_idempotent(self, monkeypatch):
        import rag.svr.task_executor_extras as extras

        fake_main = self._fake_executable_module()
        monkeypatch.delitem(sys.modules, "rag.svr.task_executor", raising=False)
        monkeypatch.setitem(sys.modules, "__main__", fake_main)
        monkeypatch.setattr(extras, "_te_module", None)

        extras.patch_task_executor()
        wrapped_once = fake_main.set_progress
        extras.patch_task_executor()

        assert fake_main.set_progress is wrapped_once


class TestFlagCombinationNormalization:
    """Bug 3: MERGE/RESOLUTION without GRAPH must auto-upgrade GRAPH."""

    def test_merge_without_graph_is_upgraded(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", False)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", True)

        GraphRAGConfig.normalize_flag_combinations()

        assert GraphRAGConfig.USE_INCREMENTAL_GRAPH is True
        assert GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE is True

    def test_resolution_without_graph_is_upgraded(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", False)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", False)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_RESOLUTION", True)

        GraphRAGConfig.normalize_flag_combinations()

        assert GraphRAGConfig.USE_INCREMENTAL_GRAPH is True

    def test_graph_already_on_is_untouched(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", True)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", True)
        monkeypatch.setattr(GraphRAGConfig, "DELETE_SUBGRAPH_ON_DOC_DELETE", True)

        GraphRAGConfig.normalize_flag_combinations()

        assert GraphRAGConfig.USE_INCREMENTAL_GRAPH is True

    def test_all_flags_off_is_untouched(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_GRAPH", False)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", False)
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_RESOLUTION", False)
        monkeypatch.setattr(GraphRAGConfig, "DELETE_SUBGRAPH_ON_DOC_DELETE", False)

        GraphRAGConfig.normalize_flag_combinations()

        assert GraphRAGConfig.USE_INCREMENTAL_GRAPH is False
        assert GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE is False
