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

"""Unit tests for the Phase 2.5 global PageRank recalc path.

Covers ``recalc_global_pagerank`` (rag/graphrag/general/index_extras.py):
  - partial-update write-back (rank_flt + content_with_weight.pagerank)
  - structural (book/chapter) nodes pinned to 0.0
  - topology truncation (entity OR relation scroll over hits_cap) aborts
    before any write
  - entities missing from the pagerank map are skipped, never zeroed
  - _final_graph_is_delta guards official phases from per-doc delta graphs

The entity write-back scan itself streams via ``search_after`` and is no
longer bounded by ``hits_cap``; the topology load enforces the cap (covered
by test_graphrag_topology.py).
"""

import asyncio
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import networkx as nx
import pytest

# Stub heavy third-party / service modules before importing index_extras
# (mirrors test_incremental_bugfixes.py).
for _m in [
    "editdistance",
    "spacy",
    "api.db.joint_services",
    "api.db.joint_services.tenant_model_service",
    "api.db.services.document_service",
    "common.misc_utils",
]:
    if _m not in sys.modules:
        sys.modules[_m] = MagicMock()

try:
    import rag.llm.siliconflow_timeout_patch  # noqa: F401
except Exception:
    for _m in ("rag.llm", "rag.llm.embedding_model", "rag.llm.siliconflow_timeout_patch"):
        if _m not in sys.modules:
            sys.modules[_m] = MagicMock()

from common.exceptions import TaskCanceledException
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.general import index_extras
from rag.graphrag.utils_extras import GraphTopologyTruncatedError


async def _run_sync(func, *args, **kwargs):
    """Drop-in for the real thread_pool_exec: run inline in the test loop."""
    return func(*args, **kwargs)


class _FakeDocStore:
    """Minimal doc-store double: capability attrs + partial update + refresh."""

    def __init__(self):
        self.es = MagicMock()  # capability marker for search_after support
        self.update_calls = []
        self.refreshed = False

    def search_with_scroll(self, index_name, query_body, fields, hits_cap=None):
        # Capability marker only; the recalc path no longer scans with scroll.
        raise AssertionError("search_with_scroll should not be called by recalc")

    def update_docs(self, index_name, updates):
        self.update_calls.append(list(updates))
        return []

    def refresh_idx(self, index_name):
        self.refreshed = True


def _entity_hit(cid, name, entity_type, pagerank=0.001, description="d"):
    return {
        "_id": cid,
        "_source": {
            "entity_kwd": name,
            "content_with_weight": json.dumps(
                {"entity_type": entity_type, "pagerank": pagerank, "description": description},
                ensure_ascii=False,
            ),
        },
    }


def _graph():
    """Alpha-Beta-Gamma chain; the structural node is filtered at load time."""
    g = nx.Graph()
    g.add_node("Alpha")
    g.add_node("Beta")
    g.add_node("Gamma")
    g.add_edge("Alpha", "Beta")
    g.add_edge("Beta", "Gamma")
    return g


def _hits():
    return [
        _entity_hit("c1", "Alpha", "organization"),
        _entity_hit("c2", "Beta", "organization"),
        _entity_hit("c3", "Gamma", "person"),
        _entity_hit("c4", "Book X", "书籍"),
    ]


def _setup(monkeypatch, hits, graph, topology_error=None):
    """Wire the fake doc store, topology loader and search_after into index_extras."""
    msgs = []
    store = _FakeDocStore()
    monkeypatch.setattr(index_extras, "settings", SimpleNamespace(docStoreConn=store))
    monkeypatch.setattr(index_extras, "thread_pool_exec", _run_sync)

    async def _fake_topology(tenant_id, kb_id, skip_entity_type=None, hits_cap=None):
        if topology_error is not None:
            raise topology_error
        return graph

    monkeypatch.setattr(index_extras, "get_graph_topology_from_index", _fake_topology)

    sa_calls = []

    def _fake_search_after(filters, index_name, kb_id, fields, sort_field, page_size=1000, max_pages=1000):
        sa_calls.append({"filters": filters, "fields": fields, "sort_field": sort_field})
        page = [dict(h["_source"], id=h["_id"]) for h in hits]

        async def _gen():
            yield page

        return _gen()

    monkeypatch.setattr(index_extras, "search_all_by_search_after", _fake_search_after)

    def _callback(msg=None, **kwargs):
        if msg:
            msgs.append(msg)

    return store, sa_calls, msgs, _callback


def _collected_updates(store):
    return {cid: fields for call in store.update_calls for cid, fields in call}


class TestRecalcGlobalPagerank:
    @pytest.mark.asyncio
    async def test_cancellation_checks_scale_with_pages_not_entities(self, monkeypatch):
        store, _, _, callback = _setup(monkeypatch, _hits(), _graph())
        check = MagicMock()
        monkeypatch.setattr(index_extras, "_has_cancel_and_exit", check)
        hit = _hits()[0]

        async def scan(*args, **kwargs):
            for offset, count in [(0, 1000), (1000, 1000), (2000, 1)]:
                yield [dict(hit["_source"], id=f"c{i}") for i in range(offset, offset + count)]

        monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="task")

        assert [len(batch) for batch in store.update_calls] == [1000, 1000, 1]
        assert len(_collected_updates(store)) == 2001
        # One initial check, then one per page and one per write, including the tail.
        assert check.call_count == 7
        assert store.refreshed is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("page_size", [1, 1000])
    async def test_cancellation_during_page_processing_prevents_write(self, monkeypatch, page_size):
        store, _, _, callback = _setup(monkeypatch, _hits(), _graph())
        cancelled = False
        closed = []
        hit = _hits()[0]

        def check(*args, **kwargs):
            if cancelled:
                raise TaskCanceledException("cancelled before write")

        def classify(entity_type):
            nonlocal cancelled
            cancelled = True
            return False

        async def scan(*args, **kwargs):
            try:
                yield [dict(hit["_source"], id=f"c{i}") for i in range(page_size)]
            finally:
                closed.append(True)

        monkeypatch.setattr(index_extras, "_has_cancel_and_exit", check)
        monkeypatch.setattr(GraphRAGConfig, "should_skip_embedding", classify)
        monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
        with pytest.raises(TaskCanceledException):
            await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="task")
        assert store.update_calls == []
        assert store.refreshed is False
        assert closed == [True]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cancelled", [False, True])
    async def test_closes_scan_when_page_processing_fails(self, monkeypatch, cancelled):
        store, _, _, callback = _setup(monkeypatch, _hits(), _graph())
        closed = []
        hit = _hits()[0]

        async def scan(*args, **kwargs):
            try:
                # Fill the write window while the generator is still suspended.
                yield [dict(hit["_source"], id=f"c{i}") for i in range(1000)]
            finally:
                closed.append(True)

        error = asyncio.CancelledError if cancelled else RuntimeError

        def fail_write(*args, **kwargs):
            raise error("write interrupted")

        monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
        monkeypatch.setattr(store, "update_docs", fail_write)
        with pytest.raises(error, match="write interrupted"):
            await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")
        assert closed == [True]
        assert store.refreshed is False

    @pytest.mark.asyncio
    async def test_writes_partial_updates_with_global_pagerank(self, monkeypatch):
        store, sa_calls, msgs, callback = _setup(monkeypatch, _hits(), _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        updates = _collected_updates(store)
        assert set(updates) == {"c1", "c2", "c3", "c4"}

        expected = nx.pagerank(_graph())

        for cid, name in [("c1", "Alpha"), ("c2", "Beta"), ("c3", "Gamma")]:
            assert updates[cid]["rank_flt"] == pytest.approx(expected[name])
            meta = json.loads(updates[cid]["content_with_weight"])
            assert meta["pagerank"] == pytest.approx(expected[name])
            # untouched meta fields survive the partial update
            assert meta["description"] == "d"
            assert meta["entity_type"] in ("organization", "person")

        # Structural node is pinned to 0.0, not left at the 0.001 placeholder.
        assert updates["c4"]["rank_flt"] == 0.0
        assert json.loads(updates["c4"]["content_with_weight"])["pagerank"] == 0.0

        # The scan must use the narrow field projection (no vector fields).
        assert sa_calls[0]["fields"] == ["entity_kwd", "content_with_weight"]
        assert sa_calls[0]["sort_field"] == "_shard_doc"
        assert store.refreshed is True
        assert any("updated 4 entities, skipped 0" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_aborts_when_topology_is_truncated(self, monkeypatch):
        # Entity or relation scroll cut off by hits_cap → no scan, no writes.
        store, sa_calls, msgs, callback = _setup(
            monkeypatch,
            _hits(),
            None,
            topology_error=GraphTopologyTruncatedError("relation scroll hit the hits_cap=50000"),
        )

        with pytest.raises(GraphTopologyTruncatedError):
            await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        assert sa_calls == []
        assert store.update_calls == []
        assert any("abort pagerank recalc" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_entity_missing_from_pagerank_is_skipped_not_zeroed(self, monkeypatch):
        hits = _hits() + [_entity_hit("c5", "Ghost", "organization")]
        store, sa_calls, msgs, callback = _setup(monkeypatch, hits, _graph())

        await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        updates = _collected_updates(store)
        assert "c5" not in updates
        assert set(updates) == {"c1", "c2", "c3", "c4"}
        assert any("skipped 1" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_fails_when_backend_lacks_update_docs(self, monkeypatch):
        store, sa_calls, msgs, callback = _setup(monkeypatch, _hits(), _graph())
        monkeypatch.delattr(_FakeDocStore, "update_docs")

        with pytest.raises(RuntimeError, match="update_docs"):
            await index_extras.recalc_global_pagerank("t1", "kb1", callback, task_id="")

        assert sa_calls == []
        assert any("update_docs" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_batches_stay_bounded_across_sparse_pages(self, monkeypatch):
        store, _, _, callback = _setup(monkeypatch, _hits(), _graph())
        valid = _hits()[0]["_source"]

        async def scan(*args, **kwargs):
            yield [dict(valid, id=f"a{i}") for i in range(999)] + [{"id": "invalid"}]
            yield [dict(valid, id=f"b{i}") for i in range(2002)]

        monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
        await index_extras.recalc_global_pagerank("t1", "kb1", callback)
        assert [len(batch) for batch in store.update_calls] == [1000, 1000, 1000, 1]
        assert len(_collected_updates(store)) == 3001

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure", ["load", "write", "refresh", "refresh_false", "scan"])
    async def test_failure_is_propagated_without_success(self, monkeypatch, failure):
        store, _, msgs, callback = _setup(
            monkeypatch, _hits(), _graph(), topology_error=RuntimeError("load failed") if failure == "load" else None,
        )
        if failure == "write":
            monkeypatch.setattr(store, "update_docs", lambda *args: ["write failed"])
        elif failure == "refresh":
            monkeypatch.setattr(store, "refresh_idx", MagicMock(side_effect=RuntimeError("refresh failed")))
        elif failure == "refresh_false":
            monkeypatch.setattr(store, "refresh_idx", lambda *args: False)
        elif failure == "scan":
            async def scan(*args, **kwargs):
                raise RuntimeError("scan failed")
                yield
            monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
        with pytest.raises(Exception, match=f"{failure.split('_')[0]} failed"):
            await index_extras.recalc_global_pagerank("t1", "kb1", callback)
        assert any("graph preserved" in m and "partially updated" in m for m in msgs)
        assert not any("recalc done" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_empty_graph_is_a_successful_noop(self, monkeypatch):
        store, calls, msgs, callback = _setup(monkeypatch, [], nx.Graph())
        await index_extras.recalc_global_pagerank("t1", "kb1", callback)
        assert calls == [] and store.update_calls == []
        assert any("no global graph" in m for m in msgs)

    @pytest.mark.asyncio
    async def test_write_progress_is_throttled_but_completion_is_immediate(self, monkeypatch):
        store, _, msgs, callback = _setup(monkeypatch, _hits(), _graph())
        now = 0
        monkeypatch.setattr(index_extras, "asyncio", SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: now), CancelledError=asyncio.CancelledError,
            timeout=asyncio.timeout,
        ))
        valid = _hits()[0]["_source"]

        async def scan(*args, **kwargs):
            nonlocal now
            for offset in range(4):
                now += 20
                yield [dict(valid, id=f"{offset}-{i}") for i in range(1000)]

        monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
        await index_extras.recalc_global_pagerank("t1", "kb1", callback)
        progress = [m for m in msgs if "pagerank write-back:" in m]
        assert len(progress) == 2
        assert "scanned=2000 updated=2000 skipped=0" in progress[0]
        assert "scanned=4000 updated=4000 skipped=0" in progress[1]
        assert "global pagerank recalc done" in msgs[-1]
        assert store.refreshed


class TestFinalGraphIsDelta:
    def test_incremental_merge_marks_graph_as_delta(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", True)
        assert index_extras._final_graph_is_delta(nx.Graph()) is True
        assert index_extras._final_graph_is_delta(None) is False

    def test_official_mode_never_delta(self, monkeypatch):
        monkeypatch.setattr(GraphRAGConfig, "USE_INCREMENTAL_MERGE", False)
        assert index_extras._final_graph_is_delta(nx.Graph()) is False


@pytest.mark.parametrize("operation", ["write", "refresh"])
@pytest.mark.parametrize("failure", [False, True])
async def test_cancel_drains_write_or_refresh(monkeypatch, caplog, operation, failure):
    store, _, msgs, callback = _setup(monkeypatch, _hits(), _graph())
    started, finish = asyncio.Event(), asyncio.Event()
    settled = []

    async def dispatch(fn, *args, **kwargs):
        target = store.update_docs if operation == "write" else store.refresh_idx
        if fn == target:
            started.set()
            await finish.wait()
            settled.append(operation)
            if failure:
                raise TimeoutError("ES request timed out")
        return fn(*args, **kwargs)

    monkeypatch.setattr(index_extras, "thread_pool_exec", dispatch)
    task = asyncio.create_task(index_extras.recalc_global_pagerank("t1", "kb1", callback))
    await started.wait()
    for _ in range(2):
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert settled == []
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert settled == [operation]
    assert not any("recalc done" in msg for msg in msgs)
    if operation == "write":
        assert store.refreshed is False
        assert len(store.update_calls) == (0 if failure else 1)
        if failure:
            assert "result unconfirmed" in caplog.text


async def test_default_pagerank_runs_beyond_five_hours(monkeypatch):
    store, _, msgs, callback = _setup(monkeypatch, _hits(), _graph())
    monkeypatch.setattr(GraphRAGConfig, "PAGERANK_TIMEOUT_SECONDS", 0)
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = 0
    valid = _hits()[0]["_source"]

    async def scan(*args, **kwargs):
        nonlocal offset
        for i in range(4):
            offset += 5400
            await asyncio.sleep(0)
            yield [dict(valid, id=f"{i}-{j}") for j in range(1000)]

    monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
    with monkeypatch.context() as clock:
        clock.setattr(loop, "time", lambda: real_time() + offset)
        await index_extras.recalc_global_pagerank("t1", "kb1", callback)
    assert sum(map(len, store.update_calls)) == 4000
    assert store.refreshed
    assert "recalc done" in msgs[-1]


@pytest.mark.parametrize("inner_timeout", [False, True])
async def test_pagerank_deadline_distinguishes_request_timeout(monkeypatch, inner_timeout):
    from unittest.mock import AsyncMock

    messages = []
    request_error = TimeoutError("ES request timed out")
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = 0

    async def run(*args):
        nonlocal offset
        if inner_timeout:
            raise request_error
        offset = 200
        for _ in range(3):
            await asyncio.sleep(0)

    worker = AsyncMock(side_effect=run)
    monkeypatch.setattr(index_extras, "_recalc_global_pagerank", worker)
    monkeypatch.setattr(GraphRAGConfig, "PAGERANK_TIMEOUT_SECONDS", 100)
    with monkeypatch.context() as clock:
        clock.setattr(loop, "time", lambda: real_time() + offset)
        with pytest.raises(TimeoutError) as caught:
            await index_extras.recalc_global_pagerank("t1", "kb1", lambda **kw: messages.append(kw["msg"]))
    worker.assert_awaited_once()
    assert "graph preserved" in messages[-1]
    if inner_timeout:
        assert caught.value is request_error
        assert "configured limit" not in messages[-1]
    else:
        assert "configured limit" in str(caught.value)


@pytest.mark.parametrize("operation", ["write", "refresh"])
async def test_phase_timeout_drains_inflight_request_before_failure(monkeypatch, operation):
    store, _, messages, callback = _setup(monkeypatch, _hits(), _graph())
    monkeypatch.setattr(GraphRAGConfig, "PAGERANK_TIMEOUT_SECONDS", 18000)
    started, finish = asyncio.Event(), asyncio.Event()
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = 0
    async def dispatch(fn, *args, **kwargs):
        if fn == (store.update_docs if operation == "write" else store.refresh_idx):
            started.set()
            await finish.wait()
        return fn(*args, **kwargs)
    monkeypatch.setattr(index_extras, "thread_pool_exec", dispatch)
    with monkeypatch.context() as clock:
        clock.setattr(loop, "time", lambda: real_time() + offset)
        task = asyncio.create_task(index_extras.recalc_global_pagerank("t1", "kb1", callback))
        await started.wait()
        offset = 18001
        for _ in range(4):
            await asyncio.sleep(0)
        assert not task.done()
        assert not any("PageRank failed" in msg for msg in messages)
        finish.set()
        with pytest.raises(TimeoutError, match="configured limit of 18000s"):
            await task
    assert len(store.update_calls) == 1
    assert store.refreshed is (operation == "refresh")
    assert "graph preserved" in messages[-1]
    assert not any("recalc done" in msg for msg in messages)


async def test_rank_only_rerun_rewrites_partial_results_without_old_cursor(monkeypatch):
    from copy import deepcopy

    store, _, _, callback = _setup(monkeypatch, _hits(), _graph())
    row = _hits()[0]["_source"]
    rows = {str(i): dict(deepcopy(row), q_2_vec=[0.2, 0.8]) for i in range(1001)}
    scans = []
    fail = True

    async def scan(*args, **kwargs):
        scans.append(True)
        snapshot = deepcopy(rows)
        yield [dict(fields, id=cid) for cid, fields in snapshot.items()]

    def update(index_name, updates):
        if fail and len(updates) == 1:
            raise TimeoutError("second batch failed")
        for cid, fields in updates:
            rows[cid].update(fields)
        store.update_calls.append(updates)
        return []

    monkeypatch.setattr(index_extras, "search_all_by_search_after", scan)
    monkeypatch.setattr(store, "update_docs", update)
    with pytest.raises(TimeoutError, match="second batch failed"):
        await index_extras.recalc_global_pagerank("t1", "kb1", callback)
    assert len(store.update_calls) == 1
    assert not store.refreshed
    fail = False
    await index_extras.recalc_global_pagerank("t1", "kb1", callback)
    expected = nx.pagerank(_graph())["Alpha"]
    assert len(scans) == 2
    assert [len(batch) for batch in store.update_calls] == [1000, 1000, 1]
    assert all(row["rank_flt"] == expected and json.loads(row["content_with_weight"])["pagerank"] == expected
               and row["q_2_vec"] == [0.2, 0.8] for row in rows.values())
    assert store.refreshed
