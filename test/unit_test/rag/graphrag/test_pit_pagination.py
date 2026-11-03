"""Exercise real pagination with multi-shard sort values and a fixed snapshot."""

import asyncio
from contextlib import aclosing
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from rag.graphrag import utils_pagination


class SnapshotSearch:
    def __init__(self):
        self.live = [
            {"id": "s0", "entity_kwd": "A", "kb_id": "kb", "knowledge_graph_kwd": "entity", "shard": 0, "doc": 0},
            {"id": "s1", "entity_kwd": "A", "kb_id": "kb", "knowledge_graph_kwd": "entity", "shard": 1, "doc": 0},
            {"id": "s2", "entity_kwd": "B", "kb_id": "kb", "knowledge_graph_kwd": "entity", "shard": 1, "doc": 1},
            {"id": "compiled", "entity_kwd": "A", "kb_id": "kb", "knowledge_graph_kwd": "entity", "compile_kwd": "artifact_page"},
            {"id": "other", "entity_kwd": "A", "kb_id": "other", "knowledge_graph_kwd": "entity"},
        ]
        self.pit_id = "pit-0"
        self.calls = 0
        self.renewals = 0
        self.requests = []
        self.close_point_in_time = MagicMock()

    def open_point_in_time(self, *, index, keep_alive):
        assert (index, keep_alive) == ("idx", "10m")
        self.snapshot = deepcopy(self.live)
        return {"id": self.pit_id}

    def search(self, *, body, allow_partial_search_results):
        assert allow_partial_search_results is False
        assert body["pit"] == {"id": self.pit_id, "keep_alive": "10m"}
        assert body["track_total_hits"] is False
        self.requests.append(deepcopy(body))
        if body["size"] == 0:
            assert body["query"] == {"match_none": {}}
            assert "search_after" not in body
            self.renewals += 1
            self.pit_id = f"pit-{self.calls + self.renewals}"
            return {"pit_id": self.pit_id, "hits": {"hits": []}}
        assert {"term": {"kb_id": "kb"}} in body["query"]["bool"]["filter"]
        assert {"terms": {"knowledge_graph_kwd": ["entity"]}} in body["query"]["bool"]["filter"]
        assert {"exists": {"field": "compile_kwd"}} in body["query"]["bool"]["must_not"]
        assert body["_source"] == ["entity_kwd"]
        assert body["sort"] in (
            [{"entity_kwd": {"order": "asc"}}, {"_shard_doc": {"order": "asc"}}],
            [{"_shard_doc": {"order": "asc"}}],
        )
        hits = []
        for row in self.snapshot:
            if row["kb_id"] != "kb" or "compile_kwd" in row:
                continue
            order = [row["entity_kwd"], (row["shard"] << 32) | row["doc"]]
            if len(body["sort"]) == 1:
                order = order[1:]
            if "search_after" not in body or order > body["search_after"]:
                hits.append({"_id": row["id"], "_source": {"entity_kwd": row["entity_kwd"]}, "sort": order})
        self.calls += 1
        self.pit_id = f"pit-{self.calls + self.renewals}"
        # Simulate indexing and a refresh while pages are consumed.
        self.live.clear()
        return {"pit_id": self.pit_id, "hits": {"hits": sorted(hits, key=lambda hit: hit["sort"])[:body["size"]]}}


@pytest.fixture
def pit(monkeypatch):
    raw = SnapshotSearch()
    monkeypatch.setattr(utils_pagination, "settings", SimpleNamespace(docStoreConn=SimpleNamespace(es=raw)))

    async def inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(utils_pagination, "thread_pool_exec", inline)
    return raw


def pages(**kwargs):
    return utils_pagination.search_all_by_search_after(
        {"knowledge_graph_kwd": ["entity"]}, "idx", "kb", ["entity_kwd"], "entity_kwd", page_size=1, **kwargs
    )


async def test_pit_reads_each_shard_once_despite_refresh(pit):
    rows = await utils_pagination.collect_all(
        {"knowledge_graph_kwd": ["entity"]}, "idx", "kb", ["entity_kwd"], "entity_kwd", page_size=1
    )
    assert [row["id"] for row in rows] == ["s0", "s1", "s2"]
    assert pit.calls == 4
    pit.close_point_in_time.assert_called_once_with(id="pit-4")


async def test_pit_empty_result_closes_latest_id(pit):
    pit.live.clear()
    assert [page async for page in pages()] == []
    pit.close_point_in_time.assert_called_once_with(id="pit-1")


@pytest.mark.parametrize("collect", [False, True])
async def test_pit_reads_more_than_a_thousand_pages(pit, collect):
    pit.live = [
        {"id": f"entity-{i}", "entity_kwd": "A", "kb_id": "kb",
         "knowledge_graph_kwd": "entity", "shard": i % 2, "doc": i}
        for i in range(1050)
    ]
    expected = {row["id"] for row in pit.live}
    if collect:
        rows = await utils_pagination.collect_all(
            {"knowledge_graph_kwd": ["entity"]}, "idx", "kb",
            ["entity_kwd"], "entity_kwd", page_size=1,
        )
    else:
        async with aclosing(pages()) as stream:
            rows = [row async for page in stream for row in page]
    assert len(rows) == len(expected)
    assert {row["id"] for row in rows} == expected
    assert pit.calls == 1051
    pit.close_point_in_time.assert_called_once_with(id="pit-1051")


@pytest.mark.parametrize("failure", ["search", "expired", "partial", "timeout", "cursor", "limit"])
async def test_pit_failures_propagate_and_close(pit, monkeypatch, failure):
    search = pit.search

    def fail(**kwargs):
        if failure in ("search", "expired"):
            raise RuntimeError(failure)
        result = search(**kwargs)
        if failure == "partial":
            result["_shards"] = {"failed": 1}
        elif failure == "timeout":
            result["timed_out"] = True
        elif failure == "cursor":
            result["hits"]["hits"][0].pop("sort")
        return result

    monkeypatch.setattr(pit, "search", fail)
    with pytest.raises(RuntimeError):
        async with aclosing(pages(max_pages=1)) as stream:
            async for _ in stream:
                pass
    pit.close_point_in_time.assert_called_once_with(id=pit.pit_id)


@pytest.mark.parametrize("partial", [False, True])
async def test_pit_open_failure_never_falls_back(pit, monkeypatch, partial):
    def fail(**kwargs):
        if partial:
            return {"id": "pit-0", "_shards": {"failed": 1}}
        raise RuntimeError("open failed")

    monkeypatch.setattr(pit, "open_point_in_time", fail)
    with pytest.raises(RuntimeError):
        await anext(pages())
    assert pit.calls == 0
    assert pit.close_point_in_time.call_count == int(partial)


@pytest.mark.parametrize("consumer_error", [False, True])
async def test_pit_early_consumer_exit_closes(pit, consumer_error):
    try:
        async with aclosing(pages()) as stream:
            async for _ in stream:
                if consumer_error:
                    raise ValueError("write failed")
                break
    except ValueError:
        assert consumer_error
    pit.close_point_in_time.assert_called_once_with(id="pit-1")


@pytest.mark.parametrize("during_open", [False, True])
async def test_pit_cancellation_closes(pit, monkeypatch, during_open):
    started, release = asyncio.Event(), asyncio.Event()
    target = pit.open_point_in_time if during_open else pit.search

    async def blocking(fn, *args, **kwargs):
        if fn == target:
            started.set()
            await release.wait()
        return fn(*args, **kwargs)

    monkeypatch.setattr(utils_pagination, "thread_pool_exec", blocking)
    task = asyncio.create_task(anext(pages()))
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    pit.close_point_in_time.assert_called_once_with(id="pit-0" if during_open else "pit-1")


class RenewalClock:
    """Advance consumer time without sleeping or replacing the event-loop clock."""

    def __init__(self):
        self.now = 0
        self.last_request = 0
        self.ticks = asyncio.Queue()
        self.completed = asyncio.Event()

    async def wait(self, stop):
        tick, stopped = asyncio.create_task(self.ticks.get()), asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait((tick, stopped), return_when=asyncio.FIRST_COMPLETED)
            if stopped in done:
                return False
            self.now += tick.result()
            return True
        finally:
            for task in (tick, stopped):
                task.cancel()
            await asyncio.gather(tick, stopped, return_exceptions=True)

    async def advance(self, seconds=60):
        self.completed.clear()
        self.ticks.put_nowait(seconds)
        await asyncio.wait_for(self.completed.wait(), timeout=2)


@pytest.fixture
def renewal_clock(pit, monkeypatch):
    clock = RenewalClock()
    search = pit.search

    def timed_search(**kwargs):
        # A request made after expiration must fail, as in Elasticsearch.
        assert clock.now - clock.last_request < 600
        result = search(**kwargs)
        clock.last_request = clock.now
        if kwargs["body"]["size"] == 0:
            clock.completed.set()
        return result

    monkeypatch.setattr(pit, "search", timed_search)
    monkeypatch.setattr(utils_pagination, "_wait_for_renewal", clock.wait)
    return clock


@pytest.mark.parametrize("delay", [204, 248, 1204])
async def test_slow_consumer_renews_without_advancing_cursor(pit, renewal_clock, delay):
    async with aclosing(pages()) as stream:
        rows = await anext(stream)
        for _ in range(delay // 60):
            await renewal_clock.advance()
        renewal_clock.now += delay % 60
        rows += [row async for page in stream for row in page]
    assert [row["id"] for row in rows] == ["s0", "s1", "s2"]
    assert pit.renewals == delay // 60
    assert [r.get("search_after") for r in pit.requests if r["size"]] == [
        None, ["A", 0], ["A", 1 << 32], ["B", (1 << 32) | 1],
    ]
    pit.close_point_in_time.assert_called_once_with(id=pit.pit_id)
    assert not [t for t in asyncio.all_tasks() if t.get_name() == "graphrag-pit-renewal"]


@pytest.mark.parametrize("failure", ["expired", "partial", "timeout"])
async def test_renewal_failure_prevents_more_pages(pit, renewal_clock, monkeypatch, failure):
    search = pit.search

    def fail(**kwargs):
        result = search(**kwargs)
        if kwargs["body"]["size"] == 0:
            if failure == "expired":
                raise RuntimeError("search context missing")
            result.update({"_shards": {"failed": 1}} if failure == "partial" else {"timed_out": True})
        return result

    monkeypatch.setattr(pit, "search", fail)
    async with aclosing(pages()) as stream:
        await anext(stream)
        await renewal_clock.advance()
        with pytest.raises(RuntimeError, match="renewal failed"):
            await anext(stream)
    assert pit.calls == 1
    pit.close_point_in_time.assert_called_once_with(id="pit-1" if failure == "expired" else "pit-2")


async def test_close_waits_for_inflight_renewal_and_preserves_cancellation(pit, renewal_clock, monkeypatch):
    started, release = asyncio.Event(), asyncio.Event()

    async def blocking(fn, *args, **kwargs):
        if fn == pit.search and kwargs["body"]["size"] == 0:
            started.set()
            await release.wait()
        return fn(*args, **kwargs)

    monkeypatch.setattr(utils_pagination, "thread_pool_exec", blocking)
    stream = pages()
    await anext(stream)
    renewal_clock.ticks.put_nowait(60)
    await started.wait()
    closing = asyncio.create_task(stream.aclose())
    await asyncio.sleep(0)
    closing.cancel()
    await asyncio.sleep(0)
    closing.cancel()
    pit.close_point_in_time.assert_not_called()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    pit.close_point_in_time.assert_called_once_with(id="pit-2")
    assert not [t for t in asyncio.all_tasks() if t.get_name() == "graphrag-pit-renewal"]


async def test_shard_doc_scan_preserves_snapshot(pit):
    rows = await utils_pagination.collect_all(
        {"knowledge_graph_kwd": ["entity"]}, "idx", "kb", ["entity_kwd"], "_shard_doc", page_size=1,
    )
    assert [row["id"] for row in rows] == ["s0", "s1", "s2"]
    pit.close_point_in_time.assert_called_once_with(id="pit-4")


async def test_close_error_does_not_mask_consumer_error(pit):
    pit.close_point_in_time.side_effect = RuntimeError("close failed")
    with pytest.raises(ValueError, match="write failed"):
        async with aclosing(pages()) as stream:
            await anext(stream)
            raise ValueError("write failed")


async def test_renewal_failure_during_final_page_cleanup_is_not_success(pit, renewal_clock, monkeypatch):
    pit.live = pit.live[:1]
    started, release = asyncio.Event(), asyncio.Event()

    async def blocking(fn, *args, **kwargs):
        if fn == pit.search and kwargs["body"]["size"] == 0:
            started.set()
            await release.wait()
            raise RuntimeError("renewal lost its context")
        return fn(*args, **kwargs)

    monkeypatch.setattr(utils_pagination, "thread_pool_exec", blocking)
    async with aclosing(utils_pagination.search_all_by_search_after(
        {"knowledge_graph_kwd": ["entity"]}, "idx", "kb", ["entity_kwd"], "entity_kwd", page_size=2,
    )) as stream:
        assert len(await anext(stream)) == 1
        renewal_clock.ticks.put_nowait(60)
        await started.wait()
        finishing = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        pit.close_point_in_time.assert_not_called()
        release.set()
        with pytest.raises(RuntimeError, match="renewal failed"):
            await finishing
    pit.close_point_in_time.assert_called_once_with(id="pit-1")


async def test_search_waits_for_renewal_and_uses_its_latest_id(pit, renewal_clock, monkeypatch):
    started, release = asyncio.Event(), asyncio.Event()

    async def blocking(fn, *args, **kwargs):
        if fn == pit.search and kwargs["body"]["size"] == 0:
            started.set()
            await release.wait()
        return fn(*args, **kwargs)

    monkeypatch.setattr(utils_pagination, "thread_pool_exec", blocking)
    async with aclosing(pages()) as stream:
        await anext(stream)
        renewal_clock.ticks.put_nowait(60)
        await started.wait()
        next_page = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        assert pit.calls == 1
        release.set()
        assert (await next_page)[0]["id"] == "s1"
        assert pit.requests[-1]["pit"]["id"] == "pit-2"
    pit.close_point_in_time.assert_called_once_with(id="pit-3")
