"""Exercise real PageRank deadline ownership through all orchestration paths."""

import asyncio

import pytest

from rag.graphrag.config import GraphRAGConfig
from test.unit_test.rag.graphrag.test_review_findings import (
    index_extras,
    make_graph,
    prepare_consumer,
    run_kb,
    runtime,  # noqa: F401 - shared service-isolation fixture
    seed_checkpoint,
    seed_graph,
    task_executor_extras,
)


@pytest.mark.parametrize("stage", ["merge", "resolution", "async"])
@pytest.mark.parametrize("limited", [False, True])
async def test_pagerank_budget_and_terminal_state(runtime, monkeypatch, stage, limited):  # noqa: F811 - pytest fixture injection
    graph = make_graph("doc1", "Alice", "Bob")
    seed_graph(runtime.store, graph)
    seed_checkpoint(runtime.store, "doc1", graph, merged=True)
    monkeypatch.setattr(GraphRAGConfig, "RECALC_GLOBAL_PAGERANK_AFTER_MERGE", True)
    monkeypatch.setattr(GraphRAGConfig, "PAGERANK_TIMEOUT_SECONDS", 1 if limited else 0)
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = 0
    calls = 0

    async def work(*args):
        nonlocal offset, calls
        calls += 1
        if stage == "resolution" and calls == 1:
            return
        offset += 20000
        for _ in range(3):
            await asyncio.sleep(0)

    monkeypatch.setattr(index_extras, "_recalc_global_pagerank", work)
    consumer = prepare_consumer(runtime, monkeypatch) if stage == "async" else None
    with monkeypatch.context() as clock:
        clock.setattr(loop, "time", lambda: real_time() + offset)
        if consumer:
            await task_executor_extras.kg_postprocess_consumer()
        elif limited:
            with pytest.raises(TimeoutError, match="PageRank phase exceeded configured limit"):
                await run_kb(runtime, ["doc1"], merge_retry_attempts=3, merge_timeout_seconds=60)
        else:
            await run_kb(runtime, ["doc1"], merge_timeout_seconds=60)
    assert calls == (1 if stage == "async" or (limited and stage == "merge") else 2)
    assert (index_extras.PHASE_RESOLUTION in runtime.phases) is not limited
    if consumer:
        terminal = [c.kwargs["prog"] for c in consumer.worker.set_progress.call_args_list
                    if c.kwargs.get("prog") is not None]
        assert terminal == ([-1.0] if limited else [1.0])
        consumer.message.ack.assert_called_once()
