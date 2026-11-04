"""Deadline ownership and worker dispatch, without services or wall-clock waits."""

import ast
import asyncio
import importlib.util
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from common.exceptions import TaskCanceledException

ROOT = Path(__file__).resolve().parents[4]
_spec = importlib.util.spec_from_file_location("tested_connection_utils", ROOT / "common/connection_utils.py")
_connection_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_connection_utils)
timeout = _connection_utils.timeout

# Import just the actual retry function; the worker's service imports are not
# needed for deadline ownership and must not depend on test collection order.
_tree = ast.parse((ROOT / "rag/graphrag/general/index.py").read_bytes())
_retry = next(n for n in _tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "_run_with_retry")
_namespace = {"asyncio": asyncio, "logging": logging, "TaskCanceledException": TaskCanceledException,
              "_has_cancel_and_exit": lambda *args: None}
exec(compile(ast.Module(body=[_retry], type_ignores=[]), str(ROOT / "rag/graphrag/general/index.py"), "exec"), _namespace)  # noqa: S102
run_with_retry = _namespace["_run_with_retry"]


@pytest.mark.parametrize("enabled", [False, True])
async def test_inner_timeout_keeps_identity_and_never_retries(monkeypatch, enabled):
    if enabled:
        monkeypatch.setenv("ENABLE_TIMEOUT_ASSERTION", "1")
    else:
        monkeypatch.delenv("ENABLE_TIMEOUT_ASSERTION", raising=False)
    error = TimeoutError("ES request failed")
    operation = AsyncMock(side_effect=error)
    on_timeout = MagicMock()
    with pytest.raises(TimeoutError) as caught:
        await timeout(10800, 3, on_timeout=on_timeout)(operation)()
    assert caught.value is error
    operation.assert_awaited_once()
    on_timeout.assert_not_called()


async def test_owned_deadline_still_retries_and_reports_its_limit(monkeypatch):
    monkeypatch.setenv("ENABLE_TIMEOUT_ASSERTION", "1")
    async def work():
        await asyncio.sleep(0)
    operation = AsyncMock(side_effect=work)
    with pytest.raises(TimeoutError, match="after 0 seconds and 2 attempts"):
        await timeout(0, 2)(operation)()
    assert operation.await_count == 2


async def test_external_cancel_is_not_retried(monkeypatch):
    monkeypatch.setenv("ENABLE_TIMEOUT_ASSERTION", "1")
    operation = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await timeout(10800, 3)(operation)()
    operation.assert_awaited_once()


@pytest.mark.parametrize("seconds", [0, 18000])
async def test_stage_does_not_relabel_or_retry_inner_timeout(seconds):
    error = TimeoutError("transport request failed")
    work = AsyncMock(side_effect=error)
    callback = MagicMock()
    with pytest.raises(TimeoutError) as caught:
        await run_with_retry("rank", work, attempts=3, timeout_seconds=seconds,
                                    backoff_seconds=0, backoff_max_seconds=0, callback=callback)
    assert caught.value is error
    work.assert_awaited_once()
    assert "inner operation timed out" in callback.call_args.kwargs["msg"]
    assert "timeout after" not in callback.call_args.kwargs["msg"]


@pytest.mark.parametrize("executor", ["main", "refactor"])
@pytest.mark.parametrize("task_type", ["graphrag", "raptor"])
async def test_worker_total_deadline_only_applies_to_other_tasks(monkeypatch, executor, task_type):
    monkeypatch.setenv("ENABLE_TIMEOUT_ASSERTION", "1")
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = 0
    calls = []

    async def work(*args):
        nonlocal offset
        calls.append(task_type)
        offset += 20000
        # Advance the real asyncio deadline scheduler using a controlled clock.
        for _ in range(3):
            await asyncio.sleep(0)
        return "done"

    path = ROOT / ("rag/svr/task_executor.py" if executor == "main" else "rag/svr/task_executor_refactor/task_handler.py")
    name = "do_handle_task" if executor == "main" else "handle"
    tree = ast.parse(await asyncio.to_thread(path.read_bytes))
    function = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    namespace = {"timeout": timeout, "_do_handle_task": work}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)  # noqa: S102
    argument = {"task_type": task_type} if executor == "main" else SimpleNamespace(
        _task_context=SimpleNamespace(task_type=task_type), _handle=work,
    )
    with monkeypatch.context() as clock:
        clock.setattr(loop, "time", lambda: real_time() + offset)
        if task_type == "graphrag":
            assert await namespace[name](argument) == "done"
        else:
            with pytest.raises(TimeoutError, match="10800"):
                await namespace[name](argument)
    assert calls == [task_type]
