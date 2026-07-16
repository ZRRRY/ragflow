#
#  Copyright 2024 The InfiniFlow Authors. All Rights Reserved.
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
"""Custom extensions for rag.svr.task_executor.

This module isolates GraphRAG incremental/optimization patches that were
previously embedded directly in ``task_executor.py``.  It is imported at the
end of ``task_executor.py`` and monkey-patches the functions it needs to
extend (``set_progress`` and ``main``).  When all custom flags are disabled
the patches still install but are no-ops, so default deployments behave like
upstream v0.26.1.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Optional

from common import settings
from common.constants import LLMType, SVR_CONSUMER_GROUP_NAME
from common.misc_utils import thread_pool_exec
from api.db.joint_services.tenant_model_service import (
    get_model_config_from_provider_instance,
    get_tenant_default_model_by_type,
)
from api.db.services.knowledgebase_service import KnowledgebaseService
from api.db.services.llm_service import LLMBundle
from api.db.services.task_service import TaskService, has_canceled
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.phase_markers import (
    PHASE_COMMUNITY,
    PHASE_RESOLUTION,
    clear_phase_markers,
    has_phase_marker,
    set_phase_marker,
)
from rag.nlp import search
from rag.svr.task_executor_limiter import kg_limiter
from rag.utils.redis_conn import REDIS_CONN, RedisDistributedLock


# -----------------------------------------------------------------------------
# Heartbeat lock v2: thread-safe current_task_id used by _heartbeat_loop.
# -----------------------------------------------------------------------------
_current_task_id_lock = threading.Lock()
_current_task_id_state: dict[str, Optional[str]] = {"tid": None}


def _get_current_task_id() -> Optional[str]:
    with _current_task_id_lock:
        return _current_task_id_state["tid"]


def _set_current_task_id(tid: Optional[str]) -> Optional[str]:
    with _current_task_id_lock:
        old = _current_task_id_state["tid"]
        _current_task_id_state["tid"] = tid
        return old


# -----------------------------------------------------------------------------
# Reconciliation of stuck GraphRAG tasks on worker boot.
# -----------------------------------------------------------------------------
async def reconcile_stuck_graphrag_tasks():
    """Boot-time scan: mark stuck graphrag tasks as done when OpenSearch has data.

    Only runs when ``RECONCILE_STUCK_ON_BOOT=1``. Uses a heartbeat TTL + grace
    window to avoid racing with a genuinely running worker.
    """
    if not GraphRAGConfig.RECONCILE_STUCK_ON_BOOT:
        logging.info("[reconcile] RECONCILE_STUCK_ON_BOOT=0, skipping")
        return

    grace_ms = GraphRAGConfig.STUCK_TASK_GRACE_MINUTES * 60 * 1000
    cutoff_ms = int(time.time() * 1000) - grace_ms
    min_nodes = GraphRAGConfig.STUCK_TASK_MIN_NODES
    min_edges = GraphRAGConfig.STUCK_TASK_MIN_EDGES
    logging.info(
        "[reconcile] starting: grace=%dmin min_nodes=%d min_edges=%d",
        GraphRAGConfig.STUCK_TASK_GRACE_MINUTES, min_nodes, min_edges,
    )

    try:
        kbs = list(
            KnowledgebaseService.model.select(
                KnowledgebaseService.model.id,
                KnowledgebaseService.model.tenant_id,
                KnowledgebaseService.model.graphrag_task_id,
            ).where(
                (KnowledgebaseService.model.graphrag_task_id.is_null(False))
                & (KnowledgebaseService.model.graphrag_task_finish_at.is_null())
            ).dicts()
        )
    except Exception:
        logging.exception("[reconcile] candidate KB list query failed")
        return
    logging.info("[reconcile] candidate %d KBs", len(kbs))

    finalized = skipped = failed = 0
    for kb in kbs:
        kb_id = kb["id"]
        tenant_id = kb["tenant_id"]
        task_id = kb["graphrag_task_id"]
        try:
            claim = f"graphrag:reconcile:{task_id}"
            if not REDIS_CONN.REDIS.set(claim, "1", ex=600, nx=True):
                logging.info("[reconcile] kb=%s task=%s already claimed, skip", kb_id, task_id)
                continue

            ok, task_obj = TaskService.get_by_id(task_id)
            if not ok or task_obj is None:
                logging.warning("[reconcile] kb=%s task=%s does not exist, skip", kb_id, task_id)
                REDIS_CONN.delete(claim)
                continue
            prog = task_obj.progress or 0
            update_time = task_obj.update_time or 0
            if not (0 < prog < 1):
                logging.info("[reconcile] kb=%s task=%s progress=%.4f not in (0,1), skip", kb_id, task_id, prog)
                REDIS_CONN.delete(claim)
                continue

            heartbeat_key = f"graphrag:hb:{task_id}"
            try:
                if REDIS_CONN.REDIS is None:
                    ttl = -2
                else:
                    ttl = REDIS_CONN.REDIS.ttl(heartbeat_key)
                    if ttl is None:
                        ttl = -2
            except Exception:
                logging.exception("[reconcile] TTL query failed task=%s, falling back to grace check", task_id)
                ttl = -2

            if ttl > 0:
                logging.info(
                    "[reconcile] kb=%s task=%s heartbeat still alive (ttl=%ds), skip",
                    kb_id, task_id, ttl,
                )
                REDIS_CONN.delete(claim)
                skipped += 1
                continue

            if update_time > cutoff_ms:
                logging.info(
                    "[reconcile] kb=%s task=%s heartbeat missing but update_time within grace, skip",
                    kb_id, task_id,
                )
                REDIS_CONN.delete(claim)
                continue

            index = search.index_name(tenant_id)
            try:
                n_nodes = await thread_pool_exec(
                    settings.docStoreConn.count, {"knowledge_graph_kwd": ["entity"]}, index, [kb_id]
                )
                n_nodes = int(n_nodes or 0)
            except Exception:
                logging.exception("[reconcile] entity count failed kb=%s", kb_id)
                n_nodes = 0
            try:
                n_edges = await thread_pool_exec(
                    settings.docStoreConn.count, {"knowledge_graph_kwd": ["relation"]}, index, [kb_id]
                )
                n_edges = int(n_edges or 0)
            except Exception:
                logging.exception("[reconcile] relation count failed kb=%s", kb_id)
                n_edges = 0

            if n_nodes < min_nodes or n_edges < min_edges:
                logging.warning(
                    "[reconcile] kb=%s task=%s only %d nodes / %d edges below threshold, skip",
                    kb_id, task_id, n_nodes, n_edges,
                )
                REDIS_CONN.delete(claim)
                skipped += 1
                continue

            msg = f"Knowledge Graph reconciled ({n_nodes} nodes, {n_edges} edges) [boot]"
            TaskService.update_progress(task_id, {"progress": 1.0, "progress_msg": msg})
            try:
                REDIS_CONN.delete(heartbeat_key)
            except Exception:
                logging.exception("[reconcile] heartbeat delete failed task=%s", task_id)
            # Setting graphrag_task_id back to None ensures the next
            # reconcile pass can pick this KB up via the is_null() filter.
            # An empty string would be matched by `=` but NOT by `is_null()`,
            # so the KB would silently become invisible to future scans.
            KnowledgebaseService.update_by_id(
                kb_id, {"graphrag_task_finish_at": datetime.now(), "graphrag_task_id": None}
            )
            try:
                clear_phase_markers(kb_id)
            except Exception:
                logging.exception("[reconcile] clear_phase_markers failed kb=%s", kb_id)
            # Intentionally do NOT delete graphrag_task_{kb_id} here: that
            # lock is owned by the live worker running the real KG pipeline,
            # and force-deleting it would cancel an in-flight task on a
            # sibling worker. The lock has its own TTL and will expire.
            REDIS_CONN.delete(claim)

            logging.info("[reconcile] FINALIZED kb=%s task=%s (%d nodes, %d edges)", kb_id, task_id, n_nodes, n_edges)
            finalized += 1
        except Exception:
            logging.exception("[reconcile] kb=%s task=%s failed", kb_id, task_id)
            failed += 1
            try:
                REDIS_CONN.delete(f"graphrag:reconcile:{task_id}")
            except Exception:
                pass
            try:
                REDIS_CONN.delete(f"graphrag:hb:{task_id}")
            except Exception:
                pass

    logging.info("[reconcile] done: finalized=%d skipped=%d failed=%d (candidates %d)", finalized, skipped, failed, len(kbs))


async def _run_reconcile_as_leader():
    """Leader-elected wrapper around reconcile_stuck_graphrag_tasks."""
    te = _get_task_executor_module()
    if te is None:
        logging.error("[reconcile] task_executor module not found, aborting leader reconcile")
        return
    _reconcile_leader_key = "graphrag:reconcile:leader"
    _reconcile_leader_ttl = 600
    _leader_token = f"{socket.gethostname()}:{os.getpid()}:{time.time()}"
    _is_leader = False
    try:
        _is_leader = bool(
            REDIS_CONN.REDIS.set(_reconcile_leader_key, _leader_token, ex=_reconcile_leader_ttl, nx=True)
        )
    except Exception:
        logging.exception("reconcile leader election failed, falling back to local decision")
        _is_leader = True

    if _is_leader:
        try:
            await reconcile_stuck_graphrag_tasks()
        except Exception:
            logging.exception("reconcile_stuck_graphrag_tasks uncaught exception")
        finally:
            try:
                if REDIS_CONN.REDIS is not None:
                    REDIS_CONN.REDIS.eval(
                        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
                        1,
                        _reconcile_leader_key,
                        _leader_token,
                    )
            except Exception:
                logging.exception("reconcile leader release failed (will rely on TTL expiry)")
    else:
        logging.info("[reconcile] non-leader worker, skipping this reconcile")
        try:
            wait_deadline = time.time() + _reconcile_leader_ttl
            while time.time() < wait_deadline and not te.stop_event.is_set():
                val = None
                try:
                    val = REDIS_CONN.REDIS.get(_reconcile_leader_key) if REDIS_CONN.REDIS else None
                except Exception:
                    pass
                if val is None:
                    break
                await asyncio.sleep(2)
        except Exception:
            logging.exception("[reconcile] waiting for leader failed")


# -----------------------------------------------------------------------------
# Heartbeat renewal loop for the currently active task.
# -----------------------------------------------------------------------------
async def _heartbeat_loop():
    """Renew the heartbeat lock for the currently active task."""
    interval = GraphRAGConfig.HEARTBEAT_INTERVAL
    ttl = GraphRAGConfig.HEARTBEAT_TTL
    value = f"{socket.gethostname()}:{os.getpid()}"
    while True:
        await asyncio.sleep(interval)
        tid = _get_current_task_id()
        if not tid:
            continue
        try:
            # XX flag: only renew if the key already exists.  This
            # prevents reviving a heartbeat that set_progress already
            # deleted during task finalize — without XX we would race
            # the delete and keep a stuck task invisible to reconcile.
            REDIS_CONN.REDIS.set(f"graphrag:hb:{tid}", value, ex=ttl, xx=True)
        except Exception:
            logging.exception("[heartbeat] renew failed task=%s", tid)


# -----------------------------------------------------------------------------
# Async KG postprocess consumer (resolution + community).
# -----------------------------------------------------------------------------
async def kg_postprocess_consumer():
    """P5-T3: background consumer for async resolution/community phases."""
    if not GraphRAGConfig.USE_ASYNC_KG_PHASES:
        return

    te = _get_task_executor_module()
    if te is None:
        logging.error("[KG-PP] task_executor module not found, consumer aborting")
        return
    queue_name = GraphRAGConfig.KG_POSTPROCESS_QUEUE
    group_name = SVR_CONSUMER_GROUP_NAME + "_kg_pp"
    consumer_name = te.CONSUMER_NAME + "_kg_pp"
    logging.info("[KG-PP] Consumer starting on %s (group=%s)", queue_name, group_name)

    while not te.stop_event.is_set():
        msg = None
        try:
            msg = REDIS_CONN.queue_consumer(queue_name, group_name, consumer_name)
        except Exception:
            logging.exception("[KG-PP] queue_consumer error")
            await asyncio.sleep(5)
            continue

        if not msg:
            await asyncio.sleep(1)
            continue

        payload = msg.get_message()
        tenant_id = payload.get("tenant_id")
        kb_id = payload.get("kb_id")
        task_id = payload.get("task_id")
        with_resolution = payload.get("with_resolution", False)
        with_community = payload.get("with_community", False)
        kb_task_llm_id = payload.get("kb_task_llm_id")
        task_language = payload.get("task_language", "English")

        logging.info(
            "[KG-PP] Processing kb=%s task=%s resolution=%s community=%s",
            kb_id, task_id, with_resolution, with_community,
        )

        try:
            if has_canceled(task_id):
                logging.info("[KG-PP] kb=%s task=%s has been cancelled, skipping", kb_id, task_id)
                msg.ack()
                continue

            chat_model_config = get_model_config_from_provider_instance(tenant_id, LLMType.CHAT, kb_task_llm_id)
            chat_model = LLMBundle(tenant_id, chat_model_config, lang=task_language)
            embd_model_config = get_tenant_default_model_by_type(tenant_id, LLMType.EMBEDDING)
            embedding_model = LLMBundle(tenant_id, embd_model_config, lang=task_language)

            try:
                kb = KnowledgebaseService.get_detail(kb_id)
                kb_parser_config = kb.get("parser_config", {}) if kb else {}
                graphrag_config = kb_parser_config.get("graphrag", {})
                entity_types = graphrag_config.get("entity_types", []) or []
            except Exception:
                logging.exception("[KG-PP] Failed to load KB parser_config for kb=%s", kb_id)
                entity_types = []

            from rag.graphrag.utils import get_graph
            final_graph = await get_graph(tenant_id, kb_id)
            if final_graph is None:
                logging.error("[KG-PP] kb=%s no persisted graph found, cannot proceed", kb_id)
                msg.ack()
                continue

            def pp_callback(msg=None, prog=None):
                if msg:
                    logging.info("[KG-PP] kb=%s: %s", kb_id, msg)

            # 唯一 lock_value：官方 acquire/spin_acquire 的第一步是
            # delete_if_equal(lock_key, lock_value)——固定 value 会让后到者
            # 先删掉先到者持有的锁再抢走，互斥完全失效。与主流程
            # batch_merge:{task_id} 的模式保持一致。
            kb_lock = RedisDistributedLock(f"graphrag_task_{kb_id}", lock_value=f"kg_pp:{task_id}", timeout=3600)
            try:
                await kb_lock.spin_acquire(stop_event=te.stop_event)
            except asyncio.CancelledError:
                logging.info("[KG-PP] kb=%s spin_acquire aborted by stop_event", kb_id)
                try:
                    msg.ack()
                except Exception:
                    logging.exception("[KG-PP] ack after cancel failed kb=%s", kb_id)
                continue
            try:
                if has_canceled(task_id):
                    logging.info("[KG-PP] kb=%s task=%s cancelled after lock acquire", kb_id, task_id)
                    msg.ack()
                    continue

                resolution_pending = with_resolution and not has_phase_marker(kb_id, PHASE_RESOLUTION)
                community_pending = with_community and not has_phase_marker(kb_id, PHASE_COMMUNITY)

                if not resolution_pending and not community_pending:
                    logging.info("[KG-PP] kb=%s all phases already done", kb_id)
                    msg.ack()
                    continue

                async with kg_limiter:
                    if resolution_pending:
                        from rag.graphrag.general.index import resolve_entities
                        subgraph_nodes = set(final_graph.nodes())
                        await resolve_entities(
                            final_graph,
                            subgraph_nodes,
                            tenant_id,
                            kb_id,
                            None,
                            chat_model,
                            embedding_model,
                            pp_callback,
                            task_id=task_id,
                            entity_types=entity_types,
                        )
                        set_phase_marker(kb_id, PHASE_RESOLUTION)
                        logging.info("[KG-PP] kb=%s resolution done", kb_id)

                    if community_pending:
                        from rag.graphrag.general.index import extract_community
                        await extract_community(
                            final_graph,
                            tenant_id,
                            kb_id,
                            None,
                            chat_model,
                            embedding_model,
                            pp_callback,
                            task_id=task_id,
                        )
                        set_phase_marker(kb_id, PHASE_COMMUNITY)
                        logging.info("[KG-PP] kb=%s community done", kb_id)

                msg.ack()
                logging.info("[KG-PP] kb=%s postprocess complete", kb_id)
            finally:
                kb_lock.release()
        except Exception:
            # Ack the message even on failure so it does not stay in the
            # Redis Stream PEL forever and get re-delivered in a loop on the
            # next boot. The failure is already logged.
            logging.exception("[KG-PP] kb=%s postprocess failed", kb_id)
            try:
                msg.ack()
            except Exception:
                logging.exception("[KG-PP] ack after failure failed kb=%s", kb_id)


# -----------------------------------------------------------------------------
# Monkey-patching helpers.
# -----------------------------------------------------------------------------
def _make_set_progress_wrapper(original_set_progress):
    """Wrap set_progress with heartbeat lock v2 bookkeeping."""
    def set_progress(task_id, from_page=0, to_page=-1, prog=None, msg="Processing..."):
        # Heartbeat lock v2: write a TTL key whenever the active task changes.
        # Only enabled when reconcile-on-boot is enabled, to avoid unconditional
        # Redis traffic in official deployments.
        prev_tid = None
        if GraphRAGConfig.RECONCILE_STUCK_ON_BOOT:
            prev_tid = _get_current_task_id()
            if prev_tid != task_id:
                _set_current_task_id(task_id)
                try:
                    REDIS_CONN.set(
                        f"graphrag:hb:{task_id}",
                        f"{socket.gethostname()}:{os.getpid()}",
                        exp=GraphRAGConfig.HEARTBEAT_TTL,
                    )
                except Exception:
                    logging.exception("[heartbeat] initial lock write failed task=%s", task_id)

        try:
            return original_set_progress(task_id, from_page=from_page, to_page=to_page, prog=prog, msg=msg)
        finally:
            # Heartbeat cleanup, decoupled from the prog value to avoid stranded
            # keys on `set_progress(task_id, prog=None)` paths (msg-only updates):
            #   1) on task switch, drop the previous task's heartbeat
            #   2) on terminal state (prog >= 1.0 or prog <= 0), drop current task's heartbeat
            if GraphRAGConfig.RECONCILE_STUCK_ON_BOOT:
                if prev_tid and prev_tid != task_id:
                    try:
                        REDIS_CONN.delete(f"graphrag:hb:{prev_tid}")
                    except Exception:
                        logging.exception("[heartbeat] previous-task lock delete failed task=%s", prev_tid)
                if prog is not None and (prog >= 1.0 or prog <= 0):
                    try:
                        REDIS_CONN.delete(f"graphrag:hb:{task_id}")
                    except Exception:
                        logging.exception("[heartbeat] terminal lock delete failed task=%s", task_id)
                    _set_current_task_id(None)

    return set_progress


def _make_main_wrapper(original_main):
    """Wrap main() with boot-time reconcile and background KG phase tasks."""
    async def main():
        te = _get_task_executor_module()
        if te is None:
            # Should never happen: this wrapper is only installed by
            # patch_task_executor(), which caches the module reference.
            return await original_main()

        # Ensure signal handlers are installed before any potentially long
        # boot-time work (matches the order in the original embedded version).
        signal.signal(signal.SIGINT, te.signal_handler)
        signal.signal(signal.SIGTERM, te.signal_handler)

        # Boot-time reconciliation: leader-elected scan for stuck graphrag tasks.
        if te.TASK_TYPE == "common" and GraphRAGConfig.RECONCILE_STUCK_ON_BOOT:
            await _run_reconcile_as_leader()

        kg_pp_task = None
        heartbeat_task = None
        try:
            # kg_pp_task is only created when async KG phases are enabled to
            # avoid an idle asyncio task in default deployments.
            if GraphRAGConfig.USE_ASYNC_KG_PHASES:
                kg_pp_task = asyncio.create_task(kg_postprocess_consumer())
            if GraphRAGConfig.RECONCILE_STUCK_ON_BOOT:
                heartbeat_task = asyncio.create_task(_heartbeat_loop())
            return await original_main()
        finally:
            to_await = []
            if kg_pp_task is not None:
                kg_pp_task.cancel()
                to_await.append(kg_pp_task)
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                to_await.append(heartbeat_task)
            if to_await:
                await asyncio.gather(*to_await, return_exceptions=True)

    return main


# -----------------------------------------------------------------------------
# task_executor module lookup.
#
# Production launches run ``python rag/svr/task_executor.py`` (see
# docker/entrypoint.sh / docker/launch_backend_service.sh), so the module is
# registered as ``__main__`` — ``sys.modules["rag.svr.task_executor"]`` does
# NOT exist and a plain lookup silently finds nothing. Unit tests import it
# as a regular module instead. All lookups must go through this helper.
# -----------------------------------------------------------------------------
_te_module = None


def _get_task_executor_module():
    """Return the live task_executor module, or None when it cannot be found."""
    global _te_module
    if _te_module is not None:
        return _te_module
    te = sys.modules.get("rag.svr.task_executor")
    if te is None:
        main_mod = sys.modules.get("__main__")
        if main_mod is not None and hasattr(main_mod, "set_progress") and hasattr(main_mod, "main"):
            te = main_mod
    if te is not None:
        _te_module = te
    return te


def patch_task_executor():
    """Install custom extensions into the already-imported task_executor module."""
    te = _get_task_executor_module()
    if te is None:
        logging.warning(
            "[task_executor_extras] task_executor module not found in sys.modules; "
            "custom patches (reconcile/heartbeat/KG-PP) NOT installed"
        )
        return
    if getattr(te, "_task_executor_patched", False):
        return

    # Preserve originals so they can be restored in tests if needed.
    te._original_set_progress = te.set_progress
    te._original_main = te.main

    te.set_progress = _make_set_progress_wrapper(te._original_set_progress)
    te.main = _make_main_wrapper(te._original_main)

    # CONSUMER_NAME is normally assigned in the ``if __name__ == "__main__"``
    # block AFTER this patch installs (script mode overwrites this default),
    # so provide a fallback for module-imported usage (e.g. tests) to keep
    # the KG-PP consumer from hitting AttributeError.
    if not hasattr(te, "CONSUMER_NAME"):
        te.CONSUMER_NAME = f"task_executor_{getattr(te, 'TASK_TYPE', 'common')}_0"

    # Backward-compat shim: only installed when reconcile-on-boot is enabled,
    # so default deployments don't pay the global __getattr__ dispatch cost.
    if GraphRAGConfig.RECONCILE_STUCK_ON_BOOT:
        def __getattr__(name):
            if name == "current_task_id":
                return _get_current_task_id()
            raise AttributeError(f"module {te.__name__!r} has no attribute {name!r}")
        te.__getattr__ = __getattr__

    te._task_executor_patched = True
    logging.info(
        "[task_executor_extras] patches installed on %s (reconcile_on_boot=%s, async_kg_phases=%s)",
        getattr(te, "__name__", "<unknown>"),
        GraphRAGConfig.RECONCILE_STUCK_ON_BOOT,
        GraphRAGConfig.USE_ASYNC_KG_PHASES,
    )
