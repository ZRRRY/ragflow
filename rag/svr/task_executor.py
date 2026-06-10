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
import argparse
import time
from typing import Optional

from rag.svr.task_executor_refactor.task_manager import TaskManager
from rag.svr.task_executor_refactor.recording_context import timed_with_recording, get_recording_context, \
    RecordingContext, set_recording_context, NullRecordingContext

start_ts = time.time()

# LiteLLM fetches a model cost map from GitHub during import unless this is set.
# Parser pods should not block startup on external network access.
import os
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")  # no internet, save about 10s

from common.misc_utils import thread_pool_exec

import asyncio
import socket
# from beartype import BeartypeConf
# from beartype.claw import beartype_all  # <-- you didn't sign up for this
# beartype_all(conf=BeartypeConf(violation_type=UserWarning))    # <-- emit warnings from all code
import random
import sys
import threading

from api.db import PIPELINE_SPECIAL_PROGRESS_FREEZE_TASK_TYPES
from api.db.services.knowledgebase_service import KnowledgebaseService
from api.db.services.pipeline_operation_log_service import PipelineOperationLogService
from api.db.joint_services.memory_message_service import handle_save_to_memory_task
from common.connection_utils import timeout
from common.metadata_utils import turn2jsonschema, update_metadata_to
from rag.utils.base64_image import image2id
from rag.utils.raptor_utils import (
    collect_raptor_chunk_ids,
    collect_raptor_methods,
    get_raptor_clustering_method,
    get_raptor_tree_builder,
    get_skip_reason,
    make_raptor_summary_chunk_id,
    should_skip_raptor,
)
from common.log_utils import init_root_logger
from common.config_utils import show_configs
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.limiter import AdaptiveConcurrencyLimiter, current_limiter
from rag.graphrag.phase_markers import PHASE_RESOLUTION, PHASE_COMMUNITY, has_phase_marker, set_phase_marker, clear_phase_markers
from rag.graphrag.utils import get_llm_cache, set_llm_cache, get_tags_from_cache, set_tags_to_cache
from rag.prompts.generator import keyword_extraction, question_proposal, content_tagging, run_toc_from_text, \
    gen_metadata
import logging
import os
from datetime import datetime
import json
import xxhash
import copy
import re
from functools import partial
from multiprocessing.context import TimeoutError
from timeit import default_timer as timer
import signal
import exceptiongroup
import faulthandler
import numpy as np
from peewee import DoesNotExist
from common.constants import LLMType, ParserType, PipelineTaskType
from api.db.services.document_service import DocumentService
from api.db.services.doc_metadata_service import DocMetadataService
from api.db.services.llm_service import LLMBundle
from api.db.services.task_service import TaskService, has_canceled, CANVAS_DEBUG_DOC_ID, GRAPH_RAPTOR_FAKE_DOC_ID
from api.db.services.file2document_service import File2DocumentService
from api.db.joint_services.tenant_model_service import get_tenant_default_model_by_type, get_model_config_from_provider_instance
from common.versions import get_ragflow_version
from api.db.db_models import close_connection
from rag.app import laws, paper, presentation, manual, qa, table, book, resume, picture, naive, one, audio, \
    email, tag
from rag.nlp import search, rag_tokenizer, add_positions
from rag.raptor import (
    RAPTOR_TREE_BUILDER,
)
from common.token_utils import num_tokens_from_string, truncate
from rag.utils.redis_conn import REDIS_CONN, RedisDistributedLock
from rag.graphrag.utils import chat_limiter
from common.signal_utils import start_tracemalloc_and_snapshot, stop_tracemalloc
from common.exceptions import TaskCanceledException
from rag.svr.task_executor_limiter import (
    task_limiter,
    chunk_limiter,
    embed_limiter,
    minio_limiter,
    kg_limiter,
)
from common import settings
from common.constants import PAGERANK_FLD, TAG_FLD, SVR_CONSUMER_GROUP_NAME
from rag.utils.table_es_metadata import (
    aggregate_table_manual_doc_metadata,
    merge_table_parser_config_from_kb,
    table_parser_strip_doc_metadata_keys,
)
from rag.nlp import search as nlp_search

BATCH_SIZE = 64


FACTORY = {
    "general": naive,
    ParserType.NAIVE.value: naive,
    ParserType.PAPER.value: paper,
    ParserType.BOOK.value: book,
    ParserType.PRESENTATION.value: presentation,
    ParserType.MANUAL.value: manual,
    ParserType.LAWS.value: laws,
    ParserType.QA.value: qa,
    ParserType.TABLE.value: table,
    ParserType.RESUME.value: resume,
    ParserType.PICTURE.value: picture,
    ParserType.ONE.value: one,
    ParserType.AUDIO.value: audio,
    ParserType.EMAIL.value: email,
    ParserType.KG.value: naive,
    ParserType.TAG.value: tag
}

TASK_TYPE_TO_PIPELINE_TASK_TYPE = {
    "dataflow": PipelineTaskType.PARSE,
    "raptor": PipelineTaskType.RAPTOR,
    "graphrag": PipelineTaskType.GRAPH_RAG,
    "mindmap": PipelineTaskType.MINDMAP,
    "memory": PipelineTaskType.MEMORY,
}

UNACKED_ITERATOR = None
# Task type and executor index (consistent with SAAS version)
TASK_TYPE = "common"
TE_IDX = "0"

BOOT_AT = datetime.now().astimezone().isoformat(timespec="milliseconds")
PENDING_TASKS = 0
LAG_TASKS = 0
DONE_TASKS = 0
FAILED_TASKS = 0

CURRENT_TASKS = {}

WORKER_HEARTBEAT_TIMEOUT = int(os.environ.get('WORKER_HEARTBEAT_TIMEOUT', '120'))

# Override kg_limiter with AdaptiveConcurrencyLimiter if configured.
if GraphRAGConfig.USE_ADAPTIVE_LIMITER:
    kg_limiter = AdaptiveConcurrencyLimiter(
        initial_limit=GraphRAGConfig.MAX_CONCURRENT_KG_TASKS,
        min_limit=GraphRAGConfig.MIN_CONCURRENT_KG_TASKS,
        max_limit=GraphRAGConfig.MAX_CONCURRENT_KG_TASKS,
        adjust_interval=GraphRAGConfig.ADAPTIVE_INTERVAL,
        degrade_threshold=GraphRAGConfig.ADAPTIVE_DEGRADE_THRESHOLD,
        increase_threshold=GraphRAGConfig.ADAPTIVE_INCREASE_THRESHOLD,
    )
    # Expose to downstream modules (utils.py / index.py) so they can record events.
    import rag.graphrag.limiter as _limiter_mod
    _limiter_mod.current_limiter = kg_limiter
else:
    kg_limiter = asyncio.Semaphore(GraphRAGConfig.MAX_CONCURRENT_KG_TASKS)

stop_event = threading.Event()

# 心跳锁 v2:记录当前正在 set_progress 的 task_id,供后台 _heartbeat_loop 续期
# Phase 4.1: 用 threading.Lock 保护多线程 set_progress 并发写 + asyncio loop 读。
# 之前 GIL 只保证单变量赋值原子，但多个 set_progress 交错时
# current_task_id 会被切到 B 的 task_id 但 graphrag:hb:{A} 锁已建立，
# reconcile 看到两个 tid 都"活着"误判为正常。
_current_task_id_lock = threading.Lock()
_current_task_id_state: dict[str, Optional[str]] = {"tid": None}


def _get_current_task_id() -> Optional[str]:
    """线程安全读 current_task_id。"""
    with _current_task_id_lock:
        return _current_task_id_state["tid"]


def _set_current_task_id(tid: Optional[str]) -> Optional[str]:
    """线程安全写 current_task_id，返回旧值。"""
    with _current_task_id_lock:
        old = _current_task_id_state["tid"]
        _current_task_id_state["tid"] = tid
        return old


# 兼容旧代码：保留模块级 current_task_id 名字，但读写都加锁
def __getattr__(name):
    if name == "current_task_id":
        return _get_current_task_id()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

def signal_handler(sig, frame):
    logging.info("Received interrupt signal, shutting down...")
    stop_event.set()
    time.sleep(1)
    sys.exit(0)


def set_progress(task_id, from_page=0, to_page=-1, prog=None, msg="Processing..."):
    try:
        # [心跳锁 v2] 首次 SETEX —— 同一 task_id 切换时(worker 接下一个 task)重写
        # Phase 4.1: 用线程安全读写替代裸 global 变量
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
                logging.exception("[heartbeat] 首次写锁失败 task=%s", task_id)

        if prog is not None and prog < 0:
            msg = "[ERROR]" + msg
        cancel = has_canceled(task_id)

        if cancel:
            msg += " [Canceled]"
            prog = -1

        if to_page > 0:
            if msg:
                if from_page < to_page:
                    msg = f"Page({from_page + 1}~{to_page + 1}): " + msg
        if msg:
            msg = datetime.now().strftime("%H:%M:%S") + " " + msg
        d = {"progress_msg": msg}
        if prog is not None:
            d["progress"] = prog

        TaskService.update_progress(task_id, d)

        close_connection()
        if cancel:
            raise TaskCanceledException(msg)
        logging.info(f"set_progress({task_id}), progress: {prog}, progress_msg: {msg}")

        # [心跳锁 v2] 终态删锁:task 完成(prog=1.0)或失败(prog=-1.0)后清锁
        if prog is not None and (prog >= 1.0 or prog <= 0):
            try:
                REDIS_CONN.delete(f"graphrag:hb:{task_id}")
            except Exception:
                logging.exception("[heartbeat] 终态删锁失败 task=%s", task_id)
            # Phase 4.1: 线程安全写
            _set_current_task_id(None)
    except TaskCanceledException:
        raise
    except DoesNotExist:
        logging.warning(f"set_progress({task_id}) got exception DoesNotExist")
    except Exception as e:
        logging.exception(f"set_progress({task_id}), progress: {prog}, progress_msg: {msg}, got exception: {e}")


async def reconcile_stuck_graphrag_tasks():
    """启动时一次性扫描:把 OS 已有数据但 MySQL progress 卡在 (0,1) 的
    graphrag task 标完成。仅当 RECONCILE_STUCK_ON_BOOT=1 时生效。

    修复方向 ③: 兜底 direction ① 未覆盖的崩溃窗口(SIGKILL 落在
    insert_chunks_bounded 中间、网络瞬断让 set_progress 丢消息等)。
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

    # 1) 候选 KB:有 graphrag_task_id、finish_at 还是空
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
        logging.exception("[reconcile] 候选 KB 列表查询失败")
        return
    logging.info("[reconcile] 候选 %d 个 KB", len(kbs))

    finalized = skipped = failed = 0
    for kb in kbs:
        kb_id = kb["id"]
        tenant_id = kb["tenant_id"]
        task_id = kb["graphrag_task_id"]
        try:
            # 2) Redis SETNX 抢占(防多 worker 重复)
            claim = f"graphrag:reconcile:{task_id}"
            if not REDIS_CONN.REDIS.set(claim, "1", ex=600, nx=True):
                logging.info("[reconcile] kb=%s task=%s 已被别的 worker 抢占,skip", kb_id, task_id)
                continue

            # 3) 加载 task,过滤 progress / update_time
            task = TaskService.get_task(task_id)
            if not task:
                logging.warning("[reconcile] kb=%s task=%s 不存在,skip", kb_id, task_id)
                REDIS_CONN.delete(claim)
                continue
            prog = task.get("progress", 0) or 0
            update_time = task.get("update_time") or 0
            if not (0 < prog < 1):
                logging.info("[reconcile] kb=%s task=%s progress=%.4f 不在 (0,1),skip", kb_id, task_id, prog)
                REDIS_CONN.delete(claim)
                continue

            # [v2] 心跳锁优先判定:TTL > 0 视为"真在跑",直接 skip
            # redis-py:ttl 返回 int 秒;key 不存在返回 -2;存在但无 TTL 返回 -1
            heartbeat_key = f"graphrag:hb:{task_id}"
            try:
                ttl = REDIS_CONN.ttl(heartbeat_key)
                if ttl is None:
                    ttl = -2  # 防御:某些版本 redis-py 在 key 不存在时返回 None
            except Exception:
                logging.exception("[reconcile] TTL 查询失败 task=%s,降级到 grace 判定", task_id)
                ttl = -2

            if ttl > 0:
                logging.info(
                    "[reconcile] kb=%s task=%s heartbeat 仍活(ttl=%ds),skip reconcile",
                    kb_id, task_id, ttl,
                )
                REDIS_CONN.delete(claim)
                skipped += 1
                continue

            # [v2 兜底] 心跳锁缺失或异常时,仍要求 update_time 距今 > STUCK_TASK_GRACE_MINUTES
            # 覆盖:KB 切换 task 空档 / 心跳锁误删 / 罕见竞态 / Redis 故障
            if update_time > cutoff_ms:
                logging.info(
                    "[reconcile] kb=%s task=%s heartbeat 缺失但 update_time 距今 < grace,skip",
                    kb_id, task_id,
                )
                REDIS_CONN.delete(claim)
                continue

            # 4) 反查 OS 节点/边数(用 tenant_id 派生索引,见 rag/nlp/search.py:index_name)
            index = search.index_name(tenant_id)
            try:
                n_nodes = await thread_pool_exec(
                    settings.docStoreConn.count, {"knowledge_graph_kwd": ["entity"]}, index, [kb_id]
                )
                n_nodes = int(n_nodes or 0)
            except Exception:
                logging.exception("[reconcile] 查 entity 数失败 kb=%s", kb_id)
                n_nodes = 0
            try:
                n_edges = await thread_pool_exec(
                    settings.docStoreConn.count, {"knowledge_graph_kwd": ["relation"]}, index, [kb_id]
                )
                n_edges = int(n_edges or 0)
            except Exception:
                logging.exception("[reconcile] 查 relation 数失败 kb=%s", kb_id)
                n_edges = 0

            if n_nodes < min_nodes or n_edges < min_edges:
                logging.warning(
                    "[reconcile] kb=%s task=%s OS 只有 %d nodes / %d edges < 阈值,不动",
                    kb_id, task_id, n_nodes, n_edges,
                )
                REDIS_CONN.delete(claim)
                skipped += 1
                continue

            # 5) 标完成 + 写 KB 的 finish_at、清 task 绑定
            msg = f"Knowledge Graph reconciled ({n_nodes} nodes, {n_edges} edges) [boot]"
            TaskService.update_progress(task_id, {"progress": 1.0, "progress_msg": msg})
            # [v2] 清心跳锁(防御性,理论上 set_progress 终态会清,reconcile 是另一条路径)
            try:
                REDIS_CONN.delete(heartbeat_key)
            except Exception:
                logging.exception("[reconcile] 清心跳锁失败 task=%s", task_id)
            KnowledgebaseService.update_by_id(
                kb_id, {"graphrag_task_finish_at": datetime.now(), "graphrag_task_id": ""}
            )
            # 6) 清 Redis 上的 phase marker 和 task 锁,避免下次任务被旧状态阻挡
            try:
                clear_phase_markers(kb_id)
            except Exception:
                logging.exception("[reconcile] clear_phase_markers 失败 kb=%s", kb_id)
            try:
                REDIS_CONN.delete(f"graphrag_task_{kb_id}")
            except Exception:
                logging.exception("[reconcile] 删 graphrag_task_%s 失败", kb_id)
            REDIS_CONN.delete(claim)

            logging.info("[reconcile] FINALIZED kb=%s task=%s (%d nodes, %d edges)", kb_id, task_id, n_nodes, n_edges)
            finalized += 1
        except Exception:
            logging.exception("[reconcile] kb=%s task=%s 失败", kb_id, task_id)
            failed += 1
            try:
                REDIS_CONN.delete(f"graphrag:reconcile:{task_id}")
            except Exception:
                pass
            # [v2] 本 task 标失败后清心跳锁,避免下次 reconcile 撞到陈旧锁
            try:
                REDIS_CONN.delete(f"graphrag:hb:{task_id}")
            except Exception:
                pass

    logging.info("[reconcile] 完成:finalized=%d skipped=%d failed=%d (候选 %d)", finalized, skipped, failed, len(kbs))


async def collect():
    global CONSUMER_NAME, DONE_TASKS, FAILED_TASKS
    global UNACKED_ITERATOR

    svr_queue_names = settings.get_svr_queue_names(TASK_TYPE)

    redis_msg = None
    try:
        if not UNACKED_ITERATOR:
            UNACKED_ITERATOR = REDIS_CONN.get_unacked_iterator(svr_queue_names, SVR_CONSUMER_GROUP_NAME, CONSUMER_NAME)
        try:
            redis_msg = next(UNACKED_ITERATOR)
        except StopIteration:
            for svr_queue_name in svr_queue_names:
                redis_msg = REDIS_CONN.queue_consumer(svr_queue_name, SVR_CONSUMER_GROUP_NAME, CONSUMER_NAME)
                if redis_msg:
                    break
    except Exception as e:
        logging.exception(f"collect got exception: {e}")
        return None, None

    if not redis_msg:
        return None, None
    msg = redis_msg.get_message()
    if not msg:
        logging.error(f"collect got empty message of {redis_msg.get_msg_id()}")
        redis_msg.ack()
        return None, None

    canceled = False
    if msg.get("doc_id", "") in [GRAPH_RAPTOR_FAKE_DOC_ID, CANVAS_DEBUG_DOC_ID]:
        task = msg
        if task["task_type"] in PIPELINE_SPECIAL_PROGRESS_FREEZE_TASK_TYPES:
            task = TaskService.get_task(msg["id"], msg["doc_ids"])
            if task:
                task["doc_id"] = msg["doc_id"]
                task["doc_ids"] = msg.get("doc_ids", []) or []
    elif msg.get("task_type") == PipelineTaskType.MEMORY.lower():
        _, task_obj = TaskService.get_by_id(msg["id"])
        task = task_obj.to_dict()
    else:
        task = TaskService.get_task(msg["id"])

    if task:
        canceled = has_canceled(task["id"])
    if not task or canceled:
        state = "is unknown" if not task else "has been cancelled"
        FAILED_TASKS += 1
        logging.warning(f"collect task {msg['id']} {state}")
        redis_msg.ack()
        return None, None

    task_type = msg.get("task_type", "")
    task["task_type"] = task_type
    if task_type[:8] == "dataflow":
        task["tenant_id"] = msg["tenant_id"]
        task["dataflow_id"] = msg["dataflow_id"]
        task["kb_id"] = msg.get("kb_id", "")
    if task_type[:6] == "memory":
        task["memory_id"] = msg["memory_id"]
        task["source_id"] = msg["source_id"]
        task["message_dict"] = msg["message_dict"]
    return redis_msg, task


async def get_storage_binary(bucket, name):
    return await thread_pool_exec(settings.STORAGE_IMPL.get, bucket, name)


@timed_with_recording
@timeout(60 * 80, 1)
async def build_chunks(task, progress_callback):
    if task["size"] > settings.DOC_MAXIMUM_SIZE:
        set_progress(task["id"], prog=-1, msg="File size exceeds( <= %dMb )" %
                                              (int(settings.DOC_MAXIMUM_SIZE / 1024 / 1024)))
        get_recording_context().record("file_size_exceeded", True)
        return []
    get_recording_context().record("file_size_exceeded", False)
    get_recording_context().record("parser_id", task["parser_id"])

    chunker = FACTORY[task["parser_id"].lower()]
    try:
        st = timer()
        bucket, name = File2DocumentService.get_storage_address(doc_id=task["doc_id"])
        binary = await get_storage_binary(bucket, name)
        if binary is None:
            raise FileNotFoundError(f"File not found: storage returned no content for {bucket}/{name}.")
        logging.info("From minio({}) {}/{}".format(timer() - st, task["location"], task["name"]))
    except TimeoutError:
        progress_callback(-1, "Internal server error: Fetch file from minio timeout. Could you try it again.")
        logging.exception(
            "Minio {}/{} got timeout: Fetch file from minio timeout.".format(task["location"], task["name"]))
        raise
    except Exception as e:
        if re.search("(No such file|not found)", str(e)):
            progress_callback(-1, "Can not find file <%s> from minio. Could you try it again?" % task["name"])
        else:
            progress_callback(-1, "Get file from minio: %s" % str(e).replace("'", ""))
        logging.exception("Chunking {}/{} got exception".format(task["location"], task["name"]))
        raise

    # Table parser column roles / mode are stored on the dataset (KB) parser_config;
    # chunk tasks carry document-level parser_config only — merge KB keys so manual roles apply.
    parser_config_for_chunk = merge_table_parser_config_from_kb(task)
    if task.get("parser_id", "").lower() == "table" and task.get("kb_parser_config"):
        logging.debug(
            "[TASK_EXECUTOR_DEBUG] table parser: merged KB keys into parser_config for chunk; "
            f"mode={parser_config_for_chunk.get('table_column_mode')}, "
            f"roles_keys={list((parser_config_for_chunk.get('table_column_roles') or {}).keys())}"
        )

    # Record chunk configuration for comparison
    from common.float_utils import normalize_overlapped_percent
    chunk_config = {
        "parser_id": task["parser_id"],
        "chunk_token_num": parser_config_for_chunk.get("chunk_token_num", 128),
        "overlapped_percent": normalize_overlapped_percent(
            parser_config_for_chunk.get("overlapped_percent", 0)
        ),
        "delimiter": parser_config_for_chunk.get("delimiter", "\n!?。；！？"),
        "from_page": task["from_page"],
        "to_page": task["to_page"],
        "language": task["language"],
        "layout_recognizer": parser_config_for_chunk.get("layout_recognizer"),
    }
    get_recording_context().record("chunk_config", chunk_config)
    get_recording_context().record("parser_config_after_merge", parser_config_for_chunk)

    try:
        async with chunk_limiter:
            task_language = task.get("language") or "Chinese"
            cks = await thread_pool_exec(
                chunker.chunk,
                task["name"],
                binary=binary,
                from_page=task["from_page"],
                to_page=task["to_page"],
                lang=task_language,
                callback=progress_callback,
                kb_id=task["kb_id"],
                parser_config=parser_config_for_chunk,
                tenant_id=task["tenant_id"],
            )
        logging.info("Chunking({}) {}/{} done".format(timer() - st, task["location"], task["name"]))
    except TaskCanceledException:
        raise
    except Exception as e:
        progress_callback(-1, "Internal server error while chunking: %s" % str(e).replace("'", ""))
        logging.exception("Chunking {}/{} got exception".format(task["location"], task["name"]))
        raise

    # Record raw chunks for comparison
    get_recording_context().record("raw_chunks", cks)

    # Extract and persist PDF outline if the parser attached it.
    outline_data = cks[0].get("__outline__") if cks else None
    get_recording_context().record("outline_data", outline_data)

    if cks and cks[0].get("__outline__"):
        outline = cks[0].pop("__outline__")
        try:
            ret = DocMetadataService.update_document_metadata(
                task["doc_id"],
                update_metadata_to({"outline": outline},
                                   DocMetadataService.get_document_metadata(task["doc_id"]) or {})
            )
            get_recording_context().save_func_return_value("DocMetadataService.update_document_metadata", ret)
            logging.info("Persisted PDF outline (%d entries) for doc %s", len(outline), task["doc_id"])
        except Exception as e:
            logging.warning("Failed to persist PDF outline for doc %s: %s", task["doc_id"], e)

    docs = []
    doc = {
        "doc_id": task["doc_id"],
        "kb_id": str(task["kb_id"])
    }
    if task["pagerank"]:
        doc[PAGERANK_FLD] = int(task["pagerank"])
    st = timer()

    @timeout(60)
    async def upload_to_minio(document, chunk):
        try:
            d = copy.deepcopy(document)
            d.update(chunk)
            d["id"] = xxhash.xxh64(
                (chunk["content_with_weight"] + str(d["doc_id"])).encode("utf-8", "surrogatepass")).hexdigest()
            d["create_time"] = str(datetime.now()).replace("T", " ")[:19]
            d["create_timestamp_flt"] = datetime.now().timestamp()

            if d.get("img_id"):
                docs.append(d)
                return

            if not d.get("image"):
                _ = d.pop("image", None)
                d["img_id"] = ""
                docs.append(d)
                return
            await image2id(d, partial(settings.STORAGE_IMPL.put, tenant_id=task["tenant_id"]), d["id"], task["kb_id"])
            docs.append(d)
        except Exception:
            logging.exception(
                "Saving image of chunk {}/{}/{} got exception".format(task["location"], task["name"], d["id"]))
            raise

    tasks = []
    for ck in cks:
        tasks.append(asyncio.create_task(upload_to_minio(doc, ck)))
    try:
        await asyncio.gather(*tasks, return_exceptions=False)
    except Exception as e:
        logging.error(f"MINIO PUT({task['name']}) got exception: {e}")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    el = timer() - st
    logging.info("MINIO PUT({}) cost {:.3f} s".format(task["name"], el))

    # Record docs after MinIO upload
    get_recording_context().record("docs_after_prep", docs)

    if task["parser_config"].get("auto_keywords", 0):
        st = timer()
        progress_callback(msg="Start to generate keywords for every chunk ...")
        chat_model_config = get_model_config_from_provider_instance(task["tenant_id"], LLMType.CHAT, task["llm_id"])
        chat_mdl = LLMBundle(task["tenant_id"], chat_model_config, lang=task["language"])

        async def doc_keyword_extraction(chat_mdl, d, topn):
            cached = get_llm_cache(chat_mdl.llm_name, d["content_with_weight"], "keywords", {"topn": topn})
            if not cached:
                if has_canceled(task["id"]):
                    progress_callback(-1, msg="Task has been canceled.")
                    return
                async with chat_limiter:
                    cached = await keyword_extraction(chat_mdl, d["content_with_weight"], topn)
                set_llm_cache(chat_mdl.llm_name, d["content_with_weight"], cached, "keywords", {"topn": topn})
            if cached:
                d["important_kwd"] = [k for k in re.split(r"[,，;；、\r\n]+", cached) if k.strip()]
                d["important_tks"] = rag_tokenizer.tokenize(" ".join(d["important_kwd"]))
            return

        tasks = []
        for d in docs:
            tasks.append(
                asyncio.create_task(doc_keyword_extraction(chat_mdl, d, task["parser_config"]["auto_keywords"])))
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error("Error in doc_keyword_extraction: {}".format(e))
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        progress_callback(msg="Keywords generation {} chunks completed in {:.2f}s".format(len(docs), timer() - st))

    # Record keywords extraction count
    keywords = [d for d in docs if d.get("important_kwd")]
    get_recording_context().record("keywords_extracted", keywords)

    if task["parser_config"].get("auto_questions", 0):
        st = timer()
        progress_callback(msg="Start to generate questions for every chunk ...")
        chat_model_config = get_model_config_from_provider_instance(task["tenant_id"], LLMType.CHAT, task["llm_id"])
        chat_mdl = LLMBundle(task["tenant_id"], chat_model_config, lang=task["language"])

        async def doc_question_proposal(chat_mdl, d, topn):
            cached = get_llm_cache(chat_mdl.llm_name, d["content_with_weight"], "question", {"topn": topn})
            if not cached:
                if has_canceled(task["id"]):
                    progress_callback(-1, msg="Task has been canceled.")
                    return
                async with chat_limiter:
                    cached = await question_proposal(chat_mdl, d["content_with_weight"], topn)
                set_llm_cache(chat_mdl.llm_name, d["content_with_weight"], cached, "question", {"topn": topn})
            if cached:
                d["question_kwd"] = cached.split("\n")
                d["question_tks"] = rag_tokenizer.tokenize("\n".join(d["question_kwd"]))

        tasks = []
        for d in docs:
            tasks.append(
                asyncio.create_task(doc_question_proposal(chat_mdl, d, task["parser_config"]["auto_questions"])))
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error("Error in doc_question_proposal", exc_info=e)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        progress_callback(msg="Question generation {} chunks completed in {:.2f}s".format(len(docs), timer() - st))

    # Record question generation
    questions = [d for d in docs if d.get("question_kwd")]
    get_recording_context().record("questions_generated", questions)

    if task["parser_config"].get("enable_metadata", False) and (task["parser_config"].get("metadata") or task["parser_config"].get("built_in_metadata")):
        st = timer()
        progress_callback(msg="Start to generate meta-data for every chunk ...")
        chat_model_config = get_model_config_from_provider_instance(task["tenant_id"], LLMType.CHAT, task["llm_id"])
        chat_mdl = LLMBundle(task["tenant_id"], chat_model_config, lang=task["language"])

        async def gen_metadata_task(chat_mdl, d):
            metadata_conf = task["parser_config"].get("metadata", [])
            built_in_metadata = list(task["parser_config"].get("built_in_metadata") or [])
            if isinstance(metadata_conf, dict):
                if not isinstance(metadata_conf.get("properties"), dict):
                    metadata_conf = {"type": "object", "properties": {}}
                if built_in_metadata:
                    metadata_conf = {
                        **metadata_conf,
                        "properties": {
                            **metadata_conf.get("properties", {}),
                            **turn2jsonschema(built_in_metadata).get("properties", {}),
                        },
                    }
            elif isinstance(metadata_conf, list):
                metadata_conf = metadata_conf + built_in_metadata
            else:
                metadata_conf = built_in_metadata
            cached = get_llm_cache(chat_mdl.llm_name, d["content_with_weight"], "metadata",
                                   metadata_conf)
            if not cached:
                if has_canceled(task["id"]):
                    progress_callback(-1, msg="Task has been canceled.")
                    return
                async with chat_limiter:
                    cached = await gen_metadata(chat_mdl,
                                                turn2jsonschema(metadata_conf),
                                                d["content_with_weight"])
                set_llm_cache(chat_mdl.llm_name, d["content_with_weight"], cached, "metadata",
                              metadata_conf)
            if cached:
                d["metadata_obj"] = cached

        tasks = []
        for d in docs:
            tasks.append(asyncio.create_task(gen_metadata_task(chat_mdl, d)))
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error("Error in doc_question_proposal", exc_info=e)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        metadata = {}
        for doc in docs:
            metadata = update_metadata_to(metadata, doc["metadata_obj"])
            del doc["metadata_obj"]
        if metadata:
            existing_meta = DocMetadataService.get_document_metadata(task["doc_id"])
            existing_meta = existing_meta if isinstance(existing_meta, dict) else {}
            metadata = update_metadata_to(metadata, existing_meta)
            ret = DocMetadataService.update_document_metadata(task["doc_id"], metadata)
            get_recording_context().save_func_return_value("DocMetadataService.update_document_metadata", ret)
        progress_callback(msg="Question generation {} chunks completed in {:.2f}s".format(len(docs), timer() - st))

    # Record metadata generation count
    metadata_list = [d for d in docs if d.get("metadata_obj")]
    get_recording_context().record("metadata_list_generated", metadata_list)

    if task["kb_parser_config"].get("tag_kb_ids", []):
        progress_callback(msg="Start to tag for every chunk ...")
        kb_ids = task["kb_parser_config"]["tag_kb_ids"]
        tenant_id = task["tenant_id"]
        topn_tags = task["kb_parser_config"].get("topn_tags", 3)
        S = 1000
        st = timer()
        examples = []
        all_tags = get_tags_from_cache(kb_ids)
        if not all_tags:
            all_tags = settings.retriever.all_tags_in_portion(tenant_id, kb_ids, S)
            set_tags_to_cache(kb_ids, all_tags)
        else:
            all_tags = json.loads(all_tags)
        chat_model_config = get_model_config_from_provider_instance(tenant_id, LLMType.CHAT, task["llm_id"])
        chat_mdl = LLMBundle(task["tenant_id"], chat_model_config, lang=task["language"])

        docs_to_tag = []
        for d in docs:
            task_canceled = has_canceled(task["id"])
            if task_canceled:
                progress_callback(-1, msg="Task has been canceled.")
                return None
            if settings.retriever.tag_content(tenant_id, kb_ids, d, all_tags, topn_tags=topn_tags, S=S) and len(
                    d[TAG_FLD]) > 0:
                examples.append({"content": d["content_with_weight"], TAG_FLD: d[TAG_FLD]})
            else:
                docs_to_tag.append(d)

        async def doc_content_tagging(chat_mdl, d, topn_tags):
            cached = get_llm_cache(chat_mdl.llm_name, d["content_with_weight"], all_tags, {"topn": topn_tags})
            if not cached:
                if has_canceled(task["id"]):
                    progress_callback(-1, msg="Task has been canceled.")
                    return
                picked_examples = random.choices(examples, k=2) if len(examples) > 2 else examples
                if not picked_examples:
                    picked_examples.append({"content": "This is an example", TAG_FLD: {'example': 1}})
                async with chat_limiter:
                    cached = await content_tagging(
                        chat_mdl,
                        d["content_with_weight"],
                        all_tags,
                        picked_examples,
                        topn_tags,
                    )
                if cached:
                    cached = json.dumps(cached)
            if cached:
                set_llm_cache(chat_mdl.llm_name, d["content_with_weight"], cached, all_tags, {"topn": topn_tags})
                d[TAG_FLD] = json.loads(cached)

        tasks = []
        for d in docs_to_tag:
            tasks.append(asyncio.create_task(doc_content_tagging(chat_mdl, d, topn_tags)))
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error("Error tagging docs: {}".format(e))
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        progress_callback(msg="Tagging {} chunks completed in {:.2f}s".format(len(docs), timer() - st))

    # Record tags applied
    tags_applied = [d for d in docs if d.get(TAG_FLD)]
    get_recording_context().record("tags_applied", tags_applied)

    # Record final chunks for comparison
    get_recording_context().record("final_chunks", docs)
    final_chunk_ids = [c.get("id") for c in docs if isinstance(c, dict) and "id" in c]
    get_recording_context().record("final_chunk_ids_count", len(final_chunk_ids))

    return docs


@timed_with_recording
def build_TOC(task, docs, progress_callback):
    progress_callback(msg="Start to generate table of content ...")
    chat_model_config = get_model_config_from_provider_instance(task["tenant_id"], LLMType.CHAT, task["llm_id"])
    chat_mdl = LLMBundle(task["tenant_id"], chat_model_config, lang=task["language"])
    docs = sorted(docs, key=lambda d: (
        d.get("page_num_int", 0)[0] if isinstance(d.get("page_num_int", 0), list) else d.get("page_num_int", 0),
        d.get("top_int", 0)[0] if isinstance(d.get("top_int", 0), list) else d.get("top_int", 0)
    ))
    toc: list[dict] = asyncio.run(
        run_toc_from_text([d["content_with_weight"] for d in docs], chat_mdl, progress_callback))
    logging.info("------------ T O C -------------\n" + json.dumps(toc, ensure_ascii=False, indent='  '))
    for ii, item in enumerate(toc):
        try:
            chunk_val = item.pop("chunk_id", None)
            if chunk_val is None or str(chunk_val).strip() == "":
                logging.warning(f"Index {ii}: chunk_id is missing or empty. Skipping.")
                continue
            curr_idx = int(chunk_val)
            if curr_idx >= len(docs):
                logging.error(f"Index {ii}: chunk_id {curr_idx} exceeds docs length {len(docs)}.")
                continue
            item["ids"] = [docs[curr_idx]["id"]]
            if ii + 1 < len(toc):
                next_chunk_val = toc[ii + 1].get("chunk_id", "")
                if str(next_chunk_val).strip() != "":
                    next_idx = int(next_chunk_val)
                    for jj in range(curr_idx + 1, min(next_idx + 1, len(docs))):
                        item["ids"].append(docs[jj]["id"])
                else:
                    logging.warning(f"Index {ii + 1}: next chunk_id is empty, range fill skipped.")
        except (ValueError, TypeError) as e:
            logging.error(f"Index {ii}: Data conversion error - {e}")
        except Exception as e:
            logging.exception(f"Index {ii}: Unexpected error - {e}")

    if toc:
        d = copy.deepcopy(docs[-1])
        d["content_with_weight"] = json.dumps(toc, ensure_ascii=False)
        d["toc_kwd"] = "toc"
        d["available_int"] = 0
        d["page_num_int"] = [100000000]
        d["id"] = xxhash.xxh64(
            (d["content_with_weight"] + str(d["doc_id"])).encode("utf-8", "surrogatepass")).hexdigest()
        return d
    return None


def init_kb(row, vector_size: int):
    idxnm = search.index_name(row["tenant_id"])
    parser_id = row.get("parser_id", None)
    return settings.docStoreConn.create_idx(idxnm, row.get("kb_id", ""), vector_size, parser_id)


@timed_with_recording
async def embedding(docs, mdl, parser_config=None, callback=None):
    if parser_config is None:
        parser_config = {}
    tts, cnts = [], []
    for d in docs:
        tts.append(d.get("docnm_kwd", "Title"))
        c = "\n".join(d.get("question_kwd", []))
        if not c:
            c = d["content_with_weight"]
        c = re.sub(r"</?(table|td|caption|tr|th)( [^<>]{0,12})?>", " ", c)
        if not c.strip():
            logging.debug("embedding(): normalized whitespace-only chunk to placeholder 'None' (len=%d)", len(c))
            c = "None"
        cnts.append(c)

    tk_count = 0
    if len(tts) == len(cnts):
        vts, c = await thread_pool_exec(mdl.encode, tts[0:1])
        tts = np.tile(vts[0], (len(cnts), 1))
        tk_count += c

    @timeout(60)
    def batch_encode(txts):
        nonlocal mdl
        return mdl.encode([truncate(c, mdl.max_length - 10) for c in txts])

    cnts_batches = []
    for i in range(0, len(cnts), settings.EMBEDDING_BATCH_SIZE):
        async with embed_limiter:
            vts, c = await thread_pool_exec(batch_encode, cnts[i: i + settings.EMBEDDING_BATCH_SIZE])
        cnts_batches.append(vts)
        tk_count += c
        callback(prog=0.7 + 0.2 * (i + 1) / len(cnts), msg="")
    cnts = np.vstack(cnts_batches) if cnts_batches else np.array([])
    filename_embd_weight = parser_config.get("filename_embd_weight", 0.1)  # due to the db support none value
    if not filename_embd_weight:
        filename_embd_weight = 0.1
    title_w = float(filename_embd_weight)
    if tts.ndim == 2 and cnts.ndim == 2 and tts.shape == cnts.shape:
        vects = title_w * tts + (1 - title_w) * cnts
    else:
        vects = cnts

    assert len(vects) == len(docs)
    vector_size = 0
    for i, d in enumerate(docs):
        v = vects[i].tolist()
        vector_size = len(v)
        d["q_%d_vec" % len(v)] = v
    return tk_count, vector_size


@timed_with_recording
async def run_dataflow(task: dict):
    from api.db.services.canvas_service import UserCanvasService
    from rag.flow.pipeline import Pipeline

    task_start_ts = timer()
    dataflow_id = task["dataflow_id"]
    doc_id = task["doc_id"]
    task_id = task["id"]
    task_dataset_id = task["kb_id"]

    if task["task_type"] == "dataflow":
        e, cvs = UserCanvasService.get_by_id(dataflow_id)
        assert e, "User pipeline not found."
        dsl = cvs.dsl
    else:
        e, pipeline_log = PipelineOperationLogService.get_by_id(dataflow_id)
        assert e, "Pipeline log not found."
        dsl = pipeline_log.dsl
        dataflow_id = pipeline_log.pipeline_id
    pipeline = Pipeline(dsl, tenant_id=task["tenant_id"], doc_id=doc_id, task_id=task_id, flow_id=dataflow_id)
    chunks = await pipeline.run(file=task["file"]) if task.get("file") else await pipeline.run()
    if doc_id == CANVAS_DEBUG_DOC_ID:
        get_recording_context().record("dataflow_debug_result", "canvas_debug_mode")
        get_recording_context().record("dataflow_chunks", chunks)
        return

    if not chunks:
        get_recording_context().record("pipeline_output_count", 0)
        get_recording_context().record("pipeline_output_type", "empty")
        ret = PipelineOperationLogService.create(document_id=doc_id, pipeline_id=dataflow_id,
                                           task_type=PipelineTaskType.PARSE, dsl=str(pipeline))
        get_recording_context().save_func_return_value("PipelineOperationLogService.create", ret)
        return

    embedding_token_consumption = chunks.get("embedding_token_consumption", 0)
    # The output key may exist with an empty payload; check presence, not truthiness.
    if "chunks" in chunks:
        chunks = copy.deepcopy(chunks["chunks"])
        output_type = "chunks"
    elif "json" in chunks:
        chunks = copy.deepcopy(chunks["json"])
        output_type = "json"
    elif "markdown" in chunks:
        chunks = [{"text": [chunks["markdown"]]}] if chunks["markdown"] else []
        output_type = "markdown"
    elif "text" in chunks:
        chunks = [{"text": [chunks["text"]]}] if chunks["text"] else []
        output_type = "text"
    elif "html" in chunks:
        chunks = [{"text": [chunks["html"]]}] if chunks["html"] else []
        output_type = "html"
    else:
        chunks = []
        output_type = "empty"

    get_recording_context().record("pipeline_output_type", output_type)
    get_recording_context().record("pipeline_output_count", len(chunks))

    # An empty normalized payload means "nothing parsed", so stop before embedding/indexing.
    if not chunks:
        ret = PipelineOperationLogService.create(document_id=doc_id, pipeline_id=dataflow_id,
                                           task_type=PipelineTaskType.PARSE, dsl=str(pipeline))
        get_recording_context().save_func_return_value("PipelineOperationLogService.create", ret)
        return

    keys = [k for o in chunks for k in list(o.keys())]
    if not any([re.match(r"q_[0-9]+_vec", k) for k in keys]):
        try:
            set_progress(task_id, prog=0.82, msg="\n-------------------------------------\nStart to embedding...")
            e, kb = KnowledgebaseService.get_by_id(task["kb_id"])
            embedding_id = kb.embd_id
            embd_model_config = get_model_config_from_provider_instance(task["tenant_id"], LLMType.EMBEDDING, embedding_id)
            embedding_model = LLMBundle(task["tenant_id"], embd_model_config)

            @timeout(60)
            def batch_encode(txts):
                nonlocal embedding_model
                return embedding_model.encode([truncate(c, embedding_model.max_length - 10) for c in txts])

            vects_batches = []
            texts = [o.get("questions", o.get("summary", o["text"])) for o in chunks]
            delta = 0.20 / (len(texts) // settings.EMBEDDING_BATCH_SIZE + 1)
            prog = 0.8
            for i in range(0, len(texts), settings.EMBEDDING_BATCH_SIZE):
                async with embed_limiter:
                    vts, c = await thread_pool_exec(batch_encode, texts[i: i + settings.EMBEDDING_BATCH_SIZE])
                vects_batches.append(vts)
                embedding_token_consumption += c
                prog += delta
                if i % (len(texts) // settings.EMBEDDING_BATCH_SIZE / 100 + 1) == 1:
                    set_progress(task_id, prog=prog, msg=f"{i + 1} / {len(texts) // settings.EMBEDDING_BATCH_SIZE}")
            vects = np.vstack(vects_batches) if vects_batches else np.array([])
            get_recording_context().record("embedding_token_consumption", embedding_token_consumption)
            get_recording_context().record("vector_size", len(vects[0]) if len(vects) > 0 else 0)

            assert len(vects) == len(chunks)
            for i, ck in enumerate(chunks):
                v = vects[i].tolist()
                ck["q_%d_vec" % len(v)] = v
        except TaskCanceledException:
            raise
        except Exception as e:
            set_progress(task_id, prog=-1, msg=f"[ERROR]: {e}")
            ret = PipelineOperationLogService.create(document_id=doc_id, pipeline_id=dataflow_id,
                                               task_type=PipelineTaskType.PARSE, dsl=str(pipeline))
            get_recording_context().save_func_return_value("PipelineOperationLogService.create", ret)
            return

    metadata = {}
    for ck in chunks:
        ck["doc_id"] = doc_id
        ck["kb_id"] = [str(task["kb_id"])]
        ck["docnm_kwd"] = task["name"]
        ck["create_time"] = str(datetime.now()).replace("T", " ")[:19]
        ck["create_timestamp_flt"] = datetime.now().timestamp()
        if not ck.get("id"):
            ck["id"] = xxhash.xxh64((ck["text"] + str(ck["doc_id"])).encode("utf-8")).hexdigest()
        if "questions" in ck:
            if "question_tks" not in ck:
                ck["question_kwd"] = ck["questions"].split("\n")
                ck["question_tks"] = rag_tokenizer.tokenize(str(ck["questions"]))
            del ck["questions"]
        if "keywords" in ck:
            if "important_tks" not in ck:
                ck["important_kwd"] = [k for k in re.split(r"[,，;；、\r\n]+", ck["keywords"]) if k.strip()]
                ck["important_tks"] = rag_tokenizer.tokenize(str(ck["keywords"]))
            del ck["keywords"]
        if "summary" in ck:
            if "content_ltks" not in ck:
                ck["content_ltks"] = rag_tokenizer.tokenize(str(ck["summary"]))
                ck["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(ck["content_ltks"])
            del ck["summary"]
        if "metadata" in ck:
            metadata = update_metadata_to(metadata, ck["metadata"])
            del ck["metadata"]
        if "content_with_weight" not in ck:
            ck["content_with_weight"] = ck["text"]
        del ck["text"]
        if "positions" in ck:
            add_positions(ck, ck["positions"])
            del ck["positions"]

    if metadata:
        existing_meta = DocMetadataService.get_document_metadata(doc_id)
        existing_meta = existing_meta if isinstance(existing_meta, dict) else {}
        metadata = update_metadata_to(metadata, existing_meta)
        get_recording_context().record("run_dataflow_metadata", metadata)
        ret = DocMetadataService.update_document_metadata(doc_id, metadata)
        get_recording_context().save_func_return_value("DocMetadataService.update_document_metadata", ret)

    start_ts = timer()
    set_progress(task_id, prog=0.82, msg="[DOC Engine]:\nStart to index...")
    e = await insert_chunks(task_id, task["tenant_id"], task["kb_id"], chunks, partial(set_progress, task_id, 0, 100000000))
    if not e:
        ret = PipelineOperationLogService.create(document_id=doc_id, pipeline_id=dataflow_id,
                                           task_type=PipelineTaskType.PARSE, dsl=str(pipeline))
        get_recording_context().save_func_return_value("PipelineOperationLogService.create", ret)
        return

    time_cost = timer() - start_ts
    task_time_cost = timer() - task_start_ts
    set_progress(task_id, prog=1., msg="Indexing done ({:.2f}s). Task done ({:.2f}s)".format(time_cost, task_time_cost))
    ret = DocumentService.increment_chunk_num(doc_id, task_dataset_id, embedding_token_consumption, len(chunks),
                                        task_time_cost)
    get_recording_context().save_func_return_value("DocumentService.increment_chunk_num", ret)
    logging.info("[Done], chunks({}), token({}), elapsed:{:.2f}".format(len(chunks), embedding_token_consumption,
                                                                        task_time_cost))
    get_recording_context().record("dataflow_chunks", chunks)
    ret = PipelineOperationLogService.create(document_id=doc_id, pipeline_id=dataflow_id, task_type=PipelineTaskType.PARSE,
                                       dsl=str(pipeline))
    get_recording_context().save_func_return_value("PipelineOperationLogService.create", ret)

RAPTOR_METHOD_SEARCH_LIMIT = 10000


async def get_raptor_chunk_field_map(doc_id: str, tenant_id: str, kb_id: str) -> dict:
    """Return stored RAPTOR marker fields for a document."""
    from common.doc_store.doc_store_base import OrderByExpr
    from rag.nlp import search as nlp_search

    async def search_fields(fields: list[str], condition: dict, order_by=None):
        """Search chunk fields in the current knowledge base."""
        res = await thread_pool_exec(
            settings.docStoreConn.search,
            fields, [], condition, [], order_by or OrderByExpr(),
            0, RAPTOR_METHOD_SEARCH_LIMIT, nlp_search.index_name(tenant_id), [kb_id]
        )
        return settings.docStoreConn.get_fields(res, fields)

    primary = await search_fields(["raptor_kwd", "extra"], {"doc_id": doc_id, "raptor_kwd": ["raptor"]})
    if collect_raptor_chunk_ids(primary):
        return primary

    try:
        return await search_fields(
            ["raptor_kwd", "extra"],
            {"doc_id": doc_id},
            OrderByExpr().desc("create_timestamp_flt"),
        )
    except Exception:
        logging.debug("RAPTOR fallback method lookup with extra field failed for doc %s", doc_id, exc_info=True)
        return primary


async def get_raptor_chunk_methods(doc_id: str, tenant_id: str, kb_id: str) -> set[str]:
    """Return the RAPTOR tree builders already stored for doc_id.

    Queries directly for raptor_kwd="raptor" rows so a non-RAPTOR leading
    chunk cannot produce a false-negative result. Legacy summary chunks that
    do not have method metadata are treated as the original RAPTOR builder.
    """
    try:
        field_map = await get_raptor_chunk_field_map(doc_id, tenant_id, kb_id)
        methods = collect_raptor_methods(field_map)
        if methods:
            logging.info(
                "Checkpoint hit: RAPTOR chunks for doc %s (tenant=%s kb=%s methods=%s) already exist",
                doc_id, tenant_id, kb_id, sorted(methods),
            )
        else:
            logging.info(
                "Checkpoint miss: no RAPTOR chunks for doc %s (tenant=%s kb=%s)",
                doc_id, tenant_id, kb_id,
            )
        return methods
    except Exception:
        logging.exception("Failed to check RAPTOR chunks for doc %s", doc_id)
        raise


async def has_raptor_chunks(doc_id: str, tenant_id: str, kb_id: str, tree_builder: str = RAPTOR_TREE_BUILDER) -> bool:
    """Return whether doc_id already has summaries for tree_builder."""
    methods = await get_raptor_chunk_methods(doc_id, tenant_id, kb_id)
    return tree_builder in methods


async def delete_raptor_chunks(doc_id: str, tenant_id: str, kb_id: str, keep_method: str | None = None):
    """Delete RAPTOR summaries for doc_id, optionally preserving one method."""
    if keep_method is None:
        logging.info(
            "delete_raptor_chunks: removing all RAPTOR summaries (doc=%s tenant=%s kb=%s)",
            doc_id, tenant_id, kb_id,
        )
        ret = await thread_pool_exec(
            settings.docStoreConn.delete,
            {"doc_id": doc_id, "raptor_kwd": ["raptor"]},
            nlp_search.index_name(tenant_id),
            kb_id,
        )
        get_recording_context().save_func_return_value("docStoreConn.delete", ret)
        return 0

    field_map = await get_raptor_chunk_field_map(doc_id, tenant_id, kb_id)
    chunk_ids = collect_raptor_chunk_ids(field_map, exclude_methods={keep_method})
    if not chunk_ids:
        logging.debug(
            "delete_raptor_chunks: no stale RAPTOR chunks to remove (doc=%s tenant=%s kb=%s keep=%s)",
            doc_id, tenant_id, kb_id, keep_method,
        )
        return 0

    logging.info(
        "delete_raptor_chunks: removing %d stale RAPTOR chunks (doc=%s tenant=%s kb=%s keep=%s)",
        len(chunk_ids), doc_id, tenant_id, kb_id, keep_method,
    )
    ret = await thread_pool_exec(
        settings.docStoreConn.delete,
        {"id": list(chunk_ids)},
        nlp_search.index_name(tenant_id),
        kb_id,
    )
    get_recording_context().save_func_return_value("docStoreConn.delete", ret)
    return len(chunk_ids)


@timeout(3600)
async def run_raptor_for_kb(row, kb_parser_config, chat_mdl, embd_mdl, vector_size, callback=None, doc_ids=[]):
    """Generate RAPTOR summaries for selected documents in a knowledge base."""
    fake_doc_id = GRAPH_RAPTOR_FAKE_DOC_ID

    raptor_config = kb_parser_config.get("raptor", {})
    raptor_ext_config = raptor_config.get("ext") or {}
    tree_builder = get_raptor_tree_builder(raptor_config)
    clustering_method = get_raptor_clustering_method(raptor_config)
    vctr_nm = "q_%d_vec" % vector_size

    res = []
    tk_count = 0
    cleanup_raptor_chunks = []
    max_errors = int(os.environ.get("RAPTOR_MAX_ERRORS", 3))
    doc_info_by_id = {}
    for doc_id in set(doc_ids):
        ok, source_doc = DocumentService.get_by_id(doc_id)
        if not ok or not source_doc:
            continue
        doc_info_by_id[doc_id] = {
            "name": getattr(source_doc, "name", ""),
            "type": getattr(source_doc, "type", ""),
            "parser_id": getattr(source_doc, "parser_id", ""),
            "parser_config": getattr(source_doc, "parser_config", {}) or {},
        }

    def schedule_raptor_cleanup(doc_id: str, keep_method: str | None = None):
        """Queue stale RAPTOR summaries for deletion after successful insert."""
        cleanup_plan = (doc_id, keep_method)
        if cleanup_plan not in cleanup_raptor_chunks:
            cleanup_raptor_chunks.append(cleanup_plan)

    def skip_raptor_doc(doc_id: str) -> bool:
        """Return whether RAPTOR should be skipped for this source document."""
        doc_info = doc_info_by_id.get(doc_id, {})
        file_type = doc_info.get("type") or row.get("type", "")
        parser_id = doc_info.get("parser_id") or row.get("parser_id", "")
        parser_config = doc_info.get("parser_config") or row.get("parser_config", {})
        if should_skip_raptor(file_type, parser_id, parser_config, raptor_config):
            skip_reason = get_skip_reason(file_type, parser_id, parser_config)
            doc_name = doc_info.get("name") or doc_id
            logging.info("Skipping Raptor for document %s: %s", doc_name, skip_reason)
            callback(msg=f"[RAPTOR] doc:{doc_id} skipped: {skip_reason}")
            return True
        return False

    async def generate(chunks, did):
        """Run RAPTOR and append generated summary chunks for one doc id."""
        nonlocal tk_count, res
        logging.info("RAPTOR: using tree_builder=%s clustering_method=%s for doc %s", tree_builder, clustering_method, did)
        from rag.raptor import RecursiveAbstractiveProcessing4TreeOrganizedRetrieval as Raptor  # Lazy load, save around 8s
        raptor = Raptor(
            raptor_config.get("max_cluster", 64),
            chat_mdl,
            embd_mdl,
            raptor_config["prompt"],
            raptor_config["max_token"],
            raptor_config["threshold"],
            max_errors=max_errors,
            tree_builder=tree_builder,
            clustering_method=clustering_method,
            psi_exact_max_leaves=raptor_ext_config.get("psi_exact_max_leaves", 4096),
            psi_bucket_size=raptor_ext_config.get("psi_bucket_size", 1024),
        )
        original_length = len(chunks)
        chunks, layers = await raptor(chunks, kb_parser_config["raptor"]["random_seed"], callback, row["id"])
        effective_doc_name = row["name"] if did == fake_doc_id else doc_info_by_id.get(did, {}).get("name") or row["name"]
        doc = {
            "doc_id": did,
            "kb_id": [str(row["kb_id"])],
            "docnm_kwd": effective_doc_name,
            "title_tks": rag_tokenizer.tokenize(effective_doc_name),
            "raptor_kwd": "raptor",
            "extra": {"raptor_method": tree_builder},
        }
        if row["pagerank"]:
            doc[PAGERANK_FLD] = int(row["pagerank"])

        # Build index→layer mapping from RAPTOR layer boundaries.
        # layers is [(start, end), ...] where layer 0 is the original chunks
        # and layer 1+ are summary layers. We skip layer 0 (original chunks).
        chunk_layer = {}
        for layer_idx, (layer_start, layer_end) in enumerate(layers):
            if layer_idx == 0:
                continue  # layer 0 = original input chunks, not summaries
            for ci in range(layer_start, layer_end):
                chunk_layer[ci] = layer_idx

        for idx, (content, vctr) in enumerate(chunks[original_length:], start=original_length):
            d = copy.deepcopy(doc)
            d["id"] = make_raptor_summary_chunk_id(content, did)
            d["create_time"] = str(datetime.now()).replace("T", " ")[:19]
            d["create_timestamp_flt"] = datetime.now().timestamp()
            d[vctr_nm] = vctr.tolist()
            d["content_with_weight"] = content
            d["content_ltks"] = rag_tokenizer.tokenize(content)
            d["content_sm_ltks"] = rag_tokenizer.fine_grained_tokenize(d["content_ltks"])
            d["raptor_layer_int"] = chunk_layer.get(idx, 1)
            res.append(d)
            tk_count += num_tokens_from_string(content)

    if raptor_config.get("scope", "file") == "file":
        dataset_methods = await get_raptor_chunk_methods(fake_doc_id, row["tenant_id"], row["kb_id"])
        remove_dataset_summaries = bool(dataset_methods)
        has_file_level_target = False
        if dataset_methods:
            callback(msg="[RAPTOR] will remove dataset-level summaries after file-level summaries are available.")

        for x, doc_id in enumerate(doc_ids):
            if skip_raptor_doc(doc_id):
                callback(prog=(x + 1.) / len(doc_ids))
                continue
            # CHECKPOINT: skip docs that already have RAPTOR chunks in the doc store
            existing_methods = await get_raptor_chunk_methods(doc_id, row["tenant_id"], row["kb_id"])
            if tree_builder in existing_methods:
                has_file_level_target = True
                if existing_methods != {tree_builder}:
                    schedule_raptor_cleanup(doc_id, tree_builder)
                    callback(msg=f"[RAPTOR] doc:{doc_id} will remove old RAPTOR summaries after insert.")
                callback(msg=f"[RAPTOR] doc:{doc_id} already has {tree_builder} RAPTOR chunks, skipping.")
                callback(prog=(x + 1.) / len(doc_ids))
                continue
            if existing_methods:
                callback(msg=f"[RAPTOR] doc:{doc_id} will migrate RAPTOR summaries to {tree_builder} after insert.")

            chunks = []
            skipped_chunks = 0
            for d in settings.retriever.chunk_list(doc_id, row["tenant_id"], [str(row["kb_id"])],
                                                   fields=["content_with_weight", vctr_nm],
                                                   sort_by_position=True):
                # Skip chunks that don't have the required vector field (may have been indexed with different embedding model)
                if vctr_nm not in d or d[vctr_nm] is None:
                    skipped_chunks += 1
                    logging.warning(f"RAPTOR: Chunk missing vector field '{vctr_nm}' in doc {doc_id}, skipping")
                    continue
                chunks.append((d["content_with_weight"], np.array(d[vctr_nm])))

            if skipped_chunks > 0:
                callback(msg=f"[WARN] Skipped {skipped_chunks} chunks without vector field '{vctr_nm}' for doc {doc_id}. Consider re-parsing the document with the current embedding model.")

            if not chunks:
                logging.warning(f"RAPTOR: No valid chunks with vectors found for doc {doc_id}")
                callback(msg=f"[WARN] No valid chunks with vectors found for doc {doc_id}, skipping")
                continue

            before_generate = len(res)
            await generate(chunks, doc_id)
            if len(res) > before_generate:
                has_file_level_target = True
                if existing_methods:
                    schedule_raptor_cleanup(doc_id, tree_builder)
            callback(prog=(x + 1.) / len(doc_ids))

        if remove_dataset_summaries:
            if has_file_level_target:
                schedule_raptor_cleanup(fake_doc_id)
            else:
                callback(msg="[RAPTOR] kept dataset-level summaries because no file-level summaries were built.")
    else:
        migrated_file_docs = 0
        file_cleanup_doc_ids = []
        skipped_doc_ids = set()
        for doc_id in set(doc_ids):
            if skip_raptor_doc(doc_id):
                skipped_doc_ids.add(doc_id)
                continue
            existing_methods = await get_raptor_chunk_methods(doc_id, row["tenant_id"], row["kb_id"])
            if existing_methods:
                file_cleanup_doc_ids.append(doc_id)
                migrated_file_docs += 1
        if migrated_file_docs:
            callback(msg=f"[RAPTOR] will remove file-level summaries for {migrated_file_docs} docs after dataset-level build succeeds.")

        existing_methods = await get_raptor_chunk_methods(fake_doc_id, row["tenant_id"], row["kb_id"])
        if tree_builder in existing_methods:
            if existing_methods != {tree_builder}:
                schedule_raptor_cleanup(fake_doc_id, tree_builder)
                callback(msg="[RAPTOR] will remove old dataset-level RAPTOR summaries after insert.")
            for doc_id in file_cleanup_doc_ids:
                schedule_raptor_cleanup(doc_id)
            callback(msg=f"[RAPTOR] dataset-level {tree_builder} summaries already exist, skipping.")
            return res, tk_count, cleanup_raptor_chunks
        migrate_dataset_summaries = bool(existing_methods)
        if migrate_dataset_summaries:
            callback(msg=f"[RAPTOR] will migrate dataset-level RAPTOR summaries to {tree_builder} after insert.")

        chunks = []
        skipped_chunks = 0
        for doc_id in doc_ids:
            if doc_id in skipped_doc_ids:
                continue
            for d in settings.retriever.chunk_list(doc_id, row["tenant_id"], [str(row["kb_id"])],
                                                   fields=["content_with_weight", vctr_nm],
                                                   sort_by_position=True):
                # Skip chunks that don't have the required vector field
                if vctr_nm not in d or d[vctr_nm] is None:
                    skipped_chunks += 1
                    logging.warning(f"RAPTOR: Chunk missing vector field '{vctr_nm}' in doc {doc_id}, skipping")
                    continue
                chunks.append((d["content_with_weight"], np.array(d[vctr_nm])))

        if skipped_chunks > 0:
            callback(msg=f"[WARN] Skipped {skipped_chunks} chunks without vector field '{vctr_nm}'. Consider re-parsing documents with the current embedding model.")

        if not chunks:
            if skipped_doc_ids and len(skipped_doc_ids) == len(set(doc_ids)):
                callback(msg="[RAPTOR] all documents were skipped by RAPTOR auto-disable rules.")
                return res, tk_count, cleanup_raptor_chunks
            logging.error(f"RAPTOR: No valid chunks with vectors found in any document for kb {row['kb_id']}")
            callback(msg=f"[ERROR] No valid chunks with vectors found. Please ensure documents are parsed with the current embedding model (vector size: {vector_size}).")
            return res, tk_count, cleanup_raptor_chunks

        before_generate = len(res)
        await generate(chunks, fake_doc_id)
        if len(res) > before_generate:
            for doc_id in file_cleanup_doc_ids:
                schedule_raptor_cleanup(doc_id)
            if migrate_dataset_summaries:
                schedule_raptor_cleanup(fake_doc_id, tree_builder)

    return res, tk_count, cleanup_raptor_chunks


async def delete_image(kb_id, chunk_id):
    try:
        async with minio_limiter:
            settings.STORAGE_IMPL.delete(kb_id, chunk_id)
    except Exception:
        logging.exception(f"Deleting image of chunk {chunk_id} got exception")
        raise


@timed_with_recording
async def insert_chunks(task_id, task_tenant_id, task_dataset_id, chunks, progress_callback):
    """
    Insert chunks into document store (Elasticsearch OR Infinity).

    Args:
        task_id: Task identifier
        task_tenant_id: Tenant ID
        task_dataset_id: Dataset/knowledge base ID
        chunks: List of chunk dictionaries to insert
        progress_callback: Callback function for progress updates
    """
    mothers = []
    mother_ids = set([])
    for ck in chunks:
        mom = ck.get("mom") or ck.get("mom_with_weight") or ""
        if not mom:
            continue
        id = xxhash.xxh64(mom.encode("utf-8")).hexdigest()
        ck["mom_id"] = id
        if id in mother_ids:
            continue
        mother_ids.add(id)
        mom_ck = copy.deepcopy(ck)
        mom_ck["id"] = id
        mom_ck["content_with_weight"] = mom
        mom_ck["available_int"] = 0
        flds = list(mom_ck.keys())
        for fld in flds:
            if fld not in ["id", "content_with_weight", "doc_id", "docnm_kwd", "kb_id", "available_int",
                           "position_int", "create_timestamp_flt", "page_num_int", "top_int"]:
                del mom_ck[fld]
        mothers.append(mom_ck)

    for b in range(0, len(mothers), settings.DOC_BULK_SIZE):
        ret = await thread_pool_exec(settings.docStoreConn.insert, mothers[b:b + settings.DOC_BULK_SIZE],
                                search.index_name(task_tenant_id), task_dataset_id, )
        get_recording_context().save_func_return_value("docStoreConn.insert", ret)
        task_canceled = has_canceled(task_id)
        if task_canceled:
            progress_callback(-1, msg="Task has been canceled.")
            return False

    for b in range(0, len(chunks), settings.DOC_BULK_SIZE):
        doc_store_result = await thread_pool_exec(settings.docStoreConn.insert, chunks[b:b + settings.DOC_BULK_SIZE],
                                                   search.index_name(task_tenant_id), task_dataset_id, )
        get_recording_context().save_func_return_value("docStoreConn.insert", doc_store_result)
        task_canceled = has_canceled(task_id)
        if task_canceled:
            # Roll back partial RAPTOR summary inserts so the next run is not
            # mistaken for a completed checkpoint by get_raptor_chunk_methods.
            raptor_ids_to_rollback = [
                c["id"] for c in chunks[:b + settings.DOC_BULK_SIZE]
                if c.get("raptor_kwd") == "raptor"
            ]
            if raptor_ids_to_rollback:
                try:
                    ret = await thread_pool_exec(
                        settings.docStoreConn.delete,
                        {"id": raptor_ids_to_rollback},
                        search.index_name(task_tenant_id),
                        task_dataset_id,
                    )
                    get_recording_context().save_func_return_value("docStoreConn.delete", ret)
                    logging.info(
                        "insert_chunks: rolled back %d partial RAPTOR chunks after cancellation (task=%s)",
                        len(raptor_ids_to_rollback), task_id,
                    )
                except Exception:
                    logging.exception(
                        "insert_chunks: failed to roll back partial RAPTOR chunks after cancellation (task=%s)",
                        task_id,
                    )
            progress_callback(-1, msg="Task has been canceled.")
            return False
        if b % 128 == 0:
            progress_callback(prog=0.8 + 0.1 * (b + 1) / len(chunks), msg="")
        if doc_store_result:
            error_message = f"Insert chunk error: {doc_store_result}, please check log file and Elasticsearch/Infinity status!"
            progress_callback(-1, msg=error_message)
            raise Exception(error_message)
        chunk_ids = [chunk["id"] for chunk in chunks[:b + settings.DOC_BULK_SIZE]]
        chunk_ids_str = " ".join(chunk_ids)
        try:
            TaskService.update_chunk_ids(task_id, chunk_ids_str)
            get_recording_context().save_func_return_value("TaskService.update_chunk_ids", None)
        except DoesNotExist:
            logging.warning(f"do_handle_task update_chunk_ids failed since task {task_id} is unknown.")
            doc_store_result = await thread_pool_exec(settings.docStoreConn.delete, {"id": chunk_ids},
                                                       search.index_name(task_tenant_id), task_dataset_id, )
            get_recording_context().save_func_return_value("docStoreConn.delete", doc_store_result)
            tasks = []
            for chunk_id in chunk_ids:
                tasks.append(asyncio.create_task(delete_image(task_dataset_id, chunk_id)))
            try:
                await asyncio.gather(*tasks, return_exceptions=False)
            except Exception as e:
                logging.error(f"delete_image failed: {e}")
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            progress_callback(-1, msg=f"Chunk updates failed since task {task_id} is unknown.")
            return False
    return True


@timeout(None)
async def do_handle_task(task):
    task_type = task.get("task_type", "")

    if task_type == "memory":
        result = await handle_save_to_memory_task(task)
        get_recording_context().save_func_return_value("handle_save_to_memory_task", result)
        return

    if task_type == "dataflow" and task.get("doc_id", "") == CANVAS_DEBUG_DOC_ID:
        await run_dataflow(task)
        return

    task_id = task["id"]
    task_from_page = task["from_page"]
    task_to_page = task["to_page"]
    task_tenant_id = task["tenant_id"]
    task_embedding_id = task["embd_id"]
    task_language = task.get("language") or "Chinese"
    if not task.get("language"):
        logging.warning("Task %s has no language set, falling back to Chinese", task_id)
    doc_task_llm_id = task["parser_config"].get("llm_id") or task["llm_id"]
    kb_task_llm_id = task['kb_parser_config'].get("llm_id") or task["llm_id"]
    task['llm_id'] = kb_task_llm_id
    task_dataset_id = task["kb_id"]
    task_doc_id = task["doc_id"]
    task_document_name = task["name"]
    task_parser_config = task["parser_config"]
    task_start_ts = timer()
    toc_thread = None
    raptor_cleanup_chunks = []

    # prepare the progress callback function
    progress_callback = partial(set_progress, task_id, task_from_page, task_to_page)

    task_canceled = has_canceled(task_id)
    if task_canceled:
        progress_callback(-1, msg="Task has been canceled.")
        return

    try:
        # bind embedding model
        if task_embedding_id:
            embd_model_config = get_model_config_from_provider_instance(task_tenant_id, LLMType.EMBEDDING, task_embedding_id)
        else:
            embd_model_config = get_tenant_default_model_by_type(task_tenant_id, LLMType.EMBEDDING)
        embedding_model = LLMBundle(task_tenant_id, embd_model_config, lang=task_language)
        vts, _ = embedding_model.encode(["ok"])
        vector_size = len(vts[0])
    except Exception as e:
        error_message = f'Fail to bind embedding model: {str(e)}'
        progress_callback(-1, msg=error_message)
        logging.exception(error_message)
        raise

    init_kb(task, vector_size)

    if task_type[:len("dataflow")] == "dataflow":
        await run_dataflow(task)
        return

    if task_type == "raptor":
        ok, kb = KnowledgebaseService.get_by_id(task_dataset_id)
        if not ok:
            progress_callback(prog=-1.0, msg="Cannot found valid dataset for RAPTOR task")
            return

        kb_parser_config = kb.parser_config
        if not kb_parser_config.get("raptor", {}).get("use_raptor", False):
            kb_parser_config.update(
                {
                    "raptor": {
                        "use_raptor": True,
                        "prompt": "Please summarize the following paragraphs. Be careful with the numbers, do not make things up. Paragraphs as following:\n      {cluster_content}\nThe above is the content you need to summarize.",
                        "max_token": 256,
                        "threshold": 0.1,
                        "max_cluster": 64,
                        "random_seed": 0,
                        "scope": "file",
                        "clustering_method": "gmm",
                        "tree_builder": "raptor",
                    },
                }
            )
            update_result = KnowledgebaseService.update_by_id(kb.id, {"parser_config": kb_parser_config})
            get_recording_context().save_func_return_value("KnowledgebaseService.update_by_id", update_result)
            if not update_result:
                progress_callback(prog=-1.0, msg="Internal error: Invalid RAPTOR configuration")
                return

        # bind LLM for raptor
        chat_model_config = get_model_config_from_provider_instance(task_tenant_id, LLMType.CHAT, kb_task_llm_id)
        chat_model = LLMBundle(task_tenant_id, chat_model_config, lang=task_language)
        # run RAPTOR
        async with kg_limiter:
            try:
                chunks, token_count, raptor_cleanup_chunks = await run_raptor_for_kb(
                    row=task,
                    kb_parser_config=kb_parser_config,
                    chat_mdl=chat_model,
                    embd_mdl=embedding_model,
                    vector_size=vector_size,
                    callback=progress_callback,
                    doc_ids=task.get("doc_ids", []),
                )
                if isinstance(kg_limiter, AdaptiveConcurrencyLimiter):
                    kg_limiter.record_event_sync("success")
            except Exception as exc:
                if isinstance(kg_limiter, AdaptiveConcurrencyLimiter):
                    err_str = str(exc).lower()
                    if any(k in err_str for k in ("rate limit", "429", "tpm limit", "too many requests", "requests per minute")):
                        kg_limiter.record_event_sync("llm_rate_limit")
                raise
        get_recording_context().record("raptor_chunks", chunks)
        get_recording_context().record("raptor_token_count", token_count)
        if fake_doc_ids := task.get("doc_ids", []):
            task_doc_id = fake_doc_ids[0]  # use the first document ID to represent this task for logging purposes
    # Either using graphrag or Standard chunking methods
    elif task_type == "graphrag":
        ok, kb = KnowledgebaseService.get_by_id(task_dataset_id)
        if not ok:
            progress_callback(prog=-1.0, msg="Cannot found valid dataset for GraphRAG task")
            return

        kb_parser_config = kb.parser_config
        if not kb_parser_config.get("graphrag", {}).get("use_graphrag", False):
            kb_parser_config.update(
                {
                    "graphrag": {
                        "use_graphrag": True,
                        "entity_types": [
                            "organization",
                            "person",
                            "geo",
                            "event",
                            "category",
                        ],
                        "method": "light",
                        "batch_chunk_token_size": 4096,
                        "retry_attempts": 2,
                        "retry_backoff_seconds": 2.0,
                        "retry_backoff_max_seconds": 60.0,
                        "build_subgraph_timeout_per_chunk_seconds": 300,
                        "build_subgraph_min_timeout_seconds": 600,
                        "merge_timeout_seconds": 180,
                        "resolution_timeout_seconds": 1800,
                        "community_timeout_seconds": 1800,
                        "lock_acquire_timeout_seconds": 600,
                    }
                }
            )
            update_result = KnowledgebaseService.update_by_id(kb.id, {"parser_config": kb_parser_config})
            get_recording_context().save_func_return_value("KnowledgebaseService.update_by_id", update_result)
            if not update_result:
                progress_callback(prog=-1.0, msg="Internal error: Invalid GraphRAG configuration")
                return

        graphrag_conf = kb_parser_config.get("graphrag", {})
        start_ts = timer()
        chat_model_config = get_model_config_from_provider_instance(task_tenant_id, LLMType.CHAT, kb_task_llm_id)
        chat_model = LLMBundle(task_tenant_id, chat_model_config, lang=task_language)
        with_resolution = graphrag_conf.get("resolution", False)
        with_community = graphrag_conf.get("community", False)
        async with kg_limiter:
            try:
                # await run_graphrag(task, task_language, with_resolution, with_community, chat_model, embedding_model, progress_callback)
                from rag.graphrag.general.index import run_graphrag_for_kb # Lazy load, save around 2s
                result = await run_graphrag_for_kb(
                    row=task,
                    doc_ids=task.get("doc_ids", []),
                    language=task_language,
                    kb_parser_config=kb_parser_config,
                    chat_model=chat_model,
                    embedding_model=embedding_model,
                    callback=progress_callback,
                    with_resolution=with_resolution,
                    with_community=with_community,
                )
                logging.info(f"GraphRAG task result for task {task}:\n{result}")
                if isinstance(kg_limiter, AdaptiveConcurrencyLimiter):
                    kg_limiter.record_event_sync("success")
            except Exception as exc:
                if isinstance(kg_limiter, AdaptiveConcurrencyLimiter):
                    err_str = str(exc).lower()
                    if any(k in err_str for k in ("rate limit", "429", "tpm limit", "too many requests", "requests per minute")):
                        kg_limiter.record_event_sync("llm_rate_limit")
                raise
        get_recording_context().record("graphrag_result", result)
        progress_callback(prog=1.0, msg="Knowledge Graph done ({:.2f}s)".format(timer() - start_ts))
        return
    elif task_type == "mindmap":
        progress_callback(1, "place holder")
        pass
        return
    else:
        # Standard chunking methods
        task['llm_id'] = doc_task_llm_id
        start_ts = timer()
        chunks = await build_chunks(task, progress_callback)
        get_recording_context().record("chunks", chunks)
        # Record chunk_ids_count for comparison
        chunk_ids = [c.get("id") for c in chunks if isinstance(c, dict) and "id" in c]
        get_recording_context().record("chunk_ids_count", len(chunk_ids))
        # Record chunks array for content comparison (first, middle, last, random)
        logging.info("Build document {}: {:.2f}s".format(task_document_name, timer() - start_ts))
        if not chunks:
            progress_callback(1., msg=f"No chunk built from {task_document_name}")
            return
        progress_callback(msg="Generate {} chunks".format(len(chunks)))
        start_ts = timer()
        try:
            token_count, vector_size = await embedding(chunks, embedding_model, task_parser_config, progress_callback)
        except TaskCanceledException:
            raise
        except Exception as e:
            error_message = "Generate embedding error:{}".format(str(e))
            progress_callback(-1, error_message)
            logging.exception(error_message)
            token_count = 0
            raise
        get_recording_context().record("token_count", token_count)
        get_recording_context().record("vector_size", vector_size)
        progress_message = "Embedding chunks ({:.2f}s)".format(timer() - start_ts)
        logging.info(progress_message)
        progress_callback(msg=progress_message)
        if task["parser_id"].lower() == "naive" and task["parser_config"].get("toc_extraction", False):
            toc_thread = asyncio.create_task(asyncio.to_thread(build_TOC, task, chunks, progress_callback))

    chunk_count = len(set([chunk["id"] for chunk in chunks]))
    start_ts = timer()

    async def _maybe_insert_chunks(_chunks):
        if has_canceled(task_id):
            progress_callback(-1, msg="Task has been canceled.")
            return False
        insert_result = await insert_chunks(task_id, task_tenant_id, task_dataset_id, _chunks, progress_callback)
        return bool(insert_result)

    try:
        if not await _maybe_insert_chunks(chunks):
            get_recording_context().record("insertion_result", "failed")
            return
        get_recording_context().record("insertion_result", "success")
        if has_canceled(task_id):
            progress_callback(-1, msg="Task has been canceled.")
            return

        if raptor_cleanup_chunks:
            cleaned_chunks = 0
            for cleanup_doc_id, keep_method in raptor_cleanup_chunks:
                ret = await delete_raptor_chunks(
                    cleanup_doc_id,
                    task_tenant_id,
                    task_dataset_id,
                    keep_method=keep_method,
                )
                cleaned_chunks += ret
                get_recording_context().save_func_return_value("delete_raptor_chunks", ret)

            if cleaned_chunks:
                progress_callback(msg=f"Cleaned up {cleaned_chunks} stale RAPTOR chunks.")

        logging.info(
            "Indexing doc({}), page({}-{}), chunks({}), elapsed: {:.2f}".format(
                task_document_name, task_from_page, task_to_page, len(chunks), timer() - start_ts
            )
        )

        ret = DocumentService.increment_chunk_num(task_doc_id, task_dataset_id, token_count, chunk_count, 0)
        get_recording_context().save_func_return_value("DocumentService.increment_chunk_num", ret)

        # Table parser (manual): push metadata/both column values to document-level metadata for UI / chat filters
        if task.get("parser_id", "").lower() == "table":
            eff_pc = merge_table_parser_config_from_kb(task)
            logging.debug(
                f"[TABLE_META_DEBUG] table post-index: table_column_mode={eff_pc.get('table_column_mode')!r}"
            )
            if eff_pc.get("table_column_mode") == "manual":
                try:
                    agg = aggregate_table_manual_doc_metadata(chunks, task)
                    logging.debug(f"[TABLE_META_DEBUG] aggregated metadata: {agg}")
                    strip_keys = table_parser_strip_doc_metadata_keys(eff_pc)
                    existing = DocMetadataService.get_document_metadata(task_doc_id)
                    existing = existing if isinstance(existing, dict) else {}
                    preserved = {k: v for k, v in existing.items() if k not in strip_keys}
                    merged = update_metadata_to(dict(preserved), agg)
                    logging.debug(
                        f"[TABLE_META_DEBUG] calling update_document_metadata for doc_id={task_doc_id}, "
                        f"meta_fields keys={list(merged.keys())}, "
                        f"table_strip_key_count={len(strip_keys)}, agg_keys={list(agg.keys())}"
                    )
                    try:
                        ret = DocMetadataService.update_document_metadata(task_doc_id, merged)
                        get_recording_context().save_func_return_value("DocMetadataService.update_document_metadata", ret)
                        logging.debug("[TABLE_META_DEBUG] update_document_metadata succeeded")
                    except Exception as ue:
                        logging.error(
                            "update_document_metadata failed (table parser, doc_id=%s): %s",
                            task_doc_id,
                            ue,
                            exc_info=True,
                        )
                except Exception as e:
                    logging.exception(
                        "Table parser document metadata aggregation failed (doc_id=%s): %s",
                        task_doc_id,
                        e,
                    )

        progress_callback(msg="Indexing done ({:.2f}s).".format(timer() - start_ts))

        if toc_thread:
            d = await toc_thread
            if d:
                get_recording_context().record("toc_chunk", [d])
                if not await _maybe_insert_chunks([d]):
                    get_recording_context().record("toc_inserted", False)
                    return
                get_recording_context().record("toc_inserted", True)
                ret = DocumentService.increment_chunk_num(task_doc_id, task_dataset_id, 0, 1, 0)
                get_recording_context().save_func_return_value("DocumentService.increment_chunk_num", ret)

        if has_canceled(task_id):
            progress_callback(-1, msg="Task has been canceled.")
            return

        task_time_cost = timer() - task_start_ts
        get_recording_context().record("task_status", "completed")
        progress_callback(prog=1.0, msg="Task done ({:.2f}s)".format(task_time_cost))
        logging.info(
            "Chunk doc({}), page({}-{}), chunks({}), token({}), elapsed:{:.2f}".format(
                task_document_name, task_from_page, task_to_page, len(chunks), token_count, task_time_cost
            )
        )

    finally:
        if toc_thread is not None and not toc_thread.done():
            toc_thread.cancel()
        if has_canceled(task_id):
            try:
                exists = await thread_pool_exec(
                    settings.docStoreConn.index_exist,
                    search.index_name(task_tenant_id),
                    task_dataset_id,
                )
                if exists:
                    ret = await thread_pool_exec(
                        settings.docStoreConn.delete,
                        {"doc_id": task_doc_id},
                        search.index_name(task_tenant_id),
                        task_dataset_id,
                    )
                    get_recording_context().save_func_return_value("docStoreConn.delete", ret)
            except Exception as e:
                logging.exception(
                    f"Remove doc({task_doc_id}) from docStore failed when task({task_id}) canceled, exception: {e}")


async def handle_task():
    global DONE_TASKS, FAILED_TASKS
    redis_msg, task = await collect()
    if not task:
        await asyncio.sleep(5)
        return

    task_type = task["task_type"]
    pipeline_task_type = TASK_TYPE_TO_PIPELINE_TASK_TYPE.get(task_type,
                                                             PipelineTaskType.PARSE) or PipelineTaskType.PARSE
    task_id = task["id"]
    try:
        CURRENT_TASKS[task["id"]] = copy.deepcopy(task)
        run_mode = os.environ.get("TE_RUN_MODE", "0")
        logging.info(f"TE_RUN_MODE is {run_mode}")

        # Check if dry-run comparison is enabled via environment variable
        if run_mode == "1": # dry run mode - compare
            set_recording_context(RecordingContext())
            await do_handle_task(task) # original execution
            # dry run mode
            logging.info(f"-----dry run task:{task_id}, {task.get('name', '')}, doc id:{task.get('doc_id', '')}")
            await TaskManager.dry_run_task(task, get_recording_context(), chat_limiter, minio_limiter, chunk_limiter,
                               embed_limiter,kg_limiter, set_progress, has_canceled)
        elif run_mode == "0": # use refactor-ed version
            # switch to refactor-ed version
            logging.info(f"-----run refactor-ed task executor:{task_id}, {task.get('name', '')}, doc id:{task.get('doc_id', '')}")
            set_recording_context(NullRecordingContext())
            await TaskManager.run_refactored_task(task, chat_limiter, minio_limiter, chunk_limiter,
                               embed_limiter,kg_limiter, set_progress, has_canceled)
        else: # original version
            logging.info(f"-----run original task executor:{task_id}, {task.get('name', '')}, doc id:{task.get('doc_id', '')}")
            set_recording_context(NullRecordingContext())
            await do_handle_task(task)

        DONE_TASKS += 1
        CURRENT_TASKS.pop(task_id, None)
        logging.info(f"handle_task done for task {json.dumps(task)}")
    except TaskCanceledException as e:
        DONE_TASKS += 1
        CURRENT_TASKS.pop(task_id, None)
        logging.info(
            f"handle_task canceled for task {task_id}: {getattr(e, 'msg', str(e))}"
        )
    except Exception as e:
        FAILED_TASKS += 1
        CURRENT_TASKS.pop(task_id, None)
        try:
            err_msg = str(e)
            while isinstance(e, exceptiongroup.ExceptionGroup):
                e = e.exceptions[0]
                err_msg += ' -- ' + str(e)
            set_progress(task_id, prog=-1, msg=f"[Exception]: {err_msg}")
        except Exception as e:
            logging.exception(f"[Exception]: {str(e)}")
            pass
        logging.exception(f"handle_task got exception for task {json.dumps(task)}")
    finally:
        try:
            if not task.get("dataflow_id", ""):
                referred_document_id = None
                if task_type in ["graphrag", "raptor", "mindmap"]:
                    doc_ids = task.get("doc_ids", [])
                    if doc_ids:
                        referred_document_id = doc_ids[0]
                ret = PipelineOperationLogService.record_pipeline_operation(document_id=task["doc_id"], pipeline_id="",
                                                                      task_type=pipeline_task_type,
                                                                      task_id=task_id, referred_document_id=referred_document_id)
                get_recording_context().save_func_return_value("PipelineOperationLogService.record_pipeline_operation", ret)
        except Exception:
            logging.exception(
                f"PipelineOperationLogService failed for task {task_id}, "
                "continuing to ack Redis message")
        try:
            redis_msg.ack()
        except Exception:
            logging.exception(f"Redis ack failed for task {task_id}")


async def get_server_ip() -> str:
    # get ip by udp
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception as e:
        logging.error(str(e))
        return 'Unknown'


async def _heartbeat_loop():
    """每 HEARTBEAT_INTERVAL 秒给 current_task_id 对应的 task 续期心跳锁。

    v2 reconcile 用 TTL 判定 task 真在跑 vs 真卡死,所以本协程必须每 <TTL 续期。
    与 report_status 一样用 while True + sleep 模式,靠 task.cancel() 终止。
    """
    interval = GraphRAGConfig.HEARTBEAT_INTERVAL
    ttl = GraphRAGConfig.HEARTBEAT_TTL
    value = f"{socket.gethostname()}:{os.getpid()}"
    while True:
        await asyncio.sleep(interval)
        # Phase 4.1: 线程安全读
        tid = _get_current_task_id()
        if not tid:
            continue  # 空闲 worker,跳过
        try:
            REDIS_CONN.set(f"graphrag:hb:{tid}", value, exp=ttl)
        except Exception:
            logging.exception("[heartbeat] 续期失败 task=%s", tid)
            # 不 break,继续尝试:短时网络抖动不应让心跳彻底停


async def report_status():
    """
    Periodically reports the executor's heartbeat
    """
    global PENDING_TASKS, LAG_TASKS, DONE_TASKS, FAILED_TASKS

    ip_address = await get_server_ip()
    pid = os.getpid()

    # Register the executor in Redis
    REDIS_CONN.sadd("TASKEXE", CONSUMER_NAME)
    redis_lock = RedisDistributedLock("clean_task_executor", lock_value=CONSUMER_NAME, timeout=60)

    while True:
        now = datetime.now()
        now_ts = now.timestamp()

        group_info = REDIS_CONN.queue_info(settings.get_svr_queue_name(0), SVR_CONSUMER_GROUP_NAME) or {}
        PENDING_TASKS = int(group_info.get("pending", 0))
        LAG_TASKS = int(group_info.get("lag", 0))

        current = copy.deepcopy(CURRENT_TASKS)
        heartbeat = json.dumps({
            "ip_address": ip_address,
            "pid": pid,
            "name": CONSUMER_NAME,
            "now": now.astimezone().isoformat(timespec="milliseconds"),
            "boot_at": BOOT_AT,
            "pending": PENDING_TASKS,
            "lag": LAG_TASKS,
            "done": DONE_TASKS,
            "failed": FAILED_TASKS,
            "current": current,
        })

        # Report heartbeat to Redis
        try:
            REDIS_CONN.zadd(CONSUMER_NAME, heartbeat, now_ts)
        except Exception as e:
            logging.warning(f"Failed to report heartbeat: {e}")
        else:
            logging.debug(f"{CONSUMER_NAME} reported heartbeat: {heartbeat}")
            pass

        # Clean up own expired heartbeat
        try:
            REDIS_CONN.zremrangebyscore(CONSUMER_NAME, 0, now_ts - 60 * 30)
        except Exception as e:
            logging.warning(f"Failed to clean heartbeat: {e}")

        # Clean other executors
        lock_acquired = False
        try:
            lock_acquired = redis_lock.acquire()
        except Exception as e:
            logging.warning(f"Failed to acquire Redis lock: {e}")
        if lock_acquired:
            try:
                task_executors = REDIS_CONN.smembers("TASKEXE") or set()
                for worker_name in task_executors:
                    if worker_name == CONSUMER_NAME:
                        continue
                    try:
                        last_heartbeat = REDIS_CONN.REDIS.zrevrange(worker_name, 0, 0, withscores=True)
                    except Exception as e:
                        logging.warning(f"Failed to read zset for {worker_name}: {e}")
                        continue

                    if not last_heartbeat or now_ts - last_heartbeat[0][1] > WORKER_HEARTBEAT_TIMEOUT:
                        logging.info(f"{worker_name} expired, removed")
                        REDIS_CONN.srem("TASKEXE", worker_name)
                        REDIS_CONN.delete(worker_name)
            except Exception as e:
                logging.warning(f"Failed to clean other executors: {e}")
            finally:
                redis_lock.release()
        await asyncio.sleep(30)


async def task_manager():
    try:
        await handle_task()
    finally:
        task_limiter.release()


async def kg_postprocess_consumer():
    """P5-T3: Background consumer for async resolution/community phases.

    Reads from the Redis Stream queue ``graphrag:postprocess`` and executes
    ``resolve_entities`` / ``extract_community`` with the same resource guards
    (``kg_limiter``) as the main GraphRAG pipeline.
    """
    if not GraphRAGConfig.USE_ASYNC_KG_PHASES:
        return

    queue_name = GraphRAGConfig.KG_POSTPROCESS_QUEUE
    group_name = SVR_CONSUMER_GROUP_NAME + "_kg_pp"
    consumer_name = CONSUMER_NAME + "_kg_pp"
    logging.info("[KG-PP] Consumer starting on %s (group=%s)", queue_name, group_name)

    while not stop_event.is_set():
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

            # Re-bind models (same pattern as the main pipeline)
            chat_model_config = get_model_config_by_type_and_name(tenant_id, LLMType.CHAT, kb_task_llm_id)
            chat_model = LLMBundle(tenant_id, chat_model_config, lang=task_language)
            embd_model_config = get_tenant_default_model_by_type(tenant_id, LLMType.EMBEDDING)
            embedding_model = LLMBundle(tenant_id, embd_model_config, lang=task_language)

            # Load KB parser_config for entity_types
            try:
                from api.db.services.knowledgebase_service import KnowledgebaseService
                kb = KnowledgebaseService.get_detail(kb_id)
                kb_parser_config = kb.get("parser_config", {}) if kb else {}
                graphrag_config = kb_parser_config.get("graphrag", {})
                entity_types = graphrag_config.get("entity_types", []) or []
            except Exception:
                logging.exception("[KG-PP] Failed to load KB parser_config for kb=%s", kb_id)
                entity_types = []

            # Load persisted graph
            from rag.graphrag.utils import get_graph
            final_graph = await get_graph(tenant_id, kb_id)
            if final_graph is None:
                logging.error("[KG-PP] kb=%s no persisted graph found, cannot proceed", kb_id)
                msg.ack()
                continue

            # Simple logging callback (no frontend progress bar for async jobs)
            def pp_callback(msg=None, prog=None):
                if msg:
                    logging.info("[KG-PP] kb=%s: %s", kb_id, msg)

            # Lock per-KB so only one postprocess runs at a time
            kb_lock = RedisDistributedLock(f"graphrag_task_{kb_id}", lock_value="postprocess", timeout=3600)
            await kb_lock.spin_acquire()
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
            logging.exception("[KG-PP] kb=%s postprocess failed", kb_id)
            # Do NOT ack – let the message remain pending for retry / dead-letter inspection


async def main():
    # Stagger executor startup to prevent connection storm to Infinity
    # Extract worker number from CONSUMER_NAME (e.g., "task_executor_abc123_5" -> 5)
    try:
        worker_num = int(CONSUMER_NAME.rsplit("_", 1)[-1])
        # Add random delay: base delay + worker_num * 2.0s + random jitter
        # This spreads out connection attempts over several seconds
        startup_delay = worker_num * 2.0 + random.uniform(0, 0.5)
        if startup_delay > 0:
            logging.info("Staggering startup by %.2fs to prevent connection storm", startup_delay)
            await asyncio.sleep(startup_delay)
    except (ValueError, IndexError):
        pass  # Non-standard consumer name, skip delay

    logging.info(r"""
    ____                      __  _
   /  _/___  ____ ____  _____/ /_(_)___  ____     ________  ______   _____  _____
   / // __ \/ __ `/ _ \/ ___/ __/ / __ \/ __ \   / ___/ _ \/ ___/ | / / _ \/ ___/
 _/ // / / / /_/ /  __(__  ) /_/ / /_/ / / / /  (__  )  __/ /   | |/ /  __/ /
/___/_/ /_/\__, /\___/____/\__/_/\____/_/ /_/  /____/\___/_/    |___/\___/_/
          /____/
    """)
    logging.info(f'RAGFlow ingestion version: {get_ragflow_version()}')
    logging.info(f"ENABLE_DRY_RUN_COMPARISON: {os.environ.get("ENABLE_DRY_RUN_COMPARISON", "0")}")
    show_configs()
    settings.init_settings()
    settings.check_and_install_torch()
    logging.info(f'default embedding config: {settings.EMBEDDING_CFG}')
    settings.print_rag_settings()
    if sys.platform != "win32":
        signal.signal(signal.SIGUSR1, start_tracemalloc_and_snapshot)
        signal.signal(signal.SIGUSR2, stop_tracemalloc)
    TRACE_MALLOC_ENABLED = int(os.environ.get('TRACE_MALLOC_ENABLED', "0"))
    if TRACE_MALLOC_ENABLED:
        start_tracemalloc_and_snapshot(None, None)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Boot-time reconciliation:把 OS 有数据、MySQL 进度卡住的 task 标完成
    # 阻塞启动而不是 create_task 是有意为之:扫描期间不消费新任务,避免与刚被恢复的 task 抢资源
    # Phase 4.2: 用 Redis SETNX 做 leader 选举，确保 N 个 worker 同时启动时只有
    # 1 个跑 reconcile，其他等它跑完（或等锁 TTL 过期）
    if TASK_TYPE == "common":
        _reconcile_leader_key = "graphrag:reconcile:leader"
        _reconcile_leader_ttl = 600  # 10 分钟，最坏情况覆盖一次完整扫描
        _leader_token = f"{socket.gethostname()}:{os.getpid()}:{time.time()}"
        _is_leader = False
        try:
            _is_leader = bool(
                REDIS_CONN.REDIS.set(_reconcile_leader_key, _leader_token, ex=_reconcile_leader_ttl, nx=True)
            )
        except Exception:
            logging.exception("reconcile leader 选举失败，回退到本地判断（所有 worker 都会跑）")
            _is_leader = True

        if _is_leader:
            try:
                await reconcile_stuck_graphrag_tasks()
            except Exception:
                logging.exception("reconcile_stuck_graphrag_tasks 未捕获异常")
            finally:
                try:
                    # 释放 leader 锁（用 token 防误删别人的锁）
                    if REDIS_CONN.REDIS is not None:
                        REDIS_CONN.REDIS.eval(
                            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
                            1,
                            _reconcile_leader_key,
                            _leader_token,
                        )
                except Exception:
                    logging.exception("reconcile leader 释放失败（依赖 TTL 自动过期）")
        else:
            logging.info("[reconcile] 非 leader worker，跳过本次 reconcile（等 leader 跑完或 TTL 过期）")
            # 非 leader 也阻塞一小段时间，等 leader 完成，避免和 leader 抢 task_manager 资源
            try:
                wait_deadline = time.time() + _reconcile_leader_ttl
                while time.time() < wait_deadline and not stop_event.is_set():
                    val = None
                    try:
                        val = REDIS_CONN.REDIS.get(_reconcile_leader_key) if REDIS_CONN.REDIS else None
                    except Exception:
                        pass
                    if val is None:
                        break  # leader 跑完了
                    await asyncio.sleep(2)
            except Exception:
                logging.exception("[reconcile] 等待 leader 完成时异常（不影响后续流程）")

    report_task = asyncio.create_task(report_status())
    kg_pp_task = asyncio.create_task(kg_postprocess_consumer())
    heartbeat_task = asyncio.create_task(_heartbeat_loop())
    tasks = []

    logging.info(f"RAGFlow ingestion is ready after {time.time() - start_ts}s initialization.")
    if isinstance(kg_limiter, AdaptiveConcurrencyLimiter):
        kg_limiter.start_monitoring()
    try:
        while not stop_event.is_set():
            await task_limiter.acquire()
            t = asyncio.create_task(task_manager())
            tasks.append(t)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        report_task.cancel()
        kg_pp_task.cancel()
        heartbeat_task.cancel()
        await asyncio.gather(report_task, kg_pp_task, heartbeat_task, return_exceptions=True)
    logging.error("BUG!!! You should not reach here!!!")


if __name__ == "__main__":
    # Parse command line arguments (consistent with SAAS version)
    parser = argparse.ArgumentParser(description='Task Executor')
    parser.add_argument("-i", "--index", type=str, default='0')
    parser.add_argument("-t", "--type", type=str, default="common", help="[common, graphrag, raptor, resume]")
    args = parser.parse_args()
    
    # Update global variables
    TASK_TYPE = args.type
    TE_IDX = args.index
    CONSUMER_NAME = f"task_executor_{TASK_TYPE}_{TE_IDX}"
    
    faulthandler.enable()
    init_root_logger(CONSUMER_NAME)
    try:
        asyncio.run(main())
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
        sys.exit(1)
