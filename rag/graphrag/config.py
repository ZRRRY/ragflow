# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""GraphRAG 功能开关与运行时配置。

所有布尔开关默认关闭（0），确保现有部署行为与官方 v0.26.0 完全一致。
可通过环境变量或 KB 级解析器配置逐步开启。

注意：所有开关在**导入时**从环境变量读取一次，模块导入后再修改环境变量
不会生效。需要热加载请使用 ``GraphRAGConfig.reload()``。
"""

import logging
import os

logger = logging.getLogger(__name__)


class GraphRAGConfig:
    """全局默认值（导入时读取一次）。"""

    # -----------------------------------------------------------------------------
    # Phase 1 – 增量图构建开关
    # -----------------------------------------------------------------------------
    # 图写入是否使用增量 delta 写入：1=增量，0=官方默认全量写入
    USE_INCREMENTAL_GRAPH = os.environ.get("USE_INCREMENTAL_GRAPH", "0") == "1"

    # -----------------------------------------------------------------------------
    # Phase 2 – 增量 Merge 开关
    # -----------------------------------------------------------------------------
    # Merge 阶段是否使用增量 merge：1=仅合并新增 subgraph，0=官方默认全图 merge
    USE_INCREMENTAL_MERGE = os.environ.get("USE_INCREMENTAL_MERGE", "0") == "1"
    # 文档删除时是否立即清理 subgraph：增量路径下自动开启，保持官方默认关闭
    DELETE_SUBGRAPH_ON_DOC_DELETE = USE_INCREMENTAL_GRAPH or USE_INCREMENTAL_MERGE

    # -----------------------------------------------------------------------------
    # Phase 2.5 – 增量合并后全局 PageRank 重算
    # -----------------------------------------------------------------------------
    # 增量 merge 全部完成后，是否加载一次全局图并重新计算 PageRank 写回索引。
    # 1=开启（merge 阶段末尾增加一次全图加载与全量 entity 更新）。
    # 0=关闭（保持现有行为：增量 merge 不更新全局 PageRank）。
    RECALC_GLOBAL_PAGERANK_AFTER_MERGE = os.environ.get(
        "RECALC_GLOBAL_PAGERANK_AFTER_MERGE", "0"
    ) == "1"

    # -----------------------------------------------------------------------------
    # Phase 3 – 增量实体消解开关
    # -----------------------------------------------------------------------------
    # 实体消解是否使用增量 resolution：1=按 entity_type 分批消解，0=官方默认全图消解
    USE_INCREMENTAL_RESOLUTION = os.environ.get("USE_INCREMENTAL_RESOLUTION", "0") == "1"

    # 增量消解内候选对召回方式：1=OpenSearch KNN，0=字符级过滤（无 embedding/ANN）
    # 仅在 USE_INCREMENTAL_RESOLUTION=1 时生效
    USE_KNN_FOR_RESOLUTION = os.environ.get("USE_KNN_FOR_RESOLUTION", "0") == "1"
    # KNN 召回 Top-K
    ENTITY_RESOLUTION_TOP_K = int(os.environ.get("ENTITY_RESOLUTION_TOP_K", "20"))
    # KNN 相似度阈值
    ENTITY_RESOLUTION_SIM_THRESHOLD = float(os.environ.get("ENTITY_RESOLUTION_SIM_THRESHOLD", "0.7"))
    # KNN 查询并发数
    ENTITY_RESOLUTION_KNN_CONCURRENCY = int(os.environ.get("ENTITY_RESOLUTION_KNN_CONCURRENCY", "8"))
    # 实体消解批大小
    RESOLUTION_BATCH_SIZE = int(os.environ.get("RESOLUTION_BATCH_SIZE", "100"))
    # 实体消解最大并发任务数
    RESOLUTION_MAX_CONCURRENT_TASKS = int(os.environ.get("RESOLUTION_MAX_CONCURRENT_TASKS", "5"))
    # 节点/边 embedding 批量调用大小（与 GRAPHRAG_INSERT_BULK_SIZE 风格一致）
    EMBED_BATCH_SIZE = int(os.environ.get("GRAPHRAG_EMBED_BATCH_SIZE", "64"))
    # 字符级 fallback：每批新节点数（与 existing_names 形成 batch×|existing| 的笛卡尔积）
    RESOLUTION_CHAR_BATCH_SIZE = int(os.environ.get("RESOLUTION_CHAR_BATCH_SIZE", "50"))
    # 字符级 fallback：候选对总数上限，达到后停止扫描（防 OOM）
    RESOLUTION_CHAR_MAX_CANDIDATES = int(os.environ.get("RESOLUTION_CHAR_MAX_CANDIDATES", "5000"))
    # set_graph_delta 末尾是否主动 refresh（与 bulk refresh="false" 配套）。
    # 1=insert 后立即 refresh（下游 query 立即可见，~1s 阻塞开销）
    # 0=依赖 OS 默认 refresh_interval（1s 自然 flush，下游 query 短暂 stale）
    SET_GRAPH_DELTA_REFRESH_AFTER_INSERT = os.environ.get("SET_GRAPH_DELTA_REFRESH_AFTER_INSERT", "1") == "1"
    # search_with_scroll 单次查询返回 hits 上限，防止大 KB 全图加载时 worker OOM。
    # 默认值 50000 保持与原硬编码一致；超大 KB 可通过环境变量提高。
    SEARCH_WITH_SCROLL_HITS_CAP = int(os.environ.get("GRAPHRAG_SEARCH_WITH_SCROLL_HITS_CAP", "50000"))

    # -----------------------------------------------------------------------------
    # Phase 4 – 异步 Community 开关
    # -----------------------------------------------------------------------------
    # Community 报告抽取是否异步执行：1=异步，0=官方默认同步
    USE_ASYNC_COMMUNITY = os.environ.get("USE_ASYNC_COMMUNITY", "0") == "1"

    # -----------------------------------------------------------------------------
    # Phase 5 – 异步后处理队列
    # -----------------------------------------------------------------------------
    # 是否把 resolution/community 推入 Redis Stream 异步后处理队列：1=异步队列，0=立即执行
    USE_ASYNC_KG_PHASES = os.environ.get("USE_ASYNC_KG_PHASES", "0") == "1"
    # 异步后处理队列名
    KG_POSTPROCESS_QUEUE = os.environ.get("KG_POSTPROCESS_QUEUE", "graphrag:postprocess")

    # -----------------------------------------------------------------------------
    # Phase 5-T4 – 启动时卡死任务兜底修复
    # -----------------------------------------------------------------------------
    # 启动时是否扫描并兜底修复卡死的 GraphRAG 任务：1=开启，0=关闭
    RECONCILE_STUCK_ON_BOOT = os.environ.get("RECONCILE_STUCK_ON_BOOT", "0") == "1"
    # 卡死任务宽限期（分钟）
    STUCK_TASK_GRACE_MINUTES = int(os.environ.get("STUCK_TASK_GRACE_MINUTES", "30"))
    # 判定卡死任务的最小节点数/边数
    STUCK_TASK_MIN_NODES = int(os.environ.get("STUCK_TASK_MIN_NODES", "3"))
    STUCK_TASK_MIN_EDGES = int(os.environ.get("STUCK_TASK_MIN_EDGES", "3"))

    # -----------------------------------------------------------------------------
    # Phase 5-T5 – 任务心跳锁（reconcile v2 存活信标）
    # -----------------------------------------------------------------------------
    # 每个 GraphRAG 任务心跳间隔（秒）
    HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "30"))
    # 心跳键 TTL（秒）
    HEARTBEAT_TTL = int(os.environ.get("HEARTBEAT_TTL", "90"))

    # -----------------------------------------------------------------------------
    # Phase 1.4 – Resume 路径安全限制
    # -----------------------------------------------------------------------------
    # resume 路径允许预加载全图的最大节点数，超过则跳过预加载
    KG_MAX_SAFE_RESUME_NODES = int(os.environ.get("KG_MAX_SAFE_RESUME_NODES", "5000"))

    # -----------------------------------------------------------------------------
    # Phase 1 – Subgraph 生成并行度
    # -----------------------------------------------------------------------------
    # 每个 GraphRAG 任务并行处理的最大文档数，与官方 v0.26.0 默认值 4 保持一致
    GRAPHRAG_MAX_PARALLEL_DOCS = int(os.environ.get("GRAPHRAG_MAX_PARALLEL_DOCS", "4"))

    # -----------------------------------------------------------------------------
    # Phase 1.5 – 书籍/章节（ChapterGraph）增强
    # -----------------------------------------------------------------------------
    # 是否在 subgraph 生成阶段提取书籍/章节实体与关系：1=开启，0=关闭（官方默认）
    USE_CHAPTER_GRAPH = os.environ.get("USE_CHAPTER_GRAPH", "0") == "1"

    # -----------------------------------------------------------------------------
    # Phase 6 – 重跑控制（1=保留/复用，0=清空/重跑）
    # -----------------------------------------------------------------------------
    # 重跑时是否保留 subgraph 产物。默认 1，与官方 v0.26.0（不清空）保持一致。
    KEEP_SUBGRAPH = os.environ.get("GRAPHRAG_KEEP_SUBGRAPH", "1") == "1"
    # 重跑时是否保留 merge 产物。默认 1，与官方 v0.26.0（不清空）保持一致。
    KEEP_MERGE = os.environ.get("GRAPHRAG_KEEP_MERGE", "1") == "1"
    # 重跑时是否保留 resolution 产物。默认 1，与官方 v0.26.0（不清空）保持一致。
    KEEP_RESOLUTION = os.environ.get("GRAPHRAG_KEEP_RESOLUTION", "1") == "1"

    # -------------------------------------------------------------------------
    # 书籍/章节类实体在 merge/set_graph 阶段跳过 vector embedding
    # -------------------------------------------------------------------------
    # 这些结构型节点不需要语义向量检索；跳过可节省 embedding 调用与存储。
    NO_EMBED_ENTITY_TYPES = frozenset({"书籍", "章节"})
    _NO_EMBED_TYPE_ALIASES = {
        "书籍": frozenset({"书籍", "book", "books", "Book", "Books", "BOOK"}),
        "章节": frozenset(
            {
                "章节",
                "chapter",
                "chapters",
                "Chapter",
                "Chapters",
                "CHAPTER",
                "section",
                "sections",
                "Section",
                "Sections",
                "SECTION",
            }
        ),
    }

    @staticmethod
    def should_skip_embedding(entity_type: str | None) -> bool:
        """Return True for book/chapter-like entity types."""
        if not entity_type:
            return False
        if entity_type in GraphRAGConfig.NO_EMBED_ENTITY_TYPES:
            return True
        et_lower = entity_type.lower()
        for aliases in GraphRAGConfig._NO_EMBED_TYPE_ALIASES.values():
            if et_lower in {a.lower() for a in aliases}:
                return True
        return False

    @classmethod
    def log_flags(cls, force: bool = False):
        """打印当前生效的所有开关。仅在主进程自动调用,worker 不重复打。

        默认情况下,仅当主进程显式调用时才打印(通过 ``_is_main_process``
        守护)。可在进程入口(如 ``api/ragflow_server.py`` / ``task_executor.py``
        的 ``main()``)显式调用一次。
        """
        if not force and not cls._is_main_process():
            return
        logger.info(
            "GraphRAGConfig: incremental_graph=%s incremental_merge=%s "
            "delete_subgraph_on_doc_delete=%s "
            "recalc_global_pagerank_after_merge=%s "
            "incremental_resolution=%s async_community=%s "
            "async_kg_phases=%s "
            "reconcile_on_boot=%s grace_min=%d min_nodes=%d min_edges=%d "
            "heartbeat_interval=%ds heartbeat_ttl=%ds "
            "knn_resolution=%s resolution_top_k=%d resolution_sim_thr=%.2f "
            "knn_concurrency=%d resolution_batch_size=%d resolution_max_concurrent=%d "
            "max_safe_resume_nodes=%d max_parallel_docs=%d "
            "search_with_scroll_hits_cap=%d "
            "use_chapter_graph=%s "
            "keep_subgraph=%s keep_merge=%s keep_resolution=%s",
            cls.USE_INCREMENTAL_GRAPH,
            cls.USE_INCREMENTAL_MERGE,
            cls.DELETE_SUBGRAPH_ON_DOC_DELETE,
            cls.RECALC_GLOBAL_PAGERANK_AFTER_MERGE,
            cls.USE_INCREMENTAL_RESOLUTION,
            cls.USE_ASYNC_COMMUNITY,
            cls.USE_ASYNC_KG_PHASES,
            cls.RECONCILE_STUCK_ON_BOOT,
            cls.STUCK_TASK_GRACE_MINUTES,
            cls.STUCK_TASK_MIN_NODES,
            cls.STUCK_TASK_MIN_EDGES,
            cls.HEARTBEAT_INTERVAL,
            cls.HEARTBEAT_TTL,
            cls.USE_KNN_FOR_RESOLUTION,
            cls.ENTITY_RESOLUTION_TOP_K,
            cls.ENTITY_RESOLUTION_SIM_THRESHOLD,
            cls.ENTITY_RESOLUTION_KNN_CONCURRENCY,
            cls.RESOLUTION_BATCH_SIZE,
            cls.RESOLUTION_MAX_CONCURRENT_TASKS,
            cls.KG_MAX_SAFE_RESUME_NODES,
            cls.GRAPHRAG_MAX_PARALLEL_DOCS,
            cls.SEARCH_WITH_SCROLL_HITS_CAP,
            cls.USE_CHAPTER_GRAPH,
            cls.KEEP_SUBGRAPH,
            cls.KEEP_MERGE,
            cls.KEEP_RESOLUTION,
        )

    @classmethod
    def _is_main_process(cls) -> bool:
        try:
            from multiprocessing import current_process

            return current_process().name == "MainProcess"
        except Exception:
            return True

    @classmethod
    def reload(cls):
        """重新从环境变量读取所有开关。仅在测试场景使用,生产部署需重启。"""
        cls.USE_INCREMENTAL_GRAPH = os.environ.get("USE_INCREMENTAL_GRAPH", "0") == "1"
        cls.USE_INCREMENTAL_MERGE = os.environ.get("USE_INCREMENTAL_MERGE", "0") == "1"
        cls.DELETE_SUBGRAPH_ON_DOC_DELETE = cls.USE_INCREMENTAL_GRAPH or cls.USE_INCREMENTAL_MERGE
        cls.RECALC_GLOBAL_PAGERANK_AFTER_MERGE = os.environ.get(
            "RECALC_GLOBAL_PAGERANK_AFTER_MERGE", "0"
        ) == "1"
        cls.USE_INCREMENTAL_RESOLUTION = os.environ.get("USE_INCREMENTAL_RESOLUTION", "0") == "1"
        cls.USE_KNN_FOR_RESOLUTION = os.environ.get("USE_KNN_FOR_RESOLUTION", "0") == "1"
        cls.ENTITY_RESOLUTION_TOP_K = int(os.environ.get("ENTITY_RESOLUTION_TOP_K", "20"))
        cls.ENTITY_RESOLUTION_SIM_THRESHOLD = float(os.environ.get("ENTITY_RESOLUTION_SIM_THRESHOLD", "0.7"))
        cls.ENTITY_RESOLUTION_KNN_CONCURRENCY = int(os.environ.get("ENTITY_RESOLUTION_KNN_CONCURRENCY", "8"))
        cls.RESOLUTION_BATCH_SIZE = int(os.environ.get("RESOLUTION_BATCH_SIZE", "100"))
        cls.RESOLUTION_MAX_CONCURRENT_TASKS = int(os.environ.get("RESOLUTION_MAX_CONCURRENT_TASKS", "5"))
        cls.EMBED_BATCH_SIZE = int(os.environ.get("GRAPHRAG_EMBED_BATCH_SIZE", "64"))
        cls.RESOLUTION_CHAR_BATCH_SIZE = int(os.environ.get("RESOLUTION_CHAR_BATCH_SIZE", "50"))
        cls.RESOLUTION_CHAR_MAX_CANDIDATES = int(os.environ.get("RESOLUTION_CHAR_MAX_CANDIDATES", "5000"))
        cls.SEARCH_WITH_SCROLL_HITS_CAP = int(os.environ.get("GRAPHRAG_SEARCH_WITH_SCROLL_HITS_CAP", "50000"))
        cls.USE_ASYNC_COMMUNITY = os.environ.get("USE_ASYNC_COMMUNITY", "0") == "1"
        cls.USE_ASYNC_KG_PHASES = os.environ.get("USE_ASYNC_KG_PHASES", "0") == "1"
        cls.KG_POSTPROCESS_QUEUE = os.environ.get("KG_POSTPROCESS_QUEUE", "graphrag:postprocess")
        cls.RECONCILE_STUCK_ON_BOOT = os.environ.get("RECONCILE_STUCK_ON_BOOT", "0") == "1"
        cls.STUCK_TASK_GRACE_MINUTES = int(os.environ.get("STUCK_TASK_GRACE_MINUTES", "30"))
        cls.STUCK_TASK_MIN_NODES = int(os.environ.get("STUCK_TASK_MIN_NODES", "3"))
        cls.STUCK_TASK_MIN_EDGES = int(os.environ.get("STUCK_TASK_MIN_EDGES", "3"))
        cls.HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "30"))
        cls.HEARTBEAT_TTL = int(os.environ.get("HEARTBEAT_TTL", "90"))
        cls.KG_MAX_SAFE_RESUME_NODES = int(os.environ.get("KG_MAX_SAFE_RESUME_NODES", "5000"))
        cls.GRAPHRAG_MAX_PARALLEL_DOCS = int(os.environ.get("GRAPHRAG_MAX_PARALLEL_DOCS", "4"))
        cls.USE_CHAPTER_GRAPH = os.environ.get("USE_CHAPTER_GRAPH", "0") == "1"
        cls.KEEP_SUBGRAPH = os.environ.get("GRAPHRAG_KEEP_SUBGRAPH", "1") == "1"
        cls.KEEP_MERGE = os.environ.get("GRAPHRAG_KEEP_MERGE", "1") == "1"
        cls.KEEP_RESOLUTION = os.environ.get("GRAPHRAG_KEEP_RESOLUTION", "1") == "1"
        logger.info("GraphRAGConfig.reload() applied; current flags logged via log_flags().")
        cls.log_flags(force=True)


# 不在 import 时自动打印,避免 worker 启动时重复污染日志。
# 需要诊断时显式调用 GraphRAGConfig.log_flags(force=True)。

# === CUSTOM BEGIN [redis-conn-monkey-patch] ===
# 原因：为 RedisDB 与 RedisDistributedLock 注入 GraphRAG 自定义方法，避免修改官方 redis_conn.py
# 日期：2026-06-20
# 关联：rag/utils/redis_conn_patch.py
from rag.utils import redis_conn_patch  # noqa: F401
# === CUSTOM END [redis-conn-monkey-patch] ===

# === CUSTOM BEGIN [siliconflow-timeout-patch] ===
# 原因：SiliconFlow Embedding 在 GraphRAG 批量请求时 30s 容易超时，
#      通过 monkey patch 注入可配置超时，避免直接修改官方 embedding_model.py。
# 日期：2026-06-21
# 关联：rag/llm/siliconflow_timeout_patch.py
from rag.llm.siliconflow_timeout_patch import install as install_siliconflow_timeout_patch

install_siliconflow_timeout_patch()
# === CUSTOM END [siliconflow-timeout-patch] ===
