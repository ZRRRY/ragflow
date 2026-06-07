# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""GraphRAG Feature Flag & runtime configuration.

All flags default to ``False`` so that existing deployments behave exactly as
before.  Enable them incrementally (per-KB or globally) via environment
variables or KB-level parser config.
"""

import logging
import os

logger = logging.getLogger(__name__)


class GraphRAGConfig:
    """Global defaults (read once at import time)."""

    # Phase 1 – Storage decoupling
    USE_INCREMENTAL_GRAPH = os.environ.get("USE_INCREMENTAL_GRAPH", "0") == "1"

    # Phase 2 – Incremental merge (reserved)
    USE_INCREMENTAL_MERGE = os.environ.get("USE_INCREMENTAL_MERGE", "0") == "1"
    MERGE_CAS_MAX_RETRIES = int(os.environ.get("MERGE_CAS_MAX_RETRIES", "10"))

    # Phase 3 – Incremental resolution (reserved)
    USE_INCREMENTAL_RESOLUTION = os.environ.get("USE_INCREMENTAL_RESOLUTION", "0") == "1"

    # Phase 4 – Async community (reserved)
    USE_ASYNC_COMMUNITY = os.environ.get("USE_ASYNC_COMMUNITY", "0") == "1"

    # Phase 5 – Scheduler concurrency (reserved)
    MAX_CONCURRENT_KG_TASKS = int(os.environ.get("MAX_CONCURRENT_KG_TASKS", "2"))
    MIN_CONCURRENT_KG_TASKS = int(os.environ.get("MIN_CONCURRENT_KG_TASKS", "1"))
    USE_ADAPTIVE_LIMITER = os.environ.get("USE_ADAPTIVE_LIMITER", "0") == "1"
    ADAPTIVE_INTERVAL = int(os.environ.get("ADAPTIVE_INTERVAL", "30"))
    ADAPTIVE_DEGRADE_THRESHOLD = int(os.environ.get("ADAPTIVE_DEGRADE_THRESHOLD", "2"))
    ADAPTIVE_INCREASE_THRESHOLD = int(os.environ.get("ADAPTIVE_INCREASE_THRESHOLD", "6"))
    ES_SLOW_THRESHOLD_MS = int(os.environ.get("ES_SLOW_THRESHOLD_MS", "3000"))

    # Phase 5-T3 – Async resolution/community via Redis Stream queue
    USE_ASYNC_KG_PHASES = os.environ.get("USE_ASYNC_KG_PHASES", "0") == "1"
    KG_POSTPROCESS_QUEUE = os.environ.get("KG_POSTPROCESS_QUEUE", "graphrag:postprocess")

    # Phase 5-T4 – Boot-time reconciliation of stuck graphrag tasks
    RECONCILE_STUCK_ON_BOOT = os.environ.get("RECONCILE_STUCK_ON_BOOT", "0") == "1"
    STUCK_TASK_GRACE_MINUTES = int(os.environ.get("STUCK_TASK_GRACE_MINUTES", "30"))
    STUCK_TASK_MIN_NODES = int(os.environ.get("STUCK_TASK_MIN_NODES", "3"))
    STUCK_TASK_MIN_EDGES = int(os.environ.get("STUCK_TASK_MIN_EDGES", "3"))

    # Phase 5-T5 – Per-task heartbeat lock (liveness beacon for reconcile v2)
    HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "30"))
    HEARTBEAT_TTL = int(os.environ.get("HEARTBEAT_TTL", "90"))

    @classmethod
    def log_flags(cls):
        logger.info(
            "GraphRAGConfig: incremental_graph=%s incremental_merge=%s "
            "incremental_resolution=%s async_community=%s max_kg_tasks=%d min_kg_tasks=%d adaptive=%s "
            "adaptive_interval=%d degrade_thr=%d increase_thr=%d es_slow_ms=%d "
            "reconcile_on_boot=%s grace_min=%d min_nodes=%d min_edges=%d "
            "heartbeat_interval=%ds heartbeat_ttl=%ds",
            cls.USE_INCREMENTAL_GRAPH,
            cls.USE_INCREMENTAL_MERGE,
            cls.USE_INCREMENTAL_RESOLUTION,
            cls.USE_ASYNC_COMMUNITY,
            cls.MAX_CONCURRENT_KG_TASKS,
            cls.MIN_CONCURRENT_KG_TASKS,
            cls.USE_ADAPTIVE_LIMITER,
            cls.ADAPTIVE_INTERVAL,
            cls.ADAPTIVE_DEGRADE_THRESHOLD,
            cls.ADAPTIVE_INCREASE_THRESHOLD,
            cls.ES_SLOW_THRESHOLD_MS,
            cls.RECONCILE_STUCK_ON_BOOT,
            cls.STUCK_TASK_GRACE_MINUTES,
            cls.STUCK_TASK_MIN_NODES,
            cls.STUCK_TASK_MIN_EDGES,
            cls.HEARTBEAT_INTERVAL,
            cls.HEARTBEAT_TTL,
        )


# Log once at import so operators can see the effective config in the first log line.
GraphRAGConfig.log_flags()
