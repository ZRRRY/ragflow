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

    @classmethod
    def log_flags(cls):
        logger.info(
            "GraphRAGConfig: incremental_graph=%s incremental_merge=%s "
            "incremental_resolution=%s async_community=%s max_kg_tasks=%d min_kg_tasks=%d adaptive=%s "
            "adaptive_interval=%d degrade_thr=%d increase_thr=%d es_slow_ms=%d",
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
        )


# Log once at import so operators can see the effective config in the first log line.
GraphRAGConfig.log_flags()
