# RAGFlow v0.26.0 移植修改记录

> **本次更新说明**：本文档于 2026-06-18 根据当前工作区（`git status --short --untracked-files=all` + `git diff --stat`）重新审计后更新。本次按文件目录路径字典序重新排序了「文件修改清单」，去除了历史重复、矛盾、过时条目；移除了未实际修改的前端文件（`knowledge-service.ts`、`use-knowledge-request.ts`、`knowledge-graph/index.tsx`）；`api/apps/restful_apis/dataset_api.py` 当前实际新增了 `DELETE /datasets/<dataset_id>/graph` 路由；`web/src/utils/api.ts` 当前 diff 仅涉及 `unbindPipelineTask` URL 调整，不再包含 `knowledgeGraph` / `deleteKnowledgeGraph` URL 的删除或恢复；`web/src/pages/dataset/knowledge-graph/use-delete-graph.ts` 在 `git status` 中标记为 `M`，但 `git hash-object` 与 index hash 一致，仅属换行符/文件元数据导致的虚假修改，无实际内容差异。
>
> 审计时当前变更规模为：**19 files changed, 2841 insertions(+), 160 deletions(-)**（`git diff --stat`，仅统计有实际内容差异的已跟踪文件）；`git status` 共显示 **20 个修改文件 + 9 个新增文件（含本文档自身）**，其中 1 个修改文件无实际内容差异。
>
> 本文件用于跟踪从 `C:/Users/zry1127/ragflow`（ZRRRY 自定义版本）向 `E:/Library/ragflow`（官方 v0.26.0）移植 GraphRAG 增量/优化路径过程中的所有文件修改。
>
> **后续每次修改代码，都必须同步更新此表格**，便于审查和回顾。

## 修改统计

- **修改文件**：20 个（`git status` M 状态；其中 19 个有实际内容差异，1 个为换行符/元数据导致的虚假修改）
- **删除文件**：0 个
- **新增文件**：9 个（含本文档 `MODIFICATIONS.md` 自身；实际代码/资源新增 8 个）
- **总变更**：`19 files changed, 2841 insertions(+), 160 deletions(-)`（`git diff --stat`，未跟踪新增文件未计入）
- **最后更新**：2026-06-18 23:09

---

## 文件修改清单

| 序号 | 文件路径 | 状态 | 改动来源 | 改动摘要 | 验证状态 | 备注/TODO |
|------|----------|------|----------|----------|----------|-----------|
| 1 | `api/apps/restful_apis/dataset_api.py` | 修改 | ZRRRY + v0.26.0 | 保留官方 `GET /datasets/<id>/graph`；**新增** `DELETE /datasets/<dataset_id>/graph` 路由，供官方前端删除按钮调用 | ✅ py_compile 通过 | 2026-06-18 审计：当前 diff 仅新增 DELETE graph 路由；`unbindPipelineTask` 保持 `/datasets/<id>/index?type=` 风格以避开路由冲突 |
| 2 | `api/apps/services/dataset_api_service.py` | 修改 | ZRRRY + v0.26.0 | `get_knowledge_graph` 默认走官方 monolithic JSON 路径；`USE_INCREMENTAL_GRAPH=1` 时走 `get_graph_from_index_for_visualization` 采样路径；节点上限 256、边上限 128 与官方一致；ChapterGraph 节点保护改为 `USE_CHAPTER_GRAPH` 控制；`delete_knowledge_graph` 与 `delete_index(wipe=true)` 清理范围增加 `merge_state` | ✅ py_compile 通过 | 2026-06-18 审计：当前未包含 `get_knowledge_graph_full` / export 代码 |
| 3 | `api/db/services/document_service.py` | 修改 | ZRRRY + v0.26.0 | 文档删除时 GraphRAG 清理逻辑条件化：增量路径（`USE_INCREMENTAL_GRAPH=1` 或 `USE_INCREMENTAL_MERGE=1`）下先删除 subgraph；官方路径保持 v0.26.0 原逻辑 | ✅ py_compile 通过 | 2026-06-18 审计：按方案 C 移植 2.5 |
| 4 | `common/doc_store_audit.py` | 新增 | ZRRRY | `docStoreConn.delete` 审计钩子，记录 KG 产物删除的调用方与条件 | ✅ py_compile 通过 | 在 `common/settings.py` 中安装；失败不阻塞原删除 |
| 5 | `common/settings.py` | 修改 | ZRRRY + v0.26.0 | 初始化 docStoreConn 后安装审计钩子 | ✅ py_compile 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 6 | `docker/.env` | 修改 | ZRRRY + v0.26.0 | 追加 GraphRAG 环境变量；`DOC_ENGINE` 默认改为 `opensearch`；保留用户调优开关值；按 Phase 分块加中文注释；新增 `USE_CHAPTER_GRAPH=1`；移除死开关 `USE_BATCHED_SUMMARIZATION`；retry/backoff/build_subgraph 参数显式覆盖官方默认值 | ✅ git diff --check 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 7 | `docker/docker-compose-base.yml` | 修改 | ZRRRY + v0.26.0 | `opensearch01` 新增 `OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g` | ✅ YAML 有效 + diff-check 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 8 | `docker/docker-compose.yml` | 修改 | ZRRRY + v0.26.0 | 在 `ragflow-cpu`/`ragflow-gpu` 新增开发热挂载；补全 gpu 服务缺失的 `../api` | ✅ YAML 有效 + diff-check 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 9 | `docker/finisher/README.md` | 新增 | ZRRRY | 卡死任务收尾脚本 `finish_stuck_graphrag.py` 使用说明 | ✅ 已复制 | 属于 `docker/finisher/` 目录 |
| 10 | `docker/finisher/finish_stuck_graphrag.py` | 新增 | ZRRRY | 手动修复 GraphRAG 卡死任务的运维脚本：校验 OS 节点/边数、修改 MySQL task progress、清理 Redis phase marker | ✅ py_compile 通过 | 属于 `docker/finisher/` 目录；默认 dry-run |
| 11 | `docker/finisher/find_kb_id.sql` | 新增 | ZRRRY | 查找 KB ID 与卡死 task 的辅助 SQL | ✅ 已复制 | 属于 `docker/finisher/` 目录 |
| 12 | `rag/graphrag/config.py` | 新增 | ZRRRY | GraphRAG 环境变量开关集中配置；所有布尔开关默认 `0`，`KEEP_*` 默认 `1` 以匹配官方不清空行为；新增 `USE_CHAPTER_GRAPH`、`USE_ASYNC_COMMUNITY`、`USE_ASYNC_KG_PHASES` 等；移除死开关 `USE_BATCHED_SUMMARIZATION`；按 Phase 分块并加中文注释 | ✅ py_compile 通过 | 2026-06-18 审计：当前为未跟踪新增文件 |
| 13 | `rag/graphrag/entity_resolution.py` | 修改 | ZRRRY + v0.26.0 | 新增 `is_similarity_str`、`build_excluded_types`；支持 `candidate_resolution` 注入；`excluded_types` 参数；batch/concurrency 改读 `GraphRAGConfig` | ✅ py_compile 通过 | 保留官方 checkpoint 参数 |
| 14 | `rag/graphrag/general/index.py` | 修改 | ZRRRY + v0.26.0 | 核心流程改造：`DEFAULT_GRAPHRAG_*` 常量默认值恢复官方并通过环境变量覆盖；`merge_state` 写入改为仅增量路径；修复 `union_nodes`→`subgraph_nodes` bug；ChapterGraph 提取改为 `USE_CHAPTER_GRAPH` 开关控制；增量 resolution 路径；`load_doc_chunks` 恢复官方无缓存直接拼接；合并阶段跳过已合并文档的判断改为双路径；新增 `_record_lock_metric`、异步 community、异步后处理队列推送 | ✅ py_compile 通过 | 2026-06-18 审计：与当前 diff 一致；注意 LF→CRLF 警告 |
| 15 | `rag/graphrag/utils.py` | 修改 | ZRRRY + v0.26.0 | 新增 merge_state 读写、query_existing_entities/relations、fetch_node_vectors、get_graph_from_index、get_graph_from_index_for_visualization、set_graph_delta、_set_graph_monolithic、批量嵌入、按 from_node 批量删边；`does_graph_contains` 条件化；`set_graph` 改为 router；`insert_chunks_bounded` 还原为官方实现；`rebuild_graph` bs 还原为 256 | ✅ py_compile 通过 | 2026-06-18 审计：与当前 diff 一致；注意 LF→CRLF 警告 |
| 16 | `rag/graphrag/utils_pagination.py` | 新增 | ZRRRY | `search_all_by_search_after` / `collect_all` OpenSearch/Elasticsearch search_after 分页工具；用于绕过 `max_result_window` 并避免 scroll 全量累积 | ✅ py_compile 通过 | Infinity/OceanBase 等后端自动回退旧路径 |
| 17 | `rag/svr/task_executor.py` | 修改 | ZRRRY + v0.26.0 | 新增心跳锁、启动 reconcile、后处理队列消费者；心跳/reconcile 启动改为 `RECONCILE_STUCK_ON_BOOT=1` 才启用；保留官方 `LoopLocalSemaphore(2)` 作为 kg_limiter | ✅ py_compile 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 18 | `rag/utils/es_conn.py` | 修改 | ZRRRY + v0.26.0 | 新增 `count` 辅助方法（reconcile / 卡死修复使用） | ✅ py_compile 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 19 | `rag/utils/opensearch_conn.py` | 修改 | ZRRRY + v0.26.0 | 新增 `knn_search_entities`、`search_with_scroll`、`count`；`insert()` bulk 改为 `refresh="false", timeout=300` | ✅ py_compile 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 20 | `rag/utils/redis_conn.py` | 修改 | ZRRRY + v0.26.0 | 新增 `RedisDB.ttl(k)` 方法，用于心跳键剩余时间查询 | ✅ py_compile 通过 | 2026-06-18 审计：与当前 diff 一致 |
| 21 | `web/src/assets/svg/data-flow/total-chunks-icon-bri.svg` | 新增 | ZRRRY | Total chunks 统计卡片亮色图标 | ✅ patch 干净应用 | 与 dataset-overview 分块统计卡片配套 |
| 22 | `web/src/assets/svg/data-flow/total-chunks-icon.svg` | 新增 | ZRRRY | Total chunks 统计卡片暗色图标 | ✅ patch 干净应用 | 与 dataset-overview 分块统计卡片配套 |
| 23 | `web/src/locales/en.ts` | 修改 | ZRRRY + v0.26.0 | 新增 `datasetOverview.totalChunks` 英文文案 | ✅ patch 干净应用 | 2026-06-18 审计：与当前 diff 一致 |
| 24 | `web/src/locales/zh.ts` | 修改 | ZRRRY + v0.26.0 | 新增 `datasetOverview.totalChunks` 中文文案 | ✅ patch 干净应用 | 2026-06-18 审计：与当前 diff 一致 |
| 25 | `web/src/pages/dataset/dataset-overview/hook.ts` | 修改 | ZRRRY + v0.26.0 | 新增 `useFetchDatasetChunkCount` hook，调用 `getKbDetail` 读取 `chunk_count` | ✅ patch 干净应用 | 2026-06-18 审计：与当前 diff 一致 |
| 26 | `web/src/pages/dataset/dataset-overview/index.tsx` | 修改 | ZRRRY + v0.26.0 | 统计卡网格从 3 列改为 4 列，新增 Total chunks 卡片 | ✅ patch 干净应用 | 2026-06-18 审计：与当前 diff 一致 |
| 27 | `web/src/pages/dataset/knowledge-graph/use-delete-graph.ts` | 修改（无实际内容差异） | v0.26.0 | 官方 v0.26.0 删除知识图谱 hook；当前工作区副本与 index hash 一致，仅换行符/文件元数据导致 `git status` 标记为 `M` | ✅ hash 一致 | 2026-06-18 审计：`git hash-object` 与 `git ls-files -s` hash 均为 `a562891d72f161b87c4a507cc77a419124387839` |
| 28 | `web/src/utils/api.ts` | 修改 | ZRRRY + v0.26.0 | `unbindPipelineTask` 改回 `/datasets/{id}/index?type={indexType}`（`wipe=false` 时拼接为 `&wipe=false`），使 pause 走 `delete_index` 支持 wipe | ✅ git diff --check 通过 | 2026-06-18 审计：当前 diff 不再涉及 `knowledgeGraph` / `deleteKnowledgeGraph` URL 的删除或恢复 |
| 29 | `MODIFICATIONS.md` | 新增（未跟踪） | ZRRRY | 本文档自身，用于跟踪移植修改 | ✅ 文档审阅 | 未纳入 git 索引；本次根据当前工作区重新审计后更新 |

---

## 关键环境变量开关

> 以下 **开关类** 环境变量：代码层面默认值为 `0`（关闭），但 `docker/.env` 中按用户当前配置设为 `1`（开启）；数值类参数保持合理默认值。

### Phase 1：增量图构建
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_INCREMENTAL_GRAPH` | `1`（代码默认 `0`） | 图写入：1=delta 增量写入，0=官方默认全量写入 |

### Phase 2：增量 Merge
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_INCREMENTAL_MERGE` | `1`（代码默认 `0`） | Merge 阶段：1=仅合并新增 subgraph，0=官方默认全图 merge |
| `GRAPHRAG_MERGE_TIMEOUT_SECONDS` | `1800` | Merge 阶段单步超时（秒） |

### Phase 2.5：增量合并后全局 PageRank 重算
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `RECALC_GLOBAL_PAGERANK_AFTER_MERGE` | `0` | 增量 merge 全部完成后是否加载一次全图重算 PageRank 并写回 entity chunks：1=开启，0=关闭。需同时开启 `USE_INCREMENTAL_MERGE=1`；仅 OpenSearch 后端（`search_with_scroll` 可用）实际生效，其他后端自动 skip。`merge_subgraph_incremental` 故意跳过全局 PageRank 以避免逐文档加载全图；本开关提供一次性补偿。 |

### Phase 3：增量实体消解
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_INCREMENTAL_RESOLUTION` | `1`（代码默认 `0`） | 实体消解：1=按 entity_type 分批消解，0=官方默认全图消解 |
| `USE_KNN_FOR_RESOLUTION` | `1`（代码默认 `0`） | 增量消解内：1=OpenSearch KNN 召回，0=字符级过滤（仅增量消解开启时生效） |
| `ENTITY_RESOLUTION_TOP_K` | `20` | KNN 召回 Top-K |
| `ENTITY_RESOLUTION_SIM_THRESHOLD` | `0.7` | KNN 相似度阈值 |
| `ENTITY_RESOLUTION_KNN_CONCURRENCY` | `8` | KNN 查询并发数 |
| `RESOLUTION_BATCH_SIZE` | `100` | 实体消解批大小 |
| `RESOLUTION_MAX_CONCURRENT_TASKS` | `5` | 实体消解最大并发任务数 |

### Phase 4：异步 Community
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_ASYNC_COMMUNITY` | `0` | Community 报告抽取：1=异步（从 index 加载全图），0=官方默认同步 |

### Phase 5：异步后处理队列
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_ASYNC_KG_PHASES` | `0` | 是否把 resolution/community 推入 Redis Stream 异步后处理队列 |
| `KG_POSTPROCESS_QUEUE` | `graphrag:postprocess` | Redis Stream 队列名 |

### Phase 5-T4/T5：卡死修复与心跳锁
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `RECONCILE_STUCK_ON_BOOT` | `0` | 启动时是否扫描并兜底修复卡死任务 |
| `STUCK_TASK_GRACE_MINUTES` | `30` | 卡死任务宽限期（分钟） |
| `STUCK_TASK_MIN_NODES` | `3` | 判定卡死任务的最小节点数 |
| `STUCK_TASK_MIN_EDGES` | `3` | 判定卡死任务的最小边数 |
| `HEARTBEAT_INTERVAL` | `30` | 每个 GraphRAG 任务心跳间隔（秒） |
| `HEARTBEAT_TTL` | `90` | 心跳键 TTL（秒） |

### Phase 1.4 / 1.5 / 6：安全限制、ChapterGraph、重跑控制
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `KG_MAX_SAFE_RESUME_NODES` | `5000` | resume 路径允许预加载全图的最大节点数 |
| `GRAPHRAG_MAX_PARALLEL_DOCS` | `8`（代码默认 `4`） | 每个 GraphRAG 任务并行处理的最大文档数 |
| `USE_CHAPTER_GRAPH` | `1`（代码默认 `0`） | 是否在 subgraph 生成阶段提取书籍/章节实体与关系 |
| `GRAPHRAG_KEEP_SUBGRAPH` | `1`（代码默认 `1`） | 重跑时是否保留 subgraph 产物（1=保留/复用，0=清空） |
| `GRAPHRAG_KEEP_MERGE` | `1`（代码默认 `1`） | 重跑时是否保留 merge 产物（1=保留/复用，0=清空） |
| `GRAPHRAG_KEEP_RESOLUTION` | `1`（代码默认 `1`） | 重跑时是否保留 resolution 产物（1=保留/复用，0=清空） |

---

## 验证记录

| 验证项 | 命令 | 结果 | 时间 |
|--------|------|------|------|
| Python 语法检查 | `python -m py_compile <files>` | ✅ 全部通过 | 2026-06-17 |
| Git diff 检查 | `git diff --check` | ✅ 无冲突/尾随空格 | 2026-06-17 |
| docker/.env 整理 | 手动检查 + `git diff --check` | ✅ 所有开关默认 0，中文注释，按 Phase 分块 | 2026-06-17 |
| rag/graphrag/config.py 整理 | `python -m py_compile` + `git diff --check` | ✅ 代码默认值统一为 0，中文注释，按 Phase 分块 | 2026-06-17 |
| dataset-overview 分块统计卡片 | `git apply --check` + `git apply` | ✅ patch 干净应用，无冲突 | 2026-06-17 |
| docker-compose.yml dev mounts | `python -c yaml.safe_load` + `git diff --check` | ✅ YAML 有效，diff-check 通过 | 2026-06-17 |
| v0.26.0 功能存在性检查 | `grep` + `Read` 逐文件比对 | ✅ 完成 20 项功能的状态判定 | 2026-06-17 |
| 待移植清单范围收敛 | 文档审查 | ✅ 已限定为「双方都存在但实现不同」的功能 | 2026-06-17 |
| merge timeout 环境变量化 | `python -m py_compile` + `git diff --check` | ✅ `GRAPHRAG_MERGE_TIMEOUT_SECONDS=1800` 生效；开关值恢复为用户原设置 | 2026-06-17 |
| Dealer 白名单修复 | `git checkout -- rag/nlp/search.py` | ⏸️ 已撤回，暂不处理 | 2026-06-17 |
| docker-compose-base OpenSearch JVM | `python -c yaml.safe_load` + `git diff --check` | ✅ `OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g` 生效 | 2026-06-17 |
| OpenSearch bulk 调优 | `python -m py_compile` + `git diff --check` | ✅ `refresh="false", timeout=300` 生效 | 2026-06-17 |
| Redis ttl | `python -m py_compile` + `git diff --check` | ✅ `RedisDB.ttl(k)` 生效 | 2026-06-17 |
| 2.7/2.9/2.10 放弃 | 文档更新 | ⏸️ Infinity count、批量摘要、connection_utils 超时兜底标记为已放弃 | 2026-06-17 |
| 当前工作区重新审计 | `git status --short --untracked-files=all` + `git diff --stat` + 逐文件 diff 审阅 | ✅ 文件清单与实际未提交变更一致 | 2026-06-18 |
| `use-delete-graph.ts` 内容一致性 | `git hash-object` vs `git ls-files -s` | ✅ hash 一致，仅换行符/元数据差异 | 2026-06-18 |

---

## 待移植功能清单

> 对比原则：**仅关注双方文件/功能都存在，但实现不同的差异**。v0.26.0 新增的架构功能（如 `internal/` Go 后端重构、新模型 provider、新前端组件等）应保留，不列入本清单。个人文档/缓存也不列入。

### 图例
- ⚠️ **实现不同**：双方都有该功能/文件，但行为或默认值不同，需要决定对齐方式
- ❌ **缺失能力**：双方都有该模块，但 v0.26.0 缺少你实现的某个能力
- 🔄 **已移植**：本次移植已完成

---

### 一、前端 Web（双方都有该文件/页面）

| # | 功能 | 对比结果 | 说明 |
|---|------|----------|------|
| 1.1 | `web/src/utils/api.ts` 解绑索引 URL | 🔄 已移植 | 官方 `unbindPipelineTask` 使用 `/datasets/{id}/{indexType}`，若同时存在 `DELETE /datasets/{id}/graph` 路由会导致 pause GraphRAG 时 wipe=false 失效。已改回你的 `/datasets/{id}/index?type={indexType}` 风格，使 pause 走 `delete_index` 支持 wipe；`DELETE /datasets/{id}/graph` 路由已新增以兼容官方前端删除按钮。 |
| 1.2 | dataset-overview 分块总数统计卡片 | 🔄 已移植 | patch `0006` 已应用，双方现在一致。 |

---

### 二、GraphRAG 核心与性能（双方都有这些模块）

| # | 功能 | 对比结果 | 说明 |
|---|------|----------|------|
| 2.1 | GraphRAG timeout/backoff/concurrency 默认值 | 🔄 已调整 | 代码常量恢复为官方默认值（retry=2、backoff=2.0/60.0、build_subgraph_per_chunk=300）；通过 `docker/.env` 显式覆盖为用户调优值。`merge_timeout_seconds` 仍通过 env 覆盖，默认 180s。 |
| 2.2 | `build_subgraph` 300s/600s 默认超时 | 🔄 已调整 | 代码常量恢复为官方 300s；`docker/.env` 显式设为 600s。 |
| 2.3 | `is_doc_merged` 基于 `merge_state` | 🔄 已调整 | `merge_state` 写入与读取仅在 `USE_INCREMENTAL_MERGE=1` 时生效；官方全量路径仍使用 `graph.source_id` 判断。 |
| 2.4 | Dealer 白名单修复（`entity_type_kwd` + dropped debug log） | ⏸️ 已撤回 | 双方都有 `rag/nlp/search.py` 的 `Dealer.get_filters`，但 v0.26.0 未加入 `entity_type_kwd` 白名单，也无 dropped keys debug 日志。已 checkout 还原，暂不处理。 |
| 2.5 | 文档删除时 GraphRAG 产物清理策略 | 🔄 已移植 | 官方路径保持 v0.26.0「移除 source_id + 标记 removed_kwd + 删除 orphan」；增量路径（`USE_INCREMENTAL_GRAPH=1` 或 `USE_INCREMENTAL_MERGE=1`）下新增「无条件删除 subgraph」。通过 `GraphRAGConfig.DELETE_SUBGRAPH_ON_DOC_DELETE` 自动联动。 |
| 2.6 | OpenSearch `bulk()` refresh 策略 | 🔄 已移植 | 双方都有 `rag/utils/opensearch_conn.py` 的 `bulk`。已改为 `refresh="false", timeout=300`，并在注释中说明大 KB 性能优化原因。 |
| 2.7 | doc store `count()` 跨后端支持 | ⏸️ 已放弃 | ES/OpenSearch 都有 `count()`，Infinity 没有。用户决定不移植。 |
| 2.8 | Redis `ttl()` | 🔄 已移植 | 双方都有 `RedisDB` 类。已新增 `ttl(key)` 方法。 |
| 2.9 | 批量 Entity/Edge 摘要 | ⏸️ 已放弃 | 双方都有 `rag/graphrag/general/extractor.py`，v0.26.0 只有单条摘要，你实现了批量摘要。用户决定不移植。 |
| 2.10 | `common/connection_utils.py` 超时兜底 | ⏸️ 已放弃 | 双方都有 `@timeout` 装饰器。v0.26.0 依赖 `ENABLE_TIMEOUT_ASSERTION` 开关；你改为始终兜底。用户决定保持 v0.26.0 原样。 |
| 2.11 | 合并阶段跳过已合并文档的判断策略 | 🔄 已调整 | 增量路径使用 `merge_state`（你的方法）；官方全量路径恢复为加载全局图检查 `source_id`（v0.26.0 原方法）。两者通过 `USE_INCREMENTAL_MERGE` 开关区分。 |
| 2.12 | 书籍/章节（ChapterGraph）增强 | 🔄 已调整 | 提取 `_extract_book_and_chapters` 与 `_link_entities_to_chapters` 改为由 `USE_CHAPTER_GRAPH` 开关控制，默认关闭以匹配官方。 |
| 2.13 | `resolve_entities` 传入 `subgraph_nodes` 而非 `union_nodes` | 🔄 已修复 | 修复 resume 路径下 `union_nodes` 为空导致 resolution 什么都不做的 bug。 |
| 2.14 | `does_graph_contains` 双路径检查 | 🔄 已调整 | subgraph 路径检查仅在 `USE_INCREMENTAL_GRAPH=1` 时执行；否则只查 monolithic graph JSON。 |
| 2.15 | 心跳锁 / reconcile 启动 | 🔄 已调整 | 心跳写入、`_heartbeat_loop`、reconcile leader 选举仅在 `RECONCILE_STUCK_ON_BOOT=1` 时启用；默认关闭以匹配官方。 |
| 2.16 | `kg_limiter` 实现 | 🔄 已删除 | 已移除 `AdaptiveConcurrencyLimiter` 及 `rag/graphrag/limiter.py`，完全使用官方 `LoopLocalSemaphore(2)`。 |
| 2.17 | `get_knowledge_graph` 默认图加载路径 | 🔄 已调整 | 默认读取 monolithic graph JSON blob；仅在 `USE_INCREMENTAL_GRAPH=1` 时 fallback 到 index-first 路径。 |
| 2.18 | `load_doc_chunks` chunk 缓存与分隔符 | 🔄 已还原 | 移除 Redis 缓存与 `\n` 分隔符，恢复官方 v0.26.0 的直接拼接、无缓存逻辑。 |
| 2.19 | `insert_chunks_bounded` 批量插入 | 🔄 已还原 | 恢复官方默认并发 4，移除 adaptive limiter 事件注入钩子，与 v0.26.0 实现一致。 |
| 2.20 | `_set_graph_monolithic` 官方化 | 🔄 已还原 | `USE_INCREMENTAL_GRAPH=0` 时 `_set_graph_monolithic()` 完全复刻官方 `set_graph()`：逐节点/边 `asyncio.gather` 嵌入、删除范围 `graph+subgraph`、逐条删除边、无 `refresh_idx`、无 `_pre_delete_added_updated`。 |
| 2.21 | `build_one` / `build_subgraph_attempt` subgraph checkpoint 检查 | 🔄 已条件化 | subgraph checkpoint 只在 `USE_INCREMENTAL_GRAPH=1` 或 `USE_INCREMENTAL_MERGE=1` 时加载；官方路径下每次都重新 LLM 提取。 |
| 2.22 | `EntityResolution` 默认 `excluded_types` | 🔄 已调整 | `excluded_types=None` 时默认空 set，与官方一致不预排除任何类型；增量路径 `resolve_entities_incremental` 显式传入 `build_excluded_types(entity_types)`。 |
| 2.23 | `rebuild_graph` 分页大小 | 🔄 已还原 | `bs` 从 5000 改回官方 256，严格对齐 v0.26.0。 |
| 2.24 | `get_knowledge_graph` 增量路径读取 | 🔄 已优化 | `USE_INCREMENTAL_GRAPH=1` 时使用 `get_graph_from_index_for_visualization` 采样读取（避免全量扫描 index）；节点上限 256、边上限 128 与官方 v0.26.0 一致；ChapterGraph 保护仍由 `USE_CHAPTER_GRAPH` 控制。 |

---

### 三、Docker / 部署 / 运维（双方都有这些文件）

| # | 功能 | 对比结果 | 说明 |
|---|------|----------|------|
| 3.1 | `docker-compose.yml` 开发热挂载 | 🔄 已移植 | 已按方案 B 移植。 |
| 3.2 | `docker/docker-compose-base.yml` OpenSearch JVM 内存 | 🔄 已移植 | 双方都有 `docker-compose-base.yml`。已新增 `OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g`。 |
| 3.3 | `docker/.env` 默认 `DOC_ENGINE` | ⚠️ 实现不同 | 官方默认 `elasticsearch`，当前工作区改为 `opensearch`。属于部署偏好，非功能移植。 |

---

### 四、API / 服务层（双方都有该文件）

| # | 功能 | 对比结果 | 说明 |
|---|------|----------|------|
| 4.1 | `task_service.py` `CANVAS_DEBUG_DOC_ID` 处理 | 🔄 已一致 | v0.26.0 已有相同逻辑，无需改动。 |
| 4.2 | `dataset_api.py` 删除图路由 | 🔄 已调整 | 当前工作区**新增** `DELETE /datasets/<id>/graph` 路由（官方 v0.26.0 前端已有删除按钮）；`unbindPipelineTask` 保持 `/datasets/<id>/index?type=` 风格以避开路由冲突。 |

---

### 五、测试（双方都有测试目录）

| # | 功能 | 对比结果 | 说明 |
|---|------|----------|------|
| 5.1 | GraphRAG 单元测试 | ❌ 缺失能力 | 双方都有 `test/unit_test/rag/graphrag/`，v0.26.0 无你的 `is_doc_merged`、`merge_state`、批量摘要等测试。**需移植**。 |

---

### 六、文档 / 配置（可选，双方都有这些文件）

| # | 功能 | 对比结果 | 说明 |
|---|------|----------|------|
| 6.1 | `.gitignore` | ⚠️ 实现不同 | 双方都有 `.gitignore`，内容差异较大。可选按你的版本整理。 |
| 6.2 | `AGENTS.md` | ⚠️ 实现不同 | 双方都有 `AGENTS.md`，内容不同。可选移植你的重写版本。 |

---

### 不列入本清单的内容

以下属于「仅一方有」，按用户要求不做为待移植功能：

- **v0.26.0 新增架构**：`internal/` Go 后端重构、`conf/models/xiaomi.json`、新 provider 协议、新前端模型管理等 —— **应保留**。
- **你的个人工作区产物**：`mydocs/`、`rag/res/deepdoc/.cache/`、日志文件、ONNX 模型等 —— **不应移植**。
- **v0.26.0 已原生实现的 GraphRAG checkpoint 机制**：`rag/graphrag/checkpoints.py` —— **应保留**，与你的增量路径互补。

---

### 已处理项

| # | 功能 | 处理方式 | 状态 |
|---|------|----------|------|
| 1.1 | `web/src/utils/api.ts` 解绑索引 URL | `unbindPipelineTask` 改回 `/datasets/{id}/index?type={indexType}`，使 pause 走 `delete_index` 支持 wipe；新增 `DELETE /datasets/{id}/graph` 路由兼容官方前端删除按钮 | ✅ 已完成 |
| doc_store_audit | `common/doc_store_audit.py` 审计钩子 | 用户决定保留：仅在 `docStoreConn.delete` 时记录审计日志，不改变删除行为 | ✅ 已保留 |
| 2.1 | GraphRAG timeout/backoff/concurrency 默认值 | `index.py` 常量恢复为官方默认值，改由 `GRAPHRAG_RETRY_ATTEMPTS`/`GRAPHRAG_RETRY_BACKOFF_SECONDS`/`GRAPHRAG_RETRY_BACKOFF_MAX_SECONDS`/`GRAPHRAG_BUILD_SUBGRAPH_TIMEOUT_PER_CHUNK_SECONDS` 环境变量覆盖；`docker/.env` 中写入用户调优值 | ✅ 已完成 |
| 2.3 | `merge_state` 条件化 | `index.py` 中 `write_merge_state` 调用仅在 `USE_INCREMENTAL_MERGE=1` 时执行 | ✅ 已完成 |
| 2.5 | 文档删除时 subgraph 清理策略 | `api/db/services/document_service.py` 中增量路径（`USE_INCREMENTAL_GRAPH=1` 或 `USE_INCREMENTAL_MERGE=1`）先删除 subgraph；官方路径保持 v0.26.0 原逻辑；`rag/graphrag/config.py` 新增 `DELETE_SUBGRAPH_ON_DOC_DELETE` 联动开关 | ✅ 已完成 |
| 2.6 | OpenSearch `bulk()` refresh 策略 | `rag/utils/opensearch_conn.py` 的 `insert()` 改为 `refresh="false", timeout=300` | ✅ 已完成 |
| 2.8 | Redis `ttl()` | `rag/utils/redis_conn.py` 新增 `ttl(key)` 方法 | ✅ 已完成 |
| 2.11 | 合并阶段跳过已合并文档的判断策略 | `rag/graphrag/general/index.py` 的 `merge_subgraph_attempt()` 改为：增量路径检查 `merge_state`，官方全量路径恢复为 `get_graph()` + `source_id` 检查 | ✅ 已完成 |
| 2.12 | ChapterGraph 开关化 | `rag/graphrag/config.py` 新增 `USE_CHAPTER_GRAPH`；`index.py` 中相关逻辑由开关控制；`docker/.env` 写入 `USE_CHAPTER_GRAPH=1` | ✅ 已完成 |
| 2.13 | `union_nodes`→`subgraph_nodes` bug | `rag/graphrag/general/index.py` 的 `resolve_entities()` 调用参数修复 | ✅ 已完成 |
| 2.14 | `does_graph_contains` 条件化 | `rag/graphrag/utils.py` 中 subgraph 路径检查仅在 `USE_INCREMENTAL_GRAPH=1` 时执行 | ✅ 已完成 |
| 2.15 | 心跳锁 / reconcile 条件化 | `rag/svr/task_executor.py` 中心跳写入/loop/reconcile leader 选举仅在 `RECONCILE_STUCK_ON_BOOT=1` 时启用 | ✅ 已完成 |
| 2.16 | `kg_limiter` 恢复官方 | 已删除 `rag/graphrag/limiter.py`，`rag/svr/task_executor.py` 完全使用官方 `LoopLocalSemaphore(2)`；`rag/graphrag/config.py` 与 `docker/.env` 移除相关环境变量 | ✅ 已完成 |
| 2.17 | `get_knowledge_graph` 默认 monolithic 路径 | `api/apps/services/dataset_api_service.py` 中默认读取 monolithic graph JSON，仅 `USE_INCREMENTAL_GRAPH=1` 时走 index-first | ✅ 已完成 |
| 2.18 | `load_doc_chunks` 还原 | `rag/graphrag/general/index.py` 中移除 Redis 缓存与 `\n` 分隔符，恢复官方直接拼接逻辑 | ✅ 已完成 |
| 2.19 | `insert_chunks_bounded` 还原 | `rag/graphrag/utils.py` 中恢复官方默认并发 4，移除 adaptive limiter 注入 | ✅ 已完成 |
| 2.20 | `_set_graph_monolithic` 官方化 | `USE_INCREMENTAL_GRAPH=0` 时 `_set_graph_monolithic()` 完全复刻官方 `set_graph()`：逐节点/边 `asyncio.gather` 嵌入、删除范围 `graph+subgraph`、逐条删除边、无 `refresh_idx`、无 `_pre_delete_added_updated` | ✅ 已完成 |
| 2.21 | subgraph checkpoint 加载条件化 | `rag/graphrag/general/index.py` 中 `build_one` / `build_subgraph_attempt` 仅在增量路径下调用 `load_subgraph_from_store` | ✅ 已完成 |
| 2.22 | `EntityResolution` 默认不排除类型 | `rag/graphrag/entity_resolution.py` 中 `excluded_types=None` 时为空 set；增量路径显式传入 `build_excluded_types` | ✅ 已完成 |
| 2.23 | `rebuild_graph` 分页大小 | `rag/graphrag/utils.py` 中 `rebuild_graph` 的 `bs` 从 5000 还原为官方 256 | ✅ 已完成 |
| 2.24 | `get_knowledge_graph` 增量路径可视化优化 | `USE_INCREMENTAL_GRAPH=1` 时走 `get_graph_from_index_for_visualization` 采样路径；节点 256 / 边 128 与官方一致；`rag/graphrag/utils.py` 新增该函数 | ✅ 已完成 |
| 2.25 | 增量 merge 后全局 PageRank 重算 | `rag/graphrag/config.py` 新增 `RECALC_GLOBAL_PAGERANK_AFTER_MERGE`（默认 `0`）；`rag/graphrag/general/index_extras.py` 新增 `recalc_global_pagerank()`，在 `run_graphrag_for_kb` merge 全部完成后调用一次。`merge_subgraph_incremental` 故意跳过全局 PageRank 以避免逐文档加载全图；本开关提供一次性补偿。仅 OpenSearch 后端实际生效（依赖 `search_with_scroll`），ES / Infinity 自动 skip | ✅ 已完成 |
| 3.2 | `docker/docker-compose-base.yml` OpenSearch JVM 内存 | 新增 `OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g` | ✅ 已完成 |
| 4.2 | `dataset_api.py` 删除图路由 | 当前工作区**新增** `DELETE /datasets/<id>/graph` 路由，以兼容官方 v0.26.0 前端删除按钮；前端删除图谱相关代码当前未修改 | ✅ 已完成 |

### 已撤回/已放弃项

| # | 功能 | 说明 | 状态 |
|---|------|------|------|
| 2.4 | Dealer 白名单修复 | 已 checkout 还原 `rag/nlp/search.py`，暂不处理 | ⏸️ 已撤回 |
| 2.7 | Infinity `count()` | 用户决定不移植 | ⏸️ 已放弃 |
| 2.9 | 批量 Entity/Edge 摘要 | 用户决定不移植 | ⏸️ 已放弃 |
| 2.10 | `common/connection_utils.py` 超时兜底修复 | 用户决定保持 v0.26.0 原样 | ⏸️ 已放弃 |
| 2.4 | `rag/graphrag/utils.py` Dealer 绕过逻辑 | 原 patch `0020` 中 `is_doc_merged` 改为绕过 `Dealer` 直接查询 `source_id`；本次移植中 `is_doc_merged` 已基于 `query_merge_state()` 判断，不再通过 Dealer 查询 `source_id`，因此该绕过逻辑不再必要 | ⏸️ 已放弃 |
| 前端删除图谱相关修改 | `web/src/services/knowledge-service.ts`、`web/src/hooks/use-knowledge-request.ts`、`web/src/pages/dataset/knowledge-graph/index.tsx` | 当前工作区这些文件均未修改；官方 v0.26.0 前端已自带删除按钮，无需额外恢复 | ⏸️ 无需处理 |

### 说明：2.4 中 `rag/graphrag/utils.py` 未按原 patch 修改的原因

你的 patch `0020` 中 `is_doc_merged` 改为绕过 `Dealer`，直接调用 `docStoreConn.search`，是为了避免 `source_id` 条件被 Dealer 白名单丢弃。

但在本次移植中，`is_doc_merged` 已改为基于 `query_merge_state()`（`merge_state` 表）判断，不再通过 Dealer 查询 `source_id`，因此原 patch 中的 `utils.py` 绕过逻辑**不再必要**。

## 待验证事项

- [ ] 端到端集成测试：`USE_INCREMENTAL_GRAPH=1 USE_INCREMENTAL_MERGE=1 USE_INCREMENTAL_RESOLUTION=1`
- [ ] 验证 `_record_lock_metric` Redis hash 写入
- [ ] 验证启动时 `RECONCILE_STUCK_ON_BOOT=1` 的 reconcile 日志
- [x] 检查 `api/apps/restful_apis/dataset_api.py` 路由冲突：`DELETE /datasets/<id>/graph` 已新增用于前端删除按钮；`unbindPipelineTask` 保持 `/datasets/<id>/index?type=` 风格以避免冲突
- [ ] 测试 OpenSearch KNN 路径在 Infinity/ES 后端下的降级行为

---

## 更新规则

每次修改 `E:/Library/ragflow` 中的代码时，请按以下步骤更新本文件：

1. 在「文件修改清单」表格中找到对应文件，更新「改动摘要」、「验证状态」、「备注/TODO」。
2. 如果是新增文件，在表格末尾新增一行。
3. 如果修改涉及环境变量开关，同步更新「关键环境变量开关」表格。
4. 完成验证后，在「验证记录」表格中追加一行。
5. 有新的待处理事项时，在「待处理事项」中追加 TODO。
