# RAGFlow dev 分支修改记录与语义漂移检查清单

> 基准：官方标签 `v0.26.4`；`dev-0.26` = v0.26.4 + GraphRAG 增量优化定制。补丁直接与标签比较。
>
> 本文档两大用途：
> 1. **第一节：语义漂移检查清单** —— 每次合并官方新版后逐项 `git diff` 核对，这是本分支维护的核心动作；
> 2. 第二~四节：修改清单、环境变量开关等参考资料。
>
> 最近升级核对：2026-10-28（v0.26.4 合并，保留现有 extras/patches 架构）。
>
> 2026-09-12：第二轮全面审查并完成修复（详见第六节）；OpenSearch 后端弃用，`common/doc_store/opensearch_conn_extras.py`、KNN 消解路径及 4 个 KNN 环境变量整体删除。

---

## 一、语义漂移检查清单（合并官方新版后必做）

extras/`*_patch` 文件不会与上游产生文本冲突，但其中的**官方逻辑副本和实现假设**会静默过时——git merge 不会为它们报任何冲突。

合并官方标签后，对下表每一项执行：

```bash
git diff <旧tag>..<新tag> -- <官方文件>
```

有输出就必须人工核对对应定制代码是否需要同步。核对完在「最近核对」列记日期。

| # | 定制位置 | 复制/依赖的官方逻辑 | 需 diff 的官方文件 | 风险等级 | 最近核对 |
|---|----------|--------------------|-------------------|---------|---------|
| 1 | `rag/graphrag/general/index_extras.py` `run_graphrag_for_kb` | 官方同名编排函数（`index.py:257`）近全文复制加增量分支，上游编排层 bugfix 不传播；增量恢复允许空图，社区阶段在消解后及每次重试时读取完整图 | `rag/graphrag/general/index.py` | **高** | 2026-10-28 |
| 2 | `index_extras.py` `resolve_entities_incremental` + `index_patch.py` wrapper | 官方 `resolve_entities`（`index.py:758`）调用约定（8 位置参数 + `task_id=`/`entity_types=`）| 同上 | **高**（曾因签名漂移致 TypeError，2026-07-16 已修复并加回归测试） | 2026-10-28 |
| 3 | `index_patch.py` `_wrap_extract_community` | 官方 `extract_community`（`index.py:804`）形参表 | 同上 | 低（2026-09-12 已改为 `inspect.signature` 按名绑定，漂移时打 WARNING 而非静默错位） | 2026-10-28 |
| 4 | `rag/svr/task_executor_extras.py` | `sys.modules` 模块查找（生产为 `__main__`）；`te.CONSUMER_NAME` 依赖 `__main__` 块赋值（有兜底默认值）；后处理按 KB 的 `embd_id` 选择模型，持有知识库锁后读图 | `rag/svr/task_executor.py` | **高** | 2026-10-28 |
| 5 | `api/apps/services/dataset_api_service_extras.py` `get_knowledge_graph` | 整函数替换官方实现；截断策略已偏离（边 128→512、丢孤立节点、无向去重） | `api/apps/services/dataset_api_service.py` | 中（上游改进被屏蔽） | 2026-10-28 |
| 6 | `rag/graphrag/document_delete_extras.py` | 官方文档删除 GraphRAG 清理流程副本（增量路径按 `source_id` 清理，不依赖 graph JSON；2026-10-27 起同步清理字段和 JSON 内来源） | `api/db/services/document_service.py` | 中 | 2026-10-28 |
| 7 | `common/doc_store/es_conn_extras.py` | 遍历 `@singleton` 闭包 cell 定位 ESConnection 类（2026-09-12 起按 `__module__` 匹配，不再硬编码类名）；注入 `count`/`search_with_scroll`/insert 包装（scroll 初始请求应用 `_source` 投影并保留 `_id`） | `rag/utils/es_conn.py`、`common/decorator.py`（`singleton`） | 低（有 RuntimeError 兜底） | 2026-10-28 |
| 8 | `rag/llm/siliconflow_timeout_patch.py` | 替换 `SILICONFLOWEmbed._call`，假定其内部超时处理结构不变 | `rag/llm/embedding_model.py`（`SILICONFLOWEmbed`） | 低（有子类守卫） | 2026-10-28 |
| 9 | `rag/utils/redis_conn_patch.py` | 替换 `RedisDistributedLock.spin_acquire`（依赖官方 `delete_if_equal` 锁协议）；新增 `RedisDB.ttl` | `rag/utils/redis_conn.py` | 中（无 hasattr 守卫，上游同名改动被静默覆盖） | 2026-10-28 |
| 10 | `rag/graphrag/entity_resolution.py` 尾部 CUSTOM 块 | import 时类重绑定 `EntityResolution`→`IncrementalEntityResolution`；依赖「CUSTOM 块位于文件末尾」的执行顺序，且与 `entity_resolution_extras.py` 存在循环 import | `rag/graphrag/entity_resolution.py` | 中（上游在文件尾部新增代码即打破） | 2026-10-28 |
| 11 | `rag/graphrag/utils.py` `set_graph` 无条件委托 | 官方原实现改名 `_set_graph_impl`，extras 包装后按 flag 路由；全量写入也必须强制刷新，全图查询截断异常不得回退旧 JSON | `rag/graphrag/utils.py` | 低 | 2026-10-28 |

> 2026-09-12：OpenSearch 后端弃用，原第 7 项 `common/doc_store/opensearch_conn_extras.py` 已整体删除（该文件 monkey-patch 从未真正生效——patch 装在了 `@singleton` 工厂闭包而非真类上）；上游官方文件 `rag/utils/opensearch_conn.py` 保持原样不动。

---

## 二、官方文件修改清单（22 个）

### patch 管理内（21 个，对应 `patches/*.patch`）

| 官方文件 | 改动摘要 | patch 文件 |
|---------|---------|-----------|
| `.gitignore` | 忽略 `docker-compose.override.yml`、`*diff*.txt`、`docs/graphrag_review` | `.gitignore.patch` |
| `.gitattributes` | 补丁默认 LF；上游 CRLF 测试的补丁保留原字节，确保上下文可应用 | `.gitattributes.patch` |
| `api/apps/restful_apis/dataset_api.py` | 新增 `delete_knowledge_graph` 转发函数（修复 main 上 `backward_compat.py` 废弃端点的坏引用） | `api_apps_restful_apis_dataset_api.py.patch` |
| `api/apps/services/dataset_api_service.py` | `get_knowledge_graph` 委托 extras；删图关键词增加 `merge_state` | `api_apps_services_dataset_api_service.py.patch` |
| `api/db/services/document_service.py` | 删文档的 GraphRAG 清理委托 extras；legacy 和增量路径均排除编译数据 | `api_db_services_document_service.py.patch` |
| `common/settings.py` | `init_settings()` 中安装 ES extras 与删除审计两个 hook（带异常兜底；OS extras hook 已于 2026-09-12 随 OS 后端弃用移除） | `common_settings.py.patch` |
| `rag/graphrag/entity_resolution.py` | 尾部 CUSTOM 块：flag 开启时类重绑定 + 额外导出 | `rag_graphrag_entity_resolution.py.patch` |
| `rag/graphrag/general/index.py` | 尾部调用 `index_patch.apply_patch()` | `rag_graphrag_general_index.py.patch` |
| `rag/graphrag/utils.py` | `does_graph_contains`/`get_graph` flag 路由；`set_graph` 无条件委托 extras；书籍/章节跳过 embedding | `rag_graphrag_utils.py.patch` |
| `rag/svr/task_executor.py` | 尾部 import 并调用 `patch_task_executor()` | `rag_svr_task_executor.py.patch` |
| `rag/utils/es_conn.py` | 仅纯 GraphRAG bulk 不等待刷新；编译/普通写入 wait_for；delete 支持 refresh；多字段缺失条件 | `rag_utils_es_conn.py.patch` |
| `test/unit_test/api/apps/services/test_delete_datasets.py` | 隔离自定义 service import 的 Quart 包和 NLP 包，保持删除测试独立运行 | `test_unit_test_api_apps_services_test_delete_datasets.py.patch` |
| `web/src/locales/en.ts` | 新增 `datasetOverview.totalChunks` 文案 | `web_src_locales_en.ts.patch` |
| `web/src/locales/zh.ts` | 同上（中文） | `web_src_locales_zh.ts.patch` |
| `web/src/pages/dataset/dataset-overview/index.tsx` | Total chunks 统计卡、grid-cols-4（带 CUSTOM 标记） | `web_src_pages_dataset_dataset-overview_index.tsx.patch` |
| `web/src/pages/user-setting/setting-model/modal/provider-modal/field-config/provider-config-map.ts` | 火山引擎 Coding/标准 API 地址选择与提交 | `web_src_pages_user-setting_setting-model_modal_provider-modal_field-config_provider-config-map.ts.patch` |
| `common/doc_store/infinity_conn_base.py` | 支持组合多个缺失字段条件，避免覆盖原 source_id 条件 | `common_doc_store_infinity_conn_base.py.patch` |
| `common/doc_store/ob_conn_base.py` | 支持组合多个缺失字段条件，避免覆盖原 source_id 条件 | `common_doc_store_ob_conn_base.py.patch` |
| `rag/graphrag/search.py` | GraphRAG 检索排除编译数据 | `rag_graphrag_search.py.patch` |
| `rag/utils/ob_conn.py` | 支持组合多个缺失字段条件，避免覆盖原 source_id 条件 | `rag_utils_ob_conn.py.patch` |
| `rag/utils/opensearch_conn.py` | 支持组合多个缺失字段条件，避免覆盖原 source_id 条件 | `rag_utils_opensearch_conn.py.patch` |

### patch 管理外（1 个格式差异）

| 官方文件 | 说明 |
|---------|------|
| `web/src/pages/user-setting/sidebar/index.tsx` | 良性差异：import 字母序，无功能改动 |

`uv.lock` 使用官方 v0.26.4 为起点，经 uv 0.13.6 离线重新锁定和检查，解析 665 个包，与官方锁文件一致。旧版 PaddleOCR 表单的格式差异已被上游接收，本次合并后无差异。

---

## 三、新增自定义文件（不与上游冲突）

| 分组 | 文件 | 用途 |
|-----|------|------|
| GraphRAG 核心 | `rag/graphrag/config.py` | 全部环境变量开关集中配置（import 时顺带安装 redis/siliconflow 两个 patch） |
| | `rag/graphrag/general/index_extras.py` | 增量编排、ChapterGraph、增量 merge/消解、异步 community、全局 PageRank 重算 |
| | `rag/graphrag/general/index_patch.py` | monkey-patch 调度器：保存原函数、按 flag 路由 |
| | `rag/graphrag/utils_extras.py` | 增量存储层：set_graph_delta、merge_state、在线组图、可视化 256 节点策略、拓扑专用加载（PageRank 用） |
| | `rag/graphrag/storage_scope.py` | GraphRAG 条件及原生查询排除 compile_kwd，隔离编译记录 |
| | `rag/graphrag/utils_pagination.py` | search_after + `_doc` tiebreaker 分页（仅 ES/OS 后端） |
| | `rag/graphrag/entity_resolution_extras.py` | 增量实体消解（excluded_types、候选注入、数字 2-gram 规则） |
| | `rag/graphrag/document_delete_extras.py` | 文档删除时的 KG 清理（显式 ID 防误删） |
| 任务执行 | `rag/svr/task_executor_extras.py` | 卡死任务 reconcile、心跳、KG 后处理 Stream 消费者 |
| 存储/连接 | `common/doc_store/es_conn_extras.py` | ES count/scroll 注入与 insert 包装（`opensearch_conn_extras.py` 已于 2026-09-12 删除） |
| | `common/doc_store_audit.py` | `docStoreConn.delete` 审计钩子（KG 产物删除留痕） |
| | `rag/utils/redis_conn_patch.py` | `RedisDB.ttl`、可取消的 `spin_acquire` |
| | `rag/llm/siliconflow_timeout_patch.py` | SiliconFlow Embedding 超时可配置 |
| API | `api/apps/restful_apis/dataset_api_extras.py` | `DELETE /api/v1/datasets/<id>/graph` 路由注册 |
| | `api/apps/services/dataset_api_service_extras.py` | KG 可视化装配与截断策略 |
| 前端 | `web/src/pages/dataset/dataset-overview/hook-extras.ts` | 分块总数统计数据源 |
| | `web/src/assets/svg/data-flow/total-chunks-icon{,-bri}.svg` | 统计卡图标（暗/亮主题） |
| 测试 | `test/unit_test/rag/graphrag/` | GraphRAG 工具、分页、checkpoint、拓扑、增量写入/删除/消解回归测试；`test_review_findings.py` 覆盖五项审查问题及失败边界 |
| 运维 | `docker/finisher/`（3 个文件） | 卡死任务收尾脚本（默认 dry-run）+ README + 辅助 SQL |
| 管理 | `patches/`（21 patch + apply/regenerate 脚本 + README） | 官方文件改动的双轨管理 |
| | `MAINTENANCE.md`、本文档 | 维护规范与漂移清单 |

---

## 四、关键环境变量开关

> 开关类环境变量代码层面默认 `0`（关闭，`rag/graphrag/config.py`），部署环境（`.env.local` / compose override）中按需开启；数值类参数保持合理默认值。

### Phase 1：增量图构建
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_INCREMENTAL_GRAPH` | `0` | 图写入：1=delta 增量写入，0=官方默认全量写入 |

### Phase 2：增量 Merge
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_INCREMENTAL_MERGE` | `0` | Merge 阶段：1=仅合并新增 subgraph，0=官方默认全图 merge |
| `GRAPHRAG_MERGE_TIMEOUT_SECONDS` | `1800` | Merge 阶段单步超时（秒）；官方 `index.py` 的默认值由 `index_patch.apply_patch()` 在运行时覆写，`index_extras.py` 自带同默认值 |
| `GRAPHRAG_SET_GRAPH_STREAM_WINDOW` | `256` | `set_graph_delta` 流式 embed→insert 的窗口大小，避免大变更集的全部向量一次性驻留内存导致 executor OOM；2026-10-27 起更新采用原 ID 覆盖写、新记录采用稳定 ID，所有写入及必要刷新成功后才执行明确移除项 |
| `GRAPHRAG_SET_GRAPH_PIPELINE_DEPTH` | `1` | 流式窗口的流水线深度：最多 d 个窗口同时在入库、下一窗口同时嵌入；内存峰值约 (d+1) 个窗口，ES 在途 bulk 请求数约 4×d（每窗口带 `GRAPHRAG_INSERT_CONCURRENCY` 路并发） |

### Phase 2.5：增量合并后全局 PageRank 重算
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `RECALC_GLOBAL_PAGERANK_AFTER_MERGE` | `0` | 增量 merge 全部完成后重算全局 PageRank 并写回 entity chunks。需同时开启 `USE_INCREMENTAL_MERGE=1`；仅 ES 后端（`search_with_scroll`/`update_docs`/`search_after` 可用）实际生效。2026-09-12 起为拓扑加载（不取 description，内存约降一个量级）+ 实体/关系 scroll 截断显式检测（截断即放弃）；`GRAPHRAG_SEARCH_WITH_SCROLL_HITS_CAP` 必须大于 max(节点数, 关系数) |

### Phase 3：增量实体消解
| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `USE_INCREMENTAL_RESOLUTION` | `0` | 实体消解：1=按 entity_type 分批消解，0=官方默认全图消解（候选召回为字符级过滤；OpenSearch KNN 路径及 `USE_KNN_FOR_RESOLUTION`/`ENTITY_RESOLUTION_TOP_K`/`ENTITY_RESOLUTION_SIM_THRESHOLD`/`ENTITY_RESOLUTION_KNN_CONCURRENCY` 已于 2026-09-12 随 OS 弃用移除） |
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
| `GRAPHRAG_MAX_PARALLEL_DOCS` | `4` | 每个 GraphRAG 任务并行处理的最大文档数 |
| `USE_CHAPTER_GRAPH` | `0` | 是否在 subgraph 生成阶段提取书籍/章节实体与关系 |
| `GRAPHRAG_KEEP_SUBGRAPH` | `1` | 重跑时是否保留 subgraph 产物（1=保留/复用，0=清空） |
| `GRAPHRAG_KEEP_MERGE` | `1` | 重跑时是否保留 merge 产物（1=保留/复用，0=清空） |
| `GRAPHRAG_KEEP_RESOLUTION` | `1` | 重跑时是否保留 resolution 产物（1=保留/复用，0=清空） |

---

## 五、已撤回/已放弃项（历史记录）

| 功能 | 说明 | 状态 |
|------|------|------|
| Dealer 白名单修复 | 已还原 `rag/nlp/search.py`，暂不处理 | ⏸️ 已撤回 |
| Infinity `count()` | 决定不移植 | ⏸️ 已放弃 |
| 批量 Entity/Edge 摘要 | 决定不移植 | ⏸️ 已放弃 |
| `common/connection_utils.py` 超时兜底修复 | 决定保持官方原样 | ⏸️ 已放弃 |
| `rag/graphrag/utils.py` Dealer 绕过逻辑 | `is_doc_merged` 已改为基于 `query_merge_state()`（merge_state 表）判断，不再经 Dealer 查 `source_id`，绕过逻辑不再必要 | ⏸️ 已放弃 |
| 前端删除图谱相关修改 | 官方 v0.26.x 前端已自带删除按钮，无需额外修改 | ⏸️ 无需处理 |

---

## 六、待验证事项与已知问题

### 已知问题（2026-07-16 全分支审查发现；4 个严重项已于同日修复）

1. ~~**`resolve_entities_incremental` 签名与全部调用点不兼容**~~ **已修复**：增量实现签名与官方 `resolve_entities` 完全对齐（8 位置参数 + `task_id=`/`entity_types=`），wrapper 回退官方实现时剔除 `entity_types`（`index_patch.py`）。回归测试：`test_incremental_bugfixes.py::TestResolveEntitiesCallConvention`。
2. ~~**`patch_task_executor()` 在生产启动方式下静默不生效**~~ **已修复**：新增 `_get_task_executor_module()`，`rag.svr.task_executor` 查不到时回退 `__main__`（生产脚本启动方式）；仍找不到时打 WARNING 而非静默 return；安装成功打 INFO；`CONSUMER_NAME` 提供模块级兜底默认值。回归测试：`TestTaskExecutorModuleLookup`。
3. ~~**flag 组合 `USE_INCREMENTAL_MERGE=1` + `USE_INCREMENTAL_GRAPH=0` 导致全图数据丢失**~~ **已修复**：`GraphRAGConfig.normalize_flag_combinations()`（import 时与 `reload()` 时执行）在 MERGE/RESOLUTION 开启而 GRAPH 关闭时自动升级 GRAPH 并打 ERROR 日志提醒修正环境变量。回归测试：`TestFlagCombinationNormalization`。
4. ~~**KG-PP 分布式锁互斥失效**~~ **已修复**：`lock_value` 改为唯一值 `kg_pp:{task_id}`，与主流程 `batch_merge:{task_id}` 的模式一致，官方 `delete_if_equal` 不再误删他人持锁。

### 2026-09-12 第二轮审查（全部 extras 文件）已修复项

5. ~~**OpenSearch extras 静默失效**~~：`opensearch_conn_extras.py` 的 monkey-patch 装在 `@singleton` 工厂闭包上而非真类，5 个注入方法从未生效但日志报 installed。**随 OS 后端弃用整体删除**（文件、settings 钩子、KNN 消解路径、`USE_KNN_FOR_RESOLUTION`/`ENTITY_RESOLUTION_TOP_K`/`ENTITY_RESOLUTION_SIM_THRESHOLD`/`ENTITY_RESOLUTION_KNN_CONCURRENCY` 四个环境变量）；OS 官方 insert 静默失败缺陷随之消失。
6. ~~**心跳键与任务状态弱一致**~~：心跳初始写失败后 `xx=True` 续约永远无法自愈，且 cancel 路径（官方 `set_progress` 抛 `TaskCanceledException`）心跳键残留成幽灵心跳。已修复：写失败回滚 `_current_task_id`、cancel 路径清理心跳、心跳循环加 Redis None 守卫。
7. ~~**删文档增量路径 subgraph 清理缺兜底**~~：增量路径 `kg_types` 补回 `"subgraph"`（恢复官方 `must_not exists source_id` 安全网）；显式 ID 列举的 10000 硬上限改为 scroll 分页，无 scroll 后端保留 cap 并打 ERROR。
8. ~~**增量消解吞 `TaskCanceledException`**~~：取消后所有批次空转到结束。已改为重抛，gather 立即中止。
9. ~~**增量消解 checkpoint 是死代码**~~（调用方从不传 `checkpoints`/`save_checkpoint`）：已接上 `load_checkpoints`/`save_checkpoint`（与官方 `RESOLUTION_CHECKPOINT` 一致），中断后可续跑。
10. ~~**`_wrap_extract_community` 按 `args[6]`/`args[7]` 位置取参**~~：已改为 `inspect.signature` 按名绑定，绑定失败打 WARNING 而非静默错位。
11. ~~**`index_patch.apply_patch` 无幂等守卫**~~：重复调用会把 `_ORIGINALS` 覆盖成已包装版本。已加 `_graphrag_index_patch_applied` 标记。
12. ~~**`recalc_global_pagerank` 每次重试都重新加载全图（大 KB OOM 风险）**~~：改为单次尝试、失败非致命（rank 保持陈旧至下次 merge）。
13. ~~**`write_merge_state` 写后无 refresh**~~（并发 retry 读到陈旧状态）：末尾加 `_post_insert_refresh`；`state="failed"` 写入失败不再掩盖原始 merge 异常。
14. ~~**`get_graph_from_index_for_visualization` 静默覆写 `max_nodes` 为 256**~~：按入参分配 centers/neighbors 预算，`max_nodes` 作为硬上限生效。
15. ~~**`does_graph_contains` 单 round-trip 只覆盖 OpenSearch**~~：已切到 ES 后端（`.es`），ES 同样享受 bool.should 单 round-trip。
16. ~~**ChapterGraph DEBUG 日志刷用户可见进度**~~：改为 `logging.debug`；~~实体-章节子串匹配假阳性~~（"AI" 匹配 "fail"）：加 ASCII 词边界守卫，CJK 子串语义不变。
17. ~~**可视化 `protected_types` 与孤立节点丢弃矛盾**~~（章节节点静默消失）：保护类型免于孤立过滤；边/节点字段缺失改 `.get` 兜底防 KeyError。
18. ~~**`es_conn_extras` 闭包定位硬编码类名 `ESConnection`**~~：改为优先按 `__module__` 匹配。
19. ~~**`doc_store_audit` `_caller_location(skip_frames=3)` 多跳一帧**~~：改为 2，审计日志定位到真实调用点。
20. **全局 PageRank 重算大 KB 改造**：图加载改为拓扑专用加载器 `get_graph_topology_from_index`（只取 `entity_kwd`/`entity_type_kwd`/`from_entity_kwd`/`to_entity_kwd`，不取 description，内存约降一个量级）；`_es_search_with_scroll` 返回新增 `_truncated` 标记，实体或关系 scroll 被 `hits_cap` 截断时重算显式放弃（修掉原"边截断无守卫"缺口）；实体写回扫描改为 `search_after` 流式分页 + 窗口化 flush，不再受 `hits_cap` 限制。回归测试：`test_graphrag_topology.py`。

### 2026-10-27 增量一致性修复

21. **写入前预删导致历史属性丢失**：移除实体/关系的更新前删除；查询保留存储 ID，已有记录按原 ID bulk index 覆盖，新记录按知识库、记录类型和规范化业务键生成 SHA-256 稳定 ID。embedding、bulk 写入或必要刷新失败时保留旧数据，重试不会重复插入。显式 `GraphChange` 移除项只在全部更新写入及刷新成功后删除。
22. **读写与无向边不一致**：文档 merge 和 delta 写入前的必要刷新不再受性能开关控制，失败进入上层重试；关系查询覆盖历史两种存储方向并统一 `get_from_to` 业务键，ID 生成、写入和按 ID 删除使用相同规范化。查询异常、非法 JSON、同业务键重复记录和分页截断均失败关闭，不自动修复历史数据。
23. **文档删除遗漏 delta 数据**：增量清理不再要求旧式 graph JSON 或 `chunk_num > 0`，按知识库与文档 `source_id` 删除 subgraph/merge_state，并同步清理记录字段及 JSON 内的来源；共享记录有剩余来源时保留，无剩余来源时删除。非增量路径行为不变。
24. **抽取类型误排除消解及同批候选遗漏**：保留 `entity_types` 调用签名但不作为排除配置；默认仅排除书籍、章节及已有英文别名。候选覆盖“新增—已有”和“新增—新增”，排除自身并排序去重，首次构建和异步全图输入走同一候选逻辑，checkpoint 批次顺序稳定。
25. **scroll 未应用字段投影**：初始请求即设置 `_source`，仅请求 `_id` 时关闭 `_source`，保留 hit 元数据 ID 并深复制查询体，避免完整描述/向量进入结果累积以及修改调用方参数。

上述改动的函数级回归位于 `test/unit_test/rag/graphrag/test_incremental_integrity.py`。2026-10-27 隔离环境中定向回归 29 项、GraphRAG 目录回归 170 项通过；本机无可用 Elasticsearch，未执行临时索引集成验证。本次未新增公共 HTTP API、数据库表、索引 mapping 或环境变量，未重建/迁移历史图谱。

### 2026-10-28 五项审查问题修复

26. **默认全量写入缺少可见性保证**：`_set_graph_monolithic` 写后执行强制刷新，不受 `SET_GRAPH_DELTA_REFRESH_AFTER_INSERT` 控制；刷新返回 `False` 或抛异常均向上传播。不提供刷新接口的后端保持现有行为。
27. **增量恢复对空图调用 copy**：增量消解直接使用子图节点集合查询索引，允许图参数为 `None`；官方消解及无子图节点的恢复路径先加载持久化图，缺失时失败，不设置完成标记。
28. **异步后处理选错嵌入模型**：先读取知识库详情，优先使用 `embd_id`；仅成功读取且未配置该字段时使用租户默认模型。知识库或指定模型不可用时中止，保持现有失败 ack 策略，不静默切换模型。
29. **社区提取使用消解前的图**：同步与异步后处理均在消解后读取最新全图，社区重试重新加载；索引社区路径不再以 `source_id` 数量判断图是否完整或最新。异步消费者在持有知识库锁后读图；缺失或加载失败不设置社区完成标记。
30. **全图查询忽略截断**：实体与关系 scroll 均检查 `_truncated`，抛出包含知识库及记录类别的 `GraphTopologyTruncatedError`；`get_graph` 透传该异常，不回退旧 JSON。保留现有查询上限与其他后端错误的兼容回退。

回归测试位于 `test/unit_test/rag/graphrag/test_review_findings.py`，包含原有 11 项复现/对照测试及刷新失败、模型选择、恢复、社区重试、持锁读取、截断失败和无 scroll 后端兼容边界。2026-10-28 在 `.venv-review` 中运行 GraphRAG 目录：210 项通过（原有 170 项 + 本组 40 项）。仅使用服务/模型替身执行真实业务函数，不等同于真实 Elasticsearch 集成验证。未新增公共接口、数据结构或配置开关，未自动重建历史图、向量或社区报告。

### 仍未处理（第二轮审查复核确认）

- `does_graph_contains` 官方 legacy 路径 size=1 假阴性、search_after 无 PIT、局部 PageRank 污染全局 rank：与第一轮记录一致。resume 空指针已于 2026-10-28 修复。
- **KG-PP 失败/cancel 即 ack 无死信**：第二轮确认 cancel 路径（`task_executor_extras.py` 消费循环内 `has_canceled` 分支）与失败路径同样受影响——任何用户取消都会走此路径；官方 Redis Stream 本身无 DLQ 机制，属于框架级限制。
- 增量消解的 `IncrementalEntityResolution.__init__` 已加 `*args/**kwargs` 前向兼容，但上游若在 `resolve_entities` 中新增调用参数仍需对照第一节第 2 项核对。

### 待验证事项

- [ ] 端到端集成测试：`USE_INCREMENTAL_GRAPH=1 USE_INCREMENTAL_MERGE=1 USE_INCREMENTAL_RESOLUTION=1`
- [ ] 验证 `_record_lock_metric` Redis hash 写入
- [ ] 验证启动时 `RECONCILE_STUCK_ON_BOOT=1` 的 reconcile 日志
- [x] 检查 `api/apps/restful_apis/dataset_api.py` 路由冲突：`DELETE /datasets/<id>/graph` 已新增用于前端删除按钮；`unbindPipelineTask` 保持 `/datasets/<id>/index?type=` 风格以避免冲突

---

## 更新规则

每次修改本仓库代码时，按以下规则更新本文档：

1. **动了 extras/patch 中的官方逻辑副本或实现假设** → 更新第一节清单对应行（「最近核对」列同步刷新）。
2. **动了官方文件** → 更新第二节清单，并重跑 `bash patches/regenerate_patches.sh`。
3. **新增自定义文件** → 在第三节表格补一行。
4. **新增/修改环境变量开关** → 同步第四节表格（默认值必须与 `rag/graphrag/config.py` 一致）。
5. **合并官方新版后** → 完成第一节全部核对，刷新「最近核对」列，并更新文档头部基准版本号。


## 八、2026-10-28：合并官方 v0.26.4

- 基线由 v0.26.1 升级至 v0.26.4（`cb93883f3`），升级前提交 `e9d16343f` 保存于 `codex/backup-before-v0.26.4-20261028`。保留全部 GraphRAG 定制及火山引擎端点选择。
- 自定义 `generate_subgraph` 对 LLM 抽取关系调用官方 `_relationship_looks_valid`，过滤否定判断与主体错配；本地生成的书籍/章节结构关系不经过此过滤。
- 全量 `_set_graph_impl` 的节点和关系 embedding 批量预热应用既有 `should_skip_embedding` 规则。书籍/章节及其关联边不调用 embedding；普通实体与关系继续批量处理；增量 delta 写图沿用原有批处理。
- 完成第一节全部 11 项核对：编排/消解/社区接口未漂移；图谱可视化算法未变；文档删除新增导航制品清理已保留，原图谱清理流程未变；ES 官方查询/更新改进与本地刷新策略并存；singleton、SiliconFlow 类和 Redis 锁协议保持兼容；新增 Redis MGET 未被覆盖；实体消解挂载仍在文件末尾。
- 补丁基线直接使用标签，新增属性配置、数据集删除测试隔离和火山引擎表单配置的 patch；生成脚本包含工作区修改。
- 修正补丁检查脚本遗漏隐藏文件的问题：`.gitignore.patch` 和 `.gitattributes.patch` 也参与检查。16 个补丁均在纯净标签副本中应用，并逐文件确认与当前定制内容一致。
- 验证：GraphRAG 220 项通过（新增 10 项），数据集删除 2 项通过；前端生产构建和定制文件 ESLint 通过。前端类型检查在合并版和相同依赖的纯净官方 v0.26.4 上均有相同 223 项错误，没有新增诊断。
- Python 1021 个文件通过语法解析；官方数据集 API 的 32 个函数保持一致，本地删除包装器和路由仍保留。Ruff 修复新增测试与合并 import 排序问题，仍有历史及上游既有诊断；相对官方标签的定制源码差异通过空白检查。完整合并差异中的上游空白诊断未作无关整理。
- 测试使用外部服务替身，不包含真实 ES/Redis/模型集成、生产数据库迁移或部署。spaCy 包已安装供测试导入，但未验证各语言 NER 模型。生产切换前仍需在隔离数据上验证多文档构图、共享来源删除、取消/恢复及可视化检索，并备份数据库与 ES。


## 九、2026-10-28：修复 v0.26.4 合并审查问题

- ES `insert` 仅对全部属于 GraphRAG 且不带 `compile_kwd` 的批次使用 `refresh=false`；编译数据、普通数据和混合批次恢复官方 `wait_for`。Wiki REDUCE 写入后 PLAN 可立即查询中间结果，GraphRAG 保留阶段末显式刷新优化。
- `storage_scope.py` 为 GraphRAG 查询和写入前查找、清理条件增加 `must_not exists compile_kwd`。覆盖 legacy JSON、增量关系/实体、scroll、search_after、拓扑/可视化、消解候选、检索、数据集删图和文档来源清理。编译 API 和编译数据格式不变；已有编译记录也被隔离。
- ES/OpenSearch/Infinity/OceanBase 条件转换接受多个缺失字段，避免隔离条件替换 legacy 清理原有的 `source_id` 条件；OpenSearch 批量更新补充同样的排除支持。
- 新增回归验证实际 Wiki 持久化/读取、ES bulk 策略与查询/更新/删除 DSL、相同业务键的混存图、分页、增量删除、legacy/增量来源清理和两个数据集删图入口。测试使用传输/存储替身；未运行真实 ES 或其他后端服务集成。
- patch 清单扩展为 21 项，新增的官方文件修改纳入 v0.26.4 补丁再生成与字节一致性验证。

验证结果：GraphRAG 237 项 + 数据集删除 2 项，共 239 项通过；本次新增 17 项回归测试。
