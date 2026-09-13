# RAGFlow dev 分支修改记录与语义漂移检查清单

> 基准：`main` = 官方 v0.26.1 release；`dev` = v0.26.1 + GraphRAG 增量优化定制。
>
> 本文档两大用途：
> 1. **第一节：语义漂移检查清单** —— 每次合并官方新版后逐项 `git diff` 核对，这是本分支维护的核心动作；
> 2. 第二~四节：修改清单、环境变量开关等参考资料。
>
> 最近全面修订：2026-07-16（按 extras/patches 架构现状重写，替代 2026-06-18 的旧版移植记录）。
>
> 2026-09-12：第二轮全面审查并完成修复（详见第六节）；OpenSearch 后端弃用，`common/doc_store/opensearch_conn_extras.py`、KNN 消解路径及 4 个 KNN 环境变量整体删除。

---

## 一、语义漂移检查清单（合并官方新版后必做）

extras/`*_patch` 文件不会与上游产生文本冲突，但其中的**官方逻辑副本和实现假设**会静默过时——git merge 不会为它们报任何冲突。

合并 main 进 dev 后，对下表每一项执行：

```bash
git diff <旧tag>..<新tag> -- <官方文件>
```

有输出就必须人工核对对应定制代码是否需要同步。核对完在「最近核对」列记日期。

| # | 定制位置 | 复制/依赖的官方逻辑 | 需 diff 的官方文件 | 风险等级 | 最近核对 |
|---|----------|--------------------|-------------------|---------|---------|
| 1 | `rag/graphrag/general/index_extras.py` `run_graphrag_for_kb` | 官方同名编排函数（`index.py:257`）近全文复制加增量分支，上游编排层 bugfix 不传播 | `rag/graphrag/general/index.py` | **高** | 2026-09-12 |
| 2 | `index_extras.py` `resolve_entities_incremental` + `index_patch.py` wrapper | 官方 `resolve_entities`（`index.py:758`）调用约定（8 位置参数 + `task_id=`/`entity_types=`）| 同上 | **高**（曾因签名漂移致 TypeError，2026-07-16 已修复并加回归测试） | 2026-09-12 |
| 3 | `index_patch.py` `_wrap_extract_community` | 官方 `extract_community`（`index.py:804`）形参表 | 同上 | 低（2026-09-12 已改为 `inspect.signature` 按名绑定，漂移时打 WARNING 而非静默错位） | 2026-09-12 |
| 4 | `rag/svr/task_executor_extras.py` | `sys.modules` 模块查找（生产为 `__main__`）；`te.CONSUMER_NAME` 依赖 `__main__` 块赋值（有兜底默认值） | `rag/svr/task_executor.py` | **高** | 2026-09-12 |
| 5 | `api/apps/services/dataset_api_service_extras.py` `get_knowledge_graph` | 整函数替换官方实现；截断策略已偏离（边 128→512、丢孤立节点、无向去重） | `api/apps/services/dataset_api_service.py` | 中（上游改进被屏蔽） | 2026-09-12 |
| 6 | `rag/graphrag/document_delete_extras.py` | 官方文档删除 GraphRAG 清理流程副本（增量路径条件化；2026-09-12 起 subgraph 的 `must_not` 兜底与官方对齐，显式 ID 列举改用 scroll 分页） | `api/db/services/document_service.py` | 中 | 2026-09-12 |
| 7 | `common/doc_store/es_conn_extras.py` | 遍历 `@singleton` 闭包 cell 定位 ESConnection 类（2026-09-12 起按 `__module__` 匹配，不再硬编码类名）；注入 `count`/`search_with_scroll`/insert 包装 | `rag/utils/es_conn.py`、`common/decorator.py`（`singleton`） | 低（有 RuntimeError 兜底） | 2026-09-12 |
| 8 | `rag/llm/siliconflow_timeout_patch.py` | 替换 `SILICONFLOWEmbed._call`，假定其内部超时处理结构不变 | `rag/llm/embedding_model.py`（`SILICONFLOWEmbed`） | 低（有子类守卫） | 2026-09-12 |
| 9 | `rag/utils/redis_conn_patch.py` | 替换 `RedisDistributedLock.spin_acquire`（依赖官方 `delete_if_equal` 锁协议）；新增 `RedisDB.ttl` | `rag/utils/redis_conn.py` | 中（无 hasattr 守卫，上游同名改动被静默覆盖） | 2026-09-12 |
| 10 | `rag/graphrag/entity_resolution.py` 尾部 CUSTOM 块 | import 时类重绑定 `EntityResolution`→`IncrementalEntityResolution`；依赖「CUSTOM 块位于文件末尾」的执行顺序，且与 `entity_resolution_extras.py` 存在循环 import | `rag/graphrag/entity_resolution.py` | 中（上游在文件尾部新增代码即打破） | 2026-09-12 |
| 11 | `rag/graphrag/utils.py` `set_graph` 无条件委托 | 官方原实现改名 `_set_graph_impl`，extras 包装后按 flag 路由 | `rag/graphrag/utils.py` | 低 | 2026-09-12 |

> 2026-09-12：OpenSearch 后端弃用，原第 7 项 `common/doc_store/opensearch_conn_extras.py` 已整体删除（该文件 monkey-patch 从未真正生效——patch 装在了 `@singleton` 工厂闭包而非真类上）；上游官方文件 `rag/utils/opensearch_conn.py` 保持原样不动。

---

## 二、官方文件修改清单（15 个）

### patch 管理内（12 个，对应 `patches/*.patch`）

| 官方文件 | 改动摘要 | patch 文件 |
|---------|---------|-----------|
| `.gitignore` | 忽略 `docker-compose.override.yml`、`*diff*.txt`、`docs/graphrag_review` | `.gitignore.patch` |
| `api/apps/restful_apis/dataset_api.py` | 新增 `delete_knowledge_graph` 转发函数（修复 main 上 `backward_compat.py` 废弃端点的坏引用） | `api_apps_restful_apis_dataset_api.py.patch` |
| `api/apps/services/dataset_api_service.py` | `get_knowledge_graph` 委托 extras；删图关键词增加 `merge_state` | `api_apps_services_dataset_api_service.py.patch` |
| `api/db/services/document_service.py` | 删文档的 GraphRAG 清理逻辑抽至 `document_delete_extras.py`（官方路径逐字保留） | `api_db_services_document_service.py.patch` |
| `common/settings.py` | `init_settings()` 中安装 ES extras 与删除审计两个 hook（带异常兜底；OS extras hook 已于 2026-09-12 随 OS 后端弃用移除） | `common_settings.py.patch` |
| `rag/graphrag/entity_resolution.py` | 尾部 CUSTOM 块：flag 开启时类重绑定 + 额外导出 | `rag_graphrag_entity_resolution.py.patch` |
| `rag/graphrag/general/index.py` | 尾部调用 `index_patch.apply_patch()` | `rag_graphrag_general_index.py.patch` |
| `rag/graphrag/utils.py` | `does_graph_contains`/`get_graph` flag 路由；`set_graph` 无条件委托 extras；书籍/章节跳过 embedding | `rag_graphrag_utils.py.patch` |
| `rag/svr/task_executor.py` | 尾部 import 并调用 `patch_task_executor()` | `rag_svr_task_executor.py.patch` |
| `web/src/locales/en.ts` | 新增 `datasetOverview.totalChunks` 文案 | `web_src_locales_en.ts.patch` |
| `web/src/locales/zh.ts` | 同上（中文） | `web_src_locales_zh.ts.patch` |
| `web/src/pages/dataset/dataset-overview/index.tsx` | Total chunks 统计卡、grid-cols-4（带 CUSTOM 标记） | `web_src_pages_dataset_dataset-overview_index.tsx.patch` |

### patch 管理外（3 个，均无需/无法纳入 patch）

| 官方文件 | 说明 |
|---------|------|
| `uv.lock` | 本地 uv 0.11.x + `pyproject.toml` 已钉住的 aliyun 镜像重新生成所致（registry URL 与字段格式差异）。**不是手工修改**，合并策略见 `MAINTENANCE.md`「官方更新时的标准流程」第 3 步 |
| `web/src/components/paddleocr-options-form-field.tsx` | 良性差异：lint-staged 钩子的 prettier 输出（数组拆行、长调用换行），无功能改动；**不可还原**——还原后下次提交会被钩子自动改回。已接受现状，合并遇冲突取任一侧均可 |
| `web/src/pages/user-setting/sidebar/index.tsx` | 良性差异：同上（import 字母序） |

---

## 三、新增自定义文件（不与上游冲突）

| 分组 | 文件 | 用途 |
|-----|------|------|
| GraphRAG 核心 | `rag/graphrag/config.py` | 全部环境变量开关集中配置（import 时顺带安装 redis/siliconflow 两个 patch） |
| | `rag/graphrag/general/index_extras.py` | 增量编排、ChapterGraph、增量 merge/消解、异步 community、全局 PageRank 重算 |
| | `rag/graphrag/general/index_patch.py` | monkey-patch 调度器：保存原函数、按 flag 路由 |
| | `rag/graphrag/utils_extras.py` | 增量存储层：set_graph_delta、merge_state、在线组图、可视化 256 节点策略、拓扑专用加载（PageRank 用） |
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
| 测试 | `test/unit_test/rag/graphrag/test_graphrag_utils_extras.py` | 书籍/章节跳过 embedding（当前唯一测试） |
| 运维 | `docker/finisher/`（3 个文件） | 卡死任务收尾脚本（默认 dry-run）+ README + 辅助 SQL |
| 管理 | `patches/`（12 patch + apply/regenerate 脚本 + README） | 官方文件改动的双轨管理 |
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
| `GRAPHRAG_SET_GRAPH_STREAM_WINDOW` | `256` | `set_graph_delta` 流式 embed→insert 的窗口大小，避免大变更集的全部向量一次性驻留内存导致 executor OOM；删除（removed + 预删旧版本）已前移到嵌入/插入之前，中途崩溃由 merge 重试自愈 |
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

### 仍未处理（第二轮审查复核确认）

- **resume 空指针**、`does_graph_contains` 官方 legacy 路径 size=1 假阴性、search_after 无 PIT、局部 PageRank 污染全局 rank：与第一轮记录一致。
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
