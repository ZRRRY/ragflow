# RAGFlow 定制分支修改记录与语义漂移检查清单

> 基准：`main` = 官方 infiniflow/ragflow 最新（跟踪 `origin/main`，当前对齐 v0.27.2）；`dev-0.27` = v0.27.2 + 精简定制（跟踪 `myfork/dev-0.27`）。
>
> 本文档两大用途：
> 1. **第一节：语义漂移检查清单** —— 每次合并官方新版后逐项 `git diff` 核对，这是本分支维护的核心动作；
> 2. 第二~五节：修改清单、环境变量、已删除定制的历史记录。
>
> 2026-09：全面升级 v0.26.1 → v0.27.2。官方知识编译（knowledge compilation）在架构上取代了旧 GraphRAG 增量定制的全部骨架功能，增量图构建/合并、增量实体消解、Community 报告、PageRank 可视化、重跑控制、删除清理、看门狗等约 5000 行定制**整体删除**；ChapterGraph（书籍/章节图谱）迁移为知识编译模板 `chapter_graph.yaml`（模板即配置，无代码漂移面）。被删除模块的历史细节见第五节与 `dev-backup-v0.26.1` 分支。

---

## 一、语义漂移检查清单（合并官方新版后必做）

patch 管理的官方文件在合并时会产生文本冲突（可见、必须解）；但自定义模块中**对官方逻辑的实现假设**会静默过时——git merge 不会为它们报任何冲突。

合并 main 进 dev-0.27 后，对下表每一项执行：

```bash
git diff <旧tag>..<新tag> -- <官方文件>
```

有输出就必须人工核对对应定制代码是否需要同步。核对完在「最近核对」列记日期。

| # | 定制位置 | 复制/依赖的官方逻辑 | 需 diff 的官方文件 | 风险等级 | 最近核对 |
|---|----------|--------------------|-------------------|---------|---------|
| 1 | `rag/llm/siliconflow_timeout_patch.py` | 替换 `SILICONFLOWEmbed._call`，假定其 `_clean_batch`/`_openai_http_embeddings`/`headers`/`base_url` 结构不变 | `rag/llm/embedding_model.py`（`SILICONFLOWEmbed`） | 低（有子类守卫，v0.27.2 已核对结构兼容） | 2026-09 |
| 2 | `common/doc_store_audit.py` | 包装 `docStoreConn.delete`，依赖各 doc store 后端 `delete` 方法签名与 `index_name` 约定 | `rag/utils/es_conn.py`、`rag/utils/infinity_conn.py`、`common/doc_store/` | 低 | 2026-09 |
| 3 | `common/settings.py` CUSTOM hooks（patch 管理） | 假定 `init_settings()` 存在且 `docStoreConn` 在 hook 安装点之前已初始化 | `common/settings.py` | 低（冲突可见；v0.27.2 已核对安装点仍成立） | 2026-09 |
| 4 | `api/apps/restful_apis/dataset_api.py` CUSTOM 块（patch 管理） | 修复 `backward_compat.py` 对 `dataset_api.delete_knowledge_graph` 的坏引用；依赖 `dataset_api_service.delete_knowledge_graph(dataset_id, tenant_id)` 签名 | `api/apps/backward_compat.py`、`api/apps/services/dataset_api_service.py` | 低（上游若自行修复坏引用，本 CUSTOM 块可整体移除——合并时先检查） | 2026-09 |
| 5 | `web/src/pages/dataset/dataset-overview/hook-extras.ts` + 统计卡 patch | 依赖数据集概览页结构与统计 API 响应字段 | `web/src/pages/dataset/dataset-overview/index.tsx`、对应 stats API | 低 | 2026-09 |
| 6 | `api/db/init_data/compilation_templates/chapter_graph.yaml` | 模板即配置：entity/relation 白名单与 prompt 规则由官方知识编译渲染；依赖模板 YAML 结构（`kind`/`display_name`/`config.entity.fields`/`config.relation.fields`/`global_rules`）与目录自动扫描加载机制 | `api/db/services/compilation_template_service.py`、`internal/service/compilation_template_service.go`、`api/db/init_data/compilation_templates/knowledge_graph.yaml`（结构参照） | 低（纯配置，无代码漂移面；结构变更时对照官方内置模板调整） | 2026-09 |

---

## 二、官方文件修改清单（9 个）

### patch 管理内（6 个，对应 `patches/*.patch`）

| 官方文件 | 改动摘要 | patch 文件 |
|---------|---------|-----------|
| `.gitignore` | 忽略 `docker-compose.override.yml`、`docs/graphrag_review/` 等自定义规则 | `.gitignore.patch` |
| `api/apps/restful_apis/dataset_api.py` | 新增 `delete_knowledge_graph` 函数（修复 v0.27.2 仍未修复的 `backward_compat.py` 坏引用；转发上游 `dataset_api_service.delete_knowledge_graph`，公开 DELETE 端点已由上游 `delete_index` 路由覆盖） | `api_apps_restful_apis_dataset_api.py.patch` |
| `common/settings.py` | `init_settings()` 中安装删除审计 hook 与 SiliconFlow 超时 patch（均带异常兜底） | `common_settings.py.patch` |
| `web/src/locales/en.ts` | 新增 `datasetOverview.totalChunks` 文案 | `web_src_locales_en.ts.patch` |
| `web/src/locales/zh.ts` | 同上（中文） | `web_src_locales_zh.ts.patch` |
| `web/src/pages/dataset/dataset-overview/index.tsx` | Total chunks 统计卡、grid-cols-4（带 CUSTOM 标记） | `web_src_pages_dataset_dataset-overview_index.tsx.patch` |

### patch 管理外（3 个，均无需/无法纳入 patch）

| 官方文件 | 说明 |
|---------|------|
| `uv.lock` | 本地 uv + `pyproject.toml` 已钉住的 aliyun 镜像重新生成所致（registry URL 与字段格式差异）。**不是手工修改**，合并策略见 `MAINTENANCE.md`「官方更新时的标准流程」 |
| `web/src/components/paddleocr-options-form-field.tsx` | 良性差异：lint-staged 钩子的 prettier 输出，无功能改动；**不可还原**——还原后下次提交会被钩子自动改回。已接受现状，合并遇冲突取任一侧均可 |
| `web/src/pages/user-setting/sidebar/index.tsx` | 良性差异：同上（import 字母序） |

---

## 三、新增自定义文件（不与上游冲突）

| 分组 | 文件 | 用途 |
|-----|------|------|
| LLM | `rag/llm/siliconflow_timeout_patch.py` | SiliconFlow Embedding 超时可配置（`SILICONFLOW_TIMEOUT`，默认 120s）；由 `common/settings.py` 的 hook 安装 |
| 存储 | `common/doc_store_audit.py` | `docStoreConn.delete` 审计钩子（KG 产物删除留痕） |
| 知识编译 | `api/db/init_data/compilation_templates/chapter_graph.yaml` | ChapterGraph 迁移产物：书籍/章节编译模板。entity 白名单含 `书籍`/`章节`（+ 6 个通用类型），relation 白名单含 `contains`（书籍→章节）/`involves`（章节→实体，含 ASCII 词边界守卫语义）。**模板即配置**：Python/Go 双端均自动扫描该目录加载，无代码漂移面。在 Ingestion Pipeline 的 Compiler 节点选用 `chapter_graph` 模板即可启用 |
| 前端 | `web/src/pages/dataset/dataset-overview/hook-extras.ts` | 分块总数统计数据源 |
| | `web/src/assets/svg/data-flow/total-chunks-icon{,-bri}.svg` | 统计卡图标（暗/亮主题） |
| 管理 | `patches/`（6 patch + apply/regenerate 脚本 + README） | 官方文件改动的双轨管理 |
| | `MAINTENANCE.md`、本文档 | 维护规范与漂移清单 |

---

## 四、关键环境变量开关

| 环境变量 | 默认值 | 控制功能 |
|----------|--------|----------|
| `SILICONFLOW_TIMEOUT` | `120` | SiliconFlow Embedding 请求超时（秒），仅对 `SILICONFLOWEmbed` 本类实例生效 |

> v0.27.2 升级已删除全部 GraphRAG 增量开关（`USE_INCREMENTAL_*`、`USE_ASYNC_*`、`HEARTBEAT_*`、`STUCK_TASK_*`、`GRAPHRAG_*`、`RESOLUTION_*`、`KG_*`、`USE_CHAPTER_GRAPH` 等，详见第五节）；ChapterGraph 能力改由编译模板承担，不再有环境变量开关。

---

## 五、已删除定制（v0.27.2 升级，历史记录）

> 删除依据：官方 v0.27.2 知识编译（`internal/ingestion/knowledge_compile/`、`internal/ingestion/component/knowledge_compiler/`）在架构上取代旧 GraphRAG 增量定制的全部骨架功能（token-fence 写锁、chunk 级 LLM 缓存、事件攒批、20s 心跳租约回收）。删除前状态见 `dev-backup-v0.26.1` 分支。

| 已删除项 | 原文件 | 去向 |
|---|---|---|
| 增量图构建/合并、重跑控制 | `rag/graphrag/general/index_extras.py`、`index_patch.py`、`utils_extras.py`、`utils_pagination.py`、`config.py` | 官方知识编译取代 |
| 增量实体消解 | `rag/graphrag/entity_resolution_extras.py` | 官方知识编译取代（不使用，整体删除） |
| Community 报告 | 同上体系 | 不使用，整体删除；v0.27.2 新 Graph 产物无社区报告 |
| PageRank 可视化 | `utils_extras.py` 可视化策略、`dataset_api_service_extras.py` | 放弃；接受官方 `mention_count` 排序与 `/artifacts/graph`（top_n=128，支持 `?node=` 实体中心 BFS） |
| 删文档 KG 清理 | `rag/graphrag/document_delete_extras.py` | 官方删除清理取代 |
| 看门狗/心跳/KG-PP 队列 | `rag/svr/task_executor_extras.py`、`docker/finisher/` | 官方 20s 心跳租约回收取代 |
| ES count/scroll 注入 | `common/doc_store/es_conn_extras.py` | 上游 `es_conn.py` 已参数化 bulk `refresh` 并新增 `refresh_idx()`；count/scroll 无存活消费者 |
| redis patch | `rag/utils/redis_conn_patch.py` | 上游已原生提供 `spin_acquire`；`RedisDB.ttl`/stop_event 语义无存活消费者 |
| ChapterGraph（代码形态） | `index_extras.py` 内书籍/章节提取 | **保留能力，迁移为配置**：`chapter_graph.yaml` 编译模板（见第三节）。已知取舍：原"书籍/章节实体跳过 embedding"优化无对应物（新系统 dedup 走 KNN 必须 embed），重跑因 chunk 级 MAP 缓存接近零成本 |
| 7 个 patch | `rag_graphrag_utils/entity_resolution/general_index`、`rag_svr_task_executor`、`api_db_services_document_service`、`api_apps_services_dataset_api_service`、`rag_utils_es_conn` 的 `.patch` | 随对应定制删除；对应官方文件已恢复上游原样 |
| 4 个定制测试 | `test_graphrag_utils_extras.py`、`test_incremental_bugfixes.py`、`test_pagerank_recalc.py`、`test_graphrag_topology.py` | 随定制删除 |

### 旧图谱数据兼容（v0.27.2）

- 普通 chunk、graph blob（legacy `GET /datasets/<id>/graph` 端点）、RAPTOR 行：继续可读；
- 旧 entity/relation 行：UI 可读，但新 chat 检索（`graph_explore`）默认过滤 `scope_kwd="dataset"`，旧行无此字段不会被 chat 读到（预期行为）。可选：对存量行 ES `_update_by_query` 补 `scope_kwd`/`mention_count_int`，或对相关 KB 触发一次新知识编译重产数据（更干净）；
- `merge_state` 表/行无任何 v0.27.2 代码引用，成孤儿（可 DROP 或保留作历史）；
- 删文档前审计：确认旧 entity/relation 行带 `source_id` 字段，缺失时先补默认值，否则官方删除清理的 `must_not exists source_id` 步骤可能误删共享行。

### 更早的历史记录

v0.26.1 时代的「已撤回/已放弃项」与两轮全分支审查记录见 `dev-backup-v0.26.1` 分支上的本文档历史版本，不再逐项搬运。

---

## 更新规则

每次修改本仓库代码时，按以下规则更新本文档：

1. **动了自定义模块中的官方逻辑假设** → 更新第一节清单对应行（「最近核对」列同步刷新）。
2. **动了官方文件** → 更新第二节清单，并重跑 `bash patches/regenerate_patches.sh`。
3. **新增自定义文件** → 在第三节表格补一行。
4. **新增/修改环境变量开关** → 同步第四节表格。
5. **合并官方新版后** → 完成第一节全部核对，刷新「最近核对」列，并更新文档头部基准版本号。
6. **改动编译模板** → 确认与官方内置模板结构一致（第一节第 6 项），无需重跑 patch。
