# RAGFlow 升级手册：v0.26.1 定制分支 → v0.27.2

> 本文档是交给执行 agent 的操作手册。基于 2026-09 对本地定制代码与上游 v0.27.2 源码的逐项对照分析（分析结论已验证到文件/行号级别）。
>
> **基线**：`dev` 分支 = v0.26.1 + GraphRAG 增量定制（HEAD `faf86a17f`，与 `fork/dev` 同步）
> **目标**：v0.27.2 + 仅保留必要定制
> **核心决策（所有者已确认，不要更改）**：
> 1. 增量实体消解（C）、Community 报告（E）——不使用，**整体删除**
> 2. PageRank 可视化（B）——可替代，**放弃**，接受官方 `mention_count` 排序与 `/artifacts/graph`
> 3. ChapterGraph（书籍/章节实体，D）——**仍在使用，必须保留**，迁移为官方知识编译的自定义模板
> 4. 其余定制（增量图构建/合并、重跑控制、删除清理、看门狗）——官方 v0.27.2 知识编译已取代，**整体删除**

---

## 阶段 0：准备与备份（必须先做）

```bash
# 0.1 确认工作区干净（允许存在 untracked 的 *.csv 分析产物）
git status --short

# 0.2 创建备份分支（代码级回滚点）
git branch dev-backup-v0.26.1 dev
git push fork dev-backup-v0.26.1

# 0.3 数据备份（升级后首次启动会自动执行 DB 迁移，含列重命名，回滚必须靠备份）
#     - MySQL: mysqldump 全量备份 ragflow 库
#     - ES: 对 ragflow_* 索引做 snapshot（或至少记录 _mapping 导出）
#     ⚠️ 不可跳过：v0.27.2 会把 artifact_task_id 重命名为 wiki_task_id、
#       迁移 model_type 取值（speech2text→asr 等），旧代码无法在这些改动上运行。

# 0.4 部署环境变量备份（后续要删一批开关）
cp docker/.env.local docker/.env.local.bak 2>/dev/null || true
```

**分支模型（2026-09 起生效，手册其余部分均以此为准）**：
- `main` → 跟踪 `origin/main`（origin = 官方 infiniflow/ragflow），即官方最新代码，本地不再在 fork 上维护 main 基线
- `dev` → 跟踪 `fork/dev`（fork = ZRRRY/ragflow），即定制分支
- 旧的「`git reset --hard vX.Y.Z` + force-push main 到 fork」流程已废弃

**网络注意事项**：本机 `git fetch` 直连 GitHub 曾失败（curl 正常）。若 `git fetch` 卡住，先解决代理（如 `git config http.proxy http://127.0.0.1:<port>`），或用此前成功拉取 fork/dev、origin/main 的方式（IDE 等）。

```bash
# 0.5 同步 main 到官方最新，并获取 v0.27.2 tag
git fetch origin
git checkout main && git merge --ff-only origin/main && git checkout dev
git fetch origin tag v0.27.2        # tag 对象 a024bea0 当前不在本地，必须拉取
git rev-parse v0.27.2               # 验证有输出；官方 release 发布于 2026-09-10
```

> 说明：本手册的全部验证结论（patch 适用性、函数签名、字段兼容性）以 **v0.27.2 tag** 为准。
> 当前 `origin/main`（11f4d1192，2026-09-10 10:16 +0800）比 tag 早约半天、仅差几个提交，
> 若选择合并 `main` 而非 tag，结论同样适用，但阶段 6 验证时需对新增差异保持留意。

---

## 阶段 1：合并上游代码

按新分支模型执行（rerere 已开启，用 merge 不用 rebase）：

```bash
git checkout -b upgrade-v0.27.2 dev
git merge v0.27.2        # 或 git merge main（main 已与官方同步，距 tag 仅几个提交）
```

冲突处理原则：

1. **uv.lock**：不人工解冲突。`git checkout --theirs uv.lock && uv lock && git add uv.lock`（pyproject.toml 已钉 aliyun 镜像，本地 uv 会重新生成本分支 diff）
2. **带 CUSTOM 标记 / patch 管理的官方文件**：本手册阶段 2 会删除大部分定制，对应文件的冲突**直接取上游版本（--theirs）**，不必费力保留定制侧。具体哪些文件取上游，见阶段 2 清单
3. **`web/src/components/paddleocr-options-form-field.tsx`、`web/src/pages/user-setting/sidebar/index.tsx`**：良性 prettier 差异，取任一侧均可
4. 其余文件正常三方合并

合并完成后先不急着提交收尾，继续阶段 2/3/4 在同一分支上完成后再统一提交。

---

## 阶段 2：删除废弃定制（本次升级的核心动作）

> 依据：官方 v0.27.2 知识编译（`internal/ingestion/knowledge_compile/`、`internal/ingestion/component/knowledge_compiler/`）在架构上取代了旧 GraphRAG 增量定制的全部骨架功能，且做得更好（token-fence 写锁、chunk 级 LLM 缓存、事件攒批、20s 心跳租约回收）。

### 2.1 删除以下自定义文件（约 5000 行）

```
rag/graphrag/general/index_extras.py        # 增量编排、ChapterGraph、PageRank 重算
rag/graphrag/general/index_patch.py         # monkey-patch 调度器
rag/graphrag/utils_extras.py                # set_graph_delta、merge_state、可视化策略
rag/graphrag/utils_pagination.py            # search_after 分页（仅供上述 extras 使用）
rag/graphrag/entity_resolution_extras.py    # 增量实体消解
rag/graphrag/document_delete_extras.py      # 删文档 KG 清理
rag/graphrag/config.py                      # 环境变量开关集中配置
rag/svr/task_executor_extras.py             # 看门狗/心跳/KG-PP 队列
common/doc_store/es_conn_extras.py          # ES count/scroll 注入、insert 包装
api/apps/restful_apis/dataset_api_extras.py # DELETE /datasets/<id>/graph 路由
api/apps/services/dataset_api_service_extras.py  # KG 可视化装配（被官方 /artifacts/graph 取代）
docker/finisher/                            # 卡死任务收尾脚本（整个目录）
test/unit_test/rag/graphrag/test_graphrag_utils_extras.py
test/unit_test/rag/graphrag/test_incremental_bugfixes.py
test/unit_test/rag/graphrag/test_pagerank_recalc.py
test/unit_test/rag/graphrag/test_graphrag_topology.py
```

**两个例外，先判断再删**：

- `rag/utils/redis_conn_patch.py`：先 `grep -rn "redis_conn_patch\|RedisDB.ttl\|spin_acquire" --include="*.py" | grep -v graphrag | grep -v task_executor_extras` 确认没有存活消费者，无则删除，有则保留
- `common/doc_store_audit.py`：默认**保留**（删除审计与图谱无关，是独立能力）

### 2.2 删除以下 patch 及对应官方文件改动（合并冲突时这些文件取 --theirs）

```
patches/rag_graphrag_utils.py.patch                  # 随 utils_extras 删除
patches/rag_graphrag_entity_resolution.py.patch      # 随 entity_resolution_extras 删除
patches/rag_graphrag_general_index.py.patch          # 随 index_patch 删除
patches/rag_svr_task_executor.py.patch               # 随 task_executor_extras 删除
patches/api_db_services_document_service.py.patch    # 随 document_delete_extras 删除
patches/api_apps_services_dataset_api_service.py.patch  # 随可视化 extras 删除
patches/rag_utils_es_conn.py.patch                   # 上游 v0.27.2 已把 refresh 参数化：
                                                     # insert(..., refresh="wait_for") 可传 "false"，
                                                     # 不再需要 patch 官方文件
```

对应官方文件**恢复为上游 v0.27.2 原样**：`rag/graphrag/utils.py`、`rag/graphrag/entity_resolution.py`、`rag/graphrag/general/index.py`、`rag/svr/task_executor.py`、`api/db/services/document_service.py`、`api/apps/services/dataset_api_service.py`、`rag/utils/es_conn.py`

### 2.3 条件保留一个 patch

`patches/api_apps_restful_apis_dataset_api.py.patch`（新增 `delete_knowledge_graph` 转发，修 backward_compat 坏引用）：
先检查上游 v0.27.2 `api/apps/backward_compat.py` 是否仍引用不存在的 `delete_knowledge_graph`——已修复则删除该 patch，未修复则保留（实测可干净打上）。

### 2.4 从部署配置删除环境变量（`.env.local` / compose override）

```
USE_INCREMENTAL_GRAPH / USE_INCREMENTAL_MERGE / USE_INCREMENTAL_RESOLUTION
RECALC_GLOBAL_PAGERANK_AFTER_MERGE
USE_ASYNC_COMMUNITY / USE_ASYNC_KG_PHASES / KG_POSTPROCESS_QUEUE
RECONCILE_STUCK_ON_BOOT / STUCK_TASK_GRACE_MINUTES / STUCK_TASK_MIN_NODES / STUCK_TASK_MIN_EDGES
HEARTBEAT_INTERVAL / HEARTBEAT_TTL
KG_MAX_SAFE_RESUME_NODES / GRAPHRAG_MAX_PARALLEL_DOCS
USE_CHAPTER_GRAPH / GRAPHRAG_KEEP_SUBGRAPH / GRAPHRAG_KEEP_MERGE / GRAPHRAG_KEEP_RESOLUTION
GRAPHRAG_MERGE_TIMEOUT_SECONDS / GRAPHRAG_SET_GRAPH_STREAM_WINDOW / GRAPHRAG_SET_GRAPH_PIPELINE_DEPTH
GRAPHRAG_SEARCH_WITH_SCROLL_HITS_CAP / RESOLUTION_BATCH_SIZE / RESOLUTION_MAX_CONCURRENT_TASKS
```

**注意**：`rag/graphrag/config.py` 删除后，它在 import 时顺带安装的两个 patch 失去安装点。处理方式见阶段 3.2。

---

## 阶段 3：保留项与 patch 重做

### 3.1 保留不动的定制

| 项 | 文件 | 说明 |
|---|---|---|
| SiliconFlow Embedding 超时 | `rag/llm/siliconflow_timeout_patch.py` | 与图谱无关，继续需要 |
| 删除审计 | `common/doc_store_audit.py` | 默认保留 |
| 数据集统计卡（前端） | `web/src/pages/dataset/dataset-overview/hook-extras.ts`、`web/src/assets/svg/data-flow/total-chunks-icon*.svg` | 与图谱无关 |

### 3.2 重做 `common/settings.py` 的 hook（原 `common_settings.py.patch`）

原 patch 在 `init_settings()` 安装 ES extras + 删除审计 hook。改为：
- 删除 ES extras 安装（`es_conn_extras` 已删）
- 保留删除审计 hook
- **新增**：在此安装 `siliconflow_timeout_patch`（原由 `rag/graphrag/config.py` import 时安装）；若阶段 2 判定保留 `redis_conn_patch`，也在此安装
- 安装代码保持原有的 try/except 异常兜底风格

### 3.3 保留并重新生成的 patch 清单（合并完成后执行）

最终 patch 管理内应剩 5~6 个：
- `.gitignore.patch`（保留）
- `common_settings.py.patch`（按 3.2 重做后重新生成）
- `web_src_locales_en.ts.patch` / `web_src_locales_zh.ts.patch`（保留，实测可打 v0.27.2）
- `web_src_pages_dataset_dataset-overview_index.tsx.patch`（保留，实测可打）
- `api_apps_restful_apis_dataset_api.py.patch`（按 2.3 判定）

```bash
bash patches/regenerate_patches.sh
bash patches/apply_patches.sh --check   # 验证全部可干净应用
```

---

## 阶段 4：ChapterGraph → 官方知识编译模板迁移（唯一保留的图谱能力）

### 4.1 背景知识（执行前必读）

- v0.27.2 知识编译把文档编译为 Graph/Tree/Wiki 等结构化制品，模板即配置：entity/relation 类型是**白名单**，LLM prompt 会渲染 YAML 中定义的类型并强制遵循
- 参考实现：`api/db/init_data/compilation_templates/knowledge_graph.yaml`（内置 Graph 模板）；模板通过 Agent 页面创建 Compilation Operator 或写入 DB
- 原 ChapterGraph 语义在 `rag/graphrag/general/index_extras.py` 中（**删除该文件前先把 ChapterGraph 相关段落提取出来**）：书籍/章节实体类型、关系类型、实体-章节子串匹配的 ASCII 词边界守卫规则（CJK 语义不变）

### 4.2 迁移步骤

1. 从 `index_extras.py` 提取 ChapterGraph 的 entity 类型（书籍/章节等）、relation 类型、抽取规则描述
2. 基于 `knowledge_graph.yaml` 的结构创建自定义编译模板（如 `chapter_graph.yaml`）：entity/relation 类型白名单 + `guideline.rules_for_entities/relations` 写入原 prompt 语义
3. 在 Ingestion Pipeline 中配置 Compiler 节点并选用该模板
4. **已知取舍（接受，不要试图绕过）**：
   - 原"书籍/章节实体跳过 embedding"优化无对应物——新系统 dedup 走 KNN 必须 embed。首次编译成本回到官方基线；重跑因 chunk 级 MAP 缓存接近零成本
   - 如书籍/章节实体量过大，在模板的抽取规则中限制产出数量

### 4.3 验收标准

用一个含书籍/章节结构的测试知识库跑通：Parser → Chunker → Compiler（chapter_graph 模板）→ Indexer，确认产物中出现 `书籍`/`章节` 类型实体及预期关系，且 `GET /datasets/<id>/artifacts/graph` 可正常渲染。

---

## 阶段 5：数据层处理（无需重建数据）

1. **MySQL / ES 迁移**：启动即自动完成（建表、加列、列重命名；ES 新字段走 dynamic_templates 自动进 mapping）。启动后检查日志无 migration 相关 ERROR
2. **旧图谱数据**：保留原样即可——legacy `GET /datasets/<id>/graph` 继续可读（含 pagerank 排序）。新 chat 检索（`graph_explore`）默认过滤 `scope_kwd="dataset"`，旧 entity/relation 行无此字段**不会被 chat 读到**，这是预期行为
3. **（可选）让旧图谱进入新 chat**：对存量 entity/relation 行执行 ES `_update_by_query` 补 `scope_kwd="dataset"` 和 `mention_count_int`；或直接对相关 KB 触发一次新知识编译重产数据（更干净）
4. **删文档前审计**：确认旧 entity/relation 行带 `source_id` 字段；缺失时先补默认值，否则官方删除清理的 `must_not exists source_id` 步骤可能误删共享行
5. **孤儿数据清理（可选）**：`merge_state` 表/行无任何 v0.27.2 代码引用，可 DROP 或保留作历史；旧 `subgraph`/`community_report` 行无害，调用官方 `delete_knowledge_graph` 时会一并清理

---

## 阶段 6：验证清单

```bash
# 静态检查
uv sync --python 3.13 --all-extras
ruff check && ruff format --check
bash patches/apply_patches.sh --check

# 单元测试（graphrag 定制测试已随阶段 2 删除，跑剩余套件）
uv run pytest test/unit_test -x -q

# 前端
cd web && npm run lint && npm run build
```

**运行时冒烟（依赖服务：MySQL/ES/Redis/MinIO）**：

- [ ] 后端启动，migration 无报错
- [ ] 创建知识库 → 上传文档 → 解析成功 → chat 问答正常（普通 chunk 检索回归）
- [ ] 阶段 4 的 chapter_graph 模板验收（4.3）
- [ ] 官方 `/datasets/<id>/artifacts/graph` 可视化正常
- [ ] 旧知识库的 legacy `GET /datasets/<id>/graph` 仍返回旧图谱（过渡期验证）
- [ ] 删除一篇文档，确认共享图谱行未被误删（阶段 5.4 审计后执行）
- [ ] 前端数据集概览页 Total chunks 统计卡正常显示

---

## 阶段 7：收尾

1. 重写 `MODIFICATIONS.md`：基准改为 v0.27.2；删除已移除模块的清单条目；ChapterGraph 模板迁移写入「定制清单」新条目（注明模板即配置，无代码漂移面）；语义漂移清单只保留存活项（siliconflow patch、settings hook、前端统计卡）
2. 更新 `MAINTENANCE.md`：「官方更新时的标准流程」重写为新分支模型——
   - 同步官方：`git checkout main && git pull --ff-only`（main 直接跟踪 origin/main，不再 reset --hard + force-push 到 fork）
   - 合并定制：`git checkout dev && git merge main`
   - 同时删除流程中涉及已删文件（index_extras、task_executor_extras 等）的说明
3. 提交策略建议（每个阶段单独 commit，便于 bisect）：
   - `merge: v0.27.2 into dev`
   - `refactor: drop GraphRAG incremental customizations superseded by knowledge compilation`
   - `refactor: re-home siliconflow/audit hooks in settings; slim patch set`
   - `feat: migrate ChapterGraph to knowledge compilation template`
   - `docs: rewrite MODIFICATIONS.md for v0.27.2 baseline`
4. `git push fork upgrade-v0.27.2`，合并回 dev 前人工复核

---

## 回滚预案

| 场景 | 操作 |
|---|---|
| 合并阶段发现不可解问题 | `git merge --abort`，回到 `dev` |
| 升级后运行异常 | 代码回滚 `git reset --hard dev-backup-v0.26.1`；**同时必须恢复 MySQL 备份**（列重命名不可逆）和 ES 快照 |
| 仅 ChapterGraph 迁移不达标 | 不影响其余升级成果；临时方案：旧 graphrag 代码在 v0.27.2 中仍完整保留（`rag/graphrag/`），可按原 flag 方式触发旧管道（但 UI 已无入口，需走 API/任务队列），再排期修复模板 |

---

## 附录 A：关键事实速查（分析结论，勿重复调查）

- 旧 `rag/graphrag/` 代码在 v0.27.2 **完整保留但已冻结**（上游全部新开发投向知识编译），后端任务类型 `graphrag`/`raptor` 仍可触发
- v0.27.2 新 Graph 产物**无 PageRank、无社区报告**；排序用 `mention_count_int`；可视化 `/artifacts/graph` 默认 top_n=128、支持 `?node=` 实体中心 BFS
- 旧数据兼容：普通 chunk ✅；graph blob ✅（legacy 端点 + chat navigation 读）；entity/relation 行 UI 可读但 chat 不读（缺 `scope_kwd`）；RAPTOR 行 ✅；`merge_state` 成孤儿
- 上游 `es_conn.py` 已把 bulk `refresh` 参数化并新增 `refresh_idx()`——这是删除 `rag_utils_es_conn.py.patch` 的依据
- patch 实测（对 v0.27.2 dry-run）：保留的 5 个均可干净应用；删除的 7 个中 3 个本来就会冲突，随删除一并消失
- 上游 v0.26.1→v0.27.2 在 `rag/graphrag/utils.py` 新增 embedding 批量缓存（`_batch_embed_cache_misses` 等），与本分支已删的流式 embed 优化同区域——删除定制后无需关心

## 附录 B：分析覆盖范围声明

- 本手册基于 v0.27.2 tag **源码静态分析**（函数签名/字段/过滤器均验证到行号），未做 v0.27.2 运行时实测
- 「旧 entity 行被 chat 的 `scope_kwd` 过滤器排除」是从查询构造代码推出的结论，逻辑链完整；阶段 6 冒烟中的旧 KB 验证即是对它的实测确认
