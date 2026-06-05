# GraphRAG 增量重构改造总结

> 本文档汇总了 RAGFlow GraphRAG 模块从「全图 monolithic」模式向「增量/分片」模式演进的所有代码改造、Bug 修复与运维配置。  
> 所有修改默认**向后兼容**（`USE_*=0` 时行为与改造前完全一致）。

---

## 1. 背景与目标

### 1.1 原始问题

原 GraphRAG 采用**单一大 JSON 文件**（`knowledge_graph_kwd="graph"`）存储整本知识图谱：

- **内存瓶颈**：每处理一本书都要把全图加载进内存做 merge、resolution、community detection。
- **并发瓶颈**：多本书同时 GraphRAG 时，`set_graph` 重写同一份 JSON 导致 last-write-wins，前面文档的数据被覆盖。
- **导出异常**：21 本书的 KB，导出知识图谱时只能看到 1 本书的节点（后文详述 Root Cause）。

### 1.2 改造目标

| 阶段 | 目标 | 状态 |
|------|------|------|
| P1 – 存储解耦 | 实体/关系分片存储到 doc store，替代 monolithic JSON | ✅ 已上线 |
| P2 – 增量合并 | merge 阶段不加载全图，只查询受影响的已有节点/边 | ✅ 已上线 |
| P3 – 增量消解 | resolution 按类型构建局部图，避免全图加载 | ✅ 已上线 |
| P4 – 异步社区 | community detection 从索引按需加载完整拓扑 | ✅ 已上线 |
| P5 – 调度与流控 | 自适应并发限流器 + Redis Stream 异步后处理队列 | ✅ 已上线（限流器默认关闭） |

---

## 2. 架构总览

### 2.1 数据存储（P1）

改造前：只有一条 `knowledge_graph_kwd="graph"` 记录，内部是完整 JSON。

改造后：

```
index: ragflow_{tenant_id}
├── knowledge_graph_kwd="graph"      # legacy monolithic JSON（保留，用于回退）
├── knowledge_graph_kwd="subgraph"   # 每本书的子图 checkpoint（保留）
├── knowledge_graph_kwd="entity"     # 【新增】每个实体一条 doc
│   ├── entity_kwd: "实体名"
│   ├── entity_type_kwd: "类型"
│   ├── content_with_weight: JSON(描述/属性)
│   └── q_xxx_vec: 嵌入向量
├── knowledge_graph_kwd="relation"   # 【新增】每条关系一条 doc
│   ├── from_entity_kwd: "A"
│   ├── to_entity_kwd: "B"
│   └── content_with_weight: JSON(描述/属性/权重)
├── knowledge_graph_kwd="community_report"  # 社区报告（改造前后都存在）
└── knowledge_graph_kwd="mindmap"    # 思维导图
```

> **关键点**：`entity` / `relation` chunks 是**增量追加**的；新文档的节点/边会新增或更新已有记录，不会删除其他文档的数据。

### 2.2 读取路径（P1 + P4）

```
get_graph(tenant_id, kb_id)
  ├─ USE_INCREMENTAL_GRAPH=1
  │    └─ get_graph_from_index()   # 从 entity + relation 分片实时组装 nx.Graph
  │         → 返回完整全局图
  └─ USE_INCREMENTAL_GRAPH=0
       └─ get_graph_from_json()    # 读取 legacy JSON blob
```

导出 API (`_fetch_raw_knowledge_graph`) 被**强制**走 `get_graph_from_index()`，不依赖开关，保证导出结果始终基于最新索引数据。

### 2.3 写入路径（P1 + P2）

```
merge_subgraph(doc_subgraph)
  ├─ USE_INCREMENTAL_MERGE=1
  │    └─ merge_subgraph_incremental()
  │         1. batch-query 已有实体/关系（仅查子图中出现的节点和边）
  │         2. 内存合并属性（description/source_id/weight 等）
  │         3. 生成 delta chunks（entity + relation）
  │         4. set_graph_delta() → 写 entity/relation 分片
  │         5. **不写** graph JSON blob（避免覆盖全局数据）
  └─ USE_INCREMENTAL_MERGE=0
       └─ 加载全图 → graph_merge() → set_graph_monolithic() → 重写 JSON blob
```

### 2.4 实体消解（P3）

```
resolve_entities
  ├─ USE_INCREMENTAL_RESOLUTION=1
  │    └─ resolve_entities_incremental()
  │         1. 按 entity_type 分组新节点
  │         2. 查询同类型的已有节点
  │         3. 构建**局部图**（新节点 + 同类型已有节点 + 相关关系）
  │         4. EntityResolution 在局部图上运行
  │         5. 收集所有局部变更 → set_graph_delta()
  └─ USE_INCREMENTAL_RESOLUTION=0
       └─ 加载全图 → EntityResolution → set_graph_monolithic()
```

> **特殊处理**：`书籍`、`章节` 类型的节点被加入 `EXCLUDED_RESOLUTION_TYPES`，跨文档同名章节不再被合并。

### 2.5 社区检测（P4）

```
extract_community
  ├─ USE_ASYNC_COMMUNITY=1 且 传入 graph 只有单文档 source_id
  │    └─ extract_community_indexed()
  │         → get_graph_from_index() 加载完整全局图
  │         → Leiden 算法在全局图上运行
  └─ 其他情况
       └─ _extract_community_core() 直接在传入 graph 上运行
```

### 2.6 异步后处理队列（P5-T3）

当 `USE_ASYNC_KG_PHASES=1` 时，`run_graphrag_for_kb` 在 subgraph 生成完成后，把 `resolution` / `community` 阶段打包进 Redis Stream `graphrag:postprocess`，然后直接返回。

`task_executor.py` 启动一个独立的 `kg_postprocess_consumer()` 协程消费该队列：

- 使用与主 pipeline 相同的 `kg_limiter` 进行并发控制。
- 使用 `RedisDistributedLock` 保证同一 KB 只有一个后处理任务在执行。
- 完成后调用 `set_phase_marker()` 标记阶段完成，防止重复执行。

### 2.7 自适应并发限流器（P5-T2）

替换静态 `asyncio.Semaphore(2)`：

```python
AdaptiveConcurrencyLimiter(
    initial_limit=MAX_CONCURRENT_KG_TASKS,  # 默认 2
    min_limit=MIN_CONCURRENT_KG_TASKS,      # 默认 1
    max_limit=MAX_CONCURRENT_KG_TASKS,      # 默认 2
    adjust_interval=30,                     # 评估窗口
    degrade_threshold=2,                    # N 次 bad 事件 → limit -= 1
    increase_threshold=6,                   # N 次 good 事件且无 bad → limit += 1
)
```

事件类型：

| 事件 | 来源 | 影响 |
|------|------|------|
| `llm_rate_limit` | LLM 返回 429 / rate limit | bad（降级） |
| `es_slow` | doc-store 写入 > 3s | bad（降级） |
| `cas_conflict` | ES 并发写入冲突 | bad（降级） |
| `success` | 任务成功完成 | good（升级） |

---

## 3. 关键 Bug 修复记录

### 3.1 Bug：21 本书导出知识图谱只显示 1 本书

#### Root Cause（三层叠加）

**第一层：增量写入覆盖了全局 Graph JSON**

`set_graph_delta` 早期实现错误地把 `delta_graph`（仅含当前文档的节点/边）写入了 `knowledge_graph_kwd="graph"` 的 JSON blob：

```python
# 错误代码（已修复前）
await set_graph(tenant_id, kb_id, embd_mdl, graph, change, callback)
# graph 在这里是 delta_graph，只含一本书的数据
```

这导致第 2~21 本书依次把 graph JSON 覆盖成只含自己的数据。

**第二层：indexed chunks 缺少 `removed_kwd`**

`graph_node_to_chunk` / `graph_edge_to_chunk` 生成的 entity/relation chunk **没有** `removed_kwd: "N"` 字段。而 `get_graph_from_index` 的查询条件中包含了 `removed_kwd: "N"`，导致所有新写入的 entity/relation 被过滤掉，返回空图。

**第三层：导出 API 读取的是被污染的 JSON blob**

`_fetch_raw_knowledge_graph` 直接读取 `knowledge_graph_kwd="graph"` 的 JSON blob，而不是从索引组装。由于 JSON 已经被覆盖成单本书，导出结果永远只有一本书。

#### 修复方案

| 文件 | 修复内容 |
|------|----------|
| `rag/graphrag/utils.py` | `set_graph_delta` **不再写入** graph JSON blob；只生成 entity/relation delta chunks 并插入 doc store |
| `rag/graphrag/utils.py` | `graph_node_to_chunk` / `graph_edge_to_chunk` 增加 `removed_kwd: "N"` |
| `rag/graphrag/utils.py` | `get_graph_from_index` **移除** `removed_kwd` 过滤条件（entity/relation 不需要此字段过滤） |
| `api/apps/services/dataset_api_service.py` | `_fetch_raw_knowledge_graph` 改为调用 `get_graph_from_index()` 组装图，不直接读取 JSON blob |

### 3.2 Bug：ES `get_fields` 返回 keyword 字段为列表

#### 现象

重新部署后导出仍然只有一本书。排查发现 `get_graph_from_index` 中：

```python
ent_name = d["entity_kwd"]   # Infinity 返回 ["ENTITY_NAME"] 而不是 "ENTITY_NAME"
graph.add_node(ent_name, ...)  # TypeError: unhashable type: 'list'
```

异常被 `except Exception: continue` 吞掉，`total_entities=0`，函数返回 `None`，导出 fallback 到被污染的 JSON blob。

#### 修复方案

在 `get_graph_from_index`、`query_existing_entities`、`query_existing_relations`、`query_node_relations` 中，对 `entity_kwd`、`from_entity_kwd`、`to_entity_kwd` 增加列表兜底：

```python
ent_name = d["entity_kwd"]
if isinstance(ent_name, list):
    ent_name = ent_name[0] if ent_name else None
```

### 3.3 Bug：导出 API 被 `USE_INCREMENTAL_GRAPH` 开关拦截

#### 现象

`_fetch_raw_knowledge_graph` 之前调用的是 `get_graph()`，而 `get_graph()` 在 `USE_INCREMENTAL_GRAPH=False`（默认值）时直接走 `get_graph_from_json()`，读取被污染的 JSON blob。

#### 修复方案

`_fetch_raw_knowledge_graph` **直接调用 `get_graph_from_index()`**，绕过开关控制。只有在索引组装返回空时，才 fallback 到 JSON blob：

```python
graph = await get_graph_from_index(kb.tenant_id, dataset_id)
if graph is None or len(graph.nodes) == 0:
    graph = await get_graph_from_json(kb.tenant_id, dataset_id)
```

---

## 4. 文件变更清单

### 4.1 新增文件

| 文件 | 说明 |
|------|------|
| `rag/graphrag/config.py` | GraphRAG 功能开关与运行时配置（全部从环境变量读取） |
| `rag/graphrag/limiter.py` | `AdaptiveConcurrencyLimiter` + `_SlidingWindow` 事件计数器 |
| `rag/graphrag/phase_markers.py` | Redis 阶段标记（resolution / community done） |

### 4.2 核心修改文件

| 文件 | 主要变更 |
|------|----------|
| `rag/graphrag/utils.py` | **P1**: `get_graph_from_index` / `get_graph_from_json` / `get_graph` 双路径路由；`_batch_embed_nodes` / `_batch_embed_edges` / `_batch_embed_items` 批量嵌入重构；`set_graph_delta` 增量写入；`query_existing_entities` / `query_existing_relations` / `query_node_relations` 批量查询；`insert_chunks_bounded` 增加 P5 事件注入；列表兜底兼容 Infinity |
| `rag/graphrag/general/index.py` | **P2**: `merge_subgraph_incremental` 增量合并；**P3**: `resolve_entities_incremental` 增量消解；**P4**: `extract_community_indexed` / `_extract_community_core` 社区检测重构；`run_graphrag_for_kb` 增加 P5-T3 异步队列投递；书籍/章节类型中文化（`书籍`/`章节`）；`_extract_book_and_chapters` 章节提取优化 |
| `rag/graphrag/entity_resolution.py` | `EXCLUDED_RESOLUTION_TYPES = {"书籍", "章节"}` |
| `api/apps/services/dataset_api_service.py` | `_fetch_raw_knowledge_graph` 强制走 `get_graph_from_index`；`get_knowledge_graph` protected_types 改为中文 |
| `rag/svr/task_executor.py` | `kg_limiter` 替换为 `AdaptiveConcurrencyLimiter`（可选）；`kg_postprocess_consumer()` 协程消费 Redis Stream；GraphRAG / RAPTOR 任务包装 try/except 以记录限流事件 |
| `rag/llm/embedding_model.py` | `SILICONFLOWEmbed` 增加 `_post_with_retry`（指数退避，重试网络/429/5xx 错误） |
| `rag/utils/es_conn.py` | 深度分页修复：无显式排序时自动使用 `id.keyword` 排序 + `search_after` |

### 4.3 配置与部署文件

| 文件 | 主要变更 |
|------|----------|
| `docker/.env` | 新增 GraphRAG P1-P5 全部开关与参数；默认 `DOC_ENGINE=opensearch` |
| `docker/docker-compose.yml` | `opensearch01` 暴露 9201 端口；`ragflow-cpu` / `ragflow-server` 增加 `../web/dist:/ragflow/web/dist` 卷映射；移除 `../deepdoc:/ragflow/deepdoc` |
| `conf/llm_factories.json` | `Qwen3-Embedding-8B` max_tokens 从 32k 修正为 16k |

### 4.4 前端修复

| 文件 | 主要变更 |
|------|----------|
| `web/src/components/list-filter-bar/filter-field.tsx` | 嵌套 filter field 拼接逻辑修复 |
| `web/src/components/list-filter-bar/filter-popover.tsx` | 多选 checkbox parent field 传递修复 |
| `web/src/hooks/use-document-request.ts` | 请求参数 `run_status` → `run` 修正 |

---

## 5. 配置与部署

### 5.1 环境变量（`docker/.env`）

```bash
# P1 – 存储解耦（必须开启才能使用后续阶段）
USE_INCREMENTAL_GRAPH=1

# P2 – 增量合并
USE_INCREMENTAL_MERGE=1

# P3 – 增量消解
USE_INCREMENTAL_RESOLUTION=1

# P4 – 异步社区（delta 图时自动从索引加载全图）
USE_ASYNC_COMMUNITY=1

# P5-T1 – 并发上限
MAX_CONCURRENT_KG_TASKS=2

# P5-T2 – 自适应限流器（默认关闭，建议压测后开启）
USE_ADAPTIVE_LIMITER=0
MIN_CONCURRENT_KG_TASKS=1
ADAPTIVE_INTERVAL=30
ADAPTIVE_DEGRADE_THRESHOLD=2
ADAPTIVE_INCREASE_THRESHOLD=6
ES_SLOW_THRESHOLD_MS=3000

# P5-T3 – 异步后处理队列（默认关闭）
USE_ASYNC_KG_PHASES=0
KG_POSTPROCESS_QUEUE=graphrag:postprocess
```

> ⚠️ **重要**：Python 在导入时读取这些环境变量，修改后必须**重启容器**才能生效。

### 5.2 启用顺序建议

1. **P1 先行**：开启 `USE_INCREMENTAL_GRAPH=1`，确认 entity/relation chunks 正常写入且 `get_graph_from_index` 能正确组装。
2. **P2 验证**：开启 `USE_INCREMENTAL_MERGE=1`，多文档依次导入，观察是否不再覆盖。
3. **P3 验证**：开启 `USE_INCREMENTAL_RESOLUTION=1`，观察 resolution 阶段内存占用是否下降。
4. **P4 验证**：开启 `USE_ASYNC_COMMUNITY=1`，观察 community detection 是否在全局图上运行。
5. **P5 验证**：压测后按需开启 `USE_ADAPTIVE_LIMITER=1` 和 `USE_ASYNC_KG_PHASES=1`。

### 5.3 回退方法

将所有 `USE_*` 设为 `0` 并重启容器：

```bash
USE_INCREMENTAL_GRAPH=0
USE_INCREMENTAL_MERGE=0
USE_INCREMENTAL_RESOLUTION=0
USE_ASYNC_COMMUNITY=0
USE_ADAPTIVE_LIMITER=0
USE_ASYNC_KG_PHASES=0
```

系统会回到 legacy monolithic JSON 模式，行为与改造前完全一致。

---

## 6. 已知问题与 TODO

| 问题 | 说明 | 优先级 |
|------|------|--------|
| `kg_postprocess_consumer` 失败不 ack | 异步队列消费异常时未调用 `msg.ack()`，消息会保留在 pending 列表中等待重试；需要增加死信队列或最大重试计数 | P2 |
| `set_graph_delta` 不写 graph JSON | 这是**设计如此**：增量路径的 canonical graph 来自索引。但如果完全关闭增量开关后旧数据仍保留在 JSON 中，可能导致新旧数据不一致 | P3 |
| `rebuild_graph` 仍读 subgraph | `rebuild_graph` 从 `knowledge_graph_kwd="subgraph"` 重建，未迁移到 `entity`/`relation` 索引；如后续弃用 subgraph 需要同步改造 | P3 |
| Infinity keyword-split 兼容性 | `InfinityConnection.get_fields` 对 keyword 字段的返回格式与 ES 有差异，当前通过 `isinstance(list)` 兜底；如 Infinity 行为变更需重新测试 | P3 |
| 批量嵌入 batch_size 硬编码 | `_batch_embed_items` 中 `batch_size = 32`，未暴露为配置项 | P4 |
| PageRank 在增量路径缺失 | `merge_subgraph_incremental` 使用 degree-based `rank` 近似，未运行全局 PageRank；`get_graph_from_index` 返回的图不含 pagerank | P2 |

---

## 7. 性能对比（预期）

| 指标 | 改造前（Monolithic） | 改造后（Incremental） | 说明 |
|------|---------------------|----------------------|------|
| Merge 内存 | O(全图节点数) | O(子图节点数 + 已存在节点数) | 只加载受影响节点 |
| Resolution 内存 | O(全图节点数) | O(同类型局部节点数) | 按类型分片 |
| Community 内存 | O(全图节点数) | O(全图节点数) 或 O(0) | P4 从索引加载；P5-T3 可异步 |
| 写入冲突 | 高（同一份 JSON） | 低（追加 chunks） | 增量追加天然避免覆盖 |
| 导出可靠性 | 依赖 JSON blob | 依赖索引组装 | 索引是 append-only，不易损坏 |

---

*文档版本：2025-05-20*  
*对应代码基线：`HEAD`（包含全部 P1-P5 改造）*
