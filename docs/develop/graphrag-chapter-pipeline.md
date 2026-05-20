# RAGFlow GraphRAG 章节层级知识图谱生成流程

> 本文档详细描述 RAGFlow GraphRAG 模块在支持 Markdown 书籍章节结构（Book → Chapter → Entity）后的完整生成链路。
> 
> **适用范围**：所有涉及 GraphRAG 且需要保留 `Book → Chapter → Entity` 层级结构的场景。

---

## 一、概述

本流程在标准 GraphRAG（实体-关系抽取）的基础上，增加了对 Markdown 文档标题层级的识别：

- **Book 实体**：从一级标题 `# 书名` 提取
- **Chapter 实体**：从**二级标题 `## 章节名`** 提取（系统仅识别 `##` 作为章节标题，不会自动识别 `###` 或其他层级）
- **Book→Chapter 关系**：`contains`（包含）
- **Chapter→Entity 关系**：`involves`（涉及）

最终构建的三层结构：

```text
Book(title)
    └── contains ──── Chapter(《title》chapter_1)
    │                     └── involves ──── Entity_A
    │                     └── involves ──── Entity_B
    └── contains ──── Chapter(《title》chapter_2)
                              └── involves ──── Entity_C
```

---

## 二、完整流程

### 阶段 1：文档解析与 Chunk 生成

**目标**：将原始 Markdown 文档切分成带语义边界的文本块（chunks），并确保章节标题（`# 书名`、`## 章节名`）保留在 chunk 内容中。

> **注意**：本流程**仅识别二级标题 `##`** 作为章节名。如果文档中使用 `###` 或其他层级表示章节，需要在 `parser_config.children_delimiter` 中配置对应分隔符，或修改正则表达式。

| 步骤 | 涉及文件 | 关键逻辑 |
|------|---------|---------|
| **1.1 Markdown 解析** | `deepdoc/parser/markdown_parser.py` | `MarkdownElementExtractor.extract_elements` 按用户配置的 `children_delimiter`（通常是 `##`）切分 section。**修改后**：使用 `pattern.split(text)` + `pattern.finditer(text)` 组合，将 `##` 保留在每个 section 的**开头**，不会丢失。 |
| **1.2 分块** | `rag/nlp/__init__.py` | `split_with_pattern` 按 `child_delimiters_pattern` 进一步切分 chunk。**修改后**：`txts[0]` 单独作为 chunk，后续 `delimiter + 后续文本` 组合成 chunk，delimiter 保留在**开头**。 |
| **1.3 合并（KB 批量模式）** | `rag/graphrag/general/index.py` → `load_doc_chunks` | `run_graphrag_for_kb` 会把多个 raw chunk 合并成 token 数 < 4096 的大 chunk。**修改后**：合并时自动补 `\n` 分隔，防止 `##` 粘在前文末尾导致正则匹配失败。 |

**输出**：`chunks: list[str]`，每个元素是一段文本，其中一级/二级标题仍保留在文本内。

**关键正则**：
- 书名：`^#\s+(.+)$`（一级标题）
- 章节名：`^##\s+(.+)$`（二级标题，**系统只识别此层级**）

---

### 阶段 2：单文档子图生成（`generate_subgraph`）

**目标**：从 chunks 中提取实体和关系，同时注入 **Book → Chapter → Entity** 三层结构。

**涉及文件**：`rag/graphrag/general/index.py`

```
┌─────────────────────────────────────────────────────────┐
│  2.1 LLM 提取原始实体/关系                                │
│     Extractor.__call__(doc_id, chunks)                   │
│     → llm_ents, llm_rels                                  │
├─────────────────────────────────────────────────────────┤
│  2.2 注入章节层级（_extract_book_and_chapters）           │
│     a. 扫描 chunks，正则 ^#\s+(.+)$ 提取 book_title       │
│     b. 扫描 chunks，正则 ^##\s+(.+)$ 提取 chapter（仅二级标题） │
│        → 生成 Book 实体、Chapter 实体                     │
│        → 生成 Book --contains--> Chapter 关系             │
│     c. 若 has_md_headers=False（## 被切掉了）              │
│        → fallback：从 i>0 的 chunk 第一行提取章节标题      │
│        → 把 chunks[0] 的实体关联到第一个章节               │
├─────────────────────────────────────────────────────────┤
│  2.3 建立章节-实体关联（_link_entities_to_chapters）      │
│     对每个 LLM 实体（排除 Book/Chapter）：                 │
│       若 entity_name.lower() in chunk_text.lower():      │
│         → 生成 Chapter --involves--> Entity 关系          │
├─────────────────────────────────────────────────────────┤
│  2.4 合并同名实体                                         │
│     merged_ents = llm_ents ∪ chapter_ents                 │
│     同名时保留 Book/Chapter 的 entity_type 和 description │
├─────────────────────────────────────────────────────────┤
│  2.5 构建 networkx.Graph（subgraph）                      │
│     遍历 ents 添加节点                                     │
│     遍历 rels 添加边（检查 has_node，缺失则记 ignored）    │
│     → tidy_graph(subgraph)                                │
│     → 保存 subgraph chunk 到 doc store                     │
└─────────────────────────────────────────────────────────┘
```

**关键输出**：
- `subgraph` 中有 3 类节点：**Book**、**Chapter**、普通 **Entity**
- 2 类注入边：**Book→Chapter** (`contains`)、**Chapter→Entity** (`involves`)
- LLM 原始边也保留

**DEBUG 日志**：搜索 `[ChapterGraph DEBUG]` 可查看：
- `chapter_ents=X, chapter_rels=Y`
- `entity_chapter_rels=Z`
- `subgraph nodes=N, edges=M`
- `ignored A/B relations due to missing entities`

---

### 阶段 3：全局图合并（`merge_subgraph`）

**目标**：把单文档的 `subgraph` 合并到知识库级别的全局图中。

```
┌─────────────────────────────────────────────────────────┐
│  3.1 加载已有全局图（get_graph）                          │
│     若不存在 → 新图 = subgraph                            │
│     若存在   → graph_merge(old_graph, subgraph, change)   │
├─────────────────────────────────────────────────────────┤
│  3.2 graph_merge 逻辑                                     │
│     节点：同名节点合并 description 和 source_id           │
│     边：   同名边合并 weight、description、keywords       │
├─────────────────────────────────────────────────────────┤
│  3.3 计算 PageRank（nx.pagerank）                         │
├─────────────────────────────────────────────────────────┤
│  3.4 set_graph 持久化                                     │
│     a. 生成 graph 的 node_link_data JSON                  │
│     b. 为每个 source（doc_id）生成 subgraph JSON          │
│     c. 为 change.added_updated_nodes 生成 entity chunks   │
│        （含 embedding）                                   │
│     d. 为 change.added_updated_edges 生成 relation chunks │
│        （含 embedding）                                   │
│     e. 删除旧 graph/subgraph → 批量插入新数据             │
└─────────────────────────────────────────────────────────┘
```

**DEBUG 日志**：`[ChapterGraph DEBUG] After merge, Book '...' has X Chapter neighbors`

---

### 阶段 4：实体消解（`resolve_entities`，可选）

**目标**：合并全局图中"相似"的实体，减少冗余。

**涉及文件**：`rag/graphrag/entity_resolution.py`

| 逻辑 | 说明 |
|------|------|
| 按 `entity_type` 分组 | Book 只和 Book 比较，Chapter 只和 Chapter 比较，**不会跨类型合并** |
| 中文相似度 | `is_similarity` 使用字符集合交集比例：`len(a & b) / max(len(a), len(b)) >= 0.8` |
| 合并节点 | `_merge_graph_nodes`：保留 `nodes[0]`，删除其余，边重定向到保留节点 |

**注意**：由于 Book/Chapter 节点名自带书名前缀（如 `"《20世纪艺术批评》前言"`），且按类型隔离，**通常不会被误合并**。

**DEBUG 日志**：`[ChapterGraph DEBUG] After resolution, Book '...' has X Chapter neighbors`

---

### 阶段 5：社区发现（`extract_community`，可选）

**目标**：用 Leiden 算法在全局图上划分社区，并用 LLM 生成社区报告。

---

### 阶段 6：前端查询与展示

**目标**：用户在前端查看知识图谱可视化。

**涉及文件**：
- 后端：`api/apps/services/dataset_api_service.py` → `get_knowledge_graph`
- 前端：`web/src/pages/dataset/knowledge-graph/force-graph.tsx`

```
┌─────────────────────────────────────────────────────────┐
│  6.1 API: GET /datasets/<id>/graph                       │
│     从 doc store 查询 knowledge_graph_kwd="graph" 的记录 │
│     加载 node_link_data JSON                              │
├─────────────────────────────────────────────────────────┤
│  6.2 数据截断（修改前的问题根源）                          │
│     原逻辑：                                               │
│       nodes = sorted(pagerank)[:256]                      │
│       edges = sorted(weight)[:128]                        │
│     问题：Book/Chapter pagerank 低、weight=1，被截断过滤   │
│                                                          │
│     修改后：                                               │
│       优先保留 entity_type="书籍"/"章节" 的节点         │
│       其余节点按 pagerank 填充至 512 上限                 │
│       边上限放宽到 512                                    │
├─────────────────────────────────────────────────────────┤
│  6.3 前端渲染（G6）                                       │
│     nodes 按 entity_type 分组着色                          │
│     edges 显示为虚线，粗细按 weight                        │
│     支持 hover-activate（高亮一度邻居）                    │
└─────────────────────────────────────────────────────────┘
```

---

## 三、数据流总览

```
原始 Markdown
    │
    ▼
[Parser] extract_elements + split_with_pattern  ──►  chunks（保留 ##）
    │
    ▼
[generate_subgraph]  LLM 抽取 + 章节注入  ──►  subgraph（Book/Chapter/Entity + 边）
    │
    ▼
[merge_subgraph]  graph_merge + set_graph  ──►  全局 graph + entity/relation chunks
    │
    ▼
[resolve_entities]  相似实体合并（可选）
    │
    ▼
[extract_community]  社区报告（可选）
    │
    ▼
[前端 API] get_knowledge_graph  ──►  G6 力导向图渲染
```

---

## 四、关键修改文件清单

| 文件 | 修改内容 | 目的 |
|------|---------|------|
| `rag/graphrag/general/index.py` | 新增 `_extract_book_and_chapters`、`_link_entities_to_chapters`、DEBUG 日志；修复 `load_doc_chunks` 换行符 | 核心：章节提取、实体关联、诊断日志 |
| `deepdoc/parser/markdown_parser.py` | `extract_elements` 保留 delimiter 在 section 开头 | 防止 `##` 在解析阶段丢失 |
| `rag/nlp/__init__.py` | `split_with_pattern` 保留 delimiter 在 chunk 开头 | 防止 `##` 在分块阶段丢失 |
| `api/apps/services/dataset_api_service.py` | `get_knowledge_graph` 保护 Book/Chapter 节点，放宽截断上限 | 防止前端展示时过滤掉层级结构 |

---

## 五、常见问题排查

### 5.1 "书名和章节标题提取到了，但没有边"

**排查步骤**：

1. 查看 `[ChapterGraph DEBUG] subgraph nodes=X, edges=Y`
   - 若 `edges=0` → 检查 `chapter_rels` 和 `ignored_rels` 日志
   - 若 `edges>0` → 问题不在生成阶段，继续下一步

2. 查看 `[ChapterGraph DEBUG] After merge, Book '...' has X Chapter neighbors`
   - 若 `X=0` → `graph_merge` 阶段丢失边，检查 `merge_subgraph` 逻辑
   - 若 `X>0` → 继续下一步

3. 查看 `[ChapterGraph DEBUG] After resolution, Book '...' has X Chapter neighbors`
   - 若 `X=0` → `EntityResolution` 合并/删除了 Book 或 Chapter 节点
   - 若 `X>0` → 后端数据正常，**问题在前端 API 截断**（见 6.2 节修复）

### 5.2 chunks 中没有 `##`

日志显示 `has_md_headers=False` 时：
- 检查 `deepdoc/parser/markdown_parser.py` 和 `rag/nlp/__init__.py` 的修改是否生效
- 检查前端 `parser_config.children_delimiter` 是否配置了 `##`
- 检查 `run_graphrag_for_kb` 的 `load_doc_chunks` 合并逻辑是否破坏了标题结构

### 5.3 `_link_entities_to_chapters` 返回空

- 检查 `llm_ents` 是否为空（LLM 未提取到实体）
- 检查 `entity_types` 配置是否与 LLM 返回的类型匹配
- 子串匹配是粗略匹配，短实体名可能匹配失败，可替换为 word-boundary 匹配

---

## 六、变更日志

### 2026-05-12 - 修复：前端 API 截断导致 Book/Chapter 节点和边丢失
- **文件**：`api/apps/services/dataset_api_service.py`
- **变更**：`get_knowledge_graph` 中优先保留 `entity_type` 为 `Book`/`Chapter` 的节点，节点上限放宽到 512，边上限放宽到 512
- **影响**：前端知识图谱可视化能正确展示 Book→Chapter→Entity 层级结构

### 2026-05-12 - 增加全局图合并与实体消解的诊断日志
- **文件**：`rag/graphrag/general/index.py`
- **变更**：`merge_subgraph` 和 `resolve_entities` 中打印 Book 节点的 Chapter 邻居数量
- **影响**：可快速定位边在哪个阶段丢失

### 2026-05-12 - 修复：合并 chunk 时缺少换行符
- **文件**：`rag/graphrag/general/index.py`
- **变更**：`load_doc_chunks` 合并时自动补 `\n` 分隔
- **影响**：防止 `##` 粘在前文末尾导致正则匹配失败

### 2026-05-12 - 修复：fallback 逻辑中 chunks[0] 无法关联章节
- **文件**：`rag/graphrag/general/index.py`
- **变更**：`_extract_book_and_chapters` fallback 逻辑给 `chunks[0]` 分配第一个章节
- **影响**：第一个 chunk 中的实体也能正确关联到章节

### 2026-05-10 - 初始实现
- **文件**：`rag/graphrag/general/index.py`、`deepdoc/parser/markdown_parser.py`、`rag/nlp/__init__.py`
- **变更**：实现 Book/Chapter 提取、Chapter-Entity 关联、delimiter 保留
- **影响**：单文档子图中新增 Book → Chapter → Entity 三层结构
