# 文档切块方法与流程

文档切块（Chunking）是 RAG（检索增强生成）系统的核心环节之一。它将原始文档拆分成语义完整、大小适中的文本块，以便进行向量嵌入和检索。切块策略的选择直接影响检索精度与大模型生成回答的质量。本文档详细介绍 RAGFlow 中的切块方法、核心算法及整体流程。

---

## 1. 为什么切块很重要

在 RAG 系统中，大语言模型（LLM）的上下文窗口是有限的，而知识库中的文档往往很长。切块的核心目标是在**保留语义完整性**与**适配模型上下文**之间取得平衡：

- **粒度太大**：超出嵌入模型或 LLM 的上下文限制，导致信息截断；同时噪声增加，影响检索相关性。
- **粒度太小**：丢失上下文背景，造成语义碎片化。例如一句话被拦腰截断，或表格、代码块被拆分。
- **边界错误**：在句子中间、段落主题切换处切分，会导致向量表示偏离原意。

因此，RAGFlow 针对不同文档类型提供了多种内置切块策略，并支持通过 ingestion pipeline 进行自定义。

---

## 2. 整体流程

在 RAGFlow 中，一份文档从上传到最终生成可检索的 Chunk，需要经历以下阶段：

```
用户上传文件
    ↓
存储至 MinIO（对象存储）
    ↓
Task Executor 拉取任务
    ↓
根据 parser_id 选择对应 Chunker 模块
    ↓
DeepDoc Parser 解析文档（提取纯文本、表格、图片、版面布局）
    ↓
Chunker 执行切块（分片 / 合并 / 层级组织）
    ↓
后处理（可选）
    ├── 自动生成关键词（auto_keywords）
    ├── 自动生成问题（auto_questions）
    ├── 元数据提取（metadata）
    ├── 标签分类（tagging）
    ├── 目录生成（TOC）
    └── RAPTOR 摘要层级构建
    ↓
向量化（Embedding）并写入向量数据库（ES / Infinity）
```

### 2.1 关键模块说明

| 模块 | 文件路径 | 职责 |
|------|---------|------|
| **任务执行器** | `rag/svr/task_executor.py` | 调度切块任务，根据 `parser_id` 映射到具体 Chunker，并触发后处理。 |
| **文档解析器** | `deepdoc/parser/` | 负责 PDF、Word、Excel、Markdown 等格式的原始解析，输出带版面信息的文本段、表格、图片。 |
| **内置 Chunker** | `rag/app/*.py` | 提供按文档类型划分的内置切块策略。 |
| **核心算法** | `rag/nlp/__init__.py` | 提供 `naive_merge`、`hierarchical_merge`、`tree_merge` 等通用合并函数。 |
| **流水线 Chunker** | `rag/flow/chunker/` | 在自定义 Ingestion Pipeline 中使用的 Token Chunker 和 Title Chunker 组件。 |

---

## 3. 内置切块方法

RAGFlow 为不同文档类型预设了多种切块模板。在创建知识库（Dataset）时，你可以根据文档内容选择最合适的策略。

### 3.1 通用型方法

| 方法 | 模块 | 适用场景 | 核心逻辑 |
|------|------|---------|---------|
| **General（通用）** | `rag/app/naive.py` | 大多数文本文档 | 基于 Token 数量进行切分。先按分隔符（如 `\n`）将文档拆成细粒度片段，再依次合并相邻片段，直到达到设定的 `chunk_token_num`（默认 512）。支持重叠（overlap）和自定义分隔符。 |
| **One（整篇）** | `rag/app/one.py` | 短文档、Prompt 模板、邮件摘要 | 将整篇文档视为一个 Chunk，不做切分。 |

### 3.2 结构化方法

| 方法 | 模块 | 适用场景 | 核心逻辑 |
|------|------|---------|---------|
| **Q&A（问答对）** | `rag/app/qa.py` | FAQ、面试题库、试卷 | 要求文档为两列结构（问题 / 答案），每行生成一个独立 Chunk。支持 Excel、CSV、TXT、PDF、DOCX、Markdown。 |
| **Table（表格）** | `rag/app/table.py` | 数据库表、产品目录、日志 | 将 Excel / CSV / TXT 的**每一行**作为一个 Chunk。表头被转化为字段元数据，支持指定某列为“仅索引”或“仅元数据”。 |
| **Book（书籍）** | `rag/app/book.py` | 章节分明的电子书、教材 | 使用 `hierarchical_merge` 检测多级标题（如 `第 x 章`、`x.x 节`），按层级将正文归属到对应标题下，保持章节完整性。 |
| **Laws（法律法规）** | `rag/app/laws.py` | 法律条文、规章制度 | 使用 `tree_merge` 构建标题树。每个 Chunk 包含完整的层级路径（如“第一章 › 第二节 › 第 x 条”），确保引用时的上下文不丢失。 |
| **Manual（手册）** | `rag/app/manual.py` | 产品说明书、操作手册 | 基于 PDF 目录大纲或标题频率启发式识别章节编号，将内容按章节分组。对过小的章节（< 32 tokens）进行向上合并，最大不超过 1024 tokens。 |
| **Paper（论文）** | `rag/app/paper.py` | 学术论文 | 先提取标题、作者、摘要，再按章节（Introduction、Method、Results...）切分，充分利用版面解析信息。 |
| **Presentation（演示文稿）** | `rag/app/presentation.py` | PPT、PDF 幻灯片 | 按页（幻灯片）组装内容，保持页码顺序，每页或相邻页合并为一个 Chunk。 |

### 3.3 特殊格式方法

| 方法 | 模块 | 适用场景 | 核心逻辑 |
|------|------|---------|---------|
| **Resume（简历）** | `rag/app/resume.py` | 简历批量解析 | 结合版面检测与 LLM 提取，将每份简历的关键字段（教育、工作经历等）组织为结构化 Chunk。 |
| **Email（邮件）** | `rag/app/email.py` | 邮件归档分析 | 解析 `.eml` 文件，提取发件人、收件人、主题、正文，再对正文应用 naive_merge。 |
| **Picture（图片）** | `rag/app/picture.py` | 含图文档、扫描件 | 通过 OCR 或 VLM（视觉大模型）提取图片中的文字，生成独立 Chunk。 |
| **Audio（音频）** | `rag/app/audio.py` | 会议录音、播客 | 先转写为文本，再进行后续切分。 |
| **Tag（标签集）** | `rag/app/tag.py` | 标签体系、关键词库 | 将标签集合直接转化为 Chunk。 |

---

## 4. 核心切块算法

RAGFlow 的切块并非简单按字符数截断，而是基于语义边界和 Token 预算进行智能合并。核心算法集中在 `rag/nlp/__init__.py` 中。

### 4.1 Naive Merge（通用合并）

这是最基础的切块算法，被 General、Email、Manual 等多种方法复用。

**流程：**
1. **初分**：按分隔符（如换行符、自定义标记）将文档拆分为细粒度的 `sections`。
2. **累积合并**：依次将 section 加入当前 Chunk，直到总 Token 数达到 `chunk_token_num`。
3. **边界保护**：如果单个 section 本身就超过 Token 上限，则允许该 section 独立成块（避免强行截断）。
4. **重叠（Overlap）**：若配置了 `overlapped_percent`（例如 10%），新 Chunk 会复制上一个 Chunk 末尾相应比例的文本，保证语义连贯性。

**针对 DOCX 的变体（`naive_merge_docx`）**：
- 识别文本、表格、图片三种类型。
- 对表格和图片 Chunk，自动附加相邻文本上下文（`table_context_size` / `image_context_size`），让向量更好地理解非文本内容。

### 4.2 Hierarchical Merge（层级合并）

用于 **Book** 等具有多级标题的文档。

**流程：**
1. **标题检测**：通过正则家族（`BULLET_PATTERN`）识别各级标题，如 `1.`, `1.1`, `1.1.1`。
2. **建立层级关系**：将正文段落归属到最近的祖先标题下。
3. **按层级输出**：每个 Chunk 包含其所属标题路径下的完整正文，或根据配置仅保留某一层级的聚合内容。

### 4.3 Tree Merge（树形合并）

用于 **Laws** 等强层级、需要完整路径引用的文档。

**流程：**
1. **构建节点树**：将各级标题（篇、章、节、条）构建为 `_ChunkNode` 树。
2. **祖先路径继承**：生成 Chunk 时，将当前节点的所有祖先标题拼接为前缀。
3. **输出**：例如“第二章 合同订立 › 第一节 一般规定 › 第四百六十九条 ...”，保证检索到具体法条时仍知晓其上位概念。

---

## 5. 父子切块（Parent-Child Chunking）

父子切块是 RAGFlow 中提升检索精度的关键机制。

### 5.1 概念

- **父 Chunk（Parent）**：较大的语义单元，通常是按照通用策略切出的完整段落（例如 512 tokens）。父 Chunk 保留完整的上下文信息。
- **子 Chunk（Child）**：在父 Chunk 内部进一步切分的更细粒度单元（例如按句子、按固定 Token 数）。子 Chunk 用于**向量检索**。

**工作流程：**
1. 检索阶段：查询向量与**子 Chunk** 进行相似度匹配，找到最相关的细粒度单元。
2. 召回阶段：系统根据子 Chunk 关联的 `mom` / `mom_with_weight` 字段，回查其对应的**父 Chunk**。
3. 生成阶段：将父 Chunk 作为上下文送入 LLM，保证模型拥有完整、连贯的背景信息。

这种设计的优势在于：子 Chunk 粒度细、匹配精度高；父 Chunk 信息全、上下文丢失少。

### 5.2 配置方式

在知识库配置或 Ingestion Pipeline 的 Token Chunker 中：
- 启用“Child chunk are used for retrieval”开关。
- 设置 **Children Delimiters**（子分隔符）：用于在父 Chunk 内部进行二次切分的标记（如 `。`、`\n`、`?`）。
- 切块器会自动将父文本存入 `mom_with_weight`，子文本存入 `content_with_weight`。

---

## 6. 自定义流水线中的切块组件

在 RAGFlow 的 **Agent Canvas** 中，用户可以构建自定义的 Ingestion Pipeline。流水线中提供两种 Chunker 组件：

### 6.1 Token Chunker

**文件路径**：`rag/flow/chunker/token_chunker.py`

**核心参数**：

| 参数 | 说明 |
|------|------|
| `delimiter_mode` | 切分模式：`token_size`（按 Token 数）、`delimiter`（按分隔符）、`one`（整篇）。 |
| `chunk_token_size` | 每个 Chunk 的最大 Token 数（默认 512）。 |
| `delimiters` | 分隔符列表（默认 `["\n"]`），支持自定义标记（用反引号包裹，如 `` `##` ``）。 |
| `overlapped_percent` | 相邻 Chunk 的重叠比例（0–30）。 |
| `children_delimiters` | 子分隔符，启用父子切块时使用。 |
| `table_context_size` / `image_context_size` | 为表格/图片 Chunk 附加的上下文 Token 数。 |

**逻辑**：
- 上游为结构化 JSON（如 Parser 输出）时，会先识别 `text` / `table` / `image` 类型，再对文本部分进行合并。
- 上游为纯文本 / Markdown / HTML 时，直接按分隔符或 Token 大小合并。

### 6.2 Title Chunker

**文件路径**：`rag/flow/chunker/title_chunker/title_chunker.py`

Title Chunker 包含两种子模式：

#### A. Hierarchy 模式（层级模式）
- 构建 `_ChunkNode` 标题树。
- 每个 Chunk 包含从根到当前节点的完整标题路径 + 正文。
- 可选配置：
  - `include_heading_content`：是否将父级标题文本单独保留为 Chunk。
  - `root_chunk_as_heading`：是否将文档开头部分作为全局标题附加到所有 Chunk（适用于简历等场景）。

#### B. Group 模式（分组模式）
- 将文档扁平化为目标层级（`hierarchy`）的段落。
- 对相邻的小段落进行智能合并：
  - 若段落小于 `MIN_GROUP_TOKENS`（32），则持续向下合并。
  - 同一章节内，若总 Token 小于 `MAX_GROUP_TOKENS`（1024），则继续合并。
- 避免过度碎片化，同时保持标题边界。

---

## 7. 如何选择切块方法

不同文档类型对切块策略的需求差异很大。以下是常见的选择建议：

| 文档类型 | 推荐方法 | 理由 |
|---------|---------|------|
| 普通文章、博客、小说 | **General** | 内容连续，按 Token + 分隔符切分即可。 |
| 论文、技术报告 | **Paper** | 需要保留摘要、章节结构，版面解析更精准。 |
| 法律法规、政策文件 | **Laws** | 条文依赖层级路径（章 › 节 › 条），tree_merge 保证引用完整。 |
| 产品手册、说明书 | **Manual** | 按章节分组，合并过小段落，兼顾检索与连贯。 |
| FAQ、问答库 | **Q&A** | 天然的两列结构，一行一 Chunk 最精准。 |
| 数据库表、CSV | **Table** | 每行一条记录，表头映射为元数据。 |
| 简历 | **Resume** | 专门解析版面与字段，提取结构化信息。 |
| 邮件归档 | **Email** | 提取收发件人、主题，正文再做通用切分。 |
| PPT / PDF 幻灯片 | **Presentation** | 按页聚合，保持单页语义内聚。 |
| 扫描件、图片 PDF | **Picture** + OCR | 先提取图中文字，再视情况选择 General 或 Laws。 |
| 短文本、Prompt 库 | **One** | 无需切分，整篇作为一个 Chunk。 |

### 7.1 进阶建议

- **追求高精度检索**：启用**父子切块**。让子 Chunk 负责命中检索，父 Chunk 负责提供完整上下文。
- **上下文极度重要**：增大 `chunk_token_num`（如 1024 或 2048），或选择标题型策略（Book / Laws / Manual）。
- **表格/图片较多**：使用 General 或 Pipeline Token Chunker，并调高 `table_context_size` 与 `image_context_size`。
- **自定义分隔符**：如果文档有固定的章节标记（如 `##`、`<h2>`、特定编号），在 General 方法中将这些标记设为分隔符，可显著提升切块边界质量。

---

## 8. 关键配置参数详解

在知识库设置或 Ingestion Pipeline 中，以下参数决定了切块的最终效果：

| 参数 | 作用范围 | 说明 |
|------|---------|------|
| **Chunk Token Number** | 通用 | 单个 Chunk 的最大 Token 数。默认 512，可根据嵌入模型和 LLM 上下文调整。 |
| **Delimiter** | General / Token Chunker | 初级切分标记。默认 `\n`，支持多分隔符和反引号正则模式。 |
| **Overlapped Percent** | General / Token Chunker | 相邻 Chunk 的重叠比例，避免在边界处丢失上下文。建议 5%–10%。 |
| **Children Delimiter** | 父子切块 | 在父 Chunk 内部分割子 Chunk 的标记，如句号、问号、换行。 |
| **Table Context Size** | DOCX / Pipeline | 为表格 Chunk 附加的上下文文本 Token 数，帮助理解表格内容。 |
| **Image Context Size** | DOCX / Pipeline | 为图片 Chunk 附加的上下文文本 Token 数。 |
| **Hierarchy** | Title Chunker (Group) | 目标标题层级，决定按哪一级标题进行扁平化分组。 |
| **Heading Levels** | Title Chunker (Hierarchy) | 用于识别标题的正则家族，如 `["^\\d+\\.", "^\\d+\\.\\d+"]`。 |

---

## 9. 总结

RAGFlow 的切块系统不是“一刀切”，而是根据文档结构、内容类型和业务目标提供了多层次的策略：

1. **内置模板**：覆盖论文、法律、表格、简历等常见场景，开箱即用。
2. **核心算法**：`naive_merge`、`hierarchical_merge`、`tree_merge` 保证语义边界清晰。
3. **父子切块**：通过“细粒度检索 + 粗粒度上下文”平衡精度与召回。
4. **流水线组件**：Token Chunker 与 Title Chunker 提供灵活可编排的自定义能力。
5. **丰富参数**：Token 数、分隔符、重叠度、上下文窗口等均可调，适配不同嵌入模型与 LLM。

合理选择切块方法，并根据实际检索效果进行参数调优，是构建高质量 RAG 知识库的关键步骤。
