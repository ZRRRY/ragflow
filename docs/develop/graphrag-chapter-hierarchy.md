# GraphRAG 章节层级图谱定制文档

> 本文档记录对 RAGFlow GraphRAG 模块的定制修改，用于在构建知识图谱时同时建立 Markdown 书籍的章节结构关系（Book → Chapter → Entity）。
> 
> **维护要求**：任何后续对本定制逻辑的修改（包括但不限于函数签名变更、新增层级、调整匹配策略）都必须同步更新此文档的【变更日志】与【详细修改说明】章节。

---

## 1. 修改目标

在 RAGFlow 默认的 GraphRAG（实体-关系抽取）基础上，增加对 Markdown 文档标题层级的识别：

1. **书籍-章节关系**：提取 `# 书名`（一级标题）和 `## 章节名`（二级标题），建立 `Book --contains--> Chapter` 关系。
2. **章节-实体关系**：将 LLM 从文本中抽取的普通实体，关联到包含该实体的章节，建立 `Chapter --involves--> Entity` 关系。
3. **多书隔离**：同一知识库内有多本书籍时，同名章节不会合并为同一个节点。

---

## 2. 修改文件清单

| 文件路径 | 修改类型 | 说明 |
|---------|---------|------|
| `rag/graphrag/general/index.py` | 新增函数 + 修改现有函数 | 核心逻辑文件：章节提取、实体关联、调试日志 |
| `deepdoc/parser/markdown_parser.py` | 修改现有函数 | `MarkdownElementExtractor.extract_elements`：修复 delimiter 丢失问题，让 `##` 保留在 section 开头 |
| `rag/nlp/__init__.py` | 修改现有函数 | `split_with_pattern`：修复 delimiter 丢失问题，让 `##` 保留在 chunk 开头 |
| `docker/docker-compose.yml` | 修改配置 | 给 `ragflow-gpu` 补上源码挂载 |

---

## 3. 详细修改说明

### 3.1 `rag/graphrag/general/index.py`

#### 3.1.1 新增 `import re`

- **位置**：文件顶部，与其他 `import` 同级
- **原因**：新增的正则提取逻辑需要使用 `re.search` / `re.finditer`。

#### 3.1.2 新增 `_extract_book_and_chapters(doc_id, chunks)`

- **位置**：`generate_subgraph` 函数之前
- **职责**：
  1. 扫描所有 chunk，通过正则 `^#\s+(.+)$` 提取书籍名（一级标题）。
  2. 对每个 chunk，通过正则 `^##\s+(.+)$` 提取章节名（二级标题）。
  3. 生成 `Book` / `Chapter` 实体，以及 `contains` 关系。
  4. 返回每个 chunk 对应的章节节点名列表（用于后续实体关联）。
- **节点命名规范**：
  - Book 实体：`entity_name = 书名`（例如：`深度学习`）
  - Chapter 实体：`entity_name = 《书名》章节名`（例如：`《深度学习》第一章 绪论`）
  - **设计理由**：Chapter 节点加书名前缀，确保同一知识库内多本书的同名章节不会合并为同一个节点。
- **关键代码**（节选）：
  ```python
  chapter_node_name = f"《{book_title}》{chapter}"
  chapter_entities.append({
      "entity_name": chapter_node_name,
      "entity_type": "Chapter",
      "description": f"《{book_title}》的章节：{chapter}",
      ...
  })
  ```
- **边界情况**：
  - 若文档中不存在 `# 书名`，则返回空列表，不会生成任何章节节点。
  - 若同一章节名在多个 chunk 中出现，通过 `seen_chapters` 去重，只生成一个 Chapter 节点。

#### 3.1.3 新增 `_link_entities_to_chapters(doc_id, chunks, entities, chunk_chapters)`

- **位置**：`_extract_book_and_chapters` 之后
- **职责**：
  1. 遍历 LLM 提取的原始实体列表。
  2. 跳过 `entity_type` 为 `Book` 或 `Chapter` 的节点（避免自环）。
  3. 对每个实体，检查其名称（小写）是否出现在每个 chunk 的文本中（简单子串匹配）。
  4. 若匹配成功，则将该实体与该 chunk 对应的所有章节建立 `involves` 关系。
- **去重机制**：使用 `seen_pairs`（`(chapter_node_name, entity_name)` 元组集合）避免重复边。
- **关键代码**（节选）：
  ```python
  if ent_name_lower in text:
      for chapter in chunk_chapters[idx]:
          pair = (chapter, ent_name)
          if pair in seen_pairs:
              continue
          ...
          relations.append({
              "src_id": chapter,
              "tgt_id": ent_name,
              "description": f"章节《{chapter}》涉及实体《{ent_name}》",
              "keywords": ["involves", "章节", "实体"],
              ...
          })
  ```
- **精度说明**：基于子串匹配，非语义匹配。如果 chunk 合并导致一个 chunk 包含多个章节，实体将关联到该 chunk 内的所有章节。

#### 3.1.4 修改 `generate_subgraph(...)`

- **位置**：函数体内，`llm_ents, llm_rels = await ext(...)` 之后
- **修改内容**：
  1. 调用 `_extract_book_and_chapters` 获取章节实体与关系。
  2. 合并 LLM 实体与章节实体（`merged_ents`）：
     - 以 `entity_name` 为 key 去重。
     - 若 LLM 提取的实体与章节/书名同名，优先保留 `Book` / `Chapter` 的 `entity_type` 和 `description`。
  3. 调用 `_link_entities_to_chapters`，将 LLM 实体与章节做文本匹配关联。
  4. 将生成的章节实体、章节关系、实体-章节关系统一注入到 `ents` 和 `rels` 列表中，随原有逻辑写入 `networkx.Graph`。
- **关键代码**（节选）：
  ```python
  llm_ents, llm_rels = await ext(doc_id, chunks, callback, task_id=task_id)

  # ---------- 注入章节层级与实体关联 ----------
  _, chapter_ents, chapter_rels, chunk_chapters = _extract_book_and_chapters(doc_id, chunks)

  ents = list(llm_ents)
  rels = list(llm_rels)

  if chapter_ents:
      # 合并 LLM 实体与章节实体，同名时保留 Book/Chapter 类型
      merged_ents = {}
      for ent in llm_ents:
          merged_ents[ent["entity_name"]] = dict(ent)
      for cent in chapter_ents:
          name = cent["entity_name"]
          if name in merged_ents:
              existing = merged_ents[name]
              existing["source_id"] = sorted(set(existing.get("source_id", []) + cent.get("source_id", [])))
              if cent.get("entity_type") in ("Book", "Chapter"):
                  existing["entity_type"] = cent["entity_type"]
                  existing["description"] = cent["description"]
          else:
              merged_ents[name] = dict(cent)
      ents = list(merged_ents.values())
      rels.extend(chapter_rels)

      # 建立章节-实体关系（只对 LLM 提取的原始实体做匹配）
      entity_chapter_rels = _link_entities_to_chapters(doc_id, chunks, llm_ents, chunk_chapters)
      rels.extend(entity_chapter_rels)
  # -------------------------------------------
  ```

### 3.2 `deepdoc/parser/markdown_parser.py`

#### 3.2.1 修改 `MarkdownElementExtractor.extract_elements(delimiter, include_meta)`

- **位置**：`deepdoc/parser/markdown_parser.py`
- **原有问题**：
  - 当 `delimiter` 非空时，`extract_elements` 使用 `pattern.finditer(text)` 提取 delimiter **之间**的文本片段。
  - delimiter 本身（如 `##`）被完全丢弃，不会出现在返回的 section `content` 中。
  - 这是 chunk 中缺失 `##` 的**第一源头**。
- **修改内容**：
  - 将 `pattern.finditer` 改为 `pattern.split(text)` + `pattern.finditer(text)` 组合。
  - 第一个 delimiter 之前的文本仍作为独立 section。
  - 后续每个 delimiter 与它后面的文本拼接成 section，delimiter 保留在 **section 开头**。
- **关键代码**（修改后节选）：
  ```python
  pattern = re.compile(dels)
  txts = [txt for txt in pattern.split(text)]
  delimiters = [text[m.start():m.end()] for m in pattern.finditer(text)]

  # 第一个 delimiter 之前的文本
  if txts[0].strip():
      sections.append({"content": txts[0].strip(), ...})

  # 每个 delimiter 与它后面的文本组成 section，delimiter 保留在开头
  pos = len(txts[0])
  for i, delim in enumerate(delimiters):
      if i + 1 < len(txts):
          combined = (delim + txts[i + 1]).strip()
          if combined:
              sections.append({"content": combined, ...})
          pos += len(delim) + len(txts[i + 1])
  ```
- **示例效果**：
  - 输入文本：`"前言\n## 第一章\n内容1\n## 第二章\n内容2"`，delimiter = `##`
  - 输出 sections：
    1. `"前言"`
    2. `"## 第一章\n内容1"`
    3. `"## 第二章\n内容2"`
  - 每个 section 开头都保留了完整的 `## 章节名`。

### 3.3 `rag/nlp/__init__.py`

#### 3.3.1 修改 `split_with_pattern(d, pattern, content, eng)`

- **位置**：`rag/nlp/__init__.py`
- **原有问题**：
  - 原循环 `for j in range(0, len(txts), 2)` 把 delimiter 放在 chunk **末尾**（`txt += txts[j + 1]`）。
  - 更严重的是：如果文本以 delimiter 开头，`txts[0]` 为空字符串，会被 `continue` 跳过，导致**第一个 delimiter 完全丢失**。
  - 对 Markdown 而言，`## 章节名` 被当作 `children_delimiter` 时，第一个章节的标题会彻底消失。
- **修改内容**：
  - 把 `txts[0]`（第一个 delimiter 之前的文本）单独作为一个 chunk。
  - 把循环改为 `for j in range(1, len(txts), 2)`，遍历所有 delimiter（奇数索引），将 `delimiter + 后续文本` 组合成 chunk。
  - 这样 delimiter 保留在 chunk 的**开头**。
- **关键代码**（修改后）：
  ```python
  txts = [txt for txt in compiled_pattern.split(content)]

  # 第一个 delimiter 之前的文本（如果存在）作为独立 chunk
  if txts[0].strip():
      dd = copy.deepcopy(d)
      tokenize(dd, txts[0], eng)
      docs.append(dd)

  # 每个 delimiter 与它后面的文本组成 chunk，delimiter 保留在 chunk 开头
  for j in range(1, len(txts), 2):
      txt = txts[j]
      if j + 1 < len(txts):
          txt += txts[j + 1]
      if not txt.strip():
          continue
      dd = copy.deepcopy(d)
      tokenize(dd, txt, eng)
      docs.append(dd)
  ```
- **示例效果**：
  - 输入：`"## 第一章\n内容1\n## 第二章\n内容2"`
  - 输出 chunks：
    1. `'## 第一章\n内容1\n'`
    2. `'## 第二章\n内容2'`
  - 每个 chunk 开头都保留了完整的 `## 章节名`。

---

## 4. 数据流与图谱结构

### 4.1 单文档内的子图（subgraph）

```text
Book(title)
    └── contains ──── Chapter(《title》chapter_1)
    │                     └── involves ──── Entity_A
    │                     └── involves ──── Entity_B
    └── contains ──── Chapter(《title》chapter_2)
                          └── involves ──── Entity_C
```

### 4.2 多文档合并后的全局图（global graph）

- **Book 节点**：若两本书书名相同，会合并为一个节点（这是 GraphRAG 的默认行为，通常是期望的）。
- **Chapter 节点**：由于节点名带了书名前缀（`《书名》章节名`），不同书籍的同名章节会保持独立。
- **Entity 节点**：不同文档提取的同名实体仍会按原有 GraphRAG 逻辑合并。

---

## 5. 已知问题与注意事项

| 问题 | 影响 | 建议 |
|-----|------|------|
| 子串匹配误差 | `_link_entities_to_chapters` 使用 `in` 做字符串匹配，短实体名（如"AI"）可能匹配到无关文本。 | 若精度不足，可替换为基于 word-boundary 或语义向量的匹配。 |
| Chunk 合并导致跨章节关联 | 若 chunk 合并后一个 chunk 包含多个 `##` 标题，该 chunk 中的实体会关联到所有章节。 | 确保 `parser_config.chunk_token_num` 足够大以容纳完整章节，或使用更严格的标题切分策略。 |
| ~~无一级标题时章节丢失~~（已修复） | 当 Markdown 中没有 `# 书名` 时，自动使用文档文件名（去除扩展名）作为 `fallback_title` 回退。 | — |
| 章节-实体关系的 `weight` 固定为 1 | 不涉及权重计算。 | 如需根据提及频次加权，可统计实体在 chunk 中的出现次数并累加 `weight`。 |

---

## 6. 后续维护指引

### 6.1 何时更新本文档

以下任何操作都必须更新本文档：
- 修改 `_extract_book_and_chapters` 或 `_link_entities_to_chapters` 的函数签名、返回值、核心逻辑。
- 调整 Chapter / Book 节点的命名格式（如前缀样式）。
- 修改实体与章节的关联策略（如从子串匹配改为 chunk-index 精确追踪）。
- 新增更多层级（如 `###` 三级标题入图）。
- 引入新的 entity_type（如 `Section`、`Volume` 等）。

### 6.2 更新格式

请在【变更日志】中以倒序时间线追加记录，格式如下：

```markdown
### YYYY-MM-DD - 修改人/原因
- **文件**：`rag/graphrag/general/index.py`
- **变更**：简述改动内容
- **影响**：对图谱结构或检索的影响
```

---

## 7. 变更日志

### 2026-05-10 - 修复：chunk 中无 `##` 时的章节标题提取 + Docker GPU 挂载
- **文件**：`rag/graphrag/general/index.py`、`docker/docker-compose.yml`
- **变更**：
  - `docker/docker-compose.yml`：给 `ragflow-gpu` 服务补上 `- ../rag:/ragflow/rag` volume 挂载，修复 GPU 模式下本地源码修改不生效的问题。
  - `_extract_book_and_chapters`：增加 `has_markdown_headers` 标志。当 chunk 中完全匹配不到 `^##\s+(.+)$` 时（说明 `##` 在 chunk 生成阶段被 `split_with_pattern` 当作 delimiter 切掉了），自动从每个后续 chunk 的第一行非空文本提取章节标题（长度限制 100 字符以内）。
  - `generate_subgraph`：增加 `[ChapterGraph DEBUG]` callback 日志，打印 chunk 预览、章节提取数量和实体关联数量。
- **根因说明**：`rag/app/naive.py` 的 `chunk()` 函数在处理 Markdown 时会调用 `tokenize_chunks(..., child_delimiters_pattern=child_deli)`。若解析配置中的 `children_delimiter` 包含 `##`，`split_with_pattern` 会按 `##` 切分 chunk 并将 `##` 本身删除，导致后续正则无法匹配。
- **影响**：
  - 即使 chunk 内容中已无 `##` 前缀，仍能正确提取章节标题并建立 Book/Chapter/Entity 层级。
  - GPU 部署模式下本地源码修改即时生效。

### 2026-05-10 - 修复：MarkdownElementExtractor 保留 delimiter 在 section 开头
- **文件**：`deepdoc/parser/markdown_parser.py`
- **变更**：
  - 修改 `MarkdownElementExtractor.extract_elements` 的 `include_meta=True` 分支。
  - 原逻辑用 `pattern.finditer(text)` 提取 delimiter 之间的文本，delimiter 本身被丢弃。
  - 新逻辑用 `pattern.split(text)` + `pattern.finditer(text)` 组合，把每个 delimiter 与它后面的文本拼接成 section，delimiter 保留在 **section 开头**。
- **原因**：`extract_elements` 是 chunk 中缺失 `##` 的**第一源头**。当用户在前端配置 delimiter 为 `` `##` `` 时，`extract_elements` 按 `##` 切分 Markdown 文本，但 `##` 本身不会出现在返回的 section content 中。
- **影响**：
  - Markdown 按 `##` 切分后的 section 开头保留完整的 `## 章节名`。
  - 后续 `split_with_pattern` 不再需要对 Markdown 做二次修复。

### 2026-05-10 - 修复：split_with_pattern 保留 delimiter 在 chunk 开头
- **文件**：`rag/nlp/__init__.py`
- **变更**：
  - 修改 `split_with_pattern`：修复文本以 delimiter 开头时第一个 delimiter 被丢弃的 bug。
  - 将 delimiter 的保留位置从 chunk **末尾**改为 chunk **开头**。
- **原因**：`split_with_pattern` 原循环 `for j in range(0, len(txts), 2)` 在 `txts[0]` 为空时直接 `continue`，导致第一个 `##` 完全丢失；且后续 delimiter 被附加在 chunk 末尾，无法被正则 `^##` 匹配。
- **影响**：
  - 作为 `extract_elements` 之后的第二道保障，确保 `##` 保留在 chunk 开头。
  - `_extract_book_and_chapters` 的正则可以正常匹配，fallback 逻辑不再被触发。

### 2026-05-10 - 初始实现
- **文件**：`rag/graphrag/general/index.py`
- **变更**：
  - 新增 `import re`。
  - 新增 `_extract_book_and_chapters`：从 chunk 文本提取 `# 书名` 和 `## 章节名`，生成 `Book` / `Chapter` 实体及 `contains` 关系。Chapter 节点名使用 `《书名》章节名` 格式以避免多书冲突。
  - 新增 `_link_entities_to_chapters`：基于子串匹配将 LLM 实体关联到对应章节，生成 `involves` 关系。
  - 修改 `generate_subgraph`：在 LLM 抽取后注入上述章节层级与实体关联逻辑，并合并同名实体（优先保留 `Book` / `Chapter` 类型）。
- **影响**：
  - 单文档子图中新增 `Book → Chapter → Entity` 三层结构。
  - 多文档场景下同名章节不再合并，保持独立。
