#
#  Copyright 2024 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import asyncio
import logging
import itertools
import os
import re
from dataclasses import dataclass
from typing import Any, Callable

import networkx as nx

from rag.graphrag.general.extractor import Extractor
from rag.nlp import is_english
import editdistance
from rag.graphrag.entity_resolution_prompt import ENTITY_RESOLUTION_PROMPT
from rag.llm.chat_model import Base as CompletionLLM
from rag.graphrag.config import GraphRAGConfig
from rag.graphrag.utils import (
    perform_variable_replacements,
    chat_limiter,
    GraphChange,
)
from api.db.services.task_service import has_canceled
from common.exceptions import TaskCanceledException


DEFAULT_RECORD_DELIMITER = "##"
DEFAULT_ENTITY_INDEX_DELIMITER = "<|>"
DEFAULT_RESOLUTION_RESULT_DELIMITER = "&&"

# Entity types that should skip resolution (book and chapter nodes are
# intentionally kept separate even if they share names across documents).
EXCLUDED_RESOLUTION_TYPES = {"书籍", "章节"}

def _has_digit_in_2gram_diff(a, b):
    def to_2gram_set(s):
        return {s[i:i + 2] for i in range(len(s) - 1)}

    set_a = to_2gram_set(a)
    set_b = to_2gram_set(b)
    diff = set_a ^ set_b
    return any(any(c.isdigit() for c in pair) for pair in diff)


def is_similarity_str(a, b):
    """模块级字符级相似度判断（不依赖 EntityResolution 实例）。"""
    if _has_digit_in_2gram_diff(a, b):
        return False

    if is_english(a) and is_english(b):
        if editdistance.eval(a, b) <= min(len(a), len(b)) // 2:
            return True
        return False

    a, b = set(a), set(b)
    max_l = max(len(a), len(b))
    if max_l < 4:
        return len(a & b) > 1

    return len(a & b) * 1.0 / max_l >= 0.8


# Phase 1.5: 实体类型别名映射。用户的 kb_parser_config["graphrag"]["entity_types"]
# 里可能用英文 (Book/Chapter)，而 LLM 抽出的 entity_type 字段是中文 (书籍/章节)；
# 或者反过来。aliases 让 build_excluded_types() 一次把多种写法归一。
_EXCLUDED_TYPE_ALIASES = {
    "书籍": {"书籍", "book", "books", "Book", "Books", "BOOK"},
    "章节": {"章节", "chapter", "chapters", "Chapter", "Chapters", "CHAPTER",
             "section", "sections", "Section", "Sections", "SECTION"},
}


def build_excluded_types(excluded_from_config: list[str] | None = None) -> set[str]:
    """根据用户 parser_config 构造 excluded types 集合。

    Phase 1.5: 原先硬编码 {书籍, 章节}，导致用户用英文 entity_types 时
    Book/Chapter 仍会被归一（同名 Book 跨 doc 错误合并）。本函数展开
    aliases 做大小写不敏感的匹配。

    Args:
        excluded_from_config: 来自 kb_parser_config["graphrag"]["entity_types"]
            的类型列表，匹配其中的项目会被加入 excluded 集合。

    Returns:
        应该跳过 resolution 的 entity_type 集合。
    """
    result: set[str] = set(EXCLUDED_RESOLUTION_TYPES)
    if not excluded_from_config:
        return result
    for cfg_type in excluded_from_config:
        if not isinstance(cfg_type, str):
            continue
        cfg_type_stripped = cfg_type.strip()
        if not cfg_type_stripped:
            continue
        # 1) 自身加进去
        result.add(cfg_type_stripped)
        # 2) 找反向别名（用户写的是"book"，把"书籍"也加进去；写"书籍"则把"book"也加进去）
        for canonical, aliases in _EXCLUDED_TYPE_ALIASES.items():
            all_aliases_lower = {a.lower() for a in aliases}
            if cfg_type_stripped.lower() in all_aliases_lower:
                result.update(aliases)
    return result


@dataclass
class EntityResolutionResult:
    """Entity resolution result class definition."""
    graph: nx.Graph
    change: GraphChange


class EntityResolution(Extractor):
    """Entity resolution class definition."""

    _resolution_prompt: str
    _output_formatter_prompt: str
    _record_delimiter_key: str
    _entity_index_delimiter_key: str
    _resolution_result_delimiter_key: str

    def __init__(
            self,
            llm_invoker: CompletionLLM,
            excluded_types: set[str] | None = None,
    ):
        super().__init__(llm_invoker)
        """Init method definition."""
        self._llm = llm_invoker
        self._resolution_prompt = ENTITY_RESOLUTION_PROMPT
        self._record_delimiter_key = "record_delimiter"
        self._entity_index_delimiter_key = "entity_index_delimiter"
        self._resolution_result_delimiter_key = "resolution_result_delimiter"
        self._input_text_key = "input_text"
        # Phase 1.5: 允许调用方注入 excluded types；缺省回退到全局硬编码
        self._excluded_types = excluded_types if excluded_types is not None else EXCLUDED_RESOLUTION_TYPES

    async def __call__(self, graph: nx.Graph,
                       subgraph_nodes: set[str],
                       prompt_variables: dict[str, Any] | None = None,
                       callback: Callable | None = None,
                       task_id: str = "",
                       candidate_resolution: dict[str, list[tuple[str, str]]] | None = None,
                       ) -> EntityResolutionResult:
        """Call method definition."""
        if prompt_variables is None:
            prompt_variables = {}

        # Wire defaults into the prompt variables
        self.prompt_variables = {
            **prompt_variables,
            self._record_delimiter_key: prompt_variables.get(self._record_delimiter_key)
                                        or DEFAULT_RECORD_DELIMITER,
            self._entity_index_delimiter_key: prompt_variables.get(self._entity_index_delimiter_key)
                                              or DEFAULT_ENTITY_INDEX_DELIMITER,
            self._resolution_result_delimiter_key: prompt_variables.get(self._resolution_result_delimiter_key)
                                                   or DEFAULT_RESOLUTION_RESULT_DELIMITER,
        }

        nodes = sorted(graph.nodes())
        # Phase 1.5: 用 self._excluded_types 替代全局硬编码
        excluded = self._excluded_types
        entity_types = sorted({
            graph.nodes[node].get('entity_type', '-')
            for node in nodes
            if graph.nodes[node].get('entity_type') not in excluded
        })
        node_clusters = {entity_type: [] for entity_type in entity_types}

        for node in nodes:
            ent_type = graph.nodes[node].get('entity_type', '-')
            if ent_type not in excluded:
                node_clusters[ent_type].append(node)

        if candidate_resolution is not None:
            # KNN 路径：外部注入 candidate pairs，跳过内部生成。
            # 仅做 excluded_types 过滤防御。
            candidate_resolution = {
                k: v for k, v in candidate_resolution.items()
                if k not in excluded
            }
        else:
            # Char-level filtering path (no in-memory ANN, no embedding).
            candidate_resolution = {entity_type: [] for entity_type in entity_types}
            for k, v in node_clusters.items():
                candidate_resolution[k] = [
                    (a, b) for a, b in itertools.combinations(v, 2)
                    if (a in subgraph_nodes or b in subgraph_nodes) and self.is_similarity(a, b)
                ]
        num_candidates = sum([len(candidates) for _, candidates in candidate_resolution.items()])
        callback(msg=f"Identified {num_candidates} candidate pairs")
        remain_candidates_to_resolve = num_candidates

        resolution_result = set()
        resolution_result_lock = asyncio.Lock()
        resolution_batch_size = GraphRAGConfig.RESOLUTION_BATCH_SIZE
        max_concurrent_tasks = GraphRAGConfig.RESOLUTION_MAX_CONCURRENT_TASKS
        semaphore = asyncio.Semaphore(max_concurrent_tasks)
        # Phase 3.3: 进度计数，供前端 progress bar 刷新
        progress_lock = asyncio.Lock()
        progress_state = {"done_batches": 0}

        async def limited_resolve_candidate(candidate_batch, result_set, result_lock):
            nonlocal remain_candidates_to_resolve, callback
            async with semaphore:
                try:
                    enable_timeout_assertion = os.environ.get("ENABLE_TIMEOUT_ASSERTION")
                    timeout_sec = 280 if enable_timeout_assertion else 1_000_000_000

                    try:
                        await asyncio.wait_for(
                            self._resolve_candidate(candidate_batch, result_set, result_lock, task_id),
                            timeout=timeout_sec
                        )
                        remain_candidates_to_resolve -= len(candidate_batch[1])
                        # Phase 3.3: progress 在 0.5..0.9 区间线性映射
                        async with progress_lock:
                            progress_state["done_batches"] += 1
                            done_b = progress_state["done_batches"]
                        if total_batches:
                            prog = 0.5 + 0.4 * (done_b / total_batches)
                        else:
                            prog = 0.9
                        callback(
                            prog=prog,
                            msg=f"Resolved {len(candidate_batch[1])} pairs, "
                                f"{remain_candidates_to_resolve} remain. "
                                f"[batch {done_b}/{total_batches}]",
                        )

                    except asyncio.TimeoutError:
                        logging.warning(f"Timeout resolving {candidate_batch}, skipping...")
                        remain_candidates_to_resolve -= len(candidate_batch[1])
                        async with progress_lock:
                            progress_state["done_batches"] += 1
                        callback(
                            msg=f"Failed to resolve {len(candidate_batch[1])} pairs due to timeout, skipped. "
                                f"{remain_candidates_to_resolve} remain."
                        )

                except Exception as exception:
                    logging.error(f"Error resolving candidate batch: {exception}")


        # Phase 3.2: 用 asyncio.Queue + 5 worker 协程持续消费 candidate batches，
        # 替代"一次性 push 几十万 task 进 tasks 列表"的模式。50w batch 时新实现
        # 内存占用从 ~500MB 降到 ~5MB（5 个 worker + 1 个 queue）。
        # 进度条同时细化（Phase 3.3）。
        all_batches: list[tuple[str, list[tuple[str, str]]]] = []
        for key, lst in candidate_resolution.items():
            if not lst:
                continue
            for i in range(0, len(lst), resolution_batch_size):
                all_batches.append((key, lst[i : i + resolution_batch_size]))
        total_batches = len(all_batches)
        callback(msg=f"Identified {num_candidates} candidate pairs across {total_batches} batches; spinning up {max_concurrent_tasks} workers.")

        batch_queue: asyncio.Queue = asyncio.Queue()
        for b in all_batches:
            batch_queue.put_nowait(b)
        # 哨兵值让 worker 知道队列已空
        _SENTINEL: tuple = ("__SENTINEL__",)
        for _ in range(max_concurrent_tasks):
            batch_queue.put_nowait(_SENTINEL)

        async def worker():
            while True:
                item = await batch_queue.get()
                if item is _SENTINEL:
                    batch_queue.task_done()
                    return
                try:
                    await limited_resolve_candidate(item, resolution_result, resolution_result_lock)
                except Exception as exc:
                    # limited_resolve_candidate 内部已经 try/except，这里只兜底
                    logging.exception("worker: unhandled error on batch type=%s: %s", item[0], exc)
                finally:
                    batch_queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(max_concurrent_tasks)]
        try:
            await asyncio.gather(*workers, return_exceptions=True)
        except Exception as e:
            logging.error("Error in worker pool: %s", e)
            raise

        callback(msg=f"Resolved {num_candidates} candidate pairs, {len(resolution_result)} of them are selected to merge.")

        change = GraphChange()
        connect_graph = nx.Graph()
        connect_graph.add_edges_from(resolution_result)

        merge_lock = asyncio.Lock()

        async def limited_merge_nodes(graph, nodes, change):
            async with merge_lock:
                await self._merge_graph_nodes(graph, nodes, change, task_id)

        tasks = []
        for sub_connect_graph in nx.connected_components(connect_graph):
            merging_nodes = list(sub_connect_graph)
            tasks.append(asyncio.create_task(limited_merge_nodes(graph, merging_nodes, change)))
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except Exception as e:
            logging.error(f"Error merging nodes: {e}")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        # Update pagerank
        pr = nx.pagerank(graph)
        for node_name, pagerank in pr.items():
            graph.nodes[node_name]["pagerank"] = pagerank

        return EntityResolutionResult(
            graph=graph,
            change=change,
        )

    async def _resolve_candidate(self, candidate_resolution_i: tuple[str, list[tuple[str, str]]], resolution_result: set[str], resolution_result_lock: asyncio.Lock, task_id: str = ""):
        if task_id:
            if has_canceled(task_id):
                logging.info(f"Task {task_id} cancelled during entity resolution candidate processing.")
                raise TaskCanceledException(f"Task {task_id} was cancelled")

        pair_txt = [
            f'When determining whether two {candidate_resolution_i[0]}s are the same, you should only focus on critical properties and overlook noisy factors.\n']
        for index, candidate in enumerate(candidate_resolution_i[1]):
            pair_txt.append(
                f'Question {index + 1}: name of{candidate_resolution_i[0]} A is {candidate[0]} ,name of{candidate_resolution_i[0]} B is {candidate[1]}')
        sent = 'question above' if len(pair_txt) == 1 else f'above {len(pair_txt)} questions'
        pair_txt.append(
            f'\nUse domain knowledge of {candidate_resolution_i[0]}s to help understand the text and answer the {sent} in the format: For Question i, Yes, {candidate_resolution_i[0]} A and {candidate_resolution_i[0]} B are the same {candidate_resolution_i[0]}./No, {candidate_resolution_i[0]} A and {candidate_resolution_i[0]} B are different {candidate_resolution_i[0]}s. For Question i+1, (repeat the above procedures)')
        pair_prompt = '\n'.join(pair_txt)
        variables = {
            **self.prompt_variables,
            self._input_text_key: pair_prompt
        }
        text = perform_variable_replacements(self._resolution_prompt, variables=variables)
        logging.info(f"Created resolution prompt {len(text)} bytes for {len(candidate_resolution_i[1])} entity pairs of type {candidate_resolution_i[0]}")
        async with chat_limiter:
            timeout_seconds = 280 if os.environ.get("ENABLE_TIMEOUT_ASSERTION") else 1000000000
            try:
                response = await asyncio.wait_for(
                    self._async_chat(text, [{"role": "user", "content": "Output:"}], {}, task_id),
                    timeout=timeout_seconds,
                )

            except asyncio.TimeoutError:
                logging.warning("_resolve_candidate._async_chat timeout, skipping...")
                return
            except Exception as e:
                logging.error(f"_resolve_candidate._async_chat failed: {e}")
                return

        logging.debug(f"_resolve_candidate chat prompt: {text}\nchat response: {response}")
        result = self._process_results(len(candidate_resolution_i[1]), response,
                                       self.prompt_variables.get(self._record_delimiter_key,
                                                            DEFAULT_RECORD_DELIMITER),
                                       self.prompt_variables.get(self._entity_index_delimiter_key,
                                                            DEFAULT_ENTITY_INDEX_DELIMITER),
                                       self.prompt_variables.get(self._resolution_result_delimiter_key,
                                                            DEFAULT_RESOLUTION_RESULT_DELIMITER))
        async with resolution_result_lock:
            for result_i in result:
                resolution_result.add(candidate_resolution_i[1][result_i[0] - 1])

    def _process_results(
            self,
            records_length: int,
            results: str,
            record_delimiter: str,
            entity_index_delimiter: str,
            resolution_result_delimiter: str
    ) -> list:
        ans_list = []
        records = [r.strip() for r in results.split(record_delimiter)]
        for record in records:
            pattern_int = fr"{re.escape(entity_index_delimiter)}(\d+){re.escape(entity_index_delimiter)}"
            match_int = re.search(pattern_int, record)
            res_int = int(str(match_int.group(1) if match_int else '0'))
            if res_int > records_length:
                continue

            pattern_bool = f"{re.escape(resolution_result_delimiter)}([a-zA-Z]+){re.escape(resolution_result_delimiter)}"
            match_bool = re.search(pattern_bool, record)
            res_bool = str(match_bool.group(1) if match_bool else '')

            if res_int and res_bool:
                if res_bool.lower() == 'yes':
                    ans_list.append((res_int, "yes"))

        return ans_list

    def _has_digit_in_2gram_diff(self, a, b):
        return _has_digit_in_2gram_diff(a, b)

    def is_similarity(self, a, b):
        return is_similarity_str(a, b)

