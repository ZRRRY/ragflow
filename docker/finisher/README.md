# GraphRAG 卡死收尾脚本

针对 RAGFlow 知识图谱任务卡在 `set_graph 之后` 的场景:OpenSearch 里的实体/边已经写进去了,但 task_executor 进程在写完 callback 链路之前被 kill(常见原因:Docker 重启、内存 OOM、@timeout 装饰器默认值 1e9 秒失效等),导致 MySQL `task` 表的 `progress` 永远停在某个中间值(典型 4.43%),前端无法进入"完成"状态。

本脚本**不会重跑实体提取**,只做三件事:

1. **校验**:从 OpenSearch `ragflow_<kb_id>` 索引读真实节点/边数。
2. **收尾**:把 MySQL `task` 表里 `progress ∈ (0, 1)` 的记录改成 `progress=1.0`。
3. **清理**:删 Redis 上 `graphrag:phase:<kb_id>:*` 和 `graphrag_task_<kb_id>` 键,避免阻挡后续任务。

所有写操作默认 `dry-run`,加 `--apply` 才会真正执行。脚本是**幂等**的,可以反复跑。

## 准备

确认 `ragflow-cpu` 容器已起,且 OS / MySQL / Redis 三个依赖都健康。

把脚本目录挂进容器(可选,只在容器内没有该目录时需要)。`docker/finisher/` 已经在仓库内,你可以临时把它 `docker cp` 进去,或者把它挂成 volume。最简单的做法是 `docker cp`:

```bash
docker cp docker/finisher/finish_stuck_graphrag.py ragflow-cpu:/ragflow/finish_stuck_graphrag.py
docker exec -it ragflow-cpu python3 /ragflow/finish_stuck_graphrag.py --help
```

## 用法

把 `<KB_ID>` 换成你的真实知识库 ID(从前端 URL 或 `task` 表查)。

### Step 1:只读检查(不动任何东西)

```bash
docker exec ragflow-cpu python3 /ragflow/finish_stuck_graphrag.py \
  --kb-id <KB_ID> check
```

输出会显示:

- OS 里的节点 / 边 / 社区报告数量
- MySQL `task` 表里 `progress ∈ (0, 1)` 的所有记录

### Step 2:dry-run 看会改哪些 task

```bash
docker exec ragflow-cpu python3 /ragflow/finish_stuck_graphrag.py \
  --kb-id <KB_ID> finish --dry-run
```

注意:不传 `--apply` 时,`finish` 默认就是 dry-run。这一步让你确认**会改哪些 task、不会改哪些**。

### Step 3:真正执行

```bash
docker exec ragflow-cpu python3 /ragflow/finish_stuck_graphrag.py \
  --kb-id <KB_ID> finish --apply
```

执行后会:

- 把匹配到的 task `progress` 改成 1.0,`progress_msg` 改成 `<N> nodes, <M> edges [manually finalized]`
- 删掉 Redis 上 `graphrag:phase:<KB_ID>:*` 和 `graphrag_task_<KB_ID>` 键

### Step 4(可选):只清 Redis,不碰 MySQL

```bash
docker exec ragflow-cpu python3 /ragflow/finish_stuck_graphrag.py \
  --kb-id <KB_ID> cleanup --apply
```

## 不会做的事(避免误用)

- **不会重跑实体提取**。如果 OS 里没找到节点/边,脚本会直接拒绝 finish 并退出码 3,需要你手动确认是否要强制 UPDATE。
- **不会改 OS 数据**。脚本对 OpenSearch 只做 `_count`,不写不删。
- **不会改 LLM / 嵌入配置**。只改 task 状态和 Redis 临时键。

## 出错回滚

如果执行完后发现改错了 task,直接 SQL 改回去即可:

```sql
UPDATE task
SET progress = 0.0443,
    progress_msg = '<原始消息>',
    update_time = <原始 update_time>
WHERE id = '<task_id>';
```

Redis 键被删了不要紧,TTL 7 天后也会自然消失,或者下次任务会自动重建。

## 相关

- 上游 issue:任务卡在 `set_graph added/updated ... from index in <秒数>s.` 这一行之后
- 涉及代码:
  - `rag/graphrag/general/index.py:1272-1316` `resolve_entities` (set_graph 调用点)
  - `rag/graphrag/utils.py:937-1139` `set_graph` / `_set_graph_monolithic` / `set_graph_delta`
  - `common/connection_utils.py:71-74` `@timeout` 装饰器(默认不生效的根因)
  - `rag/svr/task_executor.py:185-216` `set_progress` (callback 写 MySQL)
