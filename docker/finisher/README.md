# GraphRAG 卡死收尾脚本

针对 RAGFlow 知识图谱任务卡在 `set_graph 之后` 的场景:OpenSearch 里的实体/边已经写进去了,但 task_executor 进程在写完 callback 链路之前被 kill(常见原因:Docker 重启、内存 OOM、@timeout 装饰器默认值 1e9 秒失效等),导致 MySQL `task` 表的 `progress` 永远停在某个中间值(典型 4.43%),前端无法进入"完成"状态。

本脚本**不会重跑实体提取**,只做三件事:

1. **校验**:从 OpenSearch `ragflow_<tenant_id>` 索引读真实节点/边数,并通过 `kb_id` 过滤只统计当前 KB。
2. **收尾**:把 MySQL `task` 表里 `progress ∈ (0, 1)` 的记录改成 `progress=1.0`。
3. **清理**:删 Redis 上 `graphrag:phase:<kb_id>:*` 和 `graphrag_task_<kb_id>` 键,避免阻挡后续任务。

所有写操作默认 `dry-run`,加 `--apply` 才会真正执行。脚本是**幂等**的,可以反复跑。

> ## ⚠️ 运行前必读:先停 task_executor,再跑本脚本
>
> 本脚本的 `finish` / `cleanup` 操作会直接修改 MySQL `task` 表和 Redis `graphrag:phase:<kb_id>:*` 键,
> 如果与正在运行的 `task_executor` 进程并发,**会与 task_executor 抢资源**,后果包括:
>
> - `task_executor` 正在写 `task` 行时,本脚本的 UPDATE 可能拿到旧行锁后再覆盖,丢失中间进度
> - `task_executor` 正在跑某 phase 时被 cleanup 删除 phase marker,可能误判"该 phase 没跑过"导致子任务重跑
> - `task_executor` 持有 `graphrag_task_<kb_id>` Redis 锁时被 cleanup 强制删除,主流程的锁语义被破坏
>
> **强制操作顺序**:
>
> ```bash
> # 1) 先停整个 ragflow 服务(同时停 API + task_executor)
> cd docker && docker compose stop ragflow
>
> # 2) 再跑本脚本(dry-run 优先)
> docker exec docker-ragflow-cpu-1 python3 /ragflow/finish_stuck_graphrag.py --kb-id <KB_ID> finish
>
> # 3) 确认 dry-run 输出符合预期后再 --apply
> docker exec docker-ragflow-cpu-1 python3 /ragflow/finish_stuck_graphrag.py --kb-id <KB_ID> finish --apply
>
> # 4) 重启服务
> docker compose start ragflow
> ```
>
> 只停 `task_executor` 不够,API 也可能触发 task 创建,必须整个 `ragflow` 服务一起停。

## 准备

确认 `docker-ragflow-cpu-1` 容器已起,且 OS / MySQL / Redis 三个依赖都健康。
> 本机 `docker compose up -d` 默认会用目录名作前缀,在 `docker/` 目录下起服务,所以容器名前缀都是 `docker-`(完整名:`docker-ragflow-cpu-1`、`docker-mysql-1`、`docker-opensearch01-1`、`docker-redis-1`、`docker-minio-1`)。如果你是用 `docker compose -p <别的名字> up -d` 起的,把所有 `docker-` 前缀替换成你自定义的项目名。
>
> **容器名不可硬编码 (P2-19)**:本 README 中所有 `docker-XXX-1` 容器名仅在
> `docker compose -p docker` 默认配置下有效。若用了 `-p <project>` 或容器重启后
> hash 变了,**先跑下面这条确认真实容器名再操作**,避免把脚本拷进不存在的容器
> 或对错的服务发命令:
>
> ```bash
> docker ps --format '{{.Names}}' | grep -E 'ragflow|mysql|opensearch|redis|minio'
> ```
>
> 把后续命令里的 `docker-ragflow-cpu-1` / `docker-mysql-1` 等替换成你环境中的真实名字。

把脚本拷进容器(在 `docker/` 目录下执行):

```bash
# 先确认容器名(见上),再拷脚本并验证帮助输出
docker cp ./finisher/finish_stuck_graphrag.py docker-ragflow-cpu-1:/ragflow/finish_stuck_graphrag.py
docker exec -it docker-ragflow-cpu-1 python3 /ragflow/finish_stuck_graphrag.py --help
```

> 如果你的当前目录不是 `docker/`,把 `./finisher/...` 换成对应相对路径,或用绝对路径如 `C:\Users\zry1127\ragflow\docker\finisher\finish_stuck_graphrag.py`。

## 用法

把 `<KB_ID>` 换成你的真实知识库 ID(从前端 URL 或 `task` 表查)。

### Step 1:只读检查(不动任何东西)

```bash
docker exec docker-ragflow-cpu-1 python3 /ragflow/finish_stuck_graphrag.py --kb-id 97425e9e5d7411f1a78133e9dd471f84 check
```

输出会显示:

- OS 里的节点 / 边 / 社区报告数量(已按当前 KB 的 `kb_id` 过滤)
- MySQL `task` 表里 `progress ∈ (0, 1)` 的所有记录

### Step 2:dry-run 看会改哪些 task

```bash
docker exec docker-ragflow-cpu-1 python3 /ragflow/finish_stuck_graphrag.py --kb-id 97425e9e5d7411f1a78133e9dd471f84 finish --dry-run
```

注意:不传 `--apply` 时,`finish` 默认就是 dry-run。这一步让你确认**会改哪些 task、不会改哪些**。

### Step 3:真正执行

```bash
docker exec docker-ragflow-cpu-1 python3 /ragflow/finish_stuck_graphrag.py --kb-id 97425e9e5d7411f1a78133e9dd471f84 finish --apply
```

执行后会:

- 把匹配到的 task `progress` 改成 1.0,`progress_msg` 改成 `<N> nodes, <M> edges [manually finalized]`
- 删掉 Redis 上 `graphrag:phase:<KB_ID>:*` 和 `graphrag_task_<KB_ID>` 键

### Step 4(可选):只清 Redis,不碰 MySQL

```bash
docker exec docker-ragflow-cpu-1 python3 /ragflow/finish_stuck_graphrag.py \
  --kb-id <KB_ID> cleanup --apply
```

## 不会做的事(避免误用)

- **不会重跑实体提取**。如果 OS 里没找到节点/边,脚本会直接拒绝 finish 并退出码 3,需要你手动确认是否要强制 UPDATE。
- **不会改 OS 数据**。脚本对 OpenSearch 只做 `_count`,不写不删。
- **不会改 LLM / 嵌入配置**。只改 task 状态和 Redis 临时键。

## 出错回滚

**Step 0(必须):执行前先备份原始记录**

```sql
SELECT id, progress, progress_msg, update_time
FROM task
WHERE id = '<task_id>';
```

把上面查到的 `progress`、`progress_msg`、`update_time` 记下来,后续回滚用。

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
