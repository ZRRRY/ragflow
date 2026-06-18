-- ============================================================
-- 查 KB ID 的几条 SQL,在 ragflow MySQL 里跑
-- ============================================================
-- 连接方式任选一种(下面命令里的容器名以 `docker compose -f docker-compose.yml up -d`
--  在 `docker/` 目录下启动时的默认命名为准,即都带 `docker-` 前缀;如果你用了
-- `docker compose -p <项目名>` 启动,把所有 `docker-` 前缀替换成自定义项目名):
--   A) docker exec -it docker-mysql-1 mysql -uroot -pinfini_rag_flow rag_flow
--   B) docker exec docker-ragflow-cpu-1 mysql -h mysql -uroot -pinfini_rag_flow rag_flow
--   C) 用 MySQL Workbench / DBeaver 等 GUI 连 host:3306 用户 root 密码 infini_rag_flow 数据库 rag_flow
--   D) host 上: mysql -h 127.0.0.1 -P 3306 -uroot -pinfini_rag_flow rag_flow
--      (前提:docker-compose-base.yml 里 mysql 端口映射了 3306)


-- ---- 1) 列出所有 KB,看 ID 和名字 ----------------------------------------
SELECT id, name, create_date, update_date
FROM knowledgebase
ORDER BY create_date DESC;


-- ---- 2) 直接验证:前端 URL 里的 ID 是不是 KB ID 本身 --------------------
-- RAGFlow 前端的 "dataset" 对应 DB 里的 knowledgebase 表,URL 里的 dataset_id
-- 通常就是 knowledgebase.id。下面这条直接查:
--   - 有结果 -> URL 那个 ID 就是 KB ID,直接拿去当 --kb-id 用
--   - 没结果 -> 见第 3 条
SELECT id, name, create_date
FROM knowledgebase
WHERE id = '97425e9e5d7411f1a78133e9dd471f84';


-- ---- 3) 已知 KB 名字片段,模糊查 -----------------------------------------
-- 把 'xxx' 换成你 KB 名字里的关键词
SELECT id, name FROM knowledgebase
WHERE name LIKE '%xxx%';


-- ---- 4) 直接列出所有 progress 卡在 (0, 1) 的 graphrag 任务 ------------
-- 关联方式:task 没有 kb_id 字段,反查走 knowledgebase.graphrag_task_id
-- 这一条最直接:看一眼就知道哪个 KB 的哪个 task 卡住了
SELECT
    t.id            AS task_id,
    k.id            AS kb_id,
    k.name          AS kb_name,
    t.doc_id        AS doc_id,
    t.task_type,
    t.progress,
    LEFT(t.progress_msg, 200) AS progress_msg,
    FROM_UNIXTIME(t.create_time/1000) AS created,
    FROM_UNIXTIME(t.update_time/1000) AS updated
FROM knowledgebase k
JOIN task t ON t.id = k.graphrag_task_id
WHERE t.progress > 0 AND t.progress < 1
ORDER BY t.update_time DESC
LIMIT 20;
