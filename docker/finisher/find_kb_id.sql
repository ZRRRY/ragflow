-- ============================================================
-- 查 KB ID 的几条 SQL,在 ragflow MySQL 里跑
-- ============================================================
-- 连接方式任选一种:
--   A) docker exec -it ragflow-mysql mysql -uroot -pinfini_rag_flow rag_flow
--   B) docker exec ragflow-cpu mysql -h mysql -uroot -pinfini_rag_flow rag_flow
--   C) 用 MySQL Workbench / DBeaver 等 GUI 连 host:3306 用户 root 密码 infini_rag_flow 数据库 rag_flow
--   D) host 上: mysql -h 127.0.0.1 -P 3306 -uroot -pinfini_rag_flow rag_flow
--      (前提:docker-compose-base.yml 里 mysql 端口映射了 3306)


-- ---- 1) 列出所有 KB,看 ID 和名字 ----------------------------------------
SELECT id, name, create_date, update_date
FROM knowledgebase
ORDER BY create_date DESC;


-- ---- 2) 已知 dataset ID(前端 URL 里那个),反查 KB ID --------------------
-- 把你前端 URL 里的 dataset_id 替换下面那个字符串
SELECT k.id AS kb_id, k.name AS kb_name, d.id AS dataset_id, d.name AS dataset_name
FROM knowledgebase k
JOIN dataset d ON d.kb_id = k.id
WHERE d.id = '97425e9e5d7411f1a78133e9dd471f84';


-- ---- 3) 已知 KB 名字片段,模糊查 -----------------------------------------
-- 把 'xxx' 换成你 KB 名字里的关键词
SELECT id, name FROM knowledgebase
WHERE name LIKE '%xxx%';


-- ---- 4) 直接列出所有 progress 卡在 (0, 1) 的 graphrag 任务 ------------
-- 这一条最直接:看一眼就知道哪个 KB 的哪个 task 卡住了
SELECT
    t.id            AS task_id,
    t.kb_id         AS kb_id,
    k.name          AS kb_name,
    t.doc_id        AS doc_id,
    t.task_type,
    t.progress,
    LEFT(t.progress_msg, 200) AS progress_msg,
    FROM_UNIXTIME(t.create_time/1000) AS created,
    FROM_UNIXTIME(t.update_time/1000) AS updated
FROM task t
LEFT JOIN knowledgebase k ON k.id = t.kb_id
WHERE t.progress > 0 AND t.progress < 1
ORDER BY t.update_time DESC
LIMIT 20;
