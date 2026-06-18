#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
"""
finisher / finish_stuck_graphrag.py

一键收尾卡在 "set_graph 之后"的 GraphRAG 任务。

设计原则:
  1. 只读 + 幂等,任何一步出错不破坏已有数据。
  2. 默认 dry-run,加 --apply 才真正写库。
  3. 复用容器内的 service_conf.yaml 拿连接信息,不依赖外部传参。
  4. 三个外部依赖全部直连(pymysql/requests/redis),不引入 RAGFlow 自己的
     settings.BaseDataBase / ElasticSearchConnectionPool,避开:
       - elasticsearch-py 8.x 默认 Content-Type 在 OpenSearch 2.x 报 406
       - api.db.db 不存在、common.settings 需要 import 时机正确 等坑
     容器内只需要 pymysql / requests / redis / pyyaml 四个包,都在 RAGFlow venv 里。

典型场景:任务在 set_graph 写完 1526 nodes / 4266 edges / 5792 chunks 之后,
callback 链路或 Redis 锁释放卡住,task_executor 被 SIGKILL 后无法续跑。
这时 OS 里数据完好,MySQL task 表 progress 永远停在 4.43%。
本脚本:校验数据 -> 改 task.progress=1.0 -> 清 Redis phase marker。

用法(在 docker-ragflow-cpu-1 容器内,实际容器名以 `docker ps` 输出为准):
  # 1) 先看现状(不改任何东西)
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> check

  # 2) 干跑,看会改哪些 task
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> finish

  # 3) 真正执行
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> finish --apply

  # 4) 只清 Redis phase marker(独立)
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> cleanup --apply
"""
import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

# 让脚本在容器内能找到 ragflow 的包(虽然脚本主体不依赖 ragflow,留着方便 import 调试)
sys.path.insert(0, "/ragflow")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("finisher")


# ---------------------------------------------------------------------------
# 配置加载:复用容器内 service_conf.yaml,避免重复硬编码
# ---------------------------------------------------------------------------
def load_service_conf():
    """从 /ragflow/conf/service_conf.yaml 读连接配置。"""
    try:
        import yaml
    except ImportError:
        log.error("缺少 PyYAML,请在容器内装:pip install pyyaml")
        return None
    conf_path = Path("/ragflow/conf/service_conf.yaml")
    if not conf_path.exists():
        log.error("service_conf.yaml 不存在: %s", conf_path)
        return None
    with open(conf_path, "r", encoding="utf-8") as f:
        raw = f.read()
    # 模板里是 ${VAR:-default} 形式,需要手动展开
    def _sub(m):
        var = m.group(1)
        default = m.group(2)
        return os.environ.get(var, default)
    raw = re.sub(r"\$\{([A-Z_][A-Z0-9_]*):-([^}]*)\}", _sub, raw)
    return yaml.safe_load(raw)


# ---------------------------------------------------------------------------
# 连接帮助函数:解析 conf 里的 host:port 形式
# ---------------------------------------------------------------------------
def _split_host_port(value, default_port):
    """'mysql:3306' 或 'mysql' -> (host, port)。"""
    if value is None:
        return None, default_port
    s = str(value).strip()
    if "://" in s:
        u = urlparse(s)
        return u.hostname, u.port or default_port
    if ":" in s:
        h, p = s.rsplit(":", 1)
        try:
            return h, int(p)
        except ValueError:
            return s, default_port
    return s, default_port


# ---------------------------------------------------------------------------
# MySQL 直连
# ---------------------------------------------------------------------------
def mysql_connect(conf):
    import pymysql
    m = conf.get("mysql") or {}
    host, port = _split_host_port(m.get("host"), 3306)
    return pymysql.connect(
        host=host,
        port=port,
        user=m.get("user", "root"),
        password=m.get("password", ""),
        database=m.get("name", "rag_flow"),
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=10,
        read_timeout=10,
        write_timeout=10,
    )


def get_kb_tenant_id(conf, kb_id):
    """根据 KB ID 从 knowledgebase 表查询 tenant_id,用于构造 OS 索引名。"""
    conn = mysql_connect(conf)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tenant_id FROM knowledgebase WHERE id = %s",
                (kb_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"knowledgebase id={kb_id} not found")
            return row[0]
    finally:
        conn.close()


def list_stuck_tasks(conf, kb_id):
    """找出 progress ∈ (0, 1) 的 graphrag 任务。

    Task 表本身没有 kb_id 字段,关联走反查:
    knowledgebase.graphrag_task_id -> task.id
    """
    conn = mysql_connect(conf)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.id, t.doc_id, t.task_type, t.progress, t.progress_msg,
                       t.create_time, t.update_time
                FROM knowledgebase k
                JOIN task t ON t.id = k.graphrag_task_id
                WHERE k.id = %s
                  AND t.progress > 0
                  AND t.progress < 1
                ORDER BY t.create_time DESC
                """,
                (kb_id,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "doc_id": r[1],
                "task_type": r[2],
                "progress": float(r[3]) if r[3] is not None else 0.0,
                "progress_msg": (r[4] or "")[:120],
                "create_time": r[5],
                "update_time": r[6],
            }
        )
    return out


def mark_task_done(conf, task_id, progress_msg):
    """把 task.progress 改成 1.0。"""
    conn = mysql_connect(conf)
    n = 0
    try:
        with conn.cursor() as cur:
            n = cur.execute(
                """
                UPDATE task
                SET progress = 1.0,
                    progress_msg = %s,
                    update_time = %s
                WHERE id = %s
                """,
                (progress_msg, int(time.time() * 1000), task_id),
            )
        conn.commit()
    finally:
        conn.close()
    return n


# ---------------------------------------------------------------------------
# OpenSearch 直连:用 requests,避开 ES 8.x 客户端 406 Content-Type 问题
# ---------------------------------------------------------------------------
def os_count(conf, tenant_id, query, kb_id=None):
    """对指定 tenant 的 chunk 索引做一次 _count,可按 kb_id 过滤,失败返回 -1。"""
    import requests

    os_cfg = conf.get("os") or conf.get("es") or {}
    hosts = os_cfg.get("hosts", "http://opensearch01:9201")
    if isinstance(hosts, str):
        host_list = [h.strip() for h in hosts.split(",") if h.strip()]
    else:
        host_list = list(hosts)

    username = os_cfg.get("username", "admin")
    password = os_cfg.get("password", "")
    verify = bool(os_cfg.get("verify_certs", False))

    index = f"ragflow_{tenant_id}"
    must = [{"query_string": {"query": query}}]
    if kb_id:
        must.append({"term": {"kb_id": kb_id}})
    body = {"query": {"bool": {"must": must}}}

    last_err = None
    for host in host_list:
        # 容错:有时是 http://x:9200,有时是裸 x:9200
        if "://" not in host:
            host = "http://" + host
        url = f"{host.rstrip('/')}/{index}/_count"
        try:
            resp = requests.post(
                url,
                auth=(username, password) if password else None,
                json=body,
                timeout=10,
                verify=verify,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                return resp.json().get("count", 0)
            if resp.status_code == 404:
                # 索引不存在 = 0
                return 0
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            last_err = repr(e)
            continue
    log.error("OS count 失败: index=%s query=%s kb_id=%s err=%s", index, query, kb_id, last_err)
    return -1


def _os_hosts_first(conf):
    """从 conf 里挑第一个 OS host,带协议头。失败返回 None。"""
    os_cfg = conf.get("os") or conf.get("es") or {}
    hosts = os_cfg.get("hosts", "http://opensearch01:9201")
    if isinstance(hosts, str):
        first = hosts.split(",")[0].strip()
    else:
        first = list(hosts)[0]
    if "://" not in first:
        first = "http://" + first
    return first.rstrip("/"), os_cfg


def os_list_ragflow_indices(conf):
    """列 OS 里所有 ragflow_* 索引,以及每个的 node/edge/total 计数。

    返回 list of (index_name, n_node, n_edge, n_total)。失败返回 []。
    """
    import requests

    base, os_cfg = _os_hosts_first(conf)
    if not base:
        return []
    username = os_cfg.get("username", "admin")
    password = os_cfg.get("password", "")
    verify = bool(os_cfg.get("verify_certs", False))
    auth = (username, password) if password else None
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    try:
        # 用 _cat/indices 列出所有 ragflow_* 索引名
        r = requests.get(
            f"{base}/_cat/indices/ragflow_*?h=index&format=json",
            auth=auth, headers=headers, timeout=10, verify=verify,
        )
        if r.status_code != 200:
            log.warning("_cat/indices 失败: HTTP %d %s", r.status_code, r.text[:200])
            return []
        names = [row.get("index") for row in r.json() if row.get("index")]
    except Exception as e:
        log.warning("_cat/indices 异常: %s", e)
        return []

    # 对每个索引,取 entity/relation/total(不限制 kb_id,展示 tenant 级全局视图)
    out = []
    for idx in sorted(names):
        tenant_id = idx.replace("ragflow_", "", 1)
        n_node = os_count(conf, tenant_id, "knowledge_graph_kwd:entity")
        n_edge = os_count(conf, tenant_id, "knowledge_graph_kwd:relation")
        n_total = os_count(conf, tenant_id, "*")
        out.append((idx, n_node, n_edge, n_total))
    return out


# ---------------------------------------------------------------------------
# Redis phase marker 清理
# ---------------------------------------------------------------------------
def cleanup_redis(conf, kb_id, dry_run=True):
    """清掉 graphrag:phase:<kb_id>:* 和 graphrag_task_<kb_id>。"""
    import redis

    r_cfg = conf.get("redis", {})
    host, port = _split_host_port(r_cfg.get("host"), 6379)
    db = int(r_cfg.get("db", 1))
    password = r_cfg.get("password") or None
    username = r_cfg.get("username") or None

    r = redis.Redis(
        host=host, port=port, db=db,
        password=password, username=username,
        decode_responses=True,
        socket_timeout=5,
    )

    patterns = [f"graphrag:phase:{kb_id}:*", f"graphrag_task_{kb_id}"]
    total_deleted = []
    for pat in patterns:
        if "*" in pat:
            keys = list(r.scan_iter(match=pat, count=200))
        else:
            keys = [pat] if r.exists(pat) else []
        log.info("匹配 pattern=%s -> %d 个 key", pat, len(keys))
        for k in keys:
            total_deleted.append(k)
            if not dry_run:
                r.delete(k)

    return total_deleted


# ---------------------------------------------------------------------------
# 子命令
# ---------------------------------------------------------------------------
def cmd_check(conf, kb_id, os_kb_id):
    """只读:看 OS 里的数据,看 MySQL 里卡住的 task。

    当 os_kb_id != tenant_id 时,同时列两边的 OS 索引计数,帮用户诊断
    'task 关联的 KB' 与 'OS 真实写数据的 tenant 索引' 不一致的情况。
    """
    try:
        tenant_id = get_kb_tenant_id(conf, kb_id)
    except Exception as e:
        log.error("查询 knowledgebase.tenant_id 失败: %s", e)
        return 1
    os_tenant_id = os_kb_id or tenant_id

    log.info("=" * 70)
    log.info("[CHECK] MySQL KB = %s  |  OS tenant = %s", kb_id, os_tenant_id)
    log.info("=" * 70)

    # 1) OS 里的实体数(按 kb_id 过滤,同一 tenant 下可能有多个 KB)
    # 注意:实际 RAGFlow 的取值是 "entity" 和 "relation",不是 "node"/"edge"
    n_nodes = os_count(conf, os_tenant_id, "knowledge_graph_kwd:entity", kb_id=kb_id)
    n_edges = os_count(conf, os_tenant_id, "knowledge_graph_kwd:relation", kb_id=kb_id)
    n_reports = os_count(conf, os_tenant_id, "knowledge_graph_kwd:community_report", kb_id=kb_id)
    n_total = os_count(conf, os_tenant_id, "*", kb_id=kb_id)

    log.info("OS  ragflow_%s 索引:", os_tenant_id)
    log.info("  - 实体节点 (knowledge_graph_kwd:entity)  : %d", n_nodes)
    log.info("  - 关系边   (knowledge_graph_kwd:relation): %d", n_edges)
    log.info("  - 社区报告(knowledge_graph_kwd:community_report): %d", n_reports)
    log.info("  - 文档块总索引(含 entity/relation)       : %d", n_total)

    # 2) MySQL 卡住的 task(用 kb_id,不是 os_tenant_id)
    log.info("-" * 70)
    log.info("MySQL  task 表中 progress ∈ (0, 1) 的任务:")
    try:
        tasks = list_stuck_tasks(conf, kb_id)
    except Exception as e:
        log.error("读 MySQL 失败: %s", e)
        return 1
    if not tasks:
        log.info("  (无)")
    for t in tasks:
        log.info(
            "  - id=%s  doc=%s  type=%s  progress=%.4f  msg=%s",
            t["id"], t["doc_id"], t["task_type"], t["progress"], t["progress_msg"],
        )

    # 3) 总是列所有 ragflow_* 索引,帮诊断 "OS 里到底有什么"
    log.info("-" * 70)
    log.info("OS 全部 ragflow_* 索引(全局视图):")
    rows = os_list_ragflow_indices(conf)
    if not rows:
        log.info("  (没找到任何 ragflow_* 索引,或 _cat/indices 调用失败)")
    else:
        for idx, n_node, n_edge, n_total_idx in rows:
            marker = ""
            if n_node > 0 or n_edge > 0:
                marker = "  ← 这里有数据"
            tag = "  [当前 KB]" if idx == f"ragflow_{os_tenant_id}" else ""
            log.info(
                "  - %-50s  node=%-6d edge=%-6d docs=%-7d%s%s",
                idx, n_node, n_edge, n_total_idx, tag, marker,
            )
    log.info("提示:如果数据不在 `ragflow_<--os-tenant-id>` 里,改用 --os-kb-id <真实 tenant_id> 跑 finish。")

    log.info("=" * 70)
    log.info("判断:")
    if n_nodes > 0 and n_edges > 0:
        log.info("  ✓ OS 里有实体和边,主任务实质完成。可以跑 `finish --apply` 。")
    else:
        log.warning("  ✗ OS 里没找到节点/边,主任务其实没做完,finish 会骗前端 —— 不要跑。")
    if not tasks:
        log.info("  - MySQL 没有卡住的 task,无需 finish。")
    log.info("=" * 70)
    return 0


def cmd_finish(conf, kb_id, os_kb_id, dry_run):
    """把卡住的 task 标 done,默认 dry-run。"""
    try:
        tenant_id = get_kb_tenant_id(conf, kb_id)
    except Exception as e:
        log.error("查询 knowledgebase.tenant_id 失败: %s", e)
        return 1
    os_tenant_id = os_kb_id or tenant_id

    log.info("=" * 70)
    log.info("[FINISH] MySQL KB = %s  |  OS tenant = %s  |  dry_run = %s",
             kb_id, os_tenant_id, dry_run)
    log.info("=" * 70)

    # 先读 OS 真实数字(用 os_tenant_id,并按 kb_id 过滤)
    n_nodes = os_count(conf, os_tenant_id, "knowledge_graph_kwd:entity", kb_id=kb_id)
    n_edges = os_count(conf, os_tenant_id, "knowledge_graph_kwd:relation", kb_id=kb_id)
    if n_nodes < 0 or n_edges < 0:
        log.error("OS 查询失败,拒绝继续,避免误标 task done。")
        return 2
    if n_nodes == 0 and n_edges == 0:
        log.error("OS 里没有节点/边,主任务没做完,拒绝 finish!如确认要强制标 done,手动 UPDATE MySQL。")
        return 3

    progress_msg = f"Knowledge Graph done ({n_nodes} nodes, {n_edges} edges) [manually finalized]"
    log.info("将把 task.progress 设为 1.0,progress_msg = %r", progress_msg)

    try:
        tasks = list_stuck_tasks(conf, kb_id)
    except Exception as e:
        log.error("读 MySQL 失败: %s", e)
        return 1
    if not tasks:
        log.info("没有 progress ∈ (0,1) 的 task,无需 finish。")
    else:
        for t in tasks:
            log.info(
                "  -> task id=%s doc=%s progress=%.4f -> 1.0",
                t["id"], t["doc_id"], t["progress"],
            )
            if dry_run:
                log.info("     [dry-run] 跳过 UPDATE")
            else:
                try:
                    n = mark_task_done(conf, t["id"], progress_msg)
                    log.info("     UPDATE 影响行数: %d", n)
                except Exception as e:
                    log.error("     UPDATE 失败: %s", e)
                    return 1

    # 顺便清 phase marker(用 kb_id,跟 MySQL 保持一致)
    log.info("-" * 70)
    log.info("顺手清 Redis phase marker:")
    try:
        keys = cleanup_redis(conf, kb_id, dry_run=dry_run)
    except Exception as e:
        log.error("清理 Redis 失败: %s", e)
        keys = []
    log.info("  共 %d 个 key, dry_run=%s", len(keys), dry_run)
    for k in keys:
        log.info("    - %s", k)

    log.info("=" * 70)
    return 0


def cmd_cleanup(conf, kb_id, dry_run):
    log.info("[CLEANUP] KB = %s  dry_run = %s", kb_id, dry_run)
    try:
        keys = cleanup_redis(conf, kb_id, dry_run=dry_run)
    except Exception as e:
        log.error("清理 Redis 失败: %s", e)
        return 1
    log.info("共 %d 个 key", len(keys))
    for k in keys:
        log.info("  - %s", k)
    return 0


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(
        description="收尾卡在 set_graph 之后的 GraphRAG 任务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--kb-id", required=True, help="知识库 ID(32 位 hex)")
    p.add_argument(
        "action",
        choices=["check", "finish", "cleanup"],
        help="check=只读检查; finish=标 task done(默认 dry-run); cleanup=只清 Redis",
    )
    p.add_argument(
        "--apply", action="store_true",
        help="真正写库/清 Redis;不加此参数,所有写操作都是 dry-run",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="显式声明 dry-run(默认就是 dry-run,加这个只是显式;跟 --apply 互斥)",
    )
    p.add_argument(
        "--os-kb-id",
        help="OpenSearch 索引对应的 tenant_id(默认根据 --kb-id 查询 knowledgebase 表得到)。"
             "当 MySQL 里 task 关联的 KB 跟 OS 真实写数据的 tenant 索引不一致时,"
             "用这个参数指向 OS 那边真实有数据的 tenant_id,典型场景:KB 重建/迁移过。",
    )
    args = p.parse_args()

    if args.apply and args.dry_run:
        p.error("--apply 和 --dry-run 互斥,只能选一个")
    dry_run = not args.apply

    os_kb_id = args.os_kb_id or None
    if os_kb_id and os_kb_id != args.kb_id:
        log.warning("=" * 70)
        log.warning("  --os-kb-id 与 --kb-id 不同:")
        log.warning("    MySQL task 过滤用 --kb-id   = %s", args.kb_id)
        log.warning("    OpenSearch tenant 索引用 --os-kb-id = %s", os_kb_id)
        log.warning("=" * 70)

    conf = load_service_conf()
    if not conf:
        log.error("加载 service_conf 失败,退出。")
        return 1

    if not dry_run:
        log.warning("=" * 70)
        log.warning("  --apply 已设置,操作将真正写入。")
        log.warning("=" * 70)

    if args.action == "check":
        return cmd_check(conf, args.kb_id, os_kb_id)
    if args.action == "finish":
        return cmd_finish(conf, args.kb_id, os_kb_id, dry_run=dry_run)
    if args.action == "cleanup":
        return cmd_cleanup(conf, args.kb_id, dry_run=dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
