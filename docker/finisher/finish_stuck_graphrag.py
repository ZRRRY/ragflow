#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
finisher / finish_stuck_graphrag.py

一键收尾卡在 "set_graph 之后"的 GraphRAG 任务。

设计原则:
  1. 只读 + 幂等,任何一步出错不破坏已有数据。
  2. 默认 dry-run,加 --apply 才真正写库。
  3. 复用容器内的 service_conf.yaml 拿连接信息,不依赖外部传参。

典型场景:任务在 set_graph 写完 1526 nodes / 4266 edges / 5792 chunks 之后,
callback 链路或 Redis 锁释放卡住,task_executor 被 SIGKILL 后无法续跑。
这时 OS 里数据完好,MySQL task 表 progress 永远停在 4.43%。
本脚本:校验数据 -> 改 task.progress=1.0 -> 清 Redis phase marker。

用法(在 ragflow-cpu 容器内):
  # 1) 先看现状(不改任何东西)
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> check

  # 2) 干跑,看会改哪些 task
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> finish --dry-run

  # 3) 真正执行
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> finish --apply

  # 4) 只清 Redis phase marker(独立)
  python3 finish_stuck_graphrag.py --kb-id <KB_ID> cleanup --apply
"""
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

# 让脚本在容器内能找到 ragflow 的包
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
    import yaml
    conf_path = Path("/ragflow/conf/service_conf.yaml")
    if not conf_path.exists():
        log.warning("service_conf.yaml 不存在,fallback 到环境变量")
        return {}
    with open(conf_path, "r", encoding="utf-8") as f:
        # 模板里是 ${VAR:-default} 形式,需要手动展开
        raw = f.read()
    # 简单做一次 ${VAR:-default} 展开
    import re
    def _sub(m):
        var = m.group(1)
        default = m.group(2)
        return os.environ.get(var, default)
    raw = re.sub(r"\$\{([A-Z_][A-Z0-9_]*):-([^}]*)\}", _sub, raw)
    return yaml.safe_load(raw)


# ---------------------------------------------------------------------------
# OpenSearch 校验:确认实体/边/chunk 都在
# ---------------------------------------------------------------------------
def os_count(conf, kb_id, query, auth=None):
    """对 KB 的 chunk 索引做一次 _count。"""
    from elasticsearch import Elasticsearch

    os_cfg = conf.get("os") or conf.get("es") or {}
    hosts = os_cfg.get("hosts", "http://opensearch01:9201")
    username = os_cfg.get("username", "admin")
    password = os_cfg.get("password", "")

    es = Elasticsearch(
        hosts=[hosts],
        basic_auth=(username, password) if password else None,
        verify_certs=False,
        request_timeout=10,
    )

    # KB 的 chunk 索引命名规则:ragflow_<kb_id> 去掉横杠
    index = f"ragflow_{kb_id.replace('-', '')}"
    body = {"query": {"bool": {"must": [{"query_string": {"query": query}}]}}}
    try:
        resp = es.count(index=index, body=body)
        return resp.get("count", 0)
    except Exception as e:
        log.error("OS count 失败: index=%s query=%s err=%s", index, query, e)
        return -1


# ---------------------------------------------------------------------------
# MySQL task 表:列出 / 标记 完成
# ---------------------------------------------------------------------------
def list_stuck_tasks(conf, kb_id):
    """找出 progress ∈ (0, 1) 的 graphrag 任务。"""
    from api.db.db_models import Task
    from api.db.db import DB

    DB.init(conf.get("mysql", {}))
    rows = (
        Task.select()
        .where(
            (Task.kb_id == kb_id)
            & (Task.progress > 0)
            & (Task.progress < 1)
        )
        .order_by(Task.create_time.desc())
    )
    out = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "doc_id": r.doc_id,
                "progress": r.progress,
                "progress_msg": (r.progress_msg or "")[:120],
                "create_time": r.create_time,
                "update_time": r.update_time,
                "task_type": r.task_type,
            }
        )
    DB.close()
    return out


def mark_task_done(conf, task_id, progress_msg):
    """把 task.progress 改成 1.0。"""
    from api.db.db_models import Task
    from api.db.db import DB

    DB.init(conf.get("mysql", {}))
    try:
        n = (
            Task.update(
                progress=1.0,
                progress_msg=progress_msg,
                update_time=int(time.time() * 1000),
            )
            .where(Task.id == task_id)
            .execute()
        )
    finally:
        DB.close()
    return n


# ---------------------------------------------------------------------------
# Redis phase marker 清理
# ---------------------------------------------------------------------------
def cleanup_redis(conf, kb_id, dry_run=True):
    """清掉 graphrag:phase:<kb_id>:* 和 graphrag_task_<kb_id>。"""
    import redis

    r_cfg = conf.get("redis", {})
    db = int(r_cfg.get("db", 1))
    password = r_cfg.get("password") or None
    host = r_cfg.get("host", "redis:6379").split(":")[0]
    port = int(r_cfg.get("host", "redis:6379").split(":")[1])
    username = r_cfg.get("username") or None

    r = redis.Redis(
        host=host, port=port, db=db, password=password, username=username,
        decode_responses=True,
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
def cmd_check(conf, kb_id):
    """只读:看 OS 里的数据,看 MySQL 里卡住的 task。"""
    log.info("=" * 70)
    log.info("[CHECK] KB = %s", kb_id)
    log.info("=" * 70)

    # 1) OS 里的实体数
    n_nodes = os_count(conf, kb_id, "knowledge_graph_kwd:node")
    n_edges = os_count(conf, kb_id, "knowledge_graph_kwd:edge")
    n_reports = os_count(conf, kb_id, "knowledge_graph_kwd:community_report")
    n_total = os_count(conf, kb_id, "*")

    log.info("OS  ragflow_%s 索引:", kb_id.replace("-", ""))
    log.info("  - 节点 (knowledge_graph_kwd:node)        : %d", n_nodes)
    log.info("  - 边   (knowledge_graph_kwd:edge)        : %d", n_edges)
    log.info("  - 社区报告(knowledge_graph_kwd:community) : %d", n_reports)
    log.info("  - 文档块总索引(含 node/edge)             : %d", n_total)

    # 2) MySQL 卡住的 task
    log.info("-" * 70)
    log.info("MySQL  task 表中 progress ∈ (0, 1) 的任务:")
    tasks = list_stuck_tasks(conf, kb_id)
    if not tasks:
        log.info("  (无)")
    for t in tasks:
        log.info(
            "  - id=%s  doc=%s  type=%s  progress=%.4f  msg=%s",
            t["id"], t["doc_id"], t["task_type"], t["progress"], t["progress_msg"],
        )

    log.info("=" * 70)
    log.info("判断:")
    if n_nodes > 0 and n_edges > 0:
        log.info("  ✓ OS 里有实体和边,主任务实质完成。可以跑 `finish --apply`。")
    else:
        log.warning("  ✗ OS 里没找到节点/边,主任务其实没做完,finish 会骗前端 —— 不要跑。")
    if not tasks:
        log.info("  - MySQL 没有卡住的 task,无需 finish。")
    log.info("=" * 70)


def cmd_finish(conf, kb_id, dry_run):
    """把卡住的 task 标 done,默认 dry-run。"""
    log.info("=" * 70)
    log.info("[FINISH] KB = %s  dry_run = %s", kb_id, dry_run)
    log.info("=" * 70)

    # 先读 OS 真实数字
    n_nodes = os_count(conf, kb_id, "knowledge_graph_kwd:node")
    n_edges = os_count(conf, kb_id, "knowledge_graph_kwd:edge")
    if n_nodes < 0 or n_edges < 0:
        log.error("OS 查询失败,拒绝继续,避免误标 task done。")
        return 2
    if n_nodes == 0 and n_edges == 0:
        log.error("OS 里没有节点/边,主任务没做完,拒绝 finish!如确认要强制标 done,手动 UPDATE MySQL。")
        return 3

    progress_msg = f"Knowledge Graph done ({n_nodes} nodes, {n_edges} edges) [manually finalized]"
    log.info("将把 task.progress 设为 1.0,progress_msg = %r", progress_msg)

    tasks = list_stuck_tasks(conf, kb_id)
    if not tasks:
        log.info("没有 progress ∈ (0,1) 的 task,无需 finish。")
        return 0

    for t in tasks:
        log.info(
            "  -> task id=%s doc=%s progress=%.4f -> 1.0",
            t["id"], t["doc_id"], t["progress"],
        )
        if dry_run:
            log.info("     [dry-run] 跳过 UPDATE")
        else:
            n = mark_task_done(conf, t["id"], progress_msg)
            log.info("     UPDATE 影响行数: %d", n)

    # 顺便清 phase marker
    log.info("-" * 70)
    log.info("顺手清 Redis phase marker:")
    keys = cleanup_redis(conf, kb_id, dry_run=dry_run)
    log.info("  共 %d 个 key, dry_run=%s", len(keys), dry_run)
    for k in keys:
        log.info("    - %s", k)

    log.info("=" * 70)
    return 0


def cmd_cleanup(conf, kb_id, dry_run):
    log.info("[CLEANUP] KB = %s  dry_run = %s", kb_id, dry_run)
    keys = cleanup_redis(conf, kb_id, dry_run=dry_run)
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
        "--yes", action="store_true",
        help="非交互确认(本脚本不交互,留着以备扩展)",
    )
    args = p.parse_args()

    conf = load_service_conf()
    if not conf:
        log.error("加载 service_conf 失败,退出。")
        return 1

    dry_run = not args.apply
    if not dry_run:
        log.warning("=" * 70)
        log.warning("  --apply 已设置,操作将真正写入。")
        log.warning("=" * 70)

    if args.action == "check":
        return cmd_check(conf, args.kb_id)
    if args.action == "finish":
        return cmd_finish(conf, args.kb_id, dry_run=dry_run)
    if args.action == "cleanup":
        return cmd_cleanup(conf, args.kb_id, dry_run=dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
