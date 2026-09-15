# Patch 管理说明

本目录保存对官方 RAGFlow 文件的最小化修改 patch。这些 patch 是**无法通过新增文件、monkey patch 或编译模板进一步抽离**的必需修改。

## 文件列表

| Patch 文件 | 对应官方文件 | 修改原因 |
|---|---|---|
| `.gitignore.patch` | `.gitignore` | 增加 `docker-compose.override.yml`、`docs/graphrag_review/` 等自定义忽略规则 |
| `api_apps_restful_apis_dataset_api.py.patch` | `api/apps/restful_apis/dataset_api.py` | 新增 `delete_knowledge_graph`，修复 `backward_compat.py` 坏引用（v0.27.2 上游仍未定义该函数） |
| `common_settings.py.patch` | `common/settings.py` | `init_settings()` 安装删除审计 hook 与 SiliconFlow 超时 patch |
| `web_src_locales_en.ts.patch` | `web/src/locales/en.ts` | `totalChunks` i18n key |
| `web_src_locales_zh.ts.patch` | `web/src/locales/zh.ts` | `totalChunks` i18n key |
| `web_src_pages_dataset_dataset-overview_index.tsx.patch` | `web/src/pages/dataset/dataset-overview/index.tsx` | Total chunks StatCard UI |

> v0.27.2 升级删除了 7 个 patch（GraphRAG 增量定制相关 + `rag_utils_es_conn.py.patch`，上游已参数化 bulk `refresh`），详见 `MODIFICATIONS.md` 第五节。

## 官方更新时的工作流

```bash
# 1. 同步官方最新代码（main 跟踪 origin/main，不再 reset --hard + force-push 到 fork）
 git checkout main && git pull --ff-only

# 2. 合并到定制分支
 git checkout dev-0.27
 git merge main        # 或 git merge vX.Y.Z（release tag）

# 3. 解决冲突后重新生成 patch
 bash patches/regenerate_patches.sh
```

## 验证 patch 是否还能打到新版官方代码上

如果你怀疑新版官方代码会破坏这些 patch，可以在干净的 official 分支上测试：

```bash
 git worktree add --detach /tmp/ragflow-patch-check main
 cd /tmp/ragflow-patch-check && bash patches/apply_patches.sh --check
 cd - && git worktree remove --force /tmp/ragflow-patch-check
```

## 重新生成所有 patch

```bash
bash patches/regenerate_patches.sh
```

## 注意事项

- 这些 patch 仅覆盖**官方文件**的修改。所有新增自定义模块在 `dev-0.27` 分支直接维护，不通过 patch 管理。
- 每个官方修改点都带有 `=== CUSTOM BEGIN/END ===` 标记，方便官方更新时快速定位冲突。
- 如果某个 patch 长期无法直接应用，说明该官方文件结构已变，需要重新评估抽离方案。
- `apply_patches.sh` 的 glob 不含 `.gitignore.patch`（点前文件不匹配 `*.patch` glob），如需应用它请显式 `git apply patches/.gitignore.patch`。
