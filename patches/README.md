# Patch 管理说明

本目录保存对官方 RAGFlow 文件的最小化修改 patch。这些 patch 是**无法通过新增文件或 monkey patch 进一步抽离**的必需修改。

## 文件列表

| Patch 文件 | 对应官方文件 | 修改原因 |
|---|---|---|
| `.gitignore.patch` | `.gitignore` | 增加 `docs/graphrag_review/` 等自定义忽略规则 |
| `api_apps_restful_apis_dataset_api.py.patch` | `api/apps/restful_apis/dataset_api.py` | 后向兼容包装器 |
| `api_apps_services_dataset_api_service.py.patch` | `api/apps/services/dataset_api_service.py` | GraphRAG 可视化/删除策略 dispatch |
| `api_db_services_document_service.py.patch` | `api/db/services/document_service.py` | 文档删除时 KG 引用清理 dispatch |
| `common_settings.py.patch` | `common/settings.py` | 安装自定义 hook（audit / ES / OS） |
| `rag_graphrag_entity_resolution.py.patch` | `rag/graphrag/entity_resolution.py` | 增量实体消解子类化分发 |
| `rag_graphrag_general_index.py.patch` | `rag/graphrag/general/index.py` | 激活 GraphRAG incremental patch |
| `rag_graphrag_utils.py.patch` | `rag/graphrag/utils.py` | 增量图读写路由 dispatch |
| `rag_svr_task_executor.py.patch` | `rag/svr/task_executor.py` | 激活 task_executor 扩展 patch |
| `web_src_locales_en.ts.patch` | `web/src/locales/en.ts` | `totalChunks` i18n key |
| `web_src_locales_zh.ts.patch` | `web/src/locales/zh.ts` | `totalChunks` i18n key |
| `web_src_pages_dataset_dataset-overview_index.tsx.patch` | `web/src/pages/dataset/dataset-overview/index.tsx` | Total chunks StatCard UI |

## 官方更新时的工作流

```bash
# 1. 同步官方 release
 git fetch origin
 git checkout main
 git reset --hard v0.27.0        # 替换为新 release tag
 git push -f myfork main

# 2. 合并到 dev
 git checkout dev
 git merge main

# 3. 解决冲突后重新生成 patch
 bash patches/regenerate_patches.sh
```

## 验证 patch 是否还能打到新版官方代码上

如果你怀疑新版官方代码会破坏这些 patch，可以在干净的 official 分支上测试：

```bash
 git checkout -b patch-test main
 bash patches/apply_patches.sh --check
 git checkout dev
 git branch -D patch-test
```

## 重新生成所有 patch

```bash
bash patches/regenerate_patches.sh
```

## 注意事项

- 这些 patch 仅覆盖**官方文件**的修改。所有新增自定义模块在 `dev` 分支直接维护，不通过 patch 管理。
- 每个官方修改点都带有 `=== CUSTOM BEGIN/END ===` 标记，方便官方更新时快速定位冲突。
- 如果某个 patch 长期无法直接应用，说明该官方文件结构已变，需要重新评估抽离方案。
