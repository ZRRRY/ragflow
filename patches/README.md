# Patch 管理说明

本目录保存对官方 RAGFlow 文件的最小化修改 patch。这些 patch 是**无法通过新增文件或 monkey patch 进一步抽离**的必需修改。

当前基线为官方标签 `v0.26.4`，共 16 个 patch。生成脚本比较标签与工作区，包含尚未提交的合并修复，不依赖 `main` 或 merge-base。

## 文件列表

| Patch 文件 | 对应官方文件 | 修改原因 |
|---|---|---|
| `.gitignore.patch` | `.gitignore` | 增加 `docs/graphrag_review/` 等自定义忽略规则 |
| `.gitattributes.patch` | `.gitattributes` | 补丁默认 LF；上游 CRLF 测试的补丁保留原字节，避免上下文失配 |
| `api_apps_restful_apis_dataset_api.py.patch` | `api/apps/restful_apis/dataset_api.py` | 后向兼容包装器 |
| `api_apps_services_dataset_api_service.py.patch` | `api/apps/services/dataset_api_service.py` | GraphRAG 可视化/删除策略 dispatch |
| `api_db_services_document_service.py.patch` | `api/db/services/document_service.py` | 文档删除时 KG 引用清理 dispatch |
| `common_settings.py.patch` | `common/settings.py` | 安装自定义 hook（audit / ES） |
| `rag_graphrag_entity_resolution.py.patch` | `rag/graphrag/entity_resolution.py` | 增量实体消解子类化分发 |
| `rag_graphrag_general_index.py.patch` | `rag/graphrag/general/index.py` | 激活 GraphRAG incremental patch |
| `rag_graphrag_utils.py.patch` | `rag/graphrag/utils.py` | 增量图读写路由 dispatch |
| `rag_svr_task_executor.py.patch` | `rag/svr/task_executor.py` | 激活 task_executor 扩展 patch |
| `rag_utils_es_conn.py.patch` | `rag/utils/es_conn.py` | ES bulk insert 去掉 `refresh=wait_for`；`delete` 增加 `refresh` 参数（GraphRAG merge 热路径传 `refresh=false`，可见性由调用方阶段末显式 refresh 保证） |
| `web_src_locales_en.ts.patch` | `web/src/locales/en.ts` | `totalChunks` i18n key |
| `web_src_locales_zh.ts.patch` | `web/src/locales/zh.ts` | `totalChunks` i18n key |
| `web_src_pages_dataset_dataset-overview_index.tsx.patch` | `web/src/pages/dataset/dataset-overview/index.tsx` | Total chunks StatCard UI |
| `web_src_pages_user-setting_setting-model_modal_provider-modal_field-config_provider-config-map.ts.patch` | 火山引擎 provider 配置 | Coding/标准 API 地址选择与提交 |
| `test_unit_test_api_apps_services_test_delete_datasets.py.patch` | 数据集删除单元测试 | 隔离 Quart 和 NLP 包，避免自定义 import 启动应用 |

## 官方更新时的工作流

```bash
# 1. 同步官方 release
 git -c http.proxy=http://127.0.0.1:7897 -c http.sslBackend=openssl fetch origin tag v0.26.4

# 2. 合并到当前定制分支；按目标版本替换标签
 git switch dev-0.26
 git merge --no-ff --no-commit v0.26.4

# 3. 解决冲突后重新生成 patch
 bash patches/regenerate_patches.sh v0.26.4
```

## 验证 patch 是否还能打到新版官方代码上

如果你怀疑新版官方代码会破坏这些 patch，可以在干净的 official 分支上测试：

```bash
 git worktree add --detach ../ragflow-patch-test v0.26.4
 git -C ../ragflow-patch-test apply --check "$PWD"/patches/*.patch "$PWD"/patches/.*.patch
 git worktree remove ../ragflow-patch-test
```

## 重新生成所有 patch

```bash
bash patches/regenerate_patches.sh v0.26.4
```

## 注意事项

- 这些 patch 仅覆盖**官方文件**的修改。所有新增自定义模块在 `dev` 分支直接维护，不通过 patch 管理。
- 生产代码中的挂载和路由修改带 `=== CUSTOM BEGIN/END ===` 标记；属性配置、测试隔离及表单配置由对应 patch 追踪。
- 如果某个 patch 长期无法直接应用，说明该官方文件结构已变，需要重新评估抽离方案。
