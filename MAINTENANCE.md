# RAGFlow 自定义分支维护规范

> 本文档说明如何在当前自定义分支上继续开发，同时避免与官方代码过度耦合。
>
> 当前分支结构：
> - 官方基线 → 标签 `v0.26.4`；补丁直接与标签比较，不依赖 `main` 的位置。
> - `dev-0.26` → `v0.26.4` + 自定义 GraphRAG 增量优化补丁（跟踪 `fork/dev-0.26`）。
> - `codex/backup-before-v0.26.4-20261028` → 本次升级前的 `e9d16343f`。
> - `backup-20260620` → 原始补丁提交 `41946525f`（冻结，不再更新）

---

## 核心原则

**能不修改官方文件，就不修改官方文件。** 优先通过新增自定义模块实现功能。

---

## 修改代码前的检查清单

在动任何官方文件之前，先问自己：

- [ ] 这个功能是否可以通过新增 `*_extras.py` / `*_patch.py` / `*-extras.ts` 实现？
- [ ] 是否可以通过 `.env.local` / `docker-compose.override.yml` 配置化？
- [ ] 是否可以通过子类化继承官方类实现？
- [ ] 是否可以通过 Monkey Patch 在运行时替换？

只有当以上方法都不可行时，才允许修改官方文件。

---

## 必须修改官方文件时的规范

### 1. 加标准化 `CUSTOM` 标记

```python
# === CUSTOM BEGIN [feature-name] ===
# 原因：官方未提供 XX 扩展点
# 日期：2026-06-20
# 关联：your-custom-module.py
你的代码()
# === CUSTOM END [feature-name] ===
```

### 2. 同步更新 patch

```bash
# 方式一：更新单个 patch
git diff v0.26.4 -- path/to/official/file.py > patches/path_to_official_file.py.patch

# 方式二：重新生成所有 patch
bash patches/regenerate_patches.sh v0.26.4
```

### 3. 验证 patch 还能打到官方代码上

```bash
git worktree add --detach ../ragflow-patch-test v0.26.4
git -C ../ragflow-patch-test apply --check "$PWD"/patches/*.patch "$PWD"/patches/.*.patch
git worktree remove ../ragflow-patch-test
```

---

## 新增自定义模块的命名规则

| 后缀 | 用途 | 示例 |
|---|---|---|
| `*_extras.py` | 附加逻辑 / service / helper | `dataset_api_extras.py` |
| `*_patch.py` | Monkey Patch 模块 | `redis_conn_patch.py` |
| `*-extras.ts` | 前端附加模块 | `hook-extras.ts` |

自定义模块的文件名应尽量和对应官方文件同名镜像。

---

## 官方更新时的标准流程

```bash
# 1. 核验官方 release（GitHub 使用本地代理；按目标版本替换标签）
git -c http.proxy=http://127.0.0.1:7897 -c http.sslBackend=openssl fetch origin tag v0.26.4

# 2. 在当前定制分支合并准确标签；使用 merge，不要 rebase
git switch dev-0.26
git branch codex/backup-before-next-upgrade
git merge --no-ff --no-commit v0.26.4

# 3. 重点解决带 CUSTOM 标记的官方文件冲突
#
#    uv.lock 特殊处理——不要人工解冲突（必撞且无法人工解：上游 lock 由不同
#    registry/uv 版本生成）。直接取上游版本，再用本地工具链重新生成：
git checkout --theirs uv.lock
uv lock                         # pyproject.toml 已钉住 aliyun 镜像，本地 uv 会重写出本分支的 diff
git add uv.lock

# 4. 对照 MODIFICATIONS.md 第一节「语义漂移检查清单」逐项核对 extras 中的官方逻辑副本
#    （文本合并不会为它们报冲突，但官方逻辑可能已变）
git diff v0.26.1..v0.27.0 -- rag/graphrag/general/index.py   # 示例：按清单逐项 diff

# 5. 重新生成 patch（包括工作区中尚未提交的冲突修复）
bash patches/regenerate_patches.sh v0.26.4

# 6. 提交并推送
git add -A
git commit -m "sync: merge v0.26.4 and adapt GraphRAG customizations"
git -c http.proxy=http://127.0.0.1:7897 -c http.sslBackend=openssl push fork dev-0.26
```

---

## 提交前自查

```bash
# 当前改了哪些官方文件？
git diff --name-only v0.26.4

# 现状基线（2026-10-28）：17 个已存在的官方文件有差异 =
#   16 个 patch 管理内（包括 .gitattributes、删除测试隔离和火山引擎地址配置）
#   + 1 个前端 tsx（sidebar import 顺序的良性差异，见 MODIFICATIONS.md 第二节）
# uv.lock 经重新锁定和检查，与官方 v0.26.4 一致；将来由工具生成，不人工拼接
# 新增官方文件修改时，先评估是否能抽离为 extras/patch
```

---

## backup 分支

`backup-20260620` 是原始补丁的 frozen 备份，**不要再更新或删除**。如果 `dev` 搞坏，可以从这里重建：

```bash
git checkout -b dev-rebuild backup-20260620
# 然后重新 merge v0.26.1 和后续补丁
```

---

## Docker 开发环境

```bash
cd docker

# 启动（.env.local 参与 compose 模板替换）
docker compose --env-file .env --env-file .env.local up -d

# 查看合并后的最终配置
docker compose --env-file .env --env-file .env.local config
```

> 注意：`docker-compose.override.yml` 会自动合并；`.env.local` 不会自动参与 `${XXX}` 替换，必须通过 `--env-file` 或 `COMPOSE_ENV_FILES=.env,.env.local` 指定。

---

## 相关文档

- `MODIFICATIONS.md`：语义漂移检查清单（合并官方更新后逐项核对）+ 修改清单与环境变量说明
- `patches/README.md`：patch 管理详细说明
