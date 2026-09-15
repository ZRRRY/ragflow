# RAGFlow 自定义分支维护规范

> 本文档说明如何在当前自定义分支上继续开发，同时避免与官方代码过度耦合。
>
> 分支模型（2026-09 起生效）：
> - `main` → 跟踪 `origin/main`（origin = 官方 infiniflow/ragflow），即官方最新代码；本地不再在 fork 上维护 main 基线
> - `dev-0.27` → 跟踪 `myfork/dev-0.27`（myfork = ZRRRY/ragflow），即定制分支（v0.27.2 + 精简定制）
> - `dev-backup-v0.26.1` → v0.26.1 时代定制分支的冻结备份（代码级回滚点）
> - `backup-20260620` → 更早期的原始补丁备份（冻结，不再更新）
>
> 旧的「`git reset --hard vX.Y.Z` + force-push main 到 fork」流程已废弃。

---

## 核心原则

**能不修改官方文件，就不修改官方文件。** 优先通过新增自定义模块实现功能。

知识编译能力优先通过**编译模板**（`api/db/init_data/compilation_templates/*.yaml`）扩展——模板即配置，Python/Go 双端自动扫描加载，零代码漂移。

---

## 修改代码前的检查清单

在动任何官方文件之前，先问自己：

- [ ] 这个功能是否可以通过新增 `*_extras.py` / `*_patch.py` / `*-extras.ts` 实现？
- [ ] 是否可以通过 `.env.local` / `docker-compose.override.yml` 配置化？
- [ ] 是否可以通过编译模板 YAML 声明（知识编译相关能力）？
- [ ] 是否可以通过子类化继承官方类实现？
- [ ] 是否可以通过 Monkey Patch 在运行时替换？

只有当以上方法都不可行时，才允许修改官方文件。

---

## 必须修改官方文件时的规范

### 1. 加标准化 `CUSTOM` 标记

```python
# === CUSTOM BEGIN [feature-name] ===
# 原因：官方未提供 XX 扩展点
# 日期：2026-09-15
# 关联：your-custom-module.py
你的代码()
# === CUSTOM END [feature-name] ===
```

### 2. 同步更新 patch

```bash
# 方式一：更新单个 patch
git diff main...HEAD -- path/to/official/file.py > patches/path_to_official_file.py.patch

# 方式二：重新生成所有 patch
bash patches/regenerate_patches.sh
```

### 3. 验证 patch 还能打到官方代码上

```bash
git worktree add --detach /tmp/ragflow-patch-check main
cd /tmp/ragflow-patch-check && bash patches/apply_patches.sh --check   # 用本分支 patches/ 目录下的最新 patch
cd - && git worktree remove --force /tmp/ragflow-patch-check
```

---

## 新增自定义模块的命名规则

| 后缀 | 用途 | 示例 |
|---|---|---|
| `*_extras.py` | 附加逻辑 / service / helper | `hook-extras.ts` 的服务端对应物 |
| `*_patch.py` | Monkey Patch 模块 | `siliconflow_timeout_patch.py` |
| `*-extras.ts` | 前端附加模块 | `hook-extras.ts` |

自定义模块的文件名应尽量和对应官方文件同名镜像。

---

## 官方更新时的标准流程

```bash
# 1. 同步官方最新代码（main 直接跟踪 origin/main，ff-only；拒绝则说明本地 main 被污染，不要强推）
git checkout main && git pull --ff-only

# 2. 合并到定制分支（永远用 merge，不要用 rebase；rerere 已开启，重复冲突自动复用解法）
git checkout dev-0.27
git merge main        # 或合并某个 release tag，如 git merge v0.28.0

# 3. 解决冲突。带 CUSTOM 标记 / patch 管理的官方文件逐个核对；
#    已在 MODIFICATIONS.md 第五节登记删除的定制对应文件直接取上游版本（--theirs）
#
#    uv.lock 特殊处理——不要人工解冲突（必撞且无法人工解：上游 lock 由不同
#    registry/uv 版本生成）。直接取上游版本，再用本地工具链重新生成：
git checkout --theirs uv.lock
uv lock                         # pyproject.toml 已钉住 aliyun 镜像，本地 uv 会重写出本分支的 diff
git add uv.lock

# 4. 对照 MODIFICATIONS.md 第一节「语义漂移检查清单」逐项核对自定义模块中的官方逻辑假设
#    （文本合并不会为它们报冲突，但官方逻辑可能已变）
git diff v0.27.2..v0.28.0 -- rag/llm/embedding_model.py   # 示例：按清单逐项 diff

# 5. 重新生成 patch
bash patches/regenerate_patches.sh

# 6. 提交并推送
git add -A
git commit -m "sync: merge v0.28.0 and regenerate patches"
git push myfork dev-0.27
```

> 大版本升级（如 v0.27.2 → v0.28.x）建议新开版本分支（如 `dev-0.28`）而非直接在旧分支上合并，保留旧分支作为回滚点；合并回主线前人工复核。

---

## 提交前自查

```bash
# 当前改了哪些官方文件？
git diff --name-only main

# 现状基线（2026-09，v0.27.2）：9 个 =
#   6 个 patch 管理内（patches/*.patch，均有 CUSTOM 标记）
#   + uv.lock（本地工具链/registry 再生差异，正常，见上方流程第 3 步）
#   + 2 个前端 tsx（lint-staged prettier 良性差异，见 MODIFICATIONS.md 第二节）
# 新增第 10 个官方文件时，停下来评估是否能抽离为 extras/patch/编译模板
```

---

## backup 分支

- `dev-backup-v0.26.1`：v0.26.1 定制分支顶端（`faf86a17f`）的冻结备份，是 v0.27.2 升级的代码级回滚点。**不要再更新或删除**。注意：v0.27.2 首次启动会自动执行 DB 迁移（含列重命名），数据层面回滚必须同时恢复 MySQL 备份与 ES 快照，仅靠代码回滚不够。
- `backup-20260620`：更早期的原始补丁 frozen 备份，同样**不要再更新或删除**。

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

- `MODIFICATIONS.md`：语义漂移检查清单（合并官方更新后逐项核对）+ 修改清单、环境变量与已删除定制记录
- `patches/README.md`：patch 管理详细说明
