# RAGFlow 自定义分支维护规范

> 本文档说明如何在当前自定义分支上继续开发，同时避免与官方代码过度耦合。
>
> 当前分支结构：
> - `main` → 纯净 `v0.26.1` release（跟踪 `myfork/main`）
> - `dev` → `v0.26.1` + 自定义 GraphRAG 增量优化补丁 + patch 管理（跟踪 `myfork/dev`）
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
git diff main...HEAD -- path/to/official/file.py > patches/path_to_official_file.py.patch

# 方式二：重新生成所有 patch
bash patches/regenerate_patches.sh
```

### 3. 验证 patch 还能打到官方代码上

```bash
git checkout -b patch-test main
bash patches/apply_patches.sh --check
git checkout dev
git branch -D patch-test
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
# 1. 同步官方 release
git fetch origin
git checkout main
git reset --hard v0.27.0          # 替换为新 release tag
git push -f myfork main

# 2. 合并到 dev（永远用 merge，不要用 rebase；rerere 已开启，重复冲突自动复用解法）
git checkout dev
git merge main

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

# 5. 重新生成 patch
bash patches/regenerate_patches.sh

# 6. 提交并推送
git add -A
git commit -m "sync: merge v0.27.0 and regenerate patches"
git push myfork dev
```

---

## 提交前自查

```bash
# 当前改了哪些官方文件？
git diff --name-only main

# 现状基线（2026-07-16）：15 个 =
#   12 个 patch 管理内（patches/*.patch，均有 CUSTOM 标记或尾部挂载块）
#   + uv.lock（本地工具链/registry 再生差异，正常，见上方流程第 3 步）
#   + 2 个前端 tsx（lint-staged prettier 良性差异，见 MODIFICATIONS.md 第二节）
# 新增第 16 个官方文件时，停下来评估是否能抽离为 extras/patch
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
