# RAGFlow Fork 同步工作流指南

> 本文档说明如何在你的个人仓库与官方仓库之间保持同步，同时保留本地修改。

---

## 1. 当前仓库配置

已完成配置：

- `origin` → `https://github.com/ZRRRY/ragflow.git`（你的个人仓库）
- `upstream` → `https://github.com/infiniflow/ragflow.git`（官方仓库）

查看配置：

```bash
git remote -v
```

---

## 2. 日常更新流程（从官方仓库拉取更新）

### 步骤 1：暂存本地未提交的修改

如果工作区有未提交的修改，先临时保存：

```bash
git stash push -m "save before sync"
```

> 如果没有未提交的修改，可跳过此步骤。

### 步骤 2：获取官方最新代码

```bash
git fetch upstream
```

### 步骤 3：合并官方更新到本地 main 分支

```bash
git checkout main
git merge upstream/main
```

- **如果没有冲突**：自动合并完成。
- **如果有冲突**：Git 会提示冲突文件，参见下方「冲突解决」章节。

### 步骤 4：恢复本地暂存的修改

```bash
git stash pop
```

如果恢复时出现冲突，同样参考「冲突解决」章节手动处理。

### 步骤 5：推送更新后的代码到个人仓库

```bash
git push origin main
```

---

## 3. 提交个人修改到个人仓库

日常开发时，你的修改只提交到 `origin`（个人仓库），不影响官方仓库：

```bash
# 添加修改
git add .

# 提交到本地
git commit -m "描述你的修改"

# 推送到个人仓库
git push origin main
```

---

## 4. 冲突解决

当官方更新与你的本地修改出现冲突时，冲突文件中会出现如下标记：

```text
<<<<<<< HEAD
官方代码
=======
你的代码
>>>>>>> 分支名
```

### 解决方法

1. **打开冲突文件**，找到上述标记。
2. **手动编辑**，保留你需要的代码，删除冲突标记（`<<<<<<<`、`=======`、`>>>>>>>`）。
3. **标记为已解决**：

   ```bash
   git add <冲突文件>
   ```

4. **继续合并**：

   ```bash
   git commit  # 合并提交
   ```

   或者如果是 `rebase` 冲突：

   ```bash
   git rebase --continue
   ```

> **提示**：如果冲突复杂难以解决，可随时中止合并：
>
> ```bash
> git merge --abort
> # 或
> git rebase --abort
> ```

---

## 5. 使用 rebase 保持线性历史（可选）

如果你希望提交历史更干净、更线性，可以使用 `rebase` 代替 `merge`：

```bash
git fetch upstream
git checkout main
git rebase upstream/main
```

**注意**：`rebase` 会改写提交的哈希值，如果已经推送到个人仓库，后续推送需要加 `--force-with-lease`：

```bash
git push origin main --force-with-lease
```

**建议**：
- 追求历史干净 → 使用 `rebase`
- 追求操作安全、简单 → 使用 `merge`

---

## 6. 使用功能分支隔离修改（推荐）

为了避免在 `main` 分支上直接开发导致更新困难，建议为每个功能或修改创建独立分支：

```bash
# 从最新官方代码创建分支
git fetch upstream
git checkout -b feature/xxx upstream/main

# 开发、提交
git add .
git commit -m "feat: xxx"

# 推送到个人仓库
git push -u origin feature/xxx
```

### 同步官方更新到功能分支

```bash
git checkout main
git fetch upstream
git merge upstream/main
git push origin main

git checkout feature/xxx
git merge main  # 或 git rebase main
```

---

## 7. 一键同步脚本（可选）

可将以下脚本保存为 `sync-upstream.sh`，方便一键同步：

```bash
#!/bin/bash
set -e

echo "[1/4] Stashing local changes..."
git stash push -m "auto-stash before sync" || true

echo "[2/4] Fetching upstream..."
git fetch upstream

echo "[3/4] Merging upstream/main..."
git checkout main
git merge upstream/main

echo "[4/4] Restoring local changes..."
git stash pop || true

echo "[Done] Pushing to origin..."
git push origin main

echo "Sync completed!"
```

使用方式：

```bash
bash sync-upstream.sh
```

---

## 8. 常用命令速查表

| 操作 | 命令 |
|------|------|
| 查看 remote 配置 | `git remote -v` |
| 获取官方更新 | `git fetch upstream` |
| 合并官方更新 | `git merge upstream/main` |
| 变基到官方最新 | `git rebase upstream/main` |
| 暂存本地修改 | `git stash push -m "msg"` |
| 恢复暂存修改 | `git stash pop` |
| 查看暂存列表 | `git stash list` |
| 推送个人仓库 | `git push origin main` |
| 中止合并 | `git merge --abort` |
| 中止变基 | `git rebase --abort` |

---

## 9. 关键原则

1. **`origin` 只指向你的个人仓库**，所有个人修改都推送至此。
2. **`upstream` 只指向官方仓库**，仅用于拉取更新，不直接推送。
3. **更新前先 `stash` 未提交的修改**，避免工作区冲突。
4. **优先使用 `fetch` + `merge/rebase`**，避免直接使用 `git pull`。
5. **保留本地修改**：合并冲突时，根据实际需求选择保留官方代码、你的代码，或两者合并。
