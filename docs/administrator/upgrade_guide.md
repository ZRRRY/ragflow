# RAGFlow 版本更新指南

本文档说明如何将 RAGFlow 从当前版本更新到较新版本，涵盖 Docker 部署和源码部署两种方式的完整更新流程。

## 版本号说明

RAGFlow 采用语义化版本控制（Semantic Versioning），版本号格式为 `v主版本.次版本.修订号`：

- **主版本（Major）**：不兼容的 API 变更或架构重构
- **次版本（Minor）**：新增功能，向后兼容
- **修订号（Patch）**：问题修复，向后兼容

当前版本号可在以下位置查看：
- 后端：`pyproject.toml` 中的 `version` 字段
- 前端：`web/package.json` 中的 `version` 字段
- 已部署实例：页面底部或系统信息页面

## 更新前准备

### 1. 查看 Release Notes

在更新前，请务必阅读目标版本的 [Release Notes](/release_notes)，了解：
- 新增功能及破坏性变更
- 废弃的功能或配置项
- 数据库迁移要求
- 依赖版本变更

### 2. 备份数据

> ⚠️ **强烈建议在更新前进行完整备份**，尤其是在跨次版本或主版本升级时。

#### Docker 部署备份

```bash
# 停止服务
docker compose -f docker/docker-compose.yml down

# 执行备份（会在项目根目录创建 backup/ 文件夹）
bash docker/migration.sh backup

# 如需自定义备份目录名
bash docker/migration.sh backup ragflow_backup_$(date +%Y%m%d)
```

#### 手动备份关键数据

| 数据类型 | Docker Volume 名称 | 备份内容 |
|---------|-------------------|---------|
| MySQL 数据库 | `docker_mysql_data` | 知识库配置、用户数据、对话记录 |
| Elasticsearch/Infinity | `docker_esdata01` | 文档索引、向量数据 |
| MinIO 对象存储 | `docker_minio_data` | 上传的原始文件 |
| Redis 缓存 | `docker_redis_data` | 会话缓存、任务队列 |

### 3. 检查数据库迁移需求

部分版本更新涉及数据库 Schema 变更。在 [Release Notes](/release_notes) 中关注是否有以下提示：
- "Adds database migration scripts"
- "Requires database upgrade"
- 涉及表结构变更的功能更新

## Docker 部署更新

Docker 部署是最推荐的更新方式，步骤简单且环境隔离。

### 更新到最新发布版本

以更新到 `v0.25.2` 为例：

```bash
# 1. 进入项目目录
cd ragflow

# 2. 停止当前服务
docker compose -f docker/docker-compose.yml down

# 3. 更新本地代码
git pull

# 4. 切换到目标版本标签
git checkout -f v0.25.2

# 5. 更新 .env 中的镜像版本
# 编辑 docker/.env，设置：
# RAGFLOW_IMAGE=infiniflow/ragflow:v0.25.2
sed -i 's/RAGFLOW_IMAGE=.*/RAGFLOW_IMAGE=infiniflow/ragflow:v0.25.2/' docker/.env

# 6. 拉取新镜像并启动
docker compose -f docker/docker-compose.yml pull
docker compose -f docker/docker-compose.yml up -d
```

### 更新到 nightly 版本

如需使用最新的开发构建：

```bash
# 1-3 步同上

# 4. 设置 nightly 镜像
sed -i 's/RAGFLOW_IMAGE=.*/RAGFLOW_IMAGE=infiniflow/ragflow:nightly/' docker/.env

# 5. 拉取并启动
docker compose -f docker/docker-compose.yml pull
docker compose -f docker/docker-compose.yml up -d
```

### 离线环境更新

在无外网环境中：

```bash
# 在有外网的机器上拉取并导出镜像
docker pull infiniflow/ragflow:v0.25.2
docker save -o ragflow.v0.25.2.tar infiniflow/ragflow:v0.25.2

# 将 tar 文件复制到目标服务器后加载
docker load -i ragflow.v0.25.2.tar

# 更新 .env 并启动
sed -i 's/RAGFLOW_IMAGE=.*/RAGFLOW_IMAGE=infiniflow/ragflow:v0.25.2/' docker/.env
docker compose -f docker/docker-compose.yml up -d
```

## 源码部署更新

如果你通过源码直接运行 RAGFlow，更新步骤如下：

### 1. 更新代码

```bash
cd ragflow
git pull

# 如需切换到特定版本
git checkout -f v0.25.2
```

### 2. 更新 Python 依赖

```bash
# 使用 uv 同步依赖（推荐）
uv sync --python 3.12 --all-extras

# 或手动更新
uv pip install -e ".[all]"
```

### 3. 下载/更新模型依赖

```bash
uv run download_deps.py
```

### 4. 更新前端依赖（如前端有变更）

```bash
cd web
npm install
```

### 5. 数据库迁移（如需要）

如果目标版本包含数据库 Schema 变更，需要执行迁移：

#### 方式一：使用自动迁移工具

```bash
cd tools/scripts

# 查看当前 Schema 差异（Dry Run）
python db_schema_sync.py --diff \
    --host localhost --port 3306 \
    --user root --password your_password \
    --database rag_flow \
    --version v0.25.2

# 创建迁移文件
python db_schema_sync.py --create \
    --host localhost --port 3306 \
    --user root --password your_password \
    --database rag_flow \
    --version v0.25.2

# 执行迁移
python db_schema_sync.py --migrate \
    --host localhost --port 3306 \
    --user root --password your_password \
    --database rag_flow \
    --version v0.25.2
```

#### 方式二：使用数据迁移脚本

针对特定版本的数据表变更：

```bash
cd tools/scripts

# 查看可执行的迁移阶段
python mysql_migration.py --list-stages \
    --host localhost --port 3306 \
    --user root --password your_password

# 先执行 Dry Run 检查
python mysql_migration.py --stages tenant_model_provider \
    --host localhost --port 3306 \
    --user root --password your_password

# 确认无误后执行迁移
python mysql_migration.py --stages tenant_model_provider \
    --host localhost --port 3306 \
    --user root --password your_password \
    --execute
```

### 6. 重启服务

```bash
# 如果是通过脚本启动的后端服务
bash docker/launch_backend_service.sh

# 前端开发服务器
cd web
npm run dev
```

## 配置更新检查清单

版本更新后，请检查以下配置文件是否需要同步更新：

| 配置文件 | 说明 | 检查项 |
|---------|------|--------|
| `docker/.env` | Docker 环境变量 | 镜像版本、端口映射、资源限制 |
| `docker/service_conf.yaml.template` | 服务配置模板 | 新增配置项、默认值变更 |
| `api/settings.py` | 后端运行时配置 | 环境变量变更 |
| `web/.env` / `web/.env.production` | 前端环境变量 | API 地址、构建参数 |

### 常见配置变更示例

**v0.25.0 及之后版本**：
- Elasticsearch 升级至 9.x，需检查 `docker/docker-compose-base.yml` 中的 ES 镜像版本
- MinIO 官方镜像已废弃，默认切换至 `pgsty/minio`

## 更新验证

服务启动后，请依次验证以下功能：

### 1. 基础服务健康检查

```bash
# Docker 部署
docker compose -f docker/docker-compose.yml ps

# 检查各容器状态是否为 healthy
# ragflow-server / mysql / elasticsearch / redis / minio
```

### 2. 页面访问验证

- 打开 Web 界面（默认 http://localhost:80）
- 确认登录页面正常显示
- 使用现有账号登录，验证数据完整性

### 3. 核心功能验证

| 验证项 | 操作步骤 | 预期结果 |
|-------|---------|---------|
| 知识库列表 | 查看已有知识库 | 知识库配置、文档列表完整 |
| 文档解析 | 上传新文档并解析 | 解析成功，chunk 生成正常 |
| 对话功能 | 打开已有对话，发送消息 | 能正常召回并生成回复 |
| Agent 运行 | 测试已有 Agent | 工作流正常执行 |

### 4. 日志检查

```bash
# Docker 部署
docker logs -f ragflow-server

# 源码部署
tail -f ragflow-logs/api/*.log
```

关注是否有以下异常：
- 数据库连接错误
- 依赖版本不兼容的报错
- 模型加载失败

## 回滚方案

如果更新后出现问题，可按以下步骤回滚：

### Docker 部署回滚

```bash
# 1. 停止当前服务
docker compose -f docker/docker-compose.yml down

# 2. 回退代码
git checkout -f <上一个稳定版本标签>

# 3. 恢复备份数据（如果更新前已备份）
bash docker/migration.sh restore ragflow_backup_YYYYMMDD

# 4. 回退镜像版本并启动
sed -i 's/RAGFLOW_IMAGE=.*/RAGFLOW_IMAGE=infiniflow/ragflow:<上一个版本>/' docker/.env
docker compose -f docker/docker-compose.yml up -d
```

### 源码部署回滚

```bash
# 1. 回退代码
git checkout -f <上一个稳定版本标签>

# 2. 回退依赖
uv sync --python 3.12 --all-extras

# 3. 如有数据库迁移，需评估是否需要手动回退 Schema
# （db_schema_sync.py 生成的迁移文件包含 rollback() 函数）

# 4. 重启服务
```

## 常见问题

### Q: 更新后数据会丢失吗？

**A**: 正常更新（不使用 `-v` 参数删除卷）不会丢失数据。Docker 容器的升级只会替换镜像层，数据卷中的持久化数据保持不变。但仍强烈建议更新前备份。

### Q: 跨大版本升级有什么注意事项？

**A**: 
- 仔细阅读 Release Notes 中的 **Breaking Changes**
- 大版本升级通常需要执行数据库迁移
- 建议在测试环境先验证升级流程
- 如有自定义插件或修改，需检查 API 兼容性

### Q: 前端页面显示空白或报错？

**A**: 
- 清除浏览器缓存后重试
- 检查 `web/.env` 中的 API 地址配置
- 查看浏览器开发者工具（F12）的网络请求和 Console 报错

### Q: 更新后数据库连接失败？

**A**:
- 检查 MySQL 容器是否正常启动：`docker logs ragflow-mysql`
- 确认数据库迁移是否已成功执行
- 检查 `service_conf.yaml` 中的数据库连接配置

### Q: 如何确认当前运行的版本？

**A**:
```bash
# Docker 部署
docker inspect infiniflow/ragflow:$(docker compose -f docker/docker-compose.yml exec ragflow sh -c 'echo $RAGFLOW_IMAGE_TAG' 2>/dev/null || echo "current") | grep -i version

# 源码部署
cat pyproject.toml | grep version
```

## 获取帮助

如果在更新过程中遇到问题：

1. 查看 [Release Notes](/release_notes) 中对应版本的已知问题
2. 查看服务日志定位具体错误
3. 在 [GitHub Issues](https://github.com/infiniflow/ragflow/issues) 搜索类似问题
4. 提交 Issue 时附上：当前版本、目标版本、部署方式、相关日志
