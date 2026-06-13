# RAGFlow Project Instructions for AI Coding Agents

This file provides context, build instructions, and coding standards for the RAGFlow project.
It is intended to be read by AI coding agents who have no prior knowledge of the project.

## 1. Project Overview

RAGFlow is an open-source RAG (Retrieval-Augmented Generation) engine based on deep document understanding. It fuses cutting-edge RAG with Agent capabilities to create a context layer for LLMs. It offers a streamlined RAG workflow for businesses of any scale, combining LLMs to provide truthful question-answering capabilities backed by well-founded citations from various complex formatted data.

- **Version**: 0.25.6
- **License**: Apache 2.0
- **Homepage**: https://ragflow.io/

## 2. Technology Stack

The project is a **polyglot full-stack application** with three main runtime layers:

### Python Backend (Primary / Current Default)
- **Language**: Python 3.13 – 3.14 (`requires-python = ">=3.13,<3.15"` in `pyproject.toml`)
- **Web Framework**: Quart (async, Flask-like) with `quart-cors` and `quart-auth`
- **ORM**: Peewee with custom connection pooling and retry logic
- **Task Queue**: Redis-backed custom task system (no Celery/RQ)
- **Document Stores**: Elasticsearch, Infinity, OpenSearch, or OceanBase
- **Object Storage**: MinIO, S3, OSS, GCS, Azure (via `opendal` or native SDKs)
- **Cache / Sessions**: Redis (Valkey 8 in Docker)
- **Key Libraries**: LiteLLM, ONNXRuntime, OpenCV, pdfplumber, pypdf, spacy, xgboost

### Go Backend (New / In Development)
- **Language**: Go 1.25.0
- **Web Framework**: Gin
- **ORM**: GORM
- **Document Stores**: Elasticsearch, Infinity (via `internal/engine/`)
- **Storage**: MinIO/S3 (via `minio-go/v7`, `aws-sdk-go-v2`)
- **Cache**: Redis (`go-redis/v9`)
- **NLP**: C++ tokenizer library (`internal/cpp/`) built as a static library and bound via CGO

### Frontend
- **Framework**: React 18 + TypeScript
- **Build Tool**: Vite (migrated from UmiJS)
- **Routing**: React Router v7
- **Styling**: Tailwind CSS v3 + Less preprocessor
- **State Management**: Zustand (agent editor), TanStack Query (server state), local React state
- **UI Components**: Radix UI primitives + shadcn/ui style components
- **Node Requirement**: `>=18.20.4`
- **Dev Server Port**: 9222

## 3. Directory Structure

```
ragflow/
├── api/                    # Python backend API server
│   ├── apps/               # Quart blueprints (auto-discovered)
│   │   ├── *_app.py        # Legacy routes → /v1/<page_name>
│   │   ├── restful_apis/   # RESTful routes → /api/v1/
│   │   ├── services/       # Service-layer logic
│   │   └── auth/           # OAuth/OIDC handlers
│   ├── db/                 # Database models & services (Peewee)
│   │   ├── db_models.py    # All table definitions (~1,700 lines)
│   │   ├── services/       # ~30 service modules
│   │   └── runtime_config.py
│   ├── utils/              # Backend utilities
│   ├── ragflow_server.py   # Main Python server entrypoint
│   ├── settings.py         # Mostly empty; real settings in common/settings.py
│   └── validation.py       # Python version & NLTK checks
├── rag/                    # Core RAG logic
│   ├── app/                # Document chunkers (naive, paper, book, laws, resume, etc.)
│   ├── flow/               # Pipeline system (pipeline.py, chunker/, extractor/)
│   ├── graphrag/           # Knowledge-graph RAG (general/, light/, ner/, search.py)
│   ├── llm/                # LLM, Embedding, Rerank, CV, OCR, TTS abstractions
│   ├── nlp/                # Search dealer, query processing, tokenizer
│   ├── prompts/            # Prompt generators & templates
│   ├── svr/                # Background workers
│   │   ├── task_executor.py          # Main async worker (~2,400 lines)
│   │   └── task_executor_limiter.py  # Concurrency limiters
│   └── utils/              # Storage connectors (ES/Infinity/MinIO/Redis/etc.)
├── deepdoc/                # Document parsing & OCR
│   ├── parser/             # Parsers: PDF, DOCX, PPT, Excel, HTML, Markdown, etc.
│   └── vision/             # OCR, layout recognition, table structure detection
├── agent/                  # Agentic workflow components
│   ├── canvas.py           # Graph/Canvas runtime for agent workflows
│   ├── component/          # ~25 visual workflow components (LLM, retrieval, loop, etc.)
│   ├── tools/              # Built-in tools (search, finance, code exec, crawler, etc.)
│   ├── sandbox/            # Secure code execution (FastAPI executor manager + providers)
│   └── plugin/             # Plugin manager
├── memory/                 # Long-term conversation memory subsystem
│   ├── services/           # MessageService, query builders
│   └── utils/              # Memory store connectors
├── common/                 # Shared Python utilities
│   ├── settings.py         # Real configuration hub (~453 lines)
│   └── ...                 # Crypto, decorators, file utils, etc.
├── internal/               # Go backend
│   ├── admin/              # Admin service logic
│   ├── binding/            # CGO bindings to C++ tokenizer
│   ├── cache/              # Redis cache wrapper
│   ├── cli/                # CLI commands
│   ├── cpp/                # C++ text analyzer (CMake build)
│   ├── dao/                # GORM data access layer
│   ├── engine/             # ES/Infinity engine abstractions
│   ├── entity/             # Domain entities
│   ├── handler/            # HTTP/gin handlers
│   ├── ingestion/          # Ingestion worker logic
│   ├── router/             # Gin route definitions
│   ├── server/             # Server bootstrap & config
│   ├── service/            # Core business logic + tests
│   ├── storage/            # S3/MinIO abstraction
│   └── tokenizer/          # Tokenizer service (CGO wrapper)
├── cmd/                    # Go application entrypoints
│   ├── server_main.go      # Main Go RAGFlow server (port 9384)
│   ├── admin_server.go     # Go Admin server (port 9383)
│   ├── ingestion_server.go # Standalone Go ingestion worker
│   └── ragflow_cli.go      # Interactive CLI
├── web/                    # Frontend application
│   ├── src/
│   │   ├── components/     # Reusable UI components (~40+ folders)
│   │   ├── pages/          # Feature-based pages (agent, dataset, chat, memory, admin, etc.)
│   │   ├── hooks/          # Custom React hooks
│   │   ├── services/       # API service modules
│   │   ├── locales/        # i18n (15 languages)
│   │   ├── layouts/        # Page layouts
│   │   ├── interfaces/     # TypeScript type definitions
│   │   ├── utils/          # Utilities (request.ts, api.ts, next-request.ts)
│   │   ├── app.tsx         # Root component
│   │   ├── main.tsx        # Entry point
│   │   └── routes.tsx      # Centralized route definitions
│   ├── package.json
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── tailwind.config.js
│   └── jest.config.ts
├── docker/                 # Docker deployment configurations
│   ├── Dockerfile          # Multi-stage build (base → builder → production)
│   ├── docker-compose-base.yml   # Infrastructure services (MySQL, ES, Redis, MinIO, etc.)
│   ├── docker-compose.yml        # Main app services (cpu/gpu profiles)
│   ├── entrypoint.sh       # Container startup orchestrator
│   ├── launch_backend_service.sh # Python backend launcher
│   ├── launch_admin_service.sh   # Python admin launcher
│   ├── nginx/              # Nginx configs (python/go/hybrid modes)
│   ├── finisher/           # GraphRAG stuck-task reconciliation helpers
│   └── .env                # Docker Compose environment
├── helm/                   # Kubernetes Helm chart
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/          # K8s manifests
├── sdk/                    # Python SDK (`sdk/python/ragflow_sdk/`)
├── test/                   # Backend tests
│   ├── unit_test/          # Unit tests by module (agent, api, common, deepdoc, rag, memory)
│   ├── testcases/          # Integration tests (RESTful, HTTP, SDK, Web, Admin APIs)
│   ├── playwright/         # E2E browser tests
│   ├── benchmark/          # Performance tests
│   └── fixtures/           # Test data
├── conf/                   # Runtime configuration templates & mappings
│   ├── service_conf.yaml.template
│   ├── llm_factories.json  # Supported LLM provider catalog
│   ├── mapping.json        # ES index mapping
│   ├── infinity_mapping.json
│   ├── private.pem / public.pem  # RSA keys
│   └── ...
├── pyproject.toml          # Python project config, deps, ruff, pytest
├── uv.lock                 # uv lockfile
├── go.mod / go.sum         # Go module definitions
├── build.sh                # Go + C++ build script
├── download_deps.py        # Offline resource downloader (models, NLTK, Chrome, etc.)
└── .pre-commit-config.yaml # Pre-commit hooks
```

## 4. Build Instructions

### Prerequisites
- Python 3.13+ with `uv` installed
- Node.js >= 18.20.4 with `npm`
- Go 1.25.0 (for Go stack)
- cmake, g++, libpcre2-dev (for C++ tokenizer build)
- Docker & Docker Compose (for deployment)

### Backend (Python)
The project uses **uv** for dependency management.

1. **Setup Environment**:
   ```bash
   uv sync --python 3.13 --all-extras
   uv run python3 download_deps.py
   ```

2. **Run Server**:
   - **Pre-requisite**: Start dependent services (MySQL, ES/Infinity, Redis, MinIO).
     ```bash
     docker compose -f docker/docker-compose-base.yml up -d
     ```
   - **Launch Python backend**:
     ```bash
     export PYTHONPATH=$(pwd)
     bash docker/launch_backend_service.sh
     ```
   - **Launch Python admin server** (optional):
     ```bash
     bash docker/launch_admin_service.sh
     ```

### Backend (Go)
1. **Build C++ static library + Go binaries**:
   ```bash
   ./build.sh --all
   ```
   This produces `bin/server_main`, `bin/admin_server`, and `bin/ragflow_cli`.

2. **Run**:
   ```bash
   ./bin/admin_server   # port 9383
   ./bin/server_main    # port 9384
   ```

### Frontend
Located in `web/`.

1. **Install Dependencies**:
   ```bash
   cd web
   npm install
   ```

2. **Run Dev Server**:
   ```bash
   npm run dev
   ```
   Runs on port **9222** by default. The Vite dev proxy forwards `/api` to `http://127.0.0.1:9380/` (Python backend) and `/api/v1/admin` to `http://127.0.0.1:9381/`.

3. **Build for Production**:
   ```bash
   npm run build
   ```

### Docker Deployment
To run the full stack using Docker:
```bash
cd docker
docker compose -f docker-compose.yml up -d
```

The main `Dockerfile` is a multi-stage build that:
1. Installs system dependencies (Ubuntu 24.04, nginx, Node.js 20, uv, ODBC, Chrome)
2. Installs Python dependencies from `uv.lock`
3. Builds the web frontend
4. Assembles the production image with nginx configs and the entrypoint script.

### Kubernetes Deployment
Use the Helm chart:
```bash
cd helm
helm install ragflow .
```
Configure `values.yaml` for your doc engine, storage, and ingress settings.

## 5. Runtime Architecture

RAGFlow can run in multiple modes:

### Python Stack (Legacy / Current Default)
- `api/ragflow_server.py` starts the Quart HTTP server.
- `rag/svr/task_executor.py` runs as background workers consuming Redis tasks.
- Nginx serves the frontend and proxies API requests.
- The `docker/entrypoint.sh` script orchestrates which subsystems to start (webserver, task executors, data sync, MCP server, admin server).

### Go Stack (New)
- `cmd/admin_server.go` must start first (port 9383).
- `cmd/server_main.go` starts the main Gin HTTP server (port 9384).
- `cmd/ingestion_server.go` is a standalone ingestion worker.
- Nginx can proxy to the Go stack via `ragflow.conf.golang` or `ragflow.conf.hybrid`.

### Data Flow
1. Frontend (React/Vite) → HTTP `/api/v1/...` → Quart/Gin API layer
2. API layer → `api/db/services/` (Peewee/GORM) → MySQL/PostgreSQL/OceanBase
3. Background tasks → Redis queue → `task_executor.py` / Go ingestion worker
4. Document parsing → `deepdoc/` (pdfplumber, OCR, layout recognition)
5. Chunking/Embedding → `rag/app/`, `rag/llm/` → Doc store (ES/Infinity/OpenSearch)
6. Agent workflows → `agent/canvas.py` with component graph execution

## 6. Testing Instructions

### Backend Tests (Python)
- **Run all tests**:
  ```bash
  uv run pytest
  ```
- **Run with coverage**:
  ```bash
  uv run pytest --cov
  ```
- **Run specific test file**:
  ```bash
  uv run pytest test/unit_test/rag/test_some_module.py
  ```
- **Run by priority marker**:
  ```bash
  uv run pytest -m p0     # critical priority
  uv run pytest -m smoke  # smoke tests
  ```

**Test organization**:
- `test/unit_test/` — True unit tests with heavy mocking via `conftest.py`
- `test/testcases/restful_api/` — RESTful API integration tests
- `test/testcases/test_http_api/` — HTTP API integration tests
- `test/testcases/test_sdk_api/` — Python SDK integration tests
- `test/playwright/` — E2E browser tests (auth, dataset upload, chat, search, agents)
- `test/benchmark/` — Performance benchmarks

**Key pytest markers** (defined in `pyproject.toml`):
- `p0`: critical priority
- `p1`: high priority
- `p2`: medium priority
- `p3`: low priority
- `smoke`: smoke tests
- `auth`: authentication UI tests

Tests are run against **both Elasticsearch and Infinity** document stores in CI.

### Frontend Tests
```bash
cd web
npm run test       # Jest with coverage
npm run type-check # TypeScript check
```

### Go Tests
```bash
./run_go_tests.sh   # or: go test ./internal/...
```

### CI/CD
- **Workflow**: `.github/workflows/tests.yml`
- **Runner**: Self-hosted (`ragflow-test`)
- **Triggers**: Push to `main`/`*.x.x`, PRs with `ci` label (non-draft), daily cron at 00:00 CST
- **Steps**: Ruff static check → Go server build → Docker image build → pytest suites

## 7. Code Style & Conventions

### Python
- **Formatter / Linter**: Ruff (configured in `pyproject.toml`)
- **Line length**: 200 characters
- **Async lint rules**: `ASYNC`, `ASYNC1` enabled
- **Ignored rules**: `E402` (module-level import not at top)
- **Excluded files**: `.venv`, `rag/svr/discord_svr.py`
- **License header**: All source files must include the Apache 2.0 header:
  ```python
  #
  #  Copyright 2024 The InfiniFlow Authors. All Rights Reserved.
  #
  #  Licensed under the Apache License, Version 2.0 (the "License");
  #  ...
  #
  ```
- **Test file naming**: `test_*.py`
- **Class naming**: `Test*` for test classes

### Frontend
- **Linter**: ESLint (`.eslintrc.cjs`)
  - Extends: `eslint:recommended`, `@typescript-eslint/recommended`, `react/recommended`, `react-hooks/recommended`
  - Plugins: `@typescript-eslint`, `react`, `react-refresh`, `check-file`
  - `no-console`: warn (allows `warn`/`error`)
  - Enforces **kebab-case** filenames and folder names inside `src/`
- **Formatter**: Prettier (`.prettierrc`)
  - `printWidth: 80`, `singleQuote: true`, `trailingComma: "all"`
  - Plugins: `prettier-plugin-organize-imports`, `prettier-plugin-packagejson`
- **File naming**: kebab-case for folders and files (e.g., `use-chat-request.ts`, `knowledge-service.ts`)

### Go
- Standard `gofmt` formatting
- Package naming follows domain under `ragflow/internal/...`

### Pre-commit Hooks
Install at the project root:
```bash
pre-commit install
pre-commit run --all-files
```

**`.pre-commit-config.yaml`** runs:
- `check-yaml`, `check-json`, `trailing-whitespace`, `check-case-conflict`, `check-merge-conflict`, `mixed-line-ending`, `check-symlinks`
- `ruff` (lint + auto-fix)
- `ruff-format`

**Frontend Git Hooks** (Husky in `web/.husky/`):
```bash
cd web && npm run prepare  # initializes husky
```
- `pre-commit` runs `lint-staged`:
  - `*.{css,less,json}` → Prettier
  - `*.{js,jsx,ts,tsx}` → Prettier + ESLint

### Comment Conventions
- `check_comment_ascii.py` enforces **ASCII-only characters** in Python comments and docstrings.

## 8. Security Considerations

- **RSA Keys**: `conf/private.pem` and `conf/public.pem` are used for encryption. Do not commit real production keys.
- **API Tokens**: `APIToken` model stores tenant-level tokens; `SECRET_KEY` is auto-generated via Redis if missing.
- **Sandboxed Code Execution**: `agent/sandbox/` provides isolated execution with pluggable backends (`local`, `e2b`, `aliyun_codeinterpreter`, `self_managed`, `ssh`). Security tests exist in `agent/sandbox/tests/`.
- **Environment Variables**: Sensitive config (DB passwords, API keys, MinIO secrets) lives in `docker/.env` and `conf/service_conf.yaml`. Never commit these with real values.
- **CORS**: Configured in `api/apps/__init__.py` via `quart-cors`.
- **Authentication**: Multi-method auth (JWT, API token, Beta token, session fallback) implemented in `api/apps/__init__.py`.
- **SQL Injection Prevention**: Peewee ORM and GORM are used for all database access.

## 9. Key Configuration Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Python deps, ruff config, pytest config, coverage config |
| `uv.lock` | Reproducible Python dependency lockfile |
| `go.mod` / `go.sum` | Go module definitions |
| `web/package.json` | Frontend deps, scripts, lint-staged config |
| `web/vite.config.ts` | Vite build, dev proxy, code splitting, path aliases |
| `web/tsconfig.json` | TypeScript strict mode, path mapping `@/*` → `src/*` |
| `web/tailwind.config.js` | Tailwind theme tokens and plugins |
| `web/jest.config.ts` | Jest test runner config |
| `web/.eslintrc.cjs` | ESLint rules for TS/React |
| `web/.prettierrc` | Prettier formatting rules |
| `.pre-commit-config.yaml` | Root-level pre-commit hooks |
| `docker/docker-compose-base.yml` | Infrastructure services (MySQL, ES, Redis, MinIO, etc.) |
| `docker/docker-compose.yml` | App services with cpu/gpu profiles |
| `docker/.env` | Docker Compose environment variables (DB passwords, doc engine, feature flags) |
| `docker/entrypoint.sh` | Container startup orchestrator |
| `docker/nginx/ragflow.conf.python` | Nginx config for Python backend mode |
| `docker/nginx/ragflow.conf.golang` | Nginx config for Go backend mode |
| `docker/nginx/ragflow.conf.hybrid` | Nginx config for hybrid backend mode |
| `conf/service_conf.yaml.template` | Runtime service config template (DB, ES, MinIO, SMTP, OAuth, LLM defaults) |
| `conf/llm_factories.json` | Catalog of supported LLM providers & models |
| `helm/values.yaml` | Helm chart defaults for Kubernetes deployment |
| `.github/workflows/tests.yml` | CI test pipeline |
| `.github/workflows/release.yml` | Release pipeline (Docker images, PyPI packages) |

## 10. Development Tips

- **Adding a new RESTful API**: Create a new file in `api/apps/restful_apis/` (e.g., `my_api.py`). It will be auto-discovered and mounted under `/api/v1/`.
- **Adding a new agent component**: Create a new file in `agent/component/` extending `ComponentBase` from `agent/component/base.py`.
- **Adding a new document parser**: Add a module in `deepdoc/parser/` and reference it from `rag/app/` chunkers.
- **Running only Python backend without task executors**: Use `docker/entrypoint.sh --disable-taskexecutor`.
- **Switching doc engine**: Set `DOC_ENGINE` environment variable to `elasticsearch`, `infinity`, `opensearch`, or `oceanbase`.
- **GraphRAG tuning switches**: Most GraphRAG incremental/async switches (e.g., `USE_INCREMENTAL_GRAPH`, `USE_ADAPTIVE_LIMITER`, `RECONCILE_STUCK_ON_BOOT`) are read once at Python import time from `docker/.env`; restart containers after changing them. For stuck GraphRAG tasks, use `docker/finisher/finish_stuck_graphrag.py` or enable `RECONCILE_STUCK_ON_BOOT=1`.
- **Proxy mode for frontend dev**: `web/.env.development` sets `API_PROXY_SCHEME=python` by default. Change to `go` or `hybrid` when developing against the Go backend.
