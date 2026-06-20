#!/usr/bin/env bash
# Regenerate all official-file patches from the current dev branch.
# Run this after manually resolving conflicts or adjusting official-file changes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

FILES=(
    ".gitignore"
    "api/apps/restful_apis/dataset_api.py"
    "api/apps/services/dataset_api_service.py"
    "api/db/services/document_service.py"
    "common/settings.py"
    "rag/graphrag/entity_resolution.py"
    "rag/graphrag/general/index.py"
    "rag/graphrag/utils.py"
    "rag/svr/task_executor.py"
    "web/src/locales/en.ts"
    "web/src/locales/zh.ts"
    "web/src/pages/dataset/dataset-overview/index.tsx"
)

mkdir -p "$SCRIPT_DIR"

for f in "${FILES[@]}"; do
    patchname=$(echo "$f" | tr '/' '_').patch
    git diff main...HEAD -- "$f" > "$SCRIPT_DIR/$patchname"
    echo "Regenerated $SCRIPT_DIR/$patchname"
done

echo ""
echo "Done. Review the regenerated patches before committing."
