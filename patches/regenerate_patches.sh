#!/usr/bin/env bash
# Regenerate official-file patches from the working tree, including merge fixes.
# Usage: bash patches/regenerate_patches.sh [official-tag-or-commit]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

BASE="${1:-v0.26.4}"
git rev-parse --verify "${BASE}^{commit}" >/dev/null

FILES=(
    ".gitattributes"
    ".gitignore"
    "api/apps/restful_apis/dataset_api.py"
    "api/apps/services/dataset_api_service.py"
    "api/db/services/document_service.py"
    "common/settings.py"
    "rag/graphrag/entity_resolution.py"
    "rag/graphrag/general/index.py"
    "rag/graphrag/utils.py"
    "rag/svr/task_executor.py"
    "rag/utils/es_conn.py"
    "test/unit_test/api/apps/services/test_delete_datasets.py"
    "web/src/locales/en.ts"
    "web/src/locales/zh.ts"
    "web/src/pages/dataset/dataset-overview/index.tsx"
    "web/src/pages/user-setting/setting-model/modal/provider-modal/field-config/provider-config-map.ts"
)

mkdir -p "$SCRIPT_DIR"

for f in "${FILES[@]}"; do
    patchname=$(echo "$f" | tr '/' '_').patch
    git diff --no-ext-diff --binary "$BASE" -- "$f" > "$SCRIPT_DIR/$patchname"
    echo "Regenerated $SCRIPT_DIR/$patchname"
done

echo ""
echo "Done. Baseline: $BASE. Review the regenerated patches before committing."
