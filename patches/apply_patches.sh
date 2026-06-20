#!/usr/bin/env bash
# Apply or check official-file patches on top of the current branch.
# Usage:
#   bash patches/apply_patches.sh          # apply patches
#   bash patches/apply_patches.sh --check  # dry-run only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

CHECK=false
if [[ "${1:-}" == "--check" ]]; then
    CHECK=true
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$CURRENT_BRANCH" == "dev" ]]; then
    echo "Warning: you are on the 'dev' branch, which already contains these modifications."
    echo "Patches are meant to be applied to a clean official branch (e.g. 'main')."
    echo "Run with --force to proceed anyway, or switch to 'main'."
    if [[ "${1:-}" != "--force" && "${2:-}" != "--force" ]]; then
        exit 0
    fi
fi

FAILED=()
APPLIED=()

for patch in "$SCRIPT_DIR"/*.patch; do
    [[ -f "$patch" ]] || continue
    basename_patch=$(basename "$patch")

    if $CHECK; then
        if git apply --check "$patch" 2>/dev/null; then
            echo "[OK]  $basename_patch"
        else
            echo "[FAIL] $basename_patch"
            FAILED+=("$basename_patch")
        fi
    else
        if git apply --check "$patch" 2>/dev/null; then
            git apply "$patch"
            echo "[APPLIED] $basename_patch"
            APPLIED+=("$basename_patch")
        else
            echo "[SKIP] $basename_patch (does not apply cleanly)"
            FAILED+=("$basename_patch")
        fi
    fi
done

echo ""
if $CHECK; then
    if [[ ${#FAILED[@]} -eq 0 ]]; then
        echo "All patches can be applied cleanly."
    else
        echo "${#FAILED[@]} patch(es) cannot be applied:"
        for f in "${FAILED[@]}"; do echo "  - $f"; done
        exit 1
    fi
else
    echo "Applied: ${#APPLIED[@]}"
    if [[ ${#FAILED[@]} -gt 0 ]]; then
        echo "Failed: ${#FAILED[@]}"
        for f in "${FAILED[@]}"; do echo "  - $f"; done
        exit 1
    fi
fi
