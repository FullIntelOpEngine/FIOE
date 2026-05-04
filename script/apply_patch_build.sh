#!/usr/bin/env bash
# scripts/apply_patch_build.sh
# ──────────────────────────────────────────────────────────────────────────────
# AI Autofix: apply a unified diff, run tests, optionally build & push Docker.
#
# Usage:
#   apply_patch_build.sh <patch_file> <project_root> [build_docker=0] [push_image=0]
#
# Exit codes:
#   0 – success
#   1 – patch apply failed (originals restored)
#   2 – tests failed      (originals restored)
#   3 – docker build failed
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

PATCH_FILE="${1:?patch_file is required}"
PROJECT_ROOT="${2:?project_root is required}"
BUILD_DOCKER="${3:-0}"
PUSH_IMAGE="${4:-0}"

DOCKER_IMAGE="${DOCKERHUB_USERNAME:-autosourcing}/autosourcing"
DOCKER_TAG="${DOCKER_TAG:-latest}"
BACKUP_DIR="${PROJECT_ROOT}/.ai_autofix_backup_$(date +%s)"

# ── Validate patch file ────────────────────────────────────────────────────────
if [[ ! -f "$PATCH_FILE" ]]; then
  echo "ERROR: patch file not found: $PATCH_FILE" >&2
  exit 1
fi

cd "$PROJECT_ROOT"
echo "=== AI Autofix: applying patch ==="
echo "  Patch:        $PATCH_FILE"
echo "  Project root: $PROJECT_ROOT"
echo "  Build Docker: $BUILD_DOCKER / Push: $PUSH_IMAGE"

# ── Backup affected files ──────────────────────────────────────────────────────
mkdir -p "$BACKUP_DIR"

# Extract affected file paths from unified-diff header lines (--- a/path / +++ b/path)
AFFECTED=$(grep -E '^(---|[+][+][+]) [ab]/' "$PATCH_FILE" 2>/dev/null \
  | sed 's|^[+\-]* [ab]/||' \
  | sort -u \
  | head -100 || true)

for f in $AFFECTED; do
  # Security: skip anything that looks like an absolute path or traversal
  if [[ "$f" == /* ]] || [[ "$f" == *".."* ]]; then
    echo "  SKIP (unsafe path): $f"
    continue
  fi
  if [[ -f "$PROJECT_ROOT/$f" ]]; then
    mkdir -p "$BACKUP_DIR/$(dirname "$f")"
    cp "$PROJECT_ROOT/$f" "$BACKUP_DIR/$f"
    echo "  Backed up: $f"
  fi
done

# ── Restore function (called on ERR trap) ─────────────────────────────────────
restore_backup() {
  local rc=$?
  echo "!!! Error (exit $rc) — restoring backups..." >&2
  for f in $AFFECTED; do
    if [[ -f "$BACKUP_DIR/$f" ]]; then
      cp "$BACKUP_DIR/$f" "$PROJECT_ROOT/$f"
      echo "  Restored: $f" >&2
    fi
  done
  rm -rf "$BACKUP_DIR"
}
trap restore_backup ERR

# ── Apply the unified diff ─────────────────────────────────────────────────────
echo "=== Applying patch ==="
if ! patch -p1 --forward --no-backup-if-mismatch < "$PATCH_FILE"; then
  echo "ERROR: patch command failed" >&2
  exit 1
fi
echo "  Patch applied."

# ── Run tests ─────────────────────────────────────────────────────────────────
echo "=== Running tests ==="

NODE_TEST_FAILED=0
PY_TEST_FAILED=0

if [[ -f "$PROJECT_ROOT/package.json" ]]; then
  echo "  Running: npm test"
  if ! npm test --if-present 2>&1 | tee /tmp/ai_fix_npm_test.log; then
    cat /tmp/ai_fix_npm_test.log >&2
    echo "[WARN] npm test reported failures" >&2
    NODE_TEST_FAILED=1
  fi
fi

if command -v pytest &>/dev/null && \
   { [[ -f "$PROJECT_ROOT/requirements.txt" ]] || [[ -f "$PROJECT_ROOT/pyproject.toml" ]]; }; then
  echo "  Running: pytest"
  if ! pytest --tb=short -q 2>&1; then
    echo "[WARN] pytest reported failures" >&2
    PY_TEST_FAILED=1
  fi
fi

if [[ $NODE_TEST_FAILED -eq 1 || $PY_TEST_FAILED -eq 1 ]]; then
  echo "ERROR: tests failed — restoring originals" >&2
  restore_backup
  exit 2
fi

# ── Build Docker image ─────────────────────────────────────────────────────────
if [[ "$BUILD_DOCKER" == "1" ]]; then
  if [[ -f "$PROJECT_ROOT/Dockerfile" ]]; then
    echo "=== Building Docker image: ${DOCKER_IMAGE}:${DOCKER_TAG} ==="
    if ! docker build -t "${DOCKER_IMAGE}:${DOCKER_TAG}" "$PROJECT_ROOT"; then
      echo "ERROR: docker build failed" >&2
      exit 3
    fi
  else
    echo "[WARN] No Dockerfile found — skipping Docker build"
  fi
fi

# ── Push Docker image ──────────────────────────────────────────────────────────
if [[ "$PUSH_IMAGE" == "1" && "$BUILD_DOCKER" == "1" ]]; then
  if [[ -n "${DOCKERHUB_USERNAME:-}" && -n "${DOCKERHUB_TOKEN:-}" ]]; then
    echo "=== Pushing image: ${DOCKER_IMAGE}:${DOCKER_TAG} ==="
    # Use --password-stdin (token never appears in ps/logs); store credentials
    # in Docker's credential store rather than plain config.json in production.
    echo "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
    docker push "${DOCKER_IMAGE}:${DOCKER_TAG}"
  else
    echo "[WARN] DOCKERHUB_USERNAME / DOCKERHUB_TOKEN not set — skipping push"
  fi
fi

# ── Clean up backup ────────────────────────────────────────────────────────────
trap - ERR
rm -rf "$BACKUP_DIR"
echo "=== AI Autofix: patch applied successfully ==="
