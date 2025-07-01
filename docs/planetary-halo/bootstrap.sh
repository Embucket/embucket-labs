#!/usr/bin/env bash
# ------------------------------------------------------------
# Embucket • Starlight docs • TOC stub generator
# ------------------------------------------------------------
set -euo pipefail

DOCS_DIR="src/content/docs"

# ------------------------------------------------------------------
# Table of Contents (relative paths, no .md suffix for readability)
# ------------------------------------------------------------------

P0_FILES=(
  "introduction/index"
  "introduction/key-features"
  "architecture/overview"
  "getting-started/installation"
  "getting-started/configuration"
  "getting-started/running"
  "getting-started/quick-start"
  "usage/snowflake-compatibility"
  "getting-started/connection-guide"
  "faq/index"
)

P1_FILES=(
  "usage/sql-guide"
  "usage/table-management"
  "usage/data-loading"
  "usage/lakehouse-storage"
  "ui/overview"
  "integration/client-integrations"
  "deployment/deployment-guide"
  "operations/configuration-reference"
  "operations/security-authentication"
  "operations/monitoring-troubleshooting"
  "operations/performance-tuning"
  "usage/compatibility-details"
  "guides/analytics-quickstart"
  "guides/dbt-guide"
  "reference/environment-variables"
  "reference/cli-flags"
  "reference/sql-reference"
  "reference/functions-operators"
  "reference/data-types"
  "guides/lakehouse-pipeline"
)

P2_FILES=(
  "internals/query-execution"
  "internals/storage-layer"
  "internals/transactions"
  "internals/roadmap"
  "contributing/index"
  "contributing/developer-setup"
  "contributing/workflow"
  "community/release-notes"
  "community/changelog"
  "community/glossary"
  "operations/backup-recovery"
  "operations/upgrade-guide"
  "integration/bi-tools"
  "integration/spark-trino"
  "guides/real-time-ingestion"
)

create_stub () {
  local path="${1}.md"
  local title="${2}"
  mkdir -p "$(dirname "$DOCS_DIR/$path")"
  cat > "$DOCS_DIR/$path" <<EOF
---
title: "$title"
description: "TODO: add content"
draft: true
---
<!-- Stub file generated $(date -u +"%Y-%m-%dT%H:%M:%SZ") -->
EOF
  echo "Created $DOCS_DIR/$path"
}

echo "🗂  Generating P0 stubs..."
for f in "${P0_FILES[@]}"; do
  create_stub "$f" "$(basename "$f" | tr '-' ' ' | sed -E 's/^(.)/\U\1/')"
done

echo "🗂  Generating P1 stubs..."
for f in "${P1_FILES[@]}"; do
  create_stub "$f" "$(basename "$f" | tr '-' ' ' | sed -E 's/^(.)/\U\1/')"
done

echo "🗂  Generating P2 stubs..."
for f in "${P2_FILES[@]}"; do
  create_stub "$f" "$(basename "$f" | tr '-' ' ' | sed -E 's/^(.)/\U\1/')"
done

echo "✅  All stub Markdown files created under $DOCS_DIR"
