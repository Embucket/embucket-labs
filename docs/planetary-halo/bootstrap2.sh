#!/bin/bash

# This script scaffolds a documentation directory structure for Starlight
# based on the Embucket documentation plan.
# It creates directories for each section and stub .md files for each page.

# Navigate to the Starlight docs content directory
# Create it if it doesn't exist for safety.
mkdir -p src/content/docs
cd src/content/docs

# --- Create top-level directories ---
echo "Creating content directories..."
mkdir -p getting-started tutorials core-concepts guides reference/sql reference/api contributing

# --- Create stub files ---

# Home (Root level files)
echo "Creating Home pages..."
touch index.md features-at-a-glance.md use-cases.md architecture.md

# Getting Started
echo "Creating Getting Started pages..."
touch getting-started/quick-start.md getting-started/installation.md

# Tutorials
echo "Creating Tutorials pages..."
touch tutorials/running-a-dbt-project.md tutorials/connecting-to-iceberg-rest-catalog.md tutorials/building-a-simple-dashboard.md

# Core Concepts
echo "Creating Core Concepts pages..."
touch core-concepts/lakehouse-architecture.md core-concepts/query-engine-datafusion.md core-concepts/metadata-slatedb.md core-concepts/concurrency-and-scaling.md

# Guides
echo "Creating Guides pages..."
touch guides/data-loading.md guides/connecting-to-external-catalogs.md guides/performance-tuning.md guides/security.md

# Reference
echo "Creating Reference pages..."
touch reference/configuration.md
# SQL Reference
touch reference/sql/index.md reference/sql/data-types.md reference/sql/ddl.md reference/sql/dml.md reference/sql/functions.md reference/sql/snowflake-compatibility.md
# API Reference
touch reference/api/index.md reference/api/snowflake-rest-api.md reference/api/iceberg-rest-catalog-api.md

# Contributing
echo "Creating Contributing pages..."
touch contributing/contribution-guide.md contributing/development-setup.md contributing/project-roadmap.md contributing/architecture-deep-dive.md

# --- Add Starlight frontmatter to all created markdown files ---
echo "Adding Starlight frontmatter to all .md files..."

# Function to convert filename to title case
to_title_case() {
    echo "$1" | sed 's/-/ /g' | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) substr($i,2)} 1'
}

# Add frontmatter to root files
for file in index.md features-at-a-glance.md use-cases.md architecture.md; do
    TITLE=$(to_title_case "${file%.md}")
    if [ "$file" == "index.md" ]; then
        TITLE="Introduction"
    fi
    echo -e "---\ntitle: $TITLE\ndescription: Awaiting content for $TITLE.\n---" > "$file"
done

# Add frontmatter to files in subdirectories
for dir in */; do
    if [ -d "$dir" ]; then
        for file in "$dir"*.md "$dir"*/*.md; do
            if [ -f "$file" ]; then
                FILENAME=$(basename "$file" .md)
                TITLE=$(to_title_case "$FILENAME")
                if [ "$FILENAME" == "index" ]; then
                    PARENT_DIR_NAME=$(basename "$(dirname "$file")")
                    TITLE=$(to_title_case "$PARENT_DIR_NAME Overview")
                fi
                 echo -e "---\ntitle: $TITLE\ndescription: Awaiting content for $TITLE.\n---" > "$file"
            fi
        done
    fi
done

echo "✅ Scaffolding complete. Your 'src/content/docs' directory is ready!"