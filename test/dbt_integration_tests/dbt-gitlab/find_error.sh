#!/bin/bash

# Error finder script for dbt run.log
# Usage: ./find_error.sh [model_name] [error_type]

LOG_FILE="assets/run.log"

if [ $# -eq 0 ]; then
    echo "Usage: $0 [model_name] [error_type]"
    echo ""
    echo "Examples:"
    echo "  $0 workspace_marketing.wk_bizible_segments"
    echo "  $0 table not found"
    echo "  $0 column not found"
    echo "  $0 Optimizer rule"
    echo "  $0 ParserError"
    exit 1
fi

SEARCH_TERM="$*"

echo "Searching for: '$SEARCH_TERM'"
echo "=================================="

# Search for the term in the log
grep -n "$SEARCH_TERM" "$LOG_FILE" | head -20

echo ""
echo "=================================="
echo "Showing context around first match:"
echo ""

# Get the first match and show context
FIRST_MATCH=$(grep -n "$SEARCH_TERM" "$LOG_FILE" | head -1 | cut -d: -f1)
if [ ! -z "$FIRST_MATCH" ]; then
    START_LINE=$((FIRST_MATCH - 5))
    END_LINE=$((FIRST_MATCH + 10))
    sed -n "${START_LINE},${END_LINE}p" "$LOG_FILE"
fi 