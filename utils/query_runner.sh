#!/bin/bash

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <location_or_cluster_id> <sql_query>"
  exit 1
fi

LOCATION_OR_CLUSTER=$1

shift
SQL_QUERY="$@"

SQL_QUERY=$(echo "$SQL_QUERY" | tr '\n' ' ')

# TODO: This should be change. Use env variable for cluster location or provide seperate params.
if [[ $LOCATION_OR_CLUSTER == *\/* ]]; then
  LOCATION=$LOCATION_OR_CLUSTER
else
  DEFAULT_BASE_URL="0.0.0.0:3000/ui/warehouses"
  LOCATION="$DEFAULT_BASE_URL/$LOCATION_OR_CLUSTER/databases/datasets/tables/table_upload_tests/query"
fi


JSON_PAYLOAD=$(jq -n --arg query "$SQL_QUERY" '{"query": $query}')

RESPONSE=$(curl --location "$LOCATION" \
  --header "Content-Type: application/json" \
  --data "$JSON_PAYLOAD")

if echo "$RESPONSE" | jq empty > /dev/null 2>&1; then
# TODO: Find library or write recursive traversal of nested json instead of checking for result here.
    if echo "$RESPONSE" | jq -e '.result' > /dev/null 2>&1; then
        echo "$RESPONSE" | jq -r '.result' | jq
    else
        echo "$RESPONSE" | jq
    fi
else
  echo "$RESPONSE"
fi