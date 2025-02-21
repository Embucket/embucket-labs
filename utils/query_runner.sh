#!/bin/bash

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <sql_query>"
  exit 1
fi

SQL_QUERY=$1
SQL_QUERY=$(echo "$SQL_QUERY" | tr '\n' ' ')

# Set default location if not provided
LOCATION="${QUERY_LOCATION:-0.0.0.0:3000/ui/query}"

SESSION_COOKIE=""
if [ -n "$SESSION_ID" ]; then
    SESSION_COOKIE="id=${SESSION_ID}"
fi

JSON_PAYLOAD=$(jq -n --arg query "$SQL_QUERY" '{query: $query}')

RESPONSE=$(curl --location "$LOCATION" \
  --include \
  --header "Content-Type: application/json" \
  --cookie "$SESSION_COOKIE" \
  --data "$JSON_PAYLOAD")

HEADERS=$(echo "$RESPONSE" | sed -n '/^HTTP/{:a;n;/^\r$/!ba;p}')

HTTP_STATUS=$(echo "$HEADERS" | grep -oP '(?<=HTTP/1.1 )\d{3}')

# Check for errors (status codes 4xx or 5xx)
if [[ "$HTTP_STATUS" =~ ^4|5 ]]; then
  echo "Error detected. Full response:"
  echo "$RESPONSE"
fi

BODY=$(echo "$RESPONSE" | sed -n '/^\r$/,$p' | sed '1d')

if echo "$BODY" | jq empty > /dev/null 2>&1; then
  # TODO: Find library or write recursive traversal of nested json instead of checking for result here.
  if echo "$BODY" | jq -e '.result' > /dev/null 2>&1; then
    echo "$BODY" | jq -r '.result' | jq
  else
    echo "$BODY" | jq
  fi
else
  echo "$BODY"
fi

SET_COOKIE=$(echo "$RESPONSE" | grep -oP '(?<=set-cookie: id=)[^;]+')

if [ -n "$SET_COOKIE" ]; then
  echo "Set-Cookie:"
  echo "$SET_COOKIE"
fi
