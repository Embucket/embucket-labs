#!/bin/sh
set -e

INDEX_FILE=/app/dist/index.html

# Use the provided API_URL or default to http://localhost:3000 if it's not set
FINAL_API_URL=${API_URL:-http://localhost:3000}

echo "Setting API URL to $FINAL_API_URL in $INDEX_FILE"

# Use sed to replace the placeholder with the actual API_URL.
sed -i "s#__API_URL__#$FINAL_API_URL#g" $INDEX_FILE

exec "$@"