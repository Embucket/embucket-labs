#!/bin/sh
# Exit immediately if a command exits with a non-zero status.
set -e

# Define the path to the index.html file within your container.
# This path depends on where your backend server serves static files from.
# Update it to match your setup.
INDEX_FILE=/ui/dist/index.html

# Check if the API_URL environment variable is set.
if [ -z "$API_URL" ]; then
  echo "Error: API_URL environment variable is not set."
  exit 1
fi

echo "Setting API_URL to $API_URL in $INDEX_FILE"

# Use sed to replace the placeholder with the actual API_URL.
# The '#' is used as a delimiter to avoid conflicts with slashes in the URL.
sed -i "s#__API_URL__#$API_URL#g" $INDEX_FILE

# Execute the main command for the container (e.g., start your backend server).
exec "$@"