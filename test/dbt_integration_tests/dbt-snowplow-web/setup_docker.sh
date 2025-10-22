#!/bin/bash

# Setup script for Embucket Docker container with COMPLETELY CLEAN environment
# This script removes all persistent data and creates a fresh container

echo "=== Setting up Embucket Docker Container (CLEAN) ==="

# Stop and remove existing container
echo "Stopping and removing existing container..."
docker stop em 2>/dev/null || true
docker rm em 2>/dev/null || true

# Remove all embucket-related volumes (optional - be careful!)
echo "Removing all Docker volumes (this will delete ALL persistent data)..."
docker volume prune -f >/dev/null 2>&1

# Remove Docker images (choose one of the options below)
echo "Removing Docker images..."

# Option 1: Remove only the embucket image
echo "Removing embucket image..."
docker rmi embucket/embucket >/dev/null 2>&1 || true

# Create datasets directory if it doesn't exist
echo "Creating datasets directory..."
mkdir -p ./datasets

# Copy events files to datasets directory
echo "Copying events files to datasets directory..."

# Check multiple locations for CSV files
CSV_LOCATIONS=("." "./data" "/home/ec2-user/data")
FOUND_YESTERDAY=false
FOUND_TODAY=false

for location in "${CSV_LOCATIONS[@]}"; do
    if [ -f "$location/events_yesterday.csv" ] && [ "$FOUND_YESTERDAY" = false ]; then
        cp "$location/events_yesterday.csv" ./datasets/
        echo "✓ Copied events_yesterday.csv from $location"
        FOUND_YESTERDAY=true
    fi
    
    if [ -f "$location/events_today.csv" ] && [ "$FOUND_TODAY" = false ]; then
        cp "$location/events_today.csv" ./datasets/
        echo "✓ Copied events_today.csv from $location"
        FOUND_TODAY=true
    fi
done

if [ "$FOUND_YESTERDAY" = false ]; then
    echo "⚠ Warning: events_yesterday.csv not found"
fi

if [ "$FOUND_TODAY" = false ]; then
    echo "⚠ Warning: events_today.csv not found"
fi

# # Check if at least one events file exists
# if [ ! -f "./datasets/events_yesterday.csv" ] && [ ! -f "./datasets/events_today.csv" ]; then
#     echo "Error: No events files found. Please run gen_events.py first to generate the events files."
#     exit 1
# fi

# Start Embucket container with NO persistent storage
echo "Starting Embucket container with clean environment..."
docker run -d --rm --name em \
  -v $(pwd)/datasets:/app/data \
  -p 3000:3000 \
  -p 8080:8080 \
  --env OBJECT_STORE_BACKEND=memory \
  --env SLATEDB_PREFIX=memory \
  --env DATA_FORMAT=arrow \
  embucket/embucket >/dev/null 2>&1

echo "✓ Embucket container started successfully with CLEAN environment!"
echo ""
echo "To load the events data, run:"
echo "  python3 load_events.py"
echo ""
echo "Or to load specific files:"
echo "  python3 load_events.py events_yesterday.csv events_today.csv" 
