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
docker volume prune -f

# Remove Docker images (choose one of the options below)
echo "Removing Docker images..."

# Option 1: Remove only the embucket image
echo "Removing embucket image..."
docker rmi embucket/embucket 2>/dev/null || true

# Create datasets directory if it doesn't exist
echo "Creating datasets directory..."
mkdir -p ./datasets

# Copy events.csv to datasets directory
echo "Copying events.csv to datasets directory..."
cp events.csv ./datasets/

# Start Embucket container with NO persistent storage
echo "Starting Embucket container with clean environment..."
docker run -d --rm --name em \
  -v $(pwd)/datasets:/app/data \
  -p 3000:3000 \
  -p 8080:8080 \
  --env OBJECT_STORE_BACKEND=memory \
  --env SLATEDB_PREFIX=memory \
  embucket/embucket

echo "✓ Embucket container started successfully with CLEAN environment!"
echo "✓ Container name: em"
echo "✓ Ports: 3000 (API), 8080 (Web UI)"
echo "✓ Volume mount: $(pwd)/datasets -> /app/data"
echo "✓ Storage: In-memory only (no persistence)"
echo ""
echo "You can now run: python3 load_events.py"
echo ""
echo "Note: All data will be lost when container stops (--rm flag)" 
