#!/bin/bash

# Smart setup script for Embucket - works with both local and remote (EC2) instances
# Reads EMBUCKET_HOST from .env to determine if setup should be local or remote
#
# Usage:
#   ./setup_embucket.sh [--force|-f]
#
# Options:
#   --force, -f    Force re-upload of CSV files even if they already exist on EC2

set -e

# Parse command line arguments
FORCE_UPLOAD=false
if [ "$1" = "--force" ] || [ "$1" = "-f" ]; then
    FORCE_UPLOAD=true
fi

echo "=== Embucket Setup Script ==="
if [ "$FORCE_UPLOAD" = true ]; then
    echo "Mode: Force upload (will re-upload CSV files)"
fi
echo ""

# Load environment variables
if [ -f .env ]; then
    source .env
else
    echo "Error: .env file not found"
    exit 1
fi

# Get embucket host
EMBUCKET_HOST=${EMBUCKET_HOST:-localhost}

echo "Embucket host: $EMBUCKET_HOST"

# Check if this is a local or remote setup
if [ "$EMBUCKET_HOST" = "localhost" ] || [ "$EMBUCKET_HOST" = "127.0.0.1" ]; then
    echo "=== Local Setup ==="
    echo "Running setup_docker.sh locally..."
    ./setup_docker.sh
    
    echo ""
    echo "✓ Local setup complete!"
    
    # Wait for embucket to be ready
    echo ""
    echo "Waiting for Embucket to be ready..."
    sleep 10
    
    MAX_ATTEMPTS=30
    ATTEMPT=0
    
    while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
        if curl -sf "http://localhost:3000/health" > /dev/null 2>&1; then
            echo "  ✓ Embucket is ready!"
            break
        fi
        ATTEMPT=$((ATTEMPT + 1))
        echo "  Attempt $ATTEMPT/$MAX_ATTEMPTS: Waiting for Embucket..."
        sleep 2
    done
    
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "  ⚠ Warning: Embucket health check timed out"
        exit 1
    fi
    
else
    echo "=== Remote (EC2) Setup ==="
    echo "Embucket is running on remote host: $EMBUCKET_HOST"
    
    # Check if SSH key exists
    SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
    if [ ! -f "$SSH_KEY" ]; then
        echo "Error: SSH key not found at $SSH_KEY"
        echo "Set SSH_KEY environment variable to your key path"
        exit 1
    fi
    
    # Get SSH user (default to ec2-user)
    SSH_USER="${SSH_USER:-ec2-user}"
    
    echo "Using SSH key: $SSH_KEY"
    echo "SSH user: $SSH_USER"
    
    # Step 1: Check and upload CSV files to EC2 if needed
    echo ""
    echo "Step 1/4: Checking CSV files on EC2..."
    
    # Create data directory on EC2 if it doesn't exist
    ssh -i "$SSH_KEY" "$SSH_USER@$EMBUCKET_HOST" "mkdir -p /home/$SSH_USER/data" 2>/dev/null
    
    # Check if files already exist on EC2
    FILES_EXIST=$(ssh -i "$SSH_KEY" "$SSH_USER@$EMBUCKET_HOST" "ls /home/$SSH_USER/data/*.csv 2>/dev/null | wc -l" 2>/dev/null || echo "0")
    
    # Determine if we should upload
    SHOULD_UPLOAD=false
    if [ "$FORCE_UPLOAD" = true ]; then
        echo "  Force upload enabled - will re-upload CSV files"
        SHOULD_UPLOAD=true
    elif [ "$FILES_EXIST" -lt 2 ]; then
        echo "  CSV files not found on EC2 - will upload"
        SHOULD_UPLOAD=true
    else
        echo "  ✓ CSV files already exist on EC2"
        ssh -i "$SSH_KEY" "$SSH_USER@$EMBUCKET_HOST" "ls -lh /home/$SSH_USER/data/*.csv"
    fi
    
    if [ "$SHOULD_UPLOAD" = true ]; then
        echo "  Uploading CSV files to EC2..."
        
        # Find CSV files in multiple possible locations
        YESTERDAY_FILE=""
        TODAY_FILE=""
        
        # Check possible locations for events_yesterday.csv
        for loc in "events_yesterday.csv" "gen/events_yesterday.csv" "data/events_yesterday.csv"; do
            if [ -f "$loc" ]; then
                YESTERDAY_FILE="$loc"
                break
            fi
        done
        
        # Check possible locations for events_today.csv
        for loc in "events_today.csv" "gen/events_today.csv" "data/events_today.csv"; do
            if [ -f "$loc" ]; then
                TODAY_FILE="$loc"
                break
            fi
        done
        
        # Upload files if found
        if [ -n "$YESTERDAY_FILE" ]; then
            scp -i "$SSH_KEY" "$YESTERDAY_FILE" "$SSH_USER@$EMBUCKET_HOST:/home/$SSH_USER/data/events_yesterday.csv"
            SIZE=$(ls -lh "$YESTERDAY_FILE" | awk '{print $5}')
            echo "    ✓ Uploaded events_yesterday.csv ($SIZE)"
        else
            echo "    ⚠ Warning: events_yesterday.csv not found locally"
        fi
        
        if [ -n "$TODAY_FILE" ]; then
            scp -i "$SSH_KEY" "$TODAY_FILE" "$SSH_USER@$EMBUCKET_HOST:/home/$SSH_USER/data/events_today.csv"
            SIZE=$(ls -lh "$TODAY_FILE" | awk '{print $5}')
            echo "    ✓ Uploaded events_today.csv ($SIZE)"
        else
            echo "    ⚠ Warning: events_today.csv not found locally"
        fi
        
        # Verify upload
        echo "  Verifying uploaded files on EC2..."
        ssh -i "$SSH_KEY" "$SSH_USER@$EMBUCKET_HOST" "ls -lh /home/$SSH_USER/data/*.csv"
    fi
    
    # Step 2: Upload setup_docker.sh to EC2
    echo ""
    echo "Step 2/4: Uploading setup_docker.sh to EC2..."
    scp -i "$SSH_KEY" setup_docker.sh "$SSH_USER@$EMBUCKET_HOST:/home/$SSH_USER/"
    echo "  ✓ Uploaded setup_docker.sh"
    
    # Step 3: Run setup_docker.sh on EC2
    echo ""
    echo "Step 3/4: Running setup_docker.sh on EC2..."
    ssh -i "$SSH_KEY" "$SSH_USER@$EMBUCKET_HOST" "cd /home/$SSH_USER && chmod +x setup_docker.sh && ./setup_docker.sh"
    echo "  ✓ Setup complete on EC2"
    
    # Step 4: Wait for embucket to be ready
    echo ""
    echo "Step 4/4: Waiting for Embucket to be ready..."
    MAX_ATTEMPTS=30
    ATTEMPT=0
    
    while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
        if curl -sf "http://$EMBUCKET_HOST:3000/health" > /dev/null 2>&1; then
            echo "  ✓ Embucket is ready!"
            break
        fi
        ATTEMPT=$((ATTEMPT + 1))
        echo "  Attempt $ATTEMPT/$MAX_ATTEMPTS: Waiting for Embucket..."
        sleep 2
    done
    
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "  ⚠ Warning: Embucket health check timed out, but may still be starting..."
    fi
    
    echo ""
    echo "✓ Remote setup complete!"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Embucket is ready at: http://$EMBUCKET_HOST:3000"
echo ""
echo "Next steps:"
echo "  - Run incremental tests: ./incremental.sh true 10000 embucket"
echo "  - Or run full tests: ./incremental.sh false 10000 embucket"
