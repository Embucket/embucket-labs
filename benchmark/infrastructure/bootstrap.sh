#!/bin/bash

set -e

echo "========================================="
echo "Starting Embucket benchmark bootstrap process..."
echo "Timestamp: $(date)"
echo "========================================="

# Update system (Amazon Linux 2023 uses dnf)
dnf update -y

# Install required packages
echo "Installing required packages..."
dnf install -y docker awscli jq

# Start and enable Docker if not already running
if ! systemctl is-active --quiet docker; then
    echo "Starting Docker..."
    systemctl start docker
    systemctl enable docker
    usermod -a -G docker ec2-user
fi

# Install Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "Installing Docker Compose..."
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
fi

# Verify installations
docker --version
docker-compose --version

# Change ownership of files to ec2-user
chown -R ec2-user:ec2-user /home/ec2-user/

# Start Embucket with pre-configured AWS credentials
cd /home/ec2-user
echo "========================================="
echo "Starting Embucket with existing AWS user credentials..."
echo "Current directory: $(pwd)"
echo "========================================="

# Show .env file contents (masked)
echo "Checking .env file contents..."
if [ -f .env ]; then
    echo "✅ .env file exists"
    echo "File size: $(wc -l < .env) lines"
    # Show AWS_ACCESS_KEY_ID but mask the value
    if grep -q "AWS_ACCESS_KEY_ID=" .env; then
        echo "✅ AWS_ACCESS_KEY_ID found in .env"
    else
        echo "❌ AWS_ACCESS_KEY_ID not found in .env"
    fi
else
    echo "❌ .env file not found"
fi

# Verify credentials are in .env file and not empty
if grep -q "AWS_ACCESS_KEY_ID=" .env && [ "$(grep AWS_ACCESS_KEY_ID= .env | cut -d= -f2)" != "" ]; then
    echo "✅ AWS credentials found in .env file"
    echo "========================================="
    echo "Starting Embucket with automatic database initialization..."
    echo "Running: docker-compose up -d"
    echo "========================================="
    sudo -u ec2-user docker-compose up -d

    echo "========================================="
    echo "Checking container status..."
    sudo -u ec2-user docker-compose ps
    echo "========================================="
else
    echo "⚠️  No AWS credentials found in .env file."
    echo ""
    echo "It looks like you haven't provided existing AWS user credentials."
    echo "Please either:"
    echo "1. Add credentials to terraform.tfvars:"
    echo "   benchmark_s3_user_key_id = \"your-access-key\""
    echo "   benchmark_s3_user_access_key = \"your-secret-key\""
    echo "   Then run 'terraform apply' again"
    echo ""
    echo "2. Or SSH to this instance and run './setup_credentials.sh' to configure manually"
    echo "   Then run 'docker-compose up -d'"
fi

# Wait for containers to start and initialization to complete
echo "Waiting for containers to start..."
sleep 60

# Check container status
echo "Container status:"
sudo -u ec2-user docker-compose ps

# Wait for Embucket to be fully ready
echo "Waiting for Embucket API to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:3000/health > /dev/null 2>&1; then
        echo "✅ Embucket API is ready!"
        break
    fi
    echo "Attempt $i/30: Waiting for Embucket API..."
    sleep 10
done

# Check if database initialization was successful
echo "Checking database initialization..."
sleep 30
sudo -u ec2-user docker-compose logs db-init | tail -20

echo "Bootstrap completed successfully!"
echo "Embucket API should be available on port 3000"
echo "Embucket UI should be available on port 8080"
echo ""

# Get public IP with better error handling
PUBLIC_IP=$(curl -s --connect-timeout 5 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "")

if [ -n "$PUBLIC_IP" ]; then
    echo "Access URLs:"
    echo "  API: http://$PUBLIC_IP:3000"
    echo "  UI:  http://$PUBLIC_IP:8080"
else
    echo "⚠️  Could not retrieve public IP address"
    echo "Access URLs will be available via Terraform outputs after deployment"
fi
