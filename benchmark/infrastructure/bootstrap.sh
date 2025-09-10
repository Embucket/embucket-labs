#!/bin/bash

set -e

echo "Starting Embucket benchmark bootstrap process..."

# Update system (Amazon Linux 2023 uses dnf)
dnf update -y

# Install Docker if not already installed
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    dnf install -y docker
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

# Start the Docker Compose stack with database initialization
cd /home/ec2-user
echo "Starting Embucket with automatic database initialization..."
sudo -u ec2-user docker-compose up -d

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
