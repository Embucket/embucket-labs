#!/bin/bash
# User data script for EC2 instance initialization

# Log all output
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "Starting EC2 instance initialization..."

# Update system
dnf update -y

# Install basic tools
dnf install -y htop curl wget git

# Install Docker
dnf install -y docker
systemctl start docker
systemctl enable docker

# Add ec2-user to docker group
usermod -a -G docker ec2-user

echo "EC2 instance initialization completed"
