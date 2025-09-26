#!/bin/bash

set -e

echo "========================================="
echo "Starting TPC-H data generation instance setup..."
echo "Timestamp: $(date)"
echo "========================================="

# Update system (Amazon Linux 2023 uses dnf)
dnf update -y

# Install required packages
echo "Installing required packages..."
dnf install -y wget unzip awscli htop

# Install DuckDB
echo "Installing DuckDB..."
cd /tmp
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
mv duckdb /usr/local/bin/
chmod +x /usr/local/bin/duckdb

# Verify DuckDB installation
echo "Verifying DuckDB installation..."
duckdb --version

# Create working directory
mkdir -p /home/ec2-user/tpch-data
chown ec2-user:ec2-user /home/ec2-user/tpch-data

# Set up environment for ec2-user
echo 'export PATH=/usr/local/bin:$PATH' >> /home/ec2-user/.bashrc

echo "========================================="
echo "Instance setup complete!"
echo "DuckDB version: $(duckdb --version)"
echo "AWS CLI version: $(aws --version)"
echo "Timestamp: $(date)"
echo "========================================="
