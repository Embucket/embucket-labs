#!/bin/bash

# Validation script to check if the benchmark deployment is working

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🔍 Validating Embucket Benchmark Infrastructure Deployment...${NC}"

# Get instance IP from Terraform output
INSTANCE_IP=$(terraform output -raw instance_public_ip 2>/dev/null || echo "")

if [ -z "$INSTANCE_IP" ]; then
    echo -e "${RED}❌ Could not get instance IP from Terraform output${NC}"
    echo "Make sure you've run 'terraform apply' successfully"
    exit 1
fi

echo -e "${GREEN}✅ Instance IP: $INSTANCE_IP${NC}"

# Check if instance is running
echo -e "${YELLOW}🔍 Checking EC2 instance status...${NC}"
INSTANCE_ID=$(terraform output -raw instance_id 2>/dev/null || echo "")
if [ ! -z "$INSTANCE_ID" ]; then
    INSTANCE_STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "unknown")
    if [ "$INSTANCE_STATE" = "running" ]; then
        echo -e "${GREEN}✅ EC2 instance is running${NC}"
    else
        echo -e "${RED}❌ EC2 instance is not running (state: $INSTANCE_STATE)${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ Could not get instance ID${NC}"
fi

# Check if Embucket API is responding
echo -e "${YELLOW}🔍 Checking Embucket API (port 3000)...${NC}"
if curl -s --connect-timeout 10 "http://$INSTANCE_IP:3000/health" > /dev/null; then
    echo -e "${GREEN}✅ Embucket API is responding on port 3000${NC}"
else
    echo -e "${RED}❌ Embucket API is not responding on port 3000${NC}"
fi

# Check if Embucket UI is responding
echo -e "${YELLOW}🔍 Checking Embucket UI (port 8080)...${NC}"
if curl -s --connect-timeout 10 "http://$INSTANCE_IP:8080" > /dev/null; then
    echo -e "${GREEN}✅ Embucket UI is responding on port 8080${NC}"
else
    echo -e "${RED}❌ Embucket UI is not responding on port 8080${NC}"
fi

# Check database initialization
echo -e "${YELLOW}🔍 Checking database initialization...${NC}"

# Test Embucket authentication and database
AUTH_RESPONSE=$(curl -s --connect-timeout 10 \
    -X POST "http://$INSTANCE_IP:3000/ui/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"embucket","password":"embucket"}' 2>/dev/null || echo "")

if echo "$AUTH_RESPONSE" | grep -q "accessToken"; then
    echo -e "${GREEN}✅ Embucket authentication successful${NC}"

    # Extract token and test database
    TOKEN=$(echo "$AUTH_RESPONSE" | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)

    if [ ! -z "$TOKEN" ]; then
        # Test database query
        DB_RESPONSE=$(curl -s --connect-timeout 10 \
            -X POST "http://$INSTANCE_IP:3000/ui/queries" \
            -H "Content-Type: application/json" \
            -H "authorization: Bearer $TOKEN" \
            -d '{"query":"SHOW DATABASES"}' 2>/dev/null || echo "")

        if echo "$DB_RESPONSE" | grep -q "embucket"; then
            echo -e "${GREEN}✅ Database 'embucket' is accessible${NC}"
        else
            echo -e "${YELLOW}⚠️ Database query executed but benchmark database not clearly found${NC}"
        fi
    fi
else
    echo -e "${RED}❌ Failed to authenticate with Embucket for database check${NC}"
fi

# Check S3 bucket
echo -e "${YELLOW}🔍 Checking S3 bucket...${NC}"
S3_BUCKET=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
if [ ! -z "$S3_BUCKET" ]; then
    if aws s3 ls "s3://$S3_BUCKET" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ S3 bucket '$S3_BUCKET' is accessible${NC}"
    else
        echo -e "${RED}❌ S3 bucket '$S3_BUCKET' is not accessible${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ Could not get S3 bucket name${NC}"
fi

# Show access URLs
echo -e "${YELLOW}📋 Access URLs:${NC}"
echo -e "  Embucket API:  http://$INSTANCE_IP:3000"
echo -e "  Embucket UI:   http://$INSTANCE_IP:8080"

# Show SSH command
SSH_COMMAND=$(terraform output -raw ssh_command 2>/dev/null || echo "ssh -i ~/.ssh/id_rsa ec2-user@$INSTANCE_IP")
echo -e "${YELLOW}🔑 SSH Access:${NC}"
echo -e "  $SSH_COMMAND"

# Show instance information
INSTANCE_TYPE=$(terraform output -raw instance_type 2>/dev/null || echo "unknown")
AWS_REGION=$(terraform output -raw aws_region 2>/dev/null || echo "unknown")
echo -e "${YELLOW}📊 Instance Information:${NC}"
echo -e "  Instance Type: $INSTANCE_TYPE"
echo -e "  AWS Region:    $AWS_REGION"
echo -e "  S3 Bucket:     $S3_BUCKET"

echo -e "${GREEN}🎉 Validation complete!${NC}"
echo -e "${YELLOW}💡 Next steps:${NC}"
echo -e "  1. SSH to the instance: $SSH_COMMAND"
echo -e "  2. Run benchmarks against the Embucket API"
echo -e "  3. Monitor performance and scale instance type if needed"
