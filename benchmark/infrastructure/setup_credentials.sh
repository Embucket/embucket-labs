#!/bin/bash

# PowerUser Workaround: Manual AWS Credential Setup
# This script helps set up AWS credentials when IAM role creation is not permitted

echo "=== Embucket Benchmark AWS Credential Setup ==="
echo ""
echo "Due to PowerUser permission limitations, we need to set up AWS credentials manually."
echo "You have several options:"
echo ""
echo "1. Use your current AWS CLI credentials (if configured)"
echo "2. Enter AWS credentials manually"
echo "3. Use AWS SSO/Identity Center credentials"
echo ""

AWS_REGION="us-east-2"
S3_BUCKET="embucket-benchmark-us-east-2-o4uil59v"

# Check if AWS CLI is configured
if aws sts get-caller-identity --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "✅ AWS CLI is already configured!"
    CALLER_IDENTITY=$(aws sts get-caller-identity --region "$AWS_REGION" --output json)
    echo "Current identity: $(echo "$CALLER_IDENTITY" | jq -r '.Arn')"
    echo ""
    
    # Test S3 access
    if aws s3 ls "s3://$S3_BUCKET" --region "$AWS_REGION" >/dev/null 2>&1; then
        echo "✅ S3 bucket access confirmed!"
        
        # Get current credentials from AWS CLI
        AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
        AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
        AWS_SESSION_TOKEN=$(aws configure get aws_session_token)
        
        # If using SSO, we need to get temporary credentials
        if [ -z "$AWS_ACCESS_KEY_ID" ]; then
            echo "Detected AWS SSO/Identity Center. Getting temporary credentials..."
            TEMP_CREDS=$(aws sts get-session-token --region "$AWS_REGION" --output json)
            AWS_ACCESS_KEY_ID=$(echo "$TEMP_CREDS" | jq -r '.Credentials.AccessKeyId')
            AWS_SECRET_ACCESS_KEY=$(echo "$TEMP_CREDS" | jq -r '.Credentials.SecretAccessKey')
            AWS_SESSION_TOKEN=$(echo "$TEMP_CREDS" | jq -r '.Credentials.SessionToken')
        fi
        
    else
        echo "❌ S3 bucket access denied. Please check your permissions."
        echo "Required permissions for bucket: $S3_BUCKET"
        echo "- s3:GetObject"
        echo "- s3:PutObject"
        echo "- s3:DeleteObject"
        echo "- s3:ListBucket"
        exit 1
    fi
else
    echo "❌ AWS CLI not configured or credentials expired."
    echo ""
    echo "Please choose an option:"
    echo "1. Configure AWS CLI: aws configure"
    echo "2. Use AWS SSO: aws sso login --profile your-profile"
    echo "3. Enter credentials manually below"
    echo ""
    
    read -p "Enter AWS Access Key ID: " AWS_ACCESS_KEY_ID
    read -s -p "Enter AWS Secret Access Key: " AWS_SECRET_ACCESS_KEY
    echo ""
    read -p "Enter AWS Session Token (optional, press Enter to skip): " AWS_SESSION_TOKEN
fi

# Write credentials to environment file
cat > /tmp/aws_credentials.env << EOF
export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
EOF

if [ -n "$AWS_SESSION_TOKEN" ]; then
    echo "export AWS_SESSION_TOKEN=\"$AWS_SESSION_TOKEN\"" >> /tmp/aws_credentials.env
fi

echo ""
echo "✅ Credentials configured successfully!"
echo "Credentials written to /tmp/aws_credentials.env"
echo ""
echo "Next steps:"
echo "1. Source the credentials: source /tmp/aws_credentials.env"
echo "2. Start Embucket: docker-compose up -d"
echo ""
echo "Note: If using temporary credentials, they will expire and need to be refreshed."
