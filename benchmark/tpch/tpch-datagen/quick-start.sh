#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to log with color
log() {
    local color=$1
    local message=$2
    echo -e "${color}[$(date '+%Y-%m-%d %H:%M:%S')] ${message}${NC}"
}

log $BLUE "========================================="
log $BLUE "TPC-H SF=100 Data Generation Quick Start"
log $BLUE "Target: s3://embucket-testdata/tpch_data/sf_100/"
log $BLUE "========================================="

# Check prerequisites
log $BLUE "Checking prerequisites..."

if ! command -v terraform &> /dev/null; then
    log $RED "ERROR: Terraform is not installed. Please install Terraform first."
    exit 1
fi

if ! command -v aws &> /dev/null; then
    log $RED "ERROR: AWS CLI is not installed. Please install AWS CLI first."
    exit 1
fi

# Check for AWS credentials with profile
AWS_PROFILE="AdministratorAccess-767397688925"
if ! aws sts get-caller-identity --profile $AWS_PROFILE &> /dev/null; then
    log $RED "ERROR: AWS credentials not available for profile: $AWS_PROFILE"
    log $YELLOW "Please ensure your SSO session is active or the profile is configured correctly"
    exit 1
fi

log $GREEN "Using AWS profile: $AWS_PROFILE"

# Check SSH key
if [ ! -f ~/.ssh/id_rsa ]; then
    log $YELLOW "Generating SSH key pair..."
    ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
fi

log $GREEN "Prerequisites check passed!"

# Setup Terraform
log $BLUE "Setting up Terraform configuration..."
cd infrastructure

if [ ! -f terraform.tfvars ]; then
    log $BLUE "Creating terraform.tfvars..."
    cat > terraform.tfvars << EOF
# AWS Configuration
aws_region  = "us-east-2"
aws_profile = "AdministratorAccess-767397688925"

# Instance Configuration for SF=100
instance_type = "c7i.8xlarge"
root_volume_size = 500
root_volume_iops = 5000
root_volume_throughput = 500

# S3 Configuration
existing_s3_bucket = "embucket-testdata"
s3_prefix = "tpch_data"

# Scale factors
scale_factors = [100]

# SSH Configuration
public_key_path  = "~/.ssh/id_rsa.pub"
private_key_path = "~/.ssh/id_rsa"

# Environment
environment = "tpch-datagen"
EOF
    log $GREEN "Created terraform.tfvars with your configuration"
else
    log $YELLOW "terraform.tfvars already exists, using existing configuration"
fi

# Initialize and deploy
log $BLUE "Initializing Terraform..."
terraform init

log $BLUE "Planning deployment..."
terraform plan -out=tfplan

echo ""
log $YELLOW "Ready to deploy infrastructure for TPC-H SF=100 data generation."
log $YELLOW "This will:"
log $YELLOW "- Create a c7i.8xlarge EC2 instance (~$1.75/hour)"
log $YELLOW "- Generate TPC-H SF=100 data (~100GB)"
log $YELLOW "- Upload to s3://embucket-testdata/tpch_data/sf_100/"
log $YELLOW "- Estimated total cost: ~$3.50"
log $YELLOW "- Estimated time: 1-2 hours"
echo ""
log $YELLOW "Do you want to proceed? (y/N)"
read -r response

if [[ "$response" =~ ^[Yy]$ ]]; then
    log $BLUE "Deploying infrastructure..."
    terraform apply tfplan
    rm -f tfplan
    
    log $GREEN "Infrastructure deployed successfully!"
    
    # Show connection info
    echo ""
    log $GREEN "Instance Information:"
    echo "  Public IP: $(terraform output -raw instance_public_ip)"
    echo "  SSH Command: $(terraform output -raw ssh_command)"
    echo ""
    
    log $BLUE "Starting data generation..."
    log $YELLOW "This will take 1-2 hours. You can monitor progress with:"
    log $YELLOW "  ssh -i ~/.ssh/id_rsa ec2-user@$(terraform output -raw instance_public_ip)"
    log $YELLOW "  tail -f ~/tpch-data/sf100/generate_sf100.log"
    echo ""
    
    # Start data generation
    eval $(terraform output -raw data_generation_command)
    
    log $GREEN "========================================="
    log $GREEN "TPC-H SF=100 data generation completed!"
    log $GREEN "Data available at: s3://embucket-testdata/tpch_data/sf_100/"
    log $GREEN "========================================="
    
    echo ""
    log $YELLOW "Next steps:"
    log $YELLOW "1. Verify data in S3: aws s3 ls s3://embucket-testdata/tpch_data/sf_100/"
    log $YELLOW "2. Use the data in your benchmarks"
    log $YELLOW "3. Clean up infrastructure: cd .. && ./cleanup.sh"
    
else
    log $YELLOW "Deployment cancelled."
    rm -f tfplan
fi

cd ..
