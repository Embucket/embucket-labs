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

# Function to check prerequisites
check_prerequisites() {
    log $BLUE "Checking prerequisites..."
    
    # Check if terraform is installed
    if ! command -v terraform &> /dev/null; then
        log $RED "ERROR: Terraform is not installed. Please install Terraform first."
        exit 1
    fi
    
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        log $RED "ERROR: AWS CLI is not installed. Please install AWS CLI first."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log $RED "ERROR: AWS credentials not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    # Check if SSH key exists
    if [ ! -f ~/.ssh/id_rsa ]; then
        log $YELLOW "WARNING: SSH key not found at ~/.ssh/id_rsa"
        log $YELLOW "Generating SSH key pair..."
        ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
    fi
    
    log $GREEN "Prerequisites check passed!"
}

# Function to setup terraform configuration
setup_terraform() {
    log $BLUE "Setting up Terraform configuration..."
    
    cd infrastructure
    
    # Check if terraform.tfvars exists
    if [ ! -f terraform.tfvars ]; then
        log $YELLOW "Creating terraform.tfvars from example..."
        cp terraform.tfvars.example terraform.tfvars
        
        log $YELLOW "Please edit terraform.tfvars to customize your configuration:"
        log $YELLOW "- Set your preferred AWS region"
        log $YELLOW "- Choose appropriate instance type for your scale factors"
        log $YELLOW "- Adjust storage size based on your needs"
        log $YELLOW ""
        log $YELLOW "Press Enter to continue after editing terraform.tfvars..."
        read
    fi
    
    # Initialize Terraform
    log $BLUE "Initializing Terraform..."
    terraform init
    
    cd ..
}

# Function to deploy infrastructure
deploy_infrastructure() {
    log $BLUE "Deploying TPC-H data generation infrastructure..."
    
    cd infrastructure
    
    # Plan deployment
    log $BLUE "Creating Terraform plan..."
    terraform plan -out=tfplan
    
    # Ask for confirmation
    log $YELLOW "Review the plan above. Do you want to proceed with deployment? (y/N)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        log $YELLOW "Deployment cancelled."
        exit 0
    fi
    
    # Apply deployment
    log $BLUE "Applying Terraform configuration..."
    terraform apply tfplan
    
    # Clean up plan file
    rm -f tfplan
    
    log $GREEN "Infrastructure deployed successfully!"
    
    cd ..
}

# Function to show deployment information
show_deployment_info() {
    log $BLUE "Deployment Information:"
    
    cd infrastructure
    
    echo ""
    log $GREEN "Instance Information:"
    terraform output instance_public_ip
    terraform output instance_public_dns
    terraform output s3_bucket_name
    
    echo ""
    log $GREEN "SSH Command:"
    terraform output -raw ssh_command
    
    echo ""
    log $GREEN "Data Generation Command:"
    terraform output -raw data_generation_command
    
    echo ""
    log $YELLOW "Next Steps:"
    log $YELLOW "1. Wait 2-3 minutes for instance initialization to complete"
    log $YELLOW "2. Use the SSH command above to connect to the instance"
    log $YELLOW "3. Use the data generation command to start generating TPC-H data"
    log $YELLOW "4. Monitor progress with: tail -f ~/tpch-data/sf*/generate_sf*.log"
    log $YELLOW "5. Run './cleanup.sh' when data generation is complete"
    
    cd ..
}

# Function to run data generation
run_data_generation() {
    log $BLUE "Starting TPC-H data generation..."
    
    cd infrastructure
    
    # Get the data generation command
    local cmd=$(terraform output -raw data_generation_command)
    
    log $BLUE "Executing: $cmd"
    log $YELLOW "This may take several hours depending on scale factors..."
    
    # Execute the command
    eval $cmd
    
    log $GREEN "Data generation completed!"
    
    cd ..
}

# Main function
main() {
    local action=${1:-"deploy"}
    
    case $action in
        "check")
            check_prerequisites
            ;;
        "setup")
            check_prerequisites
            setup_terraform
            ;;
        "deploy")
            check_prerequisites
            setup_terraform
            deploy_infrastructure
            show_deployment_info
            ;;
        "generate")
            run_data_generation
            ;;
        "info")
            show_deployment_info
            ;;
        "help"|"-h"|"--help")
            echo "Usage: $0 [action]"
            echo ""
            echo "Actions:"
            echo "  check     - Check prerequisites only"
            echo "  setup     - Setup Terraform configuration"
            echo "  deploy    - Deploy infrastructure (default)"
            echo "  generate  - Run data generation on deployed infrastructure"
            echo "  info      - Show deployment information"
            echo "  help      - Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Deploy infrastructure"
            echo "  $0 deploy             # Deploy infrastructure"
            echo "  $0 generate           # Generate TPC-H data"
            echo "  $0 info               # Show deployment info"
            ;;
        *)
            log $RED "Unknown action: $action"
            log $YELLOW "Use '$0 help' to see available actions"
            exit 1
            ;;
    esac
}

# Check if script is being run from the correct directory
if [ ! -d "infrastructure" ]; then
    log $RED "ERROR: This script must be run from the tpch-datagen directory"
    log $YELLOW "Please cd to the tpch-datagen directory and run ./deploy.sh"
    exit 1
fi

# Run main function
main "$@"
