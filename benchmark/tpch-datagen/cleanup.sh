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

# Function to show S3 bucket contents before cleanup
show_s3_contents() {
    log $BLUE "Checking S3 bucket contents before cleanup..."
    
    cd infrastructure
    
    local bucket_name=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
    
    if [ -n "$bucket_name" ]; then
        log $BLUE "S3 Bucket: $bucket_name"
        echo ""
        
        if aws s3 ls s3://$bucket_name/ --profile AdministratorAccess-767397688925 &>/dev/null; then
            log $GREEN "Generated TPC-H data in S3:"
            aws s3 ls s3://$bucket_name/ --recursive --human-readable --summarize --profile AdministratorAccess-767397688925
            echo ""
            
            log $YELLOW "This data will remain in S3 after infrastructure cleanup."
            log $YELLOW "To delete S3 data as well, use the --delete-s3-data flag."
        else
            log $YELLOW "S3 bucket is empty or inaccessible."
        fi
    else
        log $YELLOW "Could not determine S3 bucket name."
    fi
    
    cd ..
}

# Function to cleanup S3 data
cleanup_s3_data() {
    log $BLUE "Cleaning up S3 data..."
    
    cd infrastructure
    
    local bucket_name=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
    
    if [ -n "$bucket_name" ]; then
        log $YELLOW "Deleting all data from S3 bucket: $bucket_name"
        
        # Delete all objects in the bucket
        aws s3 rm s3://$bucket_name/ --recursive --profile AdministratorAccess-767397688925
        
        log $GREEN "S3 data cleanup completed."
    else
        log $YELLOW "Could not determine S3 bucket name for cleanup."
    fi
    
    cd ..
}

# Function to cleanup infrastructure
cleanup_infrastructure() {
    log $BLUE "Cleaning up TPC-H data generation infrastructure..."
    
    cd infrastructure
    
    # Check if Terraform state exists
    if [ ! -f terraform.tfstate ]; then
        log $YELLOW "No Terraform state found. Infrastructure may already be cleaned up."
        cd ..
        return
    fi
    
    # Show what will be destroyed
    log $BLUE "Planning infrastructure destruction..."
    terraform plan -destroy
    
    echo ""
    log $YELLOW "This will destroy all AWS resources created for TPC-H data generation."
    log $YELLOW "The S3 data will be preserved unless you used --delete-s3-data flag."
    log $YELLOW ""
    log $YELLOW "Do you want to proceed with infrastructure cleanup? (y/N)"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        log $BLUE "Destroying infrastructure..."
        terraform destroy -auto-approve
        
        log $GREEN "Infrastructure cleanup completed!"
        
        # Clean up local Terraform files
        log $BLUE "Cleaning up local Terraform files..."
        rm -f terraform.tfstate*
        rm -f tfplan
        rm -rf .terraform/
        
        log $GREEN "Local cleanup completed!"
    else
        log $YELLOW "Infrastructure cleanup cancelled."
    fi
    
    cd ..
}

# Function to show cleanup summary
show_cleanup_summary() {
    log $BLUE "Cleanup Summary:"
    echo ""
    
    cd infrastructure
    
    # Check if infrastructure still exists
    if [ -f terraform.tfstate ]; then
        log $YELLOW "Infrastructure: Still exists (cleanup was cancelled or failed)"
    else
        log $GREEN "Infrastructure: Cleaned up successfully"
    fi
    
    # Check S3 bucket
    local bucket_name=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
    if [ -n "$bucket_name" ]; then
        if aws s3 ls s3://$bucket_name/ --profile AdministratorAccess-767397688925 &>/dev/null; then
            local object_count=$(aws s3 ls s3://$bucket_name/ --recursive --profile AdministratorAccess-767397688925 | wc -l)
            if [ $object_count -gt 0 ]; then
                log $GREEN "S3 Data: Preserved ($object_count objects in s3://$bucket_name/)"
            else
                log $YELLOW "S3 Data: Bucket exists but is empty"
            fi
        else
            log $GREEN "S3 Data: Bucket cleaned up"
        fi
    fi
    
    cd ..
    
    echo ""
    log $BLUE "Next Steps:"
    log $BLUE "- Your TPC-H data is available in S3 for benchmarking"
    log $BLUE "- Use the S3 paths in your benchmark configurations"
    log $BLUE "- Remember to clean up S3 data when no longer needed"
}

# Function to force cleanup everything
force_cleanup() {
    log $YELLOW "Force cleanup mode: This will delete EVERYTHING including S3 data!"
    log $YELLOW "Are you absolutely sure? Type 'DELETE EVERYTHING' to confirm:"
    read -r confirmation
    
    if [ "$confirmation" = "DELETE EVERYTHING" ]; then
        log $RED "Proceeding with force cleanup..."
        cleanup_s3_data
        
        cd infrastructure
        if [ -f terraform.tfstate ]; then
            terraform destroy -auto-approve
        fi
        rm -f terraform.tfstate*
        rm -f tfplan
        rm -rf .terraform/
        cd ..
        
        log $GREEN "Force cleanup completed. All resources deleted."
    else
        log $YELLOW "Force cleanup cancelled."
    fi
}

# Main function
main() {
    local delete_s3_data=false
    local force_cleanup=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --delete-s3-data)
                delete_s3_data=true
                shift
                ;;
            --force)
                force_cleanup=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [options]"
                echo ""
                echo "Options:"
                echo "  --delete-s3-data    Also delete generated TPC-H data from S3"
                echo "  --force             Force cleanup of everything without confirmation"
                echo "  --help, -h          Show this help message"
                echo ""
                echo "Examples:"
                echo "  $0                      # Cleanup infrastructure, preserve S3 data"
                echo "  $0 --delete-s3-data     # Cleanup infrastructure and S3 data"
                echo "  $0 --force              # Force cleanup everything"
                exit 0
                ;;
            *)
                log $RED "Unknown option: $1"
                log $YELLOW "Use '$0 --help' to see available options"
                exit 1
                ;;
        esac
    done
    
    # Check if script is being run from the correct directory
    if [ ! -d "infrastructure" ]; then
        log $RED "ERROR: This script must be run from the tpch-datagen directory"
        log $YELLOW "Please cd to the tpch-datagen directory and run ./cleanup.sh"
        exit 1
    fi
    
    log $BLUE "========================================="
    log $BLUE "TPC-H Data Generation Cleanup"
    log $BLUE "========================================="
    
    if [ "$force_cleanup" = true ]; then
        force_cleanup
    else
        show_s3_contents
        
        if [ "$delete_s3_data" = true ]; then
            cleanup_s3_data
        fi
        
        cleanup_infrastructure
        show_cleanup_summary
    fi
    
    log $BLUE "========================================="
    log $GREEN "Cleanup process completed!"
    log $BLUE "========================================="
}

# Run main function
main "$@"
