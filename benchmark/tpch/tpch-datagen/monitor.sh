#!/bin/bash

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

# Function to get instance information
get_instance_info() {
    cd infrastructure
    
    if [ ! -f terraform.tfstate ]; then
        log $RED "No Terraform state found. Infrastructure may not be deployed."
        exit 1
    fi
    
    local instance_ip=$(terraform output -raw instance_public_ip 2>/dev/null)
    local instance_id=$(terraform output -raw instance_id 2>/dev/null)
    local s3_bucket=$(terraform output -raw s3_bucket_name 2>/dev/null)
    
    cd ..
    
    echo "$instance_ip,$instance_id,$s3_bucket"
}

# Function to check instance status
check_instance_status() {
    local instance_info=$(get_instance_info)
    local instance_ip=$(echo $instance_info | cut -d',' -f1)
    local instance_id=$(echo $instance_info | cut -d',' -f2)
    
    log $BLUE "Instance Status:"
    echo "  Instance ID: $instance_id"
    echo "  Public IP: $instance_ip"
    
    # Check AWS instance status
    local instance_state=$(aws ec2 describe-instances --instance-ids $instance_id --query 'Reservations[0].Instances[0].State.Name' --output text --profile AdministratorAccess-767397688925 2>/dev/null || echo "unknown")
    echo "  AWS State: $instance_state"
    
    # Check SSH connectivity
    if ssh -i ~/.ssh/id_rsa -o ConnectTimeout=5 -o StrictHostKeyChecking=no ec2-user@$instance_ip "echo 'SSH OK'" &>/dev/null; then
        echo "  SSH Status: ✅ Connected"
    else
        echo "  SSH Status: ❌ Not accessible"
    fi
}

# Function to monitor system resources
monitor_system_resources() {
    local instance_info=$(get_instance_info)
    local instance_ip=$(echo $instance_info | cut -d',' -f1)
    
    log $BLUE "System Resources:"
    
    ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no ec2-user@$instance_ip << 'EOF'
# CPU and Memory
echo "CPU and Memory Usage:"
top -bn1 | grep "Cpu(s)" | awk '{print "  CPU: " $2 " user, " $4 " system, " $8 " idle"}'
free -h | grep "Mem:" | awk '{print "  Memory: " $3 " used / " $2 " total (" int($3/$2*100) "% used)"}'

# Disk Usage
echo ""
echo "Disk Usage:"
df -h | grep -E "^/dev/" | awk '{print "  " $1 ": " $3 " used / " $2 " total (" $5 " used)"}'

# Load Average
echo ""
echo "Load Average:"
uptime | awk -F'load average:' '{print "  " $2}'

# Process Information
echo ""
echo "TPC-H Related Processes:"
ps aux | grep -E "(duckdb|generate_tpch)" | grep -v grep | awk '{print "  PID " $2 ": " $11 " " $12 " " $13}'
EOF
}

# Function to monitor data generation progress
monitor_data_generation() {
    local instance_info=$(get_instance_info)
    local instance_ip=$(echo $instance_info | cut -d',' -f1)
    
    log $BLUE "Data Generation Progress:"
    
    ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no ec2-user@$instance_ip << 'EOF'
# Check if generation is running
if pgrep -f "generate_tpch_data.sh" > /dev/null; then
    echo "  Status: ✅ Data generation is running"
else
    echo "  Status: ⏸️  Data generation is not running"
fi

# Check working directories
echo ""
echo "Working Directories:"
if [ -d ~/tpch-data ]; then
    for dir in ~/tpch-data/sf*; do
        if [ -d "$dir" ]; then
            local sf=$(basename "$dir")
            local size=$(du -sh "$dir" 2>/dev/null | cut -f1)
            echo "  $sf: $size"
        fi
    done
else
    echo "  No working directories found"
fi

# Check for log files
echo ""
echo "Recent Log Activity:"
find ~/tpch-data -name "*.log" -type f 2>/dev/null | head -3 | while read logfile; do
    if [ -f "$logfile" ]; then
        local last_line=$(tail -1 "$logfile" 2>/dev/null)
        echo "  $(basename "$logfile"): $last_line"
    fi
done
EOF
}

# Function to monitor S3 upload progress
monitor_s3_progress() {
    local instance_info=$(get_instance_info)
    local s3_bucket=$(echo $instance_info | cut -d',' -f3)
    
    log $BLUE "S3 Upload Progress:"
    echo "  Bucket: $s3_bucket"
    
    if aws s3 ls s3://$s3_bucket/ --profile AdministratorAccess-767397688925 &>/dev/null; then
        echo ""
        echo "  Current S3 Contents:"
        aws s3 ls s3://$s3_bucket/ --recursive --human-readable --summarize --profile AdministratorAccess-767397688925 | tail -10
    else
        echo "  S3 bucket is empty or inaccessible"
    fi
}

# Function to show recent logs
show_recent_logs() {
    local instance_info=$(get_instance_info)
    local instance_ip=$(echo $instance_info | cut -d',' -f1)
    local lines=${1:-50}
    
    log $BLUE "Recent Logs (last $lines lines):"
    
    ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no ec2-user@$instance_ip << EOF
# Show cloud-init logs
echo "=== Cloud-init logs ==="
sudo tail -$lines /var/log/cloud-init-output.log 2>/dev/null || echo "No cloud-init logs found"

echo ""
echo "=== Data generation logs ==="
find ~/tpch-data -name "*.log" -type f 2>/dev/null | head -1 | while read logfile; do
    if [ -f "\$logfile" ]; then
        tail -$lines "\$logfile"
    else
        echo "No data generation logs found"
    fi
done
EOF
}

# Function to run interactive monitoring
interactive_monitor() {
    log $BLUE "Starting interactive monitoring (press Ctrl+C to exit)..."
    
    while true; do
        clear
        log $GREEN "========================================="
        log $GREEN "TPC-H Data Generation Monitor"
        log $GREEN "$(date)"
        log $GREEN "========================================="
        
        check_instance_status
        echo ""
        monitor_system_resources
        echo ""
        monitor_data_generation
        echo ""
        monitor_s3_progress
        
        echo ""
        log $YELLOW "Refreshing in 30 seconds... (Ctrl+C to exit)"
        sleep 30
    done
}

# Main function
main() {
    local action=${1:-"status"}
    
    case $action in
        "status")
            check_instance_status
            ;;
        "resources"|"system")
            monitor_system_resources
            ;;
        "progress"|"generation")
            monitor_data_generation
            ;;
        "s3")
            monitor_s3_progress
            ;;
        "logs")
            local lines=${2:-50}
            show_recent_logs $lines
            ;;
        "all")
            check_instance_status
            echo ""
            monitor_system_resources
            echo ""
            monitor_data_generation
            echo ""
            monitor_s3_progress
            ;;
        "watch"|"interactive")
            interactive_monitor
            ;;
        "help"|"-h"|"--help")
            echo "Usage: $0 [action] [options]"
            echo ""
            echo "Actions:"
            echo "  status      - Show instance status (default)"
            echo "  resources   - Show system resource usage"
            echo "  progress    - Show data generation progress"
            echo "  s3          - Show S3 upload progress"
            echo "  logs [N]    - Show recent logs (default: 50 lines)"
            echo "  all         - Show all monitoring information"
            echo "  watch       - Interactive monitoring with auto-refresh"
            echo "  help        - Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Show instance status"
            echo "  $0 all                # Show all monitoring info"
            echo "  $0 logs 100           # Show last 100 log lines"
            echo "  $0 watch              # Interactive monitoring"
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
    log $YELLOW "Please cd to the tpch-datagen directory and run ./monitor.sh"
    exit 1
fi

# Run main function
main "$@"
