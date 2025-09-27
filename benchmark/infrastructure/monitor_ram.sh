#!/bin/bash

# Simple system and Embucket container monitoring script for EC2 instance

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
REFRESH_INTERVAL=1
CONTINUOUS=true
DURATION=""

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Monitor system RAM, CPU, and Embucket container on EC2 instance"
    echo ""
    echo "Options:"
    echo "  -i, --interval SECONDS    Refresh interval in seconds (default: 1)"
    echo "  -t, --duration SECONDS    Run for specified duration then exit"
    echo "  -o, --once                Run once and exit (don't loop)"
    echo "  -h, --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                        # Real-time monitoring"
    echo "  $0 -i 5                  # Monitor every 5 seconds"
    echo "  $0 -t 60                 # Monitor for 60 seconds"
    echo "  $0 -o                    # Single snapshot"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--interval)
            REFRESH_INTERVAL="$2"
            shift 2
            ;;
        -o|--once)
            CONTINUOUS=false
            shift
            ;;
        -t|--duration)
            DURATION="$2"
            CONTINUOUS=false
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate refresh interval
if ! [[ "$REFRESH_INTERVAL" =~ ^[0-9]+$ ]] || [ "$REFRESH_INTERVAL" -lt 1 ]; then
    echo -e "${RED}❌ Invalid refresh interval. Must be a positive integer.${NC}"
    exit 1
fi

# Get instance information from Terraform
echo -e "${YELLOW}🔍 Getting instance information from Terraform...${NC}"

INSTANCE_IP=$(terraform output -raw instance_public_ip 2>/dev/null || echo "")
SSH_COMMAND=$(terraform output -raw ssh_command 2>/dev/null || echo "")

if [ -z "$INSTANCE_IP" ]; then
    echo -e "${RED}❌ Could not get instance IP from Terraform output${NC}"
    echo "Make sure you've run 'terraform apply' successfully and you're in the infrastructure directory"
    exit 1
fi

if [ -z "$SSH_COMMAND" ]; then
    echo -e "${YELLOW}⚠️ Could not get SSH command from Terraform, using default${NC}"
    SSH_COMMAND="ssh -i ~/.ssh/id_rsa ec2-user@$INSTANCE_IP"
fi

echo -e "${GREEN}✅ Instance IP: $INSTANCE_IP${NC}"

# Test SSH connectivity
echo -e "${YELLOW}🔍 Testing SSH connectivity...${NC}"
if ! $SSH_COMMAND "echo 'SSH connection successful'" >/dev/null 2>&1; then
    echo -e "${RED}❌ Cannot connect to instance via SSH${NC}"
    echo "Please check:"
    echo "  1. Instance is running"
    echo "  2. SSH key is correct and accessible"
    echo "  3. Security group allows SSH access"
    exit 1
fi
echo -e "${GREEN}✅ SSH connection successful${NC}"

# Function to get system stats
get_system_stats() {
    $SSH_COMMAND "
        # Get memory stats
        MEM_PERCENT=\$(free | awk 'NR==2{printf \"%.1f\", \$3*100/\$2}')
        MEM_USED=\$(free -h | awk 'NR==2{print \$3}')
        MEM_TOTAL=\$(free -h | awk 'NR==2{print \$2}')

        # Get CPU usage (1 second average) and core count
        CPU_PERCENT=\$(top -bn1 | grep 'Cpu(s)' | awk '{print \$2}' | sed 's/%us,//')
        CPU_CORES=\$(nproc)

        echo \"\$MEM_PERCENT|\$MEM_USED|\$MEM_TOTAL|\$CPU_PERCENT|\$CPU_CORES\"
    " 2>/dev/null
}

# Function to get Embucket container stats
get_embucket_stats() {
    $SSH_COMMAND "
        if command -v docker >/dev/null 2>&1; then
            # Get Embucket container stats
            CONTAINER_STATS=\$(docker stats --no-stream --format '{{.MemUsage}}|{{.MemPerc}}|{{.CPUPerc}}' embucket-benchmark 2>/dev/null || echo 'N/A|N/A|N/A')
            echo \"\$CONTAINER_STATS\"
        else
            echo 'N/A|N/A|N/A'
        fi
    " 2>/dev/null
}

# Function to create progress bar
create_progress_bar() {
    local percent=$1
    local bar_width=30
    local filled=$(echo "$percent" | awk -v w=$bar_width '{print int($1*w/100)}')
    local empty=$((bar_width - filled))

    local bar=""
    for ((i=0; i<filled; i++)); do bar+="█"; done
    for ((i=0; i<empty; i++)); do bar+="░"; done
    echo "$bar"
}

# Function to get color based on usage
get_usage_color() {
    local percent=$1
    local int_percent=$(echo "$percent" | cut -d'.' -f1)

    if [ "$int_percent" -gt 80 ]; then
        echo "$RED"
    elif [ "$int_percent" -gt 60 ]; then
        echo "$YELLOW"
    else
        echo "$GREEN"
    fi
}

# Function to show monitoring lines
show_monitoring_lines() {
    local timestamp=$(date '+%H:%M:%S')
    local system_stats=$(get_system_stats)
    local embucket_stats=$(get_embucket_stats)

    # Parse system stats
    local mem_percent=$(echo "$system_stats" | cut -d'|' -f1)
    local mem_used=$(echo "$system_stats" | cut -d'|' -f2)
    local mem_total=$(echo "$system_stats" | cut -d'|' -f3)
    local cpu_percent=$(echo "$system_stats" | cut -d'|' -f4)
    local cpu_cores=$(echo "$system_stats" | cut -d'|' -f5)

    # Parse Embucket stats
    local emb_mem_usage=$(echo "$embucket_stats" | cut -d'|' -f1)
    local emb_mem_percent=$(echo "$embucket_stats" | cut -d'|' -f2 | sed 's/%//')
    local emb_cpu_percent=$(echo "$embucket_stats" | cut -d'|' -f3 | sed 's/%//')

    # Create progress bars
    local mem_bar=$(create_progress_bar "$mem_percent")
    local cpu_bar=$(create_progress_bar "$cpu_percent")

    # Get colors
    local mem_color=$(get_usage_color "$mem_percent")
    local cpu_color=$(get_usage_color "$cpu_percent")
    local emb_mem_color=$(get_usage_color "$emb_mem_percent")
    local emb_cpu_color=$(get_usage_color "$emb_cpu_percent")

    # Move cursor up 3 lines if not first run, then print 3 lines
    if [ "$FIRST_RUN" != "true" ]; then
        printf "\033[3A"
    fi

    # System RAM line
    printf "${CYAN}[${timestamp}] System RAM:${NC} ${mem_color}${mem_bar}${NC} ${mem_color}${mem_percent}%%${NC} (${mem_used}/${mem_total})                    \n"

    # System CPU line
    printf "${CYAN}[${timestamp}] System CPU:${NC} ${cpu_color}${cpu_bar}${NC} ${cpu_color}${cpu_percent}%%${NC} (${cpu_cores} cores)                           \n"

    # Embucket container line
    if [ "$emb_mem_usage" != "N/A" ]; then
        printf "${CYAN}[${timestamp}] Embucket:  ${NC} RAM: ${emb_mem_color}${emb_mem_percent}%% (${emb_mem_usage})${NC} CPU: ${emb_cpu_color}${emb_cpu_percent}%%${NC} (can exceed 100%%)        \n"
    else
        printf "${CYAN}[${timestamp}] Embucket:  ${NC} ${RED}Container not found or not accessible${NC}                                \n"
    fi

    FIRST_RUN="false"
}

# Function to handle cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}🛑 Monitoring stopped${NC}"
    exit 0
}

# Set up signal handlers for graceful exit
trap cleanup SIGINT SIGTERM

# Initialize first run flag
FIRST_RUN="true"

# Main monitoring loop
echo -e "${GREEN}🚀 Starting system and Embucket monitoring...${NC}"
echo -e "${YELLOW}💡 Press Ctrl+C to stop | Updates every ${REFRESH_INTERVAL}s${NC}"
echo

if [ "$CONTINUOUS" = true ]; then
    # Reserve space for 3 lines
    echo
    echo
    echo

    # Continuous monitoring
    while true; do
        show_monitoring_lines
        sleep "$REFRESH_INTERVAL"
    done
elif [ -n "$DURATION" ]; then
    # Monitor for specified duration
    echo -e "${YELLOW}⏱️ Monitoring for $DURATION seconds...${NC}"
    echo
    echo
    echo

    END_TIME=$(($(date +%s) + DURATION))

    while [ $(date +%s) -lt $END_TIME ]; do
        show_monitoring_lines
        REMAINING=$((END_TIME - $(date +%s)))
        if [ $REMAINING -gt 0 ]; then
            sleep "$REFRESH_INTERVAL"
        fi
    done

    echo
    echo -e "${GREEN}✅ Monitoring duration completed${NC}"
else
    # Single run
    echo -e "${YELLOW}📊 System and Embucket snapshot for $INSTANCE_IP at $(date)${NC}"
    echo
    echo
    echo
    show_monitoring_lines
    echo
    echo -e "${GREEN}✅ Monitoring complete${NC}"
fi
