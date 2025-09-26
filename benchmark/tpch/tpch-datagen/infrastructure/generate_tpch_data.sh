#!/bin/bash

set -e

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check available disk space
check_disk_space() {
    local required_gb=$1
    local available_gb=$(df /home/ec2-user | tail -1 | awk '{print int($4/1024/1024)}')
    
    log "Available disk space: ${available_gb}GB, Required: ${required_gb}GB"
    
    if [ $available_gb -lt $required_gb ]; then
        log "ERROR: Insufficient disk space. Available: ${available_gb}GB, Required: ${required_gb}GB"
        exit 1
    fi
}

# Function to estimate required disk space for a scale factor
estimate_disk_space() {
    local sf=$1
    # Rough estimates based on TPC-H specification:
    # SF=1: ~1GB, SF=10: ~10GB, SF=100: ~100GB, SF=1000: ~1TB
    # Add 50% buffer for temporary files and compression
    echo $(( sf * 2 ))
}

# Function to generate TPC-H data for a specific scale factor
generate_tpch_sf() {
    local sf=$1
    local s3_bucket=$2
    local s3_prefix=$3
    local work_dir="/home/ec2-user/tpch-data/sf${sf}"
    
    log "Starting TPC-H data generation for scale factor ${sf}"
    
    # Estimate and check disk space
    local required_space=$(estimate_disk_space $sf)
    check_disk_space $required_space
    
    # Create working directory
    mkdir -p "$work_dir"
    cd "$work_dir"
    
    # Generate TPC-H data using DuckDB
    log "Generating TPC-H data with scale factor ${sf}..."
    
    # Create DuckDB script for data generation
    cat > generate_sf${sf}.sql << EOF
-- Install and load TPC-H extension
INSTALL tpch;
LOAD tpch;

-- Generate TPC-H data
CALL dbgen(sf=${sf});

-- Export each table to Parquet format
EXPORT DATABASE 'tpch_sf${sf}_data' (FORMAT parquet);

-- Show table sizes for verification
SELECT 
    table_name,
    estimated_size,
    column_count,
    estimated_size / 1024 / 1024 as size_mb
FROM duckdb_tables() 
WHERE schema_name = 'main' 
    AND table_name IN ('customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier')
ORDER BY estimated_size DESC;
EOF

    # Run DuckDB data generation
    log "Executing DuckDB data generation..."
    time duckdb tpch_sf${sf}.db < generate_sf${sf}.sql
    
    # Verify generated files
    log "Verifying generated Parquet files..."
    if [ ! -d "tpch_sf${sf}_data" ]; then
        log "ERROR: Data directory not created"
        exit 1
    fi
    
    # List generated files with sizes
    log "Generated files:"
    ls -lh tpch_sf${sf}_data/
    
    # Calculate total size
    local total_size=$(du -sh tpch_sf${sf}_data/ | cut -f1)
    log "Total data size for SF=${sf}: ${total_size}"
    
    # Upload to S3
    local s3_path="s3://${s3_bucket}/"
    if [ -n "$s3_prefix" ]; then
        s3_path="${s3_path}${s3_prefix}/"
    fi
    s3_path="${s3_path}sf_${sf}/"

    log "Uploading TPC-H SF=${sf} data to S3 path: ${s3_path}"
    aws s3 sync tpch_sf${sf}_data/ ${s3_path} \
        --storage-class STANDARD_IA \
        --metadata "scale_factor=${sf},generated_date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    # Verify S3 upload
    log "Verifying S3 upload..."
    aws s3 ls ${s3_path} --recursive --human-readable --summarize
    
    # Clean up local files to save space
    log "Cleaning up local files for SF=${sf}..."
    rm -rf tpch_sf${sf}_data/
    rm -f tpch_sf${sf}.db
    
    log "Completed TPC-H data generation for scale factor ${sf}"
}

# Main script
main() {
    local scale_factors=("$@")
    local s3_bucket="${scale_factors[-2]}"  # Second to last argument is S3 bucket
    local s3_prefix="${scale_factors[-1]}"  # Last argument is S3 prefix
    unset scale_factors[-1]  # Remove S3 prefix from scale factors array
    unset scale_factors[-1]  # Remove S3 bucket from scale factors array

    if [ ${#scale_factors[@]} -eq 0 ]; then
        log "Usage: $0 <scale_factor1> [scale_factor2] ... <s3_bucket> <s3_prefix>"
        log "Example: $0 100 1000 my-tpch-bucket benchmarks/tpch"
        log "Example: $0 100 1000 my-tpch-bucket \"\" (empty prefix)"
        exit 1
    fi
    
    log "========================================="
    log "TPC-H Data Generation Started"
    log "Scale factors: ${scale_factors[*]}"
    log "S3 bucket: ${s3_bucket}"
    log "S3 prefix: ${s3_prefix:-"(none)"}"
    log "Instance type: $(curl -s http://169.254.169.254/latest/meta-data/instance-type)"
    log "Instance ID: $(curl -s http://169.254.169.254/latest/meta-data/instance-id)"
    log "========================================="
    
    # Verify AWS credentials
    log "Verifying AWS credentials..."
    aws sts get-caller-identity
    
    # Verify S3 bucket access
    log "Verifying S3 bucket access..."
    aws s3 ls s3://${s3_bucket}/ || {
        log "ERROR: Cannot access S3 bucket ${s3_bucket}"
        exit 1
    }
    
    # Generate data for each scale factor
    local start_time=$(date +%s)
    
    for sf in "${scale_factors[@]}"; do
        local sf_start_time=$(date +%s)

        generate_tpch_sf "$sf" "$s3_bucket" "$s3_prefix"

        local sf_end_time=$(date +%s)
        local sf_duration=$((sf_end_time - sf_start_time))
        log "Scale factor ${sf} completed in ${sf_duration} seconds"
    done
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    log "========================================="
    log "TPC-H Data Generation Completed"
    log "Total time: ${total_duration} seconds"
    log "Generated scale factors: ${scale_factors[*]}"
    local final_s3_path="s3://${s3_bucket}/"
    if [ -n "$s3_prefix" ]; then
        final_s3_path="${final_s3_path}${s3_prefix}/"
    fi
    log "Data uploaded to: ${final_s3_path}"
    log "========================================="

    # Show final S3 bucket contents
    log "Final S3 bucket contents:"
    aws s3 ls ${final_s3_path} --recursive --human-readable --summarize
}

# Run main function with all arguments
main "$@"
