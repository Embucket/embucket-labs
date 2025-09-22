## Overview

This benchmark tool executes queries derived from TPC-H against both Snowflake and Embucket with cache-clearing operations to ensure clean, cache-free performance measurements. For Snowflake, it uses warehouse suspend/resume operations. For Embucket, it restarts the Docker container before each query to eliminate internal caching. It provides detailed timing metrics including compilation time, execution time, and total elapsed time.

## TPC Legal Considerations

It is important to know that TPC benchmarks are copyrighted IP of the Transaction Processing Council. Only members of the TPC consortium are allowed to publish TPC benchmark results. Fun fact: only four companies have published official TPC-DS benchmark results so far, and those results can be seen [here](https://www.tpc.org/tpcds/results/tpcds_results5.asp?orderby=dbms&version=3).

However, anyone is welcome to create derivative benchmarks under the TPC's fair use policy, and that is what we are doing here. We do not aim to run a true TPC benchmark (which is a significant endeavor). We are just running the individual queries and recording the timings.

Throughout this document and when talking about these benchmarks, you will see the term "derived from TPC-DS". We are required to use this terminology and this is explained in the [fair-use policy (PDF)](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf).

**This benchmark is a Non-TPC Benchmark. Any comparison between official TPC Results with non-TPC workloads is prohibited by the TPC.**

## Features

- **Cache Isolation**:
  - **Snowflake**: Suspends and resumes warehouse before each query
  - **Embucket**: Restarts Docker container before each query to clear internal cache
- **Result Cache Disabled**: Ensures no result caching affects benchmark results
- **Comprehensive Metrics**: Tracks compilation time, execution time, and row counts
- **CSV Export**: Saves results to CSV files for further analysis
- **Error Handling**: Graceful handling of warehouse operations and query failures
- **Dual Platform Support**: Benchmarks both Snowflake and Embucket with appropriate cache-clearing strategies

## Setup

### 1. Create Virtual Environment
```bash
python -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Connections
Create a `.env` file with your credentials:

**For Snowflake:**
```bash
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
```

**For Embucket (when using infrastructure):**
```bash
EMBUCKET_HOST=your_ec2_instance_ip
EMBUCKET_PORT=3000
SSH_KEY_PATH=~/.ssh/id_rsa
```

## Usage

Run the benchmark:
```bash
python benchmark.py
```

The benchmark will:
1. Connect to the configured platform (Snowflake and/or Embucket)
2. Execute each query derived from TPC-H with cache-clearing operations:
   - **Snowflake**: Warehouse suspend/resume before each query
   - **Embucket**: Docker container restart before each query
3. Collect performance metrics from query history
4. Display results and comparisons
5. Save detailed results to CSV files

## Embucket Container Restart Functionality

For Embucket benchmarks, the system automatically restarts the Docker container before each query to eliminate internal caching and ensure accurate performance measurements.

**How it works:**
- Before each query execution, the benchmark connects to the EC2 instance via SSH
- Stops the Embucket Docker container: `docker-compose stop embucket`
- Starts the container: `docker-compose start embucket`
- Waits for the health check to pass (~30-60 seconds)
- Creates a fresh database connection and executes the query

**Requirements:**
- `EMBUCKET_HOST` set to your EC2 instance IP
- `SSH_KEY_PATH` pointing to your private key (default: `~/.ssh/id_rsa`)
- SSH access to the EC2 instance running Embucket

**Performance Impact:**
- Adds ~30-60 seconds per query for container restart
- Significantly increases total benchmark time
- Provides cache-free, accurate performance measurements

## Output

The benchmark provides:
- **Console Output**: Formatted table with timing metrics for each query
- **CSV Files**: Separate result files for Snowflake and Embucket with detailed analysis
- **Comparison Charts**: Visual comparisons between platforms when both are run
- **Total Times**: Aggregated compilation and execution times

## Files

- `benchmark.py` - Main benchmark script with restart functionality
- `docker_manager.py` - Docker container management for Embucket restarts
- `utils.py` - Connection utilities for Snowflake and Embucket
- `tpch_queries.py` - Query definitions derived from TPC-H
- `calculate_average.py` - Result averaging and analysis
- `requirements.txt` - Python dependencies
- `infrastructure/` - Terraform infrastructure for EC2/Embucket deployment

## Requirements

- Python 3.8+
- **For Snowflake**: Account with appropriate permissions and warehouse with suspend/resume capabilities
- **For Embucket**: EC2 instance with Docker Compose and SSH access for container restarts