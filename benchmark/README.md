## Overview

This benchmark tool executes queries from multiple benchmark suites (TPC-H, ClickBench, TPC-DS) against both Snowflake and Embucket with cache-clearing operations to ensure clean, cache-free performance measurements. For Snowflake, it uses warehouse suspend/resume operations. For Embucket, it restarts the Docker container before each query to eliminate internal caching. It provides detailed timing metrics including compilation time, execution time, and total elapsed time.

## TPC Legal Considerations

It is important to know that TPC benchmarks are copyrighted IP of the Transaction Processing Council. Only members of the TPC consortium are allowed to publish TPC benchmark results. Fun fact: only four companies have published official TPC-H benchmark results so far, and those results can be seen [here](https://www.tpc.org/tpch/results/tpch_results5.asp).

However, anyone is welcome to create derivative benchmarks under the TPC's fair use policy, and that is what we are doing here. We do not aim to run a true TPC benchmark (which is a significant endeavor). We are just running the individual queries and recording the timings.

Throughout this document and when talking about these benchmarks, you will see the term "derived from TPC-H". We are required to use this terminology and this is explained in the [fair-use policy (PDF)](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf).

**This benchmark is a Non-TPC Benchmark. Any comparison between official TPC Results with non-TPC workloads is prohibited by the TPC.**

## Features

- **Multiple Benchmark Types**: Supports TPC-H, ClickBench, and TPC-DS benchmark suites
- **Cache Isolation**:
  - **Snowflake**: Suspends and resumes warehouse before each query
  - **Embucket**: Restarts Docker container before each query to clear internal cache
- **Flexible Caching Options**: Can run with or without cache clearing (`--no-cache` flag)
- **Command Line Interface**: Full CLI support for system selection, benchmark type, and run configuration
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
Create a `.env` file with your credentials (see `env_example` for reference):

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
EMBUCKET_PROTOCOL=http
EMBUCKET_USER=embucket
EMBUCKET_PASSWORD=embucket
EMBUCKET_ACCOUNT=embucket
EMBUCKET_DATABASE=benchmark_database
EMBUCKET_SCHEMA=benchmark_schema
EMBUCKET_INSTANCE=your_instance_name
SSH_KEY_PATH=~/.ssh/id_rsa
```

**Benchmark Configuration:**
```bash
BENCHMARK_TYPE=tpch  # Options: tpch, clickbench, tpcds
DATASET_S3_BUCKET=embucket-testdata
DATASET_PATH=tpch/01  # Path within S3 bucket
SNOWFLAKE_WAREHOUSE_SIZE=XSMALL
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```

## Usage

### Command Line Interface

The benchmark supports comprehensive command-line options:

```bash
# Run both Snowflake and Embucket with TPC-H (default)
python benchmark.py

# Run only Embucket with TPC-H
python benchmark.py --system embucket

# Run only Snowflake with TPC-H
python benchmark.py --system snowflake

# Run ClickBench on both systems
python benchmark.py --benchmark-type clickbench

# Run TPC-DS on Embucket only
python benchmark.py --system embucket --benchmark-type tpcds

# Run with caching enabled (no container restarts/warehouse suspends)
python benchmark.py --system embucket

# Run with caching disabled (force cache clearing)
python benchmark.py --system embucket --no-cache

# Custom number of runs and dataset path
python benchmark.py --runs 5 --dataset-path tpch/100
```

### Command Line Arguments

- `--system`: Choose platform (`snowflake`, `embucket`, `both`) - default: `both`
- `--runs`: Number of benchmark runs - default: `3`
- `--benchmark-type`: Benchmark suite (`tpch`, `clickbench`, `tpcds`) - default: `tpch`
- `--dataset-path`: Override DATASET_PATH environment variable
- `--cold-runs`: Force cache clearing (warehouse suspend for Snowflake, container restart for Embucket)
- `--disable-result-cache`: Disable Snowflake's result cache only (USE_CACHED_RESULT=FALSE), no effect on Embucket

## Caching Configurations

### Snowflake Caching Options

- **Cold run**: `--cold-runs`
  - Suspends warehouse between queries
  - Automatically disables result cache
  - Results stored in `cold/` folder

- **Warm run with result cache**: *(default, no flags)*
  - Keeps warehouse active between queries
  - Enables result cache (USE_CACHED_RESULT=TRUE)
  - Results stored in `warm/` folder

- **Warm run without result cache**: `--disable-result-cache`
  - Keeps warehouse active between queries
  - Disables result cache (USE_CACHED_RESULT=FALSE)
  - Results stored in `warm_no_result_cache/` folder

### Embucket Caching Options

- **Cold run**: `--cold-runs`
  - Restarts container between queries
  - Results stored in `cold/` folder

- **Warm run**: *(default, no flags)*
  - Keeps container running between queries
  - Results stored in `warm/` folder

### Example Usage

```bash
# Default: warm run (caching enabled) for both systems
python benchmark.py

# Cold run (cache clearing) for both systems
python benchmark.py --cold-runs

# Warm run with result cache disabled for Snowflake
python benchmark.py --system snowflake --disable-result-cache

# Cold run for Embucket only
python benchmark.py --system embucket --cold-runs

# Multiple runs with warm caching for both systems
python benchmark.py --runs 5
```

### Benchmark Process

The benchmark will:
1. Connect to the configured platform(s)
2. Execute each query from the selected benchmark suite with cache-clearing operations:
   - **Snowflake**: Warehouse suspend/resume before each query (if `--no-cache`)
   - **Embucket**: Docker container restart before each query (if `--no-cache`)
3. Collect performance metrics from query history
4. Display results and comparisons (if both platforms are run)
5. Save detailed results to CSV files
6. Calculate averages after all runs are completed

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
- `EMBUCKET_INSTANCE` for result organization
- `SSH_KEY_PATH` pointing to your private key (default: `~/.ssh/id_rsa`)
- SSH access to the EC2 instance running Embucket

**Performance Impact:**
- Adds ~30-60 seconds per query for container restart
- Significantly increases total benchmark time
- Provides cache-free, accurate performance measurements

## Output

The benchmark provides:
- **Console Output**: Formatted table with timing metrics for each query
- **CSV Files**: Individual result files for each run, organized by platform/dataset/instance
- **Average Results**: Automatically calculated after 3 runs are completed
- **Comparison Charts**: Visual comparisons between platforms when both are run (PNG files)
- **Total Times**: Aggregated compilation and execution times

**File Organization:**
- Snowflake results: `snowflake_{benchmark_type}_results/{schema}/{warehouse}/`
- Embucket results: `embucket_{benchmark_type}_results/{dataset}/{instance}/`

Where `{benchmark_type}` is one of: `tpch`, `clickbench`, or `tpcds`

## Files

- `benchmark.py` - Main benchmark script with restart functionality
- `docker_manager.py` - Docker container management for Embucket restarts
- `utils.py` - Connection utilities for Snowflake and Embucket
- `tpch/` - TPC-H benchmark utilities package (queries, DDL, table names)
- `clickbench/` - ClickBench benchmark utilities package (queries, DDL, table names)
- `tpcds/` - TPC-DS benchmark utilities package (queries, DDL, table names)
- `calculate_average.py` - Result averaging and analysis
- `config.py` - Configuration utilities
- `data_preparation.py` - Data preparation utilities
- `requirements.txt` - Python dependencies
- `env_example` - Example environment configuration file
- `infrastructure/` - Terraform infrastructure for EC2/Embucket deployment
- `tpch-datagen/` - TPC-H data generation infrastructure

## Benchmark Types

### TPC-H (Default)
Derived from the TPC-H decision support benchmark. Includes 22 complex analytical queries testing various aspects of data warehousing performance.

### ClickBench
Single-table analytical benchmark focusing on aggregation performance. Uses the `hits` table with web analytics data.

### TPC-DS
Derived from the TPC-DS decision support benchmark. More complex than TPC-H with 99 queries testing advanced analytical scenarios.

## Environment Variables

The benchmark behavior can be controlled through environment variables in your `.env` file:

- `BENCHMARK_TYPE`: Default benchmark type (`tpch`, `clickbench`, `tpcds`)
- `DATASET_PATH`: Path within S3 bucket for dataset location
- `DATASET_S3_BUCKET`: S3 bucket containing benchmark datasets
- `EMBUCKET_HOST`: EC2 instance IP for Embucket connection
- `SSH_KEY_PATH`: Path to SSH private key for container restarts

## Requirements

- Python 3.8+
- **For Snowflake**: Account with appropriate permissions and warehouse with suspend/resume capabilities
- **For Embucket**: EC2 instance with Docker Compose and SSH access for container restarts