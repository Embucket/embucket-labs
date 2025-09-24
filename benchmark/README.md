## Overview

This benchmark tool executes queries derived from TPC-H against both Snowflake and Embucket with cache-clearing operations to ensure clean, cache-free performance measurements. For Snowflake, it uses warehouse suspend/resume operations. For Embucket, it restarts the Docker container before each query to eliminate internal caching. It provides detailed timing metrics including compilation time, execution time, and total elapsed time.

## TPC Legal Considerations

It is important to know that TPC benchmarks are copyrighted IP of the Transaction Processing Council. Only members of the TPC consortium are allowed to publish TPC benchmark results. Fun fact: only four companies have published official TPC-H benchmark results so far, and those results can be seen [here](https://www.tpc.org/tpch/results/tpch_results5.asp).

However, anyone is welcome to create derivative benchmarks under the TPC's fair use policy, and that is what we are doing here. We do not aim to run a true TPC benchmark (which is a significant endeavor). We are just running the individual queries and recording the timings.

Throughout this document and when talking about these benchmarks, you will see the term "derived from TPC-H". We are required to use this terminology and this is explained in the [fair-use policy (PDF)](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf).

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
EMBUCKET_SQL_HOST=your_ec2_instance_ip
EMBUCKET_SQL_PORT=3000
EMBUCKET_SQL_PROTOCOL=http
EMBUCKET_USER=embucket
EMBUCKET_PASSWORD=embucket
EMBUCKET_ACCOUNT=embucket
EMBUCKET_DATABASE=embucket
EMBUCKET_SCHEMA=public
EMBUCKET_INSTANCE=your_instance_name
EMBUCKET_DATASET=your_dataset_name
SSH_KEY_PATH=~/.ssh/id_rsa
```

## Usage

Run the benchmark:
```bash
python benchmark.py
```

**Current Behavior**: By default, the benchmark runs **only Embucket** benchmarks for 3 iterations. To run both Snowflake and Embucket with comparisons, you need to modify the `__main__` section in `benchmark.py` to call `run_benchmark(i + 1)` instead of `run_embucket_benchmark(i + 1)`.

The benchmark will:
1. Connect to the configured platform (Embucket by default, or both if modified)
2. Execute each query derived from TPC-H with cache-clearing operations:
   - **Snowflake**: Warehouse suspend/resume before each query
   - **Embucket**: Docker container restart before each query
3. Collect performance metrics from query history
4. Display results and comparisons (if both platforms are run)
5. Save detailed results to CSV files
6. Calculate averages after 3 runs are completed

## Embucket Container Restart Functionality

For Embucket benchmarks, the system automatically restarts the Docker container before each query to eliminate internal caching and ensure accurate performance measurements.

**How it works:**
- Before each query execution, the benchmark connects to the EC2 instance via SSH
- Stops the Embucket Docker container: `docker-compose stop embucket`
- Starts the container: `docker-compose start embucket`
- Waits for the health check to pass (~30-60 seconds)
- Creates a fresh database connection and executes the query

**Requirements:**
- `EMBUCKET_SQL_HOST` set to your EC2 instance IP
- `EMBUCKET_INSTANCE` and `EMBUCKET_DATASET` for result organization
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
- Snowflake results: `snowflake_tpch_results/{schema}/{warehouse}/`
- Embucket results: `embucket_tpch_results/{dataset}/{instance}/`

## Files

- `benchmark.py` - Main benchmark script with restart functionality
- `docker_manager.py` - Docker container management for Embucket restarts
- `utils.py` - Connection utilities for Snowflake and Embucket
- `tpch_queries.py` - Query definitions derived from TPC-H
- `tpcds_queries.py` - Query definitions derived from TPC-DS (for future use)
- `calculate_average.py` - Result averaging and analysis
- `config.py` - Configuration utilities
- `data_preparation.py` - Data preparation utilities
- `requirements.txt` - Python dependencies
- `env_example` - Example environment configuration file
- `infrastructure/` - Terraform infrastructure for EC2/Embucket deployment
- `tpch-datagen/` - TPC-H data generation infrastructure
- `tpch/` - TPC-H benchmark utilities package (queries, DDL, table names)
- `tpcds_ddl/` - TPC-DS table definitions for Embucket

## Customizing Benchmark Behavior

**Default**: The benchmark runs only Embucket tests for 3 iterations.

**To run both Snowflake and Embucket with comparisons**: Modify the `__main__` section in `benchmark.py`:
```python
if __name__ == "__main__":
    for i in range(3):
        print(f"Run {i + 1} of 3")
        run_benchmark(i + 1)  # Change from run_embucket_benchmark(i + 1)
```

## Requirements

- Python 3.8+
- **For Snowflake**: Account with appropriate permissions and warehouse with suspend/resume capabilities
- **For Embucket**: EC2 instance with Docker Compose and SSH access for container restarts