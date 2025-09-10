## Overview

This benchmark tool executes queries derived from TPC-DS against Snowflake with warehouse suspend/resume operations to ensure clean, cache-free performance measurements. It provides detailed timing metrics including compilation time, execution time, and total elapsed time.

## TPC Legal Considerations

It is important to know that TPC benchmarks are copyrighted IP of the Transaction Processing Council. Only members of the TPC consortium are allowed to publish TPC benchmark results. Fun fact: only four companies have published official TPC-DS benchmark results so far, and those results can be seen [here](https://www.tpc.org/tpcds/results/tpcds_results5.asp?orderby=dbms&version=3).

However, anyone is welcome to create derivative benchmarks under the TPC's fair use policy, and that is what we are doing here. We do not aim to run a true TPC benchmark (which is a significant endeavor). We are just running the individual queries and recording the timings.

Throughout this document and when talking about these benchmarks, you will see the term "derived from TPC-DS". We are required to use this terminology and this is explained in the [fair-use policy (PDF)](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf).

**This benchmark is a Non-TPC Benchmark. Any comparison between official TPC Results with non-TPC workloads is prohibited by the TPC.**

## Features

- **Cache Isolation**: Suspends and resumes warehouse before each query to eliminate caching effects
- **Result Cache Disabled**: Ensures no result caching affects benchmark results
- **Comprehensive Metrics**: Tracks compilation time, execution time, and row counts
- **CSV Export**: Saves results to `query_results.csv` for further analysis
- **Error Handling**: Graceful handling of warehouse operations and query failures

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

### 3. Configure Snowflake Connection
Create a `.env` file with your Snowflake credentiale using env_example:
```bash
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
```

## Usage

Run the benchmark:
```bash
python benchmark.py
```

The benchmark will:
1. Connect to Snowflake using your configuration
2. Execute each query derived from TPC-DS with warehouse restart for cache isolation
3. Collect performance metrics from query history
4. Display results in a formatted table
5. Save detailed results to `query_results.csv`

## Output

The benchmark provides:
- **Console Output**: Formatted table with timing metrics for each query
- **CSV File**: `query_results.csv` with detailed results for analysis
- **Total Times**: Aggregated compilation and execution times

## Files

- `benchmark.py` - Main benchmark script
- `config.py` - Snowflake configuration management
- `tpcds_queries.py` - Query definitions derived from TPC-DS
- `data_preparation.py` - Data setup utilities
- `requirements.txt` - Python dependencies
- `tpcds_ddl/` - DDL scripts derived from TPC-DS

## Requirements

- Python 3.8+
- Snowflake account with appropriate permissions
- Warehouse with suspend/resume capabilities