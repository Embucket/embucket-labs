
# Snowflake TPCDS Benchmark

A comprehensive benchmark tool for testing Snowflake performance using TPC-DS queries with proper cache isolation.

## Overview

This benchmark tool executes TPC-DS queries against Snowflake with warehouse suspend/resume operations to ensure clean, cache-free performance measurements. It provides detailed timing metrics including compilation time, execution time, and total elapsed time.

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
2. Execute each TPC-DS query with warehouse restart for cache isolation
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
- `tpcds_queries.py` - TPC-DS query definitions
- `data_preparation.py` - Data setup utilities
- `requirements.txt` - Python dependencies
- `tpcds_ddl/` - TPC-DS DDL scripts

## Requirements

- Python 3.8+
- Snowflake account with appropriate permissions
- Warehouse with suspend/resume capabilities