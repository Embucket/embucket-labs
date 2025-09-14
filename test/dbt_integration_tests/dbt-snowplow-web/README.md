# DBT Snowplow Web Integration Tests

This project contains integration tests for running dbt-snowplow-web with both Embucket and Snowflake databases. It includes data generation, loading, and dbt execution capabilities.

## DBT Snowplow Web Run Results

### Not Incremental Run
![DBT SNowplow Web Run results](https://raw.githubusercontent.com/Embucket/embucket/assets_dbt/assets_dbt_snowplow/dbt_success_badge.svg)
![DBT Snowplow Web run results visualization](https://raw.githubusercontent.com/Embucket/embucket/assets_dbt/assets_dbt_snowplow/dbt_run_status.png)

### Incremental Run
![DBT SNowplow Web Incremental Run results](https://raw.githubusercontent.com/Embucket/embucket/assets_dbt/assets_dbt_snowplow_incremental/dbt_success_badge.svg)
![DBT Snowplow Web Incremental Run results visualization](https://raw.githubusercontent.com/Embucket/embucket/assets_dbt/assets_dbt_snowplow_incremental/dbt_run_status.png)

## How to Run

### Prerequisites

1. **Snowflake Connection Details** (if using Snowflake)
   - Snowflake account identifier
   - Snowflake username and password
   - Access to create databases and schemas
   - Warehouse with appropriate permissions

### Quick Start (Recommended)

1. **Navigate to the project directory**
   ```bash
   cd test/dbt_integration_tests/dbt-snowplow-web
   ```

2. **Run the complete test suite**
   ```bash
   # Run incremental test (1st param: true/false for incremental, 2nd param: number of sample rows, 3rd param: database target)
   ./incremental.sh false 10000 embucket
   ./incremental.sh false 10000 snowflake
   ```
   
   This script will:
   - Start Embucket in Docker (if using embucket target)
   - Generate sample data
   - Load data into the specified database (Embucket or Snowflake)
   - Run dbt-snowplow-web
   - Clean up containers (if using embucket target)


## File Structure

### Core Scripts
- **`incremental.sh`** - Main test runner script that orchestrates the entire process
- **`run_snowplow_web.sh`** - Component script used by incremental.sh for running dbt-snowplow-web
- **`setup_docker.sh`** - Sets up Docker environment for Embucket

### Database Integration
- **`db_connections.py`** - Centralized database connection module for Embucket and Snowflake
- **`load_events.py`** - Loads Snowplow events data into databases
- **`load_events_data.sql`** - SQL script for loading data into Embucket
- **`load_events_data_snowflake.sql`** - SQL script for loading data into Snowflake

### Data Generation & Processing
- **`gen_events.py`** - Generates sample Snowplow events data
- **`parse_dbt_simple.py`** - Parses dbt results and loads them into Snowflake
- **`generate_dbt_test_assets.py`** - Generates test assets and visualizations

### Configuration Files
- **`dbt_project.yml`** - dbt project configuration
- **`profiles.yml`** - dbt profiles configuration
- **`seeds.yml`** - dbt seeds configuration
- **`env_example`** - Environment variables template
- **`requirements.txt`** - Python dependencies

### Data Files
- **`events.csv`** - Sample Snowplow events data (13MB)
- **`events_today.csv`** - Today's events data
- **`events_yesterday.csv`** - Yesterday's events data
- **`datasets/`** - Directory for data files used by Embucket

### dbt-snowplow-web Project
- **`dbt-snowplow-web/`** - The actual dbt-snowplow-web project directory
- **`target/`** - dbt compilation output
- **`logs/`** - dbt execution logs

### Utilities & Documentation
- **`statistics.sh`** - Statistics generation script
- **`DATABASE_CONNECTIONS.md`** - Database connections documentation
- **`.gitignore`** - Git ignore patterns

### Generated Assets
- **`dbt_output.log`** - dbt execution output log
- **`dbt-snowplow-web/assets/`** - Generated dbt assets and visualizations

## Environment Configuration

### Embucket (Default)
- Host: `localhost`
- Port: `3000`
- Protocol: `http`
- User: `embucket`
- Password: `embucket`
- Database: `dbt_snowplow_web` (recreated on each run)
- Schema: `public_snowplow_manifest`

### Snowflake
- Account: Your Snowflake account
- User: Your Snowflake username
- Password: Your Snowflake password
- Database: `dbt_snowplow_web` (recreated on each run)
- Schema: `public_snowplow_manifest`
- Warehouse: `COMPUTE_WH`
- Role: `ACCOUNTADMIN`

## Notes

- The project runs with default user and password 'embucket' for Embucket
- No need to add .env file for basic Embucket testing
- By default, it runs with Embucket as the target database
- The incremental script automatically starts and stops Embucket in Docker
- You can run the tests multiple times - it cleans up containers before each run