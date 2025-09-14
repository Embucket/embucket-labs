# Database Connections

This document describes the centralized database connection approach used in the dbt-snowplow-web project.

## Overview

The project now uses a centralized `db_connections.py` module that provides standardized database connection functions for both Embucket and Snowflake databases. This approach eliminates code duplication and provides consistent connection handling across all scripts.

## Files

- `db_connections.py` - Centralized database connection module
- `utils.py` - Original utils file (from another project, kept for reference)

## Usage

### Import the connection functions

```python
from db_connections import create_embucket_connection, create_snowflake_connection, get_connection_config, copy_file_to_data_dir
```

### Create connections

```python
# For Embucket
conn = create_embucket_connection()

# For Snowflake
conn = create_snowflake_connection()
```

### Get connection configuration

```python
# Get configuration dictionary
config = get_connection_config('embucket')  # or 'snowflake'
```

### Test connections

```bash
# Test Embucket connection
python db_connections.py embucket

# Test Snowflake connection
python db_connections.py snowflake
```

## Environment Variables

### Embucket Configuration

```bash
export EMBUCKET_HOST=localhost
export EMBUCKET_PORT=3000
export EMBUCKET_PROTOCOL=http
export EMBUCKET_USER=embucket
export EMBUCKET_PASSWORD=embucket
export EMBUCKET_ACCOUNT=test
export EMBUCKET_DATABASE=embucket
export EMBUCKET_SCHEMA=public
export EMBUCKET_WAREHOUSE=COMPUTE_WH
export EMBUCKET_ROLE=SYSADMIN
```

### Snowflake Configuration

```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=embucket
export SNOWFLAKE_SCHEMA=public
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_ROLE=ACCOUNTADMIN
```

## Updated Scripts

The following scripts have been updated to use the centralized connection module:

- `load_events.py` - Loads Snowplow events data into databases
- `parse_dbt_simple.py` - Parses dbt results and loads them into Snowflake

## Benefits

1. **Centralized Configuration**: All database connection logic is in one place
2. **Consistent Error Handling**: Standardized error handling across all connections
3. **Easy Maintenance**: Changes to connection logic only need to be made in one file
4. **Environment Variable Management**: Automatic loading of environment variables with fallback defaults
5. **Connection Testing**: Built-in connection testing functionality

## Dependencies

The module requires the following Python packages:

- `snowflake-connector-python` - For database connections
- `python-dotenv` - For environment variable loading (optional)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Error Handling

The module provides comprehensive error handling:

- Missing environment variables are handled gracefully with informative error messages
- Connection failures are caught and reported with details
- Optional dependencies (like dotenv) are handled gracefully