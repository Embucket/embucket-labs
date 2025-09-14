#!/usr/bin/env python3
"""
Centralized database connection module for dbt-snowplow-web project.
Provides standardized connection functions for Embucket and Snowflake databases.
"""

import os
import uuid
import snowflake.connector as sf
from pathlib import Path

# Load environment variables if dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, continue without it
    pass


def create_embucket_connection():
    """Create Embucket connection with environment-based config."""
    
    # Connection config with defaults
    host = os.getenv("EMBUCKET_HOST", "localhost")
    port = os.getenv("EMBUCKET_PORT", "3000")
    protocol = os.getenv("EMBUCKET_PROTOCOL", "http")
    user = os.getenv("EMBUCKET_USER", "embucket")
    password = os.getenv("EMBUCKET_PASSWORD", "embucket")
    account = os.getenv("EMBUCKET_ACCOUNT") or f"acc_{uuid.uuid4().hex[:10]}"
    database = os.getenv("EMBUCKET_DATABASE", "embucket")
    schema = os.getenv("EMBUCKET_SCHEMA", "public")
    warehouse = os.getenv("EMBUCKET_WAREHOUSE", "COMPUTE_WH")
    role = os.getenv("EMBUCKET_ROLE", "SYSADMIN")

    connect_args = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "role": role,
        "host": host,
        "protocol": protocol,
        "port": int(port) if port else 3000,
    }

    conn = sf.connect(**connect_args)
    
    # Setup external volume and database if needed
    conn.cursor().execute(
        f"""CREATE EXTERNAL VOLUME IF NOT EXISTS local 
            STORAGE_LOCATIONS = ((NAME = 'local' STORAGE_PROVIDER = 'FILE' 
            STORAGE_BASE_URL = '{os.getcwd()}'))"""
    )
    conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database} EXTERNAL_VOLUME = 'local'")
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    
    return conn


def create_snowflake_connection():
    """Create Snowflake connection with environment-based config."""
    
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    database = os.getenv("SNOWFLAKE_DATABASE", "embucket")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "public")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    if not all([user, password, account]):
        raise ValueError("Missing one or more required Snowflake environment variables: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT")

    connect_args = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "role": role,
    }

    conn = sf.connect(**connect_args)

    # Setup schema and file format
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    conn.cursor().execute(f"USE SCHEMA {database}.{schema}")

    # Create stage if not exists
    conn.cursor().execute("CREATE OR REPLACE FILE FORMAT sf_parquet_format TYPE = parquet;")
    conn.cursor().execute("CREATE OR REPLACE TEMPORARY STAGE sf_prep_stage FILE_FORMAT = sf_parquet_format;")

    return conn


def get_connection_config(target='embucket'):
    """Get connection configuration dictionary for Embucket or Snowflake.
    
    Args:
        target (str): 'embucket' or 'snowflake'
        
    Returns:
        dict: Connection configuration dictionary
    """
    if target.lower() == 'snowflake':
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'user': os.getenv('SNOWFLAKE_USER', ''),
            'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'embucket'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'public'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        }
    else:  # embucket
        return {
            'host': os.getenv('EMBUCKET_HOST', 'localhost'),
            'port': int(os.getenv('EMBUCKET_PORT', 3000)),
            'protocol': os.getenv('EMBUCKET_PROTOCOL', 'http'),
            'user': os.getenv('EMBUCKET_USER', 'embucket'),
            'password': os.getenv('EMBUCKET_PASSWORD', 'embucket'),
            'account': os.getenv('EMBUCKET_ACCOUNT', 'acc'),
            'warehouse': os.getenv('EMBUCKET_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('EMBUCKET_DATABASE', 'embucket'),
            'schema': os.getenv('EMBUCKET_SCHEMA', 'public'),
            'role': os.getenv('EMBUCKET_ROLE', 'SYSADMIN'),
        }


def copy_file_to_data_dir(source_file, data_dir="./datasets", target='embucket'):
    """Copy the events.csv file to the data directory.
    
    Args:
        source_file (str): Path to source file
        data_dir (str): Target data directory
        target (str): Target database ('embucket' or 'snowflake')
        
    Returns:
        str: Path to the file ready for upload
    """
    import shutil
    import subprocess
    
    if target.lower() == 'snowflake':
        # For Snowflake, we don't need to copy to a specific data directory
        # The file will be uploaded directly via Snowflake's PUT command
        print(f"✓ File {source_file} ready for Snowflake upload")
        return source_file
    else:
        # For Embucket, copy to data directory
        os.makedirs(data_dir, exist_ok=True)
        target_file = os.path.join(data_dir, "events.csv")
        
        if os.path.exists(source_file):
            shutil.copy2(source_file, target_file)
            print(f"✓ File copied to {target_file}")
        else:
            print(f"✗ Source file {source_file} not found")
            return None
            
        return target_file


def test_connection(target='embucket'):
    """Test database connection.
    
    Args:
        target (str): 'embucket' or 'snowflake'
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        if target.lower() == 'snowflake':
            conn = create_snowflake_connection()
        else:
            conn = create_embucket_connection()
            
        # Test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        conn.close()
        print(f"✓ {target.capitalize()} connection successful")
        return True
        
    except Exception as e:
        print(f"✗ {target.capitalize()} connection failed: {str(e)}")
        return False


if __name__ == "__main__":
    """Test connections when run directly."""
    import sys
    
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = 'embucket'
        
    print(f"Testing {target} connection...")
    success = test_connection(target)
    sys.exit(0 if success else 1)