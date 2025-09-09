import os
import uuid
import snowflake.connector as sf
from dotenv import load_dotenv

load_dotenv()


def create_embucket_connection():
    """Create Embucket connection with environment-based config."""

    # Connection config with defaults
    host = os.getenv("EMBUCKET_SQL_HOST", "localhost")
    port = os.getenv("EMBUCKET_SQL_PORT", "3000")
    protocol = os.getenv("EMBUCKET_SQL_PROTOCOL", "http")
    user = os.getenv("EMBUCKET_USER", "embucket")
    password = os.getenv("EMBUCKET_PASSWORD", "embucket")
    account = os.getenv("EMBUCKET_ACCOUNT") or f"acc_{uuid.uuid4().hex[:10]}"
    database = os.getenv("EMBUCKET_DATABASE", "embucket")
    schema = os.getenv("EMBUCKET_SCHEMA", "public")

    connect_args = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": "embucket",
        "host": host,
        "protocol": protocol,
        "port": int(port) if port else 3000,
    }

    conn = sf.connect(**connect_args)
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
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

    if not all([user, password, account, database, schema, warehouse]):
        raise ValueError("Missing one or more required Snowflake environment variables.")

    connect_args = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
    }

    conn = sf.connect(**connect_args)

    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.cursor().execute(f"USE SCHEMA {schema}")

    # Create stage if not exists
    conn.cursor().execute("CREATE OR REPLACE FILE FORMAT sf_parquet_format TYPE = parquet;")
    conn.cursor().execute("CREATE OR REPLACE TEMPORARY STAGE sf_prep_stage FILE_FORMAT = sf_parquet_format;")

    return conn
