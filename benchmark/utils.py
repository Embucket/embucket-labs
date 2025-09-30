import os
import uuid
import snowflake.connector as sf
from dotenv import load_dotenv

load_dotenv()


def create_embucket_connection():
    """Create Embucket connection with required environment variables."""

    host = os.environ["EMBUCKET_HOST"]
    port = int(os.environ["EMBUCKET_PORT"])
    protocol = os.environ["EMBUCKET_PROTOCOL"]
    user = os.environ["EMBUCKET_USER"]
    password = os.environ["EMBUCKET_PASSWORD"]
    account = os.environ["EMBUCKET_ACCOUNT"]
    database = os.environ["EMBUCKET_DATABASE"]
    schema = os.environ["EMBUCKET_SCHEMA"]

    connect_args = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": "embucket",
        "host": host,
        "protocol": protocol,
        "port": port,
        "socket_timeout": 1200, # connector restarts query if timeout (in seconds) is reached
    }

    conn = sf.connect(**connect_args)
    return conn


def create_snowflake_connection(setup_database_schema=True, tpch_scale_factor=None):
    """Create Snowflake connection with environment-based config.

    Args:
        setup_database_schema: If True, create and use the user's database/schema.
                              If False, skip database/schema setup (for using built-in tables).
        tpch_scale_factor: Scale factor for TPC-H built-in tables (e.g., 1, 10, 100, 1000).
                          Only used when setup_database_schema=False.
    """
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PASSWORD"]
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]

    if not all([user, password, account, warehouse]):
        raise ValueError("Missing one or more required Snowflake environment variables.")

    # Only require database/schema if we're setting them up
    if setup_database_schema:
        database = os.environ["SNOWFLAKE_DATABASE"]
        schema = os.environ["SNOWFLAKE_SCHEMA"]
        if not all([database, schema]):
            raise ValueError("Missing SNOWFLAKE_DATABASE or SNOWFLAKE_SCHEMA environment variables.")

        connect_args = {
            "user": user,
            "password": password,
            "account": account,
            "database": database,
            "schema": schema,
            "warehouse": warehouse,
        }
    else:
        connect_args = {
            "user": user,
            "password": password,
            "account": account,
            "warehouse": warehouse,
        }

    conn = sf.connect(**connect_args)

    conn.cursor().execute(f'CREATE OR REPLACE WAREHOUSE "{warehouse}" WITH WAREHOUSE_SIZE = \'{os.environ["SNOWFLAKE_WAREHOUSE_SIZE"]}\';')
    conn.cursor().execute(f'USE WAREHOUSE "{warehouse}";')

    if setup_database_schema:
        conn.cursor().execute(f'CREATE DATABASE IF NOT EXISTS "{database}"')
        conn.cursor().execute(f'USE DATABASE "{database}"')
        conn.cursor().execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.cursor().execute(f'USE SCHEMA "{schema}"')

        conn.cursor().execute("CREATE OR REPLACE FILE FORMAT sf_parquet_format TYPE = parquet;")
        conn.cursor().execute("CREATE OR REPLACE TEMPORARY STAGE sf_prep_stage FILE_FORMAT = sf_parquet_format;")
    # When using built-in tables, don't set any database/schema context
    # The queries will use fully qualified names like SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.tablename

    return conn

