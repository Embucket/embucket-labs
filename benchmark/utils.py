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



def create_snowflake_connection():
    """Create Snowflake connection with environment-based config."""
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PASSWORD"]
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    database = os.environ["DATASET_NAME"]
    schema = os.environ["DATASET_SCALE_FACTOR"]
    warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]

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

    conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.cursor().execute(f"USE SCHEMA {schema}")

    conn.cursor().execute("CREATE OR REPLACE FILE FORMAT sf_parquet_format TYPE = parquet;")
    conn.cursor().execute("CREATE OR REPLACE TEMPORARY STAGE sf_prep_stage FILE_FORMAT = sf_parquet_format;")

    return conn
