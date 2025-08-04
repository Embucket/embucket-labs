import os

import snowflake.connector

url = "http://localhost:3000"
database = "embucket"
schema = "public"


def bootstrap():
    cursor = get_cursor()
    # Volume
    cursor.execute(f"CREATE EXTERNAL VOLUME IF NOT EXISTS test STORAGE_LOCATIONS = (\
        (NAME = 'file_vol' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '{os.getcwd()}/data'))")
    # Database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database} EXTERNAL_VOLUME = 'test'")
    # Schema
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")


def get_cursor():
    con = snowflake.connector.connect(
        host=os.getenv("EMBUCKET_HOST", "localhost"),
        port=os.getenv("EMBUCKET_PORT", 3000),
        protocol=os.getenv("EMBUCKET_PROTOCOL", "http"),
        user=os.getenv("EMBUCKET_USER", "embucket"),
        password=os.getenv("EMBUCKET_PASSWORD", "embucket"),
        account=os.getenv("EMBUCKET_ACCOUNT", "acc"),
        warehouse=os.getenv("EMBUCKET_WAREHOUSE", ""),
        database=os.getenv("EMBUCKET_DATABASE", database),
        schema=os.getenv("EMBUCKET_SCHEMA", schema),
        session_parameters={
            "QUERY_TAG": "dbt-testing",
        },
    )
    return con.cursor()


if __name__ == "__main__":
    bootstrap()
