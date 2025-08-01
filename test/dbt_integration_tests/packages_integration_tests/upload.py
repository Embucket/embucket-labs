import os

import snowflake.connector

url = "http://localhost:3000"
database = "embucket"
schema = "public"


def bootstrap():
    cursor = get_cursor()
    ### VOLUME
    cursor.execute(f"CREATE EXTERNAL VOLUME IF NOT EXISTS test STORAGE_LOCATIONS = (\
        (NAME = 'file_vol' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '{os.getcwd()}/data'))")
    ## DATABASE
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database} EXTERNAL_VOLUME = test")
    ## SCHEMA
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")


def get_cursor():
    USER = os.getenv("EMBUCKET_USER", "embucket")
    PASSWORD = os.getenv("EMBUCKET_PASSWORD", "embucket")
    ACCOUNT = os.getenv("EMBUCKET_ACCOUNT", "acc")
    DATABASE = os.getenv("EMBUCKET_DATABASE", database)
    SCHEMA = os.getenv("EMBUCKET_SCHEMA", schema)
    WAREHOUSE = os.getenv("EMBUCKET_WAREHOUSE", "")

    con = snowflake.connector.connect(
        host=os.getenv("EMBUCKET_HOST", "localhost"),
        port=os.getenv("EMBUCKET_PORT", 3000),
        protocol=os.getenv("EMBUCKET_PROTOCOL", "http"),
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        session_parameters={
            "QUERY_TAG": "dbt-testing",
        },
    )
    return con.cursor()


if __name__ == "__main__":
    bootstrap()
