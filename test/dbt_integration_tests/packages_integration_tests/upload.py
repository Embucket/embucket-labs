import requests
import os
import snowflake.connector

url = "http://localhost:3000"
database = "embucket"
schema = "public"


def bootstrap(catalog, schema):
    response = requests.get(f"{url}/v1/metastore/databases")
    response.raise_for_status()

    wh_list = [wh for wh in response.json() if wh["ident"] == database]
    if wh_list:
        return

    ### VOLUME
    response = requests.post(
        f"{url}/v1/metastore/volumes",
        json={
            "ident": "test",
            "type": "file",
            "path": f"{os.getcwd()}/data",
        },
    )
    response.raise_for_status()

    ## DATABASE
    response = requests.post(
        f"{url}/v1/metastore/databases",
        json={
            "volume": "test",
            "ident": database,
        },
    )
    response.raise_for_status()

    ## SCHEMA
    USER = os.getenv("EMBUCKET_USER", "xxx")
    PASSWORD = os.getenv("EMBUCKET_PASSWORD", "yyy")
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

    cursor = con.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    cursor.execute(query)


if __name__ == "__main__":
    bootstrap(database, schema)
