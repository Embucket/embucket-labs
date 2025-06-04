import requests
import os
import json

url = "http://localhost:3000"
database = "embucket"
schema = "public"

def bootstrap(catalog, schema, table_name="sample_table"):
    response = requests.get(f"{url}/v1/metastore/databases")
    response.raise_for_status()

    wh_list = [wh for wh in response.json() if wh["ident"] == catalog]
    if wh_list:
        print(f"Database {catalog} already exists, skipping creation.")
    else:
        # Create Volume
        response = requests.post(
            f"{url}/v1/metastore/volumes",
            json={
                "ident": "test",
                "type": "file",
                "path": f"{os.getcwd()}/data",
            },
        )
        response.raise_for_status()
        print(f"Volume 'test' created at {os.getcwd()}/data.")

        # Create Database
        response = requests.post(
            f"{url}/v1/metastore/databases",
            json={
                "volume": "test",
                "ident": catalog,
            },
        )
        response.raise_for_status()
        print(f"Database {catalog} created.")

    # Create Schema
    query = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        f"{url}/ui/queries",
        headers=headers,
        data=json.dumps({"query": query})
    )
    response.raise_for_status()
    print(f"Schema {catalog}.{schema} created or already exists.")

    # Create Sample Table
    table_query = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
        id INT,
        name VARCHAR,
        created_at TIMESTAMP
    )
    """
    response = requests.post(
        f"{url}/ui/queries",
        headers=headers,
        data=json.dumps({"query": table_query})
    )
    response.raise_for_status()
    print(f"Sample table {catalog}.{schema}.{table_name} created successfully.")

if __name__ == "__main__":
    bootstrap(database, schema)