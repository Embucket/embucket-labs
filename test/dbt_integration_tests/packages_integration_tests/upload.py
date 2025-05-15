import requests
import os
import json

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
    query = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
    headers = {
        "Content-Type": "application/json",
        # Add authentication headers if required, e.g.:
        # "Authorization": "Bearer <your_token_here>",
    }
    response = requests.post(
        f"{url}/ui/queries",
        headers=headers,
        data=json.dumps({"query": query})
    )
    response.raise_for_status()

if __name__ == "__main__":
    bootstrap(database, schema)
