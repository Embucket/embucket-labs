import json

import requests
from prepare import prepare_data

# Local
CATALOG = "http://127.0.0.1:3000"
WAREHOUSE_NAME = "benchmark"

# Prod
# CATALOG = "https://api.embucket.com"

TABLE_PATH = f"benchmark.public.hits"

def run_benchmark():
    data_size = 0
    final_result = {
        "system": "Icebucket (Parquet, partitioned)",
        "date": "2024-03-10",
        "machine": "local MacBook Pro M1",
        "cluster_size": 1,
        "comment": "Local run",
        "tags": ["Rust", "column-oriented", "embedded", "stateless"],
        "load_time": 0,
        "data_size": data_size,
        "result": []
    }

    TRIES = 3

    print("Running queries...")
    with open('queries.sql', 'r') as file:
        for row in file.readlines():
            query = row.replace("warehouse_database_table", TABLE_PATH)
            print(f"Query {query}")
            duration = []
            for i in range(TRIES):
                response = requests.post(
                    f"{CATALOG}/ui/query",
                    json={"query": query.format(TABLE_PATH)},
                )
                if response.status_code == 200:
                    duration.append(float(response.json().get('durationSeconds', 0)))
                else:
                    print(response.content, response.status_code)
                    duration.append(0)
            final_result["result"].append(duration)

    print("Saving results...")
    json.dump(final_result, open("result_local_non_iceberg.json", "w"))

def compare_results():
    with open('datafusion.json') as json_data:
        datafusion = json.load(json_data)['result']

    with open('result.json') as json_data:
        icebucket = json.load(json_data)['result']

    with open('compare.txt', 'w') as file:
        file.write('icebucket,datafusion,query\n')

        with open('queries.sql', 'r') as f:
            for (i, row) in enumerate(f.readlines()):
                file.write(f'{min(icebucket[i])},{min(datafusion[i])},{row}')

### print("Preparing data...")
### create volume, catalog, schema, table, upload data
### local_volume=True for local volume, False for S3
data_size = prepare_data(
    CATALOG,
    path_to_data="/Users/artem/Downloads/partitioned",
    create_volume=True,
    local_volume=True,
    create_database=True,
    create_schema=True,
    create_table=True,
    create_external=False,
)
# run_benchmark()
# compare_results()

