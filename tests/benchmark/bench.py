import json

import requests
from prepare import prepare_data

# Local
# CATALOG = "http://127.0.0.1:3000"
# Prod
CATALOG = "https://api.embucket.com"
TRIES = 3

def run_benchmark(external_table=False, comment="", file_name="result", external_table_single=False, data_size=0):
    final_result = {
        "system": "Icebucket (Parquet, partitioned)",
        "date": "2024-03-21",
        "machine": "c5n.2xlarge",
        "cluster_size": 1,
        "comment": comment,
        "tags": ["Rust", "column-oriented", "embedded", "stateless"],
        "load_time": 0,
        "data_size": data_size,
        "result": []
    }
    file_name = f"{file_name}.json"

    queries_file = 'queries_external_table.sql' if external_table else 'queries.sql'
    if external_table:
        table_path = 'benchmark_local.public.hits_single' if external_table_single else 'benchmark_local.public.hits_partitioned'
    else:
        table_path = 'benchmark.public.hits'

    print("Running queries...")
    with open(queries_file, 'r') as file:
        for row in file.readlines():
            query = row.replace("warehouse_database_table", table_path)
            print(f"Query {query}")
            duration = []
            for i in range(TRIES):
                response = requests.post(
                    f"{CATALOG}/ui/queries?worksheet_id=1",
                    json={"query": query.format(table_path)},
                )
                if response.status_code == 200:
                    duration.append(float(response.json().get('durationSeconds', 0)))
                else:
                    print(response.content, response.status_code)
                    duration.append(0)
            final_result["result"].append(duration)

    print("Saving results...")
    json.dump(final_result, open(file_name, "w"))

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
rows_counts = prepare_data(
    CATALOG,
    path_to_data="/Users/artem/Downloads/partitioned",
    create_volume=True,
    local_volume=False,
    create_database=False,
    create_schema=False,
    create_worksheet=False,
    create_table=True,
    create_external=True,
)
# run_benchmark(
#     external_table=False,
#     comment="EC2 instance with iceberg partitioned parquets on S3",
#     file_name="2024-03-24_c7a.24xlarge_iceberg_s3"
# )
# run_benchmark(
#     external_table=True,
#     external_table_single=False,
#     comment="EC2 instance with local partitioned parquets (non-iceberg)",
#     file_name="2024-03-24_c7a.24xlarge_local_partitioned"
# )
run_benchmark(
    external_table=True,
    external_table_single=True,
    comment="EC2 instance with local single parquet (non-iceberg)",
    file_name="2024-03-24_c7a.24xlarge_local_single"
)
# run_benchmark(
#     external_table=True,
#     external_table_single=False,
#     comment="local single parquet (non-iceberg)",
#     file_name="2024-03-24.local_partitioned"
# )
# compare_results()

