import json
import os

import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

# Local
DATABASE = "benchmark"
TABLE = "hits"
SCHEMA = "public"

def prepare_data(
        catalog_url,
        path_to_data=None,
        create_volume=False,
        local_volume=True,
        create_database=False,
        create_schema=False,
        create_worksheet=False,
        create_table=False,
        create_external=False,
):
    headers = {'Content-Type': 'application/json'}
    volume_ident = "local" if local_volume else "s3-artemembucket"

    if create_volume:
        if local_volume:
            payload = json.dumps({
                "type": "file",
                "path": "/Users/artem/work/reps/github.com/Embucket/control-plane-v2",
                "ident": volume_ident,
            })
        else:
            payload = json.dumps({
                "ident": volume_ident,
                "type": "s3",
                "region": "us-east-2",
                "bucket": "artemembucket",
                "endpoint": "https://s3.us-east-2.amazonaws.com",
                "credentials": {
                    "credential_type": "access_key",
                    "aws-access-key-id": "AKIA3FLDXOZOTTBXNUPI",
                    "aws-secret-access-key": "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"
                },
            })
        requests.request("POST", f'{catalog_url}/v1/metastore/volumes', headers=headers, data=payload).json()

    if create_database:
        payload = json.dumps({
            "ident": DATABASE,
            "volume": volume_ident
        })
        requests.request("POST", f'{catalog_url}/v1/metastore/databases', headers=headers, data=payload).json()

    if create_schema:
        response_s = requests.request(
            "POST", f"{catalog_url}/v1/metastore/databases/{DATABASE}/schemas", headers=headers,
            data=json.dumps({
                "ident": {
                    "schema": SCHEMA,
                    "database": DATABASE
                }
            })
        ).json()

    if create_worksheet:
        worksheet_id = requests.request(
            "POST", f"{catalog_url}/ui/worksheets", headers=headers,
            data=json.dumps({})
        ).json()['id']
    else:
        worksheet_id = 1

    if create_table:
        if create_external:
            ## CREATE HITS TABLE
            query = """
                CREATE EXTERNAL TABLE benchmark.public.hits
                STORED AS PARQUET
                LOCATION 'partitioned/'
                OPTIONS ('binary_as_string' 'true');
            """
            requests.request(
                "POST", f"{catalog_url}/ui/worksheets/{worksheet_id}/queries",
                headers=headers,
                data=json.dumps({"query": query})
            )
        else:
            ## CREATE HITS TABLE
            with open('create_table.sql', 'r') as file:
                create_table_query = file.read()
                resp = requests.request(
                    "POST", f"{catalog_url}/ui/query",
                    headers=headers,
                    data=json.dumps({"query": create_table_query})
                )

            io_impl = "org.apache.iceberg.hadoop.HadoopFileIO" if local_volume else "org.apache.iceberg.aws.s3.S3FileIO"
            if path_to_data:
                # Update4 config if required
                spark = SparkSession.builder \
                    .appName("s3") \
                    .config("spark.driver.memory", "15g") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.change.detection.mode", "warn") \
                    .config("spark.hadoop.fs.s3a.change.detection.version.required", "false") \
                    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
                    .config("spark.sql.catalog.rest.io-impl", io_impl) \
                    .config("spark.sql.catalog.rest.uri", f"{catalog_url}/catalog") \
                    .config("spark.sql.catalog.rest.warehouse", DATABASE) \
                    .config("spark.sql.defaultCatalog", "rest") \
                    .getOrCreate()
                df = spark.read.parquet(path_to_data)
                size = df.count()
                df.createOrReplaceTempView("hits")
                # check the state here http://localhost:4040/jobs/
                spark.sql("""INSERT INTO rest.public.hits select * from hits""")
                return size
# data_size = prepare_data(
#     "http://127.0.0.1:3000",
#     path_to_data="/Users/artem/Downloads/partitioned",
#     create_volume=True,
#     local_volume=True,
#     create_database=True,
#     create_schema=True,
#     create_worksheet=True,
#     create_table=True,
#     create_external=True,
# )