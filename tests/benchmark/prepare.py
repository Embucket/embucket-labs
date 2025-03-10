import json
import requests
from pyspark.sql import SparkSession

# Local
CATALOG = "http://0.0.0.0:3000"
WAREHOUSE_NAME = "benchmark"
TABLE = "hits"
DATABASE = "public"

# Prod
# CATALOG = "https://api.embucket.com"
# WAREHOUSE_NAME = "snowplow"

def prepare_data(catalog_url, local_volume=True, path_to_data=None):
    headers = {'Content-Type': 'application/json'}

    ### CREATE STORAGE PROFILE
    if local_volume:
        payload = json.dumps({
            "type": "fs"
        })
    else:
        payload = json.dumps({
            "bucket": "artemembucket",
            "credentials": {
                "awsAccessKeyId": "KEY",
                "awsSecretAccessKey": "KEY"
            },
            "region": "us-east-2",
            "endpoint": "https://s3.us-east-2.amazonaws.com",
            "type": "aws"
        })

    response_sp = requests.request("POST", f'{catalog_url}/ui/storage-profiles', headers=headers, data=payload).json()

    ### CREATE WAREHOUSE
    payload = json.dumps({
        "storageProfileId": response_sp['id'],
        "name": WAREHOUSE_NAME,
        "keyPrefix": "warehouse-prefix"
    })
    response_w = requests.request("POST", f'{catalog_url}/ui/warehouses', headers=headers, data=payload).json()

    ### CREATE DATABASE
    requests.request(
        "POST", f"{catalog_url}/ui/warehouses/{response_w['id']}/databases", headers=headers,
        data=json.dumps({"name": DATABASE})
    ).json()

    wh_id = response_w['id']

    ### CREATE HITS TABLE
    with open('create_table.sql', 'r') as file:
        create_table_query = file.read()
        requests.request(
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
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
            .config("spark.sql.catalog.rest.io-impl", io_impl) \
            .config("spark.sql.catalog.rest.uri", f"{catalog_url}/catalog") \
            .config("spark.sql.catalog.rest.warehouse", wh_id) \
            .config("spark.sql.defaultCatalog", "rest") \
            .getOrCreate()
        df = spark.read.parquet(path_to_data)
        size = df.count()
        df.createOrReplaceTempView("hits")
        # check the state here http://localhost:4040/jobs/
        spark.sql("""INSERT INTO rest.public.hits select * from hits""")
        return size
