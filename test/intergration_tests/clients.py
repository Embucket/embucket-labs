import json
import os

import pandas as pd
import pyarrow as pa
import pyiceberg.catalog
import pyiceberg.catalog.rest
import requests
import findspark
findspark.init()
from pyspark.sql import SparkSession

BASE_URL = "http://127.0.0.1:3000"
CATALOG_URL = f"{BASE_URL}/catalog"
WAREHOUSE_ID = "test_db"

class EmbucketClient:
    def __init__(self, username: str="embucket", password: str="embucket", base_url: str=BASE_URL):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.headers = {'Content-Type': 'application/json'}
        self._authenticate()

    def _authenticate(self):
        res = requests.post(
            f"{self.base_url}/ui/auth/login",
            headers=self.headers,
            data=json.dumps({"username": self.username, "password": self.password})
        )
        res.raise_for_status()
        token = res.json()["accessToken"]
        self.headers["authorization"] = f"Bearer {token}"

    def volume(self):
        vol = requests.get(
            f"{self.base_url}/v1/metastore/volumes",
            headers=self.headers,
            json={"ident": "test"},
        )
        if vol.status_code != 200:
            response = requests.post(
                f"{self.base_url}/v1/metastore/volumes",
                headers=self.headers,
                json={
                    "ident": "test",
                    "type": "file",
                    "path": f"{os.getcwd()}/data",
                },
            )
            response.raise_for_status()

    def sql(self, query: str):
        response = requests.post(
            f"{self.base_url}/ui/queries",
            headers=self.headers,
            data=json.dumps({"query": query})
        )
        response.raise_for_status()
        return response.json()


class PySparkClient:
    def __init__(self, app_name="SparkSQLClient"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "15g") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1") \
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.change.detection.mode", "error") \
            .config("spark.hadoop.fs.s3a.change.detection.version.required", "false") \
            .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
            .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
            .config("spark.sql.catalog.rest.uri", f"{CATALOG_URL}") \
            .config("spark.sql.catalog.rest.warehouse", WAREHOUSE_ID) \
            .config("spark.sql.defaultCatalog", "rest").getOrCreate()

    def sql(self, query: str, show_result: bool = True):
        df = self.spark.sql(query)
        if show_result:
            df.show()
        return df.toPandas()


class PyIcebergClient:
    def __init__(self, catalog_name: str = WAREHOUSE_ID, warehouse_path: str = None):
        self.catalog_name = catalog_name
        self.warehouse_path = warehouse_path
        self.catalog = pyiceberg.catalog.rest.RestCatalog(
            name="test-catalog",
            uri=CATALOG_URL.rstrip("/") + "/",
            warehouse=WAREHOUSE_ID,
        )

    def sql(self, query: str):
        raise NotImplementedError("Use SparkClient or REST endpoint to execute Iceberg SQL.")


embucket_client = EmbucketClient()
embucket_client.volume()
embucket_client.sql("CREATE DATABASE IF NOT EXISTS test_db EXTERNAL_VOLUME = 'test'")
embucket_client.sql("CREATE SCHEMA IF NOT EXISTS test_db.public")
embucket_client.sql("DROP TABLE IF EXISTS test_db.public.test")
embucket_client.sql("CREATE TABLE IF NOT EXISTS test_db.public.test (id int, name string)")

spark_client = PySparkClient()
spark_client.sql(f"INSERT INTO public.test VALUES (1, 'test_name')")
spark_client.sql(f"SELECT * FROM public.test ")

pyiceberg_client = PyIcebergClient()
table = pyiceberg_client.catalog.load_table(("public", "test"))
df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "name": ["a", "b", "c", "d"],
    }
)
# without schema it returns
# ValueError: Mismatch in fields:
# ┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
# ┃    ┃ Table field              ┃ Dataframe field          ┃
# ┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
# │ ❌ │ 1: id: optional int      │ 1: id: optional long     │
# │ ✅ │ 2: name: optional string │ 2: name: optional string │
# └────┴──────────────────────────┴──────────────────────────┘
schema = pa.schema([
    ("id", pa.int32()),
    ("name", pa.string()),
])
data = pa.Table.from_pandas(df, schema=schema)
table.append(data)
print(table.scan().to_arrow().to_pandas())
table.delete(delete_filter="id = 4")
print(table.scan().to_arrow().to_pandas())
#     table.overwrite(df=pa.Table.from_pandas(pd.DataFrame(
#         {
#             "id": [12, 13, 14, 15, 16],
#             "name": ["aa", "ab", "ac", "ad", "ae"],
#         }
#     )))
