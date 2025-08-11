import json
import os
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyiceberg.catalog
import pyiceberg.catalog.rest
import requests
import findspark

findspark.init()
from pyspark.sql import SparkSession
import snowflake.connector

BASE_URL = "http://127.0.0.1:3000"
CATALOG_URL = f"{BASE_URL}/catalog"
WAREHOUSE_ID = "test_db"

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")


class EmbucketClient:
    def __init__(
        self,
        username: str = os.getenv("EMBUCKET_USERNAME", "embucket"),
        password: str = os.getenv("EMBUCKET_PASSWORD", "embucket"),
        host: str = os.getenv("EMBUCKET_SNOWFLAKE_HOST", "localhost"),
        port: int = os.getenv("EMBUCKET_SNOWFLAKE_PORT", 3000),
        protocol: str = os.getenv("EMBUCKET_SNOWFLAKE_PROTOCOL", "http"),
        database: str = os.getenv("EMBUCKET_DEFAULT_DATABASE", WAREHOUSE_ID),
        schema: str = os.getenv("EMBUCKET_DEFAULT_SCHEMA", "PUBLIC"),
    ):
        """
        Connect via Snowflake Python Connector to Embucket's Snowflake-compatible SQL API.

        Notable params:
        - host: Embucket SQL API host (e.g. 'localhost:8080')
        - protocol: 'http' or 'https'
        - account: arbitrary identifier; required by connector but ignored by Embucket
        - database/schema: optional defaults for session
        """
        self.username = username
        self.password = password
        self.host = host
        self.protocol = protocol
        self.database = database
        self.schema = schema

        # Establish a connection. Keep it for reuse.
        self._conn = snowflake.connector.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            schema=self.schema,
            protocol=self.protocol,
        )

    @staticmethod
    def get_volume_sql(volume_name, vol_type=None):
        if vol_type == "s3":
            return f"""
                CREATE EXTERNAL VOLUME IF NOT EXISTS '{volume_name}' STORAGE_LOCATIONS = ((
                    NAME = 's3-volume' STORAGE_PROVIDER = 'S3'
                    STORAGE_BASE_URL = '{S3_BUCKET}'
                    CREDENTIALS=(AWS_KEY_ID='{AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}' REGION='{AWS_REGION}')
                ))"""
        elif vol_type == "minio":
            return f"""
                CREATE EXTERNAL VOLUME test STORAGE_LOCATIONS = ((
                    NAME = 's3-volume' STORAGE_PROVIDER = 'S3'
                    STORAGE_BASE_URL = 'acmecom-lakehouse'
                    STORAGE_ENDPOINT = 'http://localhost:9000'
                    CREDENTIALS=(AWS_KEY_ID='minioadmin' AWS_SECRET_KEY='minioadmin')
                ))"""
        return f"CREATE EXTERNAL VOLUME IF NOT EXISTS '{volume_name}' STORAGE_LOCATIONS = (\
            (NAME = 'file_vol' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '{os.getcwd()}/data'))"

    def create_volume(self, name: str = "test", vol_type: Optional[str] = None):
        """Create the external volume if missing using current environment defaults."""
        return self.sql(self.get_volume_sql(name, vol_type))

    def sql(self, query: str):
        """
        Execute SQL via Snowflake connector and return a dict compatible with existing tests:
        {"result": {"rows": [...], "columns": [{"name": ...}, ...]}}
        """
        with self._conn.cursor() as cur:
            cur.execute(query)
            # Some commands (DDL/DML) may not return data
            description = cur.description
            rows = cur.fetchall() if description is not None else []
            columns = (
                [{"name": d[0]} for d in description] if description is not None else []
            )
        return {"result": {"rows": rows, "columns": columns}}


class PySparkClient:
    def __init__(self, app_name="SparkSQLClient"):
        self.spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.driver.memory", "15g")
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
            )
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configurationFile=log4j2.properties",
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.change.detection.mode", "error")
            .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
            .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.iceberg.hadoop.HadoopFileIO"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                "spark.sql.catalog.rest.catalog-impl",
                "org.apache.iceberg.rest.RESTCatalog",
            )
            .config(
                "spark.sql.catalog.rest.io-impl",
                "org.apache.iceberg.hadoop.HadoopFileIO",
            )
            .config("spark.sql.catalog.rest.uri", f"{CATALOG_URL}")
            .config("spark.sql.catalog.rest.warehouse", WAREHOUSE_ID)
            .config("spark.sql.defaultCatalog", "rest")
            .getOrCreate()
        )

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

    def read_table(self, table_name: str, limit: int = None):
        """Loads an Iceberg table and returns its rows as a list of dicts."""
        print(f"Reading table '{table_name}' with pyiceberg...")
        try:
            table = self.catalog.load_table(table_name)
            print(f"Schema: {table.schema()}")
            arrow_table = table.scan().to_arrow()
            if limit is not None:
                arrow_table = arrow_table.slice(0, limit)
            rows = arrow_table.to_pylist()
            return rows
        except pyiceberg.exceptions.NoSuchTableError:
            print(f"Table '{table_name}' does not exist in the catalog.")
            return []
        except Exception as e:
            print(f"Unexpected error: {e}")
            return []

    def sql(self, query: str):
        raise NotImplementedError(
            "Use SparkClient or REST endpoint to execute Iceberg SQL."
        )
