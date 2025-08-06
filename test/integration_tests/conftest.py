import pytest
import os
from pyspark.sql import SparkSession

CATALOG_URL = "http://localhost:8080"
WAREHOUSE_ID = "test_db"
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

@pytest.fixture()
def rest_spark_session():
    spark_session = spark_session_factory(config_type='file_catalog', app_name='FileCatalogTest')
    yield spark_session
    spark_session.stop()


@pytest.fixture()
def s3_spark_session():
    spark_session = spark_session_factory(config_type='s3_catalog', app_name='S3CatalogTest')
    yield spark_session
    spark_session.stop()


def spark_session_factory(
        config_type: str,
        app_name: str,
        config_overrides: dict = None
    ):
    """
    The actual function that builds the SparkSession.

    Args:
        config_type (str): The type of configuration to use.
                           Expected values: 'file_catalog' or 's3_catalog'.
        app_name (str): The name for the Spark application.
        config_overrides (dict): A dictionary of Spark configs to add or
                                 override the base configuration.

    Returns:
        SparkSession: An initialized SparkSession object.
    """
    print(f"\nBuilding Spark session with config_type='{config_type}'...")
    builder = SparkSession.builder.appName(app_name)

    # --- Configuration Type 1: REST Client with SimpleAWSCredentialsProvider ---
    if config_type == 'file_catalog':
        builder.config("spark.driver.memory", "15g") \
               .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1") \
               .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
               .config("spark.hadoop.fs.s3a.path.style.access", "true") \
               .config("spark.hadoop.fs.s3a.change.detection.mode", "error") \
               .config("spark.hadoop.fs.s3a.change.detection.version.required", "false") \
               .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true") \
               .config("spark.hadoop.fs.s3a.impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
               .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
               .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
               .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
               .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
               .config("spark.sql.catalog.rest.uri", CATALOG_URL) \
               .config("spark.sql.catalog.rest.warehouse", WAREHOUSE_ID) \
               .config("spark.sql.defaultCatalog", "rest")

    # --- Configuration Type 2: Direct S3 Access with Explicit Keys ---
    elif config_type == 's3_catalog':
        builder.config("spark.driver.memory", "15g") \
               .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.hadoop:hadoop-aws:3.3.4") \
               .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
               .config("spark.hadoop.fs.s3a.path.style.access", "true") \
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
               .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
               .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
               .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
               .config("spark.sql.catalog.rest.uri", CATALOG_URL) \
               .config("spark.sql.catalog.rest.warehouse", WAREHOUSE_ID) \
               .config("spark.sql.defaultCatalog", "rest")
    else:
        raise ValueError(f"Unknown config_type: '{config_type}'. Expected 'file_catalog' or 's3_catalog'.")

    # Apply any specific overrides for the test
    if config_overrides:
        for key, value in config_overrides.items():
            builder.config(key, value)

    spark_session = builder.getOrCreate()
    return spark_session
