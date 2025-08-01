import os
import sys
import logging
import random
import math
from collections import OrderedDict

from dotenv import load_dotenv
import requests
from decimal import Decimal
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, StringType,
    DecimalType, DateType,
    TimestampType, DoubleType, BooleanType,
)
from clients import EmbucketClient, PyIcebergClient

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tpch-loader")

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

EMBUCKET_URL = "http://localhost:8080"
CATALOG_URL = "http://localhost:3000/catalog"
WAREHOUSE_ID = "test_s3_volume"
VOLUME = "test_s3_volume"
BASE = "tpc_h/public"
TPC_H_DATA_PATH = f"s3a://{S3_BUCKET}/{BASE}"

# Map simplified SQL types to Python type checks
TYPE_CHECKS = {
    "BIGINT": lambda v: isinstance(v, int),
    "INT": lambda v: isinstance(v, int),
    "DECIMAL": lambda v: isinstance(v, (Decimal, float, int)),  # Arrow may materialize decimals as Decimal or float
    "VARCHAR": lambda v: isinstance(v, str),
    "DATE": lambda v: hasattr(v, "year") and hasattr(v, "month") and hasattr(v, "day"),  # datetime.date
    "TIMESTAMP": lambda v: hasattr(v, "year") and hasattr(v, "hour"),  # datetime-like
    "DOUBLE": lambda v: isinstance(v, float),
    "BOOLEAN": lambda v: isinstance(v, bool) or v is None,
}


TABLE_METADATA = {
    "nation": OrderedDict([
        ("n_nationkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("n_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("n_regionkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("n_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),  # nullable for coverage
    ]),
    "region": OrderedDict([
        ("r_regionkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("r_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("r_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "supplier": OrderedDict([
        ("s_suppkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("s_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("s_address", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("s_nationkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("s_phone", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("s_acctbal", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("s_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "part": OrderedDict([
        ("p_partkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("p_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_mfgr", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_brand", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_type", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_size", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("p_container", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_retailprice", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("p_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "partsupp": OrderedDict([
        ("ps_partkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("ps_suppkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("ps_availqty", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("ps_supplycost", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("ps_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "customer": OrderedDict([
        ("c_custkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("c_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_address", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_nationkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("c_phone", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_acctbal", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("c_mktsegment", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "orders": OrderedDict([
        ("o_orderkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("o_custkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("o_orderstatus", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("o_totalprice", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("o_orderdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("o_orderpriority", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("o_clerk", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("o_shippriority", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("o_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "lineitem": OrderedDict([
        ("l_orderkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_partkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_suppkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_linenumber", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_quantity", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_extendedprice", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_discount", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_tax", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_returnflag", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_linestatus", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_shipdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("l_commitdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("l_receiptdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("l_shipinstruct", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_shipmode", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "datatypes_test": OrderedDict([
        ("id", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("ts_col", {"type": "TIMESTAMP", "nullable": True, "spark": TimestampType}),
        ("double_col", {"type": "DOUBLE", "nullable": True, "spark": DoubleType}),
        ("bool_col", {"type": "BOOLEAN", "nullable": True, "spark": BooleanType}),
        ("comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "edge_cases_test": OrderedDict([
        ("pk", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("large_int", {"type": "BIGINT", "nullable": True, "spark": LongType}),
        ("negative_int", {"type": "INT", "nullable": True, "spark": IntegerType}),
        ("high_precision_dec", {"type": "DECIMAL(38,18)", "nullable": True, "spark": lambda: DecimalType(38, 18)}),
        ("zero_dec", {"type": "DECIMAL(5,2)", "nullable": True, "spark": lambda: DecimalType(5, 2)}),
        ("empty_string", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
        ("unicode_string", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
        ("timestamp_min", {"type": "TIMESTAMP", "nullable": True, "spark": TimestampType}),
        ("timestamp_max", {"type": "TIMESTAMP", "nullable": True, "spark": TimestampType}),
        ("bool_true", {"type": "BOOLEAN", "nullable": True, "spark": BooleanType}),
        ("bool_null", {"type": "BOOLEAN", "nullable": True, "spark": BooleanType}),
        ("floating_nan", {"type": "DOUBLE", "nullable": True, "spark": DoubleType}),
        ("floating_inf", {"type": "DOUBLE", "nullable": True, "spark": DoubleType}),
])
}


def make_spark_schema_from(metadata):
    return StructType([
        StructField(name, info["spark"]() if callable(info["spark"]) else info["spark"], nullable=info["nullable"])
        for name, info in metadata.items()
    ])


def make_create_table_ddl(table_name, metadata, volume, catalog_url, base):
    col_defs = []
    for name, info in metadata.items():
        nullability = "NOT NULL" if not info["nullable"] else ""
        col_type = info["type"]
        col_defs.append(f"{name} {col_type} {nullability}".strip())
    col_block = ",\n  ".join(col_defs)
    ddl = f"""
    CREATE OR REPLACE TABLE tpc_h.public.{table_name}
      EXTERNAL_VOLUME = '{volume}'
      CATALOG         = '{catalog_url}'
      BASE_LOCATION   = '{base}/{table_name}'
    (
      {col_block}
    );
    """
    return ddl


def sample_value_for_column(name, info):
    """
    Return a plausible sample value matching the column type for TPC-H tables.
    """
    sql_type = info["type"].upper()
    if "BIGINT" in sql_type:
        return random.randint(1, 1000)
    if "INT" in sql_type:
        return random.randint(1, 100)
    if "DECIMAL" in sql_type:
        # take a mid-range value with correct precision
        if "15,2" in sql_type:
            return Decimal("12345.67")
        # fallback
        return Decimal("1.23")
    if sql_type == "VARCHAR":
        # simple synthetic strings
        return f"{name}_sample"
    if sql_type == "DATE":
        return "2023-01-01"  # Spark will coerce string to date via schema
    if sql_type == "TIMESTAMP":
        return "2023-01-01T12:34:56"
    if sql_type == "DOUBLE":
        return 3.1415
    if sql_type == "BOOLEAN":
        return True
    # default
    return None


def coerce_value(name, info, raw_value):
    sql_type = info["type"].upper()
    if raw_value is None:
        return None
    if "DECIMAL" in sql_type:
        return raw_value if isinstance(raw_value, Decimal) else Decimal(str(raw_value))
    if sql_type == "TIMESTAMP":
        if isinstance(raw_value, str):
            return datetime.fromisoformat(raw_value.replace(" ", "T"))
        if isinstance(raw_value, datetime):
            return raw_value
        raise ValueError(f"Cannot coerce {raw_value!r} to datetime for {name}")
    if sql_type == "DATE":
        if isinstance(raw_value, str):
            return datetime.fromisoformat(raw_value).date()
        if isinstance(raw_value, datetime):
            return raw_value.date()
        # allow passing a date directly
        return raw_value
    if sql_type in ("BIGINT", "INT"):
        return int(raw_value)
    if sql_type == "DOUBLE":
        return float(raw_value)
    if sql_type == "BOOLEAN":
        return bool(raw_value) if raw_value is not None else None
    return str(raw_value)


def insert_edge_case_rows(spark):
    table_name = "edge_cases_test"
    full_table = f"rest.public.{table_name}"
    logger.info("→ Generating deterministic edge-case row for edge_cases_test")

    # raw edge-case literals
    raw_edge = {
        "pk": 1,
        "large_int": 2 ** 60,
        "negative_int": -1,
        "high_precision_dec": "0.123456789012345678",
        "zero_dec": "0.00",
        "empty_string": "",
        "unicode_string": "𝔘𝔫𝔦𝔠𝔬𝔡𝔢✓",
        "timestamp_min": "1970-01-01T00:00:00",
        "timestamp_max": "9999-12-31T23:59:59",
        "bool_true": True,
        "bool_null": None,
        "floating_nan": float("nan"),
        "floating_inf": float("inf"),
    }

    # build coerced row
    coerced_row = {
        name: coerce_value(name, info, raw_edge.get(name))
        for name, info in TABLE_METADATA[table_name].items()
    }

    edge_schema = make_spark_schema_from(TABLE_METADATA[table_name])

    tmp_view = f"tmp_{table_name}"
    df = spark.createDataFrame([coerced_row], schema=edge_schema)
    df.createOrReplaceTempView(tmp_view)

    logger.info(f"→ Inserting edge-case row into {full_table} from temp view {tmp_view}")
    spark.sql(f"INSERT INTO {table_name} SELECT * FROM {tmp_view}")


def insert_sample_tpch_rows(spark):
    """
    For each non-edge TPC-H table, create one synthetic row conforming to its schema and insert it.
    """
    for table_name in TABLE_METADATA:
        if table_name == "edge_cases_test":
            continue  # skip edge table here

        logger.info(f"→ Generating and inserting sample row for {table_name}")
        metadata = TABLE_METADATA[table_name]
        # build one row
        row = {
            col: sample_value_for_column(col, info)
            for col, info in metadata.items()
        }

        # Coerce decimals / timestamps using existing coerce_value to ensure compatibility
        coerced_row = {
            name: coerce_value(name, info, row.get(name))
            for name, info in metadata.items()
        }

        schema = make_spark_schema_from(metadata)
        temp_view = f"tmp_sample_{table_name}"
        df = spark.createDataFrame([coerced_row], schema=schema)
        df.createOrReplaceTempView(temp_view)


        spark.sql(f"INSERT INTO {table_name} SELECT * FROM {temp_view}")
        logger.info(f"Inserted sample row into {table_name}")


def create_spark_session():
    if not (AWS_REGION and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        logger.warning("AWS credentials or region missing; S3 access may fail.")
    builder = (
        SparkSession.builder
            .appName("embucket-tpch-load")
            .config("spark.driver.memory", "15g")
            .config("spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,"
                    "org.apache.hadoop:hadoop-aws:3.3.4")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID or "")
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY or "")
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com" if AWS_REGION else "")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
            .config("spark.sql.catalog.rest.uri", f"{CATALOG_URL}")
            .config("spark.sql.catalog.rest.warehouse", WAREHOUSE_ID)
            .config("spark.sql.defaultCatalog", "rest")
    )
    return builder.getOrCreate()


def validate_dataframe(df, schema, table_name):
    # check non-nullable
    for field in schema.fields:
        if not field.nullable:
            null_count = df.filter(df[field.name].isNull()).count()
            if null_count > 0:
                raise AssertionError(f"[Validation] Table {table_name}: column {field.name} has {null_count} null(s) but is declared NOT NULL")
    logger.info(f"[Validation] {table_name} passed non-nullable column check.")


def upload_table(spark, table_name, schema):
    raw_path = f"{TPC_H_DATA_PATH}/{table_name}/"
    logger.info(f"→ Loading {table_name} from {raw_path}")
    df = (
        spark.read
        .option("sep", "|")
        .schema(schema)
        .csv(raw_path, header=False)
    )
    validate_dataframe(df, schema, table_name)
    df.createOrReplaceTempView(table_name)
    logger.info(f"→ Inserting into Iceberg table rest.public.{table_name}")
    spark.sql(f"INSERT INTO {table_name} SELECT * FROM {table_name}")


def create_volume(embucket_client):
    logger.info(f"Creating S3 volume '{VOLUME}' -> bucket '{S3_BUCKET}'")
    embucket_client.create_s3_volume(
        volume_name=VOLUME,
        s3_bucket=S3_BUCKET,
        aws_region=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )


def ensure_database_and_schema(embucket_client):
    logger.info("Ensuring database and schema exist")
    embucket_client.sql(f"CREATE DATABASE IF NOT EXISTS tpc_h EXTERNAL_VOLUME = '{VOLUME}';")
    embucket_client.sql("CREATE SCHEMA IF NOT EXISTS tpc_h.public;")


def create_all_tables(embucket_client):
    for name, metadata in TABLE_METADATA.items():
        ddl = make_create_table_ddl(name, metadata, VOLUME, CATALOG_URL, BASE)
        logger.info(f"Creating table tpc_h.public.{name}")
        embucket_client.sql(ddl)

# Read the table and materialize rows (e.g., for edge_cases_test)
def fetch_rows_pyiceberg(pyiceberg, table_name, limit=10):
    logger.info(f"Fetching up to {limit} rows from {table_name} via PyIceberg")
    table = pyiceberg.read_table(table_name)  # this should give you an Iceberg Table object
    try:
        # Convert to pyarrow and then to Python dict
        arrow_table = table.to_pyarrow()  # if the wrapper exposes this
        batch = arrow_table.slice(0, limit)
        pandas_df = batch.to_pandas()
        logger.info(f"Read {len(pandas_df)} rows from {table_name}")
        print(pandas_df)
        return pandas_df
    except AttributeError:
        # Fallback: if read_table returns a scan object with a .to_pyarrow() method
        scan = table  # adjust if different
        arrow_table = scan.to_pyarrow()
        pandas_df = arrow_table.to_pandas().head(limit)
        logger.info(f"Read {len(pandas_df)} rows from {table_name}")
        print(pandas_df)
        return pandas_df
    except Exception as e:
        logger.error(f"Failed to fetch rows from {table_name}: {e}")
        raise


def validate_table_with_pyiceberg(pyiceberg, table_name):
    """
    Reads up to a few rows from Iceberg via pyiceberg, ensures at least one row exists,
    and checks that each column value conforms to the declared TABLE_METADATA types.
    """
    logger.info(f"→ Validating table {table_name} via PyIceberg")
    rows = pyiceberg.read_table(table_name, limit=5)  # returns list of dicts
    if not rows:
        raise AssertionError(f"No rows found in table {table_name}")

    metadata = TABLE_METADATA.get(table_name.split(".")[-1])  # e.g., "public.customer" -> "customer"
    if metadata is None:
        logger.warning(f"No metadata for {table_name}; skipping type validation.")
        return

    # Take first row for type checking
    row = rows[0]
    mismatches = []
    for col, info in metadata.items():
        expected_type = info["type"].upper()
        val = row.get(col)
        if val is None:
            if not info["nullable"]:
                mismatches.append(f"Column {col} is non-nullable but value is NULL")
            continue

        # Determine base type token (e.g., DECIMAL(15,2) -> DECIMAL)
        base = expected_type.split("(")[0]
        checker = TYPE_CHECKS.get(base)
        if checker:
            ok = checker(val)
            # Special handling: NaN is float but semantically okay for DOUBLE
            if base == "DOUBLE" and isinstance(val, float) and math.isnan(val):
                ok = True
            if not ok:
                mismatches.append(f"Column {col}: value {val!r} (type {type(val)}) does not match expected {base}")
        else:
            logger.debug(f"No checker for base type {base}, skipping {col}")

    if mismatches:
        for msg in mismatches:
            logger.error(msg)
        raise AssertionError(f"Type mismatches in table {table_name}: {mismatches}")

    logger.info(f"Table {table_name} passed validation (has {len(rows)} row(s) and types match).")


def main():
    try:
        embucket = EmbucketClient()
        # 1. create volume
        create_volume(embucket)

        # 2. ensure DB + schema
        ensure_database_and_schema(embucket)

        # 3. create tables
        create_all_tables(embucket)

        # 4. Spark session
        spark = create_spark_session()

        # 5. Load tables
        for tbl in TABLE_METADATA:
            schema = make_spark_schema_from(TABLE_METADATA[tbl])
            upload_table(spark, tbl, schema)

        # 6. insert sample rows for TPC-H tables
        insert_sample_tpch_rows(spark)

        # 7. insert edge-case rows
        insert_edge_case_rows(spark)

        # 8. Read back via PyIceberg to validate presence
        pyiceberg = PyIcebergClient(
            catalog_name="rest",
            warehouse_path='tpc_h',  # database name
            catalog_url=CATALOG_URL
        )

        all_errors = []

        for table in list(TABLE_METADATA.keys()):
            fq = f"public.{table}"
            logger.info(f"→ Reading back table {fq} via PyIceberg")
            try:
                validate_table_with_pyiceberg(pyiceberg, fq)
                logger.info(f"Successfully validated {fq}")
            except AssertionError as e:
                logger.error(f"Validation error for {fq}: {e}")
                all_errors.append((fq, str(e)))
            except Exception as e:
                logger.error(f"Unexpected error validating {fq}: {e}")
                all_errors.append((fq, f"Unexpected: {e}"))

        if all_errors:
            logger.error("Summary of validation failures:")
            for tbl, msg in all_errors:
                logger.error(f" - {tbl}: {msg}")
        else:
            logger.info("All tables passed validation.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Embucket API error: {e}")
        sys.exit(1)
    except AssertionError as ae:
        logger.error(f"Validation failed: {ae}")
        sys.exit(2)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()