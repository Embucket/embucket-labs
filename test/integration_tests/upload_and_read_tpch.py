import os
import logging
import random
import math
import pytest

from dotenv import load_dotenv
from decimal import Decimal
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
)
from clients import EmbucketClient, PyIcebergClient
from tables_metadata import TABLE_METADATA, TYPE_CHECKS

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tpch-loader")

S3_BUCKET = os.getenv("S3_BUCKET")

EMBUCKET_URL = "http://localhost:8080"
CATALOG_URL = "http://localhost:3000/catalog"
WAREHOUSE_ID = "test_db"
VOLUME = "test_s3_volume"
BASE = f"{WAREHOUSE_ID}/public"
TPC_H_DATA_PATH = f"s3a://{S3_BUCKET}/{BASE}"


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
    CREATE OR REPLACE TABLE {WAREHOUSE_ID}.public.{table_name}
      EXTERNAL_VOLUME = '{VOLUME}'
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
    """
    Converts raw input values to appropriate Python types based on SQL type definition.

    Args:
        name: Column name for error reporting
        info: Column metadata dictionary containing type information
        raw_value: The value to be coerced to the appropriate type

    Returns:
        Value converted to the appropriate Python type (Decimal, datetime, int, etc.)
        or None if raw_value is None
    """
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


def build_row(metadata, custom_values=None):
    """
    Build a row dictionary based on table metadata with appropriate type coercion.

    Args:
        metadata: Column metadata dictionary
        custom_values: Optional dictionary of custom values to override sample values

    Returns:
        Dictionary with column names as keys and coerced values as values
    """
    # Start with sample values or empty dict
    row = {}
    for col, info in metadata.items():
        if custom_values and col in custom_values:
            # Use custom value if provided
            row[col] = custom_values[col]
        else:
            # Otherwise use sample value
            row[col] = sample_value_for_column(col, info)

    # Coerce all values to correct types
    return {
        name: coerce_value(name, info, row.get(name))
        for name, info in metadata.items()
    }


def insert_row(spark, table_name, metadata, custom_values=None):
    """
    Insert a single row into a table.
    Args:
        spark: Spark session
        table_name: Name of the table
        metadata: Column metadata for the table
        custom_values: Optional dictionary of custom values to override sample values
    """
    # Build the row with appropriate values
    coerced_row = build_row(metadata, custom_values)

    # Create DataFrame and insert
    schema = make_spark_schema_from(metadata)
    temp_view = f"tmp_sample_{table_name}"
    df = spark.createDataFrame([coerced_row], schema=schema)
    df.createOrReplaceTempView(temp_view)

    spark.sql(f"INSERT INTO public.{table_name} SELECT * FROM {temp_view}")
    logger.info(f"Inserted row into {table_name}")


def insert_edge_case_rows(spark):
    """Use the generic insert_row function for edge cases"""
    table_name = "edge_cases_test"
    logger.info(f"→ Generating deterministic edge-case row for {table_name}")
    metadata = TABLE_METADATA[table_name]

    # Define edge case values
    edge_case_values = {
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

    insert_row(spark, table_name, metadata, edge_case_values)


def validate_dataframe(df, schema, table_name):
    """ Check non-nullable """
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


def validate_table_with_pyiceberg(pyiceberg_client, table_name):
    """
    Reads up to a few rows from Iceberg via pyiceberg, ensures at least one row exists,
    and checks that each column value conforms to the declared TABLE_METADATA types.
    """
    logger.info(f"→ Validating table {table_name} via PyIceberg")
    rows = pyiceberg_client.read_table(table_name, limit=5)  # returns list of dicts
    if not rows:
        raise AssertionError(f"No rows found in table {table_name}")

    metadata = TABLE_METADATA.get(table_name.split(".")[-1])
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
        ok = checker(val)
        # Special handling: NaN is float but semantically okay for DOUBLE
        if base == "DOUBLE" and isinstance(val, float) and math.isnan(val):
            ok = True
        if not ok:
            mismatches.append(f"Column {col}: value {val!r} (type {type(val)}) does not match expected {base}")


    if mismatches:
        for msg in mismatches:
            logger.error(msg)
        raise AssertionError(f"Type mismatches in table {table_name}: {mismatches}")

    logger.info(f"Table {table_name} passed validation (has {len(rows)} row(s) and types match).")


def insert_sample_row(spark, table_name, metadata):
    """Insert a single sample row into a table."""
    row = {col: sample_value_for_column(col, info) for col, info in metadata.items()}
    coerced_row = {
        name: coerce_value(name, info, row.get(name))
        for name, info in metadata.items()
    }
    schema = make_spark_schema_from(metadata)
    temp_view = f"tmp_sample_{table_name}"
    df = spark.createDataFrame([coerced_row], schema=schema)
    df.createOrReplaceTempView(temp_view)
    spark.sql(f"INSERT INTO public.{table_name} SELECT * FROM {temp_view}")

@pytest.fixture(scope="session", autouse=False)
def load_all_tables(s3_spark_session):
    """Load TPC-H data, and insert edge-case row"""
    spark = s3_spark_session
    emb = EmbucketClient()
    emb.sql(emb.get_volume_sql(VOLUME, "s3"))

    # ensure DB & schema exist
    emb.sql(f"CREATE DATABASE IF NOT EXISTS {WAREHOUSE_ID} EXTERNAL_VOLUME = {VOLUME};")
    emb.sql(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_ID}.public;")

    # for each table: DDL + data
    for table_name, metadata in TABLE_METADATA.items():
        ddl = make_create_table_ddl(table_name, metadata, VOLUME, CATALOG_URL, BASE)
        emb.sql(ddl)

        schema = make_spark_schema_from(metadata)
        if table_name == "edge_cases_test":
            insert_edge_case_rows(spark)
        else:
            upload_table(spark, table_name, schema)

        # Insert special data based on table type
        if table_name == "edge_cases_test":
            insert_edge_case_rows(spark)
        else:
            # For regular tables, insert sample TPC-H data
            insert_sample_row(spark, table_name, metadata)
    return

@pytest.fixture
def loaded_spark(load_all_tables):
    """Yields the Spark session after all tables are loaded."""
    return load_all_tables

@pytest.fixture
def pyiceberg_client():
    return PyIcebergClient(
        catalog_name="rest",
        warehouse_path=WAREHOUSE_ID
    )

@pytest.mark.parametrize(
    "table_name, metadata",
    [
        pytest.param(name, meta, id=name)
        for name, meta in TABLE_METADATA.items()
        if name != "edge_cases_test"
    ],
)
def test_read_tpch_tables_with_embucket(table_name, metadata):
    emb = EmbucketClient()
    full_name = f"{WAREHOUSE_ID}.public.{table_name}"
    result = emb.sql(f"SELECT * FROM {full_name}")
    rows = result["result"]["rows"]
    assert rows, f"No rows in {full_name}"

    cols = [c["name"] for c in result["result"]["columns"]]
    first = dict(zip(cols, rows[0]))

    # non‐nullable check + type checks
    for col, info in metadata.items():
        val = first[col]
        if val is None:
            assert info["nullable"], f"{col!r} in {table_name} is NULL but non‐nullable"
            continue

        base = info["type"].split("(")[0]
        # allow date‐string parsing
        if base == "DATE" and isinstance(val, str):
            datetime.strptime(val, "%Y-%m-%d")
            continue

        assert TYPE_CHECKS[base](val), (
            f"{table_name}.{col}: expected type {base}, got value {val!r} (type {type(val).__name__})"
        )


def test_edge_case_table_has_correct_values():
    """
    Using Embucket, verify the single edge_cases_test row and its special values.
    """
    emb = EmbucketClient()
    result = emb.sql("SELECT * FROM test_db.public.edge_cases_test")
    rows = result["result"]["rows"]
    assert rows, "No rows in edge_cases_test"

    cols = [c["name"] for c in result["result"]["columns"]]
    row = dict(zip(cols, rows[0]))

    # Integer edge cases
    assert row["pk"] == 1
    assert row["large_int"] == 2**60
    assert row["negative_int"] == -1

    # Decimal edge cases
    assert Decimal(str(row["high_precision_dec"])) == Decimal("0.123456789012345678")
    assert Decimal(str(row["zero_dec"])) == Decimal("0.00")

    # String edge cases
    assert row["empty_string"] == ""
    assert row["unicode_string"] == "𝔘𝔫𝔦𝔠𝔬𝔡𝔢✓"

    # Timestamp edge cases
    ts_min = datetime.fromisoformat(row["timestamp_min"].replace(" ", "T"))
    assert ts_min == datetime(1970, 1, 1, 0, 0, 0)

    ts_max = datetime.fromisoformat(row["timestamp_max"].replace(" ", "T"))
    assert ts_max == datetime(9999, 12, 31, 23, 59, 59)

    # Boolean edge cases
    assert row["bool_true"] is True
    assert row["bool_null"] is None

    # Floating-point edge cases (NaN and Infinity)
    nan_val = row["floating_nan"]
    # Either returned as None or a NaN float
    assert nan_val is None or math.isnan(float(nan_val)), f"Expected NaN or None, got {nan_val!r}"

    inf_val = row["floating_inf"]
    # Either returned as None or an infinite float
    assert inf_val is None or math.isinf(float(inf_val)), f"Expected Inf or None, got {inf_val!r}"


def test_read_tpch_with_pyiceberg(pyiceberg_client):
    for table_name in TABLE_METADATA:
        if table_name == "edge_cases_test":
            continue
        validate_table_with_pyiceberg(pyiceberg_client, f"public.{table_name}")


def test_read_edge_cases_with_pyiceberg(pyiceberg_client):
    validate_table_with_pyiceberg(pyiceberg_client, "public.edge_cases_test")