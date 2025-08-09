import os
import logging
import math
import pytest

from dotenv import load_dotenv
from decimal import Decimal
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
)
from clients import EmbucketClient, PyIcebergClient
from tables_metadata import TABLES_METADATA, TYPE_CHECKS, TPC_H_INSERTION_DATA, EDGE_CASES_VALUES
from tpc_h_queries import TPC_H_QUERIES

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


def make_create_table_ddl(table_name, metadata, catalog_url, base):
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


def build_row(metadata, custom_values=None):
    """
    Build a row dictionary based on table metadata with appropriate type coercion.

    Args:
        metadata: Column metadata dictionary
        custom_values: Optional dictionary of custom values to override sample values

    Returns:
        Dictionary with column names as keys and coerced values as values
    """
    row = {}
    # First pass: get raw values (sample or custom)
    for col, info in metadata.items():
        row[col] = custom_values[col]
    # Second pass: convert values to correct types
    result = {}
    for name, info in metadata.items():
        raw_value = row.get(name)
        sql_type = info["type"].upper()

        if raw_value is None:
            result[name] = None
            continue

        # Handle each type conversion directly
        if "DECIMAL" in sql_type:
            result[name] = raw_value if isinstance(raw_value, Decimal) else Decimal(str(raw_value))
        elif sql_type == "TIMESTAMP":
            if isinstance(raw_value, str):
                result[name] = datetime.fromisoformat(raw_value.replace(" ", "T"))
            elif isinstance(raw_value, datetime):
                result[name] = raw_value
            else:
                raise ValueError(f"Cannot coerce {raw_value!r} to datetime for {name}")
        elif sql_type == "DATE":
            if isinstance(raw_value, str):
                result[name] = datetime.fromisoformat(raw_value).date()
            elif isinstance(raw_value, datetime):
                result[name] = raw_value.date()
            else:
                result[name] = raw_value  # allow passing a date directly
        elif sql_type in ("BIGINT", "INT"):
            result[name] = int(raw_value)
        elif sql_type == "DOUBLE":
            result[name] = float(raw_value)
        elif sql_type == "BOOLEAN":
            result[name] = bool(raw_value) if raw_value is not None else None
        else:
            result[name] = str(raw_value)

    return result


def insert_row(spark, table_name, metadata, custom_values=None):
    """
    Insert a single row into a table and log the inserted row.
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
    metadata = TABLES_METADATA[table_name]
    insert_row(spark, table_name, metadata, EDGE_CASES_VALUES)


def insert_tpch_data(spark):
    """Insert data into TPC-H tables based on query requirements."""
    # Insert the data for each table
    for table_name, data_rows in TPC_H_INSERTION_DATA.items():
        metadata = TABLES_METADATA.get(table_name)
        if metadata is None:
            logger.warning(f"No metadata found for {table_name}, skipping")
            continue

        logger.info(f"Inserting {len(data_rows)} rows into {table_name}")
        for row in data_rows:
            insert_row(spark, table_name, metadata, row)


@pytest.fixture(scope="session", autouse=True)
def load_all_tables(s3_spark_session):
    """Load TPC-H data and edge case data"""
    spark = s3_spark_session
    emb = EmbucketClient()
    emb.sql(emb.get_volume_sql(VOLUME, "s3"))

    # ensure DB & schema exist
    emb.sql(f"CREATE DATABASE IF NOT EXISTS {WAREHOUSE_ID} EXTERNAL_VOLUME = {VOLUME};")
    emb.sql(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_ID}.public;")

    # for each table: DDL + data
    for table_name, metadata in TABLES_METADATA.items():
        ddl = make_create_table_ddl(table_name, metadata, CATALOG_URL, BASE)
        emb.sql(ddl)

    # Insert TPC-H data for regular queries
    insert_tpch_data(spark)

    # Insert edge case data separately
    insert_edge_case_rows(spark)
    return spark


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


@pytest.fixture
def pyiceberg_table_data(pyiceberg_client, request):
    """Fixture to read table data once and reuse across tests."""
    table_name = request.param
    full_name = f"public.{table_name}"
    rows = pyiceberg_client.read_table(full_name, limit=10)
    return {
        "table_name": table_name,
        "full_name": full_name,
        "rows": rows,
        "metadata": TABLES_METADATA.get(table_name)
    }


@pytest.fixture
def embucket_table_data(request):
    """Fixture to read Embucket table data once and reuse across tests."""
    emb = EmbucketClient()
    table_name = request.param
    full_name = f"{WAREHOUSE_ID}.public.{table_name}"
    result = emb.sql(f"SELECT * FROM {full_name}")
    rows = result["result"]["rows"]
    cols = [c["name"] for c in result["result"]["columns"]]

    # Convert rows of values to list of dicts
    dict_rows = [dict(zip(cols, row)) for row in rows]

    return {
        "table_name": table_name,
        "full_name": full_name,
        "rows": dict_rows,
        "metadata": TABLES_METADATA.get(table_name)
    }

# Regular TPC-H tables tests with Embucket
@pytest.mark.parametrize(
    "embucket_table_data",
    [name for name in TABLES_METADATA.keys() if name != "edge_cases_test"],
    indirect=True
)
def test_embucket_table_has_rows(embucket_table_data):
    """Ensure each table has at least one row when queried via Embucket."""
    assert len(embucket_table_data["rows"]) > 0, \
        f"Table {embucket_table_data['full_name']} has no rows in Embucket"


@pytest.mark.parametrize(
    "embucket_table_data",
    [name for name in TABLES_METADATA.keys() if name != "edge_cases_test"],
    indirect=True
)
def test_embucket_non_nullable_columns(embucket_table_data):
    """Check that non-nullable columns don't contain NULL values when queried via Embucket."""
    rows = embucket_table_data["rows"]
    metadata = embucket_table_data["metadata"]
    table_name = embucket_table_data["table_name"]

    for row in rows:
        for col, info in metadata.items():
            if not info["nullable"]:
                assert col in row, f"Column {col} missing in {table_name}"
                assert row[col] is not None, \
                    f"Column {col} in {table_name} is NULL but declared NOT NULL"


@pytest.mark.parametrize(
    "embucket_table_data",
    [name for name in TABLES_METADATA.keys() if name != "edge_cases_test"],
    indirect=True
)
def test_embucket_column_types(embucket_table_data):
    """Verify that column values match their expected types when queried via Embucket."""
    rows = embucket_table_data["rows"]
    metadata = embucket_table_data["metadata"]
    table_name = embucket_table_data["table_name"]

    for row in rows:
        for col, info in metadata.items():
            val = row.get(col)
            if val is None:
                continue

            expected_type = info["type"].upper()
            base = expected_type.split("(")[0]
            checker = TYPE_CHECKS.get(base)

            # Special handling for DATE strings in Embucket results
            if base == "DATE" and isinstance(val, str):
                try:
                    datetime.strptime(val, "%Y-%m-%d")
                    continue
                except ValueError:
                    assert False, f"Invalid date format for {col} in {table_name}: {val}"

            assert checker is not None, f"No type checker for {base} in {table_name}"
            assert checker(val), \
                f"Column {col} in {table_name}: value {val!r} (type {type(val)}) " \
                f"does not match expected {base}"

# Edge cases tests with Embucket
@pytest.mark.parametrize("embucket_table_data", ["edge_cases_test"], indirect=True)
def test_embucket_edge_table_has_rows(embucket_table_data):
    """Ensure the edge case table has at least one row when queried via Embucket."""
    assert len(embucket_table_data["rows"]) > 0, "Edge cases table has no rows in Embucket"


@pytest.mark.parametrize("embucket_table_data", ["edge_cases_test"], indirect=True)
def test_embucket_integer_edge_cases(embucket_table_data):
    """Test integer edge cases (large integers, negative values) via Embucket."""
    row = embucket_table_data["rows"][0]
    assert row["pk"] == 1, f"Expected pk=1, got {row['pk']}"
    assert row["large_int"] == 2**60, f"Expected large_int=2^60, got {row['large_int']}"
    assert row["negative_int"] == -1, f"Expected negative_int=-1, got {row['negative_int']}"


@pytest.mark.parametrize("embucket_table_data", ["edge_cases_test"], indirect=True)
def test_embucket_decimal_edge_cases(embucket_table_data):
    """Test decimal edge cases (high precision, zero values) via Embucket."""
    row = embucket_table_data["rows"][0]
    assert Decimal(str(row["high_precision_dec"])) == Decimal("0.12345678901234568")
    assert Decimal(str(row["zero_dec"])) == Decimal("0.00")


@pytest.mark.parametrize("embucket_table_data", ["edge_cases_test"], indirect=True)
def test_embucket_string_edge_cases(embucket_table_data):
    """Test string edge cases (empty strings, Unicode characters) via Embucket."""
    row = embucket_table_data["rows"][0]
    assert row["empty_string"] == "", "Empty string not preserved"
    assert row["unicode_string"] == "𝔘𝔫𝔦𝔠𝔬𝔡𝔢✓", "Unicode characters not preserved"


@pytest.mark.parametrize("embucket_table_data", ["edge_cases_test"], indirect=True)
def test_embucket_timestamp_edge_cases(embucket_table_data):
    """Test timestamp edge cases (min/max values) via Embucket."""
    row = embucket_table_data["rows"][0]
    # Handle timestamp_min/max which is returned as integer (epoch time in milliseconds or microseconds)
    ts_min_val = row["timestamp_min"]
    assert ts_min_val == 0 or ts_min_val == 0.0, \
        f"Expected timestamp_min to be Unix epoch 0, got {ts_min_val}"
    ts_max_val = row["timestamp_max"]
    # 253402300799000000 represents 9999-12-31T23:59:59 in microseconds
    assert ts_max_val == 253402300799000000, \
        f"Expected timestamp_max to be 253402300799000000, got {ts_max_val}"


@pytest.mark.parametrize("embucket_table_data", ["edge_cases_test"], indirect=True)
def test_embucket_boolean_edge_cases(embucket_table_data):
    """Test boolean edge cases (true values, null values) via Embucket."""
    row = embucket_table_data["rows"][0]
    assert row["bool_true"] is True
    assert row["bool_null"] is None


@pytest.mark.parametrize("embucket_table_data", ["edge_cases_test"], indirect=True)
def test_embucket_floating_point_edge_cases(embucket_table_data):
    """Test floating point edge cases (NaN, Infinity) via Embucket."""
    row = embucket_table_data["rows"][0]
    nan_val = row["floating_nan"]
    assert nan_val is None or math.isnan(float(nan_val)), f"Expected NaN or None, got {nan_val!r}"

    inf_val = row["floating_inf"]
    assert inf_val is None or math.isinf(float(inf_val)), f"Expected Inf or None, got {inf_val!r}"

# Regular TPC-H tables pyiceberg tests:

@pytest.mark.parametrize(
    "pyiceberg_table_data",
    [name for name in TABLES_METADATA.keys() if name != "edge_cases_test"],
    indirect=True
)
def test_pyiceberg_table_has_rows(pyiceberg_table_data):
    """Ensure each table has at least one row."""
    assert len(pyiceberg_table_data["rows"]) > 0, \
        f"Table {pyiceberg_table_data['full_name']} has no rows"


@pytest.mark.parametrize(
    "pyiceberg_table_data",
    [name for name in TABLES_METADATA.keys() if name != "edge_cases_test"],
    indirect=True
)
def test_pyiceberg_non_nullable_columns(pyiceberg_table_data):
    """Check that non-nullable columns don't contain NULL values."""
    rows = pyiceberg_table_data["rows"]
    metadata = pyiceberg_table_data["metadata"]
    table_name = pyiceberg_table_data["table_name"]

    for row in rows:
        for col, info in metadata.items():
            if not info["nullable"]:
                assert col in row, f"Column {col} missing in {table_name}"
                assert row[col] is not None, \
                    f"Column {col} in {table_name} is NULL but declared NOT NULL"


@pytest.mark.parametrize(
    "pyiceberg_table_data",
    [name for name in TABLES_METADATA.keys() if name != "edge_cases_test"],
    indirect=True
)
def test_pyiceberg_column_types(pyiceberg_table_data):
    """Verify that column values match their expected types."""
    rows = pyiceberg_table_data["rows"]
    metadata = pyiceberg_table_data["metadata"]
    table_name = pyiceberg_table_data["table_name"]

    for row in rows:
        for col, info in metadata.items():
            val = row.get(col)
            if val is None:
                continue

            expected_type = info["type"].upper()
            base = expected_type.split("(")[0]
            checker = TYPE_CHECKS.get(base)

            assert checker is not None, f"No type checker for {base} in {table_name}"
            assert checker(val), \
                f"Column {col} in {table_name}: value {val!r} (type {type(val)}) " \
                f"does not match expected {base}"

# Edge cases tests
@pytest.mark.parametrize("pyiceberg_table_data", ["edge_cases_test"], indirect=True)
def test_pyiceberg_edge_table_has_rows(pyiceberg_table_data):
    """Ensure the edge case table has at least one row."""
    assert len(pyiceberg_table_data["rows"]) > 0, "Edge cases table has no rows"


@pytest.mark.parametrize("pyiceberg_table_data", ["edge_cases_test"], indirect=True)
def test_pyiceberg_integer_edge_cases(pyiceberg_table_data):
    """Test integer edge cases (large integers, negative values)."""
    row = pyiceberg_table_data["rows"][0]
    assert row["pk"] == 1, f"Expected pk=1, got {row['pk']}"
    assert row["large_int"] == 2**60, f"Expected large_int=2^60, got {row['large_int']}"
    assert row["negative_int"] == -1, f"Expected negative_int=-1, got {row['negative_int']}"


@pytest.mark.parametrize("pyiceberg_table_data", ["edge_cases_test"], indirect=True)
def test_pyiceberg_decimal_edge_cases(pyiceberg_table_data):
    """Test decimal edge cases (high precision, zero values)."""
    row = pyiceberg_table_data["rows"][0]
    assert Decimal(str(row["high_precision_dec"])) == Decimal("0.12345678901234568")
    assert Decimal(str(row["zero_dec"])) == Decimal("0.00")


@pytest.mark.parametrize("pyiceberg_table_data", ["edge_cases_test"], indirect=True)
def test_pyiceberg_string_edge_cases(pyiceberg_table_data):
    """Test string edge cases (empty strings, Unicode characters)."""
    row = pyiceberg_table_data["rows"][0]
    assert row["empty_string"] == "", "Empty string not preserved"
    assert row["unicode_string"] == "𝔘𝔫𝔦𝔠𝔬𝔡𝔢✓", "Unicode characters not preserved"


@pytest.mark.parametrize("pyiceberg_table_data", ["edge_cases_test"], indirect=True)
def test_pyiceberg_timestamp_edge_cases(pyiceberg_table_data):
    """Test timestamp edge cases (min/max values) via PyIceberg."""
    row = pyiceberg_table_data["rows"][0]
    ts_min_val = row["timestamp_min"]
    assert ts_min_val.year == 1970 and ts_min_val.month == 1 and ts_min_val.day == 1, \
        f"Expected timestamp_min to be 1970-01-01, got {ts_min_val}"
    # Handle timestamp_max
    ts_max_val = row["timestamp_max"]
    assert ts_max_val.year == 9999 and ts_max_val.month == 12 and ts_max_val.day == 31, \
        f"Expected timestamp_max to be 9999-12-31, got {ts_max_val}"


@pytest.mark.parametrize("pyiceberg_table_data", ["edge_cases_test"], indirect=True)
def test_pyiceberg_boolean_edge_cases(pyiceberg_table_data):
    """Test boolean edge cases (true values, null values)."""
    row = pyiceberg_table_data["rows"][0]
    assert row["bool_true"] is True
    assert row["bool_null"] is None


@pytest.mark.parametrize("pyiceberg_table_data", ["edge_cases_test"], indirect=True)
def test_pyiceberg_floating_point_edge_cases(pyiceberg_table_data):
    """Test floating point edge cases (NaN, Infinity)."""
    row = pyiceberg_table_data["rows"][0]
    nan_val = row["floating_nan"]
    assert nan_val is None or math.isnan(float(nan_val)), f"Expected NaN or None, got {nan_val!r}"

    inf_val = row["floating_inf"]
    assert inf_val is None or math.isinf(float(inf_val)), f"Expected Inf or None, got {inf_val!r}"

# TPC-H queries with current database used
tpc_h_queries = [q.replace("tpc_h", WAREHOUSE_ID) for q in TPC_H_QUERIES]

@pytest.mark.parametrize("query_idx", range(len(tpc_h_queries)))
def test_tpch_queries_with_embucket(query_idx, load_all_tables):
    """Test TPC-H queries execution with Embucket client."""
    emb = EmbucketClient()
    query = tpc_h_queries[query_idx]

    logger.info(f"Executing TPC-H query {query}")
    result = emb.sql(query)

    assert "result" in result, f"Query {query_idx + 1} did not return a result"
    assert "rows" in result["result"], f"Query {query_idx + 1} did not return rows"
    assert len(result["result"]["rows"]) > 0, f"Query {query_idx + 1} returned empty result"

    row_count = len(result["result"]["rows"])
    col_count = len(result["result"]["columns"])
    logger.info(f"Query {query_idx + 1} returned {row_count} rows with {col_count} columns")