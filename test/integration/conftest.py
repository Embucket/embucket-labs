import os
import uuid
from typing import Any, Callable
import pytest
from dotenv import load_dotenv
from pathlib import Path

from utils import (
    _get,
    create_embucket_connection,
    _load_dataset_fixture,
    EmbucketEngine,
    SparkEngine,
    MetricsRecorder,
)

load_dotenv()


@pytest.fixture(scope="session")
def test_run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="session")
def embucket_exec() -> Callable[[str], Any]:
    """Return a function to execute SQL against Embucket."""
    conn = create_embucket_connection()

    def _exec(sql: str) -> Any:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    return _exec


@pytest.fixture(scope="session", autouse=True)
def embucket_bootstrap(embucket_exec):
    """Health-check Embucket and ensure external volume/database/schema exist.

    Controlled by env vars:
      - EMBUCKET_DATABASE: database name to create/use
      - EMBUCKET_SCHEMA: schema name to create/use
      - EMBUCKET_EXTERNAL_VOLUME: volume name to create (default: 'minio_vol')
      - S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET: volume config
    """
    # Basic health check
    try:
        embucket_exec("SELECT 1")
    except Exception as e:
        pytest.skip(f"Embucket health check failed: {e}")

    db = _get("EMBUCKET_DATABASE", "analytics")
    schema = _get("EMBUCKET_SCHEMA", "public")
    vol = _get("EMBUCKET_EXTERNAL_VOLUME", "minio_vol")
    endpoint = _get("S3_ENDPOINT", "http://localhost:9000")
    ak = _get("S3_ACCESS_KEY", "AKIAIOSFODNN7EXAMPLE")
    sk = _get("S3_SECRET_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    bucket = _get("S3_BUCKET", "embucket")
    local_base_path = _get("LOCAL_BASE_PATH", os.getcwd())

    # Create external volume if we have enough info
    embucket_exec(
        f"""CREATE EXTERNAL VOLUME IF NOT EXISTS {vol} STORAGE_LOCATIONS = ((NAME = '{vol}' STORAGE_PROVIDER = 's3' STORAGE_ENDPOINT = '{endpoint}' STORAGE_BASE_URL = '{bucket}' CREDENTIALS = (AWS_KEY_ID='{ak}' AWS_SECRET_KEY='{sk}' REGION='us-east-1')));
"""
    )
    # Create local volume to enable COPY INTO for embucket
    # create external volume if not exists local STORAGE_LOCATIONS = (( NAME = 'local' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '/Users/ramp/vcs/embucket/test
    #    /integration' ));
    embucket_exec(
        f"""CREATE EXTERNAL VOLUME IF NOT EXISTS local STORAGE_LOCATIONS = ((NAME = 'local' STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '{local_base_path}'));
"""
    )
    embucket_exec(f"CREATE DATABASE IF NOT EXISTS {db} EXTERNAL_VOLUME = '{vol}'")
    embucket_exec(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")


@pytest.fixture(scope="session")
def spark() -> Any:
    """Create a SparkSession configured for Iceberg REST via Embucket and S3A (MinIO).

    Skips if pyspark is not available or required env vars are missing.
    """
    # Provide sane defaults for local dev
    rest_uri = _get("EMBUCKET_ICEBERG_REST_URI", "http://localhost:3000/catalog")
    warehouse = _get("EMBUCKET_DATABASE", "analytics")
    s3_endpoint = _get("S3_ENDPOINT", "http://localhost:9000")
    s3_key = _get("S3_ACCESS_KEY", "AKIAIOSFODNN7EXAMPLE")
    s3_secret = _get("S3_SECRET_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

    import findspark  # type: ignore

    findspark.init()
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("embucket-integration-tests")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1",
        )
        .config("spark.sql.catalog.emb", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.emb.catalog-impl", "org.apache.iceberg.rest.RESTCatalog"
        )
        .config("spark.sql.catalog.emb.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.emb.uri", rest_uri)
        .config("spark.sql.catalog.emb.warehouse", warehouse)
        .config("spark.sql.catalog.emb.cache-enabled", "false")
        .config("spark.sql.catalog.emb.s3.access-key-id", s3_key)
        .config("spark.sql.catalog.emb.s3.secret-access-key", s3_secret)
        .config("spark.sql.catalog.emb.s3.signing-region", "us-east-2")
        .config("spark.sql.catalog.emb.s3.sigv4-enabled", "true")
        .config("spark.sql.catalog.emb.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.emb.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "emb")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )

    try:
        spark = builder.getOrCreate()
    except Exception as e:
        pytest.skip(f"Failed to start Spark with Iceberg/MinIO config: {e}")

    return spark


@pytest.fixture(scope="session")
def embucket_engine(embucket_exec) -> EmbucketEngine:
    return EmbucketEngine(embucket_exec)


@pytest.fixture(scope="session")
def spark_engine(spark) -> SparkEngine:
    return SparkEngine(spark, catalog_alias="emb")


# NYC Taxi Dataset Fixtures
@pytest.fixture(scope="session")
def nyc_yellow_taxi(request, test_run_id):
    """Parameterized NYC taxi fixture that accepts engine type.

    Use with indirect=True parametrization:
    @pytest.mark.parametrize('nyc_taxi', ['spark', 'embucket'], indirect=True)
    """
    engine_type = request.param
    if engine_type not in ["spark", "embucket"]:
        raise ValueError(
            f"Unknown engine type: {engine_type}. Use 'spark' or 'embucket'."
        )

    engine = request.getfixturevalue(f"{engine_type}_engine")
    return _load_dataset_fixture("nyc_taxi_yellow", engine, test_run_id, engine_type)


@pytest.fixture(scope="session")
def nyc_green_taxi(request, test_run_id):
    """Parameterized NYC green taxi fixture that accepts engine type.

    Use with indirect=True parametrization:
    @pytest.mark.parametrize('nyc_taxi_green', ['spark', 'embucket'], indirect=True)
    """
    engine_type = request.param
    if engine_type not in ["spark", "embucket"]:
        raise ValueError(
            f"Unknown engine type: {engine_type}. Use 'spark' or 'embucket'."
        )

    engine = request.getfixturevalue(f"{engine_type}_engine")
    return _load_dataset_fixture("nyc_taxi_green", engine, test_run_id, engine_type)


@pytest.fixture(scope="session")
def fhv(request, test_run_id):
    """Parameterized NYC green taxi fixture that accepts engine type.

    Use with indirect=True parametrization:
    @pytest.mark.parametrize('nyc_taxi_green', ['spark', 'embucket'], indirect=True)
    """
    engine_type = request.param
    if engine_type not in ["spark", "embucket"]:
        raise ValueError(
            f"Unknown engine type: {engine_type}. Use 'spark' or 'embucket'."
        )

    engine = request.getfixturevalue(f"{engine_type}_engine")
    return _load_dataset_fixture("fhv", engine, test_run_id, engine_type)


# TPC-H Dataset Fixtures
@pytest.fixture(scope="session")
def tpch_table(request, test_run_id):
    """Parameterized TPC-H table fixture that accepts (table_name, engine_type) tuple.

    Use with indirect=True parametrization:
    @pytest.mark.parametrize('tpch_table', [('lineitem', 'spark'), ('orders', 'embucket')], indirect=True)
    """
    table_name, engine_type = request.param
    dataset_name = f"tpch_{table_name}"

    if engine_type not in ["spark", "embucket"]:
        raise ValueError(
            f"Unknown engine type: {engine_type}. Use 'spark' or 'embucket'."
        )

    engine = request.getfixturevalue(f"{engine_type}_engine")
    return _load_dataset_fixture(dataset_name, engine, test_run_id, engine_type)


@pytest.fixture(scope="session")
def tpch_full(request, test_run_id):
    """Parameterized TPC-H complete dataset fixture that accepts engine type.

    Loads all 8 TPC-H tables with the specified engine and returns as dict.
    Use with indirect=True parametrization:
    @pytest.mark.parametrize('tpch_full', ['spark', 'embucket'], indirect=True)
    """
    engine_type = request.param
    tables = [
        "lineitem",
        "orders",
        "part",
        "supplier",
        "customer",
        "nation",
        "region",
        "partsupp",
    ]

    if engine_type not in ["spark", "embucket"]:
        raise ValueError(
            f"Unknown engine type: {engine_type}. Use 'spark' or 'embucket'."
        )

    engine = request.getfixturevalue(f"{engine_type}_engine")

    # Load all tables with the specified engine
    loaded_tables = {}
    for table in tables:
        dataset_name = f"tpch_{table}"
        loaded_tables[table] = _load_dataset_fixture(
            dataset_name, engine, test_run_id, engine_type
        )

    return loaded_tables


@pytest.fixture(scope="session")
def tpcds_full(request, test_run_id):
    """Parameterized TPC-DS complete dataset fixture that accepts engine type.

    Loads all TPC-DS tables with the specified engine and returns as dict.
    Use with indirect=True parametrization:
    @pytest.mark.parametrize('tpcds_full', ['spark', 'embucket'], indirect=True)
    """
    engine_type = request.param
    tables = [
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    ]

    if engine_type not in ["spark", "embucket"]:
        raise ValueError(
            f"Unknown engine type: {engine_type}. Use 'spark' or 'embucket'."
        )

    engine = request.getfixturevalue(f"{engine_type}_engine")

    # Load all tables with the specified engine
    loaded_tables = {}
    for table in tables:
        dataset_name = f"tpcds_{table}"
        loaded_tables[table] = _load_dataset_fixture(
            dataset_name, engine, test_run_id, engine_type
        )

    return loaded_tables


@pytest.fixture(scope="session")
def clickbench_hits(request, test_run_id):
    """Parameterized Clickbench hits fixture that accepts engine type.

    Use with indirect=True parametrization:
    @pytest.mark.parametrize('clickbench_hits', ['spark', 'embucket'], indirect=True)
    """
    engine_type = request.param
    if engine_type not in ["spark", "embucket"]:
        raise ValueError(
            f"Unknown engine type: {engine_type}. Use 'spark' or 'embucket'."
        )

    engine = request.getfixturevalue(f"{engine_type}_engine")
    return _load_dataset_fixture("clickbench_hits", engine, test_run_id, engine_type)


@pytest.fixture(scope="session")
def metrics_recorder(test_run_id) -> MetricsRecorder:
    # You can override paths via env if you like
    artifacts_dir = Path(os.getenv("INTEGRATION_ARTIFACTS_DIR", "artifacts"))
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    rec = MetricsRecorder(artifacts_dir / f"metrics_{test_run_id}.csv", test_run_id)
    yield rec
    rec.flush()
