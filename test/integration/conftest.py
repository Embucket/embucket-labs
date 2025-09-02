import os
import uuid
from dataclasses import dataclass
from typing import Dict, Any, Callable, List, Optional, Tuple

import pytest
from dotenv import load_dotenv

load_dotenv()


def _get(key: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(key)
    return val if val is not None else default


@pytest.fixture(scope="session")
def test_run_id() -> str:
    return uuid.uuid4().hex[:8]


@dataclass
class DatasetConfig:
    name: str
    namespace: str
    table: str
    ddl: Dict[str, str]
    format: str
    sources: List[str]
    options: Dict[str, Any]
    first_col: Optional[str] = None
    numeric_col: Optional[str] = None
    queries: Optional[List[Dict[str, Any]]] = (
        None  # [{id: str, sql: str}|{id, sql_path}]
    )

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DatasetConfig":
        return DatasetConfig(
            name=d["name"],
            namespace=d["namespace"],
            table=d.get("table", d["name"]),
            ddl=d["ddl"],
            format=d.get("format", "parquet"),
            sources=d.get("sources", []),
            options=d.get("options", {}) or {},
            first_col=d.get("first_col"),
            numeric_col=d.get("numeric_col"),
            queries=d.get("queries"),
        )


def load_sql_file(path: str) -> str:
    """Load SQL content from file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def create_embucket_connection():
    """Create Embucket connection with environment-based config."""
    try:
        import snowflake.connector as sf
    except Exception as e:
        pytest.skip(f"snowflake-connector-python not available: {e}")

    # Connection config with defaults
    host = _get("EMBUCKET_SQL_HOST", "localhost")
    port = _get("EMBUCKET_SQL_PORT", "3000")
    protocol = _get("EMBUCKET_SQL_PROTOCOL", "http")
    user = _get("EMBUCKET_USER", "embucket")
    password = _get("EMBUCKET_PASSWORD", "embucket")
    account = os.getenv("EMBUCKET_ACCOUNT") or f"acc_{uuid.uuid4().hex[:10]}"
    database = _get("EMBUCKET_DATABASE", "analytics")
    schema = _get("EMBUCKET_SCHEMA", "public")

    connect_args = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": "embucket",
        "host": host,
        "protocol": protocol,
        "port": int(port) if port else 3000,
    }

    try:
        conn = sf.connect(**connect_args)
        if database:
            conn.cursor().execute(f"USE DATABASE {database}")
        if schema:
            conn.cursor().execute(f"USE SCHEMA {schema}")
        return conn
    except Exception as e:
        pytest.skip(f"Failed to connect to Embucket: {e}")


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


def _normalize_value(v: Any) -> Any:
    import decimal
    from datetime import datetime, date

    if v is None:
        return (0, None)
    if isinstance(v, (bool,)):
        return (1, bool(v))
    if isinstance(v, (int,)):
        return (2, int(v))
    if isinstance(v, (float,)):
        # round for sort key; actual compare uses tolerance
        return (3, round(float(v), 12))
    if isinstance(v, decimal.Decimal):
        return (4, str(v))
    if isinstance(v, (datetime, date)):
        return (5, v.isoformat())
    if isinstance(v, (bytes, bytearray)):
        return (6, v.hex())
    # Fallback to string repr for order stability
    return (9, str(v))


def _sort_rows(rows: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
    return sorted(rows, key=lambda row: tuple(_normalize_value(v) for v in row))


def _rows_to_tuples(rows: Any) -> List[Tuple[Any, ...]]:
    out: List[Tuple[Any, ...]] = []
    for r in rows:
        if isinstance(r, (tuple, list)):
            out.append(tuple(r))
        else:
            # Spark Row has .asDict(); but tuple(r) works too
            try:
                out.append(tuple(r))
            except TypeError:
                try:
                    d = r.asDict(recursive=True)
                    out.append(tuple(d.values()))
                except Exception:
                    out.append((r,))
    return out


def _is_number(x: Any) -> bool:
    import decimal

    return isinstance(x, (int, float, decimal.Decimal))


def _render_sql_with_table(sql: str, table_fqn: str) -> str:
    return sql.replace("{{TABLE_FQN}}", table_fqn)


def render_sql_with_aliases(sql: str, alias_to_fqn: Dict[str, str]) -> str:
    out = sql
    for alias, fqn in alias_to_fqn.items():
        out = out.replace(f"{{{{TABLE:{alias}}}}}", fqn)
    return out


def _load_dataset_fixture(
    dataset_name: str, engine, test_run_id: str, engine_name: str
):
    """Unified helper function to load datasets for fixtures.

    Args:
        dataset_name: Name of dataset in datasets.yaml
        engine: Engine instance (spark_engine or embucket_engine)
        test_run_id: Unique test run identifier
        engine_name: Engine name for table naming ("spark" or "embucket")

    Returns:
        Tuple of (dataset, table_name, engine)
    """
    import yaml

    with open("datasets.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    dataset_data = next(d for d in cfg["datasets"] if d["name"] == dataset_name)
    dataset = DatasetConfig.from_dict(dataset_data)

    # Create unique table name
    table_name = f"{dataset.table}_{test_run_id}_{engine_name}"
    engine.create_table(dataset, table_name)
    engine.load_data(dataset, table_name)
    return (dataset, table_name, engine)


def compare_result_sets(
    a: List[Tuple[Any, ...]],
    b: List[Tuple[Any, ...]],
    rel_tol: float = 1e-5,
    abs_tol: float = 1e-8,
    metrics_recorder=None,
    query_id=None,
) -> Tuple[bool, str]:
    """Compare two result sets with type tolerance and order-insensitive.

    Returns (ok, message). On failure, message contains a small diff.
    If metrics_recorder and query_id are provided, detailed mismatch info is recorded.
    """
    mismatches = []

    # Check row count
    if len(a) != len(b):
        mismatch_info = {
            "type": "row_count_mismatch",
            "spark_rows": len(a),
            "embucket_rows": len(b),
        }
        if metrics_recorder and query_id:
            metrics_recorder.add_mismatch(query_id, mismatch_info)
        return False, f"Row count differs: {len(a)} vs {len(b)}"

    sa = _sort_rows(a)
    sb = _sort_rows(b)
    import math

    # Check row data
    for i, (ra, rb) in enumerate(zip(sa, sb)):
        row_mismatches = {}

        if len(ra) != len(rb):
            mismatch_info = {
                "type": "row_structure_mismatch",
                "row_index": i,
                "spark_columns": len(ra),
                "embucket_columns": len(rb),
            }
            mismatches.append(mismatch_info)
            if len(mismatches) >= 10:  # Limit number of mismatches collected
                break
            continue

        for j, (va, vb) in enumerate(zip(ra, rb)):
            if va == vb:
                continue

            # Handle numeric approx
            if _is_number(va) and _is_number(vb):
                if math.isclose(float(va), float(vb), rel_tol=rel_tol, abs_tol=abs_tol):
                    continue
                row_mismatches[j] = {
                    "spark_value": va,
                    "embucket_value": vb,
                    "type": "numeric_mismatch",
                }
                continue

            # Normalize None vs empty string edge cases cautiously
            if va in (None, "") and vb in (None, ""):
                continue

            if str(va) != str(vb):
                row_mismatches[j] = {
                    "spark_value": va,
                    "embucket_value": vb,
                    "type": "value_mismatch",
                }

        if row_mismatches:
            mismatches.append(
                {"type": "data_mismatch", "row_index": i, "columns": row_mismatches}
            )

            if len(mismatches) >= 10:  # Limit number of mismatches collected
                break

    if mismatches:
        if metrics_recorder and query_id:
            metrics_recorder.add_mismatch(
                query_id,
                {
                    "total_mismatches": len(mismatches),
                    "details": mismatches[:10],  # Limit to first 10 for reasonable size
                },
            )

        # Format first mismatch for error message
        first = mismatches[0]
        if first["type"] == "row_structure_mismatch":
            msg = f"Row {first['row_index']} length differs: {first['spark_columns']} vs {first['embucket_columns']}"
        elif first["type"] == "data_mismatch":
            col_idx = next(iter(first["columns"]))
            col_info = first["columns"][col_idx]
            msg = f"Row {first['row_index']}, col {col_idx} differs: {col_info['spark_value']} vs {col_info['embucket_value']}"
        else:
            msg = f"Data mismatch in {len(mismatches)} rows"

        return False, msg

    return True, "OK"


class EmbucketEngine:
    def __init__(self, exec_fn: Callable[[str], Any]):
        self._exec = exec_fn

    def table_fqn(self, dataset: DatasetConfig, table_name: str) -> str:
        # Embucket uses current DB/SCHEMA; unqualified table is fine.
        _ = dataset  # Keep parameter for interface consistency
        return table_name

    def create_table(self, dataset: DatasetConfig, table_name: str) -> None:
        ddl_path = dataset.ddl["embucket"]
        sql = load_sql_file(ddl_path)
        # Drop if exists to avoid duplicate loads across tests
        try:
            self._exec(f"DROP TABLE IF EXISTS {self.table_fqn(dataset, table_name)}")
        except Exception:
            pass
        sql = _render_sql_with_table(sql, self.table_fqn(dataset, table_name))
        self._exec(sql)

    def load_data(self, dataset: DatasetConfig, table_name: str) -> None:
        """Load data using COPY INTO."""
        table_fqn = self.table_fqn(dataset, table_name)
        fmt = (dataset.format or "parquet").lower()
        if fmt not in ("parquet", "csv", "tsv"):
            raise ValueError(f"Unsupported format for COPY INTO: {fmt}")
        options = dict(dataset.options or {})
        if fmt == "tsv":
            options = {**options, "FIELD_DELIMITER": "\\t"}
        if fmt == "csv":
            # default delimiter comma; allow override
            pass

        # Normalize some common option keys for Snowflake-like COPY INTO
        if "field_delimiter" in options and "FIELD_DELIMITER" not in options:
            options["FIELD_DELIMITER"] = options.pop("field_delimiter")
        if "quote" in options and "QUOTE" not in options:
            options["QUOTE"] = options.pop("quote")
        if "escape" in options and "ESCAPE" not in options:
            options["ESCAPE"] = options.pop("escape")
        if "header" in options and "HEADER" not in options:
            # Prefer HEADER=true/false if acceptable; fallback left as-is
            val = options.pop("header")
            options["HEADER"] = str(bool(val)).lower()

        ff_parts = [f"TYPE = {fmt.upper()}"]
        for k, v in options.items():
            # string-quote non-numeric values
            vv = v if isinstance(v, (int, float)) else f"'{v}'"
            ff_parts.append(f"{k} = {vv}")
        ff = ", ".join(ff_parts)

        for uri in dataset.sources:
            local_base_path = _get("LOCAL_BASE_PATH", os.getcwd())
            sql = f"COPY INTO {table_fqn} FROM 'file://{local_base_path}/{uri}' STORAGE_INTEGRATION = local FILE_FORMAT = ({ff})"
            self._exec(sql)

    def sql(
        self, sql: str, alias_to_table: Dict[str, Tuple[DatasetConfig, str]]
    ) -> List[Tuple[Any, ...]]:
        alias_to_fqn = {
            alias: self.table_fqn(ds, table)
            for alias, (ds, table) in alias_to_table.items()
        }
        rendered_sql = render_sql_with_aliases(sql, alias_to_fqn)
        rows = self._exec(rendered_sql) or []
        return _rows_to_tuples(rows)


class SparkEngine:
    def __init__(self, spark_sess: Any, catalog_alias: str = "emb"):
        self.spark = spark_sess
        self.catalog_alias = catalog_alias

    def table_fqn(self, dataset: DatasetConfig, table_name: str) -> str:
        return f"{self.catalog_alias}.{dataset.namespace}.{table_name}"

    def create_table(self, dataset: DatasetConfig, table_name: str) -> None:
        ddl_path = dataset.ddl["spark"]
        sql = load_sql_file(ddl_path)
        # Drop if exists for idempotence
        try:
            self.spark.sql(
                f"DROP TABLE IF EXISTS {self.table_fqn(dataset, table_name)}"
            )
        except Exception:
            pass
        sql = _render_sql_with_table(sql, self.table_fqn(dataset, table_name))
        self.spark.sql(sql)

    def load_data(self, dataset: DatasetConfig, table_name: str) -> None:
        """Load data using Spark DataFrameReader."""
        target_table_fqn = self.table_fqn(dataset, table_name)
        fmt = (dataset.format or "parquet").lower()
        reader = self.spark.read
        options = dataset.options or {}
        if fmt == "tsv":
            fmt = "csv"
            options = {**options, "sep": "\t"}
        if fmt == "csv":
            # Map field_delimiter to Spark's sep if provided
            if "sep" not in options and "field_delimiter" in options:
                options = {**options, "sep": options.get("field_delimiter")}
            options = {"header": str(options.get("header", True)).lower(), **options}

        df = reader.format(fmt).options(**options).load(dataset.sources)
        df.createOrReplaceTempView("_src")
        self.spark.sql(f"INSERT INTO {target_table_fqn} SELECT * FROM _src")

    def sql(
        self, sql: str, alias_to_table: Dict[str, Tuple[DatasetConfig, str]]
    ) -> List[Tuple[Any, ...]]:
        alias_to_fqn = {
            alias: self.table_fqn(ds, table)
            for alias, (ds, table) in alias_to_table.items()
        }
        rendered_sql = render_sql_with_aliases(sql, alias_to_fqn)
        rows = self.spark.sql(rendered_sql).collect()
        return _rows_to_tuples(rows)


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


# ---- Simple perf metrics plumbing ------------------------------------------
from pathlib import Path
from datetime import datetime
import csv, os, socket, platform, json, time


class MetricsRecorder:
    FIELDS = [
        "test_run_id",
        "dataset",
        "query_id",
        "rows_spark",
        "rows_embucket",
        "time_spark_ms",
        "time_embucket_ms",
        "speedup_vs_spark",
        "passed",
        "nodeid",
        "created_at",
    ]

    def __init__(self, outfile: Path, test_run_id: str):
        self.outfile = Path(outfile)
        self.test_run_id = test_run_id
        self.rows = []
        self.mismatches = {}  # Store detailed mismatches by query_id
        self.outfile.parent.mkdir(parents=True, exist_ok=True)

    def add(self, **kw):
        row = {k: kw.get(k) for k in self.FIELDS}
        row["test_run_id"] = self.test_run_id
        row["created_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        self.rows.append(row)

    def add_mismatch(self, query_id, mismatch_details):
        """Record detailed mismatch information for a query."""
        if query_id not in self.mismatches:
            self.mismatches[query_id] = []
        self.mismatches[query_id].append(mismatch_details)

    def flush(self):
        # Write metrics CSV
        write_header = not self.outfile.exists()
        with self.outfile.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.FIELDS)
            if write_header:
                w.writeheader()
            for r in self.rows:
                w.writerow(r)

        # Write mismatches JSON if there are any
        if self.mismatches:
            mismatch_file = self.outfile.with_name(
                f"mismatches_{self.test_run_id}.json"
            )
            with mismatch_file.open("w") as f:
                json.dump(self.mismatches, f, indent=2, default=str)

        self.rows.clear()


@pytest.fixture(scope="session")
def metrics_recorder(test_run_id) -> MetricsRecorder:
    # You can override paths via env if you like
    artifacts_dir = Path(os.getenv("INTEGRATION_ARTIFACTS_DIR", "artifacts"))
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    rec = MetricsRecorder(artifacts_dir / f"metrics_{test_run_id}.csv", test_run_id)
    yield rec
    rec.flush()
