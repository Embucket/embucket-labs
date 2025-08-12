import os
import uuid
from dataclasses import dataclass
from typing import Dict, Any, Callable, List, Optional, Tuple

import pytest


def _get(key: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(key)
    return val if val is not None else default


def _env_required(keys: List[str]) -> None:
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        pytest.skip(f"Missing required env vars: {', '.join(missing)}")


@pytest.fixture(scope="session")
def test_run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="session")
def datasets_config_path() -> str:
    return os.getenv("DATASETS_YAML", "datasets.yaml")


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


def _load_datasets_from_yaml(path: str) -> List[DatasetConfig]:
    import yaml

    if not os.path.exists(path):
        pytest.skip(
            f"Dataset config not found at {path}. Provide it or set DATASETS_YAML."
        )
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    ds = cfg.get("datasets", [])
    if not ds:
        pytest.skip("No datasets defined in datasets YAML under key 'datasets'.")
    return [DatasetConfig.from_dict(x) for x in ds]


@pytest.fixture(scope="session")
def datasets(datasets_config_path: str) -> List[DatasetConfig]:
    """Provide the full list of dataset configs for tests that need them.

    This complements the dynamic parametrization that supplies the singular
    `dataset` parameter to tests, and maintains compatibility with tests that
    expect a `datasets` fixture.
    """
    return _load_datasets_from_yaml(datasets_config_path)


@dataclass
class QueryDef:
    id: str
    sql: Optional[str] = None
    sql_path: Optional[str] = None


@dataclass
class QuerySuite:
    name: str
    relations: Dict[str, str]  # alias -> dataset_name
    queries: List[QueryDef]

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "QuerySuite":
        qds = []
        for q in d.get("queries", []):
            qds.append(
                QueryDef(
                    id=q.get("id") or "query",
                    sql=q.get("sql"),
                    sql_path=q.get("sql_path"),
                )
            )
        return QuerySuite(name=d["name"], relations=d.get("relations", {}), queries=qds)


def _load_query_suites_from_yaml(path: str) -> List[QuerySuite]:
    import yaml

    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    suites = cfg.get("suites", [])
    return [QuerySuite.from_dict(x) for x in suites]


def pytest_generate_tests(metafunc):
    """Dynamically parametrize tests with dataset objects and engine pairs."""
    if "dataset" in metafunc.fixturenames:
        path = os.getenv("DATASETS_YAML", "datasets.yaml")
        datasets = _load_datasets_from_yaml(path)
        metafunc.parametrize(
            "dataset",
            datasets,
            ids=[d.name for d in datasets],
        )
    if "query_suite" in metafunc.fixturenames:
        path = os.getenv("QUERIES_YAML", "queries.yaml")
        suites = _load_query_suites_from_yaml(path)
        if suites:
            metafunc.parametrize("query_suite", suites, ids=[s.name for s in suites])


@pytest.fixture(scope="session")
def embucket_exec() -> Callable[[str], Any]:
    """Return a function to execute SQL against Embucket via the Snowflake connector.

    Requires env variables; if absent, tests are skipped.
    """
    # Sane defaults for local dev
    host = _get("EMBUCKET_SQL_HOST", "localhost")
    port = _get("EMBUCKET_SQL_PORT", "3000")
    protocol = _get("EMBUCKET_SQL_PROTOCOL", "http")
    user = _get("EMBUCKET_USER", "embucket")
    password = _get("EMBUCKET_PASSWORD", "embucket")
    account = os.getenv("EMBUCKET_ACCOUNT")  # optional; will default to random
    database = _get("EMBUCKET_DATABASE", "analytics")
    schema = _get("EMBUCKET_SCHEMA", "public")

    try:
        import snowflake.connector as sf
    except Exception as e:
        pytest.skip(f"snowflake-connector-python not available: {e}")

    connect_args: Dict[str, Any] = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": "embucket",
    }

    # Prefer explicit host/port if provided (for Snowflake-compatible endpoints)
    if host:
        connect_args["host"] = host
        connect_args["protocol"] = protocol
        if port:
            try:
                connect_args["port"] = int(port)
            except Exception:
                connect_args["port"] = 3000
    # Snowflake connector insists on an account string; Embucket ignores it.
    # Provide a random account if none specified so the connector initializes.
    if not account:
        import uuid as _uuid

        account = f"acc_{_uuid.uuid4().hex[:10]}"
    connect_args["account"] = account

    # Establish connection once per session
    try:
        conn = sf.connect(**connect_args)
    except Exception as e:
        pytest.skip(f"Failed to connect to Embucket SQL endpoint: {e}")

    # Set context if provided
    if database:
        try:
            conn.cursor().execute(f"USE DATABASE {database}")
        except Exception as e:
            conn.close()
            pytest.skip(f"Failed to USE DATABASE {database}: {e}")
    if schema:
        try:
            conn.cursor().execute(f"USE SCHEMA {schema}")
        except Exception as e:
            conn.close()
            pytest.skip(f"Failed to USE SCHEMA {schema}: {e}")

    def _exec(sql: str) -> Any:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return None
        finally:
            cur.close()

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

    # Create external volume if we have enough info
    embucket_exec(
        f"""CREATE EXTERNAL VOLUME IF NOT EXISTS {vol} STORAGE_LOCATIONS = ((NAME = '{vol}' STORAGE_PROVIDER = 's3' STORAGE_ENDPOINT = '{endpoint}' STORAGE_BASE_URL = '{bucket}' CREDENTIALS = (AWS_KEY_ID='{ak}' AWS_SECRET_KEY='{sk}' REGION='us-east-1')));
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


def _render_sql_with_table(sql: str, table_fqn: str) -> str:
    return sql.replace("{{TABLE_FQN}}", table_fqn)


def render_sql_with_aliases(sql: str, alias_to_fqn: Dict[str, str]) -> str:
    out = sql
    for alias, fqn in alias_to_fqn.items():
        out = out.replace(f"{{{{TABLE:{alias}}}}}", fqn)
    return out


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


def _sort_rows(rows: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
    return sorted(rows, key=lambda row: tuple(_normalize_value(v) for v in row))


def _is_number(x: Any) -> bool:
    import decimal

    return isinstance(x, (int, float, decimal.Decimal))


def compare_result_sets(
    a: List[Tuple[Any, ...]],
    b: List[Tuple[Any, ...]],
    rel_tol: float = 1e-6,
    abs_tol: float = 1e-9,
) -> Tuple[bool, str]:
    """Compare two result sets with type tolerance and order-insensitive.

    Returns (ok, message). On failure, message contains a small diff.
    """
    if len(a) != len(b):
        return False, f"Row count differs: {len(a)} vs {len(b)}"
    sa = _sort_rows(a)
    sb = _sort_rows(b)
    import math

    for i, (ra, rb) in enumerate(zip(sa, sb)):
        if len(ra) != len(rb):
            return False, f"Row {i} length differs: {len(ra)} vs {len(rb)}"
        for j, (va, vb) in enumerate(zip(ra, rb)):
            if va == vb:
                continue
            # Handle numeric approx
            if _is_number(va) and _is_number(vb):
                if math.isclose(float(va), float(vb), rel_tol=rel_tol, abs_tol=abs_tol):
                    continue
                return False, f"Row {i}, col {j} numeric mismatch: {va} vs {vb}"
            # Normalize None vs empty string edge cases cautiously
            if va in (None, "") and vb in (None, ""):
                continue
            if str(va) != str(vb):
                return False, f"Row {i}, col {j} differs: {va} vs {vb}"
    return True, "OK"


@pytest.fixture(scope="session")
def sql_loader() -> Callable[[str], str]:
    def _load(path: str) -> str:
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQL file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    return _load


@pytest.fixture(scope="session")
def load_into_embucket(embucket_exec) -> Callable[[str, Dict[str, Any]], None]:
    def _copy_into(table_fqn: str, ds: DatasetConfig) -> None:
        fmt = (ds.format or "parquet").lower()
        if fmt not in ("parquet", "csv", "tsv"):
            raise ValueError(f"Unsupported format for COPY INTO: {fmt}")
        options = dict(ds.options or {})
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

        for uri in ds.sources:
            sql = f"COPY INTO {table_fqn} FROM '{uri}' FILE_FORMAT = ({ff})"
            embucket_exec(sql)

    return _copy_into


@pytest.fixture(scope="session")
def load_into_spark(spark) -> Callable[[str, DatasetConfig], None]:
    def _load(target_table_fqn: str, ds: DatasetConfig) -> None:
        fmt = (ds.format or "parquet").lower()
        reader = spark.read
        options = ds.options or {}
        if fmt == "tsv":
            fmt = "csv"
            options = {**options, "sep": "\t"}
        if fmt == "csv":
            # Map field_delimiter to Spark's sep if provided
            if "sep" not in options and "field_delimiter" in options:
                options = {**options, "sep": options.get("field_delimiter")}
            options = {"header": str(options.get("header", True)).lower(), **options}

        df = reader.format(fmt).options(**options).load(ds.sources)
        df.createOrReplaceTempView("_src")
        spark.sql(f"INSERT INTO {target_table_fqn} SELECT * FROM _src")

    return _load


class EmbucketEngine:
    def __init__(self, exec_fn: Callable[[str], Any], sql_loader: Callable[[str], str]):
        self._exec = exec_fn
        self._load_sql = sql_loader

    @property
    def name(self) -> str:
        return "embucket"

    def table_fqn(self, dataset: DatasetConfig, table_name: str) -> str:
        # Embucket uses current DB/SCHEMA; unqualified table is fine.
        return table_name

    def create_table(self, dataset: DatasetConfig, table_name: str) -> None:
        ddl_path = dataset.ddl["embucket"]
        sql = self._load_sql(ddl_path)
        # Drop if exists to avoid duplicate loads across tests
        try:
            self._exec(f"DROP TABLE IF EXISTS {self.table_fqn(dataset, table_name)}")
        except Exception:
            pass
        sql = _render_sql_with_table(sql, self.table_fqn(dataset, table_name))
        self._exec(sql)

    def load(
        self,
        dataset: DatasetConfig,
        table_name: str,
        loader: Callable[[str, DatasetConfig], None],
    ) -> None:
        loader(self.table_fqn(dataset, table_name), dataset)

    def sql(
        self, dataset: DatasetConfig, table_name: str, sql_template: str
    ) -> List[Tuple[Any, ...]]:
        sql = _render_sql_with_table(sql_template, self.table_fqn(dataset, table_name))
        rows = self._exec(sql) or []
        return _rows_to_tuples(rows)


class SparkEngine:
    def __init__(
        self,
        spark_sess: Any,
        sql_loader: Callable[[str], str],
        catalog_alias: str = "emb",
    ):
        self.spark = spark_sess
        self._load_sql = sql_loader
        self.catalog_alias = catalog_alias

    @property
    def name(self) -> str:
        return "spark"

    def table_fqn(self, dataset: DatasetConfig, table_name: str) -> str:
        return f"{self.catalog_alias}.{dataset.namespace}.{table_name}"

    def create_table(self, dataset: DatasetConfig, table_name: str) -> None:
        ddl_path = dataset.ddl["spark"]
        sql = self._load_sql(ddl_path)
        # Drop if exists for idempotence
        try:
            self.spark.sql(
                f"DROP TABLE IF EXISTS {self.table_fqn(dataset, table_name)}"
            )
        except Exception:
            pass
        sql = _render_sql_with_table(sql, self.table_fqn(dataset, table_name))
        self.spark.sql(sql)

    def load(
        self,
        dataset: DatasetConfig,
        table_name: str,
        loader: Callable[[str, DatasetConfig], None],
    ) -> None:
        loader(self.table_fqn(dataset, table_name), dataset)

    def sql(
        self, dataset: DatasetConfig, table_name: str, sql_template: str
    ) -> List[Tuple[Any, ...]]:
        sql = _render_sql_with_table(sql_template, self.table_fqn(dataset, table_name))
        rows = self.spark.sql(sql).collect()
        return _rows_to_tuples(rows)


@pytest.fixture(scope="session")
def datasets_by_name(datasets: List[DatasetConfig]) -> Dict[str, DatasetConfig]:
    return {d.name: d for d in datasets}


@pytest.fixture(scope="session")
def query_suites() -> List[QuerySuite]:
    path = os.getenv("QUERIES_YAML", "queries.yaml")
    return _load_query_suites_from_yaml(path)


@pytest.fixture(scope="session")
def embucket_engine(embucket_exec, sql_loader) -> EmbucketEngine:
    return EmbucketEngine(embucket_exec, sql_loader)


@pytest.fixture(scope="session")
def spark_engine(spark, sql_loader) -> SparkEngine:
    return SparkEngine(spark, sql_loader, catalog_alias="emb")


@pytest.fixture
def writer_engine(request):
    # Indirect fixture that resolves to either embucket_engine or spark_engine
    return request.getfixturevalue(request.param)


@pytest.fixture
def reader_engine(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def engine(request):
    # Generic engine indirection for single-engine parametrization
    return request.getfixturevalue(request.param)
