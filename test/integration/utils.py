import os
import uuid
import decimal
from dataclasses import dataclass
from typing import Dict, Any, Callable, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
import csv
import json
import time
from collections import Counter
import math


def _get(key: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(key)
    return val if val is not None else default


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
    import pytest

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
    return isinstance(x, (int, float, decimal.Decimal))


def _normalize_row_for_counting(row: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Normalize a row for counter-based comparison by standardizing values within tolerance."""
    normalized = []
    for value in row:
        if _is_number(value):
            value = decimal.Decimal(value)
            value = value.quantize(decimal.Decimal("1.000000"), decimal.ROUND_FLOOR)
            normalized.append(value)
        elif isinstance(value, str):
            # Normalize string values by stripping whitespace
            normalized.append(value.strip())
        else:
            # Keep other values (None, etc.) as-is
            normalized.append(value)

    return tuple(normalized)


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
    metrics_recorder=None,
    query_id=None,
) -> Tuple[bool, str]:
    """Compare two result sets with type tolerance and order-insensitive.

    Returns (ok, message). On failure, message contains a small diff.
    If metrics_recorder and query_id are provided, detailed mismatch info is recorded.
    """
    # Check row count first
    if len(a) != len(b):
        mismatch_info = {
            "type": "row_count_mismatch",
            "spark_rows": len(a),
            "embucket_rows": len(b),
        }
        if metrics_recorder and query_id:
            metrics_recorder.add_mismatch(query_id, mismatch_info)
        return False, f"Row count differs: {len(a)} vs {len(b)}"

    # Check column count
    if len(a) > 0 and len(a[0]) != len(b[0]):
        mismatch_info = {
            "type": "column_count_mismatch",
            "spark_columns": len(a[0]),
            "embucket_columns": len(b[0]),
        }
        if metrics_recorder and query_id:
            metrics_recorder.add_mismatch(query_id, mismatch_info)
        return False, f"Column count differs: {len(a[0])} vs {len(b[0])}"

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
