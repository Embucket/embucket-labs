import os
import argparse
from utils import create_embucket_connection, create_snowflake_connection
from tpch import parametrize_tpch_ddl, get_table_names as get_tpch_table_names
from tpcds import parametrize_tpcds_ddl, get_table_names as get_tpcds_table_names
from clickbench import parametrize_clickbench_ddl, get_table_names as get_clickbench_table_names
from dotenv import load_dotenv
from constants import SystemType

load_dotenv()


def create_tables(cursor, system, benchmark_type, use_custom_dataset):
    """Create tables using the appropriate DDL statements based on benchmark type."""
    # If using Snowflake built-in tables, no need to create tables
    if system == SystemType.SNOWFLAKE and not use_custom_dataset:
        print(f"Using Snowflake's built-in {benchmark_type.upper()} sample data. No table creation needed.")
        return

    print(f"Creating tables for {system} ({benchmark_type})...")

    # Get DDL statements based on benchmark type and system
    if benchmark_type == "tpch":
        if system == SystemType.EMBUCKET:
            ddl_statements = parametrize_tpch_ddl(fully_qualified_names_for_embucket=True, use_custom_dataset=False)
        elif system == SystemType.SNOWFLAKE:
            ddl_statements = parametrize_tpch_ddl(fully_qualified_names_for_embucket=False, use_custom_dataset=use_custom_dataset)
        else:
            raise ValueError("Unsupported system")
    elif benchmark_type == "tpcds":
        if system == SystemType.EMBUCKET:
            ddl_statements = parametrize_tpcds_ddl(fully_qualified_names_for_embucket=True)
        elif system == SystemType.SNOWFLAKE:
            ddl_statements = parametrize_tpcds_ddl(fully_qualified_names_for_embucket=False)
        else:
            raise ValueError("Unsupported system")
    elif benchmark_type == "clickbench":
        if system == SystemType.EMBUCKET:
            ddl_statements = parametrize_clickbench_ddl(fully_qualified_names_for_embucket=True)
        elif system == SystemType.SNOWFLAKE:
            ddl_statements = parametrize_clickbench_ddl(fully_qualified_names_for_embucket=False)
        else:
            raise ValueError("Unsupported system")
    else:
        raise ValueError(f"Unsupported benchmark type: {benchmark_type}")

    for table_name, ddl_sql in ddl_statements:
        print(f"Creating table: {table_name}")
        cursor.execute(ddl_sql.strip())


def upload_parquet_to_snowflake_tables(cursor, dataset_path, benchmark_type, use_custom_dataset):
    """Upload parquet files to Snowflake tables from S3 stage."""
    # If using built-in tables, no need to upload data
    if not use_custom_dataset:
        print(f"Using Snowflake's built-in {benchmark_type.upper()} sample data. No data upload needed.")
        return

    # Get table names based on benchmark type
    if benchmark_type == "tpch":
        table_names = get_tpch_table_names(fully_qualified_names_for_embucket=False)
    elif benchmark_type == "tpcds":
        table_names = get_tpcds_table_names(fully_qualified_names_for_embucket=False)
    elif benchmark_type == "clickbench":
        table_names = get_clickbench_table_names(fully_qualified_names_for_embucket=False)
    else:
        raise ValueError(f"Unsupported benchmark type: {benchmark_type}")

    for table_name in table_names.values():
        print(f"Loading data into Snowflake table {table_name}...")
        s3_path = f"s3://embucket-testdata/{dataset_path}/{table_name}.parquet"
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM '{s3_path}'
            CREDENTIALS = (AWS_KEY_ID = '{os.environ["AWS_ACCESS_KEY_ID"]}'
                          AWS_SECRET_KEY = '{os.environ["AWS_SECRET_ACCESS_KEY"]}')
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """)
        result = cursor.fetchall()
        if result and result[0][0] == 'Copy executed with 0 files processed.':
            raise RuntimeError(f"No files processed for {table_name}. Check S3 path: {s3_path}")



def upload_data_into_embucket_tables(cursor, dataset_path, benchmark_type):
    """Upload parquet files to Embucket tables using COPY INTO."""
    # Get fully qualified table names based on benchmark type
    if benchmark_type == "tpch":
        table_names = get_tpch_table_names(fully_qualified_names_for_embucket=True)
    elif benchmark_type == "tpcds":
        table_names = get_tpcds_table_names(fully_qualified_names_for_embucket=True)
    elif benchmark_type == "clickbench":
        table_names = get_clickbench_table_names(fully_qualified_names_for_embucket=True)
    else:
        raise ValueError(f"Unsupported benchmark type: {benchmark_type}")

    # Use CSV for ClickBench due to date parsing issues with Parquet
    file_format = "CSV" if benchmark_type == "clickbench" else "PARQUET"
    file_extension = "csv" if benchmark_type == "clickbench" else "parquet"

    for placeholder, qualified_table_name in table_names.items():
        # Extract bare table name for the S3 path
        bare_table_name = qualified_table_name.split('.')[-1]
        print(f"Loading {file_format} data into Embucket table {qualified_table_name}...")

        if file_format == "CSV":
            copy_sql = f"COPY INTO {qualified_table_name} FROM 's3://embucket-testdata/{dataset_path}/{bare_table_name}.{file_extension}' FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',')"
        else:
            copy_sql = f"COPY INTO {qualified_table_name} FROM 's3://embucket-testdata/{dataset_path}/{bare_table_name}.{file_extension}' FILE_FORMAT = (TYPE = PARQUET)"

        cursor.execute(copy_sql)


def prepare_data_for_embucket(dataset_path, benchmark_type):
    """Prepare data for Embucket: generate data, create tables, and load data."""
    # Connect to Embucket
    cursor = create_embucket_connection().cursor()
    # Create tables (Embucket always uses custom tables)
    create_tables(cursor, SystemType.EMBUCKET, benchmark_type, use_custom_dataset=True)
    # Load data into Embucket tables
    upload_data_into_embucket_tables(cursor, dataset_path, benchmark_type)

    cursor.close()
    print(f"Embucket data preparation completed successfully for {benchmark_type}.")


def prepare_data_for_snowflake(dataset_path, benchmark_type):
    """Prepare data, create tables, and load data for Snowflake"""
    # Connect to Snowflake
    cursor = create_snowflake_connection().cursor()
    # Create tables (always use custom dataset for data preparation)
    create_tables(cursor, SystemType.SNOWFLAKE, benchmark_type, use_custom_dataset=True)
    # Load data into Snowflake tables
    upload_parquet_to_snowflake_tables(cursor, dataset_path, benchmark_type, use_custom_dataset=True)

    cursor.close()
    print(f"Snowflake data preparation completed successfully for {benchmark_type}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare data for Embucket/Snowflake benchmarks")
    parser.add_argument("--system", type=str, choices=["embucket", "snowflake", "both"],
                        default="both", help="Which system to prepare data for")
    parser.add_argument("--benchmark-type", type=str, choices=["tpch", "tpcds", "clickbench"],
                        default=os.environ.get("BENCHMARK_TYPE", "tpch"),
                        help="Benchmark type (default: from env or 'tpch')")
    parser.add_argument("--dataset-path", type=str, default=os.environ.get("DATASET_PATH", "tpch/1"),
                        help="Dataset path in format 'dataset/scale' (default: from env or 'tpch/1')")

    args = parser.parse_args()

    # Override environment variables if specified in args
    if args.dataset_path:
        os.environ["DATASET_PATH"] = args.dataset_path
    if args.benchmark_type != os.environ.get("BENCHMARK_TYPE", "tpch"):
        os.environ["BENCHMARK_TYPE"] = args.benchmark_type

    print(f"Preparing data for benchmark type: {args.benchmark_type}")
    print(f"Dataset path: {args.dataset_path}")

    if args.system.lower() in ["embucket", "both"]:
        prepare_data_for_embucket(args.dataset_path, args.benchmark_type)

    if args.system.lower() in ["snowflake", "both"]:
        prepare_data_for_snowflake(args.dataset_path, args.benchmark_type)
