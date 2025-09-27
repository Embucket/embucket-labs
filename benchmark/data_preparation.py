import os
import argparse
from utils import create_embucket_connection, create_snowflake_connection
from tpch import parametrize_tpch_ddl, get_table_names
from dotenv import load_dotenv
from constants import SystemType

load_dotenv()


def create_tables(cursor, system):
    """Create tables using the consolidated TPC-H DDL statements."""
    print(f"Creating tables for {system}...")
    # Get DDL statements with fully qualified/unqualified names for Embucket/Snowflake
    if system == SystemType.EMBUCKET:
        tpch_ddl = parametrize_tpch_ddl(fully_qualified_names_for_embucket=True)
    elif system == SystemType.SNOWFLAKE:
        tpch_ddl = parametrize_tpch_ddl(fully_qualified_names_for_embucket=False)
    else:
        raise ValueError("Unsupported system")
    for table_name, ddl_sql in tpch_ddl:
        print(f"Creating table: {table_name}")
        cursor.execute(ddl_sql.strip())


def upload_parquet_to_snowflake_tables(cursor, dataset_path):
    """Upload parquet files to Snowflake tables from S3 stage."""
    table_names = get_table_names(fully_qualified_names_for_embucket=False)
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



def upload_parquet_to_embucket_tables(cursor, dataset_path):
    """Upload parquet files to Embucket tables using COPY INTO."""
    # Get fully qualified table names using the unified logic
    table_names = get_table_names(fully_qualified_names_for_embucket=True)

    for placeholder, qualified_table_name in table_names.items():
        # Extract bare table name for the S3 path (parquet files use bare names)
        bare_table_name = qualified_table_name.split('.')[-1]
        print(f"Loading data into Embucket table {qualified_table_name}...")

        copy_sql = f"COPY INTO {qualified_table_name} FROM 's3://embucket-testdata/{dataset_path}/{bare_table_name}.parquet' FILE_FORMAT = (TYPE = PARQUET)"
        cursor.execute(copy_sql)


def prepare_data_for_embucket(dataset_path):
    """Prepare data for Embucket: generate data, create tables, and load data."""
    # Connect to Embucket
    cursor = create_embucket_connection().cursor()
    # Create tables
    create_tables(cursor, SystemType.EMBUCKET)
    # Load data into Embucket tables
    upload_parquet_to_embucket_tables(cursor, dataset_path)

    cursor.close()
    print("Embucket data preparation completed successfully.")


def prepare_data_for_snowflake(dataset_path):
    """Prepare data, create tables, and load data for Snowflake"""
    # Connect to Snowflake
    cursor = create_snowflake_connection().cursor()
    # Create tables
    create_tables(cursor, SystemType.SNOWFLAKE)
    # Load data into Snowflake tables
    upload_parquet_to_snowflake_tables(cursor, dataset_path)

    cursor.close()
    print("Snowflake data preparation completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare data for Embucket/Snowflake benchmarks")
    parser.add_argument("--system", type=str, choices=["embucket", "snowflake", "both"],
                        default="both", help="Which system to prepare data for")
    parser.add_argument("--dataset-path", type=str, default=os.environ.get("DATASET_PATH", "tpch/1"),
                        help="Dataset path in format 'dataset/scale' (default: from env or 'tpch/1')")

    args = parser.parse_args()

    # Override environment variable if specified in args
    if args.dataset_path:
        os.environ["DATASET_PATH"] = args.dataset_path

    print(f"Preparing data for dataset path: {args.dataset_path}")

    if args.system.lower() in ["embucket", "both"]:
        prepare_data_for_embucket(args.dataset_path)

    if args.system.lower() in ["snowflake", "both"]:
        prepare_data_for_snowflake(args.dataset_path)
