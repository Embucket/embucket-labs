import os
from utils import create_embucket_connection, create_snowflake_connection
from tpch import parametrize_tpch_ddl, get_table_names
from dotenv import load_dotenv

load_dotenv()


def create_tables(cursor):
    """Create tables using the consolidated TPC-H DDL statements."""
    print("Creating tables...")

    # Get DDL statements with fully qualified names for Embucket
    tpch_ddl = parametrize_tpch_ddl(fully_qualified_names_for_embucket=True)
    for table_name, ddl_sql in tpch_ddl:
        print(f"Creating table: {table_name}")
        cursor.execute(ddl_sql.strip())


def upload_parquet_to_embucket_tables(cursor, dataset, dataset_scale_factor):
    """Upload parquet files to Embucket tables using COPY INTO."""
    # Get fully qualified table names using the unified logic
    table_names = get_table_names(fully_qualified_names_for_embucket=True)

    for placeholder, qualified_table_name in table_names.items():
        # Extract bare table name for the S3 path (parquet files use bare names)
        bare_table_name = qualified_table_name.split('.')[-1]
        print(f"Loading data into Embucket table {qualified_table_name}...")

        copy_sql = f"COPY INTO {qualified_table_name} FROM 's3://embucket-testdata/{dataset}/{dataset_scale_factor}/{bare_table_name}.parquet' FILE_FORMAT = (TYPE = PARQUET)"
        cursor.execute(copy_sql)


def prepare_data_for_embucket():
    """Prepare data for Embucket: generate data, create tables, and load data."""
    dataset = os.getenv("DATASET_NAME")
    dataset_scale_factor = os.getenv("DATASET_SCALE_FACTOR")
    # Connect to Embucket
    cursor = create_embucket_connection().cursor()
    # Create tables
    create_tables(cursor)
    # Load data into Embucket tables
    upload_parquet_to_embucket_tables(cursor, dataset, dataset_scale_factor)

    cursor.close()
    print("Embucket data preparation completed successfully.")


if __name__ == "__main__":
    prepare_data_for_embucket()