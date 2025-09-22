import os
import glob
from utils import create_embucket_connection, create_snowflake_connection
from dotenv import load_dotenv

load_dotenv()

DDL_DIR = "tpch_ddl"


def create_tables(cursor):
    """Create tables in Snowflake using SQL files."""
    sql_files = glob.glob(f"{DDL_DIR}/*_embucket.sql")
    print("Creating tables...")
    for sql_file in sql_files:
        with open(sql_file, "r") as f:
            lines = f.readlines()
            if lines and lines[0].strip().startswith("--"):
                sql = "".join(lines[1:])
            else:
                sql = "".join(lines)
            cursor.execute(sql)


def upload_parquet_to_embucket_tables(cursor, dataset):
    """Upload parquet files to Embucket tables using COPY INTO."""
    tpch_table_names = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
    for table_name in tpch_table_names:
        print(f"Loading data into Embucket table {table_name}...")

        copy_sql = f"COPY INTO {table_name} FROM 's3://embucket-testdata/tpch_data/{dataset}/{table_name}.parquet' FILE_FORMAT = (TYPE = PARQUET)"
        cursor.execute(copy_sql)


def prepare_data_for_embucket():
    """Prepare data for Embucket: generate data, create tables, and load data."""
    dataset = os.getenv("EMBUCKET_DATASET")
    # Connect to Embucket
    cursor = create_embucket_connection().cursor()
    # Create tables
    create_tables(cursor)
    # Load data into Embucket tables
    upload_parquet_to_embucket_tables(cursor, dataset)

    cursor.close()
    print("Embucket data preparation completed successfully.")


if __name__ == "__main__":
    prepare_data_for_embucket()