import os
import glob
import subprocess
from utils import create_embucket_connection, create_snowflake_connection

DDL_DIR = "tpcds_ddl"
PARQUET_DIR = "tpcds_data"


def check_tpcds_data_exists():
    """Check if TPCDS data exists in the parquet directory."""
    if not os.path.exists(PARQUET_DIR):
        os.makedirs(PARQUET_DIR)
        return False

    parquet_files = glob.glob(f"{PARQUET_DIR}/*.parquet")
    return len(parquet_files) > 0


def generate_tpcds_data():
    """Generate TPCDS data using DuckDB if it doesn't exist."""
    if not check_tpcds_data_exists():
        print("TPCDS data not found. Generating using DuckDB...")
        try:
            subprocess.run(['duckdb', '-c', f"CALL dsdgen(sf=0.1); export database '{PARQUET_DIR}' (format parquet);"],
                           check=True)
            print("TPCDS data generation completed.")
        except subprocess.CalledProcessError as e:
            print(f"Error generating TPCDS data: {e}")
            raise
        except FileNotFoundError:
            print("DuckDB not found. Please install DuckDB to generate TPCDS data.")
            raise


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


def upload_parquet_to_embucket_tables(cursor):
    """Upload parquet files to Embucket tables using COPY INTO."""
    parquet_files = glob.glob(f"{PARQUET_DIR}/*.parquet")

    for parquet_file in parquet_files:
        table_name = os.path.basename(parquet_file).replace(".parquet", "")

        print(f"Loading {parquet_file} into Embucket table {table_name}...")

        copy_sql = f"COPY INTO {table_name} FROM 's3://embucket-testdata/tpcds_data/{table_name}.parquet' FILE_FORMAT = (TYPE = PARQUET)"
        cursor.execute(copy_sql)


def upload_parquet_to_snowflake_tables(cursor):
    """Upload parquet files to Snowflake tables."""
    parquet_files = glob.glob(f"{PARQUET_DIR}/*.parquet")
    for parquet_file in parquet_files:
        table_name = os.path.basename(parquet_file).replace(".parquet", "")
        # Upload file to stage
        print("uploading: ", parquet_file)
        cursor.execute(f"PUT file://{parquet_file} @sf_prep_stage;")
        # Load data into table
        cursor.execute(f"""
                    COPY INTO {table_name}
                    FROM @sf_prep_stage/{os.path.basename(parquet_file)}
                    FILE_FORMAT = (TYPE = PARQUET)
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                """)


def prepare_data_for_snowflake():
    """Main function to prepare data, create tables, and load data for Snowflake"""
    # Connect to Snowflake
    cursor = create_snowflake_connection().cursor()
    # Create tables
    create_tables(cursor)
    # Load data into Snowflake tables
    upload_parquet_to_snowflake_tables(cursor)

    cursor.close()
    print("Snowflake data preparation completed successfully.")


def prepare_data_for_embucket():
    """Prepare data for Embucket: generate data, create tables, and load data."""
    # Connect to Embucket
    cursor = create_embucket_connection().cursor()
    # Create tables
    create_tables(cursor)
    # Load data into Embucket tables
    upload_parquet_to_embucket_tables(cursor)

    cursor.close()
    print("Embucket data preparation completed successfully.")


if __name__ == "__main__":
    generate_tpcds_data()
    prepare_data_for_embucket()
    prepare_data_for_snowflake()
