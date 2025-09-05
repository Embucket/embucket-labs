import os
import glob
import subprocess
from config import DDL_DIR, PARQUET_DIR, get_snowflake_config
from snowflake.connector import connect


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


def create_tables(cursor, config):
    """Create tables in Snowflake using SQL files."""
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {config.get('schema')}")
    cursor.execute(f"USE SCHEMA {config.get('schema')}")
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


def upload_parquet_to_tables(cursor):
    """Upload parquet files to Snowflake tables."""
    # Create stage if not exists
    cursor.execute("CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format TYPE = parquet;")
    cursor.execute("CREATE OR REPLACE TEMPORARY STAGE sf_tut_stage FILE_FORMAT = sf_tut_parquet_format;")

    parquet_files = glob.glob(f"{PARQUET_DIR}/*.parquet")
    for parquet_file in parquet_files:
        table_name = os.path.basename(parquet_file).replace(".parquet", "")
        # Upload file to stage
        print("uploading: ", parquet_file)
        cursor.execute(f"PUT file://{parquet_file} @sf_tut_stage;")
        # Load data into table
        cursor.execute(f"""
                    COPY INTO {table_name}
                    FROM @sf_tut_stage/{os.path.basename(parquet_file)}
                    FILE_FORMAT = (TYPE = PARQUET)
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                """)


def prepare_data():
    """Main function to prepare data, create tables, and load data for Snowflake"""
    generate_tpcds_data()

    # Connect to Snowflake and prepare data
    sf_config = get_snowflake_config()
    sf_connection = connect(**sf_config)
    cursor = sf_connection.cursor()

    create_tables(cursor, sf_config)
    upload_parquet_to_tables(cursor)

    cursor.close()
    sf_connection.close()

    print("Data preparation completed successfully.")


if __name__ == "__main__":
    prepare_data()