import logging
from clients import EmbucketClient, PySparkClient
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("spark-read-test")

DB = 'test_db'
SCHEMA = "public"
TABLE = "spark_embucket"


def ensure_db_and_schema(emb: EmbucketClient):
    logger.info("Ensuring volume, database, and schema exist")
    emb.volume()
    emb.sql(f"CREATE DATABASE IF NOT EXISTS {DB} EXTERNAL_VOLUME = 'test'")
    emb.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}")


def spark_create_insert_update_delete(spark_client: PySparkClient):
    logger.info("Creating table and performing insert, update, delete operations in Spark")
    spark = spark_client
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} ( \
                  id INT, \
                  page_name STRING, \
                  category STRING \
               )")
    logger.info("Deleting all rows from table")
    spark.sql(f"DELETE FROM {SCHEMA}.{TABLE} WHERE TRUE")

    logger.info("Inserting sample rows")
    spark.sql(f"""
        INSERT INTO {SCHEMA}.{TABLE} VALUES
        (1, 'page_1', 'category_1'),
        (2, 'is_object', 'category_2'),
        (3, 'page_3', 'Conditional_expression')
    """)

    logger.info("Updating category for page_name='is_object'")
    spark.sql(f"UPDATE {SCHEMA}.{TABLE} SET category='updated_category' WHERE page_name='is_object'")
    logger.info("Deleting rows with category='Conditional_expression'")
    spark.sql(f"DELETE FROM {SCHEMA}.{TABLE} WHERE category='Conditional_expression'")


def embucket_read_verify(emb: EmbucketClient):
    logger.info("Reading table from Embucket to verify Spark changes")
    res = emb.sql(f"SELECT id, page_name, category FROM {DB}.{SCHEMA}.{TABLE} ORDER BY id")
    result = res.get("result", {})
    rows = result.get("rows", [])
    cols = [c["name"] for c in result.get("columns", [])]
    df = pd.DataFrame(rows, columns=cols)

    updated_ok = (df["page_name"] == "is_object") & (df["category"] == "updated_category")
    deleted_ok = not (df["category"] == "Conditional_expression").any()

    if updated_ok.any() and deleted_ok:
        logger.info("Embucket reflects Spark changes correctly.")
        print("Embucket reflects Spark changes correctly.")
    else:
        logger.warning("Embucket did NOT reflect Spark changes correctly.")
        print("Embucket did NOT reflect Spark changes correctly.")
        if not updated_ok.any():
            logger.warning("Missing update: 'is_object' not updated to 'updated_category'.")
            print("Missing update: 'is_object' not updated to 'updated_category'.")
        if not deleted_ok:
            logger.warning("Missing delete: row(s) with 'Conditional_expression' still present.")
            print("Missing delete: row(s) with 'Conditional_expression' still present.")

    return df


def test_spark_write_then_embucket_read():
    logger.info("Starting Spark write and Embucket read test")
    emb = EmbucketClient()
    ensure_db_and_schema(emb)

    spark_client = PySparkClient()
    spark_create_insert_update_delete(spark_client)

    embucket_read_verify(emb)
    logger.info("Test completed")


if __name__ == "__main__":
    test_spark_write_then_embucket_read()