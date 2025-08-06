import pandas as pd
from clients import EmbucketClient

DB = "test_db"
SCHEMA = "public"
TABLE = "spark_embucket"

def get_embucket_client():
    emb = EmbucketClient()
    emb.volume()
    emb.sql(f"CREATE DATABASE IF NOT EXISTS {DB} EXTERNAL_VOLUME = 'test'")
    emb.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}")
    return emb


def perform_spark_operations(spark):
    # create table
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            id INT,
            page_name STRING,
            category STRING
        )"""
    )

    # clear
    spark.sql(f"DELETE FROM {SCHEMA}.{TABLE} WHERE TRUE")

    # insert rows
    spark.sql(
        f"""
        INSERT INTO {SCHEMA}.{TABLE} VALUES
        (1, 'page_1', 'category_1'),
        (2, 'is_object', 'category_2'),
        (3, 'page_3', 'Conditional_expression')
        """
    )

    # update
    spark.sql(
        f"UPDATE {SCHEMA}.{TABLE} SET category='updated_category' WHERE page_name='is_object'"
    )

    # delete
    spark.sql(
        f"DELETE FROM {SCHEMA}.{TABLE} WHERE category='Conditional_expression'"
    )


def read_and_validate_from_embucket(emb: EmbucketClient) -> pd.DataFrame:
    res = emb.sql(f"SELECT id, page_name, category FROM {DB}.{SCHEMA}.{TABLE} ORDER BY id")
    result = res.get("result", {})
    rows = result.get("rows", [])
    cols = [c["name"] for c in result.get("columns", [])]
    df = pd.DataFrame(rows, columns=cols)
    return df


# --- test ---
def test_spark_embucket_sync(rest_spark_session):
    """
    Write via Spark (create/insert/update/delete) and verify Embucket sees those changes
    """
    spark = rest_spark_session

    # Perform Spark-side mutations
    perform_spark_operations(spark)

    # Read back through Embucket
    embucket_client = get_embucket_client()
    df = read_and_validate_from_embucket(embucket_client)

    # Validate update happened
    updated_mask = (df["page_name"] == "is_object") & (df["category"] == "updated_category")
    assert updated_mask.any(), "Expected row with page_name='is_object' to have category='updated_category'"

    # Validate delete happened
    assert not (df["category"] == "Conditional_expression").any(), (
        "Expected no rows with category='Conditional_expression' after delete"
    )

    # Check that unaffected row is still there
    assert ((df["page_name"] == "page_1") & (df["category"] == "category_1")).any(), (
        "Expected original row ('page_1','category_1') to persist"
    )