import pytest
from conftest import compare_result_sets
from tpch_queries import TPCH_QUERIES


def _run_cross_engine_test(
    spark_engine, embucket_engine, loaded_dataset, query_id, query_sql
):
    """Common test logic for cross-engine compatibility testing."""
    dataset, table_name, _ = loaded_dataset

    # Set up table alias for query - both engines read from the same loaded table
    alias = {"table": (dataset, table_name)}

    # Run query with both reader engines
    spark_result = spark_engine.sql(query_sql, alias)
    embucket_result = embucket_engine.sql(query_sql, alias)

    # Compare results between engines
    ok, msg = compare_result_sets(spark_result, embucket_result)
    assert (
        ok
    ), f"Query {query_id} on {dataset.name} mismatch between Spark and Embucket: {msg}"

    # Basic sanity checks
    assert len(spark_result) > 0
    assert len(embucket_result) > 0


def _run_multi_table_test(
    spark_engine, embucket_engine, loaded_tables, query_id, query_sql
):
    """Common test logic for multi-table cross-engine compatibility testing."""
    # Build alias_to_table mapping - both engines read from the same loaded tables
    alias_to_table = {}
    for alias, (dataset, table_name, _) in loaded_tables.items():
        alias_to_table[alias] = (dataset, table_name)

    # Run query with both reader engines
    spark_result = spark_engine.sql(query_sql, alias_to_table)
    embucket_result = embucket_engine.sql(query_sql, alias_to_table)

    # Compare results between engines
    ok, msg = compare_result_sets(spark_result, embucket_result)
    assert (
        ok
    ), f"Multi-table query {query_id} mismatch between Spark and Embucket: {msg}"

    # Basic sanity checks
    assert len(spark_result) > 0
    assert len(embucket_result) > 0


@pytest.mark.parametrize('nyc_taxi', ['spark', 'embucket'], indirect=True)
@pytest.mark.parametrize(
    "query_id,query_sql",
    [
        ("q_count", "SELECT COUNT(*) FROM {{TABLE:table}}"),
        (
            "q_vendor_counts",
            "SELECT vendorid, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY vendorid",
        ),
        (
            "q_payment_amounts",
            "SELECT payment_type, COUNT(*) AS c, SUM(total_amount) AS total FROM {{TABLE:table}} GROUP BY payment_type",
        ),
        (
            "q_daily_counts",
            "SELECT CAST(tpep_pickup_datetime AS DATE) AS d, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY CAST(tpep_pickup_datetime AS DATE)",
        ),
        (
            "q_vendor_avg_distance",
            "SELECT vendorid, AVG(trip_distance) AS avg_dist FROM {{TABLE:table}} GROUP BY vendorid",
        ),
    ],
    ids=[
        "q_count",
        "q_vendor_counts",
        "q_payment_amounts",
        "q_daily_counts",
        "q_vendor_avg_distance",
    ],
)
def test_nyc_taxi(
    spark_engine,
    embucket_engine,
    nyc_taxi,
    query_id,
    query_sql,
):
    """Test NYC Taxi dataset with taxi-specific queries using different loaders."""
    _run_cross_engine_test(
        spark_engine, embucket_engine, nyc_taxi, query_id, query_sql
    )


# TPC-H Benchmark Tests - Real TPC-H queries using multiple tables
@pytest.mark.parametrize('tpch_full', ['spark', 'embucket'], indirect=True)
@pytest.mark.parametrize("query_id,query_sql", TPCH_QUERIES, ids=[query_id for query_id, _ in TPCH_QUERIES])
def test_tpch_benchmark_queries(
    spark_engine, embucket_engine, tpch_full, query_id, query_sql
):
    """Test actual TPC-H benchmark queries using complete dataset with all tables and different loaders."""
    _run_multi_table_test(
        spark_engine, embucket_engine, tpch_full, query_id, query_sql
    )
