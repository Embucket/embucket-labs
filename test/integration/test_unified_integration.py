import pytest
import time
from utils import compare_result_sets
from queries.tpch_queries import TPCH_QUERIES
from queries.tpcds_queries import TPCDS_QUERIES
from queries.clickbench_queries import CLICKBENCH_QUERIES


def _run_cross_engine_test(
    spark_engine,
    embucket_engine,
    loaded_dataset,
    query_id,
    query_sql,
    metrics_recorder,
    request,
):
    dataset, table_name, _ = loaded_dataset
    alias = {"table": (dataset, table_name)}

    t0 = time.perf_counter()
    spark_result = spark_engine.sql(query_sql, alias)
    t1 = time.perf_counter()

    t2 = time.perf_counter()
    embucket_result = embucket_engine.sql(query_sql, alias)
    t3 = time.perf_counter()

    ok, msg = compare_result_sets(spark_result, embucket_result)

    # Record once per query (pair-wise)
    metrics_recorder.add(
        dataset=dataset.name,
        query_id=query_id,
        rows_spark=len(spark_result),
        rows_embucket=len(embucket_result),
        time_spark_ms=round((t1 - t0) * 1000, 3),
        time_embucket_ms=round((t3 - t2) * 1000, 3),
        speedup_vs_spark=round(((t1 - t0) / max((t3 - t2), 1e-9)), 4),
        passed=bool(ok),
        nodeid=request.node.nodeid,
    )

    assert ok, f"Query {query_id} mismatch between Spark and Embucket: {msg}"


def _run_multi_table_test(
    spark_engine,
    embucket_engine,
    loaded_tables,
    query_id,
    query_sql,
    metrics_recorder,
    request,
):
    alias_to_table = {
        alias: (ds, table) for alias, (ds, table, _) in loaded_tables.items()
    }

    t0 = time.perf_counter()
    spark_result = spark_engine.sql(query_sql, alias_to_table)
    t1 = time.perf_counter()

    t2 = time.perf_counter()
    embucket_result = embucket_engine.sql(query_sql, alias_to_table)
    t3 = time.perf_counter()

    ok, msg = compare_result_sets(
        spark_result,
        embucket_result,
        metrics_recorder=metrics_recorder,
        query_id=query_id,
    )

    any_ds = next(iter(loaded_tables.values()))[0]
    metrics_recorder.add(
        dataset=any_ds.name,
        query_id=query_id,
        rows_spark=len(spark_result),
        rows_embucket=len(embucket_result),
        time_spark_ms=round((t1 - t0) * 1000, 3),
        time_embucket_ms=round((t3 - t2) * 1000, 3),
        speedup_vs_spark=round(((t1 - t0) / max((t3 - t2), 1e-9)), 4),
        passed=bool(ok),
        nodeid=request.node.nodeid,
    )

    assert (
        ok
    ), f"Multi-table query {query_id} mismatch between Spark and Embucket: {msg}"


# NYC Yellow Taxi Tests
@pytest.mark.parametrize("nyc_yellow_taxi", ["spark", "embucket"], indirect=True)
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
def test_yellow_nyc_taxi(
    spark_engine,
    embucket_engine,
    nyc_yellow_taxi,
    query_id,
    query_sql,
    metrics_recorder,
    request,
):
    """Test NYC Yellow Taxi dataset with taxi-specific queries using different loaders."""
    _run_cross_engine_test(
        spark_engine,
        embucket_engine,
        nyc_yellow_taxi,
        query_id,
        query_sql,
        metrics_recorder,
        request,
    )


# NYC Green Taxi Tests
@pytest.mark.parametrize("nyc_green_taxi", ["spark", "embucket"], indirect=True)
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
            "SELECT CAST(lpep_pickup_datetime AS DATE) AS d, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY CAST(lpep_pickup_datetime AS DATE)",
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
def test_green_nyc_taxi(
    spark_engine,
    embucket_engine,
    nyc_green_taxi,
    query_id,
    query_sql,
    metrics_recorder,
    request,
):
    """Test NYC Green Taxi dataset with taxi-specific queries using different loaders."""
    _run_cross_engine_test(
        spark_engine,
        embucket_engine,
        nyc_green_taxi,
        query_id,
        query_sql,
        metrics_recorder,
        request,
    )


# FHV Tests
@pytest.mark.parametrize("fhv", ["spark", "embucket"], indirect=True)
@pytest.mark.parametrize(
    "query_id,query_sql",
    [
        ("q_count", "SELECT COUNT(*) FROM {{TABLE:table}}"),
        (
            "q_base_counts",
            "SELECT dispatching_base_num, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY dispatching_base_num",
        ),
        (
            "q_daily_counts",
            "SELECT CAST(pickup_datetime AS DATE) AS d, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY CAST(pickup_datetime AS DATE)",
        ),
        (
            "q_location_counts",
            "SELECT DOlocationID, COUNT(*) AS c FROM {{TABLE:table}} WHERE DOlocationID IS NOT NULL GROUP BY DOlocationID",
        ),
        (
            "q_base_affiliation",
            "SELECT dispatching_base_num, Affiliated_base_number, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY dispatching_base_num, Affiliated_base_number",
        ),
    ],
    ids=[
        "q_count",
        "q_base_counts",
        "q_daily_counts",
        "q_location_counts",
        "q_base_affiliation",
    ],
)
def test_fhv(
    spark_engine, embucket_engine, fhv, query_id, query_sql, metrics_recorder, request
):
    """Test NYC FHV Taxi dataset with taxi-specific queries using different loaders."""
    _run_cross_engine_test(
        spark_engine,
        embucket_engine,
        fhv,
        query_id,
        query_sql,
        metrics_recorder,
        request,
    )


# TPC-H Benchmark Tests - Real TPC-H queries using multiple tables
@pytest.mark.parametrize("tpch_full", ["spark", "embucket"], indirect=True)
@pytest.mark.parametrize(
    "query_id,query_sql", TPCH_QUERIES, ids=[query_id for query_id, _ in TPCH_QUERIES]
)
def test_tpch_benchmark_queries(
    spark_engine,
    embucket_engine,
    tpch_full,
    query_id,
    query_sql,
    metrics_recorder,
    request,
):
    """Test actual TPC-H benchmark queries using complete dataset with all tables and different loaders."""
    _run_multi_table_test(
        spark_engine,
        embucket_engine,
        tpch_full,
        query_id,
        query_sql,
        metrics_recorder,
        request,
    )


# TPC-DS Benchmark Tests
@pytest.mark.parametrize("tpcds_full", ["spark", "embucket"], indirect=True)
@pytest.mark.parametrize(
    "query_id,query_sql", TPCDS_QUERIES, ids=[query_id for query_id, _ in TPCDS_QUERIES]
)
def test_tpcds_benchmark_queries(
    spark_engine,
    embucket_engine,
    tpcds_full,
    query_id,
    query_sql,
    metrics_recorder,
    request,
):
    """Test TPC-DS queries using complete dataset with all tables and different loaders."""
    _run_multi_table_test(
        spark_engine,
        embucket_engine,
        tpcds_full,
        query_id,
        query_sql,
        metrics_recorder,
        request,
    )


# Clickbench Benchmark Tests
@pytest.mark.parametrize("clickbench_hits", ["spark", "embucket"], indirect=True)
@pytest.mark.parametrize(
    "query_id,query_sql",
    CLICKBENCH_QUERIES,
    ids=[query_id for query_id, _ in CLICKBENCH_QUERIES],
)
def test_clickbench_hits(
    spark_engine,
    embucket_engine,
    clickbench_hits,
    query_id,
    query_sql,
    metrics_recorder,
    request,
):
    """Test Clickbench queries using complete dataset with all tables and different loaders."""
    dataset, table_name, _ = clickbench_hits

    # Create a dictionary with "hits" as the key, similar to TPC-H test approach
    loaded_tables = {"hits": (dataset, table_name, None)}

    # Use the multi-table test function which correctly handles aliases
    _run_multi_table_test(
        spark_engine,
        embucket_engine,
        loaded_tables,
        query_id,
        query_sql,
        metrics_recorder,
        request,
    )
