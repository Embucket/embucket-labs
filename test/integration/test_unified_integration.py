import pytest
from conftest import compare_result_sets


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


@pytest.mark.parametrize(
    "query_id,query_sql",
    [
        ("q_count", "SELECT COUNT(*) FROM {{TABLE:table}}"),
        (
            "q_vendor_counts",
            "SELECT vendor_id, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY vendor_id",
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
            "SELECT vendor_id, AVG(trip_distance) AS avg_dist FROM {{TABLE:table}} GROUP BY vendor_id",
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
    spark_nyc_taxi,
    query_id,
    query_sql,
):
    """Test NYC Taxi dataset with taxi-specific queries."""
    _run_cross_engine_test(
        spark_engine, embucket_engine, spark_nyc_taxi, query_id, query_sql
    )


# TPC-H Benchmark Tests - Real TPC-H queries using multiple tables
@pytest.mark.parametrize(
    "query_id,query_sql",
    [
        (
            "tpch_q3_shipping_priority",
            """SELECT
                l.L_ORDERKEY,
                SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue,
                o.O_ORDERDATE,
                o.O_SHIPPRIORITY
            FROM
                {{TABLE:customer}} c,
                {{TABLE:orders}} o,
                {{TABLE:lineitem}} l
            WHERE
                c.C_MKTSEGMENT = 'BUILDING'
                AND c.C_CUSTKEY = o.O_CUSTKEY
                AND l.L_ORDERKEY = o.O_ORDERKEY
                AND o.O_ORDERDATE < CAST('1995-03-15' AS DATE)
                AND l.L_SHIPDATE > CAST('1995-03-15' AS DATE)
            GROUP BY
                l.L_ORDERKEY,
                o.O_ORDERDATE,
                o.O_SHIPPRIORITY
            ORDER BY
                revenue DESC,
                o.O_ORDERDATE
            LIMIT 10""",
        ),
        (
            "tpch_q5_local_supplier_volume",
            """SELECT
                n.N_NAME,
                SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
            FROM
                {{TABLE:customer}} c,
                {{TABLE:orders}} o,
                {{TABLE:lineitem}} l,
                {{TABLE:supplier}} s,
                {{TABLE:nation}} n,
                {{TABLE:region}} r
            WHERE
                c.C_CUSTKEY = o.O_CUSTKEY
                AND l.L_ORDERKEY = o.O_ORDERKEY
                AND l.L_SUPPKEY = s.S_SUPPKEY
                AND c.C_NATIONKEY = s.S_NATIONKEY
                AND s.S_NATIONKEY = n.N_NATIONKEY
                AND n.N_REGIONKEY = r.R_REGIONKEY
                AND r.R_NAME = 'ASIA'
                AND o.O_ORDERDATE >= CAST('1994-01-01' AS DATE)
                AND o.O_ORDERDATE < CAST('1995-01-01' AS DATE)
            GROUP BY
                n.N_NAME
            ORDER BY
                revenue DESC
            LIMIT 10""",
        ),
        (
            "tpch_q8_national_market_share",
            """SELECT
                o_year,
                SUM(CASE
                    WHEN nation = 'BRAZIL' THEN volume
                    ELSE 0
                END) / SUM(volume) AS mkt_share
            FROM
                (
                    SELECT
                        YEAR(o.O_ORDERDATE) AS o_year,
                        l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS volume,
                        n2.N_NAME AS nation
                    FROM
                        {{TABLE:part}} p,
                        {{TABLE:supplier}} s,
                        {{TABLE:lineitem}} l,
                        {{TABLE:orders}} o,
                        {{TABLE:customer}} c,
                        {{TABLE:nation}} n1,
                        {{TABLE:nation}} n2,
                        {{TABLE:region}} r
                    WHERE
                        p.P_PARTKEY = l.L_PARTKEY
                        AND s.S_SUPPKEY = l.L_SUPPKEY
                        AND l.L_ORDERKEY = o.O_ORDERKEY
                        AND o.O_CUSTKEY = c.C_CUSTKEY
                        AND c.C_NATIONKEY = n1.N_NATIONKEY
                        AND n1.N_REGIONKEY = r.R_REGIONKEY
                        AND r.R_NAME = 'AMERICA'
                        AND s.S_NATIONKEY = n2.N_NATIONKEY
                        AND o.O_ORDERDATE >= CAST('1995-01-01' AS DATE)
                        AND o.O_ORDERDATE <= CAST('1996-12-31' AS DATE)
                        AND p.P_TYPE = 'ECONOMY ANODIZED STEEL'
                ) AS all_nations
            GROUP BY
                o_year
            ORDER BY
                o_year
            LIMIT 10""",
        ),
    ],
    ids=[
        "tpch_q3_shipping_priority",
        "tpch_q5_local_supplier_volume",
        "tpch_q8_national_market_share",
    ],
)
def test_tpch_benchmark_queries(
    spark_engine, embucket_engine, spark_tpch_full, query_id, query_sql
):
    """Test actual TPC-H benchmark queries using complete dataset with all tables."""
    _run_multi_table_test(
        spark_engine, embucket_engine, spark_tpch_full, query_id, query_sql
    )
