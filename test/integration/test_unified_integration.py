import pytest
import os
import yaml
from conftest import compare_result_sets


def pytest_generate_tests(metafunc):
    """Dynamically generate test parameters from queries.yaml."""
    if "query_id" in metafunc.fixturenames and "query_sql" in metafunc.fixturenames:
        # Load queries from YAML
        path = os.getenv("QUERIES_YAML", "queries.yaml")
        if not os.path.exists(path):
            pytest.skip(f"Queries file not found: {path}")
            
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        
        # Extract queries from the nyc_taxi_dataset suite
        queries = []
        for suite in config.get("suites", []):
            if suite.get("name") == "nyc_taxi_dataset":
                for query in suite.get("queries", []):
                    query_id = query.get("id", "unknown")
                    query_sql = query.get("sql", "").strip()
                    if query_sql:
                        # Convert {{TABLE:table}} to match our test format
                        query_sql = query_sql.replace("{{TABLE:table}}", "{{TABLE:table}}")
                        queries.append((query_id, query_sql))
                break
        
        if not queries:
            # Fallback queries if none found in YAML
            queries = [
                ("q_count", "SELECT COUNT(*) FROM {{TABLE:table}}"),
                ("q_vendor_counts", "SELECT vendor_id, COUNT(*) AS c FROM {{TABLE:table}} GROUP BY vendor_id"),
            ]
        
        metafunc.parametrize("query_id,query_sql", queries, ids=[q[0] for q in queries])


@pytest.mark.parametrize(
    "dataset_loaded_by",
    ["nyc_taxi_yellow_loaded_by_spark", "nyc_taxi_yellow_loaded_by_embucket"],
    indirect=True
)
def test_cross_engine_query_compatibility(dataset_loaded_by, spark_reader, embucket_reader, query_id, query_sql):
    """Test cross-engine compatibility: data loaded by one engine, queried by both engines."""
    # Get the dataset info from the indirect fixture
    nyc_dataset, nyc_table, loader_result = dataset_loaded_by
    
    # If the loader result is an exception, this means loading failed (expected for Embucket)
    if isinstance(loader_result, Exception):
        pytest.fail(f"Data loading failed: {loader_result}")

    # Set up table alias for query
    alias_to_table = {"table": (nyc_dataset, nyc_table)}

    # Run query with both reader engines
    spark_result = spark_reader.run_suite_query(query_sql, alias_to_table)
    embucket_result = embucket_reader.run_suite_query(query_sql, alias_to_table)

    # Compare results
    ok, msg = compare_result_sets(spark_result, embucket_result)
    assert ok, f"Query {query_id} mismatch between Spark and Embucket: {msg}"

    # Basic sanity checks
    assert len(spark_result) > 0, f"Query {query_id} should return at least one row"
    assert len(embucket_result) > 0, f"Query {query_id} should return at least one row"
