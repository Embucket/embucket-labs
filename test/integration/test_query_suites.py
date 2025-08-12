import os
import pytest


def _unique_name(base: str, run_id: str, suffix: str = "") -> str:
    return f"{base}_{run_id}{('_' + suffix) if suffix else ''}"


@pytest.mark.parametrize(
    "writer_engine,reader_engine",
    [
        ("embucket_engine", "spark_engine"),
        ("spark_engine", "embucket_engine"),
    ],
    indirect=["writer_engine", "reader_engine"],
)
def test_query_suites_match(
    query_suite,
    writer_engine,
    reader_engine,
    datasets_by_name,
    test_run_id,
    request,
):
    if query_suite is None:
        pytest.skip("No query suites configured")

    # Prepare all relations (create + load) using the writer engine
    alias_to_table = {}
    for alias, ds_name in query_suite.relations.items():
        ds = datasets_by_name.get(ds_name)
        if not ds:
            pytest.skip(f"Dataset '{ds_name}' not found for suite {query_suite.name}")
        tname = _unique_name(ds.table, test_run_id, suffix=query_suite.name)
        writer_engine.create_table(ds, tname)
        loader = request.getfixturevalue("load_into_embucket" if writer_engine.name == "embucket" else "load_into_spark")
        writer_engine.load(ds, tname, loader)
        alias_to_table[alias] = (ds, tname)

    # Run each query in the suite on both engines and compare results
    from test.integration.test_spark_embucket_io import _compare_result_sets

    for q in query_suite.queries:
        sql = q.sql
        if not sql and q.sql_path:
            with open(q.sql_path, "r", encoding="utf-8") as f:
                sql = f.read()
        if not sql:
            pytest.skip(f"Query {q.id} has no SQL for suite {query_suite.name}")

        left = writer_engine.run_suite_query(sql, alias_to_table)
        right = reader_engine.run_suite_query(sql, alias_to_table)
        ok, msg = _compare_result_sets(left, right)
        assert ok, f"Suite {query_suite.name} query {q.id} mismatch ({writer_engine.name} vs {reader_engine.name}): {msg}"
