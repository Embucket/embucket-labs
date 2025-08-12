import pytest


def _unique_table(base: str, run_id: str, suffix: str = "") -> str:
    return f"{base}_{run_id}{('_' + suffix) if suffix else ''}"


def _rows_to_tuples(rows):
    out = []
    for r in rows:
        try:
            out.append(tuple(r))
        except TypeError:
            try:
                d = r.asDict(recursive=True)
                out.append(tuple(d.values()))
            except Exception:
                out.append((r,))
    return out


def _normalize_value(v):
    import decimal
    from datetime import datetime, date

    if v is None:
        return (0, None)
    if isinstance(v, bool):
        return (1, bool(v))
    if isinstance(v, int):
        return (2, int(v))
    if isinstance(v, float):
        return (3, round(float(v), 12))
    if isinstance(v, decimal.Decimal):
        return (4, str(v))
    if isinstance(v, (datetime, date)):
        return (5, v.isoformat())
    if isinstance(v, (bytes, bytearray)):
        return (6, v.hex())
    return (9, str(v))


def _sort_rows(rows):
    return sorted(rows, key=lambda row: tuple(_normalize_value(v) for v in row))


def _is_number(x):
    import decimal

    return isinstance(x, (int, float, decimal.Decimal))


def _compare_result_sets(a, b, rel_tol=1e-6, abs_tol=1e-9):
    import math

    if len(a) != len(b):
        return False, f"Row count differs: {len(a)} vs {len(b)}"
    sa = _sort_rows(a)
    sb = _sort_rows(b)
    for i, (ra, rb) in enumerate(zip(sa, sb)):
        if len(ra) != len(rb):
            return False, f"Row {i} length differs: {len(ra)} vs {len(rb)}"
        for j, (va, vb) in enumerate(zip(ra, rb)):
            if va == vb:
                continue
            if _is_number(va) and _is_number(vb):
                if math.isclose(float(va), float(vb), rel_tol=rel_tol, abs_tol=abs_tol):
                    continue
                return False, f"Row {i}, col {j} numeric mismatch: {va} vs {vb}"
            if va in (None, "") and vb in (None, ""):
                continue
            if str(va) != str(vb):
                return False, f"Row {i}, col {j} differs: {va} vs {vb}"
    return True, "OK"


@pytest.mark.parametrize(
    "writer_engine,reader_engine",
    [
        ("embucket_engine", "spark_engine"),
        ("spark_engine", "embucket_engine"),
    ],
    indirect=["writer_engine", "reader_engine"],
)
def test_roundtrip_rowcount(
    dataset,
    writer_engine,
    reader_engine,
    test_run_id: str,
    request,
):
    """Write with one engine and read with the other for a given dataset.

    Assert equality of total row counts between engines.
    """
    table_name = _unique_table(dataset.table, test_run_id, suffix="rt")

    # Create table via writer
    writer_engine.create_table(dataset, table_name)

    # Load via writer using the appropriate loader
    if writer_engine.name == "embucket":
        loader = request.getfixturevalue("load_into_embucket")
    else:
        loader = request.getfixturevalue("load_into_spark")
    writer_engine.load(dataset, table_name, loader)

    # Compare counts across engines
    wcount = writer_engine.count(dataset, table_name)
    rcount = reader_engine.count(dataset, table_name)
    assert (
        wcount == rcount
    ), f"Row count mismatch for {dataset.name} ({writer_engine.name}→{reader_engine.name})"


@pytest.mark.parametrize(
    "engine",
    ["embucket_engine", "spark_engine"],
    indirect=["engine"],
)
def test_simple_aggregates(
    dataset,
    engine,
    test_run_id: str,
    request,
):
    """Run basic aggregates to validate schema mapping for each engine/dataset."""
    table_name = _unique_table(dataset.table, test_run_id, suffix="agg")

    # Create and load fresh table per test to avoid duplication

    engine.create_table(dataset, table_name)
    loader = request.getfixturevalue(
        "load_into_embucket" if engine.name == "embucket" else "load_into_spark"
    )
    engine.load(dataset, table_name, loader)

    out = engine.agg(dataset, table_name, dataset.first_col, dataset.numeric_col)
    assert int(out["count_all"]) >= 0
    if dataset.first_col:
        assert int(out.get("count_distinct", 0)) >= 0


@pytest.mark.parametrize(
    "writer_engine,reader_engine",
    [
        ("embucket_engine", "spark_engine"),
        ("spark_engine", "embucket_engine"),
    ],
    indirect=["writer_engine", "reader_engine"],
)
def test_predefined_queries_match(
    dataset,
    writer_engine,
    reader_engine,
    test_run_id: str,
    load_into_embucket,
    load_into_spark,
):
    """Execute dataset-provided queries on both engines and compare results."""
    if not dataset.queries:
        pytest.skip(f"No queries defined for dataset {dataset.name}")

    table_name = _unique_table(dataset.table, test_run_id, suffix="qry")

    # Prepare table and load via writer
    writer_engine.create_table(dataset, table_name)
    if writer_engine.name == "embucket":
        writer_engine.load(dataset, table_name, load_into_embucket)
    else:
        writer_engine.load(dataset, table_name, load_into_spark)

    # Execute each query and compare results
    for q in dataset.queries:
        qid = q.get("id") or "query"
        sql = q.get("sql")
        sql_path = q.get("sql_path")
        if not sql and not sql_path:
            pytest.skip(f"Query {qid} has no sql/sql_path for dataset {dataset.name}")
        if not sql:
            with open(sql_path, "r", encoding="utf-8") as f:
                sql = f.read()

        left = writer_engine.run_query(dataset, table_name, sql)
        right = reader_engine.run_query(dataset, table_name, sql)

        ok, msg = _compare_result_sets(left, right)
        assert (
            ok
        ), f"Query {qid} mismatch on {dataset.name} ({writer_engine.name} vs {reader_engine.name}): {msg}"
