select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    iceberg_scan('/home/artem/work/reps/github.com/embucket/embucket/test/dbt_integration_tests/dbt-gitlab/data/embucket/public/sample_table/metadata/d5f48ce4-3f76-4f6c-9e48-631c847a15a2.metadata.json')
        as customer,
    iceberg_scan('/home/artem/work/reps/github.com/embucket/embucket/test/dbt_integration_tests/dbt-gitlab/data/embucket/public/sample_table/metadata/d5f48ce4-3f76-4f6c-9e48-631c847a15a2.metadata.json')
        as orders,
    iceberg_scan('/home/artem/work/reps/github.com/embucket/embucket/test/dbt_integration_tests/dbt-gitlab/data/embucket/public/sample_table/metadata/d5f48ce4-3f76-4f6c-9e48-631c847a15a2.metadata.json')
        as lineitem
where
        o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 300
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate;