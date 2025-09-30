import os
import re

from .tpch_table_names import parametrize_tpch_statements

# Original TPC-H queries with bare table names
_TPCH_QUERIES_RAW = [
    (
        "tpch-q1",
        """
        SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
        FROM
            {LINEITEM_TABLE}
        WHERE
            l_shipdate <= DATEADD(day, -116, '1998-12-01')
        GROUP BY
            l_returnflag,
            l_linestatus
        ORDER BY
            l_returnflag,
            l_linestatus;
        """
    ),
    (
        "tpch-q2",
        """
        SELECT
            s_acctbal,
            s_name,
            n_name,
            p_partkey,
            p_mfgr,
            s_address,
            s_phone,
            s_comment
        FROM
            {PART_TABLE},
            {SUPPLIER_TABLE},
            {PARTSUPP_TABLE},
            {NATION_TABLE},
            {REGION_TABLE}
        WHERE
            p_partkey = ps_partkey
          AND s_suppkey = ps_suppkey
          AND p_size = 15
          AND p_type LIKE '%BRASS'
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'EUROPE'
          AND ps_supplycost = (
            SELECT MIN(ps_supplycost)
            FROM
                {PARTSUPP_TABLE},
                {SUPPLIER_TABLE},
                {NATION_TABLE},
                {REGION_TABLE}
            WHERE
                p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'EUROPE'
        )
        ORDER BY
            s_acctbal DESC,
            n_name,
            s_name,
            p_partkey
            LIMIT 100;
        """
    ),
    (
        "tpch-q3",
        """
        SELECT
            l_orderkey,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            o_orderdate,
            o_shippriority
        FROM
            {CUSTOMER_TABLE},
            {ORDERS_TABLE},
            {LINEITEM_TABLE}
        WHERE
            c_mktsegment = 'BUILDING'
          AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate < DATE '1995-03-15'
          AND l_shipdate > DATE '1995-03-15'
        GROUP BY
            l_orderkey,
            o_orderdate,
            o_shippriority
        ORDER BY
            revenue DESC,
            o_orderdate
            LIMIT 10;
        """
    ),
    (
        "tpch-q4",
        """
        SELECT
            o_orderpriority,
            COUNT(*) AS order_count
        FROM
            {ORDERS_TABLE}
        WHERE
            o_orderdate >= DATE '1993-07-01'
          AND o_orderdate < DATEADD(month, 3, '1993-07-01')
          AND EXISTS (
            SELECT
                *
            FROM
                {LINEITEM_TABLE}
            WHERE
                l_orderkey = o_orderkey
              AND l_commitdate < l_receiptdate
        )
        GROUP BY
            o_orderpriority
        ORDER BY
            o_orderpriority;
        """
    ),
    (
        "tpch-q5",
        """
        SELECT
            n_name,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM
            {CUSTOMER_TABLE},
            {ORDERS_TABLE},
            {LINEITEM_TABLE},
            {SUPPLIER_TABLE},
            {NATION_TABLE},
            {REGION_TABLE}
        WHERE
            c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND l_suppkey = s_suppkey
          AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= DATE '1994-01-01'
          AND o_orderdate < DATEADD(year, 1, '1994-01-01')
        GROUP BY
            n_name
        ORDER BY
            revenue DESC;
        """
    ),
    (
        "tpch-q6",
        """
        SELECT
            SUM(l_extendedprice * l_discount) AS revenue
        FROM
            {LINEITEM_TABLE}
        WHERE
            l_shipdate >= DATE '1994-01-01'
          AND l_shipdate < DATEADD(year, 1, '1994-01-01')
          AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
          AND l_quantity < 24;
        """
    ),
    (
        "tpch-q7",
        """
        SELECT
            supp_nation,
            cust_nation,
            l_year,
            SUM(volume) AS revenue
        FROM
            (
                SELECT
                    n1.n_name AS supp_nation,
                    n2.n_name AS cust_nation,
                    EXTRACT(year FROM l_shipdate) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume
                FROM
                    {SUPPLIER_TABLE},
                    {LINEITEM_TABLE},
                    {ORDERS_TABLE},
                    {CUSTOMER_TABLE},
                    {NATION_TABLE} n1,
                    {NATION_TABLE} n2
                WHERE
                    s_suppkey = l_suppkey
                  AND o_orderkey = l_orderkey
                  AND c_custkey = o_custkey
                  AND s_nationkey = n1.n_nationkey
                  AND c_nationkey = n2.n_nationkey
                  AND (
                    (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                    )
                  AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            ) AS shipping
        GROUP BY
            supp_nation,
            cust_nation,
            l_year
        ORDER BY
            supp_nation,
            cust_nation,
            l_year;
        """
    ),
    (
        "tpch-q8",
        """
        SELECT
            o_year,
            SUM(CASE
                    WHEN nation = 'BRAZIL' THEN volume
                    ELSE 0
                END) / SUM(volume) AS mkt_share
        FROM
            (
                SELECT
                    EXTRACT(year FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    n2.n_name AS nation
                FROM
                    {PART_TABLE},
                    {SUPPLIER_TABLE},
                    {LINEITEM_TABLE},
                    {ORDERS_TABLE},
                    {CUSTOMER_TABLE},
                    {NATION_TABLE} n1,
                    {NATION_TABLE} n2,
                    {REGION_TABLE}
                WHERE
                    p_partkey = l_partkey
                  AND s_suppkey = l_suppkey
                  AND l_orderkey = o_orderkey
                  AND o_custkey = c_custkey
                  AND c_nationkey = n1.n_nationkey
                  AND n1.n_regionkey = r_regionkey
                  AND r_name = 'AMERICA'
                  AND s_nationkey = n2.n_nationkey
                  AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                  AND p_type = 'ECONOMY ANODIZED STEEL'
            ) AS all_nations
        GROUP BY
            o_year
        ORDER BY
            o_year;
        """
    ),
    (
        "tpch-q9",
        """
        SELECT
            nation,
            o_year,
            SUM(amount) AS sum_profit
        FROM
            (
                SELECT
                    n_name AS nation,
                    EXTRACT(year FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                FROM
                    {PART_TABLE},
                    {SUPPLIER_TABLE},
                    {LINEITEM_TABLE},
                    {PARTSUPP_TABLE},
                    {ORDERS_TABLE},
                    {NATION_TABLE}
                WHERE
                    s_suppkey = l_suppkey
                  AND ps_suppkey = l_suppkey
                  AND ps_partkey = l_partkey
                  AND p_partkey = l_partkey
                  AND o_orderkey = l_orderkey
                  AND s_nationkey = n_nationkey
                  AND p_name LIKE '%green%'
            ) AS profit
        GROUP BY
            nation,
            o_year
        ORDER BY
            nation,
            o_year DESC;
        """
    ),
    (
        "tpch-q10",
        """
        SELECT
            ps_partkey,
            SUM(ps_supplycost * ps_availqty) AS value
        FROM
            {PARTSUPP_TABLE},
            {SUPPLIER_TABLE},
            {NATION_TABLE}
        WHERE
            ps_suppkey = s_suppkey
          AND s_nationkey = n_nationkey
          AND n_name = 'GERMANY'
        GROUP BY
            ps_partkey
        HAVING
            SUM(ps_supplycost * ps_availqty) > (
            SELECT
            SUM(ps_supplycost * ps_availqty) * 0.0001
            FROM
            {PARTSUPP_TABLE},
            {SUPPLIER_TABLE},
            {NATION_TABLE}
            WHERE
            ps_suppkey = s_suppkey
           AND s_nationkey = n_nationkey
           AND n_name = 'GERMANY'
            )
        ORDER BY value DESC;
        """
    ),
    (
        "tpch-q11",
        """
        SELECT
            l_shipmode,
            SUM(CASE
                    WHEN o_orderpriority = '1-URGENT'
                        OR o_orderpriority = '2-HIGH'
                        THEN 1
                    ELSE 0
                END) AS high_line_count,
            SUM(CASE
                    WHEN o_orderpriority <> '1-URGENT'
                        AND o_orderpriority <> '2-HIGH'
                        THEN 1
                    ELSE 0
                END) AS low_line_count
        FROM
            {ORDERS_TABLE},
            {LINEITEM_TABLE}
        WHERE
            o_orderkey = l_orderkey
          AND l_shipmode IN ('MAIL', 'SHIP')
          AND l_commitdate < l_receiptdate
          AND l_shipdate < l_commitdate
          AND l_receiptdate >= DATE '1994-01-01'
          AND o_orderdate < DATEADD(year, 1, '1994-01-01')
        GROUP BY
            l_shipmode
        ORDER BY
            l_shipmode;
        """
    ),
    (
        "tpch-q12",
        """
        SELECT
            c_count,
            COUNT(*) AS custdist
        FROM
            (
                SELECT
                    c_custkey,
                    COUNT(o_orderkey)
                FROM
                    {CUSTOMER_TABLE} LEFT OUTER JOIN {ORDERS_TABLE}
                                             ON c_custkey = o_custkey
                                                 AND o_comment NOT LIKE '%special%requests%'
                GROUP BY
                    c_custkey
            ) AS c_orders (c_custkey, c_count)
        GROUP BY
            c_count
        ORDER BY
            custdist DESC,
            c_count DESC;
        """
    ),
    (
        "tpch-q13",
        """
        SELECT
            100.00 * SUM(CASE
                             WHEN p_type LIKE 'PROMO%'
                                 THEN l_extendedprice * (1 - l_discount)
                             ELSE 0
                END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM
            {LINEITEM_TABLE},
            {PART_TABLE}
        WHERE
            l_partkey = p_partkey
          AND l_shipdate >= DATE '1995-09-01'
          AND l_shipdate < DATEADD(month, 1, '1995-09-01');
        """
    ),
    (
        "tpch-q14",
        """
        WITH revenue AS (
            SELECT
                l_suppkey AS supplier_no,
                SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM
                {LINEITEM_TABLE}
            WHERE
                l_shipdate >= TO_DATE('1996-01-01')
              AND l_shipdate < TO_DATE('1996-04-01')
            GROUP BY
                l_suppkey
        )
        SELECT
            s_suppkey,
            s_name,
            s_address,
            s_phone,
            total_revenue
        FROM
            {SUPPLIER_TABLE},
            revenue
        WHERE
            s_suppkey = supplier_no
          AND total_revenue = (
            SELECT MAX(total_revenue)
            FROM revenue
        )
        ORDER BY
            s_suppkey;
        """
    ),
    (
        "tpch-q15",
        """
        SELECT
            p_brand,
            p_type,
            p_size,
            COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        FROM
            {PARTSUPP_TABLE},
            {PART_TABLE}
        WHERE
            p_partkey = ps_partkey
          AND p_brand <> 'Brand#45'
          AND p_type NOT LIKE 'MEDIUM POLISHED%'
          AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
          AND ps_suppkey NOT IN (
            SELECT
                s_suppkey
            FROM
                {SUPPLIER_TABLE}
            WHERE
                s_comment LIKE '%Customer%Complaints%'
        )
        GROUP BY
            p_brand,
            p_type,
            p_size
        ORDER BY
            supplier_cnt DESC,
            p_brand,
            p_type,
            p_size;
        """
    ),
    (
        "tpch-q16",
        """
        SELECT
            SUM(l_extendedprice) / 7.0 AS avg_yearly
        FROM
            {LINEITEM_TABLE},
            {PART_TABLE}
        WHERE
            p_partkey = l_partkey
          AND p_brand = 'Brand#23'
          AND p_container = 'MED BOX'
          AND l_quantity < (
            SELECT
                0.2 * AVG(l_quantity)
            FROM
                {LINEITEM_TABLE}
            WHERE
                l_partkey = p_partkey
        );
        """
    ),
    (
        "tpch-q17",
        """
        SELECT
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            SUM(l_quantity)
        FROM
            {CUSTOMER_TABLE},
            {ORDERS_TABLE},
            {LINEITEM_TABLE}
        WHERE
            o_orderkey IN (
                SELECT
                    l_orderkey
                FROM
                    {LINEITEM_TABLE}
                GROUP BY
                    l_orderkey
                HAVING
                    SUM(l_quantity) > 300
            )
          AND c_custkey = o_custkey
          AND o_orderkey = l_orderkey
        GROUP BY
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice
        ORDER BY
            o_totalprice DESC,
            o_orderdate
            LIMIT 100;
        """
    ),
    (
        "tpch-q18",
        """
        SELECT
            SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM
            {LINEITEM_TABLE},
            {PART_TABLE}
        WHERE
            (
                p_partkey = l_partkey
                    AND p_brand = 'Brand#12'
                    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND l_quantity >= 1 AND l_quantity <= 1 + 10
                    AND p_size BETWEEN 1 AND 5
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
           OR
            (
                p_partkey = l_partkey
                    AND p_brand = 'Brand#23'
                    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    AND l_quantity >= 10 AND l_quantity <= 10 + 10
                    AND p_size BETWEEN 1 AND 10
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
           OR
            (
                p_partkey = l_partkey
                    AND p_brand = 'Brand#34'
                    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    AND l_quantity >= 20 AND l_quantity <= 20 + 10
                    AND p_size BETWEEN 1 AND 15
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                );
        """
    ),
    (
        "tpch-q19",
        """
        SELECT
            s_name,
            s_address
        FROM
            {SUPPLIER_TABLE},
            {NATION_TABLE}
        WHERE
            s_suppkey IN (
                SELECT
                    ps_suppkey
                FROM
                    {PARTSUPP_TABLE}
                WHERE
                    ps_partkey IN (
                        SELECT
                            p_partkey
                        FROM
                            {PART_TABLE}
                        WHERE
                            p_name LIKE 'forest%'
                    )
                  AND ps_availqty > (
                    SELECT
                        0.5 * SUM(l_quantity)
                    FROM
                        {LINEITEM_TABLE}
                    WHERE
                        l_partkey = ps_partkey
                      AND l_suppkey = ps_suppkey
                      AND l_shipdate >= DATE '1994-01-01'
                      AND l_shipdate < DATEADD(year, 1, '1994-01-01')
                )
            )
          AND s_nationkey = n_nationkey
          AND n_name = 'CANADA'
        ORDER BY
            s_name;
        """
    ),
    (
        "tpch-q20",
        """
        SELECT
            s_name,
            COUNT(*) AS numwait
        FROM
            {SUPPLIER_TABLE},
            {LINEITEM_TABLE} l1,
            {ORDERS_TABLE},
            {NATION_TABLE}
        WHERE
            s_suppkey = l1.l_suppkey
          AND o_orderkey = l1.l_orderkey
          AND o_orderstatus = 'F'
          AND l1.l_receiptdate > l1.l_commitdate
          AND EXISTS (
            SELECT
                *
            FROM
                {LINEITEM_TABLE} l2
            WHERE
                l2.l_orderkey = l1.l_orderkey
              AND l2.l_suppkey <> l1.l_suppkey
        )
          AND NOT EXISTS (
            SELECT
                *
            FROM
                {LINEITEM_TABLE} l3
            WHERE
                l3.l_orderkey = l1.l_orderkey
              AND l3.l_suppkey <> l1.l_suppkey
              AND l3.l_receiptdate > l3.l_commitdate
        )
          AND s_nationkey = n_nationkey
          AND n_name = 'SAUDI ARABIA'
        GROUP BY
            s_name
        ORDER BY
            numwait DESC,
            s_name
            LIMIT 100;
        """
    ),
    (
        "tpch-q21",
        """
        SELECT
            cntrycode,
            COUNT(*) AS numcust,
            SUM(c_acctbal) AS totacctbal
        FROM
            (
                SELECT
                    SUBSTRING(c_phone, 1, 2) AS cntrycode,
                    c_acctbal
                FROM
                    {CUSTOMER_TABLE}
                WHERE
                    SUBSTRING(c_phone, 1, 2) IN
                    ('13', '31', '23', '29', '30', '18', '17')
                  AND c_acctbal > (
                    SELECT
                        AVG(c_acctbal)
                    FROM
                        {CUSTOMER_TABLE}
                    WHERE
                        c_acctbal > 0.00
                      AND SUBSTRING(c_phone, 1, 2) IN
                          ('13', '31', '23', '29', '30', '18', '17')
                )
                  AND NOT EXISTS (
                    SELECT
                        *
                    FROM
                        {ORDERS_TABLE}
                    WHERE
                        o_custkey = c_custkey
                )
            ) AS custsale
        GROUP BY
            cntrycode
        ORDER BY
            cntrycode;
        """
    ),
    (
        "tpch-q22",
        """
        SELECT
            c_custkey,
            c_name,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
        FROM
            {CUSTOMER_TABLE},
            {ORDERS_TABLE},
            {LINEITEM_TABLE},
            {NATION_TABLE}
        WHERE
            c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND o_orderdate >= DATE '1993-10-01'
          AND o_orderdate < DATEADD(month, 3, '1993-10-01')
          AND l_returnflag = 'R'
          AND c_nationkey = n_nationkey
        GROUP BY
            c_custkey,
            c_name,
            c_acctbal,
            c_phone,
            n_name,
            c_address,
            c_comment
        ORDER BY
            revenue DESC
            LIMIT 20;
        """
    ),
]


def parametrize_tpch_queries(fully_qualified_names_for_embucket, use_custom_dataset=False):
    """
    Replace table name placeholders in TPC-H queries with actual table names.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use Snowflake built-in or custom tables based on use_custom_dataset.
        use_custom_dataset (bool): Only applies when fully_qualified_names_for_embucket=False (Snowflake).
                                   If False (default), use Snowflake's built-in TPC-H tables.
                                   If True, use custom tables in user's schema.

    Returns:
        list: A list of (query_name, parametrized_query) tuples.
    """
    return parametrize_tpch_statements(_TPCH_QUERIES_RAW, fully_qualified_names_for_embucket, use_custom_dataset)
