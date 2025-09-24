import os

from .tpch_table_names import parametrize_tpch_statements

# TPC-H DDL statements with parametrized table names
_TPCH_DDL_RAW = [
    (
        "customer",
        """
        -- Snowflake-like DDL for TPC-H CUSTOMER
        CREATE OR REPLACE TABLE {CUSTOMER_TABLE} (
          c_custkey BIGINT,
          c_name VARCHAR(25),
          c_address VARCHAR(40),
          c_nationkey INT,
          c_phone VARCHAR(15),
          c_acctbal DOUBLE,
          c_mktsegment VARCHAR(10),
          c_comment VARCHAR(117)
        );
        """
    ),
    (
        "lineitem",
        """
        -- Snowflake-like DDL for TPC-H LINEITEM
        CREATE OR REPLACE TABLE {LINEITEM_TABLE} (
          l_orderkey BIGINT,
          l_partkey BIGINT,
          l_suppkey BIGINT,
          l_linenumber INT,
          l_quantity DOUBLE,
          l_extendedprice DOUBLE,
          l_discount DOUBLE,
          l_tax DOUBLE,
          l_returnflag VARCHAR(1),
          l_linestatus VARCHAR(1),
          l_shipdate DATE,
          l_commitdate DATE,
          l_receiptdate DATE,
          l_shipinstruct VARCHAR(25),
          l_shipmode VARCHAR(10),
          l_comment VARCHAR(44)
        );
        """
    ),
    (
        "nation",
        """
        -- Snowflake-like DDL for TPC-H NATION
        CREATE OR REPLACE TABLE {NATION_TABLE} (
          n_nationkey INT,
          n_name VARCHAR(25),
          n_regionkey INT,
          n_comment VARCHAR(152)
        );
        """
    ),
    (
        "orders",
        """
        -- Snowflake-like DDL for TPC-H ORDERS
        CREATE OR REPLACE TABLE {ORDERS_TABLE} (
          o_orderkey BIGINT,
          o_custkey BIGINT,
          o_orderstatus VARCHAR(1),
          o_totalprice DOUBLE,
          o_orderdate DATE,
          o_orderpriority VARCHAR(15),
          o_clerk VARCHAR(15),
          o_shippriority INT,
          o_comment VARCHAR(79)
        );
        """
    ),
    (
        "part",
        """
        -- Snowflake-like DDL for TPC-H PART
        CREATE OR REPLACE TABLE {PART_TABLE} (
          p_partkey BIGINT,
          p_name VARCHAR(55),
          p_mfgr VARCHAR(25),
          p_brand VARCHAR(10),
          p_type VARCHAR(25),
          p_size INT,
          p_container VARCHAR(10),
          p_retailprice DOUBLE,
          p_comment VARCHAR(23)
        );
        """
    ),
    (
        "partsupp",
        """
        -- Snowflake-like DDL for TPC-H PARTSUPP
        CREATE OR REPLACE TABLE {PARTSUPP_TABLE} (
          ps_partkey BIGINT,
          ps_suppkey BIGINT,
          ps_availqty INT,
          ps_supplycost DOUBLE,
          ps_comment VARCHAR(199)
        );
        """
    ),
    (
        "region",
        """
        -- Snowflake-like DDL for TPC-H REGION
        CREATE OR REPLACE TABLE {REGION_TABLE} (
          r_regionkey INT,
          r_name VARCHAR(25),
          r_comment VARCHAR(152)
        );
        """
    ),
    (
        "supplier",
        """
        -- Snowflake-like DDL for TPC-H SUPPLIER
        CREATE OR REPLACE TABLE {SUPPLIER_TABLE} (
          s_suppkey BIGINT,
          s_name VARCHAR(25),
          s_address VARCHAR(40),
          s_nationkey INT,
          s_phone VARCHAR(15),
          s_acctbal DOUBLE,
          s_comment VARCHAR(101)
        );
        """
    ),
]


def parametrize_tpch_ddl(fully_qualified_names_for_embucket):
    """
    Replace table name placeholders in TPC-H DDL statements with actual table names.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use just the default table names.

    Returns:
        list: A list of (table_name, parametrized_ddl) tuples.
    """
    return parametrize_tpch_statements(_TPCH_DDL_RAW, fully_qualified_names_for_embucket)
