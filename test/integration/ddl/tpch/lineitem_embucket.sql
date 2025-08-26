-- Snowflake-like DDL for TPC-H LINEITEM
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
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

