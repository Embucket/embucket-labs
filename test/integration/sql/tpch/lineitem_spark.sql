-- Spark SQL DDL for TPC-H LINEITEM (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  l_orderkey BIGINT,
  l_partkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber INT,
  l_quantity DOUBLE,
  l_extendedprice DOUBLE,
  l_discount DOUBLE,
  l_tax DOUBLE,
  l_returnflag STRING,
  l_linestatus STRING,
  l_shipdate DATE,
  l_commitdate DATE,
  l_receiptdate DATE,
  l_shipinstruct STRING,
  l_shipmode STRING,
  l_comment STRING
) USING iceberg;

