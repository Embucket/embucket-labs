-- Spark SQL DDL for TPC-H ORDERS (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  o_orderkey BIGINT,
  o_custkey BIGINT,
  o_orderstatus STRING,
  o_totalprice DOUBLE,
  o_orderdate DATE,
  o_orderpriority STRING,
  o_clerk STRING,
  o_shippriority INT,
  o_comment STRING
) USING iceberg;

