-- Spark SQL DDL for TPC-H CUSTOMER (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  c_custkey BIGINT,
  c_name STRING,
  c_address STRING,
  c_nationkey INT,
  c_phone STRING,
  c_acctbal DOUBLE,
  c_mktsegment STRING,
  c_comment STRING
) USING iceberg;