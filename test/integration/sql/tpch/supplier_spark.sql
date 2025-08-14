-- Spark SQL DDL for TPC-H SUPPLIER (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  s_suppkey BIGINT,
  s_name STRING,
  s_address STRING,
  s_nationkey INT,
  s_phone STRING,
  s_acctbal DOUBLE,
  s_comment STRING
) USING iceberg;