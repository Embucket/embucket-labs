-- Spark SQL DDL for TPC-H PARTSUPP (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INT,
  ps_supplycost DOUBLE,
  ps_comment STRING
) USING iceberg;