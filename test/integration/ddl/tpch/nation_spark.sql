-- Spark SQL DDL for TPC-H NATION (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  n_nationkey INT,
  n_name STRING,
  n_regionkey INT,
  n_comment STRING
) USING iceberg;