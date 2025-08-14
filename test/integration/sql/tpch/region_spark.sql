-- Spark SQL DDL for TPC-H REGION (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  r_regionkey INT,
  r_name STRING,
  r_comment STRING
) USING iceberg;