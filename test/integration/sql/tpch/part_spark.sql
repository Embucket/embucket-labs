-- Spark SQL DDL for TPC-H PART (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  p_partkey BIGINT,
  p_name STRING,
  p_mfgr STRING,
  p_brand STRING,
  p_type STRING,
  p_size INT,
  p_container STRING,
  p_retailprice DOUBLE,
  p_comment STRING
) USING iceberg;